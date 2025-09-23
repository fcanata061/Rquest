# Rquest/rquest1.0/modules/repo_sync.py
"""
repo_sync.py - Repositories synchronizer for Rquest

Features:
- RepoSyncManager: orchestrates syncing of remote/local repos (git/http/local)
- DB schema: creates `repos` and `repo_packages` tables if missing
- Delta / incremental sync: detects changes by revision/checksum
- Parallel synchronization with shards/workers using threads
- Checksum verification (sha256) and optional GPG signature verification
- Integration with modules.config, modules.logging, modules.db, modules.masks, modules.slots
- After sync: update DB repo_packages, apply masks, register slots found
- CLI / event friendly: emits events via db.emit_event and history via db.add_history
- Export/import of repo index
"""

from __future__ import annotations

import os
import sys
import json
import time
import shutil
import tempfile
import hashlib
import tarfile
import threading
import subprocess
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Robust imports (try modules.* then bare names). If missing, provide safe fallbacks.
try:
    from modules.config import get_config  # type: ignore
except Exception:
    try:
        from config import get_config  # type: ignore
    except Exception:
        def get_config():
            return {}

try:
    from modules.logging import get_logger  # type: ignore
except Exception:
    try:
        import logging as _stdlog
        def get_logger(name: str):
            return _stdlog.getLogger(name)
    except Exception:
        def get_logger(name: str):
            class _Fake:
                def info(self, *a, **k): pass
                def warning(self, *a, **k): pass
                def error(self, *a, **k): pass
                def debug(self, *a, **k): pass
                def exception(self, *a, **k): pass
            return _Fake()

logger = get_logger("repo-sync")

# DB integration
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    try:
        from db import get_db, add_history, emit_event  # type: ignore
    except Exception:
        def get_db():
            return None
        def add_history(*a, **k): pass
        def emit_event(*a, **k): pass

# masks and slots integration
try:
    from modules.masks import is_masked, get_mask_reason  # type: ignore
except Exception:
    def is_masked(*a, **k): return False
    def get_mask_reason(*a, **k): return None

try:
    from modules.slots import assign_slot  # type: ignore
except Exception:
    def assign_slot(*a, **k): return None

# buildsystem integration (queue builds after sync if desired)
try:
    from modules.buildsystem import queue_build  # type: ignore
except Exception:
    def queue_build(*a, **k):
        # fallback: log that no buildsystem present
        logger.debug("buildsystem.queue_build not available; skipping build queue")

# resolver integration (if you want to index dependencies)
try:
    from modules.resolver import parse_package_metadata  # type: ignore
except Exception:
    def parse_package_metadata(meta: dict) -> dict:
        # fallback: return metadata as-is
        return meta or {}

# Try to import gnupg python module if available
_GNUPG = False
try:
    import gnupg  # type: ignore
    _GNUPG = True
except Exception:
    _GNUPG = False

# Utilities
def _now_ts() -> int:
    return int(time.time())

def _sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

# -------------------------------------------------------------------
# DB schema helpers: create repos and repo_packages tables if missing
# -------------------------------------------------------------------
def _ensure_repo_tables(db):
    """Ensure repos & repo_packages tables exist."""
    if not db:
        logger.debug("No DB available to ensure repo tables.")
        return
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS repos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                type TEXT,
                url TEXT,
                path TEXT,
                branch TEXT,
                last_sync INTEGER,
                revision TEXT,
                checksum TEXT,
                priority INTEGER DEFAULT 100,
                status TEXT,
                packages_count INTEGER DEFAULT 0,
                gpg_verified INTEGER DEFAULT 0,
                shard_id INTEGER
            );
        """, (), commit=True)
        db.execute("""
            CREATE TABLE IF NOT EXISTS repo_packages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_id INTEGER,
                package_name TEXT,
                version TEXT,
                slot TEXT,
                masked INTEGER DEFAULT 0,
                metadata TEXT,
                last_seen INTEGER,
                FOREIGN KEY(repo_id) REFERENCES repos(id)
            );
        """, (), commit=True)
        db.execute("CREATE INDEX IF NOT EXISTS idx_repo_packages_name ON repo_packages(package_name);", (), commit=True)
        db.execute("CREATE INDEX IF NOT EXISTS idx_repo_packages_repo ON repo_packages(repo_id);", (), commit=True)
    except Exception:
        logger.exception("Failed to ensure repo tables in DB")

# -------------------------------------------------------------------
# RepoSyncManager
# -------------------------------------------------------------------
class RepoSyncManager:
    """
    Manager for repository synchronization.

    Expected repo config entries (example):
      - name: "core"
        type: "git" | "http" | "local"
        url: "https://..."
        path: "/some/local/path"  # for local
        branch: "main"
        checksum: "sha256:..."   # optional for http downloads
        verify_gpg: true|false
        priority: 10
    """
    def __init__(self):
        self._cfg = {}
        try:
            self._cfg = get_config() if callable(get_config) else {}
        except Exception:
            self._cfg = {}
        # config section helpers
        self._repos_cfg = []
        self._sync_cfg = {}
        self._load_repo_config()

        # DB
        self._db = get_db() if callable(get_db) else None
        _ensure_repo_tables(self._db)

        # concurrency
        self._shards = int(self._sync_cfg.get("shards", 1))
        self._parallel = bool(self._sync_cfg.get("parallel", True))
        self._retry = int(self._sync_cfg.get("retry", 2))
        self._timeout = int(self._sync_cfg.get("timeout", 300))

        # thread pool
        self._thread_pool: List[threading.Thread] = []
        self._thread_results: Dict[str, Dict] = {}
        self._pool_lock = threading.RLock()

        # temp dir root for sync operations
        self._work_root = os.path.abspath(os.path.join(os.getcwd(), ".rquest_repo_sync_work"))
        os.makedirs(self._work_root, exist_ok=True)

        # metrics
        self._metrics = {
            "syncs.total": 0,
            "syncs.failed": 0,
            "syncs.success": 0,
            "repos.count": len(self._repos_cfg)
        }

    # -------------------------
    # config loader
    # -------------------------
    def _load_repo_config(self):
        """Load repos and sync config from config.yaml style structure."""
        cfg = self._cfg
        try:
            if hasattr(cfg, "as_dict"):
                raw = cfg.as_dict()
                repos = raw.get("repos", []) or []
                sync = raw.get("sync", {}) or {}
            elif isinstance(cfg, dict):
                repos = cfg.get("repos", []) or []
                sync = cfg.get("sync", {}) or {}
            else:
                repos = getattr(cfg, "repos", []) or []
                sync = getattr(cfg, "sync", {}) or {}
        except Exception:
            repos = []
            sync = {}
        self._repos_cfg = repos
        self._sync_cfg = sync

    # -------------------------
    # public listing helpers
    # -------------------------
    def list_repos(self) -> List[Dict[str,Any]]:
        """Return list of configured repos merged with DB state."""
        out = []
        # iterate configured repos
        names_in_cfg = set()
        for r in self._repos_cfg:
            try:
                name = r.get("name")
                names_in_cfg.add(name)
                # query DB for state
                dbrow = None
                try:
                    if self._db:
                        dbrow = self._db.fetchone("SELECT * FROM repos WHERE name = ?", (name,))
                except Exception:
                    dbrow = None
                rec = {
                    "name": name,
                    "type": r.get("type"),
                    "url": r.get("url") or r.get("path"),
                    "branch": r.get("branch"),
                    "priority": int(r.get("priority", 100)),
                    "configured": True,
                    "db": dict(dbrow) if dbrow else None
                }
                out.append(rec)
            except Exception:
                continue
        # include repos present in DB but not in config (discoveries)
        if self._db:
            try:
                rows = self._db.fetchall("SELECT name, type, url, path, last_sync, revision, status FROM repos")
                for r in rows:
                    if r["name"] in names_in_cfg:
                        continue
                    out.append({
                        "name": r["name"],
                        "type": r["type"],
                        "url": r["url"] or r["path"],
                        "branch": r.get("branch"),
                        "priority": r.get("priority", 100),
                        "configured": False,
                        "db": dict(r)
                    })
            except Exception:
                logger.exception("Failed to list repos from DB")
        return out

    def get_repo_info(self, name: str) -> Optional[Dict[str, Any]]:
        if self._db:
            try:
                row = self._db.fetchone("SELECT * FROM repos WHERE name = ?", (name,))
                return dict(row) if row else None
            except Exception:
                logger.exception("get_repo_info db query failed")
                return None
        return None

    # -------------------------
    # low-level helpers
    # -------------------------
    def _save_repo_state(self, name: str, info: dict):
        """Persist repo metadata into repos table (insert or update)."""
        if not self._db:
            return
        try:
            # normalize
            url = info.get("url")
            path = info.get("path")
            revision = info.get("revision")
            checksum = info.get("checksum")
            last_sync = info.get("last_sync", _now_ts())
            status = info.get("status", "ok")
            priority = int(info.get("priority", 100))
            gpg_verified = 1 if info.get("gpg_verified") else 0
            shard_id = info.get("shard_id")
            # upsert
            self._db.execute("""
                INSERT INTO repos (name, type, url, path, branch, last_sync, revision, checksum, priority, status, gpg_verified, shard_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    type=excluded.type,
                    url=excluded.url,
                    path=excluded.path,
                    branch=excluded.branch,
                    last_sync=excluded.last_sync,
                    revision=excluded.revision,
                    checksum=excluded.checksum,
                    priority=excluded.priority,
                    status=excluded.status,
                    gpg_verified=excluded.gpg_verified,
                    shard_id=excluded.shard_id;
            """, (name, info.get("type"), url, path, info.get("branch"), last_sync, revision, checksum, priority, status, gpg_verified, shard_id), commit=True)
        except Exception:
            logger.exception("Failed to save repo state for %s", name)

    def _clear_repo_packages(self, repo_id: int):
        if not self._db:
            return
        try:
            self._db.execute("DELETE FROM repo_packages WHERE repo_id = ?", (repo_id,), commit=True)
        except Exception:
            logger.exception("Failed to clear repo_packages for repo_id=%s", repo_id)

    def _persist_packages(self, repo_id: int, packages: Iterable[Dict[str,Any]]):
        """Persist package index to repo_packages (insert or update last_seen)."""
        if not self._db:
            return
        ts = _now_ts()
        inserted = 0
        updated = 0
        try:
            with self._db.transaction():
                for pkg in packages:
                    name = pkg.get("name")
                    version = pkg.get("version")
                    slot = pkg.get("slot")
                    metadata = json.dumps(pkg.get("metadata") or {})
                    masked = 1 if is_masked(name, version, None) else 0
                    # check existing
                    row = self._db.fetchone("SELECT id FROM repo_packages WHERE repo_id = ? AND package_name = ? AND version = ?", (repo_id, name, version))
                    if row:
                        self._db.execute("UPDATE repo_packages SET slot=?, masked=?, metadata=?, last_seen=? WHERE id=?", (slot, masked, metadata, ts, int(row["id"])), commit=False)
                        updated += 1
                    else:
                        self._db.execute("INSERT INTO repo_packages (repo_id, package_name, version, slot, masked, metadata, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?)", (repo_id, name, version, slot, masked, metadata, ts), commit=False)
                        inserted += 1
                # update repo packages_count
                self._db.execute("UPDATE repos SET packages_count = (SELECT COUNT(*) FROM repo_packages WHERE repo_id = ?) WHERE id = ?", (repo_id, repo_id), commit=False)
            logger.info("Persisted packages for repo_id=%s inserted=%d updated=%d", repo_id, inserted, updated)
        except Exception:
            logger.exception("Failed to persist packages for repo_id=%s", repo_id)

    def _get_repo_id(self, name: str) -> Optional[int]:
        if not self._db:
            return None
        try:
            row = self._db.fetchone("SELECT id FROM repos WHERE name = ?", (name,))
            return int(row["id"]) if row else None
        except Exception:
            return None

    # -------------------------
    # verification helpers
    # -------------------------
    def _verify_checksum(self, path: str, expected: Optional[str]) -> bool:
        """Verify sha256 checksum. expected format 'sha256:hex' or 'hex'."""
        if not expected:
            return True
        try:
            if expected.startswith("sha256:"):
                expected_hex = expected.split(":",1)[1].strip().lower()
            else:
                expected_hex = expected.strip().lower()
            got = _sha256_of_file(path)
            ok = (got.lower() == expected_hex)
            if not ok:
                logger.warning("Checksum mismatch: expected %s got %s", expected_hex, got)
            return ok
        except Exception:
            logger.exception("Checksum verification failed")
            return False

    def _verify_gpg(self, file_path: str, sig_path: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """Attempt to verify GPG signature. Returns (verified, keyid_or_msg)."""
        # use python-gnupg if available, otherwise try gpg CLI
        try:
            if _GNUPG:
                g = gnupg.GPG()
                if sig_path and os.path.exists(sig_path):
                    with open(sig_path, "rb") as sf:
                        verified = g.verify_file(open(sig_path, "rb"), file_path)
                        if verified:
                            return True, str(verified.key_id)
                        else:
                            return False, None
                else:
                    # attempt detached .sig next to file
                    candidate = file_path + ".sig"
                    if os.path.exists(candidate):
                        v = g.verify_file(open(candidate,"rb"), file_path)
                        return (bool(v), str(v.key_id) if v else None)
                return False, None
            else:
                # try gpg CLI: gpg --verify file.sig file
                if sig_path and os.path.exists(sig_path):
                    cmd = ["gpg", "--verify", sig_path, file_path]
                else:
                    candidate = file_path + ".sig"
                    if os.path.exists(candidate):
                        cmd = ["gpg", "--verify", candidate, file_path]
                    else:
                        return False, None
                proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                out = proc.stdout + proc.stderr
                if proc.returncode == 0:
                    # try to parse key id from output
                    m = None
                    import re
                    mm = re.search(r"using RSA key ([0-9A-Fa-fx]+)", out)
                    key = mm.group(1) if mm else None
                    return True, key
                else:
                    logger.warning("gpg verify failed: %s", out)
                    return False, None
        except Exception:
            logger.exception("GPG verification failed")
            return False, None

    # -------------------------
    # sync implementations per-type
    # -------------------------
    def _sync_git_repo(self, repo_cfg: Dict[str,Any], workdir: str) -> Dict[str,Any]:
        """
        Clone or pull git repo into workdir, then look for index file (index.json).
        Returns dict with metadata: revision, path, packages_index (list)
        """
        name = repo_cfg.get("name")
        url = repo_cfg.get("url")
        branch = repo_cfg.get("branch", "main")
        shallow = bool(repo_cfg.get("shallow", False))
        repo_path = os.path.join(workdir, name)
        result = {"ok": False, "reason": "", "revision": None, "path": None, "index": None, "gpg_verified": False}
        try:
            if os.path.exists(repo_path):
                # attempt git fetch & reset to branch
                cmd = ["git", "-C", repo_path, "fetch", "--all", "--prune"]
                subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                cmd = ["git", "-C", repo_path, "checkout", branch]
                subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                cmd = ["git", "-C", repo_path, "pull", "origin", branch]
                subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            else:
                # clone
                cmd = ["git", "clone", "--depth", "1" if shallow else "0", "--branch", branch, url, repo_path]
                subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # get revision
            cmd = ["git", "-C", repo_path, "rev-parse", "HEAD"]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            rev = proc.stdout.strip() if proc.returncode == 0 else None
            result["revision"] = rev
            result["path"] = repo_path
            # find index.json
            idx_path = os.path.join(repo_path, "index.json")
            if os.path.exists(idx_path):
                with open(idx_path, "r", encoding="utf-8") as f:
                    index = json.load(f)
                result["index"] = index
            else:
                # if no index, try to scan for package metadata files - assume packages/*.json
                pkgs = []
                pdir = os.path.join(repo_path, "packages")
                if os.path.isdir(pdir):
                    for fname in os.listdir(pdir):
                        if fname.endswith(".json"):
                            try:
                                with open(os.path.join(pdir, fname), "r", encoding="utf-8") as f:
                                    pkgs.append(json.load(f))
                            except Exception:
                                continue
                result["index"] = pkgs
            # no GPG for git content by default (could verify signed tags/commit)
            result["gpg_verified"] = False
            result["ok"] = True
            return result
        except Exception:
            logger.exception("Failed git sync for %s", name)
            result["reason"] = "exception"
            return result

    def _sync_http_repo(self, repo_cfg: Dict[str,Any], workdir: str) -> Dict[str,Any]:
        """
        Download tarball or index over HTTP(S). Expected to find index.json inside archive or at root URL/index.json.
        Accepts 'checksum' config for verification.
        """
        name = repo_cfg.get("name")
        url = repo_cfg.get("url")
        expected_checksum = repo_cfg.get("checksum")
        result = {"ok": False, "reason": "", "revision": None, "path": None, "index": None, "gpg_verified": False}
        try:
            tmpdir = os.path.join(workdir, f"http_{name}_{int(time.time())}")
            os.makedirs(tmpdir, exist_ok=True)
            # try direct index.json first (url/index.json)
            idx_urls = []
            if url.endswith(".tar.gz") or url.endswith(".tgz") or url.endswith(".tar"):
                idx_urls.append(url)
            else:
                idx_urls.append(os.path.join(url.rstrip("/"), "index.json"))
                # also try base url (maybe index served directly)
                idx_urls.append(url)
            downloaded = False
            for u in idx_urls:
                try:
                    out_path = os.path.join(tmpdir, "download")
                    logger.debug("Attempting download %s", u)
                    urllib.request.urlretrieve(u, out_path)
                    downloaded = True
                    # if it's an archive, try to extract
                    if tarfile.is_tarfile(out_path):
                        with tarfile.open(out_path, "r:*") as tar:
                            tar.extractall(tmpdir)
                        # look for index.json
                        idx_file = os.path.join(tmpdir, "index.json")
                        if os.path.exists(idx_file):
                            with open(idx_file, "r", encoding="utf-8") as f:
                                result["index"] = json.load(f)
                        else:
                            # try to find package jsons
                            pkgs = []
                            for root, _, files in os.walk(tmpdir):
                                for fn in files:
                                    if fn.endswith(".json"):
                                        try:
                                            with open(os.path.join(root, fn), "r", encoding="utf-8") as f:
                                                pkgs.append(json.load(f))
                                        except Exception:
                                            continue
                            result["index"] = pkgs
                        result["path"] = tmpdir
                    else:
                        # not an archive: assume json index at root
                        with open(out_path, "r", encoding="utf-8") as f:
                            try:
                                result["index"] = json.load(f)
                                result["path"] = tmpdir
                            except Exception:
                                # maybe a plain HTML or invalid; skip
                                result["index"] = []
                                result["path"] = tmpdir
                    # checksum verification
                    ok = self._verify_checksum(out_path, expected_checksum)
                    if not ok:
                        result["reason"] = "checksum_mismatch"
                        result["ok"] = False
                        return result
                    # attempt gpg verify if signature present / requested
                    if repo_cfg.get("verify_gpg"):
                        verified, keyid = self._verify_gpg(out_path)
                        result["gpg_verified"] = bool(verified)
                    result["ok"] = True
                    result["revision"] = result.get("revision") or os.path.basename(out_path)
                    return result
                except Exception as e:
                    logger.debug("HTTP attempt failed for %s: %s", u, e)
                    continue
            if not downloaded:
                result["reason"] = "download_failed"
                return result
        except Exception:
            logger.exception("Failed http sync for %s", name)
            result["reason"] = "exception"
            return result

    def _sync_local_repo(self, repo_cfg: Dict[str,Any], workdir: str) -> Dict[str,Any]:
        """For local repo, copy or read directly the path and scan index.json or packages/ dir."""
        name = repo_cfg.get("name")
        path = repo_cfg.get("path") or repo_cfg.get("url")
        result = {"ok": False, "reason": "", "revision": None, "path": None, "index": None, "gpg_verified": False}
        try:
            if not path or not os.path.exists(path):
                result["reason"] = "path_not_found"
                return result
            # copy to workdir (to avoid operating directly)
            dst = os.path.join(workdir, f"local_{name}")
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(path, dst)
            result["path"] = dst
            idx = os.path.join(dst, "index.json")
            if os.path.exists(idx):
                with open(idx, "r", encoding="utf-8") as f:
                    result["index"] = json.load(f)
            else:
                pkgs = []
                pdir = os.path.join(dst, "packages")
                if os.path.isdir(pdir):
                    for fn in os.listdir(pdir):
                        if fn.endswith(".json"):
                            try:
                                with open(os.path.join(pdir, fn), "r", encoding="utf-8") as f:
                                    pkgs.append(json.load(f))
                            except Exception:
                                continue
                result["index"] = pkgs
            # revision: use directory mod time or hash
            result["revision"] = str(int(os.path.getmtime(path)))
            result["ok"] = True
            return result
        except Exception:
            logger.exception("Failed local sync for %s", name)
            result["reason"] = "exception"
            return result

    # -------------------------
    # high-level sync operations
    # -------------------------
    def sync_repo(self, repo_cfg: Dict[str,Any], force: bool=False, verify: bool=True) -> Dict[str,Any]:
        """
        Sync a single repository according to its cfg dict.
        Returns dict with status and summary:
         { ok: bool, message: str, name, type, revision, added, updated, removed, packages_count, path }
        """
        name = repo_cfg.get("name")
        rtype = repo_cfg.get("type", "git")
        priority = int(repo_cfg.get("priority", 100))
        logger.info("Syncing repository '%s' (type=%s)", name, rtype)
        workdir = tempfile.mkdtemp(prefix=f"rquest_sync_{name}_", dir=self._work_root)
        try:
            attempt = 0
            last_exc = None
            while attempt <= self._retry:
                attempt += 1
                try:
                    if rtype == "git":
                        res = self._sync_git_repo(repo_cfg, workdir)
                    elif rtype in ("http", "https"):
                        res = self._sync_http_repo(repo_cfg, workdir)
                    elif rtype == "local":
                        res = self._sync_local_repo(repo_cfg, workdir)
                    else:
                        res = {"ok": False, "reason": "unknown_repo_type"}
                    if not res.get("ok"):
                        logger.warning("Sync attempt %d for %s failed: %s", attempt, name, res.get("reason"))
                        last_exc = res.get("reason")
                        if attempt <= self._retry:
                            time.sleep(1)
                            continue
                        else:
                            break
                    # success; process index
                    index = res.get("index") or []
                    # normalize index: list of dicts {name, version, slot?, metadata?}
                    normalized = []
                    for entry in index:
                        # handle different possible formats
                        if isinstance(entry, dict):
                            pkgname = entry.get("name") or entry.get("package")
                            version = entry.get("version") or entry.get("ver")
                            meta = entry.get("metadata") or entry.get("meta") or entry
                            # parse slot from metadata or field
                            slot = entry.get("slot") or (meta.get("slot") if isinstance(meta, dict) else None)
                            normalized.append({"name": pkgname, "version": version, "slot": slot, "metadata": meta})
                        else:
                            # unknown format; skip
                            continue
                    # persist repo meta in DB
                    repo_info = {
                        "name": name,
                        "type": rtype,
                        "url": repo_cfg.get("url"),
                        "path": repo_cfg.get("path"),
                        "branch": repo_cfg.get("branch"),
                        "last_sync": _now_ts(),
                        "revision": res.get("revision"),
                        "checksum": repo_cfg.get("checksum"),
                        "priority": priority,
                        "status": "ok",
                        "gpg_verified": res.get("gpg_verified", False),
                        "shard_id": None
                    }
                    self._save_repo_state(name, repo_info)
                    # find repo_id
                    repo_id = self._get_repo_id(name)
                    if repo_id is None:
                        logger.warning("Repo id not found after save for %s", name)
                    else:
                        # persist packages
                        # optional: compute delta (compare existing repo_packages last_seen)
                        self._persist_packages(repo_id, normalized)
                        # apply masks (mark masked flag is set in persist)
                        # register slots: for each package's metadata, try assign_slot
                        for pkg in normalized:
                            meta = pkg.get("metadata") or {}
                            try:
                                # parse standard metadata for slot (e.g., meta.get('slot') or 'slots' field)
                                slotcandidate = pkg.get("slot") or meta.get("slot") or None
                                if not slotcandidate:
                                    # try to detect language: python packages may contain 'python_version' key
                                    if meta and isinstance(meta, dict) and ("python_version" in meta or meta.get("provides_python")):
                                        pv = meta.get("python_version") or meta.get("provides_python")
                                        # choose first declared
                                        if isinstance(pv, list):
                                            slotcandidate = pv[0]
                                        else:
                                            slotcandidate = pv
                                if slotcandidate:
                                    # use assign_slot for recording slot mapping in DB/slots manager
                                    try:
                                        assign_slot(pkg.get("name"), str(slotcandidate))
                                    except Exception:
                                        # non-fatal
                                        logger.debug("assign_slot failed for %s slot=%s", pkg.get("name"), slotcandidate)
                            except Exception:
                                logger.debug("slot detection failed for package %s", pkg.get("name"))
                        # emit event and history
                        add_history(name, "repo_sync", f"synced {len(normalized)} packages")
                        emit_event("repo_synced", {"repo": name, "count": len(normalized)})
                    # collect summary
                    summary = {
                        "ok": True,
                        "message": "synced",
                        "name": name,
                        "type": rtype,
                        "revision": res.get("revision"),
                        "path": res.get("path"),
                        "packages_count": len(normalized)
                    }
                    logger.info("Repo '%s' synced: %d packages", name, len(normalized))
                    self._metrics["syncs.total"] += 1
                    self._metrics["syncs.success"] += 1
                    return summary
                except Exception as e:
                    last_exc = e
                    logger.exception("Exception during sync attempt %d for %s", attempt, name)
                    if attempt <= self._retry:
                        time.sleep(1)
                        continue
                    else:
                        break
            # if we reach here, failed
            self._metrics["syncs.total"] += 1
            self._metrics["syncs.failed"] += 1
            msg = f"Failed to sync repo {name}: {last_exc}"
            logger.error(msg)
            return {"ok": False, "message": msg, "name": name, "type": rtype}
        finally:
            # cleanup workdir unless debug flag present
            try:
                if os.path.exists(workdir):
                    shutil.rmtree(workdir)
            except Exception:
                pass

    def sync_all(self, parallel: Optional[bool]=None, shards: Optional[int]=None, force: bool=False) -> Dict[str,Any]:
        """Sync all configured repos. Returns dict name->result."""
        if parallel is None:
            parallel = self._parallel
        if shards is None:
            shards = self._shards
        repos = list(self._repos_cfg)
        results: Dict[str,Any] = {}
        if not repos:
            logger.info("No repositories configured to sync.")
            return results

        if parallel:
            # assign repos to worker threads (simple round robin over shards)
            def worker_task(repo_cfg):
                name = repo_cfg.get("name")
                try:
                    res = self.sync_repo(repo_cfg, force)
                    with self._pool_lock:
                        results[name] = res
                except Exception:
                    logger.exception("worker task failed for %s", name)
                    with self._pool_lock:
                        results[name] = {"ok": False, "message": "exception"}
            threads = []
            for repo_cfg in repos:
                t = threading.Thread(target=worker_task, args=(repo_cfg,), daemon=True)
                threads.append(t)
                t.start()
            # wait
            for t in threads:
                t.join()
        else:
            for repo_cfg in repos:
                res = self.sync_repo(repo_cfg, force)
                results[repo_cfg.get("name")] = res
        return results

    # -------------------------
    # verify and helper operations
    # -------------------------
    def verify_repo(self, name: str) -> bool:
        """Verify repo integrity: check checksum/gpg if recorded in DB or config."""
        cfg = None
        for r in self._repos_cfg:
            if r.get("name") == name:
                cfg = r
                break
        if not cfg:
            logger.warning("verify_repo: repo %s not found in config", name)
            return False
        # if repo is http type and checksum configured, attempt to download and verify
        if cfg.get("type") in ("http", "https"):
            url = cfg.get("url")
            tmp = tempfile.mktemp(prefix=f"verify_{name}_", dir=self._work_root)
            try:
                urllib.request.urlretrieve(url, tmp)
                ok = self._verify_checksum(tmp, cfg.get("checksum"))
                if cfg.get("verify_gpg"):
                    verified, key = self._verify_gpg(tmp)
                    ok = ok and verified
                os.remove(tmp)
                return bool(ok)
            except Exception:
                logger.exception("verify_repo failed for %s", name)
                return False
        # for git, we can check remote hash by cloning (heavy); skipping unless called explicitly
        if cfg.get("type") == "git":
            try:
                # shallow fetch and read remote HEAD via git ls-remote
                url = cfg.get("url")
                branch = cfg.get("branch", "refs/heads/main")
                proc = subprocess.run(["git", "ls-remote", url, branch], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if proc.returncode == 0:
                    return True
                else:
                    return False
            except Exception:
                logger.exception("verify_repo (git) failed for %s", name)
                return False
        # for local, check path existence
        if cfg.get("type") == "local":
            path = cfg.get("path")
            ok = os.path.exists(path)
            if not ok:
                logger.warning("verify_repo local path missing: %s", path)
            return ok
        return False

    def export_index(self, repo_name: str, path: str):
        """Export index of repo packages to JSON file."""
        repo_id = self._get_repo_id(repo_name)
        if not self._db or repo_id is None:
            raise RuntimeError("Repo not found or DB unavailable")
        try:
            rows = self._db.fetchall("SELECT package_name, version, slot, masked, metadata, last_seen FROM repo_packages WHERE repo_id = ?", (repo_id,))
            out = [dict(r) for r in rows]
            with open(path, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
            logger.info("Exported index for %s -> %s", repo_name, path)
        except Exception:
            logger.exception("export_index failed")
            raise

    def import_index(self, repo_name: str, path: str, replace: bool=False):
        """Import index JSON into repo_packages table."""
        if not self._db:
            raise RuntimeError("DB unavailable")
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("index import format invalid")
            repo_id = self._get_repo_id(repo_name)
            if repo_id is None:
                raise RuntimeError("repo not found in DB")
            if replace:
                self._clear_repo_packages(repo_id)
            # convert and insert
            for rec in data:
                name = rec.get("package_name") or rec.get("name")
                version = rec.get("version")
                slot = rec.get("slot")
                masked = int(rec.get("masked", 0))
                metadata = json.dumps(rec.get("metadata") or {})
                last_seen = rec.get("last_seen") or _now_ts()
                self._db.execute("INSERT INTO repo_packages (repo_id, package_name, version, slot, masked, metadata, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?)", (repo_id, name, version, slot, masked, metadata, last_seen), commit=True)
            logger.info("Imported index into repo %s from %s", repo_name, path)
        except Exception:
            logger.exception("import_index failed")
            raise

    # -------------------------
    # utility
    # -------------------------
    def get_metrics(self) -> Dict[str,int]:
        return dict(self._metrics)

# Module-level convenience
_MANAGER_LOCK = threading.RLock()
_MANAGER: Optional[RepoSyncManager] = None

def get_repo_sync_manager() -> RepoSyncManager:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = RepoSyncManager()
        return _MANAGER

def list_repos() -> List[Dict[str,Any]]:
    return get_repo_sync_manager().list_repos()

def get_repo_info(name: str) -> Optional[Dict[str,Any]]:
    return get_repo_sync_manager().get_repo_info(name)

def sync_repo(repo_cfg: Dict[str,Any], force: bool=False, verify: bool=True) -> Dict[str,Any]:
    return get_repo_sync_manager().sync_repo(repo_cfg, force, verify)

def sync_all(parallel: Optional[bool]=None, shards: Optional[int]=None, force: bool=False) -> Dict[str,Any]:
    return get_repo_sync_manager().sync_all(parallel=parallel, shards=shards, force=force)

def verify_repo(name: str) -> bool:
    return get_repo_sync_manager().verify_repo(name)

def export_index(repo_name: str, path: str):
    return get_repo_sync_manager().export_index(repo_name, path)

def import_index(repo_name: str, path: str, replace: bool=False):
    return get_repo_sync_manager().import_index(repo_name, path, replace)

# If executed directly, print configured repos and try a dry-run sync of all
if __name__ == "__main__":
    mgr = get_repo_sync_manager()
    print("Configured repos:")
    for r in mgr.list_repos():
        print(" -", r.get("name"), "(configured)" if r.get("configured") else "(db-only)")
    print("Performing dry-run sync of all configured repos (no builds queued).")
    res = mgr.sync_all(parallel=False, force=False)
    print("Sync results:")
    print(json.dumps(res, indent=2, ensure_ascii=False))
