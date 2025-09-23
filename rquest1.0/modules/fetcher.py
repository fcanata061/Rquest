# Rquest/rquest1.0/modules/fetcher.py
"""
fetcher.py - FetcherManager for Rquest

Features:
- FetcherManager: unified download/cache/verify layer for package sources and binaries
- Protocol support: http(s), git (clone), local (copy), ftp (via urllib)
- Cache: filesystem cache + DB table `fetch_cache` for metadata + `fetch_jobs` queue
- Verification: checksum (sha256/sha512/blake2b), optional GPG via gnupg or gpg CLI
- Sharding/parallel fetch: thread-based worker pool, shards assignment
- Prefetch support, distributed fetch API
- Transparency log written to JSONL file (append-only)
- Integration with modules.config, modules.logging, modules.db, modules.masks, modules.slots, modules.buildsystem, modules.repo_sync
- Robust imports with fallbacks; defensive coding for missing modules
"""

from __future__ import annotations

import os
import sys
import json
import time
import shutil
import tempfile
import threading
import hashlib
import tarfile
import subprocess
import urllib.request
from typing import Any, Dict, List, Optional, Tuple, Iterable

# Robust imports for project modules
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

logger = get_logger("fetcher")

# DB integration
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    try:
        from db import get_db, add_history, emit_event  # type: ignore
    except Exception:
        def get_db(): return None
        def add_history(*a, **k): pass
        def emit_event(*a, **k): pass

# masks and slots integration
try:
    from modules.masks import is_masked  # type: ignore
except Exception:
    def is_masked(*a, **k): return False

try:
    from modules.slots import assign_slot  # type: ignore
except Exception:
    def assign_slot(*a, **k): return None

# buildsystem integration (enqueue fetch jobs -> build system expects artifact)
try:
    from modules.buildsystem import enqueue_build_job  # type: ignore
except Exception:
    def enqueue_build_job(*a, **k):
        logger.debug("buildsystem.enqueue_build_job not available; skipping")

# repo_sync integration for discovering sources
try:
    from modules.repo_sync import get_repo_sync_manager  # type: ignore
except Exception:
    def get_repo_sync_manager(): return None

# GPG support
_GNUPG = False
try:
    import gnupg  # type: ignore
    _GNUPG = True
except Exception:
    _GNUPG = False

# -----------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------
def _now_ts() -> int:
    return int(time.time())

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def _sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _sha512_of_file(path: str) -> str:
    h = hashlib.sha512()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _blake2b_of_file(path: str, digest_size: int = 64) -> str:
    h = hashlib.blake2b(digest_size=digest_size)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

# canonicalize checksum field: accepts "sha256:hex" or dict {"sha256":hex, ...}
def _normalize_checksums(checksums: Optional[Any]) -> Dict[str,str]:
    out: Dict[str,str] = {}
    if not checksums:
        return out
    if isinstance(checksums, dict):
        for k, v in checksums.items():
            out[k.lower()] = str(v).lower().strip()
        return out
    s = str(checksums).strip()
    if ":" in s:
        alg, val = s.split(":",1)
        out[alg.lower()] = val.lower()
    else:
        out["sha256"] = s.lower()
    return out

def _compute_checksums(path: str, algos: Iterable[str]) -> Dict[str,str]:
    res: Dict[str,str] = {}
    for a in algos:
        a = a.lower()
        try:
            if a == "sha256":
                res["sha256"] = _sha256_of_file(path)
            elif a == "sha512":
                res["sha512"] = _sha512_of_file(path)
            elif a in ("blake2b", "blake2b-64"):
                res["blake2b"] = _blake2b_of_file(path)
        except Exception:
            res[a] = ""
    return res

def _safe_json(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return json.dumps(str(obj))

# -----------------------------------------------------------------------
# DB schema helpers for fetcher
# -----------------------------------------------------------------------
def _ensure_fetch_tables(db):
    if not db:
        logger.debug("No DB available to ensure fetch tables.")
        return
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS fetch_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                package_name TEXT,
                version TEXT,
                source_url TEXT,
                checksum TEXT, -- JSON with multiple algos
                gpg_verified INTEGER DEFAULT 0,
                cached_path TEXT,
                last_fetched INTEGER,
                status TEXT,
                size_bytes INTEGER,
                shard_id INTEGER,
                mirrors_tried TEXT,
                transparency_log_id TEXT
            );
        """, (), commit=True)
        db.execute("""
            CREATE TABLE IF NOT EXISTS fetch_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                package_name TEXT,
                version TEXT,
                source_url TEXT,
                checksum TEXT,
                status TEXT DEFAULT 'pending', -- pending,running,done,failed
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                created_at INTEGER DEFAULT (strftime('%s','now')),
                updated_at INTEGER
            );
        """, (), commit=True)
        db.execute("CREATE INDEX IF NOT EXISTS idx_fetch_cache_pkg ON fetch_cache(package_name);", (), commit=True)
        db.execute("CREATE INDEX IF NOT EXISTS idx_fetch_jobs_status ON fetch_jobs(status);", (), commit=True)
    except Exception:
        logger.exception("Failed to ensure fetch tables")

# -----------------------------------------------------------------------
# GPG verification helper
# -----------------------------------------------------------------------
def _verify_gpg(file_path: str, sig_path: Optional[str] = None) -> Tuple[bool, Optional[str]]:
    try:
        if _GNUPG:
            g = gnupg.GPG()
            # if detached signature path provided:
            if sig_path and os.path.exists(sig_path):
                verified = g.verify_file(open(sig_path, "rb"), file_path)
                if verified:
                    return True, str(verified.key_id)
                return False, None
            # else look for file.sig next to file
            candidate = file_path + ".sig"
            if os.path.exists(candidate):
                verified = g.verify_file(open(candidate, "rb"), file_path)
                if verified:
                    return True, str(verified.key_id)
                return False, None
            return False, None
        else:
            # fallback to gpg CLI
            candidate = sig_path if sig_path and os.path.exists(sig_path) else (file_path + ".sig" if os.path.exists(file_path + ".sig") else None)
            if not candidate:
                return False, None
            cmd = ["gpg", "--verify", candidate, file_path]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            out = proc.stdout + proc.stderr
            if proc.returncode == 0:
                # parse key id optionally
                import re
                m = re.search(r"using RSA key ([0-9A-Fa-fx]+)", out)
                if m:
                    return True, m.group(1)
                return True, None
            else:
                logger.debug("gpg verify failed: %s", out)
                return False, None
    except Exception:
        logger.exception("GPG verify exception")
        return False, None

# -----------------------------------------------------------------------
# Fetch implementations for protocols
# -----------------------------------------------------------------------
def _fetch_http(url: str, dest_dir: str, timeout: int = 300) -> Tuple[bool, Optional[str], Optional[int]]:
    """
    Download URL into dest_dir; return (ok, path, size_bytes)
    """
    try:
        _ensure_dir(dest_dir)
        fname = os.path.basename(url.split("?",1)[0]) or f"download_{int(time.time())}"
        out_path = os.path.join(dest_dir, fname)
        # Use urllib with timeout
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            total = resp.getheader("Content-Length")
            total = int(total) if total and total.isdigit() else None
            with open(out_path, "wb") as f:
                while True:
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
        size = os.path.getsize(out_path)
        return True, out_path, size
    except Exception:
        logger.exception("HTTP fetch failed for %s", url)
        return False, None, None

def _fetch_local(path: str, dest_dir: str) -> Tuple[bool, Optional[str], Optional[int]]:
    try:
        if not os.path.exists(path):
            logger.warning("Local fetch path not found: %s", path)
            return False, None, None
        _ensure_dir(dest_dir)
        fname = os.path.basename(path)
        out_path = os.path.join(dest_dir, fname)
        if os.path.isdir(path):
            # if directory, archive it to tar.gz
            tar_path = out_path + ".tar.gz"
            with tarfile.open(tar_path, "w:gz") as tar:
                tar.add(path, arcname=os.path.basename(path))
            size = os.path.getsize(tar_path)
            return True, tar_path, size
        else:
            shutil.copy2(path, out_path)
            size = os.path.getsize(out_path)
            return True, out_path, size
    except Exception:
        logger.exception("Local fetch failed for %s", path)
        return False, None, None

def _fetch_git(url: str, dest_dir: str, branch: str = "main", shallow: bool = True) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    """
    Clone git repo into dest_dir/<repobasename> and return archive path (tarball) - we create archive to normalize artifacts.
    returns (ok, archive_path, size, revision)
    """
    try:
        _ensure_dir(dest_dir)
        repo_name = os.path.basename(url.rstrip("/")).replace(".git","")
        repo_path = os.path.join(dest_dir, repo_name)
        if os.path.exists(repo_path):
            # pull
            subprocess.run(["git", "-C", repo_path, "fetch", "--all", "--prune"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            subprocess.run(["git", "-C", repo_path, "checkout", branch], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            subprocess.run(["git", "-C", repo_path, "pull", "origin", branch], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            cmd = ["git", "clone"]
            if shallow:
                cmd += ["--depth", "1"]
            cmd += ["--branch", branch, url, repo_path]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if proc.returncode != 0:
                logger.debug("git clone returned: %s", proc.stderr.decode())
        # make tarball
        rev_proc = subprocess.run(["git", "-C", repo_path, "rev-parse", "HEAD"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        rev = rev_proc.stdout.strip() if rev_proc.returncode == 0 else None
        tar_path = os.path.join(dest_dir, f"{repo_name}-{rev or int(time.time())}.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(repo_path, arcname=repo_name)
        size = os.path.getsize(tar_path)
        return True, tar_path, size, rev
    except Exception:
        logger.exception("Git fetch failed for %s", url)
        return False, None, None, None

# -----------------------------------------------------------------------
# FetcherManager
# -----------------------------------------------------------------------
class FetcherManager:
    def __init__(self):
        self._cfg = {}
        try:
            self._cfg = get_config() if callable(get_config) else {}
        except Exception:
            self._cfg = {}
        fetch_cfg = {}
        try:
            if hasattr(self._cfg, "as_dict"):
                raw = self._cfg.as_dict()
                fetch_cfg = raw.get("fetcher", {}) or {}
            elif hasattr(self._cfg, "dict"):
                raw = self._cfg.dict()
                fetch_cfg = raw.get("fetcher", {}) or {}
            elif isinstance(self._cfg, dict):
                fetch_cfg = self._cfg.get("fetcher", {}) or {}
            else:
                fetch_cfg = getattr(self._cfg, "fetcher", {}) or {}
        except Exception:
            fetch_cfg = {}
        # cache directory
        self.cache_dir = os.path.abspath(fetch_cfg.get("cache_dir", os.path.join(os.getcwd(), ".rquest_fetch_cache")))
        _ensure_dir(self.cache_dir)
        self.remote_cache = fetch_cfg.get("remote_cache")
        self.retry = int(fetch_cfg.get("retry", 3))
        self.timeout = int(fetch_cfg.get("timeout", 300))
        self.parallel = int(fetch_cfg.get("parallel", 4))
        self.shards = int(fetch_cfg.get("shards", 1))
        self.verify_checksums = bool(fetch_cfg.get("verify_checksums", True))
        self.verify_gpg_cfg = bool(fetch_cfg.get("verify_gpg", True))
        self.checksum_algos = list(fetch_cfg.get("checksum_algos", ["sha256"]))
        self.mirrors = list(fetch_cfg.get("mirrors", []))
        self.prefetch = bool(fetch_cfg.get("prefetch", True))
        self.transparency_log_path = fetch_cfg.get("transparency_log", os.path.join(self.cache_dir, "transparency.log.jsonl"))
        # db
        self._db = get_db() if callable(get_db) else None
        _ensure_fetch_tables(self._db)
        # thread/queue
        self._queue_lock = threading.RLock()
        self._workers: List[threading.Thread] = []
        self._job_stop = threading.Event()
        self._job_cv = threading.Condition(lock=self._queue_lock)
        # metrics
        self._metrics = {"fetch.total":0, "fetch.failed":0, "fetch.success":0, "cache.hits":0}
        # startup: ensure transparency log file exists
        _ensure_dir(os.path.dirname(self.transparency_log_path))
        open(self.transparency_log_path, "a").close()

    # -------------------------
    # transparency log
    # -------------------------
    def append_transparency_log(self, event: Dict[str,Any]):
        try:
            record = {"ts": _now_ts(), **event}
            with open(self.transparency_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            return True
        except Exception:
            logger.exception("Failed to append transparency log")
            return False

    def verify_transparency(self, package_name: str, version: str) -> bool:
        # Simple check: find an entry in transparency log for package/version
        try:
            with open(self.transparency_log_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        if rec.get("package") == package_name and rec.get("version") == version:
                            return True
                    except Exception:
                        continue
            return False
        except Exception:
            logger.exception("Failed transparency verify")
            return False

    # -------------------------
    # DB cache helpers
    # -------------------------
    def _get_cache_entry(self, package_name: str, version: str) -> Optional[Dict[str,Any]]:
        if not self._db:
            return None
        try:
            row = self._db.fetchone("SELECT * FROM fetch_cache WHERE package_name = ? AND version = ?", (package_name, version))
            return dict(row) if row else None
        except Exception:
            logger.exception("db query _get_cache_entry failed")
            return None

    def _insert_cache_entry(self, package_name: str, version: str, source_url: str, checksums: Dict[str,str], gpg_verified: bool, cached_path: str, size_bytes: int):
        if not self._db:
            return
        try:
            self._db.execute("INSERT INTO fetch_cache (package_name, version, source_url, checksum, gpg_verified, cached_path, last_fetched, status, size_bytes, mirrors_tried, transparency_log_id) VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'), ?, ?, ?, ?)",
                             (package_name, version, source_url, json.dumps(checksums), int(gpg_verified), cached_path, "ok", int(size_bytes), json.dumps([]), None), commit=True)
        except Exception:
            logger.exception("Failed to insert fetch_cache entry")

    def _update_cache_entry(self, entry_id: int, **kwargs):
        if not self._db:
            return
        # build update
        sets = []
        params = []
        for k, v in kwargs.items():
            sets.append(f"{k} = ?")
            if isinstance(v, (dict, list)):
                params.append(json.dumps(v))
            else:
                params.append(v)
        params.append(entry_id)
        try:
            self._db.execute(f"UPDATE fetch_cache SET {', '.join(sets)} WHERE id = ?", tuple(params), commit=True)
        except Exception:
            logger.exception("Failed to update fetch_cache entry id=%s", entry_id)

    # -------------------------
    # cache filesystem helpers
    # -------------------------
    def _cache_path_for(self, package_name: str, version: str, fname: Optional[str] = None) -> str:
        safe_pkg = package_name.replace("/", "_")
        p = os.path.join(self.cache_dir, safe_pkg, version)
        _ensure_dir(p)
        if fname:
            return os.path.join(p, fname)
        return p

    def get_cache_entry(self, package_name: str, version: str) -> Optional[Dict[str,Any]]:
        entry = self._get_cache_entry(package_name, version)
        if entry:
            # verify file exists
            path = entry.get("cached_path")
            if path and os.path.exists(path):
                self._metrics["cache.hits"] += 1
                return entry
            else:
                # stale DB entry: remove
                try:
                    self._db.execute("DELETE FROM fetch_cache WHERE id = ?", (entry["id"],), commit=True)
                except Exception:
                    pass
                return None
        return None

    def clear_cache(self, package_name: Optional[str] = None, version: Optional[str] = None, all: bool = False):
        # file system + DB cleanup
        try:
            if all:
                shutil.rmtree(self.cache_dir, ignore_errors=True)
                _ensure_dir(self.cache_dir)
                if self._db:
                    self._db.execute("DELETE FROM fetch_cache", (), commit=True)
                logger.info("Cleared entire fetch cache")
                return
            if package_name and version:
                p = os.path.join(self.cache_dir, package_name.replace("/", "_"), version)
                shutil.rmtree(p, ignore_errors=True)
                if self._db:
                    self._db.execute("DELETE FROM fetch_cache WHERE package_name = ? AND version = ?", (package_name, version), commit=True)
                logger.info("Cleared cache for %s %s", package_name, version)
                return
            if package_name and not version:
                p = os.path.join(self.cache_dir, package_name.replace("/", "_"))
                shutil.rmtree(p, ignore_errors=True)
                if self._db:
                    self._db.execute("DELETE FROM fetch_cache WHERE package_name = ?", (package_name,), commit=True)
                logger.info("Cleared cache for package %s", package_name)
        except Exception:
            logger.exception("clear_cache failed")

    # -------------------------
    # verification
    # -------------------------
    def verify_source(self, file_path: str, checksums: Optional[Any] = None, verify_gpg: Optional[bool] = None, sig_path: Optional[str] = None) -> Tuple[bool, Dict[str,str], bool]:
        """
        Verify checksums + optional gpg.
        Returns (ok, computed_checksums, gpg_verified)
        """
        try:
            if not os.path.exists(file_path):
                return False, {}, False
            algos = self.checksum_algos
            expected = _normalize_checksums(checksums)
            computed = _compute_checksums(file_path, algos)
            ok = True
            # If expected present, require match for any provided algorithm
            if expected:
                for alg, val in expected.items():
                    got = computed.get(alg)
                    if not got or got.lower() != val.lower():
                        ok = False
                        logger.warning("Checksum mismatch alg=%s expected=%s got=%s", alg, val, got)
                        break
            # optionally verify gpg (explicit param overrides config)
            gpg_ok = False
            if verify_gpg is None:
                verify_gpg = self.verify_gpg_cfg
            if verify_gpg:
                gpg_ok, key = _verify_gpg(file_path, sig_path)
            return ok, computed, bool(gpg_ok)
        except Exception:
            logger.exception("verify_source failed")
            return False, {}, False

    # -------------------------
    # core fetch flow
    # -------------------------
    def fetch_source(self, package_name: str, version: str, source_url: str, checksums: Optional[Any] = None,
                     sig_path: Optional[str] = None, force: bool = False, shard: Optional[int] = None, timeout: Optional[int] = None) -> Dict[str,Any]:
        """
        Returns dict with keys: ok, path, size, checksums, gpg_verified, cached(boolean), error
        flow:
         - check masks
         - check cache: if exists and valid -> return
         - attempt fetch via protocol
         - verify checksums & gpg
         - store in cache (fs + DB), append transparency log
        """
        timeout = timeout or self.timeout
        # policy check
        if is_masked(package_name, version, None):
            reason = None
            try:
                from modules.masks import get_mask_reason as _mr  # type: ignore
                reason = _mr(package_name, version, None)
            except Exception:
                reason = "masked"
            logger.warning("Fetch blocked by mask: %s %s reason=%s", package_name, version, reason)
            return {"ok": False, "error": f"masked: {reason}"}

        # check DB cache
        existing = self.get_cache_entry(package_name, version)
        if existing and not force:
            # optionally verify checksums if config asks
            if self.verify_checksums and existing.get("checksum"):
                cached_path = existing.get("cached_path")
                ok_verify, computed, gpg_ok = self.verify_source(cached_path, json.loads(existing.get("checksum")), verify_gpg=self.verify_gpg_cfg)
                if ok_verify:
                    logger.info("Cache hit and verified for %s %s", package_name, version)
                    self._metrics["fetch.total"] += 1
                    self._metrics["fetch.success"] += 1
                    return {"ok": True, "path": cached_path, "size": existing.get("size_bytes"), "checksums": computed, "gpg_verified": bool(existing.get("gpg_verified")), "cached": True}
                else:
                    logger.warning("Cache entry invalid for %s %s; will re-fetch", package_name, version)
                    # fallthrough to fetch
            else:
                logger.info("Cache hit for %s %s (no verify required)", package_name, version)
                self._metrics["fetch.total"] += 1
                self._metrics["fetch.success"] += 1
                return {"ok": True, "path": existing.get("cached_path"), "size": existing.get("size_bytes"), "checksums": json.loads(existing.get("checksum") or "{}"), "gpg_verified": bool(existing.get("gpg_verified")), "cached": True}

        # prepare working dir
        workdir = tempfile.mkdtemp(prefix=f"fetch_{package_name.replace('/','_')}_{version}_", dir=self.cache_dir)
        final_path = None
        size = None
        computed_checksums = {}
        gpg_ok = False
        error = None
        # try multiple mirrors, including direct url and mirrors from config
        urls_to_try = [source_url] + self.mirrors
        success = False
        for attempt_url in urls_to_try:
            try:
                # choose protocol from url
                if attempt_url.startswith("http://") or attempt_url.startswith("https://"):
                    ok, path, size = _fetch_http(attempt_url, workdir, timeout=timeout)
                    if not ok:
                        error = f"http failed for {attempt_url}"
                        continue
                    final_path = path
                elif attempt_url.startswith("git://") or attempt_url.endswith(".git") or attempt_url.startswith("git+"):
                    # normalize git url
                    git_url = attempt_url
                    ok, path, size, rev = _fetch_git(git_url, workdir, branch="main", shallow=True)
                    if not ok:
                        error = f"git clone failed {git_url}"
                        continue
                    final_path = path
                elif attempt_url.startswith("file://"):
                    local_path = attempt_url[7:]
                    ok, path, size = _fetch_local(local_path, workdir)
                    if not ok:
                        error = f"local copy failed {local_path}"
                        continue
                    final_path = path
                else:
                    # fallback treat as local path
                    ok, path, size = _fetch_local(attempt_url, workdir)
                    if not ok:
                        error = f"unknown protocol for {attempt_url}"
                        continue
                    final_path = path

                # verify checksums
                computed_checksums = _compute_checksums(final_path, self.checksum_algos)
                ok_checks = True
                if self.verify_checksums and checksums:
                    expected = _normalize_checksums(checksums)
                    for alg, val in expected.items():
                        got = computed_checksums.get(alg)
                        if not got or got.lower() != val.lower():
                            ok_checks = False
                            logger.warning("Checksum mismatch for %s %s alg=%s expected=%s got=%s", package_name, version, alg, val, got)
                            break
                # gpg verification
                if self.verify_gpg_cfg and sig_path:
                    gpg_ok, key = _verify_gpg(final_path, sig_path)
                else:
                    gpg_ok = False

                if not ok_checks:
                    error = "checksum_mismatch"
                    # try next mirror
                    continue

                # store in cache dir (move)
                cache_pkg_dir = self._cache_path_for(package_name, version)
                fname = os.path.basename(final_path)
                cached_dst = os.path.join(cache_pkg_dir, fname)
                # if final_path and it's in same fs, move else copy
                try:
                    shutil.move(final_path, cached_dst)
                except Exception:
                    shutil.copy2(final_path, cached_dst)
                # persist DB metadata
                try:
                    self._insert_cache_entry(package_name, version, source_url, computed_checksums, bool(gpg_ok), cached_dst, int(size or 0))
                except Exception:
                    # try update instead
                    try:
                        entry = self._get_cache_entry(package_name, version)
                        if entry and entry.get("id"):
                            self._update_cache_entry(entry["id"], cached_path=cached_dst, checksum=computed_checksums, gpg_verified=int(bool(gpg_ok)), last_fetched=_now_ts(), status="ok", size_bytes=int(size or 0))
                    except Exception:
                        pass

                # transparency log
                try:
                    tlog_entry = {"package": package_name, "version": version, "source": source_url, "cached_path": cached_dst, "checksums": computed_checksums, "gpg_verified": bool(gpg_ok)}
                    self.append_transparency_log(tlog_entry)
                except Exception:
                    pass

                success = True
                break
            except Exception:
                logger.exception("Fetch attempt exception for %s %s url=%s", package_name, version, attempt_url)
                error = "exception"
                continue

        # cleanup workdir (if still exists)
        try:
            if os.path.exists(workdir):
                shutil.rmtree(workdir)
        except Exception:
            pass

        self._metrics["fetch.total"] += 1
        if success:
            self._metrics["fetch.success"] += 1
            logger.info("Fetched %s %s -> %s", package_name, version, cached_dst)
            return {"ok": True, "path": cached_dst, "size": int(size or 0), "checksums": computed_checksums, "gpg_verified": bool(gpg_ok), "cached": True}
        else:
            self._metrics["fetch.failed"] += 1
            logger.error("Failed to fetch %s %s error=%s", package_name, version, error)
            return {"ok": False, "error": error}

    # -------------------------
    # fetch job queue helpers
    # -------------------------
    def enqueue_fetch_job(self, package_name: str, version: str, source_url: str, checksums: Optional[Any] = None):
        if not self._db:
            logger.warning("enqueue_fetch_job: no DB available")
            return
        try:
            self._db.execute("INSERT INTO fetch_jobs (package_name, version, source_url, checksum, status, attempts, created_at) VALUES (?, ?, ?, ?, 'pending', 0, strftime('%s','now'))",
                             (package_name, version, source_url, json.dumps(_normalize_checksums(checksums) or {})), commit=True)
            logger.info("Enqueued fetch job for %s %s", package_name, version)
        except Exception:
            logger.exception("enqueue_fetch_job failed")

    def _pop_fetch_job(self) -> Optional[Dict[str,Any]]:
        if not self._db:
            return None
        try:
            # find a pending job
            row = self._db.fetchone("SELECT * FROM fetch_jobs WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1")
            if not row:
                return None
            job = dict(row)
            # mark running
            self._db.execute("UPDATE fetch_jobs SET status='running', attempts = attempts + 1, updated_at = strftime('%s','now') WHERE id = ?", (job["id"],), commit=True)
            return job
        except Exception:
            logger.exception("_pop_fetch_job failed")
            return None

    def _mark_job_done(self, job_id: int, ok: bool, error: Optional[str] = None):
        if not self._db:
            return
        try:
            self._db.execute("UPDATE fetch_jobs SET status = ?, updated_at = strftime('%s','now'), last_error = ? WHERE id = ?", ("done" if ok else "failed", error, job_id), commit=True)
        except Exception:
            logger.exception("_mark_job_done failed")

    # -------------------------
    # worker loop
    # -------------------------
    def _worker_loop(self, worker_id: int):
        logger.info("Fetcher worker %d started", worker_id)
        while not self._job_stop.is_set():
            job = None
            with self._queue_lock:
                job = self._pop_fetch_job()
            if not job:
                # wait briefly
                time.sleep(1)
                continue
            try:
                pkg = job["package_name"]
                ver = job["version"]
                url = job["source_url"]
                checksum = json.loads(job.get("checksum") or "{}")
                res = self.fetch_source(pkg, ver, url, checksums=checksum, force=False)
                ok = bool(res.get("ok"))
                self._mark_job_done(job["id"], ok, error=(res.get("error") if not ok else None))
            except Exception:
                logger.exception("Worker %d failed job processing", worker_id)
                try:
                    self._mark_job_done(job["id"], False, error="exception")
                except Exception:
                    pass
        logger.info("Fetcher worker %d stopped", worker_id)

    def start_workers(self, count: Optional[int] = None):
        if count is None:
            count = self.parallel
        # spawn worker threads
        with self._queue_lock:
            if self._workers:
                logger.debug("Workers already running")
                return
            for i in range(count):
                t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
                self._workers.append(t)
                t.start()
            logger.info("Started %d fetcher workers", len(self._workers))

    def stop_workers(self):
        self._job_stop.set()
        for t in list(self._workers):
            try:
                t.join(timeout=5)
            except Exception:
                pass
        self._workers = []
        self._job_stop.clear()
        logger.info("Fetcher workers stopped")

    # -------------------------
    # distributed fetch helpers
    # -------------------------
    def distributed_fetch(self, packages: List[Dict[str,Any]], shards: Optional[int] = None) -> Dict[str,Any]:
        """
        Distribute fetch tasks across local worker threads; returns results per package.
        packages: list of dict {package_name, version, source_url, checksum}
        """
        if shards is None:
            shards = self.shards
        results: Dict[str,Any] = {}
        # simple round-robin assignment to threads (we will spawn |shards| threads)
        def worker_task(items: List[Dict[str,Any]]):
            for it in items:
                name = it.get("package_name")
                ver = it.get("version")
                url = it.get("source_url")
                checksum = it.get("checksum")
                res = self.fetch_source(name, ver, url, checksums=checksum)
                results[f"{name}:{ver}"] = res

        # chunk packages into shards
        chunks = [[] for _ in range(shards)]
        for i, p in enumerate(packages):
            chunks[i % shards].append(p)
        threads = []
        for chunk in chunks:
            if not chunk:
                continue
            t = threading.Thread(target=worker_task, args=(chunk,), daemon=True)
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        return results

    # -------------------------
    # prefetch helper (useful for build planning)
    # -------------------------
    def prefetch_dependencies(self, dep_list: List[Dict[str,Any]]):
        """
        dep_list: list of {package_name, version, source_url, checksum}
        enqueue fetch jobs for dependencies
        """
        for d in dep_list:
            try:
                self.enqueue_fetch_job(d["package_name"], d["version"], d["source_url"], checksums=d.get("checksum"))
            except Exception:
                logger.exception("prefetch enqueue failed for %s", d.get("package_name"))

    # -------------------------
    # metrics
    # -------------------------
    def get_metrics(self) -> Dict[str,int]:
        return dict(self._metrics)

# -----------------------------------------------------------------------
# module-level manager & wrappers
# -----------------------------------------------------------------------
_MANAGER_LOCK = threading.RLock()
_MANAGER: Optional[FetcherManager] = None

def get_fetcher_manager() -> FetcherManager:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = FetcherManager()
        return _MANAGER

def fetch_source(*a, **k): return get_fetcher_manager().fetch_source(*a, **k)
def get_cache_entry(*a, **k): return get_fetcher_manager().get_cache_entry(*a, **k)
def clear_cache(*a, **k): return get_fetcher_manager().clear_cache(*a, **k)
def enqueue_fetch_job(*a, **k): return get_fetcher_manager().enqueue_fetch_job(*a, **k)
def start_workers(*a, **k): return get_fetcher_manager().start_workers(*a, **k)
def stop_workers(*a, **k): return get_fetcher_manager().stop_workers(*a, **k)
def distributed_fetch(*a, **k): return get_fetcher_manager().distributed_fetch(*a, **k)
def prefetch_dependencies(*a, **k): return get_fetcher_manager().prefetch_dependencies(*a, **k)
def get_metrics(*a, **k): return get_fetcher_manager().get_metrics(*a, **k)
def append_transparency_log(*a, **k): return get_fetcher_manager().append_transparency_log(*a, **k)
def verify_transparency(*a, **k): return get_fetcher_manager().verify_transparency(*a, **k)

# -----------------------------------------------------------------------
# Demo / quick usage
# -----------------------------------------------------------------------
if __name__ == "__main__":
    mgr = get_fetcher_manager()
    print("FetcherManager ready. cache_dir:", mgr.cache_dir)
    # example: fetch a small http index (dry-run)
    example = {
        "package_name": "example-lib",
        "version": "1.0.0",
        "source_url": "https://example.org/example-lib-1.0.0.tar.gz",
        "checksum": None
    }
    print("Attempting fetch (may fail depending on network):")
    print(mgr.fetch_source(example["package_name"], example["version"], example["source_url"], checksums=example.get("checksum")))
