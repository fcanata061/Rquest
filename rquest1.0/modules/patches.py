# Rquest/rquest1.0/modules/patches.py
"""
patches.py - PatchManager for Rquest

Responsibilities:
- Manage patch definitions (local, remote, overlays), priorities and patchsets.
- Fetch patches (uses fetcher module), verify checksum/GPG.
- Create patch queues per package/version, order by priority.
- Apply patches (with dry-run option) inside an isolated working copy (supports sandbox integration).
- Support rollback by snapshotting source tree before patching.
- Persist patch application records and patch queues in DB for audit.
- Append transparency log entries for applied/reverted patches.
- Integrations: config, logging, db, fetcher, masks, slots, buildsystem, sandbox.
"""

from __future__ import annotations

import os
import sys
import json
import time
import shutil
import tempfile
import threading
import subprocess
from typing import Any, Dict, List, Optional, Tuple

# Robust imports with fallbacks
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
                def info(self,*a,**k): pass
                def warning(self,*a,**k): pass
                def error(self,*a,**k): pass
                def debug(self,*a,**k): pass
                def exception(self,*a,**k): pass
            return _Fake()

logger = get_logger("patches")

# DB
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    try:
        from db import get_db, add_history, emit_event  # type: ignore
    except Exception:
        def get_db(): return None
        def add_history(*a, **k): pass
        def emit_event(*a, **k): pass

# fetcher
try:
    from modules.fetcher import get_fetcher_manager  # type: ignore
except Exception:
    def get_fetcher_manager(): return None

# masks and slots
try:
    from modules.masks import is_masked, get_mask_reason  # type: ignore
except Exception:
    def is_masked(*a, **k): return False
    def get_mask_reason(*a, **k): return None

try:
    from modules.slots import assign_slot  # type: ignore
except Exception:
    def assign_slot(*a, **k): return None

# sandbox integration (optional)
try:
    from modules.sandbox import run_in_sandbox  # type: ignore
except Exception:
    def run_in_sandbox(fn, *a, **k):
        # fallback: run directly
        return fn(*a, **k)

# buildsystem integration: invoked after successful patching if desired
try:
    from modules.buildsystem import notify_patches_applied  # type: ignore
except Exception:
    def notify_patches_applied(*a, **k): pass

# GPG helper reuse from fetcher if available
try:
    from modules.fetcher import _verify_gpg  # type: ignore
except Exception:
    def _verify_gpg(*a, **k):
        return (False, None)

# utilities
def _now_ts() -> int:
    return int(time.time())

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

# ---------------------------------------------------------------------
# DB schema helpers for patches
# ---------------------------------------------------------------------
def _ensure_patches_tables(db):
    if not db:
        logger.debug("No DB available to ensure patches tables.")
        return
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS patches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                package_name TEXT,
                version TEXT,
                patch_name TEXT,
                source TEXT,
                checksum TEXT,
                gpg_verified INTEGER DEFAULT 0,
                priority INTEGER DEFAULT 100,
                applied INTEGER DEFAULT 0,
                applied_at INTEGER,
                log_path TEXT,
                rollback_available INTEGER DEFAULT 0,
                transparency_id TEXT
            );
        """, (), commit=True)
        db.execute("""
            CREATE TABLE IF NOT EXISTS patch_queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                package_name TEXT,
                version TEXT,
                queue JSON,
                status TEXT DEFAULT 'pending',
                created_at INTEGER DEFAULT (strftime('%s','now')),
                updated_at INTEGER
            );
        """, (), commit=True)
        db.execute("CREATE INDEX IF NOT EXISTS idx_patches_pkg ON patches(package_name);", (), commit=True)
        db.execute("CREATE INDEX IF NOT EXISTS idx_patchqueues_pkg ON patch_queues(package_name);", (), commit=True)
    except Exception:
        logger.exception("Failed to ensure patches tables")

# ---------------------------------------------------------------------
# Patch data model (in-memory)
# ---------------------------------------------------------------------
class PatchRecord:
    def __init__(self,
                 patch_name: str,
                 source: str,
                 checksum: Optional[str] = None,
                 gpg: bool = False,
                 priority: int = 100,
                 local_path: Optional[str] = None,
                 metadata: Optional[Dict[str,Any]] = None):
        self.patch_name = patch_name
        self.source = source  # URL or local path or repo overlay id
        self.checksum = checksum
        self.gpg = bool(gpg)
        self.priority = int(priority)
        self.local_path = local_path  # path after fetching or provided if local
        self.metadata = metadata or {}

    def to_dict(self):
        return {
            "patch_name": self.patch_name,
            "source": self.source,
            "checksum": self.checksum,
            "gpg": int(self.gpg),
            "priority": self.priority,
            "local_path": self.local_path,
            "metadata": self.metadata
        }

# ---------------------------------------------------------------------
# PatchManager
# ---------------------------------------------------------------------
class PatchManager:
    def __init__(self):
        self._cfg = {}
        try:
            self._cfg = get_config() if callable(get_config) else {}
        except Exception:
            self._cfg = {}
        # load config patches section
        self._patch_cfg = {}
        try:
            if hasattr(self._cfg, "as_dict"):
                raw = self._cfg.as_dict()
                self._patch_cfg = raw.get("patches", {}) or {}
            elif isinstance(self._cfg, dict):
                self._patch_cfg = self._cfg.get("patches", {}) or {}
            else:
                self._patch_cfg = getattr(self._cfg, "patches", {}) or {}
        except Exception:
            self._patch_cfg = {}
        # db
        self._db = get_db() if callable(get_db) else None
        _ensure_patches_tables(self._db)
        # cache & work dirs
        base = os.path.abspath(self._patch_cfg.get("work_dir", os.path.join(os.getcwd(), ".rquest_patches")))
        _ensure_dir(base)
        self.work_root = base
        self.backup_root = os.path.join(self.work_root, "backups")
        _ensure_dir(self.backup_root)
        self.transparency_log = os.path.join(self.work_root, "patches_transparency.log.jsonl")
        _ensure_dir(os.path.dirname(self.transparency_log))
        open(self.transparency_log, "a").close()
        # fetcher
        self._fetcher = get_fetcher_manager() if callable(get_fetcher_manager) else None
        # concurrency
        self._lock = threading.RLock()

    # -------------------------
    # helpers: parse patch definitions from config
    # -------------------------
    def _get_rules_for_package(self, package_name: str, version: Optional[str] = None) -> List[PatchRecord]:
        """
        Read config rules and overlays to return list of PatchRecord for package/version.
        Supports both per-package 'rules' and 'patchsets' keyed by version patterns.
        """
        rules = []
        try:
            rules_section = self._patch_cfg.get("rules", {}) or {}
            pkg_rules = rules_section.get(package_name, {}) or {}
            # two modes: flat list or patchsets grouped by version
            if isinstance(pkg_rules, list):
                for p in pkg_rules:
                    pr = PatchRecord(
                        patch_name=p.get("name") or p.get("patch_name"),
                        source=p.get("url") or p.get("source") or p.get("local_path"),
                        checksum=p.get("checksum"),
                        gpg=p.get("gpg", False),
                        priority=p.get("priority", 100),
                        local_path=p.get("local_path"),
                        metadata=p
                    )
                    rules.append(pr)
            elif isinstance(pkg_rules, dict):
                # patchsets mapping
                patchsets = pkg_rules.get("patchsets", {}) or {}
                if version:
                    # try to find matching patchset key: exact, prefix, or fallback "default"
                    chosen = None
                    for key in patchsets.keys():
                        if key == version or (key.endswith("x") and version.startswith(key[:-1])):
                            chosen = key
                            break
                    if chosen is None:
                        chosen = "default" if "default" in patchsets else None
                    if chosen:
                        for p in patchsets.get(chosen, []):
                            pr = PatchRecord(
                                patch_name=p.get("name"),
                                source=p.get("url") or p.get("local_path") or p.get("source"),
                                checksum=p.get("checksum"),
                                gpg=p.get("gpg", False),
                                priority=p.get("priority", 100),
                                local_path=p.get("local_path"),
                                metadata=p
                            )
                            rules.append(pr)
                else:
                    # no version: flatten all patchsets
                    for key in patchsets.keys():
                        for p in patchsets.get(key, []):
                            pr = PatchRecord(
                                patch_name=p.get("name"),
                                source=p.get("url") or p.get("local_path") or p.get("source"),
                                checksum=p.get("checksum"),
                                gpg=p.get("gpg", False),
                                priority=p.get("priority", 100),
                                local_path=p.get("local_path"),
                                metadata=p
                            )
                            rules.append(pr)
            # overlays (repo sync) may add patches; leave room for future integration
        except Exception:
            logger.exception("Failed to load patch rules for %s", package_name)
        # sort by priority ascending (lower number = higher priority)
        rules.sort(key=lambda r: r.priority if r and getattr(r, "priority", None) is not None else 100)
        return rules

    # -------------------------
    # DB persistence helpers
    # -------------------------
    def _record_patch_db(self, package_name: str, version: str, pr: PatchRecord, applied: bool, log_path: Optional[str]=None, rollback_available: bool=False):
        if not self._db:
            return
        try:
            self._db.execute("""
                INSERT INTO patches (package_name, version, patch_name, source, checksum, gpg_verified, priority, applied, applied_at, log_path, rollback_available, transparency_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (package_name, version, pr.patch_name, pr.source, pr.checksum, int(pr.gpg), pr.priority, int(applied), (int(time.time()) if applied else None), log_path, int(rollback_available), None), commit=True)
        except Exception:
            logger.exception("Failed to insert patch record into DB for %s %s %s", package_name, version, pr.patch_name)

    def _create_queue_db(self, package_name: str, version: str, queue: List[Dict[str,Any]]):
        if not self._db:
            return None
        try:
            cur = self._db.execute("INSERT INTO patch_queues (package_name, version, queue, status, created_at) VALUES (?, ?, ?, 'pending', strftime('%s','now'))", (package_name, version, json.dumps(queue)), commit=True)
            return cur.lastrowid
        except Exception:
            logger.exception("Failed to create patch queue DB row")
            return None

    def _update_queue_db(self, queue_id: int, status: str):
        if not self._db:
            return
        try:
            self._db.execute("UPDATE patch_queues SET status = ?, updated_at = strftime('%s','now') WHERE id = ?", (status, queue_id), commit=True)
        except Exception:
            logger.exception("Failed to update patch queue status")

    def _get_queue_db(self, package_name: str, version: Optional[str]=None):
        if not self._db:
            return None
        try:
            if version:
                row = self._db.fetchone("SELECT * FROM patch_queues WHERE package_name = ? AND version = ? ORDER BY created_at DESC LIMIT 1", (package_name, version))
            else:
                row = self._db.fetchone("SELECT * FROM patch_queues WHERE package_name = ? ORDER BY created_at DESC LIMIT 1", (package_name,))
            return dict(row) if row else None
        except Exception:
            logger.exception("Failed to fetch patch queue")
            return None

    # -------------------------
    # transparency logging
    # -------------------------
    def _append_transparency(self, event: Dict[str,Any]):
        try:
            record = {"ts": _now_ts(), **event}
            with open(self.transparency_log, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:
            logger.exception("Failed to append patch transparency log")

    # -------------------------
    # fetching a patch (uses fetcher if remote)
    # -------------------------
    def fetch_patch(self, pr: PatchRecord) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Ensure patch local_path filled. Returns (ok, local_path, error).
        If pr.local_path already set and exists, treat as local file.
        If pr.source is URL, use fetcher manager to download.
        """
        # local path provided?
        if pr.local_path and os.path.exists(pr.local_path):
            return True, pr.local_path, None
        # if source looks local
        if pr.source and (pr.source.startswith("/") or pr.source.startswith("file://")):
            path = pr.source[7:] if pr.source.startswith("file://") else pr.source
            if os.path.exists(path):
                pr.local_path = path
                return True, path, None
            return False, None, "local-not-found"
        # remote: delegate to fetcher if available
        fetcher = self._fetcher if callable(get_fetcher_manager) else None
        if fetcher is None:
            # try a direct HTTP download fallback
            try:
                import urllib.request
                tmpd = tempfile.mkdtemp(prefix="patch_fetch_", dir=self.work_root)
                fname = os.path.basename(pr.source.split("?",1)[0])
                dest = os.path.join(tmpd, fname)
                urllib.request.urlretrieve(pr.source, dest)
                pr.local_path = dest
                return True, dest, None
            except Exception as e:
                logger.exception("Direct fallback fetch failed for %s", pr.source)
                return False, None, str(e)
        try:
            fm = fetcher if isinstance(fetcher, object) else get_fetcher_manager()
            res = fm.fetch_source("__patch__:"+pr.patch_name, "0", pr.source, checksums=pr.checksum, force=False)  # package_name used only for logging in fetcher
            if res.get("ok"):
                pr.local_path = res.get("path")
                return True, pr.local_path, None
            else:
                return False, None, res.get("error") or "fetch_failed"
        except Exception:
            logger.exception("fetcher.fetch_source failed for %s", pr.source)
            return False, None, "exception"

    # -------------------------
    # verify patch integrity (checksum + gpg)
    # -------------------------
    def verify_patch(self, pr: PatchRecord) -> Tuple[bool, Optional[str]]:
        """
        Returns (ok, message). Uses checksum if present and gpg if pr.gpg True.
        """
        try:
            if not pr.local_path or not os.path.exists(pr.local_path):
                ok, p, err = self.fetch_patch(pr)
                if not ok:
                    return False, f"fetch_failed:{err}"
            # checksum verification (simple: support sha256:hex or raw hex)
            if pr.checksum:
                expected = pr.checksum.strip()
                if ":" in expected:
                    alg, val = expected.split(":",1)
                    alg = alg.lower()
                else:
                    alg = "sha256"
                    val = expected
                val = val.strip().lower()
                # compute
                if alg == "sha256":
                    import hashlib
                    h = hashlib.sha256()
                    with open(pr.local_path, "rb") as f:
                        for chunk in iter(lambda: f.read(8192), b""):
                            h.update(chunk)
                    got = h.hexdigest().lower()
                    if got != val:
                        return False, f"checksum_mismatch:{alg}"
                else:
                    # unsupported alg: skip with warning
                    logger.warning("Unsupported checksum alg %s for patch %s", alg, pr.patch_name)
            # gpg verify if requested
            if pr.gpg:
                ok, key = _verify_gpg(pr.local_path)
                if not ok:
                    return False, "gpg_failed"
            return True, "ok"
        except Exception:
            logger.exception("verify_patch failed for %s", pr.patch_name)
            return False, "exception"

    # -------------------------
    # apply single patch
    # -------------------------
    def _make_backup(self, source_tree: str) -> Optional[str]:
        """
        Make a compressed backup (tar.gz) of the source_tree into backup_root and return path.
        """
        try:
            base = os.path.basename(os.path.abspath(source_tree)).rstrip("/")
            ts = int(time.time())
            backup_name = f"{base}.backup.{ts}.tar.gz"
            dst = os.path.join(self.backup_root, backup_name)
            # create tar.gz
            import tarfile
            with tarfile.open(dst, "w:gz") as tar:
                tar.add(source_tree, arcname=base)
            return dst
        except Exception:
            logger.exception("Failed to create backup for %s", source_tree)
            return None

    def _restore_backup(self, backup_path: str, restore_to: str) -> bool:
        try:
            import tarfile
            if not os.path.exists(backup_path):
                logger.warning("Backup %s not found", backup_path)
                return False
            # remove target
            if os.path.exists(restore_to):
                shutil.rmtree(restore_to)
            with tarfile.open(backup_path, "r:*") as tar:
                tar.extractall(os.path.dirname(restore_to))
            return True
        except Exception:
            logger.exception("Failed to restore backup %s", backup_path)
            return False

    def _apply_patch_command(self, patch_path: str, source_dir: str, dry_run: bool=False) -> Tuple[bool, str]:
        """
        Use 'patch -p1' or 'git apply' to apply patch. Try git apply first if .git exists.
        Returns (ok, output_or_error).
        """
        try:
            if os.path.isdir(os.path.join(source_dir, ".git")):
                # use git apply (supports --check for dry-run)
                if dry_run:
                    cmd = ["git", "-C", source_dir, "apply", "--check", patch_path]
                else:
                    cmd = ["git", "-C", source_dir, "apply", patch_path]
                proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                out = proc.stdout + proc.stderr
                ok = (proc.returncode == 0)
                return ok, out.strip()
            else:
                # fallback to patch utility
                # use -p1 by default; caller must ensure correct CWD
                if dry_run:
                    cmd = ["patch", "--dry-run", "-p1", "-i", patch_path]
                else:
                    cmd = ["patch", "-p1", "-i", patch_path]
                proc = subprocess.run(cmd, cwd=source_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                out = proc.stdout + proc.stderr
                ok = (proc.returncode == 0)
                return ok, out.strip()
        except FileNotFoundError as e:
            logger.warning("patch/git not found: %s", e)
            return False, str(e)
        except Exception:
            logger.exception("apply patch command failed")
            return False, "exception"

    def apply_patch(self, pr: PatchRecord, source_dir: str, dry_run: bool=False, sandbox: bool=False) -> Tuple[bool, str, Optional[str]]:
        """
        Apply one patch record to source_dir.
        Returns (ok, message, backup_path_or_none).
        Behavior:
          - fetch + verify patch
          - create backup of source_dir before first patch
          - apply patch (dry-run or real)
          - record DB entry and transparency log on success/fail
        """
        # sanity checks
        if is_masked(pr.patch_name, None, None):
            reason = None
            try:
                reason = get_mask_reason(pr.patch_name, None, None)
            except Exception:
                reason = "masked"
            logger.warning("Patch blocked by MASK: %s reason=%s", pr.patch_name, reason)
            return False, f"masked:{reason}", None

        # fetch and verify
        ok_fetch, local_path, err = self.fetch_patch(pr)
        if not ok_fetch:
            logger.error("Failed fetching patch %s: %s", pr.patch_name, err)
            return False, f"fetch_failed:{err}", None

        pr.local_path = local_path

        ok_verify, vmsg = self.verify_patch(pr)
        if not ok_verify:
            logger.error("Verification failed for patch %s: %s", pr.patch_name, vmsg)
            return False, f"verify_failed:{vmsg}", None

        # apply in sandbox or directly
        def _apply():
            # create backup of source_dir before applying first patch
            backup = self._make_backup(source_dir)
            if backup is None:
                logger.warning("Failed to create backup for %s before applying patch", source_dir)
            # attempt apply
            ok_apply, out = self._apply_patch_command(pr.local_path, source_dir, dry_run=dry_run)
            return ok_apply, out, backup

        if sandbox:
            try:
                ok_apply, out, backup = run_in_sandbox(_apply)
            except Exception:
                logger.exception("Sandbox application failed")
                return False, "sandbox_exception", None
        else:
            ok_apply, out, backup = _apply()

        # record DB and transparency log
        self._record_patch_db(package_name=os.path.basename(source_dir), version="", pr=pr, applied=bool(ok_apply), log_path=None, rollback_available=bool(backup))
        tlog = {"action": "apply", "patch": pr.patch_name, "package_dir": source_dir, "ok": bool(ok_apply), "output": out}
        self._append_transparency(tlog)
        # emit events/history
        add_history(pr.patch_name, "patch_apply", f"applied={ok_apply} pkg={source_dir}")
        emit_event("patch_applied", {"patch": pr.patch_name, "package_dir": source_dir, "ok": bool(ok_apply)})
        return ok_apply, out, backup if ok_apply else None

    # -------------------------
    # revert a single patch via backup (if available)
    # -------------------------
    def revert_patch(self, backup_path: str, restore_to: str) -> Tuple[bool, str]:
        """
        Restore backup to restore_to path. backup_path must be a tar archive made by _make_backup.
        """
        ok = self._restore_backup(backup_path, restore_to)
        if ok:
            tlog = {"action": "revert", "backup": backup_path, "restored_to": restore_to}
            self._append_transparency(tlog)
            add_history(os.path.basename(restore_to), "patch_revert", f"restored_from={backup_path}")
            emit_event("patch_reverted", {"backup": backup_path, "restored_to": restore_to})
            return True, "restored"
        else:
            return False, "restore_failed"

    # -------------------------
    # apply sequence (queue) for a package
    # -------------------------
    def create_patch_queue(self, package_name: str, version: Optional[str]=None) -> Dict[str,Any]:
        """
        Build a patch queue from config rules (and overlays) and persist it.
        Returns dict with queue info.
        """
        rules = self._get_rules_for_package(package_name, version)
        queue = [p.to_dict() for p in rules]
        queue_id = self._create_queue_db(package_name, version or "", queue)
        logger.info("Created patch queue for %s %s entries=%d id=%s", package_name, version, len(queue), queue_id)
        return {"id": queue_id, "package": package_name, "version": version, "queue": queue}

    def get_patch_queue(self, package_name: str, version: Optional[str]=None) -> Optional[Dict[str,Any]]:
        row = self._get_queue_db(package_name, version)
        if not row:
            return None
        try:
            return {
                "id": row["id"],
                "package_name": row["package_name"],
                "version": row["version"],
                "queue": json.loads(row["queue"]),
                "status": row["status"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"]
            }
        except Exception:
            return None

    def apply_patches(self, package_name: str, version: Optional[str], source_dir: str, dry_run: bool=False, sandbox: bool=False, abort_on_fail: Optional[bool]=None) -> Dict[str,Any]:
        """
        Apply all patches for package_name/version to source_dir.
        Returns result dict with per-patch outcomes and overall status.
        Workflow:
         - create queue if missing
         - backup once before first patch and keep backups list
         - attempt to apply patches in priority order
         - on failure: if abort_on_fail True or config says so, rollback applied patches using backups
         - return summary
        """
        with self._lock:
            res = {"package": package_name, "version": version, "applied": [], "failed": [], "skipped": [], "ok": False}
            # determine abort_on_fail default from config
            if abort_on_fail is None:
                abort_on_fail = bool(self._patch_cfg.get("global", {}).get("allow_failed", False) is False)
            # get queue
            q = self.get_patch_queue(package_name, version)
            if not q:
                q = self.create_patch_queue(package_name, version)
            queue_list = q.get("queue", [])
            backups = []
            try:
                for idx, pdict in enumerate(queue_list):
                    pr = PatchRecord(
                        patch_name=pdict.get("patch_name") or pdict.get("patch_name") or pdict.get("patch_name"),
                        source=pdict.get("source") or pdict.get("source"),
                        checksum=pdict.get("checksum"),
                        gpg=bool(pdict.get("gpg")),
                        priority=int(pdict.get("priority", 100)),
                        local_path=pdict.get("local_path"),
                        metadata=pdict.get("metadata")
                    )
                    # check masks for patch itself
                    if is_masked(pr.patch_name, version, None):
                        reason = None
                        try:
                            reason = get_mask_reason(pr.patch_name, version, None)
                        except Exception:
                            reason = "masked"
                        logger.info("Skipping masked patch %s reason=%s", pr.patch_name, reason)
                        res["skipped"].append({"patch": pr.patch_name, "reason": reason})
                        continue
                    # attempt apply
                    ok_apply, out, backup = self.apply_patch(pr, source_dir, dry_run=dry_run, sandbox=sandbox)
                    if ok_apply:
                        res["applied"].append({"patch": pr.patch_name, "output": out})
                        if backup:
                            backups.append(backup)
                    else:
                        res["failed"].append({"patch": pr.patch_name, "error": out})
                        if abort_on_fail:
                            # rollback in reverse order of backups
                            for b in reversed(backups):
                                try:
                                    self.revert_patch(b, source_dir)
                                except Exception:
                                    logger.exception("rollback failed for backup %s", b)
                            self._update_queue_db(q["id"], "failed")
                            res["ok"] = False
                            return res
                        # else continue to next patch
                # all attempted
                self._update_queue_db(q["id"], "applied")
                res["ok"] = True if not res["failed"] else (not abort_on_fail)
                # post hooks
                try:
                    notify_patches_applied(package_name, version, res)
                except Exception:
                    pass
                return res
            except Exception:
                logger.exception("apply_patches encountered exception")
                self._update_queue_db(q["id"], "failed")
                res["ok"] = False
                return res

    # -------------------------
    # audit utilities
    # -------------------------
    def audit_patches(self, package_name: str, version: Optional[str]=None) -> List[Dict[str,Any]]:
        """
        Return applied patches for package from DB for auditing.
        """
        if not self._db:
            return []
        try:
            if version:
                rows = self._db.fetchall("SELECT * FROM patches WHERE package_name = ? AND version = ? ORDER BY applied_at DESC", (package_name, version))
            else:
                rows = self._db.fetchall("SELECT * FROM patches WHERE package_name = ? ORDER BY applied_at DESC", (package_name,))
            out = []
            for r in rows:
                rec = dict(r)
                out.append(rec)
            return out
        except Exception:
            logger.exception("audit_patches db query failed")
            return []

# ---------------------------------------------------------------------
# module-level manager + convenience wrappers
# ---------------------------------------------------------------------
_MANAGER_LOCK = threading.RLock()
_MANAGER: Optional[PatchManager] = None

def get_patch_manager() -> PatchManager:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = PatchManager()
        return _MANAGER

def list_patches(package_name: str, version: Optional[str]=None):
    return get_patch_manager()._get_rules_for_package(package_name, version)

def create_patch_queue(package_name: str, version: Optional[str]=None):
    return get_patch_manager().create_patch_queue(package_name, version)

def get_patch_queue(package_name: str, version: Optional[str]=None):
    return get_patch_manager().get_patch_queue(package_name, version)

def apply_patches(package_name: str, version: Optional[str], source_dir: str, dry_run: bool=False, sandbox: bool=False, abort_on_fail: Optional[bool]=None):
    return get_patch_manager().apply_patches(package_name, version, source_dir, dry_run=dry_run, sandbox=sandbox, abort_on_fail=abort_on_fail)

def fetch_patch(*a, **k):
    pm = get_patch_manager()
    pr = PatchRecord(*a, **k) if a else None
    return pm.fetch_patch(pr) if pr else (False, None, "invalid")

def verify_patch(*a, **k):
    pm = get_patch_manager()
    return pm.verify_patch(*a, **k)

def revert_patch(*a, **k):
    return get_patch_manager().revert_patch(*a, **k)

def audit_patches(*a, **k):
    return get_patch_manager().audit_patches(*a, **k)

# ---------------------------------------------------------------------
# CLI demo
# ---------------------------------------------------------------------
if __name__ == "__main__":
    pm = get_patch_manager()
    print("PatchManager ready. work_root:", pm.work_root)
    # Example usage: list, create queue, apply to temp source
    pkg = "examplepkg"
    ver = "1.0.0"
    print("Available patch rules for", pkg, ":", [p.to_dict() for p in pm._get_rules_for_package(pkg, ver)])
    print("Creating queue...")
    q = pm.create_patch_queue(pkg, ver)
    print("Queue created:", q)
    # Create dummy source tree for demo
    demo_src = os.path.join(pm.work_root, "demo_src")
    _ensure_dir(demo_src)
    # create a simple file to patch
    with open(os.path.join(demo_src, "hello.txt"), "w", encoding="utf-8") as f:
        f.write("hello world\n")
    print("Applying patches (dry-run)...")
    result = pm.apply_patches(pkg, ver, demo_src, dry_run=True, sandbox=False, abort_on_fail=False)
    print("Dry-run result:", result)
