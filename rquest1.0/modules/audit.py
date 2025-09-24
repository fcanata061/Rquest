#!/usr/bin/env python3
# rquest1.0/modules/audit.py
# -*- coding: utf-8 -*-
"""
modules/audit.py — Auditoria de sistema: integridade de arquivos, logging e eventos.

Melhorias implementadas:
 - Lê config via modules.config
 - Integra com DB (migrations defensivas) e emit_event
 - Hooks: pre_audit, on_audit_fail, post_audit
 - Tipos de evento: new, checked, modified, missing, error
 - Batch / paralelo (ThreadPoolExecutor)
 - Incremental hashing (usa mtime+size para pular recálculo)
 - cleanup_missing, audit_dir, audit_paths, export_audit_log, get_stats
 - Retenção / purge de registros antigos configurável
"""
from __future__ import annotations

import os
import sys
import time
import json
import hashlib
import logging
import threading
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# Defensive imports from project
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    LOG = get_logger("audit")
except Exception:
    LOG = logging.getLogger("rquest.audit")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)

try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    get_db = None
    def emit_event(*a, **k):
        pass

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    get_hook_manager = None

# Configuration defaults and normalization
_CFG = get_config() if callable(get_config) else {}
_AUDIT_CFG = _CFG.get("audit", {}) if isinstance(_CFG, dict) else {}

DEFAULTS = {
    "enabled": True,
    "parallel_workers": 4,
    "retention_days": 90,
    "audit_table": "audit_log",
    "max_path_length": 1024,
    "hash_block_size": 65536,
    "skip_if_unchanged_seconds": 3600,  # if file mtime+size unchanged within 1 hour, skip rehash
    "log_level": "INFO",
    "report_dir": "/var/lib/rquest/audit_reports",
    "dry_run_default": False,
}

def _cfg_get(key: str, default=None):
    return _AUDIT_CFG.get(key, DEFAULTS.get(key, default))

# Ensure report dir exists
REPORT_DIR = Path(_cfg_get("report_dir"))
try:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass

# DB DDL
_AUDIT_TABLE = _cfg_get("audit_table", "audit_log")
_SCHEMA_DDL = f"""
CREATE TABLE IF NOT EXISTS {_AUDIT_TABLE} (
    path TEXT PRIMARY KEY,
    sha256 TEXT,
    size INTEGER,
    mtime INTEGER,
    created_at INTEGER,
    last_checked INTEGER,
    event_type TEXT,
    notes TEXT
);
"""
# optional index for retention queries
_INDEX_DDL = f"CREATE INDEX IF NOT EXISTS idx_audit_last_checked ON {_AUDIT_TABLE}(last_checked);"

# Utility functions
def _now_ts() -> int:
    return int(time.time())

def _iso_now() -> str:
    return datetime.utcnow().isoformat() + "Z"

def _normalize_path(p: str) -> str:
    return os.path.normpath(str(p))

def _sha256_file(path: str, block_size: int = None) -> str:
    block_size = block_size or int(_cfg_get("hash_block_size", 65536))
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

# Auditor class
class Auditor:
    def __init__(self, db_factory=None):
        """
        Auditor reads config and sets up DB/tables and hooks.
        db_factory: optional callable returning DB connection (for testing)
        """
        self.enabled = bool(_cfg_get("enabled", True))
        self.parallel_workers = int(_cfg_get("parallel_workers", 4))
        self.retention_days = int(_cfg_get("retention_days", 90))
        self.skip_if_unchanged_seconds = int(_cfg_get("skip_if_unchanged_seconds", 3600))
        self.hash_block_size = int(_cfg_get("hash_block_size", 65536))
        self._lock = threading.RLock()

        # logging level
        try:
            lvl = getattr(logging, str(_cfg_get("log_level", "INFO")).upper(), logging.INFO)
            LOG.setLevel(lvl)
        except Exception:
            pass

        # DB
        self.db = None
        if db_factory and callable(db_factory):
            try:
                self.db = db_factory()
            except Exception:
                LOG.debug("audit: db_factory failed", exc_info=True)
                self.db = None
        elif get_db:
            try:
                self.db = get_db()()
            except Exception:
                LOG.debug("audit: get_db init failed", exc_info=True)
                self.db = None

        if self.db:
            try:
                # create table(s)
                try:
                    self.db.execute(_SCHEMA_DDL, (), commit=True)
                except TypeError:
                    # some DB wrappers expect execute(sql) only
                    try:
                        self.db.execute(_SCHEMA_DDL)
                        self.db.commit()
                    except Exception:
                        LOG.debug("audit: fallback DDL execution failed", exc_info=True)
                try:
                    self.db.execute(_INDEX_DDL, (), commit=True)
                except Exception:
                    pass
            except Exception:
                LOG.exception("audit: failed to ensure schema")

        # hooks
        self.hooks = None
        if get_hook_manager:
            try:
                self.hooks = get_hook_manager()()
            except Exception:
                LOG.debug("audit: hooks init failed", exc_info=True)
                self.hooks = None

    # Internal DB helpers (defensive)
    def _db_fetchone(self, sql: str, params: Tuple = ()):
        if not self.db:
            return None
        try:
            r = self.db.fetchone(sql, params)
            return dict(r) if r else None
        except Exception:
            LOG.debug("audit: db.fetchone failed", exc_info=True)
            return None

    def _db_execute(self, sql: str, params: Tuple = (), commit: bool = True):
        if not self.db:
            return False
        try:
            # many DB wrappers accept commit kw
            try:
                self.db.execute(sql, params, commit=commit)
            except TypeError:
                self.db.execute(sql, params)
                if commit:
                    try:
                        self.db.commit()
                    except Exception:
                        pass
            return True
        except Exception:
            LOG.exception("audit: db.execute failed: %s", sql)
            return False

    # Public API -------------------------------------------------
    def verify_file_integrity(self, fname: str, force: bool = False, dry_run: Optional[bool] = None) -> bool:
        """
        Verify a single file's integrity.
        - If no DB present, returns True (auditing disabled fallback).
        - Uses incremental hashing: if size+mtime equal DB values and not forced, skips rehash.
        - Records or updates record in audit table.
        Returns True if file is 'ok' (matches recorded hash or recorded if new), False otherwise.
        Emits events and calls hooks when discrepancies found.
        """
        dry_run = bool(_cfg_get("dry_run_default", False)) if dry_run is None else bool(dry_run)
        if not self.enabled:
            LOG.debug("audit disabled by config")
            return True
        path = _normalize_path(fname)
        if len(path) > int(_cfg_get("max_path_length", 1024)):
            LOG.warning("audit: path too long, skipping: %s", path)
            return True

        if not os.path.exists(path):
            LOG.warning("audit: file missing: %s", path)
            # mark missing in DB
            self._record_event(path, sha256=None, size=None, mtime=None, event_type="missing", notes="file-missing")
            emit_event("audit.file.missing", {"path": path})
            if self.hooks:
                try:
                    self.hooks.run("on_audit_missing", {"path": path})
                except Exception:
                    LOG.debug("audit: hook on_audit_missing failed", exc_info=True)
            return False

        # file exists
        stat = os.stat(path)
        size = stat.st_size
        mtime = int(stat.st_mtime)
        rec = self._db_fetchone(f"SELECT sha256, size, mtime, last_checked FROM {_AUDIT_TABLE} WHERE path = ? LIMIT 1", (path,))
        now_ts = _now_ts()

        # if there is a record and not forced, maybe skip rehash
        if rec and not force:
            recorded_size = rec.get("size")
            recorded_mtime = rec.get("mtime")
            last_checked = rec.get("last_checked") or 0
            # if size and mtime unchanged and last_checked within threshold, skip rehash
            if recorded_size == size and recorded_mtime == mtime:
                age = now_ts - int(last_checked)
                if age <= int(self.skip_if_unchanged_seconds):
                    # update last_checked timestamp only
                    self._db_execute(f"UPDATE {_AUDIT_TABLE} SET last_checked = ? WHERE path = ?", (now_ts, path))
                    LOG.debug("audit: skip rehash for %s (unchanged, last_checked %s seconds ago)", path, age)
                    emit_event("audit.file.checked", {"path": path, "skipped": True})
                    return True

        # compute sha256 (may be expensive)
        try:
            if dry_run:
                LOG.info("[dry-run] audit would compute sha256 for %s", path)
                # optimistic: consider ok in dry-run mode
                return True
            sha = _sha256_file(path, block_size=self.hash_block_size)
        except Exception:
            LOG.exception("audit: sha256 calculation failed for %s", path)
            self._record_event(path, sha256=None, size=size, mtime=mtime, event_type="error", notes="sha-failed")
            emit_event("audit.file.error", {"path": path, "reason": "sha_failed"})
            return False

        # if no prior record -> new
        if not rec:
            # insert record
            ok = self._db_execute(
                f"INSERT OR REPLACE INTO {_AUDIT_TABLE} (path, sha256, size, mtime, created_at, last_checked, event_type, notes) VALUES (?,?,?,?,?,?,?,?)",
                (path, sha, size, mtime, now_ts, now_ts, "new", "initial_record")
            )
            if ok:
                LOG.info("audit: new file recorded %s", path)
                emit_event("audit.file.new", {"path": path, "sha256": sha})
                if self.hooks:
                    try:
                        self.hooks.run("on_audit_new", {"path": path, "sha256": sha})
                    except Exception:
                        LOG.debug("audit: hook on_audit_new failed", exc_info=True)
            return True

        # compare with recorded hash
        old_sha = rec.get("sha256")
        if old_sha != sha:
            LOG.warning("audit: file modified: %s (old=%s new=%s)", path, old_sha, sha)
            # update record indicating modification
            self._db_execute(
                f"UPDATE {_AUDIT_TABLE} SET sha256=?, size=?, mtime=?, last_checked=?, event_type=?, notes=? WHERE path=?",
                (sha, size, mtime, now_ts, "modified", f"modified_at_{_iso_now()}", path)
            )
            emit_event("audit.file.modified", {"path": path, "old_sha": old_sha, "new_sha": sha})
            # call hooks
            if self.hooks:
                try:
                    self.hooks.run("on_audit_fail", {"path": path, "old_sha": old_sha, "new_sha": sha})
                except Exception:
                    LOG.debug("audit: hook on_audit_fail failed", exc_info=True)
            return False

        # sha matches -> update last_checked
        self._db_execute(
            f"UPDATE {_AUDIT_TABLE} SET last_checked=?, event_type=?, notes=? WHERE path=?",
            (now_ts, "checked", "ok", path)
        )
        LOG.debug("audit: file ok %s", path)
        emit_event("audit.file.ok", {"path": path})
        return True

    def audit_paths(self, paths: Iterable[str], parallel: Optional[bool] = True, dry_run: Optional[bool] = None) -> Dict[str, bool]:
        """
        Audit multiple paths. Returns mapping path -> bool (True=ok, False=failed/missing).
        parallel: whether to perform checks in parallel using ThreadPoolExecutor.
        """
        dry_run = bool(_cfg_get("dry_run_default", False)) if dry_run is None else bool(dry_run)
        results: Dict[str, bool] = {}
        paths_list = [str(p) for p in paths]

        if not paths_list:
            return results

        # call pre_audit hook
        if self.hooks:
            try:
                self.hooks.run("pre_audit", {"paths": paths_list, "dry_run": dry_run})
            except Exception:
                LOG.debug("audit: pre_audit hook failed", exc_info=True)

        if parallel:
            workers = max(1, int(self.parallel_workers))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(self.verify_file_integrity, p, False, dry_run): p for p in paths_list}
                for fut in as_completed(futures):
                    p = futures[fut]
                    try:
                        ok = fut.result()
                    except Exception:
                        LOG.exception("audit: exception auditing %s", p)
                        ok = False
                    results[p] = ok
        else:
            for p in paths_list:
                try:
                    results[p] = self.verify_file_integrity(p, False, dry_run)
                except Exception:
                    LOG.exception("audit: exception auditing %s", p)
                    results[p] = False

        # post_audit hook
        if self.hooks:
            try:
                self.hooks.run("post_audit", {"results": results})
            except Exception:
                LOG.debug("audit: post_audit hook failed", exc_info=True)

        return results

    def audit_dir(self, directory: str, recursive: bool = True, include_patterns: Optional[List[str]] = None,
                  exclude_patterns: Optional[List[str]] = None, parallel: Optional[bool] = True,
                  dry_run: Optional[bool] = None) -> Dict[str, bool]:
        """
        Audit all files under directory. Returns mapping path -> bool.
        - include_patterns, exclude_patterns are simple substrings to match path.
        """
        dry_run = bool(_cfg_get("dry_run_default", False)) if dry_run is None else bool(dry_run)
        root = Path(directory)
        if not root.exists():
            LOG.error("audit_dir: directory not found: %s", directory)
            return {}

        def should_include(p: Path) -> bool:
            s = str(p)
            if include_patterns:
                if not any(pat in s for pat in include_patterns):
                    return False
            if exclude_patterns:
                if any(pat in s for pat in exclude_patterns):
                    return False
            return True

        files = []
        if recursive:
            for dirpath, _, filenames in os.walk(root):
                for fn in filenames:
                    fp = os.path.join(dirpath, fn)
                    if should_include(Path(fp)):
                        files.append(fp)
        else:
            for entry in root.iterdir():
                if entry.is_file() and should_include(entry):
                    files.append(str(entry))

        return self.audit_paths(files, parallel=parallel, dry_run=dry_run)

    def cleanup_missing(self) -> Dict[str, Any]:
        """
        Remove DB records for files that no longer exist on disk.
        Returns summary dict with counts.
        """
        if not self.db:
            LOG.info("audit.cleanup_missing: no DB available")
            return {"deleted": 0, "checked": 0}
        try:
            rows = []
            try:
                rows = self.db.fetchall(f"SELECT path FROM {_AUDIT_TABLE}")
            except Exception:
                # fallback: try execute and iterate
                LOG.debug("audit.cleanup_missing: db.fetchall failed, trying fallback", exc_info=True)
                return {"deleted": 0, "checked": 0}
            deleted = 0
            checked = 0
            for r in rows:
                path = r.get("path") if isinstance(r, dict) else r[0]
                checked += 1
                if not os.path.exists(path):
                    try:
                        self._db_execute(f"DELETE FROM {_AUDIT_TABLE} WHERE path = ?", (path,))
                        deleted += 1
                        LOG.info("audit.cleanup_missing: removed DB entry for missing %s", path)
                    except Exception:
                        LOG.debug("audit.cleanup_missing: failed to remove entry for %s", path, exc_info=True)
            emit_event("audit.cleanup_missing", {"deleted": deleted, "checked": checked})
            return {"deleted": deleted, "checked": checked}
        except Exception:
            LOG.exception("audit.cleanup_missing failed")
            return {"deleted": 0, "checked": 0}

    def purge_old_records(self, older_than_days: Optional[int] = None) -> Dict[str, Any]:
        """
        Purge audit records older than configured retention days (based on last_checked).
        """
        days = older_than_days if older_than_days is not None else int(self.retention_days)
        cutoff = _now_ts() - int(days) * 86400
        if not self.db:
            LOG.info("audit.purge_old_records: no DB")
            return {"purged": 0}
        try:
            # count first
            try:
                rows = self.db.fetchone(f"SELECT COUNT(*) as c FROM {_AUDIT_TABLE} WHERE last_checked < ?", (cutoff,))
                cnt = rows.get("c") if rows else 0
            except Exception:
                cnt = None
            self._db_execute(f"DELETE FROM {_AUDIT_TABLE} WHERE last_checked < ?", (cutoff,))
            LOG.info("audit.purge_old_records: purged records older than %d days", days)
            emit_event("audit.purge", {"purged_estimate": cnt})
            return {"purged_estimate": cnt}
        except Exception:
            LOG.exception("audit.purge_old_records failed")
            return {"purged": 0}

    def export_audit_log(self, out_path: Optional[str] = None, fmt: str = "json") -> str:
        """
        Export audit log to file. Returns path of exported file.
        Supported formats: json (array), jsonl (one object per line), csv (basic).
        """
        out_path = out_path or str(Path(REPORT_DIR) / f"audit_export_{int(time.time())}.{fmt}")
        try:
            rows = []
            if self.db:
                try:
                    rows = self.db.fetchall(f"SELECT path, sha256, size, mtime, created_at, last_checked, event_type, notes FROM {_AUDIT_TABLE}")
                except Exception:
                    LOG.debug("audit.export: db.fetchall failed", exc_info=True)
                    rows = []
            if fmt == "json":
                data = []
                for r in rows:
                    data.append(dict(r))
                Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                Path(out_path).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            elif fmt == "jsonl":
                Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                with open(out_path, "w", encoding="utf-8") as fh:
                    for r in rows:
                        fh.write(json.dumps(dict(r), ensure_ascii=False) + "\n")
            elif fmt == "csv":
                import csv
                Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                with open(out_path, "w", newline="", encoding="utf-8") as fh:
                    writer = csv.writer(fh)
                    writer.writerow(["path", "sha256", "size", "mtime", "created_at", "last_checked", "event_type", "notes"])
                    for r in rows:
                        row = dict(r)
                        writer.writerow([row.get(k) for k in ["path", "sha256", "size", "mtime", "created_at", "last_checked", "event_type", "notes"]])
            else:
                raise ValueError("unsupported format")
            LOG.info("audit.export_audit_log: exported to %s", out_path)
            return out_path
        except Exception:
            LOG.exception("audit.export_audit_log failed")
            raise

    def get_stats(self) -> Dict[str, Any]:
        """
        Return basic stats about audit DB and config.
        """
        stats: Dict[str, Any] = {"enabled": self.enabled, "parallel_workers": self.parallel_workers}
        if not self.db:
            stats["db"] = "unavailable"
            return stats
        try:
            try:
                row = self.db.fetchone(f"SELECT COUNT(*) as total, SUM(CASE WHEN event_type='modified' THEN 1 ELSE 0 END) as modified FROM {_AUDIT_TABLE}")
                if row:
                    stats["total_records"] = int(row.get("total", 0))
                    stats["modified"] = int(row.get("modified", 0))
            except Exception:
                stats["total_records"] = None
                stats["modified"] = None
        except Exception:
            LOG.debug("audit.get_stats failed", exc_info=True)
        return stats

    # Internal recording helper
    def _record_event(self, path: str, sha256: Optional[str], size: Optional[int],
                      mtime: Optional[int], event_type: str, notes: Optional[str] = None):
        """Insert or update audit record in DB (best-effort)."""
        if not self.db:
            return False
        now = _now_ts()
        try:
            # Upsert pattern
            self._db_execute(
                f"INSERT OR REPLACE INTO {_AUDIT_TABLE} (path, sha256, size, mtime, created_at, last_checked, event_type, notes) VALUES (?,?,?,?,?,?,?,?)",
                (path, sha256, size, mtime, now, now, event_type, notes or "")
            )
            return True
        except Exception:
            LOG.exception("audit._record_event failed for %s", path)
            return False

# CLI helper for testing
def _cli(argv=None):
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-audit", description="Audit module CLI")
    ap.add_argument("--audit", nargs="*", help="files to audit")
    ap.add_argument("--audit-dir", help="directory to audit recursively")
    ap.add_argument("--export", help="export audit log (optional path)")
    ap.add_argument("--format", default="json", help="export format: json/jsonl/csv")
    ap.add_argument("--cleanup-missing", action="store_true")
    ap.add_argument("--purge-old", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    aud = Auditor(db_factory=get_db if get_db else None)
    if args.audit:
        res = aud.audit_paths(args.audit, parallel=True, dry_run=args.dry_run)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0
    if args.audit_dir:
        res = aud.audit_dir(args.audit_dir, parallel=True, dry_run=args.dry_run)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0
    if args.cleanup_missing:
        print(aud.cleanup_missing())
        return 0
    if args.purge_old:
        print(aud.purge_old_records())
        return 0
    if args.export:
        path = aud.export_audit_log(args.export, fmt=args.format)
        print("exported to", path)
        return 0
    ap.print_help()
    return 1

if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
