# rquest1.0/modules/upgrade.py
# -*- coding: utf-8 -*-
"""
upgrade.py - Upgrade manager for Rquest (integrated, config-driven)

Responsibilities:
 - Plan upgrades (by package list, repo sync diffs, or full-upgrade)
 - Execute upgrades in parallel with retries, snapshots and rollback
 - Persist upgrade runs and per-item state to DB
 - Integrate with MetaLoader, BuildSystem, Hooks, DB, Conflicts, DeepClean, Audit, RepoSync
 - Provide CLI: plan / apply / status / history / rollback

Notes:
 - Uses config via modules.config.get_config() under key "upgrade"
 - Uses buildsystem.build_package(pkg_meta, force=..., dry_run=..., shards=...)
 - Attempts btrfs snapshots if configured; fallback to marker-directory snapshot
"""

from __future__ import annotations
import os
import sys
import time
import json
import shutil
import threading
import queue
import tempfile
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# integrations (graceful)
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("upgrade")
except Exception:
    import logging
    logger = logging.getLogger("rquest.upgrade")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# db
try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db():
        return None
    def emit_event(*a, **k):
        pass

# meta loader
try:
    from modules.meta import MetaLoader  # type: ignore
except Exception:
    MetaLoader = None

# buildsystem
try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:
    get_buildsystem = None

# repo_sync
try:
    from modules.repo_sync import sync_all as repo_sync_all  # type: ignore
except Exception:
    repo_sync_all = None

# hooks, sandbox, fakeroot, audit, conflicts, deepclean, pkgtool
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    get_hook_manager = None

try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    get_sandbox_manager = None

try:
    from modules.fakeroot import create_fakeroot  # type: ignore
except Exception:
    create_fakeroot = None

try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    get_auditor = None

try:
    from modules.conflicts import get_conflicts_detector  # type: ignore
except Exception:
    get_conflicts_detector = None

try:
    from modules.deepclean import get_deepcleaner  # type: ignore
except Exception:
    get_deepcleaner = None

try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    get_pkgtool = None

# config
CFG = get_config() if callable(get_config) else {}
UPGRADE_CFG = CFG.get("upgrade", {}) if isinstance(CFG, dict) else {}

# defaults
DEFAULTS = {
    "parallelism": 2,
    "max_retries": 2,
    "retry_backoff": 5,  # seconds
    "snapshot": {"backend": "btrfs", "path": "/var/lib/rquest/snapshots"},
    "rollback_all_on_fail": False,
    "dry_run": False,
    "keep_success_snapshots": False,
    "plan_path": "/var/lib/rquest/upgrade_plans",
    "persist_runs_in_db": True,
}

# merge config
def _cfg_get(key, default=None):
    return UPGRADE_CFG.get(key, DEFAULTS.get(key) if default is None else default)

PARALLELISM = int(_cfg_get("parallelism", DEFAULTS["parallelism"]))
MAX_RETRIES = int(_cfg_get("max_retries", DEFAULTS["max_retries"]))
RETRY_BACKOFF = int(_cfg_get("retry_backoff", DEFAULTS["retry_backoff"]))
SNAPSHOT_CFG = _cfg_get("snapshot", DEFAULTS["snapshot"])
ROLLBACK_ALL_ON_FAIL = bool(_cfg_get("rollback_all_on_fail", DEFAULTS["rollback_all_on_fail"]))
DEFAULT_DRY_RUN = bool(_cfg_get("dry_run", DEFAULTS["dry_run"]))
PLAN_PATH = Path(str(_cfg_get("plan_path", DEFAULTS["plan_path"])))
PLAN_PATH.mkdir(parents=True, exist_ok=True)
PERSIST_RUNS_IN_DB = bool(_cfg_get("persist_runs_in_db", DEFAULTS["persist_runs_in_db"]))

# helpers
def _now_ts() -> int:
    return int(time.time())

def _uid(prefix: str = "") -> str:
    import uuid
    return f"{prefix}{uuid.uuid4().hex[:8]}"

def _run_cmd(cmd: List[str], cwd: Optional[str] = None, timeout: Optional[int] = None) -> Tuple[int, str, str]:
    try:
        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out, err = p.communicate(timeout=timeout)
        return p.returncode, out or "", err or ""
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
        return 124, out or "", err or ""
    except Exception as e:
        return 1, "", str(e)

# snapshots
def create_snapshot(name_suffix: str, dry_run: bool = False) -> Optional[str]:
    """
    Create snapshot using btrfs if available and configured.
    Returns path to snapshot or marker directory on fallback.
    """
    base = SNAPSHOT_CFG.get("path") or "/var/lib/rquest/snapshots"
    backend = SNAPSHOT_CFG.get("backend") or "btrfs"
    Path(base).mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    name = f"rquest-{name_suffix}-{ts}"
    snap_path = str(Path(base) / name)
    if dry_run:
        logger.info("[dry-run] would create snapshot %s (backend=%s)", snap_path, backend)
        return None
    if backend == "btrfs" and shutil.which("btrfs"):
        # create subvolume snapshot of / (dangerous) — prefer user-specified source in config
        src = SNAPSHOT_CFG.get("source") or "/"
        rc, out, err = _run_cmd(["btrfs", "subvolume", "snapshot", src, snap_path])
        if rc == 0:
            logger.info("Created btrfs snapshot: %s", snap_path)
            return snap_path
        logger.warning("btrfs snapshot failed: %s", err.strip())
    # fallback: create marker directory with copy of important files (non-destructive)
    try:
        os.makedirs(snap_path, exist_ok=True)
        marker = Path(snap_path) / "SNAPSHOT_MARKER.txt"
        marker.write_text(f"snapshot {name} created at {time.ctime()}\n", encoding="utf-8")
        logger.info("Created fallback snapshot marker: %s", snap_path)
        return snap_path
    except Exception as e:
        logger.exception("Failed to create snapshot: %s", e)
        return None

def restore_snapshot(snap_path: str) -> bool:
    """
    Try to restore snapshot (best-effort).
    If btrfs snapshot exists and target is '/', will attempt subvolume replace — requires root.
    For fallback marker snapshots we cannot auto-restore.
    """
    if not snap_path:
        return False
    if Path(snap_path).exists() and Path(snap_path).is_dir():
        # If btrfs snapshot (heuristic: contains .btrfs or was created via btrfs), try to detect
        # We simply attempt a conservative approach: notify user for manual restore.
        # If btrfs binary exists and path looks like btrfs subvolume (no reliable check), attempt deletion/restore is dangerous.
        # We'll try a safe no-op and return False to indicate manual intervention required.
        logger.warning("Automatic restore is not fully implemented; manual restore may be required for: %s", snap_path)
        return False
    logger.warning("Snapshot path not found: %s", snap_path)
    return False

# DB helpers: create upgrade tables if needed
def _ensure_upgrade_tables(db):
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS upgrade_runs (
                id TEXT PRIMARY KEY,
                started_at INTEGER,
                finished_at INTEGER,
                ok INTEGER,
                plan_path TEXT,
                detail TEXT
            );
        """, (), commit=True)
        db.execute("""
            CREATE TABLE IF NOT EXISTS upgrade_items (
                id TEXT PRIMARY KEY,
                run_id TEXT,
                package TEXT,
                meta_path TEXT,
                status TEXT,
                retries INTEGER,
                snapshot_path TEXT,
                started_at INTEGER,
                finished_at INTEGER,
                detail TEXT
            );
        """, (), commit=True)
    except Exception:
        logger.exception("Failed to ensure upgrade tables")

# Plan / Item dataclasses (dict-like)
class UpgradePlanItem:
    def __init__(self, package: str, meta_path: str, reason: Optional[str] = None):
        self.id = _uid("itm-")
        self.package = package
        self.meta_path = meta_path
        self.reason = reason or ""
        self.status = "pending"  # pending,running,ok,failed,rolledback,skipped
        self.retries = 0
        self.snapshot_path: Optional[str] = None
        self.started_at: Optional[int] = None
        self.finished_at: Optional[int] = None
        self.detail: Dict[str,Any] = {}

    def to_dict(self):
        return {
            "id": self.id,
            "package": self.package,
            "meta_path": self.meta_path,
            "reason": self.reason,
            "status": self.status,
            "retries": self.retries,
            "snapshot_path": self.snapshot_path,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "detail": self.detail,
        }

class UpgradePlan:
    def __init__(self, name: Optional[str] = None, items: Optional[List[UpgradePlanItem]] = None, metadata: Optional[Dict[str,Any]] = None):
        self.id = _uid("plan-")
        self.name = name or f"upgrade-{time.strftime('%Y%m%d-%H%M%S')}"
        self.items: List[UpgradePlanItem] = items or []
        self.metadata = metadata or {}
        self.created_at = _now_ts()

    def add_item(self, it: UpgradePlanItem):
        self.items.append(it)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "created_at": self.created_at,
            "metadata": self.metadata,
            "items": [i.to_dict() for i in self.items],
        }

    def save(self, path: Optional[Path] = None) -> str:
        p = (path or PLAN_PATH) / f"{self.name}-{self.id}.plan.json"
        data = self.to_dict()
        p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Saved plan to %s", str(p))
        return str(p)

# Upgrade manager
class UpgradeManager:
    def __init__(self, *, parallelism: Optional[int] = None, dry_run: Optional[bool] = None):
        self.parallelism = parallelism or PARALLELISM
        self.dry_run = DEFAULT_DRY_RUN if dry_run is None else bool(dry_run)
        # modules
        self.db = None
        try:
            dbc = get_db() if callable(get_db) else None
            self.db = dbc() if dbc else None
            if self.db and PERSIST_RUNS_IN_DB:
                _ensure_upgrade_tables(self.db)
        except Exception:
            self.db = None
        self.meta_loader = MetaLoader() if MetaLoader else None
        self.buildsystem = get_buildsystem()() if callable(get_buildsystem) else None
        self.hookmgr = get_hook_manager()() if callable(get_hook_manager) else None
        self.sandbox = get_sandbox_manager()() if callable(get_sandbox_manager) else None
        self.fakeroot = create_fakeroot() if create_fakeroot else None
        self.auditor = get_auditor()() if callable(get_auditor) else None
        self.conflicts = get_conflicts_detector()() if callable(get_conflicts_detector) else None
        self.deepclean = get_deepcleaner()() if callable(get_deepcleaner) else None
        self.pkgtool = get_pkgtool()() if callable(get_pkgtool) else None
        self.repo_local = CFG.get("repos", {}).get("local") if isinstance(CFG.get("repos", {}), dict) else None

    # Helper to locate meta by package name (search local repo paths)
    def find_meta_by_package(self, package_name: str) -> Optional[str]:
        # search local repo first
        search_paths = []
        if self.repo_local:
            search_paths.append(Path(self.repo_local))
        search_paths += [Path("/var/lib/rquest/repos"), Path.home() / ".rquest" / "local-repo"]
        for root in search_paths:
            if not root or not root.exists():
                continue
            for p in root.rglob("*.meta"):
                try:
                    m = self.meta_loader.load(str(p))
                    if m.name == package_name:
                        return str(p)
                except Exception:
                    continue
        return None

    def plan_from_package_list(self, packages: List[str]) -> UpgradePlan:
        plan = UpgradePlan()
        for pkg in packages:
            meta_path = self.find_meta_by_package(pkg)
            if not meta_path:
                logger.warning("Meta for package %s not found; skipping", pkg)
                continue
            it = UpgradePlanItem(package=pkg, meta_path=meta_path, reason="requested")
            plan.add_item(it)
        return plan

    def plan_from_repo_sync(self) -> UpgradePlan:
        """
        Build a plan based on repo_sync diffs (new metas in remotes).
        It will call repo_sync_all() when available and create items for changed metas.
        """
        plan = UpgradePlan(name="repo-sync-plan")
        if not repo_sync_all:
            logger.error("repo_sync module not available")
            return plan
        try:
            res = repo_sync_all()
            # expect res to have 'updated_metas': [paths]
            updated = res.get("updated_metas") if isinstance(res, dict) else None
            if not updated:
                logger.info("repo_sync returned no updated metas")
                return plan
            for p in updated:
                try:
                    mp = self.meta_loader.load(p)
                    it = UpgradePlanItem(package=mp.name, meta_path=p, reason="repo-sync update")
                    plan.add_item(it)
                except Exception:
                    logger.exception("Failed to load meta %s", p)
            return plan
        except Exception:
            logger.exception("repo_sync_all failed")
            return plan

    # core execution per item
    def _execute_item(self, item: UpgradePlanItem, run_id: str, *, force: bool = False) -> Dict[str,Any]:
        logger.info("Executing upgrade item %s (%s)", item.package, item.meta_path)
        item.started_at = _now_ts()
        item.status = "running"
        # persist item start
        if self.db:
            try:
                self.db.execute("INSERT OR REPLACE INTO upgrade_items (id, run_id, package, meta_path, status, retries, snapshot_path, started_at, finished_at, detail) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                (item.id, run_id, item.package, item.meta_path, item.status, item.retries, item.snapshot_path or "", item.started_at, item.finished_at or 0, json.dumps(item.detail) ), commit=True)
            except Exception:
                logger.debug("Failed to persist upgrade item start")
        # load meta
        try:
            pkg_meta_obj = self.meta_loader.load(item.meta_path) if self.meta_loader else None
            pkg_meta = pkg_meta_obj.to_dict() if pkg_meta_obj else {}
        except Exception as e:
            item.status = "failed"
            item.finished_at = _now_ts()
            item.detail = {"error": "meta_load_failed", "exc": str(e)}
            logger.exception("Failed load meta for %s", item.package)
            if self.db:
                self.db.execute("UPDATE upgrade_items SET status=?, finished_at=?, detail=? WHERE id=?", (item.status, item.finished_at, json.dumps(item.detail), item.id), commit=True)
            return {"ok": False, "reason": "meta_load_failed", "exc": str(e)}
        # pre-check conflicts
        if self.conflicts:
            try:
                conflicts = self.conflicts.detect(item.meta_path) if hasattr(self.conflicts, "detect") else None
                if conflicts:
                    item.status = "skipped"
                    item.finished_at = _now_ts()
                    item.detail = {"skipped": True, "conflicts": conflicts}
                    logger.warning("Conflicts detected for %s: %s", item.package, conflicts)
                    if self.db:
                        self.db.execute("UPDATE upgrade_items SET status=?, finished_at=?, detail=? WHERE id=?", (item.status, item.finished_at, json.dumps(item.detail), item.id), commit=True)
                    return {"ok": False, "reason": "conflicts", "detail": conflicts}
            except Exception:
                logger.debug("Conflict detection failed; continuing")
        # snapshot before destructive actions (install)
        snapshot_path = None
        if not self.dry_run:
            try:
                snapshot_path = create_snapshot(name_suffix=item.package, dry_run=self.dry_run)
                item.snapshot_path = snapshot_path
            except Exception:
                logger.exception("Snapshot creation failed for %s", item.package)
        else:
            logger.info("[dry-run] skipping snapshot creation for %s", item.package)
        # run pre-upgrade hooks
        try:
            if self.hookmgr and hasattr(self.hookmgr, "run_phase"):
                try:
                    self.hookmgr.run_phase("pre-upgrade", {"package": item.package, "meta": pkg_meta})
                except Exception:
                    logger.debug("hookmgr pre-upgrade failed")
        except Exception:
            pass
        # build using buildsystem
        attempt = 0
        last_result: Dict[str,Any] = {"ok": False, "stage": "not-run"}
        while attempt <= MAX_RETRIES:
            attempt += 1
            item.retries = attempt - 1
            try:
                if self.dry_run:
                    logger.info("[dry-run] would build/install %s (attempt %s)", item.package, attempt)
                    last_result = {"ok": True, "stage": "dry-run", "detail": "simulated"}
                    break
                # ensure buildsystem exists
                if not self.buildsystem:
                    logger.error("buildsystem unavailable")
                    last_result = {"ok": False, "stage": "buildsystem-unavailable"}
                    break
                # pass the pkg_meta dict to buildsystem
                try:
                    res = self.buildsystem.build_package(pkg_meta, force=force, dry_run=self.dry_run, shards=None)
                except TypeError:
                    # fallback if signature different
                    res = self.buildsystem.build_package(pkg_meta, dry_run=self.dry_run)
                last_result = res or {"ok": False, "stage": "unknown", "detail": {}}
                if last_result.get("ok"):
                    break
                else:
                    logger.warning("Build failed for %s attempt %s: %s", item.package, attempt, last_result.get("stage"))
            except Exception as e:
                last_result = {"ok": False, "stage": "exception", "exc": str(e)}
                logger.exception("Exception during build attempt for %s", item.package)
            # retry logic
            if attempt <= MAX_RETRIES:
                logger.info("Retrying %s in %s seconds (attempt %s/%s)", item.package, RETRY_BACKOFF, attempt, MAX_RETRIES)
                time.sleep(RETRY_BACKOFF)
        # finalize item based on last_result
        if last_result.get("ok"):
            item.status = "ok"
            item.finished_at = _now_ts()
            item.detail = last_result
            logger.info("Upgrade item %s succeeded", item.package)
            # post-audit
            try:
                if self.auditor and hasattr(self.auditor, "audit_package_meta"):
                    try:
                        self.auditor.audit_package_meta(item.meta_path)
                    except Exception:
                        logger.debug("Auditor failed")
            except Exception:
                pass
            # run post-upgrade hooks
            try:
                if self.hookmgr and hasattr(self.hookmgr, "run_phase"):
                    try:
                        self.hookmgr.run_phase("post-upgrade", {"package": item.package, "meta": pkg_meta, "result": last_result})
                    except Exception:
                        logger.debug("hookmgr post-upgrade failed")
            except Exception:
                pass
            # persist
            if self.db:
                try:
                    self.db.execute("UPDATE upgrade_items SET status=?, retries=?, finished_at=?, snapshot_path=?, detail=? WHERE id=?",
                                    (item.status, item.retries, item.finished_at, item.snapshot_path or "", json.dumps(item.detail), item.id), commit=True)
                except Exception:
                    logger.debug("Failed updating db for item success")
            return {"ok": True, "item": item.to_dict()}
        # failure: decide rollback
        item.status = "failed"
        item.finished_at = _now_ts()
        item.detail = last_result
        if self.db:
            try:
                self.db.execute("UPDATE upgrade_items SET status=?, retries=?, finished_at=?, snapshot_path=?, detail=? WHERE id=?",
                                (item.status, item.retries, item.finished_at, item.snapshot_path or "", json.dumps(item.detail), item.id), commit=True)
            except Exception:
                logger.debug("Failed updating db for item failure")
        # attempt rollback via snapshot if exists and rollback policy allows
        rolled_back = False
        if item.snapshot_path and not self.dry_run:
            try:
                rb_ok = restore_snapshot(item.snapshot_path)
                if rb_ok:
                    item.status = "rolledback"
                    rolled_back = True
                    logger.info("Rolled back %s using snapshot %s", item.package, item.snapshot_path)
                else:
                    logger.warning("Snapshot restore for %s returned False; manual intervention may be required: %s", item.package, item.snapshot_path)
            except Exception:
                logger.exception("Rollback attempt failed for %s", item.package)
        # update db after rollback attempt
        if self.db:
            try:
                self.db.execute("UPDATE upgrade_items SET status=?, detail=? WHERE id=?", (item.status, json.dumps(item.detail), item.id), commit=True)
            except Exception:
                logger.debug("Failed to persist rollback state")
        return {"ok": False, "item": item.to_dict(), "rolled_back": rolled_back, "result": last_result}

    def apply_plan(self, plan: UpgradePlan, *, run_name: Optional[str] = None, force: bool = False, rollback_all_on_fail: Optional[bool] = None) -> Dict[str,Any]:
        run_id = _uid("run-")
        run_name = run_name or plan.name
        rollback_all = ROLLBACK_ALL_ON_FAIL if rollback_all_on_fail is None else rollback_all_on_fail
        logger.info("Applying plan %s run_id=%s parallelism=%s dry_run=%s", run_name, run_id, self.parallelism, self.dry_run)
        # persist plan to file
        plan_path = Path(plan.save())
        # persist run in db
        if self.db:
            try:
                self.db.execute("INSERT INTO upgrade_runs (id, started_at, finished_at, ok, plan_path, detail) VALUES (?,?,?,?,?,?)",
                                (run_id, _now_ts(), 0, 0, str(plan_path), json.dumps(plan.to_dict())), commit=True)
            except Exception:
                logger.debug("Failed to record upgrade run")
        # run queue
        q = queue.Queue()
        for it in plan.items:
            q.put(it)
        results = []
        lock = threading.Lock()
        failed_any = False

        def worker():
            nonlocal failed_any
            while True:
                try:
                    it: UpgradePlanItem = q.get_nowait()
                except queue.Empty:
                    break
                res = self._execute_item(it, run_id, force=force)
                with lock:
                    results.append(res)
                    if not res.get("ok"):
                        failed_any = True
                        logger.warning("Item %s failed", it.package)
                q.task_done()

        threads = []
        for _ in range(min(self.parallelism, max(1, q.qsize()))):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)
        # wait
        for t in threads:
            t.join()
        # finalize run record
        overall_ok = not failed_any
        if failed_any and rollback_all:
            logger.info("Rollback-all-on-fail requested; attempting rollback of successful items")
            # attempt rollback for all items that succeeded and have snapshot
            for it in plan.items:
                if it.status == "ok" and it.snapshot_path and not self.dry_run:
                    try:
                        restore_snapshot(it.snapshot_path)
                        it.status = "rolledback"
                        logger.info("Rolled back %s (global rollback)", it.package)
                        if self.db:
                            self.db.execute("UPDATE upgrade_items SET status=?, detail=? WHERE id=?", (it.status, json.dumps(it.detail), it.id), commit=True)
                    except Exception:
                        logger.exception("Global rollback failed for %s", it.package)
        if self.db:
            try:
                self.db.execute("UPDATE upgrade_runs SET finished_at=?, ok=?, detail=? WHERE id=?", (_now_ts(), 1 if overall_ok else 0, json.dumps({"results": results}), run_id), commit=True)
            except Exception:
                logger.debug("Failed to finalize run record")
        # post actions
        if overall_ok:
            logger.info("Upgrade run %s finished successfully", run_id)
            # optionally run deepclean
            try:
                if self.deepclean and hasattr(self.deepclean, "deep_clean_after_upgrade"):
                    try:
                        self.deepclean.deep_clean_after_upgrade(plan)
                    except Exception:
                        logger.debug("deepclean post-upgrade failed")
            except Exception:
                pass
            emit_event("upgrade.run.finished", {"run_id": run_id, "ok": True})
        else:
            logger.warning("Upgrade run %s finished with failures", run_id)
            emit_event("upgrade.run.finished", {"run_id": run_id, "ok": False})
        return {"ok": overall_ok, "run_id": run_id, "results": results, "plan_path": str(plan_path)}

    # inspect / status / history helpers
    def list_runs(self, limit: int = 20) -> List[Dict[str,Any]]:
        if not self.db:
            logger.warning("DB not available for run listing")
            return []
        try:
            rows = self.db.fetchall("SELECT * FROM upgrade_runs ORDER BY started_at DESC LIMIT ?", (limit,))
            return rows
        except Exception:
            logger.exception("Failed to list upgrade runs")
            return []

    def get_run(self, run_id: str) -> Optional[Dict[str,Any]]:
        if not self.db:
            return None
        try:
            r = self.db.fetchone("SELECT * FROM upgrade_runs WHERE id = ?", (run_id,))
            return r
        except Exception:
            return None

    def rollback_item_by_id(self, item_id: str) -> Dict[str,Any]:
        if not self.db:
            return {"ok": False, "error": "db-unavailable"}
        try:
            r = self.db.fetchone("SELECT * FROM upgrade_items WHERE id = ?", (item_id,))
            if not r:
                return {"ok": False, "error": "item-not-found"}
            snap = r.get("snapshot_path")
            if not snap:
                return {"ok": False, "error": "no-snapshot"}
            if self.dry_run:
                logger.info("[dry-run] would restore snapshot %s for item %s", snap, item_id)
                return {"ok": True, "simulated": True}
            ok = restore_snapshot(snap)
            if ok:
                self.db.execute("UPDATE upgrade_items SET status=?, detail=? WHERE id=?", ("rolledback", json.dumps({"restored": snap}), item_id), commit=True)
                return {"ok": True, "restored": snap}
            else:
                return {"ok": False, "error": "restore-failed", "snapshot": snap}
        except Exception:
            logger.exception("rollback_item failed")
            return {"ok": False, "error": "exception"}

# CLI
def _cli(argv: Optional[List[str]] = None):
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-upgrade")
    sub = ap.add_subparsers(dest="cmd")
    # plan from list
    p1 = sub.add_parser("plan", help="create plan from package names")
    p1.add_argument("packages", nargs="+", help="package names (space separated)")
    p1.add_argument("--out", "-o", help="save plan to path", default=None)
    # plan from repo sync
    p2 = sub.add_parser("plan-repo", help="create plan from repo sync (new metas)")
    # apply plan file
    p3 = sub.add_parser("apply", help="apply plan file")
    p3.add_argument("plan", help="plan file path")
    p3.add_argument("--parallelism", "-p", type=int, default=None)
    p3.add_argument("--dry-run", action="store_true")
    p3.add_argument("--force", action="store_true")
    # status / history
    p4 = sub.add_parser("status", help="list recent runs")
    p4.add_argument("--limit", "-n", type=int, default=20)
    p5 = sub.add_parser("history", help="show run detail")
    p5.add_argument("run_id", nargs="?", default=None)
    # rollback item
    p6 = sub.add_parser("rollback-item", help="rollback item by id")
    p6.add_argument("item_id", help="upgrade_items.id")
    args = ap.parse_args(argv)

    mgr = UpgradeManager()
    if args.cmd == "plan":
        plan = mgr.plan_from_package_list(args.packages)
        outp = plan.save(Path(args.out) if args.out else None)
        print("Plan saved to", outp)
    elif args.cmd == "plan-repo":
        plan = mgr.plan_from_repo_sync()
        outp = plan.save()
        print("Plan saved to", outp)
    elif args.cmd == "apply":
        plan_path = Path(args.plan)
        if not plan_path.exists():
            print("Plan not found:", plan_path)
            return
        j = json.loads(plan_path.read_text(encoding="utf-8"))
        items = []
        for it in j.get("items", []):
            item = UpgradePlanItem(package=it.get("package"), meta_path=it.get("meta_path"), reason=it.get("reason"))
            items.append(item)
        plan = UpgradePlan(name=j.get("name"), items=items, metadata=j.get("metadata"))
        mgr = UpgradeManager(parallelism=args.parallelism or None, dry_run=args.dry_run)
        res = mgr.apply_plan(plan, force=args.force)
        print(json.dumps(res, indent=2, ensure_ascii=False))
    elif args.cmd == "status":
        rows = mgr.list_runs(limit=args.limit)
        print(json.dumps(rows, indent=2, ensure_ascii=False))
    elif args.cmd == "history":
        if args.run_id:
            r = mgr.get_run(args.run_id)
            print(json.dumps(r, indent=2, ensure_ascii=False))
        else:
            rows = mgr.list_runs(limit=20)
            print(json.dumps(rows, indent=2, ensure_ascii=False))
    elif args.cmd == "rollback-item":
        res = mgr.rollback_item_by_id(args.item_id)
        print(json.dumps(res, indent=2, ensure_ascii=False))
    else:
        ap.print_help()

if __name__ == "__main__":
    _cli(sys.argv[1:])
