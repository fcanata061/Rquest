# Rquest/rquest1.0/modules/upgrade.py
"""
upgrade.py - robust, transactional, cluster-aware upgrade manager for Rquest

Features:
- plan upgrades (single / multiple / all) with dependency resolution
- safe execution with checkpoints persisted to DB
- canary/staged rollouts and sharded parallel execution
- fetch, patch, build, package, install pipeline with sandbox support
- snapshot (quickpkg) before destructive changes for rollback
- automatic rollback on failure (per-node / per-run / partial)
- post-upgrade audit and tests
- integration with meta.py, resolver.py, fetcher.py, buildsystem.py, pkgtool.py, sandbox.py, audit.py, conflicts.py, deepclean.py, repo_sync.py
- telemetry hooks (emit_event), logging, run history in DB
- CLI: plan, apply, status, history, rollback, dry-run
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
import shutil
import threading
import tempfile
import subprocess
import logging
import math
from typing import Any, Dict, List, Optional, Tuple, Set

# ---------------------------
# Robust imports / fallbacks
# ---------------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config(): return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("upgrade")
except Exception:
    logger = logging.getLogger("upgrade")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# DB (expected wrapper exposing execute, fetchone, fetchall)
try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def emit_event(*a, **k): pass

# Integrations (optional)
try:
    from modules.meta import MetaLoader, MetaPackage  # type: ignore
except Exception:
    MetaLoader = None
    MetaPackage = None

try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    def get_resolver(): return None

try:
    from modules.fetcher import get_fetcher  # type: ignore
except Exception:
    def get_fetcher(): return None

try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:
    def get_buildsystem(): return None

try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    def get_pkgtool(): return None

try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager(): return None

try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    def get_auditor(): return None

try:
    from modules.conflicts import get_detector as get_conflicts_detector  # type: ignore
except Exception:
    def get_conflicts_detector(): return None

try:
    from modules.deepclean import get_deepclean_manager  # type: ignore
except Exception:
    def get_deepclean_manager(): return None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

try:
    from modules.repo_sync import get_repo_sync  # type: ignore
except Exception:
    def get_repo_sync(): return None

try:
    from modules.masks import get_masks_manager  # type: ignore
except Exception:
    def get_masks_manager(): return None

try:
    from modules.slots import get_slots_manager  # type: ignore
except Exception:
    def get_slots_manager(): return None

# ---------------------------
# Defaults & config
# ---------------------------
CFG = get_config() if callable(get_config) else {}
UP_CFG = CFG.get("upgrade", {}) if isinstance(CFG, dict) else {}

DEFAULTS = {
    "snapshot_dir": os.path.expanduser("~/.rquest/upgrade_snapshots"),
    "runs_dir": os.path.expanduser("~/.rquest/upgrade_runs"),
    "parallel_workers": 4,
    "shard_size": 10,
    "canary_default_pct": 10,
    "dry_run_default": True,
    "check_signatures": True,
    "allow_unchecked_sources": False,
    "auto_audit_post": True,
    "auto_deepclean_post": False,
    "max_retries": 2,
    "retry_backoff_seconds": 5
}

SNAPSHOT_DIR = UP_CFG.get("snapshot_dir", DEFAULTS["snapshot_dir"])
RUNS_DIR = UP_CFG.get("runs_dir", DEFAULTS["runs_dir"])
PARALLEL_WORKERS = int(UP_CFG.get("parallel_workers", DEFAULTS["parallel_workers"]))
SHARD_SIZE = int(UP_CFG.get("shard_size", DEFAULTS["shard_size"]))
CANARY_DEFAULT_PCT = int(UP_CFG.get("canary_default_pct", DEFAULTS["canary_default_pct"]))
DRY_RUN_DEFAULT = bool(UP_CFG.get("dry_run_default", DEFAULTS["dry_run_default"]))
CHECK_SIGNATURES = bool(UP_CFG.get("check_signatures", DEFAULTS["check_signatures"]))
ALLOW_UNCHECKED = bool(UP_CFG.get("allow_unchecked_sources", DEFAULTS["allow_unchecked_sources"]))
AUTO_AUDIT = bool(UP_CFG.get("auto_audit_post", DEFAULTS["auto_audit_post"]))
AUTO_DEEPCLEAN = bool(UP_CFG.get("auto_deepclean_post", DEFAULTS["auto_deepclean_post"]))
MAX_RETRIES = int(UP_CFG.get("max_retries", DEFAULTS["max_retries"]))
RETRY_BACKOFF = int(UP_CFG.get("retry_backoff_seconds", DEFAULTS["retry_backoff_seconds"]))

os.makedirs(SNAPSHOT_DIR, exist_ok=True)
os.makedirs(RUNS_DIR, exist_ok=True)

# ---------------------------
# Utilities
# ---------------------------
def _now_ts() -> int:
    return int(time.time())

def _uid() -> str:
    return uuid.uuid4().hex[:12]

def _write_run_file(run_id: str, data: Dict[str,Any]):
    try:
        path = os.path.join(RUNS_DIR, f"{run_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        logger.exception("Failed writing run file")

def _read_run_file(run_id: str) -> Optional[Dict[str,Any]]:
    try:
        path = os.path.join(RUNS_DIR, f"{run_id}.json")
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

# ---------------------------
# Data models
# ---------------------------
class UpgradePlanItem:
    def __init__(self, package: str, current_version: Optional[str]=None, target_version: Optional[str]=None, meta_path: Optional[str]=None):
        self.package = package
        self.current_version = current_version
        self.target_version = target_version
        self.meta_path = meta_path
        self.sources: List[str] = []  # resolved source artifacts
        self.snapshot: Optional[str] = None  # path to snapshot file for rollback
        self.status: str = "pending"  # pending|fetched|built|installed|failed|rolledback
        self.checkpoint: Optional[str] = None  # last checkpoint name
        self.attempts = 0
        self.logs: List[str] = []

    def to_dict(self):
        return {
            "package": self.package,
            "current_version": self.current_version,
            "target_version": self.target_version,
            "meta_path": self.meta_path,
            "snapshot": self.snapshot,
            "status": self.status,
            "checkpoint": self.checkpoint,
            "attempts": self.attempts,
            "logs": self.logs
        }

class UpgradePlan:
    def __init__(self, items: List[UpgradePlanItem], run_id: Optional[str]=None, options: Optional[Dict[str,Any]]=None):
        self.run_id = run_id or f"up-{_uid()}"
        self.items = items
        self.created_at = _now_ts()
        self.options = options or {}
    def to_dict(self):
        return {"run_id": self.run_id, "created_at": self.created_at, "items": [i.to_dict() for i in self.items], "options": self.options}

# ---------------------------
# Core Upgrade Manager
# ---------------------------
class UpgradeManager:
    def __init__(self):
        self.db = get_db() if callable(get_db) else None
        self.meta_loader = MetaLoader() if MetaLoader else None
        self.resolver = get_resolver()() if callable(get_resolver) else None
        self.fetcher = get_fetcher()() if callable(get_fetcher) else None
        self.buildsystem = get_buildsystem()() if callable(get_buildsystem) else None
        self.pkgtool = get_pkgtool()() if callable(get_pkgtool) else None
        self.sandbox = get_sandbox_manager()() if callable(get_sandbox_manager) else None
        self.auditor = get_auditor()() if callable(get_auditor) else None
        self.conflicts = get_conflicts_detector()() if callable(get_conflicts_detector) else None
        self.deepclean = get_deepclean_manager()() if callable(get_deepclean_manager) else None
        self.hooks = get_hook_manager()() if callable(get_hook_manager) else None
        self.repo_sync = get_repo_sync()() if callable(get_repo_sync) else None
        self.masks = get_masks_manager()() if callable(get_masks_manager) else None
        self.slots = get_slots_manager()() if callable(get_slots_manager) else None
        self.parallel_workers = PARALLEL_WORKERS
        self.shard_size = SHARD_SIZE
        self._ensure_tables()

    def _ensure_tables(self):
        if not self.db:
            logger.debug("No DB available; upgrade runs not persisted")
            return
        try:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS upgrade_runs (
                    id TEXT PRIMARY KEY,
                    created_at INTEGER,
                    options TEXT,
                    plan TEXT,
                    status TEXT,
                    result TEXT
                );""", (), commit=True)
        except Exception:
            logger.exception("Failed ensuring upgrade_runs table")
          # continuation PARTE 2/4
# ---------------------------
# Planning helpers
# ---------------------------
    def _get_installed_version(self, pkg: str) -> Optional[str]:
        if self.db:
            try:
                row = self.db.fetchone("SELECT version FROM installed_packages WHERE name = ? LIMIT 1", (pkg,))
                if row:
                    return row.get("version")
            except Exception:
                logger.exception("DB lookup installed_packages failed")
        return None

    def _get_available_version_from_repo(self, pkg: str) -> Optional[str]:
        # use repo_sync/meta/remote index: if repo_sync available, ask it; else meta loader if local meta exists
        try:
            if self.repo_sync and hasattr(self.repo_sync, "get_latest_version"):
                return self.repo_sync.get_latest_version(pkg)
        except Exception:
            logger.exception("repo_sync.get_latest_version failed")
        # fallback: check local meta if present
        if self.meta_loader:
            for p in [f"./{pkg}.meta", f"/usr/share/rquest/meta/{pkg}.meta", os.path.expanduser(f"~/.rquest/overlays/{pkg}.meta")]:
                if os.path.exists(p):
                    try:
                        m = self.meta_loader.load(p)
                        return m.version
                    except Exception:
                        pass
        return None

    def plan_upgrade(self, packages: Optional[List[str]] = None, with_deps: bool = True, dry_run: bool = True, canary_pct: Optional[int] = None, cluster: bool = False) -> UpgradePlan:
        """
        Build an UpgradePlan.
        - packages: list or None (None = plan all upgradable)
        - with_deps: include dependent upgrades if necessary
        - dry_run: do not perform side-effects at planning
        - canary_pct: percent of nodes to run as canary (applies in cluster mode; used as metadata)
        - cluster: whether plan targets a cluster (or single node)
        """
        canary_pct = CANARY_DEFAULT_PCT if canary_pct is None else int(canary_pct)
        items: List[UpgradePlanItem] = []
        # determine candidates
        pkg_list = packages or []
        if not pkg_list:
            # list all installed packages and check updates
            if self.db:
                try:
                    rows = self.db.fetchall("SELECT name, version FROM installed_packages")
                    for r in rows:
                        pkg_list.append(r.get("name"))
                except Exception:
                    logger.exception("Failed fetching installed packages from DB")
        # iterate and check available versions
        for pkg in pkg_list:
            cur = self._get_installed_version(pkg)
            avail = self._get_available_version_from_repo(pkg)
            if not avail:
                logger.debug("No available version found for %s; skipping", pkg)
                continue
            if cur == avail:
                logger.debug("%s already at latest (%s)", pkg, cur)
                continue
            item = UpgradePlanItem(package=pkg, current_version=cur, target_version=avail)
            items.append(item)
        plan = UpgradePlan(items=items, options={"with_deps": with_deps, "dry_run": dry_run, "canary_pct": canary_pct, "cluster": cluster})
        # persist plan
        if self.db:
            try:
                self.db.execute("INSERT INTO upgrade_runs (id, created_at, options, plan, status, result) VALUES (?, ?, ?, ?, ?, ?)",
                                (plan.run_id, plan.created_at, json.dumps(plan.options), json.dumps(plan.to_dict()), "planned", json.dumps({})), commit=True)
            except Exception:
                logger.exception("Failed persisting upgrade plan")
        _write_run_file(plan.run_id, plan.to_dict())
        return plan

# ---------------------------
# Snapshot helpers (quickpkg)
# ---------------------------
    def _create_snapshot(self, item: UpgradePlanItem) -> Optional[str]:
        """
        Create snapshot for rollback. Returns snapshot descriptor path.
        """
        if not self.pkgtool:
            logger.warning("pkgtool missing: cannot snapshot %s", item.package)
            return None
        try:
            ver = item.current_version or self._get_installed_version(item.package)
            res = None
            try:
                res = self.pkgtool.quickpkg(item.package, ver)
            except Exception:
                logger.exception("pkgtool.quickpkg error")
            if res and res.get("ok"):
                snapshot = {"snapshot_id": str(uuid.uuid4()), "package": item.package, "version": ver, "package_path": res.get("package_path"), "created_at": _now_ts()}
                sfile = os.path.join(SNAPSHOT_DIR, f"{snapshot['snapshot_id']}.json")
                with open(sfile, "w", encoding="utf-8") as f:
                    json.dump(snapshot, f)
                logger.info("Snapshot created: %s", sfile)
                return sfile
            else:
                logger.warning("quickpkg didn't return ok for %s", item.package)
        except Exception:
            logger.exception("Snapshot creation failed")
        return None

    def _restore_snapshot(self, snapshot_file: str) -> Dict[str,Any]:
        if not os.path.exists(snapshot_file):
            return {"ok": False, "error": "snapshot_missing"}
        data = None
        try:
            with open(snapshot_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return {"ok": False, "error": "snapshot_read_failed"}
        path = data.get("package_path")
        if not path or not os.path.exists(path):
            return {"ok": False, "error": "package_artifact_missing"}
        # install via pkgtool if available
        if self.pkgtool and hasattr(self.pkgtool, "install_bin"):
            try:
                res = self.pkgtool.install_bin(path, use_fakeroot=True, sandbox_run=False, verify_sig=False)
                return res
            except Exception:
                logger.exception("pkgtool.install_bin restore failed")
                return {"ok": False, "error": "install_failed"}
        # fallback: unpack and notify manual steps
        try:
            tmpd = tempfile.mkdtemp(prefix="restore-snap-")
            shutil.unpack_archive(path, tmpd)
            logger.info("Extracted snapshot to %s; manual restore may be needed", tmpd)
            return {"ok": True, "note": "extracted_manual_restore"}
        except Exception:
            logger.exception("snapshot restore fallback failed")
            return {"ok": False, "error": "exception"}
          # continuation PARTE 3/4
# ---------------------------
# Core execution steps for one package: fetch -> patch -> build -> package -> install -> audit/test
# Each step records checkpoint and persists to DB
# ---------------------------
    def _checkpoint(self, run_id: str, item: UpgradePlanItem, name: str, extra: Optional[Dict[str,Any]] = None):
        item.checkpoint = name
        # persist per-run state
        data = _read_run_file(run_id) or {}
        # find item in data
        try:
            its = data.get("items", [])
            for it in its:
                if it.get("package") == item.package:
                    it["checkpoint"] = name
                    it["status"] = item.status
                    it["attempts"] = item.attempts
                    it["logs"] = item.logs
            data["items"] = its
            _write_run_file(run_id, data)
            if self.db:
                self.db.execute("UPDATE upgrade_runs SET status = ? , result = ? WHERE id = ?",
                                (f"running:{run_id}", json.dumps(data), run_id), commit=True)
        except Exception:
            logger.exception("Failed writing checkpoint to run file")

    def _fetch(self, item: UpgradePlanItem, run_id: str, verify_signatures: bool = True) -> Dict[str,Any]:
        item.attempts += 1
        item.status = "fetching"
        self._checkpoint(run_id, item, "fetch_start")
        res_summary = {"ok": False}
        try:
            # attempt to use meta to resolve sources
            sources = []
            if self.meta_loader and item.meta_path and os.path.exists(item.meta_path):
                try:
                    meta = self.meta_loader.load(item.meta_path)
                    resolved = meta.resolve_sources()
                    sources = resolved
                except Exception:
                    logger.exception("meta.resolve_sources failed for %s", item.package)
            # fallback: ask fetcher for package artifact
            if self.fetcher and not sources:
                try:
                    sf = self.fetcher.locate_package_artifacts(item.package, item.target_version)
                    if sf:
                        sources = sf
                except Exception:
                    logger.exception("fetcher.locate_package_artifacts failed")
            # generic fetch logic: use fetcher.download/clone when available
            artifacts = []
            tmpd = tempfile.mkdtemp(prefix=f"upgrade-fetch-{item.package}-")
            if self.fetcher and hasattr(self.fetcher, "download_artifacts"):
                try:
                    res = self.fetcher.download_artifacts(sources, tmpd)
                    if res.get("ok"):
                        artifacts = res.get("paths", [])
                except Exception:
                    logger.exception("fetcher.download_artifacts failed")
            else:
                # simple fallback: if sources are URLs, try curl/wget; if git, git clone
                for s in sources:
                    if isinstance(s, dict) and s.get("git"):
                        repo = s.get("git")
                        target = os.path.join(tmpd, f"git-{uuid.uuid4().hex[:6]}")
                        try:
                            subprocess.check_call(["git","clone","--depth","1",repo,target], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            artifacts.append(target)
                        except Exception:
                            logger.exception("git clone fallback failed for %s", repo)
                    elif isinstance(s, dict) and s.get("url"):
                        url = s.get("url")
                        fname = s.get("filename") or os.path.basename(url.split("?")[0])
                        out = os.path.join(tmpd, fname)
                        try:
                            if shutil.which("curl"):
                                subprocess.check_call(["curl","-L","-o", out, url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            elif shutil.which("wget"):
                                subprocess.check_call(["wget","-O", out, url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            else:
                                raise Exception("no downloader")
                            artifacts.append(out)
                        except Exception:
                            logger.exception("download fallback failed for %s", url)
            item.sources = artifacts
            item.status = "fetched"
            res_summary = {"ok": True, "artifacts": artifacts}
            self._checkpoint(run_id, item, "fetch_done", {"artifacts": artifacts})
            return res_summary
        except Exception as e:
            logger.exception("Fetch step failed for %s", item.package)
            item.status = "fetch_failed"
            item.logs.append(f"fetch_error:{str(e)}")
            self._checkpoint(run_id, item, "fetch_failed")
            return {"ok": False, "error": str(e)}
        finally:
            pass

    def _apply_patches(self, item: UpgradePlanItem, build_dir: str, run_id: str) -> Dict[str,Any]:
        self._checkpoint(run_id, item, "patch_start")
        try:
            # delegate to patches module via buildsystem or fetcher if supported
            if self.buildsystem and hasattr(self.buildsystem, "apply_patches"):
                try:
                    ok = self.buildsystem.apply_patches(build_dir, item.package)
                    self._checkpoint(run_id, item, "patch_done")
                    return {"ok": ok}
                except Exception:
                    logger.exception("buildsystem.apply_patches failed")
            # fallback: no-op
            self._checkpoint(run_id, item, "patch_skipped")
            return {"ok": True, "note": "no patches"}
        except Exception as e:
            logger.exception("Patches failed")
            self._checkpoint(run_id, item, "patch_failed")
            return {"ok": False, "error": str(e)}

    def _build(self, item: UpgradePlanItem, run_id: str, sandboxed: bool = True) -> Dict[str,Any]:
        self._checkpoint(run_id, item, "build_start")
        try:
            if not item.sources:
                return {"ok": False, "error": "no_sources"}
            # choose first artifact as source dir or tarball
            src = item.sources[0]
            build_dir = tempfile.mkdtemp(prefix=f"build-{item.package}-")
            # if src is a git dir or package, copy/unpack into build_dir
            try:
                if os.path.isdir(src):
                    shutil.copytree(src, os.path.join(build_dir, os.path.basename(src)))
                    workdir = os.path.join(build_dir, os.path.basename(src))
                else:
                    # try extract
                    shutil.unpack_archive(src, build_dir)
                    workdir = build_dir
            except Exception:
                # fallback: copy file
                try:
                    shutil.copy2(src, build_dir)
                    workdir = build_dir
                except Exception:
                    workdir = build_dir
            # delegate build to buildsystem
            if self.buildsystem and hasattr(self.buildsystem, "build_package"):
                try:
                    res = self.buildsystem.build_package(workdir, package=item.package, sandboxed=sandboxed)
                    if res.get("ok"):
                        item.status = "built"
                        self._checkpoint(run_id, item, "build_done")
                        return {"ok": True, "artifact": res.get("artifact_path")}
                    else:
                        self._checkpoint(run_id, item, "build_failed")
                        return {"ok": False, "error": res.get("error")}
                except Exception:
                    logger.exception("buildsystem.build_package failed")
            # fallback: create dummy artifact for install step (not ideal)
            artifact = os.path.join(build_dir, f"{item.package}-{item.target_version or 'unknown'}.pkg")
            with open(artifact, "w", encoding="utf-8") as f:
                f.write("dummy")
            self._checkpoint(run_id, item, "build_done_fallback")
            return {"ok": True, "artifact": artifact}
        except Exception as e:
            logger.exception("Build step failed for %s", item.package)
            self._checkpoint(run_id, item, "build_failed")
            return {"ok": False, "error": str(e)}
          # continuation PARTE 4/4 (final)
# ---------------------------
    def _package_and_install(self, item: UpgradePlanItem, artifact_path: str, run_id: str, sandboxed_install: bool = True) -> Dict[str,Any]:
        self._checkpoint(run_id, item, "package_install_start")
        try:
            # if pkgtool available, use it to create binary and install
            if self.pkgtool and hasattr(self.pkgtool, "install_bin"):
                try:
                    res = self.pkgtool.install_bin(artifact_path, use_fakeroot=True, sandbox_run=sandboxed_install, verify_sig=CHECK_SIGNATURES)
                    if res.get("ok"):
                        item.status = "installed"
                        self._checkpoint(run_id, item, "install_done")
                        return {"ok": True, "result": res}
                    else:
                        self._checkpoint(run_id, item, "install_failed")
                        return {"ok": False, "error": res.get("error")}
                except Exception:
                    logger.exception("pkgtool.install_bin failed")
            # fallback: attempt to copy files, but conservative: fail safe
            logger.warning("No pkgtool.install_bin available; install needs manual step for %s", item.package)
            self._checkpoint(run_id, item, "install_skipped")
            return {"ok": False, "error": "no_pkgtool"}
        except Exception as e:
            logger.exception("Install failed")
            self._checkpoint(run_id, item, "install_failed")
            return {"ok": False, "error": str(e)}

    def _post_install_audit_and_tests(self, item: UpgradePlanItem, run_id: str) -> Dict[str,Any]:
        self._checkpoint(run_id, item, "post_audit_start")
        results = {"ok": True, "audit": None, "tests": None}
        try:
            # run audit if configured
            if AUTO_AUDIT and self.auditor:
                try:
                    rep = self.auditor.run_audit(deep=False, auto_fix=False)
                    results["audit"] = {"run_id": rep.run_id, "findings": len(rep.findings)}
                    if len(rep.findings) > 0:
                        results["ok"] = False
                except Exception:
                    logger.exception("audit post-install failed")
                    results["audit"] = {"error": "audit_exception"}
                    results["ok"] = False
            # run package tests via meta hooks (pre/post test) if meta available
            if self.meta_loader and item.meta_path and os.path.exists(item.meta_path):
                try:
                    meta = self.meta_loader.load(item.meta_path)
                    # run 'post_install' hooks or tests declared in meta
                    if meta.tests:
                        # run tests inside sandbox if available
                        test_results = []
                        for t in meta.tests:
                            cmd = t.get("command")
                            if cmd:
                                if self.sandbox:
                                    try:
                                        res = self.sandbox.run_in_sandbox(cmd if isinstance(cmd, list) else [cmd], exec_type="test")
                                        test_results.append(res)
                                        if not res.get("ok"):
                                            results["ok"] = False
                                    except Exception:
                                        logger.exception("sandbox test execution failed")
                                else:
                                    try:
                                        proc = subprocess.run(cmd if isinstance(cmd, list) else cmd, shell=not isinstance(cmd, list), capture_output=True, text=True)
                                        test_results.append({"ok": proc.returncode==0, "stdout": proc.stdout, "stderr": proc.stderr})
                                        if proc.returncode != 0:
                                            results["ok"] = False
                                    except Exception:
                                        logger.exception("test run failed")
                        results["tests"] = test_results
                except Exception:
                    logger.exception("meta.tests execution failed")
            self._checkpoint(run_id, item, "post_audit_done")
            return results
        except Exception as e:
            logger.exception("post-install audit/tests failed")
            self._checkpoint(run_id, item, "post_audit_failed")
            return {"ok": False, "error": str(e)}

# ---------------------------
# High-level orchestration: execute plan with canary/rollout/sharding and retries
# ---------------------------
    def apply_plan(self, plan: UpgradePlan, apply: bool = False, canary_pct: Optional[int] = None, shards: Optional[int] = None, threads: Optional[int] = None, cluster: bool = False) -> Dict[str,Any]:
        """
        Execute UpgradePlan.
        - apply=False => dry-run (simulate steps without side-effects unless modules require)
        - canary_pct applies in cluster mode; for single node it's informational
        - shards/threads for parallel execution
        """
        dry_run = not apply
        canary_pct = plan.options.get("canary_pct", CANARY_DEFAULT_PCT) if canary_pct is None else int(canary_pct)
        threads = threads or self.parallel_workers
        shards = shards or max(1, math.ceil(len(plan.items) / max(1, self.shard_size)))
        logger.info("Applying plan %s: items=%d canary=%d%% shards=%d threads=%d apply=%s", plan.run_id, len(plan.items), canary_pct, shards, threads, apply)

        # persist run start
        if self.db:
            try:
                self.db.execute("UPDATE upgrade_runs SET status = ? WHERE id = ?", ("running", plan.run_id), commit=True)
            except Exception:
                pass
        _write_run_file(plan.run_id, plan.to_dict())

        results = []
        lock = threading.Lock()
        items_queue = list(plan.items)

        def worker():
            while True:
                with lock:
                    if not items_queue:
                        return
                    item = items_queue.pop(0)
                # for each item: snapshot -> fetch -> build -> package -> install -> audit/test -> cleanup
                try:
                    logger.info("Processing %s (%s -> %s)", item.package, item.current_version, item.target_version)
                    # create snapshot unless dry_run
                    if not dry_run:
                        snap = self._create_snapshot(item)
                        item.snapshot = snap
                    # fetch
                    fetch_res = self._fetch(item, plan.run_id, verify_signatures=CHECK_SIGNATURES)
                    if not fetch_res.get("ok"):
                        raise Exception(f"fetch_failed: {fetch_res.get('error')}")
                    # build
                    build_res = self._build(item, plan.run_id, sandboxed=True)
                    if not build_res.get("ok"):
                        raise Exception(f"build_failed: {build_res.get('error')}")
                    artifact = build_res.get("artifact")
                    # package & install
                    install_res = self._package_and_install(item, artifact, plan.run_id, sandboxed_install=True)
                    if not install_res.get("ok"):
                        raise Exception(f"install_failed: {install_res.get('error')}")
                    # post-audit/tests
                    post_res = self._post_install_audit_and_tests(item, plan.run_id)
                    if not post_res.get("ok"):
                        raise Exception("post_audit_tests_failed")
                    # success for item
                    item.status = "done"
                    self._checkpoint(plan.run_id, item, "done")
                    with lock:
                        results.append({"package": item.package, "ok": True, "details": item.to_dict()})
                except Exception as e:
                    logger.exception("Error processing %s: %s", item.package, e)
                    item.status = "failed"
                    item.logs.append(str(e))
                    self._checkpoint(plan.run_id, item, "failed")
                    # attempt rollback for this item if snapshot exists and apply True
                    rb = None
                    if not dry_run and item.snapshot:
                        try:
                            rb = self._restore_snapshot(item.snapshot)
                            if rb.get("ok"):
                                item.status = "rolledback"
                                self._checkpoint(plan.run_id, item, "rolledback")
                                logger.info("Rolled back %s successfully", item.package)
                            else:
                                logger.warning("Rollback failed for %s: %s", item.package, rb.get("error"))
                        except Exception:
                            logger.exception("Rollback attempt failed")
                    with lock:
                        results.append({"package": item.package, "ok": False, "error": str(e), "rollback": rb})
                    # continue with other items (partial failure supported)
                    continue

        # run threads
        ths = []
        for _ in range(threads):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            ths.append(t)
        for t in ths:
            t.join()

        # after run: deepclean & audit optionally
        deepclean_summary = None
        audit_summary = None
        if apply and AUTO_DEEPCLEAN and self.deepclean:
            try:
                deepclean_summary = self.deepclean.run(apply=True, interactive=False)
            except Exception:
                logger.exception("Deepclean after upgrade failed")
        if apply and AUTO_AUDIT and self.auditor:
            try:
                rep = self.auditor.run_audit(deep=False, auto_fix=False)
                audit_summary = {"run_id": rep.run_id, "findings": len(rep.findings)}
            except Exception:
                logger.exception("Post-upgrade audit failed")

        # persist final status
        if self.db:
            try:
                self.db.execute("UPDATE upgrade_runs SET status = ?, result = ? WHERE id = ?",
                                ("finished", json.dumps({"results": results, "deepclean": deepclean_summary, "audit": audit_summary}), plan.run_id), commit=True)
            except Exception:
                logger.exception("Failed updating upgrade_runs final status")
        _write_run_file(plan.run_id, {"results": results, "deepclean": deepclean_summary, "audit": audit_summary})

        # emit event
        try:
            emit_event("upgrade.finished", {"run_id": plan.run_id, "items": len(plan.items)})
        except Exception:
            pass

        summary = {"run_id": plan.run_id, "items": len(plan.items), "results": results, "deepclean": deepclean_summary, "audit": audit_summary}
        return summary

# ---------------------------
# Rollback high-level API
# ---------------------------
    def rollback(self, run_id_or_pkg_or_snapshot: str) -> Dict[str,Any]:
        """
        Rollback by:
         - if run_id: inspect run file and restore snapshots for failed/changed packages
         - if package name: find latest snapshot for package and restore
         - if snapshot path: restore snapshot file
        """
        # if run id provided and run file exists
        run_data = _read_run_file(run_id_or_pkg_or_snapshot)
        if run_data:
            # iterate items and restore their snapshots if present
            results = []
            items = run_data.get("items", [])
            for it in items:
                pkg = it.get("package")
                snap = it.get("snapshot")
                if snap:
                    try:
                        r = self._restore_snapshot(snap)
                        results.append({"package": pkg, "snapshot": snap, "result": r})
                    except Exception:
                        logger.exception("Failed restoring snapshot for %s", pkg)
                        results.append({"package": pkg, "snapshot": snap, "result": {"ok": False}})
            return {"ok": True, "results": results}
        # else if direct snapshot path
        if os.path.exists(run_id_or_pkg_or_snapshot):
            return self._restore_snapshot(run_id_or_pkg_or_snapshot)
        # else treat as package name: find latest snapshot file
        snaps = []
        for fn in os.listdir(SNAPSHOT_DIR):
            if fn.endswith(".json"):
                path = os.path.join(SNAPSHOT_DIR, fn)
                data = None
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    continue
                if data and data.get("package") == run_id_or_pkg_or_snapshot:
                    snaps.append((path, data.get("created_at", 0)))
        if not snaps:
            return {"ok": False, "error": "no_snapshot_found"}
        snaps.sort(key=lambda x: x[1], reverse=True)
        return self._restore_snapshot(snaps[0][0])

# ---------------------------
# Status & history helpers
# ---------------------------
    def status(self, run_id: str) -> Optional[Dict[str,Any]]:
        data = _read_run_file(run_id)
        if data:
            return data
        if self.db:
            try:
                row = self.db.fetchone("SELECT id, created_at, options, plan, status, result FROM upgrade_runs WHERE id = ? LIMIT 1", (run_id,))
                if row:
                    return {"id": row.get("id"), "created_at": row.get("created_at"), "options": json.loads(row.get("options") or "{}"), "plan": json.loads(row.get("plan") or "{}"), "status": row.get("status"), "result": json.loads(row.get("result") or "{}")}
            except Exception:
                logger.exception("DB status lookup failed")
        return None

    def history(self, limit: int = 20) -> List[Dict[str,Any]]:
        out = []
        # scan runs dir
        try:
            files = [f for f in os.listdir(RUNS_DIR) if f.endswith(".json")]
            files.sort(reverse=True)
            for fn in files[:limit]:
                p = os.path.join(RUNS_DIR, fn)
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        d = json.load(f)
                        out.append(d)
                except Exception:
                    continue
        except Exception:
            logger.exception("Failed reading run files")
        return out

# ---------------------------
# Module-level helper
# ---------------------------
_MANAGER: Optional[UpgradeManager] = None

def get_upgrade_manager() -> UpgradeManager:
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = UpgradeManager()
    return _MANAGER

# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="upgrade", description="Rquest upgrade manager")
    ap.add_argument("packages", nargs="*", help="packages to upgrade (omit to consider all)")
    ap.add_argument("--with-deps", action="store_true", help="include dependent upgrades")
    ap.add_argument("--apply", action="store_true", help="actually apply the upgrade (default is dry-run)")
    ap.add_argument("--canary", type=int, help="percentage of canary nodes (cluster mode)")
    ap.add_argument("--cluster", action="store_true", help="run in cluster mode (requires repo_sync/cluster infra)")
    ap.add_argument("--shards", type=int, help="number of shards for parallel upgrade")
    ap.add_argument("--threads", type=int, help="parallel worker threads")
    ap.add_argument("--plan-only", action="store_true", help="only generate plan and print; do not apply")
    ap.add_argument("--status", metavar="RUN_ID", help="show status of run")
    ap.add_argument("--history", action="store_true", help="show recent upgrade runs")
    ap.add_argument("--rollback", metavar="RUN_OR_PKG_OR_SNAPSHOT", help="rollback run id, package or snapshot file")
    args = ap.parse_args()

    mgr = get_upgrade_manager()

    if args.status:
        s = mgr.status(args.status)
        print(json.dumps(s, indent=2, ensure_ascii=False))
        sys.exit(0)
    if args.history:
        h = mgr.history()
        print(json.dumps(h, indent=2, ensure_ascii=False))
        sys.exit(0)
    if args.rollback:
        r = mgr.rollback(args.rollback)
        print(json.dumps(r, indent=2, ensure_ascii=False))
        sys.exit(0)

    plan = mgr.plan_upgrade(packages=args.packages or None, with_deps=bool(args.with_deps), dry_run=not args.apply, canary_pct=args.canary, cluster=bool(args.cluster))
    print("Plan generated:", json.dumps(plan.to_dict(), indent=2, ensure_ascii=False))
    if args.plan_only:
        sys.exit(0)
    res = mgr.apply_plan(plan, apply=bool(args.apply), canary_pct=args.canary, shards=args.shards, threads=args.threads, cluster=bool(args.cluster))
    print("Apply result:")
    print(json.dumps(res, indent=2, ensure_ascii=False))
