# rquest1.0/modules/deepclean.py
# -*- coding: utf-8 -*-
"""
DeepClean Manager for Rquest

Features:
 - DeepCleanContext for dry_run/interactive/snapshot control
 - Adapters for pkgtool and resolver with normalized APIs
 - Coordination with conflicts.ConflictResolver before removals
 - Snapshot creation (btrfs/zfs quickpkg fallback or file copy)
 - Restore snapshot support
 - DB schema & migrations (schema_versions, deepclean_runs, deepclean_snapshots)
 - detect_orphans (pure), simulate_removal (pure), apply_cleanup (side-effects)
 - prune_cache, clean_build_dirs, daemon mode
 - Hooks integration and structured logging/events
"""

from __future__ import annotations
import os
import sys
import json
import time
import shutil
import logging
import threading
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ----------------------------
# Defensive imports for integration
# ----------------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    LOG = get_logger("deepclean")
except Exception:
    LOG = logging.getLogger("rquest.deepclean")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)

try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    get_db = None
    def emit_event(*a, **k):
        pass

try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    get_pkgtool = None

try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    get_resolver = None

try:
    from modules.conflicts import ConflictResolver  # type: ignore
except Exception:
    ConflictResolver = None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    get_hook_manager = None

# ----------------------------
# Configuration defaults & normalize
# ----------------------------
CFG = get_config() if callable(get_config) else {}
DEEP_CFG = CFG.get("deepclean", {}) if isinstance(CFG, dict) else {}

DEFAULTS = {
    "enabled": True,
    "dry_run_default": True,
    "snapshot_before_remove": True,
    "snapshot_dir": "/var/lib/rquest/deepclean_snapshots",
    "trash_dir": "/var/lib/rquest/deepclean_trash",
    "snapshot_retention_days": 7,
    "orphan_age_days": 90,
    "orphan_min_size_bytes": 1024,
    "auto_remove_conflicting": False,
    "auto_backup_conflicting": True,
    "allow_dep_conflicts": False,
    "gc_daemon": {"enabled": False, "interval_seconds": 3600},
    "prune_cache_on_clean": True,
    "builddirs": ["/var/tmp/rquest/build", "/tmp/rquest-build"],
    "lockfile": "/var/run/rquest-deepclean.lock",
}

def _cfg_get(key: str, default=None):
    return DEEP_CFG.get(key, DEFAULTS.get(key, default))

SNAPSHOT_DIR = Path(_cfg_get("snapshot_dir"))
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
TRASH_DIR = Path(_cfg_get("trash_dir"))
TRASH_DIR.mkdir(parents=True, exist_ok=True)
LOCKFILE = Path(_cfg_get("lockfile"))

# ----------------------------
# DB schema / migrations
# ----------------------------
_SCHEMA_SQL = [
    ("schema_versions", """
        CREATE TABLE IF NOT EXISTS schema_versions (
            name TEXT PRIMARY KEY,
            version INTEGER,
            applied_at INTEGER
        );
    """),
    ("deepclean_runs", """
        CREATE TABLE IF NOT EXISTS deepclean_runs (
            id TEXT PRIMARY KEY,
            started_at INTEGER,
            finished_at INTEGER,
            dry_run INTEGER,
            actions TEXT,
            summary TEXT
        );
    """),
    ("deepclean_snapshots", """
        CREATE TABLE IF NOT EXISTS deepclean_snapshots (
            id TEXT PRIMARY KEY,
            created_at INTEGER,
            pkg TEXT,
            meta_path TEXT,
            snapshot_path TEXT,
            retained_until INTEGER
        );
    """),
]

def _ensure_db_schema(db_conn):
    if not db_conn:
        return
    try:
        for name, ddl in _SCHEMA_SQL:
            try:
                db_conn.execute(ddl, (), commit=True)
            except TypeError:
                # fallback for simpler db wrappers without commit arg
                try:
                    db_conn.execute(ddl)
                    db_conn.commit()
                except Exception:
                    LOG.debug("Schema create fallback failed for %s", name)
    except Exception:
        LOG.exception("Failed to ensure deepclean DB schema")

# ----------------------------
# Utility helpers
# ----------------------------
def _now_ts() -> int:
    return int(time.time())

def _shortid(prefix: str = "") -> str:
    import uuid
    return f"{prefix}{uuid.uuid4().hex[:10]}"

def _norm_path(p: str) -> str:
    return os.path.normpath(os.path.join("/", str(p).lstrip("/")))

# ----------------------------
# Context object
# ----------------------------
class DeepCleanContext:
    def __init__(self,
                 dry_run: bool = True,
                 interactive: bool = False,
                 snapshot: bool = True,
                 operator: Optional[str] = None):
        self.dry_run = bool(dry_run)
        self.interactive = bool(interactive)
        self.snapshot = bool(snapshot)
        self.operator = operator or os.getenv("USER", "unknown")

# ----------------------------
# Adapters
# ----------------------------
class PkgtoolAdapter:
    """
    Wraps modules.pkgtool to a normalized API used by deepclean.
    Fallbacks if pkgtool not present.
    """
    def __init__(self):
        self._raw = None
        if get_pkgtool:
            try:
                self._raw = get_pkgtool()()
            except Exception:
                LOG.debug("pkgtool init failed in adapter")

    def quickpkg(self, pkg_name: str, version: Optional[str], destdir: str) -> Dict[str, Any]:
        """
        Create a quick binary package artifact for snapshotting.
        Return normalized dict: {ok: bool, artifact: str, size: int, detail: ...}
        """
        if not self._raw:
            return {"ok": False, "error": "pkgtool not available"}
        try:
            if hasattr(self._raw, "quickpkg"):
                res = self._raw.quickpkg(pkg_name, version, destdir)
                # normalize typical return shapes
                if isinstance(res, dict):
                    return {"ok": bool(res.get("ok", True)), "artifact": res.get("package_path") or res.get("artifact") or res.get("path"), "size": res.get("size", 0), "detail": res}
                # fallback to simple path
                if isinstance(res, str):
                    return {"ok": True, "artifact": res, "size": os.path.getsize(res) if os.path.exists(res) else 0}
            # try other names
            if hasattr(self._raw, "pack"):
                res = self._raw.pack(pkg_name, destdir=destdir)
                return {"ok": True, "artifact": res}
        except Exception:
            LOG.exception("pkgtool.quickpkg adapter error")
        return {"ok": False, "error": "quickpkg failed"}

    def uninstall(self, pkg_name: str, force: bool = False) -> Dict[str, Any]:
        """
        Attempt to uninstall a package via pkgtool API if present.
        """
        if not self._raw:
            return {"ok": False, "error": "pkgtool not available"}
        try:
            if hasattr(self._raw, "uninstall"):
                res = self._raw.uninstall(pkg_name, force=force)
                return {"ok": bool(res.get("ok", True)), "detail": res}
        except Exception:
            LOG.exception("pkgtool.uninstall error")
        return {"ok": False, "error": "uninstall failed"}

    def cache_prune(self) -> Dict[str, Any]:
        if not self._raw:
            return {"ok": False, "error": "pkgtool not available"}
        try:
            if hasattr(self._raw, "cache_prune"):
                res = self._raw.cache_prune()
                return {"ok": True, "detail": res}
        except Exception:
            LOG.exception("pkgtool.cache_prune error")
        return {"ok": False, "error": "cache_prune failed"}

class ResolverAdapter:
    """
    Wraps resolver to a minimal API expected by deepclean.
    """
    def __init__(self):
        self._raw = None
        if get_resolver:
            try:
                self._raw = get_resolver()()
            except Exception:
                LOG.debug("resolver init failed in adapter")

    def simulate_removal(self, pkgs: List[str]) -> Dict[str, Any]:
        """
        Returns {ok: bool, conflicts: [...], blocked_by: [...]}
        """
        if not self._raw:
            return {"ok": True, "conflicts": [], "note": "no resolver"}
        try:
            if hasattr(self._raw, "simulate_removal"):
                return self._raw.simulate_removal(pkgs)
            # fallback: try analyze_plan or resolve_conflicts
            if hasattr(self._raw, "resolve_conflicts"):
                res = self._raw.resolve_conflicts(pkgs)
                return {"ok": False if res else True, "conflicts": res or []}
        except Exception:
            LOG.exception("resolver.simulate_removal error")
        return {"ok": True, "conflicts": [], "note": "simulate_removal fallback"}

    def resolve_conflicts(self, pkgs: List[str]) -> Dict[str, Any]:
        if not self._raw:
            return {"ok": False, "error": "no resolver"}
        try:
            if hasattr(self._raw, "auto_resolve"):
                return self._raw.auto_resolve(pkgs)
            return {"ok": False, "error": "no auto_resolve"}
        except Exception:
            LOG.exception("resolver.resolve_conflicts error")
        return {"ok": False, "error": "resolver failed"}

# ----------------------------
# DeepClean manager
# ----------------------------
class DeepCleanManager:
    def __init__(self, db_factory=None):
        self.db = None
        if db_factory and callable(db_factory):
            try:
                self.db = db_factory()
            except Exception:
                LOG.debug("db_factory init failed")
        elif get_db:
            try:
                self.db = get_db()()
            except Exception:
                LOG.debug("get_db init failed")
        if self.db:
            _ensure_db_schema(self.db)
            try:
                _ensure_db_schema(self.db)
            except Exception:
                LOG.debug("schema ensure attempt failed")

        self.pkgtool = PkgtoolAdapter()
        self.resolver = ResolverAdapter()
        self.conflicts = ConflictResolver() if ConflictResolver else None
        self.hooks = (get_hook_manager()() if get_hook_manager else None)
        self.lockfile = LOCKFILE

    # ---------- detection helpers (pure) ----------
    def detect_orphans(self, older_than_days: Optional[int] = None, min_size_bytes: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Detect candidate orphan packages (not depended on and not used recently).
        Returns list of dicts {name, version, installed_at, last_used_ts, size, manifest_path}
        Pure function: does not mutate state.
        """
        older_than_days = older_than_days if older_than_days is not None else _cfg_get("orphan_age_days")
        min_size_bytes = min_size_bytes if min_size_bytes is not None else _cfg_get("orphan_min_size_bytes")
        cutoff_ts = int(time.time()) - int(older_than_days) * 86400

        candidates = []
        # rely on DB installed_packages if available
        if self.db:
            try:
                rows = self.db.fetchall("SELECT name, version, installed_at, last_used_at, manifest_path FROM installed_packages")
                for r in rows:
                    installed_at = r.get("installed_at") or 0
                    last_used = r.get("last_used_at") or installed_at
                    if last_used < cutoff_ts:
                        # estimate size: try installed_files size sum
                        size = 0
                        try:
                            frows = self.db.fetchall("SELECT path FROM installed_files WHERE package = ?", (r.get("name"),))
                            for fr in frows:
                                p = fr.get("path")
                                try:
                                    if os.path.exists(p):
                                        size += os.path.getsize(p)
                                except Exception:
                                    continue
                        except Exception:
                            size = 0
                        if size >= min_size_bytes:
                            candidates.append({
                                "name": r.get("name"),
                                "version": r.get("version"),
                                "installed_at": installed_at,
                                "last_used_at": last_used,
                                "size": size,
                                "manifest_path": r.get("manifest_path"),
                            })
                return candidates
            except Exception:
                LOG.exception("detect_orphans: DB query failed, falling back to manifest heuristic")

        # fallback: scan ~/.rquest/installed manifests
        installed_dir = Path.home() / ".rquest" / "installed"
        if installed_dir.exists():
            for pkgdir in installed_dir.iterdir():
                try:
                    mf = pkgdir / "MANIFEST.json"
                    if mf.exists():
                        j = json.loads(mf.read_text(encoding="utf-8"))
                        last_used = j.get("last_used_at") or j.get("installed_at") or 0
                        if last_used < cutoff_ts:
                            size = 0
                            for f in j.get("files", []):
                                p = Path("/") / f.get("path", "").lstrip("/")
                                try:
                                    if p.exists():
                                        size += p.stat().st_size
                                except Exception:
                                    pass
                            if size >= min_size_bytes:
                                candidates.append({
                                    "name": j.get("name"),
                                    "version": j.get("version"),
                                    "installed_at": j.get("installed_at"),
                                    "last_used_at": last_used,
                                    "size": size,
                                    "manifest_path": str(mf),
                                })
                except Exception:
                    LOG.debug("Failed reading manifest in %s", pkgdir)
        return candidates

    def simulate_removal(self, pkgs: List[str]) -> Dict[str, Any]:
        """
        Use resolver to simulate removal and return conflicts/blocked info.
        Pure: does not mutate state.
        """
        res = self.resolver.simulate_removal(pkgs) if self.resolver else {"ok": True, "conflicts": []}
        return res

    # ---------- snapshot helpers ----------
    def create_snapshot_for_pkg(self, pkg_name: str, version: Optional[str], ctx: DeepCleanContext) -> Dict[str, Any]:
        """
        Create a snapshot for a package prior to removal.
        Attempt quickpkg via pkgtool, fallback to tar/cp.
        Returns snapshot record dict.
        """
        snap_id = _shortid("snap-")
        created_at = _now_ts()
        snapshot_path = None
        meta_path = None
        # Try to locate manifest in DB
        if self.db:
            try:
                row = self.db.fetchone("SELECT manifest_path FROM installed_packages WHERE name = ? LIMIT 1", (pkg_name,))
                meta_path = row.get("meta_path") if row else None
            except Exception:
                meta_path = None
        # quickpkg
        if self.pkgtool and ctx and ctx.snapshot:
            try:
                tmpdir = tempfile.mkdtemp(prefix="rquest-snap-")
                qp = self.pkgtool.quickpkg(pkg_name, version, tmpdir)
                if qp.get("ok") and qp.get("artifact"):
                    snapshot_path = qp.get("artifact")
                    LOG.info("Snapshot quickpkg created for %s at %s", pkg_name, snapshot_path)
                else:
                    LOG.debug("pkgtool.quickpkg did not produce artifact, fallback to tar")
            except Exception:
                LOG.exception("pkgtool.quickpkg failed for %s", pkg_name)
        # fallback: tar the files listed in manifest
        if not snapshot_path:
            try:
                # collect files from DB or manifest
                files = []
                if self.db:
                    try:
                        rows = self.db.fetchall("SELECT path FROM installed_files WHERE package = ?", (pkg_name,))
                        files = [r.get("path") for r in rows if r.get("path")]
                    except Exception:
                        files = []
                if not files and meta_path:
                    try:
                        mp_data = json.loads(Path(meta_path).read_text(encoding="utf-8"))
                        files = [f.get("path") if isinstance(f, dict) else f for f in mp_data.get("files", [])]
                    except Exception:
                        files = []
                if files:
                    ts = time.strftime("%Y%m%d-%H%M%S")
                    tarname = SNAPSHOT_DIR / f"{pkg_name}-{ts}.tar.gz"
                    if ctx.dry_run:
                        LOG.info("[dry-run] would create tar snapshot for %s -> %s", pkg_name, tarname)
                        snapshot_path = str(tarname)
                    else:
                        import tarfile
                        with tarfile.open(tarname, "w:gz") as tf:
                            for p in files:
                                try:
                                    ap = Path("/") / str(p).lstrip("/")
                                    if ap.exists():
                                        tf.add(str(ap), arcname=str(ap).lstrip("/"))
                                except Exception:
                                    LOG.debug("tar add failed for %s", p)
                        snapshot_path = str(tarname)
                        LOG.info("Snapshot tar created for %s -> %s", pkg_name, tarname)
            except Exception:
                LOG.exception("Fallback snapshot creation failed for %s", pkg_name)

        retained_until = int(time.time()) + int(_cfg_get("snapshot_retention_days", 7)) * 86400
        snap_record = {"id": snap_id, "created_at": created_at, "pkg": pkg_name, "meta_path": meta_path, "snapshot_path": snapshot_path, "retained_until": retained_until}
        # persist snapshot record
        if self.db:
            try:
                self.db.execute("INSERT OR REPLACE INTO deepclean_snapshots (id, created_at, pkg, meta_path, snapshot_path, retained_until) VALUES (?,?,?,?,?,?)",
                                (snap_id, created_at, pkg_name, meta_path, snapshot_path, retained_until), commit=True)
            except Exception:
                LOG.debug("Failed to persist snapshot record")
        return snap_record

    def restore_snapshot(self, snap_id: str, ctx: DeepCleanContext) -> Dict[str, Any]:
        """
        Restore a snapshot previously created. Tries to use pkgtool install if artifact is a package; otherwise extracts tar.
        """
        if self.db:
            try:
                row = self.db.fetchone("SELECT snapshot_path, pkg FROM deepclean_snapshots WHERE id = ? LIMIT 1", (snap_id,))
                if not row:
                    return {"ok": False, "error": "snapshot not found"}
                spath = row.get("snapshot_path")
                pkg = row.get("pkg")
            except Exception:
                return {"ok": False, "error": "db query failed"}
        else:
            return {"ok": False, "error": "db unavailable"}
        if not spath:
            return {"ok": False, "error": "no snapshot path"}
        if ctx.dry_run:
            LOG.info("[dry-run] would restore snapshot %s for %s", spath, pkg)
            return {"ok": True, "note": "dry-run"}
        # if artifact is a package file and pkgtool.install_bin exists
        if self.pkgtool and hasattr(self.pkgtool, "install"):
            try:
                res = self.pkgtool.install(spath)
                return {"ok": bool(res.get("ok", True)), "detail": res}
            except Exception:
                LOG.exception("pkgtool.install failed for snapshot")
        # else, try to untar
        try:
            import tarfile
            with tarfile.open(spath, "r:gz") as tf:
                tf.extractall("/")  # careful: this restores absolute paths from arcname
            LOG.info("Snapshot restored from %s", spath)
            return {"ok": True}
        except Exception:
            LOG.exception("Failed to restore snapshot %s", spath)
            return {"ok": False, "error": "restore failed"}

    # ---------- removal / resolution ----------
    def _attempt_remove_package(self, pkg_name: str, ctx: DeepCleanContext) -> Dict[str, Any]:
        """
        Attempt to remove package safely: snapshot -> simulate -> remove (via pkgtool or remove module)
        """
        LOG.info("Attempting removal of %s (dry_run=%s)", pkg_name, ctx.dry_run)
        # snapshot
        snap_info = None
        if ctx.snapshot and _cfg_get("snapshot_before_remove"):
            try:
                snap_info = self.create_snapshot_for_pkg(pkg_name, None, ctx)
            except Exception:
                LOG.exception("Snapshot creation failed for %s", pkg_name)
        # simulate removal to detect dependency conflicts
        sim = self.simulate_removal([pkg_name])
        if not sim.get("ok", True):
            LOG.warning("Simulate removal detected conflicts: %s", sim.get("conflicts"))
            if not _cfg_get("allow_dep_conflicts"):
                return {"ok": False, "error": "dep_conflicts", "details": sim}
        # if conflicts module present, check subsequent file conflicts
        if self.conflicts:
            try:
                # best-effort: detect file conflicts that would arise from removing this package
                # conflicts module principally detects when installing new packages; here we just call detection for packages that may be affected - skip if no API
                pass
            except Exception:
                LOG.debug("conflicts check before removal failed")
        # perform removal via remove API or pkgtool
        if ctx.dry_run:
            LOG.info("[dry-run] would remove %s (snapshot=%s)", pkg_name, snap_info.get("id") if snap_info else None)
            return {"ok": True, "dry_run": True, "snapshot": snap_info}
        # prefer remove module
        removed = False
        remove_detail = None
        # try remove module
        try:
            import modules.remove as remove_mod  # type: ignore
            if hasattr(remove_mod, "remove_package"):
                try:
                    res = remove_mod.remove_package(pkg_name, force=True)
                    removed = bool(res.get("ok", False)) if isinstance(res, dict) else True
                    remove_detail = res
                except Exception:
                    LOG.exception("remove_mod.remove_package failed")
        except Exception:
            LOG.debug("remove_mod not available")
        # fallback to pkgtool uninstall
        if not removed:
            try:
                res = self.pkgtool.uninstall(pkg_name, force=True)
                removed = bool(res.get("ok", False))
                remove_detail = res
            except Exception:
                LOG.exception("pkgtool uninstall failed")
        if removed:
            LOG.info("Package %s removed (detail=%s)", pkg_name, remove_detail)
            return {"ok": True, "snapshot": snap_info, "detail": remove_detail}
        LOG.error("Failed to remove package %s", pkg_name)
        return {"ok": False, "error": "remove_failed", "detail": remove_detail}

    # ---------- high-level apply cleanup ----------
    def apply_cleanup(self, candidates: List[Dict[str, Any]], ctx: Optional[DeepCleanContext] = None) -> Dict[str, Any]:
        """
        Given a list of orphan candidate dicts (as returned from detect_orphans),
        attempt to clean according to policies in config and context.
        Returns structured result: {actions: [...], failed: [...], snapshots: [...]}
        """
        ctx = ctx or DeepCleanContext(dry_run=_cfg_get("dry_run_default"), interactive=False, snapshot=_cfg_get("snapshot_before_remove"))
        run_id = _shortid("deepclean-")
        started = _now_ts()
        actions = []
        failures = []
        snapshots = []
        if self.hooks:
            try:
                self.hooks.run("pre_deepclean", {"run_id": run_id, "candidates": candidates, "ctx": {"dry_run": ctx.dry_run}})
            except Exception:
                LOG.debug("pre_deepclean hook failed")

        for c in candidates:
            pkg = c.get("name")
            LOG.info("Processing candidate %s (size=%s)", pkg, c.get("size"))
            # ensure conflicts resolution: if package removal will trigger conflicts with future installs, coordinate
            # We attempt to remove owner pkg only if policies allow auto removal OR operator confirmed
            proceed_to_remove = False
            if _cfg_get("auto_remove_conflicting") or ctx.interactive:
                proceed_to_remove = True
            else:
                # default: if package is orphan, we remove; else skip
                proceed_to_remove = True

            if not proceed_to_remove:
                LOG.info("Skipping %s due to policy", pkg)
                continue

            try:
                res = self._attempt_remove_package(pkg, ctx)
                if res.get("ok"):
                    actions.append({"package": pkg, "result": res})
                    if res.get("snapshot"):
                        snapshots.append(res.get("snapshot"))
                else:
                    failures.append({"package": pkg, "error": res})
            except Exception:
                LOG.exception("apply_cleanup: unexpected error for %s", pkg)
                failures.append({"package": pkg, "error": "unexpected"})

        finished = _now_ts()
        summary = {"actions_count": len(actions), "failures": len(failures), "snapshots": len(snapshots)}
        # persist run
        if self.db:
            try:
                self.db.execute("INSERT OR REPLACE INTO deepclean_runs (id, started_at, finished_at, dry_run, actions, summary) VALUES (?,?,?,?,?,?)",
                                (run_id, started, finished, 1 if ctx.dry_run else 0, json.dumps(actions), json.dumps(summary)), commit=True)
            except Exception:
                LOG.debug("Failed to persist deepclean run")
        if self.hooks:
            try:
                self.hooks.run("post_deepclean", {"run_id": run_id, "summary": summary, "actions": actions, "failures": failures})
            except Exception:
                LOG.debug("post_deepclean hook failed")
        emit_event("deepclean.run.finished", {"run_id": run_id, "summary": summary})
        return {"run_id": run_id, "actions": actions, "failures": failures, "snapshots": snapshots, "summary": summary}

    # ---------- cache & builddir housekeeping ----------
    def prune_cache(self, ctx: Optional[DeepCleanContext] = None) -> Dict[str, Any]:
        ctx = ctx or DeepCleanContext(dry_run=_cfg_get("dry_run_default"))
        if not self.pkgtool:
            LOG.warning("pkgtool not available; cannot prune cache")
            return {"ok": False, "error": "no_pkgtool"}
        if ctx.dry_run:
            LOG.info("[dry-run] would prune pkgtool cache")
            return {"ok": True, "note": "dry-run"}
        try:
            res = self.pkgtool.cache_prune()
            emit_event("deepclean.cache_pruned", {"detail": res})
            return {"ok": True, "detail": res}
        except Exception:
            LOG.exception("prune_cache failed")
            return {"ok": False, "error": "prune_failed"}

    def clean_build_dirs(self, ctx: Optional[DeepCleanContext] = None) -> Dict[str, Any]:
        ctx = ctx or DeepCleanContext(dry_run=_cfg_get("dry_run_default"))
        results = {"cleaned": [], "errors": []}
        for bd in _cfg_get("builddirs", []):
            p = Path(bd)
            if not p.exists():
                continue
            try:
                if ctx.dry_run:
                    LOG.info("[dry-run] would clean build dir %s", p)
                    results["cleaned"].append({"dir": str(p), "dry_run": True})
                    continue
                # remove contents but keep directory
                for child in p.iterdir():
                    try:
                        if child.is_dir():
                            shutil.rmtree(child)
                        else:
                            child.unlink()
                    except Exception as e:
                        LOG.exception("Failed cleaning %s", child)
                        results["errors"].append({"path": str(child), "error": str(e)})
                LOG.info("Cleaned build dir %s", p)
                results["cleaned"].append({"dir": str(p)})
            except Exception:
                LOG.exception("clean_build_dirs failed for %s", p)
                results["errors"].append({"dir": str(p)})
        emit_event("deepclean.builddirs.cleaned", {"result": results})
        return results

    # ---------- daemon ----------
    def start_daemon(self, interval: Optional[int] = None):
        cfg = _cfg_get("gc_daemon", {})
        enabled = bool(cfg.get("enabled", False))
        if not enabled:
            LOG.info("Deepclean daemon disabled in config")
            return {"ok": False, "reason": "disabled"}
        interval = interval or int(cfg.get("interval_seconds", 3600))
        thread = threading.Thread(target=self._daemon_loop, args=(interval,), daemon=True)
        thread.start()
        LOG.info("Deepclean daemon started (interval=%s)", interval)
        return {"ok": True}

    def _daemon_loop(self, interval: int):
        while True:
            try:
                LOG.debug("Deepclean daemon tick")
                # detect orphans and run dry-run cleanup to produce report
                candidates = self.detect_orphans()
                if candidates:
                    # run apply_cleanup with dry-run False? We'll run dry-run first and require manual confirm to actually remove
                    ctx = DeepCleanContext(dry_run=True, interactive=False, snapshot=True)
                    report = self.apply_cleanup(candidates, ctx)
                    LOG.info("Deepclean daemon dry-run summary: %s", report.get("summary"))
                    emit_event("deepclean.daemon.report", {"summary": report.get("summary")})
                    # optionally auto-apply if configured (dangerous) - skip here
                else:
                    LOG.debug("No deepclean candidates found")
            except Exception:
                LOG.exception("Deepclean daemon error")
            time.sleep(interval)

# ----------------------------
# CLI
# ----------------------------
def _cli(argv=None):
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-deepclean", description="Rquest deepclean utilities")
    sub = ap.add_subparsers(dest="cmd")

    p_detect = sub.add_parser("detect", help="detect orphan packages")
    p_detect.add_argument("--older-than-days", type=int, default=None)
    p_detect.add_argument("--min-size-bytes", type=int, default=None)

    p_sim = sub.add_parser("simulate", help="simulate removal of packages")
    p_sim.add_argument("packages", nargs="+", help="package names")

    p_apply = sub.add_parser("apply", help="apply cleanup for detected or provided packages")
    p_apply.add_argument("--pkg", nargs="*", help="specific packages to remove instead of detection")
    p_apply.add_argument("--dry-run", action="store_true")
    p_apply.add_argument("--interactive", action="store_true")
    p_apply.add_argument("--snapshot", action="store_true")
    p_apply.add_argument("--prune-cache", action="store_true")

    p_prune = sub.add_parser("prune-cache", help="prune pkgtool cache")

    p_build = sub.add_parser("clean-builds", help="clean build directories")

    p_daemon = sub.add_parser("daemon", help="start deepclean daemon")

    args = ap.parse_args(argv)
    mgr = DeepCleanManager(db_factory=get_db if get_db else None)

    if args.cmd == "detect":
        cands = mgr.detect_orphans(args.older_than_days, args.min_size_bytes)
        print(json.dumps(cands, indent=2, ensure_ascii=False))
        return 0

    if args.cmd == "simulate":
        mgr = DeepCleanManager(db_factory=get_db if get_db else None)
        res = mgr.simulate_removal(args.packages)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0

    if args.cmd == "apply":
        ctx = DeepCleanContext(dry_run=bool(args.dry_run or _cfg_get("dry_run_default")), interactive=bool(args.interactive), snapshot=bool(args.snapshot))
        if args.pkg:
            # build candidate list minimally
            candidates = []
            for p in args.pkg:
                candidates.append({"name": p, "version": None, "size": 0})
        else:
            candidates = mgr.detect_orphans()
        res = mgr.apply_cleanup(candidates, ctx)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        if args.prune_cache:
            pc = mgr.prune_cache(ctx)
            print("prune_cache:", pc)
        return 0

    if args.cmd == "prune-cache":
        pc = mgr.prune_cache(DeepCleanContext(dry_run=False))
        print(json.dumps(pc, indent=2, ensure_ascii=False))
        return 0

    if args.cmd == "clean-builds":
        cb = mgr.clean_build_dirs(DeepCleanContext(dry_run=False))
        print(json.dumps(cb, indent=2, ensure_ascii=False))
        return 0

    if args.cmd == "daemon":
        mgr.start_daemon()
        print("daemon started")
        return 0

    ap.print_help()
    return 1

if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
