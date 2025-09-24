# Rquest/rquest1.0/modules/deepclean.py
"""
deepclean.py - limpeza inteligente de órfãos, builds temporários, cache e GC

Funcionalidades:
- detect_orphans(dry_run=True) : detecta pacotes órfãos com base em installed_packages + resolver
- simulate_removal(candidates): tenta simular/remover verificando com resolver (se suportado) ou heurística conservadora
- apply_cleanup(candidates, snapshot=True): cria snapshots opcionais, remove pacotes, limpa builds temporários, pruna cache
- prune_cache(): delega para pkgtool.PackageCache pruning LRU/FIFO conforme política
- clean_build_dirs(): limpa diretórios de build antigos conhecidos pelo buildsystem
- GC daemon: roda periodicamente e aplica políticas soft/hard thresholds
- restore_snapshot(snapshot_id): restaura pacote a partir do snapshot salvo
- Relatórios JSON/YAML; hooks para integração
- Registro de runs no DB (deepclean_runs) e candidaturas (deepclean_candidates)
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
import shutil
import sqlite3
import threading
import tempfile
import logging
from typing import Any, Dict, List, Optional, Tuple

# ----------------------------
# Robust imports / fallbacks
# ----------------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("deepclean")
except Exception:
    logger = logging.getLogger("deepclean")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# DB accessor (expected to be a wrapper with execute/fetchone/fetchall)
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def add_history(*a, **k): pass
    def emit_event(*a, **k): pass

# resolver
try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    def get_resolver(): return None

# pkgtool
try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    def get_pkgtool(): return None

# buildsystem
try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:
    def get_buildsystem(): return None

# sandbox
try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager(): return None

# hooks
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

# masks (if present) and slots - used conservatively
try:
    from modules.masks import is_masked  # type: ignore
except Exception:
    def is_masked(*a, **k): return False

# ----------------------------
# Utilities
# ----------------------------
def _now_ts() -> int:
    return int(time.time())

def _human_bytes(n: int) -> str:
    for unit in ['B','KB','MB','GB','TB']:
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def _load_json_safe(path: str) -> Optional[Dict[str,Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

# ----------------------------
# Default config reading
# ----------------------------
CFG = get_config() if callable(get_config) else {}
DEEP_CFG = CFG.get("deepclean", {}) if isinstance(CFG, dict) else {}
DEFAULTS = {
    "enabled": True,
    "dry_run_default": True,
    "protect_labels": ["critical", "toolchain", "hold"],
    "orphan_age_days": 90,
    "orphan_min_size_bytes": 1024,
    "remove_builds": {
        "enabled": True,
        "age_days": 30,
        "keep_last_per_package": 2
    },
    "cache": {
        "max_size_bytes": 20 * 1024**3,
        "prune_policy": "lru"
    },
    "gc_daemon": {
        "enabled": False,
        "check_interval_seconds": 3600,
        "soft_threshold_percent": 80,
        "hard_threshold_percent": 95
    },
    "simulate_before_remove": True,
    "snapshot_before_remove_days": 7,
    "snapshot_dir": os.path.expanduser("~/.rquest/deepclean_snapshots"),
    "trash_dir": os.path.expanduser("~/.rquest/deepclean_trash")
}
# merge config
def _cfg_get(key, default=None):
    if key in DEEP_CFG:
        return DEEP_CFG[key]
    return DEFAULTS.get(key, default)

PROTECT_LABELS = _cfg_get("protect_labels")
ORPHAN_AGE_DAYS = int(_cfg_get("orphan_age_days"))
ORPHAN_MIN_SIZE = int(_cfg_get("orphan_min_size_bytes"))
SNAPSHOT_DIR = _cfg_get("snapshot_dir")
TRASH_DIR = _cfg_get("trash_dir")
SIMULATE_BEFORE_REMOVE = bool(_cfg_get("simulate_before_remove"))
GC_DAEMON_CFG = _cfg_get("gc_daemon")
CACHE_CFG = _cfg_get("cache")

_ensure_dir(SNAPSHOT_DIR)
_ensure_dir(TRASH_DIR)

# ----------------------------
# DB schema helpers for deepclean
# ----------------------------
def _ensure_deepclean_tables(db):
    if not db:
        logger.debug("No DB available; deepclean will operate in stateless mode.")
        return
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS deepclean_runs (
                id TEXT PRIMARY KEY,
                started_at INTEGER,
                finished_at INTEGER,
                mode TEXT,
                reclaimed_bytes INTEGER,
                removed_packages TEXT,
                status TEXT,
                operator TEXT
            );
        """, (), commit=True)
        db.execute("""
            CREATE TABLE IF NOT EXISTS deepclean_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id TEXT,
                package_name TEXT,
                version TEXT,
                path TEXT,
                created_at INTEGER,
                size INTEGER,
                reason TEXT
            );
        """, (), commit=True)
        db.execute("""
            CREATE TABLE IF NOT EXISTS deepclean_candidates (
                run_id TEXT,
                package_name TEXT,
                version TEXT,
                kind TEXT,
                estimated_size INTEGER,
                last_used_ts INTEGER,
                labels TEXT,
                reason TEXT
            );
        """, (), commit=True)
    except Exception:
        logger.exception("Failed to ensure deepclean tables")

# ----------------------------
# DeepCleanManager
# ----------------------------
class DeepCleanManager:
    def __init__(self, cfg: Optional[Dict[str,Any]] = None):
        self.cfg = cfg or DEEP_CFG
        self.db = get_db() if callable(get_db) else None
        _ensure_deepclean_tables(self.db)
        self.resolver = get_resolver() if callable(get_resolver) else None
        self.pkgtool = get_pkgtool() if callable(get_pkgtool) else None
        self.buildsystem = get_buildsystem() if callable(get_buildsystem) else None
        self.sandbox = get_sandbox_manager() if callable(get_sandbox_manager) else None
        self.hooks = get_hook_manager() if callable(get_hook_manager) else None
        self._daemon_thread: Optional[threading.Thread] = None
        self._daemon_stop = threading.Event()
        # operational defaults
        self.orphan_age_days = int(self.cfg.get("orphan_age_days", ORPHAN_AGE_DAYS))
        self.orphan_min_size = int(self.cfg.get("orphan_min_size_bytes", ORPHAN_MIN_SIZE))
        self.protect_labels = list(self.cfg.get("protect_labels", PROTECT_LABELS))
        self.snapshot_dir = self.cfg.get("snapshot_dir", SNAPSHOT_DIR)
        _ensure_dir(self.snapshot_dir)
        self.trash_dir = self.cfg.get("trash_dir", TRASH_DIR)
        _ensure_dir(self.trash_dir)
        self.cache_cfg = self.cfg.get("cache", CACHE_CFG)
        # metrics counters (simple)
        self.metrics = {"runs":0, "reclaimed_bytes":0, "orphans_removed":0, "builds_removed":0, "cache_pruned":0}

    # ----------------------------
    # utility: read installed packages from DB
    # Expect table 'installed_packages' with columns (name, version, installed_at, last_used_ts, labels)
    # If DB missing, fallback to scanning pkgtool cache or local manifests.
    # ----------------------------
    def _read_installed_packages(self) -> List[Dict[str,Any]]:
        res = []
        if self.db:
            try:
                rows = self.db.fetchall("SELECT name, version, installed_at, last_used_ts, labels FROM installed_packages")
                for r in rows:
                    labels = []
                    try:
                        labels = json.loads(r.get("labels") or "[]")
                    except Exception:
                        labels = []
                    res.append({
                        "name": r.get("name"),
                        "version": r.get("version"),
                        "installed_at": r.get("installed_at"),
                        "last_used_ts": r.get("last_used_ts") or 0,
                        "labels": labels
                    })
                return res
            except Exception:
                logger.exception("Failed reading installed_packages from DB")
        # fallback: try reading pkgtool cache index as installed list (best-effort)
        try:
            if self.pkgtool and hasattr(self.pkgtool, "cache"):
                entries = self.pkgtool.cache.list_all()
                for e in entries:
                    res.append({
                        "name": e.get("name"),
                        "version": e.get("version"),
                        "installed_at": e.get("created_at"),
                        "last_used_ts": e.get("last_access") or 0,
                        "labels": []
                    })
        except Exception:
            logger.exception("Fallback reading from pkgtool failed")
        return res

    # ----------------------------
    # detect_orphans: main detector
    # returns list of candidate dicts:
    # { name, version, estimated_size, last_used_ts, labels, reason }
    # ----------------------------
    def detect_orphans(self, dry_run: bool = True, max_candidates: Optional[int] = None) -> List[Dict[str,Any]]:
        logger.info("Detecting orphan packages (dry_run=%s)...", bool(dry_run))
        if self.hooks:
            try:
                self.hooks.run("pre_orphan_detect", context={"dry_run": dry_run})
            except Exception:
                logger.exception("pre_orphan_detect hook failed")

        installed = self._read_installed_packages()
        if not installed:
            logger.info("No installed packages found (or DB unavailable).")
            return []

        # build the "world set": explicit top-level packages -- we consider installed packages with a label 'top' or no dependents in DB
        # For simplicity, treat packages that have label 'explicit' or 'top' as top-level; otherwise attempt to compute via resolver
        top_level = [p for p in installed if "explicit" in (p.get("labels") or []) or "top" in (p.get("labels") or [])]
        # fallback: if none marked explicit, pick recent ones (< 7 days) or all as top-level
        if not top_level:
            # heuristics: mark packages with most recent installed_at as top-level up to 20
            sorted_inst = sorted(installed, key=lambda x: x.get("installed_at") or 0, reverse=True)
            top_level = sorted_inst[:min(20, len(sorted_inst))]

        # Compute dependency closure using resolver if available: union of dependencies for top-level packages
        dep_union: Set[str] = set()
        if self.resolver:
            try:
                # if resolver supports batch/world resolution, ideally call one method. Here we call resolve_dependencies for each top-level and union results.
                for top in top_level:
                    name = top.get("name")
                    version = top.get("version")
                    r = self.resolver.resolve_dependencies(name, version, fast=True)
                    if r and r.get("ok") and isinstance(r.get("tree"), dict):
                        for k in r.get("tree").keys():
                            dep_union.add(k)
            except Exception:
                logger.exception("Resolver failed while computing dependency closure; falling back to conservative heuristics")
        else:
            logger.debug("No resolver available; using conservative heuristics for orphan detection")

        # Determine candidates: installed packages not in dep_union and not protected by labels
        candidates = []
        total_estimated = 0
        for pkg in installed:
            name = pkg.get("name")
            if not name:
                continue
            if name in dep_union:
                continue
            labels = pkg.get("labels") or []
            if any(lbl in self.protect_labels for lbl in labels):
                logger.debug("Package %s has protective label %s - skipping", name, labels)
                continue
            last_used = pkg.get("last_used_ts") or 0
            age_days = (time.time() - (last_used or pkg.get("installed_at") or time.time())) / 86400.0
            # estimated size: try to get from pkgtool cache or manifest size; fallback minimal
            est_size = self._estimate_package_size(pkg.get("name"), pkg.get("version"))
            if est_size < self.orphan_min_size:
                logger.debug("Package %s size %d < min %d -> skipping", name, est_size, self.orphan_min_size)
                continue
            reason = f"orphan (age={int(age_days)}d, last_used={int(last_used)})"
            candidates.append({
                "name": name,
                "version": pkg.get("version"),
                "estimated_size": est_size,
                "last_used_ts": last_used,
                "labels": labels,
                "age_days": int(age_days),
                "reason": reason
            })
            total_estimated += est_size

        # sort candidates by priority: oldest last_used and largest size first
        candidates.sort(key=lambda x: (x["age_days"], -x["estimated_size"]), reverse=True)

        if max_candidates:
            candidates = candidates[:max_candidates]

        if self.db:
            # persist candidate snapshot for this run
            run_id = str(uuid.uuid4())
            for c in candidates:
                try:
                    self.db.execute("INSERT INTO deepclean_candidates (run_id, package_name, version, kind, estimated_size, last_used_ts, labels, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                    (run_id, c["name"], c["version"], "orphan", c["estimated_size"], c["last_used_ts"], json.dumps(c.get("labels") or []), c["reason"]), commit=True)
                except Exception:
                    pass

        logger.info("Detected %d orphan candidate(s), estimated reclaim: %s", len(candidates), _human_bytes(total_estimated))
        if self.hooks:
            try:
                self.hooks.run("post_orphan_detect", context={"candidates": candidates, "dry_run": dry_run})
            except Exception:
                logger.exception("post_orphan_detect hook failed")
        return candidates

    # ----------------------------
    # estimate package size helper
    # ----------------------------
    def _estimate_package_size(self, name: str, version: Optional[str]) -> int:
        # try pkgtool cache
        try:
            if self.pkgtool and hasattr(self.pkgtool, "cache"):
                found = self.pkgtool.cache.find(name, version)
                if found:
                    return int(found.get("size", 0) or 0)
        except Exception:
            logger.exception("pkgtool cache lookup failed")
        # try installed package manifest reading via common locations
        # heuristics: look into /var/lib/rquest/installed/<name>-<version>/MANIFEST.json
        possible = [
            os.path.expanduser(f"~/.rquest/installed/{name}-{version}/MANIFEST.json"),
            f"/var/lib/rquest/installed/{name}-{version}/MANIFEST.json",
        ]
        for p in possible:
            m = _load_json_safe(p)
            if m:
                size = sum([int(f.get("size",0) or 0) for f in m.get("files",[])])
                return size
        # fallback small size
        return 1024 * 100  # 100KB conservative

    # ----------------------------
    # simulate_removal: ensure removing candidates doesn't break world
    # returns dict: { ok: bool, conflicts: [...], protected: [...] }
    # ----------------------------
    def simulate_removal(self, candidates: List[Dict[str,Any]]) -> Dict[str,Any]:
        names = [c["name"] for c in candidates]
        logger.info("Simulating removal for %d candidates...", len(names))
        # If resolver has simulate_removal or can accept a removed set, prefer that
        if self.resolver and hasattr(self.resolver, "simulate_removal"):
            try:
                sim = self.resolver.simulate_removal(names)
                return {"ok": sim.get("ok", False), "conflicts": sim.get("conflicts", []), "details": sim}
            except Exception:
                logger.exception("resolver.simulate_removal failed; falling back to conservative checks")

        # Conservative fallback: check for direct dependency occurrences in installed manifests or pkgtool metadata
        conflicts = []
        protected = []
        # build map of installed package dependencies by scanning cached manifests for each installed package
        installed = self._read_installed_packages()
        installed_names = [p.get("name") for p in installed]
        # for each installed package, try to read its MANIFEST.json from common locations and parse declared dependencies
        for pkg in installed:
            pname = pkg.get("name")
            version = pkg.get("version")
            # skip candidates themselves
            if pname in names:
                continue
            # heuristics: check in pkgtool cache manifest if present
            dep_list = []
            try:
                found = None
                if self.pkgtool and hasattr(self.pkgtool, "cache"):
                    rec = self.pkgtool.cache.find(pname, version)
                    if rec and os.path.exists(rec.get("path")):
                        # extract manifest temporarily to read dependencies
                        td = tempfile.mkdtemp(prefix="deepclean-manifest-")
                        try:
                            self.pkgtool.install_bin(rec.get("path"), target=td, sandbox_run=False, use_fakeroot=False, verify_sig=False, clean_after=True)
                            mf = os.path.join(td, f"{pname}-{version}", "MANIFEST.json")
                            m = _load_json_safe(mf)
                            if m:
                                # manifest may contain 'metadata' or 'depends'
                                md = m.get("metadata") or {}
                                if isinstance(md, dict) and md.get("depends"):
                                    dep_list = md.get("depends") or []
                            shutil.rmtree(td, ignore_errors=True)
                        except Exception:
                            shutil.rmtree(td, ignore_errors=True)
                            pass
                # fallback: attempt to read /var/lib/rquest/installed
                possible = [
                    os.path.expanduser(f"~/.rquest/installed/{pname}-{version}/MANIFEST.json"),
                    f"/var/lib/rquest/installed/{pname}-{version}/MANIFEST.json",
                ]
                for p in possible:
                    m = _load_json_safe(p)
                    if m:
                        md = m.get("metadata") or {}
                        if isinstance(md, dict) and md.get("depends"):
                            dep_list = md.get("depends") or []
            except Exception:
                logger.exception("Error reading manifest for %s", pname)
            # check if any dependency matches names to remove
            for d in dep_list:
                dname = None
                if isinstance(d, dict):
                    dname = d.get("name")
                elif isinstance(d, str):
                    dname = d
                if not dname:
                    continue
                if dname in names:
                    # conflict
                    conflicts.append({"dependent": pname, "depends_on": dname})
        ok = len(conflicts) == 0
        logger.info("Simulation result: ok=%s conflicts=%d", ok, len(conflicts))
        return {"ok": ok, "conflicts": conflicts, "protected": protected}

# end of PART 1
# continuation of Rquest/rquest1.0/modules/deepclean.py (PART 2)

# ----------------------------
# apply_cleanup: remove candidates, snapshots, prune cache, clean build dirs
# candidates: list of dicts as detect_orphans returns
# ----------------------------
    def _create_snapshot_for(self, name: str, version: str, reason: str = "deepclean_orphan") -> Optional[str]:
        """
        Create a quick snapshot (binary) using pkgtool.quickpkg if available.
        Returns snapshot_id (filename or UUID) or None on failure.
        """
        if not self.pkgtool:
            logger.warning("pkgtool not available: cannot create snapshot for %s-%s", name, version)
            return None
        # try to find installed destdir for package - heuristics
        dest_candidates = [
            os.path.expanduser(f"~/.rquest/installed/{name}-{version}"),
            f"/var/lib/rquest/installed/{name}-{version}"
        ]
        destdir = None
        for d in dest_candidates:
            if os.path.exists(d):
                destdir = d
                break
        if not destdir:
            # try to see if pkgtool.cache has package with that name/version and extract its manifest's root to estimate dest
            logger.warning("Cannot find installed destdir for %s-%s to create snapshot", name, version)
            return None
        try:
            res = self.pkgtool.quickpkg(name, version, destdir)
            if res.get("ok"):
                pkg_path = res.get("package_path")
                snapshot_id = str(uuid.uuid4())
                created_at = _now_ts()
                size = res.get("size", 0)
                if self.db:
                    try:
                        self.db.execute("INSERT INTO deepclean_snapshots (snapshot_id, package_name, version, path, created_at, size, reason) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                        (snapshot_id, name, version, pkg_path, created_at, size, reason), commit=True)
                    except Exception:
                        logger.exception("Failed to record snapshot in DB")
                logger.info("Snapshot created for %s-%s: %s", name, version, pkg_path)
                return snapshot_id
        except Exception:
            logger.exception("Snapshot creation failed")
        return None

    def _trash_or_remove_dir(self, path: str) -> bool:
        """
        Move to trash_dir instead of immediate deletion (for safety), then schedule real delete.
        """
        try:
            if not os.path.exists(path):
                return True
            base = os.path.basename(path.rstrip("/"))
            dest = os.path.join(self.trash_dir, f"{base}-{int(time.time())}")
            shutil.move(path, dest)
            logger.info("Moved %s to trash %s", path, dest)
            # schedule deletion in background thread (delayed) - here we just leave in trash; optional GC will purge older trash
            return True
        except Exception:
            logger.exception("Failed to move %s to trash", path)
            return False

    def _remove_installed_package(self, name: str, version: Optional[str]) -> Tuple[bool, str]:
        """
        Remove an installed package by using manifest (if available) or pkgtool uninstall_by_manifest.
        Conservative: if manifest missing, do not attempt blind removal; return False.
        """
        # find manifest path heuristically
        manifest_paths = [
            os.path.expanduser(f"~/.rquest/installed/{name}-{version}/MANIFEST.json"),
            f"/var/lib/rquest/installed/{name}-{version}/MANIFEST.json"
        ]
        for mp in manifest_paths:
            if os.path.exists(mp):
                # use pkgtool.uninstall_by_manifest if available
                try:
                    if self.pkgtool and hasattr(self.pkgtool, "uninstall_by_manifest"):
                        res = self.pkgtool.uninstall_by_manifest(mp)
                        if res.get("ok"):
                            # update DB installed_packages remove record
                            if self.db:
                                try:
                                    self.db.execute("DELETE FROM installed_packages WHERE name = ? AND version = ?", (name, version), commit=True)
                                except Exception:
                                    logger.exception("Failed to remove installed_packages DB record for %s-%s", name, version)
                            return True, "removed_by_manifest"
                        else:
                            return False, "pkgtool_uninstall_failed"
                    else:
                        # fallback: remove parent directory (dangerous) -> move to trash instead
                        dest = os.path.dirname(mp)
                        ok = self._trash_or_remove_dir(dest)
                        if ok and self.db:
                            try:
                                self.db.execute("DELETE FROM installed_packages WHERE name = ? AND version = ?", (name, version), commit=True)
                            except Exception:
                                pass
                        return ok, "moved_to_trash"
                except Exception:
                    logger.exception("Error removing package via manifest")
                    return False, "exception"
        logger.warning("Manifest not found for %s-%s; skipping removal to be safe", name, version)
        return False, "manifest_missing"

    def apply_cleanup(self, candidates: List[Dict[str,Any]], snapshot: bool = True, dry_run: bool = True, interactive: bool = False) -> Dict[str,Any]:
        """
        Apply cleanup: for each candidate create snapshot if requested, remove package, prune cache and clean build dirs.
        Returns run report dict.
        """
        run_id = str(uuid.uuid4())
        start = _now_ts()
        total_reclaimed = 0
        removed_packages = []
        failed = []
        mode = "dry" if dry_run else "apply"
        logger.info("Starting deepclean run %s (dry_run=%s, snapshot=%s)", run_id, dry_run, snapshot)
        if self.hooks:
            try:
                self.hooks.run("pre_clean_run", context={"run_id": run_id, "dry_run": dry_run})
            except Exception:
                logger.exception("pre_clean_run hook failed")

        # If simulate_before_remove is configured, run simulation and abort if conflicts
        if SIMULATE_BEFORE_REMOVE and dry_run is False:
            sim = self.simulate_removal(candidates)
            if not sim.get("ok", True):
                logger.error("Simulation detected conflicts; aborting cleanup. Conflicts: %s", sim.get("conflicts"))
                return {"ok": False, "run_id": run_id, "error": "simulation_conflicts", "conflicts": sim.get("conflicts")}

        for c in candidates:
            name = c.get("name")
            version = c.get("version")
            est = c.get("estimated_size", 0)
            if interactive and not dry_run:
                resp = input(f"Remove {name}-{version} (est { _human_bytes(est) })? [y/N]: ").strip().lower()
                if resp not in ("y","yes"):
                    logger.info("Skipped %s by operator input", name)
                    continue
            # snapshot if requested
            snap_id = None
            if snapshot and not dry_run:
                # If package last_used < snapshot_before_remove_days rule for snapshot, create snapshot
                last_used = c.get("last_used_ts") or 0
                age_days = (time.time() - (last_used or 0)) / 86400.0
                if age_days >= int(self.cfg.get("snapshot_before_remove_days", DEFAULTS["snapshot_before_remove_days"])):
                    snap_id = self._create_snapshot_for(name, version, reason="deepclean_orphan")
            # actual removal
            if dry_run:
                logger.info("[dry-run] would remove %s-%s (est %s) snapshot=%s", name, version, _human_bytes(est), bool(snap_id))
                removed_packages.append({"name": name, "version": version, "estimated_size": est, "snapshot": snap_id, "status": "dry_run"})
                total_reclaimed += est
                continue
            # attempt remove
            ok, reason = self._remove_installed_package(name, version)
            if ok:
                total_reclaimed += est
                removed_packages.append({"name": name, "version": version, "size": est, "snapshot": snap_id, "status": "removed", "reason": reason})
                self.metrics["orphans_removed"] += 1
            else:
                failed.append({"name": name, "version": version, "reason": reason})
                logger.warning("Failed to remove %s-%s: %s", name, version, reason)

        # prune pkgtool cache if configured and not dry_run
        cache_pruned = []
        if self.pkgtool and not dry_run:
            try:
                # call package cache prune via pkgtool.cache._maybe_prune or prune method
                # Here we call cache._maybe_prune (internal) just to trigger pruning
                prev_size = self.pkgtool.cache.total_size() if hasattr(self.pkgtool.cache, "total_size") else None
                self.pkgtool.cache._maybe_prune()
                after_size = self.pkgtool.cache.total_size() if hasattr(self.pkgtool.cache, "total_size") else None
                reclaimed = (prev_size - after_size) if (prev_size and after_size) else 0
                if reclaimed and reclaimed > 0:
                    cache_pruned.append({"reclaimed_bytes": reclaimed})
                    self.metrics["cache_pruned"] += 1
                    total_reclaimed += reclaimed
            except Exception:
                logger.exception("Cache prune failed")

        # clean build dirs: remove old build dirs if configured
        builds_removed = 0
        if self.buildsystem and not dry_run:
            try:
                bs_cfg = self.cfg.get("remove_builds", DEFAULTS["remove_builds"])
                if bs_cfg.get("enabled", True):
                    removed = self.clean_build_dirs(age_days=bs_cfg.get("age_days", 30), keep_last_per_package=bs_cfg.get("keep_last_per_package", 2), dry_run=dry_run)
                    builds_removed = removed.get("removed_count", 0)
                    self.metrics["builds_removed"] += builds_removed
            except Exception:
                logger.exception("clean_build_dirs failed during apply_cleanup")

        # finalize run
        end = _now_ts()
        status = "ok" if not failed else "partial"
        if self.db:
            try:
                self.db.execute("INSERT INTO deepclean_runs (id, started_at, finished_at, mode, reclaimed_bytes, removed_packages, status, operator) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (run_id, start, end, mode, total_reclaimed, json.dumps(removed_packages), status, os.getenv("USER") or ""), commit=True)
            except Exception:
                logger.exception("Failed to record deepclean run in DB")
        self.metrics["reclaimed_bytes"] += total_reclaimed
        logger.info("Deepclean run %s finished: reclaimed=%s removed=%d failed=%d builds_removed=%d", run_id, _human_bytes(total_reclaimed), len(removed_packages), len(failed), builds_removed)
        if self.hooks:
            try:
                self.hooks.run("post_clean_run", context={"run_id": run_id, "removed": removed_packages, "failed": failed, "reclaimed": total_reclaimed})
            except Exception:
                logger.exception("post_clean_run hook failed")
        return {"ok": len(failed)==0, "run_id": run_id, "reclaimed_bytes": total_reclaimed, "removed": removed_packages, "failed": failed}

    # ----------------------------
    # clean_build_dirs: find and remove old build dirs known by buildsystem or caches
    # returns summary dict
    # ----------------------------
    def clean_build_dirs(self, age_days: int = 30, keep_last_per_package: int = 2, dry_run: bool = True) -> Dict[str,Any]:
        logger.info("Cleaning build dirs older than %dd (keep_last_per_package=%d) (dry_run=%s)", age_days, keep_last_per_package, dry_run)
        removed = []
        if not self.buildsystem:
            logger.warning("No buildsystem manager available; cannot list build dirs")
            return {"removed": removed, "removed_count": 0}
        try:
            # buildsystem may provide an API to list build roots; attempt common method names
            build_roots = []
            if hasattr(self.buildsystem, "list_build_roots"):
                build_roots = self.buildsystem.list_build_roots()
            elif hasattr(self.buildsystem, "cache_dir"):
                # heuristic: look under cache_dir for <pkg>-<ver> directories
                cd = getattr(self.buildsystem, "cache_dir", None)
                if cd and os.path.exists(cd):
                    for entry in os.listdir(cd):
                        build_roots.append(os.path.join(cd, entry))
            else:
                logger.warning("buildsystem does not expose build roots API; falling back to scanning common tmp dirs")
                # fallback scan /var/tmp/rquest/build* or ~/.rquest/build*
                possible = ["/var/tmp/rquest", os.path.expanduser("~/.rquest/build")]
                for p in possible:
                    if os.path.exists(p):
                        for e in os.listdir(p):
                            build_roots.append(os.path.join(p, e))
            # for each root, inspect modification time and decide removal
            cutoff = time.time() - age_days * 86400
            # group by package base if needed, preserve last N per package
            grouped = {}
            for br in build_roots:
                base = os.path.basename(br)
                # we consider directories only
                if not os.path.isdir(br):
                    continue
                mtime = os.path.getmtime(br)
                grouped.setdefault(base.split("-")[0], []).append((br, mtime))
            for pkg, lst in grouped.items():
                # sort by mtime desc
                lst.sort(key=lambda x: x[1], reverse=True)
                # keep last N
                keep = lst[:keep_last_per_package]
                to_check = lst[keep_last_per_package:]
                for (path, mtime) in to_check:
                    if mtime < cutoff:
                        if dry_run:
                            logger.info("[dry-run] would remove build dir %s (mtime=%s)", path, time.ctime(mtime))
                            removed.append({"path": path, "mtime": mtime, "action": "dry-run"})
                        else:
                            ok = self._trash_or_remove_dir(path)
                            if ok:
                                removed.append({"path": path, "mtime": mtime, "action": "removed"})
            return {"removed": removed, "removed_count": len(removed)}
        except Exception:
            logger.exception("clean_build_dirs error")
            return {"removed": removed, "removed_count": len(removed)}

    # ----------------------------
    # prune_cache: delegate to pkgtool cache pruning
    # ----------------------------
    def prune_cache(self) -> Dict[str,Any]:
        if not self.pkgtool or not hasattr(self.pkgtool, "cache"):
            logger.warning("pkgtool cache not available; cannot prune")
            return {"ok": False, "reason": "no_pkgtool_cache"}
        try:
            before = self.pkgtool.cache.total_size()
            self.pkgtool.cache._maybe_prune()
            after = self.pkgtool.cache.total_size()
            reclaimed = (before - after)
            self.metrics["cache_pruned"] += 1
            logger.info("Cache prune done: reclaimed %s", _human_bytes(reclaimed))
            return {"ok": True, "reclaimed_bytes": reclaimed}
        except Exception:
            logger.exception("prune_cache failed")
            return {"ok": False, "reason": "exception"}

    # ----------------------------
    # snapshot restore
    # ----------------------------
    def restore_snapshot(self, snapshot_id: str, target: Optional[str] = None) -> Dict[str,Any]:
        """
        Restore snapshot (created earlier) into target (or default installed path).
        """
        if not self.db:
            logger.error("DB required for snapshot restore")
            return {"ok": False, "error": "no_db"}
        try:
            row = self.db.fetchone("SELECT snapshot_id, package_name, version, path FROM deepclean_snapshots WHERE snapshot_id = ? ORDER BY created_at DESC LIMIT 1", (snapshot_id,))
            if not row:
                return {"ok": False, "error": "snapshot_not_found"}
            pkg_path = row.get("path")
            name = row.get("package_name")
            version = row.get("version")
            # target default installed location
            if not target:
                target = os.path.expanduser(f"~/.rquest/installed/{name}-{version}")
            # ensure target exists
            _ensure_dir(target)
            # use pkgtool.install_bin to restore
            if self.pkgtool:
                res = self.pkgtool.install_bin(pkg_path, target=target, use_fakeroot=True, verify_sig=False, sandbox_run=False)
                if res.get("ok"):
                    logger.info("Snapshot %s restored to %s", snapshot_id, target)
                    return {"ok": True, "target": target}
                else:
                    return {"ok": False, "error": "install_failed", "detail": res}
            else:
                # fallback: decompress tar directly
                from modules.pkgtool import _decompress_tar  # try to use function if exposed
                try:
                    _decompress_tar(pkg_path, target)
                    return {"ok": True, "target": target}
                except Exception:
                    logger.exception("restore_snapshot fallback failed")
                    return {"ok": False, "error": "restore_failed"}
        except Exception:
            logger.exception("restore_snapshot error")
            return {"ok": False, "error": "exception"}

    # ----------------------------
    # GC daemon: background thread performing scheduled cleanup based on thresholds
    # ----------------------------
    def _gc_daemon_loop(self, check_interval: int, soft_pct: int, hard_pct: int):
        logger.info("GC daemon started: interval=%ds soft=%d%% hard=%d%%", check_interval, soft_pct, hard_pct)
        while not self._daemon_stop.wait(check_interval):
            try:
                # compute disk usage on cache_dir or root FS
                cache_dir = self.pkgtool.cache.cache_dir if (self.pkgtool and hasattr(self.pkgtool, "cache")) else self.snapshot_dir
                st = shutil.disk_usage(cache_dir)
                used_pct = int((st.used / st.total) * 100) if st.total > 0 else 0
                logger.debug("GC daemon disk usage for %s: %d%%", cache_dir, used_pct)
                if used_pct >= hard_pct:
                    logger.warning("Hard threshold reached (%d%%) - running aggressive clean", used_pct)
                    # aggressive: prune cache and remove orphans regardless of age (but keep protect labels)
                    cands = self.detect_orphans(dry_run=False, max_candidates=1000)
                    # mark all as to-be-removed (non-dry)
                    self.apply_cleanup(cands, snapshot=True, dry_run=False)
                    self.prune_cache()
                elif used_pct >= soft_pct:
                    logger.info("Soft threshold reached (%d%%) - attempting conservative prune", used_pct)
                    # conservative: prune cache, remove very old build dirs
                    self.prune_cache()
                    bs_conf = self.cfg.get("remove_builds", DEFAULTS["remove_builds"])
                    self.clean_build_dirs(age_days=bs_conf.get("age_days", 30) * 2, keep_last_per_package=bs_conf.get("keep_last_per_package", 2), dry_run=False)
                else:
                    logger.debug("GC daemon: thresholds not reached (%d%%)", used_pct)
            except Exception:
                logger.exception("GC daemon loop exception")
        logger.info("GC daemon exiting")

    def start_daemon(self):
        if self._daemon_thread and self._daemon_thread.is_alive():
            logger.info("GC daemon already running")
            return {"ok": False, "reason": "already_running"}
        cfg = self.cfg.get("gc_daemon", GC_DAEMON_CFG)
        if not cfg.get("enabled", False):
            logger.info("GC daemon is disabled in configuration")
            return {"ok": False, "reason": "disabled"}
        interval = int(cfg.get("check_interval_seconds", 3600))
        soft = int(cfg.get("soft_threshold_percent", 80))
        hard = int(cfg.get("hard_threshold_percent", 95))
        self._daemon_stop.clear()
        self._daemon_thread = threading.Thread(target=self._gc_daemon_loop, args=(interval, soft, hard), daemon=True)
        self._daemon_thread.start()
        return {"ok": True}

    def stop_daemon(self):
        if not self._daemon_thread:
            return {"ok": False, "reason": "not_running"}
        self._daemon_stop.set()
        self._daemon_thread.join(timeout=10)
        return {"ok": True}

    # ----------------------------
    # run convenience for CLI
    # ----------------------------
    def run(self, apply: bool = False, interactive: bool = False, dry_run: Optional[bool] = None, max_candidates: Optional[int] = None):
        if dry_run is None:
            dry_run = bool(self.cfg.get("dry_run_default", DEFAULTS["dry_run_default"]))
        logger.info("DeepCleanManager.run apply=%s dry_run=%s interactive=%s", apply, dry_run, interactive)
        cands = self.detect_orphans(dry_run=True, max_candidates=max_candidates)
        if not cands:
            logger.info("No candidates found for cleanup")
            return {"ok": True, "candidates": []}
        if dry_run:
            # produce report and return
            report = {"candidates": cands, "estimate_reclaim": sum([c.get("estimated_size",0) for c in cands])}
            # store run record as dry-run
            run_id = str(uuid.uuid4())
            if self.db:
                try:
                    self.db.execute("INSERT INTO deepclean_runs (id, started_at, finished_at, mode, reclaimed_bytes, removed_packages, status, operator) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                    (run_id, _now_ts(), _now_ts(), "dry", 0, json.dumps([]), "dry", os.getenv("USER") or ""), commit=True)
                except Exception:
                    pass
            return {"ok": True, "report": report}
        # else apply
        return self.apply_cleanup(cands, snapshot=True, dry_run=not apply, interactive=interactive)

# ----------------------------
# module-level helper and CLI
# ----------------------------
_MANAGER: Optional[DeepCleanManager] = None

def get_deepclean_manager() -> DeepCleanManager:
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = DeepCleanManager()
    return _MANAGER

# CLI entry
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="deepclean", description="Deep cleaning of orphans, build dirs and cache")
    ap.add_argument("--apply", action="store_true", help="apply changes (default is dry-run)")
    ap.add_argument("--interactive", action="store_true", help="ask for confirmation per candidate when applying")
    ap.add_argument("--max-candidates", type=int, default=50, help="max orphan candidates to consider")
    ap.add_argument("--start-daemon", action="store_true", help="start GC daemon (if enabled in config)")
    ap.add_argument("--stop-daemon", action="store_true", help="stop GC daemon")
    ap.add_argument("--status", action="store_true", help="show last runs summary")
    args = ap.parse_args()

    mgr = get_deepclean_manager()
    if args.start_daemon:
        r = mgr.start_daemon()
        print(r)
        sys.exit(0)
    if args.stop_daemon:
        r = mgr.stop_daemon()
        print(r)
        sys.exit(0)
    if args.status:
        # fetch last 10 runs
        if mgr.db:
            try:
                rows = mgr.db.fetchall("SELECT id, started_at, finished_at, mode, reclaimed_bytes, status FROM deepclean_runs ORDER BY started_at DESC LIMIT 10")
                for r in rows:
                    print(f"{r.get('id')} {time.ctime(r.get('started_at'))} -> {time.ctime(r.get('finished_at'))} mode={r.get('mode')} reclaimed={_human_bytes(r.get('reclaimed_bytes') or 0)} status={r.get('status')}")
            except Exception:
                print("failed to read deepclean_runs")
        else:
            print("No DB available")
        sys.exit(0)

    result = mgr.run(apply=args.apply, interactive=args.interactive, dry_run=not args.apply, max_candidates=args.max_candidates)
    print(json.dumps(result, indent=2, ensure_ascii=False))
