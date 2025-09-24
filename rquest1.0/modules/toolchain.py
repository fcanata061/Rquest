# rquest1.0/modules/toolchain.py
"""
Toolchain manager for Rquest — fully integrated with buildsystem and other modules.

- Reads .meta files (yaml/json)
- Uses modules.buildsystem.build_package(pkg_meta, force=..., dry_run=..., shards=...)
  (this is the canonical API from your buildsystem.py)
- Snapshots before stages (btrfs if configured)
- Registers results in DB if available
- Fallback internal builder if buildsystem missing
- CLI: bootstrap / list / discover / status / rebuild-world
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
import shutil
import subprocess
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# optional yaml loader
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# ---------------------
# integrate other modules (graceful)
# ---------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("toolchain")
except Exception:
    logger = logging.getLogger("toolchain")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# db support
try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db():
        return None
    def emit_event(*a, **k):
        pass

# buildsystem: expected API: build_package(pkg_meta: Dict, *, force=False, dry_run=False, shards=None) -> Dict
try:
    from modules.buildsystem import get_buildsystem  # type: ignore
    _HAVE_BUILDSYSTEM = True
except Exception:
    get_buildsystem = None  # type: ignore
    _HAVE_BUILDSYSTEM = False

# sandbox & fakeroot (optional)
try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager():
        return None

try:
    from modules.fakeroot import create_fakeroot  # type: ignore
except Exception:
    create_fakeroot = None

# repo sync
try:
    from modules.repo_sync import sync_all as repo_sync_all  # type: ignore
except Exception:
    def repo_sync_all(*a, **k):
        return {"ok": False, "error": "repo_sync unavailable"}

# hooks and audit (optional)
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager():
        return None

try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    def get_auditor():
        return None

# ---------------------
# config / defaults
# ---------------------
CFG = get_config() if callable(get_config) else {}
TC_CFG = CFG.get("toolchain", {}) if isinstance(CFG, dict) else {}
TOOLCHAINS_DIR = TC_CFG.get("toolchains_dir", os.path.expanduser("~/.rquest/toolchains"))
BOOTSTRAP_BASE = TC_CFG.get("bootstrap_base", os.path.expanduser("~/.rquest/bootstrap"))
REPO_LOCAL = CFG.get("repos", {}).get("local") if isinstance(CFG.get("repos", {}), dict) else None
SNAPSHOT_CFG = CFG.get("snapshots", {}) if isinstance(CFG, dict) else {}

os.makedirs(TOOLCHAINS_DIR, exist_ok=True)
os.makedirs(BOOTSTRAP_BASE, exist_ok=True)

# ---------------------
# small helpers
# ---------------------
def _uid() -> str:
    return uuid.uuid4().hex[:10]

def _now() -> int:
    return int(time.time())

def _run(cmd: List[str], cwd: Optional[str] = None, env: Optional[Dict[str,str]] = None, timeout: Optional[int] = None) -> Tuple[int,str,str]:
    """Run cmd returning (rc, stdout, stderr)."""
    try:
        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=(env or os.environ), text=True)
        out, err = p.communicate(timeout=timeout)
        return p.returncode, out or "", err or ""
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
        return 124, out or "", err or ""
    except Exception as e:
        return 1, "", str(e)

def _read_meta(path: Path) -> Optional[Dict[str,Any]]:
    try:
        txt = path.read_text(encoding="utf-8")
        # try json first
        try:
            return json.loads(txt)
        except Exception:
            if yaml:
                try:
                    return yaml.safe_load(txt)
                except Exception:
                    return None
            return None
    except Exception:
        return None

# ---------------------
# snapshot management (btrfs preferred)
# ---------------------
def create_snapshot(name_suffix: str) -> Optional[str]:
    base = SNAPSHOT_CFG.get("path") or "/var/lib/rquest/snapshots"
    os.makedirs(base, exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    name = f"rquest-{name_suffix}-{ts}"
    backend = SNAPSHOT_CFG.get("backend")
    if backend == "btrfs" and shutil.which("btrfs"):
        snap_path = os.path.join(base, name)
        rc, out, err = _run(["btrfs", "subvolume", "snapshot", "/", snap_path])
        if rc == 0:
            logger.info("Created btrfs snapshot: %s", snap_path)
            return snap_path
        logger.warning("btrfs snapshot failed: %s", err.strip())
    # fallback: lightweight marker dir
    snap_dir = os.path.join(base, name)
    try:
        os.makedirs(snap_dir, exist_ok=True)
        marker = Path(snap_dir) / "SNAPSHOT_MARKER"
        marker.write_text(f"snapshot {name} created at {time.ctime()}\n", encoding="utf-8")
        logger.info("Created snapshot marker: %s", snap_dir)
        return snap_dir
    except Exception:
        logger.exception("Failed to create snapshot fallback")
        return None

# ---------------------
# fallback builder - best-effort when buildsystem not available
# ---------------------
def fallback_build(pkg_meta: Dict[str,Any], workdir: Optional[Path] = None, dry_run: bool=False) -> Dict[str,Any]:
    """
    Very small fallback that honors:
      - pkg_meta['source'] list
      - pkg_meta['prepare']['steps']
      - pkg_meta['build']['steps'] or autotools configure+make
      - pkg_meta['install']['steps']
    Returns dict with ok True/False and details.
    """
    res: Dict[str,Any] = {"ok": False, "errors": [], "logs": []}
    work = workdir or Path(tempfile.mkdtemp(prefix="rquest-fallback-"))
    try:
        sources = pkg_meta.get("source") or []
        if isinstance(sources, dict):
            sources = [sources]
        # download sources (basic)
        for s in sources:
            url = s.get("url") if isinstance(s, dict) else s
            if not url:
                continue
            fname = os.path.basename(url.split("?")[0])
            dest = work / fname
            if dest.exists():
                continue
            if shutil.which("wget"):
                if dry_run:
                    logger.info("[dry-run] would download %s", url)
                else:
                    rc, out, err = _run(["wget", "-c", "-O", str(dest), url], cwd=str(work))
                    if rc != 0:
                        res["errors"].append({"stage":"fetch","url":url,"err":err})
        # prepare
        prepare = pkg_meta.get("prepare", {}) or {}
        for cmd in (prepare.get("steps") or []):
            if dry_run:
                logger.info("[dry-run] would run prepare: %s", cmd)
                continue
            rc, out, err = _run(cmd.split(), cwd=str(work))
            if rc != 0:
                res["errors"].append({"stage":"prepare","cmd":cmd,"rc":rc,"err":err})
                return res
        # build
        build = pkg_meta.get("build", {}) or {}
        steps = []
        if build.get("steps"):
            steps = build.get("steps")
        elif build.get("commands"):
            steps = build.get("commands")
        elif build.get("system") == "autotools":
            cfg = build.get("configure", [])
            if cfg:
                steps.append(" ".join(cfg))
                steps.append(f"make -j{os.cpu_count() or 1}")
        for step in steps:
            if dry_run:
                logger.info("[dry-run] would run build: %s", step)
                continue
            cmd = step if isinstance(step, list) else step.split()
            rc, out, err = _run(cmd, cwd=str(work))
            if rc != 0:
                res["errors"].append({"stage":"build","cmd":step,"rc":rc,"err":err})
                return res
        # install
        install = pkg_meta.get("install", {}) or {}
        istps = install.get("steps") or install.get("commands") or ["make install"]
        for step in istps:
            if dry_run:
                logger.info("[dry-run] would run install: %s", step)
                continue
            cmd = step if isinstance(step, list) else step.split()
            rc, out, err = _run(cmd, cwd=str(work))
            if rc != 0:
                res["errors"].append({"stage":"install","cmd":step,"rc":rc,"err":err})
                return res
        res["ok"] = True
        return res
    finally:
        try:
            if workdir is None:
                shutil.rmtree(work, ignore_errors=True)
        except Exception:
            pass

# ---------------------
# ToolchainManager
# ---------------------
class ToolchainManager:
    def __init__(self):
        self.toolchains_dir = Path(TOOLCHAINS_DIR)
        self.bootstrap_base = Path(BOOTSTRAP_BASE)
        self.repo_local = REPO_LOCAL
        self.cfg = CFG
        self.db = None
        try:
            dget = get_db() if callable(get_db) else None
            self.db = dget() if dget else None
        except Exception:
            self.db = None
        # buildsystem instance (object returned by get_buildsystem())
        self.buildsystem = None
        if _HAVE_BUILDSYSTEM and callable(get_buildsystem):
            try:
                self.buildsystem = get_buildsystem()
            except Exception:
                self.buildsystem = None
        # optional modules
        try:
            self.sandbox = get_sandbox_manager()() if callable(get_sandbox_manager) else None
        except Exception:
            self.sandbox = None
        try:
            self.hooks = get_hook_manager()() if callable(get_hook_manager) else None
        except Exception:
            self.hooks = None
        try:
            self.auditor = get_auditor()() if callable(get_auditor) else None
        except Exception:
            self.auditor = None
        self._index_toolchains()

    def _index_toolchains(self):
        self.index: Dict[str,Dict[str,Any]] = {}
        try:
            idxf = self.toolchains_dir / "index.json"
            if idxf.exists():
                data = json.loads(idxf.read_text(encoding="utf-8"))
                for t in data.get("toolchains", []):
                    self.index[t.get("name")] = t
            else:
                # scan directory
                for d in self.toolchains_dir.iterdir():
                    if d.is_dir():
                        self.index[d.name] = {"name": d.name, "path": str(d)}
        except Exception:
            logger.debug("Index toolchains load failed")

    def discover_system_compilers(self) -> List[Dict[str,Any]]:
        results: List[Dict[str,Any]] = []
        candidates = ["gcc", "g++", "clang", "clang++"]
        for c in candidates:
            p = shutil.which(c)
            if not p:
                continue
            rc, out, err = _run([p, "--version"])
            ver = out.splitlines()[0] if out else None
            results.append({"name": c, "path": p, "version": ver})
        return results

    def _find_metas_for_stage(self, stage_tag: str) -> List[Path]:
        """
        Walk local repo(s) and find .meta files matching 'toolchain' tag and stage_tag.
        Heuristics: tags list or package name containing 'pass1' etc.
        """
        metas: List[Path] = []
        candidates: List[Path] = []
        if self.repo_local:
            candidates.append(Path(self.repo_local))
        # common fallbacks
        candidates += [Path("/var/lib/rquest/repos"), Path("/var/lib/rquest/local-repo"), Path.home() / ".rquest" / "local-repo"]
        for root in candidates:
            if not root or not root.exists():
                continue
            for p in root.rglob("*.meta"):
                parsed = _read_meta(p)
                if not parsed:
                    continue
                tags = parsed.get("tags") or []
                name = parsed.get("package", {}).get("name") or parsed.get("name") or p.stem
                cat = parsed.get("category") or ""
                if ("toolchain" in tags or cat == "toolchain"):
                    if stage_tag in tags or stage_tag in name or (stage_tag == "final" and "pass" not in name):
                        metas.append(p)
        metas = sorted(set(metas), key=lambda x: x.name)
        logger.debug("Found %d metas for stage %s", len(metas), stage_tag)
        return metas

    def _persist_history(self, name: str, stage: str, meta: str, ok: bool, details: Dict[str,Any]):
        try:
            if self.db:
                # expect db wrapper with execute method
                self.db.execute("CREATE TABLE IF NOT EXISTS toolchain_history (id TEXT PRIMARY KEY, name TEXT, stage TEXT, meta TEXT, ok INTEGER, ts INTEGER, details JSON)", (), commit=True)
                recid = f"tch-{_uid()}"
                self.db.execute("INSERT INTO toolchain_history (id, name, stage, meta, ok, ts, details) VALUES (?,?,?,?,?,?,?)", (recid, name, stage, meta, 1 if ok else 0, _now(), json.dumps(details)), commit=True)
        except Exception:
            logger.debug("DB persist history failed")

    def _build_one_meta(self, meta_path: Path, *, profile: str = "balanced", dry_run: bool = False, force: bool = False, shards: Optional[int] = None) -> Dict[str,Any]:
        """
        Build a single .meta:
          - parse .meta into pkg_meta (dict)
          - if buildsystem available: call buildsystem.build_package(pkg_meta, force=force, dry_run=dry_run, shards=shards)
          - else fallback_build(pkg_meta)
        """
        logger.info("Building meta %s", meta_path)
        pkg_meta = _read_meta(meta_path)
        if not pkg_meta:
            return {"ok": False, "error": "invalid_meta"}
        snap = create_snapshot(f"{meta_path.stem}") if SNAPSHOT_CFG else None
        if self.buildsystem:
            try:
                # call canonical API from buildsystem: build_package(pkg_meta, force=..., dry_run=..., shards=...)
                try:
                    res = self.buildsystem.build_package(pkg_meta, force=force, dry_run=dry_run, shards=shards)
                except TypeError:
                    # some buildsystems may accept different args; try fallback signatures
                    res = self.buildsystem.build_package(pkg_meta, dry_run=dry_run)
                ok = bool(res.get("ok"))
                self._persist_history(meta_path.stem, profile, str(meta_path), ok, res)
                return res
            except Exception as e:
                logger.exception("buildsystem.build_package failed: %s", e)
                # fallthrough to fallback
        # fallback
        res = fallback_build(pkg_meta, dry_run=dry_run)
        ok = bool(res.get("ok"))
        self._persist_history(meta_path.stem, profile, str(meta_path), ok, res)
        return res

    def bootstrap(self, name: str = "toolchain", profile: str = "balanced", stages: int = 2, dry_run: bool = False, force: bool = False, shards: Optional[int] = None) -> Dict[str,Any]:
        """
        Execute bootstrap stages (1..3) in order.
        Returns a record dict with steps/outcomes.
        """
        rec: Dict[str,Any] = {"id": f"bootstrap-{_uid()}", "name": name, "profile": profile, "stages": stages, "started_at": _now(), "steps": []}
        # sync repos first if available
        try:
            repo_res = repo_sync_all()
            logger.debug("repo_sync result: %s", repo_res)
        except Exception:
            logger.debug("repo_sync not available")

        stage_map = {1: "pass1", 2: "pass2", 3: "pass3", 4: "final"}
        to_run = list(range(1, min(int(stages), 3) + 1))
        logger.info("Bootstrapping '%s' profile=%s stages=%s", name, profile, to_run)

        # base snapshot
        base_snap = create_snapshot(f"{name}-pre") if SNAPSHOT_CFG else None
        rec["snapshot_before"] = base_snap

        for s in to_run:
            tag = stage_map.get(s, f"pass{s}")
            logger.info("Running stage %s (%s)", s, tag)
            metas = self._find_metas_for_stage(tag)
            if not metas:
                logger.warning("No metas for stage %s (%s), skipping", s, tag)
                rec["steps"].append({"stage": s, "tag": tag, "skipped": True})
                continue
            step_info = {"stage": s, "tag": tag, "items": []}
            for meta in metas:
                res = self._build_one_meta(meta, profile=profile, dry_run=dry_run, force=force, shards=shards)
                step_info["items"].append({"meta": str(meta), "result": res})
                if not res.get("ok") and not force:
                    logger.error("Meta %s failed at stage %s. Aborting bootstrap.", meta, s)
                    step_info["failed"] = True
                    rec["failed_at"] = {"stage": s, "meta": str(meta)}
                    rec["steps"].append(step_info)
                    emit_event("toolchain.bootstrap.failed", {"name": name, "stage": s, "meta": str(meta)})
                    return rec
                # optional audit
                try:
                    if self.auditor:
                        self.auditor.audit_package_meta(str(meta))
                except Exception:
                    logger.debug("Auditor absent or audit failed")
            step_info["finished_at"] = _now()
            rec["steps"].append(step_info)
            logger.info("Stage %s finished", s)

        # register resulting toolchain (directory placeholder)
        final_dir = Path(TOOLCHAINS_DIR) / name
        final_dir.mkdir(parents=True, exist_ok=True)
        rec["registered"] = {"id": f"tc-{_uid()}", "name": name, "path": str(final_dir), "profile": profile, "created_at": _now()}

        # persist registry (file + DB if available)
        try:
            idx_path = Path(TOOLCHAINS_DIR) / "index.json"
            data = idx_path.exists() and json.loads(idx_path.read_text(encoding="utf-8")) or {"toolchains": []}
            data["toolchains"] = [t for t in data.get("toolchains", []) if t.get("name") != name]
            data["toolchains"].append(rec["registered"])
            idx_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            if self.db:
                try:
                    self.db.execute("CREATE TABLE IF NOT EXISTS toolchains (id TEXT PRIMARY KEY, name TEXT, path TEXT, profile TEXT, created_at INTEGER)", (), commit=True)
                    self.db.execute("INSERT OR REPLACE INTO toolchains (id, name, path, profile, created_at) VALUES (?,?,?,?,?)", (rec["registered"]["id"], rec["registered"]["name"], rec["registered"]["path"], rec["registered"]["profile"], rec["registered"]["created_at"]), commit=True)
                except Exception:
                    logger.debug("DB register toolchain failed")
        except Exception:
            logger.exception("Failed persisting toolchain registry")

        rec["finished_at"] = _now()
        emit_event("toolchain.bootstrap.finished", {"name": name, "id": rec.get("id")})
        return rec

    def list_toolchains(self) -> Dict[str,Any]:
        idx_path = Path(TOOLCHAINS_DIR) / "index.json"
        data = idx_path.exists() and json.loads(idx_path.read_text(encoding="utf-8")) or {"toolchains": []}
        return {"ok": True, "toolchains": data.get("toolchains", [])}

    def status(self) -> Dict[str,Any]:
        return {"ok": True, "toolchains": list(self.index.keys()), "system_compilers": self.discover_system_compilers()}

    def rebuild_world(self, *, dry_run: bool = False, force: bool = False):
        """
        Rebuild all installed packages using buildsystem. Expects db.get_installed_packages()
        to return list of installed packages with 'meta_path' key.
        """
        if not self.db:
            logger.error("DB unavailable: cannot query installed packages for rebuild")
            return {"ok": False, "error": "db-unavailable"}
        try:
            pkgs = self.db.fetchall("SELECT name, version, meta_path FROM installed_packages ORDER BY name") or []
        except Exception:
            logger.exception("Failed fetching installed packages")
            return {"ok": False, "error": "db-query-failed"}
        results = []
        for p in pkgs:
            meta_path = p.get("meta_path")
            if not meta_path:
                continue
            res = self._build_one_meta(Path(meta_path), profile="balanced", dry_run=dry_run, force=force)
            results.append({"pkg": p.get("name"), "result": res})
            if not res.get("ok") and not force:
                logger.error("Rebuild aborted due to failure in %s", p.get("name"))
                break
        return {"ok": True, "results": results}

# ---------------------
# CLI for toolchain manager
# ---------------------
def _cli(argv: Optional[List[str]] = None):
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-toolchain", description="Toolchain manager for Rquest")
    ap.add_argument("action", nargs="?", choices=["bootstrap", "list", "discover", "status", "rebuild-world"], default="status")
    ap.add_argument("--name", "-n", default="toolchain")
    ap.add_argument("--profile", "-p", default="balanced")
    ap.add_argument("--stages", "-s", type=int, default=2)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--shards", type=int, default=None)
    args = ap.parse_args(argv)

    mgr = ToolchainManager()
    if args.action == "discover":
        print(json.dumps(mgr.discover_system_compilers(), indent=2, ensure_ascii=False))
    elif args.action == "list":
        print(json.dumps(mgr.list_toolchains(), indent=2, ensure_ascii=False))
    elif args.action == "status":
        print(json.dumps(mgr.status(), indent=2, ensure_ascii=False))
    elif args.action == "bootstrap":
        res = mgr.bootstrap(name=args.name, profile=args.profile, stages=args.stages, dry_run=args.dry_run, force=args.force, shards=args.shards)
        print(json.dumps(res, indent=2, ensure_ascii=False))
    elif args.action == "rebuild-world":
        res = mgr.rebuild_world(dry_run=args.dry_run, force=args.force)
        print(json.dumps(res, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    _cli()
