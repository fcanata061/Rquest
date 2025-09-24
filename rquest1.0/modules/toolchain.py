# Rquest/rquest1.0/modules/toolchain.py
"""
toolchain.py - Toolchain orchestration for Rquest (complete & functional)

Features:
 - discovers toolchain .meta files by tags (toolchain, pass1, pass2, pass3, final)
 - orchestrates bootstrap stages (pass1..pass3/final)
 - integrates with modules.buildsystem.build_package(...) when available
 - snapshot support (btrfs if present, otherwise lightweight marker)
 - uses modules.sandbox and modules.fakeroot if available
 - records results to db (if modules.db provided)
 - graceful fallback to internal builder when buildsystem not available
 - CLI for bootstrap/list/discover/status with dry-run option
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

# Try optional yaml
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# Integration stubs (import if available; fallback to no-op)
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

# DB integration
try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db():
        return None
    def emit_event(*a, **k):
        pass

# buildsystem integration (this is the primary path)
try:
    from modules.buildsystem import build_package, BuildSystem  # type: ignore
    _HAVE_BUILDSYSTEM = True
except Exception:
    build_package = None
    BuildSystem = None
    _HAVE_BUILDSYSTEM = False

# sandbox & fakeroot integration
try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager():
        return None

try:
    from modules.fakeroot import create_fakeroot  # type: ignore
except Exception:
    create_fakeroot = None

# repo_sync integration (optional)
try:
    from modules.repo_sync import sync_all as repo_sync_all  # type: ignore
except Exception:
    def repo_sync_all(*a, **k):
        return {"ok": False, "error": "repo_sync unavailable"}

# audit hooks
try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    def get_auditor():
        return None

# hooks manager
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager():
        return None

# -----------------------------------------------------------------------------
# Config and defaults
# -----------------------------------------------------------------------------
CFG = get_config() if callable(get_config) else {}
TOOLCHAINS_DIR = CFG.get("toolchain", {}).get("toolchains_dir", os.path.expanduser("~/.rquest/toolchains"))
BOOTSTRAP_BASE = CFG.get("toolchain", {}).get("bootstrap_base", os.path.expanduser("~/.rquest/bootstrap"))
REPO_LOCAL = CFG.get("repos", {}).get("local") if isinstance(CFG.get("repos", {}), dict) else CFG.get("repos", {}).get("local", None) if isinstance(CFG.get("repos"), dict) else None
SNAPSHOT_CFG = CFG.get("snapshots", {}) if isinstance(CFG, dict) else {}

os.makedirs(TOOLCHAINS_DIR, exist_ok=True)
os.makedirs(BOOTSTRAP_BASE, exist_ok=True)

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _uid() -> str:
    return uuid.uuid4().hex[:8]

def _now_ts() -> int:
    return int(time.time())

def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _write_json_atomic(path: Path, obj: Dict[str, Any]):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    tmp.replace(path)

# Run command helper
def _run(cmd: List[str], cwd: Optional[Path] = None, env: Optional[Dict[str,str]] = None, timeout: Optional[int] = None) -> Tuple[int, str, str]:
    try:
        proc = subprocess.Popen(cmd, cwd=(str(cwd) if cwd else None), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=(env or os.environ), text=True)
        out, err = proc.communicate(timeout=timeout)
        return proc.returncode, out or "", err or ""
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        return 124, out or "", err or ""
    except Exception as e:
        return 1, "", str(e)

# -----------------------------------------------------------------------------
# Minimal .meta parser (yaml/json)
# -----------------------------------------------------------------------------
def parse_meta_minimal(path: Path) -> Optional[Dict[str, Any]]:
    try:
        txt = path.read_text(encoding="utf-8")
        # try json
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

# -----------------------------------------------------------------------------
# Snapshot handling
# -----------------------------------------------------------------------------
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
            logger.info("Created btrfs snapshot %s", snap_path)
            return snap_path
        else:
            logger.warning("btrfs snapshot failed: %s", err.strip())
    # fallback: create lightweight marker dir
    snap_dir = os.path.join(base, name)
    try:
        os.makedirs(snap_dir, exist_ok=True)
        marker = Path(snap_dir) / "SNAPSHOT_MARKER"
        marker.write_text(f"snapshot {name} created at {time.ctime()}\n", encoding="utf-8")
        logger.info("Created snapshot marker %s", snap_dir)
        return snap_dir
    except Exception:
        logger.exception("Failed to create snapshot fallback")
        return None

# -----------------------------------------------------------------------------
# Buildsystem adapter wrapper: tries to call buildsystem.build_package(...)
# -----------------------------------------------------------------------------
def build_with_buildsystem(meta_path: Path, profile: Optional[str] = None, sandbox: bool = True, fakeroot: bool = False, env: Optional[Dict[str,str]] = None, dry_run: bool = False) -> Dict[str,Any]:
    """
    Adapter that calls the project's buildsystem if available.
    Expected buildsystem API (recommended):
        build_package(meta_path, profile=None, sandbox=True, fakeroot=False, env=None, dry_run=False) -> dict
    """
    if not _HAVE_BUILDSYSTEM or build_package is None:
        return {"ok": False, "error": "buildsystem-unavailable"}
    try:
        # call buildsystem
        res = build_package(str(meta_path), profile=profile, sandbox=sandbox, fakeroot=fakeroot, env=env, dry_run=dry_run)
        return res if isinstance(res, dict) else {"ok": True, "raw": res}
    except TypeError:
        # some older API may have different signature - try common variants
        try:
            res = build_package(str(meta_path), sandbox=sandbox, env=env)
            return res if isinstance(res, dict) else {"ok": True, "raw": res}
        except Exception as e:
            logger.exception("buildsystem.build_package invocation failed: %s", e)
            return {"ok": False, "error": str(e)}
    except Exception as e:
        logger.exception("buildsystem.build_package failed: %s", e)
        return {"ok": False, "error": str(e)}

# -----------------------------------------------------------------------------
# Fallback internal builder - lightweight (calls commands declared in meta)
# -----------------------------------------------------------------------------
def fallback_build_meta(meta_path: Path, env: Optional[Dict[str,str]] = None, dry_run: bool = False) -> Dict[str,Any]:
    parsed = parse_meta_minimal(meta_path)
    if not parsed:
        return {"ok": False, "error": "invalid_meta"}
    work = Path(tempfile.mkdtemp(prefix="rquest-toolchain-"))
    res: Dict[str,Any] = {"ok": False, "meta": str(meta_path), "errors": [], "logs": []}
    try:
        # download sources if any (basic)
        sources = parsed.get("source") or []
        if isinstance(sources, dict):
            sources = [sources]
        for s in sources:
            url = s.get("url") if isinstance(s, dict) else s
            if not url:
                continue
            fname = os.path.basename(url.split("?")[0])
            dest = work / fname
            if not dest.exists():
                if shutil.which("wget"):
                    rc, out, err = _run(["wget", "-c", "-O", str(dest), url], cwd=work)
                elif shutil.which("curl"):
                    rc, out, err = _run(["curl", "-L", "-o", str(dest), url], cwd=work)
                else:
                    rc, out, err = 1, "", "no-downloader"
                if rc != 0:
                    logger.warning("Failed to download source %s: %s", url, err.strip())
        # run prepare steps
        prepare = parsed.get("prepare", {}) or {}
        for cmd in (prepare.get("steps") or []):
            if dry_run:
                logger.info("[dry-run] would run prepare: %s", cmd)
                continue
            rc, out, err = _run(cmd.split(), cwd=work, env=env)
            if rc != 0:
                res["errors"].append({"stage":"prepare","cmd":cmd,"rc":rc,"err":err})
        # run build steps
        build = parsed.get("build", {}) or {}
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
            rc, out, err = _run(cmd, cwd=work, env=env)
            if rc != 0:
                res["errors"].append({"stage":"build","cmd":step,"rc":rc,"err":err})
                break
        # install
        install = parsed.get("install", {}) or {}
        istps = install.get("steps") or install.get("commands") or ["make install"]
        for step in istps:
            if dry_run:
                logger.info("[dry-run] would run install: %s", step)
                continue
            cmd = step if isinstance(step, list) else step.split()
            rc, out, err = _run(cmd, cwd=work, env=env)
            if rc != 0:
                res["errors"].append({"stage":"install","cmd":step,"rc":rc,"err":err})
                break
        if not res["errors"]:
            res["ok"] = True
        return res
    finally:
        # cleanup
        try:
            shutil.rmtree(work, ignore_errors=True)
        except Exception:
            pass

# -----------------------------------------------------------------------------
# ToolchainManager
# -----------------------------------------------------------------------------
class ToolchainManager:
    def __init__(self):
        self.toolchains_dir = Path(TOOLCHAINS_DIR)
        self.bootstrap_base = Path(BOOTSTRAP_BASE)
        self.repo_local = REPO_LOCAL
        self.db = get_db() if callable(get_db) else None
        self.hooks = get_hook_manager()() if callable(get_hook_manager) else None
        self.auditor = get_auditor()() if callable(get_auditor) else None
        self.sandbox_mgr = get_sandbox_manager()() if callable(get_sandbox_manager) else None
        self._index_toolchains()

    def _index_toolchains(self):
        self.index: Dict[str, Dict[str,Any]] = {}
        try:
            for d in self.toolchains_dir.iterdir():
                if d.is_dir():
                    self.index[d.name] = {"path": str(d)}
        except Exception:
            pass

    def discover_system_compilers(self) -> List[Dict[str,Any]]:
        bins = []
        for name in ("gcc", "g++", "clang", "clang++"):
            p = shutil.which(name)
            if p:
                rc, out, err = _run([p, "--version"])
                ver = out.splitlines()[0] if out else None
                bins.append({"name": name, "path": p, "version": ver})
        return bins

    def _find_metas_for_stage(self, stage_tag: str) -> List[Path]:
        """
        Scan local repo for metas that have 'toolchain' tag and stage_tag.
        """
        metas: List[Path] = []
        candidates = []
        if self.repo_local:
            candidates.append(Path(self.repo_local))
        candidates += [Path("/var/lib/rquest/local-repo"), Path("/var/db/rquest/local-repo"), Path.home() / ".rquest" / "local-repo"]
        for root in candidates:
            if not root or not root.exists():
                continue
            for p in root.rglob("*.meta"):
                parsed = parse_meta_minimal(p)
                if not parsed:
                    continue
                tags = parsed.get("tags") or []
                name = parsed.get("package", {}).get("name") or parsed.get("name", "")
                cat = parsed.get("category") or ""
                if ("toolchain" in tags or cat == "toolchain") and (stage_tag in tags or stage_tag in name or (stage_tag == "final" and "pass" not in name)):
                    metas.append(p)
        # deterministic order
        metas = sorted(metas, key=lambda x: x.name)
        logger.debug("Found %d metas for stage %s", len(metas), stage_tag)
        return metas

    def _record_to_db(self, name: str, stage: str, meta: str, ok: bool, details: Dict[str,Any]):
        try:
            if self.db:
                # assume sqlite wrapper with execute API
                self.db.execute("CREATE TABLE IF NOT EXISTS toolchain_history (id TEXT PRIMARY KEY, name TEXT, stage TEXT, meta TEXT, ok INTEGER, ts INTEGER, details JSON)", (), commit=True)
                rec_id = f"tch-{_uid()}"
                self.db.execute("INSERT INTO toolchain_history (id, name, stage, meta, ok, ts, details) VALUES (?,?,?,?,?,?,?)", (rec_id, name, stage, meta, 1 if ok else 0, _now_ts(), json.dumps(details)), commit=True)
        except Exception:
            logger.debug("DB record failed or DB unavailable")

    def _build_one_meta(self, meta_path: Path, profile: Optional[str], sandbox: bool, fakeroot: bool, dry_run: bool) -> Dict[str,Any]:
        # snapshot
        snap = create_snapshot(f"toolchain-{meta_path.stem}") if create_snapshot else None
        logger.info("Snapshot before building %s: %s", meta_path.name, snap)
        # prefer buildsystem
        if _HAVE_BUILDSYSTEM and build_package:
            logger.info("Using buildsystem for %s", meta_path)
            try:
                res = build_with_buildsystem(meta_path, profile=profile, sandbox=sandbox, fakeroot=fakeroot, env=None, dry_run=dry_run)
                # record
                self._record_to_db(meta_path.stem, profile or "default", str(meta_path), bool(res.get("ok")), res)
                return res
            except Exception:
                logger.exception("buildsystem build failed, falling back")
        # fallback
        logger.info("Falling back to internal builder for %s", meta_path)
        res = fallback_build_meta(meta_path, env=None, dry_run=dry_run)
        self._record_to_db(meta_path.stem, profile or "default", str(meta_path), bool(res.get("ok")), res)
        return res

    def bootstrap(self, name: str = "toolchain", profile: str = "balanced", stages: int = 2, dry_run: bool = False, force: bool = False) -> Dict[str,Any]:
        """
        Orchestrate bootstrap stages for a toolchain.
        stages: number (1..3), 3 includes final stage if metas present.
        dry_run: only simulate actions.
        """
        record: Dict[str,Any] = {"id": f"bootstrap-{_uid()}", "name": name, "profile": profile, "stages": stages, "started_at": _now_ts(), "steps": []}
        # sync repo first
        try:
            repo_res = repo_sync_all()
            logger.debug("repo_sync result: %s", repo_res)
        except Exception:
            logger.debug("repo_sync not available or failed")

        stage_map = {1: "pass1", 2: "pass2", 3: "pass3", 4: "final"}
        to_run = list(range(1, min(int(stages), 3) + 1))
        logger.info("Bootstrapping '%s' profile=%s stages=%s", name, profile, to_run)
        # create base snapshot
        base_snap = create_snapshot(f"{name}-pre") if create_snapshot else None
        record["snapshot_before"] = base_snap

        for s in to_run:
            tag = stage_map.get(s, f"pass{s}")
            logger.info("Running stage %s (%s)", s, tag)
            metas = self._find_metas_for_stage(tag)
            if not metas:
                logger.warning("No metas found for stage %s (%s), skipping", s, tag)
                record["steps"].append({"stage": s, "tag": tag, "skipped": True})
                continue
            step_info = {"stage": s, "tag": tag, "items": []}
            for meta in metas:
                meta_path = Path(meta)
                logger.info("Building meta %s", meta_path)
                # decide sandbox/fakeroot from config
                use_sandbox = CFG.get("sandbox", {}).get("enabled", True) if isinstance(CFG, dict) else True
                use_fakeroot = CFG.get("build", {}).get("fakeroot", False) if isinstance(CFG, dict) else False
                res = self._build_one_meta(meta_path, profile, sandbox=use_sandbox, fakeroot=use_fakeroot, dry_run=dry_run)
                step_info["items"].append({"meta": str(meta_path), "result": res})
                if not res.get("ok") and not force:
                    logger.error("Build failed for %s at stage %s; aborting bootstrap", meta_path, s)
                    step_info["failed"] = True
                    record["steps"].append(step_info)
                    record["failed_at"] = {"stage": s, "meta": str(meta_path)}
                    emit_event("toolchain.bootstrap.failed", {"name": name, "stage": s, "meta": str(meta_path)})
                    return record
            step_info["finished_at"] = _now_ts()
            record["steps"].append(step_info)
            logger.info("Stage %s completed", s)

        # register toolchain directory
        final_dir = self.toolchains_dir / name
        final_dir.mkdir(parents=True, exist_ok=True)
        try:
            rec = {"id": f"tc-{_uid()}", "name": name, "path": str(final_dir), "profile": profile, "created_at": _now_ts()}
            # persist (DB or index file)
            if self.db:
                try:
                    self.db.execute("CREATE TABLE IF NOT EXISTS toolchains (id TEXT PRIMARY KEY, name TEXT, path TEXT, profile TEXT, created_at INTEGER)", (), commit=True)
                    self.db.execute("INSERT OR REPLACE INTO toolchains (id, name, path, profile, created_at) VALUES (?,?,?,?,?)", (rec["id"], rec["name"], rec["path"], rec["profile"], rec["created_at"]), commit=True)
                except Exception:
                    logger.exception("Failed writing toolchain record to DB")
            idx = self.toolchains_dir / "index.json"
            data = _read_json(idx) or {"toolchains": []}
            data["toolchains"] = [t for t in data.get("toolchains", []) if t.get("name") != name]
            data["toolchains"].append(rec)
            _write_json_atomic(idx, data)
            record["registered"] = rec
        except Exception:
            logger.exception("Failed to register toolchain")
        record["finished_at"] = _now_ts()
        emit_event("toolchain.bootstrap.finished", {"name": name, "record_id": record.get("id")})
        return record

    def list_toolchains(self) -> Dict[str,Any]:
        idx = self.toolchains_dir / "index.json"
        data = _read_json(idx) or {"toolchains": []}
        return {"ok": True, "toolchains": data.get("toolchains", [])}

    def status(self) -> Dict[str,Any]:
        # brief status about available toolchains and system compilers
        return {"ok": True, "toolchains_indexed": list(self.index.keys()), "system_compilers": self.discover_system_compilers()}

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def _cli(argv: Optional[List[str]] = None):
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-toolchain", description="Rquest toolchain manager")
    ap.add_argument("action", nargs="?", default="bootstrap", choices=["bootstrap", "list", "discover", "status"])
    ap.add_argument("--name", "-n", default="default-toolchain")
    ap.add_argument("--profile", "-p", default="balanced")
    ap.add_argument("--stages", "-s", type=int, default=2)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)
    mgr = ToolchainManager()
    if args.action == "discover":
        print(json.dumps(mgr.discover_system_compilers(), indent=2, ensure_ascii=False))
    elif args.action == "list":
        print(json.dumps(mgr.list_toolchains(), indent=2, ensure_ascii=False))
    elif args.action == "status":
        print(json.dumps(mgr.status(), indent=2, ensure_ascii=False))
    elif args.action == "bootstrap":
        res = mgr.bootstrap(name=args.name, profile=args.profile, stages=args.stages, dry_run=args.dry_run, force=args.force)
        print(json.dumps(res, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    _cli()
