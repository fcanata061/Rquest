# rquest1.0/modules/buildsystem.py
# -*- coding: utf-8 -*-
"""
BuildSystem for Rquest — integrated with modules.config (config.yaml)

API:
  bs = get_buildsystem()
  res = bs.build_package(pkg_meta: dict, *, force: bool=False, dry_run: bool=False, shards: Optional[int]=None)

Respects config.yaml under key "build":
  build:
    jobs: 8
    cflags: "-O2 -march=native -pipe"
    cxxflags: "-O2 -march=native -pipe"
    ldflags: "-Wl,--as-needed"
    prefix: "/usr"
    builddir: "/var/tmp/rquest/build"
    fakeroot: false
    keep_build_dirs: false
    cache_dir: "/var/cache/rquest/build"
"""

from __future__ import annotations

import os
import sys
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# optional yaml
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# --- integrations (graceful) ---
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("buildsystem")
except Exception:
    import logging
    logger = logging.getLogger("rquest.buildsystem")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# optional components
try:
    from modules.fetcher import Fetcher  # type: ignore
except Exception:
    Fetcher = None

try:
    from modules.patches import PatchManager  # type: ignore
except Exception:
    PatchManager = None

try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager():
        return None

try:
    from modules.fakeroot import create_fakeroot  # type: ignore
except Exception:
    create_fakeroot = None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    get_hook_manager = None

try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    get_db = None
    def emit_event(*a, **k):
        pass

try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    get_auditor = None

try:
    from modules.repo_sync import sync_all as repo_sync_all  # type: ignore
except Exception:
    repo_sync_all = None

# --- config defaults read from config.yaml ---
CFG = get_config() if callable(get_config) else {}
BUILD_CFG = CFG.get("build", {}) if isinstance(CFG, dict) else {}

DEFAULT_JOBS = int(BUILD_CFG.get("jobs", os.cpu_count() or 1))
DEFAULT_CFLAGS = str(BUILD_CFG.get("cflags", "") or "")
DEFAULT_CXXFLAGS = str(BUILD_CFG.get("cxxflags", "") or "")
DEFAULT_LDFLAGS = str(BUILD_CFG.get("ldflags", "") or "")
DEFAULT_PREFIX = str(BUILD_CFG.get("prefix", "/usr"))
DEFAULT_BUILD_BASE = str(BUILD_CFG.get("builddir", "/var/tmp/rquest/build"))
DEFAULT_FAKEROOT = bool(BUILD_CFG.get("fakeroot", False))
KEEP_BUILD_DIRS = bool(BUILD_CFG.get("keep_build_dirs", False))
CACHE_DIR = Path(BUILD_CFG.get("cache_dir", "/var/cache/rquest/build"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# --- helpers ---
def _now_ts() -> int:
    return int(time.time())

def _uid() -> str:
    import uuid
    return uuid.uuid4().hex[:8]

def _safe_run(cmd: List[str], cwd: Optional[Path] = None, env: Optional[Dict[str,str]] = None, timeout: Optional[int] = None) -> Tuple[int,str,str]:
    """Run command and capture output. Return (rc, stdout, stderr)."""
    try:
        logger.debug("RUN: %s (cwd=%s)", " ".join(cmd), str(cwd) if cwd else None)
        p = subprocess.Popen(cmd, cwd=str(cwd) if cwd else None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=(env or os.environ), text=True)
        out, err = p.communicate(timeout=timeout)
        return p.returncode, out or "", err or ""
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
        return 124, out or "", err or ""
    except Exception as e:
        logger.exception("Command failed: %s", e)
        return 1, "", str(e)

# --- meta reader (yaml/json) helper for convenience ---
def _load_meta_from_path(path: Path) -> Optional[Dict[str,Any]]:
    try:
        txt = path.read_text(encoding="utf-8")
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

# --- detect build system ---
def detect_build_system(pkg_meta: Dict[str,Any], srcdir: Optional[Path] = None) -> str:
    build = pkg_meta.get("build") or {}
    if isinstance(build, dict) and build.get("system"):
        return str(build.get("system"))
    # heuristics: check srcdir for common files
    if srcdir and srcdir.exists():
        if (srcdir / "configure").exists() or (srcdir / "autogen.sh").exists():
            return "autotools"
        if (srcdir / "CMakeLists.txt").exists():
            return "cmake"
        if (srcdir / "pyproject.toml").exists() or (srcdir / "setup.py").exists():
            return "python"
        if (srcdir / "Cargo.toml").exists():
            return "cargo"
    # fallback: if sources look like tarballs, assume autotools; else generic
    sources = pkg_meta.get("source") or []
    if isinstance(sources, list) and any(str(s).endswith((".tar.gz", ".tar.xz", ".tar.bz2", ".zip")) for s in sources):
        return "autotools"
    return "generic"

# --- env assembly (merges config + meta) ---
def assemble_env(pkg_meta: Dict[str,Any], profile: Optional[str] = None) -> Dict[str,str]:
    env = dict(os.environ)
    # base flags from config
    cflags = DEFAULT_CFLAGS
    cxxflags = DEFAULT_CXXFLAGS
    ldflags = DEFAULT_LDFLAGS
    # override/append from meta build.env or meta.top-level keys
    meta_build = pkg_meta.get("build", {}) or {}
    meta_env = meta_build.get("env") or pkg_meta.get("env") or {}
    # explicit flags in meta
    if meta_env.get("CFLAGS"):
        cflags = f"{cflags} {meta_env.get('CFLAGS')}".strip() if cflags else meta_env.get("CFLAGS")
    if meta_env.get("CXXFLAGS"):
        cxxflags = f"{cxxflags} {meta_env.get('CXXFLAGS')}".strip() if cxxflags else meta_env.get("CXXFLAGS")
    if meta_env.get("LDFLAGS"):
        ldflags = f"{ldflags} {meta_env.get('LDFLAGS')}".strip() if ldflags else meta_env.get("LDFLAGS")
    # env values
    if cflags:
        env["CFLAGS"] = cflags
    if cxxflags:
        env["CXXFLAGS"] = cxxflags
    if ldflags:
        env["LDFLAGS"] = ldflags
    # MAKEFLAGS/JOBS
    jobs = str(meta_build.get("jobs") or BUILD_CFG.get("jobs") or DEFAULT_JOBS)
    env["JOBS"] = jobs
    makeflags = meta_env.get("MAKEFLAGS") or os.environ.get("MAKEFLAGS", "")
    if makeflags:
        env["MAKEFLAGS"] = makeflags
    # PATH adjustments (prefix tools)
    prefix = str(meta_build.get("prefix") or BUILD_CFG.get("prefix") or DEFAULT_PREFIX)
    env["PREFIX"] = prefix
    # other env passed in meta
    for k, v in (meta_env.items() if isinstance(meta_env, dict) else []):
        if isinstance(v, (str,int)):
            env[str(k)] = str(v)
    return env

# --- fetch sources ---
def fetch_sources(pkg_meta: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
    res = {"ok": True, "files": [], "errors": []}
    sources = pkg_meta.get("source") or []
    if isinstance(sources, dict):
        sources = [sources]
    if not sources:
        logger.debug("No sources declared in meta")
        return res
    # prefer Fetcher module
    if Fetcher:
        try:
            fetcher = Fetcher(cache_dir=str(CACHE_DIR))
            for s in sources:
                if dry_run:
                    logger.info("[dry-run] would fetch %s", s)
                    continue
                try:
                    p = fetcher.fetch(s)
                    res["files"].append(str(p))
                except Exception as e:
                    logger.exception("Fetcher failed for %s: %s", s, e)
                    res["errors"].append({"source": s, "error": str(e)})
                    res["ok"] = False
            return res
        except Exception:
            logger.debug("Fetcher module failed; falling back to wget/curl")
    # fallback: wget/curl
    for s in sources:
        url = s.get("url") if isinstance(s, dict) else s
        if not url:
            continue
        fname = os.path.basename(str(url).split("?")[0])
        dest = workdir / fname
        if dest.exists():
            res["files"].append(str(dest))
            continue
        if dry_run:
            logger.info("[dry-run] would download %s", url)
            continue
        if shutil.which("wget"):
            rc, out, err = _safe_run(["wget", "-c", "-O", str(dest), url], cwd=workdir)
        elif shutil.which("curl"):
            rc, out, err = _safe_run(["curl", "-L", "-o", str(dest), url], cwd=workdir)
        else:
            rc, out, err = 1, "", "no-downloader"
        if rc != 0:
            logger.warning("Failed to download %s: %s", url, err.strip())
            res["errors"].append({"source": url, "err": err})
            res["ok"] = False
        else:
            res["files"].append(str(dest))
    return res

# --- apply patches ---
def apply_patches(pkg_meta: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
    res = {"ok": True, "applied": [], "errors": []}
    patches = pkg_meta.get("patches") or pkg_meta.get("prepare", {}).get("patches") or []
    if not patches:
        return res
    if PatchManager:
        try:
            pm = PatchManager()
            for p in patches:
                if dry_run:
                    logger.info("[dry-run] would apply patch %s", p)
                    continue
                ok = pm.apply(p, cwd=str(workdir))
                if ok:
                    res["applied"].append(p)
                else:
                    res["errors"].append({"patch": p, "error": "apply-failed"})
                    res["ok"] = False
            return res
        except Exception:
            logger.debug("PatchManager failed; fallback to patch utility")
    # fallback to 'patch' command
    for p in patches:
        p_path = Path(p)
        if not p_path.exists():
            logger.warning("Patch not found: %s", p)
            res["errors"].append({"patch": p, "error": "not-found"})
            res["ok"] = False
            continue
        if dry_run:
            logger.info("[dry-run] would apply patch %s", p)
            continue
        rc, out, err = _safe_run(["patch", "-p1", "-i", str(p_path)], cwd=workdir)
        if rc != 0:
            res["errors"].append({"patch": p, "err": err})
            res["ok"] = False
        else:
            res["applied"].append(str(p_path))
    return res

# --- hooks ---
def run_hooks(phase: str, pkg_meta: Dict[str,Any], cwd: Path, env: Dict[str,str], dry_run: bool=False) -> Dict[str,Any]:
    res = {"ok": True, "ran": [], "errors": []}
    hooks_def = (pkg_meta.get("hooks") or {}).get(phase) or []
    if not hooks_def and get_hook_manager:
        try:
            hm = get_hook_manager()()
            hooks_def = hm.get_hooks_for((pkg_meta.get("package") or {}).get("name"), phase) or []
        except Exception:
            pass
    for h in hooks_def:
        if dry_run:
            logger.info("[dry-run] would run hook %s: %s", phase, h)
            continue
        try:
            if isinstance(h, str):
                rc, out, err = _safe_run(h.split(), cwd=cwd, env=env)
            else:
                rc, out, err = _safe_run(list(h), cwd=cwd, env=env)
            if rc != 0:
                res["errors"].append({"hook": h, "rc": rc, "err": err})
                res["ok"] = False
            else:
                res["ran"].append({"hook": h, "out": out})
        except Exception as e:
            res["errors"].append({"hook": h, "error": str(e)})
            res["ok"] = False
    return res

# --- configure / compile / test / install helpers ---
def run_configure(pkg_meta: Dict[str,Any], srcdir: Path, builddir: Optional[Path], env: Dict[str,str], dry_run: bool=False) -> Dict[str,Any]:
    build = pkg_meta.get("build") or {}
    system = build.get("system") or detect_build_system(pkg_meta, srcdir)
    out = {"ok": True, "cmds": []}
    if system == "autotools":
        cfg = build.get("configure") or build.get("args") or [f"{srcdir}/configure --prefix={env.get('PREFIX', DEFAULT_PREFIX)}"]
        bdir = builddir or srcdir
        bdir.mkdir(parents=True, exist_ok=True)
        for cmd in (cfg if isinstance(cfg, list) else [cfg]):
            if dry_run:
                logger.info("[dry-run] would run configure: %s", cmd)
                out["cmds"].append(cmd)
                continue
            rc, stdout, stderr = _safe_run(cmd.split(), cwd=bdir, env=env)
            out["cmds"].append({"cmd": cmd, "rc": rc, "out": stdout, "err": stderr})
            if rc != 0:
                out["ok"] = False
                return out
    elif system == "cmake":
        bdir = builddir or (srcdir / "build")
        bdir.mkdir(parents=True, exist_ok=True)
        cfg = build.get("configure") or [f"cmake {str(srcdir)} -DCMAKE_INSTALL_PREFIX={env.get('PREFIX', DEFAULT_PREFIX)}"]
        for cmd in (cfg if isinstance(cfg, list) else [cfg]):
            if dry_run:
                logger.info("[dry-run] would run cmake: %s", cmd)
                out["cmds"].append(cmd)
                continue
            rc, stdout, stderr = _safe_run(cmd.split(), cwd=bdir, env=env)
            out["cmds"].append({"cmd": cmd, "rc": rc, "out": stdout, "err": stderr})
            if rc != 0:
                out["ok"] = False
                return out
    elif system == "python":
        cmds = build.get("commands") or [f"python -m pip wheel . -w dist"]
        for cmd in (cmds if isinstance(cmds, list) else [cmds]):
            if dry_run:
                logger.info("[dry-run] would run python build: %s", cmd)
                out["cmds"].append(cmd)
                continue
            rc, stdout, stderr = _safe_run(cmd.split(), cwd=srcdir, env=env)
            out["cmds"].append({"cmd": cmd, "rc": rc, "out": stdout, "err": stderr})
            if rc != 0:
                out["ok"] = False
                return out
    elif system == "cargo":
        cmds = build.get("commands") or ["cargo build --release"]
        for cmd in (cmds if isinstance(cmds, list) else [cmds]):
            if dry_run:
                logger.info("[dry-run] would run cargo: %s", cmd)
                out["cmds"].append(cmd)
                continue
            rc, stdout, stderr = _safe_run(cmd.split(), cwd=srcdir, env=env)
            out["cmds"].append({"cmd": cmd, "rc": rc, "out": stdout, "err": stderr})
            if rc != 0:
                out["ok"] = False
                return out
    else:
        # generic: use 'make' by default
        make_cmd = f"make -j{env.get('JOBS', str(DEFAULT_JOBS))}"
        cmds = build.get("commands") or build.get("steps") or [make_cmd]
        for cmd in (cmds if isinstance(cmds, list) else [cmds]):
            if dry_run:
                logger.info("[dry-run] would run build step: %s", cmd)
                out["cmds"].append(cmd)
                continue
            rc, stdout, stderr = _safe_run(cmd.split(), cwd=builddir or srcdir, env=env)
            out["cmds"].append({"cmd": cmd, "rc": rc, "out": stdout, "err": stderr})
            if rc != 0:
                out["ok"] = False
                return out
    return out

def run_install(pkg_meta: Dict[str,Any], srcdir: Path, builddir: Optional[Path], env: Dict[str,str], use_fakeroot: bool=False, dry_run: bool=False) -> Dict[str,Any]:
    res = {"ok": True, "cmds": []}
    install_def = (pkg_meta.get("install") or {}).get("commands") or (pkg_meta.get("install") or {}).get("steps") or None
    if install_def:
        for cmd in (install_def if isinstance(install_def, list) else [install_def]):
            if dry_run:
                logger.info("[dry-run] would run install: %s", cmd)
                res["cmds"].append({"cmd": cmd, "skipped": True})
                continue
            parts = cmd.split()
            if use_fakeroot and create_fakeroot:
                try:
                    fr = create_fakeroot()
                    rc, out, err = fr.run(parts, cwd=str(builddir or srcdir), env=env)
                except Exception:
                    logger.exception("fakeroot.run failed; falling back to direct install")
                    rc, out, err = _safe_run(parts, cwd=builddir or srcdir, env=env)
            else:
                rc, out, err = _safe_run(parts, cwd=builddir or srcdir, env=env)
            res["cmds"].append({"cmd": cmd, "rc": rc, "out": out, "err": err})
            if rc != 0:
                res["ok"] = False
                return res
        return res
    # default install: make install
    if dry_run:
        logger.info("[dry-run] would run: make install")
        res["cmds"].append({"cmd": "make install", "skipped": True})
        return res
    if use_fakeroot and create_fakeroot:
        try:
            fr = create_fakeroot()
            rc, out, err = fr.run(["make", "install"], cwd=str(builddir or srcdir), env=env)
        except Exception:
            logger.exception("fakeroot.run failed; falling back to make install")
            rc, out, err = _safe_run(["make", "install"], cwd=builddir or srcdir, env=env)
    else:
        rc, out, err = _safe_run(["make", "install"], cwd=builddir or srcdir, env=env)
    res["cmds"].append({"cmd": "make install", "rc": rc, "out": out, "err": err})
    if rc != 0:
        res["ok"] = False
    return res

# --- quick packaging ---
def quickpkg(pkg_meta: Dict[str,Any], srcdir: Path, output_dir: Path) -> Dict[str,Any]:
    out = {"ok": True, "artifact": None}
    name = (pkg_meta.get("package") or {}).get("name") or f"pkg-{_uid()}"
    version = (pkg_meta.get("package") or {}).get("version") or "0"
    tarname = f"{name}-{version}.tar.gz"
    tarpath = output_dir / tarname
    try:
        if not srcdir.exists():
            return {"ok": False, "error": "srcdir-missing"}
        shutil.make_archive(str(tarpath.with_suffix("")), "gztar", root_dir=str(srcdir))
        out["artifact"] = str(tarpath)
        return out
    except Exception as e:
        logger.exception("quickpkg failed: %s", e)
        return {"ok": False, "error": str(e)}

# --- BuildSystem class ---
class BuildSystem:
    def __init__(self):
        self.cfg = CFG
        self.fetcher = Fetcher(cache_dir=str(CACHE_DIR)) if Fetcher else None
        self.patchmgr = PatchManager() if PatchManager else None
        self.sandbox = get_sandbox_manager()() if callable(get_sandbox_manager) else None
        self.hooks = get_hook_manager()() if callable(get_hook_manager) else None
        self.db = get_db()() if callable(get_db) else None
        self.auditor = get_auditor()() if callable(get_auditor) else None

    def prepare_workdir(self, pkg_meta: Dict[str,Any]) -> Path:
        base = Path(BUILD_CFG.get("builddir") or DEFAULT_BUILD_BASE)
        base.mkdir(parents=True, exist_ok=True)
        tmp = Path(tempfile.mkdtemp(prefix=f"rquest-build-{(pkg_meta.get('package') or {}).get('name','pkg')}-", dir=str(base)))
        return tmp

    def fetch(self, pkg_meta: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        env = assemble_env(pkg_meta)
        run_hooks("pre-fetch", pkg_meta, workdir, env, dry_run=dry_run)
        res = fetch_sources(pkg_meta, workdir, dry_run=dry_run)
        run_hooks("post-fetch", pkg_meta, workdir, env, dry_run=dry_run)
        return res

    def prepare(self, pkg_meta: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        env = assemble_env(pkg_meta)
        run_hooks("pre-prepare", pkg_meta, workdir, env, dry_run=dry_run)
        p_res = apply_patches(pkg_meta, workdir, dry_run=dry_run)
        if not p_res.get("ok"):
            return {"ok": False, "stage": "prepare", "detail": p_res}
        # run prepare steps if any
        prepare_steps = (pkg_meta.get("prepare") or {}).get("steps") or []
        for step in prepare_steps:
            if dry_run:
                logger.info("[dry-run] would run prepare: %s", step)
                continue
            rc, out, err = _safe_run(step.split(), cwd=workdir, env=env)
            if rc != 0:
                return {"ok": False, "stage": "prepare", "detail": {"cmd": step, "rc": rc, "err": err}}
        run_hooks("post-prepare", pkg_meta, workdir, env, dry_run=dry_run)
        return {"ok": True}

    def configure(self, pkg_meta: Dict[str,Any], srcdir: Path, builddir: Optional[Path], dry_run: bool=False) -> Dict[str,Any]:
        env = assemble_env(pkg_meta)
        run_hooks("pre-configure", pkg_meta, srcdir, env, dry_run=dry_run)
        cfg_res = run_configure(pkg_meta, srcdir, builddir, env, dry_run=dry_run)
        if not cfg_res.get("ok"):
            return {"ok": False, "stage": "configure", "detail": cfg_res}
        run_hooks("post-configure", pkg_meta, srcdir, env, dry_run=dry_run)
        return {"ok": True, "detail": cfg_res}

    def compile(self, pkg_meta: Dict[str,Any], srcdir: Path, builddir: Optional[Path], shards: Optional[int]=None, dry_run: bool=False) -> Dict[str,Any]:
        env = assemble_env(pkg_meta)
        run_hooks("pre-build", pkg_meta, srcdir, env, dry_run=dry_run)
        comp_res = run_configure(pkg_meta, srcdir, builddir, env, dry_run=dry_run)
        if not comp_res.get("ok"):
            return {"ok": False, "stage": "compile", "detail": comp_res}
        run_hooks("post-build", pkg_meta, srcdir, env, dry_run=dry_run)
        return {"ok": True, "detail": comp_res}

    def test(self, pkg_meta: Dict[str,Any], srcdir: Path, builddir: Optional[Path], dry_run: bool=False) -> Dict[str,Any]:
        run_hooks("pre-test", pkg_meta, srcdir, assemble_env(pkg_meta), dry_run=dry_run)
        tests = (pkg_meta.get("test") or {}).get("commands") or pkg_meta.get("build", {}).get("test") or []
        if not tests:
            return {"ok": True, "skipped": True}
        results = []
        for t in (tests if isinstance(tests, list) else [tests]):
            if dry_run:
                logger.info("[dry-run] would run test: %s", t)
                results.append({"cmd": t, "skipped": True})
                continue
            rc, out, err = _safe_run(t.split(), cwd=builddir or srcdir, env=assemble_env(pkg_meta))
            results.append({"cmd": t, "rc": rc, "out": out, "err": err})
            if rc != 0:
                return {"ok": False, "stage": "test", "detail": results}
        run_hooks("post-test", pkg_meta, srcdir, assemble_env(pkg_meta), dry_run=dry_run)
        return {"ok": True, "detail": results}

    def install(self, pkg_meta: Dict[str,Any], srcdir: Path, builddir: Optional[Path], dry_run: bool=False, use_fakeroot: bool=False) -> Dict[str,Any]:
        env = assemble_env(pkg_meta)
        run_hooks("pre-install", pkg_meta, srcdir, env, dry_run=dry_run)
        res = run_install(pkg_meta, srcdir, builddir, env, use_fakeroot=use_fakeroot, dry_run=dry_run)
        if not res.get("ok"):
            run_hooks("install-failed", pkg_meta, srcdir, env, dry_run=dry_run)
            return {"ok": False, "stage": "install", "detail": res}
        run_hooks("post-install", pkg_meta, srcdir, env, dry_run=dry_run)
        return {"ok": True, "detail": res}

    def package(self, pkg_meta: Dict[str,Any], srcdir: Path, outdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        outdir.mkdir(parents=True, exist_ok=True)
        try:
            qp = quickpkg(pkg_meta, srcdir, outdir)
            if not qp.get("ok"):
                return {"ok": False, "stage": "package", "detail": qp}
            return {"ok": True, "stage": "package", "detail": qp}
        except Exception as e:
            logger.exception("Package step failed: %s", e)
            return {"ok": False, "stage": "package", "detail": {"error": str(e)}}

    def publish(self, pkg_meta: Dict[str,Any], artifact_path: Path, dry_run: bool=False) -> Dict[str,Any]:
        dst = CACHE_DIR / "packages"
        dst.mkdir(parents=True, exist_ok=True)
        try:
            if dry_run:
                logger.info("[dry-run] would publish %s to %s", artifact_path, dst)
                return {"ok": True, "stage": "publish", "detail": {"artifact": str(artifact_path), "dest": str(dst)}}
            shutil.copy2(str(artifact_path), str(dst / artifact_path.name))
            return {"ok": True, "stage": "publish", "detail": {"artifact": str(artifact_path), "dest": str(dst / artifact_path.name)}}
        except Exception as e:
            logger.exception("publish failed: %s", e)
            return {"ok": False, "stage": "publish", "detail": {"error": str(e)}}

    def cleanup(self, workdir: Path, keep: bool=False) -> Dict[str,Any]:
        if keep or KEEP_BUILD_DIRS:
            logger.info("Keeping build dir: %s", str(workdir))
            return {"ok": True, "kept": True}
        try:
            shutil.rmtree(str(workdir), ignore_errors=True)
            return {"ok": True, "removed": True}
        except Exception as e:
            logger.exception("cleanup failed: %s", e)
            return {"ok": False, "error": str(e)}

    # --- main orchestration ---
    def build_package(self, pkg_meta: Dict[str,Any], *, force: bool=False, dry_run: bool=False, shards: Optional[int]=None) -> Dict[str,Any]:
        """
        Build and optionally install/package a package.
        pkg_meta is a dict already parsed from .meta.
        """
        name = (pkg_meta.get("package") or {}).get("name") or f"pkg-{_uid()}"
        version = (pkg_meta.get("package") or {}).get("version") or "0"
        logger.info("Building %s-%s (dry_run=%s, force=%s, shards=%s)", name, version, dry_run, force, shards)
        if repo_sync_all:
            try:
                repo_sync_all()
            except Exception:
                logger.debug("repo_sync failed/absent")
        workdir = self.prepare_workdir(pkg_meta)
        summary: Dict[str,Any] = {"ok": False, "package": name, "version": version, "started_at": _now_ts(), "stages": []}
        try:
            # FETCH
            fetch_res = self.fetch(pkg_meta, workdir, dry_run=dry_run)
            summary["stages"].append({"stage": "fetch", "result": fetch_res})
            if not fetch_res.get("ok") and not force:
                return {"ok": False, "stage": "fetch", "detail": fetch_res}
            # PREPARE
            prep_res = self.prepare(pkg_meta, workdir, dry_run=dry_run)
            summary["stages"].append({"stage": "prepare", "result": prep_res})
            if not prep_res.get("ok") and not force:
                return {"ok": False, "stage": "prepare", "detail": prep_res}
            # determine srcdir (if extraction created single dir)
            srcdir = workdir
            try:
                entries = [e for e in workdir.iterdir() if e.is_dir()]
                if len(entries) == 1:
                    srcdir = entries[0]
            except Exception:
                pass
            # CONFIGURE
            cfg_res = self.configure(pkg_meta, srcdir, None, dry_run=dry_run)
            summary["stages"].append({"stage": "configure", "result": cfg_res})
            if not cfg_res.get("ok") and not force:
                return {"ok": False, "stage": "configure", "detail": cfg_res}
            # COMPILE
            comp_res = self.compile(pkg_meta, srcdir, None, shards=shards, dry_run=dry_run)
            summary["stages"].append({"stage": "compile", "result": comp_res})
            if not comp_res.get("ok") and not force:
                return {"ok": False, "stage": "compile", "detail": comp_res}
            # TEST
            test_res = self.test(pkg_meta, srcdir, None, dry_run=dry_run)
            summary["stages"].append({"stage": "test", "result": test_res})
            if not test_res.get("ok") and not force:
                return {"ok": False, "stage": "test", "detail": test_res}
            # INSTALL
            use_fr = bool((pkg_meta.get("install") or {}).get("fakeroot", DEFAULT_FAKEROOT))
            inst_res = self.install(pkg_meta, srcdir, None, dry_run=dry_run, use_fakeroot=use_fr)
            summary["stages"].append({"stage": "install", "result": inst_res})
            if not inst_res.get("ok") and not force:
                return {"ok": False, "stage": "install", "detail": inst_res}
            # PACKAGE
            outdir = Path(pkg_meta.get("package", {}).get("output_dir") or CACHE_DIR / "packages")
            outdir.mkdir(parents=True, exist_ok=True)
            pack_res = self.package(pkg_meta, srcdir, outdir, dry_run=dry_run)
            summary["stages"].append({"stage": "package", "result": pack_res})
            if not pack_res.get("ok") and not force:
                return {"ok": False, "stage": "package", "detail": pack_res}
            # PUBLISH
            artifact = pack_res.get("detail", {}).get("artifact") if isinstance(pack_res.get("detail"), dict) else None
            pub_res = {"ok": True, "skipped": True}
            if artifact:
                pub_res = self.publish(pkg_meta, Path(artifact), dry_run=dry_run)
            summary["stages"].append({"stage": "publish", "result": pub_res})
            if not pub_res.get("ok") and not force:
                return {"ok": False, "stage": "publish", "detail": pub_res}
            # AUDIT
            try:
                if self.auditor:
                    self.auditor.audit_package_meta(pkg_meta)
            except Exception:
                logger.debug("auditor absent or failed")
            summary["ok"] = True
            summary["finished_at"] = _now_ts()
            # record in DB
            try:
                if self.db:
                    self.db.execute("CREATE TABLE IF NOT EXISTS builds (id TEXT PRIMARY KEY, pkg TEXT, version TEXT, started_at INTEGER, finished_at INTEGER, ok INTEGER, stage TEXT, detail TEXT)", (), commit=True)
                    bid = f"bld-{_uid()}"
                    self.db.execute("INSERT INTO builds (id, pkg, version, started_at, finished_at, ok, stage, detail) VALUES (?,?,?,?,?,?,?,?)",
                                    (bid, name, version, summary["started_at"], summary.get("finished_at") or _now_ts(), 1 if summary["ok"] else 0, "complete" if summary["ok"] else "partial", json.dumps(summary)), commit=True)
            except Exception:
                logger.debug("DB record build failed")
            emit_event("buildsystem.build.finished", {"pkg": name, "version": version, "ok": True})
            return {"ok": True, "stage": "complete", "detail": summary}
        except Exception as e:
            logger.exception("build_package exception: %s", e)
            emit_event("buildsystem.build.failed", {"pkg": name, "version": version, "error": str(e)})
            return {"ok": False, "stage": "exception", "detail": {"error": str(e)}}
        finally:
            keep = (pkg_meta.get("build", {}) or {}).get("keep_temps") or KEEP_BUILD_DIRS
            if keep:
                logger.info("Keeping workdir per config: %s", workdir)
            else:
                try:
                    shutil.rmtree(str(workdir), ignore_errors=True)
                except Exception:
                    logger.debug("cleanup workdir failed")

# module-level singleton
_BS: Optional[BuildSystem] = None
def get_buildsystem() -> BuildSystem:
    global _BS
    if _BS is None:
        _BS = BuildSystem()
    return _BS

def build_package(pkg_meta: Dict[str,Any], *, force: bool=False, dry_run: bool=False, shards: Optional[int]=None) -> Dict[str,Any]:
    bs = get_buildsystem()
    return bs.build_package(pkg_meta, force=force, dry_run=dry_run, shards=shards)

# if run as script, quick demo (dry-run)
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-buildsystem")
    ap.add_argument("meta", help="Path to .meta (yaml/json)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    meta_path = Path(args.meta)
    m = _load_meta_from_path(meta_path)
    if not m:
        print("Failed to load meta:", meta_path)
        sys.exit(2)
    res = build_package(m, dry_run=args.dry_run)
    print(json.dumps(res, indent=2, ensure_ascii=False))
