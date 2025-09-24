# rquest1.0/modules/buildsystem.py
# -*- coding: utf-8 -*-
"""
buildsystem.py - motor de build do Rquest (versão integrada)

API principal:
  bs = get_buildsystem()
  result = bs.build_package(pkg_meta: dict, *, force: bool=False, dry_run: bool=False, shards: Optional[int]=None)

Resultado:
  dict {
    "ok": True/False,
    "stage": "prepare|configure|compile|install|package|publish|cleanup|fetch",
    "detail": {...},  # logs, rc, errmsg, artifacts
  }

Comportamento:
  - Integra com modules.fetcher, modules.patches, modules.sandbox, modules.fakeroot, modules.hooks, modules.db, modules.logging, modules.audit quando presentes.
  - Fallback para executáveis do sistema (wget/curl, tar, make, cmake, python) se módulos faltarem.
  - Respeita dry_run (simula) e force (ignora falhas conforme apropriado).
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

# optional libs
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# --- integrations (graceful) ---
# config
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

# logging
try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("buildsystem")
except Exception:
    import logging
    logger = logging.getLogger("rquest.buildsystem")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# fetcher
try:
    from modules.fetcher import Fetcher  # type: ignore
except Exception:
    Fetcher = None

# patches
try:
    from modules.patches import PatchManager  # type: ignore
except Exception:
    PatchManager = None

# sandbox
try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager():
        return None

# fakeroot
try:
    from modules.fakeroot import create_fakeroot  # type: ignore
except Exception:
    create_fakeroot = None

# hooks
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    get_hook_manager = None

# db
try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    get_db = None
    def emit_event(*a, **k):
        pass

# audit
try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    get_auditor = None

# repo_sync
try:
    from modules.repo_sync import sync_all as repo_sync_all  # type: ignore
except Exception:
    repo_sync_all = None

# --- config and defaults ---
CFG = get_config() if callable(get_config) else {}
BUILD_CFG = CFG.get("build", {}) if isinstance(CFG, dict) else {}
DEFAULT_JOBS = BUILD_CFG.get("jobs", os.cpu_count() or 1)
KEEP_BUILD_DIRS = BUILD_CFG.get("keep_build_dirs", False)
DEFAULT_FAKEROOT = BUILD_CFG.get("fakeroot", False)
CACHE_DIR = Path(BUILD_CFG.get("cache_dir", "/var/cache/rquest/build"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# --- helpers ---
def _now_ts() -> int:
    return int(time.time())

def _uid() -> str:
    import uuid
    return uuid.uuid4().hex[:8]

def _safe_run(cmd: List[str], cwd: Optional[Path] = None, env: Optional[Dict[str,str]] = None, timeout: Optional[int] = None) -> Tuple[int,str,str]:
    """Run command and capture output. Returns (rc, stdout, stderr)"""
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

# --- detect build system ---
def detect_build_system(pkg_meta: Dict[str,Any]) -> str:
    """
    Heuristics:
      - pkg_meta['build']['system'] explicit
      - presence of configure, CMakeLists.txt, pyproject.toml/setup.py, Cargo.toml
      - default -> 'generic'
    """
    build = pkg_meta.get("build") or {}
    if isinstance(build, dict) and build.get("system"):
        return str(build.get("system"))
    # scan sources if available (pkg_meta may include 'source' tarballs or 'source_dir' hint)
    src_hint = pkg_meta.get("source_dir") or pkg_meta.get("src_dir") or None
    # if explicit path exists, check files
    if src_hint and Path(src_hint).exists():
        p = Path(src_hint)
        if (p / "configure").exists() or (p / "autogen.sh").exists():
            return "autotools"
        if (p / "CMakeLists.txt").exists():
            return "cmake"
        if (p / "pyproject.toml").exists() or (p / "setup.py").exists():
            return "python"
        if (p / "Cargo.toml").exists():
            return "cargo"
    # metadata hints
    sources = pkg_meta.get("source") or []
    # if any source is a git repo and package name contains 'rust' maybe cargo; but keep simple:
    # fallback: autotools if configure present in top-level after extract is common
    return "autotools" if isinstance(sources, list) and any(str(s).endswith((".tar.gz", ".tar.xz", ".tar.bz2", ".zip")) for s in sources) else "generic"

# --- environment assembly ---
def assemble_env(pkg_meta: Dict[str,Any], profile: Optional[str] = None) -> Dict[str,str]:
    """
    Build environment variables for the build based on pkg_meta and global config.
    Merges CFLAGS/CXXFLAGS/LDFLAGS/CPPFLAGS from meta and config.
    """
    env = dict(os.environ)
    meta_build = pkg_meta.get("build", {}) or {}
    meta_env = meta_build.get("env") or pkg_meta.get("env") or {}
    # gather flags
    flags = {}
    for key in ("CFLAGS","CXXFLAGS","LDFLAGS","CPPFLAGS","MAKEFLAGS"):
        v = meta_env.get(key) or pkg_meta.get(key) or BUILD_CFG.get(key.lower()) or ""
        if v:
            flags[key] = str(v)
    # merge
    env.update({k: v for k,v in flags.items() if v})
    # add JOBS
    env["JOBS"] = str(pkg_meta.get("build", {}).get("jobs") or BUILD_CFG.get("jobs") or DEFAULT_JOBS)
    return env

# --- fetch sources ---
def fetch_sources(pkg_meta: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
    """
    Fetch sources declared in pkg_meta['source'] into workdir/distfiles.
    Returns dict with ok and list of files.
    """
    res = {"ok": True, "files": [], "errors": []}
    sources = pkg_meta.get("source") or []
    if isinstance(sources, dict):
        sources = [sources]
    if not sources:
        logger.debug("No sources declared in meta")
        return res
    # use Fetcher module if available
    if Fetcher:
        try:
            fetcher = Fetcher(cache_dir=str(CACHE_DIR))
            for s in sources:
                if dry_run:
                    logger.info("[dry-run] would fetch %s", s)
                    continue
                try:
                    path = fetcher.fetch(s)
                    res["files"].append(path)
                except Exception as e:
                    logger.exception("Fetcher failed for %s: %s", s, e)
                    res["errors"].append({"source": s, "error": str(e)})
                    res["ok"] = False
        except Exception:
            logger.exception("Fetcher module failed; falling back to wget/curl")
            Fetch_local = None
    # fallback basic downloader
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
            logger.info("[dry-run] would download %s -> %s", url, dest)
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
        except Exception:
            logger.exception("PatchManager failed; fallback to 'patch' command")
    # fallback to using 'patch' where possible (patch files must be in workdir)
    for p in patches:
        patch_path = Path(p) if isinstance(p, str) else None
        if not patch_path or not patch_path.exists():
            # maybe patch located relative to meta: skip gracefully
            logger.warning("Patch not found: %s", p)
            res["errors"].append({"patch": p, "error": "not-found"})
            res["ok"] = False
            continue
        if dry_run:
            logger.info("[dry-run] would apply patch %s", patch_path)
            continue
        rc, out, err = _safe_run(["patch", "-p1", "-i", str(patch_path)], cwd=workdir)
        if rc != 0:
            res["errors"].append({"patch": p, "err": err})
            res["ok"] = False
        else:
            res["applied"].append(str(patch_path))
    return res

# --- run hooks ---
def run_hooks(hooks_phase: str, pkg_meta: Dict[str,Any], cwd: Path, env: Dict[str,str], dry_run: bool=False) -> Dict[str,Any]:
    """
    hooks_phase: pre-fetch, post-fetch, pre-configure, post-configure, pre-build, post-build, pre-install, post-install
    pkg_meta may include hooks inline or reference scripts under package dir.
    """
    res = {"ok": True, "ran": [], "errors": []}
    hooks_def = (pkg_meta.get("hooks") or {}).get(hooks_phase) or []
    if not hooks_def and get_hook_manager:
        try:
            hm = get_hook_manager()()
            hooks_def = hm.get_hooks_for(pkg_meta.get("package", {}).get("name"), hooks_phase) or []
        except Exception:
            pass
    for h in hooks_def:
        if dry_run:
            logger.info("[dry-run] would run hook %s: %s", hooks_phase, h)
            continue
        try:
            # allow shell commands or script paths
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

# --- configure / build / test / install helpers ---
def run_configure(pkg_meta: Dict[str,Any], srcdir: Path, builddir: Optional[Path], env: Dict[str,str], dry_run: bool=False) -> Dict[str,Any]:
    build = pkg_meta.get("build", {}) or {}
    system = build.get("system") or detect_build_system(pkg_meta)
    res = {"ok": True, "cmds": []}
    if system == "autotools":
        cfg = build.get("configure") or build.get("args") or ["../configure --prefix=/usr"]
        # support out-of-source
        bdir = builddir or srcdir
        if not bdir.exists(): bdir.mkdir(parents=True, exist_ok=True)
        for cmd in (cfg if isinstance(cfg, list) else [cfg]):
            if dry_run:
                logger.info("[dry-run] would run configure: %s", cmd)
                res["cmds"].append(cmd)
                continue
            cmd_list = cmd.split()
            rc, out, err = _safe_run(cmd_list, cwd=bdir, env=env)
            res["cmds"].append({"cmd": cmd, "rc": rc, "out": out, "err": err})
            if rc != 0:
                res["ok"] = False
                return res
    elif system == "cmake":
        bdir = builddir or (srcdir / "build")
        if not bdir.exists(): bdir.mkdir(parents=True, exist_ok=True)
        cfg = build.get("configure") or ["cmake .. -DCMAKE_INSTALL_PREFIX=/usr"]
        for cmd in (cfg if isinstance(cfg, list) else [cfg]):
            if dry_run:
                logger.info("[dry-run] would run cmake: %s", cmd)
                res["cmds"].append(cmd)
                continue
            rc, out, err = _safe_run(cmd.split(), cwd=bdir, env=env)
            res["cmds"].append({"cmd": cmd, "rc": rc, "out": out, "err": err})
            if rc != 0:
                res["ok"] = False
                return res
    elif system == "python":
        # build with pip wheel or setup.py
        cmd = build.get("commands") or ["python -m pip wheel . -w dist"]
        for c in (cmd if isinstance(cmd, list) else [cmd]):
            if dry_run:
                logger.info("[dry-run] would run python build: %s", c)
                res["cmds"].append(c)
                continue
            rc, out, err = _safe_run(c.split(), cwd=srcdir, env=env)
            res["cmds"].append({"cmd": c, "rc": rc, "out": out, "err": err})
            if rc != 0:
                res["ok"] = False
                return res
    elif system == "cargo":
        cmd = build.get("commands") or ["cargo build --release"]
        for c in (cmd if isinstance(cmd, list) else [cmd]):
            if dry_run:
                logger.info("[dry-run] would run cargo: %s", c)
                res["cmds"].append(c)
                continue
            rc, out, err = _safe_run(c.split(), cwd=srcdir, env=env)
            res["cmds"].append({"cmd": c, "rc": rc, "out": out, "err": err})
            if rc != 0:
                res["ok"] = False
                return res
    else:
        # generic: respect build.commands or make by default
        cmds = build.get("commands") or build.get("steps") or ["make -j{}".format(env.get("JOBS", DEFAULT_JOBS))]
        for c in (cmds if isinstance(cmds, list) else [cmds]):
            if dry_run:
                logger.info("[dry-run] would run build step: %s", c)
                res["cmds"].append(c)
                continue
            rc, out, err = _safe_run(c.split(), cwd=builddir or srcdir, env=env)
            res["cmds"].append({"cmd": c, "rc": rc, "out": out, "err": err})
            if rc != 0:
                res["ok"] = False
                return res
    return res

def run_make_install(srcdir: Path, builddir: Optional[Path], env: Dict[str,str], use_fakeroot: bool=False, dry_run: bool=False) -> Dict[str,Any]:
    res = {"ok": True, "cmds": []}
    inst = ["make", "install"]
    if dry_run:
        logger.info("[dry-run] would run install: %s", " ".join(inst))
        res["cmds"].append(" ".join(inst))
        return res
    # if fakeroot available and requested, use it
    if use_fakeroot and create_fakeroot:
        try:
            fr = create_fakeroot()
            rc, out, err = fr.run(inst, cwd=str(builddir or srcdir), env=env)
        except Exception:
            logger.exception("fakeroot.run failed; falling back to direct install")
            rc, out, err = _safe_run(inst, cwd=builddir or srcdir, env=env)
    else:
        rc, out, err = _safe_run(inst, cwd=builddir or srcdir, env=env)
    res["cmds"].append({"cmd": " ".join(inst), "rc": rc, "out": out, "err": err})
    if rc != 0:
        res["ok"] = False
    return res

# --- packaging (quickpkg) ---
def quickpkg(pkg_meta: Dict[str,Any], build_root: Path, output_dir: Path) -> Dict[str,Any]:
    """
    Create a simple tarball of installed files listed in pkg_meta['install']['paths'] or 'files'
    """
    out = {"ok": True, "artifact": None, "error": None}
    name = pkg_meta.get("package", {}).get("name") or f"pkg-{_uid()}"
    version = pkg_meta.get("package", {}).get("version") or "0"
    tarname = f"{name}-{version}.tar.gz"
    tarpath = output_dir / tarname
    try:
        paths = pkg_meta.get("install", {}).get("paths") or pkg_meta.get("files") or []
        if not paths:
            # fallback: archive entire build_root
            if dry_run := pkg_meta.get("_dry_run_flag"):  # not ideal; keep safe
                logger.info("[dry-run] would package %s", build_root)
            else:
                shutil.make_archive(str(tarpath.with_suffix("")), "gztar", root_dir=str(build_root))
                out["artifact"] = str(tarpath)
                return out
        # else, construct a tar with listed files
        import tarfile
        with tarfile.open(str(tarpath), "w:gz") as tf:
            for p in paths:
                pth = Path(p)
                if pth.exists():
                    tf.add(str(pth), arcname=str(pth.relative_to("/")) if pth.is_absolute() else pth.name)
        out["artifact"] = str(tarpath)
        return out
    except Exception as e:
        logger.exception("quickpkg failed: %s", e)
        out["ok"] = False
        out["error"] = str(e)
        return out

# --- main BuildSystem class ---
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
        tmp = Path(tempfile.mkdtemp(prefix=f"rquest-build-{pkg_meta.get('package',{}).get('name','pkg')}-"))
        return tmp

    def fetch(self, pkg_meta: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        # run pre-fetch hooks
        env = assemble_env(pkg_meta)
        run_hooks("pre-fetch", pkg_meta, workdir, env, dry_run=dry_run)
        res = fetch_sources(pkg_meta, workdir, dry_run=dry_run)
        run_hooks("post-fetch", pkg_meta, workdir, env, dry_run=dry_run)
        return res

    def prepare(self, pkg_meta: Dict[str,Any], workdir: Path, dry_run: bool=False) -> Dict[str,Any]:
        env = assemble_env(pkg_meta)
        # run prepare steps: apply patches, custom scripts
        run_hooks("pre-prepare", pkg_meta, workdir, env, dry_run=dry_run)
        p_res = apply_patches(pkg_meta, workdir, dry_run=dry_run)
        if not p_res.get("ok"):
            return {"ok": False, "stage": "prepare", "detail": p_res}
        # optional prepare scripts
        prepare_steps = (pkg_meta.get("prepare") or {}).get("steps") or []
        for step in prepare_steps:
            if dry_run:
                logger.info("[dry-run] would run prepare step: %s", step)
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
        # derive compile step via run_configure (system==autotools or generic make)
        compile_res = run_configure(pkg_meta, srcdir, builddir, env, dry_run=dry_run)
        if not compile_res.get("ok"):
            return {"ok": False, "stage": "compile", "detail": compile_res}
        run_hooks("post-build", pkg_meta, srcdir, env, dry_run=dry_run)
        return {"ok": True, "detail": compile_res}

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
        # allow pkg_meta['install']['commands'] override
        install_def = (pkg_meta.get("install") or {}).get("commands") or (pkg_meta.get("install") or {}).get("steps") or None
        if install_def:
            results = []
            for cmd in (install_def if isinstance(install_def, list) else [install_def]):
                if dry_run:
                    logger.info("[dry-run] would run install: %s", cmd)
                    results.append({"cmd": cmd, "skipped": True})
                    continue
                parts = cmd.split()
                if use_fakeroot and create_fakeroot:
                    try:
                        fr = create_fakeroot()
                        rc, out, err = fr.run(parts, cwd=str(builddir or srcdir), env=env)
                    except Exception:
                        logger.exception("fakeroot.run failed; fallback to direct install")
                        rc, out, err = _safe_run(parts, cwd=builddir or srcdir, env=env)
                else:
                    rc, out, err = _safe_run(parts, cwd=builddir or srcdir, env=env)
                results.append({"cmd": cmd, "rc": rc, "out": out, "err": err})
                if rc != 0:
                    run_hooks("install-failed", pkg_meta, srcdir, env, dry_run=dry_run)
                    return {"ok": False, "stage": "install", "detail": results}
            run_hooks("post-install", pkg_meta, srcdir, env, dry_run=dry_run)
            return {"ok": True, "detail": results}
        # default: make install
        res = run_make_install(srcdir, builddir, env, use_fakeroot=use_fakeroot, dry_run=dry_run)
        if not res.get("ok"):
            return {"ok": False, "stage": "install", "detail": res}
        run_hooks("post-install", pkg_meta, srcdir, env, dry_run=dry_run)
        return {"ok": True, "detail": res}

    def package(self, pkg_meta: Dict[str,Any], build_root: Path, output_dir: Path, dry_run: bool=False) -> Dict[str,Any]:
        output_dir.mkdir(parents=True, exist_ok=True)
        # quickpkg
        try:
            qp = quickpkg(pkg_meta, build_root, output_dir)
            if not qp.get("ok"):
                return {"ok": False, "stage": "package", "detail": qp}
            return {"ok": True, "stage": "package", "detail": qp}
        except Exception as e:
            logger.exception("package failed: %s", e)
            return {"ok": False, "stage": "package", "detail": {"error": str(e)}}

    def publish(self, pkg_meta: Dict[str,Any], artifact_path: Path, dry_run: bool=False) -> Dict[str,Any]:
        # Placeholder: integrate with repos (artifactory/s3/git) if required
        # For now, we simply place artifact in CACHE_DIR / 'packages'
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
            logger.info("Keeping build dir %s per config", str(workdir))
            return {"ok": True, "kept": True}
        try:
            shutil.rmtree(str(workdir), ignore_errors=True)
            return {"ok": True, "removed": True}
        except Exception as e:
            logger.exception("cleanup failed: %s", e)
            return {"ok": False, "error": str(e)}

    # --- main orchestration: build_package ---
    def build_package(self, pkg_meta: Dict[str,Any], *, force: bool=False, dry_run: bool=False, shards: Optional[int]=None) -> Dict[str,Any]:
        """
        Build and install/package a package described by pkg_meta (dict).
        Returns dict with ok, stage and detail.
        """
        name = (pkg_meta.get("package") or {}).get("name") or f"pkg-{_uid()}"
        version = (pkg_meta.get("package") or {}).get("version") or "0"
        logger.info("BuildSystem: building %s-%s (dry_run=%s, force=%s, shards=%s)", name, version, dry_run, force, shards)
        # pre-sync repos (optional)
        if repo_sync_all:
            try:
                repo_sync_all()
            except Exception:
                logger.debug("repo_sync failed / unavailable")

        workdir = self.prepare_workdir(pkg_meta)
        result_summary: Dict[str,Any] = {"ok": False, "package": name, "version": version, "started_at": _now_ts(), "stages": []}
        try:
            # FETCH
            fetch_res = self.fetch(pkg_meta, workdir, dry_run=dry_run)
            result_summary["stages"].append({"stage": "fetch", "result": fetch_res})
            if not fetch_res.get("ok") and not force:
                return {"ok": False, "stage": "fetch", "detail": fetch_res}
            # PREPARE
            prep_res = self.prepare(pkg_meta, workdir, dry_run=dry_run)
            result_summary["stages"].append({"stage": "prepare", "result": prep_res})
            if not prep_res.get("ok") and not force:
                return {"ok": False, "stage": "prepare", "detail": prep_res}
            # determine srcdir (try common patterns)
            srcdir = workdir
            # if extraction happened and created a single dir, pick it
            try:
                entries = [e for e in workdir.iterdir() if e.is_dir()]
                if len(entries) == 1:
                    srcdir = entries[0]
            except Exception:
                pass
            # configure/compile
            cfg_res = self.configure(pkg_meta, srcdir, None, dry_run=dry_run)
            result_summary["stages"].append({"stage": "configure", "result": cfg_res})
            if not cfg_res.get("ok") and not force:
                return {"ok": False, "stage": "configure", "detail": cfg_res}
            # compile
            comp_res = self.compile(pkg_meta, srcdir, None, shards=shards, dry_run=dry_run)
            result_summary["stages"].append({"stage": "compile", "result": comp_res})
            if not comp_res.get("ok") and not force:
                return {"ok": False, "stage": "compile", "detail": comp_res}
            # test
            test_res = self.test(pkg_meta, srcdir, None, dry_run=dry_run)
            result_summary["stages"].append({"stage": "test", "result": test_res})
            if not test_res.get("ok") and not force:
                return {"ok": False, "stage": "test", "detail": test_res}
            # install
            use_fakeroot = (pkg_meta.get("install", {}) or {}).get("fakeroot", DEFAULT_FAKEROOT)
            inst_res = self.install(pkg_meta, srcdir, None, dry_run=dry_run, use_fakeroot=use_fakeroot)
            result_summary["stages"].append({"stage": "install", "result": inst_res})
            if not inst_res.get("ok") and not force:
                return {"ok": False, "stage": "install", "detail": inst_res}
            # package
            outdir = Path(pkg_meta.get("package", {}).get("output_dir") or CACHE_DIR / "packages")
            outdir.mkdir(parents=True, exist_ok=True)
            pack_res = self.package(pkg_meta, srcdir, outdir, dry_run=dry_run)
            result_summary["stages"].append({"stage": "package", "result": pack_res})
            if not pack_res.get("ok") and not force:
                return {"ok": False, "stage": "package", "detail": pack_res}
            # publish
            artifact = pack_res.get("detail", {}).get("artifact") if isinstance(pack_res.get("detail"), dict) else None
            pub_res = {"ok": True, "skipped": True}
            if artifact:
                pub_res = self.publish(pkg_meta, Path(artifact), dry_run=dry_run)
            result_summary["stages"].append({"stage": "publish", "result": pub_res})
            if not pub_res.get("ok") and not force:
                return {"ok": False, "stage": "publish", "detail": pub_res}
            # audit if available
            try:
                if self.auditor:
                    self.auditor.audit_package_meta(pkg_meta)
            except Exception:
                logger.debug("auditor audit failed or unavailable")
            result_summary["ok"] = True
            result_summary["finished_at"] = _now_ts()
            # persist build record in DB if available
            try:
                if self.db:
                    self.db.execute("CREATE TABLE IF NOT EXISTS builds (id TEXT PRIMARY KEY, pkg TEXT, version TEXT, started INTEGER, finished INTEGER, ok INTEGER, detail JSON)", (), commit=True)
                    bid = f"bld-{_uid()}"
                    self.db.execute("INSERT INTO builds (id, pkg, version, started, finished, ok, detail) VALUES (?,?,?,?,?,?,?)",
                                    (bid, name, version, result_summary["started_at"], result_summary.get("finished_at") or _now_ts(), 1 if result_summary["ok"] else 0, json.dumps(result_summary)), commit=True)
            except Exception:
                logger.debug("DB build record failed")
            emit_event("buildsystem.build.finished", {"pkg": name, "version": version, "ok": True})
            return {"ok": True, "stage": "complete", "detail": result_summary}
        except Exception as e:
            logger.exception("build_package exception: %s", e)
            emit_event("buildsystem.build.failed", {"pkg": name, "version": version, "error": str(e)})
            return {"ok": False, "stage": "exception", "detail": {"error": str(e)}}
        finally:
            # cleanup
            keep = pkg_meta.get("build", {}).get("keep_temps") or KEEP_BUILD_DIRS
            if keep:
                logger.info("Keeping build dir per config: %s", workdir)
            else:
                try:
                    shutil.rmtree(str(workdir), ignore_errors=True)
                except Exception:
                    logger.debug("Cleanup of workdir failed")

# --- module-level helper to get singleton BuildSystem ---
_BS: Optional[BuildSystem] = None
def get_buildsystem() -> BuildSystem:
    global _BS
    if _BS is None:
        _BS = BuildSystem()
    return _BS

# --- convenience wrapper matching expected API ---
def build_package(pkg_meta: Dict[str,Any], *, force: bool=False, dry_run: bool=False, shards: Optional[int]=None) -> Dict[str,Any]:
    bs = get_buildsystem()
    return bs.build_package(pkg_meta, force=force, dry_run=dry_run, shards=shards)
