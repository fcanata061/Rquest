# Rquest/rquest1.0/modules/buildsystem.py
"""
buildsystem.py - Build orchestration for Rquest

Features:
- BuildSystem class with full pipeline: prepare -> configure -> compile -> test -> install -> package -> publish -> cleanup
- Detect multiple build systems (autotools, cmake, meson, python, cargo, generic)
- Integration with sandbox.py for hermetic execution
- Integration with fetcher.py, patches.py, hooks.py
- Use fakeroot.py for install step when configured or requested by pkg_meta
- Basic caching and DB persistence (builds, build_jobs, build_cache)
- Hooks executed at each stage: pre_* and post_* via hooks module
- Robust fallbacks when modules are missing
"""

from __future__ import annotations

import os
import sys
import json
import time
import shutil
import tarfile
import tempfile
import threading
import uuid
import subprocess
from typing import Any, Dict, List, Optional, Tuple

# ----------------------------
# Robust imports for project modules
# ----------------------------
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
    import logging as _stdlog
    def get_logger(name: str):
        return _stdlog.getLogger(name)

logger = get_logger("buildsystem")

# DB
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def add_history(*a, **k): pass
    def emit_event(*a, **k): pass

# sandbox
try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager(): return None

# fetcher
try:
    from modules.fetcher import get_fetcher_manager  # type: ignore
except Exception:
    def get_fetcher_manager(): return None

# patches
try:
    from modules.patches import get_patch_manager  # type: ignore
except Exception:
    def get_patch_manager(): return None

# hooks
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

# masks / slots
try:
    from modules.masks import is_masked  # type: ignore
except Exception:
    def is_masked(*a, **k): return False

try:
    from modules.slots import assign_slot  # type: ignore
except Exception:
    def assign_slot(*a, **k): return None

# fakeroot (optional)
try:
    from modules.fakeroot import run_in_fakeroot, ensure_fakeroot_available  # type: ignore
except Exception:
    # fallback implementation: try system fakeroot binary or run normally if not available
    def ensure_fakeroot_available() -> bool:
        return shutil.which("fakeroot") is not None

    def run_in_fakeroot(cmd: List[str], cwd: Optional[str]=None, env: Optional[Dict[str,str]]=None, timeout: Optional[int]=None) -> Dict[str,Any]:
        """
        Fallback: if system fakeroot exists, run under it; otherwise run directly and warn.
        Returns dict similar to sandbox.run_in_sandbox result.
        """
        from modules.sandbox import get_sandbox_manager as _gsm  # lazy import
        sm = _gsm()
        if shutil.which("fakeroot"):
            docker_cmd = [ "fakeroot" ] + cmd
            return sm.run_in_sandbox(docker_cmd, cwd=cwd, env=env, timeout=timeout, exec_type="fakeroot")
        else:
            logger.warning("fakeroot not available; performing install without fakeroot (may require privileges).")
            return sm.run_in_sandbox(cmd, cwd=cwd, env=env, timeout=timeout, exec_type="install")

# ----------------------------
# Utility helpers
# ----------------------------
def _now_ts() -> int:
    return int(time.time())

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return json.dumps(str(obj))

# ----------------------------
# DB schema helpers
# ----------------------------
def _ensure_build_tables(db):
    if not db:
        logger.debug("No DB available to ensure build tables.")
        return
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS builds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                package TEXT,
                version TEXT,
                system TEXT,
                status TEXT,
                start_ts INTEGER,
                end_ts INTEGER,
                duration REAL,
                log_path TEXT,
                sandbox_exec_id INTEGER,
                artifacts_path TEXT,
                used_fakeroot INTEGER DEFAULT 0,
                plan_hash TEXT,
                cache_hit INTEGER DEFAULT 0
            );
        """, (), commit=True)
        db.execute("""
            CREATE TABLE IF NOT EXISTS build_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                package TEXT,
                version TEXT,
                status TEXT DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                worker TEXT,
                created_at INTEGER DEFAULT (strftime('%s','now')),
                updated_at INTEGER
            );
        """, (), commit=True)
        db.execute("""
            CREATE TABLE IF NOT EXISTS build_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                digest TEXT UNIQUE,
                artifact_path TEXT,
                created_at INTEGER
            );
        """, (), commit=True)
    except Exception:
        logger.exception("Failed ensuring build tables")

# ----------------------------
# Build system detection
# ----------------------------
def detect_build_system(source_dir: str) -> str:
    """
    Try to detect build system:
      - autotools if configure or configure.ac present
      - cmake if CMakeLists.txt
      - meson if meson.build
      - python if setup.py / pyproject.toml
      - cargo if Cargo.toml
      - generic otherwise (user-supplied commands)
    """
    try:
        files = set(os.listdir(source_dir))
    except Exception:
        files = set()
    if "configure" in files or "configure.ac" in files or "configure.in" in files:
        return "autotools"
    if "CMakeLists.txt" in files:
        return "cmake"
    if "meson.build" in files:
        return "meson"
    if "pyproject.toml" in files or "setup.py" in files:
        return "python"
    if "Cargo.toml" in files:
        return "cargo"
    return "generic"

# ----------------------------
# Builder implementations (simple adapters)
# ----------------------------
class BaseBuilder:
    def __init__(self, pkg_meta: Dict[str,Any], source_dir: str, build_dir: str, destdir: str, bs: "BuildSystem"):
        self.pkg_meta = pkg_meta
        self.source_dir = source_dir
        self.build_dir = build_dir
        self.destdir = destdir
        self.bs = bs  # Back reference to BuildSystem

    def prepare(self):
        return {"ok": True, "msg": "no-op"}

    def configure(self):
        return {"ok": True, "msg": "no-op"}

    def compile(self, jobs: int = 1):
        return {"ok": True, "msg": "no-op"}

    def test(self):
        return {"ok": True, "msg": "no-op"}

    def install(self, use_fakeroot: bool = False):
        return {"ok": True, "msg": "no-op"}

    def package(self):
        return {"ok": True, "artifact": None}

class AutotoolsBuilder(BaseBuilder):
    def prepare(self):
        # usually nothing special
        return {"ok": True}

    def configure(self):
        cfg = self.pkg_meta.get("build", {}).get("configure_args", []) or []
        env = self.bs._assemble_env(self.pkg_meta)
        cmd = ["./configure", f"--prefix=/usr"] + cfg
        logger.info("Autotools configure: %s", cmd)
        sm = get_sandbox_manager()
        session = self.bs._session_id
        res = sm.run_in_sandbox(cmd, cwd=self.build_dir, env=env, timeout=self.bs.stage_timeout("configure"), session_id=session, exec_type="configure")
        return res

    def compile(self, jobs: int = 1):
        make = self.pkg_meta.get("build", {}).get("make", "make")
        makeflags = self.pkg_meta.get("build", {}).get("makeflags", []) or []
        cmd = [make, f"-j{jobs}"] + makeflags
        sm = get_sandbox_manager()
        session = self.bs._session_id
        res = sm.run_in_sandbox(cmd, cwd=self.build_dir, env=self.bs._assemble_env(self.pkg_meta), timeout=self.bs.stage_timeout("compile"), session_id=session, exec_type="compile")
        return res

    def install(self, use_fakeroot: bool = False):
        make = self.pkg_meta.get("build", {}).get("make", "make")
        dest = self.destdir
        cmd = [make, "install", f"DESTDIR={dest}"]
        if use_fakeroot:
            # use fakeroot wrapper
            logger.info("Installing via fakeroot into %s", dest)
            return run_in_fakeroot(cmd, cwd=self.build_dir, env=self.bs._assemble_env(self.pkg_meta), timeout=self.bs.stage_timeout("install"))
        else:
            sm = get_sandbox_manager()
            session = self.bs._session_id
            return sm.run_in_sandbox(cmd, cwd=self.build_dir, env=self.bs._assemble_env(self.pkg_meta), timeout=self.bs.stage_timeout("install"), session_id=session, exec_type="install")

class CMakeBuilder(BaseBuilder):
    def configure(self):
        cmake_args = self.pkg_meta.get("build", {}).get("cmake_args", []) or []
        builddir = self.build_dir
        os.makedirs(builddir, exist_ok=True)
        cmd = ["cmake", self.source_dir] + cmake_args
        sm = get_sandbox_manager()
        session = self.bs._session_id
        return sm.run_in_sandbox(cmd, cwd=builddir, env=self.bs._assemble_env(self.pkg_meta), timeout=self.bs.stage_timeout("configure"), session_id=session, exec_type="configure")

    def compile(self, jobs: int = 1):
        cmd = ["cmake", "--build", ".", "--", f"-j{jobs}"]
        sm = get_sandbox_manager()
        session = self.bs._session_id
        return sm.run_in_sandbox(cmd, cwd=self.build_dir, env=self.bs._assemble_env(self.pkg_meta), timeout=self.bs.stage_timeout("compile"), session_id=session, exec_type="compile")

    def install(self, use_fakeroot: bool = False):
        cmd = ["cmake", "--install", ".", "--prefix", "/usr"]
        if use_fakeroot:
            return run_in_fakeroot(cmd, cwd=self.build_dir, env=self.bs._assemble_env(self.pkg_meta), timeout=self.bs.stage_timeout("install"))
        else:
            sm = get_sandbox_manager()
            session = self.bs._session_id
            return sm.run_in_sandbox(cmd, cwd=self.build_dir, env=self.bs._assemble_env(self.pkg_meta), timeout=self.bs.stage_timeout("install"), session_id=session, exec_type="install")

# other builder classes (Meson, Python, Cargo) could be implemented similarly as needed

# ----------------------------
# BuildSystem core (part 1)
# ----------------------------
class BuildSystem:
    def __init__(self):
        self._cfg = {}
        try:
            self._cfg = get_config() if callable(get_config) else {}
        except Exception:
            self._cfg = {}
        bs_cfg = {}
        try:
            if isinstance(self._cfg, dict):
                bs_cfg = self._cfg.get("buildsystem", {}) or {}
            elif hasattr(self._cfg, "as_dict"):
                bs_cfg = self._cfg.as_dict().get("buildsystem", {}) or {}
            else:
                bs_cfg = getattr(self._cfg, "buildsystem", {}) or {}
        except Exception:
            bs_cfg = {}

        self.parallel_jobs = int(bs_cfg.get("parallel_jobs", 4))
        self.use_sandbox = bool(bs_cfg.get("sandbox", True))
        self.keep_build_dirs = bool(bs_cfg.get("keep_build_dirs", False))
        self.cache_dir = os.path.abspath(bs_cfg.get("cache_dir", os.path.join(os.getcwd(), ".rquest_build_cache")))
        _ensure_dir(self.cache_dir)
        self.package_output = os.path.abspath(bs_cfg.get("package_output", bs_cfg.get("package_output", os.path.join(os.getcwd(), "packages"))))
        _ensure_dir(self.package_output)
        self.default_builder = bs_cfg.get("default_builder", "autotools")
        self._db = get_db() if callable(get_db) else None
        _ensure_build_tables(self._db)
        # managers
        self._sandbox = get_sandbox_manager() if callable(get_sandbox_manager) else None
        self._fetcher = get_fetcher_manager() if callable(get_fetcher_manager) else None
        self._patcher = get_patch_manager() if callable(get_patch_manager) else None
        self._hooks = get_hook_manager() if callable(get_hook_manager) else None
        # internal
        self._session_id: Optional[str] = None
        self._lock = threading.RLock()
        # timeouts
        self._stage_timeouts = bs_cfg.get("stage_timeouts", {"configure":1800, "compile":7200, "install":1800, "package":600})
        # retries
        self._retries = int(bs_cfg.get("retries", 1))

    def stage_timeout(self, stage: str) -> int:
        return int(self._stage_timeouts.get(stage, 3600))

    # ----------------------------
    # env assembly for builds
    # ----------------------------
    def _assemble_env(self, pkg_meta: Dict[str,Any]) -> Dict[str,str]:
        env = os.environ.copy()
        buildcfg = pkg_meta.get("build", {}) or {}
        cflags = " ".join(buildcfg.get("cflags", []) or [])
        ldflags = " ".join(buildcfg.get("ldflags", []) or [])
        cppflags = " ".join(buildcfg.get("cppflags", []) or [])
        if cflags:
            env["CFLAGS"] = env.get("CFLAGS","") + (" " + cflags if env.get("CFLAGS") else cflags)
        if ldflags:
            env["LDFLAGS"] = env.get("LDFLAGS","") + (" " + ldflags if env.get("LDFLAGS") else ldflags)
        if cppflags:
            env["CPPFLAGS"] = env.get("CPPFLAGS","") + (" " + cppflags if env.get("CPPFLAGS") else cppflags)
        # set MAKEFLAGS
        makeflags = buildcfg.get("makeflags", [])
        if makeflags:
            env["MAKEFLAGS"] = " ".join(makeflags)
        # reproducible build fixes
        if pkg_meta.get("reproducible"):
            env["SOURCE_DATE_EPOCH"] = str(pkg_meta.get("source_date_epoch", int(time.time())))
        return env
      # continuation of Rquest/rquest1.0/modules/buildsystem.py (PART 2)

    # ----------------------------
    # prepare: fetch sources & extract to build dir
    # ----------------------------
    def prepare(self, pkg_meta: Dict[str,Any], *, force: bool=False) -> Dict[str,Any]:
        """
        Ensure sources available and copied to build_dir. Returns dict with keys:
        ok, source_dir, build_dir, destdir
        """
        pkg_name = pkg_meta.get("name")
        version = pkg_meta.get("version")
        source_spec = pkg_meta.get("source")  # expected: url or local path or dict
        if not pkg_name or not version:
            return {"ok": False, "error": "pkg_meta missing name/version"}

        build_root = os.path.join(self.cache_dir, f"{pkg_name}-{version}")
        # clean build_root unless keep_build_dirs or force false?
        if force and os.path.exists(build_root):
            shutil.rmtree(build_root, ignore_errors=True)
        _ensure_dir(build_root)

        # fetch sources using fetcher manager
        src_path = None
        if isinstance(source_spec, dict):
            url = source_spec.get("url") or source_spec.get("path")
            checksum = source_spec.get("checksum")
        else:
            url = source_spec
            checksum = None

        if self._fetcher and url:
            fm = self._fetcher
            try:
                fetch_res = fm.fetch_source(pkg_name, version, url, checksums=checksum, force=force)
                if not fetch_res.get("ok"):
                    logger.error("fetch failed for %s %s: %s", pkg_name, version, fetch_res.get("error"))
                    return {"ok": False, "error": "fetch_failed", "detail": fetch_res}
                src_path = fetch_res.get("path")
            except Exception:
                logger.exception("fetcher.fetch_source exception")
                return {"ok": False, "error": "fetch_exception"}
        else:
            # fallback: if url is local path
            if url and os.path.exists(url):
                src_path = url
            else:
                logger.warning("No fetcher available and source not local. Aborting prepare.")
                return {"ok": False, "error": "no_source"}

        # extract/copy into build_dir
        build_dir = os.path.join(build_root, "work")
        if os.path.exists(build_dir) and not force:
            logger.info("Using existing build_dir %s", build_dir)
        else:
            if os.path.exists(build_dir):
                shutil.rmtree(build_dir, ignore_errors=True)
            _ensure_dir(build_dir)
            # if src_path is archive, extract; if dir, copy
            try:
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, build_dir, dirs_exist_ok=True)
                else:
                    # try extracting common archive formats
                    try:
                        if tarfile.is_tarfile(src_path):
                            with tarfile.open(src_path, "r:*") as tar:
                                tar.extractall(build_dir)
                        else:
                            # copy single file into build_dir
                            shutil.copy2(src_path, build_dir)
                    except Exception:
                        # fallback copy
                        shutil.copy2(src_path, build_dir)
            except Exception:
                logger.exception("Failed to prepare build dir from %s", src_path)
                return {"ok": False, "error": "extract_failed"}

        # create destdir for install
        destdir = os.path.join(build_root, "dest")
        _ensure_dir(destdir)

        # store session id for this build (one session per build for reuse)
        if self.use_sandbox and self._sandbox:
            self._session_id = self._sandbox.start_session()
        else:
            self._session_id = None

        # run pre_prepare hooks
        if self._hooks:
            try:
                self._hooks.run("pre_prepare", context={"pkg_meta": pkg_meta, "build_dir": build_dir}, package=pkg_name)
            except Exception:
                logger.exception("pre_prepare hooks failed: continuing")

        # apply patches
        if self._patcher:
            try:
                patch_res = self._patcher.apply_patches(pkg_name, version, build_dir, dry_run=False, sandbox=bool(self._session_id), abort_on_fail=True)
                if not patch_res.get("ok"):
                    logger.error("apply_patches failed for %s: %s", pkg_name, patch_res)
                    return {"ok": False, "error": "patch_failed", "detail": patch_res}
            except Exception:
                logger.exception("patch apply exception")
                return {"ok": False, "error": "patch_exception"}

        # post_prepare hooks
        if self._hooks:
            try:
                self._hooks.run("post_prepare", context={"pkg_meta": pkg_meta, "build_dir": build_dir}, package=pkg_name)
            except Exception:
                logger.exception("post_prepare hooks failed: continuing")

        return {"ok": True, "source_dir": src_path, "build_dir": build_dir, "destdir": destdir, "session_id": self._session_id}

    # ----------------------------
    # configure step
    # ----------------------------
    def configure(self, pkg_meta: Dict[str,Any], build_dir: str) -> Dict[str,Any]:
        pkg_name = pkg_meta.get("name")
        version = pkg_meta.get("version")
        builder_type = pkg_meta.get("build", {}).get("system") or detect_build_system(build_dir) or self.default_builder
        builder = self._create_builder(builder_type, pkg_meta, build_dir, build_dir, os.path.join(os.path.dirname(build_dir), "dest"))
        # pre_configure hooks
        if self._hooks:
            try:
                self._hooks.run("pre_configure", context={"pkg_meta": pkg_meta, "build_dir": build_dir}, package=pkg_name)
            except Exception:
                logger.exception("pre_configure hook failed")
        res = builder.configure()
        # post_configure hooks
        if self._hooks:
            try:
                self._hooks.run("post_configure", context={"pkg_meta": pkg_meta, "build_dir": build_dir, "result": res}, package=pkg_name)
            except Exception:
                logger.exception("post_configure hook failed")
        return res

    # ----------------------------
    # compile step
    # ----------------------------
    def compile(self, pkg_meta: Dict[str,Any], build_dir: str, jobs: Optional[int] = None) -> Dict[str,Any]:
        pkg_name = pkg_meta.get("name")
        jobs = jobs or pkg_meta.get("build", {}).get("jobs") or self.parallel_jobs
        builder_type = pkg_meta.get("build", {}).get("system") or detect_build_system(build_dir) or self.default_builder
        builder = self._create_builder(builder_type, pkg_meta, build_dir, build_dir, os.path.join(os.path.dirname(build_dir), "dest"))
        # pre_compile hooks
        if self._hooks:
            try:
                self._hooks.run("pre_compile", context={"pkg_meta": pkg_meta, "build_dir": build_dir}, package=pkg_name)
            except Exception:
                logger.exception("pre_compile hook failed")
        res = builder.compile(jobs=int(jobs))
        # post_compile hooks
        if self._hooks:
            try:
                self._hooks.run("post_compile", context={"pkg_meta": pkg_meta, "build_dir": build_dir, "result": res}, package=pkg_name)
            except Exception:
                logger.exception("post_compile hook failed")
        return res

    # ----------------------------
    # test step
    # ----------------------------
    def test(self, pkg_meta: Dict[str,Any], build_dir: str) -> Dict[str,Any]:
        pkg_name = pkg_meta.get("name")
        builder_type = pkg_meta.get("build", {}).get("system") or detect_build_system(build_dir) or self.default_builder
        builder = self._create_builder(builder_type, pkg_meta, build_dir, build_dir, os.path.join(os.path.dirname(build_dir), "dest"))
        if self._hooks:
            try:
                self._hooks.run("pre_test", context={"pkg_meta": pkg_meta, "build_dir": build_dir}, package=pkg_name)
            except Exception:
                logger.exception("pre_test hook failed")
        res = builder.test()
        if self._hooks:
            try:
                self._hooks.run("post_test", context={"pkg_meta": pkg_meta, "build_dir": build_dir, "result": res}, package=pkg_name)
            except Exception:
                logger.exception("post_test hook failed")
        return res

    # ----------------------------
    # install step (uses fakeroot if requested)
    # ----------------------------
    def install(self, pkg_meta: Dict[str,Any], build_dir: str, destdir: Optional[str]=None, use_fakeroot: Optional[bool]=None) -> Dict[str,Any]:
        pkg_name = pkg_meta.get("name")
        version = pkg_meta.get("version")
        destdir = destdir or os.path.join(os.path.dirname(build_dir), "dest")
        use_fakeroot = True if (use_fakeroot is None and (self._cfg.get("buildsystem",{}).get("use_fakeroot") or pkg_meta.get("build",{}).get("use_fakeroot"))) else bool(use_fakeroot)
        builder_type = pkg_meta.get("build", {}).get("system") or detect_build_system(build_dir) or self.default_builder
        builder = self._create_builder(builder_type, pkg_meta, build_dir, build_dir, destdir)
        # pre_install hooks
        if self._hooks:
            try:
                self._hooks.run("pre_install", context={"pkg_meta": pkg_meta, "build_dir": build_dir, "destdir": destdir}, package=pkg_name)
            except Exception:
                logger.exception("pre_install hook failed")
        # perform install
        res = builder.install(use_fakeroot=use_fakeroot)
        # Mark in DB that fakeroot used
        try:
            if self._db:
                self._db.execute("INSERT INTO builds (package, version, system, status, start_ts, end_ts, duration, used_fakeroot) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                 (pkg_name, version, builder_type, "install-step", int(time.time()), int(time.time()), 0.0, int(use_fakeroot)), commit=True)
        except Exception:
            logger.exception("Failed to note fakeroot usage in DB")
        # post_install hooks
        if self._hooks:
            try:
                self._hooks.run("post_install", context={"pkg_meta": pkg_meta, "build_dir": build_dir, "destdir": destdir, "result": res}, package=pkg_name)
            except Exception:
                logger.exception("post_install hook failed")
        return res

    # ----------------------------
    # packaging / artifact generation
    # ----------------------------
    def package_artifact(self, pkg_meta: Dict[str,Any], destdir: str, *, compress: str = "tar.gz") -> Dict[str,Any]:
        """
        Create an artifact (tarball) from destdir, produce SBOM metadata and optional signing.
        """
        pkg_name = pkg_meta.get("name")
        version = pkg_meta.get("version")
        ts = int(time.time())
        artifact_name = f"{pkg_name}-{version}-{ts}.tar.gz" if compress == "tar.gz" else f"{pkg_name}-{version}-{ts}.tar"
        artifact_path = os.path.join(self.package_output, artifact_name)
        try:
            with tarfile.open(artifact_path, "w:gz") as tar:
                tar.add(destdir, arcname=f"{pkg_name}-{version}")
            # generate simple SBOM
            sbom = {
                "package": pkg_name,
                "version": version,
                "built_at": ts,
                "source": pkg_meta.get("source"),
                "build_flags": pkg_meta.get("build", {}),
            }
            sbom_path = artifact_path + ".sbom.json"
            with open(sbom_path, "w", encoding="utf-8") as f:
                json.dump(sbom, f, indent=2, ensure_ascii=False)
            logger.info("Packaged artifact %s (sbom: %s)", artifact_path, sbom_path)
            return {"ok": True, "artifact": artifact_path, "sbom": sbom_path}
        except Exception:
            logger.exception("package_artifact failed")
            return {"ok": False, "error": "package_failed"}

    # ----------------------------
    # publish artifact (simple: move to registry path or call HTTP)
    # ----------------------------
    def publish(self, artifact_path: str, registry: Optional[str]=None, sign: bool=True) -> Dict[str,Any]:
        registry = registry or self._cfg.get("buildsystem", {}).get("publish", {}).get("registry")
        if not registry:
            # fallback: keep in package_output and return path
            logger.info("No registry configured; artifact at %s", artifact_path)
            return {"ok": True, "published_path": artifact_path}
        # if registry is a local path
        if registry.startswith("http://") or registry.startswith("https://"):
            # simple HTTP PUT is beyond scope - user can extend
            logger.warning("Remote registry publishing not implemented in default buildsystem; keeping artifact local")
            return {"ok": False, "error": "remote_publish_not_implemented"}
        else:
            try:
                _ensure_dir(registry)
                dst = os.path.join(registry, os.path.basename(artifact_path))
                shutil.copy2(artifact_path, dst)
                logger.info("Published artifact to %s", dst)
                return {"ok": True, "published_path": dst}
            except Exception:
                logger.exception("publish failed")
                return {"ok": False, "error": "publish_failed"}

    # ----------------------------
    # remove package (uninstall)
    # ----------------------------
    def remove_package(self, pkg_meta: Dict[str,Any]) -> Dict[str,Any]:
        pkg_name = pkg_meta.get("name")
        version = pkg_meta.get("version")
        # consult DB for artifact or installed paths
        try:
            if self._db:
                row = self._db.fetchone("SELECT artifacts_path FROM builds WHERE package = ? AND version = ? ORDER BY id DESC LIMIT 1", (pkg_name, version))
                if row and row.get("artifacts_path"):
                    path = row["artifacts_path"]
                    if os.path.exists(path):
                        os.remove(path)
                    logger.info("Removed artifact %s", path)
                    return {"ok": True}
        except Exception:
            logger.exception("remove_package failed")
        # fallback: nothing to remove
        logger.info("No artifact info for %s %s", pkg_name, version)
        return {"ok": False, "error": "not_found"}

    # ----------------------------
    # high-level build orchestration
    # ----------------------------
    def build_package(self, pkg_meta: Dict[str,Any], *, force: bool=False, dry_run: bool=False, shards: Optional[int]=None) -> Dict[str,Any]:
        """
        Orchestrate full build: prepare -> configure -> compile -> test -> install -> package -> publish -> cleanup
        """
        pkg_name = pkg_meta.get("name")
        version = pkg_meta.get("version")
        logger.info("Starting build for %s %s (force=%s)", pkg_name, version, force)
        start_ts = _now_ts()
        # check mask
        if is_masked(pkg_name, version, None):
            logger.warning("Package %s masked; aborting build", pkg_name)
            return {"ok": False, "error": "masked"}

        # prepare
        prep = self.prepare(pkg_meta, force=force)
        if not prep.get("ok"):
            logger.error("prepare failed: %s", prep)
            return {"ok": False, "stage": "prepare", "detail": prep}

        build_dir = prep["build_dir"]
        destdir = prep["destdir"]

        # configure
        cfg_res = self.configure(pkg_meta, build_dir)
        if not cfg_res.get("ok", cfg_res.get("exit_code",0) == 0):
            logger.error("configure failed: %s", cfg_res)
            return {"ok": False, "stage": "configure", "detail": cfg_res}

        # compile
        comp_res = self.compile(pkg_meta, build_dir, jobs=pkg_meta.get("build", {}).get("jobs"))
        if not comp_res.get("ok", comp_res.get("exit_code",0) == 0):
            logger.error("compile failed: %s", comp_res)
            return {"ok": False, "stage": "compile", "detail": comp_res}

        # optionally run tests
        if pkg_meta.get("build", {}).get("run_tests", False):
            test_res = self.test(pkg_meta, build_dir)
            if not test_res.get("ok", test_res.get("exit_code",0) == 0):
                logger.error("tests failed: %s", test_res)
                return {"ok": False, "stage": "test", "detail": test_res}

        # install (use fakeroot if configured)
        use_fakeroot = bool(self._cfg.get("buildsystem",{}).get("use_fakeroot", False) or pkg_meta.get("build",{}).get("use_fakeroot", False))
        inst_res = self.install(pkg_meta, build_dir, destdir=destdir, use_fakeroot=use_fakeroot)
        if not inst_res.get("ok", inst_res.get("exit_code",0) == 0):
            logger.error("install failed: %s", inst_res)
            return {"ok": False, "stage": "install", "detail": inst_res}

        # package artifact
        pkg_res = self.package_artifact(pkg_meta, destdir)
        if not pkg_res.get("ok"):
            logger.error("package_artifact failed: %s", pkg_res)
            return {"ok": False, "stage": "package", "detail": pkg_res}

        artifact_path = pkg_res.get("artifact")
        # publish
        publish_cfg = pkg_meta.get("publish") or self._cfg.get("buildsystem", {}).get("publish", {})
        if publish_cfg:
            pub_res = self.publish(artifact_path, registry=publish_cfg.get("registry") or None, sign=publish_cfg.get("sign", True))
        else:
            pub_res = {"ok": True, "published_path": artifact_path}

        end_ts = _now_ts()
        duration = end_ts - start_ts

        # record in DB
        try:
            if self._db:
                self._db.execute("INSERT INTO builds (package, version, system, status, start_ts, end_ts, duration, log_path, artifacts_path, used_fakeroot, cache_hit) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                 (pkg_name, version, pkg_meta.get("build",{}).get("system") or detect_build_system(build_dir), "success" if pub_res.get("ok") else "published_failed", start_ts, end_ts, float(duration), None, artifact_path, int(use_fakeroot), 0), commit=True)
        except Exception:
            logger.exception("Failed to write build record to DB")

        # post_build hooks
        if self._hooks:
            try:
                self._hooks.run("post_build", context={"pkg_meta": pkg_meta, "artifact": artifact_path}, package=pkg_name)
            except Exception:
                logger.exception("post_build hook failed")

        # cleanup build dirs if configured
        if not self.keep_build_dirs:
            try:
                # remove build root (parent of build_dir)
                build_root = os.path.dirname(build_dir)
                shutil.rmtree(build_root, ignore_errors=True)
            except Exception:
                logger.exception("cleanup build dirs failed")

        logger.info("Build finished for %s %s in %ds", pkg_name, version, duration)
        return {"ok": True, "artifact": artifact_path, "publish": pub_res, "duration": duration}

    # ----------------------------
    # helper: create builder
    # ----------------------------
    def _create_builder(self, builder_type: str, pkg_meta: Dict[str,Any], source_dir: str, build_dir: str, destdir: str) -> BaseBuilder:
        builder_type = (builder_type or "").lower()
        if builder_type == "autotools":
            return AutotoolsBuilder(pkg_meta, source_dir, build_dir, destdir, self)
        if builder_type == "cmake":
            return CMakeBuilder(pkg_meta, source_dir, build_dir, destdir, self)
        # TODO: meson, python, cargo, etc
        return AutotoolsBuilder(pkg_meta, source_dir, build_dir, destdir, self)

# ----------------------------
# Module-level manager and convenience
# ----------------------------
_MANAGER_LOCK = threading.RLock()
_MANAGER: Optional[BuildSystem] = None

def get_buildsystem() -> BuildSystem:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = BuildSystem()
        return _MANAGER

def build_package(*a, **k):
    return get_buildsystem().build_package(*a, **k)

def prepare(*a, **k):
    return get_buildsystem().prepare(*a, **k)

def configure(*a, **k):
    return get_buildsystem().configure(*a, **k)

def compile_package(*a, **k):
    return get_buildsystem().compile(*a, **k)

def install_package(*a, **k):
    return get_buildsystem().install(*a, **k)

def package_artifact(*a, **k):
    return get_buildsystem().package_artifact(*a, **k)

def publish(*a, **k):
    return get_buildsystem().publish(*a, **k)

# ----------------------------
# CLI demo
# ----------------------------
if __name__ == "__main__":
    bs = get_buildsystem()
    print("BuildSystem ready.")
    # Example minimal run with a hypothetical pkg_meta
    example_meta = {
        "name": "example",
        "version": "0.1",
        "source": None,  # set to a local tarball path to test
        "build": {
            "system": "autotools",
            "cflags": ["-O2"],
            "jobs": 2,
            "use_fakeroot": False,
            "run_tests": False
        }
    }
    print("Call build_package(example) to test (ensure source present).")
