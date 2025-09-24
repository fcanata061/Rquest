# Rquest/rquest1.0/modules/toolchain.py
"""
toolchain.py - toolchain management for Rquest

Capabilities:
- discover system toolchains (gcc/clang and cross compilers)
- define and manage named toolchain profiles (cflags, ldflags, type)
- select toolchain per-package or globally, with fallback
- bootstrap stages: stage1 (minimal toolchain), stage2 (full toolchain), stage3 (optimized rebuild)
- integration points: config, db, buildsystem, sandbox, pkgtool, fakeroot, repo_sync, audit, hooks, logging
- snapshot integration via fakeroot or FS tools when available
- containerized builds (docker/podman) support for isolation
- ML advisor skeleton to recommend toolchain/profile
- CLI to list/use/bootstrap/build/distribute/rollback
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
from typing import Any, Dict, List, Optional, Tuple

# -----------------------
# Optional ML libs
# -----------------------
ML_AVAILABLE = False
try:
    import numpy as _np  # type: ignore
    from sklearn.ensemble import RandomForestClassifier  # type: ignore
    ML_AVAILABLE = True
except Exception:
    ML_AVAILABLE = False

# -----------------------
# Integrations (graceful)
# -----------------------
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

try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def emit_event(*a, **k): pass

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
    from modules.fakeroot import create_fakeroot  # type: ignore
except Exception:
    create_fakeroot = None

try:
    from modules.repo_sync import get_repo_sync  # type: ignore
except Exception:
    def get_repo_sync(): return None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    def get_auditor(): return None

# -----------------------
# Config defaults
# -----------------------
CFG = get_config() if callable(get_config) else {}
TC_CFG = CFG.get("toolchain", {}) if isinstance(CFG, dict) else {}

DEFAULTS = {
    "toolchains_dir": os.path.expanduser("~/.rquest/toolchains"),
    "bootstrap_base": os.path.expanduser("~/.rquest/bootstrap"),
    "default_profile": "system",
    "profiles": {},  # user-defined profiles
    "container_build": True,
    "container_runtime": "podman",  # try podman then docker
    "ml_enabled": True,
    "ml_min_data": 20
}

TOOLCHAINS_DIR = TC_CFG.get("toolchains_dir", DEFAULTS["toolchains_dir"])
BOOTSTRAP_BASE = TC_CFG.get("bootstrap_base", DEFAULTS["bootstrap_base"])
DEFAULT_PROFILE = TC_CFG.get("default_profile", DEFAULTS["default_profile"])
CONTAINER_BUILD = bool(TC_CFG.get("container_build", DEFAULTS["container_build"]))
CONTAINER_RUNTIME = TC_CFG.get("container_runtime", DEFAULTS["container_runtime"])
ML_ENABLED = bool(TC_CFG.get("ml_enabled", DEFAULTS["ml_enabled"])) and ML_AVAILABLE
ML_MIN_DATA = int(TC_CFG.get("ml_min_data", DEFAULTS["ml_min_data"]))

os.makedirs(TOOLCHAINS_DIR, exist_ok=True)
os.makedirs(BOOTSTRAP_BASE, exist_ok=True)

# -----------------------
# Utilities
# -----------------------
def _uid() -> str:
    return uuid.uuid4().hex[:10]

def _now_ts() -> int:
    return int(time.time())

def _safe_read_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _safe_write_json(path: str, obj: Dict[str, Any]):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

# -----------------------
# Simple DB wrapper (uses project db if available else local JSON files)
# -----------------------
DB = None
def _get_db():
    global DB
    if DB is None:
        try:
            dbc = get_db()() if callable(get_db) else None
            DB = dbc
        except Exception:
            DB = None
    return DB

def _persist_toolchain_record(record: Dict[str,Any]):
    db = _get_db()
    if db:
        try:
            db.execute("""CREATE TABLE IF NOT EXISTS toolchains (id TEXT PRIMARY KEY, name TEXT, profile JSON, path TEXT, created_at INTEGER)""", (), commit=True)
            db.execute("INSERT OR REPLACE INTO toolchains (id, name, profile, path, created_at) VALUES (?,?,?,?,?)",
                       (record.get("id"), record.get("name"), json.dumps(record.get("profile") or {}), record.get("path"), record.get("created_at")), commit=True)
        except Exception:
            logger.exception("Failed persisting toolchain record")
    else:
        # fallback to file
        idx = os.path.join(TOOLCHAINS_DIR, "index.json")
        data = _safe_read_json(idx) or {"toolchains": []}
        # remove existing with same name
        data["toolchains"] = [t for t in data["toolchains"] if t.get("name") != record.get("name")]
        data["toolchains"].append(record)
        _safe_write_json(idx, data)

# -----------------------
# ML Advisor skeleton
# -----------------------
class ToolchainMLAdvisor:
    def __init__(self, db):
        self.db = db
        self.model = None
        self.enabled = ML_ENABLED and (db is not None)
        if self.enabled:
            try:
                self.model = RandomForestClassifier(n_estimators=50)  # type: ignore
            except Exception:
                logger.exception("Failed creating ML model")
                self.enabled = False

    def _load_training(self):
        # Attempts to load training examples from db table 'toolchain_ml' with fields features,label
        if not self.db:
            return None
        try:
            rows = self.db.fetchall("SELECT features, label FROM toolchain_ml")
            X = []
            y = []
            for r in rows:
                try:
                    f = json.loads(r.get("features") or "{}")
                    X.append([f.get("num_files",0), f.get("total_size",0)])
                    y.append(int(r.get("label",0)))
                except Exception:
                    continue
            if len(X) >= ML_MIN_DATA:
                return X,y
        except Exception:
            logger.exception("Failed load toolchain ML data")
        return None

    def train(self):
        if not self.enabled:
            return False
        data = self._load_training()
        if not data:
            logger.info("Not enough ML data to train toolchain advisor")
            return False
        X,y = data
        try:
            self.model.fit(X,y)
            logger.info("Toolchain ML advisor trained")
            return True
        except Exception:
            logger.exception("ML training failed")
            return False

    def recommend_profile(self, features: Dict[str,Any]) -> Tuple[Optional[str], float]:
        """
        Given package/build features, return recommended profile name and score (0-1).
        Fallback to heuristics if model not available.
        """
        if self.enabled and self.model:
            try:
                x = _np.array([[features.get("num_files",0), features.get("total_size",0)]])
                p = float(self.model.predict_proba(x)[0][1])
                # map p to profile heuristically
                if p > 0.7:
                    return ("performance", p)
                elif p > 0.4:
                    return ("balanced", p)
                else:
                    return ("debug", p)
            except Exception:
                logger.exception("ML recommend failed")
        # heuristics
        size = features.get("total_size",0)
        if size > 100*(1024**2):
            return ("performance", 0.6)
        return ("balanced", 0.2)

# -----------------------
# Toolchain data classes
# -----------------------
class ToolchainProfile:
    def __init__(self, name: str, kind: str = "gcc", cflags: str = "", cxxflags: str = "", ldflags: str = "", extra_env: Optional[Dict[str,str]] = None, cross_target: Optional[str] = None):
        self.name = name
        self.kind = kind  # 'gcc' or 'clang' or 'custom'
        self.cflags = cflags
        self.cxxflags = cxxflags
        self.ldflags = ldflags
        self.extra_env = extra_env or {}
        self.cross_target = cross_target

    def to_dict(self):
        return {"name": self.name, "kind": self.kind, "cflags": self.cflags, "cxxflags": self.cxxflags, "ldflags": self.ldflags, "extra_env": self.extra_env, "cross_target": self.cross_target}
       # continuation PARTE 2/3
# -----------------------
# Main manager
# -----------------------
class ToolchainManager:
    def __init__(self):
        self.toolchains_dir = TOOLCHAINS_DIR
        self.bootstrap_base = BOOTSTRAP_BASE
        self.container_runtime = self._detect_container_runtime()
        self.profiles = self._load_profiles()
        self.db = _get_db()
        self.buildsystem = get_buildsystem()() if callable(get_buildsystem) else None
        self.pkgtool = get_pkgtool()() if callable(get_pkgtool) else None
        self.sandbox = get_sandbox_manager()() if callable(get_sandbox_manager) else None
        self.repo_sync = get_repo_sync()() if callable(get_repo_sync) else None
        self.hooks = get_hook_manager()() if callable(get_hook_manager) else None
        self.auditor = get_auditor()() if callable(get_auditor) else None
        self.ml = ToolchainMLAdvisor(self.db) if self.db else None
        # index existing toolchains
        self._index_toolchains()

    # -----------------------
    # Discover container runtime
    # -----------------------
    def _detect_container_runtime(self) -> Optional[str]:
        if CONTAINER_BUILD:
            for r in (CONTAINER_RUNTIME, "podman", "docker"):
                if shutil.which(r):
                    logger.info("Container runtime found: %s", r)
                    return r
        return None

    # -----------------------
    # Profiles (config + user)
    # -----------------------
    def _load_profiles(self) -> Dict[str, ToolchainProfile]:
        profiles = {}
        # load from config
        cfg_profiles = TC_CFG.get("profiles", DEFAULTS["profiles"])
        for name, p in cfg_profiles.items():
            profiles[name] = ToolchainProfile(name=name, kind=p.get("type","gcc"), cflags=p.get("cflags",""), cxxflags=p.get("cxxflags",""), ldflags=p.get("ldflags",""), extra_env=p.get("extra_env",{}), cross_target=p.get("target"))
        # ensure defaults
        profiles.setdefault("system", ToolchainProfile("system", kind="system"))
        profiles.setdefault("balanced", ToolchainProfile("balanced", kind="gcc", cflags="-O2", cxxflags="-O2"))
        profiles.setdefault("performance", ToolchainProfile("performance", kind="gcc", cflags="-O3 -march=native -flto", cxxflags="-O3 -march=native -flto"))
        profiles.setdefault("debug", ToolchainProfile("debug", kind="gcc", cflags="-O0 -g", cxxflags="-O0 -g"))
        return profiles

    def list_profiles(self) -> List[Dict[str,Any]]:
        return [p.to_dict() for p in self.profiles.values()]

    # -----------------------
    # Index existing toolchains (from TOOLCHAINS_DIR and DB)
    # -----------------------
    def _index_toolchains(self):
        self.index = {}
        # from DB table
        if self.db:
            try:
                rows = self.db.fetchall("SELECT id, name, profile, path, created_at FROM toolchains")
                for r in rows:
                    self.index[r.get("name")] = {"id": r.get("id"), "path": r.get("path"), "profile": json.loads(r.get("profile") or "{}")}
            except Exception:
                logger.debug("No toolchains in DB or DB unavailable")
        # scan directory fallback
        try:
            for name in os.listdir(self.toolchains_dir):
                path = os.path.join(self.toolchains_dir, name)
                if os.path.isdir(path):
                    if name not in self.index:
                        self.index[name] = {"id": None, "path": path, "profile": {}}
        except Exception:
            pass

    # -----------------------
    # Discover system compilers (gcc/clang and cross)
    # -----------------------
    def discover_system_toolchains(self) -> List[Dict[str,Any]]:
        results = []
        candidates = ["gcc", "g++", "clang", "clang++"]
        # also include cross names found in PATH
        try:
            for p in (os.environ.get("PATH") or "").split(os.pathsep):
                try:
                    for f in os.listdir(p):
                        if f.endswith("-gcc") or f.endswith("-clang"):
                            candidates.append(f)
                except Exception:
                    continue
        except Exception:
            pass
        seen = set()
        for c in candidates:
            exe = shutil.which(c)
            if not exe or c in seen:
                continue
            seen.add(c)
            try:
                out = subprocess.check_output([exe, "--version"], stderr=subprocess.STDOUT, text=True)
                # simple parse for kind/version
                first_line = out.splitlines()[0] if out else ""
                results.append({"name": c, "path": exe, "version_info": first_line})
            except Exception:
                results.append({"name": c, "path": exe, "version_info": None})
        return results

    # -----------------------
    # Select/activate toolchain for a build
    # -----------------------
    def env_for_profile(self, profile_name: str, package: Optional[str] = None) -> Dict[str,str]:
        """
        Return environment variables (CC, CXX, CFLAGS, LDFLAGS, etc.) for the given profile.
        """
        prof = self.profiles.get(profile_name) or self.profiles.get(DEFAULT_PROFILE)
        env = {}
        if prof.cross_target:
            # use cross compilers if available (e.g., aarch64-linux-gnu-gcc)
            cc = f"{prof.cross_target}-gcc"
            cxx = f"{prof.cross_target}-g++"
            if shutil.which(cc):
                env["CC"] = cc
            if shutil.which(cxx):
                env["CXX"] = cxx
            # set host triple env for buildsystems
            env["TARGET"] = prof.cross_target
        else:
            # prefer clang/gcc as configured
            if prof.kind == "clang":
                cc = shutil.which("clang") or shutil.which("clang-10") or shutil.which("clang-12")
                cxx = shutil.which("clang++") or shutil.which("clang++-10")
            elif prof.kind == "gcc":
                cc = shutil.which("gcc")
                cxx = shutil.which("g++")
            else:
                cc = shutil.which("gcc") or shutil.which("clang")
                cxx = shutil.which("g++") or shutil.which("clang++")
            if cc:
                env["CC"] = cc
            if cxx:
                env["CXX"] = cxx
        # flags
        if prof.cflags:
            env["CFLAGS"] = prof.cflags
        if prof.cxxflags:
            env["CXXFLAGS"] = prof.cxxflags
        if prof.ldflags:
            env["LDFLAGS"] = prof.ldflags
        # extras
        env.update(prof.extra_env or {})
        return env

    # -----------------------
    # Register a toolchain (after bootstrap or packaging)
    # -----------------------
    def register_toolchain(self, name: str, profile: ToolchainProfile, path: str) -> Dict[str,Any]:
        rec = {"id": f"tc-{_uid()}", "name": name, "profile": profile.to_dict(), "path": path, "created_at": _now_ts()}
        _persist_toolchain_record(rec)
        # update index
        self.index[name] = {"id": rec["id"], "path": path, "profile": profile.to_dict()}
        logger.info("Registered toolchain %s at %s", name, path)
        try:
            emit_event("toolchain.registered", {"name": name, "path": path})
        except Exception:
            pass
        return rec
      # continuation PARTE 3/3
    # -----------------------
    # Bootstrap flow: stage1 -> stage2 -> stage3
    # -----------------------
    def bootstrap(self, name: str, profile_name: str = "balanced", stages: int = 2, target: Optional[str] = None, use_container: bool = True, force: bool = False) -> Dict[str,Any]:
        """
        Create a toolchain by bootstrapping from source.
        stages: 1 (minimal), 2 (complete), 3 (optimized rebuild)
        Returns record with steps, logs and final registration (if successful).
        """
        prof = self.profiles.get(profile_name) or ToolchainProfile(profile_name)
        base_dir = os.path.join(self.bootstrap_base, f"{name}-{_uid()}")
        work_dir = os.path.join(base_dir, "work")
        _ensure_dir = lambda p: os.makedirs(p, exist_ok=True)
        _ensure_dir(work_dir)
        logs = []
        result = {"ok": False, "name": name, "profile": prof.to_dict(), "stages_requested": stages, "stages": [], "toolchain_path": None}
        logger.info("Starting bootstrap '%s' profile=%s stages=%d", name, profile_name, stages)
        # create snapshots via fakeroot if available to allow rollback
        snapshot_id = None
        try:
            if create_fakeroot:
                fr = create_fakeroot(package=f"bootstrap-{name}", version=str(_now_ts()))
                fr.create(use_ld_preload=False, destdir_mode=True)
                snap = fr.snapshot(label="pre-bootstrap")
                snapshot_id = snap.snap_id if snap else None
                logs.append({"snapshot": snap.to_dict() if snap else None})
        except Exception:
            logger.exception("fakeroot snapshot pre-bootstrap failed")

        # Stage functions:
        def run_stage1():
            # minimal tools: build binutils and minimal gcc without libstdc++/glibc
            sres = {"stage": 1, "ok": False, "log": []}
            try:
                # here we expect sources to be available (could fetch via repo_sync or fetcher)
                # For demo: try to run distro packages if allowed else no-op
                cmd = f"echo 'stage1: build binutils/gcc minimal' && mkdir -p {work_dir}/stage1 && sleep 1"
                if use_container and self.container_runtime:
                    ccmd = [self.container_runtime, "run", "--rm", "-v", f"{work_dir}:/work", "ubuntu:22.04", "bash", "-lc", cmd]
                    logger.info("Running stage1 in container: %s", " ".join(ccmd))
                    subprocess.check_call(ccmd)
                else:
                    subprocess.check_call(cmd, shell=True)
                sres["ok"] = True
                sres["log"].append("stage1 complete (simulated)")
            except Exception as e:
                logger.exception("stage1 failed")
                sres["log"].append(str(e))
            return sres

        def run_stage2():
            sres = {"stage": 2, "ok": False, "log": []}
            try:
                cmd = f"echo 'stage2: build full gcc/libc' && mkdir -p {work_dir}/stage2 && sleep 1"
                if use_container and self.container_runtime:
                    ccmd = [self.container_runtime, "run", "--rm", "-v", f"{work_dir}:/work", "ubuntu:22.04", "bash", "-lc", cmd]
                    subprocess.check_call(ccmd)
                else:
                    subprocess.check_call(cmd, shell=True)
                sres["ok"] = True
                sres["log"].append("stage2 complete (simulated)")
            except Exception as e:
                logger.exception("stage2 failed")
                sres["log"].append(str(e))
            return sres

        def run_stage3():
            sres = {"stage": 3, "ok": False, "log": []}
            try:
                cmd = f"echo 'stage3: optimized rebuild' && mkdir -p {work_dir}/stage3 && sleep 1"
                if use_container and self.container_runtime:
                    ccmd = [self.container_runtime, "run", "--rm", "-v", f"{work_dir}:/work", "ubuntu:22.04", "bash", "-lc", cmd]
                    subprocess.check_call(ccmd)
                else:
                    subprocess.check_call(cmd, shell=True)
                sres["ok"] = True
                sres["log"].append("stage3 complete (simulated)")
            except Exception as e:
                logger.exception("stage3 failed")
                sres["log"].append(str(e))
            return sres

        # run requested stages sequentially, with checkpoints and optional hooks
        try:
            if self.hooks:
                try:
                    self.hooks.run("pre_bootstrap", {"name": name, "profile": prof.to_dict()})
                except Exception:
                    logger.exception("pre_bootstrap hooks failed")
            # stage1
            if stages >= 1:
                s1 = run_stage1()
                result["stages"].append(s1); logs.append(s1)
                if not s1["ok"] and not force:
                    result["ok"] = False
                    _safe_write_json(os.path.join(base_dir, "result.json"), result)
                    return result
            # stage2
            if stages >= 2:
                s2 = run_stage2()
                result["stages"].append(s2); logs.append(s2)
                if not s2["ok"] and not force:
                    result["ok"] = False
                    _safe_write_json(os.path.join(base_dir, "result.json"), result)
                    return result
            # stage3
            if stages >= 3:
                s3 = run_stage3()
                result["stages"].append(s3); logs.append(s3)
                if not s3["ok"] and not force:
                    result["ok"] = False
                    _safe_write_json(os.path.join(base_dir, "result.json"), result)
                    return result

            # when all required stages pass, package the toolchain directory
            # For demo, we simulate a path
            toolchain_path = os.path.join(self.toolchains_dir, name)
            os.makedirs(toolchain_path, exist_ok=True)
            # write a small manifest
            manifest = {"name": name, "profile": prof.to_dict(), "created_at": _now_ts(), "stages": stages}
            _safe_write_json(os.path.join(toolchain_path, "manifest.json"), manifest)
            # register
            rec = self.register_toolchain(name, prof, toolchain_path)
            result["toolchain_path"] = toolchain_path
            result["ok"] = True
            if self.hooks:
                try:
                    self.hooks.run("post_bootstrap", {"name": name, "path": toolchain_path, "profile": prof.to_dict()})
                except Exception:
                    logger.exception("post_bootstrap hooks failed")
            # persist final result
            _safe_write_json(os.path.join(base_dir, "result.json"), result)
            return result
        except Exception:
            logger.exception("bootstrap flow failed unexpectedly")
            result["ok"] = False
            _safe_write_json(os.path.join(base_dir, "result.json"), result)
            return result

    # -----------------------
    # Build using a specific toolchain/profile for a package
    # -----------------------
    def build_with_profile(self, package_dir: str, profile_name: str, target: Optional[str] = None, use_container: bool = True, extra_env: Optional[Dict[str,str]] = None) -> Dict[str,Any]:
        """
        Orchestrate buildsystem.run with environment derived from profile.
        """
        env = self.env_for_profile(profile_name)
        if extra_env:
            env.update(extra_env)
        # if target specified, set TARGET
        if target:
            env["TARGET"] = target

        logger.info("Building %s with profile %s (container=%s)", package_dir, profile_name, use_container)
        try:
            if use_container and self.container_runtime and self.sandbox:
                # prefer sandbox to run builds in container / namespace
                try:
                    res = self.sandbox.run_in_sandbox(cmd=None, package_dir=package_dir, env=env, profile=profile_name, container=self.container_runtime)
                    return {"ok": True, "result": res}
                except Exception:
                    logger.exception("sandbox.run_in_sandbox failed; falling back")
            # fallback: call buildsystem directly
            if self.buildsystem and hasattr(self.buildsystem, "build_package_dir"):
                res = self.buildsystem.build_package_dir(package_dir, env=env, profile=profile_name)
                return {"ok": True, "result": res}
            # last fallback: try calling 'make' with env
            cmd = "make -j$(nproc) && make install DESTDIR=./pkg"
            proc = subprocess.run(cmd, shell=True, cwd=package_dir, env={**os.environ, **env}, capture_output=True, text=True)
            ok = proc.returncode == 0
            return {"ok": ok, "stdout": proc.stdout, "stderr": proc.stderr, "rc": proc.returncode}
        except Exception as e:
            logger.exception("build_with_profile failed")
            return {"ok": False, "error": str(e)}

    # -----------------------
    # Export/pack toolchain via pkgtool
    # -----------------------
    def package_toolchain(self, name: str, dest_dir: str) -> Dict[str,Any]:
        rec = self.index.get(name)
        if not rec:
            return {"ok": False, "error": "toolchain_not_found"}
        path = rec.get("path")
        try:
            if self.pkgtool and hasattr(self.pkgtool, "package_from_root"):
                res = self.pkgtool.package_from_root(path, dest_dir, metadata={"toolchain": name})
                return res
            else:
                # fallback tarball
                out = os.path.join(dest_dir, f"{name}-{_uid()}.tar.gz")
                shutil.make_archive(out.replace(".tar.gz",""), 'gztar', root_dir=path)
                return {"ok": True, "package_path": out}
        except Exception:
            logger.exception("package_toolchain failed")
            return {"ok": False, "error": "exception"}

    # -----------------------
    # Distribute toolchain via repo_sync
    # -----------------------
    def distribute_toolchain(self, name: str, nodes: List[str]) -> Dict[str,Any]:
        rec = self.index.get(name)
        if not rec:
            return {"ok": False, "error": "toolchain_not_found"}
        path = rec.get("path")
        results = {}
        if self.repo_sync and hasattr(self.repo_sync, "distribute_path"):
            for n in nodes:
                try:
                    ok = self.repo_sync.distribute_path(path, n)
                    results[n] = {"ok": ok}
                except Exception:
                    logger.exception("repo_sync distribute failed for %s", n)
                    results[n] = {"ok": False}
            return results
        # fallback: scp (requires keys)
        tmp = tempfile.mkdtemp(prefix="tc-dist-")
        try:
            archive = os.path.join(tmp, "tc.tar.gz")
            shutil.make_archive(archive.replace(".tar.gz",""), 'gztar', root_dir=path)
            for n in nodes:
                try:
                    subprocess.check_call(["scp", archive, f"{n}:~/"])
                    results[n] = {"ok": True}
                except Exception:
                    logger.exception("scp distribute failed for %s", n)
                    results[n] = {"ok": False}
            shutil.rmtree(tmp, ignore_errors=True)
            return results
        except Exception:
            logger.exception("distribute_toolchain fallback failed")
            return {"ok": False}
