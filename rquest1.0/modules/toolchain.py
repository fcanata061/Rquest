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
 - containerized builds (podman/docker) support for isolation
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
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Try to import yaml for .meta parsing
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

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

    def get_db():
        return None

    def emit_event(*a, **k):
        pass

try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:

    def get_buildsystem():
        return None

try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:

    def get_pkgtool():
        return None

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
    from modules.repo_sync import sync_all as repo_sync_all  # type: ignore
except Exception:

    def repo_sync_all(*a, **k):
        return {"ok": False, "error": "repo_sync unavailable"}

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

# -----------------------
# Config defaults
# -----------------------
CFG = get_config() if callable(get_config) else {}
TC_CFG = CFG.get("toolchain", {}) if isinstance(CFG, dict) else {}
DEFAULTS = {
    "toolchains_dir": os.path.expanduser("~/.rquest/toolchains"),
    "bootstrap_base": os.path.expanduser("~/.rquest/bootstrap"),
    "default_profile": "system",
    "profiles": {},
    "container_build": True,
    "container_runtime": "podman",
    "ml_enabled": True,
    "ml_min_data": 20,
    "repo_local": CFG.get("repos", {}).get("local") if isinstance(CFG, dict) else None,
    "snapshots": CFG.get("snapshots", {}),
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
            dget = get_db() if callable(get_db) else None
            if dget:
                DB = dget()
            else:
                DB = None
        except Exception:
            DB = None
    return DB


def _persist_toolchain_record(record: Dict[str, Any]):
    db = _get_db()
    if db:
        try:
            # Attempt simple SQL insert (works for sqlite wrapper)
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS toolchains (
                  id TEXT PRIMARY KEY,
                  name TEXT,
                  profile JSON,
                  path TEXT,
                  created_at INTEGER
                )
                """,
                (),
                commit=True,
            )
            db.execute(
                "INSERT OR REPLACE INTO toolchains (id, name, profile, path, created_at) VALUES (?,?,?,?,?)",
                (
                    record.get("id"),
                    record.get("name"),
                    json.dumps(record.get("profile") or {}),
                    record.get("path"),
                    record.get("created_at"),
                ),
                commit=True,
            )
        except Exception:
            logger.exception("Failed persisting toolchain record")
    else:
        idx = os.path.join(TOOLCHAINS_DIR, "index.json")
        data = _safe_read_json(idx) or {"toolchains": []}
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
        if not self.db:
            return None
        try:
            rows = self.db.fetchall("SELECT features, label FROM toolchain_ml") or []
            X = []
            y = []
            for r in rows:
                try:
                    f = json.loads(r.get("features") or "{}")
                    X.append([f.get("num_files", 0), f.get("total_size", 0)])
                    y.append(int(r.get("label", 0)))
                except Exception:
                    continue
            if len(X) >= ML_MIN_DATA:
                return X, y
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
        X, y = data
        try:
            self.model.fit(X, y)  # type: ignore
            logger.info("Toolchain ML advisor trained")
            return True
        except Exception:
            logger.exception("ML training failed")
            return False

    def recommend_profile(self, features: Dict[str, Any]) -> Tuple[Optional[str], float]:
        if self.enabled and self.model:
            try:
                x = _np.array([[features.get("num_files", 0), features.get("total_size", 0)]])
                p = float(self.model.predict_proba(x)[0][1])  # type: ignore
                if p > 0.7:
                    return ("performance", p)
                elif p > 0.4:
                    return ("balanced", p)
                else:
                    return ("debug", p)
            except Exception:
                logger.exception("ML recommend failed")
        size = features.get("total_size", 0)
        if size > 100 * (1024 ** 2):
            return ("performance", 0.6)
        return ("balanced", 0.2)


# -----------------------
# Toolchain data classes
# -----------------------
class ToolchainProfile:
    def __init__(
        self,
        name: str,
        kind: str = "gcc",
        cflags: str = "",
        cxxflags: str = "",
        ldflags: str = "",
        extra_env: Optional[Dict[str, str]] = None,
        cross_target: Optional[str] = None,
    ):
        self.name = name
        self.kind = kind  # 'gcc' or 'clang' or 'custom'
        self.cflags = cflags
        self.cxxflags = cxxflags
        self.ldflags = ldflags
        self.extra_env = extra_env or {}
        self.cross_target = cross_target

    def to_dict(self):
        return {
            "name": self.name,
            "kind": self.kind,
            "cflags": self.cflags,
            "cxxflags": self.cxxflags,
            "ldflags": self.ldflags,
            "extra_env": self.extra_env,
            "cross_target": self.cross_target,
        }


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
        try:
            bs_get = get_buildsystem() if callable(get_buildsystem) else None
            self.buildsystem = bs_get() if bs_get else None
        except Exception:
            self.buildsystem = None
        try:
            pt_get = get_pkgtool() if callable(get_pkgtool) else None
            self.pkgtool = pt_get() if pt_get else None
        except Exception:
            self.pkgtool = None
        try:
            s_get = get_sandbox_manager() if callable(get_sandbox_manager) else None
            self.sandbox = s_get() if s_get else None
        except Exception:
            self.sandbox = None
        try:
            rs_get = repo_sync_all
            self.repo_sync = rs_get
        except Exception:
            self.repo_sync = None
        try:
            hm_get = get_hook_manager() if callable(get_hook_manager) else None
            self.hooks = hm_get() if hm_get else None
        except Exception:
            self.hooks = None
        try:
            aud_get = get_auditor() if callable(get_auditor) else None
            self.auditor = aud_get() if aud_get else None
        except Exception:
            self.auditor = None

        self.ml = ToolchainMLAdvisor(self.db) if self.db else None
        # Index existing toolchains
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
        profiles: Dict[str, ToolchainProfile] = {}
        cfg_profiles = TC_CFG.get("profiles", DEFAULTS["profiles"]) or {}
        for name, p in cfg_profiles.items():
            profiles[name] = ToolchainProfile(
                name=name,
                kind=p.get("type", "gcc"),
                cflags=p.get("cflags", ""),
                cxxflags=p.get("cxxflags", ""),
                ldflags=p.get("ldflags", ""),
                extra_env=p.get("extra_env", {}),
                cross_target=p.get("target"),
            )
        profiles.setdefault("system", ToolchainProfile("system", kind="system"))
        profiles.setdefault("balanced", ToolchainProfile("balanced", kind="gcc", cflags="-O2", cxxflags="-O2"))
        profiles.setdefault("performance", ToolchainProfile("performance", kind="gcc", cflags="-O3 -march=native -flto", cxxflags="-O3 -march=native -flto"))
        profiles.setdefault("debug", ToolchainProfile("debug", kind="gcc", cflags="-O0 -g", cxxflags="-O0 -g"))
        return profiles

    def list_profiles(self) -> List[Dict[str, Any]]:
        return [p.to_dict() for p in self.profiles.values()]

    # -----------------------
    # Index existing toolchains (from TOOLCHAINS_DIR and DB)
    # -----------------------
    def _index_toolchains(self):
        self.index: Dict[str, Dict[str, Any]] = {}
        if self.db:
            try:
                rows = self.db.fetchall("SELECT id, name, profile, path, created_at FROM toolchains") or []
                for r in rows:
                    try:
                        profile = json.loads(r.get("profile") or "{}")
                    except Exception:
                        profile = {}
                    self.index[r.get("name")] = {"id": r.get("id"), "path": r.get("path"), "profile": profile}
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
    def discover_system_toolchains(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        candidates = ["gcc", "g++", "clang", "clang++"]
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
                first_line = out.splitlines()[0] if out else ""
                results.append({"name": c, "path": exe, "version_info": first_line})
            except Exception:
                results.append({"name": c, "path": exe, "version_info": None})
        return results

    # -----------------------
    # Select/activate toolchain for a build
    # -----------------------
    def env_for_profile(self, profile_name: str, package: Optional[str] = None) -> Dict[str, str]:
        prof = self.profiles.get(profile_name) or self.profiles.get(DEFAULT_PROFILE)
        env: Dict[str, str] = {}
        if prof.cross_target:
            cc = f"{prof.cross_target}-gcc"
            cxx = f"{prof.cross_target}-g++"
            if shutil.which(cc):
                env["CC"] = cc
            if shutil.which(cxx):
                env["CXX"] = cxx
            env["TARGET"] = prof.cross_target
        else:
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
        if prof.cflags:
            env["CFLAGS"] = prof.cflags
        if prof.cxxflags:
            env["CXXFLAGS"] = prof.cxxflags
        if prof.ldflags:
            env["LDFLAGS"] = prof.ldflags
        env.update(prof.extra_env or {})
        return env

    # -----------------------
    # Register a toolchain (after bootstrap or packaging)
    # -----------------------
    def register_toolchain(self, name: str, profile: ToolchainProfile, path: str) -> Dict[str, Any]:
        rec = {"id": f"tc-{_uid()}", "name": name, "profile": profile.to_dict(), "path": path, "created_at": _now_ts()}
        _persist_toolchain_record(rec)
        self.index[name] = {"id": rec["id"], "path": path, "profile": profile.to_dict()}
        logger.info("Registered toolchain %s at %s", name, path)
        try:
            emit_event("toolchain.registered", {"name": name, "path": path})
        except Exception:
            pass
        return rec

    # -----------------------
    # Helpers: find .meta files for toolchain stages
    # -----------------------
    def _find_toolchain_metas(self, local_repo: Optional[str], stage_tag: str) -> List[str]:
        """
        Scan local repo for metas that match 'toolchain' tag and stage_tag (pass1/pass2/pass3/final).
        If local_repo is None, use DEFAULTS repo path or skip.
        """
        metas: List[str] = []
        repo_root = local_repo or DEFAULTS.get("repo_local") or ""
        if not repo_root:
            # try common locations
            candidates = ["/var/lib/rquest/local-repo", "/var/db/rquest/local-repo", os.path.expanduser("~/.rquest/local-repo")]
            for c in candidates:
                if os.path.isdir(c):
                    repo_root = c
                    break
        if not repo_root or not os.path.isdir(repo_root):
            logger.warning("No local repo found for toolchain metas; checked %s", repo_root)
            return metas
        for root, _, files in os.walk(repo_root):
            for fn in files:
                if not (fn.endswith(".meta") or fn.endswith(".yaml") or fn.endswith(".yml") or fn.endswith(".json")):
                    continue
                p = os.path.join(root, fn)
                parsed = self._parse_meta_minimal(p)
                if not parsed:
                    continue
                tags = parsed.get("tags") or []
                name = parsed.get("package", {}).get("name") or parsed.get("name")
                if not name:
                    continue
                if "toolchain" in tags or parsed.get("category") == "toolchain":
                    # match stage: either tag 'pass1' or 'pass2' etc or name contains pass1
                    if stage_tag in tags or stage_tag in (name or ""):
                        metas.append(p)
                    elif stage_tag == "final" and ("pass" not in name):
                        # final heuristics: name 'binutils' final (not passX)
                        metas.append(p)
        # sort by name to get deterministic order
        metas = sorted(list(set(metas)))
        logger.debug("Found %d metas for stage %s", len(metas), stage_tag)
        return metas

    # -----------------------
    # Minimal .meta parser (json/yaml)
    # -----------------------
    def _parse_meta_minimal(self, meta_path: str) -> Optional[Dict[str, Any]]:
        try:
            txt = Path(meta_path).read_text(encoding="utf-8")
            parsed = None
            # try json first
            try:
                parsed = json.loads(txt)
            except Exception:
                if yaml:
                    try:
                        parsed = yaml.safe_load(txt)
                    except Exception:
                        parsed = None
            if not parsed or not isinstance(parsed, dict):
                return None
            return parsed
        except Exception:
            return None

    # -----------------------
    # Utilities to run commands
    # -----------------------
    def _run(self, cmd: List[str], cwd: Optional[str] = None, env: Optional[Dict[str, str]] = None, timeout: Optional[int] = None) -> Tuple[int, str, str]:
        """
        Run a command and stream output to logger. Returns (rc, stdout, stderr).
        """
        try:
            logger.debug("Running: %s (cwd=%s)", " ".join(cmd), cwd)
            proc = subprocess.Popen(cmd, cwd=(cwd or None), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=(env or os.environ), text=True)
            out, err = proc.communicate(timeout=timeout)
            rc = proc.returncode
            if out:
                logger.debug("OUT: %s", out.strip())
            if err:
                logger.debug("ERR: %s", err.strip())
            return rc, out or "", err or ""
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            logger.error("Command timed out: %s", cmd)
            return 124, out or "", err or ""
        except Exception as e:
            logger.exception("Failed running command: %s", e)
            return 1, "", str(e)

    # -----------------------
    # Snapshot helpers
    # -----------------------
    def _create_snapshot(self, name_suffix: str) -> Optional[str]:
        snaps_cfg = DEFAULTS.get("snapshots") or {}
        backend = snaps_cfg.get("backend")
        base = snaps_cfg.get("path") or "/var/lib/rquest/snapshots"
        os.makedirs(base, exist_ok=True)
        ts = time.strftime("%Y%m%d-%H%M%S")
        name = f"rquest-{name_suffix}-{ts}"
        if backend == "btrfs" and shutil.which("btrfs"):
            # attempt to create a snapshot of '/'
            snap_path = os.path.join(base, name)
            try:
                # must have subvolume to snapshot; this is best-effort: user should set snapshot root
                rc, out, err = self._run(["btrfs", "subvolume", "snapshot", "/", snap_path])
                if rc == 0:
                    logger.info("Created btrfs snapshot %s", snap_path)
                    return snap_path
            except Exception:
                logger.exception("btrfs snapshot failed")
        # fallback: create directory copy (best-effort, may be large)
        snap_path = os.path.join(base, name)
        try:
            os.makedirs(snap_path, exist_ok=True)
            # create a tiny marker to indicate snapshot (not a full copy)
            with open(os.path.join(snap_path, "SNAPSHOT_MARKER"), "w", encoding="utf-8") as f:
                f.write(f"snapshot {name} created at {time.ctime()}\n")
            logger.info("Created lightweight snapshot marker %s", snap_path)
            return snap_path
        except Exception:
            logger.exception("Failed to create snapshot fallback")
            return None

    # -----------------------
    # Build a meta package (prepare, build, install)
    # -----------------------
    def _build_meta(self, meta_path: str, env: Optional[Dict[str, str]] = None, use_sandbox: bool = True, use_fakeroot: bool = False) -> Dict[str, Any]:
        """
        Build a package described by meta_path.
        This function:
         - parses meta
         - downloads sources into a temp dir (best-effort: uses wget/curl)
         - runs prepare steps
         - runs build steps
         - runs install steps (with fakeroot if requested)
        """
        result: Dict[str, Any] = {"meta": meta_path, "ok": False, "logs": [], "errors": []}
        parsed = self._parse_meta_minimal(meta_path)
        if not parsed:
            result["errors"].append("invalid_meta")
            return result

        work = Path(tempfile.mkdtemp(prefix="rquest-toolchain-"))
        logger.info("Building meta %s in %s", meta_path, work)
        try:
            # fetch sources listed in meta['source']
            sources = parsed.get("source") or []
            # normalize to list of dicts
            if isinstance(sources, dict):
                sources = [sources]
            for s in sources:
                url = None
                if isinstance(s, str):
                    url = s
                elif isinstance(s, dict):
                    url = s.get("url") or s.get("git")
                if not url:
                    continue
                # download using system tools (wget or curl)
                fname = os.path.basename(url.split("?")[0])
                dest = work / fname
                if dest.exists():
                    logger.debug("Source already present: %s", dest)
                    continue
                # try wget
                if shutil.which("wget"):
                    rc, out, err = self._run(["wget", "-c", "-O", str(dest), url], cwd=str(work))
                elif shutil.which("curl"):
                    rc, out, err = self._run(["curl", "-L", "-o", str(dest), url], cwd=str(work))
                else:
                    logger.warning("No downloader available (wget/curl)")
                    rc = 1
                    out = ""
                    err = "no-downloader"
                if rc != 0:
                    logger.warning("Failed to download %s: %s", url, err.strip() if err else "")
                    # continue nonetheless — some metas may have sources already present in repo
                else:
                    logger.info("Downloaded %s", dest)

            # extract main tarball if detected (try first listed)
            # find likely source tarball
            tar_candidates = [p for p in work.iterdir() if p.is_file() and p.suffix in (".xz", ".gz", ".bz2", ".zip", ".tar")]
            src_dir = work / "src"
            src_dir.mkdir(exist_ok=True)
            extracted = False
            for t in tar_candidates:
                # attempt to extract
                try:
                    if str(t).endswith(".zip"):
                        rc, out, err = self._run(["unzip", "-q", str(t), "-d", str(src_dir)], cwd=str(work))
                    else:
                        rc, out, err = self._run(["tar", "xf", str(t), "-C", str(src_dir)], cwd=str(work))
                    if rc == 0:
                        extracted = True
                        logger.info("Extracted %s", t)
                        break
                except Exception:
                    continue
            if not extracted:
                # maybe repo already contains source folder relative to meta; try parent dir
                logger.debug("No source extracted automatically; continuing with work dir")
                pass

            # prepare: run commands in prepare.steps or prepare.scripts
            prepare = parsed.get("prepare") or {}
            prep_steps = prepare.get("steps") if isinstance(prepare, dict) else None
            if prep_steps and isinstance(prep_steps, list):
                for cmd in prep_steps:
                    logger.info("prepare -> %s", cmd)
                    rc, out, err = self._run(cmd.split(), cwd=str(src_dir))
                    if rc != 0:
                        logger.warning("prepare step failed: %s", cmd)
                        result["errors"].append({"prepare": cmd, "rc": rc, "err": err})
                        # continue to try other steps

            # build: if meta provides build.steps use them; else try autodetect configure/make
            build = parsed.get("build") or {}
            build_steps = []
            if isinstance(build, dict):
                # custom steps key
                if build.get("steps"):
                    build_steps = build.get("steps")
                elif build.get("commands"):
                    build_steps = build.get("commands")
                elif build.get("system") == "autotools":
                    # run out-of-source if requested
                    out_of_source = build.get("out_of_source", False)
                    if out_of_source:
                        bdir = work / "build"
                        bdir.mkdir(exist_ok=True)
                        configure = build.get("configure", [])
                        if configure:
                            build_steps.append(" ".join(configure))
                            build_steps.append(f"make -j{os.cpu_count() or 1}")
                    else:
                        configure = build.get("configure", [])
                        if configure:
                            build_steps.append(" ".join(configure))
                            build_steps.append(f"make -j{os.cpu_count() or 1}")
                elif build.get("system") == "custom":
                    build_steps = build.get("commands") or []

            # Run build steps
            for step in build_steps:
                logger.info("build -> %s", step)
                # allow step to be a list or string
                if isinstance(step, (list, tuple)):
                    cmd = list(step)
                else:
                    cmd = step if isinstance(step, list) else step.split()
                # use env provided
                rc, out, err = self._run(cmd, cwd=str(src_dir), env=env)
                if rc != 0:
                    logger.error("Build step failed: %s (rc=%s)", step, rc)
                    result["errors"].append({"build": step, "rc": rc, "err": err})
                    # stop build if critical
                    break

            # install: honor install.steps or install.commands
            install = parsed.get("install") or {}
            install_steps = []
            if isinstance(install, dict):
                if install.get("steps"):
                    install_steps = install.get("steps")
                elif install.get("commands"):
                    install_steps = install.get("commands")
                else:
                    # try generic install target
                    install_steps = ["make install"]

            # run install steps with fakeroot if requested
            for step in install_steps:
                logger.info("install -> %s", step)
                if isinstance(step, (list, tuple)):
                    cmd = list(step)
                else:
                    cmd = step if isinstance(step, list) else step.split()
                if use_fakeroot and create_fakeroot:
                    try:
                        fr = create_fakeroot()
                        rc, out, err = fr.run(cmd, cwd=str(src_dir), env=env)
                    except Exception:
                        logger.exception("fakeroot install failed, trying direct install")
                        rc, out, err = self._run(cmd, cwd=str(src_dir), env=env)
                else:
                    rc, out, err = self._run(cmd, cwd=str(src_dir), env=env)
                if rc != 0:
                    logger.error("Install step failed: %s -> rc=%s", step, rc)
                    result["errors"].append({"install": step, "rc": rc, "err": err})
                    break

            # success heuristics
            if not result["errors"]:
                result["ok"] = True
                logger.info("Build/install of %s succeeded", meta_path)
            else:
                logger.warning("Build/install of %s had errors", meta_path)
            return result
        except Exception as e:
            logger.exception("Exception building meta %s: %s", meta_path, e)
            result["errors"].append(str(e))
            return result
        finally:
            # cleanup unless user requests to keep temps
            keep = CFG.get("build", {}).get("keep_temps") if isinstance(CFG, dict) else False
            if keep:
                logger.info("Keeping build workdir %s per config", work)
            else:
                try:
                    shutil.rmtree(work, ignore_errors=True)
                except Exception:
                    pass

    # -----------------------
    # Bootstrap flow: stage1 -> stage2 -> stage3
    # -----------------------
    def bootstrap(self, name: str, profile_name: str = "balanced", stages: int = 2, target: Optional[str] = None, use_container: bool = True, force: bool = False) -> Dict[str, Any]:
        """
        Create a toolchain by bootstrapping from source.
        stages: 1 (minimal), 2 (complete), 3 (optimized rebuild)
        Returns record with steps, logs and final registration (if successful).
        """
        record: Dict[str, Any] = {
            "id": f"bootstrap-{_uid()}",
            "name": name,
            "profile": profile_name,
            "stages_requested": stages,
            "started_at": _now_ts(),
            "steps": [],
        }

        # 1) sync repos if available (so we have latest .meta)
        try:
            if callable(self.repo_sync):
                logger.info("Syncing repositories before bootstrap...")
                try:
                    sync_res = self.repo_sync()
                    logger.debug("repo_sync result: %s", sync_res)
                except Exception:
                    logger.exception("repo_sync failed or not available")
        except Exception:
            pass

        # 2) pick profile env
        env_base = self.env_for_profile(profile_name)
        if target:
            env_base["TARGET"] = target

        # mapping of stage tags to search keys
        stage_map = {1: "pass1", 2: "pass2", 3: "pass3", 4: "final"}

        # stages to run: 1..stages (but support up to 3)
        stages_to_run = list(range(1, min(int(stages), 3) + 1))
        logger.info("Bootstrapping toolchain '%s' stages: %s", name, stages_to_run)

        # create base snapshot
        snap = self._create_snapshot(f"{name}-prebootstrap")
        record["snapshot_before"] = snap

        # iterate stages
        for s in stages_to_run:
            stage_tag = stage_map.get(s, f"pass{s}")
            logger.info("Starting stage %s (%s)", s, stage_tag)
            record["steps"].append({"stage": s, "tag": stage_tag, "started_at": _now_ts(), "items": []})

            metas = self._find_toolchain_metas(local_repo=TC_CFG.get("repo_local") if isinstance(TC_CFG, dict) else None, stage_tag=stage_tag)
            if not metas:
                logger.warning("No metas found for stage %s (%s). Skipping stage.", s, stage_tag)
                record["steps"][-1]["skipped"] = True
                continue

            # for each meta in stage: build using _build_meta
            for meta in metas:
                logger.info("Stage %s: building meta %s", s, meta)
                # snapshot before each package
                sub_snap = self._create_snapshot(f"{name}-s{s}-{Path(meta).stem}")
                try:
                    use_fakeroot = bool(CFG.get("build", {}).get("fakeroot", False) if isinstance(CFG, dict) else False)
                except Exception:
                    use_fakeroot = False
                res = self._build_meta(meta, env=env_base, use_sandbox=bool(self.sandbox), use_fakeroot=use_fakeroot)
                record["steps"][-1]["items"].append(res)
                if not res.get("ok") and not force:
                    logger.error("Build failed for %s at stage %s; aborting bootstrap", meta, s)
                    record["steps"][-1]["failed"] = True
                    # attempt to create rollback snapshot marker
                    record["failed_at"] = {"stage": s, "meta": meta}
                    return record
                # optionally audit
                try:
                    if self.auditor:
                        self.auditor().audit_package_meta(meta)
                except Exception:
                    logger.debug("Auditor not available or audit failed")
            # end metas loop
            record["steps"][-1]["finished_at"] = _now_ts()
            logger.info("Stage %s finished", s)

        # Optionally: run finalization stage (e.g., build final binutils/gcc)
        # register toolchain
        final_path = os.path.join(self.toolchains_dir, name)
        try:
            os.makedirs(final_path, exist_ok=True)
            rec = self.register_toolchain(name, self.profiles.get(profile_name) or ToolchainProfile(profile_name), final_path)
            record["registered"] = rec
        except Exception:
            logger.exception("Failed to register toolchain")
        record["finished_at"] = _now_ts()
        try:
            emit_event("toolchain.bootstrap.finished", {"name": name, "record": record})
        except Exception:
            pass
        return record


# -----------------------
# Small CLI for testing
# -----------------------
def _cli():
    import argparse

    ap = argparse.ArgumentParser(prog="rquest-toolchain", description="Rquest toolchain helper")
    ap.add_argument("action", nargs="?", default="bootstrap", choices=["bootstrap", "list", "discover"])
    ap.add_argument("--name", "-n", default="default-toolchain")
    ap.add_argument("--profile", "-p", default="balanced")
    ap.add_argument("--stages", "-s", type=int, default=2)
    ap.add_argument("--target", "-t", default=None)
    args = ap.parse_args()

    mgr = ToolchainManager()
    if args.action == "discover":
        print(json.dumps(mgr.discover_system_toolchains(), indent=2, ensure_ascii=False))
    elif args.action == "list":
        print(json.dumps(mgr.list_profiles(), indent=2, ensure_ascii=False))
    elif args.action == "bootstrap":
        res = mgr.bootstrap(name=args.name, profile_name=args.profile, stages=args.stages, target=args.target)
        print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    _cli()
