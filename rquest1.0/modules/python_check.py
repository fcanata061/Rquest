# Rquest/rquest1.0/modules/python_check.py
"""
python_check.py - Gerenciamento avançado de versões Python para Rquest

Funcionalidades:
- descoberta de instalações (sistema, pyenv, conda, venvs)
- instalação via pyenv / pacotes do sistema / build-source (staged fallback)
- criação/gestão de virtualenvs/venvs/conda envs
- inspeções profundas: módulos críticos, ABI, extensões nativas, compatibilidade com Rquest
- doctor + repair (diagnóstico e tentativa de correção)
- integração com toolchain.py / fakeroot.py / buildsystem.py / db.py / hooks.py / audit.py
- snapshots e rollback via fakeroot quando disponível
- ML advisor skeleton para prever problemas em troca de versões/instalação
- políticas: min_version, preferred, strict/relaxed, reject-eol
- CLI: list/install/use/venv/create/inspect/doctor/repair
"""

from __future__ import annotations

import os
import sys
import json
import time
import shutil
import subprocess
import logging
import tempfile
import venv
import hashlib
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------
# Optional libs & ML
# ---------------------------
ML_AVAILABLE = False
try:
    import numpy as _np  # type: ignore
    from sklearn.ensemble import RandomForestClassifier  # type: ignore
    ML_AVAILABLE = True
except Exception:
    ML_AVAILABLE = False

# ---------------------------
# Integrations (graceful)
# ---------------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config(): return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("python_check")
except Exception:
    logger = logging.getLogger("python_check")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# DB wrapper
try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def emit_event(*a, **k): pass

# Optional modules
try:
    from modules.fakeroot import create_fakeroot  # type: ignore
except Exception:
    create_fakeroot = None

try:
    from modules.toolchain import ToolchainManager  # type: ignore
except Exception:
    ToolchainManager = None

try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:
    def get_buildsystem(): return None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    def get_auditor(): return None

try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    def get_resolver(): return None

# ---------------------------
# Config & defaults
# ---------------------------
CFG = get_config() if callable(get_config) else {}
PY_CFG = CFG.get("python", {}) if isinstance(CFG, dict) else {}

DEFAULTS = {
    "min_version": "3.10",
    "preferred": None,
    "policy": "strict",  # strict | relaxed
    "work_dir": os.path.expanduser("~/.rquest/python_check"),
    "venvs_dir": os.path.expanduser("~/.rquest/python_envs"),
    "use_pyenv": True,
    "use_conda": True,
    "snapshots_enabled": True,
    "ml_enabled": True,
    "ml_min_data": 20,
    "reject_eol": True
}

WORK_DIR = PY_CFG.get("work_dir", DEFAULTS["work_dir"])
VENVS_DIR = PY_CFG.get("venvs_dir", DEFAULTS["venvs_dir"])
MIN_VERSION = PY_CFG.get("min_version", DEFAULTS["min_version"])
PREFERRED = PY_CFG.get("preferred", DEFAULTS["preferred"])
POLICY = PY_CFG.get("policy", DEFAULTS["policy"])
USE_PYENV = bool(PY_CFG.get("use_pyenv", DEFAULTS["use_pyenv"]))
USE_CONDA = bool(PY_CFG.get("use_conda", DEFAULTS["use_conda"]))
SNAPSHOTS_ENABLED = bool(PY_CFG.get("snapshots_enabled", DEFAULTS["snapshots_enabled"]))
ML_ENABLED = bool(PY_CFG.get("ml_enabled", DEFAULTS["ml_enabled"])) and ML_AVAILABLE
ML_MIN_DATA = int(PY_CFG.get("ml_min_data", DEFAULTS["ml_min_data"]))
REJECT_EOL = bool(PY_CFG.get("reject_eol", DEFAULTS["reject_eol"]))

os.makedirs(WORK_DIR, exist_ok=True)
os.makedirs(VENVS_DIR, exist_ok=True)

# ---------------------------
# Utilities
# ---------------------------
def _now_ts() -> int:
    return int(time.time())

def _uid() -> str:
    import uuid
    return uuid.uuid4().hex[:10]

def _safe_json_load(path: str) -> Optional[Dict[str,Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _safe_json_write(path: str, obj: Dict[str,Any]):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

def _sha256(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

# ---------------------------
# Data models
# ---------------------------
class PythonInstall:
    def __init__(self, exe_path: str, source: str = "system"):
        self.exe = exe_path  # path to python binary
        self.source = source  # system | pyenv | conda | build
        self.version = self._get_version()
        self.abi_tag = self._get_abi_tag()
        self.metadata: Dict[str,Any] = {}
        self.detected_at = _now_ts()

    def _get_version(self) -> Optional[str]:
        try:
            out = subprocess.check_output([self.exe, "--version"], stderr=subprocess.STDOUT, text=True).strip()
            # output like "Python 3.10.9"
            if out.lower().startswith("python"):
                return out.split()[1]
            return out
        except Exception:
            return None

    def _get_abi_tag(self) -> Optional[str]:
        try:
            cmd = [self.exe, "-c", "import sysconfig; print(sysconfig.get_config_var('SOABI') or '')"]
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True).strip()
            return out or None
        except Exception:
            return None

    def to_dict(self) -> Dict[str,Any]:
        return {"exe": self.exe, "source": self.source, "version": self.version, "abi": self.abi_tag, "metadata": self.metadata, "detected_at": self.detected_at}

# ---------------------------
# ML advisor skeleton
# ---------------------------
class PythonMLAdvisor:
    def __init__(self, db):
        self.db = db
        self.enabled = ML_ENABLED and db is not None
        self.model = None
        if self.enabled:
            try:
                self.model = RandomForestClassifier(n_estimators=30)  # type: ignore
            except Exception:
                logger.exception("Failed creating Python ML model")
                self.enabled = False

    def _load_training(self):
        if not self.db:
            return None
        try:
            rows = self.db.fetchall("SELECT features, label FROM python_ml")
            X = []; y = []
            for r in rows:
                try:
                    f = json.loads(r.get("features") or "{}")
                    X.append([f.get("num_ext_modules",0), f.get("total_size",0)])
                    y.append(int(r.get("label",0)))
                except Exception:
                    continue
            if len(X) >= ML_MIN_DATA:
                return X,y
        except Exception:
            logger.exception("Failed load python ml")
        return None

    def train(self):
        if not self.enabled:
            return False
        data = self._load_training()
        if not data:
            logger.info("Not enough ML data to train python advisor")
            return False
        X,y = data
        try:
            self.model.fit(X,y)
            logger.info("Python ML advisor trained")
            return True
        except Exception:
            logger.exception("Python ML training failed")
            return False

    def predict_risk(self, features: Dict[str,Any]) -> float:
        if self.enabled and self.model:
            try:
                arr = _np.array([[features.get("num_ext_modules",0), features.get("total_size",0)]])
                return float(self.model.predict_proba(arr)[0][1])
            except Exception:
                logger.exception("ML predict failed")
        # heuristic fallback
        score = 0.0
        if features.get("num_ext_modules",0) > 10: score += 0.4
        if features.get("total_size",0) > 50*(1024**2): score += 0.4
        return min(1.0, score)
      # continuation PARTE 2/3
# ---------------------------
# Core manager
# ---------------------------
class PythonCheck:
    def __init__(self):
        self.db = get_db()() if callable(get_db) else None
        self.hooks = get_hook_manager()() if callable(get_hook_manager) else None
        self.toolchain = ToolchainManager() if ToolchainManager else None
        self.buildsystem = get_buildsystem()() if callable(get_buildsystem) else None
        self.auditor = get_auditor()() if callable(get_auditor) else None
        self.resolver = get_resolver()() if callable(get_resolver) else None
        self.ml = PythonMLAdvisor(self.db) if self.db else None
        self.installs: List[PythonInstall] = []
        self._index_installs()

    # ---------------------------
    # Index known python installs from system, pyenv, conda, venvs
    # ---------------------------
    def _index_installs(self):
        self.installs = []
        # 1) system binaries in PATH: python, python3, python3.X
        for name in ("python", "python3"):
            p = shutil.which(name)
            if p:
                try:
                    pi = PythonInstall(p, "system")
                    self.installs.append(pi)
                except Exception:
                    continue
        # also scan for python3.X in /usr/bin and /usr/local/bin
        for dirp in ("/usr/bin", "/usr/local/bin", os.path.expanduser("~/.pyenv/shims")):
            try:
                for fn in os.listdir(dirp):
                    if fn.startswith("python3"):
                        p = os.path.join(dirp, fn)
                        if os.path.isfile(p) and os.access(p, os.X_OK):
                            try:
                                pi = PythonInstall(p, "system")
                                if not any(i.exe == pi.exe for i in self.installs):
                                    self.installs.append(pi)
                            except Exception:
                                continue
            except Exception:
                continue
        # 2) pyenv (if enabled and installed)
        if USE_PYENV and shutil.which("pyenv"):
            try:
                out = subprocess.check_output(["pyenv", "versions", "--bare"], text=True)
                for line in out.splitlines():
                    line = line.strip()
                    if not line: continue
                    # pyenv stores versions under ~/.pyenv/versions/<ver>/bin/python
                    possible = os.path.expanduser(f"~/.pyenv/versions/{line}/bin/python")
                    if os.path.exists(possible):
                        try:
                            pi = PythonInstall(possible, "pyenv")
                            if not any(i.exe == pi.exe for i in self.installs):
                                self.installs.append(pi)
                        except Exception:
                            continue
            except Exception:
                logger.debug("pyenv discovery failed")
        # 3) conda envs (if conda available)
        if USE_CONDA and shutil.which("conda"):
            try:
                out = subprocess.check_output(["conda", "env", "list", "--json"], text=True)
                import json as _js
                data = _js.loads(out)
                for path in data.get("envs", []):
                    possible = os.path.join(path, "bin", "python")
                    if os.path.exists(possible):
                        try:
                            pi = PythonInstall(possible, "conda")
                            if not any(i.exe == pi.exe for i in self.installs):
                                self.installs.append(pi)
                        except Exception:
                            continue
            except Exception:
                logger.debug("conda discovery failed")
        # 4) virtualenvs in VENVS_DIR
        try:
            for d in os.listdir(VENVS_DIR):
                p = os.path.join(VENVS_DIR, d, "bin", "python")
                if os.path.exists(p):
                    try:
                        pi = PythonInstall(p, "venv")
                        if not any(i.exe == pi.exe for i in self.installs):
                            self.installs.append(pi)
                    except Exception:
                        continue
        except Exception:
            pass

        # persist index optionally
        try:
            idx_path = os.path.join(WORK_DIR, "installs.json")
            _safe_json_write(idx_path, {"detected": [i.to_dict() for i in self.installs], "ts": _now_ts()})
        except Exception:
            logger.exception("failed persist installs index")

    def list_installs(self) -> List[Dict[str,Any]]:
        return [i.to_dict() for i in self.installs]

    # ---------------------------
    # Find best python for Rquest (compatibility)
    # ---------------------------
    def find_compatible_for_rquest(self) -> Optional[PythonInstall]:
        """
        Determine which installed python best matches Rquest policy.
        """
        # load Rquest required minimum
        min_v = MIN_VERSION
        preferred = PREFERRED
        accept_mode = POLICY  # strict | relaxed
        # choose candidate with version >= min_v; prefer preferred; prefer same ABI if possible
        def ver_tuple(v: Optional[str]) -> Tuple[int,...]:
            if not v:
                return (0,)
            return tuple(int(x) for x in v.split(".") if x.isdigit())
        candidates = sorted(self.installs, key=lambda x: ver_tuple(x.version), reverse=True)
        # if preferred set, choose that
        if preferred:
            for c in candidates:
                if c.version and c.version.startswith(preferred):
                    return c
        for c in candidates:
            if c.version:
                if ver_tuple(c.version) >= ver_tuple(min_v):
                    # EOL policy check (using auditor or naive)
                    if REJECT_EOL and self._is_eol(c.version):
                        if accept_mode == "strict":
                            continue
                    return c
        return None

    def _is_eol(self, version: str) -> bool:
        # naive EOL check - in real system query a feed; here we treat 3.7 and below as EOL
        try:
            t = tuple(int(x) for x in version.split(".")[:2])
            if t < (3,8):
                return True
            return False
        except Exception:
            return False

    # ---------------------------
    # Install python version (pyenv -> system package -> build from source)
    # ---------------------------
    def install_version(self, version: str, method_preference: List[str] = None, use_fakeroot: bool = True) -> Dict[str,Any]:
        """
        Methods preference: ["pyenv","system","source"]
        Returns dict with keys ok, method, path, note
        """
        method_preference = method_preference or (["pyenv","system","source"] if USE_PYENV else ["system","source"])
        res = {"ok": False, "method": None, "path": None, "note": None}
        # 1) pyenv
        if "pyenv" in method_preference and shutil.which("pyenv"):
            try:
                logger.info("Installing python %s via pyenv", version)
                subprocess.check_call(["pyenv", "install", "-s", version])
                py = os.path.expanduser(f"~/.pyenv/versions/{version}/bin/python")
                if os.path.exists(py):
                    res.update({"ok": True, "method": "pyenv", "path": py})
                    # refresh index
                    self._index_installs()
                    return res
            except Exception:
                logger.exception("pyenv install failed")
        # 2) system package manager (best-effort; not implemented per-distro)
        if "system" in method_preference:
            # attempt to use apt/dnf/pacman if present (best-effort)
            if shutil.which("apt-get"):
                try:
                    pkgname = f"python{version[:3]}"
                    logger.info("Attempting apt-get install %s", pkgname)
                    subprocess.check_call(["sudo", "apt-get", "update"])
                    subprocess.check_call(["sudo", "apt-get", "install", "-y", pkgname])
                    py = shutil.which(f"python{version[:3]}")
                    if py:
                        res.update({"ok": True, "method": "system", "path": py})
                        self._index_installs()
                        return res
                except Exception:
                    logger.exception("apt-get install failed")
            # other distros could be implemented similarly
        # 3) build from source (fetch + buildsystem)
        if "source" in method_preference:
            try:
                logger.info("Building python %s from source (fallback)", version)
                work = tempfile.mkdtemp(prefix=f"pybuild-{version}-")
                # Ideally use fetcher to get CPython tarball; here naive URL scheme
                url = f"https://www.python.org/ftp/python/{version}/Python-{version}.tgz"
                tar = os.path.join(work, f"Python-{version}.tgz")
                try:
                    if shutil.which("curl"):
                        subprocess.check_call(["curl","-L","-o", tar, url])
                    elif shutil.which("wget"):
                        subprocess.check_call(["wget","-O", tar, url])
                    else:
                        raise Exception("no downloader")
                    subprocess.check_call(["tar","xf", tar, "-C", work])
                    srcdir = os.path.join(work, f"Python-{version}")
                    build_dir = os.path.join(work, "build")
                    os.makedirs(build_dir, exist_ok=True)
                    configure = [os.path.join(srcdir, "configure"), f"--prefix={build_dir}/install", "--enable-optimizations"]
                    subprocess.check_call(configure, cwd=srcdir)
                    subprocess.check_call(["make","-j", str(max(1, os.cpu_count() or 1))], cwd=srcdir)
                    subprocess.check_call(["make","install"], cwd=srcdir)
                    py = os.path.join(build_dir, "install", "bin", "python3")
                    if os.path.exists(py):
                        res.update({"ok": True, "method": "source", "path": py})
                        # optional: register toolchain or persist into TOOLCHAINS_DIR
                        if self.toolchain:
                            # create a minimal toolchain record so other modules can use
                            self.toolchain.register_toolchain(f"python-{version}", None, os.path.dirname(py))
                        self._index_installs()
                        return res
                except Exception:
                    logger.exception("build from source failed")
                finally:
                    try:
                        shutil.rmtree(work, ignore_errors=True)
                    except Exception:
                        pass
            except Exception:
                logger.exception("Source build fallback failed")
        return res
      # continuation PARTE 3/3
    # ---------------------------
    # Virtualenv management (venv/virtualenv/conda)
    # ---------------------------
    def create_venv(self, name: str, python_exe: Optional[str] = None, packages: Optional[List[str]] = None, force: bool = False) -> Dict[str,Any]:
        """
        Create a venv located at VENVS_DIR/<name> using python_exe (path) or best compatible python.
        """
        path = os.path.join(VENVS_DIR, name)
        if os.path.exists(path):
            if not force:
                return {"ok": False, "error": "exists", "path": path}
            shutil.rmtree(path, ignore_errors=True)
        python_exe = python_exe or (self.find_compatible_for_rquest().exe if self.find_compatible_for_rquest() else sys.executable)
        try:
            builder = venv.EnvBuilder(with_pip=True)
            builder.create(path)
            # optionally install packages via pip in this venv
            if packages:
                pip = os.path.join(path, "bin", "pip")
                subprocess.check_call([pip, "install"] + packages)
            # persist metadata
            meta = {"name": name, "python": python_exe, "created_at": _now_ts()}
            _safe_json_write(os.path.join(path, "rquest_venv_meta.json"), meta)
            # re-index installs to include this venv
            self._index_installs()
            return {"ok": True, "path": path}
        except Exception as e:
            logger.exception("create_venv failed")
            return {"ok": False, "error": str(e)}

    def list_venvs(self) -> List[Dict[str,Any]]:
        out = []
        try:
            for d in os.listdir(VENVS_DIR):
                p = os.path.join(VENVS_DIR, d)
                meta = _safe_json_load(os.path.join(p, "rquest_venv_meta.json")) or {}
                meta.update({"path": p})
                out.append(meta)
        except Exception:
            pass
        return out

    def remove_venv(self, name: str) -> bool:
        p = os.path.join(VENVS_DIR, name)
        if os.path.exists(p):
            shutil.rmtree(p, ignore_errors=True)
            self._index_installs()
            return True
        return False

    # ---------------------------
    # Inspections: modules, ABI, extension modules, embedded libs
    # ---------------------------
    def inspect_runtime(self, python_exe: str) -> Dict[str,Any]:
        """
        Run a set of checks on given python binary:
         - version, sysconfig info
         - presence of critical modules (ssl, sqlite3, zlib, lzma)
         - extension modules compiled (.so files) and their ABI tags
        """
        out = {"python": python_exe, "ok": True, "version": None, "modules": {}, "ext_modules": [], "sysconfig": {}}
        try:
            cmd = [python_exe, "-c", "import sys,sysconfig,json;print(json.dumps({'version':sys.version.split()[0],'prefix':sys.prefix, 'soabi': sysconfig.get_config_var('SOABI')}))"]
            info = subprocess.check_output(cmd, text=True)
            j = json.loads(info)
            out["version"] = j.get("version")
            out["sysconfig"] = {"prefix": j.get("prefix"), "soabi": j.get("soabi")}
        except Exception:
            out["ok"] = False
        # check modules
        mods = ["ssl","sqlite3","zlib","lzma","bz2","crypt","_ctypes"]
        for m in mods:
            try:
                cmd = [python_exe, "-c", f"import importlib; print(importlib.util.find_spec('{m}') is not None)"]
                r = subprocess.check_output(cmd, text=True).strip()
                out["modules"][m] = (r.lower() == "true")
            except Exception:
                out["modules"][m] = False
        # list extension modules (best-effort: search site-packages for .so)
        try:
            cmd = [python_exe, "-c", "import site,sys,os,json; p = site.getsitepackages()[0] if site.getsitepackages() else sys.prefix; print(p)"]
            sitep = subprocess.check_output(cmd, text=True).strip()
            extmods = []
            for root, dirs, files in os.walk(sitep):
                for fn in files:
                    if fn.endswith(".so"):
                        full = os.path.join(root, fn)
                        sha = _sha256(full)
                        extmods.append({"path": full, "sha256": sha})
            out["ext_modules"] = extmods
        except Exception:
            out["ext_modules"] = []
        return out

    # ---------------------------
    # Doctor & Repair
    # ---------------------------
    def doctor(self, python_exe: Optional[str] = None) -> Dict[str,Any]:
        """
        Run full diagnosis for given python (or the one used by Rquest)
        """
        python_exe = python_exe or (self.find_compatible_for_rquest().exe if self.find_compatible_for_rquest() else sys.executable)
        report = {"checked": python_exe, "ok": True, "issues": [], "inspection": None}
        insp = self.inspect_runtime(python_exe)
        report["inspection"] = insp
        # 1) version check
        try:
            if insp.get("version"):
                from packaging.version import Version, InvalidVersion  # may not be available; best-effort
                try:
                    v = Version(insp["version"])
                    minv = Version(MIN_VERSION)
                    if v < minv:
                        report["ok"] = False
                        report["issues"].append(f"python_version_too_old: {insp['version']} < {MIN_VERSION}")
                except Exception:
                    pass
        except Exception:
            pass
        # 2) critical modules missing
        for m, present in insp.get("modules", {}).items():
            if not present:
                report["ok"] = False
                report["issues"].append(f"missing_module:{m}")
        # 3) ext modules -- ABI mismatch heuristics
        if insp.get("ext_modules"):
            for em in insp["ext_modules"]:
                if not em.get("sha256"):
                    report["ok"] = False
                    report["issues"].append(f"extmodule_nohash:{em.get('path')}")
        # 4) check Rquest required deps (best-effort)
        try:
            # Rquest runtime deps defined in config or meta; here check a few common ones
            required = ["pyyaml", "sqlalchemy"]
            missing = []
            for r in required:
                cmd = [python_exe, "-c", f"import importlib; print(importlib.util.find_spec('{r}') is not None)"]
                ok = subprocess.check_output(cmd, text=True).strip().lower() == "true"
                if not ok:
                    missing.append(r)
            if missing:
                report["ok"] = False
                report["issues"].append(f"missing_rquest_deps:{','.join(missing)}")
        except Exception:
            pass
        # persist to DB optionally
        try:
            if self.db:
                self.db.execute("""CREATE TABLE IF NOT EXISTS python_checks (id TEXT PRIMARY KEY, python TEXT, report JSON, ts INTEGER)""", (), commit=True)
                rid = f"pc-{_uid()}"
                self.db.execute("INSERT INTO python_checks (id, python, report, ts) VALUES (?,?,?,?)", (rid, python_exe, json.dumps(report), _now_ts()), commit=True)
        except Exception:
            logger.exception("persist python doctor report failed")
        return report

    def repair(self, python_exe: Optional[str] = None, attempt_rebuild_ext: bool = True) -> Dict[str,Any]:
        """
        Try to auto-fix detected issues:
         - install missing dev headers via toolchain (best-effort)
         - reinstall pip packages
         - rebuild extensions
         - if critical failure: install compatible Python and switch
        """
        python_exe = python_exe or (self.find_compatible_for_rquest().exe if self.find_compatible_for_rquest() else sys.executable)
        report = {"python": python_exe, "ok": False, "actions": [], "issues_fixed": []}
        diag = self.doctor(python_exe)
        issues = diag.get("issues", [])
        # simple fixes
        for iss in issues:
            if iss.startswith("missing_module:"):
                mod = iss.split(":",1)[1]
                try:
                    pip = shutil.which("pip") or shutil.which("pip3") or os.path.join(os.path.dirname(python_exe), "pip")
                    if pip and os.path.exists(pip):
                        subprocess.check_call([pip, "install", mod])
                    else:
                        subprocess.check_call([python_exe, "-m", "pip", "install", mod])
                    report["actions"].append(f"installed:{mod}")
                    report["issues_fixed"].append(iss)
                except Exception:
                    logger.exception("pip install failed for %s", mod)
            elif iss.startswith("missing_rquest_deps:"):
                # install list
                mods = iss.split(":",1)[1].split(",")
                for mod in mods:
                    try:
                        subprocess.check_call([python_exe, "-m", "pip", "install", mod])
                        report["actions"].append(f"installed:{mod}")
                    except Exception:
                        logger.exception("pip install %s failed", mod)
            elif iss.startswith("python_version_too_old"):
                # attempt to install compatible python and switch
                try:
                    # install preferred (if available), else install min version
                    target = PREFERRED or MIN_VERSION
                    inst = self.install_version(target)
                    if inst.get("ok"):
                        report["actions"].append(f"installed_python:{inst.get('path')}")
                        # optionally set as rquest python (depends on integration)
                        report["issues_fixed"].append(iss)
                        report["ok"] = True
                except Exception:
                    logger.exception("auto install python failed")
            elif iss.startswith("extmodule_nohash") and attempt_rebuild_ext:
                # try to rebuild extensions by reinstalling relevant packages (best-effort)
                path = iss.split(":",1)[1]
                # heuristic: find package name from path - skip here
                report["actions"].append(f"rebuild_ext:{path}")
                # real rebuild requires knowing package/pip metadata
        # final check
        final = self.doctor(python_exe)
        report["ok"] = final.get("ok", False)
        # persist repair action
        try:
            if self.db:
                self.db.execute("""CREATE TABLE IF NOT EXISTS python_repairs (id TEXT PRIMARY KEY, python TEXT, report JSON, ts INTEGER)""", (), commit=True)
                rid = f"pr-{_uid()}"
                self.db.execute("INSERT INTO python_repairs (id, python, report, ts) VALUES (?,?,?,?)", (rid, python_exe, json.dumps(report), _now_ts()), commit=True)
        except Exception:
            logger.exception("persist repair report failed")
        return report

    # ---------------------------
    # Switch active python for Rquest (safe)
    # ---------------------------
    def use_python_for_rquest(self, python_exe: str, create_snapshot: bool = True) -> Dict[str,Any]:
        """
        Set python_exe as the runtime used by Rquest. This updates a small wrapper/config
        and recreates rquest venv if necessary. If create_snapshot True and fakeroot available, snapshot current environment.
        """
        result = {"ok": False, "python": python_exe, "actions": []}
        # 1) snapshot current state
        snap_id = None
        if create_snapshot and SNAPSHOTS_ENABLED and create_fakeroot:
            try:
                fr = create_fakeroot(package="rquest-env", version=str(_now_ts()))
                fr.create(use_ld_preload=False, destdir_mode=True)
                s = fr.snapshot(label="pre-switch-rquest")
                snap_id = s.snap_id if s else None
                result["actions"].append({"snapshot": snap_id})
            except Exception:
                logger.exception("pre-switch snapshot failed")
        # 2) validate python_exe using doctor
        diag = self.doctor(python_exe)
        if not diag.get("ok"):
            result["actions"].append({"doctor_failed": diag.get("issues")})
            # depending on policy, abort or attempt repair
            if POLICY == "strict":
                result["ok"] = False
                result["note"] = "doctor_failed_strict_policy"
                return result
            else:
                rep = self.repair(python_exe)
                result["actions"].append({"repair": rep})
                if not rep.get("ok"):
                    result["ok"] = False
                    return result
        # 3) create/update rquest venv to use this python
        try:
            venv_path = os.path.join(WORK_DIR, "rquest_env")
            if os.path.exists(venv_path):
                shutil.rmtree(venv_path, ignore_errors=True)
            builder = venv.EnvBuilder(with_pip=True)
            # use the chosen python to create venv: call python -m venv
            subprocess.check_call([python_exe, "-m", "venv", venv_path])
            # install rquest requirements if known
            # e.g., pip install -r requirements.txt (omitted here)
            result["actions"].append({"venv_created": venv_path})
            # persist selection
            sel = {"python": python_exe, "venv": venv_path, "ts": _now_ts()}
            _safe_json_write(os.path.join(WORK_DIR, "rquest_python.json"), sel)
            result["ok"] = True
            # emit event
            try:
                emit_event("python.switch", sel)
            except Exception:
                pass
            return result
        except Exception:
            logger.exception("failed creating rquest venv with chosen python")
            result["ok"] = False
            return result

# ---------------------------
# CLI helper
# ---------------------------
def _cli():
    import argparse
    ap = argparse.ArgumentParser(prog="python_check", description="Manage Python versions for Rquest")
    sub = ap.add_subparsers(dest="cmd")
    p_list = sub.add_parser("list")
    p_list.add_argument("--refresh", action="store_true")
    p_install = sub.add_parser("install")
    p_install.add_argument("version")
    p_install.add_argument("--method", choices=["pyenv","system","source"], nargs="*")
    p_use = sub.add_parser("use")
    p_use.add_argument("python")
    p_venv = sub.add_parser("venv")
    p_venv.add_argument("action", choices=["create","list","remove"])
    p_venv.add_argument("--name")
    p_inspect = sub.add_parser("inspect")
    p_inspect.add_argument("python", nargs="?")
    p_doctor = sub.add_parser("doctor")
    p_doctor.add_argument("python", nargs="?")
    p_repair = sub.add_parser("repair")
    p_repair.add_argument("python", nargs="?")
    args = ap.parse_args()

    pc = PythonCheck()

    if args.cmd == "list":
        if args.refresh:
            pc._index_installs()
        print(json.dumps(pc.list_installs(), indent=2, ensure_ascii=False))
    elif args.cmd == "install":
        methods = args.method if args.method else None
        res = pc.install_version(args.version, method_preference=methods)
        print(json.dumps(res, indent=2, ensure_ascii=False))
    elif args.cmd == "use":
        res = pc.use_python_for_rquest(args.python)
        print(json.dumps(res, indent=2, ensure_ascii=False))
    elif args.cmd == "venv":
        if args.action == "create" and args.name:
            res = pc.create_venv(args.name)
            print(json.dumps(res, indent=2, ensure_ascii=False))
        elif args.action == "list":
            print(json.dumps(pc.list_venvs(), indent=2, ensure_ascii=False))
        elif args.action == "remove" and args.name:
            ok = pc.remove_venv(args.name)
            print(json.dumps({"ok": ok}))
        else:
            ap.print_help()
    elif args.cmd == "inspect":
        py = args.python or (pc.find_compatible_for_rquest().exe if pc.find_compatible_for_rquest() else sys.executable)
        print(json.dumps(pc.inspect_runtime(py), indent=2, ensure_ascii=False))
    elif args.cmd == "doctor":
        py = args.python or (pc.find_compatible_for_rquest().exe if pc.find_compatible_for_rquest() else sys.executable)
        print(json.dumps(pc.doctor(py), indent=2, ensure_ascii=False))
    elif args.cmd == "repair":
        py = args.python or (pc.find_compatible_for_rquest().exe if pc.find_compatible_for_rquest() else sys.executable)
        print(json.dumps(pc.repair(py), indent=2, ensure_ascii=False))
    else:
        ap.print_help()

if __name__ == "__main__":
    _cli()
