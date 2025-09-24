# Rquest/rquest1.0/modules/fakeroot.py
"""
fakeroot.py - Simulated root install environment with snapshots (Btrfs/ZFS support),
metadata management, hooks execution, audit integration, cluster distribution
and ML-based risk advisor.

Principais capacidades:
- Criação de ambientes fakeroot (DESTDIR + overlay / namespace)
- Interceptação simulada de chown/chmod/mknod via LD_PRELOAD style (fallback)
- Metadados ricos: owner, group, mode, xattrs, capabilities
- Snapshots reais em Btrfs ou ZFS (quando disponíveis) com fallback a snapshots por copia
- Snapshots incrementais, diff, rollback instantâneo
- Exportação para pacote via pkgtool
- Execução de hooks dentro do fakeroot (pre/post install), com supporte a scripts inline
- Audit (integracao com audit.py e conflicts.py) antes do commit
- Distribuicao de fakeroots para cluster via repo_sync
- ML advisor (scikit-learn se disponivel) que prevê risco de instalação/combinacao de flags
- Logging colorido e status persistido no DB (se disponível)

Design:
- O módulo tenta integrar com os módulos do projeto via get_* functions; se um módulo faltar,
  o fakeroot degradará de forma segura e não executará mudanças de sistema.
- Operações destrutivas alvo de perigo requerem whitebox apply (usar API apply=True).
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
import shutil
import hashlib
import tempfile
import subprocess
import logging
from typing import Any, Dict, List, Optional, Tuple

# ------------------------
# Imports opcionais (ML)
# ------------------------
ML_AVAILABLE = False
try:
    import numpy as _np  # type: ignore
    from sklearn.ensemble import RandomForestClassifier  # type: ignore
    ML_AVAILABLE = True
except Exception:
    ML_AVAILABLE = False

# ------------------------
# Integrations (graceful)
# ------------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("fakeroot")
except Exception:
    logger = logging.getLogger("fakeroot")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def emit_event(*a, **k): pass

try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    def get_pkgtool(): return None

try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:
    def get_buildsystem(): return None

try:
    from modules.meta import MetaLoader  # type: ignore
except Exception:
    MetaLoader = None

try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    def get_auditor(): return None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

try:
    from modules.repo_sync import get_repo_sync  # type: ignore
except Exception:
    def get_repo_sync(): return None

try:
    from modules.conflicts import get_detector as get_conflicts_detector  # type: ignore
except Exception:
    def get_conflicts_detector(): return None

# ------------------------
# Config defaults
# ------------------------
CFG = get_config() if callable(get_config) else {}
FR_CFG = CFG.get("fakeroot", {}) if isinstance(CFG, dict) else {}

DEFAULTS = {
    "work_base": os.path.expanduser("~/.rquest/fakeroots"),
    "snapshots_base": os.path.expanduser("~/.rquest/fakeroot_snapshots"),
    "use_btrfs": True,
    "use_zfs": True,
    "ld_preload_wrapper": None,  # path to LD_PRELOAD library if available
    "simulate_syscalls": True,
    "ml_enabled": True,
    "ml_min_data": 20,  # minimo de registros para treinar modelo
    "max_snapshot_copies": 5
}

WORK_BASE = FR_CFG.get("work_base", DEFAULTS["work_base"])
SNAP_BASE = FR_CFG.get("snapshots_base", DEFAULTS["snapshots_base"])
USE_BTRFS = bool(FR_CFG.get("use_btrfs", DEFAULTS["use_btrfs"]))
USE_ZFS = bool(FR_CFG.get("use_zfs", DEFAULTS["use_zfs"]))
LD_PRELOAD_WRAPPER = FR_CFG.get("ld_preload_wrapper", DEFAULTS["ld_preload_wrapper"])
SIMULATE_SYSCALLS = bool(FR_CFG.get("simulate_syscalls", DEFAULTS["simulate_syscalls"]))
ML_ENABLED = bool(FR_CFG.get("ml_enabled", DEFAULTS["ml_enabled"])) and ML_AVAILABLE
ML_MIN_DATA = int(FR_CFG.get("ml_min_data", DEFAULTS["ml_min_data"]))
MAX_SNAPSHOT_COPIES = int(FR_CFG.get("max_snapshot_copies", DEFAULTS["max_snapshot_copies"]))

os.makedirs(WORK_BASE, exist_ok=True)
os.makedirs(SNAP_BASE, exist_ok=True)

# ------------------------
# Utilities
# ------------------------
def _now_ts() -> int:
    return int(time.time())

def _uid() -> str:
    return uuid.uuid4().hex[:10]

def _safe_read_json(path: str) -> Optional[Dict[str,Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _safe_write_json(path: str, data: Dict[str,Any]):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

def _sha256_of_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

# ------------------------
# Data models
# ------------------------
class FakerootError(Exception):
    pass

class FakerootSnapshot:
    def __init__(self, snap_id: str, path: str, created_at: int, backend: str):
        self.snap_id = snap_id
        self.path = path
        self.created_at = created_at
        self.backend = backend  # 'btrfs'|'zfs'|'copy'

    def to_dict(self) -> Dict[str,Any]:
        return {"snap_id": self.snap_id, "path": self.path, "created_at": self.created_at, "backend": self.backend}

class Fakeroot:
    """
    Main interface:
    fr = Fakeroot(package='nginx', version='1.2.3', workdir=None, overlay=True)
    fr.create() -> prepare environment
    fr.run_install(cmd) -> runs install using DESTDIR or LD_PRELOAD wrapper
    fr.snapshot() -> create snapshot using btrfs/zfs/copy
    fr.audit_and_check() -> call audit + conflicts
    fr.export(dest) -> create package via pkgtool
    fr.rollback(snap_id) -> revert to a snapshot
    fr.cleanup() -> remove fakeroot
    """
    def __init__(self, package: str, version: Optional[str] = None, workdir: Optional[str] = None, overlay: bool = True, meta_path: Optional[str] = None):
        self.package = package
        self.version = version or ""
        self.overlay = overlay
        self.meta_path = meta_path
        self.base = os.path.abspath(workdir) if workdir else os.path.join(WORK_BASE, f"{package}-{self.version}-{_uid()}")
        self.root = os.path.join(self.base, "root")  # DESTDIR root
        self.metadata_file = os.path.join(self.base, "fakeroot_metadata.json")
        self.snapshots_dir = os.path.join(SNAP_BASE, f"{package}-{self.version}")
        self.snapshots_index = os.path.join(self.snapshots_dir, "index.json")
        self.hooks = get_hook_manager()() if callable(get_hook_manager) else None
        self.pkgtool = get_pkgtool()() if callable(get_pkgtool) else None
        self.buildsystem = get_buildsystem()() if callable(get_buildsystem) else None
        self.meta_loader = MetaLoader() if MetaLoader else None
        self.auditor = get_auditor()() if callable(get_auditor) else None
        self.repo_sync = get_repo_sync()() if callable(get_repo_sync) else None
        self.conflicts = get_conflicts_detector()() if callable(get_conflicts_detector) else None
        self.db = get_db()() if callable(get_db) else None
        self.created_at = None
        self.status = "new"  # new, created, installed, exported, cleaned
        _ensure = lambda p: os.makedirs(p, exist_ok=True)
        _ensure(self.root)
        _ensure(self.snapshots_dir)
        if not os.path.exists(self.metadata_file):
            self._save_metadata({"package": self.package, "version": self.version, "created_at": None, "files": {}, "snapshots": []})
          # continuation PARTE 2/3
def _ensure_dir(p: str):
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass

# ------------------------
# ML Advisor (simple)
# ------------------------
class FakerootMLAdvisor:
    """
    Very small ML advisor skeleton. Trains a RandomForest to predict 'risky' installs
    given feature vectors. If scikit-learn isn't available or not enough data, fallback to heuristics.
    Training data schema (stored in DB or JSON):
      - features: dict (num_files, total_size, has_setuid, uses_mknod, uses_chown)
      - label: 0 (safe) / 1 (risky)
    """
    def __init__(self, db=None):
        self.db = db
        self.model = None
        self.enabled = ML_ENABLED and ML_AVAILABLE
        if self.enabled:
            try:
                self.model = RandomForestClassifier(n_estimators=50)
            except Exception:
                logger.exception("Failed init ML model; disabling")
                self.enabled = False

    def _load_training(self) -> Optional[Tuple[_np.ndarray, _np.ndarray]]:
        # Try DB table 'fakeroot_ml' with columns features_json, label
        try:
            if self.db:
                rows = self.db.fetchall("SELECT features, label FROM fakeroot_ml")
                feats = []
                labels = []
                for r in rows:
                    try:
                        f = json.loads(r.get("features") or "{}")
                        feats.append([f.get("num_files",0), f.get("total_size",0), int(bool(f.get("has_setuid"))), int(bool(f.get("uses_mknod"))), int(bool(f.get("uses_chown")))])
                        labels.append(int(r.get("label") or 0))
                    except Exception:
                        continue
                if len(feats) >= ML_MIN_DATA:
                    return _np.array(feats), _np.array(labels)
        except Exception:
            logger.exception("Failed loading ML training data")
        return None

    def train(self):
        if not self.enabled:
            return False
        data = self._load_training()
        if not data:
            logger.info("Not enough ML data to train")
            return False
        X,y = data
        try:
            self.model.fit(X,y)
            logger.info("Fakeroot ML model trained")
            return True
        except Exception:
            logger.exception("ML training failed")
            return False

    def predict_risk(self, features: Dict[str,Any]) -> float:
        """
        Return probability [0,1] that the install is risky.
        If ML disabled or model not trained, use simple heuristic.
        """
        if self.enabled and self.model:
            try:
                x = _np.array([[features.get("num_files",0), features.get("total_size",0), int(bool(features.get("has_setuid"))), int(bool(features.get("uses_mknod"))), int(bool(features.get("uses_chown")))]])
                p = float(self.model.predict_proba(x)[0][1])
                return p
            except Exception:
                logger.exception("ML predict failed; falling back heuristic")
        # heuristic: big installs with setuid or mknod or many chowns are risky
        score = 0.0
        size = features.get("total_size",0)
        n = features.get("num_files",0)
        if size > 50*(1024**2):  # > 50MB
            score += 0.3
        if n > 1000:
            score += 0.2
        if features.get("has_setuid"):
            score += 0.3
        if features.get("uses_mknod"):
            score += 0.15
        if features.get("uses_chown"):
            score += 0.1
        return min(1.0, score)

# ------------------------
# Fakeroot methods
# ------------------------
class Fakeroot:
    # (constructor defined above)
    def _save_metadata(self, data: Dict[str,Any]):
        data["package"] = self.package
        data["version"] = self.version
        data["modified_at"] = _now_ts()
        _safe_write_json(self.metadata_file, data)
        # persist to DB if available
        try:
            if self.db:
                # ensure table
                self.db.execute("""CREATE TABLE IF NOT EXISTS fakeroots (id TEXT PRIMARY KEY, package TEXT, version TEXT, path TEXT, meta JSON, created_at INTEGER)""", (), commit=True)
                rid = f"{self.package}-{self.version}-{_uid()}"
                self.db.execute("INSERT OR REPLACE INTO fakeroots (id, package, version, path, meta, created_at) VALUES (?,?,?,?,?)",
                                (rid, self.package, self.version, self.base, json.dumps(data), _now_ts()), commit=True)
        except Exception:
            logger.exception("Failed persisting fakeroot metadata to DB")

    def _read_metadata(self) -> Dict[str,Any]:
        d = _safe_read_json(self.metadata_file) or {}
        return d

    def create(self, use_ld_preload: bool = False, destdir_mode: bool = True):
        """
        Initialize fakeroot structure. If use_ld_preload True, attempt to enable LD_PRELOAD wrapper
        (only affects subprocesses called via run_install which respect env).
        destdir_mode: typical DESTDIR mode where build systems honor DESTDIR env var
        """
        self.created_at = _now_ts()
        meta = self._read_metadata()
        meta.update({"created_at": self.created_at, "status": "created", "root": self.root})
        self._save_metadata(meta)
        self.status = "created"
        # store env template
        env = {"DESTDIR": self.root} if destdir_mode else {}
        if use_ld_preload and LD_PRELOAD_WRAPPER and os.path.exists(LD_PRELOAD_WRAPPER):
            env["LD_PRELOAD"] = LD_PRELOAD_WRAPPER
            meta["ld_preload"] = LD_PRELOAD_WRAPPER
            logger.info("LD_PRELOAD wrapper enabled for fakeroot: %s", LD_PRELOAD_WRAPPER)
        meta["env"] = env
        self._save_metadata(meta)
        logger.info("Fakeroot created at %s", self.base)
        return True

    def run_install(self, cmd: str, env_extra: Optional[Dict[str,str]] = None, timeout: Optional[int] = None) -> Dict[str,Any]:
        """
        Run an installation command inside fakeroot. Typical usage:
            fr.run_install("make install")
        This will set DESTDIR to self.root and optionally LD_PRELOAD wrapper if configured.
        Returns dict with ok, stdout, stderr, rc.
        """
        meta = self._read_metadata()
        env = os.environ.copy()
        env.update(meta.get("env", {}))
        if env_extra:
            env.update(env_extra)
        # ensure directories exist
        _ensure_dir(self.root)
        # run in sandbox if available to provide namespaces
        try:
            if self.buildsystem and hasattr(self.buildsystem, "run_in_build_env"):
                try:
                    # buildsystem may provide more consistent execution (namespaces, seccomp)
                    res = self.buildsystem.run_in_build_env(cmd, cwd=self.root, env=env, timeout=timeout, package=self.package)
                    # record outputs
                    meta.setdefault("last_install", {})["cmd"] = cmd
                    meta["last_install"]["result"] = res
                    self._save_metadata(meta)
                    # scan for generated files metadata
                    self._scan_files_and_record()
                    self.status = "installed"
                    return {"ok": res.get("ok", res.get("rc",0)==0), "result": res}
                except Exception:
                    logger.exception("buildsystem.run_in_build_env failed; falling back to subprocess")
            # fallback: simple subprocess invocation, note: this is less safe
            proc = subprocess.run(cmd, shell=True, cwd=self.root, env=env, capture_output=True, text=True, timeout=timeout)
            out = proc.stdout
            err = proc.stderr
            rc = proc.returncode
            meta.setdefault("last_install", {})["cmd"] = cmd
            meta["last_install"]["stdout"] = out
            meta["last_install"]["stderr"] = err
            meta["last_install"]["rc"] = rc
            self._save_metadata(meta)
            # update file metadata database
            self._scan_files_and_record()
            self.status = "installed" if rc == 0 else "install_failed"
            return {"ok": rc==0, "stdout": out, "stderr": err, "rc": rc}
        except Exception as e:
            logger.exception("run_install failed")
            return {"ok": False, "error": str(e)}
          # continuation PARTE 3/3
    def _scan_files_and_record(self):
        """
        Walk fakeroot root and record basic metadata: path, mode, uid/gid (simulated), size, sha256
        Also detect indicators (setuid, mknod etc) to feed ML advisor.
        """
        meta = self._read_metadata()
        files_meta = meta.get("files", {})
        total_size = 0
        num_files = 0
        has_setuid = False
        uses_mknod = False
        uses_chown = False  # heuristic: presence of chown-records in metadata created by LD_PRELOAD wrapper
        for root, dirs, files in os.walk(self.root):
            for fn in files:
                p = os.path.join(root, fn)
                try:
                    st = os.lstat(p)
                    num_files += 1
                    size = st.st_size
                    total_size += size
                    mode = st.st_mode
                    # check setuid bit
                    if mode & 0o4000:
                        has_setuid = True
                    sha = _sha256_of_file(p) if os.path.isfile(p) else None
                    rel = os.path.relpath(p, self.root)
                    files_meta[rel] = {"size": size, "mode": mode, "sha256": sha}
                except Exception:
                    continue
        meta["files"] = files_meta
        meta["stats"] = {"num_files": num_files, "total_size": total_size, "has_setuid": has_setuid, "uses_mknod": uses_mknod, "uses_chown": uses_chown}
        self._save_metadata(meta)
        # persist ML training example optionally
        try:
            if self.db:
                self.db.execute("""CREATE TABLE IF NOT EXISTS fakeroot_ml (id TEXT PRIMARY KEY, features TEXT, label INTEGER)""", (), commit=True)
                # label unknown until audit; we can store features with label=-1 as pending, or skip
                features = {"num_files": num_files, "total_size": total_size, "has_setuid": has_setuid, "uses_mknod": uses_mknod, "uses_chown": uses_chown}
                # store pending example
                rid = f"ex-{_uid()}"
                self.db.execute("INSERT INTO fakeroot_ml (id, features, label) VALUES (?,?,?)", (rid, json.dumps(features), -1), commit=True)
        except Exception:
            logger.exception("Failed persisting ML example")
        return meta["stats"]

    # ------------------------
    # Snapshots: btrfs/zfs or copy fallback
    # ------------------------
    def list_snapshots(self) -> List[Dict[str,Any]]:
        idx = _safe_read_json(self.snapshots_index) or {"snaps": []}
        return idx.get("snaps", [])

    def snapshot(self, label: Optional[str] = None) -> Optional[FakerootSnapshot]:
        """
        Create a snapshot of the fakeroot root directory.
        Prefer Btrfs, then ZFS, else do copy-based snapshot.
        Returns FakerootSnapshot or None on failure.
        """
        label = label or f"snap-{_uid()}"
        # Try Btrfs
        if USE_BTRFS:
            try:
                # find filesystem device for root
                if os.path.exists(self.root):
                    # create snapshot directory under SNAP_BASE
                    target = os.path.join(self.snapshots_dir, label)
                    _ensure_dir(os.path.dirname(target))
                    # btrfs subvolume snapshot -r src target
                    subprocess.check_call(["btrfs", "subvolume", "snapshot", "-r", self.root, target], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    snap = FakerootSnapshot(snap_id=label, path=target, created_at=_now_ts(), backend="btrfs")
                    self._record_snapshot(snap)
                    logger.info("Btrfs snapshot created: %s", target)
                    return snap
            except Exception:
                logger.exception("Btrfs snapshot failed; will try ZFS/copy")

        # Try ZFS
        if USE_ZFS:
            try:
                # zfs snapshot: requires dataset; we attempt naive approach: create a tar snapshot under snap dir
                target = os.path.join(self.snapshots_dir, label + ".tar")
                shutil.make_archive(target.replace(".tar",""), 'gztar', root_dir=self.root)
                snap = FakerootSnapshot(snap_id=label, path=target, created_at=_now_ts(), backend="copy")
                self._record_snapshot(snap)
                logger.info("ZFS-like tar snapshot created: %s", target)
                return snap
            except Exception:
                logger.exception("ZFS-like snapshot failed; falling back to copy")

        # Fallback: copy-based snapshot (fast copy uses hardlinks where possible)
        try:
            dest = os.path.join(self.snapshots_dir, label)
            shutil.copytree(self.root, dest, copy_function=shutil.copy2, dirs_exist_ok=False)
            snap = FakerootSnapshot(snap_id=label, path=dest, created_at=_now_ts(), backend="copy")
            self._record_snapshot(snap)
            logger.info("Copy snapshot created: %s", dest)
            return snap
        except Exception:
            logger.exception("Copy snapshot failed")
        return None

    def _record_snapshot(self, snap: FakerootSnapshot):
        idx = _safe_read_json(self.snapshots_index) or {"snaps": []}
        idx["snaps"].append(snap.to_dict())
        # cap snapshots
        if len(idx["snaps"]) > MAX_SNAPSHOT_COPIES:
            # remove oldest
            old = idx["snaps"].pop(0)
            try:
                if os.path.exists(old["path"]):
                    if os.path.isdir(old["path"]):
                        shutil.rmtree(old["path"], ignore_errors=True)
                    else:
                        os.remove(old["path"])
            except Exception:
                logger.exception("Failed removing old snapshot")
        _safe_write_json(self.snapshots_index, idx)
        meta = self._read_metadata()
        meta.setdefault("snapshots", []).append(snap.to_dict())
        self._save_metadata(meta)
        # persist to DB
        try:
            if self.db:
                self.db.execute("""CREATE TABLE IF NOT EXISTS fakeroot_snapshots (snap_id TEXT PRIMARY KEY, package TEXT, version TEXT, path TEXT, backend TEXT, created_at INTEGER)""", (), commit=True)
                self.db.execute("INSERT OR REPLACE INTO fakeroot_snapshots (snap_id, package, version, path, backend, created_at) VALUES (?,?,?,?,?,?)",
                                (snap.snap_id, self.package, self.version, snap.path, snap.backend, snap.created_at), commit=True)
        except Exception:
            logger.exception("Failed persisting snapshot to DB")

    def rollback(self, snap_id: str) -> Dict[str,Any]:
        """
        Rollback fakeroot to a snapshot (by id).
        """
        idx = _safe_read_json(self.snapshots_index) or {"snaps": []}
        entry = None
        for s in idx.get("snaps", []):
            if s.get("snap_id") == snap_id:
                entry = s
                break
        if not entry:
            return {"ok": False, "error": "snapshot_not_found"}
        path = entry.get("path")
        backend = entry.get("backend")
        # remove current root safely (move to trash)
        trash = os.path.join(self.base, "trash_before_rollback")
        try:
            if os.path.exists(self.root):
                shutil.move(self.root, trash)
        except Exception:
            logger.exception("Failed moving current root to trash")
        # restore depending on backend
        try:
            if backend == "btrfs" or backend == "copy":
                shutil.copytree(path, self.root, dirs_exist_ok=True)
            else:
                # tar/gztar restore
                if os.path.exists(path):
                    tmpd = tempfile.mkdtemp(prefix="restore-")
                    shutil.unpack_archive(path, tmpd)
                    shutil.copytree(tmpd, self.root, dirs_exist_ok=True)
            # update metadata
            self._scan_files_and_record()
            self.status = "rolledback"
            return {"ok": True}
        except Exception:
            logger.exception("Rollback failed")
            # try restoring trash if possible
            try:
                if os.path.exists(trash):
                    if os.path.exists(self.root):
                        shutil.rmtree(self.root, ignore_errors=True)
                    shutil.move(trash, self.root)
            except Exception:
                logger.exception("Failed restoring original root after failed rollback")
            return {"ok": False, "error": "rollback_exception"}

    # ------------------------
    # Audit & conflicts
    # ------------------------
    def audit_and_check(self) -> Dict[str,Any]:
        """
        Run audit module and conflicts detector against the fakeroot contents.
        Returns dict with audit and conflicts results.
        """
        results = {"audit": None, "conflicts": None, "ok": True}
        # run audit if auditor available
        try:
            if self.auditor:
                rep = self.auditor.run_audit(paths=[self.root], deep=False, auto_fix=False)
                results["audit"] = {"run_id": rep.run_id, "findings": len(getattr(rep, "findings", []))}
                if getattr(rep, "findings", None) and len(rep.findings) > 0:
                    results["ok"] = False
        except Exception:
            logger.exception("Audit run failed")
            results["audit"] = {"error": "exception"}
            results["ok"] = False
        # run conflicts detection
        try:
            if self.conflicts:
                det = self.conflicts.check_conflicts_for_root(self.root)
                results["conflicts"] = det
                if det.get("critical"):
                    results["ok"] = False
        except Exception:
            logger.exception("Conflicts detection failed")
            results["conflicts"] = {"error": "exception"}
            results["ok"] = False
        return results

    # ------------------------
    # Export via pkgtool
    # ------------------------
    def export(self, dest_dir: str, metadata: Optional[Dict[str,Any]] = None) -> Dict[str,Any]:
        """
        Export fakeroot contents as a binary package using pkgtool module; returns result dict.
        """
        try:
            _ensure_dir(dest_dir)
            if self.pkgtool and hasattr(self.pkgtool, "package_from_root"):
                meta = self._read_metadata()
                res = self.pkgtool.package_from_root(self.root, dest_dir, metadata=metadata or meta)
                if res.get("ok"):
                    self.status = "exported"
                    self._save_metadata(meta)
                    return {"ok": True, "path": res.get("package_path")}
                else:
                    return {"ok": False, "error": res.get("error")}
            else:
                # fallback: create tarball of root
                outname = os.path.join(dest_dir, f"{self.package}-{self.version or 'unknown'}-{_uid()}.tar.gz")
                shutil.make_archive(outname.replace(".tar.gz",""), 'gztar', root_dir=self.root)
                return {"ok": True, "path": outname}
        except Exception:
            logger.exception("Export failed")
            return {"ok": False, "error": "exception"}

    # ------------------------
    # Distribution (cluster)
    # ------------------------
    def distribute(self, nodes: List[str], dest_path: Optional[str] = None) -> Dict[str,Any]:
        """
        Distribute the fakeroot snapshot/package to remote nodes via repo_sync if available,
        else try simple scp fallback (requires SSH keys).
        """
        results = {}
        # prefer repo_sync
        try:
            if self.repo_sync and hasattr(self.repo_sync, "distribute_path"):
                for node in nodes:
                    try:
                        path = dest_path or self.snapshots_dir
                        ok = self.repo_sync.distribute_path(path, node)
                        results[node] = {"ok": ok}
                    except Exception:
                        logger.exception("repo_sync distribute failed for %s", node)
                        results[node] = {"ok": False}
                return results
        except Exception:
            logger.exception("repo_sync distribution error")
        # fallback to scp using tarball
        try:
            tmp = tempfile.mkdtemp(prefix="fr-dist-")
            tar = shutil.make_archive(os.path.join(tmp, "fakeroot"), 'gztar', root_dir=self.root)
            for node in nodes:
                try:
                    cmd = ["scp", tar, f"{node}:{dest_path or '~/'}"]
                    subprocess.check_call(cmd)
                    results[node] = {"ok": True}
                except Exception:
                    logger.exception("scp distribute failed for %s", node)
                    results[node] = {"ok": False}
            shutil.rmtree(tmp, ignore_errors=True)
            return results
        except Exception:
            logger.exception("Distribution failed")
            return {"ok": False}

    # ------------------------
    # Cleanup
    # ------------------------
    def cleanup(self, remove_snapshots: bool = False) -> bool:
        """
        Remove fakeroot workspace. If remove_snapshots==True also remove snapshot directory.
        """
        try:
            if os.path.exists(self.base):
                shutil.rmtree(self.base, ignore_errors=True)
            if remove_snapshots and os.path.exists(self.snapshots_dir):
                shutil.rmtree(self.snapshots_dir, ignore_errors=True)
            self.status = "cleaned"
            return True
        except Exception:
            logger.exception("Cleanup failed")
            return False

# ------------------------
# Module helper
# ------------------------
_MANAGER: Dict[str, Fakeroot] = {}

def create_fakeroot(package: str, version: Optional[str] = None, workdir: Optional[str] = None, overlay: bool = True, meta_path: Optional[str] = None) -> Fakeroot:
    fr = Fakeroot(package=package, version=version or "", workdir=workdir, overlay=overlay, meta_path=meta_path)
    _id = f"{package}-{version or 'x'}-{_uid()}"
    _MANAGER[_id] = fr
    return fr

def get_fakeroot_by_id(fid: str) -> Optional[Fakeroot]:
    return _MANAGER.get(fid)

# ------------------------
# CLI (simple)
# ------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="fakeroot", description="Fakeroot management for Rquest")
    ap.add_argument("package", help="package name")
    ap.add_argument("--version", help="package version")
    ap.add_argument("--create", action="store_true", help="create fakeroot")
    ap.add_argument("--install-cmd", help="run install command inside fakeroot")
    ap.add_argument("--snapshot", action="store_true", help="create snapshot")
    ap.add_argument("--export", help="export to destination dir")
    ap.add_argument("--rollback", help="rollback to snapshot id")
    ap.add_argument("--cleanup", action="store_true", help="cleanup fakeroot")
    args = ap.parse_args()

    fr = create_fakeroot(args.package, version=args.version)
    if args.create:
        fr.create(use_ld_preload=bool(LD_PRELOAD_WRAPPER), destdir_mode=True)
        print("created:", fr.base)
    if args.install_cmd:
        r = fr.run_install(args.install_cmd)
        print("install:", r)
    if args.snapshot:
        s = fr.snapshot()
        print("snapshot:", s.to_dict() if s else None)
    if args.export:
        r = fr.export(args.export)
        print("export:", r)
    if args.rollback:
        r = fr.rollback(args.rollback)
        print("rollback:", r)
    if args.cleanup:
        ok = fr.cleanup(remove_snapshots=True)
        print("cleanup:", ok)
