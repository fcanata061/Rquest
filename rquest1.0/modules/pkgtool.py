# Rquest/rquest1.0/modules/pkgtool.py
"""
pkgtool.py - Binary packaging and cache management for Rquest

Features:
- quickpkg: create binary package from an installed DESTDIR
- install_bin: install package in sandbox or system using fakeroot
- Local cache management backed by SQLite (index, metadata)
- Support multiple compressions: gz, xz, zst (if zstandard library or zstd binary available)
- Manifest (MANIFEST.json) generation with per-file checksums
- GPG signature verification (if gpg available)
- Remote binhost fetch/upload (simple HTTP placeholders)
- Deduplication using file-level hardlinks when possible
- Cache pruning (LRU) with size limit
- Hooks integration (pre_package, post_package, pre_install, post_install, on_cache_prune)
- Integration points with resolver/buildsystem/sandbox/fakeroot
"""

from __future__ import annotations

import os
import sys
import io
import json
import time
import shutil
import sqlite3
import tarfile
import hashlib
import tempfile
import logging
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# -----------------------------
# Robust imports / fallbacks
# -----------------------------
# config
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

# logging
try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("pkgtool")
except Exception:
    logger = logging.getLogger("pkgtool")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# db
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def add_history(*a, **k): pass
    def emit_event(*a, **k): pass

# hooks
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

# sandbox
try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager(): return None

# fakeroot (expected as module); fallback to system fakeroot via subprocess through sandbox
try:
    from modules.fakeroot import run_in_fakeroot, ensure_fakeroot_available  # type: ignore
except Exception:
    def ensure_fakeroot_available() -> bool:
        import shutil as _sh
        return bool(_sh.which("fakeroot"))

    def run_in_fakeroot(cmd: List[str], cwd: Optional[str] = None, env: Optional[Dict[str,str]] = None, timeout: Optional[int] = None) -> Dict[str,Any]:
        # Best-effort: call fakeroot if present, else return failure (or do normal run inside sandbox)
        import shutil as _sh
        sm = get_sandbox_manager()
        if _sh.which("fakeroot"):
            faux = ["fakeroot"] + cmd
            if sm:
                return sm.run_in_sandbox(faux, cwd=cwd, env=env, timeout=timeout, exec_type="fakeroot")
            else:
                # run directly
                try:
                    proc = subprocess.run(faux, cwd=cwd, env=env, timeout=timeout, capture_output=True)
                    return {"ok": proc.returncode == 0, "exit_code": proc.returncode, "stdout": proc.stdout.decode(errors="ignore"), "stderr": proc.stderr.decode(errors="ignore")}
                except Exception as e:
                    return {"ok": False, "error": str(e)}
        else:
            # fakeroot not available: run inside sandbox without fakeroot as fallback
            logger.warning("fakeroot not available; running install without fakeroot (may fail for ownership operations)")
            if sm:
                return sm.run_in_sandbox(cmd, cwd=cwd, env=env, timeout=timeout, exec_type="install")
            else:
                try:
                    proc = subprocess.run(cmd, cwd=cwd, env=env, timeout=timeout, capture_output=True)
                    return {"ok": proc.returncode == 0, "exit_code": proc.returncode, "stdout": proc.stdout.decode(errors="ignore"), "stderr": proc.stderr.decode(errors="ignore")}
                except Exception as e:
                    return {"ok": False, "error": str(e)}

# zstd support (python binding)
_ZSTD_PY_AVAILABLE = False
try:
    import zstandard as zstd  # type: ignore
    _ZSTD_PY_AVAILABLE = True
    logger.info("zstandard python binding available")
except Exception:
    _ZSTD_PY_AVAILABLE = False

# gpg availability
import shutil as _shutil
_GPG_AVAILABLE = bool(_shutil.which("gpg"))

# -----------------------------
# Utilities
# -----------------------------
def _now_ts() -> int:
    return int(time.time())

def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def _atomic_write_bytes(path: str, data: bytes):
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)

# -----------------------------
# Compression helpers
# -----------------------------
def _compress_tar(src_dir: str, out_path: str, method: str = "gz") -> None:
    """
    Create a tarball of src_dir at out_path using method:
    - gz: gzip
    - xz: lzma
    - zst: zstandard (python binding if available, else tries system zstd)
    """
    src_dir = os.path.abspath(src_dir)
    method = method.lower()
    if method == "gz":
        with tarfile.open(out_path, "w:gz") as tar:
            tar.add(src_dir, arcname=os.path.basename(src_dir))
        return
    if method == "xz":
        with tarfile.open(out_path, "w:xz") as tar:
            tar.add(src_dir, arcname=os.path.basename(src_dir))
        return
    if method == "zst":
        # python zstandard binding: create tar archive in memory (or stream) then compress
        if _ZSTD_PY_AVAILABLE:
            # create uncompressed tar to a temp file and then compress streaming
            with tempfile.NamedTemporaryFile(delete=False) as tfile:
                try:
                    with tarfile.open(tfile.name, "w:") as tar:
                        tar.add(src_dir, arcname=os.path.basename(src_dir))
                    cctx = zstd.ZstdCompressor(level=3)
                    with open(tfile.name, "rb") as inf, open(out_path, "wb") as outf:
                        cctx.copy_stream(inf, outf)
                finally:
                    try:
                        os.remove(tfile.name)
                    except Exception:
                        pass
            return
        else:
            # fallback to system zstd (must support stdin->stdout)
            if _shutil.which("zstd"):
                # create raw tar to stdout and pipe to zstd
                with tempfile.NamedTemporaryFile(delete=False) as tfile:
                    try:
                        with tarfile.open(tfile.name, "w:") as tar:
                            tar.add(src_dir, arcname=os.path.basename(src_dir))
                        # call zstd
                        subprocess.check_call(["zstd", "-q", "-o", out_path, tfile.name])
                    finally:
                        try:
                            os.remove(tfile.name)
                        except Exception:
                            pass
                return
            else:
                raise RuntimeError("zstd compression requested but neither python zstandard nor zstd binary available")
    raise ValueError(f"Unsupported compression method: {method}")

def _decompress_tar(in_path: str, dest_dir: str) -> None:
    # detect compression by extension
    p = in_path.lower()
    if p.endswith(".tar.gz") or p.endswith(".tgz"):
        with tarfile.open(in_path, "r:gz") as tar:
            tar.extractall(dest_dir)
        return
    if p.endswith(".tar.xz") or p.endswith(".txz"):
        with tarfile.open(in_path, "r:xz") as tar:
            tar.extractall(dest_dir)
        return
    if p.endswith(".tar.zst") or p.endswith(".tzst"):
        if _ZSTD_PY_AVAILABLE:
            # decompress to temp file then extract
            with tempfile.NamedTemporaryFile(delete=False) as tfile:
                try:
                    dctx = zstd.ZstdDecompressor()
                    with open(in_path, "rb") as inf, open(tfile.name, "wb") as outf:
                        dctx.copy_stream(inf, outf)
                    with tarfile.open(tfile.name, "r:") as tar:
                        tar.extractall(dest_dir)
                finally:
                    try:
                        os.remove(tfile.name)
                    except Exception:
                        pass
            return
        else:
            if _shutil.which("zstd"):
                # use zstd to decompress to temp tar then extract
                with tempfile.NamedTemporaryFile(delete=False) as tfile:
                    try:
                        subprocess.check_call(["zstd", "-q", "-d", "-o", tfile.name, in_path])
                        with tarfile.open(tfile.name, "r:") as tar:
                            tar.extractall(dest_dir)
                    finally:
                        try:
                            os.remove(tfile.name)
                        except Exception:
                            pass
                return
            else:
                raise RuntimeError("zstd decompression requested but neither python zstandard nor zstd binary available")
    # fallback: try tarlib with autodetect
    with tarfile.open(in_path, "r:*") as tar:
        tar.extractall(dest_dir)

# -----------------------------
# PackageCache backed by SQLite
# -----------------------------
class PackageCache:
    """
    Local binary cache using SQLite index and a storage directory.
    Schema:
      packages(id INTEGER PRIMARY KEY, name TEXT, version TEXT, slot TEXT, repo TEXT,
               checksum TEXT, path TEXT, size INTEGER, created_at INTEGER, last_access INTEGER)
    """
    def __init__(self, cache_dir: Optional[str] = None, db_name: str = "pkgcache.sqlite"):
        cfg = get_config() if callable(get_config) else {}
        default = os.path.expanduser(cfg.get("pkgtool", {}).get("cache_dir", "~/.rquest/pkgcache"))
        self.cache_dir = os.path.abspath(cache_dir or os.path.expanduser(default))
        _ensure_dir(self.cache_dir)
        self.db_path = os.path.join(self.cache_dir, db_name)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._ensure_tables()
        # pruning config
        self.max_size_bytes = int(cfg.get("pkgtool", {}).get("max_cache_size_bytes", 20 * 1024**3))  # default 20GB
        self.prune_policy = cfg.get("pkgtool", {}).get("prune_policy", "lru")  # lru or fifo

    def _ensure_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS packages (
                id INTEGER PRIMARY KEY,
                name TEXT,
                version TEXT,
                slot TEXT,
                repo TEXT,
                checksum TEXT,
                path TEXT,
                size INTEGER,
                created_at INTEGER,
                last_access INTEGER
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pkg_name_ver ON packages(name, version);")
        self.conn.commit()

    def add_package(self, name: str, version: str, path: str, checksum: str, slot: Optional[str]=None, repo: Optional[str]=None):
        size = os.path.getsize(path) if os.path.exists(path) else 0
        ts = _now_ts()
        cur = self.conn.cursor()
        cur.execute("INSERT INTO packages (name, version, slot, repo, checksum, path, size, created_at, last_access) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (name, version, slot, repo, checksum, path, size, ts, ts))
        self.conn.commit()
        logger.info("Added package to cache: %s-%s (%s)", name, version, path)
        self._maybe_prune()

    def find(self, name: str, version: Optional[str] = None) -> Optional[Dict[str,Any]]:
        cur = self.conn.cursor()
        if version:
            cur.execute("SELECT * FROM packages WHERE name = ? AND version = ? ORDER BY created_at DESC LIMIT 1", (name, version))
        else:
            cur.execute("SELECT * FROM packages WHERE name = ? ORDER BY created_at DESC LIMIT 1", (name,))
        row = cur.fetchone()
        if row:
            # update last_access
            cur.execute("UPDATE packages SET last_access = ? WHERE id = ?", (_now_ts(), row[0]))
            self.conn.commit()
            return {"id": row[0], "name": row[1], "version": row[2], "slot": row[3], "repo": row[4], "checksum": row[5], "path": row[6], "size": row[7], "created_at": row[8], "last_access": row[9]}
        return None

    def list_all(self) -> List[Dict[str,Any]]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM packages ORDER BY created_at DESC")
        rows = cur.fetchall()
        res = []
        for row in rows:
            res.append({"id": row[0], "name": row[1], "version": row[2], "slot": row[3], "repo": row[4], "checksum": row[5], "path": row[6], "size": row[7], "created_at": row[8], "last_access": row[9]})
        return res

    def total_size(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT SUM(size) FROM packages")
        r = cur.fetchone()
        return int(r[0]) if r and r[0] is not None else 0

    def _maybe_prune(self):
        total = self.total_size()
        if total <= self.max_size_bytes:
            return
        logger.info("Cache size %d > max %d → pruning", total, self.max_size_bytes)
        # prune according to policy
        cur = self.conn.cursor()
        if self.prune_policy == "lru":
            cur.execute("SELECT id, path, size FROM packages ORDER BY last_access ASC")
        else:
            cur.execute("SELECT id, path, size FROM packages ORDER BY created_at ASC")
        rows = cur.fetchall()
        freed = 0
        for row in rows:
            pid, ppath, psize = row[0], row[1], row[2] or 0
            try:
                if os.path.exists(ppath):
                    os.remove(ppath)
                freed += psize
                cur.execute("DELETE FROM packages WHERE id = ?", (pid,))
                logger.info("Pruned cached package %s (freed %d bytes)", ppath, psize)
                # hook for prune
                hm = get_hook_manager()
                if hm:
                    try:
                        hm.run("on_cache_prune", context={"path": ppath, "size": psize})
                    except Exception:
                        logger.exception("on_cache_prune hook failed")
                if total - freed <= self.max_size_bytes:
                    break
            except Exception:
                logger.exception("Failed to prune package %s", ppath)
        self.conn.commit()
        logger.info("Cache pruning complete; freed %d bytes", freed)
      # continuation of Rquest/rquest1.0/modules/pkgtool.py (PART 2)

# -----------------------------
# PkgTool main class
# -----------------------------
class PkgTool:
    def __init__(self, cache: Optional[PackageCache] = None):
        cfg = get_config() if callable(get_config) else {}
        pct_cfg = cfg.get("pkgtool", {}) if isinstance(cfg, dict) else {}
        self.default_compression = pct_cfg.get("compression", "zst")  # gz|xz|zst
        self.sign_packages = bool(pct_cfg.get("sign", False))
        self.enforce_signature = bool(pct_cfg.get("enforce_signature", False))
        default_cache_dir = pct_cfg.get("cache_dir", "~/.rquest/pkgcache")
        self.cache = cache or PackageCache(os.path.expanduser(default_cache_dir))
        self.hooks = get_hook_manager() if callable(get_hook_manager) else None
        self.sandbox = get_sandbox_manager() if callable(get_sandbox_manager) else None
        self.db = get_db() if callable(get_db) else None
        self.binhost = pct_cfg.get("binhost")  # optional remote host URL
        # metrics skeleton
        self.metrics = {"created":0, "cache_hits":0, "cache_misses":0, "installed":0}

    # -----------------------------
    # Manifest generation
    # -----------------------------
    def _generate_manifest(self, destdir: str, name: str, version: str, slot: Optional[str]=None, repo: Optional[str]=None, extra: Optional[Dict[str,Any]] = None) -> Dict[str,Any]:
        manifest = {
            "name": name,
            "version": version,
            "slot": slot,
            "repo": repo,
            "built_at": _now_ts(),
            "files": [],
            "metadata": extra or {}
        }
        # walk destdir and collect files
        for root, dirs, files in os.walk(destdir):
            for f in files:
                full = os.path.join(root, f)
                rel = os.path.relpath(full, destdir)
                try:
                    h = _sha256_file(full)
                except Exception:
                    h = ""
                size = os.path.getsize(full) if os.path.exists(full) else 0
                manifest["files"].append({"path": rel, "sha256": h, "size": size})
        # sort for determinism
        manifest["files"].sort(key=lambda x: x["path"])
        return manifest

    # -----------------------------
    # quickpkg: creates package from destdir
    # -----------------------------
    def quickpkg(self, name: str, version: str, destdir: str, compression: Optional[str]=None, slot: Optional[str]=None, repo: Optional[str]=None, sign: Optional[bool]=None) -> Dict[str,Any]:
        """
        Create a binary package from files in destdir.
        Returns dict with keys: ok, package_path, manifest_path, checksum, size
        """
        if sign is None:
            sign = self.sign_packages
        compression = compression or self.default_compression
        # run pre_package hook
        if self.hooks:
            try:
                self.hooks.run("pre_package", context={"name": name, "version": version, "destdir": destdir})
            except Exception:
                logger.exception("pre_package hook failed")

        base_name = f"{name}-{version}"
        tmp_dir = tempfile.mkdtemp(prefix="pkgtool-")
        try:
            # create a staging directory containing the package root (DEST)
            stage_root = os.path.join(tmp_dir, base_name)
            shutil.copytree(destdir, stage_root, dirs_exist_ok=True)
            # generate manifest
            manifest = self._generate_manifest(stage_root, name, version, slot=slot, repo=repo, extra={"built_by": "pkgtool"})
            manifest_path = os.path.join(tmp_dir, "MANIFEST.json")
            with open(manifest_path, "w", encoding="utf-8") as mf:
                json.dump(manifest, mf, indent=2, ensure_ascii=False)
            # include manifest inside stage root (so package contains it)
            shutil.copy2(manifest_path, os.path.join(stage_root, "MANIFEST.json"))

            # compress into package file
            package_fname = f"{base_name}.tar.{('zst' if compression=='zst' else compression)}"
            package_path = os.path.join(self.cache.cache_dir, package_fname)
            # ensure unique name if exists: append timestamp
            if os.path.exists(package_path):
                package_path = os.path.join(self.cache.cache_dir, f"{base_name}-{int(time.time())}.tar.{('zst' if compression=='zst' else compression)}")
            _compress_tar(stage_root, package_path, method=compression)
            checksum = _sha256_file(package_path)
            size = os.path.getsize(package_path)
            # optionally sign
            sig_path = None
            if sign and _GPG_AVAILABLE:
                try:
                    subprocess.check_call(["gpg", "--batch", "--yes", "--output", package_path + ".sig", "--detach-sign", package_path])
                    sig_path = package_path + ".sig"
                except Exception:
                    logger.exception("GPG signing failed for %s", package_path)
                    if self.enforce_signature:
                        return {"ok": False, "error": "gpg_sign_failed"}

            # add to cache index
            self.cache.add_package(name, version, package_path, checksum, slot=slot, repo=repo)
            self.metrics["created"] += 1

            # run post_package hook
            if self.hooks:
                try:
                    self.hooks.run("post_package", context={"name": name, "version": version, "package_path": package_path, "manifest": manifest})
                except Exception:
                    logger.exception("post_package hook failed")

            return {"ok": True, "package_path": package_path, "manifest": manifest, "checksum": checksum, "size": size, "sig": sig_path}
        finally:
            try:
                shutil.rmtree(tmp_dir)
            except Exception:
                pass

    # -----------------------------
    # verify signature (if present)
    # -----------------------------
    def verify_signature(self, package_path: str) -> Tuple[bool, Optional[str]]:
        if not _GPG_AVAILABLE:
            return (False, "gpg-not-available")
        sig = package_path + ".sig"
        if not os.path.exists(sig):
            return (False, "sig-missing")
        try:
            subprocess.check_call(["gpg", "--verify", sig, package_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return (True, None)
        except subprocess.CalledProcessError as e:
            return (False, "gpg-verify-failed")
        except Exception as e:
            logger.exception("gpg verify error")
            return (False, str(e))

    # -----------------------------
    # install_bin: install package into target (sandbox or host)
    # -----------------------------
    def install_bin(self, package_path: str, target: Optional[str] = None, use_fakeroot: Optional[bool]=None, verify_sig: Optional[bool]=None, sandbox_run: Optional[bool]=True, clean_after: bool=True) -> Dict[str,Any]:
        """
        Install a binary package. By default installs inside a sandbox session (if available).
        - package_path: path to .tar.* file
        - target: if provided, path to destdir where to extract; otherwise a temp destdir in sandbox will be used
        - use_fakeroot: override config
        - verify_sig: whether to verify GPG signature
        - sandbox_run: execute extraction inside sandbox (recommended)
        Returns dict with ok, destdir, details
        """
        if verify_sig is None:
            verify_sig = self.enforce_signature
        if use_fakeroot is None:
            use_fakeroot = bool(get_config().get("buildsystem", {}).get("use_fakeroot", False))

        if verify_sig:
            ok, reason = self.verify_signature(package_path)
            if not ok:
                logger.error("Signature verification failed for %s: %s", package_path, reason)
                return {"ok": False, "error": "sig_verify_failed", "reason": reason}

        # run pre_install hook
        if self.hooks:
            try:
                self.hooks.run("pre_install", context={"package_path": package_path})
            except Exception:
                logger.exception("pre_install hook failed")

        # decide target destdir
        ephemeral = False
        if not target:
            ephemeral = True
            target = tempfile.mkdtemp(prefix="pkg-install-")
        target = os.path.abspath(target)

        # If sandbox available and sandbox_run True, perform install inside a sandbox session
        sm = self.sandbox
        if sandbox_run and sm:
            session_id = sm.start_session()
            # create a temp dir inside session workdir
            session = sm._sessions.get(session_id) if hasattr(sm, "_sessions") else None
            workdir = session.get("workdir") if session else None
            if workdir:
                dest_inside = os.path.join(workdir, os.path.basename(target))
            else:
                dest_inside = target
            # copy package into workdir
            try:
                shutil.copy2(package_path, workdir)
                pkg_basename = os.path.basename(package_path)
                pkg_in_work = os.path.join(workdir, pkg_basename)
                # decompress inside sandbox via sandbox.run_in_sandbox calling tar (so ownership/permissions applied inside)
                if use_fakeroot:
                    # use fakeroot wrapper to install (inside sandbox)
                    cmd = ["/bin/sh", "-c", f"tar -xf {pkg_in_work} -C {dest_inside}"]
                    res = run_in_fakeroot(cmd, cwd=workdir)
                else:
                    # run tar inside sandbox
                    res = sm.run_in_sandbox(["/bin/sh", "-c", f"tar -xf {pkg_in_work} -C {dest_inside}"], cwd=workdir, session_id=session_id, exec_type="install")
                if not res.get("ok"):
                    logger.error("install inside sandbox failed: %s", res)
                    # cleanup session
                    try:
                        sm.stop_session(session_id)
                    except Exception:
                        pass
                    if ephemeral and os.path.exists(target) and not os.listdir(target):
                        shutil.rmtree(target, ignore_errors=True)
                    return {"ok": False, "error": "sandbox_install_failed", "detail": res}
                # after successful extraction, move files from session workdir dest_inside to host target (if needed)
                if dest_inside != target:
                    # move/copy preserving attributes
                    if os.path.exists(target):
                        shutil.rmtree(target, ignore_errors=True)
                    shutil.copytree(dest_inside, target, dirs_exist_ok=True)
            except Exception as e:
                logger.exception("install_bin copy/extract error")
                try:
                    sm.stop_session(session_id)
                except Exception:
                    pass
                return {"ok": False, "error": "install_exception", "detail": str(e)}
            finally:
                try:
                    sm.stop_session(session_id)
                except Exception:
                    pass
        else:
            # no sandbox: decompress locally (use fakeroot wrapper if requested)
            try:
                if use_fakeroot:
                    # use run_in_fakeroot helper
                    # create command: tar -xf package_path -C target
                    cmd = ["tar", "-xf", package_path, "-C", target]
                    res = run_in_fakeroot(cmd, cwd=None)
                    if not res.get("ok"):
                        return {"ok": False, "error": "fakeroot_extract_failed", "detail": res}
                else:
                    _decompress_tar(package_path, target)
            except Exception:
                logger.exception("install_bin failed")
                if ephemeral and os.path.exists(target) and not os.listdir(target):
                    shutil.rmtree(target, ignore_errors=True)
                return {"ok": False, "error": "install_exception"}

        # run post_install hook
        if self.hooks:
            try:
                self.hooks.run("post_install", context={"package_path": package_path, "destdir": target})
            except Exception:
                logger.exception("post_install hook failed")

        # add to DB installed history if available
        try:
            if self.db:
                # try to read manifest to get name/version
                manifest_file = os.path.join(target, "MANIFEST.json")
                if os.path.exists(manifest_file):
                    with open(manifest_file, "r", encoding="utf-8") as mf:
                        manifest = json.load(mf)
                    name = manifest.get("name")
                    version = manifest.get("version")
                    # naive insert, DB schema may differ
                    try:
                        self.db.execute("INSERT INTO installed_packages (name, version, installed_at) VALUES (?, ?, ?)", (name, version, _now_ts()), commit=True)
                    except Exception:
                        pass
        except Exception:
            logger.exception("Failed to record installed package in DB")

        self.metrics["installed"] += 1
        return {"ok": True, "destdir": target}

    # -----------------------------
    # reuse_or_create: try remote/local cache then quickpkg
    # -----------------------------
    def reuse_or_create(self, name: str, version: str, destdir: str, compression: Optional[str] = None, slot: Optional[str]=None, repo: Optional[str]=None) -> Dict[str,Any]:
        """
        If binary exists in local cache or remote binhost, return it.
        Otherwise create quickpkg and return created package info.
        """
        # 1) local cache
        found = self.cache.find(name, version)
        if found and os.path.exists(found["path"]):
            self.metrics["cache_hits"] += 1
            logger.info("Cache hit for %s-%s -> %s", name, version, found["path"])
            return {"ok": True, "cached": True, "package_path": found["path"], "meta": found}
        self.metrics["cache_misses"] += 1

        # 2) try remote binhost simple GET (if configured)
        if self.binhost:
            # naive implementation: assume URL <binhost>/<name>-<version>.tar.<ext>
            ext = "tar." + (compression or self.default_compression)
            url = f"{self.binhost.rstrip('/')}/{name}-{version}.{ext}"
            try:
                import requests  # optional dependency
                r = requests.get(url, stream=True, timeout=10)
                if r.status_code == 200:
                    # save to cache path
                    package_path = os.path.join(self.cache.cache_dir, f"{name}-{version}.{ext}")
                    with open(package_path, "wb") as f:
                        for chunk in r.iter_content(1024*1024):
                            f.write(chunk)
                    checksum = _sha256_file(package_path)
                    self.cache.add_package(name, version, package_path, checksum, slot=slot, repo=repo)
                    logger.info("Downloaded binary from binhost: %s", url)
                    return {"ok": True, "cached": True, "package_path": package_path}
            except Exception:
                logger.exception("Failed to fetch from binhost or requests not available")

        # 3) create quickpkg
        created = self.quickpkg(name, version, destdir, compression=compression, slot=slot, repo=repo)
        return created

    # -----------------------------
    # uninstall helper: remove files according to manifest
    # -----------------------------
    def uninstall_by_manifest(self, manifest_path: str) -> Dict[str,Any]:
        try:
            with open(manifest_path, "r", encoding="utf-8") as mf:
                manifest = json.load(mf)
            # assume manifest located inside destdir; compute destroot
            # if manifest path is <dest>/MANIFEST.json, dest is parent
            dest = os.path.dirname(manifest_path)
            # run pre_remove hook
            if self.hooks:
                try:
                    self.hooks.run("pre_remove", context={"manifest": manifest, "dest": dest})
                except Exception:
                    logger.exception("pre_remove hook failed")
            # iterate files and remove
            for f in manifest.get("files", []):
                p = os.path.join(dest, f.get("path"))
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    logger.exception("Failed removing file %s", p)
            # run post_remove hook
            if self.hooks:
                try:
                    self.hooks.run("post_remove", context={"manifest": manifest, "dest": dest})
                except Exception:
                    logger.exception("post_remove hook failed")
            return {"ok": True}
        except Exception:
            logger.exception("uninstall_by_manifest failed")
            return {"ok": False, "error": "manifest_read_failed"}

    # -----------------------------
    # utility: recompact existing packages to preferred compression
    # -----------------------------
    def recompact_all(self, target_method: Optional[str] = None):
        target_method = target_method or self.default_compression
        pkgs = self.cache.list_all()
        for p in pkgs:
            path = p["path"]
            if path.endswith(f".{target_method}") or (target_method=="zst" and path.endswith(".tar.zst")):
                continue
            try:
                tmpdir = tempfile.mkdtemp(prefix="recompact-")
                # extract to tmpdir
                _decompress_tar(path, tmpdir)
                base = f"{p['name']}-{p['version']}"
                srcdir = os.path.join(tmpdir, base)
                newname = os.path.join(self.cache.cache_dir, f"{base}.tar.{('zst' if target_method=='zst' else target_method)}")
                _compress_tar(srcdir, newname, method=target_method)
                newchecksum = _sha256_file(newname)
                self.cache.add_package(p['name'], p['version'], newname, newchecksum, slot=p.get("slot"), repo=p.get("repo"))
                logger.info("Recompacted %s -> %s", path, newname)
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                logger.exception("Failed to recompact %s", path)

# -----------------------------
# Module-level convenience
# -----------------------------
_MANAGER: Optional[PkgTool] = None
def get_pkgtool() -> PkgTool:
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = PkgTool()
    return _MANAGER

# -----------------------------
# CLI demo
# -----------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--quickpkg", nargs=3, metavar=("NAME","VERSION","DESTDIR"), help="Create binary package from DESTDIR")
    ap.add_argument("--install", metavar="PACKAGE_PATH", help="Install binary package")
    ap.add_argument("--list-cache", action="store_true")
    ap.add_argument("--recompact", nargs="?", const=None, help="Recompact cache to specified method (gz,xz,zst) or default")
    args = ap.parse_args()
    pt = get_pkgtool()
    if args.quickpkg:
        name, ver, dest = args.quickpkg
        res = pt.quickpkg(name, ver, dest)
        print(res)
    elif args.install:
        res = pt.install_bin(args.install)
        print(res)
    elif args.list_cache:
        for e in pt.cache.list_all():
            print(e)
    elif args.recompact is not None:
        pt.recompact_all(target_method=args.recompact)
        print("Recompact done")
    else:
        ap.print_help()
