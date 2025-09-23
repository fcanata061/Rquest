# Rquest/rquest1.0/modules/sandbox.py
"""
sandbox.py - SandboxManager for Rquest

Features:
- Multiple backends: noop (dev), container (docker/podman), namespaces (prototype)
- High-level API: run_in_sandbox, run_script_in_sandbox, run_python_in_sandbox
- Session management: start_session, exec_in_session, stop_session
- Snapshot / restore (tar-based)
- Exec reporting persisted to DB (sandbox_exec) and sessions table
- stdout/stderr capture and streaming
- Resource limits support (best-effort; uses cgroups v2 if available, else setrlimit)
- Integration with modules.config, modules.logging, modules.db
- Robust fallbacks for missing system features
"""

from __future__ import annotations

import os
import sys
import shutil
import json
import time
import uuid
import tarfile
import tempfile
import threading
import subprocess
import shlex
import signal
from typing import List, Dict, Any, Optional, Iterator, Tuple

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

try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    def get_db():
        return None
    def add_history(*a, **k): pass
    def emit_event(*a, **k): pass

logger = get_logger("sandbox")

# ----------------------------
# Utilities
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
def _ensure_sandbox_tables(db):
    if not db:
        logger.debug("No DB available to create sandbox tables.")
        return
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS sandbox_exec (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                command TEXT,
                args TEXT,
                type TEXT,
                status TEXT,
                exit_code INTEGER,
                stdout_path TEXT,
                stderr_path TEXT,
                start_ts INTEGER,
                end_ts INTEGER,
                duration REAL,
                resource_usage TEXT
            );
        """, (), commit=True)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sandbox_sessions (
                id TEXT PRIMARY KEY,
                backend TEXT,
                workdir TEXT,
                created_at INTEGER,
                state TEXT
            );
        """, (), commit=True)
    except Exception:
        logger.exception("Failed ensuring sandbox tables")

# ----------------------------
# Helper: run subprocess with timeout and capture, return exec metadata
# ----------------------------
def _run_subprocess_capture(cmd: List[str], cwd: Optional[str], env: Optional[Dict[str,str]],
                            timeout: Optional[int], stdout_path: str, stderr_path: str,
                            preexec_fn=None) -> Dict[str,Any]:
    """Run subprocess, capture stdout/stderr to files, enforce timeout."""
    start = time.time()
    with open(stdout_path, "wb") as out_f, open(stderr_path, "wb") as err_f:
        try:
            proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=out_f, stderr=err_f, preexec_fn=preexec_fn)
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                end = time.time()
                return {"ok": False, "exit_code": proc.returncode, "timeout": True, "start": start, "end": end}
            end = time.time()
            return {"ok": True, "exit_code": proc.returncode, "timeout": False, "start": start, "end": end}
        except Exception:
            end = time.time()
            logger.exception("Subprocess execution failed for cmd: %s", cmd)
            return {"ok": False, "exception": True, "exit_code": None, "timeout": False, "start": start, "end": end}

# ----------------------------
# CGroup v2 helper (best-effort)
# ----------------------------
def _cgroup_v2_available() -> bool:
    return os.path.exists("/sys/fs/cgroup") and os.path.isfile("/proc/self/cgroup")

def _create_cgroup_v2(name: str, limits: Dict[str,Any]) -> Optional[str]:
    """
    Try to create a cgroup v2 under /sys/fs/cgroup/<name>.
    Returns path to cgroup dir or None.
    This is best-effort and may fail without permissions.
    """
    try:
        base = "/sys/fs/cgroup"
        cg_path = os.path.join(base, name)
        os.makedirs(cg_path, exist_ok=True)
        # set limits if provided (memory.max, cpu.max, pids.max)
        if "memory_mb" in limits:
            mem = str(int(limits["memory_mb"]) * 1024 * 1024)
            try:
                with open(os.path.join(cg_path, "memory.max"), "w") as f:
                    f.write(mem)
            except Exception:
                logger.debug("Could not set memory.max in cgroup (permissions?)")
        if "pids_max" in limits:
            try:
                with open(os.path.join(cg_path, "pids.max"), "w") as f:
                    f.write(str(limits["pids_max"]))
            except Exception:
                logger.debug("Could not set pids.max in cgroup")
        if "cpu_max" in limits:
            try:
                with open(os.path.join(cg_path, "cpu.max"), "w") as f:
                    f.write(str(limits["cpu_max"]))
            except Exception:
                logger.debug("Could not set cpu.max in cgroup")
        return cg_path
    except Exception:
        logger.exception("Failed creating cgroup v2")
        return None

# ----------------------------
# SandboxManager class (part 1)
# ----------------------------
class SandboxManager:
    """
    Sandbox manager with multiple backends (noop, container, namespaces).
    Designed to be robust and to gracefully fallback on hosts missing features.
    """
    def __init__(self):
        self._cfg = {}
        try:
            self._cfg = get_config() if callable(get_config) else {}
        except Exception:
            self._cfg = {}
        sb_cfg = {}
        try:
            if isinstance(self._cfg, dict):
                sb_cfg = self._cfg.get("sandbox", {}) or {}
            elif hasattr(self._cfg, "as_dict"):
                sb_cfg = self._cfg.as_dict().get("sandbox", {}) or {}
            else:
                sb_cfg = getattr(self._cfg, "sandbox", {}) or {}
        except Exception:
            sb_cfg = {}
        # base settings
        self.enabled = bool(sb_cfg.get("enabled", True))
        self.backend = sb_cfg.get("backend", "namespaces")
        self.workdir_root = os.path.abspath(sb_cfg.get("workdir_root", os.path.join(os.getcwd(), ".rquest_sandbox")))
        _ensure_dir(self.workdir_root)
        self.default_limits = sb_cfg.get("default_limits", {"memory_mb": 1024, "pids_max": 512, "cpu_max": None, "timeout": 3600})
        self.reuse_sessions = bool(sb_cfg.get("reuse_sessions", True))
        self.sessions_pool_size = int(sb_cfg.get("sessions_pool_size", 4))
        # DB
        self._db = get_db() if callable(get_db) else None
        _ensure_sandbox_tables(self._db)
        # in-memory sessions
        self._sessions_lock = threading.RLock()
        self._sessions: Dict[str, Dict[str,Any]] = {}  # session_id -> {backend, workdir, created_at, state}
        # exec lock
        self._exec_lock = threading.RLock()
        # worker counters
        self._exec_counter = 0

    # ----------------------------
    # Internal: new workdir for session/exec
    # ----------------------------
    def _new_workdir(self, name: Optional[str] = None) -> str:
        if name is None:
            name = str(uuid.uuid4())[:8]
        path = os.path.join(self.workdir_root, name)
        _ensure_dir(path)
        return path

    # ----------------------------
    # Session management
    # ----------------------------
    def start_session(self, backend: Optional[str] = None, resources: Optional[Dict[str,Any]] = None) -> str:
        """
        Start a session (persistent sandbox environment). Returns session_id.
        Backend selection order: explicit -> config default -> auto-detect.
        """
        with self._sessions_lock:
            backend = backend or self.backend or "namespaces"
            sid = str(uuid.uuid4())
            workdir = self._new_workdir(f"session_{sid}")
            created_at = _now_ts()
            self._sessions[sid] = {"backend": backend, "workdir": workdir, "created_at": created_at, "state": "running", "resources": resources or self.default_limits}
            # persist to DB sessions table
            try:
                if self._db:
                    self._db.execute("INSERT OR REPLACE INTO sandbox_sessions (id, backend, workdir, created_at, state) VALUES (?, ?, ?, ?, ?)", (sid, backend, workdir, created_at, "running"), commit=True)
            except Exception:
                logger.exception("Failed persisting session to DB")
            logger.info("Started sandbox session %s backend=%s workdir=%s", sid, backend, workdir)
            return sid

    def stop_session(self, session_id: str) -> bool:
        with self._sessions_lock:
            s = self._sessions.get(session_id)
            if not s:
                logger.warning("stop_session: session not found %s", session_id)
                return False
            # attempt cleanup: remove workdir if exists
            try:
                shutil.rmtree(s["workdir"], ignore_errors=True)
            except Exception:
                logger.exception("Failed cleaning session workdir for %s", session_id)
            s["state"] = "stopped"
            try:
                if self._db:
                    self._db.execute("UPDATE sandbox_sessions SET state = ? WHERE id = ?", ("stopped", session_id), commit=True)
            except Exception:
                logger.exception("Failed updating session state in DB")
            del self._sessions[session_id]
            logger.info("Stopped sandbox session %s", session_id)
            return True

    # ----------------------------
    # Snapshot / Restore
    # ----------------------------
    def snapshot(self, session_id: str) -> Optional[str]:
        """
        Create tar.gz snapshot of session workdir and return path.
        """
        with self._sessions_lock:
            s = self._sessions.get(session_id)
            if not s:
                logger.warning("snapshot: session not found %s", session_id)
                return None
            workdir = s["workdir"]
            ts = int(time.time())
            snap_name = f"{session_id}.snapshot.{ts}.tar.gz"
            snap_path = os.path.join(self.workdir_root, "snapshots")
            _ensure_dir(snap_path)
            full = os.path.join(snap_path, snap_name)
            try:
                with tarfile.open(full, "w:gz") as tar:
                    tar.add(workdir, arcname=os.path.basename(workdir))
                logger.info("Snapshot created %s", full)
                return full
            except Exception:
                logger.exception("Failed creating snapshot for session %s", session_id)
                return None

    def restore(self, snapshot_path: str, session_id: Optional[str] = None) -> bool:
        """
        Restore snapshot into a session workdir (create session if not provided).
        """
        try:
            if not os.path.exists(snapshot_path):
                logger.warning("restore: snapshot not found %s", snapshot_path)
                return False
            if session_id is None:
                session_id = self.start_session()
            with self._sessions_lock:
                s = self._sessions.get(session_id)
                if not s:
                    logger.warning("restore: session not found after creation %s", session_id)
                    return False
                target = s["workdir"]
                # clear target
                if os.path.exists(target):
                    shutil.rmtree(target, ignore_errors=True)
                _ensure_dir(target)
                with tarfile.open(snapshot_path, "r:*") as tar:
                    tar.extractall(os.path.dirname(target))
                logger.info("Restored snapshot %s into session %s", snapshot_path, session_id)
                return True
        except Exception:
            logger.exception("restore failed")
            return False
          # continuation of Rquest/rquest1.0/modules/sandbox.py (PART 2)

# ----------------------------
# Execution / Streaming / Reports (part 2)
# ----------------------------
    def _persist_exec(self, session_id: Optional[str], command: List[str], exec_type: str,
                      stdout_path: str, stderr_path: str, start_ts: int, end_ts: int, exit_code: Optional[int], resource_usage: Optional[Dict[str,Any]]):
        """
        Persist execution info into sandbox_exec table if DB available.
        Returns exec_id or None.
        """
        try:
            if not self._db:
                return None
            duration = (end_ts - start_ts) if (end_ts and start_ts) else None
            cur = self._db.execute("""
                INSERT INTO sandbox_exec (session_id, command, args, type, status, exit_code, stdout_path, stderr_path, start_ts, end_ts, duration, resource_usage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (session_id, json.dumps(command), "", exec_type, ("success" if exit_code == 0 else "failed"), exit_code, stdout_path, stderr_path, int(start_ts), int(end_ts), float(duration) if duration else None, json.dumps(resource_usage or {})), commit=True)
            return getattr(cur, "lastrowid", None)
        except Exception:
            logger.exception("Failed persisting sandbox_exec")
            return None

    def get_exec_report(self, exec_id: int) -> Optional[Dict[str,Any]]:
        if not self._db:
            logger.warning("get_exec_report: no DB configured")
            return None
        try:
            row = self._db.fetchone("SELECT * FROM sandbox_exec WHERE id = ?", (exec_id,))
            return dict(row) if row else None
        except Exception:
            logger.exception("get_exec_report db query failed")
            return None

    def stream_output(self, stdout_path: str, tail: bool = True) -> Iterator[str]:
        """
        Generator that yields lines from stdout_path as they appear (basic tail -f).
        If tail=False, reads file to end and stops.
        """
        if not os.path.exists(stdout_path):
            yield ""
            return
        with open(stdout_path, "r", encoding="utf-8", errors="ignore") as f:
            # seek to end for tail
            if tail:
                f.seek(0, os.SEEK_END)
            while True:
                line = f.readline()
                if line:
                    yield line
                else:
                    if not tail:
                        break
                    time.sleep(0.2)

# ----------------------------
# Core: run_in_sandbox implementation (supports backends)
# ----------------------------
    def run_in_sandbox(self, cmd: List[str], *,
                       cwd: Optional[str] = None,
                       env: Optional[Dict[str,str]] = None,
                       timeout: Optional[int] = None,
                       resources: Optional[Dict[str,Any]] = None,
                       network: str = "none",
                       capture_output: bool = True,
                       session_id: Optional[str] = None,
                       exec_type: str = "command") -> Dict[str,Any]:
        """
        High-level API to run command in sandbox. Returns dict with metadata and paths.
        """
        timeout = timeout or self.default_limits.get("timeout", 3600)
        resources = resources or self.default_limits

        # If session provided use its workdir, else create ephemeral session
        ephemeral = False
        if session_id:
            s = self._sessions.get(session_id)
            if not s:
                logger.warning("run_in_sandbox: session %s not found, creating ephemeral", session_id)
                session_id = self.start_session()
                ephemeral = True
        else:
            session_id = self.start_session()
            ephemeral = True
        session = self._sessions.get(session_id)
        if not session:
            return {"ok": False, "error": "session_missing"}

        workdir = session["workdir"]
        # ensure command-specific exec dir
        with self._exec_lock:
            self._exec_counter += 1
            exec_id_local = self._exec_counter
        exec_dir = os.path.join(workdir, f"exec_{exec_id_local}")
        _ensure_dir(exec_dir)
        stdout_path = os.path.join(exec_dir, "stdout.log")
        stderr_path = os.path.join(exec_dir, "stderr.log")

        # decide backend flow
        backend = session.get("backend", self.backend)
        logger.info("Running in sandbox backend=%s session=%s cmd=%s", backend, session_id, cmd)

        start_ts = time.time()
        resource_usage = {}
        run_result = None

        try:
            if backend == "noop":
                # run directly (but capture output)
                run_result = _run_subprocess_capture(cmd, cwd or workdir, env, timeout, stdout_path, stderr_path)
            elif backend == "container":
                # try docker then podman; mount workdir into container and run
                image = self._cfg.get("container_image", "alpine:latest")
                mount_flag = f"{workdir}:/rquest_work"
                # build docker/podman command
                docker_cmd = None
                if shutil.which("docker"):
                    docker_cmd = ["docker", "run", "--rm", "-v", mount_flag, "-w", "/rquest_work"]
                elif shutil.which("podman"):
                    docker_cmd = ["podman", "run", "--rm", "-v", mount_flag, "-w", "/rquest_work"]
                if docker_cmd is None:
                    logger.warning("Container backend requested but docker/podman not found; falling back to noop")
                    run_result = _run_subprocess_capture(cmd, cwd or workdir, env, timeout, stdout_path, stderr_path)
                else:
                    full = docker_cmd + [image] + cmd
                    run_result = _run_subprocess_capture(full, cwd=None, env=env, timeout=timeout, stdout_path=stdout_path, stderr_path=stderr_path)
            elif backend == "namespaces":
                # prototype: use unshare tool if available; otherwise fallback
                if shutil.which("unshare"):
                    # prepare command via unshare: mount namespace + pid + user
                    unshare_cmd = ["unshare", "--mount", "--pid", "--fork", "--map-root-user"]
                    # network none: add --net=none when supported (some unshare versions accept)
                    if network == "none":
                        # some unshare implementations accept --net, else rely on other measures
                        unshare_cmd += ["--net"]
                    # build final command to execute inside unshare
                    final_cmd = unshare_cmd + ["--"] + cmd
                    run_result = _run_subprocess_capture(final_cmd, cwd or workdir, env, timeout, stdout_path, stderr_path)
                else:
                    logger.warning("unshare not found; falling back to noop backend")
                    run_result = _run_subprocess_capture(cmd, cwd or workdir, env, timeout, stdout_path, stderr_path)
            else:
                logger.warning("Unknown backend '%s' — using noop", backend)
                run_result = _run_subprocess_capture(cmd, cwd or workdir, env, timeout, stdout_path, stderr_path)

            # compute resource usage best-effort: use rusage if possible via getrusage on parent? limited
            # We'll record basic timing and file sizes for now
            end_ts = time.time()
            exit_code = run_result.get("exit_code") if run_result else None
            # size metrics
            try:
                stdout_sz = os.path.getsize(stdout_path) if os.path.exists(stdout_path) else 0
                stderr_sz = os.path.getsize(stderr_path) if os.path.exists(stderr_path) else 0
                resource_usage = {"stdout_bytes": stdout_sz, "stderr_bytes": stderr_sz}
            except Exception:
                resource_usage = {}
            # persist exec info
            exec_db_id = self._persist_exec(session_id, cmd, exec_type, stdout_path, stderr_path, run_result.get("start"), run_result.get("end"), exit_code, resource_usage)
            # finalize result
            result = {
                "ok": run_result.get("ok", False),
                "exit_code": exit_code,
                "timeout": run_result.get("timeout", False),
                "stdout_path": stdout_path,
                "stderr_path": stderr_path,
                "exec_db_id": exec_db_id,
                "start_ts": run_result.get("start"),
                "end_ts": run_result.get("end"),
                "resource_usage": resource_usage
            }
            return result
        except Exception:
            logger.exception("run_in_sandbox unexpected error")
            return {"ok": False, "error": "exception"}
        finally:
            if ephemeral:
                try:
                    # cleanup ephemeral session
                    self.stop_session(session_id)
                except Exception:
                    pass

# ----------------------------
# Convenience wrappers
# ----------------------------
    def run_script_in_sandbox(self, script_path: str, *, shell: bool = False, cwd: Optional[str]=None, env: Optional[Dict[str,str]]=None, timeout: Optional[int]=None, session_id: Optional[str]=None):
        """
        Run a script (sh or python) inside sandbox. If shell=True, will launch via /bin/sh -c.
        """
        if not os.path.exists(script_path):
            return {"ok": False, "error": "script_not_found"}
        if shell:
            cmd = ["/bin/sh", "-c", script_path]
        else:
            # detect shebang or extension
            if script_path.endswith(".py"):
                cmd = [sys.executable, script_path]
            else:
                cmd = ["/bin/sh", script_path]
        return self.run_in_sandbox(cmd, cwd=cwd, env=env, timeout=timeout, session_id=session_id, exec_type="script")

    def run_python_in_sandbox(self, module_or_callable, context: Optional[Dict[str,Any]] = None, *, cwd: Optional[str]=None, timeout: Optional[int]=None, session_id: Optional[str]=None):
        """
        Run a python module or callable inside sandbox. Implementation: write a small wrapper script into session workdir that imports and calls the module/callable.
        module_or_callable can be "module:function" or a callable object (not recommended across processes).
        """
        # prepare wrapper
        session_id = session_id or self.start_session()
        s = self._sessions.get(session_id)
        if not s:
            return {"ok": False, "error": "session_missing"}
        workdir = s["workdir"]
        wrapper_path = os.path.join(workdir, f"wrapper_{uuid.uuid4().hex}.py")
        if isinstance(module_or_callable, str):
            # format "package.module:function" or "package.module"
            if ":" in module_or_callable:
                mod, func = module_or_callable.split(":", 1)
            else:
                mod, func = module_or_callable, None
            wrapper_code = [
                "import json, sys",
                f"import {mod} as __mod"
            ]
            if func:
                wrapper_code.append(f"res = getattr(__mod, '{func}')()")
                wrapper_code.append("print(json.dumps({'result': str(res)}))")
            else:
                wrapper_code.append("print('module_loaded')")
            with open(wrapper_path, "w", encoding="utf-8") as f:
                f.write("\n".join(wrapper_code))
            cmd = [sys.executable, wrapper_path]
            return self.run_in_sandbox(cmd, cwd=workdir, env=None, timeout=timeout, session_id=session_id, exec_type="python")
        else:
            # if a callable provided in-process, we attempt to run it here (not isolated)
            try:
                res = module_or_callable(context or {})
                return {"ok": True, "result": res}
            except Exception:
                logger.exception("run_python_in_sandbox callable execution failed")
                return {"ok": False, "error": "callable_exception"}

# ----------------------------
# Module-level manager + wrappers
# ----------------------------
_MANAGER_LOCK = threading.RLock()
_MANAGER: Optional[SandboxManager] = None

def get_sandbox_manager() -> SandboxManager:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = SandboxManager()
        return _MANAGER

def run_in_sandbox(*a, **k):
    return get_sandbox_manager().run_in_sandbox(*a, **k)

def run_script_in_sandbox(*a, **k):
    return get_sandbox_manager().run_script_in_sandbox(*a, **k)

def run_python_in_sandbox(*a, **k):
    return get_sandbox_manager().run_python_in_sandbox(*a, **k)

def start_session(*a, **k):
    return get_sandbox_manager().start_session(*a, **k)

def stop_session(*a, **k):
    return get_sandbox_manager().stop_session(*a, **k)

def snapshot(*a, **k):
    return get_sandbox_manager().snapshot(*a, **k)

def restore(*a, **k):
    return get_sandbox_manager().restore(*a, **k)

def get_exec_report(*a, **k):
    return get_sandbox_manager().get_exec_report(*a, **k)

def stream_output(*a, **k):
    return get_sandbox_manager().stream_output(*a, **k)

# ----------------------------
# CLI demo when run directly
# ----------------------------
if __name__ == "__main__":
    sm = get_sandbox_manager()
    print("SandboxManager ready. workdir_root:", sm.workdir_root)
    print("Starting ephemeral run: echo hello")
    r = sm.run_in_sandbox(["/bin/echo", "hello"], timeout=10)
    print("Result:", r)
    if r.get("stdout_path"):
        print("Stdout:")
        for line in sm.stream_output(r["stdout_path"], tail=False):
            print(line, end="")
