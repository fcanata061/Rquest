# rquest1.0/modules/logging.py
# -*- coding: utf-8 -*-
"""
Rquest logging (final refactor)

Features:
 - Integration with modules.config (hot reload)
 - Console color formatter
 - Rotating file handler
 - JSONL transparency log with atomic append and optional fsync
 - Module-level configurable log levels (module_levels)
 - Emits events via modules.db.emit_event and modules.hooks
 - Follow/tail server (Unix domain socket) for live build logs (colorized)
 - Thread-safe reconfiguration and metrics
"""

from __future__ import annotations
import os
import sys
import json
import time
import socket
import logging
import logging.handlers
import threading
import queue
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable, Tuple

# Defensive imports from the project
try:
    from modules.config import get_config, register_watch_callback
except Exception:
    # fallback minimal config if modules.config not available
    def get_config():
        return {"merged": {"logging": {}}}
    def register_watch_callback(cb):
        return

try:
    from modules.db import emit_event
except Exception:
    def emit_event(*a, **k):
        return

try:
    from modules.hooks import get_hook_manager
    HOOKS = get_hook_manager()()
except Exception:
    HOOKS = None

# Logger for this module
_logger = logging.getLogger("rquest.logging")
if not _logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ----------------------
# Color formatter
# ----------------------
class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[37m",    # light gray
        logging.INFO: "\033[36m",     # cyan
        logging.WARNING: "\033[33m",  # yellow
        logging.ERROR: "\033[31m",    # red
        logging.CRITICAL: "\033[41;37m", # white on red
    }
    RESET = "\033[0m"

    def __init__(self, fmt: str = None, datefmt: str = None, color: bool = True):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.color = color

    def format(self, record):
        msg = super().format(record)
        if self.color:
            color = self.COLORS.get(record.levelno, "")
            return f"{color}{msg}{self.RESET}"
        return msg

# ----------------------
# JSONL formatter for transparency log
# ----------------------
class JSONLineFormatter(logging.Formatter):
    def format(self, record):
        obj = {
            "timestamp": time.time(),
            "level": record.levelname,
            "module": getattr(record, "rquest_module", record.name),
            "message": record.getMessage(),
            "extra": {},
        }
        # include exception info if present
        if record.exc_info:
            obj["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(obj, ensure_ascii=False)

# ----------------------
# Module-level filter for per-module levels
# ----------------------
class ModuleLevelFilter(logging.Filter):
    def __init__(self, module_levels: Dict[str, str]):
        super().__init__()
        # convert level names to numeric
        self.module_levels = {m: getattr(logging, lvl.upper(), logging.INFO) for m, lvl in (module_levels or {}).items()}

    def filter(self, record):
        mod = getattr(record, "rquest_module", None)
        if mod and mod in self.module_levels:
            return record.levelno >= self.module_levels[mod]
        return True

# ----------------------
# Build follow (Unix Domain Socket server) manager
# ----------------------
class _BuildFollowServer(threading.Thread):
    """
    Server that accepts multiple clients on a unix domain socket and broadcasts lines to them.
    Each module may have one server instance (path configurable).
    """
    def __init__(self, socket_path: Path, color: bool = True):
        super().__init__(daemon=True)
        self.socket_path = socket_path
        self.color = color
        self._clients: List[socket.socket] = []
        self._q: "queue.Queue[str]" = queue.Queue()
        self._running = threading.Event()
        self._srv_sock: Optional[socket.socket] = None
        self._lock = threading.Lock()

    def run(self):
        # Ensure parent dir exists
        try:
            self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        # Remove stale socket
        try:
            if self.socket_path.exists():
                self.socket_path.unlink()
        except Exception:
            pass

        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind(str(self.socket_path))
            srv.listen(5)
            srv.settimeout(0.5)
            self._srv_sock = srv
            self._running.set()
        except Exception as e:
            _logger.exception("buildfollow: cannot bind socket %s: %s", self.socket_path, e)
            try:
                srv.close()
            except Exception:
                pass
            self._running.clear()
            return

        _logger.info("buildfollow: started at %s", self.socket_path)

        try:
            while self._running.is_set():
                # accept new clients non-blocking using timeout
                try:
                    client, _ = srv.accept()
                    client.setblocking(False)
                    with self._lock:
                        self._clients.append(client)
                    _logger.debug("buildfollow: client connected")
                except socket.timeout:
                    pass
                except BlockingIOError:
                    pass
                except Exception:
                    _logger.debug("buildfollow: accept error", exc_info=True)

                # broadcast queued lines
                try:
                    while not self._q.empty():
                        line = self._q.get_nowait()
                        with self._lock:
                            to_remove = []
                            for c in list(self._clients):
                                try:
                                    # send line as utf-8 bytes, ensure newline
                                    c.sendall((line + "\n").encode("utf-8"))
                                except (BrokenPipeError, ConnectionResetError):
                                    try:
                                        c.close()
                                    except Exception:
                                        pass
                                    to_remove.append(c)
                                except Exception:
                                    to_remove.append(c)
                            for c in to_remove:
                                try:
                                    self._clients.remove(c)
                                except ValueError:
                                    pass
                except Exception:
                    _logger.exception("buildfollow: broadcast error")
                # small sleep to yield
                time.sleep(0.01)
        finally:
            # cleanup clients
            with self._lock:
                for c in self._clients:
                    try:
                        c.close()
                    except Exception:
                        pass
                self._clients.clear()
            try:
                srv.close()
            except Exception:
                pass
            try:
                if self.socket_path.exists():
                    self.socket_path.unlink()
            except Exception:
                pass
            _logger.info("buildfollow: stopped %s", self.socket_path)

    def enqueue(self, line: str):
        # non-blocking enqueue
        try:
            self._q.put_nowait(line.rstrip("\n"))
        except queue.Full:
            pass

    def stop(self):
        self._running.clear()

# ----------------------
# RquestLogger (singleton)
# ----------------------
class RquestLogger:
    _instance = None
    _singleton_lock = threading.Lock()

    def __new__(cls):
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._inited = False
        return cls._instance

    def __init__(self):
        if self._inited:
            return
        self._lock = threading.RLock()

        # core python logger
        self._root = logging.getLogger("rquest")
        self._root.setLevel(logging.DEBUG)  # capture everything; handlers will filter

        # internal state
        self._handlers: List[logging.Handler] = []
        self._module_filter = ModuleLevelFilter({})
        self._metrics: Dict[str, int] = {lvl: 0 for lvl in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")}
        self._jsonl_path: Optional[Path] = None
        self._jsonl_fsync: bool = False
        self._follow_servers: Dict[str, _BuildFollowServer] = {}
        self._follow_dir: Path = Path("/run/rquest/builds")

        # initial configuration from modules.config
        try:
            cfg = get_config().merged.get("logging", {})
        except Exception:
            cfg = {}

        # apply initial config
        self._apply_config(cfg)

        # register config watcher for hot reload
        try:
            register_watch_callback(lambda new_cfg: self.reload_config())
        except Exception:
            pass

        # increment metric on each emitted record via handler
        # we'll add a metrics-incrementing filter on root
        self._root.addFilter(self._count_levels_filter)

        # default hooks
        self._hooks = HOOKS

        self._inited = True

    # ----------------------
    # Internal helpers
    # ----------------------
    def _count_levels_filter(self, record):
        # update counters
        try:
            name = record.levelname
            if name in self._metrics:
                self._metrics[name] += 1
        except Exception:
            pass
        return True

    def _atomic_append_jsonl(self, path: Path, obj: Dict[str, Any]):
        # atomic append: use os.open with O_APPEND
        try:
            line = json.dumps(obj, ensure_ascii=False) + "\n"
            flags = os.O_CREAT | os.O_WRONLY | os.O_APPEND
            fd = os.open(str(path), flags, 0o644)
            try:
                os.write(fd, line.encode("utf-8"))
                if self._jsonl_fsync:
                    os.fsync(fd)
            finally:
                os.close(fd)
        except Exception:
            _logger.exception("logging: failed atomic append to %s", path)

    # ----------------------
    # Configuration (apply/hot-reload)
    # ----------------------
    def _apply_config(self, cfg: Dict[str, Any]):
        with self._lock:
            # clean old handlers
            for h in list(self._handlers):
                try:
                    self._root.removeHandler(h)
                except Exception:
                    pass
            self._handlers.clear()

            # module-level override map
            module_levels = cfg.get("module_levels", {}) or {}
            self._module_filter = ModuleLevelFilter(module_levels)
            self._root.addFilter(self._module_filter)

            # console handler
            console_cfg = cfg.get("console", {"enabled": True})
            if console_cfg.get("enabled", True):
                ch = logging.StreamHandler(sys.stdout)
                level = getattr(logging, cfg.get("level", "INFO").upper(), logging.INFO)
                ch.setLevel(level)
                fmt = cfg.get("format") or "[%(asctime)s] [%(levelname)s] [%(rquest_module)s] %(message)s"
                datefmt = cfg.get("datefmt", "%H:%M:%S")
                color = cfg.get("color", True)
                ch.setFormatter(ColorFormatter(fmt, datefmt=datefmt, color=color))
                self._root.addHandler(ch)
                self._handlers.append(ch)

            # rotating file handler
            if cfg.get("file"):
                try:
                    file_path = Path(cfg["file"]).expanduser()
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    max_bytes = self._parse_size(cfg.get("max_size", "10M"))
                    backups = int(cfg.get("backups", 5))
                    fh = logging.handlers.RotatingFileHandler(str(file_path), maxBytes=max_bytes or 10 * 1024 * 1024, backupCount=backups, encoding="utf-8")
                    fh.setLevel(getattr(logging, cfg.get("file_level", "DEBUG").upper(), logging.DEBUG))
                    fh.setFormatter(logging.Formatter(cfg.get("format") or "%(asctime)s %(levelname)s [%(rquest_module)s] %(message)s", datefmt=cfg.get("datefmt", "%H:%M:%S")))
                    self._root.addHandler(fh)
                    self._handlers.append(fh)
                except Exception:
                    _logger.exception("logging: failed to configure file handler")

            # jsonl transparency log
            jsonl_cfg = cfg.get("jsonl", {})
            if jsonl_cfg.get("enabled"):
                try:
                    path = Path(jsonl_cfg.get("path", "/var/log/rquest/transparency.jsonl")).expanduser()
                    path.parent.mkdir(parents=True, exist_ok=True)
                    self._jsonl_path = path
                    self._jsonl_fsync = bool(jsonl_cfg.get("fsync", False))
                    jh = logging.FileHandler(str(path), encoding="utf-8")
                    jh.setLevel(getattr(logging, jsonl_cfg.get("level", "INFO").upper(), logging.INFO))
                    jh.setFormatter(JSONLineFormatter())
                    self._root.addHandler(jh)
                    self._handlers.append(jh)
                except Exception:
                    _logger.exception("logging: failed to configure jsonl handler")
            else:
                self._jsonl_path = None
                self._jsonl_fsync = False

            # follow directory
            follow_dir = cfg.get("follow_dir") or "/run/rquest/builds"
            self._follow_dir = Path(follow_dir)

            # reconfigure root level
            root_level = getattr(logging, cfg.get("level", "INFO").upper(), logging.INFO)
            self._root.setLevel(root_level)

            _logger.info("logging: configuration applied")

    def reload_config(self):
        """Reload config from modules.config and re-apply logging config."""
        try:
            cfg = get_config().merged.get("logging", {})
            self._apply_config(cfg)
            self._root.info("logging: reloaded configuration from central config")
        except Exception:
            _logger.exception("logging: reload_config failed")

    # ----------------------
    # Public API
    # ----------------------
    def get_logger(self, module_name: str) -> logging.LoggerAdapter:
        """Return a LoggerAdapter that injects 'rquest_module' into records."""
        base = logging.getLogger("rquest")
        return logging.LoggerAdapter(base, {"rquest_module": module_name})

    def parse_and_log(self, module: str, level: int, msg: str, **kwargs):
        """Helper to emit a log record and also append to jsonl atomic log."""
        adapter = self.get_logger(module)
        adapter.log(level, msg, **kwargs)
        # atomic transparency
        if self._jsonl_path:
            record = {
                "timestamp": time.time(),
                "level": logging.getLevelName(level),
                "module": module,
                "message": msg
            }
            # optionally attach extra if present
            try:
                self._atomic_append_jsonl(self._jsonl_path, record)
            except Exception:
                _logger.debug("logging: jsonl append failed", exc_info=True)
        # emit event on severe logs
        if level >= logging.ERROR:
            try:
                emit_event("log.error", {"module": module, "level": logging.getLevelName(level), "message": msg})
            except Exception:
                pass
            # run hooks on error
            if self._hooks:
                try:
                    self._hooks.run("on_log_error", {"module": module, "message": msg})
                except Exception:
                    _logger.debug("logging: hook on_log_error failed", exc_info=True)

    # build follow API
    def start_build_follow(self, module: str) -> Optional[str]:
        """
        Start a follow server for module. Returns socket path to connect (unix domain).
        Clients can use e.g. `nc -U /run/rquest/builds/<module>.sock` to follow.
        """
        with self._lock:
            if module in self._follow_servers:
                return str(self._follow_servers[module].socket_path)
            sockdir = self._follow_dir
            try:
                sockdir.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            sockpath = sockdir / f"{module}.sock"
            server = _BuildFollowServer(socket_path=sockpath, color=True)
            self._follow_servers[module] = server
            server.start()
            # small wait for server up (non-blocking)
            t0 = time.time()
            while not server._running.is_set() and time.time() - t0 < 1.0:
                time.sleep(0.01)
            if not server._running.is_set():
                _logger.warning("logging: build follow server failed to start for %s", module)
                return None
            _logger.info("logging: build follow started for %s at %s", module, sockpath)
            return str(sockpath)

    def stream_build_output(self, module: str, line: str):
        """Write a line to normal logs and broadcast to follow clients if any."""
        # log normally
        self.parse_and_log(module, logging.INFO, line)
        # broadcast
        server = self._follow_servers.get(module)
        if server:
            try:
                server.enqueue(line)
            except Exception:
                _logger.debug("logging: failed to enqueue build follow line", exc_info=True)

    def stop_build_follow(self, module: str):
        with self._lock:
            server = self._follow_servers.pop(module, None)
            if server:
                try:
                    server.stop()
                except Exception:
                    _logger.exception("logging: error stopping follow server")
                _logger.info("logging: build follow stopped for %s", module)

    def _parse_size(self, s: Any) -> Optional[int]:
        return _parse_size(s)

    def get_metrics(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._metrics)

# ----------------------
# Helper parse size (public)
# ----------------------
def _parse_size(s: Any) -> Optional[int]:
    if s is None:
        return None
    try:
        if isinstance(s, int):
            return s
        ss = str(s).strip().upper()
        if ss.endswith("KB"):
            return int(float(ss[:-2]) * 1024)
        if ss.endswith("K"):
            return int(float(ss[:-1]) * 1024)
        if ss.endswith("MB"):
            return int(float(ss[:-2]) * 1024**2)
        if ss.endswith("M"):
            return int(float(ss[:-1]) * 1024**2)
        if ss.endswith("GB"):
            return int(float(ss[:-2]) * 1024**3)
        if ss.endswith("G"):
            return int(float(ss[:-1]) * 1024**3)
        return int(float(ss))
    except Exception:
        _logger.debug("logging: parse size failed for %s", s, exc_info=True)
        return None

# ----------------------
# Public factory
# ----------------------
_GLOBAL_LOGGER = RquestLogger()

def get_logger(module: str):
    return _GLOBAL_LOGGER.get_logger(module)

def parse_and_log(module: str, level: int, msg: str, **kwargs):
    return _GLOBAL_LOGGER.parse_and_log(module, level, msg, **kwargs)

def start_build_follow(module: str) -> Optional[str]:
    return _GLOBAL_LOGGER.start_build_follow(module)

def stream_build_output(module: str, line: str):
    return _GLOBAL_LOGGER.stream_build_output(module, line)

def stop_build_follow(module: str):
    return _GLOBAL_LOGGER.stop_build_follow(module)

def reload_config():
    return _GLOBAL_LOGGER.reload_config()

def get_metrics():
    return _GLOBAL_LOGGER.get_metrics()
