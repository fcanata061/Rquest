
# Rquest/rquest1.0/modules/logging.py
"""
Rquest Logger - logging.py

Features:
- Singleton logger manager (RquestLogger)
- Console handler (colorized)
- Rotating file handler
- Optional JSON output handler
- Module-level filters / overrides
- Follow-build streaming mode (tail-like), with configurable buffer
- Metrics counters (basic)
- Alerts (desktop notify-send, email via SMTP simple integration)
- Integration with config.yaml (tries to import local config module first,
  otherwise loads YAML (PyYAML if available) or a minimal parser)
- Public API: get_logger, start_build_log, stream_build_output, end_build_log,
  set_level, add_handler, get_metrics

Usage:
  from logging import get_logger, start_build_log, stream_build_output, end_build_log
  logger = get_logger("db")
  logger.info("Hello world")
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
import threading
import time
import json
import traceback
from collections import deque, defaultdict
from datetime import datetime
from typing import Optional, Dict, Any, Deque, Callable

# ---- Try to load PyYAML, else fallback to a minimal parser ----
try:
    import yaml  # type: ignore
    _HAS_YAML = True
except Exception:
    _HAS_YAML = False

# ---- Simple helpers ----
def _human_size_to_bytes(s: str) -> int:
    """Convert like '5MB' or '10M' or '1024' to bytes"""
    if s is None:
        return 0
    s = str(s).strip().upper()
    try:
        if s.endswith("GB"):
            return int(float(s[:-2]) * 1024 ** 3)
        if s.endswith("G"):
            return int(float(s[:-1]) * 1024 ** 3)
        if s.endswith("MB"):
            return int(float(s[:-2]) * 1024 ** 2)
        if s.endswith("M"):
            return int(float(s[:-1]) * 1024 ** 2)
        if s.endswith("KB"):
            return int(float(s[:-2]) * 1024)
        if s.endswith("K"):
            return int(float(s[:-1]) * 1024)
        return int(s)
    except Exception:
        return 0

# ---- Default configuration if no config.yaml ----
_default_config = {
    "level": "INFO",
    "color": True,
    "file": None,
    "rotate": True,
    "max_size": "10MB",
    "backups": 5,
    "timestamp": True,
    "module_prefix": True,
    "follow_build": True,
    "follow_buffer": 200,
    "follow_colorize": True,
    "json_output": False,
    "metrics": True,
    "alerts": {"enable": False},
    "filters": [],
}


# ---- Config loader ----
def _load_config() -> Dict[str, Any]:
    """
    Try order:
    1) import config module (Rquest.rquest1.0.config or config in same folder)
       - expects config to expose get_config() or CONFIG dict
    2) look for config.yaml in a few candidate paths:
       - ./config.yaml
       - Rquest/rquest1.0/config.yaml (relative to repo root if running from project)
       - /etc/rquest/config.yaml
       - path from ENV var RQUEST_CONFIG
    3) fallback to defaults
    """
    # 1) Try import project config module if exists
    try:
        # prefer package-local config if present
        import importlib

        # Try different likely module names
        possible = [
            "rquest1_0.config",
            "rquest1.0.config",
            "rquest1_0.config",
            "Rquest.rquest1.0.config",
            "config",
        ]
        for name in possible:
            try:
                cfg_mod = importlib.import_module(name)
                if hasattr(cfg_mod, "get_config") and callable(cfg_mod.get_config):
                    c = cfg_mod.get_config()
                    if isinstance(c, dict):
                        return c.get("logging", c)
                if hasattr(cfg_mod, "CONFIG"):
                    cfg = getattr(cfg_mod, "CONFIG")
                    if isinstance(cfg, dict):
                        return cfg.get("logging", cfg)
                # if module has attribute 'LOGGING' or 'logging', accept
                if hasattr(cfg_mod, "LOGGING"):
                    return getattr(cfg_mod, "LOGGING")
                if hasattr(cfg_mod, "logging"):
                    return getattr(cfg_mod, "logging")
            except Exception:
                continue
    except Exception:
        pass

    # 2) Look for YAML file(s)
    paths = []
    env_path = os.getenv("RQUEST_CONFIG")
    if env_path:
        paths.append(env_path)
    # common
    paths.extend([
        os.path.join(os.getcwd(), "config.yaml"),
        os.path.join(os.getcwd(), "Rquest", "rquest1.0", "config.yaml"),
        os.path.join(os.getcwd(), "Rquest", "rquest1.0", "config.yml"),
        "/etc/rquest/config.yaml",
        "/etc/rquest/config.yml",
    ])
    for p in paths:
        if not p:
            continue
        if os.path.exists(p) and os.path.isfile(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = f.read()
                    if _HAS_YAML:
                        raw = yaml.safe_load(data)
                    else:
                        raw = _minimal_yaml_load(data)
                    if raw and isinstance(raw, dict):
                        # Accept either "logging" top-level or direct mapping
                        if "logging" in raw and isinstance(raw["logging"], dict):
                            return raw["logging"]
                        # support older layouts
                        if "rquest" in raw and isinstance(raw["rquest"], dict) and "logging" in raw["rquest"]:
                            return raw["rquest"]["logging"]
                        # if the whole file is the logging mapping
                        return raw
            except Exception:
                # ignore parse errors, try next path
                continue
    # 3) fallback
    return _default_config.copy()


def _minimal_yaml_load(text: str) -> Optional[Dict[str, Any]]:
    """
    Extremely small YAML-ish parser used only as fallback when PyYAML is not available.
    - Only supports a simple mapping and nested mapping with indentation.
    - It's intentionally limited; for production, install PyYAML.
    """
    result = {}
    current = result
    parents = [result]
    indent_levels = [0]
    for lineno, raw in enumerate(text.splitlines()):
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        # count leading spaces
        indent = len(raw) - len(raw.lstrip(" "))
        try:
            if ":" in line:
                key, val = line.split(":", 1)
                key = key.strip()
                val = val.strip()
                if val == "":
                    # new nested mapping
                    newmap = {}
                    # choose parent based on indent
                    while indent_levels and indent < indent_levels[-1]:
                        parents.pop()
                        indent_levels.pop()
                    parents[-1][key] = newmap
                    parents.append(newmap)
                    indent_levels.append(indent)
                else:
                    # scalar value
                    v = val
                    # try convert booleans, numbers
                    if v.lower() in ("true", "false"):
                        v2 = v.lower() == "true"
                    else:
                        try:
                            if "." in v:
                                v2 = float(v)
                            else:
                                v2 = int(v)
                        except Exception:
                            v2 = v
                    parents[-1][key] = v2
            else:
                # unsupported line, skip
                continue
        except Exception:
            continue
    return result


# ---- ANSI color codes ----
ANSI = {
    "RESET": "\033[0m",
    "BOLD": "\033[1m",
    "DIM": "\033[2m",
    "GRAY": "\033[90m",
    "RED": "\033[91m",
    "GREEN": "\033[92m",
    "YELLOW": "\033[93m",
    "BLUE": "\033[94m",
    "MAGENTA": "\033[95m",
    "CYAN": "\033[96m",
    "WHITE": "\033[97m",
}


# ---- Formatter classes ----
class _ColorFormatter(logging.Formatter):
    """
    Formatter that adds ANSI colors based on log level and optionally the 'build' tag.
    """

    LEVEL_COLORS = {
        logging.DEBUG: ANSI["GRAY"],
        logging.INFO: ANSI["GREEN"],
        logging.WARNING: ANSI["YELLOW"],
        logging.ERROR: ANSI["RED"],
        logging.CRITICAL: ANSI["MAGENTA"],
    }

    def __init__(self, fmt: str, datefmt: Optional[str], use_color: bool, module_prefix: bool):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.use_color = use_color
        self.module_prefix = module_prefix

    def format(self, record: logging.LogRecord) -> str:
        levelno = record.levelno
        msg = super().format(record)
        if self.use_color:
            color = self.LEVEL_COLORS.get(levelno, "")
            return f"{color}{msg}{ANSI['RESET']}"
        return msg


class _PlainFormatter(logging.Formatter):
    """Plain formatter without colors, used for file handlers or JSON output pre-format."""
    pass


# ---- JSON handler ----
class _JsonFileHandler(logging.Handler):
    """A simple handler that writes JSON lines to a file."""

    def __init__(self, filename: str, rotate: bool = False, max_bytes: int = 0, backups: int = 0):
        super().__init__()
        self.filename = filename
        self.rotate = rotate
        self.max_bytes = max_bytes
        self.backups = backups
        self._lock = threading.RLock()
        # ensure dir exists
        dirname = os.path.dirname(filename)
        if dirname and not os.path.exists(dirname):
            try:
                os.makedirs(dirname, exist_ok=True)
            except Exception:
                pass

    def emit(self, record: logging.LogRecord) -> None:
        try:
            obj = {
                "time": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
                "level": record.levelname,
                "module": getattr(record, "rquest_module", record.name),
                "message": record.getMessage(),
                "args": record.args,
                "pathname": record.pathname,
                "lineno": record.lineno,
            }
            line = json.dumps(obj, ensure_ascii=False)
            with self._lock:
                with open(self.filename, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                if self.rotate and self.max_bytes > 0:
                    try:
                        if os.path.getsize(self.filename) > self.max_bytes:
                            self._do_rotate()
                    except Exception:
                        pass
        except Exception:
            self.handleError(record)

    def _do_rotate(self):
        # simple rotation
        try:
            for i in range(self.backups - 1, 0, -1):
                s = f"{self.filename}.{i}"
                d = f"{self.filename}.{i+1}"
                if os.path.exists(s):
                    os.replace(s, d)
            if os.path.exists(self.filename):
                os.replace(self.filename, f"{self.filename}.1")
        except Exception:
            pass


# ---- RquestLogger singleton ----
class RquestLogger:
    _instance = None
    _lock = threading.RLock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(RquestLogger, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        # load config
        cfg = _load_config()
        # normalize keys
        self.config = {
            "level": cfg.get("level", _default_config["level"]).upper(),
            "color": bool(cfg.get("color", _default_config["color"])),
            "file": cfg.get("file", _default_config["file"]),
            "rotate": bool(cfg.get("rotate", _default_config["rotate"])),
            "max_size": _human_size_to_bytes(cfg.get("max_size", _default_config["max_size"])),
            "backups": int(cfg.get("backups", _default_config["backups"])),
            "timestamp": bool(cfg.get("timestamp", _default_config["timestamp"])),
            "module_prefix": bool(cfg.get("module_prefix", _default_config["module_prefix"])),
            "follow_build": bool(cfg.get("follow_build", _default_config["follow_build"])),
            "follow_buffer": int(cfg.get("follow_buffer", _default_config["follow_buffer"])),
            "follow_colorize": bool(cfg.get("follow_colorize", _default_config["follow_colorize"])),
            "json_output": bool(cfg.get("json_output", _default_config["json_output"])),
            "metrics": bool(cfg.get("metrics", _default_config["metrics"])),
            "alerts": cfg.get("alerts", _default_config["alerts"]),
            "filters": cfg.get("filters", _default_config["filters"]) or [],
        }

        # core logging.Logger
        self.root_logger = logging.getLogger("rquest")
        self.root_logger.setLevel(getattr(logging, self.config["level"], logging.INFO))
        # Prevent double handlers if re-init
        self.root_logger.propagate = False
        for h in list(self.root_logger.handlers):
            self.root_logger.removeHandler(h)

        # create handlers
        self.handlers = []
        # console handler setup
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.DEBUG)  # root level controls output
        datefmt = "%Y-%m-%d %H:%M:%S" if self.config["timestamp"] else None
        fmt_parts = []
        if self.config["module_prefix"]:
            fmt_parts.append("[%(levelname)s] [%(asctime)s] [%(rquest_module)s] %(message)s" if self.config["timestamp"] else "[%(levelname)s] [%(rquest_module)s] %(message)s")
        else:
            fmt_parts.append("[%(_levelname)s] [%(asctime)s] %(message)s" if self.config["timestamp"] else "[%(levelname)s] %(message)s")
        fmt = fmt_parts[0]
        color_formatter = _ColorFormatter(fmt=fmt, datefmt=datefmt, use_color=self.config["color"], module_prefix=self.config["module_prefix"])
        console.setFormatter(color_formatter)
        self.root_logger.addHandler(console)
        self.console_handler = console
        self.handlers.append(console)

        # file handler if configured
        self.file_handler = None
        if self.config["file"]:
            try:
                dirn = os.path.dirname(self.config["file"])
                if dirn and not os.path.exists(dirn):
                    os.makedirs(dirn, exist_ok=True)
                if self.config["rotate"] and self.config["max_size"] > 0:
                    fh = logging.handlers.RotatingFileHandler(self.config["file"], maxBytes=self.config["max_size"], backupCount=self.config["backups"], encoding="utf-8")
                else:
                    fh = logging.FileHandler(self.config["file"], encoding="utf-8")
                fh.setLevel(logging.DEBUG)
                plain_fmt = fmt  # no color
                fh.setFormatter(_PlainFormatter(fmt=plain_fmt, datefmt=datefmt))
                self.root_logger.addHandler(fh)
                self.file_handler = fh
                self.handlers.append(fh)
            except Exception:
                # if file handler fails, fallback to console-only
                self.root_logger.exception("Failed to create file handler for logging")

        # json output handler if requested
        self.json_handler = None
        if self.config["json_output"] and self.config["file"]:
            jfile = f"{self.config['file']}.json"
            jf = _JsonFileHandler(jfile, rotate=self.config["rotate"], max_bytes=self.config["max_size"], backups=self.config["backups"])
            jf.setLevel(logging.DEBUG)
            self.root_logger.addHandler(jf)
            self.json_handler = jf
            self.handlers.append(jf)

        # internal state
        self._module_loggers: Dict[str, logging.Logger] = {}
        self._filters: Dict[str, int] = {}  # module -> levelno overrides
        self._apply_filters_from_config()

        # metrics
        self._metrics = defaultdict(int)  # counters by levelname, etc.

        # follow-build streaming
        self._follow_active = False
        self._follow_buffer: Deque[str] = deque(maxlen=self.config["follow_buffer"])
        self._follow_lock = threading.RLock()
        self._current_build_meta: Dict[str, Any] = {}
        self._build_start_time: Optional[float] = None

        # exception hook
        sys.excepthook = self._handle_uncaught_exception

    def _apply_filters_from_config(self):
        try:
            fl = self.config.get("filters", [])
            for f in fl:
                if isinstance(f, dict) and "module" in f and "level" in f:
                    mod = f["module"]
                    lvl = getattr(logging, str(f["level"]).upper(), None)
                    if isinstance(lvl, int):
                        self._filters[mod] = lvl
        except Exception:
            pass

    # --- Public API for modules ---

    def get_logger(self, module_name: str) -> logging.Logger:
        """
        Return a logger instance for the module. It will add a custom filter to inject
        module name into records as 'rquest_module' and respect module-level filters.
        """
        if module_name in self._module_loggers:
            return self._module_loggers[module_name]

        logger = logging.getLogger(f"rquest.{module_name}")
        logger.setLevel(logging.DEBUG)  # capture everything; output filtered by root handlers and filters
        logger.propagate = True  # propagate to root_logger handlers

        # add our adapter via a Filter
        class _InjectModuleFilter(logging.Filter):
            def filter(inner_self, record: logging.LogRecord) -> bool:
                # attach module name for formatters
                setattr(record, "rquest_module", module_name)
                # module-level filter override
                if module_name in self._filters:
                    minlvl = self._filters[module_name]
                    return record.levelno >= minlvl
                return True

        logger.addFilter(_InjectModuleFilter())
        self._module_loggers[module_name] = logger
        return logger

    def set_level(self, level_name: str):
        lv = getattr(logging, level_name.upper(), None)
        if not isinstance(lv, int):
            raise ValueError(f"Invalid level: {level_name}")
        self.root_logger.setLevel(lv)
        # Also update stored config
        self.config["level"] = level_name.upper()

    def add_handler(self, handler: logging.Handler):
        self.root_logger.addHandler(handler)
        self.handlers.append(handler)

    def get_metrics(self) -> Dict[str, int]:
        # return a shallow copy
        return dict(self._metrics)

    # --- Logging helpers that integrate metrics and follow buffer ---

    def _record_metrics(self, levelno: int):
        try:
            if self.config["metrics"]:
                name = logging.getLevelName(levelno)
                self._metrics[f"total_{name.lower()}"] += 1
        except Exception:
            pass

    def _buffer_line(self, line: str):
        with self._follow_lock:
            if self.config["follow_build"]:
                self._follow_buffer.append(line)

    def _stream_to_follow(self, line: str):
        """
        Print to stdout immediately in follow mode (colorization already applied by formatter)
        We send raw line to stdout so user sees it as if tail -f
        """
        with self._follow_lock:
            if not self.config["follow_build"]:
                return
            # Print directly. We assume line includes newline already or not.
            try:
                sys.stdout.write(line if line.endswith("\n") else line + "\n")
                sys.stdout.flush()
            except Exception:
                pass

    # --- Build streaming API ---

    def start_build_log(self, pkg_name: str):
        """
        Begin a build session: clear follow buffer and mark active.
        Returns nothing. Use stream_build_output for lines.
        """
        with self._follow_lock:
            self._follow_buffer.clear()
            self._follow_active = True
            self._current_build_meta = {"pkg": pkg_name}
            self._build_start_time = time.time()
            self.root_logger.info(f"[BUILD START] {pkg_name}", extra={"rquest_module": "buildsystem"})
            # Also add an indicator line to buffer
            header = f"[BUILD] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [buildsystem] Starting build for {pkg_name}"
            self._buffer_line(header)
            if self.config["follow_build"]:
                self._stream_to_follow(header)

    def stream_build_output(self, line: str, level: str = "INFO", module: Optional[str] = "buildsystem"):
        """
        Stream a single line of build output.
        - level: INFO/WARNING/ERROR/DEBUG
        - module: logical module name to appear in prefix
        Behavior:
        - Logs the line to the root logger (preserving file logging)
        - Appends to follow buffer
        - Streams to console immediately (if follow_build enabled)
        """
        lvl = getattr(logging, level.upper(), logging.INFO)
        extra = {"rquest_module": module}
        # Build-specific formatting: tag with [BUILD] to visually separate (we add in message)
        msg = line.rstrip("\n")
        # Decide colorization in console occurs in formatter; here just log
        self.root_logger.log(lvl, msg, extra=extra)
        self._record_metrics(lvl)
        # push to follow buffer as plain text (we will apply simple prefix)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if self.config["timestamp"] else ""
        prefix = f"[{level.upper()}] [{ts}] [{module}] " if self.config["module_prefix"] else ""
        follow_line = prefix + msg
        self._buffer_line(follow_line)
        # stream raw follow_line to stdout for immediate tail-like view
        if self.config["follow_build"]:
            # colorize follow_line if requested (we'll add naive color based on level)
            if self.config["follow_colorize"]:
                color = ANSI["WHITE"]
                if lvl >= logging.ERROR:
                    color = ANSI["RED"]
                elif lvl >= logging.WARNING:
                    color = ANSI["YELLOW"]
                elif lvl == logging.DEBUG:
                    color = ANSI["GRAY"]
                else:
                    # INFO or others
                    color = ANSI["WHITE"]
                oline = f"{color}{follow_line}{ANSI['RESET']}"
            else:
                oline = follow_line
            self._stream_to_follow(oline)

    def end_build_log(self, success: bool = True):
        """
        End current build log: produce summary and clear active flag.
        """
        with self._follow_lock:
            if not self._follow_active:
                return
            duration = 0.0
            if self._build_start_time:
                duration = time.time() - self._build_start_time
            pkg = self._current_build_meta.get("pkg", "<unknown>")
            status = "SUCCESS" if success else "FAILURE"
            summary = f"[BUILD END] [{status}] {pkg} (duration: {duration:.2f}s)"
            self.root_logger.info(summary, extra={"rquest_module": "buildsystem"})
            self._buffer_line(summary)
            if self.config["follow_build"]:
                if self.config["follow_colorize"]:
                    color = ANSI["GREEN"] if success else ANSI["RED"]
                    self._stream_to_follow(f"{color}{summary}{ANSI['RESET']}")
                else:
                    self._stream_to_follow(summary)
            # cleanup
            self._follow_active = False
            self._current_build_meta = {}
            self._build_start_time = None

    def get_follow_buffer(self) -> str:
        """
        Return the current follow buffer as a single string (joined by newlines).
        Useful for UI or dumping recent build output.
        """
        with self._follow_lock:
            return "\n".join(list(self._follow_buffer))

    # ---- Exception handling & alerts ----

    def _handle_uncaught_exception(self, exc_type, exc_value, exc_tb):
        try:
            tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            msg = f"Uncaught exception: {exc_value}\n{tb}"
            self.root_logger.error(msg, extra={"rquest_module": "system"})
            self._record_metrics(logging.ERROR)
            # alerts if configured
            try:
                if self.config.get("alerts", {}).get("enable", False):
                    self._dispatch_alert("Uncaught exception", msg, level="error")
            except Exception:
                pass
        finally:
            # fallback to default behaviour
            sys.__excepthook__(exc_type, exc_value, exc_tb)

    def _dispatch_alert(self, title: str, body: str, level: str = "error"):
        alerts = self.config.get("alerts", {}) or {}
        if not alerts.get("enable", False):
            return
        # Desktop notify
        dest = alerts.get("on_error") if level == "error" else alerts.get("on_critical")
        if dest == "desktop":
            try:
                # try notify-send on Linux
                import shutil, subprocess
                if shutil.which("notify-send"):
                    subprocess.run(["notify-send", f"rquest: {title}", body[:200]], check=False)
            except Exception:
                pass
        # Email (very basic)
        if dest == "email" or alerts.get("on_critical") == "email":
            try:
                smtp_cfg = alerts.get("smtp", {})
                # smtp_cfg expected keys: host, port, user, password, to
                host = smtp_cfg.get("host")
                port = smtp_cfg.get("port", 25)
                user = smtp_cfg.get("user")
                password = smtp_cfg.get("password")
                to = smtp_cfg.get("to")
                if host and to:
                    import smtplib
                    from email.message import EmailMessage

                    msg = EmailMessage()
                    msg["Subject"] = f"[rquest alert] {title}"
                    msg["From"] = user or f"rquest@{host}"
                    msg["To"] = to
                    msg.set_content(body)
                    s = smtplib.SMTP(host, port, timeout=10)
                    try:
                        s.ehlo()
                        if s.has_extn("STARTTLS"):
                            s.starttls()
                            s.ehlo()
                        if user and password:
                            s.login(user, password)
                        s.send_message(msg)
                    finally:
                        try:
                            s.quit()
                        except Exception:
                            pass
            except Exception:
                # silently ignore alert failures
                pass

# ---- Module-level convenience functions (public API) ----

_manager = RquestLogger()


def get_logger(module_name: str) -> logging.Logger:
    """
    Return a logger to be used by modules.
    Example:
      logger = get_logger("db")
      logger.info("message")
    """
    return _manager.get_logger(module_name)


def start_build_log(pkg_name: str):
    _manager.start_build_log(pkg_name)


def stream_build_output(line: str, level: str = "INFO", module: Optional[str] = "buildsystem"):
    _manager.stream_build_output(line, level=level, module=module)


def end_build_log(success: bool = True):
    _manager.end_build_log(success)


def set_level(level_name: str):
    _manager.set_level(level_name)


def add_handler(handler: logging.Handler):
    _manager.add_handler(handler)


def get_metrics() -> Dict[str, int]:
    return _manager.get_metrics()


def get_follow_buffer_text() -> str:
    return _manager.get_follow_buffer()


# ---- Convenience: wrap logging methods for quick use ----
def debug(msg: str, module: str = "misc"):
    get_logger(module).debug(msg)


def info(msg: str, module: str = "misc"):
    get_logger(module).info(msg)


def warning(msg: str, module: str = "misc"):
    get_logger(module).warning(msg)


def error(msg: str, module: str = "misc"):
    get_logger(module).error(msg)


# If this module is run as __main__, demonstrate basic usage
if __name__ == "__main__":
    # Demo: print some logs and simulate a build
    demo_logger = get_logger("demo")
    demo_logger.info("Demo starting...")
    start_build_log("example-pkg-1.2.3")
    stream_build_output("./configure --prefix=/usr", "INFO", module="buildsystem")
    stream_build_output("checking for gcc... found", "INFO", module="buildsystem")
    stream_build_output("warning: deprecated API used", "WARNING", module="buildsystem")
    stream_build_output("compilation error: undefined reference", "ERROR", module="buildsystem")
    stream_build_output("linking...", "INFO", module="buildsystem")
    end_build_log(success=False)
    demo_logger.info("Demo finished.")
