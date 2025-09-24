# rquest1.0/modules/logging.py
"""
Revised logging module for Rquest v1.0

Principais correções e melhorias:
- Integração direta com rquest1.0.modules.config (usa config.get("logging"))
- Evita duplicação de parsing YAML/JSON (config centraliza isso)
- Handler JSON rotativo seguro (usa RotatingFileHandler internamente, escrita atômica por registro)
- Console handler com colorização apenas para terminal; arquivo/JSON sem cor
- Thread-safe (locks nos pontos de escrita onde necessário)
- Não remove handlers de forma agressiva; aplica reconfiguração controlada
- Opção para habilitar excepthook global via config (desligada por padrão)
- API de integração para outros módulos (set_logger, integrate_module)
- Funções auxiliares: enable_http_debug, start_build_log/stream/end_build_log
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

# Import central config module (assume o arquivo config.py já revisionado existe)
from rquest1.0.modules import config

# Default constants
_DEFAULT_LOGGER_NAME = "rquest"
_DEFAULT_LEVEL = logging.INFO
_DEFAULT_JSON_ROTATE_BYTES = 10 * 1024 * 1024  # 10 MB
_DEFAULT_JSON_BACKUP_COUNT = 5

# Internal singleton manager
_manager_lock = threading.RLock()
_manager: Optional["LoggingManager"] = None


# -------------------------
# Helpers and formatters
# -------------------------
class _ColorFormatter(logging.Formatter):
    """
    Colorizes levelname for console output. Does NOT alter message content,
    so file handlers remain plain.
    """
    COLORS = {
        "DEBUG": "\033[37m",   # light gray
        "INFO": "\033[36m",    # cyan
        "WARNING": "\033[33m", # yellow
        "ERROR": "\033[31m",   # red
        "CRITICAL": "\033[41m",# red background
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        level = record.levelname
        prefix = self.COLORS.get(level, "") 
        suffix = self.RESET if prefix else ""
        # format message using base class (will use format string provided)
        original = super().format(record)
        # Only colorize if the stream supports it (basic heuristic)
        try:
            isatty = hasattr(record, "stream") and getattr(record, "stream", None) and getattr(record.stream, "isatty", lambda: False)()
        except Exception:
            isatty = False
        if not isatty:
            # fallback: try stdout isatty
            isatty = sys.stdout.isatty()
        if isatty and prefix:
            return f"{prefix}{original}{suffix}"
        return original


class _JsonFormatter(logging.Formatter):
    """Format log record as JSON line (no pretty)."""
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "funcName": record.funcName,
            "lineNo": record.lineno,
            "message": record.getMessage(),
        }
        # Include extra fields if present
        if hasattr(record, "extra_data") and isinstance(record.extra_data, dict):
            payload.update(record.extra_data)
        return json.dumps(payload, ensure_ascii=False)


# -------------------------
# JSON Rotating Handler
# -------------------------
class JsonRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """
    Rotating file handler that writes one JSON-line per record.
    Subclasses RotatingFileHandler to reuse rotation logic. Ensures thread-safety
    when serializing each record (RotatingFileHandler already has internal locking,
    but we serialize and write explicitly to avoid double-formatting).
    """
    def __init__(
        self,
        filename: str,
        maxBytes: int = _DEFAULT_JSON_ROTATE_BYTES,
        backupCount: int = _DEFAULT_JSON_BACKUP_COUNT,
        encoding: Optional[str] = "utf-8",
    ):
        # ensure directory exists
        dirname = os.path.dirname(os.path.abspath(filename))
        if dirname and not os.path.exists(dirname):
            try:
                os.makedirs(dirname, exist_ok=True)
            except Exception:
                # If cannot create, raise to notify caller
                raise
        super().__init__(filename, maxBytes=maxBytes, backupCount=backupCount, encoding=encoding)
        # Use JSON formatter
        self.setFormatter(_JsonFormatter())
        # Use the internal lock provided by RotatingFileHandler's StreamHandler
        self._write_lock = threading.RLock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            s = self.format(record)
            with self._write_lock:
                # stream is handled by parent; call parent methods to handle rotation
                if self.shouldRollover(record):  # type: ignore[arg-type]
                    self.doRollover()
                stream = self.stream
                if stream is None:
                    stream = self._open()
                stream.write(s + "\n")
                stream.flush()
        except Exception:
            self.handleError(record)


# -------------------------
# Manager
# -------------------------
class LoggingManager:
    """
    Single manager that configures and exposes the rquest logger.
    Use get_manager() to obtain the singleton.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
        self._logger.setLevel(_DEFAULT_LEVEL)
        self._console_handler: Optional[logging.StreamHandler] = None
        self._json_handler: Optional[JsonRotatingFileHandler] = None
        self._metrics: Dict[str, int] = {}
        self._follow_active = False
        self._follow_buffer: list[str] = []
        self._build_meta: Dict[str, Any] = {}
        # Save previous global excepthook to restore if we disable
        self._prev_excepthook = sys.excepthook
        # Initialize from config if available
        self._apply_config(config.get("logging") if config else None)
        # Register config reload callback so logging reconfigures on config.load()
        try:
            config.start_watcher()  # safe no-op if watchdog missing; also ensures config module imported
        except Exception:
            # ignore
            pass
        try:
            # We use config.integrate_with_module style callback: config has _WATCH_CALLBACKS internal
            config.start_watcher  # just ensure attribute exists
        except Exception:
            pass
        # Register a callback in the config module to reconfigure logging when config reloads:
        try:
            config._WATCH_CALLBACKS.append(self._on_config_reload)
        except Exception:
            # If WATCH_CALLBACKS not available, we still allow manual reconfigure by calling manager.reconfigure()
            pass
        # Let config use this logger for its internal logs
        config.set_logger(self._logger)

    # -------------------------
    # Internal apply / reconfigure
    # -------------------------
    def _apply_config(self, cfg: Optional[Dict[str, Any]]) -> None:
        """
        Apply logging section from config. This is safe to call multiple times.
        Expected structure (example):
        logging:
            level: DEBUG
            console: true
            console_timestamp: true
            json_file: /var/log/rquest/log.json
            json_max_bytes: 10485760
            json_backup_count: 3
            unhandled_exceptions: false
        """
        with self._lock:
            try:
                if not cfg or not isinstance(cfg, dict):
                    cfg = {}
                # Level
                level = cfg.get("level", None)
                if level is not None:
                    try:
                        self._logger.setLevel(level if isinstance(level, int) else getattr(logging, str(level).upper()))
                    except Exception:
                        self._logger.setLevel(_DEFAULT_LEVEL)
                # Console handler
                console_cfg = cfg.get("console", True)
                if console_cfg:
                    if self._console_handler is None:
                        ch = logging.StreamHandler(sys.stdout)
                        fmt_parts = []
                        if cfg.get("console_timestamp", True):
                            fmt_parts.append("%(asctime)s")
                        fmt_parts.append("%(levelname)s")
                        if cfg.get("module_prefix", True):
                            fmt_parts.append("[%(name)s]")
                        fmt_parts.append("%(message)s")
                        fmt = " ".join(fmt_parts)
                        # Color formatter wraps formatting if terminal supports it
                        formatter = _ColorFormatter(fmt)
                        ch.setFormatter(formatter)
                        self._logger.addHandler(ch)
                        self._console_handler = ch
                    else:
                        # keep existing handler but maybe update format
                        pass
                else:
                    # remove console handler if present
                    if self._console_handler:
                        try:
                            self._logger.removeHandler(self._console_handler)
                        except Exception:
                            pass
                        self._console_handler = None

                # JSON file handler
                json_path = cfg.get("json_file") or cfg.get("json")
                if json_path:
                    try:
                        maxb = int(cfg.get("json_max_bytes", _DEFAULT_JSON_ROTATE_BYTES))
                    except Exception:
                        maxb = _DEFAULT_JSON_ROTATE_BYTES
                    try:
                        bc = int(cfg.get("json_backup_count", _DEFAULT_JSON_BACKUP_COUNT))
                    except Exception:
                        bc = _DEFAULT_JSON_BACKUP_COUNT

                    if self._json_handler is None or (self._json_handler.baseFilename != os.path.abspath(str(json_path))):
                        # remove old json handler
                        if self._json_handler:
                            try:
                                self._logger.removeHandler(self._json_handler)
                            except Exception:
                                pass
                            self._json_handler = None
                        # create new
                        handler = JsonRotatingFileHandler(str(json_path), maxBytes=maxb, backupCount=bc)
                        handler.setLevel(cfg.get("json_level", logging.INFO))
                        self._logger.addHandler(handler)
                        self._json_handler = handler
                    else:
                        # update rotation params if necessary (RotatingFileHandler does not expose easy change; recreate if needed)
                        pass
                else:
                    # remove if present
                    if self._json_handler:
                        try:
                            self._logger.removeHandler(self._json_handler)
                        except Exception:
                            pass
                        self._json_handler = None

                # Unhandled exceptions hook
                unhandled = bool(cfg.get("unhandled_exceptions", False))
                if unhandled:
                    # set our hook (idempotent)
                    sys.excepthook = self._handle_uncaught_exception
                else:
                    # restore previous if we changed it
                    try:
                        sys.excepthook = self._prev_excepthook
                    except Exception:
                        pass

                # Allow external modules to fetch logger via config.set_logger
                config.set_logger(self._logger)
            except Exception:
                # Ensure that manager remains usable even if config has bad types
                logging.getLogger(__name__).exception("Failed to apply logging config")

    def _on_config_reload(self):
        """Callback triggered by config watcher to re-apply logging configuration."""
        try:
            cfg = config.get("logging")
            self._apply_config(cfg)
            self._logger.info("Logging reconfigured from updated config")
        except Exception:
            self._logger.exception("Error when reloading logging config")

    # -------------------------
    # Public helpers
    # -------------------------
    def get_logger(self) -> logging.Logger:
        return self._logger

    def reconfigure(self) -> None:
        """Force reconfiguration from config module now."""
        with self._lock:
            self._apply_config(config.get("logging"))

    def set_level(self, level: int | str) -> None:
        with self._lock:
            try:
                lvl = level if isinstance(level, int) else getattr(logging, str(level).upper())
            except Exception:
                lvl = _DEFAULT_LEVEL
            self._logger.setLevel(lvl)

    def enable_http_debug(self) -> None:
        """Enable detailed urllib3/http.client debugging output."""
        import http.client as http_client

        http_client.HTTPConnection.debuglevel = 1
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        ulog = logging.getLogger("urllib3")
        ulog.setLevel(logging.DEBUG)
        ulog.propagate = True
        self._logger.debug("HTTP debug enabled")

    # -------------------------
    # Exception handling
    # -------------------------
    def _handle_uncaught_exception(self, exc_type, exc_value, exc_tb):
        """Log an uncaught exception; then call previous excepthook."""
        try:
            self._logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_tb))
        except Exception:
            # fallback to stderr if logger broken
            try:
                print("Uncaught exception", file=sys.stderr)
            except Exception:
                pass
        try:
            # call previous to preserve behavior
            if callable(self._prev_excepthook):
                self._prev_excepthook(exc_type, exc_value, exc_tb)
        except Exception:
            pass

    # -------------------------
    # Build streaming API
    # -------------------------
    def start_build_log(self, build_id: str, meta: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self._follow_active = True
            self._follow_buffer = []
            self._build_meta = dict(meta or {})
            self._build_meta["build_id"] = build_id
            self._logger.info("Starting build log: %s", build_id)

    def stream_build_output(self, message: str, level: int = logging.INFO, extra: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            # write to logger normally
            self._logger.log(level, message, extra={"extra_data": extra} if extra else None)
            if self._follow_active:
                ts = datetime.utcnow().isoformat() + "Z"
                line = f"{ts} [{logging.getLevelName(level)}] {message}"
                self._follow_buffer.append(line)
                # keep buffer bounded to avoid memory blowup
                if len(self._follow_buffer) > 10000:
                    self._follow_buffer = self._follow_buffer[-5000:]

    def end_build_log(self, summary: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self._lock:
            # produce final summary and clear buffer
            summary = summary or {}
            summary_record = {
                "meta": self._build_meta.copy(),
                "summary": summary,
                "lines": list(self._follow_buffer),
            }
            # log the summary as structured json (if json handler present)
            self._logger.info("Build finished: %s", self._build_meta.get("build_id"), extra={"extra_data": {"build_summary": summary}})
            self._follow_active = False
            self._follow_buffer = []
            self._build_meta = {}
            return summary_record

    # -------------------------
    # Integration helpers for other modules
    # -------------------------
    def integrate_module(self, module_obj: Any, module_name: Optional[str] = None) -> None:
        """
        Inject logger and module-specific config into another module.
        Will call module_obj.set_logger(logger) and module_obj.set_config(cfg) if present.
        """
        name = module_name or getattr(module_obj, "__name__", None)
        try:
            if hasattr(module_obj, "set_logger") and callable(module_obj.set_logger):
                try:
                    module_obj.set_logger(self._logger)
                except Exception:
                    self._logger.exception("Failed to call set_logger on %s", name)
            # Let config provide module config (it has helper get_module_config)
            if hasattr(module_obj, "set_config") and callable(module_obj.set_config):
                try:
                    cfg_mod = config.get_module_config(name.split(".")[-1]) if name else {}
                    module_obj.set_config(cfg_mod)
                except Exception:
                    self._logger.exception("Failed to call set_config on %s", name)
        except Exception:
            self._logger.exception("Error integrating module %s", name)


# -------------------------
# Module-level access helpers
# -------------------------
def get_manager() -> LoggingManager:
    global _manager
    with _manager_lock:
        if _manager is None:
            _manager = LoggingManager()
        return _manager


def get_logger() -> logging.Logger:
    return get_manager().get_logger()


def reconfigure() -> None:
    get_manager().reconfigure()


def set_level(level: int | str) -> None:
    get_manager().set_level(level)


def enable_http_debug() -> None:
    get_manager().enable_http_debug()


def start_build_log(build_id: str, meta: Optional[Dict[str, Any]] = None) -> None:
    get_manager().start_build_log(build_id, meta)


def stream_build_output(message: str, level: int = logging.INFO, extra: Optional[Dict[str, Any]] = None) -> None:
    get_manager().stream_build_output(message, level, extra)


def end_build_log(summary: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return get_manager().end_build_log(summary)


def integrate_module(module_obj: Any, module_name: Optional[str] = None) -> None:
    get_manager().integrate_module(module_obj, module_name)


# Immediately initialize manager so config.set_logger integration is established early
get_manager()
