"""
modules/logging.py

Refatorado para integração com modules/config.py.
Mantém API pública:
 - get_logger(name, module_prefix=None)
 - start_build_log(build_id)
 - stream_build_output(build_id, line)
 - end_build_log(build_id)
 - set_level(level)
 - add_handler(handler)
 - get_metrics()

Características:
 - usa config.get(...) para configurar (logging.level, logging.file, logging.json,
   logging.console, logging.max_bytes, logging.backups, logging.build_logs_dir)
 - reconfigura handlers automaticamente quando config mudar (usa config.watch)
 - handlers thread-safe, metrics thread-safe, buffers de build thread-safe
 - JSON file handler com rotação (via RotatingFileHandler + JsonFormatter)
"""

from __future__ import annotations

import logging
import logging.handlers
import json
import os
import sys
import threading
import time
from typing import Dict, Optional, List, Any

# integracao com o config refatorado
from . import config

# -----------------------
# Estado global e locks
# -----------------------
_HANDLER_LOCK = threading.RLock()
_handlers: List[logging.Handler] = []

_metrics: Dict[str, int] = {"debug": 0, "info": 0, "warning": 0, "error": 0, "critical": 0}
_metrics_lock = threading.Lock()

_build_buffers: Dict[str, List[str]] = {}
_build_files: Dict[str, Any] = {}  # file handles
_build_lock = threading.Lock()

_loggers: Dict[str, logging.Logger] = {}
_logger = logging.getLogger("rquest.logging")

# watcher id para reconfig
_watcher_id: Optional[str] = None

# defaults locais caso config esteja vazia
_FALLBACK = {
    "level": "INFO",
    "file": None,
    "json": False,
    "console": True,
    "max_bytes": 0,
    "backups": 0,
    "build_logs_dir": None,
}

# -----------------------
# Formatters e Handlers
# -----------------------


class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[90m",  # bright black / gray
        logging.INFO: "\033[36m",   # cyan
        logging.WARNING: "\033[33m",# yellow
        logging.ERROR: "\033[31m",  # red
        logging.CRITICAL: "\033[41m" # red background
    }
    RESET = "\033[0m"

    def __init__(self):
        super().__init__("%(asctime)s %(levelname)s [%(name)s] %(message)s", "%H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        # module_prefix preferred from record.extra if provided (LoggerAdapter)
        module_prefix = getattr(record, "module_prefix", None)
        msg = super().format(record)
        color = self.COLORS.get(record.levelno, "")
        prefix = f"[{module_prefix}] " if module_prefix else ""
        return f"{color}{prefix}{msg}{self.RESET}"


class JsonFormatter(logging.Formatter):
    """
    Gera uma linha JSON por record (newline-delimited JSON).
    """
    def format(self, record: logging.LogRecord) -> str:
        obj = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # incluir module_prefix se existir
        mp = getattr(record, "module_prefix", None)
        if mp:
            obj["module_prefix"] = mp
        return json.dumps(obj, ensure_ascii=False)


class MetricsHandler(logging.Handler):
    """
    Handler leve que atualiza contadores em _metrics.
    Deve ser adicionado ao root logger para contar todas mensagens.
    """
    def emit(self, record: logging.LogRecord):
        level = record.levelname.lower()
        with _metrics_lock:
            _metrics[level] = _metrics.get(level, 0) + 1


# -----------------------
# Helpers de (re)inicializacao
# -----------------------


def _get_cfg() -> Dict[str, Any]:
    # Obter sub-config "logging" com fallback
    raw = config.get("logging", {})
    if not isinstance(raw, dict):
        raw = {}
    merged = dict(_FALLBACK)
    merged.update(raw)
    return merged


def _close_and_remove_handlers():
    with _HANDLER_LOCK:
        root = logging.getLogger()
        for h in list(_handlers):
            try:
                root.removeHandler(h)
            except Exception:
                pass
            try:
                h.close()
            except Exception:
                pass
        _handlers.clear()


def _init_handlers():
    """
    Inicializa (ou reinicializa) handlers a partir do config.
    Essa função pode ser chamada repetidas vezes (fecha handlers anteriores).
    """
    with _HANDLER_LOCK:
        cfg = _get_cfg()
        _close_and_remove_handlers()

        handlers: List[logging.Handler] = []

        level_name = str(cfg.get("level", "INFO")).upper()
        numeric_level = getattr(logging, level_name, logging.INFO)

        # Console handler
        if bool(cfg.get("console", True)):
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(ColorFormatter())
            handlers.append(ch)

        logfile = cfg.get("file")
        json_enabled = bool(cfg.get("json", False))
        max_bytes = int(cfg.get("max_bytes", 0) or 0)
        backups = int(cfg.get("backups", 0) or 0)

        if logfile:
            logfile = os.path.abspath(os.path.expanduser(logfile))
            os.makedirs(os.path.dirname(logfile), exist_ok=True)
            if max_bytes and max_bytes > 0:
                fh = logging.handlers.RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=backups, encoding="utf-8")
            else:
                fh = logging.FileHandler(logfile, encoding="utf-8")
            if json_enabled:
                fh.setFormatter(JsonFormatter())
            else:
                fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
            handlers.append(fh)

        # Always attach metrics handler (counts across all handlers)
        metrics_h = MetricsHandler()
        # metrics handler should be last so it won't interfere formatting
        handlers.append(metrics_h)

        # Apply to root logger
        root = logging.getLogger()
        root.setLevel(numeric_level)
        for h in handlers:
            root.addHandler(h)

        _handlers.extend(handlers)
        _logger.debug("Handlers initialized: level=%s file=%s json=%s", level_name, logfile, json_enabled)


def _on_config_change():
    """
    Callback para config.watch: reinicializa handlers.
    """
    try:
        _logger.debug("Config change detected — reinitializing logging handlers")
        _init_handlers()
    except Exception as e:
        # Não deixar propagar
        _logger.exception("Error reinitializing logging handlers: %s", e)


# -----------------------
# API pública
# -----------------------


def get_logger(name: str, module_prefix: Optional[str] = None) -> logging.Logger:
    """
    Retorna um logger. Se module_prefix for fornecido, ele aparecerá nos formatters que o suportam.
    Caching por (name, module_prefix) para preservar comportamento consistente.
    """
    key = f"{name}::{module_prefix or ''}"
    if key in _loggers:
        return _loggers[key]

    # garantir handlers inicializados
    _init_handlers()

    base_logger = logging.getLogger(name)
    # propague para root (comportamento padrão)
    base_logger.propagate = True
    # set level igual ao root por padrão
    base_logger.setLevel(logging.getLogger().level)

    # criar adapter para incluir module_prefix no record
    if module_prefix:
        adapter = logging.LoggerAdapter(base_logger, {"module_prefix": module_prefix})
        _loggers[key] = adapter  # LoggerAdapter tem mesma interface de Logger
        return adapter

    _loggers[key] = base_logger
    return base_logger


def set_level(level: str):
    """Ajusta o level do root logger e de loggers cacheados."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(numeric)
    for lg in list(_loggers.values()):
        try:
            # LoggerAdapter tem logger; compatível
            if isinstance(lg, logging.LoggerAdapter):
                lg.logger.setLevel(numeric)
            else:
                lg.setLevel(numeric)
        except Exception:
            pass
    _logger.info("Logging level set to %s", level)


def add_handler(handler: logging.Handler):
    """Adiciona handler ao root logger e à lista interna (thread-safe)."""
    with _HANDLER_LOCK:
        root = logging.getLogger()
        root.addHandler(handler)
        _handlers.append(handler)


def start_build_log(build_id: str):
    """Inicializa buffer (e arquivo, se configurado) para um build."""
    with _build_lock:
        if build_id in _build_buffers:
            # já iniciado; sobrescrever buffer
            _build_buffers[build_id] = []
        else:
            _build_buffers[build_id] = []

        # abrir arquivo se configurado
        build_dir = config.get("logging.build_logs_dir", None)
        if build_dir:
            build_dir = os.path.abspath(os.path.expanduser(build_dir))
            os.makedirs(build_dir, exist_ok=True)
            filepath = os.path.join(build_dir, f"build-{build_id}.log")
            try:
                fh = open(filepath, "a", encoding="utf-8")
                _build_files[build_id] = fh
            except Exception as e:
                # logamos mas não interrompemos a execução
                get_logger(__name__).warning("Não foi possível abrir build log %s: %s", filepath, e)


def stream_build_output(build_id: str, line: str):
    """
    Escreve linha no logger de build (INFO) e armazena no buffer/arquivo (se configurado).
    """
    logger = get_logger(f"build.{build_id}", module_prefix="build")
    # manda para logging (isso irá percorrer os handlers configurados)
    logger.info(line.rstrip("\n"))

    with _build_lock:
        if build_id in _build_buffers:
            _build_buffers[build_id].append(line)
        # escreve no arquivo se existir
        fh = _build_files.get(build_id)
        if fh:
            try:
                fh.write(line)
                fh.flush()
            except Exception:
                # não deixamos explodir
                try:
                    get_logger(__name__).warning("Erro escrevendo build log para %s", build_id)
                except Exception:
                    pass


def end_build_log(build_id: str):
    """
    Fecha/encerra o log do build. Não retorna nada (compatibilidade).
    Se existir arquivo associado, ele será fechado.
    """
    with _build_lock:
        fh = _build_files.pop(build_id, None)
        if fh:
            try:
                fh.flush()
            except Exception:
                pass
            try:
                fh.close()
            except Exception:
                pass
        # mantemos buffer em memória até que outro módulo solicite com get_build_log


def get_build_log(build_id: str, clear_after: bool = False) -> Optional[str]:
    """
    Opcional: retorna o conteúdo em memória do log do build (concat das linhas).
    Se clear_after=True, limpa o buffer em memória.
    """
    with _build_lock:
        if build_id not in _build_buffers:
            return None
        content = "".join(_build_buffers[build_id])
        if clear_after:
            _build_buffers[build_id] = []
        return content


def get_metrics() -> Dict[str, int]:
    """Retorna cópia dos contadores de métricas (debug/info/warning/error/critical)."""
    with _metrics_lock:
        return dict(_metrics)


# -----------------------
# Inicializacao do modulo
# -----------------------

# Inicializa handlers à importação
try:
    _init_handlers()
except Exception:
    # evitar quebrar importadores
    try:
        _logger.exception("Erro inicializando handlers na importação")
    except Exception:
        pass

# registrar watcher para reconfiguração automática ao mudar config (se config.watch existir)
try:
    # registra apenas uma vez
    if _watcher_id is None and hasattr(config, "watch"):
        # watch(None, callback, interval) -> observa todos os arquivos carregados
        _watcher_id = config.watch(None, _on_config_change, interval=1.0)
        _logger.debug("Registered config watcher id=%s", _watcher_id)
except Exception:
    # falha em registrar watcher não deve quebrar nada
    try:
        _logger.exception("Erro registrando config watcher")
    except Exception:
        pass
