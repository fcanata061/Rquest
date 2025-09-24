"""
config.py - Sistema de configuração central do Rquest
Com suporte a YAML/JSON, validação, reload dinâmico e integração com todos os módulos.
"""

import os
import sys
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Callable

# YAML loader prioritário
try:
    import ruamel.yaml as ryaml
    _yaml = ryaml.YAML()
    _yaml.preserve_quotes = True
except ImportError:
    try:
        import yaml as _yaml
    except ImportError:
        _yaml = None

# Watchdog (melhor que polling)
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    _watchdog = True
except ImportError:
    _watchdog = False

# ---------------------------------------------------
# Estrutura Default
# ---------------------------------------------------
_DEFAULT_CONFIG: Dict[str, Any] = {
    "logging": {"level": "INFO", "file": None},
    "database": {"path": "~/.rquest/db.sqlite"},
    "build": {"jobs": 4, "cache_dir": "~/.rquest/build-cache"},
    "upgrade": {"strategy": "safe", "parallelism": 2, "snapshot": {"enabled": True, "path": "~/.rquest/snapshots"}},
    "repos": {"local": "~/.rquest/repos", "remotes": ["https://example.com/repo.git"]},
    "meta": {"meta_cache_dir": "~/.rquest/meta-cache"},
    "pkgtool": {"cache_dir": "~/.rquest/pkg-cache"},
    "toolchain": {"default": "gcc"},
    "notifier": {"enabled": True, "method": "log"}
}

# Config em memória
_CONFIG: Dict[str, Any] = {}
_CONFIG_PATH: Optional[str] = None
_WATCHERS: list[Callable[[Dict[str, Any]], None]] = []
_LOCK = threading.Lock()

_logger = logging.getLogger("rquest.config")

# ---------------------------------------------------
# Helpers
# ---------------------------------------------------
def _expand_path(path: str) -> str:
    return os.path.abspath(os.path.expandvars(os.path.expanduser(path)))


def _normalize_paths(cfg: Dict[str, Any]) -> None:
    """Normaliza todos os campos que são paths."""
    path_fields = [
        ("database", "path"),
        ("repos", "local"),
        ("build", "cache_dir"),
        ("upgrade", "snapshot", "path"),
        ("logging", "file"),
        ("meta", "meta_cache_dir"),
        ("pkgtool", "cache_dir"),
    ]
    for keys in path_fields:
        ref = cfg
        for k in keys[:-1]:
            ref = ref.get(k, {})
        last = keys[-1]
        if last in ref and ref[last]:
            ref[last] = _expand_path(ref[last])


def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Merge recursivo com override de b em a."""
    result = dict(a)
    for k, v in b.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _load_file(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        if _yaml:
            return _yaml.load(text)
        return json.loads(text)
    except Exception as e:
        _logger.error(f"Erro ao carregar config {path}: {e}", exc_info=True)
        return None


def _validate(cfg: Dict[str, Any]) -> None:
    """Valida tipos básicos e valores."""
    # Logging
    level = cfg["logging"].get("level", "INFO")
    if level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        _logger.warning(f"Nível de log inválido: {level}, ajustando para INFO")
        cfg["logging"]["level"] = "INFO"

    # Build
    if cfg["build"]["jobs"] < 1:
        _logger.warning("build.jobs < 1, ajustando para 1")
        cfg["build"]["jobs"] = 1

    # Upgrade
    if cfg["upgrade"]["parallelism"] < 1:
        _logger.warning("upgrade.parallelism < 1, ajustando para 1")
        cfg["upgrade"]["parallelism"] = 1

    # Repos
    if not cfg["repos"].get("local"):
        _logger.warning("repos.local não definido, ajustando para ~/.rquest/repos")
        cfg["repos"]["local"] = _expand_path("~/.rquest/repos")

    # Snapshot
    if cfg["upgrade"]["snapshot"]["enabled"]:
        snap_path = cfg["upgrade"]["snapshot"]["path"]
        parent = Path(snap_path).parent
        if not os.access(parent, os.W_OK):
            _logger.warning(f"Sem permissão de escrita em {parent}, desativando snapshots")
            cfg["upgrade"]["snapshot"]["enabled"] = False


# ---------------------------------------------------
# API pública
# ---------------------------------------------------
def load(explicit_path: Optional[str] = None) -> Dict[str, Any]:
    """Carrega configuração do sistema/usuário/explicit e mescla com defaults."""
    global _CONFIG, _CONFIG_PATH

    cfg = dict(_DEFAULT_CONFIG)
    search_paths = [
        "/usr/lib/rquest/config.yaml",
        "/etc/rquest/config.yaml",
        os.path.expanduser("~/.config/rquest/config.yaml"),
    ]
    if explicit_path:
        search_paths.insert(0, explicit_path)

    for path in search_paths:
        data = _load_file(path)
        if data:
            cfg = _deep_merge(cfg, data)
            _CONFIG_PATH = path

    _normalize_paths(cfg)
    _validate(cfg)

    with _LOCK:
        _CONFIG = cfg
    return cfg


def get_config() -> Dict[str, Any]:
    with _LOCK:
        return dict(_CONFIG)


def get(path: str, default: Any = None) -> Any:
    parts = path.split(".")
    ref = get_config()
    for p in parts:
        if isinstance(ref, dict) and p in ref:
            ref = ref[p]
        else:
            return default
    return ref


def save(path: Optional[str] = None) -> None:
    cfg = get_config()
    out_path = path or _CONFIG_PATH or os.path.expanduser("~/.config/rquest/config.yaml")

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            if _yaml:
                _yaml.dump(cfg, f)
            else:
                json.dump(cfg, f, indent=2)
    except Exception as e:
        _logger.error(f"Erro ao salvar config em {out_path}: {e}")


def watch(callback: Callable[[Dict[str, Any]], None]) -> None:
    """Adiciona callback chamado quando config muda."""
    _WATCHERS.append(callback)


def _notify_watchers(cfg: Dict[str, Any]) -> None:
    for cb in _WATCHERS:
        try:
            cb(cfg)
        except Exception:
            _logger.exception("Erro em watcher de config")


# ---------------------------------------------------
# Watcher de mudanças
# ---------------------------------------------------
class _ConfigEventHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and event.src_path == _CONFIG_PATH:
            _logger.info("Config alterado, recarregando...")
            new_cfg = load(_CONFIG_PATH)
            _notify_watchers(new_cfg)


def _start_watcher():
    if not _CONFIG_PATH:
        return
    if _watchdog:
        observer = Observer()
        handler = _ConfigEventHandler()
        observer.schedule(handler, Path(_CONFIG_PATH).parent, recursive=False)
        observer.daemon = True
        observer.start()
    else:
        # fallback para polling
        def _poll():
            last_mtime = None
            while True:
                try:
                    if os.path.exists(_CONFIG_PATH):
                        mtime = os.path.getmtime(_CONFIG_PATH)
                        if last_mtime and mtime != last_mtime:
                            _logger.info("Config alterado, recarregando...")
                            new_cfg = load(_CONFIG_PATH)
                            _notify_watchers(new_cfg)
                        last_mtime = mtime
                except Exception:
                    pass
                time.sleep(2)
        t = threading.Thread(target=_poll, daemon=True)
        t.start()


# ---------------------------------------------------
# Inicialização automática
# ---------------------------------------------------
try:
    load()
except Exception:
    _logger.exception("Falha ao carregar config inicial")
try:
    logging.basicConfig(level=get("logging.level", "INFO"))
except Exception:
    _logger.exception("Falha ao iniciar logging")

_start_watcher()
