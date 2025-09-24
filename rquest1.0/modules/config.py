"""
modules/config.py
Refatoração compatível com a API original do Rquest:
 - Mantém funções públicas: load, reload, get_config, get, save, init_logging, validate, watch, stop_watch
 - Melhorias:
    * thread-safe (threading.Lock)
    * watchers gerenciáveis por id
    * fallback YAML/JSON (PyYAML opcional)
    * validação opcional com jsonschema quando disponível
    * normalização genérica de caminhos (chaves com 'path', 'dir', 'file')
    * logs informativos
"""

from __future__ import annotations

import os
import json
import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

# Optional deps
try:
    import yaml  # PyYAML
    YAML_AVAILABLE = True
except Exception:
    YAML_AVAILABLE = False

try:
    import jsonschema  # optional schema validation
    JSONSCHEMA_AVAILABLE = True
except Exception:
    JSONSCHEMA_AVAILABLE = False

# -----------------------
# Module-level state
# -----------------------
_LOCK = threading.RLock()
_CONFIG: Dict[str, Any] = {}
_DEFAULTS: Dict[str, Any] = {
    # Exemplo de defaults — mantenha consistente com sua versão original
    "database": {"path": "~/rquest/db.sqlite", "timeout": 5},
    "repos": {"local": "~/rquest/repos"},
    "build": {"cache_dir": "~/.rquest/cache", "worker_count": 2},
    "logging": {"level": "INFO", "file": None},
    "snapshot": {"path": "~/rquest/snapshots", "keep": 10},
}
# Lista de paths carregados (em ordem de precedência: primeiro tem menor precedência)
_CONFIG_PATHS_LOADED: List[str] = []

# Watcher management: id -> (path, callback, interval, last_mtime)
_WATCHERS: Dict[str, Tuple[Optional[str], Callable[[], None], float, Optional[float]]] = {}
_WATCH_THREAD: Optional[threading.Thread] = None
_WATCH_THREAD_SHUTDOWN = threading.Event()

# Logger
_logger = logging.getLogger("rquest.config")

# Default user config path
_DEFAULT_USER_CONFIG_PATH = os.path.expanduser("~/.rquest/config.yaml")

# -----------------------
# Utilities
# -----------------------


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge override into base recursively. Returns merged dict (mutates base).
    Lists and non-dict values are replaced (not merged).
    """
    for k, v in override.items():
        if (
            k in base
            and isinstance(base[k], dict)
            and isinstance(v, dict)
        ):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def _is_path_like_key(key: str) -> bool:
    key_low = key.lower()
    return any(s in key_low for s in ("path", "dir", "file"))


def _normalize_paths(obj: Any) -> Any:
    """
    Walks the config structure and expands/absolutifies any string values
    whose keys look like path-like (contain 'path','dir','file').
    Returns a new normalized object (mutates dicts/lists in-place).
    """
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            if isinstance(v, str) and _is_path_like_key(k):
                # expand user and make absolute
                obj[k] = os.path.abspath(os.path.expanduser(v))
            else:
                obj[k] = _normalize_paths(v)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            obj[i] = _normalize_paths(item)
    return obj


def _load_file(path: str) -> Dict[str, Any]:
    """
    Parse YAML if possible, then JSON fallback. Raises on parse error / file error.
    """
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()

    # Try YAML first if available
    if YAML_AVAILABLE:
        try:
            parsed = yaml.safe_load(text)
            if parsed is None:
                parsed = {}
            if not isinstance(parsed, dict):
                raise ValueError(f"Config file {path} did not contain a mapping at top level.")
            return parsed
        except Exception as e:
            # If YAML parsing fails, try JSON as a fallback
            _logger.debug("YAML parse failed for %s: %s — trying JSON fallback", path, e)

    # JSON fallback
    try:
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError(f"Config file {path} did not contain a mapping at top level (json).")
        return parsed
    except Exception as e:
        # raise a helpful error
        raise ValueError(f"Failed to parse config file {path}: {e}")


def _apply_defaults(cfg: Dict[str, Any]) -> Dict[str, Any]:
    base = {}
    _deep_merge(base, _DEFAULTS)
    _deep_merge(base, cfg)
    return _normalize_paths(base)


def _ensure_user_config_dir(path: str):
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


# -----------------------
# Public API
# -----------------------


def load(
    paths: Optional[Iterable[str]] = None,
    explicit_path: Optional[str] = None,
    use_defaults: bool = True,
) -> Dict[str, Any]:
    """
    Carrega configuração seguindo precedência:
       defaults -> system paths -> user paths -> explicit_path
    - paths: iterable adicional de caminhos (serão aplicados com alta precedência)
    - explicit_path: se informado, aplicado como o de maior precedência (final)
    Retorna o dicionário de configuração carregado.
    """
    global _CONFIG, _CONFIG_PATHS_LOADED
    with _LOCK:
        _logger.debug("Loading configuration (paths=%s explicit=%s)", paths, explicit_path)
        merged = {}
        # Start from defaults
        if use_defaults:
            _deep_merge(merged, _DEFAULTS)

        # System-level default locations (example: /etc/rquest/config.yaml)
        system_paths = [
            "/etc/rquest/config.yaml",
            "/etc/rquest/config.json",
            # ... outros caminhos do sistema se desejar
        ]
        candidates: List[str] = []
        # Add system paths first (lower precedence)
        for p in system_paths:
            if os.path.exists(p):
                candidates.append(p)

        # Add provided paths (user-specified) next
        if paths:
            for p in paths:
                if p and os.path.exists(p):
                    candidates.append(p)

        # Add user's config file if exists
        if os.path.exists(_DEFAULT_USER_CONFIG_PATH):
            candidates.append(_DEFAULT_USER_CONFIG_PATH)

        # Finally explicit_path (highest precedence) if provided and exists
        if explicit_path:
            if os.path.exists(explicit_path):
                candidates.append(explicit_path)
            else:
                # if explicit path doesn't exist, we still keep it as target for save; don't attempt to load
                _logger.debug("Explicit config path %s does not exist. Will be target for save.", explicit_path)

        loaded_paths = []
        for p in candidates:
            try:
                parsed = _load_file(p)
                _deep_merge(merged, parsed)
                loaded_paths.append(os.path.abspath(p))
                _logger.info("Loaded config from %s", p)
            except Exception as e:
                _logger.warning("Skipping config %s: %s", p, e)

        # If explicit_path provided but didn't exist and not in loaded_paths,
        # we still register it as intended path for saving later.
        if explicit_path and os.path.abspath(explicit_path) not in loaded_paths:
            # keep it in path list but mark as not loaded (so save knows)
            loaded_paths.append(os.path.abspath(explicit_path))

        # Final normalization
        merged = _normalize_paths(merged)

        # update module state
        _CONFIG = merged
        _CONFIG_PATHS_LOADED = loaded_paths
        _logger.debug("Configuration loaded. Paths: %s", _CONFIG_PATHS_LOADED)
        return _CONFIG.copy()


def get_config() -> Dict[str, Any]:
    """Retorna uma cópia imutável superficial da configuração atual."""
    with _LOCK:
        return _CONFIG.copy()


def get(path: str, default: Any = None) -> Any:
    """
    Acesso por path dot-notated, ex: get("database.path")
    """
    with _LOCK:
        parts = path.split(".") if path else []
        current = _CONFIG
        for p in parts:
            if not isinstance(current, dict) or p not in current:
                return default
            current = current[p]
        return current


def reload() -> Dict[str, Any]:
    """Recarrega a configuração a partir dos caminhos previamente carregados."""
    with _LOCK:
        paths = list(_CONFIG_PATHS_LOADED)
    # Reuse load semantics but preserve defaults usage
    return load(paths=paths, explicit_path=(paths[-1] if paths else None))


def save(path: Optional[str] = None, ensure_dir: bool = True) -> str:
    """
    Salva a configuração atual em disco.
    - path: destino. Se None, salva no último path carregado que seja gravável
            ou no _DEFAULT_USER_CONFIG_PATH.
    - Retorna o caminho salvo.
    """
    with _LOCK:
        cfg_to_save = _CONFIG.copy()
        # Choose path
        candidates = []
        if path:
            candidates.append(path)
        # Prefer last loaded path that is not a system path
        candidates.extend(reversed(_CONFIG_PATHS_LOADED))
        candidates.append(_DEFAULT_USER_CONFIG_PATH)
        chosen = None
        for c in candidates:
            if c is None:
                continue
            parent = os.path.dirname(os.path.abspath(c)) or "."
            if os.access(parent, os.W_OK) or not os.path.exists(parent):
                chosen = os.path.abspath(c)
                break
        if not chosen:
            raise PermissionError("No writable location found to save configuration.")

        # ensure directory exists
        if ensure_dir:
            _ensure_user_config_dir(chosen)

        # serialize as YAML if available, else JSON
        try:
            if YAML_AVAILABLE:
                with open(chosen, "w", encoding="utf-8") as fh:
                    yaml.safe_dump(cfg_to_save, fh, default_flow_style=False, sort_keys=False)
            else:
                with open(chosen, "w", encoding="utf-8") as fh:
                    json.dump(cfg_to_save, fh, indent=2, ensure_ascii=False)
            _logger.info("Configuration saved to %s", chosen)
            # add to loaded paths if not present
            if os.path.abspath(chosen) not in _CONFIG_PATHS_LOADED:
                _CONFIG_PATHS_LOADED.append(os.path.abspath(chosen))
            return chosen
        except Exception as e:
            _logger.exception("Failed to save configuration to %s: %s", chosen, e)
            raise


def init_logging(level: Optional[str] = None, logfile: Optional[str] = None):
    """
    Inicializa logging com base na configuração atual
    """
    with _LOCK:
        cfg = _CONFIG.copy()
    log_cfg = cfg.get("logging", {}) if isinstance(cfg.get("logging", {}), dict) else {}
    level = level or log_cfg.get("level", "INFO")
    logfile = logfile if logfile is not None else log_cfg.get("file")

    numeric_level = getattr(logging, str(level).upper(), logging.INFO)
    handlers = []
    # BasicConfig via handlers
    if logfile:
        logfile = os.path.abspath(os.path.expanduser(logfile))
        _ensure_user_config_dir(logfile)
        handlers.append(logging.FileHandler(logfile))
    else:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(level=numeric_level, handlers=handlers, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    _logger.setLevel(numeric_level)
    _logger.info("Logging initialized (level=%s file=%s)", level, logfile)


def validate(schema: Optional[Dict[str, Any]] = None) -> Tuple[bool, List[str]]:
    """
    Valida a configuração atual.
    - schema: se fornecido, valida com jsonschema (se disponível). Se não disponível, tenta validação manual leve.
    Retorna (ok, list_of_messages). Mensagens podem ser warnings ou erros.
    """
    msgs: List[str] = []
    ok = True
    cfg = get_config()

    # If user supplied schema and jsonschema available, use it
    if schema is not None and JSONSCHEMA_AVAILABLE:
        try:
            jsonschema.validate(instance=cfg, schema=schema)
            _logger.debug("Config valid according to jsonschema.")
        except jsonschema.ValidationError as e:
            ok = False
            msgs.append(f"Schema validation error: {e.message}")
            _logger.warning("Schema validation failed: %s", e)
        return ok, msgs

    # Lightweight validation:
    # - required top-level keys from defaults
    for top_key in _DEFAULTS.keys():
        if top_key not in cfg:
            msgs.append(f"Missing top-level key: {top_key}")
            ok = False

    # Example: ensure worker_count >= 1
    try:
        wc = cfg.get("build", {}).get("worker_count")
        if wc is not None and isinstance(wc, int) and wc < 1:
            msgs.append("build.worker_count must be >= 1")
            ok = False
    except Exception:
        pass

    # Ensure snapshot path parent writable
    try:
        snapshot_path = cfg.get("snapshot", {}).get("path")
        if snapshot_path:
            parent = os.path.dirname(os.path.abspath(snapshot_path)) or "."
            if not os.path.exists(parent):
                try:
                    os.makedirs(parent, exist_ok=True)
                except Exception as e:
                    msgs.append(f"Cannot create parent dir for snapshot.path ({parent}): {e}")
                    ok = False
            elif not os.access(parent, os.W_OK):
                msgs.append(f"Parent dir for snapshot.path is not writable: {parent}")
                ok = False
    except Exception:
        pass

    # Warn if yaml not available
    if not YAML_AVAILABLE:
        msgs.append("PyYAML not available — YAML config files won't be parsed (JSON only).")
        _logger.debug("PyYAML not available.")

    return ok, msgs


# -----------------------
# Watcher implementation
# -----------------------

def _watch_loop(poll_interval: float = 1.0):
    """
    Background thread loop polling mtimes for watched paths.
    """
    _logger.debug("Watcher thread started with interval %.2f", poll_interval)
    while not _WATCH_THREAD_SHUTDOWN.is_set():
        with _LOCK:
            watchers_snapshot = dict(_WATCHERS)
        for wid, (path, callback, interval, last_mtime) in watchers_snapshot.items():
            # If path is None, use all loaded paths (monitor any change among them)
            paths_to_check = []
            if path:
                paths_to_check = [path]
            else:
                with _LOCK:
                    paths_to_check = list(_CONFIG_PATHS_LOADED)

            changed = False
            for p in paths_to_check:
                if not p:
                    continue
                try:
                    if os.path.exists(p):
                        mtime = os.path.getmtime(p)
                        if last_mtime is None or mtime > (last_mtime or 0):
                            # update last_mtime
                            with _LOCK:
                                entry = _WATCHERS.get(wid)
                                if entry:
                                    _WATCHERS[wid] = (entry[0], entry[1], entry[2], mtime)
                            changed = True
                            _logger.debug("Watcher %s detected change in %s", wid, p)
                            break
                except Exception as e:
                    _logger.debug("Watcher %s error checking %s: %s", wid, p, e)
            if changed:
                try:
                    callback()
                except Exception as e:
                    _logger.exception("Watcher callback %s raised exception: %s", wid, e)
        # Sleep minimal interval among watchers or provided default
        with _LOCK:
            intervals = [v[2] for v in _WATCHERS.values()] or [poll_interval]
        sleep_time = min(intervals) if intervals else poll_interval
        # Bound sleep time
        time.sleep(max(0.1, sleep_time))
    _logger.debug("Watcher thread exiting.")


def _ensure_watch_thread_running():
    global _WATCH_THREAD
    with _LOCK:
        if _WATCH_THREAD is None or not _WATCH_THREAD.is_alive():
            _WATCH_THREAD_SHUTDOWN.clear()
            _WATCH_THREAD = threading.Thread(target=_watch_loop, name="rquest-config-watch", daemon=True)
            _WATCH_THREAD.start()
            _logger.debug("Started watch thread.")


def watch(
    path: Optional[str],
    callback: Callable[[], None],
    interval: float = 1.0,
) -> str:
    """
    Registra um watcher e retorna um watcher_id.
    - path: caminho do arquivo a observar. Se None, observa todos os caminhos carregados.
    - callback: função sem argumentos chamada quando arquivo muda.
    - interval: polling interval (segundos). Mínimo 0.1s.
    """
    if interval <= 0:
        interval = 1.0
    with _LOCK:
        watcher_id = str(uuid.uuid4())
        p_abs = os.path.abspath(path) if path else None
        _WATCHERS[watcher_id] = (p_abs, callback, max(0.1, float(interval)), None)
        _logger.debug("Registered watcher %s for path %s", watcher_id, p_abs)
        _ensure_watch_thread_running()
        return watcher_id


def stop_watch(watcher_id: Optional[str] = None):
    """
    Remove watcher; se watcher_id for None, remove todos e parar thread.
    """
    global _WATCH_THREAD
    with _LOCK:
        if watcher_id:
            if watcher_id in _WATCHERS:
                del _WATCHERS[watcher_id]
                _logger.debug("Removed watcher %s", watcher_id)
        else:
            _WATCHERS.clear()
            _logger.debug("Removed all watchers")
        if not _WATCHERS:
            # shutdown thread
            _WATCH_THREAD_SHUTDOWN.set()
            if _WATCH_THREAD is not None:
                _logger.debug("Waiting for watch thread to stop...")
                _WATCH_THREAD.join(timeout=2.0)
            _WATCH_THREAD = None
            _WATCH_THREAD_SHUTDOWN.clear()


# -----------------------
# Auto-initialize on import to be similar to original behavior
# -----------------------
try:
    # Attempt to load with defaults and init logging
    load()
    init_logging()
    _logger.debug("Config module auto-initialized.")
except Exception as e:
    # Avoid breaking importers — log and continue with defaults
    _logger.exception("Error auto-initializing configuration: %s", e)
