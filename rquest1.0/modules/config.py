"""
Revised `config.py` for Rquest v1.0
- Thread-safe configuration loader with optional validation using pydantic (if installed)
- Optional YAML/ruamel support to preserve comments when available
- Optional watchdog-based file watcher to auto-reload when config file changes
- Light integration hooks for project logging and modules

Usage / integration notes (bottom of file) explain how other modules should call into this module.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

# Try to import optional dependencies
try:
    import ruamel.yaml as _ruamel_yaml  # type: ignore
    _HAS_RUAMEL = True
except Exception:
    _HAS_RUAMEL = False

try:
    from watchdog.observers import Observer  # type: ignore
    from watchdog.events import FileSystemEventHandler  # type: ignore
    _HAS_WATCHDOG = True
except Exception:
    Observer = None
    FileSystemEventHandler = object
    _HAS_WATCHDOG = False

# Pydantic is optional for validation
try:
    from pydantic import BaseModel, Extra
    _HAS_PYDANTIC = True
except Exception:
    BaseModel = object
    Extra = None
    _HAS_PYDANTIC = False

# Prefer project logger if set by other modules; fallback to stdlib logging
_project_logger = None

def set_logger(logger: Any) -> None:
    """Set a logger object used by this module.
    The logger should implement .debug/.info/.warning/.error/.exception
    Called by other modules to integrate a shared logger.
    """
    global _project_logger
    _project_logger = logger


def _get_logger():
    global _project_logger
    if _project_logger is not None:
        return _project_logger
    # lazy import to reduce startup coupling
    import logging as _stdlog

    l = _stdlog.getLogger("rquest.config")
    if not l.handlers:
        # basic default handler if none configured by app
        _stdlog.basicConfig(level=_stdlog.INFO)
    return l

LOG = _get_logger()

# Internal state
_CONFIG_LOCK = threading.RLock()
_CONFIG: Dict[str, Any] = {}
_CONFIG_PATH: Optional[Path] = None
_LAST_LOADED_AT: Optional[float] = None
_WATCHER: Optional[Any] = None
_WATCH_CALLBACKS: list[Callable[[], None]] = []

# Default schema for validation using pydantic (very permissive here - apps may override)
if _HAS_PYDANTIC:
    class _DefaultConfigModel(BaseModel):
        # keep arbitrary keys allowed, but forbid unknown top-level if you want stricter
        class Config:
            extra = Extra.allow

    _ModelClass = _DefaultConfigModel
else:
    _ModelClass = None


def _safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception as e:
        LOG.exception("Unable to read config file %s: %s", path, e)
        return ""


def _parse_json_or_yaml(text: str) -> Dict[str, Any]:
    text = text.strip()
    if not text:
        return {}

    # Try JSON first
    try:
        return json.loads(text)
    except Exception:
        pass

    # Try ruamel yaml if available
    if _HAS_RUAMEL:
        try:
            yaml = _ruamel_yaml.YAML(typ="safe")
            data = yaml.load(text)
            return data if isinstance(data, dict) else {}
        except Exception:
            LOG.debug("ruamel.yaml failed to parse config")
    else:
        # Try basic PyYAML via stdlib-safe fallback (if installed in env)
        try:
            import yaml as _yaml  # type: ignore

            data = _yaml.safe_load(text)
            return data if isinstance(data, dict) else {}
        except Exception:
            LOG.debug("yaml safe_load not available or failed")

    # Last fallback: simple key=value lines
    result: Dict[str, Any] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            result[k.strip()] = _coerce_value(v.strip())
    return result


def _coerce_value(v: str) -> Any:
    # Try to coerce strings that look like numbers/booleans/null
    if v.lower() in {"null", "none", "nil"}:
        return None
    if v.lower() in {"true", "false"}:
        return v.lower() == "true"
    try:
        if "." in v:
            return float(v)
        return int(v)
    except Exception:
        return v


def _validate_config(data: Dict[str, Any]) -> Dict[str, Any]:
    # If pydantic is present and a stricter model has been set, validate
    if _HAS_PYDANTIC and _ModelClass is not None:
        try:
            # If model is the default permissive, this will pass-through
            validated = _ModelClass(**data).dict()
            return validated
        except Exception:
            LOG.exception("Config validation failed; returning unvalidated raw config")
            return data
    return data


def _merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge override onto base (produces a new dict)."""
    out = dict(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _merge_dict(out[k], v)
        else:
            out[k] = v
    return out


def load(path: Optional[str | Path] = None, validate: bool = True) -> Dict[str, Any]:
    """Load configuration from `path` (str or Path). If path is None, will try common locations.

    Returns the loaded (and validated, if enabled) dict. Thread-safe.
    """
    global _CONFIG, _CONFIG_PATH, _LAST_LOADED_AT
    LOG.debug("Loading configuration: path=%s validate=%s", path, validate)
    with _CONFIG_LOCK:
        cfg_path = None
        if path:
            cfg_path = Path(path).expanduser().resolve()
            if not cfg_path.exists():
                LOG.warning("Config path %s does not exist", cfg_path)
                cfg_path = None
        if cfg_path is None:
            # search common locations relative to CWD and home
            candidates = [Path.cwd() / "rquest.yml", Path.cwd() / "rquest.yaml", Path.cwd() / "rquest.json", Path.home() / ".rquest.yml"]
            for c in candidates:
                if c.exists():
                    cfg_path = c.resolve()
                    break
        if cfg_path is None:
            # no config found; set empty config and return
            _CONFIG = {}
            _CONFIG_PATH = None
            _LAST_LOADED_AT = time.time()
            return _CONFIG

        text = _safe_read_text(cfg_path)
        parsed = _parse_json_or_yaml(text)
        if validate:
            parsed = _validate_config(parsed)
        _CONFIG = parsed
        _CONFIG_PATH = cfg_path
        _LAST_LOADED_AT = time.time()
        LOG.info("Loaded config from %s (keys=%s)", cfg_path, list(parsed.keys()))
        return _CONFIG


def get(path: Optional[str] = None, default: Any = None) -> Any:
    """Get a value from the loaded config using dotted path, or the whole config if path is None.

    Examples:
      get('database.host')
    """
    with _CONFIG_LOCK:
        if path is None:
            return dict(_CONFIG)
        parts = path.split(".")
        cur: Any = _CONFIG
        for p in parts:
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur


def set_value(path: str, value: Any) -> None:
    """Set value into configuration in-memory (not persisted). Dotted path supported."""
    with _CONFIG_LOCK:
        parts = path.split(".")
        cur = _CONFIG
        for p in parts[:-1]:
            if p not in cur or not isinstance(cur[p], dict):
                cur[p] = {}
            cur = cur[p]
        cur[parts[-1]] = value


def save_override(override: Dict[str, Any], path: Optional[str | Path] = None) -> Path:
    """Persist an override (merged onto existing config) into the config file.

    If ruamel.yaml is available it will try to preserve comments; otherwise writes JSON.
    Returns the path written to.
    """
    global _CONFIG_PATH
    with _CONFIG_LOCK:
        if path:
            target = Path(path).expanduser().resolve()
        elif _CONFIG_PATH is not None:
            target = _CONFIG_PATH
        else:
            # choose a default path
            target = Path.cwd() / "rquest.json"

        # Merge
        current = dict(_CONFIG) if isinstance(_CONFIG, dict) else {}
        merged = _merge_dict(current, override)

        # Write using ruamel if available and file is yaml-like, otherwise JSON
        try:
            if _HAS_RUAMEL and target.suffix in {".yml", ".yaml"}:
                yaml = _ruamel_yaml.YAML()
                yaml.indent(mapping=2, sequence=4, offset=2)
                with target.open("w", encoding="utf-8") as fh:
                    yaml.dump(merged, fh)
            else:
                # diff-friendly compact JSON
                with target.open("w", encoding="utf-8") as fh:
                    json.dump(merged, fh, ensure_ascii=False, indent=2, sort_keys=True)
            # Update in-memory copy
            _CONFIG.clear()
            _CONFIG.update(merged)
            _CONFIG_PATH = target
            LOG.info("Saved override to %s", target)
            return target
        except Exception as e:
            LOG.exception("Failed to save config to %s: %s", target, e)
            raise


def get_module_config(name: str) -> Dict[str, Any]:
    """Return config for a specific module under `modules.<name>` or empty dict."""
    with _CONFIG_LOCK:
        modules = _CONFIG.get("modules") if isinstance(_CONFIG.get("modules"), dict) else {}
        val = modules.get(name)
        return val if isinstance(val, dict) else {}


# Watcher support (optional)
class _CfgEventHandler(FileSystemEventHandler):
    def __init__(self, path: Path):
        super().__init__()
        self._path = path

    def on_modified(self, event):
        try:
            src = Path(event.src_path).resolve()
            if src == self._path:
                LOG.info("Configuration file modified on disk; reloading: %s", src)
                try:
                    load(src)
                    for cb in list(_WATCH_CALLBACKS):
                        try:
                            cb()
                        except Exception:
                            LOG.exception("Config watch callback failed")
                except Exception:
                    LOG.exception("Failed to reload config from watcher event")
        except Exception:
            LOG.exception("Error in config file event handler")


def start_watcher(callback: Optional[Callable[[], None]] = None) -> None:
    """Start a background watcher to reload config when the config file changes.

    If watchdog is not installed, this is a no-op. The optional callback is called on reload.
    Safe to call multiple times; it will register callbacks but only one observer will run.
    """
    global _WATCHER, _WATCH_CALLBACKS, _CONFIG_PATH
    if not _HAS_WATCHDOG:
        LOG.debug("watchdog not available; start_watcher is a no-op")
        return

    with _CONFIG_LOCK:
        if callback:
            _WATCH_CALLBACKS.append(callback)
        if _WATCHER is not None:
            LOG.debug("Watcher already running")
            return
        if _CONFIG_PATH is None:
            LOG.warning("No config path known; cannot start watcher until a config is loaded")
            return

        # Create and start observer
        event_handler = _CfgEventHandler(_CONFIG_PATH)
        observer = Observer()
        observer.schedule(event_handler, str(_CONFIG_PATH.parent), recursive=False)
        observer.daemon = True
        observer.start()
        _WATCHER = observer
        LOG.info("Started config watcher for %s", _CONFIG_PATH)


def stop_watcher() -> None:
    global _WATCHER
    with _CONFIG_LOCK:
        if _WATCHER is None:
            return
        try:
            _WATCHER.stop()
            _WATCHER.join(timeout=2)
        except Exception:
            LOG.exception("Error stopping watcher")
        finally:
            _WATCHER = None
            _WATCH_CALLBACKS.clear()


# Integration helpers
def integrate_with_module(module_obj: Any, module_name: Optional[str] = None) -> None:
    """Helper to integrate a module with this config.

    - If the module has `set_config` callable, it will be called with module's config dict.
    - If the module exposes `set_logger`, this module logger will be passed.
    - module_name (optional) tells which `modules.<name>` key to pass.
    """
    try:
        name = module_name or getattr(module_obj, "__name__", None)
        if not name:
            return
        cfg = get_module_config(name.split(".")[-1])
        if hasattr(module_obj, "set_config") and callable(module_obj.set_config):
            try:
                module_obj.set_config(cfg)
            except Exception:
                LOG.exception("Module %s set_config failed", name)
        if hasattr(module_obj, "set_logger") and callable(module_obj.set_logger):
            try:
                module_obj.set_logger(_get_logger())
            except Exception:
                LOG.exception("Module %s set_logger failed", name)
    except Exception:
        LOG.exception("Failed to integrate module")


# Convenience aliases
reload = load


# If this module is imported, do not auto-start watcher. Let app call load() explicitly.


# Example usage documentation
# - Other modules should call `from rquest1.0.modules import config` and then:
#     config.load()  # or config.load(path)
#     cfg = config.get()
#     cfg_db = config.get('database')
#     config.start_watcher(callback=some_hot_reload_fn)  # optional if watchdog installed
#     config.integrate_with_module(other_module, module_name='mymodule')

# - To integrate logging from the project (if you have a custom logger), call
#     config.set_logger(my_logger)

# - To persist a small override at runtime:
#     config.save_override({'modules': {'mymodule': {'enabled': True}}})

# End of file
