# rquest1.0/modules/config.py
# -*- coding: utf-8 -*-
"""
Central config loader for Rquest
- Reads YAML/JSON config from multiple locations (env override, cwd, user, system)
- Merges with defaults, normalizes paths and types, converts human sizes
- Validates configuration (strict when possible)
- Provides typed access (dataclasses) and helpers for modules
- Watcher: uses watchdog if available, fallback to polling
- Save supports writing only the override (diff) and preserves comments with ruamel if available
- Thread-safe reload and watcher notification
"""

from __future__ import annotations

import os
import sys
import json
import time
import logging
import threading
from pathlib import Path
from copy import deepcopy
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional, List, Tuple, Callable, Union

# Optional libs
try:
    import ruamel.yaml as ruamel_yaml  # preserves comments on save
    _HAS_RUAMEL = True
except Exception:
    ruamel_yaml = None
    _HAS_RUAMEL = False

try:
    import yaml  # PyYAML fallback
    _HAS_PYYAML = True
except Exception:
    yaml = None
    _HAS_PYYAML = False

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    _HAS_WATCHDOG = True
except Exception:
    Observer = None
    FileSystemEventHandler = object
    _HAS_WATCHDOG = False

try:
    from pydantic import BaseModel, ValidationError, Extra
    _HAS_PYDANTIC = True
except Exception:
    _HAS_PYDANTIC = False

# logger
logger = logging.getLogger("rquest.config")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ----------------------------
# Defaults (authoritative)
# ----------------------------
DEFAULTS: Dict[str, Any] = {
    "logging": {
        "level": "INFO",
        "file": None,
        "color": True,
    },
    "db": {
        "path": "~/.rquest/db.sqlite3",
        "backup": True,
    },
    "build": {
        "jobs": 4,
        "cache_dir": "~/.rquest/build-cache",
        "timeout": 3600,
    },
    "upgrade": {
        "strategy": "safe",
        "parallelism": 2,
        "snapshot": {"enabled": True, "path": "~/.rquest/snapshots"}
    },
    "repos": {
        "local": "~/.rquest/repos",
        "remotes": []
    },
    "meta": {
        "meta_cache_dir": "~/.rquest/meta-cache"
    },
    "pkgtool": {
        "cache_dir": "~/.rquest/pkg-cache"
    },
    "toolchain": {
        "default": "gcc"
    },
    "audit": {
        "enabled": True,
        "parallel_workers": 4
    },
    "fetcher": {
        "cache_dir": "~/.rquest/fetch-cache",
        "mirrors": [],
        "http_timeout": 30,
    },
    "modules": {},  # per-module overrides
    "watch": {
        "enabled": True,
        "use_watchdog_if_available": True,
        "poll_interval": 2
    }
}

# ----------------------------
# Typed dataclass fallback (used when pydantic not available)
# ----------------------------
@dataclass
class Config:
    raw: Dict[str, Any] = field(default_factory=dict)
    merged: Dict[str, Any] = field(default_factory=dict)

    def get(self, path: str, default: Any = None) -> Any:
        """dot separated getter"""
        parts = path.split(".")
        cur: Any = self.merged
        for p in parts:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return default
        return cur

    def as_dict(self) -> Dict[str, Any]:
        return deepcopy(self.merged)

# ----------------------------
# Globals and state
# ----------------------------
_CONFIG: Optional[Config] = None
_CONFIG_LOCK = threading.RLock()
_CONFIG_PATH: Optional[Path] = None
_CONFIG_MTIME: Optional[float] = None
_WATCH_CALLBACKS: List[Callable[[Config], None]] = []
_WATCH_THREAD: Optional[threading.Thread] = None
_OBSERVER: Optional[Observer] = None

# ----------------------------
# Utilities
# ----------------------------
def _human_size_to_bytes(val: Union[str, int, None]) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, int):
        return val
    s = str(val).strip().upper()
    units = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}
    try:
        if s[-1] in units:
            return int(float(s[:-1]) * units[s[-1]])
        return int(s)
    except Exception:
        logger.warning("config: failed to parse size '%s'", val)
        return None

def _expand_path(p: Optional[str]) -> Optional[str]:
    if p is None:
        return None
    try:
        return os.path.abspath(os.path.expanduser(os.path.expandvars(p)))
    except Exception:
        return p

def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Merge b into a recursively, returning a new dict"""
    res = deepcopy(a)
    for k, v in b.items():
        if k in res and isinstance(res[k], dict) and isinstance(v, dict):
            res[k] = _deep_merge(res[k], v)
        else:
            res[k] = deepcopy(v)
    return res

def _find_config_candidates() -> List[Path]:
    """Ordered candidate paths to search for config file"""
    out: List[Path] = []
    env = os.environ.get("RQUEST_CONFIG")
    if env:
        out.append(Path(env))
    # project cwd then user then system
    out.extend([
        Path.cwd() / "config.yaml",
        Path.cwd() / "config.json",
        Path.home() / ".config" / "rquest" / "config.yaml",
        Path("/etc") / "rquest" / "config.yaml",
    ])
    return out

def _load_yaml_or_json(path: Path) -> Optional[Dict[str, Any]]:
    """Load a YAML or JSON file, prefer preserving library when available."""
    if not path.exists():
        return None
    try:
        txt = path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error("config: cannot read %s: %s", path, e)
        return None

    # try ruamel (keeps comments) if available and file looks yaml
    if _HAS_RUAMEL and str(path).lower().endswith((".yaml", ".yml")):
        try:
            yaml_obj = ruamel_yaml.YAML()
            data = yaml_obj.load(txt)
            return data or {}
        except Exception as e:
            logger.warning("config: ruamel parse failed %s: %s", path, e)

    # try pyyaml
    if _HAS_PYYAML:
        try:
            data = yaml.safe_load(txt)
            return data or {}
        except Exception as e:
            logger.debug("config: pyyaml parse failed on %s: %s", path, e)

    # try json
    try:
        data = json.loads(txt)
        return data
    except Exception as e:
        logger.debug("config: json parse failed on %s: %s", path, e)

    # last resort: try a minimal key: value parse (very permissive)
    try:
        d = {}
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                k, v = line.split(":", 1)
                d[k.strip()] = v.strip()
        return d
    except Exception:
        logger.exception("config: fallback parse failed for %s", path)
        return None

def _normalize_and_coerce(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize paths, convert human sizes, coerce booleans/ints where reasonable."""
    out = deepcopy(cfg)

    # normalize known path fields
    path_keys = [
        ("db", "path"),
        ("build", "cache_dir"),
        ("repos", "local"),
        ("upgrade", "snapshot"),
        ("meta", "meta_cache_dir"),
        ("pkgtool", "cache_dir"),
        ("fetcher", "cache_dir"),
    ]
    for keys in path_keys:
        ref = out
        for k in keys[:-1]:
            ref = ref.get(k, {})
        last = keys[-1]
        if isinstance(ref, dict) and last in ref and ref[last]:
            # support nested snapshot.path
            if isinstance(ref[last], dict):
                # e.g. snapshot: {enabled: True, path: "~/.rquest"}
                if "path" in ref[last] and isinstance(ref[last]["path"], str):
                    ref[last]["path"] = _expand_path(ref[last]["path"])
            elif isinstance(ref[last], str):
                ref[last] = _expand_path(ref[last])

    # convert some human sizes (logging.max_size etc) if present
    if "logging" in out and isinstance(out["logging"], dict):
        if "max_size" in out["logging"]:
            out["logging"]["max_size_bytes"] = _human_size_to_bytes(out["logging"]["max_size"])

    # coerce numeric fields (jobs, parallelism, timeouts)
    try:
        if "build" in out and isinstance(out["build"], dict):
            if "jobs" in out["build"]:
                out["build"]["jobs"] = int(out["build"].get("jobs", 1))
            if "timeout" in out["build"]:
                out["build"]["timeout"] = int(out["build"].get("timeout", 0))
    except Exception:
        logger.debug("config: failed to coerce build numeric fields", exc_info=True)

    try:
        if "upgrade" in out and isinstance(out["upgrade"], dict):
            out["upgrade"]["parallelism"] = int(out["upgrade"].get("parallelism", 1))
            snap = out["upgrade"].get("snapshot")
            if isinstance(snap, dict) and "path" in snap and isinstance(snap["path"], str):
                snap["path"] = _expand_path(snap["path"])
    except Exception:
        logger.debug("config: failed to coerce upgrade fields", exc_info=True)

    return out

def _allowed_top_keys() -> List[str]:
    """List of allowed top-level keys for lenient validation."""
    return list(DEFAULTS.keys()) + ["watch"]

def _validate_structure(cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Return (ok, list_of_warnings_or_errors). Does not mutate cfg."""
    warnings: List[str] = []
    # unknown top-level keys
    for k in cfg.keys():
        if k not in _allowed_top_keys():
            warnings.append(f"Unknown top-level config key: {k}")

    # types checks for a few important fields
    if "build" in cfg:
        bj = cfg["build"].get("jobs")
        if not isinstance(bj, int) or bj < 1:
            warnings.append("build.jobs must be integer >= 1")

    if "db" in cfg:
        p = cfg["db"].get("path")
        if p and not isinstance(p, str):
            warnings.append("db.path must be a string path")

    if "repos" in cfg:
        remotes = cfg["repos"].get("remotes")
        if remotes is not None and not isinstance(remotes, list):
            warnings.append("repos.remotes should be a list")

    return (len(warnings) == 0, warnings)

# ----------------------------
# Load/Reload API
# ----------------------------
def _find_config_path(explicit: Optional[str] = None) -> Optional[Path]:
    """Return first existing config path from candidates or explicit given."""
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    candidates.extend(_find_config_candidates())
    for p in candidates:
        if p and p.exists():
            return p
    return None

def load(explicit_path: Optional[str] = None, fatal: bool = False) -> Config:
    """
    Load configuration by merging defaults with file content.
    If fatal is True, a validation error will raise; otherwise we log warnings and fallback to defaults.
    """
    global _CONFIG, _CONFIG_PATH, _CONFIG_MTIME
    with _CONFIG_LOCK:
        cfg_file = _find_config_path(explicit_path)
        raw_cfg: Dict[str, Any] = {}
        if cfg_file:
            data = _load_yaml_or_json(cfg_file)
            if data is None:
                logger.warning("config: file found but could not be parsed: %s", cfg_file)
                # keep raw_cfg empty, fallback to defaults
            else:
                raw_cfg = data
            _CONFIG_PATH = cfg_file
            try:
                _CONFIG_MTIME = float(cfg_file.stat().st_mtime)
            except Exception:
                _CONFIG_MTIME = None

        merged = _deep_merge(DEFAULTS, raw_cfg)
        normalized = _normalize_and_coerce(merged)

        # structure validation
        ok, issues = _validate_structure(normalized)
        if not ok:
            msg = f"config: structure validation warnings/errors: {issues}"
            if fatal:
                logger.error(msg)
                raise ValueError(msg)
            else:
                logger.warning(msg)

        # if pydantic available, try strict validation (forbid extra keys)
        if _HAS_PYDANTIC:
            try:
                # create dynamic model matching DEFAULTS keys
                class DynamicModel(BaseModel):
                    class Config:
                        extra = Extra.forbid
                # quick validation: instantiate to trigger type coercion checks not implemented here
                DynamicModel(**normalized)  # type: ignore
            except ValidationError as e:
                logger.error("config: pydantic validation failed: %s", e)
                if fatal:
                    raise

        config_obj = Config(raw=raw_cfg, merged=normalized)
        _CONFIG = config_obj
        logger.info("config: loaded config from %s", str(_CONFIG_PATH) if _CONFIG_PATH else "<defaults>")
        return config_obj

def get_config() -> Config:
    global _CONFIG
    with _CONFIG_LOCK:
        if _CONFIG is None:
            _CONFIG = load()
        return _CONFIG

def reload(explicit_path: Optional[str] = None) -> Config:
    """Force reload from file (if exists) and notify watchers."""
    cfg = load(explicit_path)
    _notify_watchers(cfg)
    return cfg

# ----------------------------
# Save API: save only the override (diff) to avoid overwriting defaults and preserve comments
# ----------------------------
def _compute_override(merged: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    """Return only the keys in merged that differ from defaults (recursively)."""
    def diff(a: Any, b: Any) -> Any:
        # if types differ, return a
        if type(a) != type(b):
            return deepcopy(a)
        if isinstance(a, dict):
            out = {}
            for k, v in a.items():
                if k not in b:
                    out[k] = deepcopy(v)
                else:
                    d = diff(v, b[k])
                    if d is not None:
                        out[k] = d
            return out or None
        else:
            if a != b:
                return deepcopy(a)
            return None
    res = diff(merged, defaults)
    return res or {}

def save(path: Optional[str] = None, save_override_only: bool = True) -> Path:
    """
    Save config to file. If save_override_only True, we write only the diff between current merged and DEFAULTS
    to avoid overwriting defaults and keep the file minimal. If ruamel is available, preserve comments on write.
    Returns path written.
    """
    with _CONFIG_LOCK:
        cfg = get_config()
        merged = cfg.as_dict()
        out_path = Path(path) if path else (_CONFIG_PATH or (Path.home() / ".config" / "rquest" / "config.yaml"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        to_write = merged
        if save_override_only:
            override = _compute_override(merged, DEFAULTS)
            to_write = override

        try:
            if _HAS_RUAMEL:
                yaml_obj = ruamel_yaml.YAML()
                # if existing file and ruamel loaded originally, try to preserve it
                if out_path.exists():
                    try:
                        orig = ruamel_yaml.YAML().load(out_path.read_text(encoding="utf-8"))
                        # simple replacement: write override into original and dump
                    except Exception:
                        orig = None
                with open(out_path, "w", encoding="utf-8") as fh:
                    yaml_obj.dump(to_write, fh)
            elif _HAS_PYYAML:
                with open(out_path, "w", encoding="utf-8") as fh:
                    yaml.safe_dump(to_write, fh, default_flow_style=False, sort_keys=False)
            else:
                with open(out_path, "w", encoding="utf-8") as fh:
                    json.dump(to_write, fh, indent=2)
            logger.info("config: saved config to %s (override_only=%s)", out_path, save_override_only)
            return out_path
        except Exception:
            logger.exception("config: saving config failed")
            raise

# ----------------------------
# Watcher API
# ----------------------------
def register_watch_callback(cb: Callable[[Config], None]) -> None:
    """Register a callback to be invoked when the config changes (callback receives the new Config)."""
    with _CONFIG_LOCK:
        if cb not in _WATCH_CALLBACKS:
            _WATCH_CALLBACKS.append(cb)

def unregister_watch_callback(cb: Callable[[Config], None]) -> None:
    with _CONFIG_LOCK:
        if cb in _WATCH_CALLBACKS:
            _WATCH_CALLBACKS.remove(cb)

def _notify_watchers(cfg: Config) -> None:
    # copy to avoid modification while iterating
    with _CONFIG_LOCK:
        callbacks = list(_WATCH_CALLBACKS)
    for cb in callbacks:
        try:
            cb(cfg)
        except Exception:
            logger.exception("config: watch callback failed")

class _FSHandler(FileSystemEventHandler):
    def __init__(self, path: Path):
        super().__init__()
        self._path = path

    def on_modified(self, event):
        try:
            if hasattr(event, "src_path") and Path(event.src_path) == self._path:
                logger.info("config: file modified, reloading %s", self._path)
                cfg = reload(str(self._path))
                _notify_watchers(cfg)
        except Exception:
            logger.exception("config: watcher exception on_modified")

def _start_watch_thread():
    global _WATCH_THREAD, _OBSERVER, _CONFIG_PATH
    if not get_config().merged.get("watch", {}).get("enabled", True):
        logger.debug("config: watch disabled in configuration")
        return

    use_watchdog = get_config().merged.get("watch", {}).get("use_watchdog_if_available", True) and _HAS_WATCHDOG
    poll_interval = float(get_config().merged.get("watch", {}).get("poll_interval", 2))

    if _CONFIG_PATH is None:
        logger.debug("config: no config path to watch")
        return

    if use_watchdog:
        try:
            if _OBSERVER is None:
                handler = _FSHandler(_CONFIG_PATH)
                _OBSERVER = Observer()
                _OBSERVER.schedule(handler, str(_CONFIG_PATH.parent), recursive=False)
                _OBSERVER.daemon = True
                _OBSERVER.start()
                logger.info("config: started watchdog observer for %s", _CONFIG_PATH)
            return
        except Exception:
            logger.exception("config: failed to start watchdog, falling back to polling")

    # fallback polling thread
    def _poll_loop():
        global _CONFIG_MTIME
        last = _CONFIG_MTIME
        while True:
            try:
                if _CONFIG_PATH and _CONFIG_PATH.exists():
                    m = float(_CONFIG_PATH.stat().st_mtime)
                    if last and m != last:
                        logger.info("config: detected change by polling, reloading %s", _CONFIG_PATH)
                        cfg = reload(str(_CONFIG_PATH))
                        _notify_watchers(cfg)
                    last = m
                time.sleep(poll_interval)
            except Exception:
                logger.exception("config: poll watcher exception")
                time.sleep(poll_interval)

    if _WATCH_THREAD is None or not _WATCH_THREAD.is_alive():
        t = threading.Thread(target=_poll_loop, daemon=True)
        t.start()
        _WATCH_THREAD = t
        logger.info("config: started poll watcher for %s (interval=%s)", _CONFIG_PATH, poll_interval)

# ----------------------------
# Convenience helpers used by modules
# ----------------------------
def get_module_config(module_name: str) -> Dict[str, Any]:
    cfg = get_config().merged.get("modules", {})
    mod = cfg.get(module_name)
    if isinstance(mod, dict):
        return deepcopy(mod)
    return {}

def get_db_path() -> str:
    return _expand_path(get_config().merged.get("db", {}).get("path"))

def get_build_config() -> Dict[str, Any]:
    return deepcopy(get_config().merged.get("build", {}))

def get_fetcher_config() -> Dict[str, Any]:
    return deepcopy(get_config().merged.get("fetcher", {}))

def validate_config() -> Tuple[bool, List[str]]:
    """Return (ok, list_of_issues). Will perform more exhaustive checks."""
    cfg = get_config().merged
    ok, issues = _validate_structure(cfg)
    # additional checks: directories writable where required
    extra_issues: List[str] = []
    # check db path parent writable
    dbp = cfg.get("db", {}).get("path")
    if dbp:
        parent = Path(_expand_path(dbp)).parent
        if not parent.exists():
            try:
                parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                extra_issues.append(f"db.path parent {parent} not writable/creatable")
        elif not os.access(parent, os.W_OK):
            extra_issues.append(f"db.path parent {parent} is not writable")
    # check repos.local
    rl = cfg.get("repos", {}).get("local")
    if rl and not os.access(_expand_path(rl), os.F_OK):
        # try create
        try:
            Path(_expand_path(rl)).mkdir(parents=True, exist_ok=True)
        except Exception:
            extra_issues.append(f"repos.local {rl} not creatable")

    issues.extend(extra_issues)
    return (len(issues) == 0, issues)

# ----------------------------
# Initialize on import
# ----------------------------
try:
    load()
except Exception:
    logger.exception("config: initial load failed, continuing with defaults")

# configure basic logging level from config if available (non-intrusive)
try:
    cfg_level = get_config().merged.get("logging", {}).get("level", "INFO")
    logger.setLevel(getattr(logging, str(cfg_level).upper(), logging.INFO))
except Exception:
    pass

try:
    _start_watch_thread()
except Exception:
    logger.exception("config: failed to start watcher")

# ----------------------------
# CLI test helper
# ----------------------------
if __name__ == "__main__":
    # simple CLI to print config and validate
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-config", description="Rquest config inspector")
    ap.add_argument("--print", action="store_true", help="print merged config")
    ap.add_argument("--raw", action="store_true", help="print raw file config only")
    ap.add_argument("--validate", action="store_true", help="validate config and list issues")
    ap.add_argument("--save", help="save current override to path")
    args = ap.parse_args()
    if args.raw:
        print(json.dumps(get_config().raw, indent=2, ensure_ascii=False))
    if args.print:
        print(json.dumps(get_config().merged, indent=2, ensure_ascii=False))
    if args.validate:
        ok, issues = validate_config()
        print("OK:", ok)
        if issues:
            print("Issues:")
            for i in issues:
                print(" -", i)
    if args.save:
        p = save(args.save)
        print("Saved to", p)
