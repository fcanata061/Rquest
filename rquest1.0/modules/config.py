# rquest1.0/modules/config.py
# -*- coding: utf-8 -*-
"""
Rquest central configuration loader (final version)

Features:
- Read YAML/JSON config from multiple locations (env override, cwd, user, system)
- Merge with authoritative DEFAULTS, normalize/coerce types (human sizes to bytes)
- Validate structure and types, warn or error (fatal optional)
- Provide typed access via Config dataclass (get_config(), get_module_config(), helpers)
- Watch file changes: use watchdog if available, fallback to polling
- Thread-safe load/reload and watcher notification
- Save supports writing only the overrides (diff) and preserves comments if ruamel.yaml available
- Integrates by design with modules via helpers: get_db_path(), get_build_config(), get_module_config()
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
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List, Tuple, Callable, Union

# Optional dependencies
try:
    import ruamel.yaml as ruamel_yaml  # preserves comments when saving
    _HAS_RUAMEL = True
except Exception:
    ruamel_yaml = None
    _HAS_RUAMEL = False

try:
    import yaml as pyyaml  # PyYAML for parsing
    _HAS_PYYAML = True
except Exception:
    pyyaml = None
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
# DEFAULT configuration (authoritative base)
# ----------------------------
DEFAULTS: Dict[str, Any] = {
    "logging": {
        "level": "INFO",
        "file": None,
        "color": True,
        "max_size": "10M",  # human readable
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
        "retries": 3
    },
    "modules": {},  # per-module overrides: modules.<name> -> dict
    "watch": {
        "enabled": True,
        "use_watchdog_if_available": True,
        "poll_interval": 2
    }
}

# ----------------------------
# Dataclass to hold config
# ----------------------------
@dataclass
class Config:
    raw: Dict[str, Any] = field(default_factory=dict)     # values loaded from file (if any)
    merged: Dict[str, Any] = field(default_factory=dict)  # merged with DEFAULTS

    def get(self, path: str, default: Any = None) -> Any:
        """Dot-separated getter for merged config."""
        parts = path.split(".") if path else []
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
# Module state
# ----------------------------
_CONFIG: Optional[Config] = None
_CONFIG_LOCK = threading.RLock()
_CONFIG_PATH: Optional[Path] = None
_CONFIG_MTIME: Optional[float] = None
_WATCH_CALLBACKS: List[Callable[[Config], None]] = []
_WATCHER_THREAD: Optional[threading.Thread] = None
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
    units = {"K": 1024, "KB": 1024, "M": 1024**2, "MB": 1024**2, "G": 1024**3, "GB": 1024**3, "T": 1024**4}
    try:
        for suffix, mul in units.items():
            if s.endswith(suffix):
                num = float(s[: -len(suffix)].strip())
                return int(num * mul)
        return int(float(s))
    except Exception:
        logger.warning("config: cannot parse human size '%s'", val)
        return None

def _expand_path(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    try:
        return os.path.abspath(os.path.expanduser(os.path.expandvars(val)))
    except Exception:
        return val

def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    res = deepcopy(a)
    for k, v in b.items():
        if k in res and isinstance(res[k], dict) and isinstance(v, dict):
            res[k] = _deep_merge(res[k], v)
        else:
            res[k] = deepcopy(v)
    return res

def _find_candidates(explicit: Optional[str] = None) -> List[Path]:
    candidates: List[Path] = []
    env = os.environ.get("RQUEST_CONFIG")
    if explicit:
        candidates.append(Path(explicit))
    if env:
        candidates.append(Path(env))
    candidates.extend([
        Path.cwd() / "config.yaml",
        Path.cwd() / "config.yml",
        Path.cwd() / "config.json",
        Path.home() / ".config" / "rquest" / "config.yaml",
        Path("/etc") / "rquest" / "config.yaml",
    ])
    return candidates

def _load_file(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    txt = None
    try:
        txt = path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error("config: failed reading %s: %s", path, e, exc_info=True)
        return None

    # Try ruamel (YAML with comment preservation)
    if _HAS_RUAMEL and path.suffix.lower() in (".yaml", ".yml"):
        try:
            y = ruamel_yaml.YAML()
            data = y.load(txt)
            return data or {}
        except Exception as e:
            logger.debug("config: ruamel parse fail %s: %s", path, e, exc_info=True)

    # Try pyyaml
    if _HAS_PYYAML and path.suffix.lower() in (".yaml", ".yml"):
        try:
            data = pyyaml.safe_load(txt)
            return data or {}
        except Exception as e:
            logger.debug("config: pyyaml parse fail %s: %s", path, e, exc_info=True)

    # Try JSON
    try:
        data = json.loads(txt)
        return data
    except Exception as e:
        logger.debug("config: json parse fail %s: %s", path, e, exc_info=True)

    # Last resort permissive parse (key: value lines) - best-effort
    try:
        out: Dict[str, Any] = {}
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                k, v = line.split(":", 1)
                out[k.strip()] = v.strip()
        return out
    except Exception:
        logger.exception("config: fallback parse failed for %s", path)
        return None

def _normalize_and_coerce(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize path fields, convert human sizes and coerce basic types."""
    out = deepcopy(cfg)
    # Normalize path-like entries known in DEFAULTS + common ones
    path_keys = [
        ("db", "path"),
        ("build", "cache_dir"),
        ("repos", "local"),
        ("upgrade", "snapshot"),
        ("meta", "meta_cache_dir"),
        ("pkgtool", "cache_dir"),
        ("fetcher", "cache_dir"),
        ("logging", "file"),
    ]
    for keys in path_keys:
        ref = out
        for k in keys[:-1]:
            if isinstance(ref, dict):
                ref = ref.get(k, {})
            else:
                ref = {}
        last = keys[-1]
        if isinstance(ref, dict) and last in ref and ref[last]:
            if isinstance(ref[last], dict):
                # handle nested snapshot: {enabled, path}
                if "path" in ref[last] and isinstance(ref[last]["path"], str):
                    ref[last]["path"] = _expand_path(ref[last]["path"])
            elif isinstance(ref[last], str):
                ref[last] = _expand_path(ref[last])

    # Convert human sizes
    if "logging" in out and isinstance(out["logging"], dict):
        if "max_size" in out["logging"]:
            ms = _human_size_to_bytes(out["logging"]["max_size"])
            if ms is not None:
                out["logging"]["max_size_bytes"] = ms

    # Coerce numbers
    try:
        if "build" in out and isinstance(out["build"], dict):
            if "jobs" in out["build"]:
                out["build"]["jobs"] = int(out["build"].get("jobs", 1))
            if "timeout" in out["build"]:
                out["build"]["timeout"] = int(out["build"].get("timeout", 0))
    except Exception:
        logger.debug("config: failed to coerce build fields", exc_info=True)

    try:
        if "upgrade" in out and isinstance(out["upgrade"], dict):
            out["upgrade"]["parallelism"] = int(out["upgrade"].get("parallelism", 1))
    except Exception:
        logger.debug("config: failed to coerce upgrade fields", exc_info=True)

    return out

def _allowed_top_level_keys() -> List[str]:
    return list(DEFAULTS.keys()) + ["watch"]

def _validate_structure(cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Return (ok, issues_list). Non-fatal warnings unless called with fatal=True in load."""
    warnings: List[str] = []
    # unknown top-level keys
    for k in cfg.keys():
        if k not in _allowed_top_level_keys():
            warnings.append(f"Unknown top-level config key: {k}")
    # basic type checks
    if "build" in cfg:
        bj = cfg["build"].get("jobs")
        if not isinstance(bj, int) or bj < 1:
            warnings.append("build.jobs must be integer >= 1")
    if "db" in cfg:
        p = cfg["db"].get("path")
        if p and not isinstance(p, str):
            warnings.append("db.path must be a string")
    if "repos" in cfg:
        rem = cfg["repos"].get("remotes")
        if rem is not None and not isinstance(rem, list):
            warnings.append("repos.remotes should be a list")
    return (len(warnings) == 0, warnings)

# ----------------------------
# Loading / reloading
# ----------------------------
def _find_path(explicit: Optional[str] = None) -> Optional[Path]:
    candidates = _find_candidates(explicit)
    for p in candidates:
        if p and p.exists():
            return p
    return None

def load(explicit_path: Optional[str] = None, fatal: bool = False) -> Config:
    """
    Load and merge config. If fatal=True then structural validation failures raise.
    Returns Config object.
    """
    global _CONFIG, _CONFIG_PATH, _CONFIG_MTIME
    with _CONFIG_LOCK:
        cfg_path = _find_path(explicit_path)
        raw: Dict[str, Any] = {}
        if cfg_path:
            data = _load_file(cfg_path)
            if data is None:
                logger.warning("config: file found but could not be parsed: %s", str(cfg_path))
            else:
                raw = data
            _CONFIG_PATH = cfg_path
            try:
                _CONFIG_MTIME = float(cfg_path.stat().st_mtime)
            except Exception:
                _CONFIG_MTIME = None
        merged = _deep_merge(DEFAULTS, raw)
        normalized = _normalize_and_coerce(merged)
        ok, issues = _validate_structure(normalized)
        if not ok:
            msg = f"config: validation issues: {issues}"
            if fatal:
                logger.error(msg)
                raise ValueError(msg)
            else:
                logger.warning(msg)
        # Optional stricter validation with pydantic
        if _HAS_PYDANTIC:
            try:
                class DummyModel(BaseModel):
                    class Config:
                        extra = Extra.forbid
                DummyModel(**normalized)  # quick check, might be superficial
            except ValidationError as e:
                logger.warning("config: pydantic validation issues: %s", e)
                if fatal:
                    raise
        cfg_obj = Config(raw=raw, merged=normalized)
        _CONFIG = cfg_obj
        logger.info("config: loaded merged config (from=%s)", str(_CONFIG_PATH) if _CONFIG_PATH else "<defaults>")
        return cfg_obj

def get_config() -> Config:
    global _CONFIG
    with _CONFIG_LOCK:
        if _CONFIG is None:
            _CONFIG = load()
        return _CONFIG

def reload(explicit_path: Optional[str] = None) -> Config:
    cfg = load(explicit_path)
    _notify_watchers(cfg)
    return cfg

# ----------------------------
# Save: write only override (diff) to avoid clobbering defaults
# ----------------------------
def _compute_override(merged: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    def diff(a: Any, b: Any) -> Any:
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
    r = diff(merged, defaults)
    return r or {}

def save(path: Optional[str] = None, override_only: bool = True) -> Path:
    with _CONFIG_LOCK:
        cfg = get_config()
        merged = cfg.as_dict()
        out_path = Path(path) if path else (_CONFIG_PATH or (Path.home() / ".config" / "rquest" / "config.yaml"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        to_write = merged
        if override_only:
            to_write = _compute_override(merged, DEFAULTS) or {}
        try:
            if _HAS_RUAMEL:
                yaml_obj = ruamel_yaml.YAML()
                with open(out_path, "w", encoding="utf-8") as fh:
                    yaml_obj.dump(to_write, fh)
            elif _HAS_PYYAML:
                with open(out_path, "w", encoding="utf-8") as fh:
                    pyyaml.safe_dump(to_write, fh, default_flow_style=False, sort_keys=False)
            else:
                with open(out_path, "w", encoding="utf-8") as fh:
                    json.dump(to_write, fh, indent=2, ensure_ascii=False)
            logger.info("config: saved config to %s (override_only=%s)", out_path, override_only)
            return out_path
        except Exception:
            logger.exception("config: failed to save config")
            raise

# ----------------------------
# Watcher API
# ----------------------------
def register_watch_callback(cb: Callable[[Config], None]) -> None:
    with _CONFIG_LOCK:
        if cb not in _WATCH_CALLBACKS:
            _WATCH_CALLBACKS.append(cb)

def unregister_watch_callback(cb: Callable[[Config], None]) -> None:
    with _CONFIG_LOCK:
        if cb in _WATCH_CALLBACKS:
            _WATCH_CALLBACKS.remove(cb)

def _notify_watchers(cfg: Config) -> None:
    with _CONFIG_LOCK:
        cbs = list(_WATCH_CALLBACKS)
    for cb in cbs:
        try:
            cb(cfg)
        except Exception:
            logger.exception("config: watcher callback error")

class _FSHandler(FileSystemEventHandler):
    def __init__(self, watched: Path):
        super().__init__()
        self._watched = watched

    def on_modified(self, event):
        try:
            src = getattr(event, "src_path", None)
            if src and Path(src) == self._watched:
                logger.info("config: detected modification via watchdog, reloading %s", self._watched)
                cfg = reload(str(self._watched))
                _notify_watchers(cfg)
        except Exception:
            logger.exception("config: watchdog handler error")

def _start_watcher():
    global _WATCHER_THREAD, _OBSERVER, _CONFIG_PATH, _CONFIG_MTIME
    cfg = get_config().merged
    if not cfg.get("watch", {}).get("enabled", True):
        logger.debug("config: watch disabled by config")
        return

    use_watchdog = cfg.get("watch", {}).get("use_watchdog_if_available", True) and _HAS_WATCHDOG
    poll_interval = float(cfg.get("watch", {}).get("poll_interval", 2))

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
                logger.info("config: started watchdog observer on %s", _CONFIG_PATH)
            return
        except Exception:
            logger.exception("config: failed to start watchdog, falling back to polling")

    # polling fallback
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

    if _WATCHER_THREAD is None or not _WATCHER_THREAD.is_alive():
        t = threading.Thread(target=_poll_loop, daemon=True)
        t.start()
        _WATCHER_THREAD = t
        logger.info("config: started poll watcher for %s (interval=%s)", _CONFIG_PATH, poll_interval)

# ----------------------------
# Convenience helpers for modules
# ----------------------------
def get_module_config(name: str) -> Dict[str, Any]:
    cfg = get_config().merged.get("modules", {})
    val = cfg.get(name)
    return deepcopy(val) if isinstance(val, dict) else {}

def get_db_path() -> str:
    return _expand_path(get_config().merged.get("db", {}).get("path"))

def get_build_config() -> Dict[str, Any]:
    return deepcopy(get_config().merged.get("build", {}))

def get_fetcher_config() -> Dict[str, Any]:
    return deepcopy(get_config().merged.get("fetcher", {}))

def validate_config() -> Tuple[bool, List[str]]:
    ok, issues = _validate_structure(get_config().merged)
    # extra checks: db parent writable, repos local creatable
    extra: List[str] = []
    dbp = get_config().merged.get("db", {}).get("path")
    if dbp:
        parent = Path(_expand_path(dbp)).parent
        try:
            parent.mkdir(parents=True, exist_ok=True)
            if not os.access(parent, os.W_OK):
                extra.append(f"db.path parent {parent} not writable")
        except Exception:
            extra.append(f"db.path parent {parent} not creatable")
    rl = get_config().merged.get("repos", {}).get("local")
    if rl:
        try:
            Path(_expand_path(rl)).mkdir(parents=True, exist_ok=True)
        except Exception:
            extra.append(f"repos.local {rl} not creatable")
    issues.extend(extra)
    return (len(issues) == 0, issues)

# ----------------------------
# Initialize at import
# ----------------------------
try:
    load()
except Exception:
    logger.exception("config: initial load failed, using defaults")

# Set logging level from config (best-effort)
try:
    level = get_config().merged.get("logging", {}).get("level", "INFO")
    logger.setLevel(getattr(logging, str(level).upper(), logging.INFO))
except Exception:
    pass

# Start watcher if configured
try:
    _start_watcher()
except Exception:
    logger.exception("config: failed to start watcher")

# ----------------------------
# CLI for inspection and quick ops
# ----------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-config", description="Inspect/validate rquest config")
    ap.add_argument("--print", action="store_true", help="print merged config")
    ap.add_argument("--raw", action="store_true", help="print raw file config only (if file exists)")
    ap.add_argument("--validate", action="store_true", help="validate config and list issues")
    ap.add_argument("--save", help="save current override to path")
    ap.add_argument("--path", help="explicit config path to load")
    args = ap.parse_args()
    if args.path:
        cfg = load(args.path)
    else:
        cfg = get_config()
    if args.raw:
        print(json.dumps(cfg.raw, indent=2, ensure_ascii=False))
    if args.print:
        print(json.dumps(cfg.merged, indent=2, ensure_ascii=False))
    if args.validate:
        ok, issues = validate_config()
        print("OK:", ok)
        if issues:
            print("Issues:")
            for it in issues:
                print(" -", it)
    if args.save:
        p = save(args.save)
        print("Saved to", p)
