# rquest1.0/modules/config.py
# -*- coding: utf-8 -*-
"""
Rquest configuration loader and manager.

Features:
 - load from multiple config paths (system, distro, user, env override)
 - supports YAML (PyYAML) and JSON
 - merge precedence: defaults < /usr/lib/rquest/config.yaml < /etc/rquest/config.yaml < ~/.config/rquest/config.yaml < explicit path
 - provides runtime API: get_config(), get(section, default), reload(), save(path), init_logging()
 - light validation + defaults for important sections (database, build, upgrade, repos, logging, audit)
 - optional simple file watcher callback (polling)
 - designed to be safe to import from other modules
"""

from __future__ import annotations

import os
import json
import time
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Callable, List

# try YAML
try:
    import yaml  # type: ignore
    YAML_AVAILABLE = True
except Exception:
    YAML_AVAILABLE = False

_logger = logging.getLogger("rquest.config")

# Default configuration (safe sensible defaults)
_DEFAULTS: Dict[str, Any] = {
    "database": {
        "path": "/var/lib/rquest/db.sqlite3",
        "timeout": 30,
        "journal_mode": "WAL",
        "synchronous": "NORMAL"
    },
    "build": {
        "jobs": os.cpu_count() or 1,
        "cflags": "-O2 -pipe",
        "cxxflags": "-O2 -pipe",
        "ldflags": "",
        "prefix": "/usr",
        "builddir": "/var/tmp/rquest/build",
        "fakeroot": False,
        "keep_build_dirs": False,
        "cache_dir": "/var/cache/rquest/build"
    },
    "upgrade": {
        "parallelism": 2,
        "max_retries": 2,
        "retry_backoff": 5,
        "snapshot": {"backend": "btrfs", "path": "/var/lib/rquest/snapshots", "source": "/"},
        "rollback_all_on_fail": False,
        "dry_run": False,
        "persist_runs_in_db": True,
    },
    "repos": {
        "local": os.path.expanduser("~/.rquest/repos"),
        "remotes": [],  # list of git/http endpoints
        "metas_subpath": "",  # subpath inside repo where .meta files live
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s %(levelname)s %(name)s: %(message)s",
        "console": True,
        "file": None,  # e.g. /var/log/rquest/rquest.log
    },
    "audit": {
        "paths": ["/usr", "/lib", "/etc", "/bin", "/sbin"],
        "hash_algos": ["sha256"],
        "check_permissions": True,
        "check_owner": True,
        "output_dir": os.path.expanduser("~/.rquest/audit_reports"),
    },
    "daemon": {
        "enabled": False,
    },
    "meta": {
        "meta_cache_dir": os.path.expanduser("~/.rquest/meta_cache"),
        "hooks_dir_name": "hooks",
        "inherit_search_paths": ["./", "/usr/share/rquest/meta", "~/.rquest/overlays"]
    },
    "pkgtool": {
        "cache_dir": os.path.expanduser("~/.rquest/pkgcache"),
    },
    "toolchain": {
        "bootstrap_dir": "/var/lib/rquest/toolchains",
    },
    # UI/Notifier
    "notifier": {
        "notify_send": True,
        "i3blocks": {"script": None},
        "lemonbar": {"script": None}
    }
}

# Candidate config files (in load order; later ones override earlier).
# Merge order: defaults <- /usr/lib <- /etc <- XDG_CONFIG_HOME/user <- explicit path passed to load()
_CANDIDATE_PATHS = [
    "/usr/lib/rquest/config.yaml",
    "/etc/rquest/config.yaml",
    os.path.join(os.getenv("XDG_CONFIG_HOME", os.path.expanduser("~/.config")), "rquest", "config.yaml"),
    os.path.expanduser("~/.rquest/config.yaml"),
]

# Module-level singleton data
_CONFIG: Dict[str, Any] = {}
_CONFIG_PATHS_LOADED: List[str] = []
_WATCHERS: List[Callable[[Dict[str, Any]], None]] = []
_WATCH_POLL_INTERVAL = 2.0  # seconds
_WATCH_THREAD = None
_WATCH_STOP = False

# -----------------------
# Utility helpers
# -----------------------
def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Merge b into a recursively and return result (new dict)."""
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def _load_file(path: str) -> Optional[Dict[str, Any]]:
    """Load YAML or JSON file. Returns dict or None."""
    p = Path(path)
    if not p.exists() or not p.is_file():
        return None
    try:
        txt = p.read_text(encoding="utf-8")
        # try yaml if available
        if YAML_AVAILABLE:
            try:
                data = yaml.safe_load(txt)
                if isinstance(data, dict):
                    return data
            except Exception:
                _logger.debug("YAML parse failed for %s, falling back to JSON", path)
        # fallback JSON
        try:
            data = json.loads(txt)
            if isinstance(data, dict):
                return data
        except Exception:
            _logger.warning("Failed to parse config file %s", path)
            return None
    except Exception:
        _logger.exception("Failed to read config file %s", path)
        return None

def _normalize_paths(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve some path entries to absolute expanded paths."""
    c = dict(cfg)
    # database.path
    try:
        dbp = c.get("database", {}).get("path")
        if dbp:
            c.setdefault("database", {})["path"] = os.path.abspath(os.path.expanduser(str(dbp)))
    except Exception:
        pass
    # repo local
    try:
        rp = c.get("repos", {}).get("local")
        if rp:
            c.setdefault("repos", {})["local"] = os.path.abspath(os.path.expanduser(str(rp)))
    except Exception:
        pass
    # build.cache_dir
    try:
        cd = c.get("build", {}).get("cache_dir")
        if cd:
            c.setdefault("build", {})["cache_dir"] = os.path.abspath(os.path.expanduser(str(cd)))
    except Exception:
        pass
    # snapshot path
    try:
        sp = c.get("upgrade", {}).get("snapshot", {}).get("path")
        if sp:
            c.setdefault("upgrade", {})["snapshot"]["path"] = os.path.abspath(os.path.expanduser(str(sp)))
    except Exception:
        pass
    return c

# -----------------------
# Core API
# -----------------------
def load(paths: Optional[List[str]] = None, explicit_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration merging defaults with files in _CANDIDATE_PATHS and optional explicit_path.
    Returned dict is also stored in module-level get_config().
    """
    global _CONFIG, _CONFIG_PATHS_LOADED
    cfg = dict(_DEFAULTS)  # start from defaults
    paths_to_try = list(_CANDIDATE_PATHS)
    if paths:
        # custom candidate list
        paths_to_try = paths_to_try + list(paths)
    if explicit_path:
        paths_to_try = paths_to_try + [explicit_path]
    loaded = []
    for p in paths_to_try:
        try:
            p = os.path.expanduser(str(p))
            data = _load_file(p)
            if data:
                cfg = _deep_merge(cfg, data)
                loaded.append(p)
        except Exception:
            _logger.exception("Failed loading config from %s", p)
    cfg = _normalize_paths(cfg)
    _CONFIG = cfg
    _CONFIG_PATHS_LOADED = loaded
    _logger.debug("Config loaded from: %s", loaded)
    return cfg

def get_config() -> Dict[str, Any]:
    """Return the active configuration dictionary (singleton)."""
    global _CONFIG
    if not _CONFIG:
        load()
    return _CONFIG

def get(path: str, default: Any = None) -> Any:
    """
    Convenience getter for dotted path, e.g. get('database.path')
    """
    cfg = get_config()
    parts = path.split(".")
    cur = cfg
    for p in parts:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

def reload(explicit_path: Optional[str] = None) -> Dict[str, Any]:
    """Reload configuration (useful for CLI tools to update runtime config)"""
    return load(explicit_path=explicit_path)

def save(path: Optional[str] = None) -> str:
    """Persist current configuration to given path (yaml if available else json). Returns path saved."""
    cfg = get_config()
    p = Path(path or _CONFIG_PATHS_LOADED[-1] if _CONFIG_PATHS_LOADED else os.path.expanduser("~/.rquest/config.yaml"))
    p.parent.mkdir(parents=True, exist_ok=True)
    if YAML_AVAILABLE:
        try:
            p.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
            return str(p)
        except Exception:
            _logger.exception("Save YAML failed, falling back to JSON")
    try:
        p.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
        return str(p)
    except Exception:
        _logger.exception("Failed to save config to %s", p)
        raise

# -----------------------
# Lightweight validation
# -----------------------
def validate(cfg: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    Perform quick validations and return a list of warnings (empty if ok).
    Not a strict schema validator, but checks presence and types of key fields.
    """
    warnings: List[str] = []
    c = cfg or get_config()
    # database.path
    db = c.get("database")
    if not db or not isinstance(db, dict):
        warnings.append("database section missing or invalid; using defaults")
    else:
        if not db.get("path"):
            warnings.append("database.path not set; using default")
    # repos.local
    repos = c.get("repos", {})
    if repos and not repos.get("local"):
        warnings.append("repos.local not set; recommended to set local repo path")
    # build.jobs sanity
    try:
        jobs = int(c.get("build", {}).get("jobs", 1))
        if jobs < 1:
            warnings.append("build.jobs < 1; forcing to 1")
            c.setdefault("build", {})["jobs"] = 1
    except Exception:
        warnings.append("build.jobs invalid; resetting to 1")
        c.setdefault("build", {})["jobs"] = 1
    # snapshot path writable check (if snapshot backend specified)
    try:
        sp = c.get("upgrade", {}).get("snapshot", {}).get("path")
        if sp and not os.access(os.path.dirname(sp), os.W_OK):
            warnings.append(f"upgrade.snapshot.path parent not writable: {sp}")
    except Exception:
        pass
    return warnings

# -----------------------
# Logging initialization helper
# -----------------------
def init_logging(cfg: Optional[Dict[str,Any]] = None):
    """
    Initialize python logging according to config.logging. This is safe to call multiple times.
    Returns the root logger configured.
    """
    c = cfg or get_config()
    log_cfg = c.get("logging", {}) if isinstance(c.get("logging", {}), dict) else {}
    level_name = log_cfg.get("level", "INFO")
    fmt = log_cfg.get("format", "%(asctime)s %(levelname)s %(name)s: %(message)s")
    console = bool(log_cfg.get("console", True))
    file_path = log_cfg.get("file")
    level = getattr(logging, str(level_name).upper(), logging.INFO)

    # root config
    root = logging.getLogger()
    # remove existing handlers to avoid duplicates on repeated init
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(level)
    formatter = logging.Formatter(fmt)
    if console:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        root.addHandler(ch)
    if file_path:
        try:
            p = Path(file_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(str(p), encoding="utf-8")
            fh.setLevel(level)
            fh.setFormatter(formatter)
            root.addHandler(fh)
        except Exception:
            _logger.exception("failed to create log file handler %s", file_path)
    _logger.debug("Logging initialized (level=%s file=%s console=%s)", level_name, file_path, console)
    return root

# -----------------------
# Watcher (simple poll-based)
# -----------------------
def _watch_loop(poll: float = _WATCH_POLL_INTERVAL):
    global _WATCH_STOP
    last_mtimes: Dict[str, float] = {}
    while not _WATCH_STOP:
        try:
            for p in _CONFIG_PATHS_LOADED:
                try:
                    st = Path(p).stat()
                    m = st.st_mtime
                except Exception:
                    m = 0
                if p not in last_mtimes:
                    last_mtimes[p] = m
                else:
                    if m != last_mtimes[p]:
                        last_mtimes[p] = m
                        # reload config
                        _logger.info("Config file changed, reloading: %s", p)
                        reload(explicit_path=p)
                        for cb in list(_WATCHERS):
                            try:
                                cb(get_config())
                            except Exception:
                                _logger.exception("Config watcher callback failed")
        except Exception:
            _logger.exception("config watch loop error")
        time.sleep(poll)

def watch(callback: Callable[[Dict[str,Any]], None], start_poll: bool = True):
    """
    Register a callback to be called when config files change.
    If start_poll True, start a background poll thread (simple).
    """
    global _WATCH_THREAD, _WATCH_STOP
    _WATCHERS.append(callback)
    if start_poll and (_WATCH_THREAD is None or not _WATCH_THREAD.is_alive()):
        _WATCH_STOP = False
        import threading
        _WATCH_THREAD = threading.Thread(target=_watch_loop, daemon=True)
        _WATCH_THREAD.start()
    return True

def stop_watch():
    global _WATCH_STOP
    _WATCH_STOP = True

# -----------------------
# Convenience getters for other modules
# -----------------------
def database_config() -> Dict[str,Any]:
    return get("database", _DEFAULTS["database"])

def build_config() -> Dict[str,Any]:
    return get("build", _DEFAULTS["build"])

def upgrade_config() -> Dict[str,Any]:
    return get("upgrade", _DEFAULTS["upgrade"])

def repos_config() -> Dict[str,Any]:
    return get("repos", _DEFAULTS["repos"])

def logging_config() -> Dict[str,Any]:
    return get("logging", _DEFAULTS["logging"])

# -----------------------
# Initialize module default config on import
# -----------------------
try:
    load()
except Exception:
    _logger.exception("initial config load failed")

# Set up logging early
try:
    init_logging(get_config())
except Exception:
    _logger.exception("init_logging failed")

# End of config.py
