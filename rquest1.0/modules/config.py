import os
import sys
import json
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import yaml
except ImportError:
    yaml = None

try:
    from pydantic import BaseModel, Field, validator
except ImportError:
    BaseModel = None

from dataclasses import dataclass, field

# =========================
# DEFAULT CONFIG
# =========================
DEFAULTS: Dict[str, Any] = {
    "logging": {
        "level": "INFO",
        "color": True,
        "file": "/var/log/rquest.log",
        "rotate": True,
        "max_size": 10_000_000,
        "backups": 5,
        "timestamp": True,
        "module_prefix": True,
        "follow_build": True,
        "follow_buffer": 200,
        "follow_colorize": True,
        "json_output": False,
        "metrics": True,
        "alerts": True,
        "filters": [],
    },
    "db": {
        "path": "/var/lib/rquest/rquest.db",
        "timeout": 30,
    },
    "buildsystem": {
        "parallel_jobs": 4,
        "sandbox": True,
        "cflags": ["-O2", "-pipe"],
        "ldflags": [],
    },
    "network": {
        "mirrorlist": [],
        "retries": 3,
        "timeout": 30,
    },
    "resolver": {
        "strategy": "greedy",
        "allow_downgrade": False,
    },
    "cli": {
        "default_action": "help",
        "color": True,
    },
    "toolchain": {
        "default": "gcc",
        "available": ["gcc-13.2", "llvm-17"],
        "bootstrap_stage1": True,
    },
    "python": {
        "default_version": "3.12",
        "allowed_versions": ["3.10", "3.11", "3.12"],
        "enforce_compatibility": True,
    },
    "snapshots": {
        "enabled": True,
        "backend": "btrfs",
        "auto_snapshot": True,
        "snapshot_dir": "/var/lib/rquest/snapshots",
    },
    "notifier": {
        "enabled": True,
        "methods": ["notify_send", "bar_counter"],
        "check_interval": 3600,
        "show_total_updates": True,
    },
    "audit": {
        "auto_integrity_check": True,
        "verify_filesystem": True,
        "signatures": True,
    },
}


# =========================
# UTILS
# =========================
def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge dicts."""
    result = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _merge_dicts(result[k], v)
        else:
            result[k] = v
    return result


def _load_yaml_or_json_file(path: Path) -> Dict[str, Any]:
    """Load YAML/JSON config file."""
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    if yaml:
        try:
            return yaml.safe_load(content) or {}
        except Exception:
            pass
    try:
        return json.loads(content)
    except Exception:
        raise ValueError(f"Config file at {path} is not valid YAML/JSON")


def _candidate_paths() -> list[Path]:
    """Candidate config file locations."""
    env = os.getenv("RQUEST_CONFIG")
    candidates = []
    if env:
        candidates.append(Path(env))
    cwd = Path.cwd()
    candidates += [
        cwd / "config.yaml",
        cwd / "config.yml",
        Path(__file__).resolve().parent.parent / "config.yaml",
        Path(__file__).resolve().parent.parent / "config.yml",
        Path("/etc/rquest/config.yaml"),
        Path("/etc/rquest/config.yml"),
    ]
    return candidates


def _find_config_file() -> Optional[Path]:
    for path in _candidate_paths():
        if path.exists():
            return path
    return None


# =========================
# CONFIG MODELS
# =========================
if BaseModel:

    class ConfigModel(BaseModel):
        logging: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["logging"])
        db: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["db"])
        buildsystem: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["buildsystem"])
        network: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["network"])
        resolver: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["resolver"])
        cli: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["cli"])
        toolchain: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["toolchain"])
        python: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["python"])
        snapshots: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["snapshots"])
        notifier: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["notifier"])
        audit: Dict[str, Any] = Field(default_factory=lambda: DEFAULTS["audit"])

else:

    @dataclass
    class ConfigModel:
        logging: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["logging"])
        db: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["db"])
        buildsystem: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["buildsystem"])
        network: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["network"])
        resolver: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["resolver"])
        cli: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["cli"])
        toolchain: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["toolchain"])
        python: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["python"])
        snapshots: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["snapshots"])
        notifier: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["notifier"])
        audit: Dict[str, Any] = field(default_factory=lambda: DEFAULTS["audit"])


# =========================
# GLOBALS
# =========================
_CONFIG: Optional[ConfigModel] = None
_CONFIG_PATH: Optional[Path] = None


# =========================
# API
# =========================
def load_config() -> ConfigModel:
    global _CONFIG, _CONFIG_PATH
    path = _find_config_file()
    raw = {}
    if path:
        try:
            raw = _load_yaml_or_json_file(path)
            _CONFIG_PATH = path
        except Exception as e:
            print(f"Warning: failed to load config {path}: {e}", file=sys.stderr)
    merged = _merge_dicts(DEFAULTS, raw)
    _CONFIG = ConfigModel(**merged) if BaseModel else ConfigModel(**merged)
    return _CONFIG


def get_config() -> ConfigModel:
    global _CONFIG
    if _CONFIG is None:
        return load_config()
    return _CONFIG


def reload_config() -> ConfigModel:
    return load_config()


def as_dict() -> Dict[str, Any]:
    cfg = get_config()
    if BaseModel:
        return cfg.dict()
    return cfg.__dict__


def config_path_used() -> Optional[Path]:
    return _CONFIG_PATH


# =========================
# SHORTCUTS
# =========================
def get_logging_config() -> Dict[str, Any]:
    return get_config().logging


def get_db_path() -> str:
    return get_config().db["path"]


def get_buildsystem_config() -> Dict[str, Any]:
    return get_config().buildsystem
