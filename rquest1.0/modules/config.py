# Rquest/rquest1.0/modules/config.py
"""
Rquest config loader and schema

Features:
- Loads configuration from YAML (PyYAML preferred) or JSON (fallback) or defaults.
- Priority:
    1) Environment variable RQUEST_CONFIG -> path
    2) ./config.yaml
    3) ./Rquest/rquest1.0/config.yaml
    4) /etc/rquest/config.yaml
    5) Defaults
- Uses pydantic for validation when available, otherwise uses dataclasses + manual validation.
- Exposes get_config() singleton, reload_config(), as_dict().
- Minimal, safe: avoids importing local logging module to prevent circular imports.
"""

from __future__ import annotations

import os
import json
import sys
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field, asdict
from pathlib import Path

# Try to import PyYAML
_HAS_YAML = False
try:
    import yaml  # type: ignore
    _HAS_YAML = True
except Exception:
    _HAS_YAML = False

# Try to import pydantic
_HAS_PYDANTIC = False
try:
    from pydantic import BaseModel, Field, ValidationError, validator  # type: ignore
    _HAS_PYDANTIC = True
except Exception:
    _HAS_PYDANTIC = False

# -------------------------------------------------------------------
# Defaults (comprehensive)
# -------------------------------------------------------------------
DEFAULTS = {
    "logging": {
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
    },
    "db": {
        "path": "./rquest.db",
        "timeout": 30,
    },
    "buildsystem": {
        "parallel_jobs": 4,
        "sandbox": True,
        "cflags": ["-O2", "-g"],
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
        "default_action": "install",
        "color": True,
    },
}


# -------------------------------------------------------------------
# Small helpers
# -------------------------------------------------------------------
def _human_size_to_bytes(s: Optional[str]) -> int:
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


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge override into base (returns new dict)."""
    result = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            result[k] = _merge_dicts(base[k], v)
        else:
            result[k] = v
    return result


def _load_yaml_or_json_file(path: str) -> Optional[Dict[str, Any]]:
    """
    Try load YAML (if available) or JSON. Returns dict or None if parse fails.
    """
    try:
        content = Path(path).read_text(encoding="utf-8")
    except Exception:
        return None
    if _HAS_YAML:
        try:
            parsed = yaml.safe_load(content)
            if isinstance(parsed, dict):
                return parsed
            return None
        except Exception:
            # fallback to JSON
            pass
    # Try JSON
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        # minimal fallback: try to parse only top-level mapping lines "key: value"
        parsed = {}
        for raw in content.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                key, val = line.split(":", 1)
                parsed[key.strip()] = val.strip()
        if parsed:
            return parsed
    return None


# -------------------------------------------------------------------
# Config schema definitions (pydantic or dataclasses)
# -------------------------------------------------------------------
if _HAS_PYDANTIC:
    class AlertsModel(BaseModel):
        enable: bool = False
        on_error: Optional[str] = None
        on_critical: Optional[str] = None
        smtp: Optional[Dict[str, Any]] = None

    class LoggingModel(BaseModel):
        level: str = Field("INFO")
        color: bool = True
        file: Optional[str] = None
        rotate: bool = True
        max_size: str = "10MB"
        backups: int = 5
        timestamp: bool = True
        module_prefix: bool = True
        follow_build: bool = True
        follow_buffer: int = 200
        follow_colorize: bool = True
        json_output: bool = False
        metrics: bool = True
        alerts: AlertsModel = Field(default_factory=AlertsModel)
        filters: List[Dict[str, Any]] = Field(default_factory=list)

        @validator("level")
        def _validate_level(cls, v):
            levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if isinstance(v, str) and v.upper() in levels:
                return v.upper()
            raise ValueError("Invalid logging.level, expected one of: " + ", ".join(levels))

    class DBModel(BaseModel):
        path: str = "./rquest.db"
        timeout: int = 30

    class BuildsystemModel(BaseModel):
        parallel_jobs: int = 4
        sandbox: bool = True
        cflags: List[str] = Field(default_factory=lambda: ["-O2", "-g"])
        ldflags: List[str] = Field(default_factory=list)

    class NetworkModel(BaseModel):
        mirrorlist: List[str] = Field(default_factory=list)
        retries: int = 3
        timeout: int = 30

    class ResolverModel(BaseModel):
        strategy: str = "greedy"
        allow_downgrade: bool = False

    class CLIModel(BaseModel):
        default_action: str = "install"
        color: bool = True

    class ConfigModel(BaseModel):
        logging: LoggingModel = Field(default_factory=LoggingModel)
        db: DBModel = Field(default_factory=DBModel)
        buildsystem: BuildsystemModel = Field(default_factory=BuildsystemModel)
        network: NetworkModel = Field(default_factory=NetworkModel)
        resolver: ResolverModel = Field(default_factory=ResolverModel)
        cli: CLIModel = Field(default_factory=CLIModel)

        def as_dict(self) -> dict:
            return self.dict()

else:
    # dataclass-based fallback
    @dataclass
    class AlertsModel:
        enable: bool = False
        on_error: Optional[str] = None
        on_critical: Optional[str] = None
        smtp: Optional[Dict[str, Any]] = None

    @dataclass
    class LoggingModel:
        level: str = "INFO"
        color: bool = True
        file: Optional[str] = None
        rotate: bool = True
        max_size: str = "10MB"
        backups: int = 5
        timestamp: bool = True
        module_prefix: bool = True
        follow_build: bool = True
        follow_buffer: int = 200
        follow_colorize: bool = True
        json_output: bool = False
        metrics: bool = True
        alerts: AlertsModel = field(default_factory=AlertsModel)
        filters: List[Dict[str, Any]] = field(default_factory=list)

        def __post_init__(self):
            valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if isinstance(self.level, str):
                if self.level.upper() not in valid:
                    print(f"[config] warning: invalid logging.level '{self.level}', using 'INFO'")
                    self.level = "INFO"
                else:
                    self.level = self.level.upper()
            else:
                self.level = "INFO"

    @dataclass
    class DBModel:
        path: str = "./rquest.db"
        timeout: int = 30

    @dataclass
    class BuildsystemModel:
        parallel_jobs: int = 4
        sandbox: bool = True
        cflags: List[str] = field(default_factory=lambda: ["-O2", "-g"])
        ldflags: List[str] = field(default_factory=list)

    @dataclass
    class NetworkModel:
        mirrorlist: List[str] = field(default_factory=list)
        retries: int = 3
        timeout: int = 30

    @dataclass
    class ResolverModel:
        strategy: str = "greedy"
        allow_downgrade: bool = False

    @dataclass
    class CLIModel:
        default_action: str = "install"
        color: bool = True

    @dataclass
    class ConfigModel:
        logging: LoggingModel = field(default_factory=LoggingModel)
        db: DBModel = field(default_factory=DBModel)
        buildsystem: BuildsystemModel = field(default_factory=BuildsystemModel)
        network: NetworkModel = field(default_factory=NetworkModel)
        resolver: ResolverModel = field(default_factory=ResolverModel)
        cli: CLIModel = field(default_factory=CLIModel)

        def as_dict(self) -> dict:
            return asdict(self)


# -------------------------------------------------------------------
# Loader
# -------------------------------------------------------------------
_config_singleton: Optional[ConfigModel] = None
_config_path_used: Optional[str] = None


def _candidate_paths() -> List[str]:
    paths = []
    env_path = os.getenv("RQUEST_CONFIG")
    if env_path:
        paths.append(env_path)
    # local cwd
    paths.extend([
        os.path.join(os.getcwd(), "config.yaml"),
        os.path.join(os.getcwd(), "config.yml"),
        os.path.join(os.getcwd(), "Rquest", "rquest1.0", "config.yaml"),
        os.path.join(os.getcwd(), "Rquest", "rquest1.0", "config.yml"),
        "/etc/rquest/config.yaml",
        "/etc/rquest/config.yml",
    ])
    return paths


def _find_config_file() -> Optional[str]:
    for p in _candidate_paths():
        if p and os.path.exists(p) and os.path.isfile(p):
            return p
    return None


def _load_raw_config() -> Dict[str, Any]:
    """
    Load raw mapping from config file (yaml/json) or return empty dict.
    """
    p = _find_config_file()
    global _config_path_used
    _config_path_used = p
    if p:
        parsed = _load_yaml_or_json_file(p)
        if isinstance(parsed, dict):
            # If top-level contains 'logging' etc inside 'rquest' or 'rquest1.0', try to find
            if "rquest" in parsed and isinstance(parsed["rquest"], dict):
                parsed = parsed["rquest"]
            return parsed
    return {}


def _normalize_and_merge(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge raw dict into DEFAULTS (recursive).
    Also coerce certain fields (like max_size).
    """
    merged = _merge_dicts(DEFAULTS, raw or {})
    # Ensure types
    # Example: ensure follow_buffer is int
    try:
        merged["logging"]["follow_buffer"] = int(merged["logging"].get("follow_buffer", DEFAULTS["logging"]["follow_buffer"]))
    except Exception:
        merged["logging"]["follow_buffer"] = DEFAULTS["logging"]["follow_buffer"]
    # Ensure backups
    try:
        merged["logging"]["backups"] = int(merged["logging"].get("backups", DEFAULTS["logging"]["backups"]))
    except Exception:
        merged["logging"]["backups"] = DEFAULTS["logging"]["backups"]
    # max_size keep as string here; other modules can convert using helper
    return merged


def _build_config_model(raw_merged: Dict[str, Any]) -> ConfigModel:
    """
    Construct ConfigModel (pydantic or dataclass) from merged dict.
    """
    # Ensure nested defaults exist
    r = raw_merged or {}
    # pydantic path
    if _HAS_PYDANTIC:
        try:
            cfg = ConfigModel.parse_obj(r)
            return cfg
        except Exception as e:
            # Validation error: print and fall back to defaults merged where possible
            print(f"[config] validation error: {e}", file=sys.stderr)
            try:
                # Attempt partial parse: construct by sections
                constructed = {}
                for key in ("logging", "db", "buildsystem", "network", "resolver", "cli"):
                    constructed[key] = r.get(key, DEFAULTS.get(key, {}))
                cfg = ConfigModel.parse_obj(constructed)
                return cfg
            except Exception:
                # last resort: return default model
                return ConfigModel()
    else:
        # dataclass path: create instances by mapping keys
        try:
            logging_section = r.get("logging", DEFAULTS["logging"])
            alerts = logging_section.get("alerts", DEFAULTS["logging"]["alerts"])
            if isinstance(alerts, dict):
                alerts_obj = AlertsModel(**alerts) if not _HAS_PYDANTIC else AlertsModel.parse_obj(alerts)
            else:
                alerts_obj = AlertsModel()
            log_model = LoggingModel(
                level=logging_section.get("level", DEFAULTS["logging"]["level"]),
                color=logging_section.get("color", DEFAULTS["logging"]["color"]),
                file=logging_section.get("file", DEFAULTS["logging"]["file"]),
                rotate=logging_section.get("rotate", DEFAULTS["logging"]["rotate"]),
                max_size=logging_section.get("max_size", DEFAULTS["logging"]["max_size"]),
                backups=int(logging_section.get("backups", DEFAULTS["logging"]["backups"])),
                timestamp=logging_section.get("timestamp", DEFAULTS["logging"]["timestamp"]),
                module_prefix=logging_section.get("module_prefix", DEFAULTS["logging"]["module_prefix"]),
                follow_build=bool(logging_section.get("follow_build", DEFAULTS["logging"]["follow_build"])),
                follow_buffer=int(logging_section.get("follow_buffer", DEFAULTS["logging"]["follow_buffer"])),
                follow_colorize=bool(logging_section.get("follow_colorize", DEFAULTS["logging"]["follow_colorize"])),
                json_output=bool(logging_section.get("json_output", DEFAULTS["logging"]["json_output"])),
                metrics=bool(logging_section.get("metrics", DEFAULTS["logging"]["metrics"])),
                alerts=alerts_obj,
                filters=logging_section.get("filters", DEFAULTS["logging"]["filters"]) or [],
            )
            db_section = r.get("db", DEFAULTS["db"])
            db_model = DBModel(path=db_section.get("path", DEFAULTS["db"]["path"]),
                               timeout=int(db_section.get("timeout", DEFAULTS["db"]["timeout"])))
            bs_section = r.get("buildsystem", DEFAULTS["buildsystem"])
            bs_model = BuildsystemModel(
                parallel_jobs=int(bs_section.get("parallel_jobs", DEFAULTS["buildsystem"]["parallel_jobs"])),
                sandbox=bool(bs_section.get("sandbox", DEFAULTS["buildsystem"]["sandbox"])),
                cflags=bs_section.get("cflags", DEFAULTS["buildsystem"]["cflags"]) or [],
                ldflags=bs_section.get("ldflags", DEFAULTS["buildsystem"]["ldflags"]) or [],
            )
            net_section = r.get("network", DEFAULTS["network"])
            net_model = NetworkModel(
                mirrorlist=net_section.get("mirrorlist", DEFAULTS["network"]["mirrorlist"]) or [],
                retries=int(net_section.get("retries", DEFAULTS["network"]["retries"])),
                timeout=int(net_section.get("timeout", DEFAULTS["network"]["timeout"])),
            )
            resolver_section = r.get("resolver", DEFAULTS["resolver"])
            resolver_model = ResolverModel(
                strategy=resolver_section.get("strategy", DEFAULTS["resolver"]["strategy"]),
                allow_downgrade=bool(resolver_section.get("allow_downgrade", DEFAULTS["resolver"]["allow_downgrade"]))
            )
            cli_section = r.get("cli", DEFAULTS["cli"])
            cli_model = CLIModel(
                default_action=cli_section.get("default_action", DEFAULTS["cli"]["default_action"]),
                color=bool(cli_section.get("color", DEFAULTS["cli"]["color"]))
            )
            cfg = ConfigModel(
                logging=log_model,
                db=db_model,
                buildsystem=bs_model,
                network=net_model,
                resolver=resolver_model,
                cli=cli_model,
            )
            return cfg
        except Exception as e:
            print(f"[config] fallback build error: {e}", file=sys.stderr)
            return ConfigModel()


# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------
def load_config() -> ConfigModel:
    """
    Load configuration (raw file -> merged -> model) and return model instance.
    This does NOT memoize; use get_config() to get singleton.
    """
    raw = _load_raw_config()
    merged = _normalize_and_merge(raw)
    cfg_model = _build_config_model(merged)
    return cfg_model


def get_config() -> ConfigModel:
    """
    Return singleton config model. Loads it on first call.
    """
    global _config_singleton
    if _config_singleton is None:
        _config_singleton = load_config()
    return _config_singleton


def reload_config() -> ConfigModel:
    """
    Force reload from disk (if any) and replace singleton.
    """
    global _config_singleton
    _config_singleton = load_config()
    return _config_singleton


def as_dict() -> Dict[str, Any]:
    cfg = get_config()
    # pydantic BaseModel has .dict(), dataclass has as_dict
    try:
        if _HAS_PYDANTIC and isinstance(cfg, ConfigModel):
            return cfg.as_dict()
    except Exception:
        pass
    try:
        return cfg.as_dict()
    except Exception:
        # fallback: try to convert dataclass
        try:
            return asdict(cfg)
        except Exception:
            return {}


def config_path_used() -> Optional[str]:
    """
    Returns the path of the config file used (or None if defaults).
    """
    return _config_path_used


# -------------------------------------------------------------------
# Utilities for other modules
# -------------------------------------------------------------------
def get_logging_config() -> Dict[str, Any]:
    c = get_config()
    # Return logging section as plain dict
    d = {}
    try:
        if _HAS_PYDANTIC:
            d = c.logging.dict()
        else:
            # dataclass
            d = asdict(c.logging)
    except Exception:
        # last resort: map fields manually
        d = c.logging if isinstance(c.logging, dict) else {}
    return d


def get_db_path() -> str:
    c = get_config()
    return getattr(c.db, "path", DEFAULTS["db"]["path"])


def get_buildsystem_config() -> Dict[str, Any]:
    c = get_config()
    try:
        if _HAS_PYDANTIC:
            return c.buildsystem.dict()
        return asdict(c.buildsystem)
    except Exception:
        return DEFAULTS["buildsystem"]


# -------------------------------------------------------------------
# If run as script, print resolved config
# -------------------------------------------------------------------
if __name__ == "__main__":
    cfg = get_config()
    print("Configuration (merged):")
    try:
        import pprint
        pprint.pprint(as_dict())
    except Exception:
        print(as_dict())
    print("Config file used:", config_path_used())
