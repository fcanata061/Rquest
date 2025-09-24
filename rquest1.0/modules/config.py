"""
config.py – Sistema de configuração central do Rquest
-----------------------------------------------------
Implementações e melhorias:
1. Cache de configuração carregada e crua (raw) com controle de mtime.
2. Logging de avisos em parsing e erros.
3. Conversão automática de tamanhos humanos ("10MB" → 10485760).
4. Validação estrita de campos desconhecidos com Pydantic (se disponível).
5. Reload seguro sensível a modificações do arquivo.
6. Suporte a seções por módulo (modules.<nome>).
7. Função validate_config() para checagens adicionais.
8. Parsing robusto: tenta YAML e depois JSON.
"""

import os
import sys
import json
import yaml
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union
from pathlib import Path
from threading import RLock
from copy import deepcopy

try:
    from pydantic import BaseModel, ValidationError, Extra
    USE_PYDANTIC = True
except ImportError:
    USE_PYDANTIC = False


# ========================
# Utilidades
# ========================

def _human_size_to_bytes(value: Union[str, int]) -> int:
    """Converte '10MB', '5G', '100K' → bytes inteiros"""
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        return 0
    units = {"K": 1024, "M": 1024**2, "G": 1024**3}
    value = value.strip().upper()
    try:
        if value[-1] in units:
            return int(float(value[:-1]) * units[value[-1]])
        return int(value)
    except Exception:
        logging.warning(f"[config] Valor inválido para tamanho: {value}, usando 0")
        return 0


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Mescla recursivamente dicionários"""
    result = deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _merge_dicts(result[k], v)
        else:
            result[k] = v
    return result


def _load_yaml_or_json_file(path: Path) -> Dict[str, Any]:
    """Carrega arquivo YAML ou JSON com fallback robusto"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            if path.suffix in [".yaml", ".yml"]:
                return yaml.safe_load(f) or {}
            if path.suffix == ".json":
                return json.load(f)
            # tenta yaml primeiro
            try:
                return yaml.safe_load(f) or {}
            except Exception:
                f.seek(0)
                return json.load(f)
    except Exception as e:
        logging.warning(f"[config] Falha ao carregar {path}: {e}")
        return {}


# ========================
# Defaults
# ========================

DEFAULTS: Dict[str, Any] = {
    "logging": {
        "level": "INFO",
        "color": True,
        "max_size": "10MB",
        "follow_buffer": 1000,
    },
    "db": {
        "path": "~/.rquest/db.sqlite3",
        "backup": True,
    },
    "buildsystem": {
        "default": "make",
        "timeout": 300,
    },
    "network": {
        "retries": 3,
        "timeout": 30,
        "proxy": None,
    },
    "resolver": {
        "parallel": 4,
        "cache": True,
    },
    "cli": {
        "interactive": True,
        "color": True,
    },
    "modules": {}  # configurações específicas por módulo
}


# ========================
# Modelos
# ========================

if USE_PYDANTIC:
    class ConfigModel(BaseModel, extra=Extra.forbid):
        logging: Dict[str, Any] = {}
        db: Dict[str, Any] = {}
        buildsystem: Dict[str, Any] = {}
        network: Dict[str, Any] = {}
        resolver: Dict[str, Any] = {}
        cli: Dict[str, Any] = {}
        modules: Dict[str, Dict[str, Any]] = {}

else:
    @dataclass
    class ConfigModel:
        logging: Dict[str, Any] = field(default_factory=dict)
        db: Dict[str, Any] = field(default_factory=dict)
        buildsystem: Dict[str, Any] = field(default_factory=dict)
        network: Dict[str, Any] = field(default_factory=dict)
        resolver: Dict[str, Any] = field(default_factory=dict)
        cli: Dict[str, Any] = field(default_factory=dict)
        modules: Dict[str, Dict[str, Any]] = field(default_factory=dict)


# ========================
# Loader
# ========================

_CONFIG: Optional[ConfigModel] = None
_CONFIG_RAW: Optional[Dict[str, Any]] = None
_CONFIG_LOCK = RLock()
_CONFIG_PATH: Optional[Path] = None
_CONFIG_MTIME: Optional[float] = None


def _find_config_file() -> Optional[Path]:
    """Localiza config.yaml/config.json"""
    env = os.environ.get("RQUEST_CONFIG")
    if env and Path(env).exists():
        return Path(env)
    for candidate in [
        Path.cwd() / "config.yaml",
        Path.cwd() / "config.json",
        Path.home() / ".config/rquest/config.yaml",
        Path("/etc/rquest/config.yaml"),
    ]:
        if candidate.exists():
            return candidate
    return None


def _normalize_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza tipos e valores"""
    cfg = deepcopy(cfg)
    if "logging" in cfg and "max_size" in cfg["logging"]:
        cfg["logging"]["max_size"] = _human_size_to_bytes(cfg["logging"]["max_size"])
    return cfg


def _load_raw_config() -> Dict[str, Any]:
    """Carrega configuração crua com cache de mtime"""
    global _CONFIG_PATH, _CONFIG_MTIME, _CONFIG_RAW
    path = _find_config_file()
    if not path:
        return {}
    try:
        mtime = path.stat().st_mtime
        if _CONFIG_PATH == path and _CONFIG_MTIME == mtime and _CONFIG_RAW:
            return _CONFIG_RAW
        cfg = _load_yaml_or_json_file(path)
        _CONFIG_PATH, _CONFIG_MTIME = path, mtime
        _CONFIG_RAW = cfg
        return cfg
    except Exception as e:
        logging.error(f"[config] Erro ao carregar {path}: {e}")
        return {}


def load_config(force: bool = False) -> ConfigModel:
    """Carrega configuração mesclada com defaults"""
    global _CONFIG, _CONFIG_RAW
    with _CONFIG_LOCK:
        if _CONFIG and not force:
            return _CONFIG

        raw = _load_raw_config()
        merged = _merge_dicts(DEFAULTS, raw)
        merged = _normalize_config(merged)
        _CONFIG_RAW = merged

        if USE_PYDANTIC:
            try:
                _CONFIG = ConfigModel(**merged)
            except ValidationError as e:
                logging.error(f"[config] Erro de validação: {e}")
                sys.exit(1)
        else:
            _CONFIG = ConfigModel(**merged)

        return _CONFIG


def get_config() -> ConfigModel:
    return load_config()


def reload_config() -> ConfigModel:
    return load_config(force=True)


def validate_config() -> bool:
    """Valida coerência da configuração atual"""
    cfg = get_config()
    ok = True

    db_path = cfg.db.get("path")
    if not isinstance(db_path, str):
        logging.error("[config] db.path inválido (não é string)")
        ok = False

    timeout = cfg.buildsystem.get("timeout", 0)
    if not isinstance(timeout, int) or timeout <= 0:
        logging.error("[config] buildsystem.timeout inválido")
        ok = False

    return ok


# ========================
# Funções helpers
# ========================

def get_logging_config() -> Dict[str, Any]:
    return get_config().logging

def get_db_path() -> str:
    return os.path.expanduser(get_config().db.get("path"))

def get_buildsystem_config() -> Dict[str, Any]:
    return get_config().buildsystem

def get_module_config(module_name: str) -> Dict[str, Any]:
    """Retorna config específica de um módulo"""
    return get_config().modules.get(module_name, {})
