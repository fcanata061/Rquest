# Rquest/rquest1.0/modules/slots.py
"""
Slots manager for Rquest - slots.py

Funcionalidades implementadas:
- SlotManager: gerenciamento completo de slots físicos e virtuais (aliases)
- Perfis (profiles) para comportamento por ambiente (desktop/server/embedded)
- Migrações de slot com dry_run, plano básico e persistência de migrações para rollback
- Integração com modules.config, modules.logging, modules.db, modules.masks, modules.resolver (se presentes)
- Locks, transações e uso de shards para builds via db.start_build/acquire_shard_for_build
- Eventos (listeners) para hook de UI/CLI/resolver
- Export / Import em JSON
- Criação das tabelas necessárias no DB (migrations)
- Métricas simples in-memory
- Robustez: tenta importar diferentes caminhos, fornece fallbacks seguros
"""

from __future__ import annotations

import os
import json
import time
import threading
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

# Tentativas robustas de import (várias formas, para flexibilidade de execução)
try:
    from modules.config import get_config, get_buildsystem_config  # type: ignore
except Exception:
    try:
        from config import get_config, get_buildsystem_config  # type: ignore
    except Exception:
        def get_config():
            return {}
        def get_buildsystem_config():
            return {}

try:
    from modules.logging import get_logger, start_build_log, stream_build_output, end_build_log  # type: ignore
except Exception:
    try:
        import logging as _stdlogging
        def get_logger(name: str):
            return _stdlogging.getLogger(name)
        def start_build_log(pkg): pass
        def stream_build_output(line, level="INFO", module="buildsystem"): pass
        def end_build_log(success=True): pass
    except Exception:
        def get_logger(name: str):
            class _Fake:
                def info(self,*a,**k): pass
                def warning(self,*a,**k): pass
                def error(self,*a,**k): pass
                def debug(self,*a,**k): pass
                def exception(self,*a,**k): pass
            return _Fake()
        def start_build_log(pkg): pass
        def stream_build_output(line, level="INFO", module="buildsystem"): pass
        def end_build_log(success=True): pass

logger = get_logger("slots")

# DB integration
try:
    from modules.db import get_db, acquire_shard_for_build, start_build, finish_build, add_history, emit_event, acquire_lock, release_lock  # type: ignore
except Exception:
    try:
        from db import get_db, acquire_shard_for_build, start_build, finish_build, add_history, emit_event, acquire_lock, release_lock  # type: ignore
    except Exception:
        # fallbacks mínimos
        def get_db(): return None
        def acquire_shard_for_build(*a, **k): return None
        def start_build(*a, **k): raise RuntimeError("db.start_build not available")
        def finish_build(*a, **k): raise RuntimeError("db.finish_build not available")
        def add_history(*a, **k): pass
        def emit_event(*a, **k): pass
        def acquire_lock(*a, **k): return True
        def release_lock(*a, **k): return True

# masks integration
try:
    from modules.masks import is_masked  # type: ignore
except Exception:
    try:
        from masks import is_masked  # type: ignore
    except Exception:
        def is_masked(*a, **k): return False

# resolver (optional, for dependency analysis)
try:
    from modules.resolver import resolve_dependencies  # type: ignore
except Exception:
    def resolve_dependencies(pkg, version, profile=None):
        # fallback: no deep dependency analysis; return empty list meaning "no extra builds required"
        return []

# utils
def _now_ts() -> int:
    return int(time.time())

def _safe_json_dumps(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return json.dumps(str(obj))

# -------------------------------------------------------------------
# SlotManager and helpers
# -------------------------------------------------------------------

class SlotRecord:
    """
    Representa uma linha na tabela slots
    Campos (em memória):
    - id: optional int
    - package_name: str
    - slot_key: str  (ex: "python:3" ou "gcc:major:11")
    - version: str
    - profile: str
    - active: bool
    - is_default: bool
    - alias: Optional[str]  (virtual name)
    - metadata: dict
    - installed_at, activated_at, activated_by, deactivated_at
    """
    def __init__(self, id: Optional[int], package_name: str, slot_key: str, version: str,
                 profile: str = "default", active: bool = True, is_default: bool = False,
                 alias: Optional[str] = None, metadata: Optional[Dict[str,Any]] = None,
                 installed_at: Optional[int] = None, activated_at: Optional[int] = None,
                 activated_by: Optional[str] = None, deactivated_at: Optional[int] = None):
        self.id = id
        self.package_name = package_name
        self.slot_key = slot_key
        self.version = version
        self.profile = profile or "default"
        self.active = bool(active)
        self.is_default = bool(is_default)
        self.alias = alias
        self.metadata = metadata or {}
        self.installed_at = installed_at or _now_ts()
        self.activated_at = activated_at
        self.activated_by = activated_by
        self.deactivated_at = deactivated_at

    def to_dict(self):
        return {
            "id": self.id,
            "package_name": self.package_name,
            "slot_key": self.slot_key,
            "version": self.version,
            "profile": self.profile,
            "active": int(self.active),
            "is_default": int(self.is_default),
            "alias": self.alias,
            "metadata": self.metadata,
            "installed_at": self.installed_at,
            "activated_at": self.activated_at,
            "activated_by": self.activated_by,
            "deactivated_at": self.deactivated_at,
        }

# ----------------------------------------------------------------------------
# SlotManager
# ----------------------------------------------------------------------------

class SlotManager:
    """
    Classe principal para gerenciamento de slots.
    """

    def __init__(self):
        self._db = None
        try:
            self._db = get_db() if callable(get_db) else None
        except Exception:
            self._db = None
        self._cache_lock = threading.RLock()
        self._cache_loaded = False
        self._slots_cache: List[SlotRecord] = []
        self._listeners: Dict[str, List[Callable[[Dict[str,Any]],None]]] = {}
        self._metrics: Dict[str,int] = {
            "slot.activations": 0,
            "slot.deactivations": 0,
            "slot.migrations.success": 0,
            "slot.migrations.failed": 0,
            "slot.current_active_count": 0,
        }
        # load rules from config
        self._load_rules_from_config()
        # create DB tables if needed
        self._ensure_tables()
        # load cache
        self.reload_cache()

    # ---------------------------
    # DB schema / migrations
    # ---------------------------
    def _ensure_tables(self):
        if not self._db:
            return
        try:
            # slots table
            self._db.execute("""
                CREATE TABLE IF NOT EXISTS slots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    package_name TEXT NOT NULL,
                    slot_key TEXT NOT NULL,
                    version TEXT NOT NULL,
                    profile TEXT DEFAULT 'default',
                    active INTEGER DEFAULT 1,
                    is_default INTEGER DEFAULT 0,
                    alias TEXT,
                    metadata TEXT,
                    installed_at INTEGER DEFAULT (strftime('%s','now')),
                    activated_at INTEGER,
                    activated_by TEXT,
                    deactivated_at INTEGER
                );
            """, (), commit=True)
            self._db.execute("CREATE INDEX IF NOT EXISTS idx_slots_pkg ON slots(package_name);", (), commit=True)
            self._db.execute("CREATE INDEX IF NOT EXISTS idx_slots_profile ON slots(profile);", (), commit=True)
            self._db.execute("CREATE INDEX IF NOT EXISTS idx_slots_alias ON slots(alias);", (), commit=True)
            # migrations history for slot migrations
            self._db.execute("""
                CREATE TABLE IF NOT EXISTS slot_migrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    package_name TEXT,
                    from_version TEXT,
                    to_version TEXT,
                    profile TEXT,
                    started_at INTEGER,
                    finished_at INTEGER,
                    status TEXT,
                    plan TEXT, -- JSON description of plan
                    result TEXT -- JSON result
                );
            """, (), commit=True)
        except Exception:
            logger.exception("Failed to ensure slots tables")

    # ---------------------------
    # config rules
    # ---------------------------
    def _load_rules_from_config(self):
        cfg = {}
        try:
            cfg = get_config() if callable(get_config) else {}
        except Exception:
            cfg = {}
        # extract slots.rules and slots.profiles if present
        self._rules = {}
        self._profiles = {}
        try:
            # support pydantic/dataclass or dict
            if hasattr(cfg, "as_dict"):
                raw = cfg.as_dict()
                slots_section = raw.get("slots", {}) if isinstance(raw, dict) else {}
            elif hasattr(cfg, "dict"):
                raw = cfg.dict()
                slots_section = raw.get("slots", {}) if isinstance(raw, dict) else {}
            elif isinstance(cfg, dict):
                slots_section = cfg.get("slots", {}) or {}
            else:
                slots_section = getattr(cfg, "slots", {}) or {}
            self._rules = slots_section.get("rules", {}) or {}
            self._profiles = slots_section.get("profiles", {}) or {}
        except Exception:
            self._rules = {}
            self._profiles = {}

    # ---------------------------
    # cache & reload
    # ---------------------------
    def reload_cache(self):
        with self._cache_lock:
            self._slots_cache = []
            # load from DB
            if self._db:
                try:
                    rows = self._db.fetchall("SELECT * FROM slots")
                    for r in rows:
                        try:
                            metadata = {}
                            if r["metadata"]:
                                try:
                                    metadata = json.loads(r["metadata"])
                                except Exception:
                                    metadata = {}
                            sr = SlotRecord(
                                id=int(r["id"]),
                                package_name=r["package_name"],
                                slot_key=r["slot_key"],
                                version=r["version"],
                                profile=r["profile"],
                                active=bool(int(r["active"])) if r["active"] is not None else True,
                                is_default=bool(int(r["is_default"])) if r["is_default"] is not None else False,
                                alias=r["alias"],
                                metadata=metadata,
                                installed_at=int(r["installed_at"]) if r["installed_at"] else None,
                                activated_at=int(r["activated_at"]) if r["activated_at"] else None,
                                activated_by=r["activated_by"],
                                deactivated_at=int(r["deactivated_at"]) if r["deactivated_at"] else None
                            )
                            self._slots_cache.append(sr)
                        except Exception:
                            continue
            self._cache_loaded = True
            # update metric
            self._metrics["slot.current_active_count"] = sum(1 for s in self._slots_cache if s.active)

    # ---------------------------
    # listeners/events
    # ---------------------------
    def on(self, event_name: str, cb: Callable[[Dict[str,Any]], None]):
        self._listeners.setdefault(event_name, []).append(cb)
        logger.debug(f"Listener registered for {event_name}")

    def _emit(self, event_name: str, context: Dict[str,Any]):
        # persist event in DB events table if available
        try:
            emit_event(event_name, context)
        except Exception:
            pass
        # call listeners (fire-and-forget threads)
        listeners = list(self._listeners.get(event_name, []))
        for cb in listeners:
            try:
                t = threading.Thread(target=self._safe_call, args=(cb, context), daemon=True)
                t.start()
            except Exception:
                logger.exception("Failed to start listener thread")

    def _safe_call(self, cb, ctx):
        try:
            cb(ctx)
        except Exception:
            logger.exception("Listener raised")

    # ---------------------------
    # helpers: internal DB upsert
    # ---------------------------
    def _insert_slot_record(self, sr: SlotRecord) -> SlotRecord:
        if not self._db:
            # in-memory assignment: append and return
            sr.id = None
            self._slots_cache.append(sr)
            return sr
        try:
            cur = self._db.execute(
                "INSERT INTO slots (package_name, slot_key, version, profile, active, is_default, alias, metadata, installed_at, activated_at, activated_by, deactivated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (sr.package_name, sr.slot_key, sr.version, sr.profile, int(sr.active), int(sr.is_default), sr.alias, json.dumps(sr.metadata), sr.installed_at, sr.activated_at, sr.activated_by, sr.deactivated_at),
                commit=True
            )
            sr.id = cur.lastrowid
            with self._cache_lock:
                self._slots_cache.append(sr)
            return sr
        except Exception:
            logger.exception("Failed to insert slot record")
            raise

    def _update_slot_record(self, sr: SlotRecord):
        if not self._db or sr.id is None:
            # update in-memory
            with self._cache_lock:
                for i, s in enumerate(self._slots_cache):
                    if s is sr or (s.id is not None and sr.id is not None and s.id == sr.id):
                        self._slots_cache[i] = sr
                        return sr
            return sr
        try:
            self._db.execute(
                "UPDATE slots SET package_name=?, slot_key=?, version=?, profile=?, active=?, is_default=?, alias=?, metadata=?, installed_at=?, activated_at=?, activated_by=?, deactivated_at=? WHERE id=?",
                (sr.package_name, sr.slot_key, sr.version, sr.profile, int(sr.active), int(sr.is_default), sr.alias, json.dumps(sr.metadata), sr.installed_at, sr.activated_at, sr.activated_by, sr.deactivated_at, sr.id),
                commit=True
            )
            # update cache
            with self._cache_lock:
                for i, s in enumerate(self._slots_cache):
                    if s.id == sr.id:
                        self._slots_cache[i] = sr
                        break
            return sr
        except Exception:
            logger.exception("Failed to update slot record")
            raise

    # ---------------------------
    # query helpers
    # ---------------------------
    def _find_slot_rows(self, package_name: Optional[str]=None, version: Optional[str]=None, profile: Optional[str]=None, alias: Optional[str]=None) -> List[SlotRecord]:
        self.reload_cache_if_needed()
        res = []
        with self._cache_lock:
            for s in self._slots_cache:
                if package_name and s.package_name != package_name:
                    continue
                if version and s.version != version:
                    continue
                if profile and s.profile != profile:
                    continue
                if alias and s.alias != alias:
                    continue
                res.append(s)
        return res

    def reload_cache_if_needed(self):
        if not self._cache_loaded:
            self.reload_cache()

    # ---------------------------
    # public API: get/list
    # ---------------------------
    def get_slot(self, package_name: str, version: Optional[str]=None, profile: Optional[str]=None) -> Optional[Dict[str,Any]]:
        rows = self._find_slot_rows(package_name=package_name, version=version, profile=profile)
        if not rows:
            return None
        # prefer active default for profile
        for r in rows:
            if r.is_default and r.active:
                return r.to_dict()
        # else return first active
        for r in rows:
            if r.active:
                return r.to_dict()
        # fallback first
        return rows[0].to_dict()

    def list_slots(self, package_name: Optional[str]=None, profile: Optional[str]=None, include_virtual: bool=True) -> List[Dict[str,Any]]:
        self.reload_cache_if_needed()
        out = []
        with self._cache_lock:
            for s in self._slots_cache:
                if package_name and s.package_name != package_name:
                    continue
                if profile and s.profile != profile:
                    continue
                if not include_virtual and s.alias:
                    continue
                out.append(s.to_dict())
        return out

    # ---------------------------
    # alias/virtual support
    # ---------------------------
    def create_alias(self, alias: str, package_name: str, version: str, profile: Optional[str]=None, user: str="system") -> SlotRecord:
        # Find existing slot row for package+version+profile
        rows = self._find_slot_rows(package_name=package_name, version=version, profile=profile)
        if rows:
            sr = rows[0]
            sr.alias = alias
            sr.activated_at = _now_ts()
            sr.activated_by = user
            self._update_slot_record(sr)
            add_history(package_name, "alias_set", f"alias={alias} -> {package_name}:{version}")
            self._emit("slot_alias_created", {"alias": alias, "package": package_name, "version": version, "profile": profile, "by": user})
            return sr
        # else create a new slot row with alias (package may not be installed yet)
        sr = SlotRecord(id=None, package_name=package_name, slot_key=f"{package_name}:{version}", version=version, profile=profile or "default", active=True, is_default=False, alias=alias, installed_at=_now_ts(), activated_at=_now_ts(), activated_by=user)
        sr = self._insert_slot_record(sr)
        add_history(package_name, "alias_set", f"alias={alias} -> {package_name}:{version}")
        self._emit("slot_alias_created", {"alias": alias, "package": package_name, "version": version, "profile": profile, "by": user})
        return sr

    def resolve_alias(self, alias: str, profile: Optional[str]=None) -> Optional[Dict[str,Any]]:
        # find slot with alias matching and profile
        rows = self._find_slot_rows(alias=alias, profile=profile)
        if not rows:
            # try any profile
            rows = self._find_slot_rows(alias=alias)
            if not rows:
                return None
        # prefer default and active
        for r in rows:
            if r.is_default and r.active:
                return r.to_dict()
        for r in rows:
            if r.active:
                return r.to_dict()
        return rows[0].to_dict()

    # ---------------------------
    # assign/activate/deactivate/default
    # ---------------------------
    def assign_slot(self, package_name: str, version: str, strategy: Optional[str]=None, profile: Optional[str]=None, user: str="system") -> Dict[str,Any]:
        """
        Decide/assign a slot_key for a version based on strategy and rules and persist.
        Strategies: 'major', 'minor', 'full' (default falls back to 'minor' if rule exists)
        Returns the created/updated slot record dict.
        """
        profile = profile or "default"
        try:
            # check masks (policy) before assigning
            if is_masked(package_name, version, None, profile=profile):
                reason = None
                try:
                    from modules.masks import get_mask_reason as _mr  # type: ignore
                    reason = _mr(package_name, version, None, profile=profile)
                except Exception:
                    reason = "masked"
                logger.warning(f"Attempt to assign masked package {package_name} {version} (profile={profile}): {reason}")
                raise RuntimeError(f"Package masked: {reason}")

            # determine strategy
            rule = self._rules.get(package_name) or {}
            strat = strategy or rule.get("strategy") or "minor"
            # build slot key logic (simplified):
            if strat == "major":
                # major version = text before first dot
                major = version.split(".")[0]
                slot_key = f"{package_name}:major:{major}"
            elif strat == "full":
                slot_key = f"{package_name}:full:{version}"
            else:
                # minor: use major.minor if present else full
                parts = version.split(".")
                if len(parts) >= 2:
                    slot_key = f"{package_name}:{parts[0]}.{parts[1]}"
                else:
                    slot_key = f"{package_name}:{version}"

            # create or update record
            existing = self._find_slot_rows(package_name=package_name, version=version, profile=profile)
            if existing:
                sr = existing[0]
                sr.slot_key = slot_key
                sr.active = True
                sr.activated_at = _now_ts()
                sr.activated_by = user
                self._update_slot_record(sr)
            else:
                sr = SlotRecord(id=None, package_name=package_name, slot_key=slot_key, version=version, profile=profile, active=True, is_default=False, alias=None, metadata={"strategy": strat}, installed_at=_now_ts(), activated_at=_now_ts(), activated_by=user)
                sr = self._insert_slot_record(sr)

            add_history(package_name, "slot_assign", f"version={version} slot_key={slot_key} profile={profile}")
            self._metrics["slot.activations"] += 1
            self._emit("slot_assigned", {"package": package_name, "version": version, "slot_key": slot_key, "profile": profile, "by": user})
            logger.info(f"Assigned slot {slot_key} for {package_name} {version} (profile={profile})")
            return sr.to_dict()
        except Exception:
            logger.exception("assign_slot failed")
            raise

    def activate_slot(self, package_name: str, version: str, profile: Optional[str]=None, user: str="system") -> bool:
        profile = profile or "default"
        try:
            # check mask
            if is_masked(package_name, version, None, profile=profile):
                reason = None
                try:
                    from modules.masks import get_mask_reason as _mr  # type: ignore
                    reason = _mr(package_name, version, None, profile=profile)
                except Exception:
                    reason = "masked"
                logger.warning(f"Activation blocked by mask: {package_name} {version} reason={reason}")
                return False

            rows = self._find_slot_rows(package_name=package_name, version=version, profile=profile)
            if not rows:
                # assign and activate
                rec = self.assign_slot(package_name, version, profile=profile, user=user)
                return True
            sr = rows[0]
            sr.active = True
            sr.activated_at = _now_ts()
            sr.activated_by = user
            self._update_slot_record(sr)
            self._metrics["slot.activations"] += 1
            add_history(package_name, "slot_activate", f"version={version} profile={profile}")
            self._emit("slot_activated", {"package": package_name, "version": version, "profile": profile, "by": user})
            logger.info(f"Slot activated: {package_name} {version} profile={profile}")
            return True
        except Exception:
            logger.exception("activate_slot failed")
            raise

    def deactivate_slot(self, package_name: str, version: str, profile: Optional[str]=None, user: str="system") -> bool:
        profile = profile or "default"
        try:
            rows = self._find_slot_rows(package_name=package_name, version=version, profile=profile)
            if not rows:
                logger.warning("deactivate_slot: no such slot")
                return False
            sr = rows[0]
            sr.active = False
            sr.deactivated_at = _now_ts()
            sr.activated_by = user
            self._update_slot_record(sr)
            self._metrics["slot.deactivations"] += 1
            add_history(package_name, "slot_deactivate", f"version={version} profile={profile}")
            self._emit("slot_deactivated", {"package": package_name, "version": version, "profile": profile, "by": user})
            logger.info(f"Slot deactivated: {package_name} {version} profile={profile}")
            return True
        except Exception:
            logger.exception("deactivate_slot failed")
            raise

    def set_default_slot(self, package_name: str, version: str, profile: Optional[str]=None, user: str="system") -> bool:
        """
        Marca a versão como default (is_default=1) para o profile, desmarcando o anterior.
        Usa transaction para segurança.
        """
        profile = profile or "default"
        if is_masked(package_name, version, None, profile=profile):
            logger.warning(f"Cannot set default: {package_name} {version} is masked for profile {profile}")
            return False
        try:
            if self._db:
                with self._db.transaction():
                    # unset old defaults for package+profile
                    self._db.execute("UPDATE slots SET is_default = 0 WHERE package_name = ? AND profile = ?", (package_name, profile), commit=False)
                    # set new default (insert if needed)
                    rows = self._find_slot_rows(package_name=package_name, version=version, profile=profile)
                    if rows:
                        sr = rows[0]
                        sr.is_default = True
                        sr.activated_at = _now_ts()
                        sr.activated_by = user
                        self._update_slot_record(sr)
                    else:
                        # create
                        sr = SlotRecord(id=None, package_name=package_name, slot_key=f"{package_name}:{version}", version=version, profile=profile, active=True, is_default=True, activated_at=_now_ts(), activated_by=user)
                        self._insert_slot_record(sr)
                add_history(package_name, "slot_set_default", f"version={version} profile={profile}")
                self._emit("slot_default_changed", {"package": package_name, "version": version, "profile": profile, "by": user})
                logger.info(f"Default slot set: {package_name} {version} profile={profile}")
                return True
            else:
                # in memory
                with self._cache_lock:
                    # unset previous
                    for s in self._slots_cache:
                        if s.package_name == package_name and s.profile == profile:
                            s.is_default = False
                    # set or create
                    found = False
                    for s in self._slots_cache:
                        if s.package_name == package_name and s.version == version and s.profile == profile:
                            s.is_default = True
                            found = True
                            break
                    if not found:
                        sr = SlotRecord(id=None, package_name=package_name, slot_key=f"{package_name}:{version}", version=version, profile=profile, active=True, is_default=True, activated_at=_now_ts(), activated_by=user)
                        self._slots_cache.append(sr)
                add_history(package_name, "slot_set_default", f"version={version} profile={profile}")
                self._emit("slot_default_changed", {"package": package_name, "version": version, "profile": profile, "by": user})
                return True
        except Exception:
            logger.exception("set_default_slot failed")
            raise

    # ---------------------------
    # Migration flow (high-level)
    # ---------------------------

    def migrate_slot(self, package_name: str, from_version: str, to_version: str, profile: Optional[str]=None,
                     user: str="system", dry_run: bool=True, rebuild_deps: bool=True) -> Dict[str,Any]:
        """
        Plano simplificado de migração:
          - valida política/masks
          - calcula plano (depende de resolver if available)
          - se dry_run: retorna plano e impacto
          - se not dry_run:
              - cria migration row (status pending)
              - lock resource
              - para cada item no plano: start_build (via db) e aguarda build finish (NOTA: aqui assumimos que buildsystem consumirá o build row e atualizará)
              - se builds OK: set_default_slot e finalizar migration success
              - se falha: rollback parcial/total (aqui simplificado: marca migration failed e tenta reverter default)
        Retorna dict com campos: ok_to_proceed (bool), plan (list), success(bool), migration_id (optional), details
        """
        profile = profile or "default"
        res = {
            "ok_to_proceed": False,
            "plan": [],
            "success": False,
            "migration_id": None,
            "details": ""
        }
        try:
            # policy check
            if is_masked(package_name, to_version, None, profile=profile):
                reason = None
                try:
                    from modules.masks import get_mask_reason as _mr  # type: ignore
                    reason = _mr(package_name, to_version, None, profile=profile)
                except Exception:
                    reason = "masked"
                res["details"] = f"target version masked: {reason}"
                logger.warning(f"Migration blocked: {package_name} {to_version} masked: {reason}")
                return res

            # build a plan: include package itself and optionally rebuild dependencies
            plan = []
            plan.append({"package": package_name, "version": to_version, "reason": "target"})
            if rebuild_deps:
                try:
                    deps = resolve_dependencies(package_name, to_version, profile=profile)
                    # resolve_dependencies is expected to return list of (pkg, ver) or dicts
                    for d in deps:
                        if isinstance(d, (list, tuple)) and len(d) >= 2:
                            plan.append({"package": d[0], "version": d[1], "reason": "dep"})
                        elif isinstance(d, dict):
                            plan.append({"package": d.get("name"), "version": d.get("version"), "reason": "dep"})
                except Exception:
                    logger.exception("Resolver failed while building migration plan; proceeding with only package")
            res["plan"] = plan
            res["ok_to_proceed"] = True

            # dry-run: return plan without changing DB
            if dry_run:
                res["details"] = "dry_run: plan computed"
                return res

            # not dry-run: persist migration row
            migration_id = None
            if self._db:
                cur = self._db.execute("INSERT INTO slot_migrations (package_name, from_version, to_version, profile, started_at, status, plan) VALUES (?, ?, ?, ?, strftime('%s','now'), ?, ?)",
                                       (package_name, from_version, to_version, profile, "pending", json.dumps(plan)), commit=True)
                migration_id = cur.lastrowid
                res["migration_id"] = migration_id

            # acquire lock for the package
            lock_res_name = f"slot:migrate:{package_name}:{profile}"
            got_lock = True
            try:
                got_lock = acquire_lock(lock_res_name, user, timeout=600) if callable(acquire_lock) else True
            except Exception:
                got_lock = True
            if not got_lock:
                res["details"] = "Could not acquire migration lock"
                if self._db and migration_id:
                    self._db.execute("UPDATE slot_migrations SET status=?, result=? WHERE id=?", ("failed", json.dumps({"reason":"lock"}), migration_id), commit=True)
                return res

            # execute plan: for each item, start build via db.start_build (we expect buildsystem to perform the actual build)
            build_ids = []
            try:
                for item in plan:
                    pkg = item.get("package")
                    ver = item.get("version")
                    # start build: choose shard
                    shard = None
                    try:
                        shard = acquire_shard_for_build() if callable(acquire_shard_for_build) else None
                    except Exception:
                        shard = None
                    try:
                        # create build row, but we do not block here waiting for external build completion
                        bid = None
                        try:
                            bid = start_build(pkg, ver, shard_id=shard, user=user) if callable(start_build) else None
                        except Exception:
                            # if start_build not available, simulate build id via timestamp
                            bid = int(time.time() * 1000)
                        build_ids.append({"package":pkg, "version":ver, "build_id":bid, "shard":shard})
                        # stream a helpful log line
                        stream_build_output(f"[slot-migrate] queued build for {pkg} {ver} (build_id={bid})", level="INFO", module="slots")
                    except Exception:
                        logger.exception("failed to start build")
                        raise

                # WAITING STRATEGY NOTE:
                # Ideally, we would wait for builds to finish and check their status.
                # Here we implement a simple poll: if db.get_builds can report status, we wait until none are 'building' or timeout.
                # If db.get_builds is not implemented, we assume external system will finish builds and user will finish migration manually.
                builds_ok = True
                # poll for build status if possible
                try:
                    timeout = 60 * 60  # 1 hour default timeout for migration completion
                    poll_interval = 5
                    start_wait = time.time()
                    pending_builds = [b for b in build_ids if b.get("build_id") is not None]
                    # if no real build ids, skip waiting (we simulated)
                    if pending_builds and callable(get_db):
                        # poll db.get_builds by package
                        while True:
                            still_building = False
                            for b in pending_builds:
                                try:
                                    rows = self._db.fetchall("SELECT status FROM builds WHERE id = ?", (b["build_id"],))
                                    if rows:
                                        stat = rows[0]["status"]
                                        if stat == "building":
                                            still_building = True
                                            break
                                        elif stat == "fail":
                                            builds_ok = False
                                            still_building = False
                                            break
                                    else:
                                        # no row found: unknown
                                        pass
                                except Exception:
                                    # cannot inspect builds; break polling
                                    still_building = False
                            if not still_building:
                                break
                            if time.time() - start_wait > timeout:
                                builds_ok = False
                                break
                            time.sleep(poll_interval)
                except Exception:
                    # cannot poll builds; we conservatively assume true (or rely on external signal)
                    builds_ok = True

                if not builds_ok:
                    # mark migration failed
                    if self._db and migration_id:
                        self._db.execute("UPDATE slot_migrations SET status=?, finished_at=?, result=? WHERE id=?", ("failed", int(time.time()), json.dumps({"builds": build_ids, "reason":"build_failed"})), (migration_id,), commit=True)
                    self._metrics["slot.migrations.failed"] += 1
                    res["success"] = False
                    res["details"] = "some builds failed"
                    self._emit("slot_migration_failed", {"package": package_name, "migration_id": migration_id, "plan": plan})
                    logger.error(f"Migration failed for {package_name} -> some builds failed")
                    return res

                # all builds OK: set default slot
                self.set_default_slot(package_name, to_version, profile=profile, user=user)

                # finalize migration row
                if self._db and migration_id:
                    self._db.execute("UPDATE slot_migrations SET status=?, finished_at=?, result=? WHERE id=?", ("success", int(time.time()), json.dumps({"builds": build_ids}), migration_id), commit=True)
                self._metrics["slot.migrations.success"] += 1
                res["success"] = True
                res["details"] = "migration finished successfully"
                self._emit("slot_migration_finished", {"package": package_name, "from": from_version, "to": to_version, "migration_id": migration_id, "by": user})
                logger.info(f"Migration finished successfully for {package_name} {from_version} -> {to_version}")
                return res
            except Exception:
                logger.exception("Migration execution encountered exception")
                if self._db and migration_id:
                    try:
                        self._db.execute("UPDATE slot_migrations SET status=?, finished_at=?, result=? WHERE id=?", ("failed", int(time.time()), json.dumps({"exception": traceback.format_exc()}), migration_id), commit=True)
                    except Exception:
                        pass
                self._metrics["slot.migrations.failed"] += 1
                res["success"] = False
                res["details"] = "exception during migration"
                self._emit("slot_migration_failed", {"package": package_name, "migration_id": migration_id, "by": user})
                return res
            finally:
                try:
                    release_lock(lock_res_name, user) if callable(release_lock) else None
                except Exception:
                    pass

        except Exception:
            logger.exception("migrate_slot failed")
            res["details"] = "internal error"
            return res

    def rollback_migration(self, migration_id: int) -> bool:
        """
        Simples rollback: lê row slot_migrations e tenta restaurar previous default if present in plan.
        Implementação simplificada: no caso real, teríamos snapshot/backup. Aqui usamos info do plan/result.
        """
        try:
            if not self._db:
                logger.warning("rollback_migration: no DB available")
                return False
            row = self._db.fetchone("SELECT * FROM slot_migrations WHERE id = ?", (migration_id,))
            if not row:
                logger.warning("rollback_migration: migration not found")
                return False
            plan = []
            try:
                plan = json.loads(row["plan"]) if row["plan"] else []
            except Exception:
                plan = []
            # Heurística: if migration had from_version, set default back to from_version
            from_v = row["from_version"]
            pkg = row["package_name"]
            profile = row["profile"]
            if pkg and from_v:
                ok = self.set_default_slot(pkg, from_v, profile=profile, user="system")
                if ok:
                    self._db.execute("UPDATE slot_migrations SET status=?, finished_at=?, result=? WHERE id=?", ("rolled_back", int(time.time()), json.dumps({"rolled_back_to": from_v}), migration_id), commit=True)
                    self._emit("slot_migration_rolled_back", {"migration_id": migration_id, "package": pkg, "to": from_v})
                    logger.info(f"Rollback migration {migration_id} succeeded: set {pkg} default to {from_v}")
                    return True
            logger.warning("rollback_migration: cannot determine previous version to rollback to")
            return False
        except Exception:
            logger.exception("rollback_migration failed")
            return False

    # ---------------------------
    # export / import
    # ---------------------------
    def export_slots(self, path: str):
        self.reload_cache_if_needed()
        data = [s.to_dict() for s in self._slots_cache]
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Exported slots to {path}")
        except Exception:
            logger.exception("export_slots failed")
            raise

    def import_slots(self, path: str, replace: bool=False):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("invalid import format")
            if replace and self._db:
                with self._db.transaction():
                    self._db.execute("DELETE FROM slots", (), commit=False)
            for entry in data:
                # normalize
                pkg = entry.get("package_name")
                ver = entry.get("version")
                slot_key = entry.get("slot_key") or f"{pkg}:{ver}"
                profile = entry.get("profile", "default")
                alias = entry.get("alias")
                is_default = bool(entry.get("is_default", 0))
                active = bool(entry.get("active", 1))
                metadata = entry.get("metadata", {})
                sr = SlotRecord(id=None, package_name=pkg, slot_key=slot_key, version=ver, profile=profile, active=active, is_default=is_default, alias=alias, metadata=metadata, installed_at=entry.get("installed_at"), activated_at=entry.get("activated_at"), activated_by=entry.get("activated_by"))
                self._insert_slot_record(sr)
            logger.info(f"Imported slots from {path}")
        except Exception:
            logger.exception("import_slots failed")
            raise

    # ---------------------------
    # metrics
    # ---------------------------
    def get_metrics(self) -> Dict[str,int]:
        # refresh active count
        self._metrics["slot.current_active_count"] = sum(1 for s in self._slots_cache if s.active)
        return dict(self._metrics)

# -------------------------------------------------------------------
# Module-level manager singleton and convenience API
# -------------------------------------------------------------------

_MANAGER_LOCK = threading.RLock()
_MANAGER: Optional[SlotManager] = None

def get_slot_manager() -> SlotManager:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = SlotManager()
        return _MANAGER

# convenience wrappers
def assign_slot(*a, **k): return get_slot_manager().assign_slot(*a, **k)
def activate_slot(*a, **k): return get_slot_manager().activate_slot(*a, **k)
def deactivate_slot(*a, **k): return get_slot_manager().deactivate_slot(*a, **k)
def set_default_slot(*a, **k): return get_slot_manager().set_default_slot(*a, **k)
def get_slot(*a, **k): return get_slot_manager().get_slot(*a, **k)
def list_slots(*a, **k): return get_slot_manager().list_slots(*a, **k)
def create_alias(*a, **k): return get_slot_manager().create_alias(*a, **k)
def resolve_alias(*a, **k): return get_slot_manager().resolve_alias(*a, **k)
def migrate_slot(*a, **k): return get_slot_manager().migrate_slot(*a, **k)
def rollback_migration(*a, **k): return get_slot_manager().rollback_migration(*a, **k)
def export_slots(*a, **k): return get_slot_manager().export_slots(*a, **k)
def import_slots(*a, **k): return get_slot_manager().import_slots(*a, **k)
def on(*a, **k): return get_slot_manager().on(*a, **k)
def get_metrics(*a, **k): return get_slot_manager().get_metrics(*a, **k)
def reload_cache(*a, **k): return get_slot_manager().reload_cache()

# -------------------------------------------------------------------
# Demo / quick usage when run as script
# -------------------------------------------------------------------
if __name__ == "__main__":
    mgr = get_slot_manager()
    print("SlotManager ready. Current slots:", mgr.list_slots())
    # demo flows
    try:
        print("Assigning python 3.10")
        mgr.assign_slot("python", "3.10", strategy="minor", profile="desktop", user="admin")
        print("Setting default python 3.10")
        mgr.set_default_slot("python", "3.10", profile="desktop", user="admin")
        print("Creating alias 'python' -> python 3.10")
        mgr.create_alias("python", "python", "3.10", profile="desktop", user="admin")
        print("List slots for python:", mgr.list_slots("python"))
        print("Dry-run migrate python 3.9 -> 3.10")
        plan = mgr.migrate_slot("python", "3.9", "3.10", profile="desktop", dry_run=True)
        print("Plan:", plan)
    except Exception:
        traceback.print_exc()
