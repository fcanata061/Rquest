# Rquest/rquest1.0/modules/masks.py
"""
masks.py - sistema de políticas / máscaras de pacotes para Rquest

Funcionalidades:
- Carrega máscaras iniciais do config (config.yaml) e permite máscaras dinâmicas persistidas no DB.
- Integração com modules.config, modules.db, modules.logging (tenta vários caminhos).
- Persiste máscaras em tabela `masks` (cria tabela se não existir).
- Suporte a:
    - regras por pacote (nome exato, wildcard com glob/regex)
    - regras por versão (expressões simples: =1.2.3, >=1.2, <2.0, *, etc.)
    - regras por arquitetura
    - keywords (stable/testing/masked)
    - máscaras temporárias (valid_until timestamp)
    - perfis (server/desktop/etc) vindos do config.yaml
- API pública:
    - is_masked(name, version=None, arch=None, profile=None) -> bool
    - get_mask_reason(name, version=None, arch=None, profile=None) -> Optional[str]
    - enforce_masks(pkg_list, profile=None) -> filtered pkg_list
    - add_mask(...), remove_mask(mask_id), list_masks(...)
    - import_masks(path), export_masks(path)
- Emite eventos em DB: 'mask_added', 'mask_removed', 'mask_blocked_attempt'
- Logs via get_logger("masks")

Observações:
- Versão de comparação de versão é simples e usa parsing sem dependências externas.
  Não é um resolvedor semântico completo, mas cobre operadores comuns (=, >=, <=, >, <, *).
- Para matching de pacotes, suportamos:
    - nome exato "glibc"
    - wildcard estilo shell "libfoo-*"
    - regex: prefix "re:" ex: "re:^lib.*-dev$"
"""

from __future__ import annotations

import os
import re
import time
import json
import fnmatch
from typing import Any, Dict, List, Optional, Tuple

# Tentativa robusta de importar dependências do projeto
try:
    from modules.config import get_config, get_logging_config  # type: ignore
except Exception:
    try:
        from config import get_config, get_logging_config  # type: ignore
    except Exception:
        # fallback simples
        def get_config():
            return {}
        def get_logging_config():
            return {}

# Import do DB
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    try:
        from db import get_db, add_history, emit_event  # type: ignore
    except Exception:
        # fallback minimal que apenas keeps things in memory
        get_db = None
        def add_history(*a, **k): pass
        def emit_event(*a, **k): pass

# Import do logger (projeto) com fallback para stdlib
try:
    from modules.logging import get_logger  # type: ignore
except Exception:
    try:
        import logging as _stdlogging
        def get_logger(name: str):
            return _stdlogging.getLogger(name)
    except Exception:
        def get_logger(name: str):
            class _Fake:
                def info(self,*a,**k): pass
                def warning(self,*a,**k): pass
                def error(self,*a,**k): pass
                def debug(self,*a,**k): pass
                def exception(self,*a,**k): pass
            return _Fake()

logger = get_logger("masks")

# ---------------------------------------------------------------------
# Helpers: version comparator (básico), pattern matching etc.
# ---------------------------------------------------------------------
def _now_ts() -> int:
    return int(time.time())

def _parse_version(v: Optional[str]) -> Tuple:
    """
    Converte string de versão em tupla de inteiros/strings para comparação básica.
    Exemplos:
      "1.2.3" -> (1,2,3)
      "1.2a" -> (1,2,'a')
    """
    if v is None:
        return tuple()
    parts = re.split(r'[\.\-+_]', str(v))
    parsed = []
    for p in parts:
        if p.isdigit():
            parsed.append(int(p))
        else:
            # separa prefix numérico se houver (ex: "1a" -> 1, 'a')
            m = re.match(r'^(\d+)([^\d].*)$', p)
            if m:
                parsed.append(int(m.group(1)))
                parsed.append(m.group(2))
            else:
                parsed.append(p)
    return tuple(parsed)

def _compare_versions(vleft: Optional[str], op: str, vright: Optional[str]) -> bool:
    """Comparador simples para operadores comuns. '*' sempre corresponde."""
    if op == "*" or vright == "*" or vleft == "*" or vleft is None or vright is None:
        # se qualquer lado for *, considere match (operator '*' means always true)
        if op == "*" or vright == "*" or vleft == "*":
            return True
    left = _parse_version(vleft)
    right = _parse_version(vright)
    try:
        if op == "=" or op == "==":
            return left == right
        if op == ">":
            return left > right
        if op == "<":
            return left < right
        if op == ">=":
            return left >= right
        if op == "<=":
            return left <= right
    except Exception:
        # se falhar na comparação por tipos diferentes, fallback para string compare
        try:
            if op in ("=", "=="):
                return str(vleft) == str(vright)
            if op == ">":
                return str(vleft) > str(vright)
            if op == "<":
                return str(vleft) < str(vright)
            if op == ">=":
                return str(vleft) >= str(vright)
            if op == "<=":
                return str(vleft) <= str(vright)
        except Exception:
            return False
    return False

def _match_version_rule(version: Optional[str], rule: str) -> bool:
    """
    rule examples:
      "*"
      "=1.2.3"
      ">=1.2"
      "<2.0"
      "1.2" (treated as =1.2)
    """
    r = rule.strip()
    if r == "*" or r == "":
        return True
    # operator at start?
    m = re.match(r'^(>=|<=|>|<|==|=)\s*(.+)$', r)
    if m:
        op = m.group(1)
        ver = m.group(2).strip()
        return _compare_versions(version, op, ver)
    # else treat as equality
    return _compare_versions(version, "=", r)

def _match_name_pattern(name: str, pattern: str) -> bool:
    """
    Suporta:
      - exact name 'glibc'
      - glob pattern 'libfoo-*'
      - regex prefix 're:^lib.*-dev$'
    """
    if not pattern:
        return False
    pattern = str(pattern)
    if pattern.startswith("re:"):
        try:
            rx = re.compile(pattern[3:])
            return bool(rx.match(name))
        except re.error:
            return False
    # glob matching (case-sensitive)
    if "*" in pattern or "?" in pattern or "[" in pattern:
        return fnmatch.fnmatch(name, pattern)
    # exact match
    return name == pattern

# ---------------------------------------------------------------------
# Masks model (in-memory representation) and DB schema management
# ---------------------------------------------------------------------

class MaskRecord:
    """
    Representa uma máscara persistida ou carregada de config.
    Campos principais:
      id: int | None (None se não persistido)
      name_pattern: str (nome exato, glob ou 're:...')
      version_rule: str (ex: '<2.0', '>=1.2', '*')
      arch_rule: Optional[str] (ex: 'x86_64' or '!armv7' or glob)
      keyword: Optional[str] (stable/testing/masked)
      reason: Optional[str]
      added_by: Optional[str]
      valid_until: Optional[int] timestamp
      profile: Optional[str] (aplica apenas para determinado perfil)
      created_at: int
    """
    def __init__(self,
                 id: Optional[int],
                 name_pattern: str,
                 version_rule: str = "*",
                 arch_rule: Optional[str] = None,
                 keyword: Optional[str] = None,
                 reason: Optional[str] = None,
                 added_by: Optional[str] = None,
                 valid_until: Optional[int] = None,
                 profile: Optional[str] = None,
                 created_at: Optional[int] = None):
        self.id = id
        self.name_pattern = name_pattern
        self.version_rule = version_rule or "*"
        self.arch_rule = arch_rule
        self.keyword = keyword
        self.reason = reason
        self.added_by = added_by
        self.valid_until = valid_until
        self.profile = profile
        self.created_at = created_at or _now_ts()

    def is_expired(self) -> bool:
        return self.valid_until is not None and _now_ts() > int(self.valid_until)

    def matches(self, package_name: str, version: Optional[str] = None, arch: Optional[str] = None, profile: Optional[str] = None) -> bool:
        # profile: if mask has profile and it's different, then mask doesn't match
        if self.profile and profile and (self.profile != profile):
            return False
        if self.profile and not profile:
            # mask requires profile but none provided: don't match
            return False
        if self.is_expired():
            return False
        if not _match_name_pattern(package_name, self.name_pattern):
            return False
        if not _match_version_rule(version, self.version_rule):
            return False
        if self.arch_rule:
            ar = str(self.arch_rule)
            # support negation with leading '!' e.g. '!armv7'
            if ar.startswith("!"):
                if arch and _match_name_pattern(arch, ar[1:]):
                    return False
            else:
                if arch and not _match_name_pattern(arch, ar):
                    return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name_pattern": self.name_pattern,
            "version_rule": self.version_rule,
            "arch_rule": self.arch_rule,
            "keyword": self.keyword,
            "reason": self.reason,
            "added_by": self.added_by,
            "valid_until": self.valid_until,
            "profile": self.profile,
            "created_at": self.created_at,
        }

# ---------------------------------------------------------------------
# Manager class
# ---------------------------------------------------------------------

class MasksManager:
    """
    Manager central: mantém cache em memória e operações persistidas no DB.
    """

    def __init__(self):
        self._db = None
        try:
            if callable(get_db):
                self._db = get_db()
            else:
                self._db = None
        except Exception:
            self._db = None
        # cache of MaskRecord objects loaded from DB + config
        self._masks: List[MaskRecord] = []
        self._cache_loaded = False
        # load config defaults
        self._load_initial_from_config()

    # -----------------------------
    # DB table management
    # -----------------------------
    def _ensure_table(self):
        if not self._db:
            return
        # create table masks if missing
        try:
            self._db.execute("""
                CREATE TABLE IF NOT EXISTS masks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name_pattern TEXT NOT NULL,
                    version_rule TEXT DEFAULT '*',
                    arch_rule TEXT,
                    keyword TEXT,
                    reason TEXT,
                    added_by TEXT,
                    valid_until INTEGER,
                    profile TEXT,
                    created_at INTEGER DEFAULT (strftime('%s','now'))
                );
            """, (), commit=True)
            self._db.execute("CREATE INDEX IF NOT EXISTS idx_masks_name ON masks(name_pattern);", (), commit=True)
            self._db.execute("CREATE INDEX IF NOT EXISTS idx_masks_profile ON masks(profile);", (), commit=True)
        except Exception:
            logger.exception("Failed to ensure masks table in DB")

    # -----------------------------
    # load initial masks from config.yaml (if present)
    # -----------------------------
    def _load_initial_from_config(self):
        cfg = {}
        try:
            cfg = get_config() if callable(get_config) else {}
        except Exception:
            cfg = {}
        masks_cfg = {}
        # try to extract masks section safely (support pydantic/dataclass or dict)
        try:
            if hasattr(cfg, "logging") or hasattr(cfg, "db"):
                # pydantic/dataclass style: as_dict() if available
                try:
                    raw = cfg.as_dict() if hasattr(cfg, "as_dict") else (cfg.dict() if hasattr(cfg, "dict") else None)
                    if isinstance(raw, dict):
                        masks_cfg = raw.get("masks", {}) or {}
                except Exception:
                    # fallback attribute access
                    masks_cfg = getattr(cfg, "masks", {}) or {}
            elif isinstance(cfg, dict):
                masks_cfg = cfg.get("masks", {}) or {}
        except Exception:
            masks_cfg = {}

        # load package masks from config
        pkglist = masks_cfg.get("packages", []) or []
        for p in pkglist:
            # each p expected to be dict with name, version, reason, optional profile/arch
            try:
                name = p.get("name")
                vr = p.get("version", "*")
                ar = p.get("arch")
                reason = p.get("reason")
                profile = p.get("profile")
                mr = MaskRecord(id=None, name_pattern=name, version_rule=vr, arch_rule=ar, reason=reason, profile=profile, added_by="config", created_at=_now_ts())
                self._masks.append(mr)
            except Exception:
                continue

        # arch rules & keywords may be global, convert to masks entries if needed
        arch_cfg = masks_cfg.get("arch", {}) or {}
        deny_list = arch_cfg.get("deny", []) or []
        for arch in deny_list:
            # mask any package for that arch via a special pattern '*' with arch_rule
            mr = MaskRecord(id=None, name_pattern="*", version_rule="*", arch_rule=arch, reason="arch denied (config)", added_by="config", created_at=_now_ts())
            self._masks.append(mr)

        # keywords may be present but are more informational; we don't convert them to masks automatically here.

        # set cache loaded flag: but still attempt to load persisted masks from DB lazily
        self._cache_loaded = False

    def _load_from_db(self):
        # load persisted masks
        if self._cache_loaded:
            return
        # ensure table present
        if self._db:
            self._ensure_table()
            try:
                rows = self._db.fetchall("SELECT * FROM masks")
                for r in rows:
                    try:
                        mr = MaskRecord(
                            id=int(r["id"]),
                            name_pattern=r["name_pattern"],
                            version_rule=r["version_rule"] or "*",
                            arch_rule=r["arch_rule"],
                            keyword=r["keyword"],
                            reason=r["reason"],
                            added_by=r["added_by"],
                            valid_until=int(r["valid_until"]) if r["valid_until"] else None,
                            profile=r["profile"],
                            created_at=int(r["created_at"]) if r["created_at"] else None,
                        )
                        # avoid duplicates from config and DB by checking id or pattern
                        self._masks.append(mr)
                    except Exception:
                        continue
            except Exception:
                logger.exception("Failed to load masks from DB")
        self._cache_loaded = True

    def reload(self):
        # clear in-memory masks and reload from config + DB
        self._masks = []
        self._cache_loaded = False
        self._load_initial_from_config()
        self._load_from_db()

    # -----------------------------
    # CRUD operations (persisted)
    # -----------------------------
    def add_mask(self,
                 name_pattern: str,
                 version_rule: str = "*",
                 arch_rule: Optional[str] = None,
                 keyword: Optional[str] = None,
                 reason: Optional[str] = None,
                 added_by: Optional[str] = "manual",
                 valid_until: Optional[int] = None,
                 profile: Optional[str] = None) -> MaskRecord:
        """
        Adiciona máscara persistente em DB e cache.
        Retorna MaskRecord criado (com id se persistido).
        """
        self._load_from_db()
        # sanitize inputs
        npat = str(name_pattern)
        vr = version_rule or "*"
        try:
            if self._db:
                # ensure table
                self._ensure_table()
                cur = self._db.execute(
                    "INSERT INTO masks (name_pattern, version_rule, arch_rule, keyword, reason, added_by, valid_until, profile, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, strftime('%s','now'))",
                    (npat, vr, arch_rule, keyword, reason, added_by, valid_until, profile),
                    commit=True
                )
                mid = cur.lastrowid
                mr = MaskRecord(id=mid, name_pattern=npat, version_rule=vr, arch_rule=arch_rule, keyword=keyword, reason=reason, added_by=added_by, valid_until=valid_until, profile=profile, created_at=_now_ts())
                self._masks.append(mr)
                logger.info(f"Mask added: id={mid} pattern={npat} version={vr} arch={arch_rule} profile={profile}")
                # persist history & event
                try:
                    add_history(npat, "mask_add", f"id={mid} reason={reason}")
                    emit_event("mask_added", {"id": mid, "pattern": npat, "version": vr, "profile": profile, "by": added_by})
                except Exception:
                    pass
                return mr
            else:
                # if no DB, just add in-memory
                mr = MaskRecord(id=None, name_pattern=npat, version_rule=vr, arch_rule=arch_rule, keyword=keyword, reason=reason, added_by=added_by, valid_until=valid_until, profile=profile, created_at=_now_ts())
                self._masks.append(mr)
                logger.info(f"(mem) Mask added: pattern={npat} version={vr} arch={arch_rule} profile={profile}")
                return mr
        except Exception:
            logger.exception("Failed to add mask")
            raise

    def remove_mask(self, mask_id: int) -> bool:
        self._load_from_db()
        try:
            if self._db and mask_id is not None:
                with self._db.transaction():
                    self._db.execute("DELETE FROM masks WHERE id = ?", (mask_id,), commit=False)
                # remove from cache
                self._masks = [m for m in self._masks if not (m.id == mask_id)]
                logger.info(f"Mask removed: id={mask_id}")
                try:
                    add_history("", "mask_remove", f"id={mask_id}")
                    emit_event("mask_removed", {"id": mask_id})
                except Exception:
                    pass
                return True
            else:
                # in-memory removal by ID (None IDs can't be removed)
                removed = False
                newlist = []
                for m in self._masks:
                    if m.id == mask_id:
                        removed = True
                        continue
                    newlist.append(m)
                self._masks = newlist
                if removed:
                    logger.info(f"(mem) Mask removed: id={mask_id}")
                return removed
        except Exception:
            logger.exception("Failed to remove mask")
            return False

    def list_masks(self, active_only: bool = True, profile: Optional[str] = None) -> List[Dict[str, Any]]:
        self._load_from_db()
        out = []
        for m in self._masks:
            if active_only and m.is_expired():
                continue
            if profile and m.profile and (m.profile != profile):
                continue
            out.append(m.to_dict())
        return out

    # -----------------------------
    # Matching / enforcement API
    # -----------------------------
    def _find_matching_masks(self, package_name: str, version: Optional[str] = None, arch: Optional[str] = None, profile: Optional[str] = None) -> List[MaskRecord]:
        self._load_from_db()
        matches = []
        for m in self._masks:
            if m.matches(package_name, version, arch, profile):
                matches.append(m)
        return matches

    def is_masked(self, package_name: str, version: Optional[str] = None, arch: Optional[str] = None, profile: Optional[str] = None) -> bool:
        matches = self._find_matching_masks(package_name, version, arch, profile)
        return len(matches) > 0

    def get_mask_reason(self, package_name: str, version: Optional[str] = None, arch: Optional[str] = None, profile: Optional[str] = None) -> Optional[str]:
        """
        Retorna a razão da primeira máscara correspondente (prioridade de persistidos + config).
        """
        matches = self._find_matching_masks(package_name, version, arch, profile)
        if not matches:
            return None
        # prefer mask with explicit reason and non-expired (already filtered by matches)
        for m in matches:
            if m.reason:
                return m.reason
        return matches[0].reason if matches else None

    def enforce_masks(self, pkg_list: List[Dict[str, Any]], profile: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Recebe lista de pacotes (cada dict com keys: name, version?, arch?) e retorna apenas os permitidos.
        Também registra eventos e logs para tentativas bloqueadas.
        """
        allowed = []
        for item in pkg_list:
            name = item.get("name")
            ver = item.get("version")
            arch = item.get("arch")
            if self.is_masked(name, ver, arch, profile=profile):
                reason = self.get_mask_reason(name, ver, arch, profile=profile)
                logger.warning(f"Package masked: {name} {ver or ''} reason={reason}")
                try:
                    emit_event("mask_blocked_attempt", {"package": name, "version": ver, "arch": arch, "reason": reason, "profile": profile})
                    add_history(name, "mask_blocked", f"attempt blocked reason={reason}")
                except Exception:
                    pass
                # don't include in allowed
            else:
                allowed.append(item)
        return allowed

    # -----------------------------
    # Import / Export mask rules
    # -----------------------------
    def export_masks(self, path: str):
        self._load_from_db()
        data = [m.to_dict() for m in self._masks]
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Masks exported to {path}")
        except Exception:
            logger.exception("Failed to export masks")
            raise

    def import_masks(self, path: str, add_if_exists: bool = True):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # expect list of mask dicts
            for entry in data:
                try:
                    # attempt to add each mask (persist)
                    self.add_mask(
                        name_pattern=entry.get("name_pattern") or entry.get("name"),
                        version_rule=entry.get("version_rule", "*"),
                        arch_rule=entry.get("arch_rule"),
                        keyword=entry.get("keyword"),
                        reason=entry.get("reason"),
                        added_by=entry.get("added_by", "import"),
                        valid_until=entry.get("valid_until"),
                        profile=entry.get("profile")
                    )
                except Exception:
                    logger.exception("Failed to import mask entry")
            logger.info(f"Imported masks from {path}")
        except Exception:
            logger.exception("Failed to import masks")
            raise

# ---------------------------------------------------------------------
# Module-level manager singleton and convenience functions
# ---------------------------------------------------------------------

_manager: Optional[MasksManager] = None

def _get_manager() -> MasksManager:
    global _manager
    if _manager is None:
        _manager = MasksManager()
    return _manager

def is_masked(name: str, version: Optional[str] = None, arch: Optional[str] = None, profile: Optional[str] = None) -> bool:
    return _get_manager().is_masked(name, version=version, arch=arch, profile=profile)

def get_mask_reason(name: str, version: Optional[str] = None, arch: Optional[str] = None, profile: Optional[str] = None) -> Optional[str]:
    return _get_manager().get_mask_reason(name, version=version, arch=arch, profile=profile)

def enforce_masks(pkg_list: List[Dict[str,Any]], profile: Optional[str] = None) -> List[Dict[str,Any]]:
    return _get_manager().enforce_masks(pkg_list, profile=profile)

def add_mask(name_pattern: str, version_rule: str = "*", arch_rule: Optional[str] = None,
             keyword: Optional[str] = None, reason: Optional[str] = None,
             added_by: Optional[str] = "manual", valid_until: Optional[int] = None,
             profile: Optional[str] = None) -> MaskRecord:
    return _get_manager().add_mask(name_pattern=name_pattern, version_rule=version_rule, arch_rule=arch_rule, keyword=keyword, reason=reason, added_by=added_by, valid_until=valid_until, profile=profile)

def remove_mask(mask_id: int) -> bool:
    return _get_manager().remove_mask(mask_id)

def list_masks(active_only: bool = True, profile: Optional[str] = None) -> List[Dict[str,Any]]:
    return _get_manager().list_masks(active_only=active_only, profile=profile)

def export_masks(path: str):
    return _get_manager().export_masks(path)

def import_masks(path: str):
    return _get_manager().import_masks(path)

def reload():
    return _get_manager().reload()

# ---------------------------------------------------------------------
# Demo / quick tests when run directly
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Demo: adicionar máscaras, listar, testar
    logger.info("Running masks demo")
    reload()
    m1 = add_mask("glibc", "<2.35", reason="Incompatível com toolchain", added_by="admin", profile=None)
    m2 = add_mask("systemd", "*", reason="Bloqueado pela política", added_by="admin", profile="server")
    print("List all masks:", list_masks(active_only=False))
    print("List active masks:", list_masks())
    print("Is glibc masked? ->", is_masked("glibc", "2.34"))
    print("Is systemd masked for server? ->", is_masked("systemd", "1.0", profile="server"))
    print("Is systemd masked for desktop? ->", is_masked("systemd", "1.0", profile="desktop"))
    # enforce example
    pkgs = [{"name":"glibc","version":"2.34"},{"name":"vim","version":"8.2"},{"name":"systemd","version":"245"}]
    allowed = enforce_masks(pkgs, profile="server")
    print("Allowed after enforce (server):", allowed)
