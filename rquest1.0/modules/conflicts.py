# rquest1.0/modules/conflicts.py
# -*- coding: utf-8 -*-
"""
Rquest conflicts detector/resolver

Agora com:
 - Modo interativo (policy.interactive)
 - Auto-rename (policy.auto_rename)
 - Integração com upgrade.apply_plan (via resolve_plan_conflicts)
"""

from __future__ import annotations
import os
import json
import logging
import sys
import shutil
from typing import Any, Dict, List, Optional

# graceful imports
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config(): return {}

try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def emit_event(*a, **k): pass

try:
    from modules.meta import MetaLoader  # type: ignore
except Exception:
    MetaLoader = None

try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    get_resolver = None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    get_hook_manager = None

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("conflicts")
except Exception:
    logger = logging.getLogger("rquest.conflicts")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# config
CFG = get_config() if callable(get_config) else {}
CONFLICTS_CFG = CFG.get("conflicts", {}) if isinstance(CFG, dict) else {}

POLICY_OVERWRITE = bool(CONFLICTS_CFG.get("overwrite_on_conflict", False))
POLICY_INTERACTIVE = bool(CONFLICTS_CFG.get("interactive", False))
POLICY_ALLOW_DEP_CONFLICT = bool(CONFLICTS_CFG.get("allow_dep_conflict", False))
POLICY_WHITELIST_PATHS = CONFLICTS_CFG.get("whitelist_paths", []) or []
POLICY_LOG_ONLY = bool(CONFLICTS_CFG.get("log_only", False))
POLICY_AUTO_RENAME = bool(CONFLICTS_CFG.get("auto_rename", False))

_CONFLICTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS conflicts_log (
    id TEXT PRIMARY KEY,
    detected_at INTEGER,
    meta_path TEXT,
    conflict_type TEXT,
    detail TEXT
);
"""

class ConflictError(Exception):
    pass

class ConflictDetector:
    def __init__(self):
        # DB
        dbc = get_db() if callable(get_db) else None
        self.db = None
        if dbc:
            try:
                self.db = dbc()
                self.db.execute(_CONFLICTS_TABLE_SQL, (), commit=True)
            except Exception:
                self.db = None
        # meta loader
        self.meta_loader = MetaLoader() if MetaLoader else None
        # resolver
        rc = get_resolver() if callable(get_resolver) else None
        self.resolver = rc() if rc else None
        # hooks
        hm = get_hook_manager() if callable(get_hook_manager) else None
        self.hooks = hm() if hm else None

    def _installed_files_map(self):
        mapping = {}
        if self.db:
            try:
                rows = self.db.fetchall("SELECT package, version, path, sha256 FROM installed_files")
                for r in rows:
                    mapping[os.path.normpath(r["path"])] = {
                        "package": r["package"],
                        "version": r["version"],
                        "sha256": r["sha256"],
                    }
                return mapping
            except Exception:
                pass
        return mapping

    def detect_file_conflicts(self, meta_path: str) -> List[Dict[str, Any]]:
        conflicts = []
        if not self.meta_loader:
            return conflicts
        try:
            mp = self.meta_loader.load(meta_path)
        except Exception:
            return conflicts
        files = mp.raw.get("files", [])
        installed_map = self._installed_files_map()
        for f in files:
            path = f.get("path") if isinstance(f, dict) else f
            abs_path = os.path.normpath(os.path.join("/", str(path).lstrip("/")))
            if any(abs_path.startswith(w) for w in POLICY_WHITELIST_PATHS):
                continue
            if abs_path in installed_map:
                rec = installed_map[abs_path]
                if rec["package"] != mp.name:
                    conflicts.append({
                        "type": "file_conflict",
                        "path": abs_path,
                        "existing_pkg": rec["package"],
                        "new_pkg": mp.name,
                    })
        return conflicts

    def detect_dep_conflicts(self, pkg_names: List[str]) -> List[Dict[str, Any]]:
        if not self.resolver: return []
        try:
            if hasattr(self.resolver, "resolve_conflicts"):
                return self.resolver.resolve_conflicts(pkg_names) or []
        except Exception:
            return []
        return []

    def _interactive_choice(self, conflict: Dict[str, Any]) -> str:
        print(f"\nConflito detectado: {conflict}")
        print("Escolha uma ação:")
        print("[o] Sobrescrever")
        print("[s] Saltar instalação do arquivo")
        print("[r] Renomear automaticamente")
        print("[a] Abortar")
        choice = input("Opção: ").strip().lower()
        return choice or "a"

    def _apply_auto_rename(self, conflict: Dict[str, Any], plan_item):
        old_path = conflict["path"]
        new_path = old_path + ".rquest"
        logger.warning("Auto-rename %s -> %s", old_path, new_path)
        # atualiza plano se tiver lista de arquivos
        if hasattr(plan_item, "files"):
            for f in plan_item.files:
                if f.get("path") == old_path:
                    f["path"] = new_path
        return new_path

    def resolve_plan_conflicts(self, plan) -> bool:
        all_conflicts = []
        for item in getattr(plan, "items", []):
            fc = self.detect_file_conflicts(item.meta_path)
            for c in fc:
                if POLICY_LOG_ONLY:
                    logger.info("Conflito (log-only): %s", c)
                    continue
                if POLICY_OVERWRITE:
                    logger.warning("Sobrescrevendo %s", c)
                    continue
                if POLICY_AUTO_RENAME:
                    self._apply_auto_rename(c, item)
                    continue
                if POLICY_INTERACTIVE:
                    choice = self._interactive_choice(c)
                    if choice == "o":
                        continue
                    elif choice == "s":
                        plan.items.remove(item)
                        break
                    elif choice == "r":
                        self._apply_auto_rename(c, item)
                        continue
                    else:
                        raise ConflictError(f"Abortado pelo usuário: {c}")
                raise ConflictError(f"Conflito fatal: {c}")
            all_conflicts.extend(fc)

        deps = self.detect_dep_conflicts([i.package for i in getattr(plan, "items", [])])
        if deps and not POLICY_ALLOW_DEP_CONFLICT:
            raise ConflictError(f"Conflitos de dependência: {deps}")

        emit_event("conflicts.resolved", {"files": len(all_conflicts), "deps": len(deps)})
        return True

# conveniência
def detect(meta_path: str):
    return ConflictDetector().detect_file_conflicts(meta_path)

def resolve_plan_conflicts(plan):
    return ConflictDetector().resolve_plan_conflicts(plan)

# integração com upgrade.apply_plan
try:
    import modules.upgrade as upgrade
    _orig_apply = upgrade.apply_plan
    def wrapped_apply(plan, *a, **k):
        cd = ConflictDetector()
        cd.resolve_plan_conflicts(plan)
        return _orig_apply(plan, *a, **k)
    upgrade.apply_plan = wrapped_apply
    logger.info("Integração: conflitos verificados antes de upgrade.apply_plan")
except Exception:
    logger.debug("Upgrade não disponível para integração")
