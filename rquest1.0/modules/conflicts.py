# rquest1.0/modules/conflicts.py
# -*- coding: utf-8 -*-
"""
Rquest - conflicts.py (resolução ativa)

Funcionalidades:
 - detecta conflitos de arquivos e dependências a partir de .meta e do banco
 - resolve automaticamente segundo políticas (backup, remove dono, prefer_new/prefer_installed)
 - auto-rename: altera o plano para renomear arquivos conflitantes
 - modo interativo (terminal) para decisões quando configurado
 - persiste eventos em tabela conflicts_log (se DB disponível)
 - integra com modules.remove, modules.resolver, modules.db, modules.meta, modules.hooks, modules.logging

Uso:
  from modules.conflicts import ConflictResolver, detect, resolve_plan_conflicts
  cr = ConflictResolver()
  cr.resolve_plan_conflicts(plan)   # plan: objeto UpgradePlan com .items -> UpgradePlanItem(meta_path, package, ...)
"""

from __future__ import annotations
import os
import sys
import json
import time
import shutil
import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------- integrações (graceful) ----------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    LOG = get_logger("conflicts")
except Exception:
    LOG = logging.getLogger("rquest.conflicts")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)

# DB
try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db():
        return None
    def emit_event(*a, **k):
        pass

# Meta loader
try:
    from modules.meta import MetaLoader  # type: ignore
except Exception:
    MetaLoader = None

# Resolver
try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    get_resolver = None

# Remove API
try:
    import modules.remove as remove_mod  # type: ignore
except Exception:
    remove_mod = None

# Hooks
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    get_hook_manager = None

# Pkgtool (fallback installer/uninstaller)
try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    get_pkgtool = None

# ---------- configuração e defaults ----------
CFG = get_config() if callable(get_config) else {}
CONFLICTS_CFG = CFG.get("conflicts", {}) if isinstance(CFG, dict) else {}

# políticas configuráveis
POLICY = {
    "auto_remove_conflicting": bool(CONFLICTS_CFG.get("auto_remove_conflicting", True)),
    "auto_backup_conflicting": bool(CONFLICTS_CFG.get("auto_backup_conflicting", True)),
    "prefer_new": bool(CONFLICTS_CFG.get("prefer_new", False)),
    "prefer_installed": bool(CONFLICTS_CFG.get("prefer_installed", False)),
    "allow_dep_conflicts": bool(CONFLICTS_CFG.get("allow_dep_conflicts", False)),
    "interactive": bool(CONFLICTS_CFG.get("interactive", False)),
    "auto_rename": bool(CONFLICTS_CFG.get("auto_rename", False)),
    "whitelist_paths": CONFLICTS_CFG.get("whitelist_paths", []) or [],
    "log_only": bool(CONFLICTS_CFG.get("log_only", False)),
    "backup_dir": str(CONFLICTS_CFG.get("backup_dir", "/var/lib/rquest/conflict_backups")),
    "dry_run": bool(CONFLICTS_CFG.get("dry_run", False)),
}

BACKUP_DIR = Path(POLICY["backup_dir"])
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

# DB conflicts table
_CONFLICTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS conflicts_log (
    id TEXT PRIMARY KEY,
    detected_at INTEGER,
    meta_path TEXT,
    conflict_type TEXT,
    detail TEXT
);
"""

# ---------- utilitários ----------
def _now_ts() -> int:
    return int(time.time())

def _shortid(obj: Any) -> str:
    try:
        j = json.dumps(obj, sort_keys=True)
        return hashlib.sha1(j.encode("utf-8")).hexdigest()[:12]
    except Exception:
        return str(int(time.time()))

def _norm_path(p: str) -> str:
    return os.path.normpath(os.path.join("/", str(p).lstrip("/")))

def _is_whitelisted(path: str) -> bool:
    for w in POLICY["whitelist_paths"]:
        if path.startswith(os.path.normpath(w)):
            return True
    return False

def _backup_path_of(path: str) -> str:
    # e.g. /var/lib/rquest/conflict_backups/20250924-120000/usr_bin_foo
    ts = time.strftime("%Y%m%d-%H%M%S")
    rel = path.lstrip("/").replace("/", "_")
    bp = BACKUP_DIR / f"{ts}" / rel
    bp.parent.mkdir(parents=True, exist_ok=True)
    return str(bp)

# ---------- classes / lógica principal ----------
class ConflictError(Exception):
    """Erro levantado quando um conflito não puder ser resolvido automaticamente"""
    pass

class ConflictResolver:
    """
    Classe principal: detecta e resolve conflitos para um plano.
    """

    def __init__(self):
        # DB
        self._db = None
        dbc = get_db() if callable(get_db) else None
        if dbc:
            try:
                self._db = dbc()
                # ensure conflicts table
                try:
                    self._db.execute(_CONFLICTS_TABLE_SQL, (), commit=True)
                except Exception:
                    pass
            except Exception:
                LOG.exception("Falha ao abrir DB para conflicts")
                self._db = None

        # meta loader
        self.meta = None
        if MetaLoader:
            try:
                self.meta = MetaLoader()
            except Exception:
                LOG.debug("MetaLoader init failed")

        # resolver
        self.resolver = None
        rc = get_resolver() if callable(get_resolver) else None
        if rc:
            try:
                self.resolver = rc()
            except Exception:
                LOG.debug("Resolver init failed")

        # remove API
        self.remove_api = None
        if remove_mod:
            # remove_mod may expose function or manager
            if hasattr(remove_mod, "remove_package"):
                self.remove_api = remove_mod.remove_package
            elif hasattr(remove_mod, "RemoveManager") or hasattr(remove_mod, "get_remove_manager"):
                try:
                    if hasattr(remove_mod, "get_remove_manager"):
                        rm = remove_mod.get_remove_manager()
                    else:
                        rm = remove_mod.RemoveManager()
                    if hasattr(rm, "remove_package"):
                        self.remove_api = rm.remove_package
                except Exception:
                    LOG.debug("remove manager init failed")

        # hooks manager
        self.hooks = None
        if get_hook_manager:
            try:
                self.hooks = get_hook_manager()()
            except Exception:
                LOG.debug("hooks manager not ready")

        # pkgtool fallback for reinstallation/uninstall
        self.pkgtool = None
        if get_pkgtool:
            try:
                self.pkgtool = get_pkgtool()()
            except Exception:
                LOG.debug("pkgtool not ready")

    # ---------- carregadores do sistema ----------
    def _installed_files_map(self) -> Dict[str, Dict[str, Any]]:
        """
        Retorna mapa path -> { package, version, sha256, mode, uid, gid }.
        Usa DB.installed_files se disponível; senão tenta manifests em ~/.rquest/installed/
        """
        mapping: Dict[str, Dict[str, Any]] = {}
        if self._db:
            try:
                rows = self._db.fetchall("SELECT package, version, path, sha256, mode, uid, gid FROM installed_files")
                for r in rows:
                    p = _norm_path(r.get("path", ""))
                    mapping[p] = {
                        "package": r.get("package"),
                        "version": r.get("version"),
                        "sha256": r.get("sha256"),
                        "mode": r.get("mode"),
                        "uid": r.get("uid"),
                        "gid": r.get("gid"),
                    }
                return mapping
            except Exception:
                LOG.debug("Falha ao buscar installed_files do DB, fallback para manifests")

        # fallback manifests
        home = Path.home() / ".rquest" / "installed"
        if home.exists():
            for manifest in home.glob("*/*/MANIFEST.json") if home.exists() else []:
                try:
                    j = json.loads(manifest.read_text(encoding="utf-8"))
                    pkg = j.get("name")
                    ver = j.get("version")
                    for f in j.get("files", []):
                        ap = _norm_path(str(f.get("path") or ""))
                        mapping[ap] = {
                            "package": pkg,
                            "version": ver,
                            "sha256": f.get("sha256"),
                            "mode": f.get("mode"),
                            "uid": f.get("uid"),
                            "gid": f.get("gid"),
                        }
                except Exception:
                    LOG.debug("Erro lendo manifest %s", manifest)
        return mapping

    def _installed_packages_map(self) -> Dict[str, Dict[str, Any]]:
        mapping: Dict[str, Dict[str, Any]] = {}
        if self._db:
            try:
                rows = self._db.fetchall("SELECT name, version, meta_path FROM installed_packages")
                for r in rows:
                    mapping[r.get("name")] = {"version": r.get("version"), "meta_path": r.get("meta_path")}
                return mapping
            except Exception:
                LOG.debug("installed_packages not available")
        return mapping

    # ---------- detecção ----------
    def detect_file_conflicts_for_meta(self, meta_path: str) -> List[Dict[str, Any]]:
        """
        Lê .meta e detecta conflitos de arquivos existentes no sistema.
        Retorna lista de dicts com keys: type, path, existing_pkg, existing_sha, new_pkg, new_sha, meta_path
        """
        conflicts: List[Dict[str, Any]] = []
        if not self.meta:
            LOG.warning("MetaLoader indisponível; não é possível detectar conflitos de arquivo para %s", meta_path)
            return conflicts
        try:
            mp = self.meta.load(meta_path)
        except Exception as e:
            LOG.exception("Falha ao carregar meta %s: %s", meta_path, e)
            return conflicts

        # Tentamos obter lista de arquivos previstos pelo .meta
        files = []
        # mp.raw pode variar; tentamos suportar várias formas
        raw = getattr(mp, "raw", None) or getattr(mp, "data", None) or {}
        if isinstance(raw, dict):
            files = raw.get("files") or raw.get("install_files") or raw.get("manifest_files") or []
        if not files and hasattr(mp, "files"):
            files = getattr(mp, "files", [])

        if not files:
            # se não há arquivos declarados, não podemos prever conflitos
            LOG.debug("Meta %s não declara arquivos explicitamente", meta_path)
            return conflicts

        installed_map = self._installed_files_map()

        for entry in files:
            if isinstance(entry, str):
                rel = entry
                new_sha = None
            elif isinstance(entry, dict):
                rel = entry.get("path") or entry.get("dest") or entry.get("file")
                new_sha = entry.get("sha256") or entry.get("sha1") or entry.get("md5")
            else:
                continue
            if not rel:
                continue
            abs_path = _norm_path(rel)
            if _is_whitelisted(abs_path):
                LOG.debug("Ignorando caminho whitelisted %s", abs_path)
                continue
            existing = installed_map.get(abs_path)
            if existing:
                existing_pkg = existing.get("package")
                existing_sha = existing.get("sha256")
                if existing_pkg == getattr(mp, "name", None):
                    # mesmo pacote (upgrade) -> notar como upgrade_overwrite
                    if existing_sha and new_sha and existing_sha != new_sha:
                        conflicts.append({
                            "type": "upgrade_overwrites",
                            "path": abs_path,
                            "existing_pkg": existing_pkg,
                            "existing_sha": existing_sha,
                            "new_pkg": getattr(mp, "name", None),
                            "new_sha": new_sha,
                            "meta_path": meta_path,
                        })
                else:
                    # pacote diferente -> potencial conflito
                    if existing_sha and new_sha and existing_sha == new_sha:
                        conflicts.append({
                            "type": "file_same_content",
                            "path": abs_path,
                            "existing_pkg": existing_pkg,
                            "existing_sha": existing_sha,
                            "new_pkg": getattr(mp, "name", None),
                            "new_sha": new_sha,
                            "meta_path": meta_path,
                        })
                    else:
                        conflicts.append({
                            "type": "file_conflict",
                            "path": abs_path,
                            "existing_pkg": existing_pkg,
                            "existing_sha": existing_sha,
                            "new_pkg": getattr(mp, "name", None),
                            "new_sha": new_sha,
                            "meta_path": meta_path,
                        })
        # persistir ocorrências
        if conflicts and self._db:
            try:
                for c in conflicts:
                    cid = f"conf-{_shortid(c)}"
                    self._db.execute("INSERT OR REPLACE INTO conflicts_log (id, detected_at, meta_path, conflict_type, detail) VALUES (?,?,?,?,?)",
                                     (cid, _now_ts(), meta_path, c.get("type"), json.dumps(c)), commit=True)
            except Exception:
                LOG.debug("Falha ao persistir conflicts_log")
        return conflicts

    def detect_dep_conflicts(self, pkgs: List[str]) -> List[Dict[str, Any]]:
        """
        Usa resolver para detectar conflitos de versão/dependência.
        """
        if not self.resolver:
            LOG.debug("Resolver não disponível; pulando detecção de conflitos de dependência")
            return []
        try:
            if hasattr(self.resolver, "resolve_conflicts"):
                res = self.resolver.resolve_conflicts(pkgs)
            elif hasattr(self.resolver, "analyze_plan"):
                res = self.resolver.analyze_plan(pkgs)
            else:
                LOG.debug("Resolver presente mas não tem API conhecida")
                return []
            # normalizar
            if isinstance(res, dict) and "conflicts" in res:
                return res.get("conflicts", [])
            if isinstance(res, list):
                return res
            return [{"type": "resolver_unknown", "detail": res}]
        except Exception:
            LOG.exception("Erro ao consultar resolver")
            return []

    # ---------- resolução de um conflito de arquivo ----------
    def _try_remove_owner_pkg(self, owner_pkg: str) -> Tuple[bool, str]:
        """
        Tenta remover o pacote dono do arquivo conflituoso.
        Retorna (ok, message).
        Usa modules.remove ou pkgtool como fallback.
        """
        if POLICY["dry_run"]:
            return (True, f"[dry-run] would remove {owner_pkg}")
        if self.remove_api:
            try:
                res = self.remove_api(owner_pkg, force=True)
                return (True, f"removed via remove_api: {res}")
            except Exception:
                LOG.exception("remove_api falhou para %s", owner_pkg)
        # fallback: pkgtool uninstall if available
        if self.pkgtool and hasattr(self.pkgtool, "uninstall"):
            try:
                res = self.pkgtool.uninstall(owner_pkg)
                return (True, f"removed via pkgtool.uninstall: {res}")
            except Exception:
                LOG.exception("pkgtool.uninstall falhou")
        # não temos meios automáticos
        return (False, "no remove API available")

    def _backup_existing_file(self, path: str) -> Tuple[bool, str]:
        try:
            bp = _backup_path_of(path)
            if POLICY["dry_run"]:
                return (True, f"[dry-run] would backup {path} -> {bp}")
            shutil.copy2(path, bp)
            LOG.info("Backup de %s em %s", path, bp)
            return (True, bp)
        except Exception as e:
            LOG.exception("Backup falhou para %s", path)
            return (False, str(e))

    def _apply_auto_rename_to_plan_item(self, plan_item: Any, old_path: str, new_path: str) -> bool:
        """
        Aplica rename ao plan item: modifica lista de arquivos no meta embutido/obj do item se existir.
        PlanItem pode ter atributos .meta_path, .files, ou podemos carregar meta e escrever um override.
        Retorna True se conseguiu modificar plan_item.
        """
        # se plan_item tiver .files (lista dicts), altere
        try:
            if hasattr(plan_item, "files") and isinstance(getattr(plan_item, "files"), list):
                for f in plan_item.files:
                    # várias formas de representar path
                    if f.get("path") == old_path or f.get("dest") == old_path or f.get("file") == old_path:
                        f["path"] = new_path
                LOG.info("Plan item modificado (rename) em memória para %s", new_path)
                return True
        except Exception:
            LOG.debug("Falha ao aplicar rename em plan_item.files")

        # fallback: se plan_item tem meta_path, gerar um 'override' temporário em memória:
        try:
            if hasattr(plan_item, "meta_path") and plan_item.meta_path and self.meta:
                mp = self.meta.load(plan_item.meta_path)
                raw = getattr(mp, "raw", None) or {}
                files = raw.get("files")
                if isinstance(files, list):
                    changed = False
                    for f in files:
                        if isinstance(f, dict) and (f.get("path") == old_path or f.get("dest") == old_path):
                            f["path"] = new_path
                            changed = True
                    if changed:
                        # we cannot write back meta file; but we attach override files list to plan_item
                        plan_item._conflicts_files_override = files
                        LOG.info("Applied in-memory rename override to plan_item for %s", plan_item.meta_path)
                        return True
        except Exception:
            LOG.debug("Falha fallback rename via meta loader")
        return False

    def _interactive_resolution(self, conflict: Dict[str, Any], plan_item: Optional[Any]) -> Dict[str, Any]:
        """
        Pergunta ao usuário no terminal e retorna ação tomada.
        Actions: 'overwrite', 'skip_new', 'remove_owner', 'rename_new', 'abort'
        """
        print("\nConflito detectado:")
        print(json.dumps(conflict, indent=2, ensure_ascii=False))
        print("Ações disponíveis:")
        print("[o] sobrescrever (prefer_new)")
        print("[s] pular arquivo do novo pacote (skip_new)")
        print("[r] renomear o arquivo novo no plano (rename_new)")
        print("[m] remover pacote dono do arquivo (remove_owner)")
        print("[a] abortar toda a operação")
        choice = input("Escolha [o/s/r/m/a]: ").strip().lower()
        if choice == "o":
            return {"action": "overwrite"}
        if choice == "s":
            return {"action": "skip_new"}
        if choice == "r":
            return {"action": "rename"}
        if choice == "m":
            return {"action": "remove_owner"}
        return {"action": "abort"}

    def _resolve_single_file_conflict(self, conflict: Dict[str, Any], plan_item: Optional[Any]) -> Dict[str, Any]:
        """
        Resolve um único conflito de arquivo; retorna dict com resultado.
        """
        path = conflict.get("path")
        existing_pkg = conflict.get("existing_pkg")
        new_pkg = conflict.get("new_pkg")
        result: Dict[str, Any] = {"conflict": conflict, "resolved": False, "action": None, "note": None}

        if POLICY["log_only"]:
            result.update({"resolved": False, "action": "log_only", "note": "log-only policy"})
            LOG.info("Conflito (log-only): %s", conflict)
            return result

        # if prefer_installed -> skip new
        if POLICY["prefer_installed"]:
            result.update({"resolved": True, "action": "keep_installed", "note": f"keeping {existing_pkg}"})
            LOG.info("Política prefer_installed: mantendo %s", path)
            return result

        # if prefer_new -> overwrite (no removal)
        if POLICY["prefer_new"]:
            result.update({"resolved": True, "action": "overwrite", "note": "policy prefer_new"})
            LOG.info("Política prefer_new: %s será sobrescrito", path)
            return result

        # Auto-rename
        if POLICY["auto_rename"]:
            new_path = f"{path}.rquest"
            ok = self._apply_auto_rename_to_plan_item(plan_item, path, new_path) if plan_item else False
            if ok:
                result.update({"resolved": True, "action": "rename_new", "new_path": new_path})
                LOG.info("Auto-rename aplicado: %s -> %s", path, new_path)
                return result
            else:
                LOG.debug("Auto-rename falhou no plano; caindo para outras estratégias")

        # Auto-backup?
        backup_ok = (True, "")  # default
        if POLICY["auto_backup_conflicting"] and os.path.exists(path):
            backup_ok = self._backup_existing_file(path)
            if not backup_ok[0]:
                LOG.warning("Backup do arquivo %s falhou: %s", path, backup_ok[1])

        # Auto-remove owner if configured
        if POLICY["auto_remove_conflicting"] and existing_pkg:
            ok, msg = self._try_remove_owner_pkg(existing_pkg)
            if ok:
                result.update({"resolved": True, "action": "removed_owner", "note": msg})
                LOG.info("Removed owner package %s to resolve conflict %s", existing_pkg, path)
                return result
            else:
                LOG.debug("Tentativa de remover owner falhou: %s", msg)

        # If interactive allowed, ask user
        if POLICY["interactive"]:
            choice = self._interactive_resolution(conflict, plan_item)
            act = choice.get("action")
            if act == "overwrite":
                result.update({"resolved": True, "action": "overwrite", "note": "user chose overwrite"})
                return result
            if act == "skip_new":
                result.update({"resolved": True, "action": "skip_new", "note": "user chose skip_new"})
                # remove the file from plan_item's file list if possible
                try:
                    if plan_item:
                        self._remove_file_from_plan_item(plan_item, path)
                except Exception:
                    LOG.debug("Falha ao remover arquivo do plan_item")
                return result
            if act == "rename":
                new_path = f"{path}.rquest"
                ok = self._apply_auto_rename_to_plan_item(plan_item, path, new_path)
                if ok:
                    result.update({"resolved": True, "action": "rename_new", "new_path": new_path})
                    return result
                else:
                    result.update({"resolved": False, "action": "rename_failed"})
                    return result
            if act == "remove_owner":
                ok, msg = self._try_remove_owner_pkg(existing_pkg)
                result.update({"resolved": ok, "action": "removed_owner" if ok else "remove_failed", "note": msg})
                return result
            # abort
            raise ConflictError(f"Abortado pelo usuário na resolução do conflito: {conflict}")

        # fallback: cannot resolve
        LOG.error("Não foi possível resolver automaticamente o conflito %s; política atual não permite ações", conflict)
        raise ConflictError(f"Unresolvable conflict: {conflict}")

    def _remove_file_from_plan_item(self, plan_item: Any, path: str) -> bool:
        """
        Remove referência ao arquivo 'path' do plan_item (se possível).
        """
        try:
            if hasattr(plan_item, "files") and isinstance(plan_item.files, list):
                plan_item.files = [f for f in plan_item.files if (f.get("path") or f.get("dest") or f.get("file")) != path]
                LOG.debug("Arquivo %s removido do plan_item.files", path)
                return True
            # fallback: attach override to skip in memory
            if not hasattr(plan_item, "_conflicts_skip_files"):
                plan_item._conflicts_skip_files = set()
            plan_item._conflicts_skip_files.add(path)
            LOG.debug("Arquivo %s marcado para pular no plan_item (override)", path)
            return True
        except Exception:
            LOG.exception("Falha _remove_file_from_plan_item")
            return False

    # ---------- resolução de dependências ----------
    def resolve_dep_conflicts(self, pkg_list: List[str]) -> bool:
        """
        Tenta resolver conflitos de dependências via resolver; retorna True se plan ok.
        """
        if not self.resolver:
            LOG.debug("Resolver ausente; não podemos resolver conflitos de dependência automaticamente")
            # se policy allows, we proceed; else abort
            return POLICY["allow_dep_conflicts"]

        try:
            conflicts = self.detect_dep_conflicts(pkg_list)
            if not conflicts:
                return True
            LOG.warning("Conflitos de dependência detectados: %s", conflicts)
            # attempt automatic resolution if resolver supports it
            if hasattr(self.resolver, "auto_resolve"):
                try:
                    ok = self.resolver.auto_resolve(pkg_list)
                    if ok:
                        LOG.info("Resolver auto_resolve resolveu os conflitos")
                        return True
                except Exception:
                    LOG.exception("resolver.auto_resolve falhou")
            # if policy allows, continue with warning
            if POLICY["allow_dep_conflicts"]:
                LOG.warning("allow_dep_conflicts=true: seguindo apesar de conflitos")
                return True
            # otherwise abort
            raise ConflictError(f"Dependency conflicts: {conflicts}")
        except ConflictError:
            raise
        except Exception:
            LOG.exception("Erro ao resolver dependências")
            if POLICY["allow_dep_conflicts"]:
                return True
            return False

    # ---------- API: resolver de plano ----------
    def resolve_plan_conflicts(self, plan: Any) -> bool:
        """
        Itera sobre plan.items (UpgradePlanItem) e detecta e resolve conflitos.
        Plan.item deve ter: .package (nome), .meta_path (caminho), possivelmente .files list.
        Retorna True se o plano está pronto para aplicar; lança ConflictError caso abort.
        """
        LOG.info("Iniciando resolução de conflitos para plano %s", getattr(plan, "name", "(no-name)"))
        items = getattr(plan, "items", []) or []
        file_conflicts_all: List[Dict[str, Any]] = []
        # detect por item
        for item in list(items):  # shallow copy as we may mutate
            meta_path = getattr(item, "meta_path", None)
            if not meta_path:
                LOG.debug("Item sem meta_path: %s", getattr(item, "package", None))
                continue
            conflicts = self.detect_file_conflicts_for_meta(meta_path)
            if not conflicts:
                continue
            LOG.info("Conflicts encontrados para %s: %d", getattr(item, "package", None), len(conflicts))
            for c in conflicts:
                try:
                    res = self._resolve_single_file_conflict(c, item)
                    file_conflicts_all.append({"conflict": c, "result": res})
                    # if action = skip_new and we removed item file entirely, maybe skip item if no files left?
                    if res.get("action") == "skip_new":
                        # check if plan_item ended up with zero files -> treat as skip package install
                        skip_pkg = False
                        if hasattr(item, "files"):
                            if not item.files:
                                skip_pkg = True
                        if hasattr(item, "_conflicts_skip_files"):
                            # if all its files are marked skipped -> skip_pkg
                            # (we cannot know total files count reliably; we assume skip_new used only for specific file)
                            pass
                        if skip_pkg:
                            LOG.info("Item %s não tem mais arquivos para instalar -> removendo do plano", getattr(item, "package", None))
                            if item in plan.items:
                                plan.items.remove(item)
                except ConflictError as ce:
                    LOG.error("Conflito não resolvível para item %s: %s", getattr(item, "package", None), ce)
                    raise

        # agora checar conflitos de dependências entre items
        pkg_list = [getattr(i, "package", None) for i in plan.items if getattr(i, "package", None)]
        if pkg_list:
            ok_deps = self.resolve_dep_conflicts(pkg_list)
            if not ok_deps:
                raise ConflictError("Não foi possível resolver conflitos de dependências")
        # Report summary event
        try:
            emit_event("conflicts.plan_resolved", {"plan": getattr(plan, "name", None), "file_conflicts": len(file_conflicts_all)})
        except Exception:
            pass
        LOG.info("Resolução de conflitos completa; %d conflitos tratados", len(file_conflicts_all))
        return True

# ---------- conveniências/CLI ----------
def detect(meta_path: str) -> List[Dict[str, Any]]:
    cr = ConflictResolver()
    return cr.detect_file_conflicts_for_meta(meta_path)

def resolve_plan_conflicts(plan: Any) -> bool:
    cr = ConflictResolver()
    return cr.resolve_plan_conflicts(plan)

def _cli(argv=None):
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-conflicts", description="Detect/resolve conflicts")
    ap.add_argument("--meta", help=".meta path to analyze", default=None)
    ap.add_argument("--plan", help="plan json path (saved plan)", default=None)
    ap.add_argument("--apply", action="store_true", help="attempt to resolve conflicts for given plan")
    args = ap.parse_args(argv)
    if args.meta:
        print(json.dumps(detect(args.meta), indent=2, ensure_ascii=False))
        return
    if args.plan:
        ppath = Path(args.plan)
        if not ppath.exists():
            print("Plan file not found:", ppath)
            return
        j = json.loads(ppath.read_text(encoding="utf-8"))
        # create lightweight plan object
        class _Item:
            def __init__(self, package, meta_path, files=None):
                self.package = package
                self.meta_path = meta_path
                self.files = files or []
        class _Plan:
            def __init__(self, name, items):
                self.name = name
                self.items = items
        items = []
        for it in j.get("items", []):
            items.append(_Item(it.get("package"), it.get("meta_path"), it.get("files", [])))
        plan = _Plan(j.get("name"), items)
        print("Plan loaded:", plan.name)
        if args.apply:
            try:
                ok = resolve_plan_conflicts(plan)
                print("Plan ready:", ok)
            except Exception as e:
                print("Resolve failed:", e)
        else:
            # just detect
            for it in plan.items:
                cs = detect(it.meta_path)
                print(f"Item {it.package} conflicts:", json.dumps(cs, indent=2, ensure_ascii=False))
        return
    ap.print_help()

if __name__ == "__main__":
    _cli(sys.argv[1:])
