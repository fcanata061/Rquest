#!/usr/bin/env python3
# rquest1.0/modules/upgrade.py
# -*- coding: utf-8 -*-
"""
Rquest - upgrade.py

Responsabilidades:
 - construir/gerar um plano de upgrade (a partir de nomes de pacotes / metas / resolver)
 - validar plano (dependências)
 - delegar resolução de conflitos a modules.conflicts.ConflictResolver
 - aplicar plano via buildsystem/pkgtool (quando disponível)
 - lidar com --force, --dry-run
 - registrar runs no DB (upgrade_runs)
 - emitir hooks: pre_upgrade, on_conflict, post_upgrade
 - CLI amigável com saída colorida
"""

from __future__ import annotations
import os
import sys
import time
import json
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path

# ---------------------------
# integrações com módulos do rquest (graceful)
# ---------------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    LOG = get_logger("upgrade")
except Exception:
    LOG = logging.getLogger("rquest.upgrade")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)

# DB integration
try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    get_db = None
    def emit_event(*a, **k):
        pass

# buildsystem (apply/install)
try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:
    get_buildsystem = None

# conflicts resolver
try:
    from modules.conflicts import ConflictResolver, ConflictError  # type: ignore
except Exception:
    ConflictResolver = None
    ConflictError = Exception  # fallback generic

# meta loader (to build plan from metas)
try:
    from modules.meta import MetaLoader  # type: ignore
except Exception:
    MetaLoader = None

# resolver to compute dependency tree / plan
try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    get_resolver = None

# hooks manager
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    get_hook_manager = None

# pkgtool fallback (install prebuilt)
try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    get_pkgtool = None

# color helpers (simple ANSI)
def _c(txt: str, code: str) -> str:
    return f"\x1b[{code}m{txt}\x1b[0m"

def green(txt: str) -> str:
    return _c(txt, "32")

def red(txt: str) -> str:
    return _c(txt, "31")

def yellow(txt: str) -> str:
    return _c(txt, "33")

def blue(txt: str) -> str:
    return _c(txt, "34")

# ---------------------------
# defaults / config
# ---------------------------
CFG = get_config() if callable(get_config) else {}
UPGRADE_CFG = CFG.get("upgrade", {}) if isinstance(CFG, dict) else {}
DEFAULT_PARALLEL = int(UPGRADE_CFG.get("parallelism", 2))
DRY_RUN_DEFAULT = bool(UPGRADE_CFG.get("dry_run", False))

# ---------------------------
# helper: DB table for upgrade runs
# ---------------------------
_UPGRADE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS upgrade_runs (
    id TEXT PRIMARY KEY,
    started_at INTEGER,
    finished_at INTEGER,
    packages TEXT,
    success INTEGER,
    detail TEXT
);
"""

def _ensure_db_tables(db_conn):
    if not db_conn:
        return
    try:
        db_conn.execute(_UPGRADE_TABLE_SQL, (), commit=True)
    except Exception:
        # some db wrappers have different signatures; try simple exe
        try:
            db_conn.execute(_UPGRADE_TABLE_SQL)
            db_conn.commit()
        except Exception:
            LOG.debug("Não foi possível garantir tabela upgrade_runs no DB")

# ---------------------------
# plan abstractions (lightweight)
# ---------------------------
class UpgradePlanItem:
    def __init__(self, package: str, meta_path: Optional[str] = None, files: Optional[List[Dict[str,Any]]] = None):
        self.package = package
        self.meta_path = meta_path
        # files: list of dicts {path, sha256,...} optionally present
        self.files = files or []

    def to_dict(self) -> Dict[str,Any]:
        return {"package": self.package, "meta_path": self.meta_path, "files": self.files}

class UpgradePlan:
    def __init__(self, name: Optional[str] = None):
        self.name = name or f"plan-{int(time.time())}"
        self.items: List[UpgradePlanItem] = []

    def add_item(self, item: UpgradePlanItem):
        self.items.append(item)

    def packages(self) -> List[str]:
        return [i.package for i in self.items]

    def to_dict(self) -> Dict[str,Any]:
        return {"name": self.name, "items": [i.to_dict() for i in self.items]}

# ---------------------------
# Upgrade manager
# ---------------------------
class UpgradeManager:
    def __init__(self, db_factory=None):
        self.db = db_factory() if callable(db_factory) else (db_factory() if db_factory else (get_db()() if get_db else None))
        if self.db:
            _ensure_db_tables(self.db)
        self.buildsystem = (get_buildsystem()() if get_buildsystem else None)
        self.conflict_resolver = (ConflictResolver() if ConflictResolver else None)
        self.meta_loader = (MetaLoader()() if MetaLoader else None) if MetaLoader else None
        self.resolver = (get_resolver()() if get_resolver else None)
        self.hooks = (get_hook_manager()() if get_hook_manager else None) if get_hook_manager else None
        self.pkgtool = (get_pkgtool()() if get_pkgtool else None) if get_pkgtool else None

    # --- plan creation helpers ---
    def plan_from_package_names(self, pkg_names: List[str]) -> UpgradePlan:
        """
        Gera um plano simples a partir de nomes de pacotes:
        - usa resolver (se disponível) para expandir dependências
        - tenta localizar .meta via meta_loader para itens
        """
        plan = UpgradePlan(name=f"plan-for-{'_'.join(pkg_names)}")
        if self.resolver and hasattr(self.resolver, "expand_plan"):
            try:
                expanded = self.resolver.expand_plan(pkg_names)
                # expected expanded -> list of dicts {name, meta_path?, files?}
                for it in expanded:
                    plan.add_item(UpgradePlanItem(it.get("name") or it.get("package"), it.get("meta_path"), it.get("files")))
                return plan
            except Exception:
                LOG.debug("Resolver expand_plan falhou; caindo para heurística")
        # fallback: add direct items, try to guess meta via meta_loader
        for p in pkg_names:
            meta_path = None
            files = []
            if self.meta_loader:
                # heurística: procurar .meta no repo local paths (config.repos.local)
                try:
                    # meta_loader.search_by_name pode não existir; defensivo:
                    if hasattr(self.meta_loader, "find_meta_for"):
                        meta_path = self.meta_loader.find_meta_for(p)
                    elif hasattr(self.meta_loader, "search_by_name"):
                        meta_path = self.meta_loader.search_by_name(p)
                except Exception:
                    meta_path = None
                # if meta_path, try to load files list
                try:
                    if meta_path:
                        mp = self.meta_loader.load(meta_path)
                        raw = getattr(mp, "raw", {}) or {}
                        files = raw.get("files", []) or []
                except Exception:
                    LOG.debug("Falha ao carregar meta para obter arquivos")
            plan.add_item(UpgradePlanItem(p, meta_path, files))
        return plan

    # --- core apply logic ---
    def apply_plan(self, plan: UpgradePlan, force: bool = False, dry_run: bool = False, parallel: int = DEFAULT_PARALLEL) -> Dict[str,Any]:
        """
        Valida plan (resolver dependências), chama ConflictResolver, aplica plan via buildsystem/pkgtool.
        Retorna dicionário com resultado.
        """
        run_id = f"upgrade-{int(time.time())}"
        started_at = int(time.time())
        LOG.info(blue(f"Iniciando upgrade run {run_id}: packages={plan.packages()}"))
        if self.hooks:
            try:
                self.hooks.run("pre_upgrade", {"plan": plan.to_dict(), "run_id": run_id})
            except Exception:
                LOG.debug("pre_upgrade hook falhou")

        # 1) detect/resolve conflicts via ConflictResolver
        try:
            if self.conflict_resolver:
                try:
                    # if force, skip ConflictError by passing some flag? We'll follow semantics:
                    LOG.debug("Executando ConflictResolver.resolve_plan_conflicts")
                    self.conflict_resolver.resolve_plan_conflicts(plan)
                except ConflictError as ce:
                    # if force==True, try to continue with warning
                    if force:
                        LOG.warning(yellow(f"ConflictError detectado mas --force fornecido: {ce}"))
                        # try to emit hook and continue
                        try:
                            if self.hooks:
                                self.hooks.run("on_conflict", {"run_id": run_id, "error": str(ce), "plan": plan.to_dict()})
                        except Exception:
                            pass
                    else:
                        LOG.error(red(f"Conflito detectado: {ce} — abortando upgrade. Use --force para ignorar (não recomendado)."))
                        # emit hook and stop
                        try:
                            if self.hooks:
                                self.hooks.run("on_conflict", {"run_id": run_id, "error": str(ce), "plan": plan.to_dict()})
                        except Exception:
                            pass
                        raise
            else:
                LOG.debug("ConflictResolver não disponível; pulando resolução de conflitos (risco aumentado)")
        except ConflictError:
            # persist run as failed
            if self.db:
                try:
                    self.db.execute("INSERT OR REPLACE INTO upgrade_runs (id, started_at, finished_at, packages, success, detail) VALUES (?,?,?,?,?,?)",
                                    (run_id, started_at, int(time.time()), json.dumps(plan.packages()), 0, "conflict"), commit=True)
                except Exception:
                    LOG.debug("Falha ao persistir upgrade_runs no DB")
            raise

        # 2) dependency check via resolver (if available)
        if self.resolver and hasattr(self.resolver, "verify_plan"):
            try:
                ok = self.resolver.verify_plan(plan.to_dict())
                if not ok:
                    if force:
                        LOG.warning(yellow("Plan verification falhou mas --force foi fornecido; seguindo"))
                    else:
                        LOG.error(red("Plan verification falhou — abortando upgrade"))
                        raise Exception("plan verification failed")
            except Exception as e:
                LOG.debug("Resolver verify_plan erro: %s", e)
                if not force:
                    raise

        # 3) apply items (buildsystem / pkgtool)
        results = []
        overall_ok = True
        for item in plan.items:
            pkg = item.package
            LOG.info(blue(f"Aplicando item: {pkg} (meta={item.meta_path})"))
            if dry_run:
                LOG.info(yellow(f"[dry-run] pulando aplicação real de {pkg}"))
                results.append({"package": pkg, "ok": True, "note": "dry-run"})
                continue

            applied = False
            # prefer buildsystem.build_package if available
            try:
                if self.buildsystem and hasattr(self.buildsystem, "build_and_install"):
                    try:
                        res = self.buildsystem.build_and_install(item, parallel=parallel, force=force)
                        applied = bool(res.get("ok", False))
                        results.append({"package": pkg, "ok": applied, "detail": res})
                    except Exception as e:
                        LOG.exception("buildsystem.build_and_install falhou para %s", pkg)
                        results.append({"package": pkg, "ok": False, "detail": str(e)})
                        applied = False
                elif self.buildsystem and hasattr(self.buildsystem, "build_package"):
                    try:
                        res = self.buildsystem.build_package(item, force=force, dry_run=dry_run)
                        # attempt install step if build returns artifact
                        artifact = res.get("detail", {}).get("artifact") if isinstance(res, dict) else None
                        if artifact and self.pkgtool and hasattr(self.pkgtool, "install_bin"):
                            inst = self.pkgtool.install_bin(artifact, target=None, use_fakeroot=True)
                            applied = bool(inst.get("ok", False))
                            results.append({"package": pkg, "ok": applied, "detail": {"build": res, "install": inst}})
                        else:
                            results.append({"package": pkg, "ok": res.get("ok", False) if isinstance(res, dict) else False, "detail": res})
                            applied = bool(res.get("ok", False) if isinstance(res, dict) else False)
                    except Exception as e:
                        LOG.exception("buildsystem.build_package falhou para %s", pkg)
                        results.append({"package": pkg, "ok": False, "detail": str(e)})
                        applied = False
                elif self.pkgtool and hasattr(self.pkgtool, "build_and_install"):
                    try:
                        res = self.pkgtool.build_and_install(pkg, force=force)
                        applied = bool(res.get("ok", False))
                        results.append({"package": pkg, "ok": applied, "detail": res})
                    except Exception as e:
                        LOG.exception("pkgtool.build_and_install falhou para %s", pkg)
                        results.append({"package": pkg, "ok": False, "detail": str(e)})
                        applied = False
                else:
                    # fallback: cannot apply
                    LOG.error(red(f"Nenhum mecanismo disponível para construir/instalar {pkg}."))
                    results.append({"package": pkg, "ok": False, "detail": "no build/install backend"})
                    applied = False
            except Exception:
                LOG.exception("Erro inesperado ao aplicar item %s", pkg)
                results.append({"package": pkg, "ok": False, "detail": "unexpected error"})
                applied = False

            if not applied:
                overall_ok = False
                # continue applying remaining items? depends on policy 'stop_on_error'
                # we'll stop by default
                LOG.error(red(f"Falha ao aplicar pacote {pkg}; interrompendo plano"))
                break

        finished_at = int(time.time())
        success_flag = 1 if overall_ok else 0

        # persist run result
        if self.db:
            try:
                self.db.execute("INSERT OR REPLACE INTO upgrade_runs (id, started_at, finished_at, packages, success, detail) VALUES (?,?,?,?,?,?)",
                                (run_id, started_at, finished_at, json.dumps(plan.packages()), success_flag, json.dumps(results)), commit=True)
            except Exception:
                LOG.debug("Falha ao persistir upgrade_runs no DB")

        # hooks post
        if self.hooks:
            try:
                self.hooks.run("post_upgrade", {"run_id": run_id, "plan": plan.to_dict(), "success": bool(overall_ok), "results": results})
            except Exception:
                LOG.debug("post_upgrade hook falhou")

        emit_event("upgrade.run.finished", {"run_id": run_id, "success": bool(overall_ok), "packages": plan.packages()})
        LOG.info(green(f"Upgrade run {run_id} finalizado (success={overall_ok})"))
        return {"run_id": run_id, "success": overall_ok, "results": results}

# ---------------------------
# CLI
# ---------------------------
def _cli(argv=None):
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-upgrade", description="Rquest upgrade manager")
    sub = ap.add_subparsers(dest="cmd")

    p_upgrade = sub.add_parser("upgrade", help="upgrade packages by names")
    p_upgrade.add_argument("packages", nargs="+", help="package names to upgrade/install")
    p_upgrade.add_argument("--force", action="store_true", help="ignore conflicts when possible")
    p_upgrade.add_argument("--dry-run", action="store_true", help="do not perform destructive actions")
    p_upgrade.add_argument("--parallel", type=int, default=DEFAULT_PARALLEL, help="parallel jobs for buildsystem")
    p_upgrade.add_argument("--export-report", help="path to write JSON report of the run")

    p_apply = sub.add_parser("apply-plan", help="apply a saved plan JSON")
    p_apply.add_argument("plan_file", help="path to plan.json")
    p_apply.add_argument("--force", action="store_true")
    p_apply.add_argument("--dry-run", action="store_true")
    p_apply.add_argument("--parallel", type=int, default=DEFAULT_PARALLEL)

    args = ap.parse_args(argv)
    um = UpgradeManager(db_factory=get_db if get_db else None)

    try:
        if args.cmd == "upgrade":
            plan = um.plan_from_package_names(args.packages)
            LOG.info(f"Plano gerado com {len(plan.items)} itens")
            res = um.apply_plan(plan, force=args.force, dry_run=args.dry_run or DRY_RUN_DEFAULT, parallel=args.parallel)
            print(json.dumps(res, indent=2, ensure_ascii=False))
            if getattr(args, "export_report", None):
                Path(args.export_report).write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding="utf-8")
            return 0

        if args.cmd == "apply-plan":
            p = Path(args.plan_file)
            if not p.exists():
                print(red(f"Plan file not found: {p}"))
                return 2
            j = json.loads(p.read_text(encoding="utf-8"))
            plan = UpgradePlan(name=j.get("name"))
            for it in j.get("items", []):
                plan.add_item(UpgradePlanItem(it.get("package"), it.get("meta_path"), it.get("files", [])))
            res = um.apply_plan(plan, force=args.force, dry_run=args.dry_run or DRY_RUN_DEFAULT, parallel=args.parallel)
            print(json.dumps(res, indent=2, ensure_ascii=False))
            return 0

        ap.print_help()
        return 1
    except ConflictError as ce:
        LOG.error(red(f"Upgrade abortado por conflito: {ce}"))
        # emit event and exit non-zero
        try:
            emit_event("upgrade.conflict.abort", {"error": str(ce)})
        except Exception:
            pass
        return 3
    except Exception as e:
        LOG.exception("Erro no upgrade: %s", e)
        return 4

if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
