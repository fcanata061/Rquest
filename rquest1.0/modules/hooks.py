# Rquest/modules/hooks.py

import os
import subprocess
import importlib
from typing import List, Dict, Any, Optional
from modules import config, db, logging, sandbox

logger = logging.get_logger("hooks")


class HookManager:
    def __init__(self):
        self.hooks: Dict[str, List[Dict[str, Any]]] = {}
        self._load_global_hooks()

    # -----------------------------
    # Carregamento de hooks
    # -----------------------------
    def _load_global_hooks(self):
        cfg = config.get("hooks", {})
        for event, entries in cfg.get("events", {}).items():
            for entry in entries:
                self.register(
                    event=event,
                    name=entry.get("name"),
                    type=entry.get("type"),
                    command_or_module=entry.get("command") or entry.get("module"),
                    priority=entry.get("priority", 10),
                    origin="config"
                )

    def load_from_meta(self, meta: Dict[str, Any], pkg_name: str):
        hooks_cfg = meta.get("hooks", {})
        for event, entries in hooks_cfg.items():
            for entry in entries:
                self.register(
                    event=event,
                    name=entry.get("name", f"{pkg_name}-{entry.get('type')}"),
                    type=entry.get("type"),
                    command_or_module=entry.get("command") or entry.get("module"),
                    priority=entry.get("priority", 10),
                    origin="meta",
                    package=pkg_name
                )

    def load_from_dir(self, pkg_dir: str, pkg_name: str):
        hooks_dir = os.path.join(pkg_dir, "hooks")
        if not os.path.isdir(hooks_dir):
            return
        for fname in os.listdir(hooks_dir):
            if fname.startswith("pre_") or fname.startswith("post_"):
                event = fname.replace(".sh", "").replace(".py", "")
                fpath = os.path.join(hooks_dir, fname)
                if fname.endswith(".sh"):
                    self.register(
                        event=event,
                        name=fname,
                        type="script",
                        command_or_module=fpath,
                        origin="dir",
                        package=pkg_name
                    )
                elif fname.endswith(".py"):
                    module_name = f"hooks.{fname[:-3]}"
                    self.register(
                        event=event,
                        name=fname,
                        type="python",
                        command_or_module=module_name,
                        origin="dir",
                        package=pkg_name
                    )

    def load_from_db(self):
        rows = db.get_all("hooks")
        for row in rows:
            self.register(
                event=row["event"],
                name=row["name"],
                type=row["type"],
                command_or_module=row["command"] or row["module"],
                priority=row["priority"],
                origin="db"
            )

    # -----------------------------
    # Registro e gerenciamento
    # -----------------------------
    def register(self, event: str, name: str, type: str, command_or_module: str,
                 priority: int = 10, origin: str = "runtime", package: Optional[str] = None):
        if event not in self.hooks:
            self.hooks[event] = []
        self.hooks[event].append({
            "name": name,
            "type": type,
            "command_or_module": command_or_module,
            "priority": priority,
            "origin": origin,
            "package": package,
            "enabled": True
        })
        self.hooks[event].sort(key=lambda h: h["priority"])

    def unregister(self, event: str, name: str):
        if event in self.hooks:
            self.hooks[event] = [h for h in self.hooks[event] if h["name"] != name]

    def list(self, event: Optional[str] = None) -> List[Dict[str, Any]]:
        if event:
            return self.hooks.get(event, [])
        all_hooks = []
        for ev, entries in self.hooks.items():
            all_hooks.extend(entries)
        return all_hooks

    def enable(self, event: str, name: str):
        for hook in self.hooks.get(event, []):
            if hook["name"] == name:
                hook["enabled"] = True

    def disable(self, event: str, name: str):
        for hook in self.hooks.get(event, []):
            if hook["name"] == name:
                hook["enabled"] = False

    # -----------------------------
    # Execução de hooks
    # -----------------------------
    def run(self, event: str, context: Optional[Dict[str, Any]] = None,
            package: Optional[str] = None) -> bool:
        hooks = self.hooks.get(event, [])
        if not hooks:
            return True

        logger.info(f"[hooks] Executando {len(hooks)} hooks para evento '{event}'")

        for hook in hooks:
            if not hook["enabled"]:
                continue
            if package and hook.get("package") and hook["package"] != package:
                continue

            name = hook["name"]
            origin = hook["origin"]
            htype = hook["type"]
            cmd = hook["command_or_module"]

            logger.info(f"[hooks] Rodando hook {name} ({event}, origin={origin})")

            try:
                if htype == "script":
                    result = sandbox.run_in_sandbox(["/bin/sh", "-c", cmd], context)
                elif htype == "python":
                    module = importlib.import_module(cmd)
                    result = sandbox.run_python_in_sandbox(module, context)
                else:
                    logger.warn(f"[hooks] Tipo de hook desconhecido: {htype}")
                    continue

                if result != 0:
                    logger.error(f"[hooks] Hook {name} falhou com código {result}")
                    if config.get("hooks.global.stop_on_failure", True):
                        return False
                else:
                    logger.info(f"[hooks] Hook {name} finalizado com sucesso")

                # registrar no banco
                db.insert("hooks_log", {
                    "event": event,
                    "name": name,
                    "origin": origin,
                    "status": "success" if result == 0 else "failed"
                })

            except Exception as e:
                logger.error(f"[hooks] Erro ao executar hook {name}: {e}")
                if config.get("hooks.global.stop_on_failure", True):
                    return False
        return True
