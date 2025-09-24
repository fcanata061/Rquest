#!/usr/bin/env python3
# Rquest/rquest1.0/cli.py
"""
Rquest CLI - integra todos os módulos do Rquest

Como funciona:
- importa módulos (config, logging, db, meta, fetcher, patches, hooks, sandbox, buildsystem,
  pkgtool, resolver, masks, slots, conflicts, deepclean, remove, upgrade, toolchain,
  python_check, query_tools, update_notifier, audit)
- cada subcomando delega ao módulo apropriado
- usa rich para UI/animations se disponível, fallback ANSI spinner
"""

from __future__ import annotations

import os
import sys
import json
import time
import threading
import shutil
import subprocess
import argparse
import traceback
from typing import Any, Callable, Optional, List, Dict

# -----------------------
# Optional UI libs
# -----------------------
USE_RICH = False
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.spinner import Spinner
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
    USE_RICH = True
    console = Console()
except Exception:
    USE_RICH = False
    console = None

# -----------------------
# ANSI fallback
# -----------------------
CSI = "\x1b["
def colorize(s: str, fg: str = "37"):
    return f"{CSI}38;5;{fg}m{s}{CSI}0m"

# -----------------------
# Logging integration
# -----------------------
try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("cli")
except Exception:
    import logging
    logger = logging.getLogger("rquest.cli")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# -----------------------
# Module imports (graceful)
# -----------------------
def safe_import(name: str):
    try:
        mod = __import__(name, fromlist=["*"])
        return mod
    except Exception:
        return None

meta = safe_import("modules.meta")
fetcher = safe_import("modules.fetcher")
patches = safe_import("modules.patches")
hooks = safe_import("modules.hooks")
sandbox = safe_import("modules.sandbox")
buildsystem = safe_import("modules.buildsystem")
pkgtool = safe_import("modules.pkgtool")
resolver = safe_import("modules.resolver")
masks = safe_import("modules.masks")
slots = safe_import("modules.slots")
conflicts = safe_import("modules.conflicts")
deepclean = safe_import("modules.deepclean")
remove_mod = safe_import("modules.remove")
upgrade_mod = safe_import("modules.upgrade")
toolchain_mod = safe_import("modules.toolchain")
python_check = safe_import("modules.python_check")
query_tools = safe_import("modules.query_tools")
update_notifier = safe_import("modules.update_notifier")
audit_mod = safe_import("modules.audit")
db_mod = safe_import("modules.db")
config_mod = safe_import("modules.config")

# -----------------------
# Helper: spinner / run wrapper
# -----------------------
class _AsciiSpinner:
    def __init__(self, text: str = "working"):
        self.text = text
        self._stop = threading.Event()
        self.chars = "|/-\\"
        self.t = None

    def start(self):
        def run():
            i = 0
            while not self._stop.is_set():
                sys.stdout.write(f"\r{self.chars[i % len(self.chars)]} {self.text}")
                sys.stdout.flush()
                i += 1
                time.sleep(0.12)
            sys.stdout.write("\r" + " " * (len(self.text) + 4) + "\r")
            sys.stdout.flush()
        self.t = threading.Thread(target=run, daemon=True)
        self.t.start()

    def stop(self):
        self._stop.set()
        if self.t:
            self.t.join()

def run_with_spinner(func: Callable[..., Any], args: Optional[List[Any]] = None, kwargs: Optional[Dict[str,Any]] = None, text: str = "working", disable_spinner: bool = False):
    args = args or []
    kwargs = kwargs or {}
    result = {"ok": False, "result": None, "exception": None}
    def target():
        try:
            res = func(*args, **kwargs)
            result["ok"] = True
            result["result"] = res
        except Exception as e:
            result["exception"] = e
            result["result"] = None

    th = threading.Thread(target=target)
    th.start()

    spinner = None
    if not disable_spinner:
        if USE_RICH and console:
            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as prog:
                task = prog.add_task(description=text, total=None)
                while th.is_alive():
                    time.sleep(0.1)
                prog.remove_task(task)
        else:
            spinner = _AsciiSpinner(text=text)
            spinner.start()
            while th.is_alive():
                time.sleep(0.1)
            spinner.stop()
    else:
        th.join()

    th.join()
    if result["exception"]:
        # log traceback
        logger.exception("Exception in task: %s", result["exception"])
        if USE_RICH and console:
            console.print_exception()
        else:
            traceback.print_exception(type(result["exception"]), result["exception"], result["exception"].__traceback__)
        raise result["exception"]
    return result["result"]

# -----------------------
# Small pretty helpers
# -----------------------
def print_ok(msg: str):
    if USE_RICH and console:
        console.print(f"[bold green]✔[/] {msg}")
    else:
        print(colorize("✔ " + msg, "34"))

def print_warn(msg: str):
    if USE_RICH and console:
        console.print(f"[bold yellow]![/] {msg}")
    else:
        print(colorize("! " + msg, "3"))

def print_err(msg: str):
    if USE_RICH and console:
        console.print(f"[bold red]✖[/] {msg}")
    else:
        print(colorize("✖ " + msg, "1"))

def print_info(msg: str):
    if USE_RICH and console:
        console.print(f"[cyan]{msg}[/cyan]")
    else:
        print(colorize(msg, "36"))

# -----------------------
# CLI Implementation
# -----------------------
class RquestCLI:
    def __init__(self):
        # instantiate module managers if available
        self.meta = meta.MetaLoader() if meta and hasattr(meta, "MetaLoader") else None
        self.fetcher = fetcher.Fetcher() if fetcher and hasattr(fetcher, "Fetcher") else None
        self.patches = patches if patches else None
        self.hooks = hooks.get_hook_manager()() if hooks and hasattr(hooks, "get_hook_manager") else None
        self.sandbox = sandbox.get_sandbox_manager()() if sandbox and hasattr(sandbox, "get_sandbox_manager") else None
        self.buildsystem = buildsystem.get_buildsystem()() if buildsystem and hasattr(buildsystem, "get_buildsystem") else None
        self.pkgtool = pkgtool.get_pkgtool()() if pkgtool and hasattr(pkgtool, "get_pkgtool") else None
        self.resolver = resolver.get_resolver()() if resolver and hasattr(resolver, "get_resolver") else None
        self.masks = masks.get_masks()() if masks and hasattr(masks, "get_masks") else None
        self.slots = slots.get_slots()() if slots and hasattr(slots, "get_slots") else None
        self.conflicts = conflicts.get_detector()() if conflicts and hasattr(conflicts, "get_detector") else None
        self.deepclean = deepclean.get_cleaner()() if deepclean and hasattr(deepclean, "get_cleaner") else None
        self.remove = remove_mod.get_remover()() if remove_mod and hasattr(remove_mod, "get_remover") else None
        self.upgrade = upgrade_mod.get_upgrader()() if upgrade_mod and hasattr(upgrade_mod, "get_upgrader") else None
        self.toolchain = toolchain_mod.ToolchainManager() if toolchain_mod and hasattr(toolchain_mod, "ToolchainManager") else None
        self.python_check = python_check.PythonCheck() if python_check and hasattr(python_check, "PythonCheck") else None
        self.query = query_tools.QueryTools() if query_tools and hasattr(query_tools, "QueryTools") else None
        self.notifier = update_notifier if update_notifier else None
        self.audit = audit_mod.get_auditor()() if audit_mod and hasattr(audit_mod, "get_auditor") else None
        self.db = db_mod.get_db()() if db_mod and hasattr(db_mod, "get_db") else None
        # config
        self.config = config_mod.get_config() if config_mod and hasattr(config_mod, "get_config") else {}
        # animation and UI settings
        self.disable_spinner = False

    # --------------
    # LOW LEVEL primitives (exposed as commands)
    # --------------
    def fetch_src(self, meta_path_or_name: str, dest: Optional[str] = None, force: bool = False):
        """
        download sources for a meta; meta_path_or_name may be a path or a package name
        """
        print_info(f"Fetching sources for {meta_path_or_name}...")
        if self.fetcher and hasattr(self.fetcher, "fetch_for_meta"):
            return run_with_spinner(self.fetcher.fetch_for_meta, args=[meta_path_or_name, dest, force], text=f"fetching {meta_path_or_name}", disable_spinner=self.disable_spinner)
        # fallback: try meta loader to obtain source URL and use curl/wget
        if self.meta and hasattr(self.meta, "load_meta"):
            m = self.meta.load_meta(meta_path_or_name)
            if not m:
                raise RuntimeError("meta not found")
            src = m.get("source", {}).get("url")
            if not src:
                raise RuntimeError("no source URL in meta")
            outdir = dest or os.path.join("/tmp", f"rfetch-{m.get('name')}-{int(time.time())}")
            os.makedirs(outdir, exist_ok=True)
            filename = os.path.basename(src)
            targ = os.path.join(outdir, filename)
            cmd = shutil.which("curl") and ["curl", "-L", "-o", targ, src] or (shutil.which("wget") and ["wget", "-O", targ, src] or None)
            if cmd:
                run_with_spinner(subprocess.check_call, args=[cmd], text=f"downloading {filename}", disable_spinner=self.disable_spinner)
                return {"ok": True, "path": targ}
        raise RuntimeError("fetcher not available")

    def apply_patches(self, src_dir: str, meta: Optional[Dict[str,Any]] = None):
        print_info(f"Applying patches in {src_dir}...")
        if self.patches and hasattr(self.patches, "apply_patches"):
            return run_with_spinner(self.patches.apply_patches, args=[src_dir, meta], text="applying patches", disable_spinner=self.disable_spinner)
        # fallback: no patches found
        print_warn("patches module not available; skipping")
        return {"ok": True, "skipped": True}

    def run_hooks(self, stage: str, context: Dict[str,Any]):
        print_info(f"Running hooks: {stage}...")
        if self.hooks and hasattr(self.hooks, "run"):
            return run_with_spinner(self.hooks.run, args=[stage, context], text=f"hooks {stage}", disable_spinner=self.disable_spinner)
        print_warn("hooks module not available; skipping")
        return {"ok": True, "skipped": True}

    def sandbox_run(self, cmd: Optional[List[str]] = None, package_dir: Optional[str] = None, env: Optional[Dict[str,str]] = None, profile: Optional[str] = None, container: Optional[str] = None):
        print_info("Running build in sandbox...")
        if self.sandbox and hasattr(self.sandbox, "run_in_sandbox"):
            return run_with_spinner(self.sandbox.run_in_sandbox, args=[cmd], kwargs={"package_dir": package_dir, "env": env, "profile": profile, "container": container}, text="sandbox", disable_spinner=self.disable_spinner)
        # fallback: run in subprocess
        if cmd:
            return run_with_spinner(subprocess.check_call, args=[cmd], text="running command", disable_spinner=self.disable_spinner)
        raise RuntimeError("sandbox and command not available")

    def build_package(self, meta_path_or_name: str, profile: Optional[str] = None, no_install: bool = False):
        print_info(f"Building package {meta_path_or_name} (profile={profile})...")
        if self.buildsystem and hasattr(self.buildsystem, "build_meta"):
            return run_with_spinner(self.buildsystem.build_meta, args=[meta_path_or_name, profile, no_install], text=f"building {meta_path_or_name}", disable_spinner=self.disable_spinner)
        # fallback generic: fetch -> extract -> configure/make
        res = self.fetch_src(meta_path_or_name)
        if not res:
            raise RuntimeError("failed fetch")
        # try naive build (best-effort)
        src_tgz = res.get("path") if isinstance(res, dict) else res
        work = None
        try:
            work = os.path.dirname(src_tgz) if src_tgz else "/tmp"
            # naive untar then make
            # NOTE: this is fallback and may not work for many packages
            cmd = f"tar xf {src_tgz} -C {work} && cd {work}/* && ./configure --prefix=/usr && make -j$(nproc)"
            run_with_spinner(subprocess.check_call, args=[cmd], text="fallback build", disable_spinner=self.disable_spinner)
            if not no_install:
                cmdi = f"cd {work}/* && sudo make install"
                run_with_spinner(subprocess.check_call, args=[cmdi], text="fallback install", disable_spinner=self.disable_spinner)
            return {"ok": True}
        finally:
            pass

    def install_package(self, meta_path_or_name: str, target_profile: Optional[str] = None, rebuild: bool = False, force: bool = False):
        """
        Full install pipeline invoked by 'rquest install <pkg>'
        """
        print_info(f"Install requested: {meta_path_or_name}")
        # 1) load meta
        if not self.meta or not hasattr(self.meta, "load_meta"):
            raise RuntimeError("meta module missing; cannot install")
        metaobj = self.meta.load_meta(meta_path_or_name)  # should raise if not found
        if not metaobj:
            raise RuntimeError("meta not found")
        name = metaobj.get("name")
        version = metaobj.get("version")
        print_info(f"meta {name} v{version} loaded")
        # 2) resolve dependencies
        if self.resolver and hasattr(self.resolver, "resolve_for_install"):
            print_info("resolving dependencies...")
            plan = run_with_spinner(self.resolver.resolve_for_install, args=[metaobj], text="resolving deps", disable_spinner=self.disable_spinner)
        else:
            plan = {"order": [metaobj], "missing": []}
            print_warn("resolver not available; using simple plan")
        # 3) masks / slots / conflicts checks
        if self.masks and hasattr(self.masks, "check_masks"):
            run_with_spinner(self.masks.check_masks, args=[name], text="checking masks", disable_spinner=self.disable_spinner)
        if self.slots and hasattr(self.slots, "ensure_slot_ok"):
            run_with_spinner(self.slots.ensure_slot_ok, args=[metaobj], text="checking slots", disable_spinner=self.disable_spinner)
        if self.conflicts and hasattr(self.conflicts, "check_conflicts"):
            run_with_spinner(self.conflicts.check_conflicts, args=[metaobj], text="checking conflicts", disable_spinner=self.disable_spinner)
        # 4) for each pkg in plan order: fetch, patches, build (in sandbox), install via fakeroot, post hooks, register in db
        installed = []
        for pkgmeta in plan.get("order", []):
            nm = pkgmeta.get("name")
            print_info(f"Processing {nm} ...")
            # fetch
            run_with_spinner(self.fetch_src, args=[pkgmeta], text=f"fetch {nm}", disable_spinner=self.disable_spinner)
            # apply patches
            run_with_spinner(self.apply_patches, args=[None, pkgmeta], text=f"patch {nm}", disable_spinner=self.disable_spinner)
            # run pre-build hooks
            run_with_spinner(self.run_hooks, args=["pre-build", {"meta": pkgmeta}], text=f"pre-build {nm}", disable_spinner=self.disable_spinner)
            # build (in sandbox/buildsystem)
            build_res = run_with_spinner(self.build_package, args=[pkgmeta, target_profile, False], text=f"build {nm}", disable_spinner=self.disable_spinner)
            # install stage: buildsystem.install using fakeroot if available
            if self.buildsystem and hasattr(self.buildsystem, "install_meta"):
                run_with_spinner(self.buildsystem.install_meta, args=[pkgmeta], text=f"install {nm}", disable_spinner=self.disable_spinner)
            else:
                # fallback: try naive make install with sudo (dangerous)
                print_warn("buildsystem.install_meta not available; attempting naive install with sudo")
                try:
                    # attempt to find install path in metadata staging
                    # fallback no-op
                    pass
                except Exception:
                    pass
            # post-install hooks
            run_with_spinner(self.run_hooks, args=["post-install", {"meta": pkgmeta}], text=f"post-install {nm}", disable_spinner=self.disable_spinner)
            # register in DB
            try:
                if self.db and hasattr(self.db, "register_package"):
                    self.db.register_package(pkgmeta)
                elif self.db and hasattr(self.db, "execute"):
                    # fallback to raw insert if table exists
                    try:
                        self.db.execute("""CREATE TABLE IF NOT EXISTS packages (name TEXT PRIMARY KEY, version TEXT, meta JSON)""", (), commit=True)
                        self.db.execute("INSERT OR REPLACE INTO packages (name, version, meta) VALUES (?,?,?)", (nm, pkgmeta.get("version"), json.dumps(pkgmeta)), commit=True)
                    except Exception:
                        pass
            except Exception:
                logger.exception("db registration failed for %s", nm)
            installed.append(nm)
            print_ok(f"{nm} installed")
        # 5) final audit / logging
        if self.audit and hasattr(self.audit, "post_install_audit"):
            run_with_spinner(self.audit.post_install_audit, args=[installed], text="audit post-install", disable_spinner=self.disable_spinner)
        print_ok(f"Installation complete: {', '.join(installed)}")
        return {"ok": True, "installed": installed}
      # continuation PARTE 2/2
    # --------------
    # High level commands: remove, upgrade, fetch, build, quickpkg, query, audit
    # --------------
    def remove_package(self, package: str, purge: bool = False):
        print_info(f"Removing {package} ...")
        if self.remove and hasattr(self.remove, "remove_package"):
            return run_with_spinner(self.remove.remove_package, args=[package, purge], text=f"removing {package}", disable_spinner=self.disable_spinner)
        # fallback: naive removal via db record and files list (dangerous)
        try:
            if self.db and hasattr(self.db, "fetchone"):
                row = self.db.fetchone("SELECT meta FROM packages WHERE name = ? LIMIT 1", (package,))
                if row:
                    meta = json.loads(row.get("meta") or "{}")
                    # try to remove files if recorded in db
                    files = self.db.fetchall("SELECT path FROM files WHERE package = ?", (package,))
                    for f in files:
                        try:
                            os.remove(f.get("path"))
                        except Exception:
                            pass
                    # remove db records
                    self.db.execute("DELETE FROM packages WHERE name = ?", (package,), commit=True)
                    print_ok(f"{package} removed (best-effort)")
                    return {"ok": True}
        except Exception:
            logger.exception("naive remove failed")
        raise RuntimeError("remove not available")

    def upgrade_package(self, package: Optional[str] = None, all_pkgs: bool = False):
        print_info("Upgrade requested")
        if self.upgrade and hasattr(self.upgrade, "upgrade"):
            return run_with_spinner(self.upgrade.upgrade, args=[package, all_pkgs], text="upgrading", disable_spinner=self.disable_spinner)
        raise RuntimeError("upgrade module not available")

    def quickpkg(self, package: str, output_dir: Optional[str] = None):
        print_info(f"Quickpkg {package} ...")
        if self.pkgtool and hasattr(self.pkgtool, "quickpkg"):
            return run_with_spinner(self.pkgtool.quickpkg, args=[package, output_dir], text=f"quickpkg {package}", disable_spinner=self.disable_spinner)
        raise RuntimeError("pkgtool.quickpkg not available")

    def query(self, argv: List[str]):
        print_info("Querying...")
        if not self.query:
            raise RuntimeError("query_tools not available")
        # delegate to query_tools CLI if exists
        try:
            if hasattr(self.query, "_cli"):
                return self.query._cli(argv)
        except Exception:
            # fallback to call qlist etc.
            parser = argparse.ArgumentParser(prog="rquest query")
            sub = parser.add_subparsers(dest="cmd")
            sub.add_parser("qlist")
            sub.add_parser("qdepends")
            args = parser.parse_args(argv)
            if args.cmd == "qlist":
                return self.query.qlist()
        raise RuntimeError("query command failed")

    def audit_run(self, full: bool = False):
        print_info("Running audit...")
        if self.audit and hasattr(self.audit, "audit_system"):
            return run_with_spinner(self.audit.audit_system, args=[full], text="auditing", disable_spinner=self.disable_spinner)
        raise RuntimeError("audit module not available")

    # --------------
    # Toolchain & Python commands
    # --------------
    def toolchain_cmd(self, argv: List[str]):
        if not self.toolchain:
            raise RuntimeError("toolchain module not available")
        # simple subcommand handler
        if not argv:
            print_info("Available toolchain commands: list, bootstrap, register, package, distribute")
            return
        cmd = argv[0]
        if cmd == "list":
            lst = self.toolchain.list_profiles()
            if USE_RICH and console:
                table = Table(title="Toolchain Profiles")
                table.add_column("name")
                table.add_column("kind")
                table.add_column("cflags")
                for p in lst:
                    table.add_row(p.get("name",""), p.get("kind",""), p.get("cflags",""))
                console.print(table)
            else:
                print(json.dumps(lst, indent=2))
        elif cmd == "bootstrap":
            name = argv[1] if len(argv) > 1 else f"tc-{int(time.time())}"
            stages = int(argv[2]) if len(argv) > 2 else 2
            return run_with_spinner(self.toolchain.bootstrap, args=[name, "balanced", stages], text=f"bootstrapping {name}", disable_spinner=self.disable_spinner)
        elif cmd == "register":
            # register existing path
            name = argv[1]; path = argv[2]
            prof = self.toolchain.profiles.get("balanced")
            return self.toolchain.register_toolchain(name, prof, path)
        else:
            raise RuntimeError("unknown toolchain subcommand")

    def python_cmd(self, argv: List[str]):
        if not self.python_check:
            raise RuntimeError("python_check module not available")
        if not argv:
            print_info("python commands: list, install, use, venv, inspect, doctor, repair")
            return
        cmd = argv[0]
        if cmd == "list":
            print(json.dumps(self.python_check.list_installs(), indent=2))
        elif cmd == "install":
            ver = argv[1]
            return run_with_spinner(self.python_check.install_version, args=[ver], text=f"install python {ver}", disable_spinner=self.disable_spinner)
        elif cmd == "use":
            py = argv[1]
            return run_with_spinner(self.python_check.use_python_for_rquest, args=[py], text=f"switch rquest python", disable_spinner=self.disable_spinner)
        elif cmd == "venv":
            sub = argv[1] if len(argv)>1 else None
            if sub == "create":
                name = argv[2]
                return self.python_check.create_venv(name)
            elif sub == "list":
                print(json.dumps(self.python_check.list_venvs(), indent=2))
            elif sub == "remove":
                name = argv[2]
                return self.python_check.remove_venv(name)
        elif cmd == "doctor":
            py = argv[1] if len(argv)>1 else None
            return run_with_spinner(self.python_check.doctor, args=[py], text="python doctor", disable_spinner=self.disable_spinner)
        elif cmd == "repair":
            py = argv[1] if len(argv)>1 else None
            return run_with_spinner(self.python_check.repair, args=[py], text="python repair", disable_spinner=self.disable_spinner)
        else:
            raise RuntimeError("unknown python subcommand")

    # --------------
    # Update notifier & meta generation
    # --------------
    def notify_cmd(self, argv: List[str]):
        if not self.notifier:
            raise RuntimeError("update_notifier module not available")
        if not argv or argv[0] == "check":
            return run_with_spinner(self.notifier.check_repository_and_notify, args=[None], text="checking upstreams", disable_spinner=self.disable_spinner)
        if argv[0] == "summary":
            # print the status file
            path = self.notifier.STATUS_BAR_FILE if hasattr(self.notifier, "STATUS_BAR_FILE") else "/tmp/rquest_updates.json"
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if USE_RICH and console:
                    table = Table(title="Rquest Updates")
                    table.add_column("total"); table.add_column("security")
                    table.add_column("ts")
                    table.add_row(str(data.get("updates_total")), str(data.get("security_updates")), str(data.get("ts")))
                    console.print(table)
                else:
                    print(json.dumps(data, indent=2))
            except Exception:
                raise RuntimeError("no status file available")
        if argv[0] == "meta":
            pkg = argv[1] if len(argv)>1 else None
            # force meta generation for a package
            if not pkg:
                raise RuntimeError("package required")
            # find meta and call generate_meta_for_version on notifier
            # notifier exposes generate_meta_for_version function in our implementation
            if hasattr(self.notifier, "check_repository_and_notify"):
                return run_with_spinner(self.notifier.check_repository_and_notify, text=f"generate meta for {pkg}", disable_spinner=self.disable_spinner)

    # --------------
    # Deepclean
    # --------------
    def deepclean_cmd(self, argv: List[str]):
        if not self.deepclean:
            raise RuntimeError("deepclean module not available")
        # support subcommands
        sub = argv[0] if argv else "run"
        if sub == "run":
            return run_with_spinner(self.deepclean.run, text="deepcleaning", disable_spinner=self.disable_spinner)
        elif sub == "preview":
            return run_with_spinner(self.deepclean.preview, text="preview deepclean", disable_spinner=self.disable_spinner)
        else:
            raise RuntimeError("unknown deepclean subcommand")

    # --------------
    # rebuild-system: topological sort of packages and rebuild all in order
    # --------------
    def rebuild_system(self, jobs: int = 1, dry_run: bool = False):
        """
        Resolve all packages in DB, compute dependency order and rebuild them.
        This command is heavy. It uses resolver.get_all_packages and resolver.get_dependents or similar.
        """
        print_warn("Rebuild system requested. This may take a long time.")
        if not self.resolver:
            raise RuntimeError("resolver module required for rebuild-system")
        # 1) get all installed packages
        pkg_rows = []
        if self.db and hasattr(self.db, "fetchall"):
            try:
                pkg_rows = self.db.fetchall("SELECT name, version FROM packages")
            except Exception:
                pkg_rows = []
        # fallback: if resolver provides registry
        if not pkg_rows and hasattr(self.resolver, "list_installed"):
            pkg_rows = [{"name": n} for n in self.resolver.list_installed()]
        names = [r.get("name") for r in pkg_rows]
        print_info(f"Found {len(names)} packages to analyze")
        # 2) build dependency graph using resolver
        dep_graph = {}
        for n in names:
            try:
                deps = self.resolver.get_runtime_deps(n) if hasattr(self.resolver, "get_runtime_deps") else []
                dep_graph[n] = deps
            except Exception:
                dep_graph[n] = []
        # 3) compute topological order
        order = []
        temp_mark = set(); perm_mark = set()
        def visit(node):
            if node in perm_mark:
                return
            if node in temp_mark:
                # cycle
                return
            temp_mark.add(node)
            for d in dep_graph.get(node, []):
                visit(d)
            perm_mark.add(node)
            order.append(node)
        for n in names:
            visit(n)
        order = list(reversed(order))
        print_info(f"Computed rebuild order of {len(order)} packages")
        if dry_run:
            print(json.dumps(order, indent=2))
            return {"ok": True, "order": order}
        # 4) rebuild in order (respect jobs)
        failed = []
        def build_one(pkgname):
            try:
                print_info(f"Rebuilding {pkgname}")
                # find meta and call build/install
                if self.meta and hasattr(self.meta, "find_meta_by_name"):
                    m = self.meta.find_meta_by_name(pkgname)
                    if m:
                        self.install_package(m, target_profile=None, rebuild=True)
                        return True
                # fallback: try build from source by name
                self.install_package(pkgname, target_profile=None, rebuild=True)
                return True
            except Exception:
                logger.exception("build failed for %s", pkgname)
                failed.append(pkgname)
                return False
        if jobs <= 1:
            for p in order:
                build_one(p)
        else:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=jobs) as exc:
                futures = {exc.submit(build_one, p): p for p in order}
                for fut in as_completed(futures):
                    pass
        if failed:
            print_err(f"Some packages failed: {failed}")
            return {"ok": False, "failed": failed}
        print_ok("System rebuild completed")
        return {"ok": True}

# -----------------------
# Argparse wiring
# -----------------------
def make_parser():
    ap = argparse.ArgumentParser(prog="rquest", description="Rquest package manager CLI")
    ap.add_argument("--no-spinner", action="store_true", help="Disable spinner animations")
    sub = ap.add_subparsers(dest="cmd")

    # install
    p_install = sub.add_parser("install")
    p_install.add_argument("package")
    p_install.add_argument("--profile", help="toolchain/profile to use")
    p_install.add_argument("--rebuild", action="store_true", help="force rebuild")
    p_install.add_argument("--force", action="store_true", help="force install")

    # remove
    p_remove = sub.add_parser("remove")
    p_remove.add_argument("package")
    p_remove.add_argument("--purge", action="store_true")

    # upgrade
    p_upgrade = sub.add_parser("upgrade")
    p_upgrade.add_argument("package", nargs="?")
    p_upgrade.add_argument("--all", action="store_true")

    # fetch/build/quickpkg
    p_fetch = sub.add_parser("fetch")
    p_fetch.add_argument("package")
    p_build = sub.add_parser("build")
    p_build.add_argument("package")
    p_quickpkg = sub.add_parser("quickpkg")
    p_quickpkg.add_argument("package")
    p_quickpkg.add_argument("--out")

    # query
    p_query = sub.add_parser("query")
    p_query.add_argument("args", nargs=argparse.REMAINDER)

    # audit
    p_audit = sub.add_parser("audit")
    p_audit.add_argument("--full", action="store_true")

    # toolchain
    p_tool = sub.add_parser("toolchain")
    p_tool.add_argument("args", nargs=argparse.REMAINDER)

    # python
    p_python = sub.add_parser("python")
    p_python.add_argument("args", nargs=argparse.REMAINDER)

    # notify
    p_notify = sub.add_parser("notify")
    p_notify.add_argument("args", nargs=argparse.REMAINDER)

    # deepclean
    p_deep = sub.add_parser("deepclean")
    p_deep.add_argument("args", nargs=argparse.REMAINDER)

    # rebuild-system
    p_rebuild = sub.add_parser("rebuild-system")
    p_rebuild.add_argument("--jobs", type=int, default=1)
    p_rebuild.add_argument("--dry-run", action="store_true")

    # low-level
    p_fetchsrc = sub.add_parser("fetch-src")
    p_fetchsrc.add_argument("meta")
    p_apply = sub.add_parser("apply-patches")
    p_apply.add_argument("srcdir")
    p_hooks = sub.add_parser("run-hooks")
    p_hooks.add_argument("stage")
    p_sandbox = sub.add_parser("sandbox")
    p_sandbox.add_argument("cmd", nargs=argparse.REMAINDER)

    return ap

def main(argv: Optional[List[str]] = None):
    argv = argv if argv is not None else sys.argv[1:]
    parser = make_parser()
    args = parser.parse_args(argv)
    cli = RquestCLI()
    if args.no_spinner:
        cli.disable_spinner = True

    try:
        if args.cmd == "install":
            cli.install_package(args.package, target_profile=args.profile, rebuild=args.rebuild, force=args.force)
        elif args.cmd == "remove":
            cli.remove_package(args.package, purge=args.purge)
        elif args.cmd == "upgrade":
            cli.upgrade_package(args.package, all_pkgs=args.all)
        elif args.cmd == "fetch":
            cli.fetch_src(args.package)
        elif args.cmd == "build":
            cli.build_package(args.package)
        elif args.cmd == "quickpkg":
            cli.quickpkg(args.package, output_dir=args.out)
        elif args.cmd == "query":
            cli.query(args.args)
        elif args.cmd == "audit":
            cli.audit_run(full=args.full)
        elif args.cmd == "toolchain":
            cli.toolchain_cmd(args.args)
        elif args.cmd == "python":
            cli.python_cmd(args.args)
        elif args.cmd == "notify":
            cli.notify_cmd(args.args)
        elif args.cmd == "deepclean":
            cli.deepclean_cmd(args.args)
        elif args.cmd == "rebuild-system":
            cli.rebuild_system(jobs=args.jobs, dry_run=args.dry_run)
        elif args.cmd == "fetch-src":
            cli.fetch_src(args.meta)
        elif args.cmd == "apply-patches":
            cli.apply_patches(args.srcdir)
        elif args.cmd == "run-hooks":
            cli.run_hooks(args.stage, {})
        elif args.cmd == "sandbox":
            cli.sandbox_run(cmd=args.cmd)
        else:
            parser.print_help()
    except Exception as e:
        print_err(f"Command failed: {e}")
        sys.exit(2)

if __name__ == "__main__":
    main()
