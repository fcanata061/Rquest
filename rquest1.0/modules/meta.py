# rquest1.0/modules/meta.py
"""
meta.py — loader, validator e runtime helper para arquivos .meta (receitas)

Funcionalidades:
 - Parser: YAML / TOML / INI fallback
 - Inheritance (inherits) e includes
 - Template expansion (via ${VAR})
 - Variantes, perfis, condicionais nos campos
 - Hooks inline / scripts — registro e execução via sandbox / hooks manager
 - Cache de parsing em DB e memória
 - Integração com resolver, fetcher, buildsystem, pkgtool, sandbox
 - CLI para inspect / validate / fetch / resolve
"""

from __future__ import annotations
import os
import sys
import json
import re
import uuid
import time
import shutil
import tempfile
import hashlib
import subprocess
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

# optional parsers
try:
    import yaml  # type: ignore
    YAML_AVAILABLE = True
except Exception:
    YAML_AVAILABLE = False

try:
    import toml  # type: ignore
    TOML_AVAILABLE = True
except Exception:
    TOML_AVAILABLE = False

# integrations (graceful)
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("meta")
except Exception:
    logger = logging.getLogger("rquest.meta")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

try:
    from modules.db import get_db  # type: ignore
except Exception:
    def get_db():
        return None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager():
        return None

try:
    from modules.fetcher import get_fetcher  # type: ignore
except Exception:
    def get_fetcher():
        return None

try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    def get_resolver():
        return None

try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:
    def get_buildsystem():
        return None

try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager():
        return None

try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    def get_pkgtool():
        return None

# ML optional
_ML_AVAILABLE = False
try:
    import sklearn  # type: ignore
    _ML_AVAILABLE = True
except Exception:
    _ML_AVAILABLE = False

# -----------------------
# Utils
# -----------------------
def _now() -> int:
    return int(time.time())

def _sha256_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None

def _ensure_dir(d: str):
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass

TEMPLATE_RE = re.compile(r"\$\{([A-Za-z0-9_]+)\}")

def _expand_template(s: str, ctx: Dict[str, str]) -> str:
    def repl(m):
        k = m.group(1)
        return ctx.get(k, m.group(0))
    return TEMPLATE_RE.sub(repl, s)

def _deep_merge(a: Dict[str,Any], b: Dict[str,Any]) -> Dict[str,Any]:
    res = dict(a)
    for k, v in b.items():
        if k in res and isinstance(res[k], dict) and isinstance(v, dict):
            res[k] = _deep_merge(res[k], v)
        elif k in res and isinstance(res[k], list) and isinstance(v, list):
            combined = list(res[k])
            for x in v:
                if x not in combined:
                    combined.append(x)
            res[k] = combined
        else:
            res[k] = v
    return res

def _try_cast_value(v: str) -> Any:
    v = v.strip()
    lv = v.lower()
    if lv in ("true","yes","on"): return True
    if lv in ("false","no","off"): return False
    try:
        if "." in v:
            return float(v)
        else:
            return int(v)
    except Exception:
        pass
    if "," in v:
        return [x.strip() for x in v.split(",") if x.strip()]
    return v

# -----------------------
# Config defaults
# -----------------------
CFG = get_config() if callable(get_config) else {}
META_CFG = CFG.get("meta", {}) if isinstance(CFG, dict) else {}
DEFAULTS = {
    "meta_cache_dir": os.path.expanduser("~/.rquest/meta_cache"),
    "allow_unchecked_sources": False,
    "hooks_dir_name": "hooks",
    "inherit_search_paths": ["./", "/usr/share/rquest/meta", "~/.rquest/overlays"],
    "default_variant": None,
}
META_CACHE_DIR = os.path.abspath(META_CFG.get("meta_cache_dir", DEFAULTS["meta_cache_dir"]))
_allow_unchecked = bool(META_CFG.get("allow_unchecked_sources", DEFAULTS["allow_unchecked_sources"]))
HOOKS_DIR_NAME = META_CFG.get("hooks_dir_name", DEFAULTS["hooks_dir_name"])
INHERIT_PATHS = [os.path.expanduser(p) for p in META_CFG.get("inherit_search_paths", DEFAULTS["inherit_search_paths"])]
_default_variant = META_CFG.get("default_variant", DEFAULTS["default_variant"])
_ensure_dir(META_CACHE_DIR)

# -----------------------
# Data structures
# -----------------------
class MetaError(Exception):
    pass

class MetaPackage:
    def __init__(self, source_path: str, data: Dict[str,Any], overlays: Optional[List[str]] = None):
        self.source_path = os.path.abspath(source_path)
        self.raw = data.copy()
        self.overlays = overlays or []
        # canonical fields
        self.name: str = data.get("name") or data.get("pkgname") or ""
        self.version: str = str(data.get("version") or data.get("ver") or "")
        self.description: str = data.get("description") or ""
        self.category: str = data.get("category") or ""
        self.slot: str = str(data.get("slot") or data.get("slot", "0"))
        # source list
        sr = data.get("source") or data.get("sources") or []
        self.sources: List[Dict[str,Any]] = sr if isinstance(sr, list) else ([sr] if sr else [])
        # dependencies
        self.build_depends: List[Dict[str,Any]] = data.get("build_depends") or data.get("build-depends") or []
        self.runtime_depends: List[Dict[str,Any]] = data.get("runtime_depends") or data.get("runtime-depends") or []
        self.test_depends: List[Dict[str,Any]] = data.get("test_depends") or data.get("test-depends") or []
        # use / variants / profiles
        self.use_flags: Dict[str,Any] = data.get("use_flags") or data.get("useflags") or {}
        self.enabled_flags: List[str] = data.get("enabled", []) or data.get("use", []) or []
        self.variants: Dict[str,Any] = data.get("variants") or {}
        self.profiles: Dict[str,Any] = data.get("profiles") or {}
        # hooks / patches
        self.hooks: Dict[str,Any] = data.get("hooks") or {}
        self.patches: List[Any] = data.get("patches") or []
        # includes / inherits
        inh = data.get("inherits") or []
        self.inherits: List[str] = inh if isinstance(inh, list) else ([inh] if inh else [])
        self.includes: List[str] = data.get("includes") or []
        # security / container / tests
        self.security: Dict[str,Any] = data.get("security") or {}
        self.container: Dict[str,Any] = data.get("container") or {}
        self.tests: List[Any] = data.get("tests") or []
        # context & computed
        self.ctx: Dict[str,str] = {}
        self._computed_sources: Optional[List[Dict[str,Any]]] = None
        self.id: str = f"{self.name}-{self.version}-{uuid.uuid4().hex[:8]}"

    def build_context(self, variant: Optional[str] = None, extra: Optional[Dict[str,str]] = None) -> Dict[str,str]:
        ctx = {
            "NAME": self.name,
            "VERSION": self.version,
            "SLOT": self.slot,
            "VARIANT": variant or _default_variant or "",
        }
        if extra:
            for k,v in extra.items():
                ctx[str(k)] = str(v)
        self.ctx = ctx
        return ctx

    def resolve_sources(self, variant: Optional[str] = None, extra_ctx: Optional[Dict[str,str]] = None) -> List[Dict[str,Any]]:
        if self._computed_sources is not None:
            return self._computed_sources
        ctx = self.build_context(variant=variant, extra=extra_ctx)
        out: List[Dict[str,Any]] = []
        for s in self.sources:
            if isinstance(s, str):
                out.append({"url": _expand_template(s, ctx)})
            elif isinstance(s, dict):
                d2 = {}
                for k, v in s.items():
                    if isinstance(v, str):
                        d2[k] = _expand_template(v, ctx)
                    else:
                        d2[k] = v
                out.append(d2)
        self._computed_sources = out
        return out

    def merge_parent(self, parent: MetaPackage):
        # list merging
        def ml(a, b):
            res = list(a) if a else []
            if b:
                for x in b:
                    if x not in res:
                        res.append(x)
            return res
        self.sources = ml(self.sources, parent.sources)
        self.patches = ml(self.patches, parent.patches)
        self.build_depends = ml(self.build_depends, parent.build_depends)
        self.runtime_depends = ml(self.runtime_depends, parent.runtime_depends)
        self.test_depends = ml(self.test_depends, parent.test_depends)
        # dict merging
        self.use_flags = {**parent.use_flags, **self.use_flags}
        self.variants = {**parent.variants, **self.variants}
        self.profiles = {**parent.profiles, **self.profiles}
        # hooks inherit missing
        ph = parent.hooks or {}
        for k,v in ph.items():
            if k not in self.hooks:
                self.hooks[k] = v
        # scalar fallback
        if not self.description:
            self.description = parent.description
        if not self.category:
            self.category = parent.category
        if not self.slot:
            self.slot = parent.slot
        # security / container
        self.security = {**parent.security, **self.security}
        self.container = {**parent.container, **self.container}

    def to_dict(self) -> Dict[str,Any]:
        return {
            "name": self.name,
            "version": self.version,
            "slot": self.slot,
            "description": self.description,
            "category": self.category,
            "sources": self.sources,
            "build_depends": self.build_depends,
            "runtime_depends": self.runtime_depends,
            "enabled_flags": self.enabled_flags,
            "use_flags": self.use_flags,
            "variants": self.variants,
            "profiles": self.profiles,
            "hooks": self.hooks,
            "patches": self.patches,
            "security": self.security,
            "container": self.container,
            "tests": self.tests,
            "meta": self.raw.get("meta", {}),
            "source_path": self.source_path,
        }

    def as_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)

class MetaLoader:
    def __init__(self):
        dbc = get_db()
        self.db = dbc() if callable(dbc) else None
        hm = get_hook_manager()
        self.hooks = hm() if callable(hm) else None
        fc = get_fetcher()
        self.fetcher = fc() if callable(fc) else None
        rc = get_resolver()
        self.resolver = rc() if callable(rc) else None
        b = get_buildsystem()
        self.buildsystem = b() if callable(b) else None
        sm = get_sandbox_manager()
        self.sandbox = sm() if callable(sm) else None
        pc = get_pkgtool()
        self.pkgtool = pc() if callable(pc) else None
        self.parsed_cache: Dict[str, Tuple[int, MetaPackage]] = {}  # path → (mtime, MetaPackage)

    def _parse_file(self, path: str) -> Dict[str,Any]:
        text = _read_text(path)
        if text is None:
            raise MetaError(f"Cannot read meta file: {path}")
        if YAML_AVAILABLE:
            try:
                d = yaml.safe_load(text)
                if isinstance(d, dict):
                    return d
            except Exception:
                logger.debug("YAML parse failed on %s", path)
        if TOML_AVAILABLE:
            try:
                d = toml.loads(text)
                if isinstance(d, dict):
                    return d
            except Exception:
                logger.debug("TOML parse failed on %s", path)
        # fallback INI-like
        d: Dict[str,Any] = {}
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            if ":" in ln:
                k, v = ln.split(":", 1)
                d[k.strip()] = _try_cast_value(v.strip())
            elif "=" in ln:
                k, v = ln.split("=", 1)
                d[k.strip()] = _try_cast_value(v.strip())
        return d

    def load(self, path: str, variant: Optional[str] = None, profile: Optional[str] = None, expand_includes: bool = True) -> MetaPackage:
        path = os.path.abspath(os.path.expanduser(path))
        try:
            st = os.stat(path)
        except Exception:
            raise MetaError(f"Meta file not found: {path}")
        mtime = int(st.st_mtime)
        cached = self.parsed_cache.get(path)
        if cached and cached[0] == mtime:
            return cached[1]
        # also check DB cache
        if self.db:
            try:
                # table meta_cache exists?
                r = self.db.fetchone("SELECT mtime, json FROM meta_cache WHERE path = ?", (path,))
                if r and r.get("mtime") == mtime:
                    pkg = MetaPackage(path, json.loads(r.get("json")), overlays=None)
                    self.parsed_cache[path] = (mtime, pkg)
                    return pkg
            except Exception:
                pass
        # parse fresh
        data = self._parse_file(path)
        pkg = MetaPackage(path, data, overlays=None)
        # includes
        if expand_includes and pkg.includes:
            for inc in pkg.includes:
                incp = self._find_include(inc, base_dir=os.path.dirname(path))
                if incp:
                    try:
                        idata = self._parse_file(incp)
                        merged = _deep_merge(idata, pkg.raw)
                        pkg.raw = merged
                    except Exception:
                        logger.exception("include %s failed for %s", inc, path)
        # inheritance
        for parent in pkg.inherits:
            pp = self._find_include(parent, base_dir=os.path.dirname(path))
            if pp:
                try:
                    par = self.load(pp, variant=variant, profile=profile, expand_includes=expand_includes)
                    pkg.merge_parent(par)
                except Exception:
                    logger.exception("inheritance %s -> %s failed", pp, path)
        # apply variant/profile overrides
        pkg.build_context(variant=variant, extra={"PROFILE": profile or ""})
        if variant and variant in pkg.variants:
            pkg.raw = _deep_merge(pkg.raw, pkg.variants.get(variant) or {})
            pkg = MetaPackage(path, pkg.raw, overlays=None)
        if profile and profile in pkg.profiles:
            pkg.raw = _deep_merge(pkg.raw, pkg.profiles.get(profile) or {})
            pkg = MetaPackage(path, pkg.raw, overlays=None)
        # cache in memory
        self.parsed_cache[path] = (mtime, pkg)
        # persist to DB cache
        if self.db:
            try:
                self.db.execute("""CREATE TABLE IF NOT EXISTS meta_cache (path TEXT PRIMARY KEY, mtime INTEGER, json TEXT)""", (), commit=True)
                j = json.dumps(pkg.to_dict(), ensure_ascii=False)
                self.db.execute("INSERT OR REPLACE INTO meta_cache (path, mtime, json) VALUES (?, ?, ?)", (path, mtime, j), commit=True)
            except Exception:
                logger.exception("meta cache write failed")
        return pkg

    def _find_include(self, ref: str, base_dir: Optional[str] = None) -> Optional[str]:
        # absolute or user path
        rp = os.path.expanduser(ref)
        if os.path.isabs(rp) and os.path.exists(rp):
            return rp
        if base_dir:
            cand = os.path.join(base_dir, ref)
            if os.path.exists(cand):
                return os.path.abspath(cand)
        for p in INHERIT_PATHS:
            cand = os.path.join(os.path.expanduser(p), ref)
            if os.path.exists(cand):
                return os.path.abspath(cand)
        return None

    def validate(self, pkg: MetaPackage, strict: bool = True) -> Tuple[bool, List[str]]:
        errs: List[str] = []
        if not pkg.name:
            errs.append("missing name")
        if not pkg.version:
            errs.append("missing version")
        if not pkg.sources and not _allow_unchecked:
            errs.append("no sources declared")
        if not isinstance(pkg.use_flags, dict):
            errs.append("use_flags must be dict")
        for kind in ("build_depends", "runtime_depends", "test_depends"):
            val = getattr(pkg, kind)
            if val and not isinstance(val, list):
                errs.append(f"{kind} must be list")
        ok = len(errs) == 0
        if strict and not ok:
            raise MetaError("validation failed: " + "; ".join(errs))
        return ok, errs

    def expand_hooks(self, pkg: MetaPackage, package_dir: Optional[str] = None) -> Dict[str,Any]:
        normalized: Dict[str,Any] = {}
        hdir = (package_dir and os.path.join(package_dir, HOOKS_DIR_NAME))
        for stage, h in (pkg.hooks or {}).items():
            if isinstance(h, str):
                # inline or file path
                if os.path.exists(h):
                    normalized[stage] = {"type":"file", "path": os.path.abspath(h)}
                else:
                    normalized[stage] = {"type":"inline", "script": h}
            elif isinstance(h, dict):
                if "file" in h and os.path.exists(h["file"]):
                    normalized[stage] = {"type":"file", "path": os.path.abspath(h["file"])}
                elif "inline" in h:
                    normalized[stage] = {"type":"inline", "script": h["inline"]}
                else:
                    # fallback: treat keys as other data
                    for k, v in h.items():
                        if k in ("file","path") and isinstance(v, str) and os.path.exists(v):
                            normalized[stage] = {"type":"file","path": os.path.abspath(v)}
                            break
                    else:
                        if "inline" in h:
                            normalized[stage] = {"type":"inline", "script": h["inline"]}
            elif isinstance(h, list):
                items = []
                for it in h:
                    if isinstance(it, str):
                        if os.path.exists(it):
                            items.append({"type":"file","path": os.path.abspath(it)})
                        else:
                            items.append({"type":"inline","script": it})
                    elif isinstance(it, dict):
                        if "file" in it and os.path.exists(it["file"]):
                            items.append({"type":"file","path": os.path.abspath(it["file"])})
                        elif "inline" in it:
                            items.append({"type":"inline","script": it["inline"]})
                normalized[stage] = {"type":"list", "items": items}
            # resolve relative file inside hooks dir
            if normalized.get(stage, {}).get("type") == "file":
                p = normalized[stage]["path"]
                if not os.path.isabs(p) and package_dir:
                    cand = os.path.join(package_dir, p)
                    if os.path.exists(cand):
                        normalized[stage]["path"] = os.path.abspath(cand)
        # register in hook manager
        if self.hooks:
            for stage, desc in normalized.items():
                try:
                    self.hooks.register(stage, desc)
                except Exception:
                    logger.exception("Failed to register hook %s", stage)
        return normalized

    def run_hook(self, hook_desc: Dict[str,Any], context: Optional[Dict[str,Any]] = None, timeout: Optional[int] = None) -> Dict[str,Any]:
        context = context or {}
        sandbox = self.sandbox
        if not hook_desc:
            return {"ok": True, "note": "no hook"}
        wd = tempfile.mkdtemp(prefix="meta-hook-")
        env = os.environ.copy()
        env["_META_HOOK_CONTEXT"] = json.dumps(context)
        exec_path = None
        if hook_desc.get("type") == "inline" and "script" in hook_desc:
            sp = os.path.join(wd, "inline-hook.sh")
            with open(sp, "w", encoding="utf-8") as f:
                f.write("#!/bin/sh\n")
                f.write(hook_desc["script"] + "\n")
            os.chmod(sp, 0o755)
            exec_path = sp
        elif hook_desc.get("type") == "file" and "path" in hook_desc:
            if os.path.exists(hook_desc["path"]):
                exec_path = hook_desc["path"]
        if not exec_path:
            shutil.rmtree(wd, ignore_errors=True)
            return {"ok": False, "error": "no executable hook"}
        try:
            if sandbox:
                try:
                    return sandbox.run_in_sandbox([exec_path], cwd=wd, env=env, timeout=timeout, exec_type="hook")
                except Exception:
                    logger.exception("sandbox hook execution failed; fallback to direct")
            proc = subprocess.run([exec_path], cwd=wd, env=env, timeout=timeout, capture_output=True, text=True)
            return {"ok": proc.returncode == 0, "exit_code": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
        except Exception as e:
            logger.exception("hook exec error: %s", e)
            return {"ok": False, "error": str(e)}
        finally:
            shutil.rmtree(wd, ignore_errors=True)

    def resolve_dependencies(self, pkg: MetaPackage, flags: Optional[List[str]] = None, arch: Optional[str] = None) -> Dict[str,Any]:
        deps = pkg.build_depends or []
        rts = pkg.runtime_depends or []
        tds = pkg.test_depends or []
        # apply conditional filters
        cond = self.evaluate_conditional_deps(pkg, flags=flags, arch=arch)
        deps2 = cond.get("build_depends", [])
        rts2 = cond.get("runtime_depends", [])
        tds2 = cond.get("test_depends", [])
        if self.resolver and hasattr(self.resolver, "resolve_package_deps"):
            try:
                return self.resolver.resolve_package_deps(pkg.name, pkg.version, {
                    "build": deps2,
                    "runtime": rts2,
                    "test": tds2,
                })
            except Exception:
                logger.exception("resolver failed")
        return {"ok": True, "deps": {"build": deps2, "runtime": rts2, "test": tds2}}

    def evaluate_conditional_deps(self, pkg: MetaPackage, flags: Optional[List[str]] = None, arch: Optional[str] = None) -> Dict[str,List[str]]:
        flags = flags or pkg.enabled_flags or []
        arch = arch or os.uname().machine
        def cond_ok(expr: Optional[str]) -> bool:
            if not expr:
                return True
            parts = re.split(r"\s*(?:&&|and)\s*", expr.strip())
            for p in parts:
                p = p.strip()
                if p.startswith("use:"):
                    f = p.split(":",1)[1]
                    if f not in flags:
                        return False
                elif p.startswith("arch:"):
                    a = p.split(":",1)[1]
                    if a != arch:
                        return False
                else:
                    return False
            return True
        def expand(lst: List[Any]) -> List[str]:
            res: List[str] = []
            for it in lst:
                if isinstance(it, str):
                    res.append(it)
                elif isinstance(it, dict):
                    cond = it.get("if") or it.get("condition")
                    if cond_ok(cond):
                        nm = it.get("name") or it.get("pkg") or next(iter(it.values()), None)
                        if isinstance(nm, str):
                            res.append(nm)
            return res
        return {
            "build_depends": expand(pkg.build_depends or []),
            "runtime_depends": expand(pkg.runtime_depends or []),
            "test_depends": expand(pkg.test_depends or []),
        }

# CLI
def inspect_meta(path: str, variant: Optional[str] = None, profile: Optional[str] = None, verbose: bool = False):
    ml = MetaLoader()
    pkg = ml.load(path, variant=variant, profile=profile)
    if verbose:
        print(pkg.as_json())
    else:
        d = {"name": pkg.name, "version": pkg.version, "sources": pkg.resolve_sources(variant=variant)}
        print(json.dumps(d, indent=2, ensure_ascii=False))

def validate_meta(path: str, strict: bool = True):
    ml = MetaLoader()
    pkg = ml.load(path)
    ok, errs = ml.validate(pkg, strict=strict)
    if ok:
        print("OK")
        return 0
    else:
        print("Errors:")
        for e in errs:
            print(" -", e)
        return 1

def fetch_meta_sources(path: str, variant: Optional[str] = None, out: Optional[str] = None) -> List[str]:
    ml = MetaLoader()
    pkg = ml.load(path, variant=variant)
    return ml.fetch_sources(pkg, variant=variant, destdir=out)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-meta")
    ap.add_argument("meta", help=".meta file path")
    ap.add_argument("--inspect", action="store_true")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--variant", type=str)
    ap.add_argument("--profile", type=str)
    ap.add_argument("--out", type=str)
    args = ap.parse_args()
    if args.inspect:
        inspect_meta(args.meta, variant=args.variant, profile=args.profile, verbose=True)
    elif args.validate:
        rc = validate_meta(args.meta, strict=True)
        sys.exit(rc)
    elif args.fetch:
        paths = fetch_meta_sources(args.meta, variant=args.variant, out=args.out)
        print("Fetched:", paths)
    else:
        ap.print_help()
