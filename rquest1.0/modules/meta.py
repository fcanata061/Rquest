# Rquest/rquest1.0/modules/meta.py
"""
meta.py - loader, validator, and runtime helper for .meta recipe files

Features:
- Parse .meta in YAML/TOML/INI-like formats (YAML preferred if PyYAML present)
- Support multiple sources: http(s), ftp, git, ipfs, torrent (fetcher integration)
- Template expansion in source URLs (NAME, VERSION, SLOT, VARIANT)
- Inheritance (inherits: parent.meta) and includes (snippets)
- Variants / profiles / build_matrix / overlays
- Conditional dependencies (if: "use:gtk" or "arch:x86_64")
- Hooks: inline (script text) and external script files in package dir
- Hook registration with hooks.py and execution in sandbox if available
- Integration with resolver (dependency checking), pkgtool (cache/quickpkg), fetcher (downloading)
- Security policies (sandbox enforcement hints)
- Container test hints
- Caching of parsed metadata in DB (installed meta cache)
- ML prioritizer skeleton to suggest flags/variants
- CLI for inspect/validate/resolve-sources/expand-hooks
"""

from __future__ import annotations

import os
import sys
import json
import re
import uuid
import time
import shutil
import hashlib
import tempfile
import subprocess
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

# Optional parsers
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

# Integration points with project modules
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config(): return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("meta")
except Exception:
    logger = logging.getLogger("meta")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# DB for caching meta parsing results and storing metadata (optional)
try:
    from modules.db import get_db  # type: ignore
except Exception:
    def get_db(): return None

# hooks manager (register hooks extracted from .meta)
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

# fetcher, resolver, buildsystem, sandbox, pkgtool integration (optional)
try:
    from modules.fetcher import get_fetcher  # type: ignore
except Exception:
    def get_fetcher(): return None

try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    def get_resolver(): return None

try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:
    def get_buildsystem(): return None

try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager(): return None

try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    def get_pkgtool(): return None

# ML optional
_ML_AVAILABLE = False
try:
    import sklearn  # type: ignore
    _ML_AVAILABLE = True
except Exception:
    _ML_AVAILABLE = False

# -----------------------
# Utilities
# -----------------------
def _now_ts() -> int:
    return int(time.time())

def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

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

def _ensure_dir(p: str):
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass

# Basic safe template substitution for ${VAR}
TEMPLATE_RE = re.compile(r"\$\{([A-Za-z0-9_]+)\}")

def _expand_template(s: str, ctx: Dict[str,str]) -> str:
    def repl(m):
        k = m.group(1)
        return ctx.get(k, m.group(0))
    return TEMPLATE_RE.sub(repl, s)

# Normalize path
def _norm(p: str) -> str:
    return os.path.normpath(os.path.expanduser(p))

# -----------------------
# Config defaults
# -----------------------
CFG = get_config() if callable(get_config) else {}
META_CFG = CFG.get("meta", {}) if isinstance(CFG, dict) else {}
DEFAULTS = {
    "meta_cache_dir": os.path.expanduser("~/.rquest/meta_cache"),
    "allow_unchecked_sources": False,
    "hooks_dir_name": "hooks",  # relative inside package dir
    "inherit_search_paths": ["./", "/usr/share/rquest/meta", "~/.rquest/overlays"],
    "default_variant": None,
}
META_CACHE_DIR = _norm(META_CFG.get("meta_cache_dir", DEFAULTS["meta_cache_dir"]))
_allow_unchecked = bool(META_CFG.get("allow_unchecked_sources", DEFAULTS["allow_unchecked_sources"]))
HOOKS_DIR_NAME = META_CFG.get("hooks_dir_name", DEFAULTS["hooks_dir_name"])
INHERIT_PATHS = [os.path.expanduser(p) for p in META_CFG.get("inherit_search_paths", DEFAULTS["inherit_search_paths"])]
_default_variant = META_CFG.get("default_variant", DEFAULTS["default_variant"])
_ensure_dir(META_CACHE_DIR)

# -----------------------
# Data models
# -----------------------
class MetaError(Exception):
    pass

class MetaPackage:
    """
    High-level representation of a parsed .meta
    """
    def __init__(self, source_path: str, data: Dict[str,Any], overlays: Optional[List[str]] = None):
        self.source_path = os.path.abspath(source_path)
        self.raw = data  # original parsed dict
        self.overlays = overlays or []
        # canonical fields with defaults
        self.name: str = data.get("name") or data.get("pkgname") or ""
        self.version: str = str(data.get("version") or data.get("ver") or "")
        self.description: str = data.get("description") or ""
        self.category: str = data.get("category") or data.get("section") or ""
        self.slot: str = data.get("slot") or data.get("slot", "0")
        # sources: list of dicts {url/git/ipfs/torrent, checksum, attrs...}
        self.sources: List[Dict[str,Any]] = data.get("source") or data.get("sources") or []
        if isinstance(self.sources, dict):
            self.sources = [self.sources]
        # dependencies separated
        self.build_depends: List[Dict[str,Any]] = data.get("build_depends") or data.get("build-depends") or []
        self.runtime_depends: List[Dict[str,Any]] = data.get("runtime_depends") or data.get("runtime-depends") or []
        self.test_depends: List[Dict[str,Any]] = data.get("test_depends") or data.get("test-depends") or []
        # use flags and variants
        self.use_flags: Dict[str,Any] = data.get("use_flags") or data.get("useflags") or {}
        self.enabled_flags: List[str] = data.get("enabled", []) or data.get("use", []) or []
        # variants and profiles
        self.variants: Dict[str, Dict[str,Any]] = data.get("variants") or {}
        self.profiles: Dict[str, Dict[str,Any]] = data.get("profiles") or {}
        self.default_variant = data.get("default_variant") or _default_variant
        # build matrix
        self.build_matrix: Dict[str, List[Any]] = data.get("build_matrix") or {}
        # hooks: dict of stage -> string (inline) or path
        self.hooks: Dict[str,Any] = data.get("hooks") or {}
        # patches list
        self.patches: List[str] = data.get("patches") or []
        # includes/inherits
        self.inherits: List[str] = data.get("inherits") or ([] if not data.get("inherits") else ([data.get("inherits")] if isinstance(data.get("inherits"), str) else data.get("inherits")))
        self.includes: List[str] = data.get("includes") or []
        # security/policy
        self.security: Dict[str,Any] = data.get("security") or {}
        # container hints
        self.container: Dict[str,Any] = data.get("container") or {}
        # tests
        self.tests: List[Dict[str,Any]] = data.get("tests") or []
        # arbitrary metadata
        self.meta: Dict[str,Any] = data.get("meta") or {}
        # internal computed fields
        self.ctx: Dict[str,str] = {}  # context variables for template expansion
        self._computed_sources: Optional[List[Dict[str,Any]]] = None
        # id
        self.id = f"{self.name}-{self.version}-{uuid.uuid4().hex[:8]}"

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
            "use_flags": self.use_flags,
            "enabled_flags": self.enabled_flags,
            "variants": self.variants,
            "profiles": self.profiles,
            "hooks": self.hooks,
            "security": self.security,
            "container": self.container,
            "tests": self.tests,
            "meta": self.meta,
            "source_path": self.source_path
        }

    def as_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)

    # compute template context
    def build_context(self, variant: Optional[str] = None, extra: Optional[Dict[str,str]] = None):
        self.ctx = {
            "NAME": self.name,
            "VERSION": self.version,
            "SLOT": self.slot,
            "VARIANT": variant or self.default_variant or ""
        }
        if extra:
            for k,v in extra.items():
                self.ctx[str(k)] = str(v)
        return self.ctx

    # expand and normalize sources (apply templates, fill defaults)
    def resolve_sources(self, variant: Optional[str] = None, extra_ctx: Optional[Dict[str,str]] = None) -> List[Dict[str,Any]]:
        if self._computed_sources is not None:
            return self._computed_sources
        ctx = self.build_context(variant=variant, extra=extra_ctx)
        res = []
        for s in self.sources:
            # each s is dict possibly with keys: url/git/ipfs/torrent, checksum, mirror, depth, branch, commit
            if isinstance(s, str):
                # plain URL string
                sdict = {"url": _expand_template(s, ctx)}
            else:
                sdict = dict(s)  # shallow copy
                # expand templated strings inside sdict values
                for k,v in list(sdict.items()):
                    if isinstance(v, str):
                        sdict[k] = _expand_template(v, ctx)
            res.append(sdict)
        self._computed_sources = res
        return res

    # merge parent metadata into this package (for inheritance)
    def merge_parent(self, parent: "MetaPackage"):
        # shallow merge policies:
        # - fields that are lists: extend if not present
        # - dicts: update
        # - scalar fields: overridden by child if set; else inherit
        def merge_list(a, b):
            if not a:
                return list(b) if b else []
            res = list(a)
            if b:
                for item in b:
                    if item not in res:
                        res.append(item)
            return res
        # lists
        self.sources = merge_list(self.sources, parent.sources)
        self.patches = merge_list(self.patches, parent.patches)
        self.build_depends = merge_list(self.build_depends, parent.build_depends)
        self.runtime_depends = merge_list(self.runtime_depends, parent.runtime_depends)
        self.test_depends = merge_list(self.test_depends, parent.test_depends)
        # dicts
        self.use_flags = {**parent.use_flags, **self.use_flags}
        self.variants = {**parent.variants, **self.variants}
        self.profiles = {**parent.profiles, **self.profiles}
        # hooks: parent hooks not present should be inherited
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
        # security
        self.security = {**parent.security, **self.security}
        # container
        self.container = {**parent.container, **self.container}

# -----------------------
# MetaLoader
# -----------------------
class MetaLoader:
    """
    Responsible for loading .meta files, resolving inheritance/includes, validating content,
    registering hooks and interacting with fetcher/resolver as needed.
    """
    def __init__(self):
        self.db = get_db()() if callable(get_db) else None  # if db returns callable wrapper
        self.hooks = get_hook_manager()() if callable(get_hook_manager) else None
        self.fetcher = get_fetcher()() if callable(get_fetcher) else None
        self.resolver = get_resolver()() if callable(get_resolver) else None
        self.buildsystem = get_buildsystem()() if callable(get_buildsystem) else None
        self.sandbox = get_sandbox_manager()() if callable(get_sandbox_manager) else None
        self.pkgtool = get_pkgtool()() if callable(get_pkgtool) else None
        self.cache_dir = META_CACHE_DIR
        self.parsed_cache: Dict[str, MetaPackage] = {}

    # -----------------------
    # file parsing helpers
    # -----------------------
    def _parse_file(self, path: str) -> Dict[str,Any]:
        text = _read_text(path)
        if text is None:
            raise MetaError(f"Cannot read .meta file: {path}")
        # Try YAML first
        if YAML_AVAILABLE:
            try:
                data = yaml.safe_load(text)
                if isinstance(data, dict):
                    return data
            except Exception:
                logger.debug("YAML parse failed for %s, trying TOML/INI", path)
        # Try TOML
        if TOML_AVAILABLE:
            try:
                data = toml.loads(text)
                if isinstance(data, dict):
                    return data
            except Exception:
                logger.debug("TOML parse failed for %s, trying INI", path)
        # Fallback: simple INI-like or key: value pairs -> convert
        data = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                k,v = line.split(":",1)
                data[k.strip()] = _try_cast_value(v.strip())
            elif "=" in line:
                k,v = line.split("=",1)
                data[k.strip()] = _try_cast_value(v.strip())
            else:
                # unstructured: skip
                continue
        return data

    # -----------------------
    # load meta (resolves inheritance/includes)
    # -----------------------
    def load(self, path: str, overlays: Optional[List[str]] = None, variant: Optional[str] = None, profile: Optional[str] = None, expand_includes: bool = True) -> MetaPackage:
        path = os.path.abspath(os.path.expanduser(path))
        # caching by absolute path and mtimestamp
        cache_key = path
        stat = None
        try:
            stat = os.stat(path)
        except Exception:
            raise MetaError(f"Meta file not found: {path}")
        mtime = int(stat.st_mtime)
        # quick cache check in memory
        if cache_key in self.parsed_cache:
            return self.parsed_cache[cache_key]
        # parse
        data = self._parse_file(path) or {}
        pkg = MetaPackage(path, data, overlays=overlays)
        # resolve includes first
        if expand_includes and pkg.includes:
            for inc in pkg.includes:
                inc_path = self._find_include(inc, base_dir=os.path.dirname(path))
                if inc_path:
                    try:
                        inc_data = self._parse_file(inc_path)
                        # merge includes: include fields not present override? we'll merge simply by updating raw dict
                        data = _deep_merge(inc_data, data)
                        pkg.raw = data
                    except Exception:
                        logger.exception("Failed loading include %s for %s", inc, path)
        # inheritance
        parents = pkg.inherits or []
        for parent_ref in parents:
            parent_path = self._find_include(parent_ref, base_dir=os.path.dirname(path))
            if parent_path:
                try:
                    parent_pkg = self.load(parent_path, overlays=overlays, variant=variant, profile=profile, expand_includes=expand_includes)
                    pkg.merge_parent(parent_pkg)
                except Exception:
                    logger.exception("Failed merging parent %s into %s", parent_ref, path)
            else:
                logger.warning("Parent %s not found for %s", parent_ref, path)
        # finalize defaults and compute templates
        pkg.build_context(variant=variant, extra={"PROFILE": profile or ""})
        # apply variant/profile overrides if requested
        if variant and variant in pkg.variants:
            vdata = pkg.variants.get(variant) or {}
            # merge variant into pkg.raw and refresh fields
            pkg.raw = _deep_merge(pkg.raw, vdata)
            # rebuild fields
            pkg = MetaPackage(path, pkg.raw, overlays=overlays)
            pkg.build_context(variant=variant, extra={"PROFILE": profile or ""})
        if profile and profile in pkg.profiles:
            pdat = pkg.profiles.get(profile) or {}
            pkg.raw = _deep_merge(pkg.raw, pdat)
            pkg = MetaPackage(path, pkg.raw, overlays=overlays)
            pkg.build_context(variant=variant, extra={"PROFILE": profile or ""})
        # cache in memory
        self.parsed_cache[cache_key] = pkg
        # persist cache into DB for speed (optional)
        try:
            if self.db:
                try:
                    self.db.execute("""
                        CREATE TABLE IF NOT EXISTS meta_cache (
                            path TEXT PRIMARY KEY,
                            mtime INTEGER,
                            json TEXT
                        );""", (), commit=True)
                    j = json.dumps(pkg.to_dict(), ensure_ascii=False)
                    self.db.execute("INSERT OR REPLACE INTO meta_cache (path, mtime, json) VALUES (?, ?, ?)", (path, mtime, j), commit=True)
                except Exception:
                    logger.exception("meta cache DB write failed")
        except Exception:
            pass
        return pkg

    # -----------------------
    # find includes/parents by searching inherit paths and base dir
    # -----------------------
    def _find_include(self, ref: str, base_dir: Optional[str] = None) -> Optional[str]:
        # if ref is absolute or relative path
        refp = os.path.expanduser(ref)
        if os.path.isabs(refp) and os.path.exists(refp):
            return refp
        # try relative to base_dir
        if base_dir:
            candidate = os.path.join(base_dir, ref)
            if os.path.exists(candidate):
                return os.path.abspath(candidate)
        # search in INHERIT_PATHS
        for p in INHERIT_PATHS:
            candidate = os.path.join(os.path.expanduser(p), ref)
            if os.path.exists(candidate):
                return os.path.abspath(candidate)
        # not found
        return None

    # -----------------------
    # validate package metadata (semantic checks)
    # -----------------------
    def validate(self, pkg: MetaPackage, strict: bool = True) -> Tuple[bool, List[str]]:
        errors: List[str] = []
        if not pkg.name:
            errors.append("missing name")
        if not pkg.version:
            errors.append("missing version")
        # sources must be present unless allowed
        if not pkg.sources and not _allow_unchecked:
            errors.append("no sources declared")
        # validate use_flags structure
        if not isinstance(pkg.use_flags, dict):
            errors.append("use_flags must be a mapping")
        # validate dependencies format - ensure list/dict
        for kind in ("build_depends","runtime_depends","test_depends"):
            val = getattr(pkg, kind)
            if val and not isinstance(val, (list,tuple)):
                errors.append(f"{kind} must be a list")
        ok = len(errors) == 0
        if strict and not ok:
            raise MetaError("validation failed: " + "; ".join(errors))
        return ok, errors

    # -----------------------
    # expand and register hooks: inline command strings or external script paths
    # For each hook stage, normalize to dict {type: 'inline'|'file', content: str, path: Optional[str]}
    # Register with hooks manager if available
    # -----------------------
    def expand_hooks(self, pkg: MetaPackage, package_dir: Optional[str] = None) -> Dict[str,Dict[str,Any]]:
        normalized: Dict[str,Dict[str,Any]] = {}
        hooks_dir = package_dir and os.path.join(package_dir, HOOKS_DIR_NAME)
        for stage, h in (pkg.hooks or {}).items():
            if isinstance(h, str):
                # single string: could be inline script or path
                if os.path.exists(h):
                    normalized[stage] = {"type": "file", "path": os.path.abspath(h)}
                else:
                    # inline
                    normalized[stage] = {"type": "inline", "script": h}
            elif isinstance(h, dict):
                # support form {file: "hooks/pre.sh"} or {inline: "echo hi"}
                if "file" in h and os.path.exists(h["file"]):
                    normalized[stage] = {"type":"file","path": os.path.abspath(h["file"])}
                elif "inline" in h:
                    normalized[stage] = {"type":"inline","script": h["inline"]}
                else:
                    # treat keys as properties; try file key names
                    found = False
                    for k,v in h.items():
                        if k in ("file","path") and isinstance(v,str) and os.path.exists(v):
                            normalized[stage] = {"type":"file","path":os.path.abspath(v)}
                            found = True
                            break
                    if not found and "inline" in h:
                        normalized[stage] = {"type":"inline","script": h.get("inline")}
            elif isinstance(h, list):
                # multiple hooks: store as list of normalized hooks under stage.list
                subs = []
                for item in h:
                    if isinstance(item, str):
                        if os.path.exists(item):
                            subs.append({"type":"file","path":os.path.abspath(item)})
                        else:
                            subs.append({"type":"inline","script":item})
                    elif isinstance(item, dict):
                        if "file" in item and os.path.exists(item["file"]):
                            subs.append({"type":"file","path":os.path.abspath(item["file"])})
                        elif "inline" in item:
                            subs.append({"type":"inline","script": item["inline"]})
                normalized[stage] = {"type":"list","items": subs}
            else:
                # unknown format: skip
                continue
            # merge with hooks_dir if file not absolute: try to resolve inside package dir's hooks folder
            if normalized.get(stage,{}).get("type") == "file":
                p = normalized[stage]["path"]
                if not os.path.isabs(p) and package_dir:
                    cand = os.path.join(package_dir, p)
                    if os.path.exists(cand):
                        normalized[stage]["path"] = os.path.abspath(cand)
            # Register with hook manager
            if self.hooks:
                try:
                    # we register hook bodies or file paths as they are; hook manager decides how to execute
                    self.hooks.register(stage, normalized[stage])
                except Exception:
                    logger.exception("Failed to register hook %s for %s", stage, pkg.name)
        return normalized

    # -----------------------
    # evaluate conditional dependencies and flags
    # conditions support:
    # - "use:gtk" meaning use flag enabled
    # - "arch:x86_64"
    # - simple expression "use:gtk and arch:x86_64" not implemented fully; implement basic AND/OR later
    # returns filtered list of deps
    # dep entries can be strings or dicts {name: "...", if: "..."}
    # -----------------------
    def evaluate_conditional_deps(self, pkg: MetaPackage, flags: Optional[List[str]] = None, arch: Optional[str] = None) -> Dict[str,List[str]]:
        flags = flags or pkg.enabled_flags or []
        arch = arch or os.uname().machine
        def cond_check(expr: Optional[str]) -> bool:
            if not expr:
                return True
            expr = expr.strip()
            # support simple forms "use:gtk" or "arch:x86_64" or "use:gtk && arch:x86_64" (split by && / and)
            parts = re.split(r"\s*(?:&&|and)\s*", expr)
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
                    # unknown condition -> be conservative and return False
                    return False
            return True

        def expand_list(lst):
            out = []
            for item in lst:
                if isinstance(item, str):
                    out.append(item)
                elif isinstance(item, dict):
                    cond = item.get("if") or item.get("condition")
                    if cond_check(cond):
                        name = item.get("name") or item.get("pkg") or next(iter(item.values()))
                        out.append(name)
            return out

        return {
            "build_depends": expand_list(pkg.build_depends or []),
            "runtime_depends": expand_list(pkg.runtime_depends or []),
            "test_depends": expand_list(pkg.test_depends or [])
        }

    # -----------------------
    # fetch sources (calls fetcher if available). Returns list of local paths
    # supports:
    # - http(s): download
    # - git: clone (depth if provided)
    # - ipfs/torrent: delegated to fetcher if available
    # If fetcher not available, uses subprocess git or curl/wget as fallback for basic cases.
    # -----------------------
    def fetch_sources(self, pkg: MetaPackage, variant: Optional[str] = None, destdir: Optional[str] = None, verify_checksums: bool = True) -> List[str]:
        destdir = destdir or tempfile.mkdtemp(prefix=f"meta-fetch-{pkg.name}-")
        _ensure_dir(destdir)
        fetched_paths: List[str] = []
        sources = pkg.resolve_sources(variant=variant)
        fetcher = self.fetcher
        for s in sources:
            # normalize keys
            if "git" in s or "repo" in s:
                repo = s.get("git") or s.get("repo")
                branch = s.get("branch")
                commit = s.get("commit")
                depth = s.get("depth", 1)
                target = os.path.join(destdir, f"{pkg.name}-git-{uuid.uuid4().hex[:6]}")
                if fetcher and hasattr(fetcher, "git_clone"):
                    try:
                        res = fetcher.git_clone(repo, target, branch=branch, commit=commit, depth=depth)
                        if res.get("ok"):
                            fetched_paths.append(target)
                            continue
                    except Exception:
                        logger.exception("fetcher.git_clone failed")
                # fallback to git subprocess
                try:
                    cmd = ["git", "clone", "--depth", str(depth), repo, target]
                    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if branch:
                        subprocess.check_call(["git","-C",target,"checkout",branch], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if commit:
                        subprocess.check_call(["git","-C",target,"checkout",commit], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    fetched_paths.append(target)
                    continue
                except Exception:
                    logger.exception("git clone fallback failed for %s", repo)
                    continue
            elif "url" in s or "http" in s or "https" in s:
                url = s.get("url") or s.get("http") or s.get("https")
                fname = s.get("filename") or os.path.basename(url.split("?")[0]) or f"{pkg.name}-{uuid.uuid4().hex[:6]}"
                outpath = os.path.join(destdir, fname)
                if fetcher and hasattr(fetcher, "download"):
                    try:
                        res = fetcher.download(url, outpath, mirrors=s.get("mirrors"))
                        if res.get("ok"):
                            fetched_paths.append(outpath)
                            # optionally verify checksum
                            if verify_checksums and s.get("checksum"):
                                cs = s.get("checksum")
                                alg,val = cs.split(":",1) if ":" in cs else ("sha256", cs)
                                h = _sha256_file(outpath)
                                if alg.lower() == "sha256" and h != val:
                                    logger.error("Checksum mismatch for %s", outpath)
                                    raise MetaError("checksum mismatch")
                            continue
                    except Exception:
                        logger.exception("fetcher.download failed for %s", url)
                # fallback to curl/wget
                try:
                    if shutil.which("curl"):
                        subprocess.check_call(["curl","-L","-o",outpath,url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    elif shutil.which("wget"):
                        subprocess.check_call(["wget","-O",outpath,url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    else:
                        raise MetaError("neither curl nor wget available for fetching")
                    # checksum verify
                    if verify_checksums and s.get("checksum"):
                        cs = s.get("checksum")
                        alg,val = cs.split(":",1) if ":" in cs else ("sha256", cs)
                        h = _sha256_file(outpath)
                        if alg.lower() == "sha256" and h != val:
                            logger.error("Checksum mismatch for %s", outpath)
                            raise MetaError("checksum mismatch")
                    fetched_paths.append(outpath)
                    continue
                except Exception:
                    logger.exception("download fallback failed for %s", url)
                    continue
            elif "ipfs" in s or "torrent" in s:
                # delegate to fetcher
                if fetcher and hasattr(fetcher, "fetch_special"):
                    try:
                        res = fetcher.fetch_special(s, destdir)
                        if res.get("ok"):
                            fetched_paths.append(res.get("path"))
                            continue
                    except Exception:
                        logger.exception("fetcher.fetch_special failed")
                logger.warning("Special source type not supported without fetcher (ipfs/torrent)")
                continue
            else:
                logger.warning("Unknown source descriptor: %s", s)
        return fetched_paths

    # -----------------------
    # register hooks to hook manager (normalized bodies or file paths)
    # -----------------------
    def register_hooks(self, pkg: MetaPackage, package_dir: Optional[str] = None):
        expanded = self.expand_hooks(pkg, package_dir=package_dir)
        # We already registered in expand_hooks if hooks manager available.
        return expanded

    # -----------------------
    # run a hook for a package:
    # - stage: e.g., pre_configure, post_install
    # - context: dict forwarded as env vars or via stdin
    # Execution:
    # - If sandbox available, run inside sandbox.run_in_sandbox
    # - If inline script, write to temp script and execute
    # - If file script, execute directly (possibly in sandbox)
    # Return: {ok: bool, exit_code, stdout, stderr}
    # -----------------------
    def run_hook(self, hook_descriptor: Dict[str,Any], context: Optional[Dict[str,Any]] = None, timeout: Optional[int] = None) -> Dict[str,Any]:
        context = context or {}
        sandbox = self.sandbox
        if not hook_descriptor:
            return {"ok": True, "note": "no-op"}
        htype = hook_descriptor.get("type")
        # create temporary working dir for hook
        wd = tempfile.mkdtemp(prefix="meta-hook-")
        env = os.environ.copy()
        # inject context into env as JSON
        env["_META_HOOK_CONTEXT"] = json.dumps(context)
        # Provide convenience variables
        if "script" in hook_descriptor and isinstance(hook_descriptor["script"], str):
            # write script file
            script = hook_descriptor["script"]
            script_path = os.path.join(wd, "inline-hook.sh")
            with open(script_path, "w", encoding="utf-8") as f:
                f.write("#!/bin/sh\n")
                f.write(script + "\n")
            os.chmod(script_path, 0o755)
            exec_path = script_path
        elif "path" in hook_descriptor and os.path.exists(hook_descriptor["path"]):
            exec_path = hook_descriptor["path"]
        else:
            shutil.rmtree(wd, ignore_errors=True)
            return {"ok": False, "error": "no_executable"}
        try:
            if sandbox:
                try:
                    res = sandbox.run_in_sandbox([exec_path], cwd=wd, env=env, timeout=timeout, exec_type="hook")
                    return res
                except Exception:
                    logger.exception("sandbox hook execution failed, falling back to direct exec")
            # direct exec fallback
            proc = subprocess.run([exec_path], cwd=wd, env=env, timeout=timeout, capture_output=True, text=True, shell=False)
            return {"ok": proc.returncode == 0, "exit_code": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
        except Exception as e:
            logger.exception("hook execution error")
            return {"ok": False, "error": str(e)}
        finally:
            shutil.rmtree(wd, ignore_errors=True)

    # -----------------------
    # resolve dependencies: convenience wrapper around resolver
    # returns resolver result or simple mapping
    # -----------------------
    def resolve_dependencies(self, pkg: MetaPackage, flags: Optional[List[str]] = None, arch: Optional[str] = None) -> Dict[str,Any]:
        # prepare evaluated deps
        deps = self.evaluate_conditional_deps(pkg, flags=flags, arch=arch)
        # if resolver available call it to compute full trees
        if self.resolver and hasattr(self.resolver, "resolve_package_deps"):
            try:
                return self.resolver.resolve_package_deps(pkg.name, pkg.version, deps)
            except Exception:
                logger.exception("resolver.resolve_package_deps failed")
        # fallback: return evaluated deps lists
        return {"ok": True, "deps": deps}

    # -----------------------
    # helpers: convert simple toml/ini snippet values to python types
    # -----------------------
def _try_cast_value(v: str) -> Any:
    v = v.strip()
    if v.lower() in ("true","yes","on"):
        return True
    if v.lower() in ("false","no","off"):
        return False
    # numeric?
    try:
        if "." in v:
            return float(v)
        else:
            return int(v)
    except Exception:
        pass
    # list comma separated
    if "," in v:
        return [x.strip() for x in v.split(",") if x.strip()]
    return v

# -----------------------
# utility deep merge for dicts (child overrides parent)
# -----------------------
def _deep_merge(a: Dict[str,Any], b: Dict[str,Any]) -> Dict[str,Any]:
    """Return merged dict: values from b override a; lists are concatenated uniquely."""
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

# -----------------------
# ML skeleton for recommending variants/flags based on historical DB data
# -----------------------
class MetaMLAdvisor:
    def __init__(self):
        self.enabled = _ML_AVAILABLE and bool(META_CFG.get("ml_enabled", False))
        # placeholder; real training uses historical DB data
        self.model = None
        if self.enabled:
            try:
                from sklearn.ensemble import RandomForestClassifier  # type: ignore
                self.model = RandomForestClassifier(n_estimators=10)
            except Exception:
                logger.exception("Failed creating ML model; disabling")
                self.enabled = False

    def suggest_flags(self, pkg: MetaPackage) -> List[str]:
        # placeholder: recommend flags that have passed tests historically; fallback to empty
        if not self.enabled or not self.model:
            return []
        # real implementation would query DB for historical builds and features
        return []

# -----------------------
# CLI helpers
# -----------------------
def inspect_meta(path: str, variant: Optional[str] = None, profile: Optional[str] = None, verbose: bool = False):
    loader = MetaLoader()
    pkg = loader.load(path, variant=variant, profile=profile)
    print(pkg.as_json() if verbose else json.dumps({"name":pkg.name,"version":pkg.version,"sources":pkg.resolve_sources(variant=variant)}, indent=2, ensure_ascii=False))

def validate_meta(path: str, strict: bool = True):
    loader = MetaLoader()
    pkg = loader.load(path)
    ok, errs = loader.validate(pkg, strict=strict)
    if ok:
        print("OK")
        return 0
    else:
        print("Validation errors:")
        for e in errs:
            print(" -", e)
        return 1

def fetch_meta_sources(path: str, variant: Optional[str] = None, out: Optional[str] = None):
    loader = MetaLoader()
    pkg = loader.load(path, variant=variant)
    fetched = loader.fetch_sources(pkg, variant=variant, destdir=out)
    print("Fetched:", fetched)
    return fetched

# -----------------------
# CLI entrypoint
# -----------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="meta", description="Load and inspect .meta recipe files")
    ap.add_argument("meta", nargs="?", help="path to .meta file")
    ap.add_argument("--inspect", action="store_true", help="inspect and show basic info")
    ap.add_argument("--validate", action="store_true", help="validate meta file")
    ap.add_argument("--fetch", action="store_true", help="fetch declared sources")
    ap.add_argument("--variant", type=str, help="select variant")
    ap.add_argument("--profile", type=str, help="select profile")
    ap.add_argument("--out", type=str, help="destination directory for fetch")
    args = ap.parse_args()
    if not args.meta:
        ap.print_help()
        sys.exit(0)
    if args.inspect:
        inspect_meta(args.meta, variant=args.variant, profile=args.profile, verbose=True)
    elif args.validate:
        rc = validate_meta(args.meta, strict=True)
        sys.exit(rc)
    elif args.fetch:
        fetch_meta_sources(args.meta, variant=args.variant, out=args.out)
    else:
        ap.print_help()
