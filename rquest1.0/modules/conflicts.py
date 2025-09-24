# Rquest/rquest1.0/modules/conflicts.py
"""
conflicts.py - Detection and resolution of package / file / repo / system conflicts for Rquest

Features:
- Scan installed packages and package metadata to detect file-level and package-level conflicts
- Heuristic: identical file content => allow sharing; different content => conflict
- Resolution strategies: ask, keep-first, replace, sandbox-test
- Sandbox simulation of resolutions (if sandbox module available)
- Hooks: pre_conflict_scan, post_conflict_scan, pre_conflict_resolve, post_conflict_resolve, on_conflict_abort
- Audit daemon for periodic scans (optionally inotify-based monitoring)
- Reports: JSON / YAML / HTML
- Integration with db.py, resolver.py, pkgtool.py, pkgtool manifests, deepclean (for orphaning) and buildsystem
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
import shutil
import hashlib
import tempfile
import logging
import threading
import html
from typing import Any, Dict, List, Optional, Tuple, Set

# ---------------------------
# Robust imports / fallbacks
# ---------------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("conflicts")
except Exception:
    logger = logging.getLogger("conflicts")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# DB wrapper expected: execute/fetchone/fetchall
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def add_history(*a, **k): pass
    def emit_event(*a, **k): pass

# resolver for dependency-aware checks
try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    def get_resolver(): return None

# pkgtool to read manifests and installed file lists
try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    def get_pkgtool(): return None

# sandbox used to simulate installs and test resolution choices
try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager(): return None

# hooks
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

# deepclean integration
try:
    from modules.deepclean import get_deepclean_manager  # type: ignore
except Exception:
    def get_deepclean_manager(): return None

# YAML optional (for export)
try:
    import yaml  # type: ignore
    _YAML_AVAILABLE = True
except Exception:
    _YAML_AVAILABLE = False

# inotify optional monitoring
_INOTIFY_AVAILABLE = False
try:
    import inotify.adapters  # type: ignore
    _INOTIFY_AVAILABLE = True
except Exception:
    _INOTIFY_AVAILABLE = False

# ---------------------------
# Utilities
# ---------------------------
def _now_ts() -> int:
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

def _safe_read_json(path: str) -> Optional[Dict[str,Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _human_bytes(n: int) -> str:
    for unit in ['B','KB','MB','GB','TB']:
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"

# ---------------------------
# Config defaults and policy
# ---------------------------
CFG = get_config() if callable(get_config) else {}
CONFLICT_CFG = CFG.get("conflicts", {}) if isinstance(CFG, dict) else {}

DEFAULTS = {
    "resolution": "ask",  # ask | keep-first | replace | sandbox-test
    "protect_labels": ["critical", "toolchain", "hold"],
    "critical_paths": ["/etc/passwd", "/boot", "/lib/modules"],
    "forbidden_overwrites": ["glibc", "systemd"],
    "audit": {
        "enabled": False,
        "interval_seconds": 3600,
        "monitor_paths": ["/usr", "/lib", "/etc"]
    },
    "report": {
        "output_dir": os.path.expanduser("~/.rquest/reports")
    }
}

def _conf_get(key, default=None):
    return CONFLICT_CFG.get(key, DEFAULTS.get(key, default))

RESOLUTION_DEFAULT = _conf_get("resolution")
PROTECT_LABELS = _conf_get("protect_labels")
CRITICAL_PATHS = _conf_get("critical_paths")
FORBIDDEN_OVERWRITES = _conf_get("forbidden_overwrites")
AUDIT_CFG = _conf_get("audit")
REPORT_DIR = _conf_get("report").get("output_dir") if isinstance(_conf_get("report"), dict) else DEFAULTS["report"]["output_dir"]
os.makedirs(REPORT_DIR, exist_ok=True)

# ---------------------------
# Data structures
# ---------------------------
class FileClaim:
    """
    Represents ownership claim of a file by a package.
    """
    def __init__(self, path: str, package: str, version: str, pkg_id: Optional[str]=None, checksum: Optional[str]=None):
        self.path = os.path.normpath(path)
        self.package = package
        self.version = version
        self.pkg_id = pkg_id or f"{package}-{version}"
        self.checksum = checksum

    def to_dict(self):
        return {"path": self.path, "package": self.package, "version": self.version, "pkg_id": self.pkg_id, "checksum": self.checksum}

class ConflictRecord:
    """
    Represents a conflict - either file-level or package-level.
    conflict_type: 'file', 'package', 'repo', 'system'
    details: structured details depending on type
    severity: 'info'|'warn'|'critical'
    """
    def __init__(self, conflict_type: str, details: Dict[str,Any], severity: str = "warn"):
        self.id = str(uuid.uuid4())
        self.type = conflict_type
        self.details = details
        self.severity = severity
        self.ts = _now_ts()

    def to_dict(self):
        return {"id": self.id, "type": self.type, "details": self.details, "severity": self.severity, "ts": self.ts}

# ---------------------------
# Core: ConflictDetector
# ---------------------------
class ConflictDetector:
    def __init__(self):
        self.db = get_db() if callable(get_db) else None
        self.resolver = get_resolver() if callable(get_resolver) else None
        self.pkgtool = get_pkgtool() if callable(get_pkgtool) else None
        self.sandbox = get_sandbox_manager() if callable(get_sandbox_manager) else None
        self.hooks = get_hook_manager() if callable(get_hook_manager) else None
        self.protect_labels = PROTECT_LABELS
        self.critical_paths = [os.path.normpath(p) for p in CRITICAL_PATHS]
        self.forbidden_overwrites = set(FORBIDDEN_OVERWRITES)

    # -----------------------
    # Helpers to enumerate file claims
    # Returns dict: path -> list[FileClaim]
    # -----------------------
    def gather_file_claims(self, deep_scan: bool = False) -> Dict[str, List[FileClaim]]:
        claims: Dict[str, List[FileClaim]] = {}
        # Primary source: db table that records file->package mapping, if exists
        if self.db:
            try:
                # expected table installed_files (package,version,path,sha256)
                rows = self.db.fetchall("SELECT package, version, path, sha256 FROM installed_files")
                for r in rows:
                    p = r.get("path")
                    if not p:
                        continue
                    claim = FileClaim(path=p, package=r.get("package"), version=r.get("version"), checksum=r.get("sha256"))
                    claims.setdefault(os.path.normpath(p), []).append(claim)
                logger.debug("Gathered %d file claims from DB", sum(len(v) for v in claims.values()))
            except Exception:
                logger.exception("DB installed_files query failed; falling back to manifest scanning")
        # Fallback/augment: read manifests from pkgtool cache or installed manifests
        if self.pkgtool:
            try:
                # pkgtool.cache.list_all() returns records with path to package tarball; we can inspect MANIFEST.json by extracting or reading cache
                entries = self.pkgtool.cache.list_all() if hasattr(self.pkgtool.cache, "list_all") else []
                for e in entries:
                    # attempt to read manifest: either in same dir as package (extracted before) or open package to read MANIFEST.json (costly)
                    # For performance, only inspect local unpacked manifests in ~/.rquest/installed or pkgtool.extract_manifest (if provided)
                    manifest_paths = [
                        os.path.expanduser(f"~/.rquest/installed/{e.get('name')}-{e.get('version')}/MANIFEST.json"),
                        os.path.join(self.pkgtool.cache.cache_dir, f"{e.get('name')}-{e.get('version')}.MANIFEST.json")
                    ]
                    manifest = None
                    for mp in manifest_paths:
                        manifest = _safe_read_json(mp)
                        if manifest:
                            break
                    if manifest:
                        for f in manifest.get("files", []):
                            path = f.get("path")
                            # installed absolute path might be included in metadata; otherwise assume path relative to prefix => try to resolve absolute
                            # Here we assume manifest stores relative paths; we try to reconstruct possible absolute locations
                            # Common: manifests store relative to prefix like usr/bin/foo -> /usr/bin/foo
                            candidate_paths = [os.path.join("/", path.lstrip("/")), path]
                            for cp in candidate_paths:
                                claim = FileClaim(path=cp, package=e.get("name"), version=e.get("version"), checksum=f.get("sha256"))
                                claims.setdefault(os.path.normpath(cp), []).append(claim)
            except Exception:
                logger.exception("pkgtool manifest scanning failed")
        # deep_scan: walk system paths provided in config
        if deep_scan:
            scan_paths = CONFLICT_CFG.get("scan_paths") or ["/usr", "/lib", "/bin", "/sbin", "/etc"]
            for root in scan_paths:
                if not os.path.exists(root):
                    continue
                for dirpath, _, files in os.walk(root):
                    for fn in files:
                        p = os.path.join(dirpath, fn)
                        # if we already have claim for this path, skip
                        if os.path.normpath(p) in claims:
                            continue
                        # try to find owning package via DB reverse lookup if available
                        if self.db:
                            try:
                                row = self.db.fetchone("SELECT package, version, sha256 FROM installed_files WHERE path = ? LIMIT 1", (p,))
                                if row:
                                    claims.setdefault(os.path.normpath(p), []).append(FileClaim(path=p, package=row.get("package"), version=row.get("version"), checksum=row.get("sha256")))
                                    continue
                            except Exception:
                                pass
                        # else mark as ghost file with package None
                        claims.setdefault(os.path.normpath(p), []).append(FileClaim(path=p, package="", version="", checksum=_sha256_file(p)))
        return claims

    # -----------------------
    # Detect file-level conflicts
    # returns list of ConflictRecord
    # -----------------------
    def detect_file_conflicts(self, deep_scan: bool = False) -> List[ConflictRecord]:
        if self.hooks:
            try:
                self.hooks.run("pre_conflict_scan", context={"deep_scan": deep_scan})
            except Exception:
                logger.exception("pre_conflict_scan hook failed")
        claims_map = self.gather_file_claims(deep_scan=deep_scan)
        conflicts: List[ConflictRecord] = []
        for path, claims in claims_map.items():
            # normalize path and ignore directories etc.
            if not path or path.strip()=="":
                continue
            if len(claims) <= 1:
                # no conflict unless claim by none (ghost file)
                if len(claims)==1 and (not claims[0].package or claims[0].package==""):
                    # ghost file
                    cr = ConflictRecord("file", {"path": path, "kind": "ghost", "claims": [c.to_dict() for c in claims]}, severity="info")
                    conflicts.append(cr)
                continue
            # multiple claims exist: decide if identical
            checksums = set(c.checksum for c in claims if c.checksum)
            # if all non-empty checksums identical => safe to share
            if len(checksums) == 1:
                # identical content, not a harmful conflict
                logger.debug("File %s claimed by multiple packages but identical content -> no conflict", path)
                continue
            # else real conflict
            details = {"path": path, "claims": [c.to_dict() for c in claims]}
            # severity critical if path under critical paths or one package is forbidden_overwrites or packages include protected labels
            severity = "warn"
            normpath = os.path.normpath(path)
            for cp in self.critical_paths:
                if normpath == cp or normpath.startswith(os.path.normpath(cp) + os.sep):
                    severity = "critical"
                    break
            for c in claims:
                if c.package in self.forbidden_overwrites:
                    severity = "critical"
                    break
            cr = ConflictRecord("file", details, severity=severity)
            conflicts.append(cr)

        if self.hooks:
            try:
                self.hooks.run("post_conflict_scan", context={"conflicts": [c.to_dict() for c in conflicts]})
            except Exception:
                logger.exception("post_conflict_scan hook failed")
        logger.info("Detected %d file-level conflict(s)", len([c for c in conflicts if c.type=="file"]))
        return conflicts

    # -----------------------
    # Detect package-level conflicts (ex: mutual excludes, slots)
    # returns list of ConflictRecord
    # -----------------------
    def detect_package_conflicts(self) -> List[ConflictRecord]:
        conflicts: List[ConflictRecord] = []
        # 1) mutual excludes / !pkg in metadata - use resolver to detect
        try:
            if self.resolver and hasattr(self.resolver, "detect_conflicts"):
                # if resolver exposes detailed conflict detection, prefer it
                res = self.resolver.detect_conflicts()
                for c in res.get("conflicts", []):
                    cr = ConflictRecord("package", {"detail": c}, severity=c.get("severity", "warn"))
                    conflicts.append(cr)
            else:
                # fallback: scan installed_packages for explicit 'conflicts' metadata fields if available in DB
                if self.db:
                    try:
                        rows = self.db.fetchall("SELECT package, version, metadata FROM installed_packages")
                        for r in rows:
                            md = {}
                            try:
                                md = json.loads(r.get("metadata") or "{}")
                            except Exception:
                                md = {}
                            if md.get("conflicts"):
                                # conflict declared by package; record it
                                cr = ConflictRecord("package", {"package": r.get("package"), "conflicts_declared": md.get("conflicts")}, severity="warn")
                                conflicts.append(cr)
                    except Exception:
                        logger.exception("Failed scanning installed_packages metadata for conflicts")
        except Exception:
            logger.exception("Package-level conflict detection failed")
        # 2) slot conflicts: ask slots module or use simple heuristic: two installed packages occupying same exclusive slot
        try:
            # attempt to query installed_packages and find duplicates by slot key
            if self.db:
                rows = self.db.fetchall("SELECT package, version, slot FROM installed_packages")
                slot_map = {}
                for r in rows:
                    slot = r.get("slot") or "default"
                    slot_map.setdefault(slot, []).append({"package": r.get("package"), "version": r.get("version")})
                for slot, pkgs in slot_map.items():
                    if len(pkgs) > 1:
                        details = {"slot": slot, "packages": pkgs}
                        sev = "warn"
                        # if slot reserved (e.g., toolchain) mark critical
                        if slot in ("toolchain", "python") or any(p["package"] in self.forbidden_overwrites for p in pkgs):
                            sev = "critical"
                        conflicts.append(ConflictRecord("package_slot", details, severity=sev))
        except Exception:
            logger.exception("Slot conflict detection failed")
        logger.info("Detected %d package-level conflict(s)", len([c for c in conflicts if c.type.startswith("package")]))
        return conflicts

    # -----------------------
    # Master scan: combine file and package conflicts and produce unified list
    # -----------------------
    def scan_conflicts(self, deep_scan: bool = False) -> List[ConflictRecord]:
        file_conflicts = self.detect_file_conflicts(deep_scan=deep_scan)
        pkg_conflicts = self.detect_package_conflicts()
        all_conflicts = file_conflicts + pkg_conflicts
        # persist to DB history if available
        if self.db:
            try:
                for c in all_conflicts:
                    self.db.execute("INSERT INTO conflicts_history (id, type, details, severity, ts) VALUES (?, ?, ?, ?, ?)",
                                    (c.id, c.type, json.dumps(c.details), c.severity, c.ts), commit=True)
            except Exception:
                # if conflicts_history table doesn't exist, create it
                try:
                    self.db.execute("""
                        CREATE TABLE IF NOT EXISTS conflicts_history (
                            id TEXT PRIMARY KEY,
                            type TEXT,
                            details TEXT,
                            severity TEXT,
                            ts INTEGER
                        );
                    """, (), commit=True)
                    for c in all_conflicts:
                        self.db.execute("INSERT INTO conflicts_history (id, type, details, severity, ts) VALUES (?, ?, ?, ?, ?)",
                                        (c.id, c.type, json.dumps(c.details), c.severity, c.ts), commit=True)
                except Exception:
                    logger.exception("Failed to persist conflicts_history")
        return all_conflicts
      # continuation of Rquest/rquest1.0/modules/conflicts.py (PART 2)

# ---------------------------
# ConflictResolver: strategies to resolve conflicts
# ---------------------------
class ConflictResolver:
    def __init__(self, detector: Optional[ConflictDetector] = None, strategy: Optional[str] = None):
        self.detector = detector or ConflictDetector()
        self.strategy = strategy or RESOLUTION_DEFAULT
        self.hooks = get_hook_manager() if callable(get_hook_manager) else None
        self.sandbox = get_sandbox_manager() if callable(get_sandbox_manager) else None
        self.pkgtool = get_pkgtool() if callable(get_pkgtool) else None
        self.resolver = get_resolver() if callable(get_resolver) else None
        self.db = get_db() if callable(get_db) else None

    # -----------------------
    # resolution helpers
    # -----------------------
    def _is_critical_conflict(self, conflict: ConflictRecord) -> bool:
        return conflict.severity == "critical"

    def _ask_user_choice(self, conflict: ConflictRecord) -> Dict[str,Any]:
        # interactive prompt: list options and ask user. Conservative defaults to keep-first
        details = conflict.details
        print("Conflict detected:")
        print(json.dumps(details, indent=2, ensure_ascii=False))
        print("Choose resolution:")
        print("  [1] keep-first (preserve currently installed)")
        print("  [2] replace (use incoming candidate)")
        print("  [3] skip (leave as-is)")
        print("  [4] sandbox-test (simulate choices in sandbox)")
        choice = input("Select 1-4 [1]: ").strip() or "1"
        mapping = {"1": "keep-first", "2": "replace", "3": "skip", "4": "sandbox-test"}
        return {"strategy": mapping.get(choice, "keep-first")}

    def _sandbox_test_resolution(self, conflict: ConflictRecord, prefer_pkg: Optional[str] = None) -> Dict[str,Any]:
        """
        Attempt to simulate resolution in sandbox:
        - create a temporary sandbox session
        - attempt to perform the install/remove actions implied by prefer_pkg or by choosing one package
        - run quick checks (resolver.resolve_dependencies) inside the sandboxed view if possible
        Return dict with ok bool and logs.
        """
        if not self.sandbox:
            return {"ok": False, "error": "no_sandbox"}
        session_id = None
        try:
            session_id = self.sandbox.start_session()
            # We will not actually install remote packages; instead we emulate file operations:
            # For file conflicts: create tmp directories where we simulate placing package files and see overlaps.
            # For package conflicts: run resolver simulation if available.
            # This is best-effort — more complex actions depend on project specifics
            logs = []
            # run resolver simulate if available
            if self.resolver and hasattr(self.resolver, "simulate_resolution"):
                try:
                    sim = self.resolver.simulate_resolution(conflict.details)
                    logs.append({"resolver_sim": sim})
                except Exception as e:
                    logs.append({"resolver_error": str(e)})
            # sandbox actions could be extended
            self.sandbox.stop_session(session_id)
            return {"ok": True, "logs": logs}
        except Exception as e:
            logger.exception("sandbox_test_resolution failed")
            if session_id:
                try:
                    self.sandbox.stop_session(session_id)
                except Exception:
                    pass
            return {"ok": False, "error": str(e)}

    # -----------------------
    # apply a single conflict resolution according to strategy
    # conflict: ConflictRecord
    # strategy: optional override
    # options: extra params like prefer_package
    # returns dict { ok: bool, action: str, detail: ... }
    # -----------------------
    def resolve_one(self, conflict: ConflictRecord, strategy: Optional[str] = None, options: Optional[Dict[str,Any]] = None) -> Dict[str,Any]:
        strategy = strategy or self.strategy
        options = options or {}
        if self.hooks:
            try:
                self.hooks.run("pre_conflict_resolve", context={"conflict": conflict.to_dict(), "strategy": strategy, "options": options})
            except Exception:
                logger.exception("pre_conflict_resolve hook failed")
        # If critical and strategy not forced, abort and call on_conflict_abort hook
        if self._is_critical_conflict(conflict) and strategy not in ("replace", "sandbox-test", "ask"):
            logger.warning("Critical conflict %s with default strategy %s -> aborting", conflict.id, strategy)
            if self.hooks:
                try:
                    self.hooks.run("on_conflict_abort", context={"conflict": conflict.to_dict()})
                except Exception:
                    logger.exception("on_conflict_abort hook failed")
            return {"ok": False, "action": "abort", "reason": "critical_conflict"}

        # File-level conflict: choose which package to keep or perform merge if etc file
        if conflict.type == "file":
            path = conflict.details.get("path")
            claims = conflict.details.get("claims", [])
            # pick candidate packages names
            pkgs = [c.get("package") for c in claims if c.get("package")]
            # interactive
            if strategy == "ask":
                choice = self._ask_user_choice(conflict)
                strategy = choice.get("strategy", strategy)

            if strategy == "keep-first":
                # keep the first claim (by list order) and remove files from others or move them aside
                keep = pkgs[0] if pkgs else None
                actions = []
                for c in claims:
                    if c.get("package") == keep:
                        continue
                    # for packages that would be overwritten, we move their conflicting file into package-specific backup stored in trash or manifest update
                    # Conservative: do not delete files, create backup in trash
                    src = path
                    bak_dir = os.path.join(os.path.expanduser("~/.rquest/conflicts_backups"), c.get("package"))
                    try:
                        os.makedirs(bak_dir, exist_ok=True)
                        dst = os.path.join(bak_dir, os.path.basename(path))
                        if os.path.exists(src):
                            shutil.copy2(src, dst)
                        actions.append({"pkg": c.get("package"), "action": "backup_file", "dest": dst})
                    except Exception:
                        actions.append({"pkg": c.get("package"), "action": "backup_failed"})
                result = {"ok": True, "action": "keep-first", "keep": keep, "actions": actions}
            elif strategy == "replace":
                # replace means prefer newer package files (requires knowledge of versions and possibly reinstall)
                # we attempt to choose the claim with highest version string (naive lexicographic)
                best = None
                for c in claims:
                    if not best or (c.get("version") or "") > (best.get("version") or ""):
                        best = c
                actions = [{"pkg": c.get("package"), "note": "file overwritten" if c!=best else "kept"} for c in claims]
                result = {"ok": True, "action": "replace", "chosen": best.get("package") if best else None, "notes": actions}
            elif strategy == "sandbox-test":
                res = self._sandbox_test_resolution(conflict)
                result = {"ok": res.get("ok", False), "action": "sandbox-test", "detail": res}
            elif strategy == "skip":
                result = {"ok": True, "action": "skip"}
            else:
                result = {"ok": False, "action": "unknown_strategy"}
        elif conflict.type.startswith("package"):
            # package-level conflicts: use resolver to find consistent set
            if strategy == "ask":
                choice = self._ask_user_choice(conflict)
                strategy = choice.get("strategy", strategy)
            if strategy in ("keep-first", "replace"):
                # naive: prefer package with highest version or keep-first
                pkgs = conflict.details.get("packages") or []
                if strategy == "keep-first":
                    chosen = pkgs[0] if pkgs else None
                else:
                    # choose highest version
                    chosen = None
                    for p in pkgs:
                        if not chosen or (p.get("version") or "") > (chosen.get("version") or ""):
                            chosen = p
                # call resolver to do safe removal/install if available
                if self.resolver and hasattr(self.resolver, "plan_installation"):
                    # best-effort: call resolver with preference or invoke plan_installation after choosing
                    try:
                        # This is domain specific; we simply return chosen decision here
                        result = {"ok": True, "action": strategy, "chosen": chosen}
                    except Exception:
                        result = {"ok": False, "action": strategy, "error": "resolver_integration_failed"}
                else:
                    result = {"ok": True, "action": strategy, "chosen": chosen}
            elif strategy == "sandbox-test":
                res = self._sandbox_test_resolution(conflict)
                result = {"ok": res.get("ok", False), "action": "sandbox-test", "detail": res}
            else:
                result = {"ok": False, "action": "unknown_strategy"}
        else:
            result = {"ok": False, "action": "unsupported_conflict_type"}

        # run post hook
        if self.hooks:
            try:
                self.hooks.run("post_conflict_resolve", context={"conflict": conflict.to_dict(), "result": result})
            except Exception:
                logger.exception("post_conflict_resolve hook failed")
        return result

    # -----------------------
    # Resolve a list of conflicts in batch, honoring strategy and safety checks
    # returns list of resolution results and summary
    # -----------------------
    def resolve_conflicts(self, conflicts: List[ConflictRecord], strategy: Optional[str] = None, interactive: bool = False) -> Dict[str,Any]:
        strategy = strategy or self.strategy
        results = []
        aborted = []
        for c in conflicts:
            if self._is_critical_conflict(c) and strategy not in ("ask","sandbox-test","replace"):
                logger.warning("Critical conflict %s detected - skipping automatic resolution (strategy=%s)", c.id, strategy)
                aborted.append({"id": c.id, "reason": "critical_conflict"})
                if self.hooks:
                    try:
                        self.hooks.run("on_conflict_abort", context={"conflict": c.to_dict()})
                    except Exception:
                        logger.exception("on_conflict_abort hook failed")
                continue
            if interactive:
                # prompt for this conflict
                print("Conflict:")
                print(json.dumps(c.details, indent=2))
                s = input(f"Choose strategy for this conflict [default={strategy}]: ").strip()
                chosen_strategy = s if s else strategy
            else:
                chosen_strategy = strategy
            res = self.resolve_one(c, strategy=chosen_strategy)
            results.append({"conflict_id": c.id, "result": res})
        summary = {"total": len(conflicts), "resolved": len([r for r in results if r.get("result",{}).get("ok")]), "aborted": len(aborted)}
        return {"ok": True, "summary": summary, "results": results, "aborted": aborted}

# ---------------------------
# Reporting utilities
# ---------------------------
def export_report(conflicts: List[ConflictRecord], fmt: str = "json", path: Optional[str] = None) -> str:
    """
    Export conflicts list to JSON/YAML/HTML file. Returns path to written file.
    """
    if not path:
        stamp = time.strftime("%Y%m%d-%H%M%S")
        path = os.path.join(REPORT_DIR, f"conflicts-{stamp}.{fmt}")
    data = [c.to_dict() for c in conflicts]
    try:
        if fmt == "json":
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return path
        if fmt == "yaml" and _YAML_AVAILABLE:
            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(data, f)
            return path
        if fmt == "html":
            # simple HTML table
            rows = []
            for c in conflicts:
                rows.append(f"<tr><td>{html.escape(c.id)}</td><td>{html.escape(c.type)}</td><td>{html.escape(c.severity)}</td><td><pre>{html.escape(json.dumps(c.details, indent=2))}</pre></td><td>{time.ctime(c.ts)}</td></tr>")
            body = "<table border='1'><tr><th>ID</th><th>Type</th><th>Severity</th><th>Details</th><th>When</th></tr>" + "".join(rows) + "</table>"
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"<html><body><h1>Conflicts report</h1>{body}</body></html>")
            return path
        # fallback to json
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return path
    except Exception:
        logger.exception("export_report failed")
        raise

# ---------------------------
# Audit daemon - periodic scan and optionally inotify monitoring
# ---------------------------
class ConflictAuditDaemon:
    def __init__(self, detector: Optional[ConflictDetector] = None, interval: int = 3600, monitor_paths: Optional[List[str]] = None):
        self.detector = detector or ConflictDetector()
        self.interval = interval
        self.monitor_paths = monitor_paths or AUDIT_CFG.get("monitor_paths", [])
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._inotify_thread: Optional[threading.Thread] = None
        self._inotify_adapter = None

    def _audit_loop(self):
        logger.info("Conflict audit loop started (interval=%ds)", self.interval)
        while not self._stop.wait(self.interval):
            try:
                conflicts = self.detector.scan_conflicts(deep_scan=False)
                # if any critical conflicts, emit event
                critical = [c for c in conflicts if c.severity == "critical"]
                if critical:
                    logger.warning("Audit found %d critical conflicts", len(critical))
                    emit_event("conflicts.critical", {"count": len(critical), "ids": [c.id for c in critical]})
                # write a report snapshot
                try:
                    export_report(conflicts, fmt="json")
                except Exception:
                    pass
            except Exception:
                logger.exception("Error in audit loop")
        logger.info("Conflict audit loop stopped")

    def _inotify_monitor(self):
        if not _INOTIFY_AVAILABLE:
            logger.info("inotify not available; monitor disabled")
            return
        try:
            adapter = inotify.adapters.Inotify()
            self._inotify_adapter = adapter
            # add watches
            for p in self.monitor_paths:
                if os.path.exists(p):
                    adapter.add_watch(p)
            logger.info("Inotify monitor started on paths: %s", self.monitor_paths)
            for event in adapter.event_gen(yield_nones=False):
                ((path, type_names, watch_path, filename)) = event
                logger.debug("inotify event %s %s %s", path, type_names, filename)
                # simple reaction: run a quick conflict scan for changed file
                try:
                    conflicts = self.detector.detect_file_conflicts(deep_scan=False)
                    if conflicts:
                        emit_event("conflicts.detected", {"count": len(conflicts)})
                except Exception:
                    logger.exception("inotify scan error")
                if self._stop.is_set():
                    break
            adapter.remove_watch(*self.monitor_paths)
        except Exception:
            logger.exception("inotify monitor failed")

    def start(self):
        if self._thread and self._thread.is_alive():
            return {"ok": False, "reason": "already_running"}
        self._stop.clear()
        self._thread = threading.Thread(target=self._audit_loop, daemon=True)
        self._thread.start()
        # start inotify thread if available
        if _INOTIFY_AVAILABLE and self.monitor_paths:
            self._inotify_thread = threading.Thread(target=self._inotify_monitor, daemon=True)
            self._inotify_thread.start()
        return {"ok": True}

    def stop(self):
        if not self._thread:
            return {"ok": False, "reason": "not_running"}
        self._stop.set()
        self._thread.join(timeout=5)
        if self._inotify_thread:
            self._inotify_thread.join(timeout=5)
        return {"ok": True}

# ---------------------------
# Module-level helpers and CLI
# ---------------------------
_DETECTOR: Optional[ConflictDetector] = None
_RESOLVER: Optional[ConflictResolver] = None
_DAEMON: Optional[ConflictAuditDaemon] = None

def get_detector() -> ConflictDetector:
    global _DETECTOR
    if _DETECTOR is None:
        _DETECTOR = ConflictDetector()
    return _DETECTOR

def get_resolver() -> ConflictResolver:
    global _RESOLVER
    if _RESOLVER is None:
        _RESOLVER = ConflictResolver()
    return _RESOLVER

def get_daemon() -> ConflictAuditDaemon:
    global _DAEMON
    if _DAEMON is None:
        _DAEMON = ConflictAuditDaemon(detector=get_detector(), interval=AUDIT_CFG.get("interval_seconds", 3600), monitor_paths=AUDIT_CFG.get("monitor_paths", []))
    return _DAEMON

# CLI
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="conflicts", description="Scan and resolve package/file conflicts")
    ap.add_argument("--scan", action="store_true", help="scan for conflicts")
    ap.add_argument("--deep", action="store_true", help="perform deeper scan (walk system paths)")
    ap.add_argument("--export", metavar="FMT", help="export report (json,yaml,html)")
    ap.add_argument("--resolve", metavar="STRAT", help="resolve detected conflicts with strategy (ask|keep-first|replace|sandbox-test)")
    ap.add_argument("--interactive", action="store_true", help="interactive resolution")
    ap.add_argument("--start-daemon", action="store_true", help="start audit daemon")
    ap.add_argument("--stop-daemon", action="store_true", help="stop audit daemon")
    args = ap.parse_args()

    detector = get_detector()
    resolver = get_resolver()

    if args.start_daemon:
        print(get_daemon().start())
        sys.exit(0)
    if args.stop_daemon:
        print(get_daemon().stop())
        sys.exit(0)

    if args.scan:
        confs = detector.scan_conflicts(deep_scan=bool(args.deep))
        print(f"Found {len(confs)} conflicts")
        if args.export:
            path = export_report(confs, fmt=args.export)
            print("Report written to", path)
        else:
            for c in confs:
                print(json.dumps(c.to_dict(), indent=2, ensure_ascii=False))
        if args.resolve:
            res = resolver.resolve_conflicts(confs, strategy=args.resolve, interactive=args.interactive)
            print(json.dumps(res, indent=2, ensure_ascii=False))
            sys.exit(0)
    else:
        ap.print_help()
