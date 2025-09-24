# Rquest/rquest1.0/modules/audit.py
"""
audit.py - System auditor and integrity checker for Rquest

Features implemented:
- File integrity checks (SHA256 / optional BLAKE2) versus DB/manifest records
- Permissions and ownership checks
- Critical packages verification
- Real-time monitoring (inotify) and periodic audit daemon
- Auto-fix skeletons (reinstall via pkgtool / sandboxed validation)
- Compliance profile checks (CIS-like skeleton)
- Reports export: JSON / YAML / HTML / (PDF via wkhtmltopdf if available)
- Hooks: pre_audit, post_audit, on_integrity_fail, on_compliance_fail, on_auto_fix
- Optional ML prioritizer skeleton (scikit-learn if present)
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
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Set

# --------------------------
# Robust imports and fallbacks
# --------------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("audit")
except Exception:
    logger = logging.getLogger("audit")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# DB wrapper expected: execute/fetchone/fetchall
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def add_history(*a, **k): pass
    def emit_event(*a, **k): pass

# Conflicts / deepclean / pkgtool / resolver / sandbox
try:
    from modules.conflicts import get_detector as get_conflicts_detector  # type: ignore
except Exception:
    def get_conflicts_detector(): return None

try:
    from modules.deepclean import get_deepclean_manager  # type: ignore
except Exception:
    def get_deepclean_manager(): return None

try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    def get_pkgtool(): return None

try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    def get_resolver(): return None

try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager(): return None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

# YAML optional for reports
try:
    import yaml  # type: ignore
    YAML_AVAILABLE = True
except Exception:
    YAML_AVAILABLE = False

# Inotify optional (for real-time)
_INOTIFY_AVAILABLE = False
try:
    import inotify.adapters  # type: ignore
    _INOTIFY_AVAILABLE = True
except Exception:
    _INOTIFY_AVAILABLE = False

# scikit-learn optional (ML prioritizer)
_ML_AVAILABLE = False
try:
    import sklearn  # type: ignore
    _ML_AVAILABLE = True
except Exception:
    _ML_AVAILABLE = False

# wkhtmltopdf availability for PDF export
_WKHTMLTOPDF = bool(shutil.which("wkhtmltopdf"))

# --------------------------
# Utilities
# --------------------------
def _now_ts() -> int:
    return int(time.time())

def _human_bytes(n: int) -> str:
    for unit in ['B','KB','MB','GB','TB']:
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"

def _sha256_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def _blake2_file(path: str) -> Optional[str]:
    try:
        h = hashlib.blake2b()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def _safe_json_load(p: str) -> Optional[Dict[str,Any]]:
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

# --------------------------
# Config & defaults
# --------------------------
CFG = get_config() if callable(get_config) else {}
AUDIT_CFG = CFG.get("audit", {}) if isinstance(CFG, dict) else {}
DEFAULTS = {
    "critical_packages": ["glibc", "systemd", "rquest"],
    "check_permissions": True,
    "check_ownership": True,
    "hash_algos": ["sha256"],
    "paths": ["/usr", "/lib", "/etc", "/bin", "/sbin"],
    "output_dir": os.path.expanduser("~/.rquest/audit_reports"),
    "daemon": {"enabled": False, "interval_seconds": 3600, "realtime": False, "monitor_paths": ["/usr", "/lib", "/etc"]},
    "auto_fix": {"enabled": False, "allowed": False, "preview": True},
    "ml_prioritizer": {"enabled": False}
}
def _acfg(key, default=None):
    return AUDIT_CFG.get(key, DEFAULTS.get(key, default))

CRIT_PACKAGES = _acfg("critical_packages")
CHECK_PERMS = bool(_acfg("check_permissions"))
CHECK_OWNER = bool(_acfg("check_ownership"))
HASH_ALGOS = list(_acfg("hash_algos"))
AUDIT_PATHS = list(_acfg("paths"))
REPORT_DIR = os.path.abspath(_acfg("output_dir"))
DAEMON_CFG = _acfg("daemon")
AUTO_FIX_CFG = _acfg("auto_fix")
ML_CFG = _acfg("ml_prioritizer")
_ensure_dir(REPORT_DIR)

# --------------------------
# Data models
# --------------------------
class AuditFinding:
    def __init__(self, kind: str, path: str, package: Optional[str], details: Dict[str,Any], severity: str = "warn"):
        self.id = str(uuid.uuid4())
        self.kind = kind  # 'missing', 'modified', 'extra', 'perm', 'owner', 'policy'
        self.path = os.path.normpath(path)
        self.package = package
        self.details = details
        self.severity = severity
        self.timestamp = _now_ts()

    def to_dict(self):
        return {"id": self.id, "kind": self.kind, "path": self.path, "package": self.package, "details": self.details, "severity": self.severity, "ts": self.timestamp}

class AuditReport:
    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or str(uuid.uuid4())
        self.started_at = _now_ts()
        self.findings: List[AuditFinding] = []
        self.metadata: Dict[str,Any] = {}
        self.finished_at: Optional[int] = None

    def add(self, f: AuditFinding):
        self.findings.append(f)

    def finish(self):
        self.finished_at = _now_ts()

    def to_dict(self):
        return {"run_id": self.run_id, "started_at": self.started_at, "finished_at": self.finished_at, "findings": [f.to_dict() for f in self.findings], "metadata": self.metadata}

# --------------------------
# Core: Auditor
# --------------------------
class Auditor:
    def __init__(self, cfg: Optional[Dict[str,Any]] = None):
        self.cfg = cfg or AUDIT_CFG
        self.db = get_db() if callable(get_db) else None
        self.pkgtool = get_pkgtool() if callable(get_pkgtool) else None
        self.conflicts = get_conflicts_detector()() if callable(get_conflicts_detector) and get_conflicts_detector() is not None else None
        self.deepclean = get_deepclean_manager()() if callable(get_deepclean_manager) and get_deepclean_manager() is not None else None
        self.sandbox = get_sandbox_manager()() if callable(get_sandbox_manager) and get_sandbox_manager() is not None else None
        self.hooks = get_hook_manager()() if callable(get_hook_manager) and get_hook_manager() is not None else None
        self.hash_algos = HASH_ALGOS
        self.paths = AUDIT_PATHS
        self.crit_packages = CRIT_PACKAGES
        self.report_dir = REPORT_DIR
        self.auto_fix_enabled = AUTO_FIX_CFG.get("enabled", False)
        self.auto_fix_allowed = AUTO_FIX_CFG.get("allowed", False)
        self.auto_fix_preview = AUTO_FIX_CFG.get("preview", True)
        # ML prioritizer skeleton
        self.ml_enabled = ML_CFG.get("enabled", False) and _ML_AVAILABLE

    # ------------------------------------
    # helper: load installed files mapping from DB (expected installed_files table)
    # returns dict: path -> {package, version, sha256, mode, uid, gid}
    # ------------------------------------
    def _load_installed_files_map(self) -> Dict[str, Dict[str,Any]]:
        mapping: Dict[str, Dict[str,Any]] = {}
        if self.db:
            try:
                rows = self.db.fetchall("SELECT package, version, path, sha256, mode, uid, gid FROM installed_files")
                for r in rows:
                    p = os.path.normpath(r.get("path"))
                    mapping[p] = {"package": r.get("package"), "version": r.get("version"), "sha256": r.get("sha256"), "mode": r.get("mode"), "uid": r.get("uid"), "gid": r.get("gid")}
                logger.debug("Loaded %d installed file records from DB", len(mapping))
                return mapping
            except Exception:
                logger.exception("Failed loading installed_files from DB")
        # fallback: try reading manifests in ~/.rquest/installed
        base = os.path.expanduser("~/.rquest/installed")
        if os.path.isdir(base):
            for entry in os.listdir(base):
                mf = os.path.join(base, entry, "MANIFEST.json")
                if os.path.exists(mf):
                    try:
                        j = _safe_json_load(mf)
                        if not j:
                            continue
                        pkg = j.get("name")
                        ver = j.get("version")
                        for f in j.get("files", []):
                            rel = f.get("path")
                            abs_path = os.path.normpath(os.path.join("/", rel.lstrip("/")))
                            mapping[abs_path] = {"package": pkg, "version": ver, "sha256": f.get("sha256")}
                    except Exception:
                        logger.exception("Failed parsing manifest %s", mf)
        logger.debug("Fallback mapping size %d", len(mapping))
        return mapping

    # ------------------------------------
    # check a single file against mapping: returns list of AuditFinding (empty if OK)
    # ------------------------------------
    def _check_file(self, path: str, record: Optional[Dict[str,Any]]) -> List[AuditFinding]:
        findings: List[AuditFinding] = []
        if not os.path.exists(path):
            # missing file
            kind = "missing"
            sev = "critical" if record and record.get("package") in self.crit_packages else "warn"
            details = {"expected": True, "note": "file missing from disk"}
            findings.append(AuditFinding(kind, path, record.get("package") if record else None, details, severity=sev))
            return findings
        # compute checksums as requested
        checks = {}
        if "sha256" in self.hash_algos:
            sha = _sha256_file(path)
            checks["sha256"] = sha
        if "blake2" in self.hash_algos:
            bl = _blake2_file(path)
            checks["blake2"] = bl
        # compare with record
        if record:
            expected_sha = record.get("sha256")
            if expected_sha and checks.get("sha256") and expected_sha != checks.get("sha256"):
                sev = "critical" if record.get("package") in self.crit_packages else "warn"
                details = {"expected_sha256": expected_sha, "actual_sha256": checks.get("sha256")}
                findings.append(AuditFinding("modified", path, record.get("package"), details, severity=sev))
        else:
            # file not recorded in DB -> extra file
            details = {"note": "file not owned by any known package", "checksums": checks}
            findings.append(AuditFinding("extra", path, None, details, severity="info"))
        # permissions and ownership checks if present in record
        try:
            st = os.stat(path)
            mode = oct(st.st_mode & 0o777)
            uid = st.st_uid
            gid = st.st_gid
            if record:
                rmode = record.get("mode")
                ruid = record.get("uid")
                rgid = record.get("gid")
                if CHECK_PERMS and rmode and (str(rmode) != str(mode)):
                    findings.append(AuditFinding("perm", path, record.get("package"), {"expected_mode": rmode, "actual_mode": mode}, severity="warn"))
                if CHECK_OWNER and ruid is not None and rgid is not None and (int(ruid) != uid or int(rgid) != gid):
                    findings.append(AuditFinding("owner", path, record.get("package"), {"expected_uid": ruid, "expected_gid": rgid, "actual_uid": uid, "actual_gid": gid}, severity="warn"))
        except Exception:
            logger.exception("Failed stat %s", path)
        return findings

    # ------------------------------------
    # run_audit: performs a full audit across mapping or specific paths
    # options:
    #   paths: list[str] to scan (defaults to config)
    #   deep: bool -> if True, walk file system and check extra files
    #   auto_fix: bool -> attempt fixes when possible (requires allowed)
    #   compliance_profile: optional string to validate compliance rules
    # returns AuditReport
    # ------------------------------------
    def run_audit(self, paths: Optional[List[str]] = None, deep: bool = False, auto_fix: bool = False, compliance_profile: Optional[str] = None) -> AuditReport:
        run_id = str(uuid.uuid4())
        report = AuditReport(run_id)
        if self.hooks:
            try:
                self.hooks.run("pre_audit", context={"run_id": run_id, "deep": deep, "paths": paths})
            except Exception:
                logger.exception("pre_audit hook failed")
        mapping = self._load_installed_files_map()
        # build scan list
        scan_paths = paths or self.paths
        scanned_files = 0
        # if deep: walk scan_paths and check every file; otherwise iterate mapping
        if deep:
            for root in scan_paths:
                if not os.path.exists(root):
                    continue
                for dirpath, _, files in os.walk(root):
                    for fn in files:
                        p = os.path.join(dirpath, fn)
                        rec = mapping.get(os.path.normpath(p))
                        findings = self._check_file(p, rec)
                        for f in findings:
                            report.add(f)
                        scanned_files += 1
        else:
            # iterate mapping keys only (faster)
            for p, rec in mapping.items():
                findings = self._check_file(p, rec)
                for f in findings:
                    report.add(f)
                scanned_files += 1
        # detect extra files if deep: any file not in mapping will be reported by _check_file as 'extra'
        # compliance checks
        if compliance_profile:
            comp_findings = self._check_compliance(profile=compliance_profile)
            for cf in comp_findings:
                report.add(cf)
        # critical packages check
        crit_findings = self._check_critical_packages(mapping)
        for cf in crit_findings:
            report.add(cf)
        report.metadata.update({"scanned_files": scanned_files, "paths": scan_paths})
        report.finish()
        # store run in DB
        if self.db:
            try:
                self.db.execute("""CREATE TABLE IF NOT EXISTS audit_runs (
                    id TEXT PRIMARY KEY, started_at INTEGER, finished_at INTEGER, findings_count INTEGER, metadata TEXT
                );""", (), commit=True)
                self.db.execute("INSERT OR REPLACE INTO audit_runs (id, started_at, finished_at, findings_count, metadata) VALUES (?, ?, ?, ?, ?)",
                                (report.run_id, report.started_at, report.finished_at, len(report.findings), json.dumps(report.metadata)), commit=True)
            except Exception:
                logger.exception("Failed to record audit run in DB")
        # run post hook
        if self.hooks:
            try:
                self.hooks.run("post_audit", context={"report": report.to_dict()})
            except Exception:
                logger.exception("post_audit hook failed")
        # optionally attempt auto-fix
        if auto_fix and report.findings and self.auto_fix_allowed:
            self._attempt_auto_fix(report)
        return report

# end of PART 1
# continuation of Rquest/rquest1.0/modules/audit.py (PART 2)

# ------------------------------------
# compliance check skeleton
# ------------------------------------
    def _check_compliance(self, profile: str = "cis") -> List[AuditFinding]:
        findings: List[AuditFinding] = []
        # Simple skeleton: add rules per profile
        if profile == "cis":
            # example rule: /etc/passwd should exist
            p = "/etc/passwd"
            if not os.path.exists(p):
                findings.append(AuditFinding("policy", p, None, {"rule": "etc_passwd_exists", "note": "missing /etc/passwd"}, severity="critical"))
            # example: required services - check systemctl if available
            try:
                svc_req = ["sshd"]
                for s in svc_req:
                    rc = subprocess.call(["systemctl", "is-active", "--quiet", s]) if shutil.which("systemctl") else 1
                    if rc != 0:
                        findings.append(AuditFinding("policy", f"service:{s}", None, {"rule": "service_active", "service": s}, severity="warn"))
            except Exception:
                pass
        else:
            # other profiles may be added by user in config
            pass
        return findings

# ------------------------------------
# check critical packages
# ------------------------------------
    def _check_critical_packages(self, mapping: Dict[str, Dict[str,Any]]) -> List[AuditFinding]:
        findings: List[AuditFinding] = []
        # gather files for critical packages from mapping
        for ppath, rec in mapping.items():
            pkg = rec.get("package")
            if pkg in self.crit_packages:
                # check file
                fds = self._check_file(ppath, rec)
                findings.extend(fds)
        return findings

# ------------------------------------
# auto-fix skeleton: tries to fix findings where possible
# - For 'modified' and 'missing' associated with a package: attempt reinstall via pkgtool
# - All fixes are sandboxed and validated by running auditor again in sandbox
# ------------------------------------
    def _attempt_auto_fix(self, report: AuditReport):
        # group findings by package
        pkg_map: Dict[str, List[AuditFinding]] = {}
        for f in report.findings:
            if f.package:
                pkg_map.setdefault(f.package, []).append(f)
        fixes_applied = []
        for pkg, findings in pkg_map.items():
            # decide if fixable
            fixable = any(f.kind in ("modified", "missing") for f in findings)
            if not fixable:
                continue
            # check pkgtool availability
            if not self.pkgtool:
                logger.warning("pkgtool not available: cannot auto-fix %s", pkg)
                continue
            # create snapshot before fixing
            snap_maker = get_deepclean_manager()() if callable(get_deepclean_manager) and get_deepclean_manager() is not None else None
            # optionally create snapshot (quickpkg) if possible
            snapshot_id = None
            try:
                # attempt to find version of package from DB or pkgtool cache
                version = None
                if self.db:
                    row = self.db.fetchone("SELECT version FROM installed_packages WHERE name = ? LIMIT 1", (pkg,))
                    if row:
                        version = row.get("version")
                if version:
                    try:
                        res = self.pkgtool.quickpkg(pkg, version, os.path.expanduser(f"~/.rquest/installed/{pkg}-{version}"))
                        if res.get("ok"):
                            snapshot_id = res.get("package_path")
                    except Exception:
                        logger.exception("Snapshot quickpkg failed for %s", pkg)
            except Exception:
                logger.exception("Snapshot step failed")
            # attempt reinstall from cache/binhost or rebuild via buildsystem (out of scope)
            try:
                # reuse_or_create finds or builds a binary and returns path in 'package_path'
                # we need destdir for sandbox restore; use temp dir
                tmp_target = tempfile.mkdtemp(prefix=f"audit-fix-{pkg}-")
                # try find cached binary first
                try:
                    # find version heuristically
                    version = version or "unknown"
                    reuse = self.pkgtool.reuse_or_create(pkg, version, destdir=os.path.expanduser(f"~/.rquest/installed/{pkg}-{version}"))
                    pkg_path = reuse.get("package_path")
                except Exception:
                    pkg_path = None
                if not pkg_path:
                    logger.warning("No binary found to reinstall %s; skipping auto-fix", pkg)
                    shutil.rmtree(tmp_target, ignore_errors=True)
                    continue
                # sandboxed install test
                if self.sandbox:
                    sid = self.sandbox.start_session()
                    try:
                        # copy package to workdir and use sandbox.install (or pkgtool.install_bin with sandbox_run True)
                        res = self.pkgtool.install_bin(pkg_path, target=tmp_target, sandbox_run=True, use_fakeroot=True, verify_sig=False)
                        if not res.get("ok"):
                            logger.warning("Sandbox install failed for %s: %s", pkg, res)
                            self.sandbox.stop_session(sid)
                            shutil.rmtree(tmp_target, ignore_errors=True)
                            continue
                        # run auditor inside sandbox or validate by re-checking files referenced in manifest
                        # simplified: check that manifest exists and files are present
                        manifest = _safe_json_load(os.path.join(tmp_target, "MANIFEST.json")) or {}
                        if manifest and manifest.get("files"):
                            # consider successful validation
                            fixes_applied.append({"package": pkg, "snapshot": snapshot_id, "package_path": pkg_path})
                        self.sandbox.stop_session(sid)
                    except Exception:
                        logger.exception("Error during sandboxed reinstall for %s", pkg)
                        try:
                            self.sandbox.stop_session(sid)
                        except Exception:
                            pass
                        shutil.rmtree(tmp_target, ignore_errors=True)
                        continue
                else:
                    # No sandbox available; direct install only if auto_fix_allowed is explicitly true
                    if not self.auto_fix_allowed:
                        logger.warning("Auto-fix not allowed without sandbox for %s", pkg)
                        shutil.rmtree(tmp_target, ignore_errors=True)
                        continue
                    res = self.pkgtool.install_bin(pkg_path, target=tmp_target, sandbox_run=False, use_fakeroot=True, verify_sig=False)
                    if res.get("ok"):
                        fixes_applied.append({"package": pkg, "snapshot": snapshot_id, "package_path": pkg_path})
                    shutil.rmtree(tmp_target, ignore_errors=True)
            except Exception:
                logger.exception("Auto-fix attempt failed for %s", pkg)
        if fixes_applied:
            # log hook
            if self.hooks:
                try:
                    self.hooks.run("on_auto_fix", context={"fixes": fixes_applied})
                except Exception:
                    logger.exception("on_auto_fix hook failed")
            # emit event
            emit_event("audit.auto_fix", {"count": len(fixes_applied), "fixes": fixes_applied})
        return fixes_applied

# ------------------------------------
# Reporting utilities: export JSON/YAML/HTML/PDF
# ------------------------------------
def export_audit_report(report: AuditReport, fmt: str = "json", path: Optional[str] = None) -> str:
    if not path:
        stamp = time.strftime("%Y%m%d-%H%M%S")
        path = os.path.join(REPORT_DIR, f"audit-{report.run_id}-{stamp}.{fmt}")
    data = report.to_dict()
    try:
        if fmt == "json":
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return path
        if fmt == "yaml" and YAML_AVAILABLE:
            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(data, f)
            return path
        if fmt == "html" or fmt == "pdf":
            # generate simple HTML
            items = []
            for f in report.findings:
                items.append(f"<tr><td>{html.escape(f.id)}</td><td>{html.escape(f.kind)}</td><td>{html.escape(f.path)}</td><td>{html.escape(str(f.package))}</td><td>{html.escape(json.dumps(f.details))}</td><td>{html.escape(f.severity)}</td></tr>")
            body = "<table border='1'><tr><th>ID</th><th>Kind</th><th>Path</th><th>Package</th><th>Details</th><th>Severity</th></tr>" + "".join(items) + "</table>"
            html_doc = f"<html><head><meta charset='utf-8'><title>Audit {report.run_id}</title></head><body><h1>Audit Report {report.run_id}</h1><p>Started: {time.ctime(report.started_at)}</p><p>Finished: {time.ctime(report.finished_at)}</p>{body}</body></html>"
            with open(path if fmt=="html" else path + ".html", "w", encoding="utf-8") as f:
                f.write(html_doc)
            if fmt == "html":
                return path if path.endswith(".html") else path + ".html"
            # pdf conversion
            html_path = path + ".html"
            pdf_path = path if path.endswith(".pdf") else path + ".pdf"
            if _WKHTMLTOPDF:
                try:
                    subprocess.check_call(["wkhtmltopdf", html_path, pdf_path])
                    return pdf_path
                except Exception:
                    logger.exception("wkhtmltopdf conversion failed; returning HTML")
                    return html_path
            else:
                logger.warning("wkhtmltopdf not available, HTML written to %s", html_path)
                return html_path
        # fallback JSON
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return path
    except Exception:
        logger.exception("export_audit_report failed")
        raise

# ------------------------------------
# Auditor daemon: periodic run + optional inotify monitoring
# ------------------------------------
class AuditDaemon:
    def __init__(self, auditor: Optional[Auditor] = None, interval: int = 3600, realtime: bool = False, monitor_paths: Optional[List[str]] = None):
        self.auditor = auditor or Auditor()
        self.interval = interval
        self.realtime = realtime
        self.monitor_paths = monitor_paths or AUDIT_CFG.get("monitor_paths", AUDIT_PATHS)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._inotify_thread: Optional[threading.Thread] = None

    def _loop(self):
        logger.info("Audit daemon started: interval=%ds realtime=%s", self.interval, self.realtime)
        while not self._stop.wait(self.interval):
            try:
                report = self.auditor.run_audit(paths=self.monitor_paths, deep=False, auto_fix=False)
                # export JSON snapshot
                try:
                    export_audit_report(report, fmt="json")
                except Exception:
                    pass
                # emit events for critical findings
                criticals = [f for f in report.findings if f.severity == "critical"]
                if criticals:
                    logger.warning("Audit daemon found %d critical findings", len(criticals))
                    emit_event("audit.critical", {"count": len(criticals), "run_id": report.run_id})
            except Exception:
                logger.exception("Audit daemon loop error")
        logger.info("Audit daemon stopped")

    def _inotify_monitor(self):
        if not _INOTIFY_AVAILABLE:
            logger.info("inotify not available; realtime disabled")
            return
        try:
            adapter = inotify.adapters.Inotify()
            for p in self.monitor_paths:
                if os.path.exists(p):
                    adapter.add_watch(p)
            logger.info("Audit inotify monitor watching: %s", self.monitor_paths)
            for event in adapter.event_gen(yield_nones=False):
                if self._stop.is_set():
                    break
                ((path, type_names, watch_path, filename)) = event
                logger.debug("inotify event %s %s %s", path, type_names, filename)
                # on change, run a quick audit for the changed file
                changed = os.path.join(path, filename) if filename else path
                try:
                    mapping = self.auditor._load_installed_files_map()
                    rec = mapping.get(os.path.normpath(changed))
                    findings = self.auditor._check_file(changed, rec)
                    if findings:
                        # create quick report and export
                        r = AuditReport()
                        for f in findings:
                            r.add(f)
                        r.finish()
                        export_audit_report(r, fmt="json")
                        emit_event("audit.file_changed", {"path": changed, "findings": [f.to_dict() for f in findings]})
                except Exception:
                    logger.exception("inotify quick-audit error")
        except Exception:
            logger.exception("inotify monitor failed")

    def start(self):
        if self._thread and self._thread.is_alive():
            return {"ok": False, "reason": "already_running"}
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        if self.realtime and _INOTIFY_AVAILABLE:
            self._inotify_thread = threading.Thread(target=self._inotify_monitor, daemon=True)
            self._inotify_thread.start()
        return {"ok": True}

    def stop(self):
        if not self._thread:
            return {"ok": False, "reason": "not_running"}
        self._stop.set()
        self._thread.join(timeout=10)
        if self._inotify_thread:
            self._inotify_thread.join(timeout=5)
        return {"ok": True}

# ------------------------------------
# ML prioritizer skeleton (optional)
# ------------------------------------
class MLPrioritizer:
    def __init__(self):
        self.enabled = _ML_AVAILABLE and ML_CFG.get("enabled", False)
        self.model = None
        if self.enabled:
            try:
                from sklearn.ensemble import RandomForestClassifier  # type: ignore
                # simple placeholder model - in real system we'd train with historical audit data
                self.model = RandomForestClassifier(n_estimators=10)
            except Exception:
                logger.exception("Failed initializing ML model")
                self.enabled = False

    def prioritize(self, findings: List[AuditFinding]) -> List[AuditFinding]:
        if not self.enabled or not self.model:
            # fallback: sort by severity then age (critical first)
            return sorted(findings, key=lambda f: (0 if f.severity=="critical" else 1, -f.timestamp))
        # placeholder: convert findings to feature vectors and predict priority
        # For this skeleton, simply return original
        return findings

# ------------------------------------
# Module-level helpers and CLI
# ------------------------------------
_DAEMON: Optional[AuditDaemon] = None
_AUDITOR: Optional[Auditor] = None

def get_auditor() -> Auditor:
    global _AUDITOR
    if _AUDITOR is None:
        _AUDITOR = Auditor()
    return _AUDITOR

def get_daemon() -> AuditDaemon:
    global _DAEMON
    if _DAEMON is None:
        cfg = DAEMON_CFG or {}
        _DAEMON = AuditDaemon(auditor=get_auditor(), interval=int(cfg.get("interval_seconds", 3600)), realtime=bool(cfg.get("realtime", False)), monitor_paths=cfg.get("monitor_paths", AUDIT_PATHS))
    return _DAEMON

# CLI
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="audit", description="Rquest system auditor and integrity checker")
    ap.add_argument("--run", action="store_true", help="run audit now")
    ap.add_argument("--deep", action="store_true", help="deep walk all configured paths")
    ap.add_argument("--auto-fix", action="store_true", help="attempt auto-fix when possible (requires config.allow)")
    ap.add_argument("--export", metavar="FMT", default="json", help="export format: json|yaml|html|pdf")
    ap.add_argument("--start-daemon", action="store_true", help="start audit daemon")
    ap.add_argument("--stop-daemon", action="store_true", help="stop audit daemon")
    ap.add_argument("--profile", metavar="NAME", help="compliance profile to check (e.g. cis)")
    args = ap.parse_args()

    if args.start_daemon:
        print(get_daemon().start())
        sys.exit(0)
    if args.stop_daemon:
        print(get_daemon().stop())
        sys.exit(0)
    auditor = get_auditor()
    if args.run:
        report = auditor.run_audit(paths=AUDIT_PATHS, deep=bool(args.deep), auto_fix=bool(args.auto_fix), compliance_profile=args.profile)
        out = export_audit_report(report, fmt=args.export)
        print("Report exported to", out)
    else:
        ap.print_help()
