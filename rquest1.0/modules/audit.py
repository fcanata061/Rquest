# rquest1.0/modules/audit.py
# -*- coding: utf-8 -*-
"""
Rquest audit module — integridade, permissões e compliance.

Features:
 - read config via modules.config (section "audit")
 - uses modules.db for installed_files and audit_runs
 - audit whole system or per-package (from .meta)
 - export reports: json, yaml, html (+ pdf via wkhtmltopdf if available)
 - audit daemon with optional realtime inotify
 - auto-fix skeleton using pkgtool/buildsystem with sandbox support
 - hooks integration: pre_audit, post_audit, on_integrity_fail, on_auto_fix
"""

from __future__ import annotations
import os
import sys
import time
import json
import uuid
import shutil
import hashlib
import logging
import subprocess
import tempfile
import threading
import html as _html
from pathlib import Path
from typing import Any, Dict, List, Optional

# --- integrations (graceful fallbacks) ---
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("audit")
except Exception:
    logger = logging.getLogger("rquest.audit")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db():
        return None
    def emit_event(*a, **k):
        pass

try:
    from modules.meta import MetaLoader  # type: ignore
except Exception:
    MetaLoader = None

try:
    from modules.pkgtool import get_pkgtool  # type: ignore
except Exception:
    get_pkgtool = None

try:
    from modules.buildsystem import get_buildsystem  # type: ignore
except Exception:
    get_buildsystem = None

try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    get_hook_manager = None

try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    get_sandbox_manager = None

try:
    from modules.deepclean import get_deepcleaner  # type: ignore
except Exception:
    get_deepcleaner = None

# optional libs
try:
    import yaml  # type: ignore
    YAML_AVAILABLE = True
except Exception:
    YAML_AVAILABLE = False

_INOTIFY_AVAILABLE = False
try:
    import inotify.adapters  # type: ignore
    _INOTIFY_AVAILABLE = True
except Exception:
    _INOTIFY_AVAILABLE = False

_WKHTMLTOPDF = bool(shutil.which("wkhtmltopdf"))

# -------------------------
# Utilities
# -------------------------
def _now_ts() -> int:
    return int(time.time())

def _uid(prefix: str = "") -> str:
    import uuid
    return f"{prefix}{uuid.uuid4().hex[:8]}"

def _sha256_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def _ensure_dir(p: str):
    try:
        Path(p).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

# -------------------------
# Load config
# -------------------------
CFG = get_config() if callable(get_config) else {}
AUDIT_CFG = CFG.get("audit", {}) if isinstance(CFG, dict) else {}

DEFAULTS = {
    "paths": ["/usr", "/lib", "/etc", "/bin", "/sbin"],
    "hash_algos": ["sha256"],
    "check_permissions": True,
    "check_owner": True,
    "critical_packages": ["rquest", "glibc", "systemd"],
    "output_dir": os.path.expanduser("~/.rquest/audit_reports"),
    "daemon": {"enabled": False, "interval_seconds": 3600, "realtime": False, "monitor_paths": ["/usr", "/lib", "/etc"]},
    "auto_fix": {"enabled": False, "allowed": False, "preview": True},
    "report_formats": ["json", "yaml", "html"],
}
def _acfg(k, default=None):
    return AUDIT_CFG.get(k, DEFAULTS.get(k, default))

AUDIT_PATHS: List[str] = _acfg("paths")
HASH_ALGOS: List[str] = list(_acfg("hash_algos"))
CHECK_PERMS: bool = bool(_acfg("check_permissions"))
CHECK_OWNER: bool = bool(_acfg("check_owner"))
CRITICAL_PACKAGES: List[str] = _acfg("critical_packages")
REPORT_DIR: str = _acfg("output_dir")
DAEMON_CFG = _acfg("daemon")
AUTO_FIX_CFG = _acfg("auto_fix")
_report_formats = _acfg("report_formats", ["json"])

_ensure_dir(REPORT_DIR)

# -------------------------
# Data models
# -------------------------
class AuditFinding:
    def __init__(self, kind: str, path: str, package: Optional[str], details: Dict[str,Any], severity: str = "warn"):
        self.id = str(uuid.uuid4())
        self.kind = kind  # 'missing', 'modified', 'extra', 'perm', 'owner', 'policy'
        self.path = os.path.normpath(path)
        self.package = package
        self.details = details
        self.severity = severity
        self.ts = _now_ts()

    def to_dict(self):
        return {
            "id": self.id,
            "kind": self.kind,
            "path": self.path,
            "package": self.package,
            "details": self.details,
            "severity": self.severity,
            "ts": self.ts,
        }

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
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "findings": [f.to_dict() for f in self.findings],
            "metadata": self.metadata,
        }

# -------------------------
# Auditor implementation
# -------------------------
class Auditor:
    def __init__(self, cfg: Optional[Dict[str,Any]] = None):
        self.cfg = cfg or AUDIT_CFG
        dbc = get_db() if callable(get_db) else None
        self.db = (dbc() if callable(dbc) else None) if dbc else None
        self.meta_loader = MetaLoader() if MetaLoader else None
        self.pkgtool = (get_pkgtool()() if callable(get_pkgtool) else None) if get_pkgtool else None
        self.buildsystem = (get_buildsystem()() if callable(get_buildsystem) else None) if get_buildsystem else None
        self.hookmgr = (get_hook_manager()() if callable(get_hook_manager) else None) if get_hook_manager else None
        self.sandboxmgr = (get_sandbox_manager()() if callable(get_sandbox_manager) else None) if get_sandbox_manager else None
        self.deepclean = (get_deepcleaner()() if callable(get_deepcleaner) else None) if get_deepcleaner else None
        self.paths = AUDIT_PATHS
        self.hash_algos = HASH_ALGOS
        self.crit_pkgs = CRITICAL_PACKAGES
        self.output_dir = REPORT_DIR
        self.auto_fix_cfg = AUTO_FIX_CFG

        # ensure DB tables exist
        if self.db:
            try:
                self.db.execute("""
                    CREATE TABLE IF NOT EXISTS installed_files (
                        id TEXT PRIMARY KEY,
                        package TEXT,
                        version TEXT,
                        path TEXT,
                        sha256 TEXT,
                        mode INTEGER,
                        uid INTEGER,
                        gid INTEGER
                    )
                """, (), commit=True)
                self.db.execute("""
                    CREATE TABLE IF NOT EXISTS audit_runs (
                        id TEXT PRIMARY KEY,
                        started_at INTEGER,
                        finished_at INTEGER,
                        findings_count INTEGER,
                        metadata TEXT
                    )
                """, (), commit=True)
            except Exception:
                logger.exception("Failed ensure audit DB tables")

    def _load_installed_files_map(self) -> Dict[str, Dict[str,Any]]:
        mapping: Dict[str, Dict[str,Any]] = {}
        if self.db:
            try:
                rows = self.db.fetchall("SELECT package, version, path, sha256, mode, uid, gid FROM installed_files")
                for r in rows:
                    p = os.path.normpath(r.get("path"))
                    mapping[p] = {
                        "package": r.get("package"),
                        "version": r.get("version"),
                        "sha256": r.get("sha256"),
                        "mode": r.get("mode"),
                        "uid": r.get("uid"),
                        "gid": r.get("gid"),
                    }
                logger.debug("Loaded %d installed files from DB", len(mapping))
                return mapping
            except Exception:
                logger.exception("Failed to load installed_files from DB")
        # fallback: scan manifests in ~/.rquest/installed/
        base = Path.home() / ".rquest" / "installed"
        if base.exists():
            for entry in base.iterdir():
                if entry.is_dir():
                    mf = entry / "MANIFEST.json"
                    if mf.exists():
                        try:
                            j = json.loads(mf.read_text(encoding="utf-8"))
                            for f in j.get("files", []):
                                abs_path = os.path.normpath(str(Path("/") / f.get("path").lstrip("/")))
                                mapping[abs_path] = {
                                    "package": j.get("name"),
                                    "version": j.get("version"),
                                    "sha256": f.get("sha256"),
                                    "mode": f.get("mode"),
                                    "uid": f.get("uid"),
                                    "gid": f.get("gid"),
                                }
                        except Exception:
                            logger.exception("Failed parse manifest %s", mf)
        return mapping

    def _check_file(self, path: str, record: Optional[Dict[str,Any]]) -> List[AuditFinding]:
        findings: List[AuditFinding] = []
        if not os.path.exists(path):
            sev = "critical" if record and record.get("package") in self.crit_pkgs else "high"
            findings.append(AuditFinding("missing", path, record.get("package") if record else None, {"note": "file missing"}, severity=sev))
            # hook
            try:
                if self.hookmgr:
                    self.hookmgr.run("on_integrity_fail", {"path": path, "package": record.get("package") if record else None})
            except Exception:
                logger.debug("hook on_integrity_fail failed")
            return findings

        checksums: Dict[str,Optional[str]] = {}
        if "sha256" in self.hash_algos:
            checksums["sha256"] = _sha256_file(path)
        # compare
        if record:
            expected = record.get("sha256")
            if expected and checksums.get("sha256") and expected != checksums.get("sha256"):
                sev = "critical" if record.get("package") in self.crit_pkgs else "high"
                findings.append(AuditFinding("modified", path, record.get("package"), {"expected_sha256": expected, "actual_sha256": checksums.get("sha256")}, severity=sev))
        else:
            # file not tracked => extra
            findings.append(AuditFinding("extra", path, None, {"note": "file not tracked in DB", "checksums": checksums}, severity="info"))

        # permissions / ownership
        try:
            st = os.stat(path)
            if record:
                rmode = record.get("mode")
                ruid = record.get("uid")
                rgid = record.get("gid")
                if CHECK_PERMS and rmode is not None:
                    if int(rmode) != (st.st_mode & 0o777):
                        findings.append(AuditFinding("perm", path, record.get("package"), {"expected": rmode, "actual": oct(st.st_mode & 0o777)}, severity="warn"))
                if CHECK_OWNER and ruid is not None and rgid is not None:
                    if int(ruid) != st.st_uid or int(rgid) != st.st_gid:
                        findings.append(AuditFinding("owner", path, record.get("package"), {"expected_uid": ruid, "expected_gid": rgid, "actual_uid": st.st_uid, "actual_gid": st.st_gid}, severity="warn"))
        except Exception:
            logger.exception("Stat failed for %s", path)

        return findings

    def run_audit(self, paths: Optional[List[str]] = None, deep: bool = False, auto_fix: bool = False, compliance_profile: Optional[str] = None) -> AuditReport:
        run_id = _uid("audit-")
        report = AuditReport(run_id)
        # pre hook
        try:
            if self.hookmgr:
                self.hookmgr.run("pre_audit", {"run_id": run_id, "deep": deep})
        except Exception:
            logger.debug("pre_audit hook failed")
        mapping = self._load_installed_files_map()
        scanned = 0

        if deep:
            scan_paths = paths or self.paths
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
                        scanned += 1
        else:
            for p, rec in mapping.items():
                findings = self._check_file(p, rec)
                for f in findings:
                    report.add(f)
                scanned += 1

        # compliance checks (simple skeleton)
        if compliance_profile:
            try:
                comp = self._check_compliance(compliance_profile)
                for f in comp:
                    report.add(f)
            except Exception:
                logger.exception("Compliance check failed")

        # critical packages validation
        try:
            crits = self._check_critical_packages(mapping)
            for f in crits:
                report.add(f)
        except Exception:
            logger.exception("Critical package check failed")

        report.metadata.update({"scanned": scanned, "paths": paths or self.paths, "deep": deep})
        report.finish()

        # persist run
        if self.db:
            try:
                self.db.execute("INSERT OR REPLACE INTO audit_runs (id, started_at, finished_at, findings_count, metadata) VALUES (?,?,?,?,?)",
                                (report.run_id, report.started_at, report.finished_at, len(report.findings), json.dumps(report.metadata)), commit=True)
            except Exception:
                logger.exception("Failed to persist audit run")

        # post hook
        try:
            if self.hookmgr:
                self.hookmgr.run("post_audit", {"run_id": run_id, "report": report.to_dict()})
        except Exception:
            logger.debug("post_audit hook failed")

        # optionally auto fix
        if auto_fix and self.auto_fix_cfg.get("enabled", False):
            try:
                self.attempt_auto_fix(report)
            except Exception:
                logger.exception("Auto-fix failed")

        # emit event
        emit_event("audit.run.finished", {"run_id": run_id, "findings": len(report.findings)})
        return report

    def audit_package(self, meta_path: str) -> AuditReport:
        if not self.meta_loader:
            raise RuntimeError("MetaLoader not available")
        mp = self.meta_loader.load(meta_path)
        pkg_name = mp.name
        report = AuditReport(_uid("audit-pkg-"))
        try:
            if self.hookmgr:
                self.hookmgr.run("pre_audit", {"package": pkg_name})
        except Exception:
            logger.debug("pre_audit hook failed")
        # get list of files from DB manifest (prefer) else from mp.raw["files"]
        mapping = self._load_installed_files_map()
        files = []
        # If meta contains 'files' field, use it; else use installed_files mapping
        mfiles = mp.raw.get("files") if isinstance(mp.raw, dict) else None
        if mfiles:
            for f in mfiles:
                p = os.path.normpath(str(Path("/") / f.get("path").lstrip("/")))
                rec = mapping.get(p)
                findings = self._check_file(p, rec)
                for fd in findings:
                    report.add(fd)
        else:
            # check DB entries for this package
            for p, rec in mapping.items():
                if rec.get("package") == pkg_name:
                    for fd in self._check_file(p, rec):
                        report.add(fd)

        report.finish()
        try:
            if self.hookmgr:
                self.hookmgr.run("post_audit", {"package": pkg_name, "report": report.to_dict()})
        except Exception:
            logger.debug("post_audit hook failed")
        emit_event("audit.package.finished", {"package": pkg_name, "findings": len(report.findings)})
        return report

    # basic compliance skeleton
    def _check_compliance(self, profile: str) -> List[AuditFinding]:
        findings: List[AuditFinding] = []
        # Example for 'cis' profile
        if profile == "cis":
            if not os.path.exists("/etc/passwd"):
                findings.append(AuditFinding("policy", "/etc/passwd", None, {"rule": "exists"}, severity="critical"))
            # check that sshd is active (best-effort)
            if shutil.which("systemctl"):
                try:
                    rc = subprocess.call(["systemctl", "is-active", "--quiet", "sshd"])
                    if rc != 0:
                        findings.append(AuditFinding("policy", "service:sshd", None, {"rule": "service_active"}, severity="warn"))
                except Exception:
                    pass
        return findings

    def _check_critical_packages(self, mapping: Dict[str, Dict[str,Any]]) -> List[AuditFinding]:
        findings: List[AuditFinding] = []
        for path, rec in mapping.items():
            pkg = rec.get("package")
            if pkg in self.crit_pkgs:
                fds = self._check_file(path, rec)
                findings.extend(fds)
        return findings

    # Auto-fix: try reinstall via pkgtool/buildsystem
    def attempt_auto_fix(self, report: AuditReport) -> List[Dict[str,Any]]:
        results: List[Dict[str,Any]] = []
        if not self.pkgtool:
            logger.warning("pkgtool not available; cannot auto-fix")
            return results
        # group findings by package
        by_pkg: Dict[str, List[AuditFinding]] = {}
        for f in report.findings:
            if f.package:
                by_pkg.setdefault(f.package, []).append(f)
        for pkg, findings in by_pkg.items():
            # only handle missing/modified
            if not any(f.kind in ("missing", "modified") for f in findings):
                continue
            # optionally create snapshot via deepclean or quickpkg
            snapshot_info = None
            try:
                if self.deepclean:
                    # ask deepclean for a snapshot method if available
                    if hasattr(self.deepclean, "create_snapshot_for_package"):
                        snapshot_info = self.deepclean.create_snapshot_for_package(pkg)
            except Exception:
                logger.exception("snapshot creation failed for %s", pkg)

            # try reuse binary or rebuild via buildsystem/pkgtool
            installed_version = None
            if self.db:
                try:
                    row = self.db.fetchone("SELECT version FROM installed_packages WHERE name = ? LIMIT 1", (pkg,))
                    if row:
                        installed_version = row.get("version")
                except Exception:
                    pass

            package_path = None
            # try pkgtool.reuse_or_create if available
            try:
                pt = self.pkgtool
                if hasattr(pt, "reuse_or_create"):
                    res = pt.reuse_or_create(pkg, installed_version or "", destdir=str(Path(tempfile.mkdtemp()) / pkg))
                    if res and res.get("package_path"):
                        package_path = res.get("package_path")
                # if no binary, attempt build via buildsystem
                if not package_path and self.buildsystem and hasattr(self.buildsystem, "build_package"):
                    # try to find meta in repo or installed records
                    meta_path = None
                    if self.meta_loader:
                        # try to find local meta by name (search simple locations)
                        # this is a lightweight heuristic: search cwd and ~/.rquest
                        for p in [Path.cwd(), Path.home() / ".rquest" / "repo"]:
                            for f in p.rglob("*.meta"):
                                try:
                                    m = self.meta_loader.load(str(f))
                                    if m.name == pkg:
                                        meta_path = str(f)
                                        break
                                except Exception:
                                    continue
                            if meta_path:
                                break
                    if meta_path:
                        mp = self.meta_loader.load(meta_path)
                        res = self.buildsystem.build_package(mp.to_dict(), force=True, dry_run=False)
                        if res.get("ok") and res.get("detail", {}).get("artifact"):
                            package_path = res["detail"]["artifact"]
            except Exception:
                logger.exception("Auto-fix build/reuse failed for %s", pkg)

            # Attempt installation (sandboxed if possible)
            if package_path:
                try:
                    if self.sandboxmgr and hasattr(self.sandboxmgr, "start_session"):
                        sess = self.sandboxmgr.start_session()
                        try:
                            # prefer pkgtool.install_bin with sandbox option
                            if hasattr(self.pkgtool, "install_bin"):
                                inst = self.pkgtool.install_bin(package_path, target=None, sandbox_run=True, use_fakeroot=True)
                                results.append({"package": pkg, "installed": inst})
                            else:
                                # fallback: attempt direct install (dangerous)
                                inst = {"ok": False, "error": "no install API"}
                            # stop session
                        finally:
                            try:
                                self.sandboxmgr.stop_session(sess)
                            except Exception:
                                pass
                    else:
                        if not self.auto_fix_cfg.get("allowed", False):
                            logger.warning("Auto-fix not allowed without sandbox for %s", pkg)
                            continue
                        if hasattr(self.pkgtool, "install_bin"):
                            inst = self.pkgtool.install_bin(package_path, target=None, sandbox_run=False, use_fakeroot=True)
                            results.append({"package": pkg, "installed": inst})
                except Exception:
                    logger.exception("Auto-fix install failed for %s", pkg)

                # post-check: re-audit package
                try:
                    ar = self.audit_package(meta_path) if meta_path else None
                    results.append({"package": pkg, "post_audit_findings": len(ar.findings) if ar else None})
                except Exception:
                    logger.exception("Post-audit failed for %s", pkg)
            else:
                logger.warning("No package artifact available to fix %s", pkg)

            # call hook
            try:
                if self.hookmgr:
                    self.hookmgr.run("on_auto_fix", {"package": pkg, "snapshot": snapshot_info, "results": results})
            except Exception:
                logger.debug("on_auto_fix hook failed")

        emit_event("audit.auto_fix_completed", {"count": len(results)})
        return results

# -------------------------
# Reporting utilities
# -------------------------
def export_audit_report(report: AuditReport, fmt: str = "json", path: Optional[str] = None) -> str:
    fmt = (fmt or "json").lower()
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out_dir = REPORT_DIR
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    base = Path(out_dir) / f"audit-{report.run_id}-{stamp}"
    if not path:
        path = str(base) + (".json" if fmt == "json" else (".yaml" if fmt == "yaml" else (".html" if fmt == "html" else ".json")))
    try:
        data = report.to_dict()
        if fmt == "json":
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return path
        if fmt == "yaml" and YAML_AVAILABLE:
            with open(path, "w", encoding="utf-8") as f:
                yaml.safe_dump(data, f)
            return path
        # HTML
        if fmt in ("html", "pdf"):
            rows = []
            for f in report.findings:
                rows.append(f"<tr><td>{_html.escape(f.id)}</td><td>{_html.escape(f.kind)}</td><td>{_html.escape(f.path)}</td><td>{_html.escape(str(f.package or ''))}</td><td>{_html.escape(json.dumps(f.details))}</td><td>{_html.escape(f.severity)}</td></tr>")
            html_doc = f"""<html><head><meta charset="utf-8"><title>Rquest Audit {report.run_id}</title></head><body>
<h1>Rquest Audit</h1>
<p>Run: {report.run_id} started: {time.ctime(report.started_at)} finished: {time.ctime(report.finished_at or _now_ts())}</p>
<table border="1" cellpadding="4" cellspacing="0"><thead><tr><th>ID</th><th>Kind</th><th>Path</th><th>Package</th><th>Details</th><th>Severity</th></tr></thead><tbody>
{''.join(rows)}
</tbody></table></body></html>"""
            html_path = str(Path(path) if path.endswith(".html") else Path(str(path)+".html"))
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_doc)
            if fmt == "pdf":
                if _WKHTMLTOPDF:
                    pdf_path = str(Path(path) if path.endswith(".pdf") else Path(str(path)+".pdf"))
                    try:
                        subprocess.check_call(["wkhtmltopdf", html_path, pdf_path])
                        return pdf_path
                    except Exception:
                        logger.exception("wkhtmltopdf failed; returning html")
                        return html_path
                else:
                    logger.warning("wkhtmltopdf not available; returning html")
                    return html_path
            return html_path
        # fallback JSON
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return path
    except Exception:
        logger.exception("export audit report failed")
        raise

# -------------------------
# Daemon
# -------------------------
class AuditDaemon:
    def __init__(self, auditor: Optional[Auditor] = None, interval: int = 3600, realtime: bool = False, monitor_paths: Optional[List[str]] = None):
        self.auditor = auditor or Auditor()
        self.interval = interval
        self.realtime = realtime
        self.monitor_paths = monitor_paths or AUDIT_PATHS
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._inotify_thread: Optional[threading.Thread] = None

    def _loop(self):
        logger.info("Audit daemon started interval=%s realtime=%s", self.interval, self.realtime)
        while not self._stop.wait(self.interval):
            try:
                report = self.auditor.run_audit(paths=self.monitor_paths, deep=False, auto_fix=False)
                try:
                    export_audit_report(report, fmt="json")
                except Exception:
                    pass
                criticals = [f for f in report.findings if f.severity in ("critical", "high")]
                if criticals:
                    logger.warning("Audit daemon found %d critical/high findings", len(criticals))
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
            logger.info("Audit inotify watching: %s", self.monitor_paths)
            for event in adapter.event_gen(yield_nones=False):
                if self._stop.is_set():
                    break
                try:
                    (_, type_names, path, filename) = event
                    changed = os.path.join(path, filename) if filename else path
                    mapping = self.auditor._load_installed_files_map()
                    rec = mapping.get(os.path.normpath(changed))
                    findings = self.auditor._check_file(changed, rec)
                    if findings:
                        r = AuditReport(_uid("watch-"))
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

# module-level helpers
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

# -------------------------
# CLI
# -------------------------
def _cli(argv=None):
    import argparse
    ap = argparse.ArgumentParser(prog="rquest-audit", description="Rquest audit")
    ap.add_argument("--run", action="store_true", help="run audit now")
    ap.add_argument("--deep", action="store_true", help="deep walk configured paths")
    ap.add_argument("--package", metavar="META", help="audit a single package using .meta path")
    ap.add_argument("--auto-fix", action="store_true", help="attempt auto-fix when possible")
    ap.add_argument("--export", metavar="FMT", default="json", help="export format: json|yaml|html|pdf")
    ap.add_argument("--start-daemon", action="store_true", help="start audit daemon")
    ap.add_argument("--stop-daemon", action="store_true", help="stop audit daemon")
    ap.add_argument("--profile", metavar="NAME", help="compliance profile")
    args = ap.parse_args(argv)
    if args.start_daemon:
        print(get_daemon().start())
        return
    if args.stop_daemon:
        print(get_daemon().stop())
        return
    auditor = get_auditor()
    if args.run or args.package:
        if args.package:
            r = auditor.audit_package(args.package)
        else:
            r = auditor.run_audit(paths=AUDIT_PATHS, deep=args.deep, auto_fix=args.auto_fix, compliance_profile=args.profile)
        out = export_audit_report(r, fmt=args.export)
        print("Report saved to", out)
        return
    ap.print_help()

if __name__ == "__main__":
    _cli(sys.argv[1:])
