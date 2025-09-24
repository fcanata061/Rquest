# Rquest/rquest1.0/modules/update_notifier.py
"""
update_notifier.py
- verifica upstreams (http/https, ftp, git, rsync, custom) para pacotes do repositório
- identifica novas versões, cria .meta automáticos em /extra/<pkg>/ (multiversion)
- integra com meta.py, fetcher.py, repo_sync.py, db.py, audit.py
- notifica via notify-send (dbus) e imprime no terminal
- escreve arquivo JSON para status-bar (total updates, security updates, detalhes)
- scheduler/daemon leve, comando CLI para checar, listar, gerar meta, status-bar output
- cache e politicas configuráveis via config.yaml
"""

from __future__ import annotations

import os
import sys
import json
import time
import re
import shutil
import hashlib
import tempfile
import threading
import subprocess
import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen, Request
from html.parser import HTMLParser
from ftplib import FTP, error_perm

# ----------------------
# Integrations (graceful)
# ----------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config(): return {}

try:
    from modules.meta import MetaLoader, MetaWriter  # type: ignore
except Exception:
    MetaLoader = None
    MetaWriter = None

try:
    from modules.fetcher import fetch_remote_head  # type: ignore
except Exception:
    fetch_remote_head = None

try:
    from modules.db import get_db, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def emit_event(*a, **k): pass

try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    get_auditor = None

try:
    from modules.repo_sync import get_repo_sync  # type: ignore
except Exception:
    get_repo_sync = None

# ----------------------
# Config
# ----------------------
CFG = get_config() if callable(get_config) else {}
NOTIF_CFG = CFG.get("notifier", {}) if isinstance(CFG, dict) else {}

DEFAULTS = {
    "interval": 3600,
    "mode": "both",  # desktop | cli | both
    "auto_generate_meta": True,
    "keep_old_versions": 5,
    "status_bar_file": "/tmp/rquest_updates.json",
    "notify_critical_only": False,
    "cache_ttl": 3600,
    "extra_dir": os.path.expanduser("~/Rquest/rquest1.0/extra"),
    "max_parallel_checks": 8,
    "user_agent": "RquestUpdateNotifier/1.0",
}

INTERVAL = int(NOTIF_CFG.get("interval", DEFAULTS["interval"]))
MODE = NOTIF_CFG.get("mode", DEFAULTS["mode"])
AUTO_GEN_META = bool(NOTIF_CFG.get("auto_generate_meta", DEFAULTS["auto_generate_meta"]))
KEEP_OLD = int(NOTIF_CFG.get("keep_old_versions", DEFAULTS["keep_old_versions"]))
STATUS_BAR_FILE = NOTIF_CFG.get("status_bar_file", DEFAULTS["status_bar_file"])
NOTIFY_CRITICAL_ONLY = bool(NOTIF_CFG.get("notify_critical_only", DEFAULTS["notify_critical_only"]))
CACHE_TTL = int(NOTIF_CFG.get("cache_ttl", DEFAULTS["cache_ttl"]))
EXTRA_DIR = os.path.expanduser(NOTIF_CFG.get("extra_dir", DEFAULTS["extra_dir"]))
MAX_PARALLEL = int(NOTIF_CFG.get("max_parallel_checks", DEFAULTS["max_parallel_checks"]))
USER_AGENT = NOTIF_CFG.get("user_agent", DEFAULTS["user_agent"])

os.makedirs(EXTRA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(STATUS_BAR_FILE), exist_ok=True)

# ----------------------
# Logger
# ----------------------
try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("update_notifier")
except Exception:
    logger = logging.getLogger("update_notifier")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# ----------------------
# Simple cache
# ----------------------
_CACHE: Dict[str, Tuple[float, Any]] = {}
def cache_get(key: str):
    ent = _CACHE.get(key)
    if not ent: return None
    ts, val = ent
    if time.time() - ts > CACHE_TTL:
        del _CACHE[key]
        return None
    return val

def cache_set(key: str, value: Any):
    _CACHE[key] = (time.time(), value)

# ----------------------
# HTML index parser (simple)
# ----------------------
class SimpleIndexParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links: List[str] = []
    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            href = None
            for k,v in attrs:
                if k.lower() == "href":
                    href = v
                    break
            if href:
                self.links.append(href)
              # continuation PARTE 2/3
# ----------------------
# Helpers: version extraction and comparison
# ----------------------
_version_re = re.compile(r"(\d+(?:\.\d+)+)")

def extract_versions_from_names(names: List[str]) -> List[str]:
    found = set()
    for n in names:
        m = _version_re.search(n)
        if m:
            found.add(m.group(1))
    return sorted(found, key=lambda s: tuple(int(x) for x in s.split(".")))

def parse_version_from_url(url: str) -> Optional[str]:
    # try extract version token from URL path
    p = urlparse(url).path
    m = _version_re.search(p)
    if m:
        return m.group(1)
    return None

def sha256_of_file(path: str) -> Optional[str]:
    try:
        import hashlib
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def safe_mkdir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

# ----------------------
# Low-level upstream checkers
# ----------------------
def http_list_index(url: str) -> List[str]:
    """
    Try to parse an HTTP directory listing and return candidate filenames/links.
    """
    key = f"http_index:{url}"
    cached = cache_get(key)
    if cached is not None:
        return cached
    headers = {"User-Agent": USER_AGENT}
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=15) as resp:
            content = resp.read().decode(errors="ignore")
            p = SimpleIndexParser()
            p.feed(content)
            links = p.links
            cache_set(key, links)
            return links
    except Exception:
        logger.debug("http_list_index failed for %s", url)
        cache_set(key, [])
        return []

def ftp_list_index(url: str) -> List[str]:
    key = f"ftp_index:{url}"
    cached = cache_get(key)
    if cached is not None:
        return cached
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port or 21
        path = parsed.path or "/"
        ftp = FTP()
        ftp.connect(host, port, timeout=10)
        ftp.login()  # anonymous
        # navigate
        names = []
        try:
            ftp.cwd(path)
            names = ftp.nlst()
        except error_perm:
            # maybe path listing not allowed, try parent
            try:
                parent = os.path.dirname(path)
                ftp.cwd(parent)
                names = ftp.nlst(path)
            except Exception:
                names = []
        ftp.quit()
        cache_set(key, names)
        return names
    except Exception:
        logger.debug("ftp_list_index failed for %s", url)
        cache_set(key, [])
        return []

def git_list_tags(repo_url: str) -> List[str]:
    key = f"git_tags:{repo_url}"
    cached = cache_get(key)
    if cached is not None:
        return cached
    try:
        # use ls-remote --tags
        out = subprocess.check_output(["git", "ls-remote", "--tags", repo_url], stderr=subprocess.DEVNULL, text=True, timeout=20)
        tags = []
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 2:
                ref = parts[1]
                if ref.startswith("refs/tags/"):
                    tag = ref.split("/", 2)[2]
                    tags.append(tag)
        cache_set(key, tags)
        return tags
    except Exception:
        logger.debug("git_list_tags failed for %s", repo_url)
        cache_set(key, [])
        return []

def rsync_list(url: str) -> List[str]:
    key = f"rsync:{url}"
    cached = cache_get(key)
    if cached is not None:
        return cached
    try:
        # rsync --list-only <url>
        out = subprocess.check_output(["rsync", "--list-only", url], stderr=subprocess.DEVNULL, text=True, timeout=20)
        names = []
        for line in out.splitlines():
            # crude parse: last column is filename
            parts = line.strip().split()
            if parts:
                names.append(parts[-1])
        cache_set(key, names)
        return names
    except Exception:
        logger.debug("rsync_list failed for %s", url)
        cache_set(key, [])
        return []

# ----------------------
# High-level check upstream for a package (meta entry)
# meta: dict with fields like name, version, source: {type,url,mirror?}
# returns list of available versions (sorted), and mapping version->source_url (best candidate)
# ----------------------
def check_upstream_versions(meta: Dict[str,Any]) -> Tuple[List[str], Dict[str,str]]:
    """
    Returns (versions_list, version_to_url)
    """
    source = meta.get("source") or {}
    s_type = source.get("type") or "http"
    url = source.get("url")
    versions = []
    vmap: Dict[str,str] = {}
    if not url:
        return versions, vmap

    try:
        if s_type in ("http", "https"):
            # try index listing
            links = http_list_index(url)
            # gather filenames and parse versions
            names = [l.split("/")[-1] for l in links if l and not l.startswith("?")]
            vs = extract_versions_from_names(names)
            for v in vs:
                # find a matching filename containing v
                candidate = None
                for n in names:
                    if v in n:
                        candidate = urljoin(url, n)
                        break
                if candidate:
                    versions.append(v)
                    vmap[v] = candidate
            # also try if url points to a JSON API or releases page (GitHub)
            if "github.com" in url and not versions:
                # try tags via git
                tags = git_list_tags(url)
                for t in tags:
                    m = _version_re.search(t)
                    if m:
                        vv = m.group(1)
                        versions.append(vv)
                        vmap[vv] = f"{url}.git"
        elif s_type == "ftp":
            names = ftp_list_index(url)
            vs = extract_versions_from_names(names)
            for v in vs:
                candidate = None
                for n in names:
                    if v in n:
                        candidate = urljoin(url, n)
                        break
                if candidate:
                    versions.append(v)
                    vmap[v] = candidate
        elif s_type == "git":
            # expect url to be git repo
            tags = git_list_tags(url)
            for t in tags:
                m = _version_re.search(t)
                if m:
                    vv = m.group(1)
                    versions.append(vv)
                    vmap[vv] = f"{url}#tag={t}"
        elif s_type == "rsync":
            names = rsync_list(url)
            vs = extract_versions_from_names(names)
            for v in vs:
                candidate = None
                for n in names:
                    if v in n:
                        candidate = os.path.join(url, n)
                        break
                if candidate:
                    versions.append(v)
                    vmap[v] = candidate
        elif s_type == "custom":
            # allow meta to specify a check_cmd that prints versions (one per line)
            check_cmd = source.get("check_cmd")
            if check_cmd:
                try:
                    out = subprocess.check_output(check_cmd, shell=True, text=True, timeout=30)
                    for line in out.splitlines():
                        line = line.strip()
                        if line:
                            m = _version_re.search(line)
                            if m:
                                vv = m.group(1)
                                versions.append(vv)
                                vmap[vv] = line
                except Exception:
                    logger.debug("custom check_cmd failed for %s", meta.get("name"))
    except Exception:
        logger.exception("check_upstream_versions failed for %s", meta.get("name"))

    # dedupe and sort
    versions = sorted(set(versions), key=lambda s: tuple(int(x) for x in s.split(".")))
    return versions, vmap
  # continuation PARTE 3/3
# ----------------------
# Generate .meta for a new version in extra/<pkg>/
# ----------------------
def generate_meta_for_version(base_meta: Dict[str,Any], version: str, source_url: str, extra_dir: str = EXTRA_DIR) -> Optional[str]:
    """
    Create a new .meta file copying base_meta with updated version and source.
    Compute checksum for tarballs when possible.
    Returns path to created .meta or None on failure.
    """
    pkgname = base_meta.get("name") or "unknown"
    pkg_dir = os.path.join(extra_dir, pkgname)
    safe_mkdir(pkg_dir)
    fname = f"{pkgname}-{version}.meta"
    out_path = os.path.join(pkg_dir, fname)

    meta_copy = dict(base_meta)  # shallow copy
    meta_copy["version"] = version
    meta_copy["source"] = meta_copy.get("source", {}).copy()
    meta_copy["source"]["url"] = source_url

    # attempt to fetch and compute checksum for tarballs (http/ftp)
    checksum = None
    try:
        parsed = urlparse(source_url)
        if parsed.scheme in ("http","https","ftp"):
            # fetch to temp and compute sha256
            tmp = tempfile.mktemp(suffix=os.path.basename(source_url))
            try:
                if parsed.scheme in ("http","https"):
                    headers = {"User-Agent": USER_AGENT}
                    req = Request(source_url, headers=headers)
                    with urlopen(req, timeout=30) as resp, open(tmp, "wb") as out:
                        shutil.copyfileobj(resp, out)
                else:
                    # ftp download
                    ftp = FTP(parsed.hostname, timeout=15)
                    ftp.login()
                    path = parsed.path
                    with open(tmp, "wb") as out:
                        ftp.retrbinary(f"RETR {path}", out.write)
                    ftp.quit()
                checksum = sha256_of_file(tmp)
            except Exception:
                logger.debug("failed download for checksum %s", source_url)
            finally:
                try:
                    if os.path.exists(tmp):
                        os.remove(tmp)
                except Exception:
                    pass
    except Exception:
        pass

    if checksum:
        meta_copy.setdefault("checksums", {})["sha256"] = checksum

    # write out .meta file (if MetaWriter available use it)
    try:
        if MetaWriter:
            mw = MetaWriter()
            mw.write_meta_file(out_path, meta_copy)
        else:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(meta_copy, f, indent=2, ensure_ascii=False)
        # pruning old versions: keep only KEEP_OLD
        try:
            files = [fn for fn in os.listdir(pkg_dir) if fn.startswith(pkgname) and fn.endswith(".meta")]
            if len(files) > KEEP_OLD:
                # sort by version extracted
                def ver_key(fn):
                    m = _version_re.search(fn)
                    if m:
                        return tuple(int(x) for x in m.group(1).split("."))
                    return (0,)
                files_sorted = sorted(files, key=ver_key)
                to_remove = files_sorted[:-KEEP_OLD]
                for rm in to_remove:
                    try:
                        os.remove(os.path.join(pkg_dir, rm))
                    except Exception:
                        pass
        except Exception:
            pass
        logger.info("meta generated: %s", out_path)
        try:
            emit_event("notifier.meta_generated", {"path": out_path, "pkg": pkgname, "version": version})
        except Exception:
            pass
        return out_path
    except Exception:
        logger.exception("generate_meta_for_version failed for %s %s", pkgname, version)
        return None

# ----------------------
# Compare local repo version (from meta) vs upstream versions
# ----------------------
def find_new_versions_for_meta(meta: Dict[str,Any]) -> Dict[str,Any]:
    """
    Returns a dict:
      { "name": .., "current": vcur, "upstream": [v1,v2...], "new": [vn...], "vmap": {...} }
    """
    name = meta.get("name")
    cur = meta.get("version")
    upstream_versions, vmap = check_upstream_versions(meta)
    new = [v for v in upstream_versions if _version_tuple(v) > _version_tuple(cur)]
    return {"name": name, "current": cur, "upstream": upstream_versions, "new": new, "vmap": vmap}

def _version_tuple(v: Optional[str]) -> Tuple[int,...]:
    if not v:
        return (0,)
    try:
        return tuple(int(x) for x in v.split("."))
    except Exception:
        return (0,)

# ----------------------
# Notifier: create system notification (notify-send) and/or CLI print
# ----------------------
def send_notification(title: str, body: str, critical: bool = False):
    if MODE in ("desktop","both"):
        # try notify-send
        try:
            subprocess.run(["notify-send", title, body], check=False)
        except Exception:
            logger.debug("notify-send missing or failed")
    if MODE in ("cli","both"):
        icon = "‼️ " if critical else "ℹ️ "
        print(f"{icon}{title}: {body}")

# ----------------------
# Build status JSON for status bar
# ----------------------
def build_status_json(updates: List[Dict[str,Any]], out_file: str = STATUS_BAR_FILE):
    total = len(updates)
    security = sum(1 for u in updates if u.get("security"))
    payload = {"updates_total": total, "security_updates": security, "packages": updates, "ts": int(time.time())}
    try:
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    except Exception:
        logger.exception("failed write status bar file")

# ----------------------
# Main check loop for the repository: iterate metas and check upstreams
# ----------------------
def load_local_metas(repo_dir: Optional[str] = None) -> List[Dict[str,Any]]:
    """
    Load metas from repo. If MetaLoader available, use it; else scan EXTRA_DIR and repo_dir.
    repo_dir defaults to repository root if provided in config.
    """
    metas = []
    try:
        if MetaLoader:
            ml = MetaLoader()
            metas = ml.load_all_metas()
            return metas
    except Exception:
        logger.debug("MetaLoader not available or failed")
    # fallback: scan EXTRA_DIR for .meta and also a repo_dir if provided
    def load_meta_file(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                if path.endswith(".json") or path.endswith(".meta"):
                    return json.load(f)
                else:
                    # try parse simple key: value
                    return json.load(f)
        except Exception:
            return None
    # scan extra dir
    try:
        for root, dirs, files in os.walk(EXTRA_DIR):
            for fn in files:
                if fn.endswith(".meta") or fn.endswith(".json"):
                    p = os.path.join(root, fn)
                    m = load_meta_file(p)
                    if m:
                        metas.append(m)
    except Exception:
        pass
    # optionally scan repo_dir
    repo_dir = repo_dir or CFG.get("repo_dir")
    if repo_dir:
        try:
            for root, dirs, files in os.walk(repo_dir):
                for fn in files:
                    if fn.endswith(".meta"):
                        p = os.path.join(root, fn)
                        m = load_meta_file(p)
                        if m:
                            metas.append(m)
        except Exception:
            pass
    return metas

# ----------------------
# High-level function checking repository and optionally generating metas
# ----------------------
def check_repository_and_notify(repo_dir: Optional[str] = None, generate_meta: bool = AUTO_GEN_META, limit: Optional[int] = None) -> Dict[str,Any]:
    """
    Scans repository metas, checks upstreams (parallel up to MAX_PARALLEL), returns summary with updates list.
    If generate_meta True, will create .meta files for discovered new versions.
    """
    metas = load_local_metas(repo_dir)
    updates: List[Dict[str,Any]] = []
    lock = threading.Lock()
    sem = threading.Semaphore(MAX_PARALLEL)

    def worker(meta):
        nonlocal updates
        try:
            sem.acquire()
            res = find_new_versions_for_meta(meta)
            if res.get("new"):
                for v in res["new"]:
                    entry = {"name": meta.get("name"), "current": res.get("current"), "new_version": v, "source": res["vmap"].get(v)}
                    # check security via auditor if present
                    security_flag = False
                    try:
                        if get_auditor:
                            auditor = get_auditor()()
                            # auditor.query_vulnerabilities or similar
                            if hasattr(auditor, "is_version_vulnerable"):
                                security_flag = auditor.is_version_vulnerable(meta.get("name"), v)
                    except Exception:
                        pass
                    entry["security"] = security_flag
                    # generate .meta if requested
                    meta_created = None
                    if generate_meta:
                        src_url = res["vmap"].get(v)
                        if src_url:
                            meta_created = generate_meta_for_version(meta, v, src_url)
                            entry["meta_path"] = meta_created
                    with lock:
                        updates.append(entry)
        except Exception:
            logger.exception("worker failed for meta %s", meta.get("name"))
        finally:
            try:
                sem.release()
            except Exception:
                pass

    threads = []
    for m in metas[:limit] if limit else metas:
        t = threading.Thread(target=worker, args=(m,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # sort updates by security then name
    updates_sorted = sorted(updates, key=lambda x: (not x.get("security", False), x.get("name")))
    # send notifications
    # build summary string
    total = len(updates_sorted)
    critical_count = sum(1 for u in updates_sorted if u.get("security"))
    if total > 0:
        summary_title = f"Rquest: {total} updates available"
        summary_body = f"{critical_count} security updates\n"
        for u in updates_sorted[:10]:
            summary_body += f"{u['name']}: {u['current']} → {u['new_version']}\n"
        # choose whether to notify based on NOTIFY_CRITICAL_ONLY
        if (NOTIFY_CRITICAL_ONLY and critical_count>0) or (not NOTIFY_CRITICAL_ONLY):
            send_notification(summary_title, summary_body, critical=(critical_count>0))
    # write status bar file
    build_status_json(updates_sorted, STATUS_BAR_FILE)
    # persist into DB
    try:
        db = get_db()() if callable(get_db) else None
        if db:
            db.execute("""CREATE TABLE IF NOT EXISTS notifier_history (id TEXT PRIMARY KEY, payload JSON, ts INTEGER)""", (), commit=True)
            nid = f"not-{int(time.time())}"
            db.execute("INSERT INTO notifier_history (id, payload, ts) VALUES (?,?,?)", (nid, json.dumps({"total": total, "critical": critical_count, "items": updates_sorted}), int(time.time())), commit=True)
    except Exception:
        logger.exception("failed persist notifier history")
    return {"total": total, "critical": critical_count, "items": updates_sorted}
