# Rquest/rquest1.0/modules/repo_sync.py
"""
repo_sync.py
- Sincroniza múltiplos remotes git que contém pastas com .meta
- Consolida (merge) no repo local (uma árvore de diretórios)
- Valida .meta (YAML/JSON parse), calcula hashes, evita sobrescrever sem mudança
- Emite eventos para db / notifier / audit
- Logs coloridos via modules.logging (se disponível)
- Execução paralela controlada por semáforo
"""

from __future__ import annotations

import os
import sys
import shutil
import json
import time
import hashlib
import tempfile
import threading
import subprocess
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# -------------------------
# Integrations (graceful)
# -------------------------
try:
    from modules.config import get_config
except Exception:
    def get_config():
        # fallback minimal config shape
        return {"repos": {"local": "/var/db/rquest/local-repo", "remotes": []}, "network": {"timeout": 30}}

try:
    from modules.logging import get_logger
    logger = get_logger("repo_sync")
except Exception:
    logger = logging.getLogger("repo_sync")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

try:
    # Meta loader/writer to validate and write nicely
    from modules.meta import MetaLoader, MetaWriter  # type: ignore
except Exception:
    MetaLoader = None
    MetaWriter = None

try:
    from modules.db import get_db, emit_event
except Exception:
    def get_db(): return None
    def emit_event(*a, **k): pass

try:
    from modules.audit import get_auditor
except Exception:
    get_auditor = None

try:
    from modules.update_notifier import build_status_json  # optional use to update bar
except Exception:
    build_status_json = None

# -------------------------
# Helpers
# -------------------------
CFG = get_config() or {}
REPOS_CFG = CFG.get("repos") or {}
LOCAL_REPO_ROOT = Path(REPOS_CFG.get("local") or os.path.expanduser("~/.rquest/local-repo"))
REMOTES = REPOS_CFG.get("remotes") or REPOS_CFG.get("remote") or REPOS_CFG.get("remotes", [])  # tolerate variants

# concurrency
MAX_PARALLEL = int(CFG.get("network",{}).get("parallel_downloads", 4) or 4)

# ensure local repo exists
os.makedirs(LOCAL_REPO_ROOT, exist_ok=True)

# small lock for thread-safe writes
_write_lock = threading.Lock()

def _now_ts() -> int:
    return int(time.time())

def _sha256_of_file(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def _safe_copy(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)

def _run(cmd: List[str], cwd: Optional[Path] = None, timeout: Optional[int] = None) -> Tuple[int, str, str]:
    try:
        proc = subprocess.Popen(cmd, cwd=(str(cwd) if cwd else None), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out, err = proc.communicate(timeout=timeout)
        return proc.returncode, out or "", err or ""
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        return 124, out or "", err or ""

# -------------------------
# Remote structure normalization
# -------------------------
def _normalize_remotes(raw_remotes: Any) -> List[Dict[str,Any]]:
    """
    Accepts either list of dicts or dict mapping names -> dict.
    Normalize to list of remotes:
    { "name":..., "url":..., "branch":..., "type": "git", "dest": <optional> }
    """
    rems: List[Dict[str,Any]] = []
    if not raw_remotes:
        return rems
    if isinstance(raw_remotes, dict):
        # mapping
        for k,v in raw_remotes.items():
            if isinstance(v, str):
                rems.append({"name": k, "type": "git", "url": v, "branch": "main", "sync_to": str(LOCAL_REPO_ROOT)})
            elif isinstance(v, dict):
                entry = dict(v)
                entry.setdefault("name", k)
                entry.setdefault("type", "git")
                entry.setdefault("branch", entry.get("branch","main"))
                entry.setdefault("sync_to", entry.get("sync_to", str(LOCAL_REPO_ROOT)))
                rems.append(entry)
    elif isinstance(raw_remotes, list):
        for e in raw_remotes:
            if isinstance(e, str):
                rems.append({"name": Path(e).stem, "type": "git", "url": e, "branch": "main", "sync_to": str(LOCAL_REPO_ROOT)})
            elif isinstance(e, dict):
                entry = dict(e)
                entry.setdefault("name", entry.get("name") or entry.get("url") or f"remote-{len(rems)}")
                entry.setdefault("type", entry.get("type","git"))
                entry.setdefault("branch", entry.get("branch","main"))
                entry.setdefault("sync_to", entry.get("sync_to", str(LOCAL_REPO_ROOT)))
                rems.append(entry)
    return rems

REMOTES_NORM = _normalize_remotes(REMOTES)
# PART 2 - core sync logic
from concurrent.futures import ThreadPoolExecutor, as_completed

def _ensure_clone(remote: Dict[str,Any], work_dir: Path) -> Tuple[bool, str]:
    """
    Ensure there's a local clone for the remote in work_dir/<name>
    Returns (ok, path_to_clone)
    """
    name = remote.get("name")
    url = remote.get("url")
    branch = remote.get("branch","main")
    repo_dir = work_dir / name
    try:
        if repo_dir.exists() and (repo_dir / ".git").exists():
            # pull
            logger.info(f"[repo_sync] pulling remote {name} ({url}) into {repo_dir}")
            rc, out, err = _run(["git", "fetch", "--all"], cwd=repo_dir, timeout=120)
            if rc != 0:
                logger.warning(f"git fetch failed for {name}: {err.strip()}")
            rc2, out2, err2 = _run(["git", "checkout", branch], cwd=repo_dir, timeout=60)
            # pull latest
            rc3, out3, err3 = _run(["git", "pull", "origin", branch], cwd=repo_dir, timeout=120)
            if rc3 != 0:
                logger.warning(f"git pull failed for {name}: {err3.strip()}")
            return True, str(repo_dir)
        else:
            # clone
            logger.info(f"[repo_sync] cloning {url} into {repo_dir}")
            repo_dir.parent.mkdir(parents=True, exist_ok=True)
            rc, out, err = _run(["git", "clone", "--depth", "1", "--branch", branch, url, str(repo_dir)], timeout=300)
            if rc != 0:
                logger.error(f"git clone failed for {url}: {err.strip()}")
                return False, ""
            return True, str(repo_dir)
    except Exception as e:
        logger.exception("clone/pull failed for remote %s: %s", name, e)
        return False, ""

def _discover_meta_files(repo_path: Path) -> List[Path]:
    """
    Find .meta/.json files under repo_path. Return absolute paths.
    """
    out: List[Path] = []
    for root, dirs, files in os.walk(repo_path):
        for fn in files:
            if fn.endswith(".meta") or fn.endswith(".json") or fn.endswith(".yaml") or fn.endswith(".yml"):
                # naive filter: require filename having name-version.meta (but accept generic)
                p = Path(root) / fn
                out.append(p)
    return out

def _validate_meta_file(path: Path) -> Tuple[bool, Optional[Dict[str,Any]], Optional[str]]:
    """
    Validate meta by parsing YAML/JSON and checking required fields (name, version, source).
    Return (ok, parsed_dict, error_or_none)
    """
    try:
        text = path.read_text(encoding="utf-8")
        parsed = None
        # try JSON
        try:
            parsed = json.loads(text)
        except Exception:
            # try YAML safe_load if available
            try:
                import yaml
                parsed = yaml.safe_load(text)
            except Exception:
                parsed = None
        if not parsed or not isinstance(parsed, dict):
            return False, None, "parse_failed"
        # basic required fields
        if "name" not in parsed or "version" not in parsed:
            return False, parsed, "missing_name_or_version"
        # optional source check
        src = parsed.get("source") or {}
        if not src:
            # allow .meta without source? Permit but warn
            logger.debug("meta %s missing source block", path)
        return True, parsed, None
    except Exception as e:
        return False, None, str(e)

def _merge_one_meta_into_local(src_meta_path: Path, local_root: Path, policy: str = "overwrite_if_newer") -> Dict[str,Any]:
    """
    Merge a single .meta into the local repo.
    Policies:
      - overwrite_if_newer: overwrite if source file mtime > dest mtime or different hash
      - skip_if_exists: do not overwrite existing
      - always_overwrite
    Returns dict with result details.
    """
    try:
        ok, parsed, err = _validate_meta_file(src_meta_path)
        if not ok:
            return {"ok": False, "error": "invalid_meta", "detail": err, "src": str(src_meta_path)}
        # determine destination path inside local (preserve structure after repo root)
        # We will place metas under local_root/<relative_dir_from_repo>/<filename>
        # But src_meta_path is absolute inside the clone. We can't rely on repo root path here.
        # Simplify: create local structure: local_root/<pkgname>/<filename>
        pkgname = parsed.get("name")
        dest_dir = local_root / pkgname
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / src_meta_path.name
        src_hash = _sha256_of_file(src_meta_path)
        dest_hash = _sha256_of_file(dest_file) if dest_file.exists() else None
        # decide action
        action = "skip"
        if not dest_file.exists():
            _safe_copy(src_meta_path, dest_file)
            action = "created"
        else:
            if policy == "skip_if_exists":
                action = "skip"
            elif policy == "always_overwrite":
                _safe_copy(src_meta_path, dest_file)
                action = "overwritten"
            else:
                # overwrite_if_newer
                try:
                    src_mtime = src_meta_path.stat().st_mtime
                    dest_mtime = dest_file.stat().st_mtime
                    if src_hash != dest_hash or src_mtime > dest_mtime:
                        _safe_copy(src_meta_path, dest_file)
                        action = "updated"
                    else:
                        action = "identical"
                except Exception:
                    # fallback to hash compare
                    if src_hash != dest_hash:
                        _safe_copy(src_meta_path, dest_file)
                        action = "updated"
                    else:
                        action = "identical"
        return {"ok": True, "action": action, "src": str(src_meta_path), "dest": str(dest_file), "src_hash": src_hash, "dest_hash": dest_hash}
    except Exception as e:
        logger.exception("merge failed for %s", src_meta_path)
        return {"ok": False, "error": str(e), "src": str(src_meta_path)}
        # PART 3 - orchestration, index, CLI
def sync_remote(remote: Dict[str,Any], tmp_dir: Path, local_root: Path, policy: str = "overwrite_if_newer") -> Dict[str,Any]:
    """
    Clone/pull remote, discover metas and merge into local_root.
    Returns summary dict.
    """
    name = remote.get("name")
    url = remote.get("url")
    summary = {"remote": name, "url": url, "ok": False, "merged": [], "errors": []}
    ok, repo_path = _ensure_clone(remote, tmp_dir)
    if not ok or not repo_path:
        summary["errors"].append("clone_failed")
        return summary
    repo_p = Path(repo_path)
    metas = _discover_meta_files(repo_p)
    logger.info(f"[repo_sync] discovered {len(metas)} meta files in {name}")
    sem_res = []
    # use threadpool to merge metas (IO bound)
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as exc:
        futures = {exc.submit(_merge_one_meta_into_local, p, local_root, policy): p for p in metas}
        for fut in as_completed(futures):
            p = futures[fut]
            try:
                res = fut.result()
                if res.get("ok"):
                    summary["merged"].append(res)
                else:
                    summary["errors"].append(res)
            except Exception as e:
                logger.exception("exception merging %s: %s", p, e)
                summary["errors"].append({"src": str(p), "error": str(e)})
    summary["ok"] = True
    return summary

def index_local_repo(local_root: Path) -> Dict[str,Any]:
    """
    Build a simple index of local repo metas (name -> [versions])
    Persist to local_root/.index.json
    """
    idx: Dict[str, List[str]] = {}
    details: Dict[str,Any] = {}
    for pkg_dir in local_root.iterdir():
        if not pkg_dir.is_dir():
            continue
        versions = []
        for fn in pkg_dir.iterdir():
            if fn.is_file() and (fn.suffix in (".meta", ".json", ".yaml", ".yml")):
                ok, parsed, err = _validate_meta_file(fn)
                if ok and parsed:
                    versions.append(parsed.get("version"))
                    details.setdefault(parsed.get("name"), []).append({"file": str(fn), "version": parsed.get("version"), "sha256": _sha256_of_file(fn)})
        if versions:
            idx[pkg_dir.name] = sorted([v for v in versions if v], key=lambda s: tuple(int(x) for x in s.split(".") if x.isdigit()))
    index_path = local_root / ".index.json"
    try:
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump({"index": idx, "details": details, "ts": _now_ts()}, f, indent=2, ensure_ascii=False)
    except Exception:
        logger.exception("failed write index")
    return {"ok": True, "count": len(idx), "index_path": str(index_path)}

def sync_all(remotes: Optional[List[Dict[str,Any]]] = None, policy: str = "overwrite_if_newer", tmp_base: Optional[Path] = None, emit_events: bool = True) -> Dict[str,Any]:
    """
    Top-level entry: synchronize all remotes and merge into local repo.
    """
    rems = remotes or REMOTES_NORM
    tmp_base = Path(tmp_base) if tmp_base else Path(tempfile.mkdtemp(prefix="rquest-reposync-"))
    tmp_base.mkdir(parents=True, exist_ok=True)
    logger.info(f"[repo_sync] starting sync into {LOCAL_REPO_ROOT} from {len(rems)} remotes")
    results = []
    try:
        with ThreadPoolExecutor(max_workers=min(len(rems) or 1, MAX_PARALLEL)) as exc:
            futures = {exc.submit(sync_remote, r, tmp_base, LOCAL_REPO_ROOT, policy): r for r in rems}
            for fut in as_completed(futures):
                r = futures[fut]
                try:
                    res = fut.result()
                    results.append(res)
                    logger.info(f"[repo_sync] finished sync remote {r.get('name')}: merged {len(res.get('merged',[]))}")
                except Exception as e:
                    logger.exception("sync remote exception %s: %s", r.get("name"), e)
                    results.append({"remote": r.get("name"), "ok": False, "error": str(e)})
        # index local
        idx_res = index_local_repo(LOCAL_REPO_ROOT)
        # optionally emit event and update notifier status JSON
        total_new = sum(len(r.get("merged", [])) for r in results)
        if emit_events:
            try:
                emit_event("repo.sync", {"remotes": [r.get("remote") for r in results], "merged": total_new, "details": results})
            except Exception:
                pass
        # try to call notifier status writer with simple summary
        try:
            updates = []
            for r in results:
                for m in r.get("merged", []):
                    updates.append({"name": Path(m["dest"]).parent.name, "file": m.get("dest")})
            if build_status_json:
                build_status_json(updates, out_file=str(LOCAL_REPO_ROOT / ".repo_updates.json"))
        except Exception:
            logger.exception("failed to build status json")
        return {"ok": True, "tmp": str(tmp_base), "results": results, "index": idx_res}
    finally:
        # cleanup tmp clones
        try:
            shutil.rmtree(tmp_base, ignore_errors=True)
        except Exception:
            pass

# -------------------------
# CLI
# -------------------------
def _cli_main(argv: Optional[List[str]] = None):
    import argparse
    ap = argparse.ArgumentParser(prog="repo_sync", description="Sync remote metas into local repo")
    ap.add_argument("action", nargs="?", default="sync", choices=["sync","status","index"])
    ap.add_argument("--policy", choices=["overwrite_if_newer","skip_if_exists","always_overwrite"], default="overwrite_if_newer")
    ap.add_argument("--tmp", help="temporary base dir for clones")
    ap.add_argument("--no-events", action="store_true", help="do not emit events")
    args = ap.parse_args(argv)
    if args.action == "sync":
        res = sync_all(policy=args.policy, tmp_base=Path(args.tmp) if args.tmp else None, emit_events=(not args.no_events))
        print(json.dumps(res, indent=2, ensure_ascii=False))
    elif args.action == "index":
        res = index_local_repo(LOCAL_REPO_ROOT)
        print(json.dumps(res, indent=2, ensure_ascii=False))
    elif args.action == "status":
        idxf = LOCAL_REPO_ROOT / ".index.json"
        if idxf.exists():
            print(idxf.read_text(encoding="utf-8"))
        else:
            print(json.dumps({"ok": False, "error": "no index"}))

if __name__ == "__main__":
    _cli_main()
