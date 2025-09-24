# Rquest/rquest1.0/modules/query_tools.py
"""
query_tools.py - consultas avançadas para Rquest
- qlist, qdepends, qfiles, qwho, qsearch, qvuln, qimpact, qsql, qgrep, qowner
- multi-format output (table/json/yaml)
- caching/indexing, SQL-like queries, graph exports (dot), fuzzy search
- temporal queries using fakeroot snapshots
- ML advisor skeleton for suggestions
- cluster queries via repo_sync
"""

from __future__ import annotations

import os
import sys
import json
import time
import re
import sqlite3
import shutil
import tempfile
import subprocess
import logging
from typing import Any, Dict, List, Optional, Tuple, Iterable, Set
from difflib import get_close_matches

# Optional libs
try:
    import yaml  # type: ignore
    YAML_AVAILABLE = True
except Exception:
    YAML_AVAILABLE = False

try:
    import networkx as nx  # type: ignore
    NX_AVAILABLE = True
except Exception:
    NX_AVAILABLE = False

# ML optional
ML_AVAILABLE = False
try:
    import numpy as _np  # type: ignore
    from sklearn.ensemble import RandomForestClassifier  # type: ignore
    ML_AVAILABLE = True
except Exception:
    ML_AVAILABLE = False

# Integrations (graceful)
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config(): return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("query")
except Exception:
    logger = logging.getLogger("query")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

try:
    from modules.db import get_db  # type: ignore
except Exception:
    def get_db(): return None

try:
    from modules.resolver import get_resolver  # type: ignore
except Exception:
    def get_resolver(): return None

try:
    from modules.meta import MetaLoader  # type: ignore
except Exception:
    MetaLoader = None

try:
    from modules.conflicts import get_detector as get_conflicts_detector  # type: ignore
except Exception:
    def get_conflicts_detector(): return None

try:
    from modules.audit import get_auditor  # type: ignore
except Exception:
    def get_auditor(): return None

try:
    from modules.fakeroot import create_fakeroot  # type: ignore
except Exception:
    create_fakeroot = None

try:
    from modules.repo_sync import get_repo_sync  # type: ignore
except Exception:
    def get_repo_sync(): return None

# Config
CFG = get_config() if callable(get_config) else {}
QT_CFG = CFG.get("query_tools", {}) if isinstance(CFG, dict) else {}
CACHE_DB = os.path.expanduser(QT_CFG.get("cache_db", "~/.rquest/query_cache.sqlite"))
DEFAULT_OUTPUT = QT_CFG.get("default_output", "table")
FZF_THRESHOLD = float(QT_CFG.get("fuzzy_threshold", 0.6))

# Ensure cache db dir
CACHE_DB = os.path.expanduser(CACHE_DB)
os.makedirs(os.path.dirname(CACHE_DB), exist_ok=True)

# -----------------------------
# Lightweight DB wrapper for queries (falls back to modules.db if present)
# -----------------------------
class LocalDB:
    def __init__(self):
        self.conn = sqlite3.connect(CACHE_DB, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._ensure_tables()

    def _ensure_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS packages (
                name TEXT PRIMARY KEY,
                version TEXT,
                category TEXT,
                repo TEXT,
                installed_at INTEGER,
                origin TEXT,
                meta JSON
            );""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                package TEXT,
                sha256 TEXT,
                size INTEGER,
                mode INTEGER,
                owner TEXT,
                groupname TEXT
            );""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS deps (
                package TEXT,
                kind TEXT,
                depends_on TEXT
            );""")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_pkg ON files(package);
        """)
        self.conn.commit()

    def execute(self, sql: str, params: Tuple = (), commit: bool = False):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        if commit:
            self.conn.commit()
        return cur

    def fetchall(self, sql: str, params: Tuple = ()) -> List[Dict[str,Any]]:
        cur = self.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]

    def fetchone(self, sql: str, params: Tuple = ()) -> Optional[Dict[str,Any]]:
        cur = self.execute(sql, params)
        r = cur.fetchone()
        return dict(r) if r else None

# Determine DB backend (prefer project db)
_DB = None
def _get_db():
    global _DB
    if _DB is None:
        try:
            project_db = get_db()() if callable(get_db) else None
            if project_db:
                _DB = project_db
            else:
                _DB = LocalDB()
        except Exception:
            _DB = LocalDB()
    return _DB

# -----------------------------
# Utility output formatters
# -----------------------------
def _to_json(data: Any) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False)

def _to_yaml(data: Any) -> str:
    if YAML_AVAILABLE:
        return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    else:
        return _to_json(data)

def _print_table(rows: List[Dict[str,Any]], cols: Optional[List[str]] = None, max_width: int = 120):
    if not rows:
        print("(empty)")
        return
    if cols is None:
        cols = list(rows[0].keys())
    # compute widths
    widths = {c: max(len(str(c)), max((len(str(r.get(c,""))) ) for r in rows)) for c in cols}
    line = " | ".join(c.ljust(min(widths[c], max_width)) for c in cols)
    print(line)
    print("-" * min(sum(widths.values()) + 3*len(cols), max_width))
    for r in rows:
        print(" | ".join(str(r.get(c,"")).ljust(min(widths[c], max_width)) for c in cols))

# -----------------------------
# Fuzzy search helper
# -----------------------------
def fuzzy_search(query: str, choices: Iterable[str], n: int = 10) -> List[str]:
    # use difflib for fuzzy
    choices_list = list(choices)
    matches = get_close_matches(query, choices_list, n=n, cutoff=0.0)
    # filter by token match heuristics
    filtered = []
    qlow = query.lower()
    for m in matches:
        score = 0.0
        if qlow in m.lower():
            score += 0.6
        # simple similarity ratio fallback using difflib
        # accept those with substring relevance
        filtered.append(m)
    # dedupe and return
    seen = set()
    out = []
    for x in filtered:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out[:n]

# -----------------------------
# ML Advisor skeleton for query tools (recommender)
# -----------------------------
class QueryMLAdvisor:
    def __init__(self, db):
        self.db = db
        self.model = None
        self.enabled = ML_AVAILABLE and bool(QT_CFG.get("ml_enabled", True))
        if self.enabled:
            try:
                self.model = RandomForestClassifier(n_estimators=50)
            except Exception:
                logger.exception("Failed init query ML model")
                self.enabled = False

    def _load_training(self):
        # load examples from fakeroot_ml if present (features,label)
        try:
            rows = self.db.fetchall("SELECT features, label FROM fakeroot_ml")
            X = []
            y = []
            for r in rows:
                try:
                    feat = json.loads(r.get("features") or "{}")
                    X.append([feat.get("num_files",0), feat.get("total_size",0)])
                    y.append(int(r.get("label",0)))
                except Exception:
                    continue
            if len(X) >= int(QT_CFG.get("ml_min_data", 20)):
                return X,y
        except Exception:
            pass
        return None

    def train(self):
        if not self.enabled: return False
        data = self._load_training()
        if not data:
            logger.info("Not enough data to train Query ML advisor")
            return False
        X,y = data
        try:
            self.model.fit(X,y)
            logger.info("Query ML advisor trained")
            return True
        except Exception:
            logger.exception("Query ML training failed")
            return False

    def predict(self, features: Dict[str,Any]) -> float:
        if self.enabled and self.model:
            try:
                x = _np.array([[features.get("num_files",0), features.get("total_size",0)]])
                return float(self.model.predict_proba(x)[0][1])
            except Exception:
                logger.exception("ML predict failed")
        # fallback heuristic
        score = 0.0
        if features.get("total_size",0) > 50*(1024**2): score += 0.5
        if features.get("num_files",0) > 1000: score += 0.3
        return min(1.0, score)
      # continuation PARTE 2/3
# -----------------------------
# Core QueryTools class
# -----------------------------
class QueryTools:
    def __init__(self):
        self.db = _get_db()
        self.resolver = get_resolver()() if callable(get_resolver) else None
        self.meta_loader = MetaLoader()() if callable(MetaLoader) else None
        self.conflicts = get_conflicts_detector()() if callable(get_conflicts_detector) else None
        self.auditor = get_auditor()() if callable(get_auditor) else None
        self.repo_sync = get_repo_sync()() if callable(get_repo_sync) else None
        self.ml = QueryMLAdvisor(self.db) if self.db else None

    # -------------------------
    # qlist: list installed packages with filters
    # -------------------------
    def qlist(self, pattern: Optional[str] = None, category: Optional[str] = None, status: Optional[str] = None, output: str = DEFAULT_OUTPUT, limit: Optional[int] = None, cluster: Optional[str] = None) -> None:
        """
        pattern: regex or name substring or fuzzy
        status: installed/orphans/upgradable
        cluster: if provided, query repo_sync for cluster-wide lists (role or 'all')
        """
        # cluster mode
        if cluster and self.repo_sync:
            try:
                packs = self.repo_sync.cluster_list_packages(cluster)
                rows = [{"name": p.get("name"), "version": p.get("version"), "node": p.get("node")} for p in packs]
                self._output(rows, output)
                return
            except Exception:
                logger.exception("repo_sync cluster_list_packages failed; falling back to local")

        # local
        rows = []
        try:
            if hasattr(self.db, "fetchall"):
                q = "SELECT name, version, category, repo, installed_at, origin FROM packages"
                params = []
                conds = []
                if category:
                    conds.append("category = ?"); params.append(category)
                if status == "orphans":
                    # naive: package not referenced in deps table as depends_on
                    conds.append("name NOT IN (SELECT package FROM deps)")
                if conds:
                    q += " WHERE " + " AND ".join(conds)
                if limit:
                    q += f" LIMIT {int(limit)}"
                rows = self.db.fetchall(q, tuple(params))
            else:
                rows = []
        except Exception:
            logger.exception("qlist DB error")
            rows = []

        # filter pattern (regex or fuzzy)
        if pattern:
            # try regex
            try:
                prog = re.compile(pattern)
                rows = [r for r in rows if prog.search(r.get("name",""))]
            except Exception:
                # fuzzy fallback
                names = [r.get("name") for r in rows]
                matches = fuzzy_search(pattern, names, n=limit or 20)
                rows = [r for r in rows if r.get("name") in matches]
        self._output(rows, output)

    # -------------------------
    # qdepends: dependency tree/graph for package
    # -------------------------
    def qdepends(self, package: str, kind: str = "both", depth: int = 5, format: str = "text", output: Optional[str] = None) -> Any:
        """
        kind: build/runtime/both
        format: text/json/dot
        """
        # prefer resolver if available
        if self.resolver and hasattr(self.resolver, "resolve_tree"):
            try:
                tree = self.resolver.resolve_tree(package, kind=kind, depth=depth)
                # tree assumed hierarchical dict
                if format == "json" or output == "json":
                    self._output(tree, "json")
                    return tree
                elif format == "dot":
                    if NX_AVAILABLE:
                        G = nx.DiGraph()
                        self._add_tree_to_graph(G, tree, package)
                        dot = nx.nx_pydot.to_pydot(G).to_string()  # type: ignore
                        if output == "json":
                            print(json.dumps({"dot": dot}))
                        else:
                            print(dot)
                        return dot
                    else:
                        dot = self._tree_to_dot(tree, package)
                        print(dot)
                        return dot
                else:
                    # pretty-print text
                    self._print_tree(tree)
                    return tree
            except Exception:
                logger.exception("resolver.resolve_tree failed; falling back to DB")
        # fallback: build using deps table
        tree = self._build_tree_from_db(package, kind=kind, depth=depth)
        if format == "json" or output == "json":
            self._output(tree, "json")
            return tree
        elif format == "dot":
            if NX_AVAILABLE:
                G = nx.DiGraph()
                self._add_tree_to_graph(G, tree, package)
                dot = nx.nx_pydot.to_pydot(G).to_string()  # type: ignore
                print(dot)
                return dot
            else:
                dot = self._tree_to_dot(tree, package)
                print(dot)
                return dot
        else:
            self._print_tree(tree)
            return tree

    def _build_tree_from_db(self, pkg: str, kind: str = "both", depth: int = 5):
        # BFS build
        seen = set()
        def node(name, d):
            if d < 0 or name in seen: return None
            seen.add(name)
            children = []
            rows = self.db.fetchall("SELECT depends_on FROM deps WHERE package = ? AND (kind = ? OR ? = 'both')", (name, kind, kind))
            for r in rows:
                dep = r.get("depends_on")
                c = node(dep, d-1)
                if c is not None:
                    children.append(c)
            return {"name": name, "children": children}
        return node(pkg, depth)

    def _print_tree(self, tree: Dict[str,Any], indent: str = ""):
        if not tree:
            print("(empty)")
            return
        name = tree.get("name")
        print(indent + str(name))
        for c in tree.get("children", []) or []:
            self._print_tree(c, indent + "  ")

    def _add_tree_to_graph(self, G, tree, root):
        if not tree:
            return
        name = tree.get("name")
        G.add_node(name)
        for c in tree.get("children", []) or []:
            G.add_edge(name, c.get("name"))
            self._add_tree_to_graph(G, c, root)

    def _tree_to_dot(self, tree, root):
        lines = ["digraph deps {"]
        def walk(t):
            if not t: return
            n = t.get("name")
            for c in t.get("children", []) or []:
                lines.append(f'  "{n}" -> "{c.get("name")}" ;')
                walk(c)
        walk(tree)
        lines.append("}")
        return "\n".join(lines)

    # -------------------------
    # qfiles: list files for package
    # -------------------------
    def qfiles(self, package: str, output: str = DEFAULT_OUTPUT) -> None:
        try:
            rows = self.db.fetchall("SELECT path, sha256, size, mode, owner, groupname FROM files WHERE package = ?", (package,))
            self._output(rows, output)
        except Exception:
            logger.exception("qfiles DB error")
            self._output([], output)

    # -------------------------
    # qwho: find which package provides a file
    # -------------------------
    def qwho(self, filepath: str, fallback_search: bool = True) -> Optional[str]:
        try:
            r = self.db.fetchone("SELECT package FROM files WHERE path = ? LIMIT 1", (filepath,))
            if r:
                print(r.get("package"))
                return r.get("package")
            # fallback: try suffix matching or basename
            if fallback_search:
                b = os.path.basename(filepath)
                r2 = self.db.fetchall("SELECT package, path FROM files WHERE path LIKE ? LIMIT 5", (f"%{b}%",))
                if r2:
                    print(json.dumps(r2, indent=2, ensure_ascii=False))
                    return r2[0].get("package")
        except Exception:
            logger.exception("qwho DB error")
        print("not found")
        return None
      # continuation PARTE 3/3
    # -------------------------
    # qsearch: search packages by name/description/meta with fuzzy fallback and semantic optional
    # -------------------------
    def qsearch(self, query: str, output: str = DEFAULT_OUTPUT, max_results: int = 50, fuzzy: bool = True) -> None:
        # search name exact/like first
        try:
            qlike = f"%{query}%"
            rows = self.db.fetchall("SELECT name, version, category, repo, origin FROM packages WHERE name LIKE ? OR category LIKE ? OR meta LIKE ? LIMIT ?", (qlike, qlike, qlike, max_results))
            if rows:
                self._output(rows, output)
                return
            # fuzzy fallback
            if fuzzy:
                all_names = [r.get("name") for r in self.db.fetchall("SELECT name FROM packages")]
                matches = fuzzy_search(query, all_names, n=max_results)
                rows = []
                for m in matches:
                    r = self.db.fetchone("SELECT name, version, category, repo FROM packages WHERE name = ? LIMIT 1", (m,))
                    if r:
                        rows.append(r)
                self._output(rows, output)
                return
            self._output([], output)
        except Exception:
            logger.exception("qsearch DB error")
            self._output([], output)

    # -------------------------
    # qvuln: query vulnerabilities (requires auditor integration or external feed)
    # -------------------------
    def qvuln(self, package: Optional[str] = None, severity: Optional[str] = None, output: str = DEFAULT_OUTPUT) -> None:
        # prefer auditor module
        try:
            if self.auditor and hasattr(self.auditor, "query_vulnerabilities"):
                res = self.auditor.query_vulnerabilities(package=package, severity=severity)
                self._output(res, output)
                return
        except Exception:
            logger.exception("auditor.query_vulnerabilities failed")
        # fallback to DB table 'vulns' if available
        try:
            if package:
                rows = self.db.fetchall("SELECT cve, severity, description FROM vulns WHERE package = ?", (package,))
            else:
                rows = self.db.fetchall("SELECT package, cve, severity, description FROM vulns")
            # filter severity if provided
            if severity:
                rows = [r for r in rows if r.get("severity") == severity]
            self._output(rows, output)
        except Exception:
            logger.exception("qvuln DB fallback failed")
            self._output([], output)

    # -------------------------
    # qimpact: compute impact of removing/upgrading a package
    # -------------------------
    def qimpact(self, package: str, kind: str = "remove", output: str = DEFAULT_OUTPUT) -> None:
        """
        kind: 'remove'|'upgrade'
        For remove: list reverse-dependents and estimate disruption
        For upgrade: list packages that may need rebuild (reverse-deps)
        """
        impacted = []
        # prefer resolver
        if self.resolver and hasattr(self.resolver, "get_reverse_dependents"):
            try:
                r = self.resolver.get_reverse_dependents(package)
                if isinstance(r, list):
                    impacted = r
            except Exception:
                logger.exception("resolver.get_reverse_dependents failed")
        else:
            try:
                rows = self.db.fetchall("SELECT package FROM deps WHERE depends_on = ?", (package,))
                impacted = list({r.get("package") for r in rows})
            except Exception:
                logger.exception("qimpact DB error")
        # estimate severity heuristics
        severity = "low"
        if len(impacted) > 50:
            severity = "high"
        elif len(impacted) > 10:
            severity = "medium"
        out = {"package": package, "kind": kind, "impacted_count": len(impacted), "impacted": impacted, "severity": severity}
        self._output(out, output)

    # -------------------------
    # qsql: run SQL-like queries against the cache DB (careful)
    # -------------------------
    def qsql(self, sql: str, params: Tuple = (), output: str = "json") -> None:
        try:
            # Only allow read queries for safety (SELECT)
            if not sql.strip().lower().startswith("select"):
                print("Only SELECT queries are allowed")
                return
            rows = self.db.fetchall(sql, params)
            self._output(rows, output)
        except Exception:
            logger.exception("qsql failed")
            print("query error")

    # -------------------------
    # qgrep: grep inside files provided by packages (careful on binary)
    # -------------------------
    def qgrep(self, needle: str, package: Optional[str] = None, ignore_case: bool = True, max_results: int = 100, output: str = DEFAULT_OUTPUT) -> None:
        flags = re.IGNORECASE if ignore_case else 0
        results = []
        try:
            files = []
            if package:
                rows = self.db.fetchall("SELECT path FROM files WHERE package = ?", (package,))
                files = [r.get("path") for r in rows]
            else:
                rows = self.db.fetchall("SELECT path FROM files")
                files = [r.get("path") for r in rows]
            for p in files:
                if len(results) >= max_results:
                    break
                try:
                    with open(p, "r", encoding="utf-8", errors="ignore") as f:
                        for ln, line in enumerate(f, start=1):
                            if re.search(needle, line, flags):
                                results.append({"path": p, "line": ln, "text": line.strip()})
                                break
                except Exception:
                    continue
            self._output(results, output)
        except Exception:
            logger.exception("qgrep failed")
            self._output([], output)

    # -------------------------
    # qowner: owner query - who 'owns' a file path (pkg)
    # -------------------------
    def qowner(self, filepath: str) -> None:
        pkg = self.qwho(filepath, fallback_search=True)
        # qwho prints result already

    # -------------------------
    # watch: simple watch loop to re-run a query
    # -------------------------
    def watch(self, func, interval: int = 2, *args, **kwargs):
        try:
            while True:
                func(*args, **kwargs)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("watch stopped")

    # -------------------------
    # _output helper: route output to selected format
    # -------------------------
    def _output(self, payload: Any, fmt: str = DEFAULT_OUTPUT):
        if fmt == "json":
            print(_to_json(payload))
        elif fmt == "yaml":
            print(_to_yaml(payload))
        else:
            # table / text by default
            if isinstance(payload, list):
                _print_table(payload)
            elif isinstance(payload, dict):
                # print key: val lines
                for k,v in payload.items():
                    print(f"{k}: {v}")
            else:
                print(str(payload))

# -----------------------------
# CLI wiring
# -----------------------------
def _cli():
    import argparse
    ap = argparse.ArgumentParser(prog="qtool", description="Query tools for Rquest")
    sub = ap.add_subparsers(dest="cmd")
    # qlist
    p_list = sub.add_parser("qlist")
    p_list.add_argument("--pattern")
    p_list.add_argument("--category")
    p_list.add_argument("--status")
    p_list.add_argument("--output", choices=["table","json","yaml"], default=DEFAULT_OUTPUT)
    p_list.add_argument("--limit", type=int)
    p_list.add_argument("--cluster")
    # qdepends
    p_dep = sub.add_parser("qdepends")
    p_dep.add_argument("package")
    p_dep.add_argument("--kind", choices=["build","runtime","both"], default="both")
    p_dep.add_argument("--format", choices=["text","json","dot"], default="text")
    # qfiles
    p_files = sub.add_parser("qfiles")
    p_files.add_argument("package")
    p_files.add_argument("--output", choices=["table","json","yaml"], default=DEFAULT_OUTPUT)
    # qwho
    p_who = sub.add_parser("qwho")
    p_who.add_argument("path")
    # qsearch
    p_search = sub.add_parser("qsearch")
    p_search.add_argument("query")
    p_search.add_argument("--output", choices=["table","json","yaml"], default=DEFAULT_OUTPUT)
    # qvuln
    p_vuln = sub.add_parser("qvuln")
    p_vuln.add_argument("--package")
    p_vuln.add_argument("--severity")
    p_vuln.add_argument("--output", choices=["table","json","yaml"], default=DEFAULT_OUTPUT)
    # qimpact
    p_impact = sub.add_parser("qimpact")
    p_impact.add_argument("package")
    p_impact.add_argument("--kind", choices=["remove","upgrade"], default="remove")
    p_impact.add_argument("--output", choices=["table","json","yaml"], default=DEFAULT_OUTPUT)
    # qsql
    p_sql = sub.add_parser("qsql")
    p_sql.add_argument("sql")
    p_sql.add_argument("--output", choices=["json","yaml"], default="json")
    # qgrep
    p_grep = sub.add_parser("qgrep")
    p_grep.add_argument("needle")
    p_grep.add_argument("--package")
    p_grep.add_argument("--output", choices=["table","json","yaml"], default=DEFAULT_OUTPUT)
    # watch wrapper
    p_watch = sub.add_parser("watch")
    p_watch.add_argument("subcmd", help="command to watch, e.g. qlist")
    p_watch.add_argument("--interval", type=int, default=2)
    args = ap.parse_args()

    qt = QueryTools()

    if args.cmd == "qlist":
        qt.qlist(pattern=args.pattern, category=args.category, status=args.status, output=args.output, limit=args.limit, cluster=args.cluster)
    elif args.cmd == "qdepends":
        qt.qdepends(args.package, kind=args.kind, format=args.format)
    elif args.cmd == "qfiles":
        qt.qfiles(args.package, output=args.output)
    elif args.cmd == "qwho":
        qt.qwho(args.path)
    elif args.cmd == "qsearch":
        qt.qsearch(args.query, output=args.output)
    elif args.cmd == "qvuln":
        qt.qvuln(package=args.package, severity=args.severity, output=args.output)
    elif args.cmd == "qimpact":
        qt.qimpact(args.package, kind=args.kind, output=args.output)
    elif args.cmd == "qsql":
        qt.qsql(args.sql, output=args.output)
    elif args.cmd == "qgrep":
        qt.qgrep(args.needle, package=args.package, output=args.output)
    elif args.cmd == "watch":
        # simplistic: support only qlist watch pattern
        if args.subcmd == "qlist":
            def run():
                qt.qlist()
            qt.watch(run, interval=args.interval)
        else:
            print("watch supports only qlist in this CLI helper")
    else:
        ap.print_help()

# run CLI
if __name__ == "__main__":
    _cli()
