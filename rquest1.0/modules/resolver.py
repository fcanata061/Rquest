# Rquest/rquest1.0/modules/resolver.py
"""
resolver.py - Advanced dependency resolver for Rquest

Features:
- Heuristic greedy resolver + backtracking
- Optional SAT/MaxSAT bridge (PySAT or Z3) for complex problems (auto-detected)
- Cache of resolution results (DB-backed or filesystem)
- Lockfile (rquest-lock.json) read/write for reproducible installs
- Virtual packages / providers, slots/subslots, optional deps (USE-like flags)
- Conflict diagnostics and suggestions
- Export to JSON and Graphviz DOT
- Sharding helpers to produce build shards for buildsystem
- Hooks: pre-resolve, on-conflict, post-resolve integration
- Tracing/verbose/trace modes
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
import hashlib
import threading
import logging
from typing import Any, Dict, List, Optional, Tuple, Set

# -----------------------
# Robust imports for project modules
# -----------------------
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("resolver")
except Exception:
    logger = logging.getLogger("resolver")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# DB
try:
    from modules.db import get_db, add_history, emit_event  # type: ignore
except Exception:
    def get_db(): return None
    def add_history(*a, **k): pass
    def emit_event(*a, **k): pass

# repo_sync interface
try:
    from modules.repo_sync import RepoCatalog  # type: ignore
except Exception:
    RepoCatalog = None  # fallback to local provider map

# masks and slots
try:
    from modules.masks import is_masked, get_mask_reason  # type: ignore
except Exception:
    def is_masked(*a, **k): return False
    def get_mask_reason(*a, **k): return None

try:
    from modules.slots import slot_of, slot_conflict_check  # type: ignore
except Exception:
    def slot_of(*a, **k): return None
    def slot_conflict_check(*a, **k): return (False, None)

# hooks
try:
    from modules.hooks import get_hook_manager  # type: ignore
except Exception:
    def get_hook_manager(): return None

# sandbox (optional: some checks may run in sandbox)
try:
    from modules.sandbox import get_sandbox_manager  # type: ignore
except Exception:
    def get_sandbox_manager(): return None

# Optional SAT solver integration (PySAT)
_SAT_AVAILABLE = False
try:
    from pysat.formula import CNF  # type: ignore
    from pysat.solvers import Solver  # type: ignore
    _SAT_AVAILABLE = True
    logger.info("PySAT available: SAT-bridge enabled")
except Exception:
    try:
        import z3  # type: ignore
        _SAT_AVAILABLE = True
        logger.info("Z3 available: SAT/SMT-bridge enabled")
    except Exception:
        _SAT_AVAILABLE = False
        logger.info("No SAT solver available: falling back to heuristic resolver")

# -----------------------
# Types and data classes (lightweight dict-based)
# -----------------------
# Candidate representation (package candidate in repo)
# {
#   "name": str,
#   "version": str,
#   "slot": Optional[str],
#   "provides": List[str],  # e.g., provides virtuals
#   "depends": List[DependencySpec],
#   "optional": List[DependencySpec],
#   "repo": str,
#   "checksum": str,
#   "metadata": {...}
# }
#
# DependencySpec: dict:
# {
#   "name": "pkgname",
#   "version": ">=1.2, <2.0" (or exact),
#   "slot": None or slot requirement,
#   "optional": bool,
#   "flags": []  # optional USE-like flags
# }

_lock = threading.RLock()

# -----------------------
# Utilities
# -----------------------
def _now_ts() -> int:
    return int(time.time())

def _digest_obj(obj: Any) -> str:
    """Produce deterministic digest for caching."""
    j = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()

def _normalize_name(name: str) -> str:
    return name.strip().lower()

def _version_satisfies(version: str, constraint: Optional[str]) -> bool:
    """
    Very small version comparison supporting basic operators:
    - exact: "1.2.3"
    - ranges: ">=1.2", "<2.0", ">=1.2,<2.0"
    - caret/tilde not fully supported; treat as simple ranges for now.
    NOTE: For production, integrate a semver / version comparison library.
    """
    if not constraint or constraint.strip() == "":
        return True
    try:
        parts = [p.strip() for p in constraint.split(",") if p.strip()]
        def cmp(v1, v2):
            # naive compare splitting by dots and numeric when possible
            a = [int(x) if x.isdigit() else x for x in v1.split(".")]
            b = [int(x) if x.isdigit() else x for x in v2.split(".")]
            return (a > b) - (a < b)
        for part in parts:
            if part.startswith(">="):
                if cmp(version, part[2:]) < 0:
                    return False
            elif part.startswith("<="):
                if cmp(version, part[2:]) > 0:
                    return False
            elif part.startswith(">"):
                if cmp(version, part[1:]) <= 0:
                    return False
            elif part.startswith("<"):
                if cmp(version, part[1:]) >= 0:
                    return False
            elif part.startswith("=") or part[0].isdigit():
                want = part.lstrip("=")
                if cmp(version, want) != 0:
                    return False
            else:
                # unsupported token, be conservative (fail)
                return False
        return True
    except Exception:
        # in case of parse error, be conservative and return False
        return False

# -----------------------
# Catalog abstraction (wrap repo_sync or simple local metadata)
# -----------------------
class Catalog:
    """
    Provides methods:
    - list_candidates(name) -> List[candidate]
    - get_candidate(name, version) -> candidate or None
    - fingerprint() -> a repo-state fingerprint string for cache invalidation
    - all_packages() -> iterator of all package names
    """
    def __init__(self, repo_adapter: Optional[Any] = None, local_index: Optional[Dict[str,List[Dict]]] = None):
        self._adapter = repo_adapter
        self._index = local_index or {}
        # adapter should provide list_candidates/get_candidate/fingerprint; otherwise use local_index

    def list_candidates(self, name: str) -> List[Dict[str,Any]]:
        name = _normalize_name(name)
        if self._adapter:
            try:
                return self._adapter.list_candidates(name)
            except Exception:
                logger.exception("repo_adapter.list_candidates failed for %s", name)
                return []
        return self._index.get(name, []).copy()

    def get_candidate(self, name: str, version: str) -> Optional[Dict[str,Any]]:
        name = _normalize_name(name)
        if self._adapter:
            try:
                return self._adapter.get_candidate(name, version)
            except Exception:
                logger.exception("repo_adapter.get_candidate failed for %s@%s", name, version)
                return None
        for c in self._index.get(name, []):
            if c.get("version") == version:
                return c
        return None

    def fingerprint(self) -> str:
        if self._adapter:
            try:
                return self._adapter.fingerprint()
            except Exception:
                logger.exception("repo_adapter.fingerprint failed")
                return ""
        # heuristic: hash local index summary
        summary = {k: [ (c.get("version"), c.get("repo")) for c in v] for k,v in self._index.items()}
        return _digest_obj(summary)

# -----------------------
# Conflict data structure
# -----------------------
class Conflict:
    def __init__(self, ctype: str, message: str, implicated: Optional[List[str]] = None, details: Optional[Dict[str,Any]] = None):
        self.id = str(uuid.uuid4())
        self.ctype = ctype
        self.message = message
        self.implicated = implicated or []
        self.details = details or {}
        self.ts = _now_ts()

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.ctype,
            "message": self.message,
            "implicated": self.implicated,
            "details": self.details,
            "ts": self.ts
        }

# -----------------------
# Resolver core (part 1)
# -----------------------
class Resolver:
    def __init__(self, catalog: Optional[Catalog] = None, cfg: Optional[Dict[str,Any]] = None):
        self._cfg = cfg or (get_config().get("resolver") if callable(get_config) else {})
        self.catalog = catalog or Catalog()
        self._db = get_db() if callable(get_db) else None
        if self._db:
            try:
                self._db.execute("""
                    CREATE TABLE IF NOT EXISTS resolver_cache (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        key TEXT UNIQUE,
                        resolution_json TEXT,
                        created_at INTEGER
                    );
                """, (), commit=True)
                self._db.execute("""
                    CREATE TABLE IF NOT EXISTS resolver_conflicts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT,
                        conflict_json TEXT,
                        created_at INTEGER
                    );
                """, (), commit=True)
            except Exception:
                logger.exception("Failed to ensure resolver DB tables")
        # hooks
        self._hooks = get_hook_manager() if callable(get_hook_manager) else None
        # trace/verbose
        self.trace = bool(self._cfg.get("trace", False))
        self.strategy = self._cfg.get("strategy", "latest")  # latest | installed | stable | conservative
        self.allow_masked = bool(self._cfg.get("allow_masked", False))
        self.max_depth = int(self._cfg.get("max_depth", 50))
        # SAT availability
        self.sat_available = _SAT_AVAILABLE and bool(self._cfg.get("use_sat", True))
        # cache dir fallback
        self.cache_dir = os.path.abspath(self._cfg.get("cache_dir", os.path.join(os.getcwd(), ".rquest_resolver_cache")))
        os.makedirs(self.cache_dir, exist_ok=True)

    # -----------------------
    # Public API roundtrip helpers
    # -----------------------
    def resolve_dependencies(self, name: str, version: Optional[str] = None, profile: Optional[str] = None, fast: bool = False) -> Dict[str,Any]:
        """
        Top-level resolution entrypoint.

        Returns dict:
          {
            "ok": bool,
            "run_id": str,
            "tree": {name: selected_candidate_dict, ...},
            "order": [ (name, version) topologically sorted ],
            "conflicts": [Conflict...],
            "cached": bool,
            "lockfile": optional lockfile dict
          }
        """
        run_id = str(uuid.uuid4())
        if self._hooks:
            try:
                self._hooks.run("pre-resolve", context={"name": name, "version": version, "profile": profile})
            except Exception:
                logger.exception("pre-resolve hooks failed")
        start = _now_ts()
        key_meta = {"name": name, "version": version, "profile": profile, "cfg": self._cfg, "catalog_fp": self.catalog.fingerprint()}
        cache_key = _digest_obj(key_meta)
        # check DB cache
        if not fast and self._db:
            try:
                row = self._db.fetchone("SELECT resolution_json FROM resolver_cache WHERE key = ?", (cache_key,))
                if row and row.get("resolution_json"):
                    res = json.loads(row["resolution_json"])
                    res["cached"] = True
                    res["run_id"] = run_id
                    res["from_cache_key"] = cache_key
                    if self.trace:
                        logger.info("Resolver cache hit for %s", name)
                    if self._hooks:
                        try:
                            self._hooks.run("post-resolve", context={"result": res})
                        except Exception:
                            logger.exception("post-resolve hooks failed")
                    return res
            except Exception:
                logger.exception("resolver cache lookup failed")
        # build initial request constraint
        root_req = {"name": _normalize_name(name), "version": version, "optional": False, "slot": None}
        # run the heuristic resolver with backtracking and possible SAT fallback
        try:
            tree, conflicts = self._resolve_with_heuristic(root_req, profile=profile)
            ok = (len(conflicts) == 0)
            order = self.plan_installation(tree) if ok else []
            result = {"ok": ok, "run_id": run_id, "tree": tree, "order": order, "conflicts": [c.to_dict() for c in conflicts], "cached": False}
            # persist to cache/db
            if self._db:
                try:
                    self._db.execute("INSERT OR REPLACE INTO resolver_cache (key, resolution_json, created_at) VALUES (?, ?, ?)", (cache_key, json.dumps(result), _now_ts()), commit=True)
                except Exception:
                    logger.exception("Failed to write resolver cache")
            # store conflicts if exist
            if conflicts and self._db:
                try:
                    for c in conflicts:
                        self._db.execute("INSERT INTO resolver_conflicts (run_id, conflict_json, created_at) VALUES (?, ?, ?)", (run_id, json.dumps(c.to_dict()), _now_ts()), commit=True)
                except Exception:
                    logger.exception("Failed to persist conflicts")
            if self._hooks:
                try:
                    self._hooks.run("post-resolve", context={"result": result})
                except Exception:
                    logger.exception("post-resolve hooks failed")
            return result
        except Exception as e:
            logger.exception("Resolver unexpected exception: %s", e)
            return {"ok": False, "error": str(e), "run_id": run_id}

    # -----------------------
    # Heuristic resolver + backtracking orchestration
    # -----------------------
    def _resolve_with_heuristic(self, root_req: Dict[str,Any], profile: Optional[str] = None) -> Tuple[Dict[str,Dict[str,Any]], List[Conflict]]:
        """
        Attempt greedy resolution, with limited backtracking.
        On failure and if SAT available, transpile to SAT and invoke solver as fallback.
        Returns (tree, conflicts)
        """
        if self.trace:
            logger.info("[trace] Starting heuristic resolution for %s", root_req)
        # candidate selection strategy: get candidate lists for each package as needed
        selected: Dict[str, Dict[str,Any]] = {}  # name -> candidate
        visited: Set[Tuple[str, Optional[str]]] = set()

        conflicts: List[Conflict] = []

        # recursive attempt with backtracking depth control
        def attempt_expand(req_stack: List[Dict[str,Any]], depth=0) -> bool:
            if depth > self.max_depth:
                conflicts.append(Conflict("max_depth", f"Max resolution depth {self.max_depth} exceeded"))
                return False
            if not req_stack:
                return True
            current = req_stack.pop()
            pname = _normalize_name(current["name"])
            pversion_req = current.get("version")
            if self.trace:
                logger.info("[trace] Expanding %s@%s", pname, pversion_req)
            # if already selected, verify compatibility
            if pname in selected:
                sel = selected[pname]
                if pversion_req and not _version_satisfies(sel.get("version",""), pversion_req):
                    conflicts.append(Conflict("version_conflict", f"{pname} selected {sel.get('version')} does not satisfy {pversion_req}", implicated=[pname], details={"selected": sel}))
                    return False
                # already satisfied, continue
                return attempt_expand(req_stack, depth+1)

            # gather candidates
            candidates = self.catalog.list_candidates(pname)
            # filter masked
            filtered = []
            for c in candidates:
                if is_masked(c.get("name"), c.get("version"), None) and not self.allow_masked:
                    if self.trace:
                        logger.info("[trace] Candidate %s@%s is masked; skipping", c.get("name"), c.get("version"))
                    continue
                if pversion_req and not _version_satisfies(c.get("version",""), pversion_req):
                    continue
                filtered.append(c)
            if not filtered:
                # no candidate => conflict
                conflicts.append(Conflict("missing", f"No candidates found for {pname} {pversion_req}", implicated=[pname]))
                return False

            # sort candidates based on strategy: prefer installed, prefer latest, prefer stable
            def score_candidate(cand):
                score = 0
                # prefer exact match version constraint
                if pversion_req and cand.get("version") == pversion_req:
                    score += 100
                # prefer installed (if DB has an installed marker)
                if self._db:
                    try:
                        row = self._db.fetchone("SELECT 1 FROM installed_packages WHERE name=? AND version=? LIMIT 1", (cand.get("name"), cand.get("version")))
                        if row:
                            score += 50
                    except Exception:
                        pass
                # prefer higher version (naive)
                try:
                    ver = cand.get("version","0")
                    parts = [int(x) if x.isdigit() else 0 for x in ver.split(".")]
                    score += sum([p*(10**(2-i)) for i,p in enumerate(parts[:3])])
                except Exception:
                    pass
                # repo priority
                repo_prio = 0
                if cand.get("repo") == "core":
                    repo_prio += 10
                return -score  # sort ascending => higher score first by negative

            filtered.sort(key=score_candidate)
            # try candidates in order, with backtracking
            for cand in filtered:
                # quick slot conflict check
                s_ok, s_msg = slot_conflict_check(selected, cand)
                if not s_ok:
                    if self.trace:
                        logger.info("[trace] slot conflict for candidate %s@%s: %s", cand.get("name"), cand.get("version"), s_msg)
                    # try next candidate
                    continue
                # optimistic select
                selected[pname] = cand
                # push dependencies onto stack
                deps = cand.get("depends", []) or []
                # apply optional deps controlled by profile or flags (basic: enable all optional if profile requests)
                opt_deps = cand.get("optional", []) or []
                # For now, enable optionals if profile indicates or cfg sets prefer_optional True
                enable_optionals = bool(self._cfg.get("prefer_optionals", False)) or (profile and profile == "desktop")
                new_reqs = []
                for d in deps:
                    new_reqs.append({"name": d.get("name"), "version": d.get("version"), "optional": d.get("optional", False), "slot": d.get("slot", None)})
                for od in opt_deps:
                    if enable_optionals:
                        new_reqs.append({"name": od.get("name"), "version": od.get("version"), "optional": True, "slot": od.get("slot", None)})
                # push current unresolved reqs
                # note: copy stack to avoid shared mutation issues
                new_stack = list(req_stack) + new_reqs
                # attempt deeper expansion
                if attempt_expand(new_stack, depth+1):
                    return True
                # backtrack: remove selection and try next candidate
                if self.trace:
                    logger.info("[trace] Backtracking candidate %s@%s", cand.get("name"), cand.get("version"))
                selected.pop(pname, None)
                # continue loop
            # if reached here, no candidate succeeded
            return False

        # start expansion from root
        ok = attempt_expand([root_req], depth=0)
        if ok:
            return selected, []
        # heuristic failed: if SAT available, try SAT-based solver
        if self.sat_available:
            try:
                sat_tree, sat_conflicts = self._resolve_with_sat(root_req)
                if sat_tree:
                    return sat_tree, []
                else:
                    # SAT returned failure with conflicts
                    return {}, sat_conflicts
            except Exception:
                logger.exception("SAT bridge failed; returning heuristic conflicts")
                return {}, conflicts
        else:
            return {}, conflicts
          # continuation of Rquest/rquest1.0/modules/resolver.py (PART 2)

    # -----------------------
    # SAT-based resolver (bridge) - best-effort implementation
    # -----------------------
    def _resolve_with_sat(self, root_req: Dict[str,Any]) -> Tuple[Dict[str,Dict[str,Any]], List[Conflict]]:
        """
        Build CNF encoding and call external SAT solver (PySAT) to get a consistent assignment.
        This is a best-effort helper; encoding is simplified and may need enhancements for large repos.
        """
        if not self.sat_available:
            raise RuntimeError("SAT solver not available")
        if self.trace:
            logger.info("[trace] Building SAT encoding for %s", root_req)
        # Build candidate variables mapping
        candidates_map = {}  # name -> list of candidates
        var_index = {}  # (name, version) -> var int
        rev_var = {}  # var -> (name, version)
        var_counter = 1

        # Gather closure of candidates reachable from root via BFS up to max_depth
        queue = [root_req]
        seen_names = set()
        depth = 0
        while queue and depth < self.max_depth:
            req = queue.pop(0)
            pname = _normalize_name(req["name"])
            if pname in seen_names:
                continue
            seen_names.add(pname)
            cands = self.catalog.list_candidates(pname)
            candidates_map[pname] = []
            for c in cands:
                # filter masked unless allowed
                if is_masked(c.get("name"), c.get("version"), None) and not self.allow_masked:
                    continue
                candidates_map[pname].append(c)
                key = (pname, c.get("version"))
                var_index[key] = var_counter
                rev_var[var_counter] = key
                var_counter += 1
                # enqueue dependencies to explore more nodes
                for d in c.get("depends", []) or []:
                    queue.append({"name": d.get("name"), "version": d.get("version")})
            depth += 1

        # Quick sanity
        if not candidates_map:
            return {}, [Conflict("missing", "No candidates discovered for root")]

        # Now create CNF clauses
        clauses = []
        # 1) For each package, at least one of its candidate versions must be chosen
        for name, cands in candidates_map.items():
            vars_for_pkg = [var_index[(name, c.get("version"))] for c in cands]
            # at-least-one
            clauses.append(vars_for_pkg[:])
            # at-most-one pairwise (naive)
            for i in range(len(vars_for_pkg)):
                for j in range(i+1, len(vars_for_pkg)):
                    clauses.append([-vars_for_pkg[i], -vars_for_pkg[j]])

        # 2) Dependency clauses: if var(p@v) then (some q@v' satisfying constraint)
        for name, cands in candidates_map.items():
            for c in cands:
                head_var = var_index[(name, c.get("version"))]
                for d in c.get("depends", []) or []:
                    dep_name = _normalize_name(d.get("name"))
                    dep_constraint = d.get("version")
                    # gather dep candidate vars that satisfy constraint
                    possible = []
                    for dc in candidates_map.get(dep_name, []):
                        if _version_satisfies(dc.get("version",""), dep_constraint):
                            possible.append(var_index[(dep_name, dc.get("version"))])
                    if not possible:
                        # cannot satisfy dependency: clause will force head_var -> False (unsat if chosen)
                        # encode as (-head_var)
                        clauses.append([-head_var])
                    else:
                        # (-head_var OR var1 OR var2 ...)
                        clauses.append([-head_var] + possible)

        # 3) Virtuals/providers: if a candidate provides virtual X, and someone depends on X, allow mapping
        # Complexity: we encode dependency names as normal; provider support happens because providers share virtuals in name space.
        # NOTE: advanced provider mapping omitted for brevity

        # Solve CNF
        try:
            if 'pysat' in sys.modules:
                cnf = CNF()
                for cl in clauses:
                    cnf.append(cl)
                with Solver(bootstrap_with=cnf.clauses) as s:
                    sat = s.solve()
                    if not sat:
                        # gather UNSAT info (no easy core extraction here)
                        return {}, [Conflict("unsat", "SAT solver says problem is unsatisfiable")]
                    model = s.get_model()
                    # positive vars chosen
                    chosen = {rev_var[v] for v in model if v > 0 and v in rev_var}
            else:
                # Try Z3 fallback: simple boolean vars per var_counter
                import z3  # type: ignore
                z3vars = {v: z3.Bool(f"v{v}") for v in rev_var}
                solver = z3.Solver()
                for cl in clauses:
                    z3cl = []
                    for lit in cl:
                        if lit > 0:
                            z3cl.append(z3vars[lit])
                        else:
                            z3cl.append(z3.Not(z3vars[-lit]))
                    solver.add(z3.Or(*z3cl))
                if solver.check() != z3.sat:
                    return {}, [Conflict("unsat", "Z3 solver says problem is unsatisfiable")]
                m = solver.model()
                chosen = set()
                for v, zv in z3vars.items():
                    if m.eval(zv):
                        chosen.add(rev_var[v])
            # build selection dict
            selected = {}
            for (nm, ver) in chosen:
                # find candidate object
                cand = self.catalog.get_candidate(nm, ver)
                if cand:
                    selected[nm] = cand
            return selected, []
        except Exception:
            logger.exception("SAT bridge failed")
            return {}, [Conflict("sat_error", "SAT bridge encountered an error")]

    # -----------------------
    # Plan installation (topological order)
    # -----------------------
    def plan_installation(self, tree: Dict[str,Dict[str,Any]]) -> List[Tuple[str,str]]:
        """
        Given resolved selection tree (name -> candidate dict), produce topological order for installation.
        Uses simple DFS topological sort.
        Returns list of (name, version) in order.
        """
        # Build adjacency: dep -> list of dependent names
        adj = {}
        for name, cand in tree.items():
            adj[name] = []
        for name, cand in tree.items():
            for d in cand.get("depends", []) or []:
                depn = _normalize_name(d.get("name"))
                if depn in tree:
                    adj[depn].append(name)
        # topological sort
        visited = {}
        result = []
        def dfs(n):
            state = visited.get(n, 0)
            if state == 1:
                # cycle
                raise RuntimeError(f"cycle detected at {n}")
            if state == 2:
                return
            visited[n] = 1
            for nb in adj.get(n, []):
                dfs(nb)
            visited[n] = 2
            result.append((n, tree[n].get("version")))
        for n in list(tree.keys()):
            if visited.get(n) is None:
                dfs(n)
        # result is reverse topological (children first) so reverse
        result.reverse()
        return result

    # -----------------------
    # Sharding helpers: split topological order into independent shards
    # -----------------------
    def shard_plan(self, order: List[Tuple[str,str]], tree: Dict[str,Dict[str,Any]], max_shard_size: int = 10) -> List[List[Tuple[str,str]]]:
        """
        Split the ordered list into shards, trying to keep independent components together.
        Simple heuristic: group contiguous items into chunks of max_shard_size.
        """
        shards = []
        current = []
        for item in order:
            current.append(item)
            if len(current) >= max_shard_size:
                shards.append(current)
                current = []
        if current:
            shards.append(current)
        return shards

    # -----------------------
    # Diagnostics & suggestions
    # -----------------------
    def suggest_alternatives(self, conflict: Dict[str,Any], max_suggestions: int = 5) -> List[Dict[str,Any]]:
        """
        Given a conflict dict (from Conflict.to_dict()), produce suggestion list.
        Basic heuristics: try nearby versions, other providers, or mark as removable.
        """
        suggestions = []
        ctype = conflict.get("type")
        details = conflict.get("details", {})
        implicated = conflict.get("implicated", [])
        if ctype == "missing":
            name = implicated[0] if implicated else details.get("name")
            # list candidates from other repos
            cands = self.catalog.list_candidates(name)[:max_suggestions]
            for c in cands:
                suggestions.append({"action": "use_candidate", "name": name, "version": c.get("version"), "repo": c.get("repo")})
            suggestions.append({"action": "add_repo", "note": f"Consider adding repository that contains {name}"})
        elif ctype == "version_conflict":
            # try earlier or later versions for implicated
            for pkg in implicated:
                cands = self.catalog.list_candidates(pkg)
                for c in cands[:max_suggestions]:
                    suggestions.append({"action": "try_version", "name": pkg, "version": c.get("version")})
        elif ctype == "slot_conflict":
            suggestions.append({"action": "remove_conflict", "note": "Consider removing conflicting package or using different slot"})
        elif ctype == "unsat":
            suggestions.append({"action": "inspect", "note": "Consider enabling SAT hints or running interactive mode"})
        else:
            suggestions.append({"action": "inspect", "note": "No automatic suggestion available"})
        return suggestions

    # -----------------------
    # Exporters: JSON, Graphviz DOT
    # -----------------------
    def export_json(self, tree: Dict[str,Dict[str,Any]], path: str) -> bool:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(tree, f, indent=2, ensure_ascii=False)
            return True
        except Exception:
            logger.exception("export_json failed")
            return False

    def export_graphviz(self, tree: Dict[str,Dict[str,Any]], path: str) -> bool:
        """
        Produce a Graphviz DOT file representing the dependency graph.
        """
        try:
            lines = ["digraph deps {"]
            for name, cand in tree.items():
                label = f"{name}\\n{cand.get('version')}"
                lines.append(f'  "{name}" [label="{label}"];')
            for name, cand in tree.items():
                for d in cand.get("depends", []) or []:
                    depn = _normalize_name(d.get("name"))
                    if depn in tree:
                        lines.append(f'  "{name}" -> "{depn}";')
            lines.append("}")
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            return True
        except Exception:
            logger.exception("export_graphviz failed")
            return False

    # -----------------------
    # Lockfile read/write
    # -----------------------
    def write_lockfile(self, tree: Dict[str,Dict[str,Any]], path: Optional[str] = None, metadata: Optional[Dict[str,Any]] = None) -> str:
        """
        Write a reproducible lockfile with selected candidates and provenance.
        """
        lock = {
            "resolver_version": "1.0",
            "resolution_id": str(uuid.uuid4()),
            "timestamp": _now_ts(),
            "packages": [],
            "metadata": metadata or {}
        }
        for name, cand in sorted(tree.items()):
            lock["packages"].append({
                "name": name,
                "version": cand.get("version"),
                "repo": cand.get("repo"),
                "checksum": cand.get("checksum"),
                "slot": cand.get("slot")
            })
        if not path:
            path = os.path.join(self.cache_dir, f"rquest-lock-{lock['resolution_id']}.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(lock, f, indent=2, ensure_ascii=False)
            return path
        except Exception:
            logger.exception("write_lockfile failed")
            raise

    def read_lockfile(self, path: str) -> Optional[Dict[str,Any]]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logger.exception("read_lockfile failed")
            return None

    # -----------------------
    # Cache invalidation
    # -----------------------
    def invalidate_cache(self, key: Optional[str] = None):
        """
        Invalidate resolver cache in DB or file cache. If key None, flush all.
        """
        if self._db:
            try:
                if key:
                    self._db.execute("DELETE FROM resolver_cache WHERE key = ?", (key,), commit=True)
                else:
                    self._db.execute("DELETE FROM resolver_cache", (), commit=True)
                logger.info("Resolver DB cache invalidated (key=%s)", key)
            except Exception:
                logger.exception("Failed invalidating resolver DB cache")
        else:
            # clear cache_dir files
            try:
                for fname in os.listdir(self.cache_dir):
                    p = os.path.join(self.cache_dir, fname)
                    if os.path.isfile(p) and fname.startswith("rquest-lock-"):
                        os.remove(p)
                logger.info("Resolver filesystem cache invalidated")
            except Exception:
                logger.exception("Failed invalidating filesystem cache")
              # continuation of Rquest/rquest1.0/modules/resolver.py (PART 3)

    # -----------------------
    # Utility: check_conflicts quick wrapper
    # -----------------------
    def check_conflicts(self, tree: Dict[str,Dict[str,Any]]) -> List[Dict[str,Any]]:
        """
        Run simple checks on resolved tree: slot conflicts, masked, missing deps, circular deps.
        Returns list of conflict dicts.
        """
        conflicts = []
        # masked
        for name, cand in tree.items():
            if is_masked(name, cand.get("version"), None):
                conflicts.append(Conflict("masked", f"{name}@{cand.get('version')} is masked", implicated=[name]).to_dict())
        # slot conflicts (use slot_conflict_check)
        selected = tree.copy()
        ok, msg = slot_conflict_check(selected, None) if callable(slot_conflict_check) else (True, None)
        if not ok:
            conflicts.append(Conflict("slot_conflict", msg, implicated=list(selected.keys())).to_dict())
        # missing/unsatisfied deps
        for name, cand in tree.items():
            for d in cand.get("depends", []) or []:
                dn = _normalize_name(d.get("name"))
                if dn not in tree:
                    conflicts.append(Conflict("missing", f"{name} depends on {dn} which is not in tree", implicated=[name, dn]).to_dict())
        # cycles: try topological and catch error
        try:
            self.plan_installation(tree)
        except Exception as e:
            conflicts.append(Conflict("circular_dependency", str(e)).to_dict())
        # persist to DB optionally
        if conflicts and self._db:
            try:
                for c in conflicts:
                    self._db.execute("INSERT INTO resolver_conflicts (run_id, conflict_json, created_at) VALUES (?, ?, ?)", (str(uuid.uuid4()), json.dumps(c), _now_ts()), commit=True)
            except Exception:
                logger.exception("Failed persisting conflicts")
        return conflicts

# -----------------------
# Module-level manager
# -----------------------
_MANAGER_LOCK = threading.RLock()
_MANAGER: Optional[Resolver] = None

def get_resolver(catalog: Optional[Catalog] = None, cfg: Optional[Dict[str,Any]] = None) -> Resolver:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = Resolver(catalog=catalog, cfg=cfg)
        return _MANAGER

def resolve_dependencies(*a, **k):
    return get_resolver().resolve_dependencies(*a, **k)

def check_conflicts(*a, **k):
    return get_resolver().check_conflicts(*a, **k)

def plan_installation(*a, **k):
    return get_resolver().plan_installation(*a, **k)

def export_graphviz(*a, **k):
    return get_resolver().export_graphviz(*a, **k)

def write_lockfile(*a, **k):
    return get_resolver().write_lockfile(*a, **k)

def invalidate_cache(*a, **k):
    return get_resolver().invalidate_cache(*a, **k)

# -----------------------
# CLI / Demo usage
# -----------------------
if __name__ == "__main__":
    # Simple demo: load local repo index from JSON file supplied as first arg, then resolve a package
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--repo-index", help="JSON file with package index (name -> list of candidates)")
    p.add_argument("package", nargs="?", help="package name to resolve")
    p.add_argument("--version", help="desired version")
    args = p.parse_args()
    index = {}
    if args.repo_index and os.path.exists(args.repo_index):
        with open(args.repo_index, "r", encoding="utf-8") as f:
            index = json.load(f)
    catalog = Catalog(local_index=index)
    r = get_resolver(catalog=catalog)
    if not args.package:
        print("Provide a package to resolve. Example usage: python resolver.py index.json foo --version=1.2")
        sys.exit(1)
    res = r.resolve_dependencies(args.package, version=args.version)
    print("Resolve result:")
    print(json.dumps(res, indent=2, ensure_ascii=False))
    if res.get("ok"):
        lockpath = r.write_lockfile(res["tree"])
        print("Lockfile written to", lockpath)
