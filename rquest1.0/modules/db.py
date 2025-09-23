# Rquest/rquest1.0/modules/db.py
"""
Rquest DB module - db.py

Features:
- SQLite backend (singleton)
- Versioned migrations (meta.schema_version)
- Tables: packages, dependencies, history, builds, shards, events, users, locks, configs
- Build sharding: shards table, worker registration, acquire/release shard for builds, heartbeat
- High-level API for packages, dependencies, builds, events, history, users, locks, config cache
- Transaction context manager for safe transactions with rollback
- Event listeners (callbacks) and async dispatch (threads)
- Integration with config.py (get_config) and logging.py (get_logger)
- Export/import JSON
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
import json
import traceback
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple

# Robust imports from modules package (try several name variations to be flexible)
try:
    from modules.config import get_config, get_db_path, get_buildsystem_config  # type: ignore
except Exception:
    try:
        # alternative names in case module is imported differently
        from config import get_config, get_db_path, get_buildsystem_config  # type: ignore
    except Exception:
        # minimal fallback
        def get_config():
            return {}
        def get_db_path():
            return "./rquest.db"
        def get_buildsystem_config():
            return {"max_shards": 4}

# logging integration (use project's logging if available)
try:
    from modules.logging import get_logger  # type: ignore
except Exception:
    try:
        from logging import getLogger as _std_get_logger  # fallback to stdlib
        def get_logger(name: str):
            return _std_get_logger(name)
    except Exception:
        def get_logger(name: str):
            class _Fake:
                def info(self, *a, **k): pass
                def warning(self, *a, **k): pass
                def error(self, *a, **k): pass
                def debug(self, *a, **k): pass
                def exception(self, *a, **k): pass
            return _Fake()

logger = get_logger("db")

# ---------------------------------------------------------------------------
# Constants and defaults
# ---------------------------------------------------------------------------
DEFAULT_DB_TIMEOUT = 30  # seconds
DEFAULT_MAX_SHARDS = 4
DEFAULT_SCHEMA_VERSION = 1

# Lock to synchronize singleton creation
_singleton_lock = threading.RLock()
_db_singleton = None  # type: ignore

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _now_ts() -> int:
    return int(time.time())

def _iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------

class Database:
    """
    Singleton Database class managing SQLite connection, migrations, sharding of builds,
    events, high-level API and listeners.
    """

    def __new__(cls, *args, **kwargs):
        global _db_singleton
        with _singleton_lock:
            if _db_singleton is None:
                _db_singleton = super(Database, cls).__new__(cls)
        return _db_singleton

    def __init__(self, db_path: Optional[str] = None, timeout: Optional[int] = None):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        cfg = None
        try:
            cfg = get_config()
        except Exception:
            cfg = None

        if db_path is None:
            try:
                db_path = get_db_path() if callable(get_db_path) else "./rquest.db"
            except Exception:
                db_path = "./rquest.db"
        if timeout is None:
            try:
                timeout = getattr(cfg, "db", {}).get("timeout", DEFAULT_DB_TIMEOUT) if cfg else DEFAULT_DB_TIMEOUT
                # if pydantic/dataclass style, try attribute access
                if hasattr(cfg, "db") and not isinstance(timeout, int):
                    timeout = getattr(cfg.db, "timeout", DEFAULT_DB_TIMEOUT)
            except Exception:
                timeout = DEFAULT_DB_TIMEOUT

        self.db_path = os.path.abspath(db_path)
        self.timeout = int(timeout or DEFAULT_DB_TIMEOUT)
        self._conn_lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._listeners: Dict[str, List[Callable[[Dict[str, Any]], None]]] = {}
        self._listener_lock = threading.RLock()
        self._migration_lock = threading.RLock()

        # internal cache for config keys
        self._config_cache: Dict[str, Any] = {}

        # For shard heartbeat management
        self._shard_heartbeat_interval = 10  # seconds
        self._shard_ttl = 30  # seconds (if no heartbeat for > ttl consider shard dead)

        # connect and migrate
        try:
            self.connect()
        except Exception as e:
            logger.exception(f"DB connect failed ({e})")
            raise

    # -------------------------
    # Connection management
    # -------------------------
    def connect(self):
        """Open SQLite connection and ensure schema/migrations."""
        with self._conn_lock:
            if self._conn:
                return
            # Ensure directory exists
            d = os.path.dirname(self.db_path)
            if d and not os.path.exists(d):
                try:
                    os.makedirs(d, exist_ok=True)
                except Exception:
                    pass
            # open connection
            try:
                self._conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
                self._conn.row_factory = sqlite3.Row
                # enable WAL mode for better concurrency
                try:
                    self._conn.execute("PRAGMA journal_mode=WAL;")
                except Exception:
                    pass
                logger.info(f"Connected to DB: {self.db_path}")
            except sqlite3.DatabaseError as e:
                # Try backup & recreate if DB corrupted
                logger.exception("Database error on connect, attempting backup and recreate.")
                try:
                    bak = f"{self.db_path}.bak.{int(time.time())}"
                    os.rename(self.db_path, bak)
                    logger.warning(f"Moved corrupted DB to {bak}")
                except Exception:
                    logger.exception("Failed to move corrupted DB file.")
                # retry create
                self._conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
                self._conn.row_factory = sqlite3.Row
            # run migrations
            self._ensure_migrations()

    def close(self):
        with self._conn_lock:
            if self._conn:
                try:
                    self._conn.close()
                    logger.info("Database connection closed")
                except Exception:
                    logger.exception("Error closing DB")
                finally:
                    self._conn = None

    # -------------------------
    # Transaction helpers
    # -------------------------
    @contextmanager
    def transaction(self):
        """
        Context manager for transactions. Commits if block succeeds, rollback on exception.
        Usage:
            with db.transaction():
                db.execute(...)
        """
        if self._conn is None:
            self.connect()
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN;")
            yield
            self._conn.commit()
        except Exception:
            try:
                self._conn.rollback()
            except Exception:
                pass
            logger.exception("Transaction failed and was rolled back.")
            raise
        finally:
            cur.close()

    def execute(self, sql: str, params: Tuple = (), commit: bool = False) -> sqlite3.Cursor:
        """Execute a SQL statement with optional commit."""
        if self._conn is None:
            self.connect()
        try:
            cur = self._conn.cursor()
            cur.execute(sql, params)
            if commit:
                self._conn.commit()
            return cur
        except Exception:
            logger.exception("SQL execution error: %s -- params=%s", sql, params)
            raise

    def fetchone(self, sql: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
        cur = self.execute(sql, params)
        row = cur.fetchone()
        cur.close()
        return row

    def fetchall(self, sql: str, params: Tuple = ()) -> List[sqlite3.Row]:
        cur = self.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        return rows

    # -------------------------
    # Migrations
    # -------------------------
    def _ensure_migrations(self):
        """
        Ensure meta table exists and migrations up to DEFAULT_SCHEMA_VERSION are applied.
        Future: extend migrations array and run incrementally.
        """
        with self._migration_lock:
            # create meta table if not exists
            self.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """, (), commit=True)
            # read schema version
            row = self.fetchone("SELECT value FROM meta WHERE key = 'schema_version';")
            current = int(row["value"]) if row and row["value"] else 0
            logger.debug(f"Current DB schema version: {current}")
            migrations = self._migrations()
            # apply migrations in order
            for ver, fn in sorted(migrations.items()):
                if ver > current:
                    logger.info(f"Applying migration {ver}")
                    try:
                        with self.transaction():
                            fn(self)
                            # update meta
                            self.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?);", ("schema_version", str(ver)), commit=False)
                        logger.info(f"Migration {ver} applied")
                        current = ver
                    except Exception:
                        logger.exception(f"Failed to apply migration {ver}")
                        raise

    def _migrations(self) -> Dict[int, Callable[['Database'], None]]:
        """
        Return a dict of migration functions keyed by target version.
        Add new migrations here to evolve schema.
        """
        migrations: Dict[int, Callable[['Database'], None]] = {}

        def mig_1(db: 'Database'):
            # initial schema: packages, dependencies, history, builds, shards, events, users, locks, configs
            db.execute("""
                CREATE TABLE IF NOT EXISTS packages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    version TEXT,
                    release INTEGER,
                    arch TEXT,
                    build_date INTEGER,
                    status TEXT,
                    UNIQUE(name)
                );
            """, (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_packages_name ON packages(name);", (), commit=False)

            db.execute("""
                CREATE TABLE IF NOT EXISTS dependencies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    package_name TEXT NOT NULL,
                    depends_on TEXT NOT NULL,
                    type TEXT DEFAULT 'runtime'
                );
            """, (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_deps_pkg ON dependencies(package_name);", (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_deps_dep ON dependencies(depends_on);", (), commit=False)

            db.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    package_name TEXT,
                    action TEXT,
                    message TEXT,
                    user TEXT,
                    timestamp INTEGER DEFAULT (strftime('%s','now'))
                );
            """, (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_history_pkg ON history(package_name);", (), commit=False)

            db.execute("""
                CREATE TABLE IF NOT EXISTS shards (
                    shard_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    worker TEXT,
                    active INTEGER DEFAULT 1,
                    last_heartbeat INTEGER
                );
            """, (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_shards_worker ON shards(worker);", (), commit=False)

            db.execute("""
                CREATE TABLE IF NOT EXISTS builds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    package_name TEXT,
                    version TEXT,
                    status TEXT, -- building, success, fail
                    shard_id INTEGER,
                    log_path TEXT,
                    started_at INTEGER,
                    finished_at INTEGER,
                    FOREIGN KEY(shard_id) REFERENCES shards(shard_id)
                );
            """, (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_builds_pkg ON builds(package_name);", (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_builds_status ON builds(status);", (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_builds_shard ON builds(shard_id);", (), commit=False)

            db.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT,
                    context TEXT, -- JSON
                    timestamp INTEGER DEFAULT (strftime('%s','now'))
                );
            """, (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);", (), commit=False)

            db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    role TEXT,
                    last_action INTEGER
                );
            """, (), commit=False)
            db.execute("CREATE INDEX IF NOT EXISTS idx_users_name ON users(name);", (), commit=False)

            db.execute("""
                CREATE TABLE IF NOT EXISTS locks (
                    resource TEXT PRIMARY KEY,
                    owner TEXT,
                    expires_at INTEGER
                );
            """, (), commit=False)

            db.execute("""
                CREATE TABLE IF NOT EXISTS configs (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """, (), commit=False)

        migrations[1] = mig_1
        # future migrations: migrations[2] = mig_2, etc.
        return migrations

    # -------------------------
    # Event listeners
    # -------------------------
    def on(self, event_name: str, callback: Callable[[Dict[str, Any]], None]):
        """Register a callback for an event. Callback receives a dict context."""
        with self._listener_lock:
            self._listeners.setdefault(event_name, []).append(callback)
        logger.debug(f"Listener registered for event '{event_name}'")

    def _emit(self, event_name: str, context: Dict[str, Any]):
        """Emit event to DB (persist) and dispatch callbacks asynchronously."""
        try:
            # persist event
            self.execute("INSERT INTO events (event_type, context, timestamp) VALUES (?, ?, strftime('%s','now'))", (event_name, json.dumps(context)), commit=True)
        except Exception:
            logger.exception("Failed to persist event to DB")

        with self._listener_lock:
            callbacks = list(self._listeners.get(event_name, []))
        # dispatch in threads to not block core flow
        for cb in callbacks:
            try:
                t = threading.Thread(target=self._safe_call, args=(cb, context), daemon=True)
                t.start()
            except Exception:
                logger.exception("Failed to start listener thread")

    def _safe_call(self, cb: Callable[[Dict[str, Any]], None], context: Dict[str, Any]):
        try:
            cb(context)
        except Exception:
            logger.exception("Listener raised exception")

    # -------------------------
    # High-level API: packages
    # -------------------------
    def add_package(self, name: str, version: Optional[str] = None, release: Optional[int] = None,
                    arch: Optional[str] = None, status: str = "installed", user: str = "system"):
        now = _now_ts()
        try:
            with self.transaction():
                # upsert package
                self.execute("""
                    INSERT INTO packages (name, version, release, arch, build_date, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        version=excluded.version,
                        release=excluded.release,
                        arch=excluded.arch,
                        build_date=excluded.build_date,
                        status=excluded.status;
                """, (name, version, release, arch, now, status), commit=False)
                # record history and event
                self.add_history(name, "add", f"package added/updated status={status}", user=user)
                self._emit("package_added", {"package": name, "version": version, "status": status, "user": user, "ts": now})
                logger.info(f"Package added/updated: {name} ({version})")
        except Exception:
            logger.exception("Failed to add package")
            raise

    def update_package(self, name: str, version: Optional[str] = None, release: Optional[int] = None,
                       status: Optional[str] = None, user: str = "system"):
        try:
            with self.transaction():
                # build SET clause dynamically
                sets = []
                params: List[Any] = []
                if version is not None:
                    sets.append("version = ?")
                    params.append(version)
                if release is not None:
                    sets.append("release = ?")
                    params.append(release)
                if status is not None:
                    sets.append("status = ?")
                    params.append(status)
                if not sets:
                    return
                params.append(name)
                sql = f"UPDATE packages SET {', '.join(sets)} WHERE name = ?"
                self.execute(sql, tuple(params), commit=False)
                self.add_history(name, "update", f"updated fields: {', '.join(sets)}", user=user)
                self._emit("package_updated", {"package": name, "fields": sets, "user": user})
                logger.info(f"Package updated: {name}")
        except Exception:
            logger.exception("Failed to update package")
            raise

    def remove_package(self, name: str, user: str = "system"):
        try:
            with self.transaction():
                self.execute("DELETE FROM packages WHERE name = ?", (name,), commit=False)
                self.add_history(name, "remove", "package removed", user=user)
                self._emit("package_removed", {"package": name, "user": user})
                logger.info(f"Package removed: {name}")
        except Exception:
            logger.exception("Failed to remove package")
            raise

    def get_package(self, name: str) -> Optional[Dict[str, Any]]:
        row = self.fetchone("SELECT * FROM packages WHERE name = ?", (name,))
        if not row:
            return None
        return dict(row)

    def list_packages(self, status: Optional[str] = None, arch: Optional[str] = None, regex: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM packages"
        clauses = []
        params: List[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if arch:
            clauses.append("arch = ?")
            params.append(arch)
        if regex:
            clauses.append("name LIKE ?")
            params.append(f"%{regex}%")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        rows = self.fetchall(sql, tuple(params))
        return [dict(r) for r in rows]

    def search_packages(self, pattern: str) -> List[Dict[str, Any]]:
        rows = self.fetchall("SELECT * FROM packages WHERE name LIKE ?", (f"%{pattern}%",))
        return [dict(r) for r in rows]

    # -------------------------
    # Dependencies API
    # -------------------------
    def add_dependency(self, pkg_name: str, depends_on: str, dep_type: str = "runtime"):
        try:
            with self.transaction():
                self.execute("INSERT INTO dependencies (package_name, depends_on, type) VALUES (?, ?, ?);", (pkg_name, depends_on, dep_type), commit=False)
                self.add_history(pkg_name, "add_dependency", f"{pkg_name} -> {depends_on} ({dep_type})")
                self._emit("dependency_added", {"package": pkg_name, "depends_on": depends_on, "type": dep_type})
                logger.info(f"Dependency added: {pkg_name} -> {depends_on}")
        except Exception:
            logger.exception("Failed to add dependency")
            raise

    def get_dependencies(self, pkg_name: str) -> List[Dict[str, Any]]:
        rows = self.fetchall("SELECT depends_on, type FROM dependencies WHERE package_name = ?", (pkg_name,))
        return [dict(r) for r in rows]

    def get_reverse_dependencies(self, pkg_name: str) -> List[Dict[str, Any]]:
        rows = self.fetchall("SELECT package_name, type FROM dependencies WHERE depends_on = ?", (pkg_name,))
        return [dict(r) for r in rows]

    # -------------------------
    # History & Events
    # -------------------------
    def add_history(self, package_name: str, action: str, message: str, user: str = "system"):
        try:
            self.execute("INSERT INTO history (package_name, action, message, user, timestamp) VALUES (?, ?, ?, ?, strftime('%s','now'))", (package_name, action, message, user), commit=True)
        except Exception:
            logger.exception("Failed to add history")

    def get_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows = self.fetchall("SELECT * FROM history ORDER BY timestamp DESC LIMIT ?", (limit,))
        return [dict(r) for r in rows]

    def emit_event(self, event_type: str, context: Dict[str, Any]):
        self._emit(event_type, context)

    def get_events(self, event_type: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        if event_type:
            rows = self.fetchall("SELECT * FROM events WHERE event_type = ? ORDER BY timestamp DESC LIMIT ?", (event_type, limit))
        else:
            rows = self.fetchall("SELECT * FROM events ORDER BY timestamp DESC LIMIT ?", (limit,))
        out = []
        for r in rows:
            ctx = {}
            try:
                ctx = json.loads(r["context"]) if r["context"] else {}
            except Exception:
                ctx = {"raw": r["context"]}
            out.append({"id": r["id"], "event_type": r["event_type"], "context": ctx, "timestamp": r["timestamp"]})
        return out

    # -------------------------
    # Users API
    # -------------------------
    def add_user(self, name: str, role: str = "viewer"):
        try:
            with self.transaction():
                self.execute("INSERT OR IGNORE INTO users (name, role, last_action) VALUES (?, ?, ?);", (name, role, _now_ts()), commit=False)
                self.execute("UPDATE users SET role = ? WHERE name = ?;", (role, name), commit=False)
            logger.info(f"User ensured: {name} (role={role})")
        except Exception:
            logger.exception("Failed to add/set user")
            raise

    def get_user(self, name: str) -> Optional[Dict[str, Any]]:
        row = self.fetchone("SELECT * FROM users WHERE name = ?", (name,))
        return dict(row) if row else None

    def set_user_role(self, name: str, role: str):
        try:
            with self.transaction():
                self.execute("UPDATE users SET role = ? WHERE name = ?;", (role, name), commit=False)
            logger.info(f"User {name} role set to {role}")
        except Exception:
            logger.exception("Failed to set user role")
            raise

    # -------------------------
    # Locks API (simple lease locks)
    # -------------------------
    def acquire_lock(self, resource: str, owner: str, timeout: int = 60) -> bool:
        expires = _now_ts() + int(timeout)
        try:
            with self.transaction():
                # if exists and not expired, fail
                row = self.fetchone("SELECT owner, expires_at FROM locks WHERE resource = ?", (resource,))
                if row:
                    if row["expires_at"] and int(row["expires_at"]) > _now_ts():
                        # locked
                        return False
                # insert or replace
                self.execute("INSERT OR REPLACE INTO locks (resource, owner, expires_at) VALUES (?, ?, ?)", (resource, owner, expires), commit=False)
            logger.debug(f"Lock acquired: {resource} by {owner} until {expires}")
            return True
        except Exception:
            logger.exception("Failed to acquire lock")
            return False

    def release_lock(self, resource: str, owner: str) -> bool:
        try:
            with self.transaction():
                row = self.fetchone("SELECT owner FROM locks WHERE resource = ?", (resource,))
                if not row:
                    return True
                if row["owner"] != owner:
                    logger.warning("Lock release attempted by non-owner")
                    return False
                self.execute("DELETE FROM locks WHERE resource = ?", (resource,), commit=False)
            logger.debug(f"Lock released: {resource} by {owner}")
            return True
        except Exception:
            logger.exception("Failed to release lock")
            return False

    def check_lock(self, resource: str) -> Optional[Dict[str, Any]]:
        row = self.fetchone("SELECT * FROM locks WHERE resource = ?", (resource,))
        return dict(row) if row else None

    # -------------------------
    # Config cache
    # -------------------------
    def cache_config(self, key: str, value: Any):
        try:
            v = json.dumps(value)
            with self.transaction():
                self.execute("INSERT OR REPLACE INTO configs (key, value) VALUES (?, ?);", (key, v), commit=False)
            self._config_cache[key] = value
            logger.debug(f"Config cached: {key}")
        except Exception:
            logger.exception("Failed to cache config")

    def get_cached_config(self, key: str) -> Optional[Any]:
        if key in self._config_cache:
            return self._config_cache[key]
        row = self.fetchone("SELECT value FROM configs WHERE key = ?", (key,))
        if not row:
            return None
        try:
            v = json.loads(row["value"])
            self._config_cache[key] = v
            return v
        except Exception:
            return row["value"]

    # -------------------------
    # Builds & Sharding
    # -------------------------
    def register_shard(self, name: str, worker: str) -> int:
        """
        Register a shard (worker). Returns shard_id.
        If worker already has a shard, update heartbeat and return existing.
        """
        now = _now_ts()
        try:
            with self.transaction():
                row = self.fetchone("SELECT shard_id FROM shards WHERE worker = ?", (worker,))
                if row:
                    shard_id = int(row["shard_id"])
                    self.execute("UPDATE shards SET last_heartbeat = ?, active = 1 WHERE shard_id = ?", (now, shard_id), commit=False)
                    return shard_id
                else:
                    cur = self.execute("INSERT INTO shards (name, worker, active, last_heartbeat) VALUES (?, ?, 1, ?);", (name, worker, now), commit=True)
                    # sqlite3.Cursor.lastrowid for last insert id
                    shard_id = cur.lastrowid
                    logger.info(f"Shard registered: id={shard_id} worker={worker}")
                    return shard_id
        except Exception:
            logger.exception("Failed to register shard")
            raise

    def heartbeat_shard(self, shard_id: int):
        now = _now_ts()
        try:
            self.execute("UPDATE shards SET last_heartbeat = ?, active = 1 WHERE shard_id = ?", (now, shard_id), commit=True)
        except Exception:
            logger.exception("Failed to heartbeat shard")

    def list_shards(self) -> List[Dict[str, Any]]:
        rows = self.fetchall("SELECT * FROM shards")
        return [dict(r) for r in rows]

    def _prune_dead_shards(self):
        """Mark shards with heartbeat older than ttl as inactive."""
        cutoff = _now_ts() - self._shard_ttl
        try:
            self.execute("UPDATE shards SET active = 0 WHERE last_heartbeat IS NOT NULL AND last_heartbeat < ?", (cutoff,), commit=True)
        except Exception:
            logger.exception("Failed to prune dead shards")

    def acquire_shard_for_build(self, preferred_shard: Optional[int] = None) -> Optional[int]:
        """
        Acquire a shard id for a new build. Strategy:
         - prune dead shards
         - if preferred_shard provided and active, return it
         - else choose shard with least 'building' builds
         - else create new shard up to max_shards (from config)
        Note: registering a new shard normally happens from worker side via register_shard; this
        function assumes shards already exist, but will fallback to create a "local" shard entry.
        """
        self._prune_dead_shards()
        try:
            # If preferred shard specified and active
            if preferred_shard:
                row = self.fetchone("SELECT active FROM shards WHERE shard_id = ?", (preferred_shard,))
                if row and int(row["active"]) == 1:
                    return preferred_shard
            # count building builds per shard
            rows = self.fetchall("""
                SELECT s.shard_id, COUNT(b.id) as building_count
                FROM shards s
                LEFT JOIN builds b ON b.shard_id = s.shard_id AND b.status = 'building'
                WHERE s.active = 1
                GROUP BY s.shard_id
                ORDER BY building_count ASC, s.shard_id ASC
            """)
            if rows:
                # choose first (least loaded)
                return int(rows[0]["shard_id"])
            # no active shards: create a local shard entry (name=local)
            max_shards = DEFAULT_MAX_SHARDS
            try:
                bs_cfg = get_buildsystem_config() if callable(get_buildsystem_config) else {}
                if isinstance(bs_cfg, dict):
                    max_shards = int(bs_cfg.get("max_shards", max_shards))
                else:
                    max_shards = int(getattr(bs_cfg, "max_shards", max_shards))
            except Exception:
                pass
            total = self.fetchone("SELECT COUNT(*) as c FROM shards")
            total_num = int(total["c"]) if total else 0
            if total_num < max_shards:
                # create a new shard record (worker unknown)
                cur = self.execute("INSERT INTO shards (name, worker, active, last_heartbeat) VALUES (?, ?, 1, ?)", ("auto", None, _now_ts()), commit=True)
                return cur.lastrowid
            # otherwise fallback to first shard id
            r2 = self.fetchone("SELECT shard_id FROM shards LIMIT 1")
            return int(r2["shard_id"]) if r2 else None
        except Exception:
            logger.exception("Failed to acquire shard for build")
            return None

    def start_build(self, package_name: str, version: Optional[str] = None, shard_id: Optional[int] = None, user: str = "builder") -> int:
        """
        Create a build row with status 'building'. If shard_id not provided, attempt to acquire one.
        Returns build_id.
        """
        try:
            if shard_id is None:
                shard_id = self.acquire_shard_for_build()
            now = _now_ts()
            with self.transaction():
                cur = self.execute("INSERT INTO builds (package_name, version, status, shard_id, started_at) VALUES (?, ?, 'building', ?, ?)", (package_name, version, shard_id, now), commit=False)
                build_id = cur.lastrowid
                self.add_history(package_name, "build_start", f"build_id={build_id} shard={shard_id}", user=user)
                self._emit("build_started", {"package": package_name, "build_id": build_id, "shard": shard_id, "user": user})
                logger.info(f"Build started: id={build_id} pkg={package_name} shard={shard_id}")
                return build_id
        except Exception:
            logger.exception("Failed to start build")
            raise

    def finish_build(self, build_id: int, status: str = "success", log_path: Optional[str] = None, user: str = "builder"):
        """
        Mark build finished with status (success/fail). Update finished_at, log_path.
        """
        try:
            now = _now_ts()
            with self.transaction():
                self.execute("UPDATE builds SET status = ?, finished_at = ?, log_path = ? WHERE id = ?", (status, now, log_path, build_id), commit=False)
                row = self.fetchone("SELECT package_name FROM builds WHERE id = ?", (build_id,))
                pkg = row["package_name"] if row else "<unknown>"
                self.add_history(pkg, "build_finish", f"build_id={build_id} status={status} log={log_path}", user=user)
                self._emit("build_finished", {"package": pkg, "build_id": build_id, "status": status, "log": log_path, "user": user})
                logger.info(f"Build finished: id={build_id} pkg={pkg} status={status}")
        except Exception:
            logger.exception("Failed to finish build")
            raise

    def get_builds(self, package_name: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        if package_name:
            rows = self.fetchall("SELECT * FROM builds WHERE package_name = ? ORDER BY started_at DESC LIMIT ?", (package_name, limit))
        else:
            rows = self.fetchall("SELECT * FROM builds ORDER BY started_at DESC LIMIT ?", (limit,))
        return [dict(r) for r in rows]

    # -------------------------
    # Export / Import
    # -------------------------
    def export_db(self, path: str):
        """
        Export DB tables to JSON file (dictionary of tables -> list of rows)
        """
        try:
            tables = ["packages", "dependencies", "history", "builds", "shards", "events", "users", "locks", "configs"]
            out = {}
            for t in tables:
                rows = self.fetchall(f"SELECT * FROM {t}")
                out[t] = [dict(r) for r in rows]
            with open(path, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
            logger.info(f"DB exported to {path}")
        except Exception:
            logger.exception("Failed to export DB")
            raise

    def import_db(self, path: str, replace: bool = False):
        """
        Import DB from file exported by export_db.
        If replace=True, clear existing tables first.
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            with self.transaction():
                if replace:
                    for t in data.keys():
                        self.execute(f"DELETE FROM {t};", (), commit=False)
                for t, rows in data.items():
                    if not rows:
                        continue
                    # prepare insert columns from first row keys
                    cols = list(rows[0].keys())
                    placeholders = ", ".join(["?"] * len(cols))
                    colnames = ", ".join(cols)
                    for r in rows:
                        vals = tuple(r.get(c) for c in cols)
                        self.execute(f"INSERT INTO {t} ({colnames}) VALUES ({placeholders});", vals, commit=False)
            logger.info(f"DB imported from {path}")
        except Exception:
            logger.exception("Failed to import DB")
            raise

    # -------------------------
    # Utility & cleanup
    # -------------------------
    def vacuum(self):
        try:
            self.execute("VACUUM;", (), commit=True)
            logger.info("Database vacuumed")
        except Exception:
            logger.exception("Vacuum failed")

    def backup(self, path: str):
        try:
            # sqlite backup API
            if self._conn:
                dest = sqlite3.connect(path)
                with dest:
                    self._conn.backup(dest)
                dest.close()
                logger.info(f"Database backed up to {path}")
        except Exception:
            logger.exception("Backup failed")
            raise

# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------

_db_instance: Optional[Database] = None
_db_instance_lock = threading.RLock()

def get_db() -> Database:
    global _db_instance
    with _db_instance_lock:
        if _db_instance is None:
            # Determine path and timeout from config if available
            try:
                cfg = get_config()
                dbpath = None
                timeout = None
                try:
                    # dataclass/pydantic may have attributes
                    db_section = getattr(cfg, "db", None)
                    if db_section:
                        if isinstance(db_section, dict):
                            dbpath = db_section.get("path")
                            timeout = db_section.get("timeout")
                        else:
                            dbpath = getattr(db_section, "path", None)
                            timeout = getattr(db_section, "timeout", None)
                except Exception:
                    pass
                _db_instance = Database(db_path=dbpath, timeout=timeout)
            except Exception:
                _db_instance = Database()
        return _db_instance

# expose most used functions at module level
def add_package(*a, **k): return get_db().add_package(*a, **k)
def update_package(*a, **k): return get_db().update_package(*a, **k)
def remove_package(*a, **k): return get_db().remove_package(*a, **k)
def get_package(*a, **k): return get_db().get_package(*a, **k)
def list_packages(*a, **k): return get_db().list_packages(*a, **k)
def search_packages(*a, **k): return get_db().search_packages(*a, **k)

def add_dependency(*a, **k): return get_db().add_dependency(*a, **k)
def get_dependencies(*a, **k): return get_db().get_dependencies(*a, **k)
def get_reverse_dependencies(*a, **k): return get_db().get_reverse_dependencies(*a, **k)

def add_history(*a, **k): return get_db().add_history(*a, **k)
def get_history(*a, **k): return get_db().get_history(*a, **k)
def emit_event(*a, **k): return get_db().emit_event(*a, **k)
def get_events(*a, **k): return get_db().get_events(*a, **k)

def add_user(*a, **k): return get_db().add_user(*a, **k)
def get_user(*a, **k): return get_db().get_user(*a, **k)
def set_user_role(*a, **k): return get_db().set_user_role(*a, **k)

def acquire_lock(*a, **k): return get_db().acquire_lock(*a, **k)
def release_lock(*a, **k): return get_db().release_lock(*a, **k)
def check_lock(*a, **k): return get_db().check_lock(*a, **k)

def register_shard(*a, **k): return get_db().register_shard(*a, **k)
def heartbeat_shard(*a, **k): return get_db().heartbeat_shard(*a, **k)
def list_shards(*a, **k): return get_db().list_shards(*a, **k)
def acquire_shard_for_build(*a, **k): return get_db().acquire_shard_for_build(*a, **k)

def start_build(*a, **k): return get_db().start_build(*a, **k)
def finish_build(*a, **k): return get_db().finish_build(*a, **k)
def get_builds(*a, **k): return get_db().get_builds(*a, **k)

def cache_config(*a, **k): return get_db().cache_config(*a, **k)
def get_cached_config(*a, **k): return get_db().get_cached_config(*a, **k)

def export_db(*a, **k): return get_db().export_db(*a, **k)
def import_db(*a, **k): return get_db().import_db(*a, **k)
def vacuum(*a, **k): return get_db().vacuum(*a, **k)
def backup(*a, **k): return get_db().backup(*a, **k)

# ---------------------------------------------------------------------------
# If run as script, demo usage
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    db = get_db()
    # demo: add user, package, dependency, start build, finish build, export
    db.add_user("builder", "builder")
    db.add_package("example-pkg", "1.2.3", 1, "x86_64", "installed", user="builder")
    db.add_dependency("example-pkg", "libfoo >= 1.0", "runtime")
    shard = db.register_shard("local", "worker-1")
    build_id = db.start_build("example-pkg", "1.2.3", shard_id=shard, user="builder")
    db.finish_build(build_id, status="success", log_path="/var/log/rquest/example-pkg-build.log", user="builder")
    print("Packages:", db.list_packages())
    print("Builds:", db.get_builds(limit=10))
    db.export_db("./rquest-export.json")
    print("Exported DB to ./rquest-export.json")
