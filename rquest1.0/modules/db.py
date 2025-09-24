# rquest1.0/modules/db.py
"""
db.py - SQLite wrapper and schema for Rquest

Features:
 - Reads DB path & options from modules.config.get_config() if available
 - Thread-safe singleton connection wrapper
 - Auto migration (initial schema v1)
 - High-level helpers for packages, builds, toolchains, installed packages
 - Event emitting / listeners
 - JSON detail fields support
 - Backup / export / import utilities
"""

from __future__ import annotations

import os
import sqlite3
import json
import threading
import time
import shutil
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Callable

# try to use the project's logging and config modules (graceful fallback)
try:
    from modules.config import get_config  # type: ignore
except Exception:
    def get_config():
        return {}

try:
    from modules.logging import get_logger  # type: ignore
    logger = get_logger("db")
except Exception:
    import logging
    logger = logging.getLogger("rquest.db")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

# -----------------------
# Configuration
# -----------------------
CFG = get_config() if callable(get_config) else {}
DB_CFG = CFG.get("database", {}) if isinstance(CFG, dict) else {}
DB_PATH = DB_CFG.get("path") or CFG.get("db_path") or "/var/lib/rquest/db.sqlite3"
DB_TIMEOUT = float(DB_CFG.get("timeout", 30))
DB_JOURNAL = DB_CFG.get("journal_mode", "WAL")
DB_SYNC = str(DB_CFG.get("synchronous", "NORMAL")).upper()

# ensure directory exists
_db_dir = os.path.dirname(DB_PATH)
if _db_dir and not os.path.exists(_db_dir):
    try:
        os.makedirs(_db_dir, exist_ok=True)
    except Exception:
        # fallback to current dir
        DB_PATH = os.path.join(os.getcwd(), "rquest-db.sqlite3")

# -----------------------
# Helper utilities
# -----------------------
def _now_ts() -> int:
    return int(time.time())

def _to_json_safe(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return json.dumps(str(obj))

def _from_json_safe(s: Optional[str]) -> Any:
    if s is None:
        return None
    try:
        return json.loads(s)
    except Exception:
        return s

# -----------------------
# DB class
# -----------------------
class DB:
    def __init__(self, path: str = DB_PATH, timeout: float = DB_TIMEOUT):
        self.path = path
        self.timeout = float(timeout)
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._listeners: Dict[str, List[Callable[[Dict[str,Any]], None]]] = {}
        self._connect()
        self._apply_pragmas()
        self._apply_migrations()

    def _connect(self):
        with self._lock:
            need_init = not os.path.exists(self.path)
            try:
                self._conn = sqlite3.connect(self.path, timeout=self.timeout, check_same_thread=False)
                self._conn.row_factory = sqlite3.Row
                logger.info("DB connected: %s", self.path)
            except Exception as e:
                logger.exception("Failed to connect to DB %s: %s", self.path, e)
                raise

    def _apply_pragmas(self):
        try:
            cur = self._conn.cursor()
            cur.execute(f"PRAGMA journal_mode = {DB_JOURNAL};")
            cur.execute(f"PRAGMA synchronous = {DB_SYNC};")
            cur.close()
            logger.debug("Applied DB pragmas: journal=%s synchronous=%s", DB_JOURNAL, DB_SYNC)
        except Exception:
            logger.exception("Failed applying DB pragmas")

    # -----------------------
    # low-level execute / fetch
    # -----------------------
    def execute(self, sql: str, params: tuple = (), commit: bool = False) -> sqlite3.Cursor:
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute(sql, params)
                if commit:
                    self._conn.commit()
                return cur
            except Exception:
                self._conn.rollback()
                logger.exception("SQL execute failed: %s -- params=%s", sql, params)
                raise

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        cur = self.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return None
        return dict(row)

    def fetchall(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        cur = self.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]

    @contextmanager
    def transaction(self):
        with self._lock:
            try:
                cur = self._conn.cursor()
                cur.execute("BEGIN")
                yield
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                logger.exception("Transaction failed, rolled back")
                raise
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

    # -----------------------
    # schema / migrations
    # -----------------------
    def _apply_migrations(self):
        """
        Basic migrations. We keep a simple version in 'meta' table.
        v1: initial schema with packages, installed_packages, builds, toolchains, toolchain_history, events, shards
        Future migrations: add columns, indexes.
        """
        try:
            self.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                  key TEXT PRIMARY KEY,
                  value TEXT
                );
            """, commit=True)

            ver = self._get_schema_version()
            if ver is None:
                ver = 0

            if ver < 1:
                logger.info("Applying DB migration v1 (initial schema)")
                with self.transaction():
                    # packages - repository recipes and metadata
                    self.execute("""
                        CREATE TABLE IF NOT EXISTS packages (
                          name TEXT PRIMARY KEY,
                          version TEXT,
                          category TEXT,
                          summary TEXT,
                          meta_path TEXT,
                          metadata JSON,
                          added_at INTEGER
                        );
                    """)
                    # installed packages - what is currently installed
                    self.execute("""
                        CREATE TABLE IF NOT EXISTS installed_packages (
                          name TEXT PRIMARY KEY,
                          version TEXT,
                          meta_path TEXT,
                          files TEXT,
                          installed_at INTEGER
                        );
                    """)
                    # builds - build history and details
                    self.execute("""
                        CREATE TABLE IF NOT EXISTS builds (
                          id TEXT PRIMARY KEY,
                          pkg TEXT,
                          version TEXT,
                          started_at INTEGER,
                          finished_at INTEGER,
                          ok INTEGER,
                          stage TEXT,
                          detail TEXT
                        );
                    """)
                    # toolchains registry
                    self.execute("""
                        CREATE TABLE IF NOT EXISTS toolchains (
                          id TEXT PRIMARY KEY,
                          name TEXT UNIQUE,
                          path TEXT,
                          profile TEXT,
                          created_at INTEGER
                        );
                    """)
                    # toolchain history
                    self.execute("""
                        CREATE TABLE IF NOT EXISTS toolchain_history (
                          id TEXT PRIMARY KEY,
                          toolchain_name TEXT,
                          stage TEXT,
                          meta TEXT,
                          ok INTEGER,
                          ts INTEGER,
                          detail TEXT
                        );
                    """)
                    # events (auditable)
                    self.execute("""
                        CREATE TABLE IF NOT EXISTS events (
                          id TEXT PRIMARY KEY,
                          name TEXT,
                          payload TEXT,
                          ts INTEGER
                        );
                    """)
                    # shards / build workers
                    self.execute("""
                        CREATE TABLE IF NOT EXISTS shards (
                          id TEXT PRIMARY KEY,
                          name TEXT,
                          last_heartbeat INTEGER,
                          metadata TEXT
                        );
                    """)
                    # locks
                    self.execute("""
                        CREATE TABLE IF NOT EXISTS locks (
                          name TEXT PRIMARY KEY,
                          owner TEXT,
                          ts INTEGER
                        );
                    """)
                    # upgrade schema version
                    self._set_schema_version(1)
                ver = 1
                logger.info("DB migration v1 applied")
            # future migrations here...
        except Exception:
            logger.exception("Failed applying migrations")

    def _get_schema_version(self) -> Optional[int]:
        try:
            row = self.fetchone("SELECT value FROM meta WHERE key = 'schema_version'")
            if row and row.get("value"):
                return int(row["value"])
        except Exception:
            pass
        return None

    def _set_schema_version(self, v: int):
        try:
            self.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)", (str(int(v)),), commit=True)
        except Exception:
            logger.exception("Failed setting schema_version")

    # -----------------------
    # high-level helpers: packages
    # -----------------------
    def add_package(self, name: str, version: str, meta_path: str, category: Optional[str] = None, summary: Optional[str] = None, metadata: Optional[Dict[str,Any]] = None):
        metadata_json = _to_json_safe(metadata or {})
        now = _now_ts()
        try:
            self.execute("""
                INSERT OR REPLACE INTO packages (name, version, category, summary, meta_path, metadata, added_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (name, version, category, summary, meta_path, metadata_json, now), commit=True)
            logger.debug("Package recorded: %s-%s", name, version)
        except Exception:
            logger.exception("add_package failed for %s", name)
            raise

    def get_package(self, name: str) -> Optional[Dict[str,Any]]:
        row = self.fetchone("SELECT * FROM packages WHERE name = ?", (name,))
        if not row:
            return None
        # deserialize metadata
        row["metadata"] = _from_json_safe(row.get("metadata"))
        return row

    def list_packages(self) -> List[Dict[str,Any]]:
        rows = self.fetchall("SELECT * FROM packages ORDER BY name")
        for r in rows:
            r["metadata"] = _from_json_safe(r.get("metadata"))
        return rows

    def remove_package(self, name: str):
        try:
            self.execute("DELETE FROM packages WHERE name = ?", (name,), commit=True)
        except Exception:
            logger.exception("remove_package failed for %s", name)
            raise

    # -----------------------
    # installed packages (what's on disk)
    # -----------------------
    def add_installed_package(self, name: str, version: str, meta_path: str, files: Optional[List[str]] = None):
        files_json = _to_json_safe(files or [])
        now = _now_ts()
        try:
            self.execute("""
                INSERT OR REPLACE INTO installed_packages (name, version, meta_path, files, installed_at)
                VALUES (?, ?, ?, ?, ?)
            """, (name, version, meta_path, files_json, now), commit=True)
            logger.debug("Installed package recorded: %s-%s", name, version)
        except Exception:
            logger.exception("add_installed_package failed for %s", name)
            raise

    def remove_installed_package(self, name: str):
        try:
            self.execute("DELETE FROM installed_packages WHERE name = ?", (name,), commit=True)
            logger.debug("Installed package removed: %s", name)
        except Exception:
            logger.exception("remove_installed_package failed for %s", name)
            raise

    def get_installed_package(self, name: str) -> Optional[Dict[str,Any]]:
        r = self.fetchone("SELECT * FROM installed_packages WHERE name = ?", (name,))
        if not r:
            return None
        r["files"] = _from_json_safe(r.get("files"))
        return r

    def get_installed_packages(self, order_by_deps: bool = False) -> List[Dict[str,Any]]:
        # order_by_deps currently ignored (resolver handles ordering)
        rows = self.fetchall("SELECT * FROM installed_packages ORDER BY name")
        for r in rows:
            r["files"] = _from_json_safe(r.get("files"))
        return rows

    # -----------------------
    # builds history
    # -----------------------
    def record_build(self, pkg: str, version: str, ok: bool, stage: str, detail: Optional[Dict[str,Any]] = None) -> str:
        bid = f"bld-{_uid()}"
        now = _now_ts()
        detail_text = _to_json_safe(detail or {})
        try:
            self.execute("""
                INSERT INTO builds (id, pkg, version, started_at, finished_at, ok, stage, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (bid, pkg, version, now, now, 1 if ok else 0, stage, detail_text), commit=True)
            logger.debug("Recorded build %s %s ok=%s", pkg, version, ok)
            return bid
        except Exception:
            logger.exception("record_build failed for %s", pkg)
            raise

    def list_builds(self, limit: int = 100) -> List[Dict[str,Any]]:
        rows = self.fetchall("SELECT * FROM builds ORDER BY finished_at DESC LIMIT ?", (limit,))
        for r in rows:
            r["detail"] = _from_json_safe(r.get("detail"))
        return rows

    def get_build(self, bid: str) -> Optional[Dict[str,Any]]:
        r = self.fetchone("SELECT * FROM builds WHERE id = ?", (bid,))
        if not r:
            return None
        r["detail"] = _from_json_safe(r.get("detail"))
        return r

    # -----------------------
    # toolchains
    # -----------------------
    def add_toolchain(self, name: str, path: str, profile: Optional[str] = None) -> str:
        tid = f"tc-{_uid()}"
        now = _now_ts()
        try:
            self.execute("""
                INSERT OR REPLACE INTO toolchains (id, name, path, profile, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (tid, name, path, profile or "", now), commit=True)
            logger.debug("Toolchain added: %s -> %s", name, path)
            return tid
        except Exception:
            logger.exception("add_toolchain failed for %s", name)
            raise

    def list_toolchains(self) -> List[Dict[str,Any]]:
        rows = self.fetchall("SELECT * FROM toolchains ORDER BY name")
        return rows

    def get_toolchain(self, name: str) -> Optional[Dict[str,Any]]:
        r = self.fetchone("SELECT * FROM toolchains WHERE name = ?", (name,))
        return r

    def add_toolchain_history(self, toolchain_name: str, stage: str, meta: str, ok: bool, detail: Optional[Dict[str,Any]] = None) -> str:
        tid = f"tch-{_uid()}"
        now = _now_ts()
        dt = _to_json_safe(detail or {})
        try:
            self.execute("""
                INSERT INTO toolchain_history (id, toolchain_name, stage, meta, ok, ts, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (tid, toolchain_name, stage, meta, 1 if ok else 0, now, dt), commit=True)
            logger.debug("Toolchain history recorded: %s %s ok=%s", toolchain_name, stage, ok)
            return tid
        except Exception:
            logger.exception("add_toolchain_history failed for %s", toolchain_name)
            raise

    def list_toolchain_history(self, name: Optional[str] = None, limit: int = 200) -> List[Dict[str,Any]]:
        if name:
            rows = self.fetchall("SELECT * FROM toolchain_history WHERE toolchain_name = ? ORDER BY ts DESC LIMIT ?", (name, limit))
        else:
            rows = self.fetchall("SELECT * FROM toolchain_history ORDER BY ts DESC LIMIT ?", (limit,))
        for r in rows:
            r["detail"] = _from_json_safe(r.get("detail"))
        return rows

    # -----------------------
    # events / listeners
    # -----------------------
    def emit_event(self, name: str, payload: Optional[Dict[str,Any]] = None) -> str:
        eid = f"evt-{_uid()}"
        now = _now_ts()
        ptext = _to_json_safe(payload or {})
        try:
            self.execute("INSERT INTO events (id, name, payload, ts) VALUES (?, ?, ?, ?)", (eid, name, ptext, now), commit=True)
        except Exception:
            logger.exception("emit_event DB insert failed")
        # notify listeners (in-thread)
        listeners = list(self._listeners.get(name, []))
        for cb in listeners:
            try:
                cb({"id": eid, "name": name, "payload": payload or {}, "ts": now})
            except Exception:
                logger.exception("Event listener callback failed for %s", name)
        return eid

    def on(self, name: str, callback: Callable[[Dict[str,Any]], None]):
        if name not in self._listeners:
            self._listeners[name] = []
        self._listeners[name].append(callback)
        logger.debug("Listener added for event %s", name)

    def get_events(self, limit: int = 100) -> List[Dict[str,Any]]:
        rows = self.fetchall("SELECT * FROM events ORDER BY ts DESC LIMIT ?", (limit,))
        for r in rows:
            r["payload"] = _from_json_safe(r.get("payload"))
        return rows

    # -----------------------
    # locks (simple advisory)
    # -----------------------
    def acquire_lock(self, name: str, owner: str) -> bool:
        now = _now_ts()
        try:
            self.execute("INSERT OR REPLACE INTO locks (name, owner, ts) VALUES (?, ?, ?)", (name, owner, now), commit=True)
            return True
        except Exception:
            logger.debug("acquire_lock failed for %s", name)
            return False

    def release_lock(self, name: str):
        try:
            self.execute("DELETE FROM locks WHERE name = ?", (name,), commit=True)
            return True
        except Exception:
            logger.exception("release_lock failed for %s", name)
            return False

    def check_lock(self, name: str) -> Optional[Dict[str,Any]]:
        r = self.fetchone("SELECT * FROM locks WHERE name = ?", (name,))
        return r

    # -----------------------
    # shards - basic registry for build workers
    # -----------------------
    def register_shard(self, shard_id: str, name: str, metadata: Optional[Dict[str,Any]] = None) -> bool:
        now = _now_ts()
        meta_text = _to_json_safe(metadata or {})
        try:
            self.execute("INSERT OR REPLACE INTO shards (id, name, last_heartbeat, metadata) VALUES (?, ?, ?, ?)",
                         (shard_id, name, now, meta_text), commit=True)
            return True
        except Exception:
            logger.exception("register_shard failed for %s", shard_id)
            return False

    def heartbeat_shard(self, shard_id: str):
        now = _now_ts()
        try:
            self.execute("UPDATE shards SET last_heartbeat = ? WHERE id = ?", (now, shard_id), commit=True)
            return True
        except Exception:
            logger.exception("heartbeat_shard failed for %s", shard_id)
            return False

    def list_shards(self) -> List[Dict[str,Any]]:
        rows = self.fetchall("SELECT * FROM shards ORDER BY last_heartbeat DESC")
        for r in rows:
            r["metadata"] = _from_json_safe(r.get("metadata"))
        return rows

    # -----------------------
    # utilities
    # -----------------------
    def backup(self, dest_path: str) -> bool:
        try:
            # ensure DB is synced
            with self._lock:
                self._conn.commit()
                shutil.copy2(self.path, dest_path)
            logger.info("DB backup created: %s", dest_path)
            return True
        except Exception:
            logger.exception("DB backup failed")
            return False

    def vacuum(self) -> bool:
        try:
            self.execute("VACUUM;", commit=True)
            logger.info("DB vacuum finished")
            return True
        except Exception:
            logger.exception("DB vacuum failed")
            return False

    def export_json(self) -> Dict[str,Any]:
        out: Dict[str, Any] = {}
        out["meta"] = {"exported_at": _now_ts(), "db_path": self.path}
        out["packages"] = self.fetchall("SELECT * FROM packages")
        out["installed_packages"] = self.fetchall("SELECT * FROM installed_packages")
        out["builds"] = self.fetchall("SELECT * FROM builds")
        out["toolchains"] = self.fetchall("SELECT * FROM toolchains")
        out["toolchain_history"] = self.fetchall("SELECT * FROM toolchain_history")
        out["events"] = self.fetchall("SELECT * FROM events")
        return out

    def import_json(self, data: Dict[str,Any]) -> bool:
        try:
            with self.transaction():
                for p in data.get("packages", []):
                    self.execute("INSERT OR REPLACE INTO packages (name, version, category, summary, meta_path, metadata, added_at) VALUES (?,?,?,?,?,?,?)",
                                 (p.get("name"), p.get("version"), p.get("category"), p.get("summary"), p.get("meta_path"), _to_json_safe(p.get("metadata")), p.get("added_at")))
                for ip in data.get("installed_packages", []):
                    self.execute("INSERT OR REPLACE INTO installed_packages (name, version, meta_path, files, installed_at) VALUES (?,?,?,?,?)",
                                 (ip.get("name"), ip.get("version"), ip.get("meta_path"), _to_json_safe(ip.get("files")), ip.get("installed_at")))
                for b in data.get("builds", []):
                    self.execute("INSERT OR REPLACE INTO builds (id, pkg, version, started_at, finished_at, ok, stage, detail) VALUES (?,?,?,?,?,?,?,?)",
                                 (b.get("id"), b.get("pkg"), b.get("version"), b.get("started_at"), b.get("finished_at"), b.get("ok"), b.get("stage"), _to_json_safe(b.get("detail"))))
                for t in data.get("toolchains", []):
                    self.execute("INSERT OR REPLACE INTO toolchains (id, name, path, profile, created_at) VALUES (?,?,?,?,?)",
                                 (t.get("id"), t.get("name"), t.get("path"), t.get("profile"), t.get("created_at")))
                for th in data.get("toolchain_history", []):
                    self.execute("INSERT OR REPLACE INTO toolchain_history (id, toolchain_name, stage, meta, ok, ts, detail) VALUES (?,?,?,?,?,?,?)",
                                 (th.get("id"), th.get("toolchain_name"), th.get("stage"), th.get("meta"), th.get("ok"), th.get("ts"), _to_json_safe(th.get("detail"))))
                for e in data.get("events", []):
                    self.execute("INSERT OR REPLACE INTO events (id, name, payload, ts) VALUES (?,?,?,?)",
                                 (e.get("id"), e.get("name"), _to_json_safe(e.get("payload")), e.get("ts")))
            return True
        except Exception:
            logger.exception("DB import failed")
            return False

    def close(self):
        try:
            with self._lock:
                if self._conn:
                    self._conn.commit()
                    self._conn.close()
                    self._conn = None
                    logger.info("DB connection closed")
        except Exception:
            logger.exception("Failed closing DB")

# -----------------------
# Singleton accessor
# -----------------------
_DB_SINGLETON: Optional[DB] = None
def get_db() -> DB:
    global _DB_SINGLETON
    if _DB_SINGLETON is None:
        _DB_SINGLETON = DB()
    return _DB_SINGLETON

# -----------------------
# Convenience module-level wrappers
# -----------------------
_db = get_db()
execute = _db.execute
fetchone = _db.fetchone
fetchall = _db.fetchall
transaction = _db.transaction
add_package = _db.add_package
get_package = _db.get_package
list_packages = _db.list_packages
remove_package = _db.remove_package
add_installed_package = _db.add_installed_package
get_installed_packages = _db.get_installed_packages
remove_installed_package = _db.remove_installed_package
record_build = _db.record_build
list_builds = _db.list_builds
get_build = _db.get_build
add_toolchain = _db.add_toolchain
list_toolchains = _db.list_toolchains
add_toolchain_history = _db.add_toolchain_history
list_toolchain_history = _db.list_toolchain_history
emit_event = _db.emit_event
on = _db.on
backup = _db.backup
vacuum = _db.vacuum
export_json = _db.export_json
import_json = _db.import_json
close = _db.close

# -----------------------
# If invoked directly, print DB status
# -----------------------
if __name__ == "__main__":
    d = get_db()
    print("DB path:", d.path)
    print("Tables present:", [r["name"] for r in d.fetchall("SELECT name FROM sqlite_master WHERE type='table'")])
    print("Packages:", d.list_packages()[:10])
