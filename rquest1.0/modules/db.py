# rquest1.0/modules/db.py
"""
DB module for Rquest v1.0 — versão completa e integrada.

Funcionalidades principais:
- Wrapper thread-safe para sqlite3 (check_same_thread=False)
- Row factory (sqlite3.Row) para acesso por nome de coluna
- Pragmas configuráveis (WAL, foreign_keys, busy_timeout, synchronous)
- Context manager e decorator para transações (commit/rollback automáticos)
- Migrations simples (tabela rquest_migrations + apply_migrations)
- Backup/restore usando sqlite3.Connection.backup quando disponível
- Methods: connect, close, execute, executemany, executescript, fetchone, fetchall, iterate
- Streaming de grandes consultas com fetchmany
- Reconfigura se `config.get("db.path")` mudar (usa callback do config)
- Logging integrado com rquest1.0.modules.logging
- Função get_default_db() para singleton compartilhado
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Generator, Iterable, List, Optional, Sequence, Tuple

from rquest1.0.modules import config, logging as rlogging

_logger = rlogging.get_logger()


class DBError(Exception):
    """Exceção genérica do módulo DB."""


@dataclass
class DBConfig:
    """Estrutura de configuração do DB (valores padrão razoáveis)."""
    path: str
    timeout: float = 5.0  # seconds for sqlite connect busy timeout
    detect_types: int = 0
    wal: bool = True
    journal_mode: str = "WAL"
    foreign_keys: bool = True
    busy_timeout_ms: int = 5000
    synchronous: str = "NORMAL"  # OFF, NORMAL, FULL
    pragmas: Optional[dict] = None  # pragas adicionais


class DB:
    """
    Wrapper completo para sqlite3.

    Exemplos de uso:
        db = DB()  # usa config.get('db.path')
        with db:
            rows = db.fetchall("SELECT * FROM users")

        @db.transactional()
        def insert_user(...):
            ...

    """

    def __init__(self, path: Optional[str | Path] = None, cfg: Optional[DBConfig] = None) -> None:
        # Carrega configuração do config.py se necessário
        if cfg is None:
            if path is None:
                p = config.get("db.path")
                if not p:
                    raise DBError("Configuração `db.path` não encontrada (config.get('db.path') retornou vazio).")
                path = p
            # monta DBConfig com valores vindo de config (se presentes)
            cfg = DBConfig(
                path=str(path),
                timeout=float(config.get("db.timeout") or 5.0),
                detect_types=int(config.get("db.detect_types") or 0),
                wal=bool(config.get("db.wal") if config.get("db.wal") is not None else True),
                journal_mode=str(config.get("db.journal_mode") or "WAL"),
                foreign_keys=bool(config.get("db.foreign_keys") if config.get("db.foreign_keys") is not None else True),
                busy_timeout_ms=int(config.get("db.busy_timeout_ms") or 5000),
                synchronous=str(config.get("db.synchronous") or "NORMAL"),
                pragmas=config.get("db.pragmas") or None,
            )
        self._cfg = cfg
        self._path = Path(self._cfg.path).expanduser().resolve()
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()
        self._stmt_lock = threading.RLock()  # para proteger caches se quiser futuramente
        # registro callback para alteração de config (se disponível)
        try:
            # config._WATCH_CALLBACKS é usado pelos outros módulos; se não existir, ignora
            config._WATCH_CALLBACKS.append(self._on_config_reload)
        except Exception:
            # fallback silencioso
            pass

    # ------------------------
    # Reação à alteração de config
    # ------------------------
    def _on_config_reload(self) -> None:
        """Callback chamado quando config é recarregado; atualiza path se necessário."""
        try:
            new_path = config.get("db.path")
            if not new_path:
                return
            new_resolved = Path(new_path).expanduser().resolve()
            if new_resolved != self._path:
                _logger.info("DB: db.path mudou de %s -> %s; reiniciando conexão", self._path, new_resolved)
                with self._lock:
                    self.close()
                    self._path = new_resolved
        except Exception:
            _logger.exception("Erro ao processar reload de config no DB")

    # ------------------------
    # Conexão e pragmas
    # ------------------------
    def connect(self) -> sqlite3.Connection:
        """Garante que uma conexão está aberta e aplicada as pragmas."""
        with self._lock:
            if self._conn is not None:
                return self._conn
            # assegura diretório
            parent = self._path.parent
            if parent and not parent.exists():
                try:
                    parent.mkdir(parents=True, exist_ok=True)
                except Exception:
                    _logger.exception("Falha ao criar diretório para DB: %s", parent)
                    # deixamos o sqlite lançar erro depois
            # abrir conexão
            try:
                conn = sqlite3.connect(
                    str(self._path),
                    timeout=self._cfg.timeout,
                    detect_types=self._cfg.detect_types,
                    check_same_thread=False,  # permitir uso multi-thread controlado por locks
                )
                conn.row_factory = sqlite3.Row
                # aplicar pragmas seguros
                cur = conn.cursor()
                try:
                    if self._cfg.journal_mode:
                        # journal_mode retorna value; asseguramos mesmo assim
                        val = self._cfg.journal_mode
                        cur.execute(f"PRAGMA journal_mode = {val};")
                        _logger.debug("PRAGMA journal_mode = %s", val)
                    if self._cfg.foreign_keys:
                        cur.execute("PRAGMA foreign_keys = ON;")
                    if self._cfg.busy_timeout_ms:
                        cur.execute(f"PRAGMA busy_timeout = {int(self._cfg.busy_timeout_ms)};")
                    if self._cfg.synchronous:
                        cur.execute(f"PRAGMA synchronous = {self._cfg.synchronous};")
                    # aplica pragmas adicionais do dict
                    if self._cfg.pragmas:
                        for k, v in (self._cfg.pragmas.items() if isinstance(self._cfg.pragmas, dict) else []):
                            cur.execute(f"PRAGMA {k} = {v};")
                finally:
                    cur.close()
                self._conn = conn
                _logger.debug("Conectado ao DB: %s", self._path)
                return self._conn
            except Exception as e:
                _logger.exception("Erro ao conectar ao DB %s: %s", self._path, e)
                raise DBError(f"Erro ao conectar ao DB: {e}") from e

    def is_connected(self) -> bool:
        with self._lock:
            return self._conn is not None

    def close(self) -> None:
        """Fecha conexão se aberta."""
        with self._lock:
            if self._conn is None:
                return
            try:
                self._conn.close()
                _logger.debug("Conexão ao DB fechada: %s", self._path)
            except Exception:
                _logger.exception("Erro ao fechar conexão DB")
            finally:
                self._conn = None

    # ------------------------
    # Execução de comandos
    # ------------------------
    def execute(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        commit: bool = False,
        many: bool = False,
    ) -> sqlite3.Cursor:
        """
        Executa SQL e retorna o cursor.
        - params: parâmetros para execute
        - commit: se True, faz commit após execução
        - many: se True, espera que params seja Sequence[Sequence] e usa executemany
        """
        conn = self.connect()
        try:
            cur = conn.cursor()
            if many:
                if not isinstance(params, (list, tuple)):
                    raise DBError("`many=True` requer params como Sequence[Sequence].")
                cur.executemany(sql, params)  # type: ignore[arg-type]
            else:
                if params is not None:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
            if commit:
                conn.commit()
            return cur
        except Exception as e:
            _logger.exception("Erro ao executar SQL: %s | params=%s", sql, params)
            # tenta rollback seguro
            try:
                conn.rollback()
            except Exception:
                _logger.debug("Rollback falhou (ignored).")
            raise DBError(f"Erro ao executar SQL: {e}") from e

    def executescript(self, script: str, commit: bool = True) -> None:
        """Executa múltiplos comandos SQL via executescript (útil para DDL e migrations)."""
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.executescript(script)
            if commit:
                conn.commit()
        except Exception as e:
            _logger.exception("Erro ao executar script SQL")
            try:
                conn.rollback()
            except Exception:
                pass
            raise DBError(f"Erro ao executar script SQL: {e}") from e
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any]], commit: bool = False) -> sqlite3.Cursor:
        """Atalho para executemany."""
        return self.execute(sql, params=seq_of_params, commit=commit, many=True)

    # ------------------------
    # Fetch helpers
    # ------------------------
    def fetchone(self, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[sqlite3.Row]:
        cur = self.execute(sql, params)
        row = cur.fetchone()
        try:
            cur.close()
        except Exception:
            pass
        return row

    def fetchall(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[sqlite3.Row]:
        cur = self.execute(sql, params)
        rows = cur.fetchall()
        try:
            cur.close()
        except Exception:
            pass
        return rows

    def iterate(self, sql: str, params: Optional[Sequence[Any]] = None, chunk: int = 200) -> Generator[List[sqlite3.Row], None, None]:
        """
        Itera sobre resultados de forma paginada (fetchmany). Retorna listas de rows.
        Uso: for chunk in db.iterate(...): for row in chunk: ...
        """
        conn = self.connect()
        try:
            cur = conn.cursor()
            if params is not None:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            while True:
                rows = cur.fetchmany(chunk)
                if not rows:
                    break
                yield rows
        except Exception as e:
            _logger.exception("Erro ao iterar resultados: %s | params=%s", sql, params)
            raise DBError(f"Erro ao iterar resultados: {e}") from e
        finally:
            try:
                cur.close()
            except Exception:
                pass

    # ------------------------
    # Transaction helpers (context manager + decorator)
    # ------------------------
    @contextlib.contextmanager
    def transaction(self, writable: bool = True) -> Generator[sqlite3.Cursor, None, None]:
        """
        Context manager para transação:
            with db.transaction():
                cur = ...
                cur.execute(...)
        Commit no sucesso, rollback em exceção.
        """
        conn = self.connect()
        cur = conn.cursor()
        try:
            yield cur
            if writable:
                conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                _logger.debug("Rollback falhou")
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def transactional(self, writable: bool = True) -> Callable:
        """
        Decorador para funções que precisam de transação.
        Exemplo:
            @db.transactional()
            def do_stuff(...):
                db.execute(...)
        A função decorada recebe os mesmos args; o decorator garante commit/rollback.
        """
        def decorator(fn: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                with self.transaction(writable=writable):
                    return fn(*args, **kwargs)
            wrapper.__wrapped__ = fn
            return wrapper
        return decorator

    # ------------------------
    # Migrations simples
    # ------------------------
    def _ensure_migrations_table(self) -> None:
        """Cria tabela de migrations se necessário."""
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS rquest_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT,
                applied_at TEXT
            );
            """,
            commit=True,
        )

    def get_applied_migrations(self) -> List[int]:
        """Retorna lista de versões aplicadas (ordenada)."""
        try:
            rows = self.fetchall("SELECT version FROM rquest_migrations ORDER BY version;")
            return [int(r["version"]) for r in rows]
        except DBError:
            return []

    def get_current_version(self) -> int:
        """Retorna a maior versão aplicada (0 se nada)."""
        rows = self.fetchall("SELECT MAX(version) as v FROM rquest_migrations;")
        if not rows or rows[0]["v"] is None:
            return 0
        return int(rows[0]["v"])

    def apply_migrations(self, migrations: Iterable[Tuple[int, str, str]]) -> List[int]:
        """
        Aplica uma lista de migrations (version:int, name:str, sql:str).
        Retorna lista de versões aplicadas agora.
        """
        applied: List[int] = []
        with self._lock:
            self._ensure_migrations_table()
            current = self.get_current_version()
            # ordenar por versão asc
            for version, name, sql in sorted(migrations, key=lambda x: int(x[0])):
                if int(version) <= int(current):
                    continue
                try:
                    _logger.info("Aplicando migration %s: %s", version, name)
                    self.executescript(sql, commit=True)
                    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    self.execute(
                        "INSERT INTO rquest_migrations (version, name, applied_at) VALUES (?, ?, ?);",
                        (int(version), name, now),
                        commit=True,
                    )
                    applied.append(int(version))
                except Exception:
                    _logger.exception("Falha ao aplicar migration %s (%s)", version, name)
                    raise
        return applied

    # ------------------------
    # Backup / restore
    # ------------------------
    def backup_to(self, dest_path: str | Path, pages: int = 0, sleep: float = 0.250) -> None:
        """
        Faz backup online para outro arquivo SQLite usando sqlite3.Connection.backup (se disponível).
        - pages: 0 copia tudo; >0 copia em blocos.
        """
        dest = Path(dest_path).expanduser().resolve()
        dest.parent.mkdir(parents=True, exist_ok=True)
        conn = self.connect()
        try:
            dest_conn = sqlite3.connect(str(dest))
            try:
                # usa API backup se disponível (Python 3.7+)
                if hasattr(conn, "backup"):
                    conn.backup(dest_conn, pages=pages, progress=None, sleep=sleep)
                else:
                    # fallback simples: dump / executescript (não ideal para grande DB)
                    cur = conn.cursor()
                    data = "\n".join(conn.iterdump())
                    dest_conn.executescript(data)
                dest_conn.commit()
                _logger.info("Backup do DB para %s finalizado", dest)
            finally:
                dest_conn.close()
        except Exception:
            _logger.exception("Falha ao criar backup para %s", dest)
            raise DBError("Backup falhou")

    def restore_from(self, src_path: str | Path) -> None:
        """
        Restaura DB substituindo o arquivo atual pelo src_path.
        ATENÇÃO: substitui dados; use com cautela.
        """
        src = Path(src_path).expanduser().resolve()
        if not src.exists():
            raise DBError("Arquivo de restauração não encontrado: %s" % src)
        with self._lock:
            # fechar conexão atual antes de substituir
            self.close()
            try:
                # sobrescreve arquivo
                target = self._path
                import shutil

                shutil.copy2(str(src), str(target))
                _logger.info("DB restaurado a partir de %s", src)
            except Exception:
                _logger.exception("Falha ao restaurar DB")
                raise DBError("Falha ao restaurar DB") from None

    # ------------------------
    # Utility
    # ------------------------
    def vacuum(self) -> None:
        """Executa VACUUM no banco (reorganiza)."""
        try:
            self.execute("VACUUM;", commit=True)
            _logger.info("VACUUM concluído")
        except Exception:
            _logger.exception("VACUUM falhou")
            raise

    # ------------------------
    # Context manager
    # ------------------------
    def __enter__(self) -> "DB":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        # se alguma exceção, não suprime; commit automático se não houver exceção
        if exc_type is None:
            try:
                if self._conn:
                    self._conn.commit()
            except Exception:
                _logger.exception("Commit no __exit__ falhou")
        else:
            try:
                if self._conn:
                    self._conn.rollback()
            except Exception:
                _logger.exception("Rollback no __exit__ falhou")
        self.close()
        return False

    # ------------------------
    # Propriedades
    # ------------------------
    @property
    def path(self) -> Path:
        return self._path

    @property
    def connection(self) -> Optional[sqlite3.Connection]:
        return self._conn


# ------------------------
# Singleton / helpers do módulo
# ------------------------
_default_db_lock = threading.RLock()
_default_db: Optional[DB] = None


def get_default_db(create_if_missing: bool = True) -> Optional[DB]:
    """
    Retorna uma instância singleton de DB (cria usando config se necessário).
    """
    global _default_db
    with _default_db_lock:
        if _default_db is None and create_if_missing:
            _default_db = DB()
        return _default_db


def set_default_db(db: Optional[DB]) -> None:
    """Define manualmente a instância default (útil para testes)."""
    global _default_db
    with _default_db_lock:
        _default_db = db


# ------------------------
# Decorator de conveniência para usar o default DB em funções
# ------------------------
def with_db(func: Callable) -> Callable:
    """
    Decorator para injetar default DB como primeiro argumento se função não receber um db.
    Uso:
        @with_db
        def fn(db, arg1, ...):
            ...
    """
    def wrapper(*args, **kwargs):
        db = kwargs.get("db", None)
        if db is None:
            db = get_default_db()
            if db is None:
                raise DBError("Default DB não disponível")
            kwargs["db"] = db
        return func(*args, **kwargs)
    wrapper.__wrapped__ = func
    return wrapper
