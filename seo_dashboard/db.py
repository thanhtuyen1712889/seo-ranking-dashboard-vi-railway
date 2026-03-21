from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Sequence

try:
    import libsql
except ModuleNotFoundError:  # pragma: no cover - optional runtime dependency
    libsql = None

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_URL = "sqlite:////data/seo_dashboard.db" if Path("/data").exists() else "sqlite:///./data/seo_dashboard.db"
DEFAULT_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "").strip() or DEFAULT_SQLITE_URL


class RowCompat:
    """Provide sqlite3.Row-like access for libsql tuple rows."""

    __slots__ = ("_columns", "_values", "_index")

    def __init__(self, columns: Sequence[str], values: Sequence[Any]) -> None:
        self._columns = tuple(columns)
        self._values = tuple(values)
        self._index = {name: idx for idx, name in enumerate(self._columns)}

    def __getitem__(self, key: int | slice | str) -> Any:
        if isinstance(key, (int, slice)):
            return self._values[key]
        idx = self._index.get(str(key))
        if idx is None:
            raise KeyError(key)
        return self._values[idx]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> list[str]:
        return list(self._columns)

    def get(self, key: str, default: Any = None) -> Any:
        if key in self._index:
            return self[key]
        return default

    def items(self) -> list[tuple[str, Any]]:
        return [(name, self._values[idx]) for idx, name in enumerate(self._columns)]

    def __repr__(self) -> str:
        return f"RowCompat({dict(self.items())!r})"


class CursorCompat:
    __slots__ = ("_raw",)

    def __init__(self, raw_cursor: Any) -> None:
        self._raw = raw_cursor

    def _convert(self, row: Any) -> Any:
        if row is None:
            return None
        if isinstance(row, tuple) and getattr(self._raw, "description", None):
            columns = [item[0] for item in self._raw.description]
            return RowCompat(columns, row)
        return row

    def fetchone(self) -> Any:
        return self._convert(self._raw.fetchone())

    def fetchall(self) -> list[Any]:
        return [self._convert(item) for item in self._raw.fetchall()]

    def fetchmany(self, size: int | None = None) -> list[Any]:
        if size is None:
            rows = self._raw.fetchmany()
        else:
            rows = self._raw.fetchmany(size)
        return [self._convert(item) for item in rows]

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                break
            yield row

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)


class ConnectionCompat:
    __slots__ = ("_raw", "_backend")

    def __init__(self, raw_connection: Any, backend: str) -> None:
        self._raw = raw_connection
        self._backend = backend

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> CursorCompat:
        cursor = self._raw.execute(sql, () if params is None else params)
        return CursorCompat(cursor)

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any]]) -> CursorCompat:
        cursor = self._raw.executemany(sql, seq_of_params)
        return CursorCompat(cursor)

    def executescript(self, script: str) -> CursorCompat:
        cursor = self._raw.executescript(script)
        return CursorCompat(cursor)

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        self._raw.close()

    def sync(self) -> None:
        sync = getattr(self._raw, "sync", None)
        if callable(sync):
            sync()

    def __enter__(self) -> ConnectionCompat:
        enter = getattr(self._raw, "__enter__", None)
        if callable(enter):
            enter()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        exit_method = getattr(self._raw, "__exit__", None)
        if callable(exit_method):
            return bool(exit_method(exc_type, exc, tb))
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()
        return False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)


def _resolve_sqlite_path(url: str) -> Path:
    raw_path = url.removeprefix("sqlite:///")
    path = Path(raw_path) if raw_path.startswith("/") else (ROOT / raw_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def resolve_database_target(database_url: str | None = None) -> dict[str, Any]:
    url = (database_url or os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL).strip()
    if url.startswith("sqlite:///"):
        return {
            "backend": "sqlite",
            "url": url,
            "sqlite_path": _resolve_sqlite_path(url),
        }
    if url.startswith("libsql://") or url.startswith("https://"):
        replica_env = os.getenv("TURSO_REPLICA_PATH", "./data/turso-replica.db")
        replica_path = Path(replica_env) if Path(replica_env).is_absolute() else (ROOT / replica_env).resolve()
        replica_path.parent.mkdir(parents=True, exist_ok=True)
        auth_token = os.getenv("TURSO_AUTH_TOKEN", "").strip()
        sync_interval = int(os.getenv("TURSO_SYNC_INTERVAL_SECONDS", "5") or "5")
        return {
            "backend": "turso",
            "url": url,
            "sqlite_path": replica_path,
            "auth_token": auth_token,
            "sync_interval": sync_interval,
        }
    raise ValueError("Unsupported DATABASE_URL. Use sqlite:///... or libsql://...")


DB_TARGET = resolve_database_target()
DB_PATH = DB_TARGET["sqlite_path"]


def get_connection() -> ConnectionCompat:
    backend = str(DB_TARGET["backend"])
    if backend == "sqlite":
        raw_connection = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        raw_connection.row_factory = sqlite3.Row
        connection = ConnectionCompat(raw_connection, backend)
    elif backend == "turso":
        if libsql is None:
            raise RuntimeError("DATABASE_URL is libsql:// but libsql package is not installed.")
        raw_connection = libsql.connect(
            str(DB_PATH),
            sync_url=str(DB_TARGET["url"]),
            sync_interval=int(DB_TARGET["sync_interval"]),
            auth_token=str(DB_TARGET.get("auth_token") or ""),
        )
        connection = ConnectionCompat(raw_connection, backend)
    else:
        raise ValueError("Unsupported database backend.")

    # Keep pragmas best-effort because some backends may not support all options.
    for statement in ("PRAGMA foreign_keys=ON;", "PRAGMA journal_mode=WAL;", "PRAGMA synchronous=NORMAL;"):
        try:
            connection.execute(statement)
        except Exception:
            continue
    return connection


def _column_exists(connection: ConnectionCompat, table: str, column: str) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def _ensure_column(connection: ConnectionCompat, table: str, column: str, ddl: str) -> None:
    if not _column_exists(connection, table, column):
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db() -> None:
    with get_connection() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sheet_url TEXT,
                sheet_gid TEXT,
                source_name TEXT,
                source_type TEXT DEFAULT 'upload',
                refresh_interval_minutes INTEGER DEFAULT 30,
                anthropic_api_key TEXT,
                saved_view_state TEXT DEFAULT '{}',
                last_pulled_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                group_name TEXT,
                cluster_name TEXT,
                sub_cluster_name TEXT,
                target_url TEXT,
                found_url TEXT,
                search_volume INTEGER,
                best_rank REAL,
                kpi_target INTEGER,
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(project_id, keyword),
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS rankings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword_id INTEGER NOT NULL,
                rank_date TEXT NOT NULL,
                position REAL,
                delta_from_prev REAL,
                delta_from_baseline REAL,
                UNIQUE(keyword_id, rank_date),
                FOREIGN KEY(keyword_id) REFERENCES keywords(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS clusters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                group_name TEXT NOT NULL,
                kpi_target INTEGER,
                kpi_type TEXT,
                keyword_count INTEGER DEFAULT 0,
                target_keywords INTEGER DEFAULT 0,
                UNIQUE(project_id, name, group_name),
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ai_insights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                insight_date TEXT NOT NULL,
                range_end TEXT,
                insight_type TEXT NOT NULL,
                cluster_name TEXT,
                keyword TEXT,
                author TEXT DEFAULT 'AI',
                is_pinned INTEGER DEFAULT 0,
                content_vi TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                event_date TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                impact_type TEXT NOT NULL,
                is_manual INTEGER DEFAULT 0,
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS shared_views (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                share_type TEXT NOT NULL,
                share_token TEXT NOT NULL UNIQUE,
                title TEXT,
                password_hash TEXT,
                state_json TEXT NOT NULL,
                snapshot_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_keywords_project ON keywords(project_id);
            CREATE INDEX IF NOT EXISTS idx_keywords_group ON keywords(project_id, group_name);
            CREATE INDEX IF NOT EXISTS idx_rankings_keyword_date ON rankings(keyword_id, rank_date);
            CREATE INDEX IF NOT EXISTS idx_events_project_date ON events(project_id, event_date);
            CREATE INDEX IF NOT EXISTS idx_ai_project_date ON ai_insights(project_id, insight_date);
            CREATE INDEX IF NOT EXISTS idx_shared_views_project ON shared_views(project_id, share_type);
            """
        )

        _ensure_column(connection, "projects", "sheet_gid", "sheet_gid TEXT")
        _ensure_column(connection, "projects", "source_name", "source_name TEXT")
        _ensure_column(connection, "projects", "source_type", "source_type TEXT DEFAULT 'upload'")
        _ensure_column(
            connection,
            "projects",
            "refresh_interval_minutes",
            "refresh_interval_minutes INTEGER DEFAULT 30",
        )
        _ensure_column(connection, "projects", "anthropic_api_key", "anthropic_api_key TEXT")
        _ensure_column(connection, "projects", "saved_view_state", "saved_view_state TEXT DEFAULT '{}'")
        _ensure_column(connection, "projects", "refresh_status", "refresh_status TEXT DEFAULT 'idle'")
        _ensure_column(connection, "projects", "refresh_started_at", "refresh_started_at TEXT")
        _ensure_column(connection, "projects", "refresh_finished_at", "refresh_finished_at TEXT")
        _ensure_column(connection, "projects", "refresh_error", "refresh_error TEXT")
        _ensure_column(connection, "projects", "refresh_result_json", "refresh_result_json TEXT")
        _ensure_column(connection, "keywords", "sub_cluster_name", "sub_cluster_name TEXT")
        _ensure_column(connection, "keywords", "target_url", "target_url TEXT")
        _ensure_column(connection, "keywords", "found_url", "found_url TEXT")
        _ensure_column(connection, "keywords", "notes", "notes TEXT DEFAULT ''")
        _ensure_column(connection, "clusters", "target_keywords", "target_keywords INTEGER DEFAULT 0")
        _ensure_column(connection, "events", "is_manual", "is_manual INTEGER DEFAULT 0")
        _ensure_column(connection, "ai_insights", "cluster_name", "cluster_name TEXT")
        _ensure_column(connection, "ai_insights", "keyword", "keyword TEXT")
        _ensure_column(connection, "ai_insights", "range_end", "range_end TEXT")
        _ensure_column(connection, "ai_insights", "author", "author TEXT DEFAULT 'AI'")
        _ensure_column(connection, "ai_insights", "is_pinned", "is_pinned INTEGER DEFAULT 0")
        connection.commit()


@contextmanager
def transaction() -> Iterator[ConnectionCompat]:
    connection = get_connection()
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def ping_database() -> dict[str, Any]:
    started_at = datetime.now()
    with get_connection() as connection:
        row = connection.execute("SELECT 1 AS alive").fetchone()
    elapsed_ms = int((datetime.now() - started_at).total_seconds() * 1000)
    return {
        "backend": str(DB_TARGET["backend"]),
        "database_url": str(DB_TARGET["url"]),
        "alive": bool(row and row["alive"] == 1),
        "latency_ms": elapsed_ms,
    }
