from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATABASE_URL = "sqlite:////data/seo_dashboard.db" if Path("/data").exists() else "sqlite:///./data/seo_dashboard.db"


def resolve_database_path(database_url: str | None = None) -> Path:
    url = (database_url or os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL).strip()
    if not url.startswith("sqlite:///"):
        raise ValueError("Only sqlite DATABASE_URL values are supported.")
    raw_path = url.removeprefix("sqlite:///")
    if raw_path.startswith("/"):
        path = Path(raw_path)
    else:
        path = (ROOT / raw_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


DB_PATH = resolve_database_path()


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL;")
    connection.execute("PRAGMA foreign_keys=ON;")
    connection.execute("PRAGMA synchronous=NORMAL;")
    return connection


def _column_exists(connection: sqlite3.Connection, table: str, column: str) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def _ensure_column(connection: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
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
def transaction() -> Iterator[sqlite3.Connection]:
    connection = get_connection()
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
