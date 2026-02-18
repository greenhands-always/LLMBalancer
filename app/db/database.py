"""SQLite database initialization and connection management."""

from __future__ import annotations

import aiosqlite
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

DB_PATH = os.environ.get("ULRDS_DB_PATH", "ulrds.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT PRIMARY KEY,
    level           INTEGER NOT NULL,
    type            TEXT NOT NULL DEFAULT 'text',
    priority        INTEGER NOT NULL DEFAULT 5,
    payload         TEXT NOT NULL,
    callback_url    TEXT,
    allow_downgrade INTEGER NOT NULL DEFAULT 0,
    max_wait_seconds INTEGER NOT NULL DEFAULT 600,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'QUEUED',
    result          TEXT,
    error_message   TEXT,
    used_provider_id TEXT,
    created_at      REAL NOT NULL,
    dispatched_at   REAL,
    completed_at    REAL,
    execution_time_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority DESC, created_at ASC);

CREATE TABLE IF NOT EXISTS providers (
    provider_id     TEXT PRIMARY KEY,
    base_url        TEXT NOT NULL,
    api_key         TEXT NOT NULL,
    model_name      TEXT NOT NULL,
    level           INTEGER NOT NULL,
    rpm_limit       INTEGER NOT NULL DEFAULT 0,
    max_concurrent  INTEGER NOT NULL DEFAULT 1,
    timeout_seconds INTEGER NOT NULL DEFAULT 120,
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    next_available_time REAL,
    disabled_reason TEXT,
    updated_at      REAL
);

CREATE TABLE IF NOT EXISTS task_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL,
    provider_id     TEXT,
    event           TEXT NOT NULL,
    detail          TEXT,
    timestamp       REAL NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

CREATE INDEX IF NOT EXISTS idx_task_logs_task_id ON task_logs(task_id);
"""


async def init_db(db_path: str | None = None) -> None:
    """Initialize database with schema."""
    path = db_path or DB_PATH
    async with aiosqlite.connect(path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.executescript(SCHEMA_SQL)
        await db.commit()


@asynccontextmanager
async def get_db(db_path: str | None = None) -> AsyncGenerator[aiosqlite.Connection, None]:
    """Get an async database connection."""
    path = db_path or DB_PATH
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=5000")
    try:
        yield db
    finally:
        await db.close()
