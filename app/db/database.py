"""SQLite database initialization, connection management, and schema migration."""

from __future__ import annotations

import logging
import time
import aiosqlite
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

logger = logging.getLogger("ulrds.db")

DB_PATH = os.environ.get("ULRDS_DB_PATH", "ulrds.db")

# ── Schema (two-table design) ──────────────────────────────────────────

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
    notes           TEXT NOT NULL DEFAULT '',
    updated_at      REAL
);

CREATE TABLE IF NOT EXISTS provider_models (
    provider_id     TEXT NOT NULL,
    model_name      TEXT NOT NULL,
    level           INTEGER NOT NULL DEFAULT 1,
    rpm_limit       INTEGER NOT NULL DEFAULT 0,
    max_concurrent  INTEGER NOT NULL DEFAULT 1,
    timeout_seconds INTEGER NOT NULL DEFAULT 120,
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    next_available_time REAL,
    disabled_reason TEXT,
    updated_at      REAL,
    PRIMARY KEY (provider_id, model_name),
    FOREIGN KEY (provider_id) REFERENCES providers(provider_id) ON DELETE CASCADE
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


# ── Migration: single-table → two-table ───────────────────────────────

async def _maybe_migrate(db: aiosqlite.Connection) -> None:
    """Migrate old single-table providers schema to two-table design.

    States handled:
    - Old schema:       providers has model_name column → full migration
    - Partial migration: _providers_old exists (previous run crashed) → resume
    - New schema:       nothing to do
    """
    # Keep FK off during the whole migration to avoid ordering problems
    await db.execute("PRAGMA foreign_keys=OFF")

    cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {row[0] for row in await cursor.fetchall()}

    has_backup = "_providers_old" in existing

    if not has_backup:
        cursor = await db.execute("PRAGMA table_info(providers)")
        cols = {row[1] for row in await cursor.fetchall()}
        if "model_name" not in cols:
            # Already on new schema (or fresh DB)
            await db.execute("PRAGMA foreign_keys=ON")
            return

    source = "_providers_old" if has_backup else "providers"
    logger.info("Migrating providers schema to two-table design (source=%s)...", source)

    # Read all old rows
    cursor = await db.execute(f"SELECT * FROM {source}")  # noqa: S608
    desc = cursor.description
    old_rows = [dict(zip([d[0] for d in desc], row)) for row in await cursor.fetchall()]

    # Rebuild providers table with new lean schema
    if not has_backup:
        # Rename first, then create new table
        await db.execute("ALTER TABLE providers RENAME TO _providers_old")
    # Drop the current (possibly empty) providers table created by a failed migration
    await db.execute("DROP TABLE IF EXISTS providers")
    await db.execute("""
        CREATE TABLE providers (
            provider_id TEXT PRIMARY KEY,
            base_url    TEXT NOT NULL,
            api_key     TEXT NOT NULL,
            notes       TEXT NOT NULL DEFAULT '',
            updated_at  REAL
        )
    """)

    # Create provider_models if needed
    await db.execute("DROP TABLE IF EXISTS provider_models")
    await db.execute("""
        CREATE TABLE provider_models (
            provider_id     TEXT NOT NULL,
            model_name      TEXT NOT NULL,
            level           INTEGER NOT NULL DEFAULT 1,
            rpm_limit       INTEGER NOT NULL DEFAULT 0,
            max_concurrent  INTEGER NOT NULL DEFAULT 1,
            timeout_seconds INTEGER NOT NULL DEFAULT 120,
            status          TEXT NOT NULL DEFAULT 'ACTIVE',
            next_available_time REAL,
            disabled_reason TEXT,
            updated_at      REAL,
            PRIMARY KEY (provider_id, model_name),
            FOREIGN KEY (provider_id) REFERENCES providers(provider_id) ON DELETE CASCADE
        )
    """)

    # Group old rows by connection to derive base provider_id
    seen: dict[tuple, str] = {}
    inserted_providers: set[str] = set()
    model_rows: list[tuple] = []

    for p in old_rows:
        model_name = str(p.get("model_name") or "unknown")
        group_key = (p["base_url"], p["api_key"])

        if group_key not in seen:
            old_pid = str(p["provider_id"])
            if model_name != "unknown" and old_pid.endswith(f"__{model_name}"):
                base_pid = old_pid[: -(len(model_name) + 2)]
            elif "__" in old_pid:
                base_pid = old_pid.rsplit("__", 1)[0]
            else:
                base_pid = old_pid
            seen[group_key] = base_pid

        base_pid = seen[group_key]
        level = int(p.get("level") or p.get("preferred_level") or 1)

        if base_pid not in inserted_providers:
            await db.execute(
                "INSERT INTO providers (provider_id, base_url, api_key, notes, updated_at)"
                " VALUES (?, ?, ?, '', ?)",
                (base_pid, p["base_url"], p["api_key"], time.time()),
            )
            inserted_providers.add(base_pid)

        model_rows.append((
            base_pid, model_name, level,
            int(p.get("rpm_limit", 0)),
            int(p.get("max_concurrent", 1)),
            int(p.get("timeout_seconds", 120)),
            str(p.get("status") or "ACTIVE"),
            time.time(),
        ))

    # Insert all model rows (FK still OFF so no constraint check)
    for row in model_rows:
        await db.execute(
            "INSERT OR IGNORE INTO provider_models"
            " (provider_id, model_name, level, rpm_limit, max_concurrent,"
            "  timeout_seconds, status, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            row,
        )

    await db.execute("DROP TABLE IF EXISTS _providers_old")
    await db.commit()
    await db.execute("PRAGMA foreign_keys=ON")
    logger.info(
        "Migration complete: %d provider connections, %d models",
        len(inserted_providers), len(model_rows),
    )


# ── Public API ────────────────────────────────────────────────────────

async def init_db(db_path: str | None = None) -> None:
    """Initialize database: run migration if needed, then apply schema."""
    path = db_path or DB_PATH
    async with aiosqlite.connect(path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await _maybe_migrate(db)
        # Apply schema (CREATE TABLE IF NOT EXISTS — safe to run after migration)
        await db.executescript(SCHEMA_SQL)
        await db.execute("PRAGMA foreign_keys=ON")
        await db.commit()


@asynccontextmanager
async def get_db(db_path: str | None = None) -> AsyncGenerator[aiosqlite.Connection, None]:
    """Get an async database connection."""
    path = db_path or DB_PATH
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=5000")
    await db.execute("PRAGMA foreign_keys=ON")
    try:
        yield db
    finally:
        await db.close()
