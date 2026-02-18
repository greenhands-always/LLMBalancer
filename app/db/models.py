"""Database CRUD operations for tasks, providers, and logs."""

from __future__ import annotations

import json
import time
from typing import Any

import aiosqlite


# ── Task CRUD ──────────────────────────────────────────────────────────

async def create_task(db: aiosqlite.Connection, task: dict) -> None:
    """Insert a new task into the database."""
    await db.execute(
        """INSERT INTO tasks
           (task_id, level, type, priority, payload, callback_url,
            allow_downgrade, max_wait_seconds, max_retries, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'QUEUED', ?)""",
        (
            task["task_id"],
            task["level"],
            task["type"],
            task["priority"],
            json.dumps(task["payload"], ensure_ascii=False),
            task.get("callback_url"),
            1 if task.get("allow_downgrade") else 0,
            task.get("max_wait_seconds", 600),
            task.get("max_retries", 3),
            time.time(),
        ),
    )
    await db.commit()


async def get_task(db: aiosqlite.Connection, task_id: str) -> dict | None:
    """Fetch a single task by ID."""
    cursor = await db.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return _row_to_task(row)


async def get_queued_tasks(db: aiosqlite.Connection) -> list[dict]:
    """Fetch all QUEUED tasks ordered by priority DESC, created_at ASC."""
    cursor = await db.execute(
        "SELECT * FROM tasks WHERE status = 'QUEUED' ORDER BY priority DESC, created_at ASC"
    )
    rows = await cursor.fetchall()
    return [_row_to_task(r) for r in rows]


async def update_task_status(
    db: aiosqlite.Connection,
    task_id: str,
    status: str,
    **kwargs: Any,
) -> None:
    """Update task status and optional fields."""
    sets = ["status = ?"]
    vals: list[Any] = [status]

    for field in ("result", "error_message", "used_provider_id",
                  "dispatched_at", "completed_at", "execution_time_ms",
                  "retry_count"):
        if field in kwargs:
            sets.append(f"{field} = ?")
            val = kwargs[field]
            if field == "result" and not isinstance(val, str):
                val = json.dumps(val, ensure_ascii=False)
            vals.append(val)

    vals.append(task_id)
    await db.execute(
        f"UPDATE tasks SET {', '.join(sets)} WHERE task_id = ?", vals
    )
    await db.commit()


# ── Provider CRUD ──────────────────────────────────────────────────────

async def upsert_provider(db: aiosqlite.Connection, provider: dict) -> None:
    """Insert or update a provider record."""
    await db.execute(
        """INSERT INTO providers
           (provider_id, base_url, api_key, model_name, level,
            rpm_limit, max_concurrent, timeout_seconds, status, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)
           ON CONFLICT(provider_id) DO UPDATE SET
            base_url=excluded.base_url, api_key=excluded.api_key,
            model_name=excluded.model_name, level=excluded.level,
            rpm_limit=excluded.rpm_limit,
            max_concurrent=excluded.max_concurrent, timeout_seconds=excluded.timeout_seconds,
            updated_at=excluded.updated_at""",
        (
            provider["provider_id"],
            provider["base_url"],
            provider["api_key"],
            provider["model_name"],
            provider["level"],
            provider.get("rpm_limit", 0),
            provider.get("max_concurrent", 1),
            provider.get("timeout_seconds", 120),
            time.time(),
        ),
    )
    await db.commit()


async def get_all_providers(db: aiosqlite.Connection) -> list[dict]:
    """Fetch all provider records."""
    cursor = await db.execute("SELECT * FROM providers")
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def update_provider_status(
    db: aiosqlite.Connection,
    provider_id: str,
    status: str,
    next_available_time: float | None = None,
    disabled_reason: str | None = None,
) -> None:
    """Update provider status."""
    await db.execute(
        """UPDATE providers
           SET status = ?, next_available_time = ?, disabled_reason = ?, updated_at = ?
           WHERE provider_id = ?""",
        (status, next_available_time, disabled_reason, time.time(), provider_id),
    )
    await db.commit()


# ── Task Log ──────────────────────────────────────────────────────────

async def add_task_log(
    db: aiosqlite.Connection,
    task_id: str,
    event: str,
    provider_id: str | None = None,
    detail: str | None = None,
) -> None:
    """Append a log entry for a task."""
    await db.execute(
        "INSERT INTO task_logs (task_id, provider_id, event, detail, timestamp) VALUES (?, ?, ?, ?, ?)",
        (task_id, provider_id, event, detail, time.time()),
    )
    await db.commit()


async def get_task_logs(db: aiosqlite.Connection, task_id: str) -> list[dict]:
    """Fetch all log entries for a task."""
    cursor = await db.execute(
        "SELECT * FROM task_logs WHERE task_id = ? ORDER BY timestamp ASC", (task_id,)
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


# ── Helpers ────────────────────────────────────────────────────────────

def _row_to_task(row: aiosqlite.Row) -> dict:
    d = dict(row)
    if isinstance(d.get("payload"), str):
        d["payload"] = json.loads(d["payload"])
    d["allow_downgrade"] = bool(d.get("allow_downgrade"))
    return d
