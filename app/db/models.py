"""Database CRUD operations for tasks, providers, and logs."""

from __future__ import annotations

import json
import time
from typing import Any

import aiosqlite


# ── Task CRUD ──────────────────────────────────────────────────────────

async def create_task(db: aiosqlite.Connection, task: dict) -> None:
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
    cursor = await db.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
    row = await cursor.fetchone()
    return _row_to_task(row) if row else None


async def get_queued_tasks(db: aiosqlite.Connection) -> list[dict]:
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
    sets = ["status = ?"]
    vals: list[Any] = [status]

    for field in ("result", "error_message", "used_provider_id",
                  "dispatched_at", "completed_at", "execution_time_ms", "retry_count"):
        if field in kwargs:
            sets.append(f"{field} = ?")
            val = kwargs[field]
            if field == "result" and not isinstance(val, str):
                val = json.dumps(val, ensure_ascii=False)
            vals.append(val)

    vals.append(task_id)
    await db.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE task_id = ?", vals)
    await db.commit()


# ── Provider connection CRUD ───────────────────────────────────────────

async def upsert_provider_connection(db: aiosqlite.Connection, provider: dict) -> None:
    """Insert or update a provider's connection info (base_url, api_key, notes)."""
    await db.execute(
        """INSERT INTO providers (provider_id, base_url, api_key, notes, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(provider_id) DO UPDATE SET
             base_url=excluded.base_url,
             api_key=excluded.api_key,
             notes=excluded.notes,
             updated_at=excluded.updated_at""",
        (
            provider["provider_id"],
            provider["base_url"],
            provider["api_key"],
            provider.get("notes", ""),
            time.time(),
        ),
    )
    await db.commit()


async def get_all_providers(db: aiosqlite.Connection) -> list[dict]:
    """Get all providers with their models nested."""
    cursor = await db.execute("SELECT * FROM providers ORDER BY provider_id")
    providers = [dict(r) for r in await cursor.fetchall()]

    cursor = await db.execute(
        "SELECT * FROM provider_models ORDER BY provider_id, model_name"
    )
    all_models = [dict(r) for r in await cursor.fetchall()]

    models_by_pid: dict[str, list] = {}
    for m in all_models:
        models_by_pid.setdefault(m["provider_id"], []).append(m)

    for p in providers:
        p["models"] = models_by_pid.get(p["provider_id"], [])

    return providers


async def get_provider_by_id(db: aiosqlite.Connection, provider_id: str) -> dict | None:
    """Get a single provider with its models."""
    cursor = await db.execute(
        "SELECT * FROM providers WHERE provider_id = ?", (provider_id,)
    )
    row = await cursor.fetchone()
    if not row:
        return None
    provider = dict(row)
    cursor = await db.execute(
        "SELECT * FROM provider_models WHERE provider_id = ? ORDER BY model_name",
        (provider_id,),
    )
    provider["models"] = [dict(r) for r in await cursor.fetchall()]
    return provider


async def delete_provider(db: aiosqlite.Connection, provider_id: str) -> None:
    """Delete a provider and all its models (cascade)."""
    await db.execute("DELETE FROM providers WHERE provider_id = ?", (provider_id,))
    await db.commit()


# ── Provider model CRUD ────────────────────────────────────────────────

async def upsert_provider_model(
    db: aiosqlite.Connection, provider_id: str, model: dict
) -> None:
    """Insert or update a single model under a provider."""
    await db.execute(
        """INSERT INTO provider_models
           (provider_id, model_name, level, rpm_limit, max_concurrent,
            timeout_seconds, status, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?)
           ON CONFLICT(provider_id, model_name) DO UPDATE SET
             level=excluded.level,
             rpm_limit=excluded.rpm_limit,
             max_concurrent=excluded.max_concurrent,
             timeout_seconds=excluded.timeout_seconds,
             updated_at=excluded.updated_at""",
        (
            provider_id,
            model["model_name"],
            int(model.get("level", 1)),
            int(model.get("rpm_limit", 0)),
            int(model.get("max_concurrent", 1)),
            int(model.get("timeout_seconds", 120)),
            time.time(),
        ),
    )
    await db.commit()


async def delete_provider_model(
    db: aiosqlite.Connection, provider_id: str, model_name: str
) -> None:
    """Delete a single model from a provider."""
    await db.execute(
        "DELETE FROM provider_models WHERE provider_id = ? AND model_name = ?",
        (provider_id, model_name),
    )
    await db.commit()


async def get_all_provider_models_flat(db: aiosqlite.Connection) -> list[dict]:
    """Flat join of providers + provider_models for ProviderManager loading.

    Each record includes connection info + model config.
    The caller uses {provider_id}__{model_name} as the runtime key.
    """
    cursor = await db.execute(
        """SELECT
               p.provider_id, p.base_url, p.api_key,
               m.model_name, m.level, m.rpm_limit, m.max_concurrent,
               m.timeout_seconds, m.status, m.disabled_reason
           FROM providers p
           JOIN provider_models m ON p.provider_id = m.provider_id
           ORDER BY p.provider_id, m.model_name"""
    )
    return [dict(r) for r in await cursor.fetchall()]


# ── Task Log ──────────────────────────────────────────────────────────

async def add_task_log(
    db: aiosqlite.Connection,
    task_id: str,
    event: str,
    provider_id: str | None = None,
    detail: str | None = None,
) -> None:
    await db.execute(
        "INSERT INTO task_logs (task_id, provider_id, event, detail, timestamp) VALUES (?, ?, ?, ?, ?)",
        (task_id, provider_id, event, detail, time.time()),
    )
    await db.commit()


async def get_task_logs(db: aiosqlite.Connection, task_id: str) -> list[dict]:
    cursor = await db.execute(
        "SELECT * FROM task_logs WHERE task_id = ? ORDER BY timestamp ASC", (task_id,)
    )
    return [dict(r) for r in await cursor.fetchall()]


# ── Helpers ────────────────────────────────────────────────────────────

def _row_to_task(row: aiosqlite.Row) -> dict:
    d = dict(row)
    if isinstance(d.get("payload"), str):
        d["payload"] = json.loads(d["payload"])
    d["allow_downgrade"] = bool(d.get("allow_downgrade"))
    return d
