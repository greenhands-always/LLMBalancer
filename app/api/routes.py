"""API route definitions for ULRDS."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.auth import verify_token
from app.api.schemas import (
    ProviderInfoResponse,
    StatsResponse,
    TaskStatusResponse,
    TaskSubmitRequest,
    TaskSubmitResponse,
)
from app.db.database import get_db
from app.db.models import create_task, get_task, get_task_logs

router = APIRouter(prefix="/api/v1", dependencies=[Depends(verify_token)])


def _get_app_state():
    """Lazily import to avoid circular imports at module level."""
    from main import app_state
    return app_state


# ── Task endpoints ─────────────────────────────────────────────────────

@router.post("/task/submit", response_model=TaskSubmitResponse)
async def submit_task(req: TaskSubmitRequest):
    """Submit a new task to the scheduler queue."""
    from app.api import routes as routes_module
    import logging
    logger = logging.getLogger("ulrds.api")
    
    state = _get_app_state()

    task_id = str(uuid.uuid4())
    task = {
        "task_id": task_id,
        "level": req.model_config_data.level,
        "type": req.model_config_data.type,
        "priority": req.priority,
        "payload": req.payload,
        "callback_url": req.callback_url,
        "allow_downgrade": req.allow_downgrade,
        "max_wait_seconds": req.max_wait_seconds,
        "max_retries": 3,
        "retry_count": 0,
    }

    logger.info(
        "Task submitted: task_id=%s level=%d priority=%d allow_downgrade=%s",
        task_id, task["level"], task["priority"], task["allow_downgrade"]
    )

    # Persist to database
    async with get_db() as db:
        await create_task(db, task)

    # Add to in-memory queue
    import time
    task["created_at"] = time.time()
    await state["queue"].push(task)

    logger.info("Task queued: task_id=%s queue_size=%d", task_id, state["queue"].size)

    return TaskSubmitResponse(
        task_id=task_id,
        status="QUEUED",
        message="Task submitted successfully",
    )


@router.get("/task/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str):
    """Query task status and result."""
    async with get_db() as db:
        task = await get_task(db, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    return TaskStatusResponse(
        task_id=task["task_id"],
        status=task["status"],
        level=task["level"],
        priority=task["priority"],
        result=task.get("result"),
        error_message=task.get("error_message"),
        used_provider_id=task.get("used_provider_id"),
        created_at=task["created_at"],
        dispatched_at=task.get("dispatched_at"),
        completed_at=task.get("completed_at"),
        execution_time_ms=task.get("execution_time_ms"),
        retry_count=task.get("retry_count", 0),
    )


@router.get("/task/{task_id}/logs")
async def get_task_log_entries(task_id: str):
    """Get all log entries for a task (for debugging)."""
    async with get_db() as db:
        task = await get_task(db, task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        logs = await get_task_logs(db, task_id)
    return {"task_id": task_id, "logs": logs}


# ── Provider endpoints ─────────────────────────────────────────────────

@router.get("/providers", response_model=list[ProviderInfoResponse])
async def list_providers():
    """List all providers and their current status."""
    state = _get_app_state()
    providers = state["provider_manager"].get_all_providers()
    return [
        ProviderInfoResponse(**p.to_dict())
        for p in providers
    ]


@router.post("/providers/{provider_id}/reset")
async def reset_provider(provider_id: str):
    """Manually reset a DISABLED provider back to ACTIVE."""
    state = _get_app_state()
    success = state["provider_manager"].reset_provider(provider_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Provider {provider_id} not found")
    return {"message": f"Provider {provider_id} reset to ACTIVE"}


# ── Stats endpoint ─────────────────────────────────────────────────────

@router.get("/stats", response_model=StatsResponse)
async def get_stats():
    """Get system statistics."""
    state = _get_app_state()
    pm_stats = state["provider_manager"].get_stats()

    async with get_db() as db:
        cursor = await db.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
        rows = await cursor.fetchall()
        task_counts = {row[0]: row[1] for row in rows}

    return StatsResponse(
        queue_depth=state["queue"].size,
        tasks_dispatched=task_counts.get("DISPATCHED", 0),
        tasks_completed=task_counts.get("COMPLETED", 0),
        tasks_failed=task_counts.get("FAILED", 0) + task_counts.get("TIMEOUT", 0),
        **pm_stats,
    )


# ── Debug endpoints ────────────────────────────────────────────────


@router.get("/debug/scheduler")
async def debug_scheduler():
    """Debug endpoint: show current scheduler state."""
    state = _get_app_state()
    pm = state["provider_manager"]

    all_providers = pm.get_all_providers()
    providers_by_status: dict[str, list] = {}

    # Dynamic: collect actual levels from providers
    all_levels = sorted({p.level for p in all_providers})
    available_by_level: dict[int, list] = {lv: [] for lv in all_levels}

    for p in all_providers:
        status_val = p.status.value
        if status_val not in providers_by_status:
            providers_by_status[status_val] = []

        providers_by_status[status_val].append({
            "provider_id": p.provider_id,
            "model_name": p.model_name,
            "level": p.level,
            "concurrent": f"{p.current_concurrent}/{p.max_concurrent}",
            "is_available": p.is_available(),
        })

        if p.is_available():
            # A level-N provider can serve any request where level <= N
            for lv in all_levels:
                if lv <= p.level:
                    available_by_level[lv].append(p.provider_id)

    queue_size = state["queue"].size
    next_task = await state["queue"].peek()

    return {
        "scheduler_running": state["scheduler"]._running,
        "queue": {
            "size": queue_size,
            "next_task": {
                "task_id": next_task["task_id"],
                "level": next_task["level"],
                "priority": next_task["priority"],
                "allow_downgrade": next_task.get("allow_downgrade", False),
            } if next_task else None,
        },
        "providers": {
            "total": len(all_providers),
            "by_status": providers_by_status,
            "available_count_by_level": {
                lv: len(pids) for lv, pids in available_by_level.items()
            },
        },
    }
