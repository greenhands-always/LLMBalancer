"""Core scheduler: the brain that orchestrates task dispatch.

Redesign highlights:
- Uses backoff when no providers available (no more busy-loop)
- Wakes up immediately when a provider becomes available (via _wake_event)
- Wakes up immediately when a new task arrives (via _wake_event)
- Clean separation: _loop handles timing, _dispatch_cycle handles logic
"""

from __future__ import annotations

import asyncio
import logging
import time

from app.api.schemas import ErrorCategory
from app.core.executor import execute_llm_request
from app.core.queue import TaskQueue
from app.db.database import get_db
from app.db.models import add_task_log, update_task_status
from app.services.callback import deliver_callback
from app.services.error_classifier import classify_error
from app.services.provider_manager import ProviderManager, ProviderState

logger = logging.getLogger("ulrds.scheduler")

# Backoff constants
_INITIAL_BACKOFF = 0.5    # 500ms
_MAX_BACKOFF = 10.0       # 10 seconds
_BACKOFF_MULTIPLIER = 2.0


class Scheduler:
    """Main scheduler loop that dispatches tasks to providers."""

    def __init__(
        self,
        queue: TaskQueue,
        provider_manager: ProviderManager,
    ) -> None:
        self._queue = queue
        self._pm = provider_manager
        self._running = False
        self._task: asyncio.Task | None = None
        self._wake_event = asyncio.Event()

    @property
    def wake_event(self) -> asyncio.Event:
        """Expose the wake event so external triggers can notify the scheduler."""
        return self._wake_event

    def notify(self) -> None:
        """Wake up the scheduler (e.g., when a provider becomes available or new task arrives)."""
        self._wake_event.set()

    async def start(self) -> None:
        """Start the scheduler background loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("Scheduler started")

    async def stop(self) -> None:
        """Gracefully stop the scheduler."""
        self._running = False
        self._wake_event.set()  # unblock if waiting
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Scheduler stopped")

    async def _loop(self) -> None:
        """Main scheduling loop with intelligent backoff."""
        logger.info("Scheduler loop started")
        backoff = 0.0

        while self._running:
            try:
                # Decide how long to wait
                wait_time = backoff if backoff > 0 else _MAX_BACKOFF

                # Wait for wake signal (new task / provider available) OR timeout
                self._wake_event.clear()
                try:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=wait_time)
                except asyncio.TimeoutError:
                    pass

                if not self._running:
                    break

                # Nothing in queue → reset backoff, wait for tasks
                if self._queue.size == 0:
                    backoff = 0.0
                    continue

                # Try to dispatch
                dispatched = await self._dispatch_cycle()

                if dispatched > 0:
                    backoff = 0.0  # reset on success
                else:
                    # Increase backoff: no provider was available
                    backoff = min(
                        (backoff or _INITIAL_BACKOFF) * _BACKOFF_MULTIPLIER,
                        _MAX_BACKOFF,
                    )
                    logger.debug(
                        "No dispatch this cycle (queue=%d), backing off %.1fs",
                        self._queue.size, backoff,
                    )

            except asyncio.CancelledError:
                logger.info("Scheduler loop cancelled")
                break
            except Exception:
                logger.exception("Unexpected error in scheduler loop")
                await asyncio.sleep(1)

    async def _dispatch_cycle(self) -> int:
        """Try to dispatch as many tasks as possible in one cycle.

        Returns the number of tasks dispatched.
        """
        dispatched = 0
        max_per_cycle = 50

        while dispatched < max_per_cycle:
            task = await self._queue.peek()
            if task is None:
                break

            now = time.time()
            task_id = task["task_id"]

            # Check task timeout (max_wait_seconds)
            if now - task["created_at"] > task.get("max_wait_seconds", 600):
                logger.warning("Task timeout in queue: %s", task_id)
                await self._handle_timeout(task)
                await self._queue.pop()
                continue

            # Find a matching provider (level = minimum required)
            provider = self._pm.find_provider(
                level=task["level"],
                allow_downgrade=task.get("allow_downgrade", False),
            )

            if provider is None:
                break

            # Acquire provider slot
            if not provider.acquire():
                logger.debug(
                    "Failed to acquire provider %s (concurrent=%d/%d)",
                    provider.provider_id, provider.current_concurrent, provider.max_concurrent,
                )
                break

            # Pop task from queue and dispatch
            popped = await self._queue.pop()
            if popped is None:
                provider.release()
                break

            logger.info(
                "Dispatching task %s -> provider %s (model=%s, level=%d)",
                task_id, provider.provider_id, provider.model_name, provider.level,
            )

            asyncio.create_task(self._execute_task(popped, provider))
            dispatched += 1

        if dispatched > 0:
            logger.info("Dispatched %d tasks this cycle", dispatched)

        return dispatched

    async def _execute_task(self, task: dict, provider: ProviderState) -> None:
        """Execute a task on a provider, handle result or error."""
        task_id = task["task_id"]

        try:
            async with get_db() as db:
                await update_task_status(
                    db, task_id, "DISPATCHED",
                    dispatched_at=time.time(),
                    used_provider_id=provider.provider_id,
                )
                await add_task_log(
                    db, task_id, "DISPATCHED",
                    provider_id=provider.provider_id,
                    detail=f"model={provider.model_name}",
                )

            result = await execute_llm_request(provider, task["payload"])

            if result.success:
                await self._handle_success(task, provider, result)
            else:
                await self._handle_error(task, provider, result)

        except Exception:
            logger.exception("Unhandled error executing task %s", task_id)
            await self._requeue_or_fail(task, "Unhandled execution error")
        finally:
            provider.release()
            # release() triggers _on_release → scheduler.notify() → wake up scheduler

    def _notify_sync_waiter(self, task_id: str) -> None:
        """Signal any SSE waiter for this task_id."""
        from app.api.routes import _sync_events
        event = _sync_events.get(task_id)
        if event is not None:
            event.set()

    async def _handle_success(self, task: dict, provider: ProviderState, result) -> None:
        """Handle successful LLM response."""
        task_id = task["task_id"]

        async with get_db() as db:
            await update_task_status(
                db, task_id, "COMPLETED",
                result=result.result,
                completed_at=time.time(),
                execution_time_ms=result.execution_time_ms,
                used_provider_id=provider.provider_id,
            )
            await add_task_log(
                db, task_id, "COMPLETED",
                provider_id=provider.provider_id,
                detail=f"time={result.execution_time_ms}ms",
            )

        logger.info(
            "Task completed: %s provider=%s time=%dms",
            task_id, provider.provider_id, result.execution_time_ms,
        )

        self._notify_sync_waiter(task_id)

        if task.get("callback_url"):
            asyncio.create_task(
                deliver_callback(
                    callback_url=task["callback_url"],
                    task_id=task_id,
                    status="COMPLETED",
                    result=result.result,
                    used_provider_id=provider.provider_id,
                    execution_time_ms=result.execution_time_ms,
                )
            )

    async def _handle_error(self, task: dict, provider: ProviderState, result) -> None:
        """Handle failed LLM response: classify error and take action."""
        task_id = task["task_id"]

        classification = classify_error(result.status_code, result.body)

        async with get_db() as db:
            await add_task_log(
                db, task_id, "ERROR",
                provider_id=provider.provider_id,
                detail=f"status={result.status_code} category={classification.category.value} body={result.body[:200]}",
            )

        if classification.should_disable_provider:
            provider.set_disabled(
                f"{classification.category.value}: status={result.status_code}"
            )
        elif classification.cooldown_seconds > 0:
            provider.set_cooldown(classification.cooldown_seconds)

        if classification.should_retry_task:
            await self._requeue_or_fail(
                task,
                f"{classification.category.value} from {provider.provider_id}",
            )
        else:
            async with get_db() as db:
                await update_task_status(
                    db, task_id, "FAILED",
                    error_message=f"{classification.category.value}: {result.body[:500]}",
                    completed_at=time.time(),
                    execution_time_ms=result.execution_time_ms,
                )
                await add_task_log(db, task_id, "FAILED", detail="Not retryable")

            logger.warning("Task failed (not retryable): %s — %s", task_id, classification.category.value)

            self._notify_sync_waiter(task_id)

            if task.get("callback_url"):
                asyncio.create_task(
                    deliver_callback(
                        callback_url=task["callback_url"],
                        task_id=task_id,
                        status="FAILED",
                        error_message=f"{classification.category.value}: {result.body[:500]}",
                        execution_time_ms=result.execution_time_ms,
                    )
                )

    async def _handle_timeout(self, task: dict) -> None:
        """Handle a task that exceeded its max_wait_seconds."""
        task_id = task["task_id"]

        async with get_db() as db:
            await update_task_status(
                db, task_id, "TIMEOUT",
                error_message="Exceeded max_wait_seconds in queue",
                completed_at=time.time(),
            )
            await add_task_log(db, task_id, "TIMEOUT", detail="Queue wait timeout")

        logger.warning("Task timed out in queue: %s", task_id)

        self._notify_sync_waiter(task_id)

        if task.get("callback_url"):
            asyncio.create_task(
                deliver_callback(
                    callback_url=task["callback_url"],
                    task_id=task_id,
                    status="TIMEOUT",
                    error_message="Exceeded max_wait_seconds waiting for available provider",
                )
            )

    async def _requeue_or_fail(self, task: dict, reason: str) -> None:
        """Requeue a task if retries remain, otherwise mark it as failed."""
        task_id = task["task_id"]
        retry_count = task.get("retry_count", 0) + 1
        max_retries = task.get("max_retries", 3)

        if retry_count >= max_retries:
            async with get_db() as db:
                await update_task_status(
                    db, task_id, "FAILED",
                    error_message=f"Max retries ({max_retries}) reached. Last: {reason}",
                    completed_at=time.time(),
                    retry_count=retry_count,
                )
                await add_task_log(db, task_id, "FAILED", detail=f"Max retries reached: {reason}")

            logger.warning("Task exhausted retries: %s (%d/%d)", task_id, retry_count, max_retries)

            self._notify_sync_waiter(task_id)

            if task.get("callback_url"):
                asyncio.create_task(
                    deliver_callback(
                        callback_url=task["callback_url"],
                        task_id=task_id,
                        status="FAILED",
                        error_message=f"Max retries reached. Last error: {reason}",
                    )
                )
            return

        task["retry_count"] = retry_count
        async with get_db() as db:
            await update_task_status(
                db, task_id, "QUEUED",
                retry_count=retry_count,
            )
            await add_task_log(
                db, task_id, "REQUEUED",
                detail=f"retry {retry_count}/{max_retries}: {reason}",
            )

        await self._queue.push(task)
        logger.info("Task requeued: %s (retry %d/%d) — %s", task_id, retry_count, max_retries, reason)
