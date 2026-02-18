"""Callback delivery service with exponential backoff retry."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import aiohttp

from app.api.schemas import CallbackPayload

logger = logging.getLogger("ulrds.callback")

# Retry intervals in seconds (exponential backoff)
RETRY_DELAYS = [2, 5, 10]


async def deliver_callback(
    callback_url: str,
    task_id: str,
    status: str,
    result: Optional[Any] = None,
    error_message: Optional[str] = None,
    used_provider_id: Optional[str] = None,
    execution_time_ms: Optional[int] = None,
) -> bool:
    """Send task result to client's callback URL.

    Retries up to 3 times with exponential backoff (2s, 5s, 10s).
    Returns True if callback was delivered successfully.
    """
    payload = CallbackPayload(
        task_id=task_id,
        status=status,
        result=result,
        error_message=error_message,
        used_provider_id=used_provider_id,
        execution_time_ms=execution_time_ms,
    )
    data = payload.model_dump()

    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    callback_url,
                    json=data,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if 200 <= resp.status < 300:
                        logger.info(
                            "Callback delivered: task=%s url=%s status=%d",
                            task_id, callback_url, resp.status,
                        )
                        return True
                    logger.warning(
                        "Callback got non-2xx: task=%s url=%s status=%d (attempt %d)",
                        task_id, callback_url, resp.status, attempt + 1,
                    )
        except Exception as e:
            logger.warning(
                "Callback failed: task=%s url=%s error=%s (attempt %d)",
                task_id, callback_url, str(e), attempt + 1,
            )

        if attempt < len(RETRY_DELAYS):
            delay = RETRY_DELAYS[attempt]
            logger.debug("Retrying callback in %ds...", delay)
            await asyncio.sleep(delay)

    logger.error(
        "Callback exhausted all retries: task=%s url=%s — result available via GET /api/v1/task/%s",
        task_id, callback_url, task_id,
    )
    return False
