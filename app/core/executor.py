"""Task executor: calls LLM provider APIs via aiohttp."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp

from app.services.provider_manager import ProviderState

logger = logging.getLogger("ulrds.executor")


@dataclass
class ExecutionResult:
    """Result of a single LLM API call."""
    success: bool
    status_code: int
    body: str
    result: Optional[Any] = None
    execution_time_ms: int = 0


async def execute_llm_request(
    provider: ProviderState,
    payload: dict,
) -> ExecutionResult:
    """Send a chat completion request to an LLM provider.

    Follows the OpenAI-compatible /v1/chat/completions API format.
    """
    url = f"{provider.base_url.rstrip('/')}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {provider.api_key}",
    }

    request_body = {
        "model": provider.model_name,
        "messages": payload.get("messages", []),
        "stream": False,
    }

    # Pass through optional parameters
    for key in ("temperature", "max_tokens", "top_p", "frequency_penalty",
                "presence_penalty", "stop", "response_format"):
        if key in payload:
            request_body[key] = payload[key]

    timeout = aiohttp.ClientTimeout(total=provider.timeout_seconds)
    start = time.time()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=request_body,
                headers=headers,
                timeout=timeout,
            ) as resp:
                body = await resp.text()
                elapsed_ms = int((time.time() - start) * 1000)

                if 200 <= resp.status < 300:
                    result = _extract_content(body)
                    logger.info(
                        "LLM request success: provider=%s model=%s time=%dms",
                        provider.provider_id, provider.model_name, elapsed_ms,
                    )
                    return ExecutionResult(
                        success=True,
                        status_code=resp.status,
                        body=body,
                        result=result,
                        execution_time_ms=elapsed_ms,
                    )
                else:
                    logger.warning(
                        "LLM request failed: provider=%s status=%d time=%dms body=%s",
                        provider.provider_id, resp.status, elapsed_ms,
                        body[:500],
                    )
                    return ExecutionResult(
                        success=False,
                        status_code=resp.status,
                        body=body,
                        execution_time_ms=elapsed_ms,
                    )

    except aiohttp.ClientError as e:
        elapsed_ms = int((time.time() - start) * 1000)
        error_msg = f"Connection error: {type(e).__name__}: {e}"
        logger.error(
            "LLM request error: provider=%s error=%s time=%dms",
            provider.provider_id, error_msg, elapsed_ms,
        )
        return ExecutionResult(
            success=False,
            status_code=502,
            body=error_msg,
            execution_time_ms=elapsed_ms,
        )
    except TimeoutError:
        elapsed_ms = int((time.time() - start) * 1000)
        logger.error(
            "LLM request timeout: provider=%s timeout=%ds",
            provider.provider_id, provider.timeout_seconds,
        )
        return ExecutionResult(
            success=False,
            status_code=504,
            body=f"Request timed out after {provider.timeout_seconds}s",
            execution_time_ms=elapsed_ms,
        )


def _extract_content(body: str) -> Any:
    """Extract the assistant's message content from OpenAI-compatible response."""
    try:
        data = json.loads(body)
        choices = data.get("choices", [])
        if choices:
            message = choices[0].get("message", {})
            return message.get("content", "")
        return data
    except (json.JSONDecodeError, KeyError, IndexError):
        return body
