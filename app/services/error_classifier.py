"""Error classification: rule engine + optional LLM fallback.

Analyzes HTTP error responses from LLM providers and categorizes them
to drive the provider state machine (cooldown / disable / retry).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from app.api.schemas import ErrorCategory

logger = logging.getLogger("ulrds.error_classifier")


@dataclass
class ClassificationResult:
    category: ErrorCategory
    cooldown_seconds: float
    should_retry_task: bool
    should_disable_provider: bool
    detail: str


# ── Rule definitions ───────────────────────────────────────────────────
# Each rule: (status_code_min, status_code_max, keywords, category)

_RULES: list[tuple[int, int, list[str], ErrorCategory]] = [
    (429, 429, ["rate limit", "quota", "exceeded", "too many", "ratelimit",
                "requests per minute", "tokens per minute", "tpm", "rpm"],
     ErrorCategory.QUOTA_EXCEEDED),

    (401, 403, ["auth", "unauthorized", "forbidden", "invalid key", "invalid api",
                "api key", "permission", "denied", "expired token"],
     ErrorCategory.AUTH_FAILED),

    (500, 503, ["server", "internal", "bad gateway", "unavailable", "overloaded",
                "capacity", "temporarily"],
     ErrorCategory.SERVER_BUSY),

    (400, 400, ["content filter", "safety", "blocked", "moderation", "harmful",
                "policy", "flagged", "content_policy"],
     ErrorCategory.CONTENT_FILTER),
]

# ── Decision matrix ────────────────────────────────────────────────────

_DECISIONS: dict[ErrorCategory, tuple[float, bool, bool]] = {
    # category: (cooldown_seconds, should_retry_task, should_disable_provider)
    ErrorCategory.QUOTA_EXCEEDED: (600.0, True, False),   # 10 min cooldown
    ErrorCategory.SERVER_BUSY:    (60.0, True, False),     # 1 min cooldown
    ErrorCategory.AUTH_FAILED:    (0.0, True, True),       # disable, retry on other
    ErrorCategory.CONTENT_FILTER: (0.0, False, False),     # task fails, no retry
    ErrorCategory.UNKNOWN:        (300.0, True, False),    # 5 min cooldown, cautious
}


def classify_error(status_code: int, body: str) -> ClassificationResult:
    """Classify an API error using rule engine.

    Args:
        status_code: HTTP status code from the provider.
        body: Response body text (JSON string or plain text).

    Returns:
        ClassificationResult with category, cooldown, and action flags.
    """
    body_lower = body.lower()
    category = _match_rules(status_code, body_lower)

    if category is None:
        category = _fallback_by_status_code(status_code)

    cooldown, retry, disable = _DECISIONS[category]

    result = ClassificationResult(
        category=category,
        cooldown_seconds=cooldown,
        should_retry_task=retry,
        should_disable_provider=disable,
        detail=f"status={status_code}, category={category.value}",
    )

    logger.info(
        "Error classified: status=%d category=%s cooldown=%.0fs retry=%s disable=%s",
        status_code, category.value, cooldown, retry, disable,
    )
    return result


def _match_rules(status_code: int, body_lower: str) -> Optional[ErrorCategory]:
    """Try to match error against predefined rules."""
    for code_min, code_max, keywords, category in _RULES:
        if code_min <= status_code <= code_max:
            for kw in keywords:
                if kw in body_lower:
                    return category
    return None


def _fallback_by_status_code(status_code: int) -> ErrorCategory:
    """Fallback classification based solely on status code ranges."""
    if status_code == 429:
        return ErrorCategory.QUOTA_EXCEEDED
    if status_code in (401, 403):
        return ErrorCategory.AUTH_FAILED
    if 500 <= status_code <= 599:
        return ErrorCategory.SERVER_BUSY
    return ErrorCategory.UNKNOWN


# ── LLM fallback interface (Phase 2 placeholder) ──────────────────────

async def classify_with_llm(
    status_code: int,
    body: str,
    llm_base_url: Optional[str] = None,
    llm_api_key: Optional[str] = None,
) -> Optional[ErrorCategory]:
    """Phase 2: Use a small LLM to classify UNKNOWN errors.

    Currently a placeholder. When implemented, this will call a local
    small model (e.g. Qwen-1.5B) with a structured prompt to classify
    the error into one of the known categories.

    Returns None if LLM classification is not available or fails.
    """
    # TODO: Phase 2 implementation
    # Prompt template:
    # "分析以下API错误信息，从给定类别中选择一个最匹配的：
    #  [QUOTA_EXCEEDED, AUTH_FAILED, SERVER_BUSY, CONTENT_FILTER, UNKNOWN]。
    #  只输出类别名称。
    #  HTTP状态码: {status_code}
    #  错误内容: {body}"
    logger.debug("LLM fallback not yet implemented, returning None")
    return None
