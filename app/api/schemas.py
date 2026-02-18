"""Pydantic models for API request/response schemas."""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# ── Enums ──────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    QUEUED = "QUEUED"
    DISPATCHED = "DISPATCHED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


class ProviderStatus(str, Enum):
    ACTIVE = "ACTIVE"
    COOLDOWN = "COOLDOWN"
    DISABLED = "DISABLED"


class ErrorCategory(str, Enum):
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    AUTH_FAILED = "AUTH_FAILED"
    SERVER_BUSY = "SERVER_BUSY"
    CONTENT_FILTER = "CONTENT_FILTER"
    UNKNOWN = "UNKNOWN"


# ── Request Models ─────────────────────────────────────────────────────

class ModelConfig(BaseModel):
    level: int = Field(..., ge=1, description="最低能力等级 (>=1, 无上限)")
    type: str = Field(default="text", pattern="^(text|image)$")


class TaskSubmitRequest(BaseModel):
    model_config_data: ModelConfig = Field(..., alias="model_config")
    priority: int = Field(default=5, ge=0, description="越大越优先, 0=后台任务")
    payload: dict = Field(..., description="请求内容 (messages 等)")
    callback_url: Optional[str] = Field(default=None, description="完成后回调地址")
    allow_downgrade: bool = Field(default=False, description="是否允许降级调度")
    max_wait_seconds: int = Field(default=600, ge=10, description="最大排队等待秒数")

    class Config:
        populate_by_name = True


# ── Response Models ────────────────────────────────────────────────────

class TaskSubmitResponse(BaseModel):
    task_id: str
    status: TaskStatus
    message: str = "Task submitted successfully"


class TaskStatusResponse(BaseModel):
    task_id: str
    status: TaskStatus
    level: int
    priority: int
    result: Optional[Any] = None
    error_message: Optional[str] = None
    used_provider_id: Optional[str] = None
    created_at: float
    dispatched_at: Optional[float] = None
    completed_at: Optional[float] = None
    execution_time_ms: Optional[int] = None
    retry_count: int = 0


class ProviderInfoResponse(BaseModel):
    provider_id: str
    model_name: str
    level: int
    rpm_limit: int
    max_concurrent: int
    status: ProviderStatus
    next_available_time: Optional[float] = None
    disabled_reason: Optional[str] = None
    current_concurrent: int = 0


class StatsResponse(BaseModel):
    queue_depth: int
    tasks_dispatched: int
    tasks_completed: int
    tasks_failed: int
    providers_active: int
    providers_cooldown: int
    providers_disabled: int
    level_availability: dict[int, int]


class CallbackPayload(BaseModel):
    """Payload sent to client's callback_url."""
    task_id: str
    status: str
    result: Optional[Any] = None
    error_message: Optional[str] = None
    used_provider_id: Optional[str] = None
    execution_time_ms: Optional[int] = None
