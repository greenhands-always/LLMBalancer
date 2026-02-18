"""Provider pool management: state machine, RPM tracking, concurrency control.

Redesign principles:
- Provider has a single `level` (capability level), not max_level + preferred_level
- Task level = minimum required capability: provider.level >= task.level means eligible
- Prefer lowest sufficient provider first (save powerful models for harder tasks)
- Level is not hardcoded to 1-5; any positive integer works
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional

import yaml

from app.api.schemas import ProviderStatus

logger = logging.getLogger("ulrds.provider_manager")


@dataclass
class ProviderState:
    """Runtime state for a single provider."""
    provider_id: str
    base_url: str
    api_key: str
    model_name: str
    level: int  # capability level (higher = more capable)
    rpm_limit: int
    max_concurrent: int
    timeout_seconds: int

    status: ProviderStatus = ProviderStatus.ACTIVE
    next_available_time: float = 0.0
    disabled_reason: Optional[str] = None

    # Runtime tracking (not persisted)
    current_concurrent: int = 0
    _request_timestamps: deque = field(default_factory=deque)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # Callback: called when a slot is freed so the scheduler can wake up
    _on_release: Optional[Callable] = field(default=None, repr=False)

    def is_available(self) -> bool:
        """Check if provider can accept a request right now."""
        now = time.time()

        if self.status == ProviderStatus.DISABLED:
            return False

        if self.status == ProviderStatus.COOLDOWN:
            if now < self.next_available_time:
                return False
            self.status = ProviderStatus.ACTIVE
            self.next_available_time = 0.0
            logger.info("Provider %s recovered from COOLDOWN", self.provider_id)

        if self.current_concurrent >= self.max_concurrent:
            return False

        if self.rpm_limit > 0:
            self._prune_old_timestamps(now)
            if len(self._request_timestamps) >= self.rpm_limit:
                return False

        return True

    def acquire(self) -> bool:
        """Try to acquire a slot. Returns True if successful."""
        if not self.is_available():
            return False
        self.current_concurrent += 1
        if self.rpm_limit > 0:
            self._request_timestamps.append(time.time())
        return True

    def release(self) -> None:
        """Release a concurrency slot after request completes."""
        self.current_concurrent = max(0, self.current_concurrent - 1)
        if self._on_release:
            self._on_release()

    def set_cooldown(self, seconds: float) -> None:
        """Put provider into COOLDOWN state."""
        self.status = ProviderStatus.COOLDOWN
        self.next_available_time = time.time() + seconds
        logger.warning(
            "Provider %s -> COOLDOWN for %ds (until %.0f)",
            self.provider_id, seconds, self.next_available_time,
        )

    def set_disabled(self, reason: str) -> None:
        """Permanently disable provider."""
        self.status = ProviderStatus.DISABLED
        self.disabled_reason = reason
        logger.error("Provider %s -> DISABLED: %s", self.provider_id, reason)

    def reset(self) -> None:
        """Manually reset provider to ACTIVE."""
        self.status = ProviderStatus.ACTIVE
        self.next_available_time = 0.0
        self.disabled_reason = None
        logger.info("Provider %s manually reset to ACTIVE", self.provider_id)
        if self._on_release:
            self._on_release()

    def _prune_old_timestamps(self, now: float) -> None:
        """Remove timestamps older than 60 seconds for RPM tracking."""
        cutoff = now - 60.0
        while self._request_timestamps and self._request_timestamps[0] < cutoff:
            self._request_timestamps.popleft()

    def to_dict(self) -> dict:
        """Serialize for API response."""
        return {
            "provider_id": self.provider_id,
            "model_name": self.model_name,
            "level": self.level,
            "rpm_limit": self.rpm_limit,
            "max_concurrent": self.max_concurrent,
            "status": self.status.value,
            "next_available_time": self.next_available_time if self.status == ProviderStatus.COOLDOWN else None,
            "disabled_reason": self.disabled_reason,
            "current_concurrent": self.current_concurrent,
        }


class ProviderManager:
    """Manages the pool of LLM providers."""

    def __init__(self, on_provider_available: Optional[Callable] = None) -> None:
        self._providers: dict[str, ProviderState] = {}
        self._on_provider_available = on_provider_available

    def load_from_db_records(self, records: list[dict]) -> None:
        """Load providers from flat DB records (providers JOIN provider_models).

        Each record must include: provider_id, base_url, api_key, model_name,
        level, rpm_limit, max_concurrent, timeout_seconds, status.
        Runtime key = "{provider_id}__{model_name}".
        """
        for p in records:
            raw_status = p.get("status", "ACTIVE")
            try:
                status = ProviderStatus(raw_status)
            except ValueError:
                status = ProviderStatus.ACTIVE
            # Don't restore COOLDOWN on startup — treat as ACTIVE
            if status == ProviderStatus.COOLDOWN:
                status = ProviderStatus.ACTIVE

            runtime_id = f"{p['provider_id']}__{p['model_name']}"
            state = ProviderState(
                provider_id=runtime_id,
                base_url=p["base_url"],
                api_key=p["api_key"],
                model_name=p["model_name"],
                level=int(p["level"]),
                rpm_limit=int(p.get("rpm_limit", 0)),
                max_concurrent=int(p.get("max_concurrent", 1)),
                timeout_seconds=int(p.get("timeout_seconds", 120)),
                status=status,
                _on_release=self._on_provider_available,
            )
            self._providers[runtime_id] = state
            logger.info(
                "Loaded provider from DB: %s (model=%s, level=%d)",
                runtime_id, p["model_name"], p["level"],
            )

    def add_provider(self, provider_id: str, model: dict, base_url: str, api_key: str) -> "ProviderState":
        """Add a provider-model to the runtime pool.

        Args:
            provider_id: Base provider ID (e.g., "openai")
            model: Model config dict with model_name, level, rpm_limit, etc.
            base_url: Connection base URL
            api_key: API key

        Runtime key: "{provider_id}__{model_name}"
        """
        runtime_id = f"{provider_id}__{model['model_name']}"
        state = ProviderState(
            provider_id=runtime_id,
            base_url=base_url,
            api_key=api_key,
            model_name=model["model_name"],
            level=int(model.get("level", 1)),
            rpm_limit=int(model.get("rpm_limit", 0)),
            max_concurrent=int(model.get("max_concurrent", 1)),
            timeout_seconds=int(model.get("timeout_seconds", 120)),
            _on_release=self._on_provider_available,
        )
        self._providers[runtime_id] = state
        logger.info("Added provider to runtime pool: %s", runtime_id)
        return state

    def remove_provider(self, runtime_id: str) -> bool:
        """Remove a provider-model from the runtime pool by runtime key.

        Accepts either a runtime key ("{provider_id}__{model_name}") or
        a bare provider_id to remove all models under that provider.
        """
        if runtime_id in self._providers:
            del self._providers[runtime_id]
            logger.info("Removed provider from runtime pool: %s", runtime_id)
            return True
        # Try removing all models under a base provider_id
        prefix = f"{runtime_id}__"
        removed = [k for k in list(self._providers) if k.startswith(prefix)]
        for k in removed:
            del self._providers[k]
            logger.info("Removed provider from runtime pool: %s", k)
        return bool(removed)

    def update_provider_model(
        self, provider_id: str, model_name: str, updates: dict
    ) -> bool:
        """Update runtime fields of a provider-model. Returns False if not found."""
        runtime_id = f"{provider_id}__{model_name}"
        p = self._providers.get(runtime_id)
        if p is None:
            return False
        for field in ("base_url", "api_key", "level", "rpm_limit",
                      "max_concurrent", "timeout_seconds"):
            if field in updates and updates[field] is not None:
                setattr(p, field, updates[field])
        logger.info("Updated provider in runtime pool: %s", runtime_id)
        return True

    def update_provider_connection(self, provider_id: str, base_url: str, api_key: str) -> None:
        """Update base_url and api_key for all models of a provider."""
        prefix = f"{provider_id}__"
        for k, p in self._providers.items():
            if k.startswith(prefix):
                p.base_url = base_url
                p.api_key = api_key
        logger.info("Updated connection info for all models of: %s", provider_id)

    def load_from_yaml(self, path: str) -> None:
        """Load provider configurations from YAML file.

        Supports two formats:
        - New multi-model: providers[].models[] list
        - Legacy flat:     providers[] with model_name field
        """
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not data:
            logger.warning("providers.yaml is empty or has no data")
            return

        for p in data.get("providers", []) or []:
            if "models" in p:
                # New multi-model format
                for m in p.get("models", []):
                    runtime_id = f"{p['provider_id']}__{m['model_name']}"
                    level = m.get("level", 1)
                    state = ProviderState(
                        provider_id=runtime_id,
                        base_url=p["base_url"],
                        api_key=p["api_key"],
                        model_name=m["model_name"],
                        level=int(level),
                        rpm_limit=int(m.get("rpm_limit", 0)),
                        max_concurrent=int(m.get("max_concurrent", 1)),
                        timeout_seconds=int(m.get("timeout_seconds", 120)),
                        _on_release=self._on_provider_available,
                    )
                    self._providers[runtime_id] = state
                    logger.info(
                        "Loaded provider: %s (model=%s, level=%d)",
                        runtime_id, m["model_name"], level,
                    )
            else:
                # Legacy flat format (model_name at top level)
                level = p.get("level") or p.get("preferred_level") or p.get("max_level", 1)
                model_name = p.get("model_name", "unknown")
                # Keep old provider_id as runtime key for backward compat
                runtime_id = p["provider_id"]
                state = ProviderState(
                    provider_id=runtime_id,
                    base_url=p["base_url"],
                    api_key=p["api_key"],
                    model_name=model_name,
                    level=int(level),
                    rpm_limit=int(p.get("rpm_limit", 0)),
                    max_concurrent=int(p.get("max_concurrent", 1)),
                    timeout_seconds=int(p.get("timeout_seconds", 120)),
                    _on_release=self._on_provider_available,
                )
                self._providers[runtime_id] = state
                logger.info(
                    "Loaded provider (legacy): %s (model=%s, level=%d)",
                    runtime_id, model_name, level,
                )

    def find_provider(
        self,
        level: int,
        allow_downgrade: bool = False,
    ) -> Optional[ProviderState]:
        """Find the best available provider for a given minimum level.

        Matching logic (level = minimum required capability):
        1. Filter providers where provider.level >= requested level AND is_available
        2. Sort by level ASC (prefer cheapest sufficient provider), then concurrent ASC
        3. If allow_downgrade and no match found, try providers below requested level
           (prefer the highest level below the request)
        """
        candidates: list[ProviderState] = []
        downgrade_candidates: list[ProviderState] = []
        unavailable_count = 0

        for p in self._providers.values():
            if not p.is_available():
                unavailable_count += 1
                continue
            if p.level >= level:
                candidates.append(p)
            elif allow_downgrade:
                downgrade_candidates.append(p)

        if candidates:
            selected = min(candidates, key=lambda p: (p.level, p.current_concurrent))
            logger.debug(
                "Selected provider %s (level=%d) for request level=%d",
                selected.provider_id, selected.level, level,
            )
            return selected

        if downgrade_candidates:
            selected = max(
                downgrade_candidates,
                key=lambda p: (p.level, -p.current_concurrent),
            )
            logger.info(
                "Downgrade: selected provider %s (level=%d) for request level=%d",
                selected.provider_id, selected.level, level,
            )
            return selected

        if unavailable_count > 0:
            logger.debug(
                "No provider for level>=%d: %d providers unavailable",
                level, unavailable_count,
            )
        else:
            logger.warning("No provider exists with level>=%d", level)

        return None

    def get_provider(self, provider_id: str) -> Optional[ProviderState]:
        """Get a provider by ID."""
        return self._providers.get(provider_id)

    def get_all_providers(self) -> list[ProviderState]:
        """Get all providers."""
        return list(self._providers.values())

    def reset_provider(self, provider_id: str) -> bool:
        """Manually reset a provider to ACTIVE. Returns False if not found."""
        p = self._providers.get(provider_id)
        if p is None:
            return False
        p.reset()
        return True

    def get_stats(self) -> dict:
        """Get aggregated provider statistics."""
        now = time.time()
        active = sum(
            1 for p in self._providers.values()
            if p.status == ProviderStatus.ACTIVE or
            (p.status == ProviderStatus.COOLDOWN and now >= p.next_available_time)
        )
        cooldown = sum(
            1 for p in self._providers.values()
            if p.status == ProviderStatus.COOLDOWN and now < p.next_available_time
        )
        disabled = sum(
            1 for p in self._providers.values()
            if p.status == ProviderStatus.DISABLED
        )

        # Dynamic level availability based on actual provider levels
        all_levels = sorted({p.level for p in self._providers.values()})
        level_avail: dict[int, int] = {}
        for lv in all_levels:
            count = sum(
                1 for p in self._providers.values()
                if p.level >= lv and p.is_available()
            )
            level_avail[lv] = count

        return {
            "providers_active": active,
            "providers_cooldown": cooldown,
            "providers_disabled": disabled,
            "level_availability": level_avail,
        }
