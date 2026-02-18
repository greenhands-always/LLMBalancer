"""ULRDS — Unified LLM Resource Dispatcher & Scheduler.

Entry point: initializes database, loads providers, starts scheduler,
and serves the FastAPI application.
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager

from dotenv import load_dotenv

# 先加载 .env，避免其他模块提前导入 config.settings
load_dotenv()

import uvicorn
from fastapi import FastAPI

from app.api.routes import router
from app.config.router import config_router
from app.core.queue import TaskQueue
from app.core.scheduler import Scheduler
from app.db.database import DB_PATH, init_db, get_db
from app.db.models import (
    get_queued_tasks,
    get_all_provider_models_flat,
    upsert_provider_connection,
    upsert_provider_model,
)
from app.services.provider_manager import ProviderManager
from config import settings

# ── Logging ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("ulrds")

# ── Shared application state ──────────────────────────────────────────

app_state: dict = {}


def _parse_providers_yaml(path: str) -> list[dict]:
    """Parse providers.yaml into structured list for DB insertion.

    Handles two formats:
    - New:    providers[].models[]
    - Legacy: providers[] with model_name at top level (grouped by connection)
    """
    import yaml

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    raw_list = (data or {}).get("providers", []) or []
    result: list[dict] = []
    # For legacy format: group by (base_url, api_key) → provider_id
    seen: dict[tuple, int] = {}  # (base_url, api_key) → index in result

    for p in raw_list:
        if "models" in p:
            # New multi-model format
            models = [
                {
                    "model_name": str(m.get("model_name", "unknown")),
                    "level": int(m.get("level") or m.get("preferred_level") or 1),
                    "rpm_limit": int(m.get("rpm_limit", 0)),
                    "max_concurrent": int(m.get("max_concurrent", 1)),
                    "timeout_seconds": int(m.get("timeout_seconds", 120)),
                }
                for m in p.get("models", [])
                if m.get("model_name")
            ]
            result.append({
                "provider_id": str(p["provider_id"]),
                "base_url": str(p.get("base_url", "")),
                "api_key": str(p.get("api_key", "")),
                "notes": str(p.get("notes", "")),
                "models": models,
            })
        else:
            # Legacy flat format — group by connection
            model_name = str(p.get("model_name") or "unknown")
            level = int(p.get("level") or p.get("preferred_level") or p.get("max_level", 1))
            group_key = (str(p.get("base_url", "")), str(p.get("api_key", "")))

            if group_key not in seen:
                old_pid = str(p["provider_id"])
                if model_name != "unknown" and old_pid.endswith(f"__{model_name}"):
                    base_pid = old_pid[: -(len(model_name) + 2)]
                elif "__" in old_pid:
                    base_pid = old_pid.rsplit("__", 1)[0]
                else:
                    base_pid = old_pid
                seen[group_key] = len(result)
                result.append({
                    "provider_id": base_pid,
                    "base_url": group_key[0],
                    "api_key": group_key[1],
                    "notes": "",
                    "models": [],
                })

            result[seen[group_key]]["models"].append({
                "model_name": model_name,
                "level": level,
                "rpm_limit": int(p.get("rpm_limit", 0)),
                "max_concurrent": int(p.get("max_concurrent", 1)),
                "timeout_seconds": int(p.get("timeout_seconds", 120)),
            })

    return result


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown logic."""
    # ── Startup ────────────────────────────────────────────────────────
    logger.info("Starting ULRDS...")

    # 1. Initialize database
    import app.db.database as db_module
    db_module.DB_PATH = settings.db_path
    await init_db(settings.db_path)
    logger.info("Database initialized: %s", settings.db_path)

    # 2. Create scheduler first (for the wake event)
    queue = TaskQueue()
    pm = ProviderManager()
    scheduler = Scheduler(queue=queue, provider_manager=pm)

    # Wire up notifications: provider release + queue push → wake scheduler
    pm._on_provider_available = scheduler.notify
    queue.set_on_push(scheduler.notify)

    # 3. Load providers — DB is the primary source.
    #    On first run (empty DB), parse providers.yaml and migrate it to DB.
    async with get_db(settings.db_path) as db:
        flat_models = await get_all_provider_models_flat(db)

    if flat_models:
        pm.load_from_db_records(flat_models)
        logger.info("Loaded %d provider-model entries from database", len(flat_models))
    else:
        try:
            structured = _parse_providers_yaml(settings.providers_yaml)
            async with get_db(settings.db_path) as db:
                for prov in structured:
                    await upsert_provider_connection(db, prov)
                    for m in prov.get("models", []):
                        await upsert_provider_model(db, prov["provider_id"], m)
            # Reload from DB so ProviderManager gets the canonical runtime keys
            async with get_db(settings.db_path) as db:
                flat_models = await get_all_provider_models_flat(db)
            pm.load_from_db_records(flat_models)
            total_models = sum(len(p.get("models", [])) for p in structured)
            logger.info(
                "Migrated %d providers (%d models) from %s to database (first run)",
                len(structured), total_models, settings.providers_yaml,
            )
        except FileNotFoundError:
            logger.info("No providers.yaml found — visit /config to configure providers")
        except Exception:
            logger.exception("Failed to load providers.yaml")

    # 4. Reload persisted QUEUED tasks
    async with get_db(settings.db_path) as db:
        queued = await get_queued_tasks(db)
    if queued:
        await queue.load_from_db(queued)

    # 5. Start scheduler
    await scheduler.start()

    # 6. Store in app state for access by routes
    app_state["queue"] = queue
    app_state["scheduler"] = scheduler
    app_state["provider_manager"] = pm

    logger.info("ULRDS is ready — listening on %s:%d", settings.host, settings.port)

    yield

    # ── Shutdown ───────────────────────────────────────────────────────
    logger.info("Shutting down ULRDS...")
    await scheduler.stop()
    logger.info("ULRDS stopped")


# ── FastAPI app ────────────────────────────────────────────────────────

app = FastAPI(
    title="ULRDS — Unified LLM Resource Dispatcher",
    description="异构大模型统一调度与负载均衡系统",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)
app.include_router(config_router)


@app.get("/health")
async def health():
    """Health check endpoint (no auth required)."""
    return {"status": "ok", "service": "ulrds"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )
