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
from app.core.queue import TaskQueue
from app.core.scheduler import Scheduler
from app.db.database import DB_PATH, init_db, get_db
from app.db.models import get_queued_tasks
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


#加载环境变量
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

    # 3. Load providers from YAML
    try:
        pm.load_from_yaml(settings.providers_yaml)
        logger.info("Loaded %d providers", len(pm.get_all_providers()))
    except FileNotFoundError:
        logger.warning("providers.yaml not found at %s — starting with empty provider pool", settings.providers_yaml)
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
