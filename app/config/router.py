"""Provider configuration management: REST API + Web UI."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import yaml
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field

from app.api.auth import verify_token
from app.db.database import get_db
from app.db import models as db_models

logger = logging.getLogger("ulrds.config")

config_router = APIRouter(prefix="/config", tags=["config"])

UI_FILE = Path(__file__).parent / "ui" / "index.html"


def _get_pm():
    from main import app_state
    return app_state.get("provider_manager")


# ── Schemas ────────────────────────────────────────────────────────────

class ProviderConnectionBody(BaseModel):
    provider_id: str
    base_url: str
    api_key: str
    notes: str = ""


class ProviderModelBody(BaseModel):
    model_name: str
    level: int = Field(default=1, ge=1)
    rpm_limit: int = Field(default=0, ge=0)
    max_concurrent: int = Field(default=1, ge=1)
    timeout_seconds: int = Field(default=120, ge=5)


class ImportRequest(BaseModel):
    content: str


# ── UI ─────────────────────────────────────────────────────────────────

@config_router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def config_ui():
    return HTMLResponse(content=UI_FILE.read_text(encoding="utf-8"))


# ── Provider connection CRUD ───────────────────────────────────────────

@config_router.get("/providers", dependencies=[Depends(verify_token)])
async def list_providers():
    """List all providers with models and current runtime status."""
    pm = _get_pm()
    async with get_db() as db:
        providers = await db_models.get_all_providers(db)

    for prov in providers:
        for m in prov.get("models", []):
            runtime_id = f"{prov['provider_id']}__{m['model_name']}"
            runtime = pm.get_provider(runtime_id) if pm else None
            m["runtime_status"] = runtime.status.value if runtime else m.get("status", "ACTIVE")
            m["current_concurrent"] = runtime.current_concurrent if runtime else 0
            m["disabled_reason"] = (
                runtime.disabled_reason if runtime else m.get("disabled_reason")
            )
    return providers


@config_router.post("/providers", dependencies=[Depends(verify_token)], status_code=201)
async def create_provider(req: ProviderConnectionBody):
    """Create a new provider connection (no models yet)."""
    async with get_db() as db:
        existing = await db_models.get_provider_by_id(db, req.provider_id)
        if existing:
            raise HTTPException(
                status_code=409, detail=f"Provider '{req.provider_id}' already exists"
            )
        await db_models.upsert_provider_connection(db, req.model_dump())

    logger.info("Created provider connection: %s", req.provider_id)
    return {"provider_id": req.provider_id, "message": "Provider created"}


@config_router.put("/providers/{provider_id}", dependencies=[Depends(verify_token)])
async def update_provider(provider_id: str, req: ProviderConnectionBody):
    """Update provider connection info (base_url, api_key, notes)."""
    pm = _get_pm()
    async with get_db() as db:
        existing = await db_models.get_provider_by_id(db, provider_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Provider '{provider_id}' not found")

        data = req.model_dump()
        if not data.get("api_key"):
            data["api_key"] = existing["api_key"]
        await db_models.upsert_provider_connection(db, data)

    if pm:
        pm.update_provider_connection(provider_id, data["base_url"], data["api_key"])

    logger.info("Updated provider connection: %s", provider_id)
    return {"provider_id": provider_id, "message": "Provider updated"}


@config_router.delete("/providers/{provider_id}", dependencies=[Depends(verify_token)])
async def delete_provider(provider_id: str):
    """Delete provider and all its models."""
    pm = _get_pm()
    async with get_db() as db:
        existing = await db_models.get_provider_by_id(db, provider_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Provider '{provider_id}' not found")
        await db_models.delete_provider(db, provider_id)

    if pm:
        pm.remove_provider(provider_id)  # removes all models under this provider

    logger.info("Deleted provider: %s", provider_id)
    return {"provider_id": provider_id, "message": "Provider deleted"}


# ── Model CRUD ─────────────────────────────────────────────────────────

@config_router.post(
    "/providers/{provider_id}/models",
    dependencies=[Depends(verify_token)],
    status_code=201,
)
async def add_model(provider_id: str, req: ProviderModelBody):
    """Add a model to an existing provider."""
    pm = _get_pm()
    async with get_db() as db:
        prov = await db_models.get_provider_by_id(db, provider_id)
        if not prov:
            raise HTTPException(status_code=404, detail=f"Provider '{provider_id}' not found")
        # Check duplicate
        existing_names = {m["model_name"] for m in prov.get("models", [])}
        if req.model_name in existing_names:
            raise HTTPException(
                status_code=409,
                detail=f"Model '{req.model_name}' already exists under '{provider_id}'",
            )
        await db_models.upsert_provider_model(db, provider_id, req.model_dump())

    if pm:
        pm.add_provider(
            provider_id=provider_id,
            model=req.model_dump(),
            base_url=prov["base_url"],
            api_key=prov["api_key"],
        )
        if pm._on_provider_available:
            pm._on_provider_available()

    logger.info("Added model %s to provider %s", req.model_name, provider_id)
    return {"provider_id": provider_id, "model_name": req.model_name, "message": "Model added"}


@config_router.put(
    "/providers/{provider_id}/models/{model_name}",
    dependencies=[Depends(verify_token)],
)
async def update_model(provider_id: str, model_name: str, req: ProviderModelBody):
    """Update a model's scheduling config."""
    pm = _get_pm()
    async with get_db() as db:
        prov = await db_models.get_provider_by_id(db, provider_id)
        if not prov:
            raise HTTPException(status_code=404, detail=f"Provider '{provider_id}' not found")
        existing_names = {m["model_name"] for m in prov.get("models", [])}
        if model_name not in existing_names:
            raise HTTPException(
                status_code=404,
                detail=f"Model '{model_name}' not found under '{provider_id}'",
            )
        updates = req.model_dump()
        updates["model_name"] = model_name  # keep original name
        await db_models.upsert_provider_model(db, provider_id, updates)

    if pm:
        pm.update_provider_model(provider_id, model_name, req.model_dump())

    logger.info("Updated model %s/%s", provider_id, model_name)
    return {"provider_id": provider_id, "model_name": model_name, "message": "Model updated"}


@config_router.delete(
    "/providers/{provider_id}/models/{model_name}",
    dependencies=[Depends(verify_token)],
)
async def delete_model(provider_id: str, model_name: str):
    """Remove a model from a provider."""
    pm = _get_pm()
    async with get_db() as db:
        prov = await db_models.get_provider_by_id(db, provider_id)
        if not prov:
            raise HTTPException(status_code=404, detail=f"Provider '{provider_id}' not found")
        existing_names = {m["model_name"] for m in prov.get("models", [])}
        if model_name not in existing_names:
            raise HTTPException(
                status_code=404,
                detail=f"Model '{model_name}' not found under '{provider_id}'",
            )
        await db_models.delete_provider_model(db, provider_id, model_name)

    if pm:
        runtime_id = f"{provider_id}__{model_name}"
        pm.remove_provider(runtime_id)

    logger.info("Deleted model %s/%s", provider_id, model_name)
    return {"provider_id": provider_id, "model_name": model_name, "message": "Model deleted"}


# ── Import / Export ────────────────────────────────────────────────────

@config_router.post("/import", dependencies=[Depends(verify_token)])
async def import_providers(req: ImportRequest):
    """Import providers from YAML or JSON.

    Supports:
    - New multi-model format: providers[].models[]
    - Legacy flat format:     providers[] with model_name field
    """
    content = req.content.strip()
    parsed = None
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        try:
            parsed = yaml.safe_load(content)
        except yaml.YAMLError as exc:
            raise HTTPException(status_code=422, detail=f"Invalid YAML/JSON: {exc}")

    raw_list = parsed if isinstance(parsed, list) else parsed.get("providers", []) if isinstance(parsed, dict) else []
    if not raw_list:
        raise HTTPException(status_code=422, detail="No providers found in content")

    pm = _get_pm()
    imported_providers = 0
    imported_models = 0
    errors: list[str] = []

    for raw in raw_list:
        pid = str(raw.get("provider_id", "")).strip()
        if not pid:
            errors.append("Skipped entry: missing provider_id")
            continue
        try:
            # Upsert provider connection
            async with get_db() as db:
                await db_models.upsert_provider_connection(db, {
                    "provider_id": pid,
                    "base_url": str(raw.get("base_url", "")),
                    "api_key": str(raw.get("api_key", "")),
                    "notes": str(raw.get("notes", "")),
                })
            imported_providers += 1

            # Collect models
            if "models" in raw:
                model_entries = raw["models"]
            elif "model_name" in raw:
                # Legacy flat format
                level = raw.get("level") or raw.get("preferred_level") or raw.get("max_level", 1)
                model_entries = [{
                    "model_name": raw["model_name"],
                    "level": level,
                    "rpm_limit": raw.get("rpm_limit", 0),
                    "max_concurrent": raw.get("max_concurrent", 1),
                    "timeout_seconds": raw.get("timeout_seconds", 120),
                }]
            else:
                model_entries = []

            for m in model_entries:
                model_name = str(m.get("model_name", "")).strip()
                if not model_name:
                    errors.append(f"Provider '{pid}': model entry missing model_name")
                    continue
                level = int(m.get("level") or m.get("preferred_level") or 1)
                model_data = {
                    "model_name": model_name,
                    "level": level,
                    "rpm_limit": int(m.get("rpm_limit", 0)),
                    "max_concurrent": int(m.get("max_concurrent", 1)),
                    "timeout_seconds": int(m.get("timeout_seconds", 120)),
                }
                async with get_db() as db:
                    prov = await db_models.get_provider_by_id(db, pid)
                    await db_models.upsert_provider_model(db, pid, model_data)

                if pm and prov:
                    runtime_id = f"{pid}__{model_name}"
                    if pm.get_provider(runtime_id):
                        pm.update_provider_model(pid, model_name, model_data)
                    else:
                        pm.add_provider(pid, model_data, prov["base_url"], prov["api_key"])
                imported_models += 1

        except Exception as exc:
            errors.append(f"Provider '{pid}': {exc}")

    if pm and imported_models > 0 and pm._on_provider_available:
        pm._on_provider_available()

    return {
        "imported_providers": imported_providers,
        "imported_models": imported_models,
        "errors": errors,
        "message": f"Imported {imported_providers} providers, {imported_models} models"
        + (f" ({len(errors)} errors)" if errors else ""),
    }


@config_router.get("/export", dependencies=[Depends(verify_token)])
async def export_providers():
    """Export all providers as YAML (new multi-model format)."""
    async with get_db() as db:
        providers = await db_models.get_all_providers(db)

    export_data = {
        "providers": [
            {
                "provider_id": p["provider_id"],
                "base_url": p["base_url"],
                "api_key": p["api_key"],
                **({"notes": p["notes"]} if p.get("notes") else {}),
                "models": [
                    {
                        "model_name": m["model_name"],
                        "level": m["level"],
                        "rpm_limit": m["rpm_limit"],
                        "max_concurrent": m["max_concurrent"],
                        "timeout_seconds": m["timeout_seconds"],
                    }
                    for m in p.get("models", [])
                ],
            }
            for p in providers
        ]
    }

    yaml_content = yaml.dump(
        export_data, allow_unicode=True, default_flow_style=False, sort_keys=False
    )
    return Response(
        content=yaml_content,
        media_type="text/yaml",
        headers={"Content-Disposition": "attachment; filename=providers.yaml"},
    )
