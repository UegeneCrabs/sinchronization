from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI, HTTPException

from app.models import SyncRequest, SyncResponse
from app.services.sheets_client import GoogleApiSheetsClient
from app.services.sync_service import SyncService

app = FastAPI(title="Google Sheets Sync API", version="1.0.0")
logger = logging.getLogger(__name__)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/sync/google-sheets", response_model=SyncResponse)
async def sync_google_sheets(payload: SyncRequest) -> SyncResponse:
    try:
        service = getattr(app.state, "sync_service", None)
        if service is None:
            service = SyncService(sheets_client=GoogleApiSheetsClient())
            app.state.sync_service = service

        return await asyncio.to_thread(service.run, payload)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Sync failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc