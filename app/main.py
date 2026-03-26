from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.models import SyncRequest
from app.services.sheets_client import GoogleApiSheetsClient
from app.services.sync_service import SyncService

logger = logging.getLogger(__name__)


class SyncAcceptedResponse(BaseModel):
    status: Literal["accepted"]
    job_id: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: Literal["queued", "running", "done", "failed"]
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None


@dataclass
class SyncJob:
    job_id: str
    payload: SyncRequest
    status: Literal["queued", "running", "done", "failed"]
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None


async def worker_loop(app: FastAPI) -> None:
    while True:
        job_id = await app.state.job_queue.get()
        job: SyncJob = app.state.jobs[job_id]

        try:
            job.status = "running"
            job.started_at = datetime.now(timezone.utc).isoformat()

            await asyncio.to_thread(app.state.sync_service.run, job.payload)

            job.status = "done"
            job.finished_at = datetime.now(timezone.utc).isoformat()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Background sync failed for job_id=%s", job_id)
            job.status = "failed"
            job.error = str(exc)
            job.finished_at = datetime.now(timezone.utc).isoformat()
        finally:
            app.state.job_queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.sync_service = SyncService(sheets_client=GoogleApiSheetsClient())
    app.state.job_queue: asyncio.Queue[str] = asyncio.Queue()
    app.state.jobs: dict[str, SyncJob] = {}

    app.state.worker_task = asyncio.create_task(worker_loop(app))
    try:
        yield
    finally:
        app.state.worker_task.cancel()
        try:
            await app.state.worker_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Google Sheets Sync API",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/sync/google-sheets", response_model=SyncAcceptedResponse, status_code=202)
async def sync_google_sheets(payload: SyncRequest) -> SyncAcceptedResponse:
    job_id = uuid4().hex

    app.state.jobs[job_id] = SyncJob(
        job_id=job_id,
        payload=payload,
        status="queued",
        created_at=datetime.now(timezone.utc).isoformat(),
    )

    await app.state.job_queue.put(job_id)

    return SyncAcceptedResponse(
        status="accepted",
        job_id=job_id,
    )


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str) -> JobStatusResponse:
    job: SyncJob | None = app.state.jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatusResponse(
        job_id=job.job_id,
        status=job.status,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error=job.error,
    )