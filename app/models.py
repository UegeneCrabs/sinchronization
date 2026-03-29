from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl, field_validator


class SourceConfig(BaseModel):
    spreadsheetUrl: HttpUrl
    sheetName: str
    headers: list[str] = Field(default_factory=list)


class TargetConfig(BaseModel):
    sheetName: str
    spreadsheetUrl: HttpUrl
    headers: list[str] = Field(default_factory=list)
    mapping: dict[str, str] = Field(default_factory=dict)
    colorRange: str | None = None

    @field_validator("colorRange")
    @classmethod
    def validate_color_range(cls, value: str | None) -> str | None:
        if value is None:
            return value

        raw = value.strip().upper()
        if not raw:
            return None

        parts = raw.split(":")
        if len(parts) > 2:
            raise ValueError("colorRange must be a single column or column range, for example 'C' or 'C:G'")

        for part in parts:
            part = part.strip()
            if not part or not part.isalpha():
                raise ValueError("colorRange must contain only A1 column letters, for example 'C:G'")

        return raw


class ColumnNames(BaseModel):
    barcodeColumn: str = "BARCODE"
    statusColumn: str = "STATUS"
    juridicalColumn: str = "Юр лицо"


class FiltersConfig(BaseModel):
    juridicalPerson: str
    excludeStatuses: list[str] = Field(default_factory=list)
    columnNames: ColumnNames = Field(default_factory=ColumnNames)


class SyncOptions(BaseModel):
    dryRun: bool = False
    includeColoring: bool = True
    loggingLevel: Literal["debug", "info", "warning", "error"] = "info"


class SyncRequest(BaseModel):
    apiVersion: str = "v1"
    runId: str | None = None
    projectName: str
    platformType: str = "OZON"
    source: SourceConfig
    targets: list[TargetConfig] = Field(default_factory=list)
    filters: FiltersConfig
    options: SyncOptions = Field(default_factory=SyncOptions)


class SheetSyncResult(BaseModel):
    sheetName: str
    status: Literal["success", "error"]
    processedRows: int = 0
    orangeCells: int = 0
    missingCount: int = 0
    duplicateCount: int = 0
    durationMs: int = 0
    error: str | None = None


class SyncSummary(BaseModel):
    processedRows: int = 0
    orangeCells: int = 0
    missingCount: int = 0
    duplicateCount: int = 0
    errors: int = 0


class SyncResponse(BaseModel):
    status: Literal["success", "partial_success", "error"]
    runId: str
    traceId: str
    startedAt: datetime
    finishedAt: datetime
    durationMs: int
    summary: SyncSummary
    targets: list[SheetSyncResult] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)