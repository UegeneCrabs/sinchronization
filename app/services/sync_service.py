from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
import logging

from app.models import SheetSyncResult, SyncRequest, SyncResponse, SyncSummary, TargetConfig
from app.services.header_resolver import find_headers
from app.services.sheets_client import (
    CellStyleUpdate,
    CellValueUpdate,
    SheetWritePayload,
    SheetsClient,
)


@dataclass
class SourceInfo:
    data: list[list]
    header_mapping: dict[str, int]
    header_row_index: int
    barcodes_set: set[str]


class SyncService:
    def __init__(self, sheets_client: SheetsClient) -> None:
        self.sheets_client = sheets_client
        self.logger = logging.getLogger(__name__)

    def run(self, request: SyncRequest) -> SyncResponse:
        started_at = datetime.now(timezone.utc)
        start_ts = time.perf_counter()
        run_id = request.runId or str(uuid.uuid4())
        trace_id = str(uuid.uuid4())

        source_info = self._get_source_data(request)
        results: list[SheetSyncResult] = []
        self.logger.info("sync_started run_id=%s project=%s targets=%s", run_id, request.projectName, len(request.targets))

        for target in request.targets:
            target_start = time.perf_counter()
            try:
                result = self._process_target(request, target, source_info)
                result.durationMs = int((time.perf_counter() - target_start) * 1000)
                results.append(result)
                self.logger.info(
                    "target_processed run_id=%s sheet=%s processed=%s orange=%s missing=%s duplicates=%s duration_ms=%s",
                    run_id,
                    result.sheetName,
                    result.processedRows,
                    result.orangeCells,
                    result.missingCount,
                    result.duplicateCount,
                    result.durationMs,
                )
            except Exception as exc:  # noqa: BLE001
                results.append(
                    SheetSyncResult(
                        sheetName=target.sheetName,
                        status="error",
                        error=str(exc),
                        durationMs=int((time.perf_counter() - target_start) * 1000),
                    )
                )
                self.logger.exception("target_failed run_id=%s sheet=%s", run_id, target.sheetName)

        summary = SyncSummary(
            processedRows=sum(item.processedRows for item in results),
            orangeCells=sum(item.orangeCells for item in results),
            missingCount=sum(item.missingCount for item in results),
            duplicateCount=sum(item.duplicateCount for item in results),
            errors=sum(1 for item in results if item.status == "error"),
        )

        status = "success"
        if summary.errors:
            status = "partial_success" if summary.errors < len(results) else "error"

        finished_at = datetime.now(timezone.utc)
        self.logger.info(
            "sync_finished run_id=%s status=%s processed=%s orange=%s missing=%s duplicates=%s errors=%s duration_ms=%s",
            run_id,
            status,
            summary.processedRows,
            summary.orangeCells,
            summary.missingCount,
            summary.duplicateCount,
            summary.errors,
            int((time.perf_counter() - start_ts) * 1000),
        )
        return SyncResponse(
            status=status,
            runId=run_id,
            traceId=trace_id,
            startedAt=started_at,
            finishedAt=finished_at,
            durationMs=int((time.perf_counter() - start_ts) * 1000),
            summary=summary,
            targets=results,
            meta={"projectName": request.projectName, "platformType": request.platformType},
        )

    def _get_source_data(self, request: SyncRequest) -> SourceInfo:
        data = self.sheets_client.read_sheet(
            spreadsheet_url=str(request.source.spreadsheetUrl),
            sheet_name=request.source.sheetName,
        )
        if not data:
            return SourceInfo(data=[[]], header_mapping={}, header_row_index=1, barcodes_set=set())

        required_headers = list(request.source.headers)
        columns = request.filters.columnNames
        for header in [columns.statusColumn, columns.juridicalColumn]:
            if header not in required_headers:
                required_headers.append(header)

        source_header_mapping, source_header_row_index = find_headers(data, required_headers)
        source_barcode_col = source_header_mapping[columns.barcodeColumn]
        source_status_col = source_header_mapping[columns.statusColumn]
        source_juridical_col = source_header_mapping[columns.juridicalColumn]

        barcodes_set: set[str] = set()
        for row in data[source_header_row_index:]:
            barcode = _cell(row, source_barcode_col).strip()
            status = _cell(row, source_status_col).strip()
            juridical = _cell(row, source_juridical_col).strip()
            if not barcode or juridical != request.filters.juridicalPerson:
                continue
            if status in request.filters.excludeStatuses:
                continue
            barcodes_set.add(barcode)

        return SourceInfo(
            data=data,
            header_mapping=source_header_mapping,
            header_row_index=source_header_row_index,
            barcodes_set=barcodes_set,
        )

    def _process_target(
        self, request: SyncRequest, target: TargetConfig, source_info: SourceInfo
    ) -> SheetSyncResult:
        target_data = self.sheets_client.read_sheet(str(target.spreadsheetUrl), target.sheetName)
        if not target_data:
            raise ValueError(f"Target sheet is empty or unavailable: {target.sheetName}")

        target_header_mapping, target_header_row_index = find_headers(target_data, target.headers)
        barcode_header = request.filters.columnNames.barcodeColumn
        barcode_col_target = target_header_mapping[barcode_header]

        target_barcode_map: dict[str, int] = {}
        for i in range(target_header_row_index, len(target_data)):
            bc = _cell(target_data[i], barcode_col_target).strip()
            if bc:
                target_barcode_map[bc] = i

        working_data = [row[:] for row in target_data]
        orange_cells: set[tuple[int, int]] = set()

        source_barcode_col = source_info.header_mapping[request.filters.columnNames.barcodeColumn]
        source_status_col = source_info.header_mapping[request.filters.columnNames.statusColumn]
        source_juridical_col = source_info.header_mapping[request.filters.columnNames.juridicalColumn]

        for src_row in source_info.data[source_info.header_row_index:]:
            src_bc = _cell(src_row, source_barcode_col).strip()
            src_status = _cell(src_row, source_status_col).strip()
            src_juridical = _cell(src_row, source_juridical_col).strip()
            if not src_bc or src_juridical != request.filters.juridicalPerson:
                continue
            if src_status in request.filters.excludeStatuses:
                continue

            new_values: dict[str, str] = {}
            for target_field, source_field in target.mapping.items():
                if source_field in source_info.header_mapping:
                    source_col = source_info.header_mapping[source_field]
                    new_values[target_field] = _cell(src_row, source_col)

            if src_bc in target_barcode_map:
                row_idx = target_barcode_map[src_bc]
                _ensure_width(working_data[row_idx], _max_col(target_header_mapping) + 1)
                for key, new_value in new_values.items():
                    col_idx = target_header_mapping[key]
                    old_value = _cell(working_data[row_idx], col_idx).strip()
                    if old_value != new_value.strip():
                        working_data[row_idx][col_idx] = new_value
                        orange_cells.add((row_idx, col_idx))
            else:
                row_idx = self._find_or_append_empty_row(
                    working_data, target_header_row_index, barcode_col_target
                )
                _ensure_width(working_data[row_idx], _max_col(target_header_mapping) + 1)
                for key, new_value in new_values.items():
                    col_idx = target_header_mapping[key]
                    working_data[row_idx][col_idx] = new_value
                    orange_cells.add((row_idx, col_idx))
                target_barcode_map[src_bc] = row_idx

        missing_cells = self._find_missing_barcodes(
            data=working_data,
            header_mapping=target_header_mapping,
            barcode_header=barcode_header,
            source_barcodes=source_info.barcodes_set,
            header_row_index=target_header_row_index,
        )
        duplicate_cells = self._find_duplicate_barcodes(
            data=working_data,
            header_mapping=target_header_mapping,
            barcode_header=barcode_header,
            header_row_index=target_header_row_index,
        )

        if not request.options.dryRun:
            payload = SheetWritePayload(
                values=working_data,
                value_updates=[
                    CellValueUpdate(row=row, col=col, value=_cell(working_data[row], col))
                    for row, col in sorted(orange_cells)
                ],
                background_updates=self._build_style_updates(
                    include_coloring=request.options.includeColoring,
                    orange_cells=orange_cells,
                    missing_cells=missing_cells,
                    duplicate_cells=duplicate_cells,
                ),
            )
            self.sheets_client.write_sheet(
                spreadsheet_url=str(target.spreadsheetUrl), sheet_name=target.sheetName, payload=payload
            )

        return SheetSyncResult(
            sheetName=target.sheetName,
            status="success",
            processedRows=max(0, len(working_data) - target_header_row_index),
            orangeCells=len(orange_cells),
            missingCount=len(missing_cells),
            duplicateCount=len(duplicate_cells),
        )

    @staticmethod
    def _find_or_append_empty_row(data: list[list], data_start_index: int, barcode_col_idx: int) -> int:
        for i in range(data_start_index, len(data)):
            if _cell(data[i], barcode_col_idx).strip() == "":
                return i
        data.append([""] * max(1, len(data[0]) if data else barcode_col_idx + 1))
        return len(data) - 1

    @staticmethod
    def _find_missing_barcodes(
        data: list[list],
        header_mapping: dict[str, int],
        barcode_header: str,
        source_barcodes: set[str],
        header_row_index: int,
    ) -> set[tuple[int, int]]:
        barcode_col = header_mapping.get(barcode_header)
        if barcode_col is None:
            return set()
        missing = set()
        for i in range(header_row_index, len(data)):
            value = _cell(data[i], barcode_col).strip()
            if value and value not in source_barcodes:
                missing.add((i, barcode_col))
        return missing

    @staticmethod
    def _find_duplicate_barcodes(
        data: list[list], header_mapping: dict[str, int], barcode_header: str, header_row_index: int
    ) -> set[tuple[int, int]]:
        barcode_col = header_mapping.get(barcode_header)
        if barcode_col is None:
            return set()
        seen: set[str] = set()
        duplicates = set()
        for i in range(header_row_index, len(data)):
            value = _cell(data[i], barcode_col).strip()
            if not value:
                continue
            if value in seen:
                duplicates.add((i, barcode_col))
            else:
                seen.add(value)
        return duplicates

    @staticmethod
    def _build_style_updates(
        include_coloring: bool,
        orange_cells: set[tuple[int, int]],
        missing_cells: set[tuple[int, int]],
        duplicate_cells: set[tuple[int, int]],
    ) -> list[CellStyleUpdate]:
        if not include_coloring:
            return []
        updates = [CellStyleUpdate(row=row, col=col, color="orange") for row, col in sorted(orange_cells)]
        updates.extend(CellStyleUpdate(row=row, col=col, color="red") for row, col in sorted(missing_cells))
        updates.extend(
            CellStyleUpdate(row=row, col=col, color="lightblue") for row, col in sorted(duplicate_cells)
        )
        return updates


def _cell(row: list, index: int) -> str:
    if index < 0 or index >= len(row):
        return ""
    value = row[index]
    return "" if value is None else str(value)


def _max_col(mapping: dict[str, int]) -> int:
    return max(mapping.values()) if mapping else 0


def _ensure_width(row: list, width: int) -> None:
    if len(row) < width:
        row.extend([""] * (width - len(row)))
