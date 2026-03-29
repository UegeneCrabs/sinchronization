from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

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
    rows: list[list]
    header_mapping: dict[str, int]
    header_row_index: int
    barcodes_set: set[str]


@dataclass
class StyleBuildResult:
    updates: list[CellStyleUpdate]
    white_cells: int
    orange_cells: int
    red_cells: int
    lightblue_cells: int
    desired_cells_count: int
    current_cells_count: int
    changed_style_cells_count: int
    target_cols_count: int
    desired_prepare_ms: int
    current_read_ms: int
    diff_build_ms: int
    total_ms: int


class SyncService:
    def __init__(self, sheets_client: SheetsClient) -> None:
        self.sheets_client = sheets_client
        self.logger = logging.getLogger("uvicorn.error")

    def run(self, request: SyncRequest) -> SyncResponse:
        started_at = datetime.now(timezone.utc)
        start_ts = time.perf_counter()
        run_id = request.runId or str(uuid.uuid4())
        trace_id = str(uuid.uuid4())

        source_info = self._get_source_data(request)
        results: list[SheetSyncResult] = []

        self.logger.info(
            "sync_started run_id=%s project=%s targets=%s",
            run_id,
            request.projectName,
            len(request.targets),
        )

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
            return SourceInfo(
                data=[[]],
                rows=[],
                header_mapping={},
                header_row_index=1,
                barcodes_set=set(),
            )

        required_headers = list(request.source.headers)
        columns = request.filters.columnNames

        for header in [columns.statusColumn, columns.juridicalColumn]:
            if header not in required_headers:
                required_headers.append(header)

        source_header_mapping, source_header_row_index = find_headers(data, required_headers)

        source_barcode_col = source_header_mapping[columns.barcodeColumn]
        source_status_col = source_header_mapping[columns.statusColumn]
        source_juridical_col = source_header_mapping[columns.juridicalColumn]

        wb_activity_col = 5
        ozon_flag_col = 84

        platform_type = (request.platformType or "").strip().upper()
        project_name = (request.projectName or "").strip().upper()

        is_wb = platform_type == "WB" or "WB" in project_name
        is_ozon = platform_type == "OZON" or "OZON" in project_name or "ОЗОН" in project_name

        supplier_col = source_header_mapping.get("Поставщик")
        article_col = source_header_mapping.get("Артикул")

        barcodes_set: set[str] = set()

        deduped_map: dict[tuple[str, str], list] = {}
        suppliers_map: dict[tuple[str, str], list[str]] = {}

        for row in data[source_header_row_index:]:
            barcode = _cell(row, source_barcode_col).strip()
            status = _cell(row, source_status_col).strip()
            juridical = _cell(row, source_juridical_col).strip()

            if not barcode or juridical != request.filters.juridicalPerson:
                continue

            if status in request.filters.excludeStatuses:
                continue

            if is_wb:
                wb_activity = _cell(row, wb_activity_col).strip()
                if wb_activity == "Старьё":
                    continue

            if is_ozon:
                ozon_flag = _cell(row, ozon_flag_col).strip()
                if ozon_flag == "Нет на ОЗОН":
                    continue

            article = _cell(row, article_col).strip() if article_col is not None else ""
            dedupe_key = (barcode, article)

            if dedupe_key not in deduped_map:
                deduped_map[dedupe_key] = row[:]
                suppliers_map[dedupe_key] = []

            if supplier_col is not None:
                supplier_value = _cell(row, supplier_col).strip()
                if supplier_value and supplier_value not in suppliers_map[dedupe_key]:
                    suppliers_map[dedupe_key].append(supplier_value)

            barcodes_set.add(barcode)

        deduped_rows: list[list] = []

        for dedupe_key, row in deduped_map.items():
            final_row = row[:]

            if supplier_col is not None:
                suppliers = suppliers_map.get(dedupe_key, [])
                if suppliers:
                    _ensure_width(final_row, supplier_col + 1)
                    final_row[supplier_col] = "/".join(suppliers)

            deduped_rows.append(final_row)

        self.logger.info(
            "source_dedupe_profile source_sheet=%s raw_rows=%s deduped_rows=%s unique_barcodes=%s",
            request.source.sheetName,
            max(0, len(data) - source_header_row_index),
            len(deduped_rows),
            len(barcodes_set),
        )

        return SourceInfo(
            data=data,
            rows=deduped_rows,
            header_mapping=source_header_mapping,
            header_row_index=source_header_row_index,
            barcodes_set=barcodes_set,
        )

    def _process_target(
        self,
        request: SyncRequest,
        target: TargetConfig,
        source_info: SourceInfo,
    ) -> SheetSyncResult:
        total_start = time.perf_counter()

        read_start = time.perf_counter()
        target_data = self.sheets_client.read_sheet(str(target.spreadsheetUrl), target.sheetName)
        read_ms = int((time.perf_counter() - read_start) * 1000)

        if not target_data:
            raise ValueError(f"Target sheet is empty or unavailable: {target.sheetName}")

        headers_start = time.perf_counter()
        target_header_mapping, target_header_row_index = find_headers(target_data, target.headers)
        headers_ms = int((time.perf_counter() - headers_start) * 1000)

        barcode_header = request.filters.columnNames.barcodeColumn
        barcode_col_target = target_header_mapping[barcode_header]

        barcode_map_start = time.perf_counter()
        target_barcode_map: dict[str, int] = {}
        for i in range(target_header_row_index, len(target_data)):
            bc = _cell(target_data[i], barcode_col_target).strip()
            if bc:
                target_barcode_map[bc] = i
        barcode_map_ms = int((time.perf_counter() - barcode_map_start) * 1000)

        working_copy_start = time.perf_counter()
        working_data = [row[:] for row in target_data]
        orange_cells: set[tuple[int, int]] = set()
        added_products_count = 0
        working_copy_ms = int((time.perf_counter() - working_copy_start) * 1000)

        source_barcode_col = source_info.header_mapping[request.filters.columnNames.barcodeColumn]
        source_status_col = source_info.header_mapping[request.filters.columnNames.statusColumn]
        source_juridical_col = source_info.header_mapping[request.filters.columnNames.juridicalColumn]

        merge_start = time.perf_counter()
        for src_row in source_info.rows:
            src_bc = _cell(src_row, source_barcode_col).strip()

            if not src_bc:
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
                    working_data,
                    target_header_row_index,
                    barcode_col_target,
                )
                _ensure_width(working_data[row_idx], _max_col(target_header_mapping) + 1)

                for key, new_value in new_values.items():
                    col_idx = target_header_mapping[key]
                    working_data[row_idx][col_idx] = new_value
                    orange_cells.add((row_idx, col_idx))

                target_barcode_map[src_bc] = row_idx
                added_products_count += 1

        merge_ms = int((time.perf_counter() - merge_start))

        missing_start = time.perf_counter()
        missing_cells = self._find_missing_barcodes(
            data=working_data,
            header_mapping=target_header_mapping,
            barcode_header=barcode_header,
            source_barcodes=source_info.barcodes_set,
            header_row_index=target_header_row_index,
        )
        missing_ms = int((time.perf_counter() - missing_start) * 1000)

        duplicates_start = time.perf_counter()
        duplicate_cells = self._find_duplicate_barcodes(
            data=working_data,
            header_mapping=target_header_mapping,
            barcode_header=barcode_header,
            header_row_index=target_header_row_index,
        )
        duplicates_ms = int((time.perf_counter() - duplicates_start) * 1000)

        payload_prepare_start = time.perf_counter()

        value_updates = [
            CellValueUpdate(row=row, col=col, value=_cell(working_data[row], col))
            for row, col in sorted(orange_cells)
        ]

        style_result = self._build_style_updates(
            include_coloring=request.options.includeColoring,
            sheets_client=self.sheets_client,
            spreadsheet_url=str(target.spreadsheetUrl),
            sheet_name=target.sheetName,
            working_data=working_data,
            target_header_mapping=target_header_mapping,
            target_header_row_index=target_header_row_index,
            barcode_col_target=barcode_col_target,
            target_mapping=target.mapping,
            target_color_range=getattr(target, "colorRange", None),
            orange_cells=orange_cells,
            missing_cells=missing_cells,
            duplicate_cells=duplicate_cells,
        )
        background_updates = style_result.updates
        payload_prepare_ms = int((time.perf_counter() - payload_prepare_start) * 1000)

        write_ms = 0
        if not request.options.dryRun:
            write_start = time.perf_counter()
            payload = SheetWritePayload(
                values=working_data,
                value_updates=value_updates,
                background_updates=background_updates,
            )
            self.sheets_client.write_sheet(
                spreadsheet_url=str(target.spreadsheetUrl),
                sheet_name=target.sheetName,
                payload=payload,
            )
            write_ms = int((time.perf_counter() - write_start) * 1000)

        total_ms = int((time.perf_counter() - total_start) * 1000)

        self.logger.info(
            (
                "sheet_profile sheet=%s "
                "source_rows=%s target_rows=%s "
                "read_ms=%s headers_ms=%s barcode_map_ms=%s working_copy_ms=%s "
                "merge_ms=%s missing_ms=%s duplicates_ms=%s payload_prepare_ms=%s write_ms=%s total_ms=%s "
                "style_desired_prepare_ms=%s style_current_read_ms=%s style_diff_build_ms=%s style_total_ms=%s "
                "target_cols=%s "
                "added_products=%s red_marked=%s duplicates=%s "
                "orange_cells=%s white_cells=%s red_cells=%s lightblue_cells=%s "
                "desired_cells=%s current_cells=%s changed_style_cells=%s "
                "value_updates=%s background_updates=%s"
            ),
            target.sheetName,
            max(0, len(source_info.data) - source_info.header_row_index),
            max(0, len(target_data) - target_header_row_index),
            read_ms,
            headers_ms,
            barcode_map_ms,
            working_copy_ms,
            merge_ms,
            missing_ms,
            duplicates_ms,
            payload_prepare_ms,
            write_ms,
            total_ms,
            style_result.desired_prepare_ms,
            style_result.current_read_ms,
            style_result.diff_build_ms,
            style_result.total_ms,
            style_result.target_cols_count,
            added_products_count,
            len(missing_cells),
            len(duplicate_cells),
            len(orange_cells),
            style_result.white_cells,
            style_result.red_cells,
            style_result.lightblue_cells,
            style_result.desired_cells_count,
            style_result.current_cells_count,
            style_result.changed_style_cells_count,
            len(value_updates),
            len(background_updates),
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

        missing: set[tuple[int, int]] = set()
        for i in range(header_row_index, len(data)):
            value = _cell(data[i], barcode_col).strip()
            if value and value not in source_barcodes:
                missing.add((i, barcode_col))
        return missing

    @staticmethod
    def _find_duplicate_barcodes(
        data: list[list],
        header_mapping: dict[str, int],
        barcode_header: str,
        header_row_index: int,
    ) -> set[tuple[int, int]]:
        barcode_col = header_mapping.get(barcode_header)
        if barcode_col is None:
            return set()

        rows_by_barcode: dict[str, list[int]] = {}
        for i in range(header_row_index, len(data)):
            value = _cell(data[i], barcode_col).strip()
            if not value:
                continue
            rows_by_barcode.setdefault(value, []).append(i)

        duplicates: set[tuple[int, int]] = set()
        for rows in rows_by_barcode.values():
            if len(rows) > 1:
                for row_idx in rows:
                    duplicates.add((row_idx, barcode_col))

        return duplicates

    @staticmethod
    def _build_style_updates(
        include_coloring: bool,
        sheets_client: SheetsClient,
        spreadsheet_url: str,
        sheet_name: str,
        working_data: list[list],
        target_header_mapping: dict[str, int],
        target_header_row_index: int,
        barcode_col_target: int,
        target_mapping: dict[str, str],
        target_color_range: str | None,
        orange_cells: set[tuple[int, int]],
        missing_cells: set[tuple[int, int]],
        duplicate_cells: set[tuple[int, int]],
    ) -> StyleBuildResult:
        if not include_coloring:
            return StyleBuildResult(
                updates=[],
                white_cells=0,
                orange_cells=0,
                red_cells=0,
                lightblue_cells=0,
                desired_cells_count=0,
                current_cells_count=0,
                changed_style_cells_count=0,
                target_cols_count=0,
                desired_prepare_ms=0,
                current_read_ms=0,
                diff_build_ms=0,
                total_ms=0,
            )

        total_start = time.perf_counter()
        desired_prepare_start = time.perf_counter()

        target_cols: list[int] = []
        for target_field in target_mapping.keys():
            col_idx = target_header_mapping.get(target_field)
            if col_idx is not None:
                target_cols.append(col_idx)
        target_cols = sorted(set(target_cols))

        if target_color_range:
            range_start_col, range_end_col = _parse_a1_column_range(target_color_range)
        else:
            range_start_col = min(target_cols) if target_cols else barcode_col_target
            range_end_col = max(target_cols) if target_cols else barcode_col_target

        occupied_barcode_rows: set[int] = set()
        for row_idx in range(target_header_row_index, len(working_data)):
            if _cell(working_data[row_idx], barcode_col_target).strip():
                occupied_barcode_rows.add(row_idx)

        duplicate_rows = {row_idx for row_idx, _ in duplicate_cells}
        missing_rows = {row_idx for row_idx, _ in missing_cells}

        desired_non_white_map: dict[tuple[int, int], str] = {}

        for row_idx, col_idx in orange_cells:
            if row_idx not in occupied_barcode_rows:
                continue
            if range_start_col <= col_idx <= range_end_col:
                desired_non_white_map[(row_idx, col_idx)] = "orange"

        for row_idx in occupied_barcode_rows:
            barcode_cell = (row_idx, barcode_col_target)

            if row_idx in duplicate_rows:
                desired_non_white_map[barcode_cell] = "lightblue"
            elif row_idx in missing_rows:
                desired_non_white_map[barcode_cell] = "red"

        desired_prepare_ms = int((time.perf_counter() - desired_prepare_start) * 1000)

        current_read_start = time.perf_counter()

        current_range_map = sheets_client.read_background_colors_in_range(
            spreadsheet_url=spreadsheet_url,
            sheet_name=sheet_name,
            row_indexes=occupied_barcode_rows,
            start_col=range_start_col,
            end_col=range_end_col,
        )

        barcode_read_cells_count = 0
        if range_start_col <= barcode_col_target <= range_end_col:
            current_barcode_map = {
                (row_idx, barcode_col_target): current_range_map.get((row_idx, barcode_col_target), "white")
                for row_idx in occupied_barcode_rows
            }
        else:
            current_barcode_map = sheets_client.read_background_colors_in_range(
                spreadsheet_url=spreadsheet_url,
                sheet_name=sheet_name,
                row_indexes=occupied_barcode_rows,
                start_col=barcode_col_target,
                end_col=barcode_col_target,
            )
            barcode_read_cells_count = len(occupied_barcode_rows)

        current_read_ms = int((time.perf_counter() - current_read_start) * 1000)

        diff_build_start = time.perf_counter()

        managed_colors = {"orange", "red", "lightblue"}
        current_managed_map: dict[tuple[int, int], str] = {}

        for cell, color in current_range_map.items():
            if color in managed_colors:
                current_managed_map[cell] = color

        for cell, color in current_barcode_map.items():
            if color in managed_colors:
                current_managed_map[cell] = color

        candidate_cells = set(current_managed_map) | set(desired_non_white_map)

        updates: list[CellStyleUpdate] = []
        white_cells = 0
        orange_count = 0
        red_count = 0
        lightblue_count = 0

        for row_idx, col_idx in sorted(candidate_cells):
            current = current_managed_map.get((row_idx, col_idx), "white")
            desired = desired_non_white_map.get((row_idx, col_idx), "white")

            if current == desired:
                continue

            updates.append(CellStyleUpdate(row=row_idx, col=col_idx, color=desired))

            if desired == "white":
                white_cells += 1
            elif desired == "orange":
                orange_count += 1
            elif desired == "red":
                red_count += 1
            elif desired == "lightblue":
                lightblue_count += 1

        diff_build_ms = int((time.perf_counter() - diff_build_start) * 1000)
        total_ms = int((time.perf_counter() - total_start) * 1000)

        return StyleBuildResult(
            updates=updates,
            white_cells=white_cells,
            orange_cells=orange_count,
            red_cells=red_count,
            lightblue_cells=lightblue_count,
            desired_cells_count=len(desired_non_white_map),
            current_cells_count=len(current_range_map) + barcode_read_cells_count,
            changed_style_cells_count=len(updates),
            target_cols_count=len(target_cols),
            desired_prepare_ms=desired_prepare_ms,
            current_read_ms=current_read_ms,
            diff_build_ms=diff_build_ms,
            total_ms=total_ms,
        )


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


def _parse_a1_column_range(value: str) -> tuple[int, int]:
    raw = (value or "").strip().upper()
    if ":" not in raw:
        col = _a1_col_to_index(raw)
        return col, col

    start_part, end_part = raw.split(":", 1)
    start_col = _a1_col_to_index(start_part.strip())
    end_col = _a1_col_to_index(end_part.strip())

    if end_col < start_col:
        raise ValueError(f"Invalid colorRange: {value}")

    return start_col, end_col


def _a1_col_to_index(col: str) -> int:
    if not col or not col.isalpha():
        raise ValueError(f"Invalid A1 column: {col}")

    result = 0
    for ch in col:
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result - 1