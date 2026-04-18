from __future__ import annotations

import os
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

        debug_barcode = os.getenv("DEBUG_BARCODE", "").strip()

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

        for row_idx, row in enumerate(data[source_header_row_index:], start=source_header_row_index):
            raw_barcode = _cell(row, source_barcode_col)
            barcode = _normalized_identifier(raw_barcode)
            status = _normalized_plain(_cell(row, source_status_col))
            juridical = _normalized_plain(_cell(row, source_juridical_col))

            raw_article = _cell(row, article_col) if article_col is not None else ""
            article = _normalized_identifier(raw_article) if article_col is not None else ""

            if debug_barcode and (barcode == debug_barcode or raw_barcode.strip().lstrip("'") == debug_barcode):
                self.logger.info(
                    "debug_source_row sheet=%s row=%s raw_barcode=%r normalized_barcode=%r raw_article=%r normalized_article=%r status=%r juridical=%r",
                    request.source.sheetName,
                    row_idx,
                    raw_barcode,
                    barcode,
                    raw_article,
                    article,
                    status,
                    juridical,
                )

            if not barcode or juridical != request.filters.juridicalPerson:
                continue

            if status in request.filters.excludeStatuses:
                continue

            if is_wb:
                wb_activity = _normalized_plain(_cell(row, wb_activity_col))
                if debug_barcode and barcode == debug_barcode:
                    self.logger.info(
                        "debug_source_wb_filter row=%s wb_activity=%r",
                        row_idx,
                        wb_activity,
                    )
                if wb_activity == "Старьё":
                    continue

            if is_ozon:
                ozon_flag = _normalized_plain(_cell(row, ozon_flag_col))
                if debug_barcode and barcode == debug_barcode:
                    self.logger.info(
                        "debug_source_ozon_filter row=%s ozon_flag=%r",
                        row_idx,
                        ozon_flag,
                    )
                if ozon_flag == "Нет на ОЗОН":
                    continue

            dedupe_key = (barcode, article)

            if debug_barcode and barcode == debug_barcode:
                self.logger.info(
                    "debug_source_passed_filters row=%s dedupe_key=%r supplier_raw=%r",
                    row_idx,
                    dedupe_key,
                    _cell(row, supplier_col) if supplier_col is not None else "",
                )

            if dedupe_key not in deduped_map:
                clean_row = row[:]

                _ensure_width(clean_row, source_barcode_col + 1)
                clean_row[source_barcode_col] = barcode

                if article_col is not None:
                    _ensure_width(clean_row, article_col + 1)
                    clean_row[article_col] = article

                deduped_map[dedupe_key] = clean_row
                suppliers_map[dedupe_key] = []

                if debug_barcode and barcode == debug_barcode:
                    self.logger.info(
                        "debug_source_dedup_create row=%s stored_barcode=%r stored_article=%r",
                        row_idx,
                        clean_row[source_barcode_col],
                        clean_row[article_col] if article_col is not None else "",
                    )

            if supplier_col is not None:
                supplier_value = _normalized_plain(_cell(row, supplier_col))
                if supplier_value and supplier_value not in suppliers_map[dedupe_key]:
                    suppliers_map[dedupe_key].append(supplier_value)

                    if debug_barcode and barcode == debug_barcode:
                        self.logger.info(
                            "debug_source_supplier_append row=%s supplier=%r suppliers_now=%r",
                            row_idx,
                            supplier_value,
                            suppliers_map[dedupe_key],
                        )

            barcodes_set.add(barcode)

        deduped_rows: list[list] = []

        for dedupe_key, row in deduped_map.items():
            final_row = row[:]
            barcode, article = dedupe_key

            if supplier_col is not None:
                suppliers = suppliers_map.get(dedupe_key, [])
                if suppliers:
                    _ensure_width(final_row, supplier_col + 1)
                    final_row[supplier_col] = "/".join(suppliers)

            deduped_rows.append(final_row)

            if debug_barcode and barcode == debug_barcode:
                self.logger.info(
                    "debug_source_final_dedup barcode=%r article=%r final_barcode_cell=%r final_article_cell=%r final_supplier=%r",
                    barcode,
                    article,
                    final_row[source_barcode_col],
                    final_row[article_col] if article_col is not None else "",
                    final_row[supplier_col] if supplier_col is not None and supplier_col < len(final_row) else "",
                )

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
        debug_barcode = os.getenv("DEBUG_BARCODE", "").strip()

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

        source_barcode_col = source_info.header_mapping[request.filters.columnNames.barcodeColumn]

        source_article_col = None
        for article_header in ("ARTICLE", "Артикул", "Артикул продавца"):
            if article_header in source_info.header_mapping:
                source_article_col = source_info.header_mapping[article_header]
                break

        target_article_col = None
        for article_header in ("ARTICLE", "Артикул", "Артикул продавца"):
            if article_header in target_header_mapping:
                target_article_col = target_header_mapping[article_header]
                break

        if target_article_col is None:
            for target_field, source_field in target.mapping.items():
                if source_field in {"ARTICLE", "Артикул", "Артикул продавца"} and target_field in target_header_mapping:
                    target_article_col = target_header_mapping[target_field]
                    break

        identifier_target_cols: set[int] = {barcode_col_target}
        numeric_identifier_target_cols: set[int] = {barcode_col_target}

        if target_article_col is not None:
            identifier_target_cols.add(target_article_col)
            numeric_identifier_target_cols.add(target_article_col)

        barcode_map_start = time.perf_counter()
        target_barcode_map: dict[str, int] = {}
        for i in range(target_header_row_index, len(target_data)):
            raw_bc = _cell(target_data[i], barcode_col_target)
            bc = _normalized_identifier(raw_bc)
            if bc:
                target_barcode_map[bc] = i

            if debug_barcode and (bc == debug_barcode or raw_bc.strip().lstrip("'") == debug_barcode):
                self.logger.info(
                    "debug_target_existing_row sheet=%s row=%s raw_barcode=%r normalized_barcode=%r raw_article=%r normalized_article=%r target_article_col=%r",
                    target.sheetName,
                    i,
                    raw_bc,
                    bc,
                    _cell(target_data[i], target_article_col) if target_article_col is not None else "",
                    _normalized_identifier(
                        _cell(target_data[i], target_article_col)) if target_article_col is not None else "",
                    target_article_col,
                )
        barcode_map_ms = int((time.perf_counter() - barcode_map_start) * 1000)

        working_copy_start = time.perf_counter()
        working_data = [row[:] for row in target_data]
        orange_cells: set[tuple[int, int]] = set()
        force_numeric_identifier_cells: set[tuple[int, int]] = set()
        added_products_count = 0
        working_copy_ms = int((time.perf_counter() - working_copy_start) * 1000)

        merge_start = time.perf_counter()
        for src_row in source_info.rows:
            src_bc = _normalized_identifier(_cell(src_row, source_barcode_col))
            if not src_bc:
                continue

            src_article = _normalized_identifier(
                _cell(src_row, source_article_col)) if source_article_col is not None else ""

            if debug_barcode and src_bc == debug_barcode:
                self.logger.info(
                    "debug_merge_source_hit sheet=%s src_barcode=%r src_article=%r target_row_exists=%s target_row_idx=%r source_article_col=%r target_article_col=%r",
                    target.sheetName,
                    src_bc,
                    src_article,
                    src_bc in target_barcode_map,
                    target_barcode_map.get(src_bc),
                    source_article_col,
                    target_article_col,
                )

            new_values_by_col: dict[int, str] = {}

            for target_field, source_field in target.mapping.items():
                if source_field not in source_info.header_mapping:
                    continue
                if target_field not in target_header_mapping:
                    continue

                source_col = source_info.header_mapping[source_field]
                target_col = target_header_mapping[target_field]
                raw_value = _cell(src_row, source_col)

                if source_col == source_barcode_col or (
                        source_article_col is not None and source_col == source_article_col
                ):
                    value = _normalized_identifier(raw_value)
                else:
                    value = _normalized_plain(raw_value)

                new_values_by_col[target_col] = value

                if debug_barcode and src_bc == debug_barcode:
                    self.logger.info(
                        "debug_merge_mapping sheet=%s src_barcode=%r target_field=%r source_field=%r raw_value=%r normalized_value=%r target_col=%s",
                        target.sheetName,
                        src_bc,
                        target_field,
                        source_field,
                        raw_value,
                        value,
                        target_col,
                    )

            new_values_by_col[barcode_col_target] = src_bc

            if source_article_col is not None and target_article_col is not None:
                new_values_by_col[target_article_col] = src_article

            if debug_barcode and src_bc == debug_barcode:
                self.logger.info(
                    "debug_merge_new_values sheet=%s src_barcode=%r barcode_target_col=%s article_target_col=%r new_values_by_col=%r",
                    target.sheetName,
                    src_bc,
                    barcode_col_target,
                    target_article_col,
                    new_values_by_col,
                )

            if src_bc in target_barcode_map:
                row_idx = target_barcode_map[src_bc]
                _ensure_width(working_data[row_idx], _max_col(target_header_mapping) + 1)

                if debug_barcode and src_bc == debug_barcode:
                    self.logger.info(
                        "debug_merge_existing_before sheet=%s row=%s barcode_raw=%r barcode_norm=%r article_raw=%r article_norm=%r",
                        target.sheetName,
                        row_idx,
                        _cell(working_data[row_idx], barcode_col_target),
                        _normalized_identifier(_cell(working_data[row_idx], barcode_col_target)),
                        _cell(working_data[row_idx], target_article_col) if target_article_col is not None else "",
                        _normalized_identifier(
                            _cell(working_data[row_idx], target_article_col)) if target_article_col is not None else "",
                    )

                for col_idx, new_value in new_values_by_col.items():
                    old_raw = _cell(working_data[row_idx], col_idx)

                    if col_idx in identifier_target_cols:
                        old_value_normalized = _normalized_identifier(old_raw)
                        should_update = old_raw != new_value or old_value_normalized != new_value

                        if debug_barcode and src_bc == debug_barcode:
                            self.logger.info(
                                "debug_merge_compare_identifier sheet=%s row=%s col=%s old_raw=%r old_normalized=%r new_value=%r should_update=%s",
                                target.sheetName,
                                row_idx,
                                col_idx,
                                old_raw,
                                old_value_normalized,
                                new_value,
                                should_update,
                            )

                        if should_update:
                            working_data[row_idx][col_idx] = new_value
                            orange_cells.add((row_idx, col_idx))

                        if col_idx in numeric_identifier_target_cols:
                            force_numeric_identifier_cells.add((row_idx, col_idx))

                            if debug_barcode and src_bc == debug_barcode:
                                self.logger.info(
                                    "debug_force_numeric_identifier sheet=%s row=%s col=%s old_raw=%r new_value=%r",
                                    target.sheetName,
                                    row_idx,
                                    col_idx,
                                    old_raw,
                                    new_value,
                                )
                    else:
                        old_value = _normalized_plain(old_raw)
                        should_update = old_value != new_value

                        if debug_barcode and src_bc == debug_barcode:
                            self.logger.info(
                                "debug_merge_compare_plain sheet=%s row=%s col=%s old_raw=%r old_normalized=%r new_value=%r should_update=%s",
                                target.sheetName,
                                row_idx,
                                col_idx,
                                old_raw,
                                old_value,
                                new_value,
                                should_update,
                            )

                        if should_update:
                            working_data[row_idx][col_idx] = new_value
                            orange_cells.add((row_idx, col_idx))
            else:
                row_idx = self._find_or_append_empty_row(
                    working_data,
                    target_header_row_index,
                    barcode_col_target,
                )
                _ensure_width(working_data[row_idx], _max_col(target_header_mapping) + 1)

                if debug_barcode and src_bc == debug_barcode:
                    self.logger.info(
                        "debug_merge_append_row sheet=%s row=%s before_barcode=%r before_article=%r",
                        target.sheetName,
                        row_idx,
                        _cell(working_data[row_idx], barcode_col_target),
                        _cell(working_data[row_idx], target_article_col) if target_article_col is not None else "",
                    )

                for col_idx, new_value in new_values_by_col.items():
                    working_data[row_idx][col_idx] = new_value
                    orange_cells.add((row_idx, col_idx))

                    if col_idx in numeric_identifier_target_cols:
                        force_numeric_identifier_cells.add((row_idx, col_idx))

                    if debug_barcode and src_bc == debug_barcode:
                        self.logger.info(
                            "debug_merge_append_value sheet=%s row=%s col=%s stored_value=%r",
                            target.sheetName,
                            row_idx,
                            col_idx,
                            working_data[row_idx][col_idx],
                        )

                target_barcode_map[src_bc] = row_idx
                added_products_count += 1

        merge_ms = int((time.perf_counter() - merge_start) * 1000)

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

        cells_to_write = sorted(orange_cells | force_numeric_identifier_cells)

        value_updates = []
        for row, col in cells_to_write:
            raw_value = _cell(working_data[row], col)

            if col in numeric_identifier_target_cols:
                final_value = _to_sheet_number_if_possible(raw_value)
            else:
                final_value = raw_value

            value_updates.append(CellValueUpdate(row=row, col=col, value=final_value))

            if debug_barcode and col in numeric_identifier_target_cols:
                normalized = _normalized_identifier(raw_value)
                if normalized == debug_barcode or (
                        target_article_col is not None and col == target_article_col
                ):
                    self.logger.info(
                        "debug_value_update_pre_write sheet=%s row=%s col=%s raw_value=%r final_value=%r final_type=%s normalized=%r",
                        target.sheetName,
                        row,
                        col,
                        raw_value,
                        final_value,
                        type(final_value).__name__,
                        normalized,
                    )

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
            value = _normalized_identifier(_cell(data[i], barcode_col))
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
            value = _normalized_identifier(_cell(data[i], barcode_col))
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


def _cell(row: list, index: int) -> str:
    if index < 0 or index >= len(row):
        return ""
    value = row[index]
    return "" if value is None else str(value)


def _normalized_plain(value: str) -> str:
    return value.strip()


def _normalized_identifier(value: str) -> str:
    if value is None:
        return ""

    result = str(value).replace("\u200b", "").replace("\ufeff", "").strip()

    while result.startswith(("'", "’", "`")):
        result = result[1:].lstrip()

    return result


def _normalize_by_header(header_name: str, value: str) -> str:
    if header_name in {"BARCODE", "Баркод", "ШК", "Артикул"}:
        return _normalized_identifier(value)
    return _normalized_plain(value)

def _to_sheet_number_if_possible(value: str):
    normalized = _normalized_identifier(value)
    if normalized.isdigit():
        try:
            return int(normalized)
        except ValueError:
            return normalized
    return normalized