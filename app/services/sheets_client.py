from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import os
from pathlib import Path
import random
import threading
import time
from collections import Counter, defaultdict, deque
from typing import Iterable, Literal, Protocol

from app.utils import extract_spreadsheet_id

VALUE_BATCH_SIZE = 500
FORMAT_BATCH_SIZE = 500


@dataclass
class CellStyleUpdate:
    row: int
    col: int
    color: str


@dataclass
class CellValueUpdate:
    row: int
    col: int
    value: str


@dataclass
class RangeStyleUpdate:
    start_row: int
    end_row: int
    start_col: int
    end_col: int
    color: str


@dataclass
class SheetWritePayload:
    values: list[list]
    value_updates: list[CellValueUpdate] = field(default_factory=list)
    background_updates: list[CellStyleUpdate] = field(default_factory=list)


class SheetsClient(Protocol):
    def read_sheet(self, spreadsheet_url: str, sheet_name: str) -> list[list]:
        ...

    def write_sheet(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        payload: SheetWritePayload,
    ) -> None:
        ...

    def read_background_colors(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        cells: set[tuple[int, int]],
    ) -> dict[tuple[int, int], str]:
        ...

    def read_background_colors_in_range(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        row_indexes: set[int],
        start_col: int,
        end_col: int,
    ) -> dict[tuple[int, int], str]:
        ...


class InMemorySheetsClient:
    """Test/dry-run client storing sheet data in process memory."""

    def __init__(self, seed_data: dict[tuple[str, str], list[list]] | None = None) -> None:
        self._data = seed_data or {}
        self.style_updates: dict[tuple[str, str], list[CellStyleUpdate]] = {}

    def read_sheet(self, spreadsheet_url: str, sheet_name: str) -> list[list]:
        return [row[:] for row in self._data.get((spreadsheet_url, sheet_name), [])]

    def write_sheet(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        payload: SheetWritePayload,
    ) -> None:
        self._data[(spreadsheet_url, sheet_name)] = [row[:] for row in payload.values]
        self.style_updates[(spreadsheet_url, sheet_name)] = list(payload.background_updates)

    def read_background_colors(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        cells: set[tuple[int, int]],
    ) -> dict[tuple[int, int], str]:
        result: dict[tuple[int, int], str] = {}
        applied = self.style_updates.get((spreadsheet_url, sheet_name), [])

        style_map: dict[tuple[int, int], str] = {}
        for item in applied:
            style_map[(item.row, item.col)] = item.color

        for cell in cells:
            result[cell] = style_map.get(cell, "white")

        return result

    def read_background_colors_in_range(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        row_indexes: set[int],
        start_col: int,
        end_col: int,
    ) -> dict[tuple[int, int], str]:
        result: dict[tuple[int, int], str] = {}
        applied = self.style_updates.get((spreadsheet_url, sheet_name), [])

        style_map: dict[tuple[int, int], str] = {}
        for item in applied:
            style_map[(item.row, item.col)] = item.color

        for row_idx in row_indexes:
            for col_idx in range(start_col, end_col + 1):
                result[(row_idx, col_idx)] = style_map.get((row_idx, col_idx), "white")

        return result


class RequestRateLimiter:
    """
    Общий лимитер на все Google API вызовы внутри процесса.

    Работает по sliding window на 60 секунд:
    - отдельный лимит на read
    - отдельный лимит на write
    - общий лимит на одновременные запросы
    """

    def __init__(
        self,
        *,
        read_per_minute: int,
        write_per_minute: int,
        max_inflight: int,
    ) -> None:
        self._read_per_minute = read_per_minute
        self._write_per_minute = write_per_minute
        self._read_timestamps: deque[float] = deque()
        self._write_timestamps: deque[float] = deque()
        self._condition = threading.Condition()
        self._inflight = threading.BoundedSemaphore(value=max_inflight)

    def acquire(self, request_kind: Literal["read", "write"]) -> None:
        queue = self._read_timestamps if request_kind == "read" else self._write_timestamps
        limit = self._read_per_minute if request_kind == "read" else self._write_per_minute

        with self._condition:
            while True:
                now = time.monotonic()
                self._drop_expired(queue, now)

                if len(queue) < limit:
                    queue.append(now)
                    return

                oldest = queue[0]
                wait_seconds = max(0.05, 60.0 - (now - oldest))
                self._condition.wait(timeout=wait_seconds)

    def release_waiters(self) -> None:
        with self._condition:
            self._condition.notify_all()

    def acquire_inflight(self) -> None:
        self._inflight.acquire()

    def release_inflight(self) -> None:
        self._inflight.release()

    @staticmethod
    def _drop_expired(queue: deque[float], now: float) -> None:
        while queue and (now - queue[0]) >= 60.0:
            queue.popleft()


class GoogleApiSheetsClient:
    """
    Google Sheets API client:
    - отдельный service/http на поток
    - retry на 429/5xx и квотные 403
    - truncated exponential backoff + jitter
    - общий limiter на read/write
    """

    def __init__(
        self,
        *,
        read_per_minute: int = 45,
        write_per_minute: int = 45,
        max_inflight_requests: int = 4,
        max_retries: int = 6,
        max_backoff_seconds: int = 32,
        http_timeout_seconds: int = 120,
    ) -> None:
        try:
            import google_auth_httplib2
            import httplib2
            from google.oauth2 import service_account
            from googleapiclient.discovery import build
            from googleapiclient.errors import HttpError
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Google client libraries are not installed for this Python interpreter. "
                "Install with: python -m pip install google-api-python-client google-auth google-auth-httplib2"
            ) from exc

        self._service_account = service_account
        self._build = build
        self._httplib2 = httplib2
        self._google_auth_httplib2 = google_auth_httplib2
        self._http_error_cls = HttpError

        creds_file = self._resolve_credentials_file()
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self._credentials = self._service_account.Credentials.from_service_account_file(
            creds_file,
            scopes=scopes,
        )

        self._thread_local = threading.local()
        self._sheet_id_cache: dict[tuple[str, str], int] = {}
        self._sheet_id_cache_lock = threading.Lock()

        self._rate_limiter = RequestRateLimiter(
            read_per_minute=read_per_minute,
            write_per_minute=write_per_minute,
            max_inflight=max_inflight_requests,
        )

        self._max_retries = max_retries
        self._max_backoff_seconds = max_backoff_seconds
        self._http_timeout_seconds = http_timeout_seconds
        self.logger = logging.getLogger("uvicorn.error")

    @staticmethod
    def _resolve_credentials_file() -> str:
        env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if env_path and Path(env_path).exists():
            return env_path

        local_default = Path.cwd() / "missilesupply-828b4f393287.json"
        if local_default.exists():
            return str(local_default)

        raise RuntimeError(
            "Service account key not found. Set GOOGLE_APPLICATION_CREDENTIALS "
            "or place missilesupply-828b4f393287.json in the project root."
        )

    def _get_service(self):
        service = getattr(self._thread_local, "service", None)
        if service is not None:
            return service

        http = self._httplib2.Http(timeout=self._http_timeout_seconds)
        authed_http = self._google_auth_httplib2.AuthorizedHttp(self._credentials, http=http)
        service = self._build(
            "sheets",
            "v4",
            http=authed_http,
            cache_discovery=False,
        )

        self._thread_local.service = service
        return service

    def _execute_with_retry(
        self,
        request_factory,
        *,
        request_kind: Literal["read", "write"],
        operation_name: str,
        sheet_name: str,
    ):
        last_error = None

        for attempt in range(self._max_retries + 1):
            self._rate_limiter.acquire(request_kind)
            self._rate_limiter.acquire_inflight()

            try:
                request = request_factory()
                return request.execute()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                retriable, reason = self._is_retryable_error(exc)

                if not retriable or attempt >= self._max_retries:
                    raise

                sleep_seconds = min(
                    (2 ** attempt) + random.uniform(0, 1),
                    self._max_backoff_seconds,
                )

                self.logger.warning(
                    (
                        "google_api_retry operation=%s sheet=%s request_kind=%s "
                        "attempt=%s sleep_s=%.2f reason=%s"
                    ),
                    operation_name,
                    sheet_name,
                    request_kind,
                    attempt + 1,
                    sleep_seconds,
                    reason,
                )
            finally:
                self._rate_limiter.release_inflight()
                self._rate_limiter.release_waiters()

            time.sleep(sleep_seconds)

        raise last_error

    def _is_retryable_error(self, exc: Exception) -> tuple[bool, str]:
        if isinstance(exc, TimeoutError):
            return True, "timeout"

        if isinstance(exc, self._http_error_cls):
            status = getattr(exc.resp, "status", None)
            reason = self._extract_http_error_reason(exc)

            if status in {429, 500, 502, 503, 504}:
                return True, f"http_{status}:{reason}"

            if status == 403 and reason in {
                "rateLimitExceeded",
                "userRateLimitExceeded",
                "quotaExceeded",
            }:
                return True, f"http_403:{reason}"

            return False, f"http_{status}:{reason}"

        message = str(exc).lower()
        if any(token in message for token in ("timed out", "timeout", "connection reset", "temporarily unavailable")):
            return True, "network_transient"

        return False, exc.__class__.__name__

    @staticmethod
    def _extract_http_error_reason(exc: Exception) -> str:
        content = getattr(exc, "content", b"")
        if not content:
            return "unknown"

        try:
            payload = json.loads(content.decode("utf-8"))
        except Exception:
            return "unknown"

        error = payload.get("error", {})
        errors = error.get("errors", [])
        if errors and isinstance(errors, list):
            first = errors[0]
            if isinstance(first, dict):
                return str(first.get("reason") or first.get("message") or "unknown")

        return str(error.get("message") or "unknown")

    def _get_sheet_id(self, spreadsheet_id: str, sheet_name: str) -> int:
        cache_key = (spreadsheet_id, sheet_name)

        with self._sheet_id_cache_lock:
            cached = self._sheet_id_cache.get(cache_key)
            if cached is not None:
                return cached

        sheet_id = self._resolve_sheet_id_remote(spreadsheet_id, sheet_name)

        with self._sheet_id_cache_lock:
            self._sheet_id_cache[cache_key] = sheet_id

        return sheet_id

    def _resolve_sheet_id_remote(self, spreadsheet_id: str, sheet_name: str) -> int:
        service = self._get_service()
        meta = self._execute_with_retry(
            lambda: service.spreadsheets().get(spreadsheetId=spreadsheet_id),
            request_kind="read",
            operation_name="resolve_sheet_id",
            sheet_name=sheet_name,
        )

        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("title") == sheet_name:
                return int(props["sheetId"])

        raise ValueError(f"Sheet not found: {sheet_name}")

    def read_sheet(self, spreadsheet_url: str, sheet_name: str) -> list[list]:
        started = time.perf_counter()

        spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
        if not spreadsheet_id:
            raise ValueError("Invalid spreadsheet URL")

        service = self._get_service()
        result = self._execute_with_retry(
            lambda: (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=sheet_name)
            ),
            request_kind="read",
            operation_name="read_sheet",
            sheet_name=sheet_name,
        )
        values = result.get("values", [])

        self.logger.info(
            "read_sheet_profile sheet=%s rows=%s read_ms=%s",
            sheet_name,
            len(values),
            int((time.perf_counter() - started) * 1000),
        )
        return values

    def read_background_colors(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        cells: set[tuple[int, int]],
    ) -> dict[tuple[int, int], str]:
        started = time.perf_counter()

        spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
        if not spreadsheet_id:
            raise ValueError("Invalid spreadsheet URL")

        if not cells:
            return {}

        resolve_start = time.perf_counter()
        sheet_id = self._get_sheet_id(spreadsheet_id, sheet_name)
        resolve_sheet_id_ms = int((time.perf_counter() - resolve_start) * 1000)

        cells_by_row: dict[int, list[int]] = defaultdict(list)
        for row_idx, col_idx in cells:
            cells_by_row[row_idx].append(col_idx)

        ranges: list[dict[str, int]] = []
        for row_idx, cols in sorted(cells_by_row.items()):
            for col_start, col_end in _group_indexes_to_ranges(cols):
                ranges.append(
                    {
                        "sheetId": sheet_id,
                        "startRowIndex": row_idx,
                        "endRowIndex": row_idx + 1,
                        "startColumnIndex": col_start,
                        "endColumnIndex": col_end,
                    }
                )

        service = self._get_service()
        grid_start = time.perf_counter()
        response = self._execute_with_retry(
            lambda: (
                service.spreadsheets()
                .get(
                    spreadsheetId=spreadsheet_id,
                    ranges=[_grid_range_to_a1(sheet_name, grid_range) for grid_range in ranges],
                    includeGridData=True,
                    fields=(
                        "sheets(data(startRow,startColumn,rowData(values("
                        "effectiveFormat(backgroundColor,backgroundColorStyle),"
                        "userEnteredFormat(backgroundColor,backgroundColorStyle)"
                        "))))"
                    ),
                )
            ),
            request_kind="read",
            operation_name="read_background_colors",
            sheet_name=sheet_name,
        )
        grid_read_ms = int((time.perf_counter() - grid_start) * 1000)

        result: dict[tuple[int, int], str] = {(row, col): "white" for row, col in cells}

        parse_start = time.perf_counter()
        data_blocks = _extract_data_blocks(response)

        for grid_range, data_block in zip(ranges, data_blocks):
            row_data = data_block.get("rowData", [])
            start_row = data_block.get("startRow", grid_range["startRowIndex"])
            start_col = data_block.get("startColumn", grid_range["startColumnIndex"])

            for row_offset, row_item in enumerate(row_data):
                values = row_item.get("values", [])
                for col_offset, cell in enumerate(values):
                    row_idx = start_row + row_offset
                    col_idx = start_col + col_offset
                    key = (row_idx, col_idx)

                    if key not in result:
                        continue

                    background = _extract_background_color(cell)
                    result[key] = _rgb_to_color_name(background)

        parse_ms = int((time.perf_counter() - parse_start) * 1000)

        self.logger.info(
            (
                "read_background_profile sheet=%s cells=%s row_ranges=%s "
                "resolve_sheet_id_ms=%s grid_read_ms=%s parse_ms=%s total_ms=%s"
            ),
            sheet_name,
            len(cells),
            len(ranges),
            resolve_sheet_id_ms,
            grid_read_ms,
            parse_ms,
            int((time.perf_counter() - started) * 1000),
        )
        return result

    def read_background_colors_in_range(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        row_indexes: set[int],
        start_col: int,
        end_col: int,
    ) -> dict[tuple[int, int], str]:
        started = time.perf_counter()

        spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
        if not spreadsheet_id:
            raise ValueError("Invalid spreadsheet URL")

        if not row_indexes:
            return {}

        if start_col < 0 or end_col < start_col:
            return {}

        resolve_start = time.perf_counter()
        sheet_id = self._get_sheet_id(spreadsheet_id, sheet_name)
        resolve_sheet_id_ms = int((time.perf_counter() - resolve_start) * 1000)

        row_groups = _group_indexes_to_ranges(list(row_indexes))

        ranges: list[dict[str, int]] = []
        for row_start, row_end in row_groups:
            ranges.append(
                {
                    "sheetId": sheet_id,
                    "startRowIndex": row_start,
                    "endRowIndex": row_end,
                    "startColumnIndex": start_col,
                    "endColumnIndex": end_col + 1,
                }
            )

        service = self._get_service()
        grid_start = time.perf_counter()
        response = self._execute_with_retry(
            lambda: (
                service.spreadsheets()
                .get(
                    spreadsheetId=spreadsheet_id,
                    ranges=[_grid_range_to_a1(sheet_name, grid_range) for grid_range in ranges],
                    includeGridData=True,
                    fields=(
                        "sheets(data(startRow,startColumn,rowData(values("
                        "effectiveFormat(backgroundColor,backgroundColorStyle),"
                        "userEnteredFormat(backgroundColor,backgroundColorStyle)"
                        "))))"
                    ),
                )
            ),
            request_kind="read",
            operation_name="read_background_colors_in_range",
            sheet_name=sheet_name,
        )
        grid_read_ms = int((time.perf_counter() - grid_start) * 1000)

        result: dict[tuple[int, int], str] = {}
        for row_idx in row_indexes:
            for col_idx in range(start_col, end_col + 1):
                result[(row_idx, col_idx)] = "white"

        parse_start = time.perf_counter()
        data_blocks = _extract_data_blocks(response)

        for grid_range, data_block in zip(ranges, data_blocks):
            row_data = data_block.get("rowData", [])
            start_row = data_block.get("startRow", grid_range["startRowIndex"])
            start_col_idx = data_block.get("startColumn", grid_range["startColumnIndex"])

            for row_offset, row_item in enumerate(row_data):
                values = row_item.get("values", [])
                for col_offset, cell in enumerate(values):
                    row_idx = start_row + row_offset
                    col_idx = start_col_idx + col_offset
                    key = (row_idx, col_idx)

                    if key not in result:
                        continue

                    background = _extract_background_color(cell)
                    result[key] = _rgb_to_color_name(background)

        parse_ms = int((time.perf_counter() - parse_start) * 1000)

        self.logger.info(
            (
                "read_background_band_profile sheet=%s rows=%s row_groups=%s start_col=%s end_col=%s "
                "ranges=%s resolve_sheet_id_ms=%s grid_read_ms=%s parse_ms=%s total_ms=%s"
            ),
            sheet_name,
            len(row_indexes),
            len(row_groups),
            start_col,
            end_col,
            len(ranges),
            resolve_sheet_id_ms,
            grid_read_ms,
            parse_ms,
            int((time.perf_counter() - started) * 1000),
        )
        return result

    def write_sheet(self, spreadsheet_url: str, sheet_name: str, payload: SheetWritePayload) -> None:
        total_start = time.perf_counter()
        debug_barcode = os.getenv("DEBUG_BARCODE", "").strip()

        spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
        if not spreadsheet_id:
            raise ValueError("Invalid spreadsheet URL")

        service = self._get_service()

        value_write_ms = 0
        resolve_sheet_id_ms = 0
        background_group_ms = 0
        background_write_ms = 0

        background_input_count = len(payload.background_updates)
        grouped_background_count = 0
        color_counter = Counter(update.color for update in payload.background_updates)

        if payload.value_updates:
            started = time.perf_counter()

            sanitized_value_updates = []
            debug_written_cells: list[tuple[int, int, object]] = []

            for item in payload.value_updates:
                sanitized_value = _sanitize_outgoing_sheet_value(item.value)

                if debug_barcode:
                    raw_norm = _normalized_identifier_for_debug(item.value)
                    sanitized_norm = _normalized_identifier_for_debug(sanitized_value)
                    if raw_norm == debug_barcode or sanitized_norm == debug_barcode:
                        self.logger.info(
                            "debug_write_sanitize sheet=%s row=%s col=%s raw_value=%r raw_type=%s sanitized_value=%r sanitized_type=%s raw_norm=%r sanitized_norm=%r",
                            sheet_name,
                            item.row,
                            item.col,
                            item.value,
                            type(item.value).__name__,
                            sanitized_value,
                            type(sanitized_value).__name__,
                            raw_norm,
                            sanitized_norm,
                        )
                        debug_written_cells.append((item.row, item.col, sanitized_value))

                sanitized_value_updates.append(
                    CellValueUpdate(
                        row=item.row,
                        col=item.col,
                        value=sanitized_value,
                    )
                )

            for chunk in _chunked(sanitized_value_updates, VALUE_BATCH_SIZE):
                data = []
                for item in chunk:
                    if debug_barcode:
                        normalized = _normalized_identifier_for_debug(item.value)
                        if normalized == debug_barcode:
                            self.logger.info(
                                "debug_write_payload sheet=%s row=%s col=%s final_value=%r final_type=%s a1=%s",
                                sheet_name,
                                item.row,
                                item.col,
                                item.value,
                                type(item.value).__name__,
                                _a1(item.col, item.row),
                            )

                    data.append(
                        {
                            "range": f"{sheet_name}!{_a1(item.col, item.row)}",
                            "values": [[item.value]],
                        }
                    )

                self._execute_with_retry(
                    lambda chunk_data=data: (
                        service.spreadsheets()
                        .values()
                        .batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body={
                                "valueInputOption": "RAW",
                                "data": chunk_data,
                            },
                        )
                    ),
                    request_kind="write",
                    operation_name="write_values_batch",
                    sheet_name=sheet_name,
                )

            if debug_written_cells:
                for row_idx, col_idx, expected_value in debug_written_cells:
                    a1 = _a1(col_idx, row_idx)

                    read_back = self._execute_with_retry(
                        lambda read_range=f"{sheet_name}!{a1}": (
                            service.spreadsheets()
                            .values()
                            .get(
                                spreadsheetId=spreadsheet_id,
                                range=read_range,
                                valueRenderOption="UNFORMATTED_VALUE",
                            )
                        ),
                        request_kind="read",
                        operation_name="debug_read_after_write",
                        sheet_name=sheet_name,
                    )

                    read_back_values = read_back.get("values", [])
                    read_back_value = ""
                    if read_back_values and read_back_values[0]:
                        read_back_value = read_back_values[0][0]

                    self.logger.info(
                        "debug_read_after_write sheet=%s a1=%s expected_value=%r expected_type=%s read_back_value=%r read_back_type=%s",
                        sheet_name,
                        a1,
                        expected_value,
                        type(expected_value).__name__,
                        read_back_value,
                        type(read_back_value).__name__,
                    )

            value_write_ms = int((time.perf_counter() - started) * 1000)

        if payload.background_updates:
            resolve_start = time.perf_counter()
            sheet_id = self._get_sheet_id(spreadsheet_id, sheet_name)
            resolve_sheet_id_ms = int((time.perf_counter() - resolve_start) * 1000)

            started = time.perf_counter()
            grouped_updates = _group_cell_style_updates(payload.background_updates)
            grouped_background_count = len(grouped_updates)
            background_group_ms = int((time.perf_counter() - started) * 1000)

            started = time.perf_counter()
            for chunk in _chunked(grouped_updates, FORMAT_BATCH_SIZE):
                requests = []
                for update in chunk:
                    rgb = _to_rgb(update.color)
                    requests.append(
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": update.start_row,
                                    "endRowIndex": update.end_row,
                                    "startColumnIndex": update.start_col,
                                    "endColumnIndex": update.end_col,
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb,
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor",
                            }
                        }
                    )

                self._execute_with_retry(
                    lambda body_requests=requests: (
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body={"requests": body_requests},
                        )
                    ),
                    request_kind="write",
                    operation_name="write_background_batch",
                    sheet_name=sheet_name,
                )

            background_write_ms = int((time.perf_counter() - started) * 1000)

        total_ms = int((time.perf_counter() - total_start) * 1000)

        self.logger.info(
            (
                "write_profile sheet=%s "
                "value_updates=%s background_updates_in=%s background_updates_grouped=%s "
                "white=%s orange=%s red=%s lightblue=%s "
                "value_write_ms=%s resolve_sheet_id_ms=%s background_group_ms=%s background_write_ms=%s total_ms=%s"
            ),
            sheet_name,
            len(payload.value_updates),
            background_input_count,
            grouped_background_count,
            color_counter.get("white", 0),
            color_counter.get("orange", 0),
            color_counter.get("red", 0),
            color_counter.get("lightblue", 0),
            value_write_ms,
            resolve_sheet_id_ms,
            background_group_ms,
            background_write_ms,
            total_ms,
        )


def _extract_data_blocks(response: dict) -> list[dict]:
    blocks: list[dict] = []
    for sheet in response.get("sheets", []):
        blocks.extend(sheet.get("data", []))
    return blocks


def _extract_background_color(cell: dict) -> dict[str, float]:
    effective_rgb = cell.get("effectiveFormat", {}).get("backgroundColor", {})
    if effective_rgb:
        return effective_rgb

    effective_style_rgb = (
        cell.get("effectiveFormat", {})
        .get("backgroundColorStyle", {})
        .get("rgbColor", {})
    )
    if effective_style_rgb:
        return effective_style_rgb

    entered_rgb = cell.get("userEnteredFormat", {}).get("backgroundColor", {})
    if entered_rgb:
        return entered_rgb

    entered_style_rgb = (
        cell.get("userEnteredFormat", {})
        .get("backgroundColorStyle", {})
        .get("rgbColor", {})
    )
    if entered_style_rgb:
        return entered_style_rgb

    return {}


def _chunked(items: list, size: int) -> Iterable[list]:
    if size <= 0:
        raise ValueError("Chunk size must be greater than 0")

    for i in range(0, len(items), size):
        yield items[i:i + size]


def _to_rgb(color: str) -> dict[str, float]:
    mapping = {
        "white": {"red": 1, "green": 1, "blue": 1},
        "orange": {"red": 1, "green": 0.6, "blue": 0},
        "red": {"red": 1, "green": 0, "blue": 0},
        "lightblue": {"red": 0.68, "green": 0.85, "blue": 0.9},
    }
    return mapping.get(color, mapping["white"])


def _rgb_to_color_name(rgb: dict[str, float]) -> str:
    if not rgb:
        return "white"

    red = round(rgb.get("red", 0), 2)
    green = round(rgb.get("green", 0), 2)
    blue = round(rgb.get("blue", 0), 2)

    candidates = {
        "white": (1.00, 1.00, 1.00),
        "orange": (1.00, 0.60, 0.00),
        "red": (1.00, 0.00, 0.00),
        "lightblue": (0.68, 0.85, 0.90),
    }

    best_name = "white"
    best_distance = float("inf")

    for name, (r, g, b) in candidates.items():
        distance = abs(red - r) + abs(green - g) + abs(blue - b)
        if distance < best_distance:
            best_distance = distance
            best_name = name

    return best_name


def _a1(col_idx: int, row_idx: int) -> str:
    col = ""
    n = col_idx
    while n >= 0:
        col = chr((n % 26) + 65) + col
        n = (n // 26) - 1
    return f"{col}{row_idx + 1}"


def _grid_range_to_a1(sheet_name: str, grid_range: dict[str, int]) -> str:
    start_col = _a1_col(grid_range["startColumnIndex"])
    end_col = _a1_col(grid_range["endColumnIndex"] - 1)
    start_row = grid_range["startRowIndex"] + 1
    end_row = grid_range["endRowIndex"]
    return f"{sheet_name}!{start_col}{start_row}:{end_col}{end_row}"


def _a1_col(col_idx: int) -> str:
    col = ""
    n = col_idx
    while n >= 0:
        col = chr((n % 26) + 65) + col
        n = (n // 26) - 1
    return col


def _group_indexes_to_ranges(indexes: list[int]) -> list[tuple[int, int]]:
    if not indexes:
        return []

    sorted_indexes = sorted(set(indexes))
    ranges: list[tuple[int, int]] = []

    start = sorted_indexes[0]
    prev = sorted_indexes[0]

    for idx in sorted_indexes[1:]:
        if idx == prev + 1:
            prev = idx
            continue

        ranges.append((start, prev + 1))
        start = idx
        prev = idx

    ranges.append((start, prev + 1))
    return ranges


def _group_cell_style_updates(updates: list[CellStyleUpdate]) -> list[RangeStyleUpdate]:
    if not updates:
        return []

    unique_updates: dict[tuple[int, int], str] = {}
    for item in updates:
        unique_updates[(item.row, item.col)] = item.color

    by_row_and_color: dict[tuple[int, str], list[int]] = defaultdict(list)
    for (row, col), color in unique_updates.items():
        by_row_and_color[(row, color)].append(col)

    ranges: list[RangeStyleUpdate] = []

    for (row, color), cols in by_row_and_color.items():
        sorted_cols = sorted(cols)
        start_col = sorted_cols[0]
        prev_col = sorted_cols[0]

        for col in sorted_cols[1:]:
            if col == prev_col + 1:
                prev_col = col
                continue

            ranges.append(
                RangeStyleUpdate(
                    start_row=row,
                    end_row=row + 1,
                    start_col=start_col,
                    end_col=prev_col + 1,
                    color=color,
                )
            )
            start_col = col
            prev_col = col

        ranges.append(
            RangeStyleUpdate(
                start_row=row,
                end_row=row + 1,
                start_col=start_col,
                end_col=prev_col + 1,
                color=color,
            )
        )

    ranges.sort(key=lambda x: (x.start_row, x.start_col, x.color))
    return ranges



def _sanitize_outgoing_sheet_value(value):
    if value is None:
        return ""

    if isinstance(value, (int, float)):
        return value

    result = str(value).replace("\u200b", "").replace("\ufeff", "").strip()

    while result.startswith(("'", "’", "`")):
        result = result[1:].lstrip()

    return result


def _normalized_identifier_for_debug(value) -> str:
    if value is None:
        return ""

    result = str(value).replace("\u200b", "").replace("\ufeff", "").strip()

    while result.startswith(("'", "’", "`")):
        result = result[1:].lstrip()

    return result