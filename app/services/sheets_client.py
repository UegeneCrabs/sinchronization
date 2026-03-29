from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Iterable, Protocol
import time
import logging
from collections import Counter, defaultdict

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


class GoogleApiSheetsClient:
    """Google Sheets API client.

    Requires a configured Google credential environment (ADC or service account).
    """

    def __init__(self) -> None:
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Google client libraries are not installed for this Python interpreter. "
                "Install with: python -m pip install google-api-python-client google-auth"
            ) from exc

        creds_file = self._resolve_credentials_file()
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = service_account.Credentials.from_service_account_file(creds_file, scopes=scopes)
        self._service = build("sheets", "v4", credentials=credentials)
        self._sheet_id_cache: dict[tuple[str, str], int] = {}
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

    def read_sheet(self, spreadsheet_url: str, sheet_name: str) -> list[list]:
        started = time.perf_counter()

        spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
        if not spreadsheet_id:
            raise ValueError("Invalid spreadsheet URL")

        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=sheet_name)
            .execute()
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

        cache_key = (spreadsheet_id, sheet_name)
        sheet_id = self._sheet_id_cache.get(cache_key)

        resolve_sheet_id_ms = 0
        if sheet_id is None:
            resolve_start = time.perf_counter()
            sheet_id = _resolve_sheet_id(self._service, spreadsheet_id, sheet_name)
            self._sheet_id_cache[cache_key] = sheet_id
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

        grid_start = time.perf_counter()
        response = (
            self._service.spreadsheets()
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
            .execute()
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

        cache_key = (spreadsheet_id, sheet_name)
        sheet_id = self._sheet_id_cache.get(cache_key)

        resolve_sheet_id_ms = 0
        if sheet_id is None:
            resolve_start = time.perf_counter()
            sheet_id = _resolve_sheet_id(self._service, spreadsheet_id, sheet_name)
            self._sheet_id_cache[cache_key] = sheet_id
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

        grid_start = time.perf_counter()
        response = (
            self._service.spreadsheets()
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
            .execute()
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

        spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
        if not spreadsheet_id:
            raise ValueError("Invalid spreadsheet URL")

        value_write_ms = 0
        resolve_sheet_id_ms = 0
        background_group_ms = 0
        background_write_ms = 0

        background_input_count = len(payload.background_updates)
        grouped_background_count = 0
        color_counter = Counter(update.color for update in payload.background_updates)

        if payload.value_updates:
            started = time.perf_counter()
            for chunk in _chunked(payload.value_updates, VALUE_BATCH_SIZE):
                data = [
                    {
                        "range": f"{sheet_name}!{_a1(item.col, item.row)}",
                        "values": [[item.value]],
                    }
                    for item in chunk
                ]
                (
                    self._service.spreadsheets()
                    .values()
                    .batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={
                            "valueInputOption": "RAW",
                            "data": data,
                        },
                    )
                    .execute()
                )
            value_write_ms = int((time.perf_counter() - started) * 1000)

        if payload.background_updates:
            cache_key = (spreadsheet_id, sheet_name)
            sheet_id = self._sheet_id_cache.get(cache_key)

            if sheet_id is None:
                started = time.perf_counter()
                sheet_id = _resolve_sheet_id(self._service, spreadsheet_id, sheet_name)
                self._sheet_id_cache[cache_key] = sheet_id
                resolve_sheet_id_ms = int((time.perf_counter() - started) * 1000)

            started = time.perf_counter()
            grouped_updates = _group_cell_style_updates(payload.background_updates)
            grouped_background_count = len(grouped_updates)
            background_group_ms = int((time.perf_counter() - started) * 1000)

            try:
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

                    (
                        self._service.spreadsheets()
                        .batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body={"requests": requests},
                        )
                        .execute()
                    )

                background_write_ms = int((time.perf_counter() - started) * 1000)
            except Exception:
                self.logger.exception("background_update_failed sheet=%s", sheet_name)
                return

        total_ms = int((time.perf_counter() - total_start) * 1000)
        self.logger.info(
            (
                "write_profile sheet=%s "
                "value_updates=%s "
                "background_updates_in=%s "
                "background_updates_grouped=%s "
                "white=%s orange=%s red=%s lightblue=%s "
                "value_write_ms=%s "
                "resolve_sheet_id_ms=%s "
                "background_group_ms=%s "
                "background_write_ms=%s "
                "total_ms=%s"
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


def _resolve_sheet_id(service, spreadsheet_id: str, sheet_name: str) -> int:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            return int(props["sheetId"])

    raise ValueError(f"Sheet not found: {sheet_name}")


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