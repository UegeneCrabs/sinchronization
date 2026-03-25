from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Iterable, Protocol

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
        spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
        if not spreadsheet_id:
            raise ValueError("Invalid spreadsheet URL")

        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=sheet_name)
            .execute()
        )
        return result.get("values", [])

    def write_sheet(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        payload: SheetWritePayload,
    ) -> None:
        spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
        if not spreadsheet_id:
            raise ValueError("Invalid spreadsheet URL")

        if payload.value_updates:
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

        if not payload.background_updates:
            return

        cache_key = (spreadsheet_id, sheet_name)
        sheet_id = self._sheet_id_cache.get(cache_key)
        if sheet_id is None:
            sheet_id = _resolve_sheet_id(self._service, spreadsheet_id, sheet_name)
            self._sheet_id_cache[cache_key] = sheet_id

        try:
            for chunk in _chunked(payload.background_updates, FORMAT_BATCH_SIZE):
                requests = []
                for update in chunk:
                    rgb = _to_rgb(update.color)
                    requests.append(
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": update.row,
                                    "endRowIndex": update.row + 1,
                                    "startColumnIndex": update.col,
                                    "endColumnIndex": update.col + 1,
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
        except Exception:
            # Some sheets have protected formatting ranges.
            # Data sync should still complete even if coloring is blocked.
            return


def _resolve_sheet_id(service, spreadsheet_id: str, sheet_name: str) -> int:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            return int(props["sheetId"])

    raise ValueError(f"Sheet not found: {sheet_name}")


def _chunked(items: list, size: int) -> Iterable[list]:
    if size <= 0:
        raise ValueError("Chunk size must be greater than 0")

    for i in range(0, len(items), size):
        yield items[i : i + size]


def _to_rgb(color: str) -> dict[str, float]:
    mapping = {
        "white": {"red": 1, "green": 1, "blue": 1},
        "orange": {"red": 1, "green": 0.6, "blue": 0},
        "red": {"red": 1, "green": 0, "blue": 0},
        "lightblue": {"red": 0.68, "green": 0.85, "blue": 0.9},
    }
    return mapping.get(color, mapping["white"])


def _a1(col_idx: int, row_idx: int) -> str:
    col = ""
    n = col_idx
    while n >= 0:
        col = chr((n % 26) + 65) + col
        n = (n // 26) - 1
    return f"{col}{row_idx + 1}"