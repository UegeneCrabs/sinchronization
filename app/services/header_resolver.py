from __future__ import annotations

HEADER_SEARCH_LIMIT = 25


def find_headers(data: list[list], expected_headers: list[str]) -> tuple[dict[str, int], int]:
    for i in range(min(HEADER_SEARCH_LIMIT, len(data))):
        result = _check_row(data[i], expected_headers)
        if result:
            return result, i + 1
    for i in range(HEADER_SEARCH_LIMIT, len(data)):
        result = _check_row(data[i], expected_headers)
        if result:
            return result, i + 1
    raise ValueError(f"Headers not found: {', '.join(expected_headers)}")


def _check_row(row: list, expected_headers: list[str]) -> dict[str, int] | None:
    if not row or len(row) < len(expected_headers):
        return None
    mapping: dict[str, int] = {}
    for header in expected_headers:
        try:
            mapping[header] = row.index(header)
        except ValueError:
            return None
    return mapping
