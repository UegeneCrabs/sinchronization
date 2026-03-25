import re


def extract_spreadsheet_id(url: str) -> str | None:
    if not url:
        return None
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    return match.group(1) if match else None
