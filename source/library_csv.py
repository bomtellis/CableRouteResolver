"""CSV exports for project room-type and endpoint-asset libraries."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable, Mapping, Sequence


ASSET_CSV_FIELDS = (
    "id",
    "name",
    "ADB_Code",
    "Group",
    "connection_type",
    "category_id",
    "qty",
    "data_points",
    "capability_keywords",
)

ROOM_TYPE_CSV_FIELDS = (
    "id",
    "name",
    "scenario_group",
    "asset_ids",
    "assets",
)


def _csv_value(value):
    """Return a spreadsheet-friendly value without discarding nested data."""
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _fieldnames(rows: Sequence[Mapping], preferred_fields: Iterable[str]) -> list[str]:
    fields = []
    seen = set()
    for field in preferred_fields:
        field = str(field)
        if field not in seen:
            fields.append(field)
            seen.add(field)

    extras = {
        str(key)
        for row in rows
        for key in row
        if str(key) not in seen
    }
    fields.extend(sorted(extras, key=str.casefold))
    return fields


def write_library_csv(path, rows, *, preferred_fields=()) -> int:
    """Write mapping rows as UTF-8 CSV and return the number exported.

    Preferred fields are emitted first. Any additional keys found in the data
    follow alphabetically, which keeps exports deterministic while retaining
    project-specific metadata.
    """
    clean_rows = [dict(row) for row in rows if isinstance(row, Mapping)]
    fields = _fieldnames(clean_rows, preferred_fields)
    if not fields:
        raise ValueError("At least one CSV field is required.")

    destination = Path(path)
    with destination.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in clean_rows:
            writer.writerow({field: _csv_value(row.get(field)) for field in fields})
    return len(clean_rows)


def export_assets_csv(path, assets) -> int:
    return write_library_csv(path, assets, preferred_fields=ASSET_CSV_FIELDS)


def export_room_types_csv(path, room_types) -> int:
    return write_library_csv(path, room_types, preferred_fields=ROOM_TYPE_CSV_FIELDS)
