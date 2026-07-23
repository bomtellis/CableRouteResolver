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

ROOM_TYPE_ASSET_CSV_FIELDS = (
    "room_type_id",
    "room_type_name",
    "scenario_group",
    "asset_id",
    "asset_name",
    "ADB_Code",
    "Group",
    "category_id",
    "qty_in_room",
    "data_points_each",
    "total_data_points",
    "requested_by",
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


def room_type_asset_assignment_rows(room_types, assets_by_id=None) -> list[dict]:
    """Flatten room-type assignments into one spreadsheet row per asset."""

    assets_by_id = dict(assets_by_id or {})
    result = []
    for room_type in room_types or []:
        if not isinstance(room_type, Mapping):
            continue
        room_type_id = str(room_type.get("id", "") or "").strip()
        room_type_name = str(room_type.get("name", "") or "").strip()
        scenario_group = str(room_type.get("scenario_group", "") or "").strip()
        assignments = [
            row
            for row in room_type.get("assets", []) or []
            if isinstance(row, Mapping)
        ]
        if not assignments:
            assignments = [
                {"asset_id": asset_id, "qty": 1}
                for asset_id in room_type.get("asset_ids", []) or []
            ]
        for assignment in assignments:
            asset_id = str(
                assignment.get("asset_id", assignment.get("id", "")) or ""
            ).strip()
            if not asset_id:
                continue
            asset = assets_by_id.get(asset_id, {})
            try:
                quantity = max(1, int(assignment.get("qty", 1) or 1))
            except (TypeError, ValueError):
                quantity = 1
            try:
                data_points = max(0, int(asset.get("data_points", 0) or 0))
            except (TypeError, ValueError):
                data_points = 0
            result.append(
                {
                    "room_type_id": room_type_id,
                    "room_type_name": room_type_name,
                    "scenario_group": scenario_group,
                    "asset_id": asset_id,
                    "asset_name": str(asset.get("name", "") or "").strip(),
                    "ADB_Code": str(
                        asset.get("ADB_Code", asset.get("adb_code", "")) or ""
                    ).strip(),
                    "Group": str(
                        asset.get("Group", asset.get("group", "")) or ""
                    ).strip(),
                    "category_id": str(
                        asset.get("category_id", asset.get("category", "")) or ""
                    ).strip(),
                    "qty_in_room": quantity,
                    "data_points_each": data_points,
                    "total_data_points": quantity * data_points,
                    "requested_by": str(
                        assignment.get("requested_by", "") or ""
                    ).strip(),
                }
            )
    return result


def export_room_type_asset_assignments_csv(
    path, room_types, assets_by_id=None
) -> int:
    rows = room_type_asset_assignment_rows(room_types, assets_by_id)
    return write_library_csv(
        path,
        rows,
        preferred_fields=ROOM_TYPE_ASSET_CSV_FIELDS,
    )
