"""Long-table Excel export for room-type asset details."""

from __future__ import annotations

from collections.abc import Mapping

from asset_bundles import resolve_room_type_asset_connections
from asset_ports import asset_input_ports, room_asset_port_summary
from xlsx_workbook import WorksheetSpec, write_xlsx


ROOM_ASSET_DETAIL_HEADERS = (
    "Room Type ID",
    "Room Type",
    "Scenario Group",
    "Record Type",
    "Asset ID",
    "Description",
    "Category ID",
    "Category",
    "Group",
    "ADB Code",
    "Requested By",
    "Quantity",
    "Physical Inputs Each",
    "Final Network Ports",
    "Network Port Detail",
)


def _text(value) -> str:
    return str(value or "").strip()


def _positive_int(value, default: int = 1) -> int:
    try:
        return max(1, int(value or default))
    except (TypeError, ValueError):
        return default


def _room_assignments(room_type: Mapping) -> list[dict]:
    assignments = [
        dict(row)
        for row in room_type.get("assets", []) or []
        if isinstance(row, Mapping)
        and _text(row.get("asset_id", row.get("id")))
    ]
    if assignments:
        return assignments
    return [
        {"asset_id": _text(asset_id), "qty": 1}
        for asset_id in room_type.get("asset_ids", []) or []
        if _text(asset_id)
    ]


def _category_name(value, fallback: str) -> str:
    if isinstance(value, Mapping):
        return _text(value.get("name", value.get("description", fallback)))
    return _text(value) or fallback


def room_asset_detail_rows(
    room_types,
    assets_by_id=None,
    asset_categories_by_id=None,
    asset_bundles=None,
) -> list[list[object]]:
    """Flatten assigned assets and calculated connection cables into rows."""

    assets_by_id = dict(assets_by_id or {})
    asset_categories_by_id = dict(asset_categories_by_id or {})
    rows: list[list[object]] = []
    for room_type in room_types or []:
        if not isinstance(room_type, Mapping):
            continue
        assignments = _room_assignments(room_type)
        if not assignments:
            continue
        summary = room_asset_port_summary(
            assignments,
            assets_by_id,
            resolve_room_type_asset_connections(
                dict(room_type),
                asset_bundles,
            ),
        )
        final_counts: dict[str, int] = {}
        port_notes: dict[str, list[str]] = {}
        for status in summary.get("network_input_ports", []):
            if not isinstance(status, Mapping):
                continue
            asset_id = _text(status.get("asset_id"))
            if not asset_id:
                continue
            if bool(status.get("counted_upstream", True)):
                final_counts[asset_id] = final_counts.get(asset_id, 0) + 1
            reason = _text(status.get("reason"))
            if reason and reason not in port_notes.setdefault(asset_id, []):
                port_notes[asset_id].append(reason)

        room_type_id = _text(room_type.get("id"))
        room_type_name = _text(room_type.get("name"))
        scenario_group = _text(room_type.get("scenario_group"))
        room_rows: list[list[object]] = []
        for assignment in assignments:
            asset_id = _text(assignment.get("asset_id", assignment.get("id")))
            asset = assets_by_id.get(asset_id, {}) or {}
            category_id = _text(
                asset.get("category_id", asset.get("category"))
            )
            category_name = _category_name(
                asset_categories_by_id.get(category_id),
                category_id,
            )
            room_rows.append(
                [
                    room_type_id,
                    room_type_name,
                    scenario_group,
                    "Assigned asset",
                    asset_id,
                    _text(asset.get("name")) or "(missing asset)",
                    category_id,
                    category_name,
                    _text(asset.get("Group", asset.get("group"))),
                    _text(asset.get("ADB_Code", asset.get("adb_code"))),
                    _text(assignment.get("requested_by")),
                    _positive_int(assignment.get("qty")),
                    asset_input_ports(asset),
                    final_counts.get(asset_id, 0),
                    " ".join(port_notes.get(asset_id, [])),
                ]
            )
        for asset_id, quantity in summary.get("connection_assets", {}).items():
            asset_id = _text(asset_id)
            if not asset_id or int(quantity or 0) <= 0:
                continue
            asset = assets_by_id.get(asset_id, {}) or {}
            category_id = _text(
                asset.get("category_id", asset.get("category"))
            )
            category_name = _category_name(
                asset_categories_by_id.get(category_id),
                category_id,
            )
            room_rows.append(
                [
                    room_type_id,
                    room_type_name,
                    scenario_group,
                    "Calculated connection cable",
                    asset_id,
                    _text(asset.get("name")) or "(missing connection asset)",
                    category_id,
                    category_name,
                    _text(asset.get("Group", asset.get("group"))),
                    _text(asset.get("ADB_Code", asset.get("adb_code"))),
                    "",
                    int(quantity),
                    0,
                    0,
                    "Calculated from effective room-type and bundle connections.",
                ]
            )
        category_index = ROOM_ASSET_DETAIL_HEADERS.index("Category")
        asset_index = ROOM_ASSET_DETAIL_HEADERS.index("Asset ID")
        record_type_index = ROOM_ASSET_DETAIL_HEADERS.index("Record Type")
        room_rows.sort(
            key=lambda row: (
                _text(row[category_index]).casefold(),
                _text(row[asset_index]).casefold(),
                _text(row[record_type_index]).casefold(),
            )
        )
        rows.extend(room_rows)
    return rows


def export_room_asset_detail_xlsx(
    path,
    room_types,
    assets_by_id=None,
    asset_categories_by_id=None,
    asset_bundles=None,
) -> tuple[str, int]:
    """Write the long-table room asset detail workbook."""

    detail_rows = room_asset_detail_rows(
        room_types,
        assets_by_id,
        asset_categories_by_id,
        asset_bundles,
    )
    destination = write_xlsx(
        path,
        [
            WorksheetSpec(
                name="Room Asset Detail",
                rows=[ROOM_ASSET_DETAIL_HEADERS, *detail_rows],
                header_rows={0},
                freeze_row=1,
                auto_filter_row=0,
            )
        ],
    )
    return destination, len(detail_rows)
