"""Excel and PDF exports for room-type asset details."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
)

from asset_bundles import (
    resolve_room_type_asset_connections,
    room_asset_source_labels,
)
from asset_ports import (
    asset_input_ports,
    asset_output_ports,
    is_connection_asset,
    room_asset_port_summary,
)
from project_summary_report import (
    _p as _project_report_paragraph,
    _styles as _project_report_styles,
    _table as _project_report_table,
)
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
    "Bundle",
    "Qty per room",
    "Inputs per device",
    "Outputs per device",
    "Connected to device",
    "Network Port Detail",
    "Final network port total",
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


def _connected_to_devices(summary: Mapping) -> dict[str, str]:
    """Return effective upstream device IDs for each receiving asset."""

    upstream_by_target: dict[str, list[str]] = {}
    for demand in summary.get("network_demands", []) or []:
        if not isinstance(demand, Mapping):
            continue
        for connection in demand.get("asset_connections", []) or []:
            if not isinstance(connection, Mapping):
                continue
            source_id = _text(connection.get("from_asset_id"))
            target_id = _text(connection.get("to_asset_id"))
            if not source_id or not target_id:
                continue
            sources = upstream_by_target.setdefault(target_id, [])
            if source_id not in sources:
                sources.append(source_id)
    return {
        target_id: ", ".join(source_ids)
        for target_id, source_ids in upstream_by_target.items()
    }


def room_asset_detail_rows(
    room_types,
    assets_by_id=None,
    asset_categories_by_id=None,
    asset_bundles=None,
    include_connection_assets=True,
) -> list[list[object]]:
    """Flatten assigned assets and calculated connection cables into rows."""

    assets_by_id = dict(assets_by_id or {})
    asset_categories_by_id = dict(asset_categories_by_id or {})
    rows: list[list[object]] = []
    for room_type in room_types or []:
        if not isinstance(room_type, Mapping):
            continue
        assignments = _room_assignments(room_type)
        if not include_connection_assets:
            assignments = [
                assignment
                for assignment in assignments
                if not is_connection_asset(
                    assets_by_id.get(
                        _text(
                            assignment.get(
                                "asset_id",
                                assignment.get("id"),
                            )
                        ),
                        {},
                    )
                )
            ]
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
        connected_to = _connected_to_devices(summary)

        room_type_id = _text(room_type.get("id"))
        room_type_name = _text(room_type.get("name"))
        scenario_group = _text(room_type.get("scenario_group"))
        source_labels = room_asset_source_labels(
            dict(room_type),
            asset_bundles,
        )
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
                    source_labels.get(asset_id, "Manual"),
                    _positive_int(assignment.get("qty")),
                    asset_input_ports(asset),
                    asset_output_ports(asset),
                    connected_to.get(asset_id, ""),
                    " ".join(port_notes.get(asset_id, [])),
                    final_counts.get(asset_id, 0),
                ]
            )
        for asset_id, quantity in (
            summary.get("connection_assets", {}).items()
            if include_connection_assets
            else ()
        ):
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
                    "",
                    int(quantity),
                    0,
                    0,
                    "",
                    "Calculated from effective room-type and bundle connections.",
                    0,
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


def categorised_room_asset_breakdowns(
    room_types,
    assets_by_id=None,
    asset_categories_by_id=None,
    asset_bundles=None,
    include_connection_assets=True,
) -> list[dict]:
    """Return room details in the project report's categorised table shape."""

    assets_by_id = dict(assets_by_id or {})
    asset_categories_by_id = dict(asset_categories_by_id or {})
    category_order = {
        str(category_id or "").strip(): index
        for index, category_id in enumerate(asset_categories_by_id)
    }
    header_index = {
        header: ROOM_ASSET_DETAIL_HEADERS.index(header)
        for header in ROOM_ASSET_DETAIL_HEADERS
    }
    result = []
    for room_type in room_types or []:
        if not isinstance(room_type, Mapping):
            continue
        detail_rows = room_asset_detail_rows(
            [room_type],
            assets_by_id,
            asset_categories_by_id,
            asset_bundles,
            include_connection_assets,
        )
        assets = []
        for detail_row in detail_rows:
            asset_id = _text(detail_row[header_index["Asset ID"]])
            asset = assets_by_id.get(asset_id, {}) or {}
            category_id = _text(detail_row[header_index["Category ID"]])
            quantity = max(
                0,
                int(detail_row[header_index["Qty per room"]] or 0),
            )
            inputs_per_device = max(
                0,
                int(detail_row[header_index["Inputs per device"]] or 0),
            )
            outputs_per_device = max(
                0,
                int(detail_row[header_index["Outputs per device"]] or 0),
            )
            final_network_ports = max(
                0,
                int(
                    detail_row[
                        header_index["Final network port total"]
                    ]
                    or 0
                ),
            )
            manufacturer = _text(asset.get("manufacturer"))
            model = _text(asset.get("model"))
            record_type = detail_row[header_index["Record Type"]]
            assets.append(
                {
                    "asset_id": asset_id,
                    "asset_name": _text(
                        detail_row[header_index["Description"]]
                    ),
                    "category_id": category_id,
                    "category_name": _text(
                        detail_row[header_index["Category"]]
                    )
                    or "Uncategorised",
                    "category_order": category_order.get(
                        category_id,
                        len(category_order),
                    ),
                    "adb_code": _text(detail_row[header_index["ADB Code"]]),
                    "group": _text(detail_row[header_index["Group"]])
                    or (
                        "Calculated connection cable"
                        if record_type == "Calculated connection cable"
                        else ""
                    ),
                    "make_model": " ".join(
                        part for part in (manufacturer, model) if part
                    ),
                    "bundle": _text(
                        detail_row[header_index["Bundle"]]
                    ),
                    "qty_per_room": quantity,
                    "inputs_per_device": inputs_per_device,
                    "outputs_per_device": outputs_per_device,
                    "connected_to_device": _text(
                        detail_row[header_index["Connected to device"]]
                    ),
                    "final_network_ports": final_network_ports,
                    "asset_subtotal": quantity,
                    "record_type": (
                        "connection_cable"
                        if record_type == "Calculated connection cable"
                        else "assigned_asset"
                    ),
                }
            )
        assets.sort(
            key=lambda row: (
                row["category_order"],
                row["category_name"].casefold(),
                row["asset_id"].casefold(),
            )
        )
        result.append(
            {
                "room_type_id": _text(room_type.get("id")),
                "room_name": _text(room_type.get("name"))
                or _text(room_type.get("id"))
                or "Unnamed room type",
                "scenario_group": _text(room_type.get("scenario_group")),
                "placed_rooms": 1,
                "assets": assets,
                "assets_per_room": sum(
                    row["qty_per_room"] for row in assets
                ),
                "final_network_ports_per_room": sum(
                    row["final_network_ports"] for row in assets
                ),
                "asset_total": sum(row["asset_subtotal"] for row in assets),
            }
        )
    return result


_CATEGORISED_BREAKDOWN_HEADERS = (
    "Asset ID",
    "Description",
    "Bundle",
    "ADB code",
    "Grouping",
    "Make / model",
    "Qty per room",
    "Inputs per device",
    "Outputs per device",
    "Connected to device",
    "Asset total",
    "Final network port total",
)


def _categorised_breakdown_worksheet(room_breakdowns) -> WorksheetSpec:
    rows = []
    title_rows = set()
    section_rows = set()
    header_rows = set()
    for room_index, room in enumerate(room_breakdowns):
        if room_index:
            rows.append([])
        title_rows.add(len(rows))
        room_label = (
            f"{room['room_type_id']} - {room['room_name']}"
            if room["room_type_id"]
            else room["room_name"]
        )
        rows.append([room_label])
        if room.get("scenario_group"):
            rows.append(["Scenario group", room["scenario_group"]])
        header_rows.add(len(rows))
        rows.append(list(_CATEGORISED_BREAKDOWN_HEADERS))

        current_category_id = object()
        for asset in room["assets"]:
            if asset["category_id"] != current_category_id:
                section_rows.add(len(rows))
                rows.append([f"Category: {asset['category_name']}"])
                current_category_id = asset["category_id"]
            rows.append(
                [
                    asset["asset_id"],
                    asset["asset_name"],
                    asset["bundle"],
                    asset["adb_code"],
                    asset["group"],
                    asset["make_model"],
                    asset["qty_per_room"],
                    asset["inputs_per_device"],
                    asset["outputs_per_device"],
                    asset["connected_to_device"],
                    asset["asset_subtotal"],
                    asset["final_network_ports"],
                ]
            )
        if not room["assets"]:
            rows.append(["No assets are assigned to this room type."])
        header_rows.add(len(rows))
        rows.append(
            [
                "Total",
                "",
                "",
                "",
                "",
                "",
                room["assets_per_room"],
                "",
                "",
                "",
                room["asset_total"],
                room["final_network_ports_per_room"],
            ]
        )
    return WorksheetSpec(
        name="Categorised Breakdown",
        rows=rows,
        title_rows=title_rows,
        section_rows=section_rows,
        header_rows=header_rows,
    )


def export_room_asset_detail_xlsx(
    path,
    room_types,
    assets_by_id=None,
    asset_categories_by_id=None,
    asset_bundles=None,
    include_connection_assets=True,
) -> tuple[str, int]:
    """Write the long-table room asset detail workbook."""

    detail_rows = room_asset_detail_rows(
        room_types,
        assets_by_id,
        asset_categories_by_id,
        asset_bundles,
        include_connection_assets,
    )
    room_breakdowns = categorised_room_asset_breakdowns(
        room_types,
        assets_by_id,
        asset_categories_by_id,
        asset_bundles,
        include_connection_assets,
    )
    destination = write_xlsx(
        path,
        [
            _categorised_breakdown_worksheet(room_breakdowns),
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


def _pdf_footer(canvas, document) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#5b6573"))
    canvas.drawString(
        document.leftMargin,
        9 * mm,
        "CableRouteResolver room type asset breakdown",
    )
    canvas.drawRightString(
        document.pagesize[0] - document.rightMargin,
        9 * mm,
        f"Page {document.page}",
    )
    canvas.restoreState()


def _pdf_room_asset_table_rows(room: Mapping, styles):
    paragraph = _project_report_paragraph
    rows = [[
        paragraph("Asset ID", styles["header"]),
        paragraph("Description", styles["header"]),
        paragraph("ADB code", styles["header"]),
        paragraph("Grouping", styles["header"]),
        paragraph("Make / model", styles["header"]),
        paragraph("Qty per room", styles["header"]),
        paragraph("Inputs per device", styles["header"]),
        paragraph("Outputs per device", styles["header"]),
        paragraph("Connected to device", styles["header"]),
        paragraph("Asset total", styles["header"]),
        paragraph("Final network port total", styles["header"]),
    ]]
    group_rows = []
    current_category_id = object()
    for asset in room["assets"]:
        category_id = asset["category_id"]
        if category_id != current_category_id:
            group_rows.append(len(rows))
            rows.append(
                [
                    paragraph(
                        f"Category: {asset['category_name']}",
                        styles["group"],
                    )
                ]
                + [paragraph("", styles["small"])] * 10
            )
            current_category_id = category_id
        rows.append([
            paragraph(asset["asset_id"], styles["small"]),
            paragraph(asset["asset_name"], styles["small"]),
            paragraph(asset["adb_code"], styles["small"]),
            paragraph(asset["group"], styles["small"]),
            paragraph(asset["make_model"], styles["small"]),
            paragraph(asset["qty_per_room"], styles["small"]),
            paragraph(asset["inputs_per_device"], styles["small"]),
            paragraph(asset["outputs_per_device"], styles["small"]),
            paragraph(asset["connected_to_device"], styles["small"]),
            paragraph(asset["asset_subtotal"], styles["small"]),
            paragraph(asset["final_network_ports"], styles["small"]),
        ])
    rows.append([
        paragraph("Total", styles["header"]),
        paragraph("", styles["header"]),
        paragraph("", styles["header"]),
        paragraph("", styles["header"]),
        paragraph("", styles["header"]),
        paragraph(room["assets_per_room"], styles["header"]),
        paragraph("", styles["header"]),
        paragraph("", styles["header"]),
        paragraph("", styles["header"]),
        paragraph(room["asset_total"], styles["header"]),
        paragraph(room["final_network_ports_per_room"], styles["header"]),
    ])
    return rows, group_rows


def export_room_asset_detail_pdf(
    path,
    room_types,
    assets_by_id=None,
    asset_categories_by_id=None,
    asset_bundles=None,
    include_connection_assets=True,
) -> tuple[str, int]:
    """Write a room-by-room PDF asset breakdown."""

    selected_room_types = [
        dict(room_type)
        for room_type in room_types or []
        if isinstance(room_type, Mapping)
    ]
    room_breakdowns = categorised_room_asset_breakdowns(
        selected_room_types,
        assets_by_id,
        asset_categories_by_id,
        asset_bundles,
        include_connection_assets,
    )
    destination = Path(path)
    if destination.suffix.casefold() != ".pdf":
        destination = destination.with_suffix(".pdf")
    destination.parent.mkdir(parents=True, exist_ok=True)

    styles = _project_report_styles()
    document = SimpleDocTemplate(
        str(destination),
        pagesize=landscape(A4),
        leftMargin=14 * mm,
        rightMargin=14 * mm,
        topMargin=15 * mm,
        bottomMargin=18 * mm,
        title="Room Type Asset Breakdown",
        author="CableRouteResolver",
    )
    styles["_max_table_width"] = document.width
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    story = [
        Paragraph("Room Type Asset Breakdown", styles["title"]),
        Paragraph(
            f"Generated: {generated} | Selected room types: "
            f"{len(room_breakdowns)}",
            styles["body"],
        ),
        Spacer(1, 4 * mm),
    ]

    for room_index, room in enumerate(room_breakdowns):
        if room_index:
            story.append(PageBreak())
        room_label = (
            f"{escape(room['room_type_id'])} - {escape(room['room_name'])}"
            if room["room_type_id"]
            else escape(room["room_name"])
        )
        story.append(Paragraph(room_label, styles["h2"]))
        story.append(
            Paragraph(
                "Scenario group: "
                + escape(room.get("scenario_group") or "Not assigned"),
                styles["body"],
            )
        )
        story.append(Spacer(1, 3 * mm))
        if not room["assets"]:
            story.append(
                Paragraph(
                    "No assets are assigned to this room type.",
                    styles["body"],
                )
            )
        table_rows, group_rows = _pdf_room_asset_table_rows(room, styles)
        story.append(
            _project_report_table(
                table_rows,
                [
                    23 * mm,
                    43 * mm,
                    21 * mm,
                    22 * mm,
                    29 * mm,
                    16 * mm,
                    18 * mm,
                    18 * mm,
                    27 * mm,
                    17 * mm,
                    29 * mm,
                ],
                styles,
                numeric_columns=(5, 6, 7, 9, 10),
                total_rows=(-1,),
                group_rows=group_rows,
            )
        )

    document.build(
        story,
        onFirstPage=_pdf_footer,
        onLaterPages=_pdf_footer,
    )
    return str(destination), sum(
        len(room["assets"]) for room in room_breakdowns
    )
