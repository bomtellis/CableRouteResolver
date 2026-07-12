from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.pagesizes import A3, A4, LETTER, LEGAL, landscape, portrait
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents

from models import JsonStore

PROJECT_SUMMARY_SECTIONS = [
    ("overall_summary", "Overall summary"),
    ("room_summary", "Room type summary"),
    ("room_details", "Room asset details"),
    ("use_cases", "Use cases"),
    ("network_summary", "Network summary, topology and layers"),
    ("network_equipment", "Network equipment required"),
    ("power_draw", "Power draw and theoretical kWh"),
    ("rack_power_fibre", "Rack, power and fibre requirements"),
]

PROJECT_SUMMARY_PAPER_SIZES = {
    "A4": A4,
    "A3": A3,
    "Letter": LETTER,
    "Legal": LEGAL,
}


class ProjectSummaryDocTemplate(SimpleDocTemplate):
    def afterFlowable(self, flowable):
        if not isinstance(flowable, Paragraph):
            return
        style_name = getattr(flowable.style, "name", "")
        if style_name == "ProjectReportH1":
            self.notify("TOCEntry", (0, flowable.getPlainText(), self.page))


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _natural_key(value):
    return tuple(
        int(part) if part.isdigit() else part
        for part in re.split(r"(\d+)", _text(value).casefold())
    )


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return int(default)


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _project_name(data: Mapping | None) -> str:
    project = data.get("project", {}) if isinstance(data, Mapping) else {}
    return _text(project.get("name")) or "Cable Routing Project"


def _asset_ports(asset: Mapping) -> int:
    asset = asset or {}
    return max(
        0,
        _int(
            asset.get(
                "data_points",
                asset.get("data_points_each", asset.get("cables", 1)),
            ),
            0,
        ),
    )


def _asset_label(asset_id: str, asset: Mapping | None) -> str:
    asset = asset or {}
    name = _text(asset.get("name")) or asset_id
    return f"{asset_id} - {name}" if asset_id and name != asset_id else name


def _asset_make_model(asset: Mapping | None) -> str:
    asset = asset or {}
    parts = [_text(asset.get("manufacturer")), _text(asset.get("model"))]
    return " ".join(part for part in parts if part)


def _page_footer(canvas, document) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#555555"))
    canvas.drawString(document.leftMargin, 12 * mm, "CableRouteResolver project report")
    revision_label = getattr(document, "revision_label", "")
    if revision_label:
        canvas.drawCentredString(document.pagesize[0] / 2, 12 * mm, revision_label)
    canvas.drawRightString(
        document.pagesize[0] - document.rightMargin,
        12 * mm,
        f"Page {document.page}",
    )
    canvas.restoreState()


def _styles():
    styles = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "ProjectReportTitle",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=22,
            spaceAfter=10,
            textColor=colors.HexColor("#1f2933"),
        ),
        "h1": ParagraphStyle(
            "ProjectReportH1",
            parent=styles["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=15,
            leading=18,
            spaceBefore=6,
            spaceAfter=8,
            textColor=colors.HexColor("#1f2933"),
        ),
        "subtitle": ParagraphStyle(
            "ProjectReportSubtitle",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=15,
            spaceAfter=8,
            textColor=colors.HexColor("#243b53"),
        ),
        "h2": ParagraphStyle(
            "ProjectReportH2",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=11,
            leading=14,
            spaceBefore=8,
            spaceAfter=5,
            textColor=colors.HexColor("#243b53"),
        ),
        "body": ParagraphStyle(
            "ProjectReportBody",
            parent=styles["Normal"],
            fontSize=8,
            leading=10,
        ),
        "small": ParagraphStyle(
            "ProjectReportSmall",
            parent=styles["Normal"],
            fontSize=7,
            leading=8.5,
        ),
        "header": ParagraphStyle(
            "ProjectReportTableHeader",
            parent=styles["Normal"],
            fontName="Helvetica-Bold",
            fontSize=7.2,
            leading=8.5,
            alignment=1,
            textColor=colors.white,
        ),
        "toc0": ParagraphStyle(
            "ProjectReportTOC0",
            parent=styles["Normal"],
            fontName="Helvetica-Bold",
            fontSize=9,
            leading=12,
            leftIndent=0,
            firstLineIndent=0,
            spaceBefore=2,
        ),
        "toc1": ParagraphStyle(
            "ProjectReportTOC1",
            parent=styles["Normal"],
            fontSize=8,
            leading=10,
            leftIndent=8 * mm,
            firstLineIndent=0,
            textColor=colors.HexColor("#425466"),
        ),
    }


def _p(value, style):
    text = _text(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return Paragraph(text or "-", style)


def _table(rows, widths, styles, *, numeric_columns: Iterable[int] = (), total_rows: Iterable[int] = ()):
    max_width = styles.get("_max_table_width") if isinstance(styles, dict) else None
    if max_width:
        total_width = sum(widths)
        if total_width > max_width:
            scale = max_width / total_width
            widths = [width * scale for width in widths]
    table = Table(rows, colWidths=widths, repeatRows=1, hAlign="LEFT")
    commands = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2933")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    for column in numeric_columns:
        commands.append(("ALIGN", (column, 1), (column, -1), "RIGHT"))
    row_count = len(rows)
    for row in total_rows:
        row_index = row_count + row if row < 0 else row
        if 0 <= row_index < row_count:
            commands.extend(
                [
                    ("BACKGROUND", (0, row_index), (-1, row_index), colors.HexColor("#1f2933")),
                    ("TEXTCOLOR", (0, row_index), (-1, row_index), colors.white),
                    ("FONTNAME", (0, row_index), (-1, row_index), "Helvetica-Bold"),
                ]
            )
    table.setStyle(TableStyle(commands))
    return table


def _summary_cards(items, styles):
    rows = [[_p("Metric", styles["header"]), _p("Value", styles["header"])]]
    for label, value in items:
        rows.append([_p(label, styles["body"]), _p(value, styles["body"])])
    return _table(rows, [88 * mm, 42 * mm], styles)


def _room_type_sections(store: JsonStore):
    data = store.data
    assets_by_id = {
        _text(asset.get("id")): asset
        for asset in data.get("assets", [])
        if isinstance(asset, dict) and _text(asset.get("id"))
    }
    placed_counts = store.placed_room_type_counts()
    room_rows = []
    room_totals = []
    total_assets = 0
    total_ports = 0

    for room_type in data.get("room_types", []) or []:
        if not isinstance(room_type, dict):
            continue
        room_type_id = _text(room_type.get("id"))
        room_name = _text(room_type.get("name")) or room_type_id
        placed_rooms = max(0, placed_counts.get(room_type_id, 0))
        room_asset_total = 0
        room_port_total = 0
        room_assets_per_room = 0
        room_ports_per_room = 0
        rows = []
        for asset_row in store.room_type_asset_rows(room_type):
            asset_id = _text(asset_row.get("asset_id"))
            if not asset_id:
                continue
            asset = assets_by_id.get(asset_id, {})
            qty_per_room = max(1, _int(asset_row.get("qty"), 1))
            ports_each = _asset_ports(asset)
            asset_subtotal = placed_rooms * qty_per_room
            port_subtotal = asset_subtotal * ports_each
            port_per_room = qty_per_room * ports_each
            room_assets_per_room += qty_per_room
            room_ports_per_room += port_per_room
            room_asset_total += asset_subtotal
            room_port_total += port_subtotal
            rows.append(
                {
                    "asset_id": asset_id,
                    "asset_name": _text(asset.get("name")) or asset_id,
                    "asset": _asset_label(asset_id, asset),
                    "adb_code": _text(asset.get("ADB_Code", asset.get("adb_code"))),
                    "group": _text(asset.get("Group", asset.get("group"))),
                    "make_model": _asset_make_model(asset),
                    "qty_per_room": qty_per_room,
                    "ports_each": ports_each,
                    "port_per_room": port_per_room,
                    "asset_subtotal": asset_subtotal,
                    "port_subtotal": port_subtotal,
                }
            )
        rows.sort(key=lambda row: _natural_key(row.get("asset_id")))
        total_assets += room_asset_total
        total_ports += room_port_total
        room_totals.append(
            {
                "room_type_id": room_type_id,
                "room_name": room_name,
                "placed_rooms": placed_rooms,
                "assets_per_room": room_assets_per_room,
                "ports_per_room": room_ports_per_room,
                "asset_total": room_asset_total,
                "port_total": room_port_total,
            }
        )
        room_rows.append(
            {
                "room_type_id": room_type_id,
                "room_name": room_name,
                "placed_rooms": placed_rooms,
                "assets": rows,
                "assets_per_room": room_assets_per_room,
                "ports_per_room": room_ports_per_room,
                "asset_total": room_asset_total,
                "port_total": room_port_total,
            }
        )

    return {
        "assets_by_id": assets_by_id,
        "room_rows": room_rows,
        "room_totals": room_totals,
        "total_assets": total_assets,
        "total_ports": total_ports,
    }


def _scenario_rows(store: JsonStore, assets_by_id: Mapping[str, Mapping]):
    rows = []
    for scenario in store.scenario_definitions():
        if not isinstance(scenario, dict) or not bool(scenario.get("enabled", True)):
            continue
        room_type_ids = []
        for group in scenario.get("room_groups", []) or []:
            room_type_ids.extend(store.room_type_ids_for_scenario_group(group))
        asset_ids = []
        for group in scenario.get("asset_groups", []) or []:
            asset_ids.extend(store.asset_ids_for_scenario_group(group))
        replacement_asset_ids = []
        for group in scenario.get("replacement_asset_groups", []) or []:
            replacement_asset_ids.extend(store.asset_ids_for_scenario_group(group))
        rows.append(
            {
                "name": _text(scenario.get("name")) or "Scenario",
                "type": _text(scenario.get("scenario_type")) or "standard",
                "mode": _text(scenario.get("mode")) or "add",
                "qty": max(1, _int(scenario.get("qty"), 1)),
                "room_groups": list(scenario.get("room_groups", []) or []),
                "asset_groups": list(scenario.get("asset_groups", []) or []),
                "replacement_asset_groups": list(scenario.get("replacement_asset_groups", []) or []),
                "room_type_ids": sorted(set(room_type_ids), key=str.casefold),
                "asset_ids": sorted(set(asset_ids), key=_natural_key),
                "replacement_asset_ids": sorted(set(replacement_asset_ids), key=_natural_key),
                "notes": _text(scenario.get("notes")),
            }
        )
    return rows


def _network_asset_maps(data: Mapping):
    assets = {
        _text(asset.get("id")): asset
        for asset in data.get("network_assets", []) or []
        if isinstance(asset, dict) and _text(asset.get("id"))
    }
    instances = [
        instance
        for instance in data.get("network_asset_instances", []) or []
        if isinstance(instance, dict)
    ]
    return assets, instances


def _network_type_label(value: str) -> str:
    return (_text(value) or "other").replace("_", " ").title()


def _rack_units_for_instance(instance: Mapping, asset: Mapping) -> int:
    stack_members = max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1
    rack_units = _int(asset.get("rack_units"))
    if _text(asset.get("asset_type")) == "network_switch":
        rack_units = max(1, _int(asset.get("switch_rack_unit_allowance"), rack_units or 1)) * stack_members
    return max(0, rack_units)


def _network_summary(data: Mapping):
    assets, instances = _network_asset_maps(data)
    settings = data.get("network_settings", {}) if isinstance(data.get("network_settings"), dict) else {}
    design_summary = data.get("network_design_summary", {}) if isinstance(data.get("network_design_summary"), dict) else {}
    locations = {
        _text(location.get("name") or location.get("id")): location
        for location in data.get("locations", []) or []
        if isinstance(location, dict) and _text(location.get("name") or location.get("id"))
    }

    def polan_placeholder_rack_capacity(location_name: str, rack_name: str) -> int | None:
        location_name = _text(location_name)
        rack_name = _text(rack_name)
        location = locations.get(location_name, {})
        is_polan = _text(location.get("kind")).casefold() == "polan" or location_name.startswith("AUTO-POLAN-")
        if is_polan and rack_name.startswith("AUTO-RACK-"):
            return 1
        return None

    equipment = {}
    role_counts = Counter()
    type_counts = Counter()
    rack_usage = defaultdict(lambda: {"used_u": 0, "items": 0, "capacity_u": 0, "floor": "", "location": ""})
    pdu_count = 0
    ups_count = 0
    for instance in instances:
        asset_id = _text(instance.get("asset_id"))
        asset = assets.get(asset_id, {})
        asset_type = _text(asset.get("asset_type")) or _text(instance.get("design_role")) or "other"
        role = _text(instance.get("design_role")) or asset_type or "unspecified"
        role_counts[role] += 1
        type_counts[asset_type] += 1
        if asset_type == "pdu":
            pdu_count += 1
        if asset_type == "ups":
            ups_count += 1
        key = (asset_id, _text(asset.get("name")) or asset_id, asset_type, _text(asset.get("manufacturer")), _text(asset.get("model")))
        equipment.setdefault(key, {"qty": 0, "rack_units": 0})
        equipment[key]["qty"] += 1
        equipment[key]["rack_units"] += _rack_units_for_instance(instance, asset)
        rack_name = _text(instance.get("rack_name"))
        if rack_name:
            row = rack_usage[rack_name]
            row["used_u"] += _rack_units_for_instance(instance, asset)
            row["items"] += 1
            capacity_u = polan_placeholder_rack_capacity(instance.get("location_name"), rack_name)
            if capacity_u is None:
                capacity_u = _int(instance.get("rack_size_u"), _int(settings.get("default_rack_size_u"), 42))
            row["capacity_u"] = max(row["capacity_u"], capacity_u)
            row["floor"] = instance.get("floor", row["floor"])
            row["location"] = _text(instance.get("location_name")) or row["location"]

    for rack in data.get("network_racks", []) or []:
        if not isinstance(rack, dict):
            continue
        rack_name = _text(rack.get("name") or rack.get("id"))
        if not rack_name:
            continue
        row = rack_usage[rack_name]
        capacity_u = polan_placeholder_rack_capacity(rack.get("location_name"), rack_name)
        if capacity_u is None:
            capacity_u = _int(rack.get("capacity_u"), _int(settings.get("default_rack_size_u"), 42))
        row["capacity_u"] = max(row["capacity_u"], capacity_u)
        row["floor"] = rack.get("floor", row["floor"])
        row["location"] = _text(rack.get("location_name")) or row["location"]

    layer_links = Counter()
    instance_role = {
        _text(instance.get("id")): _text(instance.get("design_role"))
        or _text(assets.get(_text(instance.get("asset_id")), {}).get("asset_type"))
        or "unspecified"
        for instance in instances
    }
    for connection in data.get("network_connections", []) or []:
        if not isinstance(connection, dict):
            continue
        left = instance_role.get(_text(connection.get("from_instance_id")), "unknown")
        right = instance_role.get(_text(connection.get("to_instance_id")), "unknown")
        medium = _text(connection.get("medium")) or "link"
        layer_links[(left, right, medium)] += 1

    patch_leads = Counter()
    for lead in data.get("network_patch_leads", []) or []:
        if isinstance(lead, dict):
            patch_leads[_text(lead.get("lead_type") or lead.get("medium") or lead.get("cable_type") or "patch lead")] += 1

    fibre_cables = data.get("network_fibre_cables", []) or []
    fibre_splices = data.get("network_fibre_splices", []) or []
    fibre_nodes = data.get("network_fibre_nodes", []) or []

    return {
        "settings": settings,
        "design_summary": design_summary,
        "role_counts": role_counts,
        "type_counts": type_counts,
        "equipment": equipment,
        "rack_usage": dict(rack_usage),
        "pdu_count": pdu_count,
        "ups_count": ups_count,
        "layer_links": layer_links,
        "patch_leads": patch_leads,
        "fibre_cables": fibre_cables,
        "fibre_splices": fibre_splices,
        "fibre_nodes": fibre_nodes,
    }


def _topology_group(role: str) -> str:
    role_key = _text(role).casefold()
    role_key = role_key.replace("-", "_").replace(" ", "_")
    if role_key in {"external_network", "edge_router", "router", "internet", "wan"}:
        return "edge"
    if role_key in {"firewall", "core", "core_switch", "core_router"}:
        return "core"
    if role_key in {"distribution", "distribution_switch", "aggregation", "aggregation_switch"}:
        return "distribution"
    if role_key in {"olt", "olt_primary", "olt_secondary", "optical_line_terminal"}:
        return "olt"
    if "splitter" in role_key:
        return "splitter"
    if role_key in {"ont", "access_switch", "wireless_access_point", "ap", "optical_network_terminal"}:
        return "access"
    if role_key in {"endpoint", "client", "device", "field_device"}:
        return "endpoint"
    if role_key in {"pdu", "rack_pdu", "ups", "rack_ups", "power_device", "power"}:
        return "power"
    return "other"


def _topology_group_label(group: str) -> str:
    return {
        "edge": "External / Edge",
        "core": "Core",
        "distribution": "Distribution",
        "olt": "OLT",
        "splitter": "Splitters",
        "access": "ONT / Access",
        "endpoint": "Endpoints",
        "power": "Power",
        "other": "Other",
    }.get(group, _network_type_label(group))


def _topology_diagram(network: Mapping, styles):
    group_order = ["edge", "core", "distribution", "olt", "splitter", "access", "endpoint", "other"]
    group_counts = Counter()
    for role, count in network["role_counts"].items():
        group_counts[_topology_group(role)] += count

    edge_media = defaultdict(Counter)
    linked_groups = set()
    for (left, right, medium), count in network["layer_links"].items():
        left_group = _topology_group(left)
        right_group = _topology_group(right)
        if left_group == right_group:
            continue
        edge_media[(left_group, right_group)][medium] += count
        group_counts.setdefault(left_group, 0)
        group_counts.setdefault(right_group, 0)
        linked_groups.add(left_group)
        linked_groups.add(right_group)

    groups = [group for group in group_order if group in linked_groups]
    if not groups:
        groups = [group for group in group_order if group in group_counts and group != "power"]
    if not groups:
        return Paragraph("No topology links configured.", styles["body"])

    width = min(float(styles.get("_max_table_width") or 250 * mm), 250 * mm)
    height = 48 * mm
    margin = 6 * mm
    gap = 7 * mm
    block_count = len(groups)
    block_width = max(22 * mm, (width - (margin * 2) - (gap * (block_count - 1))) / block_count)
    if block_width > 38 * mm:
        block_width = 38 * mm
        gap = (width - (margin * 2) - (block_width * block_count)) / max(1, block_count - 1)
    block_height = 18 * mm
    y = 17 * mm
    drawing = Drawing(width, height)
    fill = colors.HexColor("#e8f1fb")
    stroke = colors.HexColor("#315f8f")
    line_color = colors.HexColor("#5b677a")
    centers = {}

    for index, group in enumerate(groups):
        x = margin + index * (block_width + gap)
        centers[group] = (x + block_width / 2, y + block_height / 2)
        drawing.add(Rect(x, y, block_width, block_height, rx=2, ry=2, fillColor=fill, strokeColor=stroke, strokeWidth=0.8))
        drawing.add(String(x + block_width / 2, y + 10.5 * mm, _topology_group_label(group), textAnchor="middle", fontName="Helvetica-Bold", fontSize=6.5, fillColor=colors.HexColor("#1f2933")))
        drawing.add(String(x + block_width / 2, y + 5.2 * mm, f"{group_counts[group]} items", textAnchor="middle", fontName="Helvetica", fontSize=6, fillColor=colors.HexColor("#425466")))

    group_index = {group: index for index, group in enumerate(groups)}
    display_edges = defaultdict(Counter)
    for (left, right), media_counts in edge_media.items():
        if left not in group_index or right not in group_index:
            continue
        left_index = group_index[left]
        right_index = group_index[right]
        if left_index == right_index:
            continue
        if abs(left_index - right_index) != 1:
            continue
        ordered_pair = (left, right) if left_index < right_index else (right, left)
        display_edges[ordered_pair].update(media_counts)

    for left, right in sorted(
        display_edges,
        key=lambda pair: (group_index[pair[0]], group_index[pair[1]]),
    ):
        x1, y1 = centers[left]
        x2, y2 = centers[right]
        if x2 < x1:
            x1, x2 = x2, x1
        start_x = x1 + block_width / 2
        end_x = x2 - block_width / 2
        if end_x <= start_x:
            continue
        media = display_edges.get((left, right)) or {}
        if not media:
            continue
        drawing.add(Line(start_x, y1, end_x - 2 * mm, y2, strokeColor=line_color, strokeWidth=0.7))
        drawing.add(Polygon([end_x, y2, end_x - 2 * mm, y2 + 1.3 * mm, end_x - 2 * mm, y2 - 1.3 * mm], fillColor=line_color, strokeColor=line_color))
        medium, count = sorted(media.items(), key=lambda item: (-item[1], _text(item[0]).casefold()))[0]
        extra_types = max(0, len(media) - 1)
        label = f"{_network_type_label(medium)} x{count}"
        if extra_types:
            label = f"{label} +{extra_types}"
        label_y = y1 + 7.2 * mm
        label_font = "Helvetica"
        label_size = 5.1
        available_label_width = max(0.0, end_x - start_x - 1.5 * mm)
        if stringWidth(label, label_font, label_size) <= available_label_width:
            drawing.add(String((start_x + end_x) / 2, label_y, label, textAnchor="middle", fontName=label_font, fontSize=label_size, fillColor=colors.HexColor("#425466")))

    return drawing


def _instance_power_w(instance: Mapping, asset: Mapping) -> float:
    power = _float(instance.get("power_input_w"), -1.0)
    if power < 0:
        power = _float(asset.get("power_input_w"), 0.0)
    return max(0.0, power)


def _instance_stack_members(instance: Mapping) -> int:
    return max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1


def _instance_poe_budget_w(instance: Mapping, asset: Mapping) -> float:
    return max(0.0, _float(asset.get("poe_budget_w"), 0.0)) * _instance_stack_members(instance)


def _power_draw_summary(data: Mapping):
    assets, instances = _network_asset_maps(data)
    instances_by_id = {_text(instance.get("id")): instance for instance in instances if _text(instance.get("id"))}
    instance_roles = {
        instance_id: _text(instance.get("design_role"))
        or _text(assets.get(_text(instance.get("asset_id")), {}).get("asset_type"))
        or "unspecified"
        for instance_id, instance in instances_by_id.items()
    }
    instance_power = {}
    instance_poe_loads = Counter()
    instance_poe_assignment_counts = Counter()
    endpoint_assets_by_id = {
        _text(asset.get("id")): asset
        for asset in data.get("assets", []) or []
        if isinstance(asset, dict) and _text(asset.get("id"))
    }
    poe_loads_by_asset = {}
    for assignment in data.get("network_endpoint_assignments", []) or []:
        if not isinstance(assignment, dict):
            continue
        instance_id = _text(assignment.get("network_instance_id"))
        endpoint_asset_id = _text(assignment.get("endpoint_asset_id"))
        endpoint_asset = endpoint_assets_by_id.get(endpoint_asset_id, {})
        endpoint_asset_name = (
            _text(assignment.get("endpoint_asset_name"))
            or _text(endpoint_asset.get("name"))
            or endpoint_asset_id
            or "Unspecified endpoint asset"
        )
        poe_power_w = max(0.0, _float(assignment.get("poe_power_w"), 0.0))
        if instance_id:
            instance_poe_loads[instance_id] += poe_power_w
            instance_poe_assignment_counts[instance_id] += 1
        if poe_power_w > 0.0:
            key = (endpoint_asset_id, endpoint_asset_name)
            poe_loads_by_asset.setdefault(
                key,
                {
                    "qty": 0,
                    "poe_load_w": 0.0,
                    "unit_poe_values": [],
                },
            )
            poe_loads_by_asset[key]["qty"] += 1
            poe_loads_by_asset[key]["poe_load_w"] += poe_power_w
            poe_loads_by_asset[key]["unit_poe_values"].append(poe_power_w)

    by_layer = Counter()
    poe_by_layer = Counter()
    poe_budget_by_layer = Counter()
    by_equipment = {}
    base_total_w = 0.0
    poe_load_total_w = 0.0
    poe_budget_total_w = 0.0

    for instance_id, instance in instances_by_id.items():
        asset = assets.get(_text(instance.get("asset_id")), {})
        power_w = _instance_power_w(instance, asset)
        poe_load_w = max(0.0, float(instance_poe_loads.get(instance_id, 0.0)))
        poe_budget_w = _instance_poe_budget_w(instance, asset)
        non_poe_power_w = max(0.0, power_w - poe_budget_w)
        if power_w <= 0.0 and poe_load_w <= 0.0 and poe_budget_w <= 0.0:
            continue
        role = instance_roles.get(instance_id, "unspecified")
        group = _topology_group(role)
        by_layer[group] += power_w
        poe_by_layer[group] += poe_load_w
        poe_budget_by_layer[group] += poe_budget_w
        base_total_w += power_w
        poe_load_total_w += poe_load_w
        poe_budget_total_w += poe_budget_w
        key = (
            _text(asset.get("asset_type")) or role or "other",
            _text(instance.get("asset_id")),
            _text(asset.get("name")) or _text(instance.get("asset_id")),
            _text(asset.get("manufacturer")),
            _text(asset.get("model")),
        )
        by_equipment.setdefault(
            key,
            {
                "qty": 0,
                "power_w": 0.0,
                "poe_load_w": 0.0,
                "poe_assignment_count": 0,
                "poe_budget_w": 0.0,
                "minimum_w": 0.0,
                "maximum_w": 0.0,
                "unit_power_values": [],
                "unit_minimum_values": [],
                "unit_maximum_values": [],
            },
        )
        by_equipment[key]["qty"] += 1
        by_equipment[key]["power_w"] += power_w
        by_equipment[key]["poe_load_w"] += poe_load_w
        by_equipment[key]["poe_assignment_count"] += int(instance_poe_assignment_counts.get(instance_id, 0))
        by_equipment[key]["poe_budget_w"] += poe_budget_w
        by_equipment[key]["minimum_w"] += non_poe_power_w + poe_load_w
        by_equipment[key]["maximum_w"] += power_w
        by_equipment[key]["unit_power_values"].append(power_w)
        by_equipment[key]["unit_minimum_values"].append(non_poe_power_w + poe_load_w)
        by_equipment[key]["unit_maximum_values"].append(power_w)
        instance_power[instance_id] = power_w

    incoming_power_links = Counter()
    for link in data.get("network_power_connections", []) or []:
        if isinstance(link, dict):
            target_id = _text(link.get("to_instance_id"))
            if target_id:
                incoming_power_links[target_id] += 1

    flows = {}
    for link in data.get("network_power_connections", []) or []:
        if not isinstance(link, dict):
            continue
        source_id = _text(link.get("from_instance_id"))
        target_id = _text(link.get("to_instance_id"))
        if not source_id or not target_id:
            continue
        target_power = instance_power.get(target_id)
        if target_power is None:
            target_power = max(0.0, _float(link.get("load_w"), 0.0))
        else:
            target_power = target_power / max(1, incoming_power_links.get(target_id, 1))
        source_role = instance_roles.get(source_id, "power")
        target_role = instance_roles.get(target_id, "load")
        key = (_topology_group(source_role), _topology_group(target_role))
        flows.setdefault(key, {"links": 0, "load_w": 0.0})
        flows[key]["links"] += 1
        flows[key]["load_w"] += target_power

    return {
        "total_w": base_total_w,
        "base_total_w": base_total_w,
        "poe_load_total_w": poe_load_total_w,
        "poe_budget_total_w": poe_budget_total_w,
        "minimum_total_w": sum(row["minimum_w"] for row in by_equipment.values()),
        "maximum_total_w": base_total_w,
        "by_layer": by_layer,
        "poe_by_layer": poe_by_layer,
        "poe_budget_by_layer": poe_budget_by_layer,
        "by_equipment": by_equipment,
        "poe_loads_by_asset": poe_loads_by_asset,
        "flows": flows,
        "daily_kwh": base_total_w * 24.0 / 1000.0,
        "monthly_kwh": base_total_w * 24.0 * 30.0 / 1000.0,
        "annual_kwh": base_total_w * 24.0 * 365.0 / 1000.0,
        "minimum_daily_kwh": sum(row["minimum_w"] for row in by_equipment.values()) * 24.0 / 1000.0,
        "minimum_monthly_kwh": sum(row["minimum_w"] for row in by_equipment.values()) * 24.0 * 30.0 / 1000.0,
        "minimum_annual_kwh": sum(row["minimum_w"] for row in by_equipment.values()) * 24.0 * 365.0 / 1000.0,
    }


def _power_unit_draw_label(values: Iterable[float]) -> str:
    cleaned = sorted(max(0.0, float(value)) for value in values or [])
    if not cleaned:
        return "0.0"
    if abs(cleaned[0] - cleaned[-1]) < 0.05:
        return f"{cleaned[0]:.1f}"
    return f"{cleaned[0]:.1f} - {cleaned[-1]:.1f}"


def export_project_summary_pdf(
    data: Mapping,
    output_path: str | Path,
    *,
    source_path: str = "",
    sections: Iterable[str] | None = None,
    report_options: Mapping | None = None,
) -> Path:
    report_options = dict(report_options or {})
    selected_sections = set(sections or [])
    if not selected_sections:
        selected_sections = {section_id for section_id, _label in PROJECT_SUMMARY_SECTIONS}
    store = JsonStore(dict(data or {}))
    report_data = store.data
    room_report = _room_type_sections(store)
    scenarios = _scenario_rows(store, room_report["assets_by_id"])
    network = _network_summary(report_data)
    power_draw = _power_draw_summary(report_data)
    styles = _styles()
    report_title = _text(report_options.get("project_name")) or _project_name(report_data)
    paper_size_name = _text(report_options.get("paper_size")) or "A4"
    page_size = PROJECT_SUMMARY_PAPER_SIZES.get(paper_size_name, A4)
    orientation = (_text(report_options.get("orientation")) or "Landscape").casefold()
    page_size = portrait(page_size) if orientation == "portrait" else landscape(page_size)

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    document = ProjectSummaryDocTemplate(
        str(destination),
        pagesize=page_size,
        leftMargin=14 * mm,
        rightMargin=14 * mm,
        topMargin=15 * mm,
        bottomMargin=18 * mm,
        title=f"{report_title} room and network report",
        author="CableRouteResolver",
    )
    styles["_max_table_width"] = document.width

    generated_text = "Generated: " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    revision_number = _int(report_options.get("revision_number"), 0)
    revision_text = f"Revision: {revision_number}" if revision_number else "Revision: Not available"
    document.revision_label = revision_text if revision_number else ""
    story = [
        Spacer(1, 24 * mm),
        Paragraph(f"{report_title}", styles["title"]),
        Paragraph("Room, Use Case and Network Report", styles["subtitle"]),
        Spacer(1, 5 * mm),
        Paragraph(generated_text, styles["body"]),
        Paragraph(revision_text, styles["body"]),
    ]
    if source_path:
        story.append(Paragraph(f"Project file: {_text(source_path)}", styles["body"]))
    story.append(PageBreak())
    story.append(Paragraph("Table of Contents", styles["title"]))
    toc = TableOfContents()
    toc.levelStyles = [styles["toc0"], styles["toc1"]]
    story.append(toc)
    story.append(PageBreak())

    def start_section(title: str, *, page_break: bool = False) -> None:
        if page_break and story and not isinstance(story[-1], PageBreak):
            story.append(PageBreak())
        story.append(Paragraph(title, styles["h1"]))

    placed_rooms = sum(room["placed_rooms"] for room in room_report["room_totals"])
    if "overall_summary" in selected_sections:
        start_section("Overall Summary")
        story.append(
            _summary_cards(
                [
                    ("Room types configured", len(room_report["room_rows"])),
                    ("Placed rooms with room types", placed_rooms),
                    ("Endpoint assets required", room_report["total_assets"]),
                    ("Data ports required", room_report["total_ports"]),
                    ("Use cases configured", len(scenarios)),
                    ("Network asset instances", sum(network["type_counts"].values())),
                    ("Network racks", len(network["rack_usage"])),
                    ("Rack PDUs", network["pdu_count"]),
                    ("UPS systems", network["ups_count"]),
                    ("Fibre splices", len(network["fibre_splices"])),
                ],
                styles,
            )
        )
        story.append(Spacer(1, 5 * mm))

    if "room_summary" in selected_sections:
        start_section("Room Type Summary")
        total_placed_rooms = 0
        total_assets_per_room = 0
        total_ports_per_room = 0
        rows = [[
            _p("Room type", styles["header"]),
            _p("Placed rooms", styles["header"]),
            _p("Assets per room", styles["header"]),
            _p("Data ports per room", styles["header"]),
        ]]
        for room in room_report["room_totals"]:
            total_placed_rooms += room["placed_rooms"]
            total_assets_per_room += room["assets_per_room"]
            total_ports_per_room += room["ports_per_room"]
            rows.append([
                _p(f"{room['room_type_id']} - {room['room_name']}", styles["body"]),
                _p(room["placed_rooms"], styles["body"]),
                _p(room["assets_per_room"], styles["body"]),
                _p(room["ports_per_room"], styles["body"]),
            ])
        rows.append([
            _p("Total", styles["header"]),
            _p(total_placed_rooms, styles["header"]),
            _p(total_assets_per_room, styles["header"]),
            _p(total_ports_per_room, styles["header"]),
        ])
        story.append(_table(rows, [125 * mm, 28 * mm, 32 * mm, 38 * mm], styles, numeric_columns=(1, 2, 3), total_rows=(-1,)))

    if "room_details" in selected_sections:
        start_section("Room Types and Contained Assets")
        for room_index, room in enumerate(room_report["room_rows"]):
            story.append(PageBreak())
            story.append(
                Paragraph(
                    f"{room['room_type_id']} - {room['room_name']} ({room['placed_rooms']} placed)",
                    styles["h2"],
                )
            )
            rows = [[
                _p("Asset ID", styles["header"]),
                _p("Description", styles["header"]),
                _p("ADB code", styles["header"]),
                _p("Grouping", styles["header"]),
                _p("Make / model", styles["header"]),
                _p("Qty per room", styles["header"]),
                _p("Ports each", styles["header"]),
                _p("Ports per room", styles["header"]),
                _p("Asset total", styles["header"]),
                _p("Port total", styles["header"]),
            ]]
            for asset in room["assets"]:
                rows.append([
                    _p(asset["asset_id"], styles["small"]),
                    _p(asset["asset_name"], styles["small"]),
                    _p(asset["adb_code"], styles["small"]),
                    _p(asset["group"], styles["small"]),
                    _p(asset["make_model"], styles["small"]),
                    _p(asset["qty_per_room"], styles["small"]),
                    _p(asset["ports_each"], styles["small"]),
                    _p(asset["port_per_room"], styles["small"]),
                    _p(asset["asset_subtotal"], styles["small"]),
                    _p(asset["port_subtotal"], styles["small"]),
                ])
            rows.append([
                _p("Total", styles["header"]),
                _p("", styles["header"]),
                _p("", styles["header"]),
                _p("", styles["header"]),
                _p("", styles["header"]),
                _p(room["assets_per_room"], styles["header"]),
                _p("", styles["header"]),
                _p(room["ports_per_room"], styles["header"]),
                _p(room["asset_total"], styles["header"]),
                _p(room["port_total"], styles["header"]),
            ])
            story.append(_table(rows, [27 * mm, 48 * mm, 24 * mm, 28 * mm, 34 * mm, 18 * mm, 18 * mm, 22 * mm, 22 * mm, 22 * mm], styles, numeric_columns=(5, 6, 7, 8, 9), total_rows=(-1,)))
            story.append(Spacer(1, 3 * mm))

    if "use_cases" in selected_sections:
        start_section("Use Cases", page_break=True)
        if not scenarios:
            story.append(Paragraph("No use cases have been configured.", styles["body"]))
        for index, scenario in enumerate(scenarios):
            if index:
                story.append(PageBreak())
            story.append(Paragraph(scenario["name"], styles["h1"]))
            story.append(
                _summary_cards(
                    [
                        ("Type", scenario["type"]),
                        ("Mode", scenario["mode"]),
                        ("Quantity", scenario["qty"]),
                        ("Room groups", ", ".join(scenario["room_groups"]) or "-"),
                        ("Asset groups", ", ".join(scenario["asset_groups"]) or "-"),
                        ("Replacement asset groups", ", ".join(scenario["replacement_asset_groups"]) or "-"),
                        ("Notes", scenario["notes"] or "-"),
                    ],
                    styles,
                )
            )
            story.append(Spacer(1, 4 * mm))
            rows = [[_p("Asset", styles["header"]), _p("Make / model", styles["header"]), _p("Ports each", styles["header"])]]
            scenario_asset_ids = scenario["replacement_asset_ids"] if scenario["type"] == "replacement" else scenario["asset_ids"]
            for asset_id in scenario_asset_ids:
                asset = room_report["assets_by_id"].get(asset_id, {})
                rows.append([_p(_asset_label(asset_id, asset), styles["body"]), _p(_asset_make_model(asset), styles["body"]), _p(_asset_ports(asset), styles["body"])])
            if len(rows) == 1:
                rows.append([_p("No assets resolved from this use case.", styles["body"]), _p("", styles["body"]), _p("", styles["body"])])
            story.append(_table(rows, [130 * mm, 68 * mm, 28 * mm], styles, numeric_columns=(2,)))

    if "network_summary" in selected_sections:
        start_section("Network Summary", page_break=True)
        settings = network["settings"]
        story.append(
            _summary_cards(
                [
                    ("Technology", settings.get("technology", "")),
                    ("Topology model", settings.get("topology_model", "")),
                    ("Rack deployment model", settings.get("rack_deployment_model", "")),
                    ("Aggregation rack mode", settings.get("aggregation_rack_mode", "")),
                    ("Default rack size U", settings.get("default_rack_size_u", "")),
                    ("Default PDU outlets", settings.get("default_pdu_outlet_count", "")),
                    ("Default PDU capacity W", settings.get("default_pdu_capacity_w", "")),
                    ("Spare capacity percent", settings.get("spare_capacity_percent", "")),
                ],
                styles,
            )
        )
        story.append(Spacer(1, 4 * mm))

        story.append(Paragraph("High-Level Topology", styles["h2"]))
        story.append(_topology_diagram(network, styles))

        story.append(Paragraph("Network Layers", styles["h2"]))
        rows = [[_p("Layer / role", styles["header"]), _p("Instances", styles["header"])]]
        for role, count in sorted(network["role_counts"].items(), key=lambda item: item[0].casefold()):
            rows.append([_p(_network_type_label(role), styles["body"]), _p(count, styles["body"])])
        story.append(_table(rows, [125 * mm, 28 * mm], styles, numeric_columns=(1,)))

    if "network_equipment" in selected_sections:
        start_section("Network Equipment Required", page_break=True)
        rows = [[
            _p("Type", styles["header"]),
            _p("Asset", styles["header"]),
            _p("Make", styles["header"]),
            _p("Model", styles["header"]),
            _p("Qty", styles["header"]),
            _p("Rack U", styles["header"]),
        ]]
        for (asset_id, name, asset_type, manufacturer, model), row in sorted(network["equipment"].items(), key=lambda item: (item[0][2], item[0][1])):
            rows.append([
                _p(_network_type_label(asset_type), styles["small"]),
                _p(f"{asset_id} - {name}" if asset_id else name, styles["small"]),
                _p(manufacturer, styles["small"]),
                _p(model, styles["small"]),
                _p(row["qty"], styles["small"]),
                _p(row["rack_units"], styles["small"]),
            ])
        if len(rows) == 1:
            rows.append([_p("No network equipment configured.", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"])])
        story.append(_table(rows, [45 * mm, 88 * mm, 38 * mm, 42 * mm, 18 * mm, 18 * mm], styles, numeric_columns=(4, 5)))

    if "power_draw" in selected_sections:
        start_section("Power Draw and Theoretical Energy", page_break=True)
        story.append(
            _summary_cards(
                [
                    ("Minimum draw with actual PoE load W", f"{power_draw['minimum_total_w']:.1f}"),
                    ("Installed PoE budget W", f"{power_draw['poe_budget_total_w']:.1f}"),
                    ("Theoretical maximum draw W", f"{power_draw['maximum_total_w']:.1f}"),
                    ("Minimum per day kWh", f"{power_draw['minimum_daily_kwh']:.2f}"),
                    ("Minimum per 30 days kWh", f"{power_draw['minimum_monthly_kwh']:.2f}"),
                    ("Minimum per year kWh", f"{power_draw['minimum_annual_kwh']:.2f}"),
                    ("Theoretical maximum per year kWh", f"{power_draw['annual_kwh']:.2f}"),
                ],
                styles,
            )
        )
        story.append(Spacer(1, 4 * mm))

        story.append(Paragraph("Power Draw by Topology Layer", styles["h2"]))
        rows = [[
            _p("Topology layer", styles["header"]),
            _p("PoE budget W", styles["header"]),
            _p("Max W", styles["header"]),
            _p("Share", styles["header"]),
        ]]
        total_w = max(0.0, power_draw["maximum_total_w"])
        layer_groups = set(power_draw["by_layer"]) | set(power_draw["poe_budget_by_layer"])
        for group in sorted(layer_groups, key=lambda value: _natural_key(_topology_group_label(value))):
            base_w = float(power_draw["by_layer"].get(group, 0.0))
            budget_w = float(power_draw["poe_budget_by_layer"].get(group, 0.0))
            max_w = base_w
            share = f"{(max_w / total_w * 100.0):.1f}%" if total_w > 0.0 else "-"
            rows.append([
                _p(_topology_group_label(group), styles["body"]),
                _p(f"{budget_w:.1f}", styles["body"]),
                _p(f"{max_w:.1f}", styles["body"]),
                _p(share, styles["body"]),
            ])
        if len(rows) == 1:
            rows.append([_p("No powered network assets configured.", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"])])
        story.append(_table(rows, [72 * mm, 30 * mm, 30 * mm, 24 * mm], styles, numeric_columns=(1, 2)))
        story.append(Spacer(1, 3 * mm))

        story.append(Paragraph("PoE Load by Endpoint Asset", styles["h2"]))
        rows = [[
            _p("Asset ID", styles["header"]),
            _p("Asset", styles["header"]),
            _p("Qty", styles["header"]),
            _p("PoE W each", styles["header"]),
            _p("Total PoE W", styles["header"]),
        ]]
        total_poe_qty = 0
        total_poe_load_w = 0.0
        for (asset_id, name), row in sorted(power_draw["poe_loads_by_asset"].items(), key=lambda item: (_natural_key(item[0][0]), _natural_key(item[0][1]))):
            qty = int(row.get("qty", 0) or 0)
            total_w = float(row.get("poe_load_w", 0.0) or 0.0)
            total_poe_qty += qty
            total_poe_load_w += total_w
            rows.append([
                _p(asset_id or "-", styles["small"]),
                _p(name, styles["small"]),
                _p(qty, styles["small"]),
                _p(_power_unit_draw_label(row.get("unit_poe_values", [])), styles["small"]),
                _p(f"{total_w:.1f}", styles["small"]),
            ])
        if len(rows) == 1:
            rows.append([_p("No PoE endpoint load configured.", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"])])
        else:
            rows.append([
                _p("Total PoE budget", styles["header"]),
                Paragraph("", styles["header"]),
                _p(total_poe_qty, styles["header"]),
                Paragraph("", styles["header"]),
                _p(f"{total_poe_load_w:.1f}", styles["header"]),
            ])
        story.append(_table(rows, [34 * mm, 84 * mm, 18 * mm, 30 * mm, 32 * mm], styles, numeric_columns=(2, 3, 4), total_rows=(-1,) if total_poe_load_w > 0.0 else ()))
        story.append(Spacer(1, 3 * mm))

        story.append(Paragraph("Actual PoE Load by Supplying Equipment", styles["h2"]))
        rows = [[
            _p("Type", styles["header"]),
            _p("Asset", styles["header"]),
            _p("Endpoint assignments", styles["header"]),
            _p("Actual PoE load W", styles["header"]),
        ]]
        for (asset_type, asset_id, name, _manufacturer, _model), row in sorted(power_draw["by_equipment"].items(), key=lambda item: (_natural_key(item[0][0]), _natural_key(item[0][1]))):
            if float(row.get("poe_load_w", 0.0)) <= 0.0 and int(row.get("poe_assignment_count", 0)) <= 0:
                continue
            rows.append([
                _p(_network_type_label(asset_type), styles["small"]),
                _p(f"{asset_id} - {name}" if asset_id else name, styles["small"]),
                _p(row.get("poe_assignment_count", 0), styles["small"]),
                _p(f"{row.get('poe_load_w', 0.0):.1f}", styles["small"]),
            ])
        if len(rows) == 1:
            rows.append([_p("No PoE endpoint load configured.", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"])])
        story.append(_table(rows, [42 * mm, 88 * mm, 42 * mm, 34 * mm], styles, numeric_columns=(2, 3)))
        story.append(Spacer(1, 3 * mm))

        story.append(Paragraph("Power Flow Through Topology", styles["h2"]))
        rows = [[_p("From layer", styles["header"]), _p("To layer", styles["header"]), _p("Power feeds", styles["header"]), _p("Allocated load W", styles["header"])]]
        for (source_group, target_group), row in sorted(power_draw["flows"].items(), key=lambda item: (_natural_key(_topology_group_label(item[0][0])), _natural_key(_topology_group_label(item[0][1])))):
            rows.append([
                _p(_topology_group_label(source_group), styles["body"]),
                _p(_topology_group_label(target_group), styles["body"]),
                _p(row["links"], styles["body"]),
                _p(f"{row['load_w']:.1f}", styles["body"]),
            ])
        if len(rows) == 1:
            rows.append([_p("No power feed records configured.", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"])])
        story.append(_table(rows, [58 * mm, 58 * mm, 28 * mm, 34 * mm], styles, numeric_columns=(2, 3)))

        story.append(Paragraph("Powered Equipment", styles["h2"]))
        rows = [[
            _p("Type", styles["header"]),
            _p("Asset", styles["header"]),
            _p("Make / model", styles["header"]),
            _p("Qty", styles["header"]),
            _p("PoE budget W", styles["header"]),
            _p("Max W", styles["header"]),
        ]]
        for (asset_type, asset_id, name, manufacturer, model), row in sorted(power_draw["by_equipment"].items(), key=lambda item: (_natural_key(item[0][0]), _natural_key(item[0][1]))):
            make_model = " ".join(part for part in (manufacturer, model) if part)
            rows.append([
                _p(_network_type_label(asset_type), styles["small"]),
                _p(f"{asset_id} - {name}" if asset_id else name, styles["small"]),
                _p(make_model, styles["small"]),
                _p(row["qty"], styles["small"]),
                _p(f"{row['poe_budget_w']:.1f}", styles["small"]),
                _p(f"{row['maximum_w']:.1f}", styles["small"]),
            ])
        if len(rows) == 1:
            rows.append([_p("No powered equipment configured.", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"])])
        story.append(_table(rows, [38 * mm, 80 * mm, 48 * mm, 16 * mm, 28 * mm, 28 * mm], styles, numeric_columns=(3, 4, 5)))

    if "rack_power_fibre" in selected_sections:
        start_section("Rack, Power and Fibre Requirements", page_break=True)
        rows = [[_p("Rack", styles["header"]), _p("Location", styles["header"]), _p("Floor", styles["header"]), _p("Capacity U", styles["header"]), _p("Used U", styles["header"]), _p("Items", styles["header"])]]
        for rack_name, row in sorted(network["rack_usage"].items(), key=lambda item: item[0].casefold()):
            rows.append([_p(rack_name, styles["body"]), _p(row["location"], styles["body"]), _p(row["floor"], styles["body"]), _p(row["capacity_u"], styles["body"]), _p(row["used_u"], styles["body"]), _p(row["items"], styles["body"])])
        if len(rows) == 1:
            rows.append([_p("No racks configured.", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"]), _p("", styles["body"])])
        story.append(_table(rows, [62 * mm, 70 * mm, 20 * mm, 24 * mm, 22 * mm, 20 * mm], styles, numeric_columns=(3, 4, 5)))
        story.append(Spacer(1, 4 * mm))
        story.append(
            _summary_cards(
                [
                    ("Racks", len(network["rack_usage"])),
                    ("PDUs", network["pdu_count"]),
                    ("UPS systems", network["ups_count"]),
                    ("Patch leads", sum(network["patch_leads"].values())),
                    ("Fibre cables", len(network["fibre_cables"])),
                    ("Fibre splice records", len(network["fibre_splices"])),
                    ("Fibre nodes / enclosures", len(network["fibre_nodes"])),
                ],
                styles,
            )
        )

        if network["patch_leads"]:
            story.append(Paragraph("Patch Leads", styles["h2"]))
            rows = [[_p("Lead type", styles["header"]), _p("Quantity", styles["header"])]]
            for lead_type, count in sorted(network["patch_leads"].items(), key=lambda item: item[0].casefold()):
                rows.append([_p(_network_type_label(lead_type), styles["body"]), _p(count, styles["body"])])
            story.append(_table(rows, [95 * mm, 28 * mm], styles, numeric_columns=(1,)))

    document.multiBuild(story, onFirstPage=_page_footer, onLaterPages=_page_footer)
    return destination
