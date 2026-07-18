"""PDF comparison report for unapplied zone-based equipment-room designs."""

from __future__ import annotations

import heapq
import math
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Sequence

from reportlab.lib import colors
from reportlab.lib.pagesizes import A0, A1, A2, A3, A4, landscape
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas

from dxf_scene import DXFScene
from floor_plan_pdf import _draw_dxf, _entity_bounds, model_floors


PAPER_SIZES = {
    "A0": landscape(A0),
    "A1": landscape(A1),
    "A2": landscape(A2),
    "A3": landscape(A3),
    "A4": landscape(A4),
}


def _studio_enabled(settings, key, default=True):
    return bool((settings or {}).get(key, default))


def _studio_callout(settings, key):
    value = ((settings or {}).get("callouts", {}) or {}).get(str(key), {})
    return value if isinstance(value, dict) else {}


def _zone_callout_text(zone, usage_rows):
    label = _zone_label(zone)
    matching = [row for row in usage_rows if row.get("zone_id") == zone.get("id")]
    usage = []
    for row in matching:
        kind = "DER" if row.get("kind") == "distributed_equipment_room" else "CR"
        usage.append(f"{kind} +{int(row.get('proposed', 0))}")
    if usage:
        label += " | " + ", ".join(usage)
    return label[:110]


def _text(value) -> str:
    return str(value or "").strip()


def _floor_dxf_paths(data: dict) -> Dict[int, str]:
    result = {}
    for row in data.get("floor_dxf_files", []):
        if isinstance(row, dict) and _text(row.get("filepath")):
            result[int(row.get("floor", 0) or 0)] = _text(row.get("filepath"))
    return result


def _floor_name(data: dict, floor: int, dxf_path: str) -> str:
    building = data.get("building", {}) if isinstance(data.get("building"), dict) else {}
    names = building.get("floor_names", {})
    if isinstance(names, dict):
        value = _text(names.get(str(floor), names.get(floor)))
        if value:
            return value
    for row in data.get("floors", []):
        if isinstance(row, dict) and int(row.get("floor", row.get("number", 0)) or 0) == floor:
            value = _text(row.get("name"))
            if value:
                return value
    return Path(dxf_path).stem if dxf_path else f"Floor {floor}"


def _data_points(data: dict) -> Dict[str, dict]:
    return {
        _text(row.get("name")): row
        for row in data.get("data_points", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }


def _all_graph_points(data: dict) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    for section in ("locations", "data_points"):
        for row in data.get(section, []):
            if isinstance(row, dict) and _text(row.get("name")):
                result[_text(row.get("name"))] = row
    for row in data.get("corridors", {}).get("nodes", []):
        if isinstance(row, dict) and _text(row.get("name")):
            result[_text(row.get("name"))] = row
    for transition in data.get("transitions", []):
        if not isinstance(transition, dict):
            continue
        transition_id = _text(transition.get("id"))
        for floor_key, position in (transition.get("floor_locations", {}) or {}).items():
            if not isinstance(position, dict):
                continue
            floor = int(floor_key)
            name = f"{transition_id}-F{floor}"
            result[name] = {
                "name": name,
                "floor": floor,
                "x": float(position.get("x", 0.0)),
                "y": float(position.get("y", 0.0)),
            }
    return result


def _floor_graph(data: dict, floor: int):
    all_points = _all_graph_points(data)
    adjacency = {}
    edges = []
    for edge in data.get("corridors", {}).get("edges", []):
        if not isinstance(edge, dict):
            continue
        start_name, end_name = _text(edge.get("from")), _text(edge.get("to"))
        start, end = all_points.get(start_name), all_points.get(end_name)
        if not start or not end:
            continue
        if int(start.get("floor", 0)) != floor or int(end.get("floor", 0)) != floor:
            continue
        distance = math.hypot(
            float(end.get("x", 0.0)) - float(start.get("x", 0.0)),
            float(end.get("y", 0.0)) - float(start.get("y", 0.0)),
        )
        adjacency.setdefault(start_name, []).append((end_name, distance))
        adjacency.setdefault(end_name, []).append((start_name, distance))
        edges.append((start_name, end_name))
    return all_points, adjacency, edges


def _nearest_graph_node(adjacency, all_points, x, y):
    candidates = [name for name in adjacency if name in all_points]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda name: math.hypot(
            float(all_points[name].get("x", 0.0)) - x,
            float(all_points[name].get("y", 0.0)) - y,
        ),
    )


def _shortest_path(adjacency, start, end):
    if not start or not end:
        return None, math.inf
    queue = [(0.0, start)]
    distances = {start: 0.0}
    previous = {}
    while queue:
        distance, name = heapq.heappop(queue)
        if distance != distances.get(name):
            continue
        if name == end:
            path = [name]
            while name in previous:
                name = previous[name]
                path.append(name)
            return list(reversed(path)), distance
        for neighbour, weight in adjacency.get(name, []):
            candidate = distance + weight
            if candidate < distances.get(neighbour, math.inf):
                distances[neighbour] = candidate
                previous[neighbour] = name
                heapq.heappush(queue, (candidate, neighbour))
    return None, math.inf


def _assignment_route(room, point, graph):
    all_points, adjacency, _edges = graph
    room_x, room_y = float(room.get("x", 0.0)), float(room.get("y", 0.0))
    point_x, point_y = float(point.get("x", 0.0)), float(point.get("y", 0.0))
    start = _text(room.get("anchor_name"))
    if start not in adjacency:
        start = _nearest_graph_node(adjacency, all_points, room_x, room_y)
    end_name = _text(point.get("name"))
    end = _text(
        (room.get("data_point_anchor_names", {}) or {}).get(end_name)
    )
    if end not in adjacency:
        end = end_name if end_name in adjacency else _nearest_graph_node(
            adjacency, all_points, point_x, point_y
        )
    graph_path, graph_distance = _shortest_path(adjacency, start, end)
    if graph_path:
        coordinates = [(room_x, room_y)]
        coordinates.extend(
            (float(all_points[name].get("x", 0.0)), float(all_points[name].get("y", 0.0)))
            for name in graph_path
        )
        coordinates.append((point_x, point_y))
        spur_distance = math.hypot(
            float(all_points[start].get("x", 0.0)) - room_x,
            float(all_points[start].get("y", 0.0)) - room_y,
        ) + math.hypot(
            point_x - float(all_points[end].get("x", 0.0)),
            point_y - float(all_points[end].get("y", 0.0)),
        )
        return coordinates, graph_distance + spur_distance + float(point.get("extension_distance_m", 0.0) or 0.0), False
    return (
        [(room_x, room_y), (point_x, point_y)],
        math.hypot(point_x - room_x, point_y - room_y)
        + float(point.get("extension_distance_m", 0.0) or 0.0),
        True,
    )


def _zone_label(zone: dict) -> str:
    cr_limit = int(zone.get("max_comms_rooms", 0) or 0)
    der_limit = int(zone.get("max_distributed_equipment_rooms", 0) or 0)
    return (
        f"{_text(zone.get('name')) or _text(zone.get('id')) or 'Zone'} | "
        f"CR {cr_limit or 'unlimited'} | DER {der_limit or 'unlimited'}"
    )


def _option_counts(plan: dict):
    suggestions = list(plan.get("suggestions", []) or [])
    comms = sum(1 for row in suggestions if row.get("kind") == "comms_room")
    ders = len(suggestions) - comms
    assigned = sum(len(row.get("data_point_names", []) or []) for row in suggestions)
    ports = sum(int(row.get("ports", 0) or 0) for row in suggestions)
    unassigned_points = len(plan.get("unassigned", []) or [])
    unassigned_ports = int(plan.get("unassigned_port_count", 0) or 0)
    return comms, ders, assigned, ports, unassigned_points, unassigned_ports


def _draw_cover(
    c,
    page_size,
    *,
    project_name,
    plans,
    strategy_names,
    planning_options,
    room_port_limits,
    paper_size,
    scale,
    floor_scope,
    revision_number,
):
    width, height = page_size
    margin = 15 * mm
    c.setFillColor(colors.white)
    c.rect(0, 0, width, height, stroke=0, fill=1)
    c.setFillColor(colors.HexColor("#102a3a"))
    c.setFont("Helvetica-Bold", 22)
    c.drawString(margin, height - 24 * mm, "Zone-based Equipment Room Design Options")
    c.setFont("Helvetica-Bold", 13)
    c.drawString(margin, height - 34 * mm, project_name[:90])
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.HexColor("#52616c"))
    subtitle = (
        "Unapplied planning outcomes only - no rooms or connections were changed "
        "when this report was generated."
    )
    c.drawString(margin, height - 41 * mm, subtitle)

    settings_y = height - 55 * mm
    settings = [
        ("Cable limit", f"{float(planning_options.get('max_distance_m', 0.0)):.2f} m"),
        (
            "Planning scope",
            (
                f"Floor {int(planning_options.get('scope_floor', 0) or 0)} only"
                if _text(planning_options.get("scope")) == "current"
                else "All selected floors"
            ),
        ),
        (
            "Floor rule",
            "Same floor" if planning_options.get("same_floor_only") else "Cross-floor allowed",
        ),
        ("CR room capacity", f"{int(room_port_limits.get('comms_room', 0))} ports"),
        ("DER capacity", f"{int(room_port_limits.get('distributed_equipment_room', 0))} ports"),
        ("Drawing sheets", f"{paper_size} landscape at 1:{scale}"),
        ("Floors exported", "All floors" if floor_scope == "all" else f"Floor {floor_scope} only"),
    ]
    column_width = (width - 2 * margin) / 3.0
    for index, (label, value) in enumerate(settings):
        col, row = index % 3, index // 3
        x = margin + col * column_width
        y = settings_y - row * 12 * mm
        c.setFont("Helvetica-Bold", 6.5)
        c.setFillColor(colors.HexColor("#647480"))
        c.drawString(x, y, label.upper())
        c.setFont("Helvetica", 9)
        c.setFillColor(colors.HexColor("#17212b"))
        c.drawString(x, y - 4.5 * mm, value[:55])

    top = settings_y - 30 * mm
    table_x = margin
    table_w = width - 2 * margin
    columns = [0.05, 0.19, 0.06, 0.06, 0.10, 0.11, 0.11, 0.11, 0.19]
    headings = [
        "OPTION", "STRATEGY", "CR", "DER", "ASSIGNED PTS", "ASSIGNED PORTS",
        "UNASSIGNED PTS", "UNASSIGNED PORTS", "EST. CABLE",
    ]
    row_h = 12 * mm
    c.setFillColor(colors.HexColor("#203746"))
    c.rect(table_x, top - row_h, table_w, row_h, stroke=0, fill=1)
    cursor = table_x
    for fraction, heading in zip(columns, headings):
        c.setFont("Helvetica-Bold", 7)
        c.setFillColor(colors.white)
        c.drawString(cursor + 2 * mm, top - 7.5 * mm, heading)
        cursor += table_w * fraction

    y = top - row_h
    for index, plan in enumerate(plans, 1):
        comms, ders, assigned, ports, unassigned, unassigned_ports = _option_counts(plan)
        values = [
            str(index),
            strategy_names.get(plan.get("strategy"), _text(plan.get("strategy"))),
            str(comms),
            str(ders),
            str(assigned),
            str(ports),
            str(unassigned),
            str(unassigned_ports),
            f"{float(plan.get('total_cable_length_m', 0.0) or 0.0):,.1f} m",
        ]
        fill = colors.HexColor("#f2f6f8") if index % 2 else colors.white
        c.setFillColor(fill)
        c.setStrokeColor(colors.HexColor("#c9d2d8"))
        c.rect(table_x, y - row_h, table_w, row_h, stroke=1, fill=1)
        cursor = table_x
        for fraction, value in zip(columns, values):
            c.setFont("Helvetica-Bold" if value == str(index) else "Helvetica", 8)
            c.setFillColor(colors.HexColor("#17212b"))
            c.drawString(cursor + 2 * mm, y - 7.5 * mm, value[:46])
            cursor += table_w * fraction
        y -= row_h

    y -= 8 * mm
    c.setFont("Helvetica", 7.5)
    c.setFillColor(colors.HexColor("#52616c"))
    scope_note = (
        "Counts cover unconnected demand on the selected planning floor only; "
        "other floors are excluded and this is not a whole-model port total."
        if _text(planning_options.get("scope")) == "current"
        else "Counts cover unconnected demand in the selected planning scope, not every port in the model."
    )
    c.drawString(table_x, y, scope_note[:145])
    y -= 6 * mm
    if planning_options.get("same_floor_only"):
        c.drawString(
            table_x,
            y,
            "Same-floor satisfaction is evaluated independently for each floor; zero unassigned ports means that floor is fully satisfied.",
        )
        y -= 6 * mm

    failed_plans = [
        (index, plan)
        for index, plan in enumerate(plans, 1)
        if int(plan.get("unassigned_port_count", 0) or 0) > 0
    ]
    if failed_plans:
        c.setFont("Helvetica-Bold", 8)
        c.setFillColor(colors.HexColor("#a12b2b"))
        c.drawString(table_x, y, "Coverage shortfall reasons")
        y -= 5 * mm
        for option_index, plan in failed_plans:
            for reason_row in plan.get("shortfall_reasons", []) or []:
                reason_text = (
                    f"Option {option_index}, Floor {int(reason_row.get('floor', 0))}: "
                    f"{int(reason_row.get('port_count', 0))} port(s) - "
                    f"{_text(reason_row.get('reason'))}"
                )
                c.setFont("Helvetica", 7)
                c.setFillColor(colors.HexColor("#52616c"))
                c.drawString(table_x, y, reason_text[:155])
                y -= 4.5 * mm
            recommended_changes = list(
                plan.get("recommended_zone_changes", []) or []
            )
            for change in recommended_changes:
                kind = (
                    "DERs"
                    if change.get("kind") == "distributed_equipment_room"
                    else "comms rooms"
                )
                change_text = (
                    f"Option {option_index} verified alteration: "
                    f"{_text(change.get('zone_name'))} - increase maximum {kind} "
                    f"from {int(change.get('current_limit', 0))} "
                    f"to {int(change.get('suggested_limit', 0))}."
                )
                c.setFont("Helvetica-Bold", 7)
                c.setFillColor(colors.HexColor("#176b3a"))
                c.drawString(table_x, y, change_text[:155])
                y -= 4.5 * mm

    changed_plans = [
        (index, plan)
        for index, plan in enumerate(plans, 1)
        if plan.get("recommended_zone_changes")
        and int(plan.get("unassigned_port_count", 0) or 0) <= 0
    ]
    if changed_plans:
        c.setFont("Helvetica-Bold", 8)
        c.setFillColor(colors.HexColor("#176b3a"))
        c.drawString(table_x, y, "Zone alterations included in satisfactory plans")
        y -= 5 * mm
        for option_index, plan in changed_plans:
            for change in plan.get("recommended_zone_changes", []) or []:
                kind = (
                    "DERs"
                    if change.get("kind") == "distributed_equipment_room"
                    else "comms rooms"
                )
                change_text = (
                    f"Option {option_index}: {_text(change.get('zone_name'))} - "
                    f"maximum {kind} {int(change.get('current_limit', 0))} to "
                    f"{int(change.get('suggested_limit', 0))}."
                )
                c.setFont("Helvetica", 7)
                c.setFillColor(colors.HexColor("#52616c"))
                c.drawString(table_x, y, change_text[:155])
                y -= 4.5 * mm

    y -= 2 * mm
    c.setFont("Helvetica-Bold", 10)
    c.setFillColor(colors.HexColor("#203746"))
    c.drawString(margin, y, "Per-zone availability used by each option")
    y -= 7 * mm
    for option_index, plan in enumerate(plans, 1):
        c.setFont("Helvetica-Bold", 8)
        c.setFillColor(colors.HexColor("#17212b"))
        c.drawString(margin, y, f"Option {option_index}")
        x = margin + 22 * mm
        for usage in plan.get("zone_usage", []):
            if not usage.get("existing") and not usage.get("proposed"):
                continue
            kind = "DER" if usage.get("kind") == "distributed_equipment_room" else "CR"
            limit = int(usage.get("limit", 0) or 0)
            value = (
                f"{usage.get('zone_name')} {kind} "
                f"{int(usage.get('existing', 0))}+{int(usage.get('proposed', 0))}/"
                f"{limit or 'unlimited'}"
            )
            c.setFont("Helvetica", 7)
            width_needed = c.stringWidth(value + "   ", "Helvetica", 7)
            if x + width_needed > width - margin:
                y -= 5 * mm
                x = margin + 22 * mm
            c.drawString(x, y, value)
            x += width_needed
        y -= 7 * mm
        if y < 22 * mm:
            break

    c.setFont("Helvetica", 6.5)
    c.setFillColor(colors.HexColor("#52616c"))
    footer = (
        f"Revision {'R' + str(revision_number) if revision_number else 'Unrevised'} | "
        f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | Cover sheet"
    )
    c.drawRightString(width - margin, 9 * mm, footer)


def _draw_callout_box(
    c,
    *,
    anchor_x,
    anchor_y,
    label,
    colour,
    box_position,
    font_size=6.5,
    label_override=None,
):
    if label_override is not None:
        label = str(label_override)
    box_x, box_y, box_width, box_height, rail = box_position
    c.saveState()
    c.setStrokeColor(colors.HexColor(colour))
    c.setLineWidth(0.35 * mm)
    if rail in {"above", "top"}:
        attach_x, attach_y = box_x + box_width / 2.0, box_y
    elif rail in {"below", "bottom"}:
        attach_x, attach_y = box_x + box_width / 2.0, box_y + box_height
    elif rail == "left":
        attach_x, attach_y = box_x + box_width, box_y + box_height / 2.0
    else:
        attach_x, attach_y = box_x, box_y + box_height / 2.0
    leader = c.beginPath()
    leader.moveTo(anchor_x, anchor_y)
    if rail in {"above", "below", "top", "bottom"}:
        leader.lineTo(anchor_x, attach_y)
    else:
        leader.lineTo(attach_x, anchor_y)
    leader.lineTo(attach_x, attach_y)
    c.drawPath(leader, stroke=1, fill=0)
    c.rect(box_x, box_y, box_width, box_height, stroke=1, fill=0)
    usable_width = max(1.0, box_width - 4 * mm)
    fitted_size = float(font_size)
    while fitted_size > 4.2 and c.stringWidth(
        label, "Helvetica-Bold", fitted_size
    ) > usable_width:
        fitted_size -= 0.25
    while label and c.stringWidth(
        label, "Helvetica-Bold", fitted_size
    ) > usable_width:
        label = label[:-4].rstrip() + "..." if len(label) > 4 else ""
    c.setFont("Helvetica-Bold", fitted_size)
    c.setFillColor(colors.HexColor(colour))
    text_y = box_y + (box_height - fitted_size) / 2.0 + 1.0
    c.drawString(box_x + 2 * mm, text_y, label)
    c.restoreState()


def _draw_zone(
    c,
    zone,
    transform,
    usage_rows,
    *,
    draw_box=True,
    draw_label=True,
    callout_box=None,
    label_override=None,
    font_scale=1.0,
):
    left, bottom = transform(float(zone.get("min_x", 0.0)), float(zone.get("min_y", 0.0)))
    right, top = transform(float(zone.get("max_x", 0.0)), float(zone.get("max_y", 0.0)))
    x, y = min(left, right), min(bottom, top)
    width, height = abs(right - left), abs(top - bottom)
    c.saveState()
    if draw_box:
        c.setStrokeColor(colors.HexColor("#2474a8"))
        c.setFillColor(colors.HexColor("#eaf5fb"))
        c.setLineWidth(0.4 * mm)
        c.setDash(2 * mm, 1 * mm)
        c.rect(x, y, width, height, stroke=1, fill=1)
        c.setDash()
    label = _zone_callout_text(zone, usage_rows)
    if draw_label:
        if callout_box is None:
            callout_box = (x + 5 * mm, top - 10 * mm, 60 * mm, 7 * mm, "top")
        _draw_callout_box(
            c,
            anchor_x=x,
            anchor_y=top,
            label=label,
            colour="#2474a8",
            box_position=callout_box,
            label_override=label_override,
            font_size=6.5 * float(font_scale),
        )
    c.restoreState()


def _draw_routing_graph(c, graph, transform):
    all_points, _adjacency, edges = graph
    c.saveState()
    c.setStrokeColor(colors.HexColor("#78909c"))
    c.setLineWidth(0.28 * mm)
    for start, end in edges:
        a = transform(float(all_points[start].get("x", 0.0)), float(all_points[start].get("y", 0.0)))
        b = transform(float(all_points[end].get("x", 0.0)), float(all_points[end].get("y", 0.0)))
        c.line(*a, *b)
    c.restoreState()


def _draw_assignment(c, room, coordinates, transform, *, fallback=False):
    colour = "#f39c12" if room.get("kind") == "distributed_equipment_room" else "#0b7a5c"
    c.saveState()
    c.setStrokeColor(colors.HexColor(colour))
    c.setLineWidth(0.52 * mm)
    if fallback:
        c.setDash(2 * mm, 1.2 * mm)
    path = c.beginPath()
    first = transform(*coordinates[0])
    path.moveTo(*first)
    for coordinate in coordinates[1:]:
        path.lineTo(*transform(*coordinate))
    c.drawPath(path, stroke=1, fill=0)
    c.restoreState()


def _draw_data_point(c, point, transform, *, status="assigned"):
    x, y = transform(float(point.get("x", 0.0)), float(point.get("y", 0.0)))
    colour = {
        "assigned": "#7c4dcc",
        "unassigned": "#d64545",
        "other": "#65747f",
    }.get(status, "#65747f")
    c.saveState()
    c.setStrokeColor(colors.HexColor(colour))
    c.setFillColor(colors.HexColor(colour))
    size = 1.3 * mm
    path = c.beginPath()
    path.moveTo(x, y + size)
    path.lineTo(x + size, y)
    path.lineTo(x, y - size)
    path.lineTo(x - size, y)
    path.close()
    c.drawPath(path, stroke=1, fill=1)
    if status == "unassigned":
        c.setLineWidth(0.45 * mm)
        c.line(x - size, y - size, x + size, y + size)
        c.line(x - size, y + size, x + size, y - size)
    c.restoreState()


def _draw_proposed_room(c, room, transform):
    x, y = transform(float(room.get("x", 0.0)), float(room.get("y", 0.0)))
    is_der = room.get("kind") == "distributed_equipment_room"
    colour = "#f39c12" if is_der else "#0b7a5c"
    c.saveState()
    c.setFillColor(colors.HexColor(colour))
    c.setStrokeColor(colors.white)
    c.setLineWidth(0.45 * mm)
    size = 2.8 * mm
    if is_der:
        c.rect(x - size, y - size, size * 2, size * 2, stroke=1, fill=1)
    else:
        c.circle(x, y, size, stroke=1, fill=1)
    c.restoreState()


def _room_equipment_counts(room, planning_options):
    options = planning_options or {}
    ports = max(0, int(room.get("ports", 0) or 0))
    ports_per_switch = max(
        1, int(options.get("access_ports_per_switch", 48) or 48)
    )
    switches = int(math.ceil(ports / ports_per_switch)) if ports else 0
    if room.get("kind") == "distributed_equipment_room":
        switches_per_cabinet = max(
            1, int(options.get("der_max_switches", 2) or 2)
        )
    else:
        switches_per_cabinet = max(
            1, int(options.get("comms_switches_per_cabinet", 1) or 1)
        )
    cabinets = (
        int(math.ceil(switches / switches_per_cabinet)) if switches else 0
    )
    return switches, cabinets


def _room_callout_text(room, planning_options=None):
    is_der = room.get("kind") == "distributed_equipment_room"
    switches, cabinets = _room_equipment_counts(room, planning_options)
    switch_label = "switch" if switches == 1 else "switches"
    cabinet_label = "rack cabinet" if cabinets == 1 else "rack cabinets"
    return (
        f"{'DER' if is_der else 'CR'} | {room.get('zone_name')} | "
        f"{int(room.get('ports', 0))}/{int(room.get('port_limit', 0))} ports | "
        f"{switches} {switch_label} | {cabinets} {cabinet_label}"
    )[:130]


def _draw_room_label_callout(
    c,
    room,
    transform,
    *,
    planning_options=None,
    callout_box=None,
    label_override=None,
    font_scale=1.0,
):
    anchor_x, anchor_y = transform(
        float(room.get("x", 0.0)), float(room.get("y", 0.0))
    )
    is_der = room.get("kind") == "distributed_equipment_room"
    colour = "#f39c12" if is_der else "#0b7a5c"
    label = _room_callout_text(room, planning_options)
    if callout_box is None:
        callout_box = (
            anchor_x + 5 * mm,
            anchor_y + 4 * mm,
            90 * mm,
            7 * mm,
            "above",
        )
    _draw_callout_box(
        c,
        anchor_x=anchor_x,
        anchor_y=anchor_y,
        label=label,
        colour=colour,
        box_position=callout_box,
        font_size=6.2 * float(font_scale),
        label_override=label_override,
    )


def _draw_max_distance_callout(
    c,
    room,
    point,
    transform,
    *,
    max_distance=0.0,
    fallback=False,
    callout_box=None,
    label_override=None,
    font_scale=1.0,
):
    if point:
        anchor_x, anchor_y = transform(
            float(point.get("x", 0.0)), float(point.get("y", 0.0))
        )
        point_name = _text(point.get("name"))
        callout = f"MAX {point_name}: {max_distance:.2f} m"
    else:
        anchor_x, anchor_y = transform(
            float(room.get("x", 0.0)), float(room.get("y", 0.0))
        )
        callout = "MAX: no assigned points"
    if fallback and point:
        callout += " (fallback)"
    colour = (
        "#f39c12"
        if room.get("kind") == "distributed_equipment_room"
        else "#0b7a5c"
    )
    if callout_box is None:
        callout_box = (
            anchor_x + 10 * mm,
            anchor_y - 10 * mm,
            55 * mm,
            6 * mm,
            "top",
        )
    _draw_callout_box(
        c,
        anchor_x=anchor_x,
        anchor_y=anchor_y,
        label=callout,
        colour=colour,
        box_position=callout_box,
        font_size=6.2 * float(font_scale),
        label_override=label_override,
    )


def _draw_sheet_title_block(
    c,
    page_size,
    *,
    project_name,
    option_number,
    strategy_name,
    floor_name,
    floor,
    counts,
    paper_size,
    scale,
    source_drawing,
    page_number,
    page_count,
):
    width, _height = page_size
    x, y, h = 10 * mm, 7 * mm, 28 * mm
    w = width - 20 * mm
    c.setFillColor(colors.HexColor("#f5f7f9"))
    c.setStrokeColor(colors.HexColor("#263440"))
    c.setLineWidth(0.35 * mm)
    c.rect(x, y, w, h, stroke=1, fill=1)
    split = w * 0.44
    c.line(x + split, y, x + split, y + h)
    c.setFillColor(colors.HexColor("#102a3a"))
    c.setFont("Helvetica-Bold", 12)
    c.drawString(x + 4 * mm, y + 19 * mm, project_name[:68])
    c.setFont("Helvetica-Bold", 9)
    c.drawString(x + 4 * mm, y + 12 * mm, f"Option {option_number} - {strategy_name}")
    c.setFont("Helvetica", 7.5)
    c.drawString(x + 4 * mm, y + 6 * mm, f"{floor_name} (Floor {floor})")

    comms, ders, assigned, ports, unassigned, unassigned_ports = counts
    key_x = x + split + 4 * mm
    legend = [
        ("#0b7a5c", "CR / route", "circle"),
        ("#f39c12", "DER / route", "square"),
        ("#78909c", "Routing graph", "line"),
        ("#7c4dcc", "Assigned point", "diamond"),
        ("#d64545", "Unassigned point", "diamond"),
        ("#65747f", "Other data point", "diamond"),
    ]
    for index, (colour, label, shape) in enumerate(legend):
        lx = key_x + (index % 2) * 42 * mm
        ly = y + 22 * mm - (index // 2) * 7.5 * mm
        c.setFillColor(colors.HexColor(colour))
        if shape == "circle":
            c.circle(lx + 2 * mm, ly, 1.6 * mm, stroke=0, fill=1)
        elif shape == "line":
            c.setStrokeColor(colors.HexColor(colour))
            c.setLineWidth(0.45 * mm)
            c.line(lx, ly, lx + 4 * mm, ly)
        else:
            c.rect(lx + 0.4 * mm, ly - 1.6 * mm, 3.2 * mm, 3.2 * mm, stroke=0, fill=1)
        c.setFillColor(colors.HexColor("#17212b"))
        c.setFont("Helvetica-Bold", 6.5)
        c.drawString(lx + 6 * mm, ly - 1.1 * mm, label)

    info_x = x + w * 0.73
    rows = [
        ("ROOMS", f"{comms} CR / {ders} DER"),
        ("DEMAND", f"{assigned} points / {ports} ports"),
        ("UNASSIGNED", f"{unassigned} points / {unassigned_ports} ports"),
        ("SCALE", f"1:{scale} on {paper_size}"),
        ("SOURCE", Path(source_drawing).name if source_drawing else "No mapped DXF"),
        ("SHEET", f"{page_number} of {page_count}"),
    ]
    for index, (label, value) in enumerate(rows):
        col, row = index // 3, index % 3
        tx = info_x + col * 45 * mm
        ty = y + 22 * mm - row * 8 * mm
        c.setFont("Helvetica-Bold", 5.2)
        c.setFillColor(colors.HexColor("#647480"))
        c.drawString(tx, ty, label)
        c.setFont("Helvetica", 6.8)
        c.setFillColor(colors.HexColor("#17212b"))
        c.drawString(tx, ty - 3.2 * mm, value[:44])


def export_zone_design_options_pdf(
    data: dict,
    plans: Sequence[dict],
    output_path: str,
    *,
    strategy_names: dict,
    planning_options: dict,
    room_port_limits: dict,
    source_path: str = "",
    paper_size: str = "A1",
    scale: int = 100,
    floor_scope="all",
    revision_number: int = 0,
    studio_settings=None,
    layout_manifest=None,
    preview_background=False,
) -> str:
    from pdf_report_annotations import draw_pdf_studio_annotations

    studio_settings = dict(studio_settings or {})
    if layout_manifest is not None:
        layout_manifest.clear()
    paper_size = _text(paper_size).upper()
    if paper_size not in PAPER_SIZES:
        raise ValueError(f"Unsupported paper size: {paper_size}")
    if not plans:
        raise ValueError("No zone design options are available to export.")
    scale = int(scale)
    if scale <= 0:
        raise ValueError("Drawing scale must be greater than zero.")

    page_size = PAPER_SIZES[paper_size]
    page_width, page_height = page_size
    drawing_left, drawing_bottom = 10 * mm, 40 * mm
    drawing_width, drawing_height = page_width - 20 * mm, page_height - 52 * mm
    safe_margin = 10 * mm
    points_per_metre = (1000.0 / scale) * mm
    dxf_paths = _floor_dxf_paths(data)
    dxf_cache = {}
    graph_cache = {}
    points = _data_points(data)
    zones = [row for row in data.get("equipment_room_placement_zones", []) if isinstance(row, dict)]

    sheet_payloads = []
    fit_failures = []
    for option_number, plan in enumerate(plans, 1):
        if floor_scope == "all":
            floors = set(model_floors(data))
            floors.update(int(row.get("floor", 0)) for row in plan.get("suggestions", []))
            floors.update(
                int(points[name].get("floor", 0))
                for name in plan.get("unassigned", [])
                if name in points
            )
            if not floors:
                floors.update(int(zone.get("floor", 0)) for zone in zones)
        else:
            floors = {int(floor_scope)}
        for floor in sorted(floors):
            dxf_path = dxf_paths.get(floor, "")
            if dxf_path not in dxf_cache:
                if dxf_path:
                    path = Path(dxf_path)
                    if not path.exists():
                        raise FileNotFoundError(f"Floor {floor} DXF does not exist: {path}")
                    dxf_cache[dxf_path] = list(DXFScene.load_content(str(path)).get("entities", []))
                else:
                    dxf_cache[dxf_path] = []
            entities = dxf_cache[dxf_path]
            if floor not in graph_cache:
                graph_cache[floor] = _floor_graph(data, floor)
            graph = graph_cache[floor]
            floor_zones = [zone for zone in zones if int(zone.get("floor", 0)) == floor]
            suggestions = [
                row for row in plan.get("suggestions", [])
                if int(row.get("floor", 0)) == floor
            ]
            floor_point_names = {
                name for name, point in points.items()
                if int(point.get("floor", 0)) == floor
            }
            bounds = list(_entity_bounds(entities))
            for zone in floor_zones:
                bounds.append(
                    (
                        float(zone.get("min_x", 0.0)),
                        float(zone.get("min_y", 0.0)),
                        float(zone.get("max_x", 0.0)),
                        float(zone.get("max_y", 0.0)),
                    )
                )
            for row in suggestions:
                x, y = float(row.get("x", 0.0)), float(row.get("y", 0.0))
                bounds.append((x, y, x, y))
            for name in floor_point_names:
                point = points.get(name)
                if point:
                    x, y = float(point.get("x", 0.0)), float(point.get("y", 0.0))
                    bounds.append((x, y, x, y))
            for start, end in graph[2]:
                for name in (start, end):
                    node = graph[0][name]
                    x, y = float(node.get("x", 0.0)), float(node.get("y", 0.0))
                    bounds.append((x, y, x, y))
            if not bounds:
                bounds = [(0.0, 0.0, 1.0, 1.0)]
            content_bounds = (
                min(row[0] for row in bounds),
                min(row[1] for row in bounds),
                max(row[2] for row in bounds),
                max(row[3] for row in bounds),
            )
            span_x = content_bounds[2] - content_bounds[0] + 4.0
            span_y = content_bounds[3] - content_bounds[1] + 4.0
            usable_width = drawing_width - 2 * safe_margin
            usable_height = drawing_height - 2 * safe_margin
            if span_x * points_per_metre > usable_width or span_y * points_per_metre > usable_height:
                minimum = int(
                    math.ceil(
                        max(
                            span_x * 1000.0 / (usable_width / mm),
                            span_y * 1000.0 / (usable_height / mm),
                        )
                    )
                )
                fit_failures.append((option_number, floor, minimum))
            sheet_payloads.append(
                (
                    option_number, plan, floor, dxf_path, entities, floor_zones,
                    suggestions, content_bounds, graph,
                )
            )

    if fit_failures:
        details = ", ".join(
            f"Option {option} floor {floor} needs approximately 1:{minimum}"
            for option, floor, minimum in fit_failures
        )
        raise ValueError(f"The selected 1:{scale} scale does not fit on {paper_size}. {details}.")

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    pdf = canvas.Canvas(str(destination), pagesize=page_size, pageCompression=1)
    project_name = _text(data.get("project", {}).get("name")) or (
        Path(source_path).stem if source_path else "Cable Routing Project"
    )
    show_cover = _studio_enabled(studio_settings, "show_cover", True)
    page_count = int(show_cover) + len(sheet_payloads)
    if show_cover:
        _draw_cover(
            pdf,
            page_size,
            project_name=project_name,
            plans=plans,
            strategy_names=strategy_names,
            planning_options=planning_options,
            room_port_limits=room_port_limits,
            paper_size=paper_size,
            scale=scale,
            floor_scope=floor_scope,
            revision_number=int(revision_number or 0),
        )
        if not preview_background:
            draw_pdf_studio_annotations(pdf, 0, studio_settings)
        pdf.showPage()

    for sheet_offset, payload in enumerate(sheet_payloads):
        sheet_index = sheet_offset + 1 + int(show_cover)
        (
            option_number, plan, floor, dxf_path, entities, floor_zones,
            suggestions, bounds, graph,
        ) = payload
        min_x, min_y, max_x, max_y = bounds
        centre_x, centre_y = (min_x + max_x) / 2.0, (min_y + max_y) / 2.0
        page_centre_x = drawing_left + drawing_width / 2.0
        page_centre_y = drawing_bottom + drawing_height / 2.0

        def transform(x, y):
            return (
                page_centre_x + (float(x) - centre_x) * points_per_metre,
                page_centre_y + (float(y) - centre_y) * points_per_metre,
            )

        pdf.setTitle(f"{project_name} - Zone design options")
        pdf.setAuthor("CableRouteResolver")
        pdf.setSubject("Unapplied zone-based placement design comparison")
        pdf.setFillColor(colors.white)
        pdf.rect(0, 0, page_width, page_height, stroke=0, fill=1)
        pdf.setStrokeColor(colors.HexColor("#263440"))
        pdf.setLineWidth(0.35 * mm)
        pdf.rect(drawing_left, drawing_bottom, drawing_width, drawing_height, stroke=1, fill=0)
        clip = pdf.beginPath()
        clip.rect(drawing_left, drawing_bottom, drawing_width, drawing_height)
        pdf.saveState()
        pdf.clipPath(clip, stroke=0, fill=0)
        if _studio_enabled(studio_settings, "show_zone_boundaries", True):
            for zone in floor_zones:
                _draw_zone(
                    pdf,
                    zone,
                    transform,
                    plan.get("zone_usage", []),
                    draw_box=True,
                    draw_label=False,
                )
        _draw_dxf(pdf, entities, transform)
        if _studio_enabled(studio_settings, "show_routing_graph", True):
            _draw_routing_graph(pdf, graph, transform)
        room_maxima = {}
        for room in suggestions:
            maximum = ("", 0.0, False)
            for point_name in room.get("data_point_names", []):
                point = points.get(point_name)
                if point and int(point.get("floor", 0)) == floor:
                    coordinates, distance, fallback = _assignment_route(room, point, graph)
                    _draw_assignment(pdf, room, coordinates, transform, fallback=fallback)
                    if not maximum[0] or distance > maximum[1]:
                        maximum = (point_name, distance, fallback)
            room_maxima[id(room)] = maximum
        drawn_points = set()
        for room in suggestions:
            for point_name in room.get("data_point_names", []):
                point = points.get(point_name)
                if (
                    point
                    and int(point.get("floor", 0)) == floor
                    and point_name not in drawn_points
                ):
                    _draw_data_point(pdf, point, transform)
                    drawn_points.add(point_name)
            _draw_proposed_room(pdf, room, transform)
        explicitly_unassigned = set(plan.get("unassigned", []))
        for point_name, point in points.items():
            if int(point.get("floor", 0)) != floor or point_name in drawn_points:
                continue
            status = "unassigned" if point_name in explicitly_unassigned else "other"
            if status != "other" or _studio_enabled(
                studio_settings, "show_other_data_points", True
            ):
                _draw_data_point(pdf, point, transform, status=status)

        frame_right = drawing_left + drawing_width
        frame_top = drawing_bottom + drawing_height
        dxf_world_bounds = list(_entity_bounds(entities))
        dxf_rectangles = []
        for world_left, world_bottom, world_right, world_top in dxf_world_bounds:
            entity_left, entity_bottom = transform(world_left, world_bottom)
            entity_right, entity_top = transform(world_right, world_top)
            dxf_rectangles.append(
                (
                    min(entity_left, entity_right),
                    min(entity_bottom, entity_top),
                    max(entity_left, entity_right),
                    max(entity_bottom, entity_top),
                )
            )
        zone_rectangles = {}
        for zone in floor_zones:
            left, bottom = transform(
                float(zone.get("min_x", 0.0)), float(zone.get("min_y", 0.0))
            )
            right, top = transform(
                float(zone.get("max_x", 0.0)), float(zone.get("max_y", 0.0))
            )
            zone_rectangles[str(zone.get("id", ""))] = (
                min(left, right),
                min(bottom, top),
                max(left, right),
                max(bottom, top),
            )

        def nearest_side(anchor_x, anchor_y, rectangle):
            left, bottom, right, top = rectangle
            return min(
                (
                    (abs(anchor_y - top), "above"),
                    (abs(anchor_y - bottom), "below"),
                    (abs(anchor_x - left), "left"),
                    (abs(anchor_x - right), "right"),
                )
            )[1]

        def desired_callout_box(anchor_x, anchor_y, rectangle, side, width):
            left, bottom, right, top = rectangle
            height = 7 * mm
            gap = 2 * mm
            if side == "above":
                x, y = anchor_x - width / 2.0, top + gap
            elif side == "below":
                x, y = anchor_x - width / 2.0, bottom - gap - height
            elif side == "left":
                x, y = left - gap - width, anchor_y - height / 2.0
            else:
                x, y = right + gap, anchor_y - height / 2.0
            x = min(max(x, drawing_left + 1 * mm), frame_right - width - 1 * mm)
            y = min(max(y, drawing_bottom + 1 * mm), frame_top - height - 1 * mm)
            return x, y, width, height, side

        callout_records = []
        for zone_index, zone in enumerate(floor_zones):
            zone_rectangle = zone_rectangles[str(zone.get("id", ""))]
            anchor_x, anchor_y = zone_rectangle[0], zone_rectangle[3]
            placement_rectangle = (anchor_x, anchor_y, anchor_x, anchor_y)
            callout_records.append(
                {
                    "key": ("zone", zone_index),
                    "studio_key": (
                        f"option:{option_number}:floor:{floor}:zone:"
                        f"{_text(zone.get('id')) or zone_index}"
                    ),
                    "kind": "zone",
                    "label": _zone_callout_text(
                        zone, plan.get("zone_usage", [])
                    ),
                    "colour": "#2474a8",
                    "anchor_x": anchor_x,
                    "anchor_y": anchor_y,
                    "rectangle": placement_rectangle,
                    "side": "above",
                    "width": 70 * mm,
                }
            )
        for room_index, room in enumerate(suggestions):
            rectangle = zone_rectangles.get(str(room.get("zone_id", "")))
            room_x, room_y = transform(
                float(room.get("x", 0.0)), float(room.get("y", 0.0))
            )
            if rectangle is None:
                rectangle = (room_x, room_y, room_x, room_y)
            placement_rectangle = (room_x, room_y, room_x, room_y)
            callout_records.append(
                {
                    "key": ("room_label", room_index),
                    "studio_key": (
                        f"option:{option_number}:floor:{floor}:room:{room_index}:label"
                    ),
                    "kind": "room",
                    "label": _room_callout_text(room, planning_options),
                    "colour": (
                        "#f39c12"
                        if room.get("kind") == "distributed_equipment_room"
                        else "#0b7a5c"
                    ),
                    "anchor_x": room_x,
                    "anchor_y": room_y,
                    "rectangle": placement_rectangle,
                    "side": nearest_side(
                        room_x, room_y, rectangle
                    ),
                    "width": 90 * mm,
                }
            )
            max_name, max_distance, max_fallback = room_maxima[id(room)]
            point = points.get(max_name)
            anchor = point or room
            anchor_x, anchor_y = transform(
                float(anchor.get("x", 0.0)), float(anchor.get("y", 0.0))
            )
            point_rectangle = (anchor_x, anchor_y, anchor_x, anchor_y)
            callout_records.append(
                {
                    "key": ("room_max", room_index),
                    "studio_key": (
                        f"option:{option_number}:floor:{floor}:room:{room_index}:maximum"
                    ),
                    "kind": "maximum",
                    "label": (
                        f"MAX {_text(point.get('name'))}: {max_distance:.2f} m"
                        + (" (fallback)" if max_fallback else "")
                        if point
                        else "MAX: no assigned points"
                    ),
                    "colour": (
                        "#f39c12"
                        if room.get("kind") == "distributed_equipment_room"
                        else "#0b7a5c"
                    ),
                    "anchor_x": anchor_x,
                    "anchor_y": anchor_y,
                    "rectangle": point_rectangle,
                    "side": nearest_side(
                        anchor_x, anchor_y, rectangle
                    ),
                    "width": 55 * mm,
                }
            )

        callout_slots = {}
        placed_boxes = []

        def overlaps(first, second):
            margin = 1.5 * mm
            return not (
                first[0] + first[2] + margin <= second[0]
                or second[0] + second[2] + margin <= first[0]
                or first[1] + first[3] + margin <= second[1]
                or second[1] + second[3] + margin <= first[1]
            )

        def overlaps_dxf(box):
            margin = 0.8 * mm
            for left, bottom, right, top in dxf_rectangles:
                if not (
                    box[0] + box[2] + margin <= left
                    or box[0] >= right + margin
                    or box[1] + box[3] + margin <= bottom
                    or box[1] >= top + margin
                ):
                    return True
            return False

        def ordered_sides(record):
            left, bottom, right, top = record["rectangle"]
            distances = {
                "above": abs(record["anchor_y"] - top),
                "below": abs(record["anchor_y"] - bottom),
                "left": abs(record["anchor_x"] - left),
                "right": abs(record["anchor_x"] - right),
            }
            preferred = record["side"]
            return [preferred] + [
                side
                for side, _distance in sorted(
                    distances.items(), key=lambda item: item[1]
                )
                if side != preferred
            ]

        for record in callout_records:
            selected = None
            non_dxf_fallback = None
            tested = set()
            callout_override = _studio_callout(
                studio_settings, record["studio_key"]
            )
            if all(
                name in callout_override
                for name in ("x_pt", "y_pt", "width_pt", "height_pt")
            ):
                override_width = max(18 * mm, float(callout_override["width_pt"]))
                override_height = max(5 * mm, float(callout_override["height_pt"]))
                selected = (
                    min(
                        max(float(callout_override["x_pt"]), drawing_left + 1 * mm),
                        frame_right - override_width - 1 * mm,
                    ),
                    min(
                        max(float(callout_override["y_pt"]), drawing_bottom + 1 * mm),
                        frame_top - override_height - 1 * mm,
                    ),
                    override_width,
                    override_height,
                    str(callout_override.get("rail", record["side"])),
                )
            for side in ([] if selected is not None else ordered_sides(record)):
                base = desired_callout_box(
                    record["anchor_x"],
                    record["anchor_y"],
                    record["rectangle"],
                    side,
                    record["width"],
                )
                for attempt in range(81):
                    offset_index = (
                        0
                        if attempt == 0
                        else ((attempt + 1) // 2) * (1 if attempt % 2 else -1)
                    )
                    shift = offset_index * 9 * mm
                    x, y, width, height, candidate_side = base
                    if candidate_side in {"above", "below"}:
                        x += shift
                    else:
                        y += shift
                    x = min(
                        max(x, drawing_left + 1 * mm),
                        frame_right - width - 1 * mm,
                    )
                    y = min(
                        max(y, drawing_bottom + 1 * mm),
                        frame_top - height - 1 * mm,
                    )
                    candidate = (x, y, width, height, candidate_side)
                    candidate_key = tuple(round(value, 3) for value in candidate[:4])
                    if candidate_key in tested:
                        continue
                    tested.add(candidate_key)
                    if overlaps_dxf(candidate):
                        continue
                    if non_dxf_fallback is None:
                        non_dxf_fallback = candidate
                    if not any(
                        overlaps(candidate, other) for other in placed_boxes
                    ):
                        selected = candidate
                        break
                if selected is not None:
                    break
            if selected is None:
                selected = non_dxf_fallback
            if selected is None:
                raise ValueError(
                    "Callouts do not fit outside the DXF background at the "
                    f"selected {paper_size} 1:{scale} layout."
                )
            default_visible = _studio_enabled(
                studio_settings,
                {
                    "zone": "show_zone_callouts",
                    "room": "show_room_callouts",
                    "maximum": "show_max_distance_callouts",
                }[record["kind"]],
                True,
            )
            visible = bool(callout_override.get("visible", default_visible))
            callout_slots[record["key"]] = {
                "box": selected,
                "visible": visible,
                "label": str(callout_override.get("text", record["label"])),
                "font_scale": max(
                    0.5, min(2.5, float(studio_settings.get("font_scale", 1.0) or 1.0))
                ),
            }
            if visible:
                placed_boxes.append(selected)
            if layout_manifest is not None:
                layout_manifest.append(
                    {
                        "page": int(sheet_index - 1),
                        "page_number": int(sheet_index),
                        "page_width_pt": float(page_width),
                        "page_height_pt": float(page_height),
                        "key": record["studio_key"],
                        "kind": record["kind"],
                        "text": str(callout_override.get("text", record["label"])),
                        "colour": record["colour"],
                        "x_pt": float(selected[0]),
                        "y_pt": float(selected[1]),
                        "width_pt": float(selected[2]),
                        "height_pt": float(selected[3]),
                        "rail": str(selected[4]),
                        "anchor_x_pt": float(record["anchor_x"]),
                        "anchor_y_pt": float(record["anchor_y"]),
                        "visible": visible,
                        "option_number": int(option_number),
                        "floor": int(floor),
                    }
                )

        for zone_index, zone in enumerate(floor_zones):
            slot = callout_slots.get(("zone", zone_index), {})
            if slot.get("visible", False) and not preview_background:
                _draw_zone(
                    pdf,
                    zone,
                    transform,
                    plan.get("zone_usage", []),
                    draw_box=False,
                    draw_label=True,
                    callout_box=slot.get("box"),
                    label_override=slot.get("label"),
                    font_scale=slot.get("font_scale", 1.0),
                )
        for room_index, room in enumerate(suggestions):
            label_slot = callout_slots.get(("room_label", room_index), {})
            if label_slot.get("visible", False) and not preview_background:
                _draw_room_label_callout(
                    pdf,
                    room,
                    transform,
                    planning_options=planning_options,
                    callout_box=label_slot.get("box"),
                    label_override=label_slot.get("label"),
                    font_scale=label_slot.get("font_scale", 1.0),
                )
            max_name, max_distance, max_fallback = room_maxima[id(room)]
            maximum_slot = callout_slots.get(("room_max", room_index), {})
            if maximum_slot.get("visible", False) and not preview_background:
                _draw_max_distance_callout(
                    pdf,
                    room,
                    points.get(max_name),
                    transform,
                    max_distance=max_distance,
                    fallback=max_fallback,
                    callout_box=maximum_slot.get("box"),
                    label_override=maximum_slot.get("label"),
                    font_scale=maximum_slot.get("font_scale", 1.0),
                )
        pdf.restoreState()

        floor_name = _floor_name(data, floor, dxf_path)
        strategy = strategy_names.get(plan.get("strategy"), _text(plan.get("strategy")))
        pdf.setFont("Helvetica-Bold", 8)
        pdf.setFillColor(colors.HexColor("#263440"))
        pdf.drawString(
            drawing_left + 2 * mm,
            page_height - 9 * mm,
            f"OPTION {option_number} - {strategy.upper()} - {floor_name.upper()}",
        )
        if _studio_enabled(studio_settings, "show_title_block", True):
            _draw_sheet_title_block(
                pdf,
                page_size,
                project_name=project_name,
                option_number=option_number,
                strategy_name=strategy,
                floor_name=floor_name,
                floor=floor,
                counts=_option_counts(plan),
                paper_size=paper_size,
                scale=scale,
                source_drawing=dxf_path,
                page_number=sheet_index,
                page_count=page_count,
            )
        if not preview_background:
            draw_pdf_studio_annotations(
                pdf, int(sheet_index - 1), studio_settings
            )
        pdf.showPage()

    pdf.save()
    return str(destination)
