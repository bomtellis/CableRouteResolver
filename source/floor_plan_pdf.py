"""Scaled, multi-page PDF floor-plan export."""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import A0, A1, A2, landscape
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

from dxf_scene import DXFScene


FLOOR_PLAN_PAPER_SIZES = {
    "A0": landscape(A0),
    "A1": landscape(A1),
    "A2": landscape(A2),
}


def _text(value) -> str:
    return str(value or "").strip()


def _floor_dxf_paths(data: dict) -> Dict[int, str]:
    result: Dict[int, str] = {}
    for row in data.get("floor_dxf_files", []):
        if not isinstance(row, dict):
            continue
        try:
            floor = int(row.get("floor", 0))
        except (TypeError, ValueError):
            continue
        path = _text(row.get("filepath"))
        if path:
            result[floor] = path
    return result


def model_floors(data: dict) -> List[int]:
    floors = set(_floor_dxf_paths(data))
    for section in (
        "locations",
        "data_points",
        "departments",
        "equipment_room_placement_zones",
    ):
        for row in data.get(section, []):
            if isinstance(row, dict):
                try:
                    floors.add(int(row.get("floor", 0)))
                except (TypeError, ValueError):
                    pass
    for row in data.get("corridors", {}).get("nodes", []):
        if isinstance(row, dict):
            try:
                floors.add(int(row.get("floor", 0)))
            except (TypeError, ValueError):
                pass
    for transition in data.get("transitions", []):
        if not isinstance(transition, dict):
            continue
        for floor in (transition.get("floor_locations", {}) or {}):
            try:
                floors.add(int(floor))
            except (TypeError, ValueError):
                pass
    return sorted(floors)


def _floor_points(data: dict, floor: int) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    for section in ("locations", "data_points"):
        for row in data.get(section, []):
            if not isinstance(row, dict):
                continue
            try:
                row_floor = int(row.get("floor", 0))
            except (TypeError, ValueError):
                continue
            name = _text(row.get("name"))
            if row_floor == floor and name:
                result[name] = row
    for row in data.get("corridors", {}).get("nodes", []):
        if not isinstance(row, dict):
            continue
        try:
            row_floor = int(row.get("floor", 0))
        except (TypeError, ValueError):
            continue
        name = _text(row.get("name"))
        if row_floor == floor and name:
            result[name] = row
    return result


def _cabinet_counts(data: dict) -> Dict[Tuple[int, str], int]:
    cabinets: Dict[Tuple[int, str], set] = {}
    locations = {
        _text(row.get("name")): row
        for row in data.get("locations", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }
    for row in data.get("network_racks", []):
        if not isinstance(row, dict):
            continue
        location = _text(row.get("location_name"))
        if not location:
            continue
        try:
            floor = int(row.get("floor", locations.get(location, {}).get("floor", 0)))
        except (TypeError, ValueError):
            floor = 0
        cabinet = _text(row.get("name")) or _text(row.get("id"))
        if cabinet:
            cabinets.setdefault((floor, location), set()).add(cabinet)
    for row in data.get("network_asset_instances", []):
        if not isinstance(row, dict):
            continue
        location = _text(row.get("location_name"))
        cabinet = _text(row.get("rack_name"))
        if not location or not cabinet:
            continue
        try:
            floor = int(row.get("floor", locations.get(location, {}).get("floor", 0)))
        except (TypeError, ValueError):
            floor = 0
        cabinets.setdefault((floor, location), set()).add(cabinet)
    return {key: len(value) for key, value in cabinets.items()}


def _location_port_counts(data: dict) -> Dict[Tuple[int, str], int]:
    locations = {
        _text(row.get("name")): row
        for row in data.get("locations", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }
    instances = {
        _text(row.get("id")): row
        for row in data.get("network_asset_instances", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    counts: Dict[Tuple[int, str], int] = {}
    for assignment in data.get("network_endpoint_assignments", []):
        if not isinstance(assignment, dict):
            continue
        location = _text(assignment.get("source_location"))
        instance = instances.get(_text(assignment.get("network_instance_id")), {})
        if not location:
            location = _text(instance.get("location_name"))
        if not location:
            continue
        location_row = locations.get(location, {})
        try:
            floor = int(instance.get("floor", location_row.get("floor", 0)))
        except (TypeError, ValueError):
            floor = 0
        key = (floor, location)
        counts[key] = counts.get(key, 0) + 1

    data_point_names = {
        _text(row.get("name"))
        for row in data.get("data_points", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }
    fallback: Dict[Tuple[int, str], int] = {}
    for connection in data.get("connections", []):
        if not isinstance(connection, dict):
            continue
        left = _text(connection.get("from"))
        right = _text(connection.get("to"))
        location = ""
        if left in locations and right in data_point_names:
            location = left
        elif right in locations and left in data_point_names:
            location = right
        if not location:
            continue
        try:
            quantity = max(1, int(connection.get("qty", 1) or 1))
        except (TypeError, ValueError):
            quantity = 1
        try:
            floor = int(locations[location].get("floor", 0))
        except (TypeError, ValueError):
            floor = 0
        key = (floor, location)
        fallback[key] = fallback.get(key, 0) + quantity
    for key, value in fallback.items():
        counts.setdefault(key, value)
    return counts


def _location_switch_counts(data: dict) -> Dict[Tuple[int, str], int]:
    locations = {
        _text(row.get("name")): row
        for row in data.get("locations", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }
    assets = {
        _text(row.get("id")): row
        for row in data.get("network_assets", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    counts: Dict[Tuple[int, str], int] = {}
    for instance in data.get("network_asset_instances", []):
        if not isinstance(instance, dict):
            continue
        asset = assets.get(_text(instance.get("asset_id")), {})
        role = _text(instance.get("design_role")).lower()
        asset_type = _text(asset.get("asset_type")).lower()
        if "switch" not in role and asset_type != "network_switch":
            continue
        location = _text(instance.get("location_name"))
        if not location:
            continue
        try:
            floor = int(instance.get("floor", locations.get(location, {}).get("floor", 0)))
        except (TypeError, ValueError):
            floor = 0
        try:
            members = max(1, int(instance.get("stack_member_count", 1) or 1))
        except (TypeError, ValueError):
            members = 1
        key = (floor, location)
        counts[key] = counts.get(key, 0) + members
    return counts


def _entity_bounds(entities: Sequence[dict]) -> List[Tuple[float, float, float, float]]:
    result = []
    for entity in entities:
        bounds = entity.get("bbox") if isinstance(entity, dict) else None
        if bounds and len(bounds) == 4:
            result.append(tuple(float(value) for value in bounds))
    return result


def _content_bounds(
    data: dict,
    floor: int,
    entities: Sequence[dict],
    *,
    include_placement_zones: bool = False,
):
    bounds = _entity_bounds(entities)
    for row in _floor_points(data, floor).values():
        try:
            x, y = float(row.get("x", 0.0)), float(row.get("y", 0.0))
        except (TypeError, ValueError):
            continue
        bounds.append((x, y, x, y))
    if include_placement_zones:
        for zone in data.get("equipment_room_placement_zones", []):
            if not isinstance(zone, dict) or int(zone.get("floor", 0) or 0) != floor:
                continue
            try:
                bounds.append(
                    (
                        float(zone.get("min_x", 0.0)),
                        float(zone.get("min_y", 0.0)),
                        float(zone.get("max_x", 0.0)),
                        float(zone.get("max_y", 0.0)),
                    )
                )
            except (TypeError, ValueError):
                continue
    if not bounds:
        return None
    return (
        min(row[0] for row in bounds),
        min(row[1] for row in bounds),
        max(row[2] for row in bounds),
        max(row[3] for row in bounds),
    )


def _draw_dxf(c: canvas.Canvas, entities: Sequence[dict], transform) -> None:
    c.setStrokeColor(colors.HexColor("#59636d"))
    c.setFillColor(colors.HexColor("#303840"))
    c.setLineWidth(0.16 * mm)
    for entity in entities:
        kind = _text(entity.get("type")).upper()
        if kind == "LINE":
            x1, y1 = transform(*entity["start"])
            x2, y2 = transform(*entity["end"])
            c.line(x1, y1, x2, y2)
        elif kind == "POLYLINE":
            points = [transform(*point) for point in entity.get("points", [])]
            if len(points) < 2:
                continue
            path = c.beginPath()
            path.moveTo(*points[0])
            for point in points[1:]:
                path.lineTo(*point)
            if entity.get("closed"):
                path.close()
            c.drawPath(path, stroke=1, fill=0)
        elif kind in {"CIRCLE", "ARC"}:
            cx, cy = entity.get("center", (0.0, 0.0))
            radius = float(entity.get("radius", 0.0))
            left, bottom = transform(cx - radius, cy - radius)
            right, top = transform(cx + radius, cy + radius)
            if kind == "CIRCLE":
                c.ellipse(left, bottom, right, top, stroke=1, fill=0)
            else:
                c.arc(
                    left,
                    bottom,
                    right,
                    top,
                    float(entity.get("start_angle", 0.0)),
                    float(entity.get("end_angle", 0.0))
                    - float(entity.get("start_angle", 0.0)),
                )
        elif kind == "TEXT":
            value = _text(entity.get("text"))
            if not value:
                continue
            x, y = transform(*entity.get("insert", (0.0, 0.0)))
            height_pt = abs(
                transform(0.0, float(entity.get("height", 0.0)))[1]
                - transform(0.0, 0.0)[1]
            )
            # DXF annotation heights are authored for CAD viewports and become
            # visually dominant on a printed sheet. Retain their relative size
            # while reducing them to a restrained drawing-note range.
            font_size = max(1.5, min(6.0, height_pt * 0.45))
            c.saveState()
            c.translate(x, y)
            c.rotate(float(entity.get("rotation", 0.0)))
            c.setFont("Helvetica", font_size)
            c.drawString(0, 0, value[:200])
            c.restoreState()


def _draw_corridors(c: canvas.Canvas, data: dict, floor: int, transform) -> None:
    points = _floor_points(data, floor)
    c.saveState()
    c.setStrokeColor(colors.HexColor("#2474a8"))
    c.setLineWidth(0.25 * mm)
    c.setDash(2 * mm, 1 * mm)
    for edge in data.get("corridors", {}).get("edges", []):
        if not isinstance(edge, dict):
            continue
        left = points.get(_text(edge.get("from")))
        right = points.get(_text(edge.get("to")))
        if not left or not right:
            continue
        c.line(
            *transform(float(left.get("x", 0.0)), float(left.get("y", 0.0))),
            *transform(float(right.get("x", 0.0)), float(right.get("y", 0.0))),
        )
    c.restoreState()


def _draw_placement_zones(
    c: canvas.Canvas, data: dict, floor: int, transform
) -> None:
    zones = [
        zone
        for zone in data.get("equipment_room_placement_zones", [])
        if isinstance(zone, dict) and int(zone.get("floor", 0) or 0) == floor
    ]
    for zone in zones:
        try:
            left, bottom = transform(
                float(zone.get("min_x", 0.0)), float(zone.get("min_y", 0.0))
            )
            right, top = transform(
                float(zone.get("max_x", 0.0)), float(zone.get("max_y", 0.0))
            )
        except (TypeError, ValueError):
            continue
        left, right = sorted((left, right))
        bottom, top = sorted((bottom, top))
        allow_comms = bool(zone.get("allow_comms_room", False))
        allow_der = bool(zone.get("allow_distributed_equipment_room", False))
        colour = "#2563eb" if allow_comms and allow_der else (
            "#047857" if allow_comms else "#7c3aed"
        )
        c.saveState()
        c.setStrokeColor(colors.HexColor(colour))
        c.setFillColor(colors.HexColor("#dbeafe"))
        if hasattr(c, "setFillAlpha"):
            c.setFillAlpha(0.24)
        c.setLineWidth(0.55 * mm)
        c.setDash(2.2 * mm, 1.1 * mm)
        c.rect(left, bottom, right - left, top - bottom, stroke=1, fill=1)
        if hasattr(c, "setFillAlpha"):
            c.setFillAlpha(1.0)
        c.setDash()
        name = _text(zone.get("name")) or _text(zone.get("id")) or "Placement zone"
        required_ports = max(0, int(zone.get("required_ports", 0) or 0))
        comms_limit = max(0, int(zone.get("max_comms_rooms", 0) or 0))
        der_limit = max(
            0, int(zone.get("max_distributed_equipment_rooms", 0) or 0)
        )
        room_types = "Comms + DER" if allow_comms and allow_der else (
            "Comms" if allow_comms else "DER"
        )
        details = f"{room_types} | Limits {comms_limit} CR / {der_limit} DER"
        if required_ports:
            details += f" | {required_ports} ports"
        label_x = left + 1.5 * mm
        label_y = top - 3.5 * mm
        c.setFillColor(colors.HexColor(colour))
        c.setFont("Helvetica-Bold", 7.2)
        c.drawString(label_x, label_y, name[:72])
        c.setFont("Helvetica", 6.2)
        c.drawString(label_x, label_y - 3.0 * mm, details[:100])
        c.restoreState()


def _draw_comms_rooms(
    c: canvas.Canvas,
    data: dict,
    floor: int,
    transform,
    cabinet_counts: Dict[Tuple[int, str], int],
    port_counts: Dict[Tuple[int, str], int],
    switch_counts: Dict[Tuple[int, str], int],
    *,
    page_index: int = 0,
    page_label: str = "",
    layout_manifest=None,
    preview_background: bool = False,
) -> None:
    rooms = [
        row
        for row in data.get("locations", [])
        if isinstance(row, dict)
        and _text(row.get("kind")).lower()
        in {"comms_room", "mer", "distributed_equipment_room"}
        and int(row.get("floor", 0) or 0) == floor
    ]
    placed_boxes = []
    placed_leaders = []
    page_width, page_height = c._pagesize

    def box_overlaps(first, second, margin=1.2 * mm):
        return not (
            first[2] + margin <= second[0]
            or second[2] + margin <= first[0]
            or first[3] + margin <= second[1]
            or second[3] + margin <= first[1]
        )

    def leader_for(anchor, box):
        left, bottom, right, top = box
        centre_x, centre_y = (left + right) / 2.0, (bottom + top) / 2.0
        dx, dy = centre_x - anchor[0], centre_y - anchor[1]
        if abs(dx) > abs(dy):
            attach = (
                left if dx >= 0 else right,
                min(max(anchor[1], bottom), top),
            )
            elbow = (attach[0], anchor[1])
        else:
            attach = (
                min(max(anchor[0], left), right),
                bottom if dy >= 0 else top,
            )
            elbow = (anchor[0], attach[1])
        return [segment for segment in ((anchor, elbow), (elbow, attach)) if segment[0] != segment[1]]

    def segment_hits_box(segment, box, margin=0.8 * mm):
        (x1, y1), (x2, y2) = segment
        left, bottom, right, top = (
            box[0] - margin,
            box[1] - margin,
            box[2] + margin,
            box[3] + margin,
        )
        if abs(x1 - x2) < 0.01:
            return left <= x1 <= right and max(min(y1, y2), bottom) <= min(max(y1, y2), top)
        if abs(y1 - y2) < 0.01:
            return bottom <= y1 <= top and max(min(x1, x2), left) <= min(max(x1, x2), right)
        return False

    def leaders_cross(first, second, margin=0.5 * mm):
        (ax, ay), (bx, by) = first
        (cx, cy), (dx, dy) = second
        first_vertical = abs(ax - bx) < 0.01
        second_vertical = abs(cx - dx) < 0.01
        if first_vertical != second_vertical:
            vertical = first if first_vertical else second
            horizontal = second if first_vertical else first
            vx = vertical[0][0]
            hy = horizontal[0][1]
            return (
                min(vertical[0][1], vertical[1][1]) - margin <= hy <= max(vertical[0][1], vertical[1][1]) + margin
                and min(horizontal[0][0], horizontal[1][0]) - margin <= vx <= max(horizontal[0][0], horizontal[1][0]) + margin
            )
        if first_vertical:
            return abs(ax - cx) <= margin and max(min(ay, by), min(cy, dy)) <= min(max(ay, by), max(cy, dy)) + margin
        return abs(ay - cy) <= margin and max(min(ax, bx), min(cx, dx)) <= min(max(ax, bx), max(cx, dx)) + margin

    for room in rooms:
        name = _text(room.get("name")) or "Comms room"
        x, y = transform(float(room.get("x", 0.0)), float(room.get("y", 0.0)))
        count = cabinet_counts.get((floor, name), 0)
        ports = port_counts.get((floor, name), 0)
        switches = switch_counts.get((floor, name), 0)
        c.setFillColor(colors.HexColor("#007a5e"))
        c.setStrokeColor(colors.white)
        c.setLineWidth(0.5 * mm)
        c.circle(x, y, 3.2 * mm, stroke=1, fill=1)
        c.setStrokeColor(colors.HexColor("#007a5e"))
        label = name
        detail = f"Cabinets: {count} | Data ports: {ports} | Switches: {switches}"
        c.setFont("Helvetica-Bold", 9.0)
        label_width = max(
            stringWidth(label, "Helvetica-Bold", 9.0),
            stringWidth(detail, "Helvetica-Bold", 9.0),
        )
        width = label_width + 4 * mm
        height = 10.5 * mm
        gap = 7 * mm
        anchor = (x, y)
        bases = (
            (x + gap, y - height / 2.0, "vertical"),
            (x - width - gap, y - height / 2.0, "vertical"),
            (x - width / 2.0, y + gap, "horizontal"),
            (x - width / 2.0, y - height - gap, "horizontal"),
        )
        selected = None
        best = None
        best_score = None
        tested = set()
        for base_x, base_y, shift_axis in bases:
            for attempt in range(31):
                offset_index = 0 if attempt == 0 else ((attempt + 1) // 2) * (1 if attempt % 2 else -1)
                box_x = base_x + (offset_index * 10 * mm if shift_axis == "horizontal" else 0.0)
                box_y = base_y + (offset_index * 10 * mm if shift_axis == "vertical" else 0.0)
                box_x = min(max(box_x, 8 * mm), page_width - width - 8 * mm)
                box_y = min(max(box_y, 18 * mm), page_height - height - 8 * mm)
                box = (box_x, box_y, box_x + width, box_y + height)
                key = tuple(round(value, 2) for value in box)
                if key in tested:
                    continue
                tested.add(key)
                leaders = leader_for(anchor, box)
                score = sum(box_overlaps(box, other) for other in placed_boxes)
                score += sum(
                    segment_hits_box(segment, other)
                    for segment in leaders
                    for other in placed_boxes
                )
                score += sum(
                    segment_hits_box(segment, box)
                    for segment in placed_leaders
                )
                score += sum(
                    leaders_cross(segment, other)
                    for segment in leaders
                    for other in placed_leaders
                )
                if best_score is None or score < best_score:
                    best, best_score = (box, leaders), score
                if score == 0:
                    selected = (box, leaders)
                    break
            if selected is not None:
                break
        box, leader_segments = selected or best
        box_x, box_y, box_right, box_top = box
        placed_boxes.append(box)
        placed_leaders.extend(leader_segments)
        callout = {
            "key": f"floor-plan-room:{int(floor)}:{name}",
            "page": int(page_index),
            "page_label": str(page_label or f"Floor {floor}"),
            "kind": "equipment_room",
            "name": f"{name} - Floor {floor}",
            "location_name": name,
            "floor": int(floor),
            "x_pt": box_x,
            "y_pt": box_y,
            "width_pt": width,
            "height_pt": height,
            "anchor_x_pt": x,
            "anchor_y_pt": y,
            "colour": "#007a5e",
            "line_width_pt": 0.85,
            "font_size_pt": 9.0,
            "wrap_text": True,
            "text": f"{label}\n{detail}",
            "visible": True,
        }
        if layout_manifest is not None:
            layout_manifest.append(callout)
        if preview_background:
            continue
        for segment in leader_segments:
            c.line(segment[0][0], segment[0][1], segment[1][0], segment[1][1])
        c.setFillColor(colors.white)
        c.roundRect(
            box_x,
            box_y,
            width,
            height,
            1.2 * mm,
            stroke=0,
            fill=1,
        )
        c.setFillColor(colors.HexColor("#0f1720"))
        c.drawString(box_x + 2 * mm, box_y + 6.2 * mm, label)
        c.setFont("Helvetica", 9.0)
        c.setFillColor(colors.HexColor("#33434f"))
        c.drawString(box_x + 2 * mm, box_y + 1.8 * mm, detail)


def _draw_key(
    c: canvas.Canvas,
    page_size,
    *,
    project_name: str,
    floor: int,
    paper_size: str,
    scale: int,
    revision_number: int,
    source_path: str,
    source_drawing: str,
    page_number: int,
    page_count: int,
    include_placement_zones: bool = False,
) -> None:
    width, _height = page_size
    x, y, h = 12 * mm, 8 * mm, 25 * mm
    w = width - 24 * mm
    c.setFillColor(colors.HexColor("#f3f6f8"))
    c.setStrokeColor(colors.HexColor("#263440"))
    c.setLineWidth(0.35 * mm)
    c.rect(x, y, w, h, stroke=1, fill=1)
    title_w = w * 0.39
    c.line(x + title_w, y, x + title_w, y + h)
    c.setFillColor(colors.HexColor("#102a3a"))
    c.setFont("Helvetica-Bold", 13)
    c.drawString(x + 4 * mm, y + 15.5 * mm, project_name[:70])
    c.setFont("Helvetica-Bold", 9)
    sheet_title = (
        "Equipment room placement zones"
        if include_placement_zones
        else "Cable routing floor plan"
    )
    c.drawString(x + 4 * mm, y + 8.5 * mm, f"Floor {floor} - {sheet_title}")
    c.setFont("Helvetica", 7)
    note = (
        "Dashed blue, green and purple areas identify permitted equipment-room zones."
        if include_placement_zones
        else "Green markers identify comms rooms and installed cabinet totals."
    )
    c.drawString(x + 4 * mm, y + 3.5 * mm, note)

    key_x = x + title_w + 4 * mm
    column = (w - title_w - 8 * mm) / 3.0
    rows = [
        ("SCALE", f"1:{scale}"),
        ("PAPER", f"{paper_size} landscape"),
        ("REVISION", f"R{revision_number}" if revision_number else "Unrevised"),
        ("SOURCE MODEL", Path(source_path).name if source_path else "Unsaved project"),
        ("SOURCE DRAWING", Path(source_drawing).name if source_drawing else "No mapped DXF"),
        ("SHEET", f"{page_number} of {page_count}"),
        ("GENERATED", datetime.now().strftime("%Y-%m-%d %H:%M")),
    ]
    for index, (label, value) in enumerate(rows):
        col = index % 3
        row = index // 3
        tx = key_x + col * column
        ty = y + h - 5.5 * mm - row * 8 * mm
        c.setFont("Helvetica-Bold", 5.5)
        c.setFillColor(colors.HexColor("#52616c"))
        c.drawString(tx, ty, label)
        c.setFont("Helvetica", 7.2)
        c.setFillColor(colors.HexColor("#111820"))
        c.drawString(tx, ty - 3.2 * mm, value[:58])


def export_floor_plans_pdf(
    data: dict,
    output_path: str,
    *,
    source_path: str = "",
    paper_size: str = "A1",
    scale: int = 100,
    revision_number: int = 0,
    layout_manifest=None,
    preview_background: bool = False,
    include_placement_zones: bool = False,
    floors: Iterable[int] | None = None,
) -> str:
    if layout_manifest is not None:
        layout_manifest.clear()
    paper_size = _text(paper_size).upper()
    if paper_size not in FLOOR_PLAN_PAPER_SIZES:
        raise ValueError(f"Unsupported paper size: {paper_size}")
    scale = int(scale)
    if scale <= 0:
        raise ValueError("Drawing scale must be greater than zero.")
    available_floors = model_floors(data)
    if not available_floors:
        raise ValueError("No floors are present in the model.")
    if floors is None:
        selected_floors = available_floors
    else:
        requested = {int(floor) for floor in floors}
        selected_floors = [
            floor for floor in available_floors if floor in requested
        ]
        unavailable = sorted(requested.difference(available_floors))
        if unavailable:
            raise ValueError(
                "Selected floor(s) are not present in the model: "
                + ", ".join(str(floor) for floor in unavailable)
            )
    if not selected_floors:
        raise ValueError("Select at least one floor to export.")

    page_size = FLOOR_PLAN_PAPER_SIZES[paper_size]
    page_width, page_height = page_size
    drawing_left = 12 * mm
    drawing_bottom = 40 * mm
    drawing_width = page_width - 24 * mm
    drawing_height = page_height - 54 * mm
    content_safe_margin = 15 * mm
    usable_drawing_width = drawing_width - 2 * content_safe_margin
    usable_drawing_height = drawing_height - 2 * content_safe_margin
    points_per_metre = (1000.0 / scale) * mm
    dxf_paths = _floor_dxf_paths(data)
    floor_payloads = []
    fit_failures = []

    for floor in selected_floors:
        dxf_path = dxf_paths.get(floor, "")
        entities: List[dict] = []
        if dxf_path:
            path = Path(dxf_path)
            if not path.exists():
                raise FileNotFoundError(f"Floor {floor} DXF does not exist: {path}")
            entities = list(DXFScene.load_content(str(path)).get("entities", []))
        bounds = _content_bounds(
            data,
            floor,
            entities,
            include_placement_zones=include_placement_zones,
        )
        if bounds is None:
            bounds = (0.0, 0.0, 1.0, 1.0)
        min_x, min_y, max_x, max_y = bounds
        required_width = (max_x - min_x + 4.0) * points_per_metre
        required_height = (max_y - min_y + 4.0) * points_per_metre
        if (
            required_width > usable_drawing_width + 0.1
            or required_height > usable_drawing_height + 0.1
        ):
            minimum_scale = int(
                math.ceil(
                    max(
                        (max_x - min_x + 4.0)
                        * 1000.0
                        / (usable_drawing_width / mm),
                        (max_y - min_y + 4.0)
                        * 1000.0
                        / (usable_drawing_height / mm),
                    )
                )
            )
            fit_failures.append((floor, minimum_scale))
        floor_payloads.append((floor, dxf_path, entities, bounds))

    if fit_failures:
        details = ", ".join(
            f"Floor {floor} needs approximately 1:{minimum} or smaller"
            for floor, minimum in fit_failures
        )
        raise ValueError(
            f"The selected 1:{scale} scale does not fit every floor on {paper_size}. "
            + details
            + "."
        )

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    pdf = canvas.Canvas(str(destination), pagesize=page_size, pageCompression=1)
    project_name = _text(data.get("project", {}).get("name")) or (
        Path(source_path).stem if source_path else "Cable Routing Project"
    )
    cabinet_counts = _cabinet_counts(data)
    port_counts = _location_port_counts(data)
    switch_counts = _location_switch_counts(data)

    for page_number, (floor, dxf_path, entities, bounds) in enumerate(
        floor_payloads, start=1
    ):
        pdf.setTitle(
            f"{project_name} - "
            f"{'Equipment room placement zones' if include_placement_zones else 'Floor plans'}"
        )
        pdf.setAuthor("CableRouteResolver")
        pdf.setSubject(f"Scaled floor drawings at 1:{scale}")
        pdf.setFillColor(colors.white)
        pdf.rect(0, 0, page_width, page_height, stroke=0, fill=1)
        pdf.setStrokeColor(colors.HexColor("#263440"))
        pdf.setLineWidth(0.35 * mm)
        pdf.rect(
            drawing_left,
            drawing_bottom,
            drawing_width,
            drawing_height,
            stroke=1,
            fill=0,
        )

        min_x, min_y, max_x, max_y = bounds
        centre_x = (min_x + max_x) / 2.0
        centre_y = (min_y + max_y) / 2.0
        page_centre_x = drawing_left + drawing_width / 2.0
        page_centre_y = drawing_bottom + drawing_height / 2.0

        def transform(x: float, y: float):
            return (
                page_centre_x + (float(x) - centre_x) * points_per_metre,
                page_centre_y + (float(y) - centre_y) * points_per_metre,
            )

        clip = pdf.beginPath()
        clip.rect(drawing_left, drawing_bottom, drawing_width, drawing_height)
        pdf.saveState()
        pdf.clipPath(clip, stroke=0, fill=0)
        _draw_dxf(pdf, entities, transform)
        if include_placement_zones:
            _draw_placement_zones(pdf, data, floor, transform)
        _draw_corridors(pdf, data, floor, transform)
        _draw_comms_rooms(
            pdf,
            data,
            floor,
            transform,
            cabinet_counts,
            port_counts,
            switch_counts,
            page_index=page_number - 1,
            page_label=f"Floor {floor}",
            layout_manifest=layout_manifest,
            preview_background=preview_background,
        )
        pdf.restoreState()

        pdf.setFont("Helvetica-Bold", 8)
        pdf.setFillColor(colors.HexColor("#263440"))
        pdf.drawString(drawing_left + 2 * mm, page_height - 9 * mm, f"FLOOR {floor}")
        _draw_key(
            pdf,
            page_size,
            project_name=project_name,
            floor=floor,
            paper_size=paper_size,
            scale=scale,
            revision_number=int(revision_number or 0),
            source_path=source_path,
            source_drawing=dxf_path,
            page_number=page_number,
            page_count=len(floor_payloads),
            include_placement_zones=include_placement_zones,
        )
        pdf.showPage()

    pdf.save()
    return str(destination)
