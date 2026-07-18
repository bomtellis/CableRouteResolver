"""Scaled PDF drawing sheets for comms-room and DER graph extents."""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Sequence

from reportlab.lib import colors
from reportlab.lib.pagesizes import A0, A1, A2, A3, A4, landscape
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas

from dxf_scene import DXFScene
from floor_plan_pdf import (
    _cabinet_counts,
    _draw_dxf,
    _entity_bounds,
    _location_port_counts,
    _location_switch_counts,
)


PAPER_SIZES = {
    "A0": landscape(A0),
    "A1": landscape(A1),
    "A2": landscape(A2),
    "A3": landscape(A3),
    "A4": landscape(A4),
}


def _text(value) -> str:
    return str(value or "").strip()


def _floor_dxf_paths(data: dict) -> Dict[int, str]:
    result = {}
    for row in data.get("floor_dxf_files", []):
        if not isinstance(row, dict):
            continue
        path = _text(row.get("filepath"))
        if path:
            result[int(row.get("floor", 0) or 0)] = path
    return result


def _floor_name(data: dict, floor: int, dxf_path: str) -> str:
    building = data.get("building", {}) if isinstance(data.get("building"), dict) else {}
    floor_names = building.get("floor_names", {})
    if isinstance(floor_names, dict):
        value = _text(floor_names.get(str(floor), floor_names.get(floor)))
        if value:
            return value
    for row in data.get("floors", []):
        if isinstance(row, dict) and int(row.get("floor", row.get("number", 0)) or 0) == floor:
            value = _text(row.get("name"))
            if value:
                return value
    if dxf_path:
        return Path(dxf_path).stem
    return f"Floor {floor}"


def _served_point_count(data: dict, room_name: str) -> int:
    data_points = {
        _text(row.get("name"))
        for row in data.get("data_points", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }
    return len(
        {
            _text(row.get("to"))
            for row in data.get("connections", [])
            if isinstance(row, dict)
            and _text(row.get("from")) == room_name
            and _text(row.get("to")) in data_points
        }
    )


def _floor_data_point_statuses(data: dict, floor: int, room_name: str):
    equipment_rooms = {
        _text(row.get("name"))
        for row in data.get("locations", [])
        if isinstance(row, dict)
        and _text(row.get("name"))
        and _text(row.get("kind")).lower()
        in {"comms_room", "mer", "distributed_equipment_room"}
    }
    sources_by_target = {}
    for row in data.get("connections", []):
        if not isinstance(row, dict):
            continue
        target = _text(row.get("to"))
        source = _text(row.get("from"))
        if target and source in equipment_rooms:
            sources_by_target.setdefault(target, set()).add(source)

    result = []
    for source in data.get("data_points", []):
        if not isinstance(source, dict) or int(source.get("floor", 0) or 0) != floor:
            continue
        point = dict(source)
        name = _text(point.get("name"))
        if not name:
            continue
        connected_rooms = sources_by_target.get(name, set())
        if room_name in connected_rooms:
            status = "current"
            other_rooms = sorted(connected_rooms - {room_name})
        elif connected_rooms:
            status = "other"
            other_rooms = sorted(connected_rooms)
        else:
            status = "unconnected"
            other_rooms = []
        point["_extent_status"] = status
        point["_extent_other_rooms"] = other_rooms
        result.append(point)
    return result


def _status_counts(points):
    return {
        status: sum(point.get("_extent_status") == status for point in points)
        for status in ("current", "unconnected", "other")
    }


def _polyline_bounds(polylines: Iterable[Sequence[Sequence[float]]]):
    result = []
    for polyline in polylines:
        for point in polyline:
            if isinstance(point, (list, tuple)) and len(point) >= 2:
                x, y = float(point[0]), float(point[1])
                result.append((x, y, x, y))
    return result


def _draw_polylines(c, polylines, transform, colour, width_mm, dash=None):
    c.saveState()
    c.setStrokeColor(colors.HexColor(colour))
    c.setLineWidth(width_mm * mm)
    if dash:
        c.setDash(*[value * mm for value in dash])
    for polyline in polylines:
        points = [transform(float(point[0]), float(point[1])) for point in polyline]
        if len(points) < 2:
            continue
        path = c.beginPath()
        path.moveTo(*points[0])
        for point in points[1:]:
            path.lineTo(*point)
        c.drawPath(path, stroke=1, fill=0)
    c.restoreState()


def _draw_maximum_tag(
    c,
    payload,
    transform,
    *,
    page_index=0,
    page_label="",
    layout_manifest=None,
    preview_background=False,
):
    point = payload.get("current_max_point")
    target = _text(payload.get("current_max_target"))
    distance = float(payload.get("current_max_distance_m", 0.0) or 0.0)
    if not point or not target:
        return
    x, y = transform(float(point[0]), float(point[1]))
    leader_x, leader_y = x + 16 * mm, y + 12 * mm
    c.saveState()
    c.setStrokeColor(colors.HexColor("#d35400"))
    c.setFillColor(colors.HexColor("#f39c12"))
    c.setLineWidth(0.45 * mm)
    c.circle(x, y, 2.2 * mm, stroke=1, fill=1)
    label = f"MAX CURRENT: {target} - {distance:.2f} m"
    width = max(48 * mm, c.stringWidth(label, "Helvetica-Bold", 7) + 5 * mm)
    callout = {
        "key": f"room-extents-maximum:{int(page_index)}:{_text(payload.get('name'))}:{target}",
        "page": int(page_index),
        "page_label": str(page_label or _text(payload.get("name"))),
        "kind": "maximum_distance",
        "name": f"Maximum distance - {_text(payload.get('name'))}",
        "floor": int(payload.get("floor", 0) or 0),
        "x_pt": leader_x,
        "y_pt": leader_y - 3.5 * mm,
        "width_pt": width,
        "height_pt": 7 * mm,
        "anchor_x_pt": x,
        "anchor_y_pt": y,
        "colour": "#d35400",
        "line_width_pt": 1.2,
        "font_size_pt": 7.0,
        "text": label,
        "visible": True,
    }
    if layout_manifest is not None:
        layout_manifest.append(callout)
    if preview_background:
        c.restoreState()
        return
    c.line(x + 2.2 * mm, y + 2.2 * mm, leader_x, leader_y)
    c.setFillColor(colors.white)
    c.roundRect(leader_x, leader_y - 3.5 * mm, width, 7 * mm, 1 * mm, stroke=1, fill=1)
    c.setFillColor(colors.HexColor("#7c2d12"))
    c.setFont("Helvetica-Bold", 7)
    c.drawString(leader_x + 2.5 * mm, leader_y - 1.1 * mm, label)
    c.restoreState()


def _maximum_tag_bounds(c, payload, transform):
    point = payload.get("current_max_point")
    target = _text(payload.get("current_max_target"))
    if not point or not target:
        return None
    x, y = transform(float(point[0]), float(point[1]))
    leader_x, leader_y = x + 16 * mm, y + 12 * mm
    distance = float(payload.get("current_max_distance_m", 0.0) or 0.0)
    label = f"MAX CURRENT: {target} - {distance:.2f} m"
    width = max(48 * mm, c.stringWidth(label, "Helvetica-Bold", 7) + 5 * mm)
    return leader_x, leader_y - 3.5 * mm, leader_x + width, leader_y + 3.5 * mm


def _draw_room_tag(
    c,
    payload,
    transform,
    drawing_rect,
    *,
    port_count,
    cabinet_count,
    switch_count,
    reserved_boxes=(),
    page_index=0,
    page_label="",
    layout_manifest=None,
    preview_background=False,
):
    """Tag only the selected equipment room with its capacity summary."""
    room_name = _text(payload.get("name")) or "Equipment room"
    anchor_x, anchor_y = transform(
        float(payload.get("x", 0.0)), float(payload.get("y", 0.0))
    )
    left, bottom, right, top = drawing_rect
    title = room_name[:64]
    summary = (
        f"Ports: {int(port_count)} | Cabinets: {int(cabinet_count)} | "
        f"Switches: {int(switch_count)}"
    )
    width = max(
        52 * mm,
        c.stringWidth(title, "Helvetica-Bold", 7) + 6 * mm,
        c.stringWidth(summary, "Helvetica", 6.5) + 6 * mm,
    )
    width = min(width, max(30 * mm, right - left - 4 * mm))
    height = 11 * mm
    gap = 7 * mm
    candidates = [
        (anchor_x + gap, anchor_y + gap),
        (anchor_x + gap, anchor_y - height - gap),
        (anchor_x - width - gap, anchor_y + gap),
        (anchor_x - width - gap, anchor_y - height - gap),
    ]
    reserved = [box for box in reserved_boxes if box]

    def overlap(box, other):
        margin = 1 * mm
        return not (
            box[2] + margin <= other[0]
            or other[2] + margin <= box[0]
            or box[3] + margin <= other[1]
            or other[3] + margin <= box[1]
        )

    selected = None
    best = None
    best_overlap_count = None
    for box_x, box_y in candidates:
        box_x = min(max(box_x, left + 2 * mm), right - width - 2 * mm)
        box_y = min(max(box_y, bottom + 2 * mm), top - height - 2 * mm)
        box = (box_x, box_y, box_x + width, box_y + height)
        overlap_count = sum(overlap(box, other) for other in reserved)
        if overlap_count == 0:
            selected = box
            break
        if best_overlap_count is None or overlap_count < best_overlap_count:
            best = box
            best_overlap_count = overlap_count
    box_x, box_y, box_right, box_top = selected or best

    callout = {
        "key": f"room-extents-room:{int(page_index)}:{room_name}",
        "page": int(page_index),
        "page_label": str(page_label or room_name),
        "kind": "equipment_room",
        "name": f"{room_name} capacity",
        "floor": int(payload.get("floor", 0) or 0),
        "x_pt": box_x,
        "y_pt": box_y,
        "width_pt": width,
        "height_pt": height,
        "anchor_x_pt": anchor_x,
        "anchor_y_pt": anchor_y,
        "colour": "#0b6b50",
        "line_width_pt": 1.1,
        "font_size_pt": 6.5,
        "text": f"{title}\n{summary}",
        "visible": True,
    }
    if layout_manifest is not None:
        layout_manifest.append(callout)
    if preview_background:
        return callout

    attach_x = min(max(anchor_x, box_x), box_right)
    attach_y = min(max(anchor_y, box_y), box_top)
    c.saveState()
    c.setStrokeColor(colors.HexColor("#0b6b50"))
    c.setLineWidth(0.4 * mm)
    c.line(anchor_x, anchor_y, attach_x, attach_y)
    c.setFillColor(colors.white)
    c.roundRect(box_x, box_y, width, height, 1 * mm, stroke=1, fill=1)
    c.setFillColor(colors.HexColor("#0b6b50"))
    c.setFont("Helvetica-Bold", 7)
    c.drawString(box_x + 3 * mm, box_y + 6.4 * mm, title)
    c.setFont("Helvetica", 6.5)
    c.drawString(box_x + 3 * mm, box_y + 2.5 * mm, summary)
    c.restoreState()
    return callout


def _draw_data_point_callouts(
    c, points, transform, drawing_rect, *, reserved_boxes=()
):
    left, bottom, right, top = drawing_rect
    styles = {
        "current": ("#0b7a5c", "circle"),
        "unconnected": ("#d64545", "diamond"),
        "other": ("#d97706", "square"),
    }
    placed_boxes = [tuple(box) for box in reserved_boxes if box]

    def overlaps(first, second):
        margin = 0.7 * mm
        return not (
            first[2] + margin <= second[0]
            or second[2] + margin <= first[0]
            or first[3] + margin <= second[1]
            or second[3] + margin <= first[1]
        )

    ordered = sorted(
        points,
        key=lambda point: (
            {"current": 0, "unconnected": 1, "other": 2}.get(
                point.get("_extent_status"), 3
            ),
            -float(point.get("y", 0.0)),
            float(point.get("x", 0.0)),
            _text(point.get("name")),
        ),
    )
    for point in ordered:
        status = point.get("_extent_status", "unconnected")
        colour, marker = styles.get(status, styles["unconnected"])
        anchor_x, anchor_y = transform(
            float(point.get("x", 0.0)), float(point.get("y", 0.0))
        )
        name = _text(point.get("name")) or "Data point"
        if status == "other":
            rooms = ", ".join(point.get("_extent_other_rooms", [])) or "OTHER ROOM"
            label = f"{name} -> {rooms}"
        elif status == "current":
            label = f"{name} -> THIS ROOM"
        else:
            label = f"{name} - UNCONNECTED"
        label = label[:72]
        font_size = 5.0
        box_width = max(
            18 * mm,
            min(48 * mm, c.stringWidth(label, "Helvetica-Bold", font_size) + 4 * mm),
        )
        box_height = 5 * mm
        gap = 2.5 * mm
        candidates = [
            (anchor_x + gap, anchor_y + gap, "right"),
            (anchor_x + gap, anchor_y - box_height - gap, "right"),
            (anchor_x - box_width - gap, anchor_y + gap, "left"),
            (anchor_x - box_width - gap, anchor_y - box_height - gap, "left"),
            (anchor_x - box_width / 2.0, anchor_y + gap, "above"),
            (anchor_x - box_width / 2.0, anchor_y - box_height - gap, "below"),
        ]
        selected = None
        best_candidate = None
        best_overlap_count = None
        for box_x, box_y, rail in candidates:
            box_x = min(max(box_x, left + 1 * mm), right - box_width - 1 * mm)
            box_y = min(max(box_y, bottom + 1 * mm), top - box_height - 1 * mm)
            candidate = (box_x, box_y, box_x + box_width, box_y + box_height, rail)
            overlap_count = sum(overlaps(candidate, other) for other in placed_boxes)
            if overlap_count == 0:
                selected = candidate
                break
            if best_overlap_count is None or overlap_count < best_overlap_count:
                best_candidate = candidate
                best_overlap_count = overlap_count
        if selected is None:
            selected = best_candidate
        if selected is None:
            continue
        box_x, box_y, box_right, box_top, rail = selected
        placed_boxes.append(selected)

        c.saveState()
        c.setStrokeColor(colors.HexColor(colour))
        c.setFillColor(colors.HexColor(colour))
        c.setLineWidth(0.3 * mm)
        marker_size = 1.35 * mm
        if marker == "circle":
            c.circle(anchor_x, anchor_y, marker_size, stroke=1, fill=1)
        elif marker == "square":
            c.rect(
                anchor_x - marker_size,
                anchor_y - marker_size,
                marker_size * 2,
                marker_size * 2,
                stroke=1,
                fill=1,
            )
        else:
            path = c.beginPath()
            path.moveTo(anchor_x, anchor_y + marker_size)
            path.lineTo(anchor_x + marker_size, anchor_y)
            path.lineTo(anchor_x, anchor_y - marker_size)
            path.lineTo(anchor_x - marker_size, anchor_y)
            path.close()
            c.drawPath(path, stroke=1, fill=1)
            c.setStrokeColor(colors.white)
            c.setLineWidth(0.25 * mm)
            c.line(
                anchor_x - marker_size * 0.55,
                anchor_y - marker_size * 0.55,
                anchor_x + marker_size * 0.55,
                anchor_y + marker_size * 0.55,
            )
            c.line(
                anchor_x - marker_size * 0.55,
                anchor_y + marker_size * 0.55,
                anchor_x + marker_size * 0.55,
                anchor_y - marker_size * 0.55,
            )
            c.setStrokeColor(colors.HexColor(colour))

        if rail == "left":
            attach_x, attach_y = box_right, min(max(anchor_y, box_y), box_top)
        elif rail == "right":
            attach_x, attach_y = box_x, min(max(anchor_y, box_y), box_top)
        elif rail == "above":
            attach_x, attach_y = min(max(anchor_x, box_x), box_right), box_y
        else:
            attach_x, attach_y = min(max(anchor_x, box_x), box_right), box_top
        c.line(anchor_x, anchor_y, attach_x, attach_y)
        c.setFillColor(colors.white)
        c.rect(box_x, box_y, box_right - box_x, box_top - box_y, stroke=1, fill=1)
        fitted_size = font_size
        while fitted_size > 4.0 and c.stringWidth(
            label, "Helvetica-Bold", fitted_size
        ) > box_right - box_x - 3 * mm:
            fitted_size -= 0.2
        c.setFont("Helvetica-Bold", fitted_size)
        c.setFillColor(colors.HexColor(colour))
        c.drawString(box_x + 1.5 * mm, box_y + 1.45 * mm, label)
        c.restoreState()


def _draw_title_block(
    c,
    page_size,
    *,
    project_name,
    floor_name,
    floor,
    room_name,
    room_kind,
    port_count,
    cabinet_count,
    switch_count,
    payload,
    paper_size,
    scale,
    revision_number,
    source_path,
    source_drawing,
    sheet_number,
    sheet_count,
):
    width, _height = page_size
    x, y, h = 10 * mm, 7 * mm, 29 * mm
    w = width - 20 * mm
    c.setFillColor(colors.HexColor("#f5f7f9"))
    c.setStrokeColor(colors.HexColor("#263440"))
    c.setLineWidth(0.35 * mm)
    c.rect(x, y, w, h, stroke=1, fill=1)
    title_w = w * 0.42
    c.line(x + title_w, y, x + title_w, y + h)

    c.setFillColor(colors.HexColor("#102a3a"))
    c.setFont("Helvetica-Bold", 12)
    c.drawString(x + 4 * mm, y + 20 * mm, project_name[:64])
    c.setFont("Helvetica-Bold", 9)
    c.drawString(x + 4 * mm, y + 14 * mm, f"{floor_name} (Floor {floor})")
    c.drawString(x + 4 * mm, y + 9 * mm, f"{room_name} - {room_kind}")
    c.setFont("Helvetica", 7)
    c.drawString(
        x + 4 * mm,
        y + 4 * mm,
        f"Ports: {port_count} | Cabinets: {cabinet_count} | Switches: {switch_count}",
    )

    key_x = x + title_w + 4 * mm
    info_x = x + w * 0.72
    extent_rows = [
        ("#35a7ff", "MAX GRAPH", "line"),
        ("#18c37e", "SERVED ROUTES", "line"),
        ("#8bd3ff", "EXTENT BOUNDARY", "dash"),
        ("#f39c12", "FURTHEST POINT", "circle"),
    ]
    for index, (colour, label, marker) in enumerate(extent_rows):
        marker_x = key_x + 4 * mm
        marker_y = y + 23 * mm - index * 6 * mm
        c.setStrokeColor(colors.HexColor(colour))
        c.setFillColor(colors.HexColor(colour))
        c.setLineWidth(0.55 * mm)
        if marker == "circle":
            c.circle(marker_x, marker_y, 1.5 * mm, stroke=0, fill=1)
        else:
            if marker == "dash":
                c.setDash(1.5 * mm, 0.8 * mm)
            c.line(marker_x - 3 * mm, marker_y, marker_x + 3 * mm, marker_y)
            c.setDash()
        c.setFont("Helvetica-Bold", 5.2)
        c.setFillColor(colors.HexColor("#1f2937"))
        c.drawString(marker_x + 5 * mm, marker_y - 1.1 * mm, label)

    rows = [
        ("SCALE / PAPER", f"1:{scale} / {paper_size} landscape"),
        ("DISTANCE LIMIT", f"{float(payload.get('distance_limit_m', 0.0)):.2f} m"),
        ("MAX CURRENT", f"{float(payload.get('current_max_distance_m', 0.0)):.2f} m"),
        ("REVISION", f"R{revision_number}" if revision_number else "Unrevised"),
        ("SOURCE", Path(source_drawing).name if source_drawing else "No mapped DXF"),
        ("SHEET", f"{sheet_number} of {sheet_count}"),
        ("GENERATED", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ("MODEL", Path(source_path).name if source_path else "Unsaved project"),
    ]
    row_height = 3.25 * mm
    for index, (label, value) in enumerate(rows):
        column = index // 4
        row = index % 4
        tx = info_x + column * 47 * mm
        ty = y + 24 * mm - row * 6.3 * mm
        c.setFont("Helvetica-Bold", 5.2)
        c.setFillColor(colors.HexColor("#52616c"))
        c.drawString(tx, ty, label)
        c.setFont("Helvetica", 6.6)
        c.setFillColor(colors.HexColor("#111820"))
        c.drawString(tx, ty - row_height, value[:46])


def export_equipment_room_extents_pdf(
    data: dict,
    extent_payloads: Sequence[dict],
    output_path: str,
    *,
    source_path: str = "",
    paper_size: str = "A1",
    scale: int = 100,
    revision_number: int = 0,
    layout_manifest=None,
    preview_background: bool = False,
) -> str:
    if layout_manifest is not None:
        layout_manifest.clear()
    paper_size = _text(paper_size).upper()
    if paper_size not in PAPER_SIZES:
        raise ValueError(f"Unsupported paper size: {paper_size}")
    scale = int(scale)
    if scale <= 0:
        raise ValueError("Drawing scale must be greater than zero.")
    if not extent_payloads:
        raise ValueError("No comms rooms or DERs are available to export.")

    dxf_paths = _floor_dxf_paths(data)
    dxf_cache = {}
    page_size = PAPER_SIZES[paper_size]
    page_width, page_height = page_size
    drawing_left, drawing_bottom = 10 * mm, 41 * mm
    drawing_width = page_width - 20 * mm
    drawing_height = page_height - 53 * mm
    safe_margin = 10 * mm
    points_per_metre = (1000.0 / scale) * mm
    payloads = []
    fit_failures = []

    for payload in extent_payloads:
        floor = int(payload.get("floor", 0))
        room_name = _text(payload.get("name"))
        floor_points = _floor_data_point_statuses(data, floor, room_name)
        dxf_path = dxf_paths.get(floor, "")
        if dxf_path not in dxf_cache:
            if dxf_path:
                path = Path(dxf_path)
                if not path.exists():
                    raise FileNotFoundError(f"Floor {floor} DXF does not exist: {path}")
                dxf_cache[dxf_path] = list(
                    DXFScene.load_content(str(path)).get("entities", [])
                )
            else:
                dxf_cache[dxf_path] = []
        entities = dxf_cache[dxf_path]
        bounds = _entity_bounds(entities)
        bounds.extend(_polyline_bounds(payload.get("possible_polylines", [])))
        bounds.extend(_polyline_bounds(payload.get("current_polylines", [])))
        point = payload.get("current_max_point")
        if point:
            bounds.append((float(point[0]), float(point[1]), float(point[0]), float(point[1])))
        x, y = float(payload.get("x", 0.0)), float(payload.get("y", 0.0))
        bounds.append((x, y, x, y))
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
            minimum_scale = int(
                math.ceil(
                    max(
                        span_x * 1000.0 / (usable_width / mm),
                        span_y * 1000.0 / (usable_height / mm),
                    )
                )
            )
            fit_failures.append((_text(payload.get("name")), minimum_scale))
        payloads.append((payload, dxf_path, entities, content_bounds, floor_points))

    if fit_failures:
        details = ", ".join(
            f"{name} needs approximately 1:{minimum}"
            for name, minimum in fit_failures
        )
        raise ValueError(
            f"The selected 1:{scale} scale does not fit on {paper_size}. {details}."
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

    for sheet_number, (payload, dxf_path, entities, bounds, floor_points) in enumerate(payloads, 1):
        floor = int(payload.get("floor", 0))
        room_name = _text(payload.get("name"))
        room = next(
            (
                row for row in data.get("locations", [])
                if isinstance(row, dict) and _text(row.get("name")) == room_name
            ),
            {},
        )
        room_kind = _text(room.get("kind")).replace("_", " ").title()
        count_key = (floor, room_name)
        port_count = port_counts.get(count_key, 0)
        cabinet_count = cabinet_counts.get(count_key, 0)
        switch_count = switch_counts.get(count_key, 0)
        min_x, min_y, max_x, max_y = bounds
        centre_x, centre_y = (min_x + max_x) / 2.0, (min_y + max_y) / 2.0
        page_centre_x = drawing_left + drawing_width / 2.0
        page_centre_y = drawing_bottom + drawing_height / 2.0

        def transform(x, y):
            return (
                page_centre_x + (float(x) - centre_x) * points_per_metre,
                page_centre_y + (float(y) - centre_y) * points_per_metre,
            )

        pdf.setTitle(f"{project_name} - Equipment room extents")
        pdf.setAuthor("CableRouteResolver")
        pdf.setSubject(f"{room_name} graph extents at 1:{scale}")
        pdf.setFillColor(colors.white)
        pdf.rect(0, 0, page_width, page_height, stroke=0, fill=1)
        pdf.setStrokeColor(colors.HexColor("#263440"))
        pdf.setLineWidth(0.35 * mm)
        pdf.rect(drawing_left, drawing_bottom, drawing_width, drawing_height, stroke=1, fill=0)
        clip = pdf.beginPath()
        clip.rect(drawing_left, drawing_bottom, drawing_width, drawing_height)
        pdf.saveState()
        pdf.clipPath(clip, stroke=0, fill=0)
        _draw_dxf(pdf, entities, transform)
        _draw_polylines(pdf, payload.get("possible_polylines", []), transform, "#35a7ff", 0.45)
        _draw_polylines(pdf, payload.get("current_polylines", []), transform, "#18c37e", 0.7)
        boundary = payload.get("boundary_polyline", [])
        if boundary:
            _draw_polylines(pdf, [boundary], transform, "#168fd0", 0.55, dash=(3, 1.5))
        room_x, room_y = transform(float(payload.get("x", 0.0)), float(payload.get("y", 0.0)))
        pdf.setFillColor(colors.HexColor("#0b6b50"))
        pdf.setStrokeColor(colors.white)
        pdf.circle(room_x, room_y, 2.8 * mm, stroke=1, fill=1)
        _draw_room_tag(
            pdf,
            payload,
            transform,
            (
                drawing_left,
                drawing_bottom,
                drawing_left + drawing_width,
                drawing_bottom + drawing_height,
            ),
            port_count=port_count,
            cabinet_count=cabinet_count,
            switch_count=switch_count,
            reserved_boxes=[_maximum_tag_bounds(pdf, payload, transform)],
            page_index=sheet_number - 1,
            page_label=f"{room_name} - Floor {floor}",
            layout_manifest=layout_manifest,
            preview_background=preview_background,
        )
        _draw_maximum_tag(
            pdf,
            payload,
            transform,
            page_index=sheet_number - 1,
            page_label=f"{room_name} - Floor {floor}",
            layout_manifest=layout_manifest,
            preview_background=preview_background,
        )
        pdf.restoreState()

        floor_name = _floor_name(data, floor, dxf_path)
        pdf.setFillColor(colors.HexColor("#263440"))
        pdf.setFont("Helvetica-Bold", 8)
        pdf.drawString(
            drawing_left + 2 * mm,
            page_height - 9 * mm,
            f"{floor_name.upper()} - {room_name} EXTENTS",
        )
        _draw_title_block(
            pdf,
            page_size,
            project_name=project_name,
            floor_name=floor_name,
            floor=floor,
            room_name=room_name,
            room_kind=room_kind,
            port_count=port_count,
            cabinet_count=cabinet_count,
            switch_count=switch_count,
            payload=payload,
            paper_size=paper_size,
            scale=scale,
            revision_number=int(revision_number or 0),
            source_path=source_path,
            source_drawing=dxf_path,
            sheet_number=sheet_number,
            sheet_count=len(payloads),
        )
        pdf.showPage()

    pdf.save()
    return str(destination)
