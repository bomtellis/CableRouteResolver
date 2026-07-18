"""Reusable Report Studio annotation rendering for any PDF report."""

from __future__ import annotations

import math
from io import BytesIO
from pathlib import Path

from reportlab.lib import colors
from reportlab.pdfgen import canvas


def _colour(value, default="#d92d20"):
    try:
        return colors.HexColor(str(value or default))
    except Exception:
        return colors.HexColor(default)


def page_annotations(settings, page_index):
    return [
        row
        for row in (settings or {}).get("annotations", []) or []
        if isinstance(row, dict)
        and int(row.get("page", -1)) == int(page_index)
        and bool(row.get("visible", True))
    ]


def page_callouts(callout_manifest, settings, page_index):
    """Resolve generated callout positions against Report Studio overrides."""
    overrides = (settings or {}).get("callouts", {}) or {}
    result = []
    for source in callout_manifest or []:
        if not isinstance(source, dict) or int(source.get("page", -1)) != int(
            page_index
        ):
            continue
        row = dict(source)
        row.update(dict(overrides.get(str(row.get("key", "")), {}) or {}))
        if bool(row.get("visible", True)):
            result.append(row)
    return result


def _draw_wrapped_text(
    c, text, x, y, width, height, font_size, colour, font_name="Helvetica"
):
    lines = str(text or "").splitlines() or [""]
    fitted = max(4.0, float(font_size))
    c.setFillColor(colour)
    c.setFont(font_name, fitted)
    top = y + height - fitted - 2.0
    for source_line in lines:
        words = source_line.split() or [""]
        line = ""
        for word in words:
            candidate = (line + " " + word).strip()
            if line and c.stringWidth(candidate, font_name, fitted) > width - 4.0:
                if top < y + 1.0:
                    return
                c.drawString(x + 2.0, top, line)
                top -= fitted * 1.25
                line = word
            else:
                line = candidate
        if top < y + 1.0:
            return
        c.drawString(x + 2.0, top, line)
        top -= fitted * 1.25


def _cloud_perimeter_points(x, y, width, height, spacing):
    result = []
    count_x = max(2, int(math.ceil(width / spacing)))
    count_y = max(2, int(math.ceil(height / spacing)))
    for index in range(count_x + 1):
        px = x + width * index / count_x
        result.extend(((px, y), (px, y + height)))
    for index in range(1, count_y):
        py = y + height * index / count_y
        result.extend(((x, py), (x + width, py)))
    return result


def _draw_generated_callout(c, row):
    x = float(row.get("x_pt", 0.0) or 0.0)
    y = float(row.get("y_pt", 0.0) or 0.0)
    width = max(1.0, float(row.get("width_pt", 1.0) or 1.0))
    height = max(1.0, float(row.get("height_pt", 1.0) or 1.0))
    anchor_x = float(row.get("anchor_x_pt", x) or x)
    anchor_y = float(row.get("anchor_y_pt", y) or y)
    colour = _colour(row.get("colour"), "#2474a8")
    line_width = max(0.25, float(row.get("line_width_pt", 1.2) or 1.2))
    font_size = max(4.0, float(row.get("font_size_pt", 7.0) or 7.0))
    centre_x, centre_y = x + width / 2.0, y + height / 2.0
    dx, dy = centre_x - anchor_x, centre_y - anchor_y
    if abs(dx) > abs(dy):
        attach_x = x if dx >= 0 else x + width
        attach_y = min(max(anchor_y, y), y + height)
        elbow_x, elbow_y = attach_x, anchor_y
    else:
        attach_x = min(max(anchor_x, x), x + width)
        attach_y = y if dy >= 0 else y + height
        elbow_x, elbow_y = anchor_x, attach_y
    c.saveState()
    c.setStrokeColor(colour)
    c.setLineWidth(line_width)
    path = c.beginPath()
    path.moveTo(anchor_x, anchor_y)
    path.lineTo(elbow_x, elbow_y)
    path.lineTo(attach_x, attach_y)
    c.drawPath(path, stroke=1, fill=0)
    c.setFillColor(colors.white)
    c.rect(x, y, width, height, stroke=1, fill=1)
    _draw_wrapped_text(
        c,
        row.get("text", ""),
        x,
        y,
        width,
        height,
        font_size,
        colour,
        font_name="Helvetica-Bold",
    )
    c.restoreState()


def draw_pdf_studio_annotations(c, page_index, settings, callout_manifest=None):
    """Draw Report Studio annotations onto an active ReportLab canvas page."""
    for row in page_callouts(callout_manifest, settings, page_index):
        _draw_generated_callout(c, row)
    for row in page_annotations(settings, page_index):
        kind = str(row.get("type", "text") or "text").strip().lower()
        x = float(row.get("x_pt", 0.0) or 0.0)
        y = float(row.get("y_pt", 0.0) or 0.0)
        width = max(1.0, float(row.get("width_pt", 1.0) or 1.0))
        height = max(1.0, float(row.get("height_pt", 1.0) or 1.0))
        colour = _colour(row.get("colour"))
        line_width = max(0.25, float(row.get("line_width_pt", 1.5) or 1.5))
        font_size = max(4.0, float(row.get("font_size_pt", 9.0) or 9.0))
        c.saveState()
        c.setStrokeColor(colour)
        c.setFillColor(colour)
        c.setLineWidth(line_width)
        if kind == "text":
            _draw_wrapped_text(
                c, row.get("text", "Text"), x, y, width, height, font_size, colour
            )
        elif kind == "callout":
            anchor_x = float(row.get("anchor_x_pt", x) or x)
            anchor_y = float(row.get("anchor_y_pt", y) or y)
            attach_x = x if anchor_x <= x + width / 2.0 else x + width
            attach_y = min(max(anchor_y, y), y + height)
            path = c.beginPath()
            path.moveTo(anchor_x, anchor_y)
            path.lineTo(attach_x, anchor_y)
            path.lineTo(attach_x, attach_y)
            c.drawPath(path, stroke=1, fill=0)
            c.rect(x, y, width, height, stroke=1, fill=0)
            _draw_wrapped_text(
                c,
                row.get("text", "Callout"),
                x,
                y,
                width,
                height,
                font_size,
                colour,
            )
        elif kind == "rectangle":
            c.rect(x, y, width, height, stroke=1, fill=0)
        elif kind == "revision_cloud":
            radius = max(2.0, min(7.0, float(row.get("cloud_radius_pt", 4.0) or 4.0)))
            for px, py in _cloud_perimeter_points(x, y, width, height, radius * 1.65):
                c.circle(px, py, radius, stroke=1, fill=0)
        elif kind == "polyline":
            points = [
                (float(point[0]), float(point[1]))
                for point in row.get("points_pt", []) or []
                if isinstance(point, (list, tuple)) and len(point) >= 2
            ]
            if len(points) >= 2:
                path = c.beginPath()
                path.moveTo(*points[0])
                for point in points[1:]:
                    path.lineTo(*point)
                c.drawPath(path, stroke=1, fill=0)
        c.restoreState()


def apply_pdf_studio_annotations(
    input_path, output_path, settings, callout_manifest=None
):
    """Merge Report Studio annotations over an existing PDF without rasterising it."""
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError as exc:
        raise ImportError(
            "PDF annotation export requires pypdf. Install the project requirements."
        ) from exc

    source = Path(input_path)
    destination = Path(output_path)
    reader = PdfReader(str(source))
    writer = PdfWriter()
    for page_index, page in enumerate(reader.pages):
        annotations = page_annotations(settings, page_index)
        callouts = page_callouts(callout_manifest, settings, page_index)
        if annotations or callouts:
            width = float(page.mediabox.width)
            height = float(page.mediabox.height)
            packet = BytesIO()
            overlay_canvas = canvas.Canvas(packet, pagesize=(width, height))
            draw_pdf_studio_annotations(
                overlay_canvas,
                page_index,
                settings,
                callout_manifest=callout_manifest,
            )
            overlay_canvas.save()
            packet.seek(0)
            overlay_page = PdfReader(packet).pages[0]
            page.merge_page(overlay_page)
        writer.add_page(page)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as handle:
        writer.write(handle)
    return str(destination)
