"""Reusable Report Studio annotation rendering for any PDF report."""

from __future__ import annotations

import base64
import math
from io import BytesIO
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase.pdfmetrics import stringWidth
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
    sources_by_key = {
        str(row.get("key", "")): row
        for row in callout_manifest or []
        if isinstance(row, dict)
    }
    result = []
    for source in callout_manifest or []:
        if not isinstance(source, dict) or int(source.get("page", -1)) != int(
            page_index
        ):
            continue
        row = resolve_callout_override(
            source, overrides.get(str(source.get("key", "")), {})
        )
        joined_keys = [
            str(key)
            for key in row.get("joined_callout_keys", []) or []
            if str(key)
        ]
        if joined_keys:
            capacity_lines = []
            for key in [str(source.get("key", ""))] + joined_keys:
                member_source = sources_by_key.get(key, {})
                for line in str(member_source.get("text", "")).splitlines():
                    line = line.strip()
                    if (
                        line
                        and "Cabinets:" in line
                        and "Switches:" in line
                    ):
                        capacity_lines.append(line)
            current_lines = []
            capacity_index = 0
            for source_line in str(row.get("text", "")).splitlines():
                line = source_line.strip()
                if not line:
                    continue
                if "Cabinets:" in line and "Switches:" in line:
                    if capacity_index < len(capacity_lines):
                        current_lines.append(capacity_lines[capacity_index])
                        capacity_index += 1
                    continue
                current_lines.append(line)
            current_lines.extend(capacity_lines[capacity_index:])
            row["text"] = "\n".join(current_lines)
        fitted_width, fitted_height = fitted_generated_callout_size(
            row.get("text", ""), row.get("font_size_pt", 7.0)
        )
        auto_fit = bool(row.get("auto_fit_text", False)) or (
            bool(joined_keys) and "auto_fit_text" not in row
        )
        previous_width = float(row.get("width_pt", 1.0) or 1.0)
        previous_height = float(row.get("height_pt", 1.0) or 1.0)
        row["width_pt"] = (
            fitted_width
            if auto_fit
            else max(18.0, float(row.get("width_pt", fitted_width) or fitted_width))
        )
        row["height_pt"] = (
            fitted_height
            if auto_fit
            else max(5.0, float(row.get("height_pt", fitted_height) or fitted_height))
        )
        if auto_fit:
            if str(row.get("rail", "")) == "left":
                row["x_pt"] = max(0.0, float(row.get("x_pt", 0.0) or 0.0) + (
                    previous_width - fitted_width
                ))
            row["y_pt"] = max(0.0, float(row.get("y_pt", 0.0) or 0.0) + (
                previous_height - fitted_height
            ))
        if bool(row.get("visible", True)) and not bool(row.get("joined_into_key")):
            result.append(row)
    return result


def resolve_callout_override(source, override=None):
    """Apply saved layout changes while retaining mandatory generated details."""
    row = dict(source or {})
    row.update(dict(override or {}))
    if (
        str(source.get("kind", "")).strip() == "equipment_room"
        and not (row.get("joined_callout_keys", []) or [])
    ):
        generated_lines = str(source.get("text", "")).splitlines()
        capacity_line = next(
            (
                line.strip()
                for line in generated_lines
                if "Cabinets:" in line and "Switches:" in line
            ),
            "",
        )
        current_lines = [
            line.strip()
            for line in str(row.get("text", "")).splitlines()
            if line.strip()
            and not ("Cabinets:" in line and "Switches:" in line)
        ]
        if capacity_line:
            current_lines.append(capacity_line)
            row["text"] = "\n".join(current_lines)
    return row


def fitted_generated_callout_size(text, font_size=7.0):
    """Return a compact box that keeps every bold generated-callout line visible."""
    font_size = max(4.0, float(font_size or 7.0))
    lines = str(text or "").splitlines() or [""]
    width = max(
        18.0,
        max(stringWidth(line, "Helvetica-Bold", font_size) for line in lines)
        + 6.0,
    )
    height = max(5.0, len(lines) * font_size * 1.25 + 4.0)
    return width, height


def _draw_wrapped_text(
    c,
    text,
    x,
    y,
    width,
    height,
    font_size,
    colour,
    font_name="Helvetica",
    wrap_text=True,
):
    lines = str(text or "").splitlines() or [""]
    fitted = max(4.0, float(font_size))
    c.setFillColor(colour)
    c.setFont(font_name, fitted)
    top = y + height - fitted - 2.0
    for source_line in lines:
        if not wrap_text:
            if top < y + 1.0:
                return
            c.drawString(x + 2.0, top, source_line)
            top -= fitted * 1.25
            continue
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
    anchor_x = float(row.get("anchor_x_pt", x))
    anchor_y = float(row.get("anchor_y_pt", y))
    colour = _colour(row.get("colour"), "#2474a8")
    line_width = max(0.25, float(row.get("line_width_pt", 1.2) or 1.2))
    font_size = max(4.0, float(row.get("font_size_pt", 7.0) or 7.0))
    leaders = [route for route in row.get("leaders_pt", []) or [] if isinstance(route, dict)]
    if not leaders:
        leaders = [
            {
                "anchor_x_pt": anchor_x,
                "anchor_y_pt": anchor_y,
                "points_pt": row.get("leader_points_pt", []) or [],
            }
        ]
    c.saveState()
    c.setStrokeColor(colour)
    c.setLineWidth(line_width)
    path = c.beginPath()
    for route in leaders:
        route_anchor_x = float(route.get("anchor_x_pt", anchor_x))
        route_anchor_y = float(route.get("anchor_y_pt", anchor_y))
        points = [
            (float(point[0]), float(point[1]))
            for point in route.get("points_pt", []) or []
            if isinstance(point, (list, tuple)) and len(point) >= 2
        ]
        reference_x, reference_y = points[-1] if points else (
            route_anchor_x,
            route_anchor_y,
        )
        centre_x, centre_y = x + width / 2.0, y + height / 2.0
        dx, dy = centre_x - reference_x, centre_y - reference_y
        if abs(dx) > abs(dy):
            attach_x = x if dx >= 0 else x + width
            attach_y = min(max(reference_y, y), y + height)
            elbow_x, elbow_y = attach_x, route_anchor_y
        else:
            attach_x = min(max(reference_x, x), x + width)
            attach_y = y if dy >= 0 else y + height
            elbow_x, elbow_y = route_anchor_x, attach_y
        path.moveTo(route_anchor_x, route_anchor_y)
        if points:
            for point in points:
                path.lineTo(*point)
        else:
            path.lineTo(elbow_x, elbow_y)
        path.lineTo(attach_x, attach_y)
    c.drawPath(path, stroke=1, fill=0)
    c.setFillColor(colors.white)
    c.rect(x, y, width, height, stroke=1, fill=1)
    clip = c.beginPath()
    clip.rect(x, y, width, height)
    c.clipPath(clip, stroke=0, fill=0)
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
        wrap_text=bool(row.get("wrap_text", True)),
    )
    c.restoreState()


def _draw_network_snippet(c, row, x, y, width, height, colour, line_width):
    if bool(row.get("show_leader", True)):
        anchor_x = float(row.get("anchor_x_pt", x) or x)
        anchor_y = float(row.get("anchor_y_pt", y) or y)
        points = [
            (float(point[0]), float(point[1]))
            for point in row.get("leader_points_pt", []) or []
            if isinstance(point, (list, tuple)) and len(point) >= 2
        ]
        reference_x, reference_y = points[-1] if points else (anchor_x, anchor_y)
        centre_x, centre_y = x + width / 2.0, y + height / 2.0
        dx, dy = centre_x - reference_x, centre_y - reference_y
        if abs(dx) > abs(dy):
            attach_x = x if dx >= 0 else x + width
            attach_y = min(max(reference_y, y), y + height)
            elbow = (anchor_x, attach_y)
        else:
            attach_x = min(max(reference_x, x), x + width)
            attach_y = y if dy >= 0 else y + height
            elbow = (attach_x, anchor_y)
        path = c.beginPath()
        path.moveTo(anchor_x, anchor_y)
        if points:
            for point in points:
                path.lineTo(*point)
        else:
            path.lineTo(*elbow)
        path.lineTo(attach_x, attach_y)
        c.drawPath(path, stroke=1, fill=0)
        c.setFillColor(colour)
        c.circle(anchor_x, anchor_y, 2.2, stroke=0, fill=1)

    c.setFillColor(colors.white)
    c.roundRect(x, y, width, height, 4.0, stroke=1, fill=1)
    padding = 4.0
    title_height = 15.0
    details = str(row.get("resolved_callouts", "") or "").strip()
    detail_lines = details.splitlines() if details else []
    details_height = len(detail_lines) * 14.0 + (4.0 if detail_lines else 0.0)
    c.setFillColor(colour)
    c.setFont("Helvetica-Bold", 7.2)
    source_title = str(row.get("title", "Network view") or "Network view")
    title = source_title
    cut = len(source_title)
    denominator = int(row.get("scale_denominator", 0) or 0)
    title_width = width - padding * 2.0 - (48.0 if denominator > 0 else 0.0)
    while cut > 1 and c.stringWidth(title, "Helvetica-Bold", 7.2) > title_width:
        cut -= 1
        title = source_title[:cut].rstrip() + "..."
    c.drawString(x + padding, y + height - 10.5, title)
    if denominator > 0:
        scale_label = f"Scale 1:{denominator}"
        c.setFont("Helvetica", 6.0)
        c.drawRightString(x + width - padding, y + height - 10.5, scale_label)

    image_x = x + padding
    image_y = y + details_height + padding
    image_width = max(1.0, width - padding * 2.0)
    image_height = max(1.0, height - title_height - details_height - padding * 2.0)
    try:
        image_data = base64.b64decode(str(row.get("image_png_base64", "")))
        image = ImageReader(BytesIO(image_data))
        source_width, source_height = image.getSize()
        requested_width = float(row.get("drawing_content_width_pt", 0.0) or 0.0)
        requested_height = float(row.get("drawing_content_height_pt", 0.0) or 0.0)
        target_width = min(image_width, requested_width) if requested_width > 0.0 else image_width
        target_height = min(image_height, requested_height) if requested_height > 0.0 else image_height
        factor = min(
            target_width / max(1.0, float(source_width)),
            target_height / max(1.0, float(source_height)),
        )
        draw_width = max(1.0, float(source_width) * factor)
        draw_height = max(1.0, float(source_height) * factor)
        c.drawImage(
            image,
            image_x + (image_width - draw_width) / 2.0,
            image_y + (image_height - draw_height) / 2.0,
            width=draw_width,
            height=draw_height,
            preserveAspectRatio=True,
            mask="auto",
        )
    except Exception:
        c.setFillColor(colors.HexColor("#f1f5f9"))
        c.rect(image_x, image_y, image_width, image_height, stroke=0, fill=1)
        c.setFillColor(colors.HexColor("#64748b"))
        c.setFont("Helvetica", 7.0)
        c.drawCentredString(x + width / 2.0, image_y + image_height / 2.0, "Network view unavailable")
    if detail_lines:
        _draw_wrapped_text(
            c,
            details,
            x + padding,
            y + 1.0,
            width - padding * 2.0,
            details_height - 2.0,
            6.2,
            colors.HexColor("#334155"),
        )


def _draw_page_reference(c, row, x, y, width, height, colour):
    target_number = int(row.get("target_page", 0) or 0) + 1
    target_title = str(row.get("target_page_title", "") or "").strip()
    c.setFillColor(colors.HexColor("#eff6ff"))
    c.roundRect(x, y, width, height, 5.0, stroke=1, fill=1)
    c.setFillColor(colour)
    c.setFont("Helvetica-Bold", min(11.0, max(6.0, height * 0.22)))
    c.drawString(x + 7.0, y + height - min(15.0, height * 0.36), f"Go to page {target_number}  >")
    if target_title:
        c.setFillColor(colors.HexColor("#334155"))
        c.setFont("Helvetica", min(8.0, max(5.0, height * 0.17)))
        c.drawString(x + 7.0, y + 7.0, target_title[:80])


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
        elif kind == "network_snippet":
            _draw_network_snippet(
                c, row, x, y, width, height, colour, line_width
            )
        elif kind == "page_reference":
            _draw_page_reference(c, row, x, y, width, height, colour)
        c.restoreState()


def append_pdf_studio_blank_pages(input_path, output_path, extra_pages):
    """Append configured blank sheets for Report Studio's live preview."""
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError as exc:
        raise ImportError("PDF page composition requires pypdf.") from exc
    reader = PdfReader(str(input_path))
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    for row in extra_pages or []:
        if not isinstance(row, dict):
            continue
        writer.add_blank_page(
            width=max(72.0, float(row.get("width_pt", 595.276) or 595.276)),
            height=max(72.0, float(row.get("height_pt", 841.89) or 841.89)),
        )
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as handle:
        writer.write(handle)
    return str(destination)


def add_pdf_studio_page_reference_links(input_path, output_path, settings):
    """Add internal page-link annotations without changing page artwork."""
    try:
        from pypdf import PdfReader, PdfWriter
        from pypdf.annotations import Link
    except ImportError as exc:
        raise ImportError("PDF page links require pypdf.") from exc
    reader = PdfReader(str(input_path)); writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    page_count = len(reader.pages)
    for row in (settings or {}).get("annotations", []) or []:
        if not isinstance(row, dict) or str(row.get("type", "")) != "page_reference" or not bool(row.get("visible", True)):
            continue
        source_page = int(row.get("page", -1)); target_page = int(row.get("target_page", -1))
        if not (0 <= source_page < page_count and 0 <= target_page < page_count):
            continue
        x = float(row.get("x_pt", 0.0) or 0.0); y = float(row.get("y_pt", 0.0) or 0.0)
        width = max(1.0, float(row.get("width_pt", 1.0) or 1.0)); height = max(1.0, float(row.get("height_pt", 1.0) or 1.0))
        writer.add_annotation(source_page, Link(rect=(x, y, x + width, y + height), target_page_index=target_page))
    packet = BytesIO(); writer.write(packet)
    destination = Path(output_path); destination.parent.mkdir(parents=True, exist_ok=True); destination.write_bytes(packet.getvalue())
    return str(destination)


def apply_pdf_studio_annotations(
    input_path, output_path, settings, callout_manifest=None
):
    """Merge Report Studio annotations over an existing PDF without rasterising it."""
    try:
        from pypdf import PdfReader, PdfWriter
        from pypdf._page import PageObject
        from pypdf.annotations import Link
    except ImportError as exc:
        raise ImportError(
            "PDF annotation export requires pypdf. Install the project requirements."
        ) from exc

    source = Path(input_path)
    destination = Path(output_path)
    reader = PdfReader(str(source))
    writer = PdfWriter()
    pages = list(reader.pages)
    for row in (settings or {}).get("extra_pages", []) or []:
        if not isinstance(row, dict):
            continue
        pages.append(
            PageObject.create_blank_page(
                width=max(72.0, float(row.get("width_pt", 595.276) or 595.276)),
                height=max(72.0, float(row.get("height_pt", 841.89) or 841.89)),
            )
        )
    for page_index, page in enumerate(pages):
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
    page_count = len(pages)
    for row in (settings or {}).get("annotations", []) or []:
        if not isinstance(row, dict) or str(row.get("type", "")) != "page_reference" or not bool(row.get("visible", True)):
            continue
        source_page = int(row.get("page", -1))
        target_page = int(row.get("target_page", -1))
        if not (0 <= source_page < page_count and 0 <= target_page < page_count):
            continue
        x = float(row.get("x_pt", 0.0) or 0.0); y = float(row.get("y_pt", 0.0) or 0.0)
        width = max(1.0, float(row.get("width_pt", 1.0) or 1.0)); height = max(1.0, float(row.get("height_pt", 1.0) or 1.0))
        writer.add_annotation(
            page_number=source_page,
            annotation=Link(
                rect=(x, y, x + width, y + height),
                target_page_index=target_page,
            ),
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as handle:
        writer.write(handle)
    return str(destination)
