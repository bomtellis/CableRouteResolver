"""PDF export for room-type asset RFIs and their audit history."""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Mapping

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _p(value, style):
    return Paragraph(escape(_text(value)).replace("\n", "<br/>"), style)


def _project_name(data: Mapping) -> str:
    project = data.get("project", {}) if isinstance(data, Mapping) else {}
    return _text(project.get("name")) if isinstance(project, Mapping) else "Cable Routing Project"


def _footer(canvas, document):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#5b6573"))
    canvas.drawString(document.leftMargin, 10 * mm, "CableRouteResolver room type asset RFI list")
    canvas.drawRightString(document.pagesize[0] - document.rightMargin, 10 * mm, f"Page {document.page}")
    canvas.restoreState()


def _styled_table(rows, widths, *, repeat_rows=1):
    table = Table(rows, colWidths=widths, repeatRows=repeat_rows, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#263440")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#ccd3da")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f7f9")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def export_room_type_asset_rfi_pdf(
    project_data: Mapping,
    output_path: str | Path,
    *,
    source_path: str = "",
    revision_number: int = 0,
) -> Path:
    """Export outstanding RFIs by room type followed by the full audit history."""

    data = project_data if isinstance(project_data, Mapping) else {}
    state = data.get("room_type_asset_rfi", {})
    if not isinstance(state, Mapping):
        state = {}
    queries = [dict(item) for item in state.get("queries", []) or [] if isinstance(item, Mapping)]
    history = [dict(item) for item in state.get("history", []) or [] if isinstance(item, Mapping)]
    outstanding = [
        item
        for item in queries
        if _text(item.get("status") or "outstanding").casefold() != "resolved"
    ]
    outstanding.sort(
        key=lambda item: (
            _text(item.get("room_type_name")).casefold(),
            _text(item.get("room_type_id")).casefold(),
            _text(item.get("asset_name")).casefold(),
            _text(item.get("asset_id")).casefold(),
            _text(item.get("id")).casefold(),
        )
    )
    history.sort(key=lambda item: (_text(item.get("timestamp")), _text(item.get("rfi_id"))))

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    styles = getSampleStyleSheet()
    title = ParagraphStyle(
        "RfiTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#263440"),
        spaceAfter=7,
    )
    heading = ParagraphStyle(
        "RfiHeading",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=13,
        leading=16,
        textColor=colors.HexColor("#263440"),
        spaceBefore=6,
        spaceAfter=5,
    )
    body = ParagraphStyle("RfiBody", parent=styles["BodyText"], fontSize=8, leading=10)
    header = ParagraphStyle(
        "RfiHeader", parent=body, fontName="Helvetica-Bold", textColor=colors.white, alignment=1
    )
    metadata = ParagraphStyle(
        "RfiMetadata", parent=body, fontSize=8.5, leading=11, textColor=colors.HexColor("#5b6573")
    )

    document = SimpleDocTemplate(
        str(destination),
        pagesize=landscape(A4),
        leftMargin=13 * mm,
        rightMargin=13 * mm,
        topMargin=14 * mm,
        bottomMargin=16 * mm,
        title=f"{_project_name(data) or 'Cable Routing Project'} - Room Type Asset RFI List",
        author="CableRouteResolver",
    )
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    story = [
        Paragraph(f"{escape(_project_name(data) or 'Cable Routing Project')} - Room Type Asset RFI List", title),
        Paragraph(
            f"Generated: {generated} | Project revision: {revision_number or 'unsaved'} | "
            f"Outstanding queries: {len(outstanding)} | Historical events: {len(history)}",
            metadata,
        ),
    ]
    if source_path:
        story.append(_p(f"Project file: {source_path}", metadata))
    story.extend([Spacer(1, 3 * mm), Paragraph("Current Outstanding Queries", heading)])

    if not outstanding:
        story.append(Paragraph("There are no outstanding room type asset queries.", body))
    else:
        room_keys = []
        grouped = {}
        for item in outstanding:
            key = (_text(item.get("room_type_id")), _text(item.get("room_type_name")))
            if key not in grouped:
                grouped[key] = []
                room_keys.append(key)
            grouped[key].append(item)
        for room_type_id, room_name in room_keys:
            label = f"{room_type_id} - {room_name}".strip(" -") or "Unknown room type"
            rows = [[_p("RFI", header), _p("Asset", header), _p("Query reason", header), _p("Raised", header)]]
            for item in grouped[(room_type_id, room_name)]:
                asset_label = (
                    f"{_text(item.get('asset_id'))} - {_text(item.get('asset_name'))}".strip(" -")
                    or "Room type (general)"
                )
                rows.append(
                    [
                        _p(item.get("id"), body),
                        _p(asset_label, body),
                        _p(item.get("reason"), body),
                        _p(item.get("created_at"), body),
                    ]
                )
            story.append(
                KeepTogether(
                    [
                        Paragraph(escape(label), heading),
                        _styled_table(rows, [22 * mm, 62 * mm, 145 * mm, 38 * mm]),
                        Spacer(1, 3 * mm),
                    ]
                )
            )

    story.extend([PageBreak(), Paragraph("Complete RFI and Asset Change History", title)])
    story.append(
        Paragraph(
            "This append-only audit list records query reasons, resolutions, and reasons supplied when assets were added, removed, or assignments were replaced.",
            metadata,
        )
    )
    story.append(Spacer(1, 3 * mm))
    if not history:
        story.append(Paragraph("No RFI or room type asset change history has been recorded.", body))
    else:
        rows = [
            [
                _p("Date / time", header),
                _p("RFI", header),
                _p("Action", header),
                _p("Room type", header),
                _p("Asset", header),
                _p("Reason / decision", header),
            ]
        ]
        for item in history:
            room_label = f"{_text(item.get('room_type_id'))} - {_text(item.get('room_type_name'))}".strip(" -")
            asset_label = (
                f"{_text(item.get('asset_id'))} - {_text(item.get('asset_name'))}".strip(" -")
                or "Room type (general)"
            )
            action = _text(item.get("action")).replace("_", " ").title()
            rows.append(
                [
                    _p(item.get("timestamp"), body),
                    _p(item.get("rfi_id"), body),
                    _p(action, body),
                    _p(room_label, body),
                    _p(asset_label, body),
                    _p(item.get("note"), body),
                ]
            )
        story.append(
            _styled_table(
                rows,
                [31 * mm, 19 * mm, 34 * mm, 48 * mm, 48 * mm, 87 * mm],
            )
        )

    document.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return destination
