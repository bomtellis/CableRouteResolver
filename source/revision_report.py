from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _project_name(data: Mapping | None) -> str:
    if not isinstance(data, Mapping):
        return "Cable Routing Project"
    project = data.get("project", {})
    if not isinstance(project, Mapping):
        return "Cable Routing Project"
    return _text(project.get("name")) or "Cable Routing Project"


def _format_saved_time(value: str) -> str:
    text = _text(value)
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%d %H:%M:%S UTC")


def _page_footer(canvas, document) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#555555"))
    canvas.drawString(document.leftMargin, 12 * mm, "CableRouteResolver revision history")
    canvas.drawRightString(
        document.pagesize[0] - document.rightMargin,
        12 * mm,
        f"Page {document.page}",
    )
    canvas.restoreState()


def export_revision_history_pdf(
    revisions: Iterable[Mapping],
    output_path: str | Path,
    *,
    project_data: Mapping | None = None,
    source_path: str = "",
) -> Path:
    revision_rows = sorted(
        [dict(row) for row in revisions or []],
        key=lambda row: int(row.get("revision_number", 0) or 0),
    )
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "RevisionTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        spaceAfter=10,
        textColor=colors.HexColor("#1f2933"),
    )
    subtitle_style = ParagraphStyle(
        "RevisionSubtitle",
        parent=styles["Normal"],
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#4b5563"),
        spaceAfter=8,
    )
    body_style = ParagraphStyle(
        "RevisionBody",
        parent=styles["Normal"],
        fontSize=8.5,
        leading=11,
    )
    note_style = ParagraphStyle(
        "RevisionNote",
        parent=body_style,
        fontSize=8.2,
        leading=10.5,
    )
    header_style = ParagraphStyle(
        "RevisionHeader",
        parent=body_style,
        fontName="Helvetica-Bold",
        textColor=colors.white,
        alignment=1,
    )

    document = SimpleDocTemplate(
        str(destination),
        pagesize=A4,
        leftMargin=16 * mm,
        rightMargin=16 * mm,
        topMargin=18 * mm,
        bottomMargin=20 * mm,
        title=f"{_project_name(project_data)} revision history",
        author="CableRouteResolver",
    )

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    story = [
        Paragraph(f"{_project_name(project_data)} - Revision History", title_style),
        Paragraph(f"Generated: {generated}", subtitle_style),
    ]
    if source_path:
        story.append(Paragraph(f"Project file: {_text(source_path)}", subtitle_style))

    if not revision_rows:
        story.append(Spacer(1, 6 * mm))
        story.append(Paragraph("No saved revision history is available.", body_style))
    else:
        summary = (
            f"{len(revision_rows)} saved revision"
            f"{'s' if len(revision_rows) != 1 else ''} "
            f"from revision {revision_rows[0].get('revision_number')} "
            f"to revision {revision_rows[-1].get('revision_number')}."
        )
        story.append(Paragraph(summary, subtitle_style))
        story.append(Spacer(1, 4 * mm))

        table_data = [
            [
                Paragraph("Rev", header_style),
                Paragraph("Saved", header_style),
                Paragraph("Change notes", header_style),
                Paragraph("Changed", header_style),
                Paragraph("Deleted", header_style),
                Paragraph("Records", header_style),
            ]
        ]
        for revision in revision_rows:
            table_data.append(
                [
                    Paragraph(_text(revision.get("revision_number")), body_style),
                    Paragraph(_format_saved_time(revision.get("created_utc", "")), body_style),
                    Paragraph(_text(revision.get("notes")) or "No notes recorded.", note_style),
                    Paragraph(_text(revision.get("changed_chunks", 0)), body_style),
                    Paragraph(_text(revision.get("deleted_chunks", 0)), body_style),
                    Paragraph(_text(revision.get("indexed_records", 0)), body_style),
                ]
            )

        table = Table(
            table_data,
            colWidths=[13 * mm, 33 * mm, 88 * mm, 18 * mm, 17 * mm, 19 * mm],
            repeatRows=1,
            hAlign="LEFT",
        )
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2933")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#ffffff")),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        story.append(table)

        story.append(PageBreak())
        story.append(Paragraph("Revision Notes", title_style))
        for revision in revision_rows:
            heading = (
                f"Revision {_text(revision.get('revision_number'))} - "
                f"{_format_saved_time(revision.get('created_utc', ''))}"
            )
            story.append(Paragraph(heading, styles["Heading3"]))
            story.append(Paragraph(_text(revision.get("notes")) or "No notes recorded.", body_style))
            story.append(Spacer(1, 3 * mm))

    document.build(story, onFirstPage=_page_footer, onLaterPages=_page_footer)
    return destination
