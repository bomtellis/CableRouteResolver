from __future__ import annotations

from datetime import datetime, timezone
from html import escape
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


def _revision_change_items(value) -> list[str]:
    """Return saved revision notes as stable, ordered change entries."""
    text = _text(value)
    if not text:
        return ["No changes recorded."]
    items = [_text(item) for item in text.split("|") if _text(item)]
    return items or [text]


def _split_top_level_commas(value: str) -> list[str]:
    """Split asset labels without treating commas in descriptions as separators."""
    items = []
    start = 0
    parenthesis_depth = 0
    for index, character in enumerate(value):
        if character == "(":
            parenthesis_depth += 1
        elif character == ")":
            parenthesis_depth = max(0, parenthesis_depth - 1)
        elif character == "," and parenthesis_depth == 0:
            item = _text(value[start:index])
            if item:
                items.append(item)
            start = index + 1
    final_item = _text(value[start:])
    if final_item:
        items.append(final_item)
    return items


def _revision_event_parts(change: str):
    """Return the saved event heading and body from a revision change."""
    text = _text(change)
    heading, separator, body = text.partition(":")
    heading = _text(heading)
    body = _text(body)
    if (
        not separator
        or not heading
        or not body
        or len(heading) > 80
        or any(character in heading for character in "\\/")
    ):
        return None
    return heading, body


def _condensation_parts(change: str):
    """Return heading, retained item, removed items, and reason."""
    event = _revision_event_parts(change)
    if event is None:
        return None
    heading, body = event
    if not heading.endswith("Condensation") or not body.startswith("Condensed "):
        return None
    condensed_text, separator, target_text = body[len("Condensed ") :].rpartition(
        " into "
    )
    if not separator:
        return None
    main_item, reason_separator, reason = target_text.partition(". Reason:")
    main_item = _text(main_item).rstrip(".")
    condensed_items = _split_top_level_commas(condensed_text)
    if not main_item or not condensed_items:
        return None
    return (
        heading,
        main_item,
        condensed_items,
        _text(reason) if reason_separator else "",
    )


def _asset_condensation_parts(change: str):
    """Return main asset, condensed assets, and reason for compatibility."""
    parts = _condensation_parts(change)
    if parts is None or parts[0] != "Asset Condensation":
        return None
    _heading, main_asset, condensed_assets, reason = parts
    return main_asset, condensed_assets, reason


def _asset_expansion_parts(change: str):
    """Return expanded asset, replacement assets, and reason."""
    event = _revision_event_parts(change)
    if event is None:
        return None
    heading, body = event
    if heading != "Asset Expansion" or not body.startswith("Expanded "):
        return None
    source_text, separator, target_text = body[len("Expanded ") :].partition(
        " into "
    )
    if not separator:
        return None
    replacements_text, reason_separator, reason = target_text.partition(". Reason:")
    source_asset = _text(source_text)
    replacement_assets = _split_top_level_commas(replacements_text)
    if not source_asset or len(replacement_assets) != 2:
        return None
    return (
        source_asset,
        replacement_assets,
        _text(reason) if reason_separator else "",
    )


def _revision_change_flowables(
    change: str,
    note_style: ParagraphStyle,
    condensation_action_style: ParagraphStyle,
    condensation_heading_style: ParagraphStyle,
    condensation_bullet_style: ParagraphStyle,
    condensation_reason_style: ParagraphStyle,
):
    expansion = _asset_expansion_parts(change)
    if expansion is not None:
        source_asset, replacement_assets, reason = expansion
        flowables = [
            Paragraph("Asset Expansion", condensation_action_style),
            Paragraph(escape(source_asset), condensation_heading_style),
        ]
        flowables.extend(
            Paragraph(f"- {escape(item)}", condensation_bullet_style)
            for item in replacement_assets
        )
        if reason:
            flowables.append(
                Paragraph(f"Reason: {escape(reason)}", condensation_reason_style)
            )
        return flowables

    condensation = _condensation_parts(change)
    if condensation is not None:
        heading, main_item, condensed_items, reason = condensation
        flowables = [
            Paragraph(escape(heading), condensation_action_style),
            Paragraph(escape(main_item), condensation_heading_style),
        ]
        flowables.extend(
            Paragraph(f"- {escape(item)}", condensation_bullet_style)
            for item in condensed_items
        )
        if reason:
            flowables.append(
                Paragraph(f"Reason: {escape(reason)}", condensation_reason_style)
            )
        return flowables

    event = _revision_event_parts(change)
    if event is None:
        return Paragraph(escape(change), note_style)
    heading, body = event
    summary, reason_separator, reason = body.rpartition(". Reason:")
    if not reason_separator:
        summary, reason = body, ""
    flowables = [
        Paragraph(escape(heading), condensation_action_style),
        Paragraph(escape(_text(summary)), note_style),
    ]
    if reason:
        flowables.append(
            Paragraph(f"Reason: {escape(_text(reason))}", condensation_reason_style)
        )
    return flowables


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
    condensation_action_style = ParagraphStyle(
        "AssetCondensationAction",
        parent=note_style,
        fontName="Helvetica-Bold",
        fontSize=7.8,
        leading=9.5,
        textColor=colors.HexColor("#425466"),
        spaceAfter=1,
    )
    condensation_heading_style = ParagraphStyle(
        "AssetCondensationHeading",
        parent=note_style,
        fontName="Helvetica-Bold",
        fontSize=9,
        leading=11,
        spaceAfter=2,
    )
    condensation_bullet_style = ParagraphStyle(
        "AssetCondensationBullet",
        parent=note_style,
        leftIndent=9,
        firstLineIndent=-6,
        spaceAfter=1,
    )
    condensation_reason_style = ParagraphStyle(
        "AssetCondensationReason",
        parent=note_style,
        fontName="Helvetica-Oblique",
        fontSize=7.8,
        leading=9.5,
        spaceBefore=2,
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
                Paragraph("Changes", header_style),
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
                    Paragraph(str(len(_revision_change_items(revision.get("notes")))), body_style),
                    Paragraph(_text(revision.get("changed_chunks", 0)), body_style),
                    Paragraph(_text(revision.get("deleted_chunks", 0)), body_style),
                    Paragraph(_text(revision.get("indexed_records", 0)), body_style),
                ]
            )

        table = Table(
            table_data,
            colWidths=[13 * mm, 38 * mm, 19 * mm, 25 * mm, 23 * mm, 28 * mm],
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
        story.append(Paragraph("Ordered Revision Changes", title_style))
        story.append(
            Paragraph(
                "Each saved revision is expanded into its recorded changes in storage order.",
                subtitle_style,
            )
        )
        change_table_data = [
            [
                Paragraph("Rev", header_style),
                Paragraph("Order", header_style),
                Paragraph("Change", header_style),
            ]
        ]
        for revision in revision_rows:
            revision_number = _text(revision.get("revision_number"))
            for order, change in enumerate(
                _revision_change_items(revision.get("notes")), start=1
            ):
                change_table_data.append(
                    [
                        Paragraph(escape(revision_number), body_style),
                        Paragraph(str(order), body_style),
                        _revision_change_flowables(
                            change,
                            note_style,
                            condensation_action_style,
                            condensation_heading_style,
                            condensation_bullet_style,
                            condensation_reason_style,
                        ),
                    ]
                )
        change_table = Table(
            change_table_data,
            colWidths=[14 * mm, 14 * mm, 150 * mm],
            repeatRows=1,
            hAlign="LEFT",
        )
        change_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2933")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                    ("ALIGN", (0, 1), (1, -1), "RIGHT"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        story.append(change_table)

    document.build(story, onFirstPage=_page_footer, onLaterPages=_page_footer)
    return destination
