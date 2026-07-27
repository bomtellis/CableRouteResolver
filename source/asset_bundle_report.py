"""PDF export showing the assets included in each reusable asset bundle."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Mapping

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from asset_bundles import normalise_asset_bundles


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return int(default)


def _natural_key(value):
    return tuple(
        int(part) if part.isdigit() else part
        for part in re.split(r"(\d+)", _text(value).casefold())
    )


def _paragraph(value, style):
    text = escape(_text(value)).replace("\n", "<br/>")
    return Paragraph(text or "-", style)


def _blank(style):
    return Paragraph("", style)


def asset_bundle_rows(project_data: Mapping) -> list[dict]:
    """Return bundle rows enriched with names, categories, and data-point totals."""
    data = project_data if isinstance(project_data, Mapping) else {}
    category_names = {
        _text(category.get("id")): _text(
            category.get("name", category.get("id", ""))
        )
        for category in data.get("asset_categories", []) or []
        if isinstance(category, Mapping) and _text(category.get("id"))
    }
    assets_by_id = {
        _text(asset.get("id")): asset
        for asset in data.get("assets", []) or []
        if isinstance(asset, Mapping) and _text(asset.get("id"))
    }

    rows = []
    for bundle in normalise_asset_bundles(data.get("asset_bundles", []) or []):
        asset_rows = []
        for assignment in bundle.get("assets", []) or []:
            asset_id = _text(assignment.get("asset_id"))
            asset = assets_by_id.get(asset_id)
            quantity = max(1, _int(assignment.get("qty", 1), 1))
            if asset is None:
                asset_rows.append(
                    {
                        "id": asset_id,
                        "name": "Missing from asset library",
                        "category": "Missing asset",
                        "quantity": quantity,
                        "data_points_each": 0,
                        "total_data_points": 0,
                        "missing": True,
                    }
                )
                continue
            category_id = _text(
                asset.get("category_id", asset.get("category", ""))
            )
            data_points_each = max(
                0,
                _int(
                    asset.get(
                        "data_points",
                        asset.get("data_points_each", asset.get("cables", 1)),
                    ),
                    1,
                ),
            )
            asset_rows.append(
                {
                    "id": asset_id,
                    "name": _text(asset.get("name")) or asset_id,
                    "category": category_names.get(category_id, category_id)
                    or "Uncategorised",
                    "quantity": quantity,
                    "data_points_each": data_points_each,
                    "total_data_points": quantity * data_points_each,
                    "missing": False,
                }
            )
        rows.append(
            {
                **bundle,
                "assets": asset_rows,
                "asset_types": len(asset_rows),
                "total_items": sum(row["quantity"] for row in asset_rows),
                "total_data_points": sum(
                    row["total_data_points"] for row in asset_rows
                ),
            }
        )
    return sorted(
        rows,
        key=lambda bundle: (
            _natural_key(bundle["id"]),
            bundle["name"].casefold(),
        ),
    )


def _footer(canvas, document) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#5b6573"))
    canvas.drawString(document.leftMargin, 9 * mm, "CableRouteResolver asset bundles")
    revision_number = getattr(document, "revision_number", 0)
    if revision_number:
        canvas.drawCentredString(
            document.pagesize[0] / 2,
            9 * mm,
            f"Project revision {revision_number}",
        )
    canvas.drawRightString(
        document.pagesize[0] - document.rightMargin,
        9 * mm,
        f"Page {document.page}",
    )
    canvas.restoreState()


def export_asset_bundle_pdf(
    project_data: Mapping,
    output_path: str | Path,
    *,
    source_path: str = "",
    revision_number: int = 0,
) -> Path:
    """Export the reusable asset bundles and every asset included in each one."""
    data = project_data if isinstance(project_data, Mapping) else {}
    bundles = asset_bundle_rows(data)
    project = data.get("project", {}) if isinstance(data, Mapping) else {}
    project_name = (
        _text(project.get("name")) if isinstance(project, Mapping) else ""
    ) or "Cable Routing Project"

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "AssetBundleTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#263440"),
        spaceAfter=5,
    )
    metadata_style = ParagraphStyle(
        "AssetBundleMetadata",
        parent=styles["Normal"],
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#5b6573"),
    )
    heading_style = ParagraphStyle(
        "AssetBundleHeading",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=15,
        textColor=colors.HexColor("#263440"),
        spaceBefore=8,
        spaceAfter=3,
        keepWithNext=True,
    )
    body_style = ParagraphStyle(
        "AssetBundleBody",
        parent=styles["Normal"],
        fontSize=7.5,
        leading=9.2,
    )
    description_style = ParagraphStyle(
        "AssetBundleDescription",
        parent=body_style,
        textColor=colors.HexColor("#5b6573"),
        spaceAfter=3,
        keepWithNext=True,
    )
    number_style = ParagraphStyle(
        "AssetBundleNumber",
        parent=body_style,
        alignment=TA_RIGHT,
    )
    header_style = ParagraphStyle(
        "AssetBundleHeader",
        parent=body_style,
        fontName="Helvetica-Bold",
        fontSize=7.2,
        leading=8.4,
        alignment=TA_CENTER,
        textColor=colors.white,
    )

    document = SimpleDocTemplate(
        str(destination),
        pagesize=landscape(A4),
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=13 * mm,
        bottomMargin=15 * mm,
        title=f"{project_name} - Asset Bundles",
        author="CableRouteResolver",
    )
    document.revision_number = max(0, _int(revision_number))

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    revision_label = str(revision_number) if revision_number else "Not available"
    story = [
        Paragraph(f"{escape(project_name)} - Asset Bundles", title_style),
        Paragraph(
            f"Generated: {generated} | Project revision: {revision_label}",
            metadata_style,
        ),
    ]
    if source_path:
        story.append(_paragraph(f"Project file: {source_path}", metadata_style))

    distinct_assets = {
        row["id"] for bundle in bundles for row in bundle["assets"]
    }
    missing_references = sum(
        1 for bundle in bundles for row in bundle["assets"] if row["missing"]
    )
    summary_data = [
        [
            _paragraph("Bundles", header_style),
            _paragraph("Distinct assets", header_style),
            _paragraph("Items across bundle recipes", header_style),
            _paragraph("Data points across bundle recipes", header_style),
            _paragraph("Missing references", header_style),
        ],
        [
            _paragraph(len(bundles), number_style),
            _paragraph(len(distinct_assets), number_style),
            _paragraph(sum(bundle["total_items"] for bundle in bundles), number_style),
            _paragraph(
                sum(bundle["total_data_points"] for bundle in bundles), number_style
            ),
            _paragraph(missing_references, number_style),
        ],
    ]
    summary = Table(
        summary_data,
        colWidths=[document.width / 5] * 5,
        hAlign="LEFT",
    )
    summary.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#263440")),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#ccd3da")),
                ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#f5f7f9")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    story.extend([Spacer(1, 4 * mm), summary])

    if not bundles:
        story.extend(
            [
                Paragraph("Bundle Details", heading_style),
                _paragraph("No asset bundles are configured.", body_style),
            ]
        )

    for bundle in bundles:
        story.append(
            Paragraph(
                f"{escape(bundle['name'])} [{escape(bundle['id'])}]",
                heading_style,
            )
        )
        description = bundle.get("description") or "No description provided."
        story.append(_paragraph(description, description_style))
        table_rows = [
            [
                Paragraph("Asset ID", header_style),
                Paragraph("Asset name", header_style),
                Paragraph("Category", header_style),
                Paragraph("Qty", header_style),
                Paragraph("Data points each", header_style),
                Paragraph("Total data points", header_style),
            ]
        ]
        for row in bundle["assets"]:
            table_rows.append(
                [
                    _paragraph(row["id"], body_style),
                    _paragraph(row["name"], body_style),
                    _paragraph(row["category"], body_style),
                    _paragraph(row["quantity"], number_style),
                    _paragraph(row["data_points_each"], number_style),
                    _paragraph(row["total_data_points"], number_style),
                ]
            )
        if not bundle["assets"]:
            table_rows.append(
                [_paragraph("No assets are included in this bundle.", body_style)]
                + [_paragraph("", body_style) for _index in range(5)]
            )
        table_rows.append(
            [
                _paragraph("Bundle total", body_style),
                _paragraph(f"{bundle['asset_types']} asset type(s)", body_style),
                _blank(body_style),
                _paragraph(bundle["total_items"], number_style),
                _blank(body_style),
                _paragraph(bundle["total_data_points"], number_style),
            ]
        )
        table = Table(
            table_rows,
            colWidths=[27 * mm, 72 * mm, 54 * mm, 20 * mm, 31 * mm, 33 * mm],
            repeatRows=1,
            hAlign="LEFT",
        )
        table_style = [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#263440")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#ccd3da")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            (
                "ROWBACKGROUNDS",
                (0, 1),
                (-1, -2),
                [colors.white, colors.HexColor("#f5f7f9")],
            ),
            ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e8edf2")),
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]
        for row_index, row in enumerate(bundle["assets"], start=1):
            if row["missing"]:
                table_style.append(
                    (
                        "BACKGROUND",
                        (0, row_index),
                        (-1, row_index),
                        colors.HexColor("#fff3cd"),
                    )
                )
        table.setStyle(TableStyle(table_style))
        story.append(table)

    document.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return destination
