"""PDF export for the project endpoint-asset register."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Mapping

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import A3, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from models import JsonStore


ASSET_REGISTER_COLUMNS = (
    {"id": "id", "label": "Asset ID", "header": "Asset ID", "width_mm": 18},
    {"id": "name", "label": "Asset name", "header": "Asset name", "width_mm": 60},
    {"id": "adb_code", "label": "ADB code", "header": "ADB code", "width_mm": 20},
    {"id": "group", "label": "Group", "header": "Group", "width_mm": 25},
    {"id": "category", "label": "Category", "header": "Category", "width_mm": 28},
    {"id": "connection", "label": "Connection", "header": "Connection", "width_mm": 20},
    {"id": "quantity", "label": "Library quantity", "header": "Library<br/>qty", "width_mm": 12, "numeric": True},
    {"id": "data_points_each", "label": "Data points each", "header": "Data points<br/>each", "width_mm": 14, "numeric": True},
    {"id": "library_data_points", "label": "Library data points", "header": "Library data<br/>points", "width_mm": 16, "numeric": True},
    {"id": "north_south", "label": "North-south concurrency", "header": "N-S<br/>concurrency", "width_mm": 14, "numeric": True},
    {"id": "east_west", "label": "East-west concurrency", "header": "E-W<br/>concurrency", "width_mm": 14, "numeric": True},
    {"id": "deployed_rooms", "label": "Deployed rooms", "header": "Deployed<br/>rooms", "width_mm": 15, "numeric": True},
    {"id": "deployed_items", "label": "Deployed items", "header": "Deployed<br/>items", "width_mm": 15, "numeric": True},
    {"id": "deployed_data_points", "label": "Deployed data points", "header": "Deployed data<br/>points", "width_mm": 17, "numeric": True},
    {"id": "capabilities", "label": "Capabilities / functions", "header": "Capabilities / functions", "width_mm": 52},
)


def asset_register_column_ids() -> list[str]:
    return [column["id"] for column in ASSET_REGISTER_COLUMNS]


def _selected_columns(columns) -> list[dict]:
    requested = {
        _text(column_id) for column_id in (columns or asset_register_column_ids())
    }
    selected = [
        dict(column)
        for column in ASSET_REGISTER_COLUMNS
        if column["id"] in requested
    ]
    if not selected:
        raise ValueError("Select at least one asset-register column.")
    return selected


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


def _factor_percent(value) -> str:
    try:
        percent = max(0.0, min(1.0, float(value))) * 100.0
    except (TypeError, ValueError):
        percent = 100.0
    return f"{percent:.1f}%"


def _natural_key(value):
    return tuple(
        int(part) if part.isdigit() else part
        for part in re.split(r"(\d+)", _text(value).casefold())
    )


def _capability_text(asset: Mapping) -> str:
    value = asset.get(
        "capability_keywords",
        asset.get("capabilities", asset.get("function_keywords", "")),
    )
    if isinstance(value, (list, tuple, set)):
        values = [_text(item) for item in value if _text(item)]
    else:
        values = [
            item.strip()
            for item in re.split(r"[;,\n]+", _text(value))
            if item.strip()
        ]
    seen = set()
    unique = []
    for item in values:
        key = item.casefold()
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return "; ".join(unique)


def asset_register_rows(project_data: Mapping) -> list[dict]:
    """Return normalised asset rows with calculated deployment totals."""
    store = JsonStore(dict(project_data or {}))
    data = store.data
    category_names = {
        _text(category.get("id")): _text(
            category.get("name", category.get("id", ""))
        )
        for category in data.get("asset_categories", []) or []
        if isinstance(category, Mapping) and _text(category.get("id"))
    }
    deployment = store.asset_deployment_summary()
    rows = []
    for asset in data.get("assets", []) or []:
        if not isinstance(asset, Mapping):
            continue
        asset_id = _text(asset.get("id"))
        quantity = max(0, _int(asset.get("qty", 1), 1))
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
        category_id = _text(asset.get("category_id", asset.get("category", "")))
        deployed = deployment.get(asset_id, {})
        rows.append(
            {
                "id": asset_id,
                "name": _text(asset.get("name")) or asset_id,
                "adb_code": _text(asset.get("ADB_Code", asset.get("adb_code", ""))),
                "group": _text(asset.get("Group", asset.get("group", ""))),
                "category": category_names.get(category_id, category_id)
                or "Uncategorised",
                "connection": _text(
                    asset.get(
                        "connection_type",
                        asset.get("type_of_connection", "wired"),
                    )
                ),
                "quantity": quantity,
                "data_points_each": data_points_each,
                "library_data_points": quantity * data_points_each,
                "north_south": _factor_percent(
                    asset.get("north_south_concurrency_factor", 1.0)
                ),
                "east_west": _factor_percent(
                    asset.get("east_west_concurrency_factor", 1.0)
                ),
                "deployed_rooms": max(0, _int(deployed.get("deployed_rooms", 0))),
                "deployed_items": max(0, _int(deployed.get("deployed_items", 0))),
                "deployed_data_points": max(
                    0, _int(deployed.get("deployed_data_points", 0))
                ),
                "capabilities": _capability_text(asset),
            }
        )
    return sorted(rows, key=lambda row: (_natural_key(row["id"]), row["name"].casefold()))


def _paragraph(value, style):
    text = escape(_text(value)).replace("\n", "<br/>")
    return Paragraph(text or "-", style)


def _footer(canvas, document) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#5b6573"))
    canvas.drawString(document.leftMargin, 9 * mm, "CableRouteResolver asset register")
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


def export_asset_register_pdf(
    project_data: Mapping,
    output_path: str | Path,
    *,
    source_path: str = "",
    revision_number: int = 0,
    columns=None,
) -> Path:
    """Export all project endpoint assets and their calculated deployment totals."""
    data = project_data if isinstance(project_data, Mapping) else {}
    rows = asset_register_rows(data)
    selected_columns = _selected_columns(columns)
    project = data.get("project", {}) if isinstance(data, Mapping) else {}
    project_name = (
        _text(project.get("name")) if isinstance(project, Mapping) else ""
    ) or "Cable Routing Project"

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "AssetRegisterTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#263440"),
        spaceAfter=5,
    )
    metadata_style = ParagraphStyle(
        "AssetRegisterMetadata",
        parent=styles["Normal"],
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#5b6573"),
    )
    heading_style = ParagraphStyle(
        "AssetRegisterHeading",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=15,
        textColor=colors.HexColor("#263440"),
        spaceBefore=5,
        spaceAfter=4,
    )
    body_style = ParagraphStyle(
        "AssetRegisterBody",
        parent=styles["Normal"],
        fontSize=6.7,
        leading=8.2,
    )
    number_style = ParagraphStyle(
        "AssetRegisterNumber",
        parent=body_style,
        alignment=TA_RIGHT,
    )
    header_style = ParagraphStyle(
        "AssetRegisterHeader",
        parent=body_style,
        fontName="Helvetica-Bold",
        fontSize=6.5,
        leading=7.5,
        alignment=TA_CENTER,
        textColor=colors.white,
    )
    metric_label_style = ParagraphStyle(
        "AssetRegisterMetricLabel",
        parent=body_style,
        fontName="Helvetica-Bold",
        alignment=TA_CENTER,
        textColor=colors.HexColor("#425466"),
    )
    metric_value_style = ParagraphStyle(
        "AssetRegisterMetricValue",
        parent=body_style,
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=13,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#1f2933"),
    )

    document = SimpleDocTemplate(
        str(destination),
        pagesize=landscape(A3),
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=13 * mm,
        bottomMargin=15 * mm,
        title=f"{project_name} - Asset Register",
        author="CableRouteResolver",
    )
    document.revision_number = max(0, _int(revision_number))

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    revision_label = str(revision_number) if revision_number else "Not available"
    story = [
        Paragraph(f"{escape(project_name)} - Asset Register", title_style),
        Paragraph(
            f"Generated: {generated} | Project revision: {revision_label}",
            metadata_style,
        ),
    ]
    if source_path:
        story.append(_paragraph(f"Project file: {source_path}", metadata_style))

    metric_labels = [
        "Asset records",
        "Categories used",
        "Library items",
        "Library data points",
        "Deployed items",
        "Deployed data points",
    ]
    metric_values = [
        len(rows),
        len({row["category"] for row in rows}),
        sum(row["quantity"] for row in rows),
        sum(row["library_data_points"] for row in rows),
        sum(row["deployed_items"] for row in rows),
        sum(row["deployed_data_points"] for row in rows),
    ]
    story.append(Spacer(1, 4 * mm))
    metrics = Table(
        [
            [_paragraph(label, metric_label_style) for label in metric_labels],
            [_paragraph(value, metric_value_style) for value in metric_values],
        ],
        colWidths=[document.width / len(metric_labels)] * len(metric_labels),
        hAlign="LEFT",
    )
    metrics.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f5f7f9")),
                ("BOX", (0, 0), (-1, -1), 0.4, colors.HexColor("#ccd3da")),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#dfe4e8")),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.extend([metrics, Paragraph("Project Asset Library", heading_style)])

    table_rows = [
        [Paragraph(column["header"], header_style) for column in selected_columns]
    ]
    for row in rows:
        table_rows.append(
            [
                _paragraph(
                    row[column["id"]],
                    number_style if column.get("numeric") else body_style,
                )
                for column in selected_columns
            ]
        )
    if not rows:
        table_rows.append(
            [_paragraph("No project assets are configured.", body_style)]
            + [
                _paragraph("", body_style)
                for _index in range(len(selected_columns) - 1)
            ]
        )

    register = Table(
        table_rows,
        colWidths=[column["width_mm"] * mm for column in selected_columns],
        repeatRows=1,
        hAlign="LEFT",
    )
    register.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#263440")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#ccd3da")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                (
                    "ROWBACKGROUNDS",
                    (0, 1),
                    (-1, -1),
                    [colors.white, colors.HexColor("#f5f7f9")],
                ),
                ("LEFTPADDING", (0, 0), (-1, -1), 3),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    story.append(register)
    document.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return destination
