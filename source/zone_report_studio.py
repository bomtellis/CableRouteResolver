"""Interactive layout studio for zone-design option PDF reports."""

from __future__ import annotations

import math
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

from PySide6.QtCore import QCoreApplication, QEvent, QSize, Qt, QTimer
from PySide6.QtGui import QColor, QBrush, QFont, QPainterPath, QPen, QPixmap
from PySide6.QtPdf import QPdfDocument
from PySide6.QtWidgets import (
    QCheckBox,
    QColorDialog,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QGraphicsItem,
    QGraphicsPathItem,
    QGraphicsRectItem,
    QGraphicsScene,
    QGraphicsSimpleTextItem,
    QGraphicsView,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QMessageBox,
    QInputDialog,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSplitter,
    QVBoxLayout,
    QWidget,
)


class _StudioGraphicsView(QGraphicsView):
    def __init__(self, scene, zoom_callback, annotation_controller=None, parent=None):
        super().__init__(scene, parent)
        self.zoom_callback = zoom_callback
        self.annotation_controller = annotation_controller

    def wheelEvent(self, event):
        if event.modifiers() & Qt.ControlModifier:
            self.zoom_callback(1.2 if event.angleDelta().y() > 0 else 1.0 / 1.2)
            event.accept()
            return
        super().wheelEvent(event)

    def mousePressEvent(self, event):
        if self.annotation_controller is not None and self.annotation_controller.annotation_mouse_press(
            event, self.mapToScene(event.position().toPoint())
        ):
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self.annotation_controller is not None and self.annotation_controller.annotation_mouse_move(
            event, self.mapToScene(event.position().toPoint())
        ):
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if self.annotation_controller is not None and self.annotation_controller.annotation_mouse_release(
            event, self.mapToScene(event.position().toPoint())
        ):
            return
        super().mouseReleaseEvent(event)

    def mouseDoubleClickEvent(self, event):
        if self.annotation_controller is not None and self.annotation_controller.annotation_mouse_double_click(
            event, self.mapToScene(event.position().toPoint())
        ):
            return
        super().mouseDoubleClickEvent(event)


class _CalloutItem(QGraphicsRectItem):
    def __init__(self, record, scale, page_height_pt, moved_callback=None):
        self.record = record
        self.scale = float(scale)
        self.page_height_pt = float(page_height_pt)
        self.moved_callback = moved_callback
        width = float(record["width_pt"]) * self.scale
        height = float(record["height_pt"]) * self.scale
        super().__init__(0.0, 0.0, width, height)
        x = float(record["x_pt"]) * self.scale
        y = (
            self.page_height_pt
            - float(record["y_pt"])
            - float(record["height_pt"])
        ) * self.scale
        self.setPos(x, y)
        colour = QColor(str(record.get("colour", "#2474a8")))
        self.setPen(QPen(colour, 2.0))
        self.setBrush(QBrush(QColor(255, 255, 255, 232)))
        self.setFlags(
            QGraphicsItem.ItemIsMovable
            | QGraphicsItem.ItemIsSelectable
            | QGraphicsItem.ItemSendsGeometryChanges
        )
        self.setZValue(20)
        self.leader_item = QGraphicsPathItem(self)
        self.leader_item.setPen(QPen(colour, 1.6))
        self.leader_item.setZValue(-1)
        self.text_item = QGraphicsSimpleTextItem(self)
        self.text_item.setBrush(QBrush(colour))
        font = QFont("Arial", 9)
        font.setBold(True)
        self.text_item.setFont(font)
        self.set_text(str(record.get("text", "")))
        self.set_callout_visible(bool(record.get("visible", True)))
        self._update_leader()

    def set_text(self, value):
        self.record["text"] = str(value)
        self.text_item.setText(str(value))
        self._fit_text()

    def _fit_text(self):
        self.text_item.setScale(1.0)
        bounds = self.text_item.boundingRect()
        available = max(10.0, self.rect().width() - 12.0)
        factor = min(1.0, available / max(1.0, bounds.width()))
        self.text_item.setScale(factor)
        fitted_height = bounds.height() * factor
        self.text_item.setPos(6.0, max(2.0, (self.rect().height() - fitted_height) / 2.0))

    def set_size_points(self, width_pt, height_pt):
        self.setRect(
            0.0,
            0.0,
            max(18.0, float(width_pt)) * self.scale,
            max(5.0, float(height_pt)) * self.scale,
        )
        self._fit_text()
        self._update_leader()

    def set_callout_visible(self, visible):
        self.record["visible"] = bool(visible)
        self.setOpacity(1.0 if visible else 0.22)
        self.setToolTip(
            "Included in the PDF" if visible else "Hidden from the final PDF"
        )

    def sync_record(self):
        self.record["x_pt"] = float(self.pos().x() / self.scale)
        self.record["y_pt"] = float(
            self.page_height_pt
            - (self.pos().y() + self.rect().height()) / self.scale
        )
        self.record["width_pt"] = float(self.rect().width() / self.scale)
        self.record["height_pt"] = float(self.rect().height() / self.scale)
        if "anchor_x_pt" in self.record and "anchor_y_pt" in self.record:
            centre_x = self.record["x_pt"] + self.record["width_pt"] / 2.0
            centre_y = self.record["y_pt"] + self.record["height_pt"] / 2.0
            dx = centre_x - float(self.record["anchor_x_pt"])
            dy = centre_y - float(self.record["anchor_y_pt"])
            if abs(dx) > abs(dy):
                self.record["rail"] = "right" if dx >= 0 else "left"
            else:
                self.record["rail"] = "above" if dy >= 0 else "below"
        self._update_leader()
        return self.record

    def _update_leader(self):
        if not hasattr(self, "leader_item"):
            return
        anchor_x = float(self.record.get("anchor_x_pt", 0.0)) * self.scale
        anchor_y = (
            self.page_height_pt - float(self.record.get("anchor_y_pt", 0.0))
        ) * self.scale
        relative_anchor_x = anchor_x - self.pos().x()
        relative_anchor_y = anchor_y - self.pos().y()
        centre_x = self.pos().x() + self.rect().width() / 2.0
        centre_y = self.pos().y() + self.rect().height() / 2.0
        dx = centre_x - anchor_x
        dy = centre_y - anchor_y
        if abs(dx) > abs(dy):
            rail = "right" if dx >= 0 else "left"
        else:
            rail = "below" if dy >= 0 else "above"
        self.record["rail"] = rail
        if rail == "above":
            attach_x, attach_y = self.rect().width() / 2.0, self.rect().height()
        elif rail == "below":
            attach_x, attach_y = self.rect().width() / 2.0, 0.0
        elif rail == "left":
            attach_x, attach_y = self.rect().width(), self.rect().height() / 2.0
        else:
            attach_x, attach_y = 0.0, self.rect().height() / 2.0
        path = QPainterPath()
        path.moveTo(relative_anchor_x, relative_anchor_y)
        if rail in {"above", "below"}:
            path.lineTo(relative_anchor_x, attach_y)
        else:
            path.lineTo(attach_x, relative_anchor_y)
        path.lineTo(attach_x, attach_y)
        self.leader_item.setPath(path)

    def itemChange(self, change, value):
        result = super().itemChange(change, value)
        if change == QGraphicsItem.ItemPositionHasChanged:
            self._update_leader()
        return result

    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        self.sync_record()
        if self.moved_callback is not None:
            self.moved_callback()


class _AnnotationItem(QGraphicsRectItem):
    def __init__(self, record, scale, page_height_pt, edit_callback=None):
        self.record = record
        self.scale = float(scale)
        self.page_height_pt = float(page_height_pt)
        self.edit_callback = edit_callback
        width = max(8.0, float(record.get("width_pt", 8.0))) * self.scale
        height = max(8.0, float(record.get("height_pt", 8.0))) * self.scale
        super().__init__(0.0, 0.0, width, height)
        self.setPos(
            float(record.get("x_pt", 0.0)) * self.scale,
            (
                self.page_height_pt
                - float(record.get("y_pt", 0.0))
                - float(record.get("height_pt", 8.0))
            )
            * self.scale,
        )
        self.setFlags(
            QGraphicsItem.ItemIsMovable
            | QGraphicsItem.ItemIsSelectable
            | QGraphicsItem.ItemSendsGeometryChanges
        )
        self.setZValue(30)
        self.shape_item = QGraphicsPathItem(self)
        self.text_item = QGraphicsSimpleTextItem(self)
        self._redraw()

    def _redraw(self):
        kind = str(self.record.get("type", "text"))
        colour = QColor(str(self.record.get("colour", "#d92d20")))
        pen = QPen(
            colour,
            max(1.0, float(self.record.get("line_width_pt", 1.5)) * self.scale),
        )
        self.shape_item.setPen(pen)
        self.text_item.setBrush(QBrush(colour))
        font = QFont("Arial", max(6, int(float(self.record.get("font_size_pt", 9)))))
        self.text_item.setFont(font)
        path = QPainterPath()
        width, height = self.rect().width(), self.rect().height()
        if kind == "rectangle":
            path.addRect(0.0, 0.0, width, height)
            self.text_item.setText("")
        elif kind == "revision_cloud":
            radius = max(4.0, min(12.0, 4.0 * self.scale))
            spacing = radius * 1.65
            count_x = max(2, int(width / spacing))
            count_y = max(2, int(height / spacing))
            for index in range(count_x + 1):
                x = width * index / count_x
                path.addEllipse(x - radius, -radius, radius * 2, radius * 2)
                path.addEllipse(x - radius, height - radius, radius * 2, radius * 2)
            for index in range(1, count_y):
                y = height * index / count_y
                path.addEllipse(-radius, y - radius, radius * 2, radius * 2)
                path.addEllipse(width - radius, y - radius, radius * 2, radius * 2)
            self.text_item.setText("")
        elif kind == "polyline":
            points = self.record.get("points_pt", []) or []
            if points:
                origin_x = float(self.record.get("x_pt", 0.0))
                top_y = float(self.record.get("y_pt", 0.0)) + float(
                    self.record.get("height_pt", 0.0)
                )
                first_x = (float(points[0][0]) - origin_x) * self.scale
                first_y = (top_y - float(points[0][1])) * self.scale
                path.moveTo(first_x, first_y)
                for point in points[1:]:
                    path.lineTo(
                        (float(point[0]) - origin_x) * self.scale,
                        (top_y - float(point[1])) * self.scale,
                    )
            self.text_item.setText("")
        elif kind == "callout":
            path.addRect(0.0, 0.0, width, height)
            anchor_x = (
                float(self.record.get("anchor_x_pt", 0.0)) * self.scale
                - self.pos().x()
            )
            anchor_y = (
                (
                    self.page_height_pt
                    - float(self.record.get("anchor_y_pt", 0.0))
                )
                * self.scale
                - self.pos().y()
            )
            attach_x = 0.0 if anchor_x <= width / 2.0 else width
            attach_y = min(max(anchor_y, 0.0), height)
            path.moveTo(anchor_x, anchor_y)
            path.lineTo(attach_x, anchor_y)
            path.lineTo(attach_x, attach_y)
            self._set_text()
        else:
            self._set_text()
            editor_pen = QPen(QColor("#667085"), 1.0, Qt.DashLine)
            self.setPen(editor_pen)
        if kind != "text":
            self.setPen(QPen(Qt.NoPen))
        self.shape_item.setPath(path)

    def _set_text(self):
        self.text_item.setScale(1.0)
        self.text_item.setText(str(self.record.get("text", "")))
        self.text_item.setPos(4.0, 3.0)
        bounds = self.text_item.boundingRect()
        available = max(10.0, self.rect().width() - 8.0)
        self.text_item.setScale(min(1.0, available / max(1.0, bounds.width())))

    def sync_record(self):
        old_x = float(self.record.get("x_pt", 0.0))
        old_y = float(self.record.get("y_pt", 0.0))
        new_x = float(self.pos().x() / self.scale)
        new_y = float(
            self.page_height_pt
            - (self.pos().y() + self.rect().height()) / self.scale
        )
        if self.record.get("type") == "polyline":
            delta_x, delta_y = new_x - old_x, new_y - old_y
            self.record["points_pt"] = [
                [float(point[0]) + delta_x, float(point[1]) + delta_y]
                for point in self.record.get("points_pt", []) or []
            ]
        self.record["x_pt"] = new_x
        self.record["y_pt"] = new_y
        self.record["width_pt"] = float(self.rect().width() / self.scale)
        self.record["height_pt"] = float(self.rect().height() / self.scale)
        self._redraw()
        return self.record

    def itemChange(self, change, value):
        result = super().itemChange(change, value)
        if change == QGraphicsItem.ItemPositionHasChanged and hasattr(self, "shape_item"):
            self._redraw()
        return result

    def mouseDoubleClickEvent(self, event):
        if self.edit_callback is not None:
            self.edit_callback(self)
            event.accept()
            return
        super().mouseDoubleClickEvent(event)


class PdfReportStudioDialog(QDialog):
    """Preview, annotate, and customise any generated PDF report."""

    DEFAULTS = {
        "show_cover": True,
        "show_zone_boundaries": True,
        "show_zone_callouts": True,
        "show_room_callouts": True,
        "show_max_distance_callouts": True,
        "show_routing_graph": True,
        "show_other_data_points": True,
        "show_title_block": True,
        "font_scale": 1.0,
        "callouts": {},
        "annotations": [],
    }

    def __init__(
        self,
        preview_builder,
        initial_settings=None,
        parent=None,
        report_title="PDF Report Studio",
        show_report_controls=True,
    ):
        super().__init__(parent)
        self.preview_builder = preview_builder
        self.settings = deepcopy(self.DEFAULTS)
        self.settings.update(deepcopy(initial_settings or {}))
        self.settings["callouts"] = deepcopy(
            (initial_settings or {}).get("callouts", {}) or {}
        )
        self.settings["annotations"] = deepcopy(
            (initial_settings or {}).get("annotations", []) or []
        )
        self.preview_path = ""
        self.manifest = []
        self.document = QPdfDocument(self)
        self.scene = QGraphicsScene(self)
        self.callout_items = []
        self.annotation_items = []
        self.current_page = 0
        self.zoom_factor = 1.0
        self._updating_properties = False
        self._property_item = None
        self.active_annotation_tool = "select"
        self.annotation_start = None
        self.callout_anchor = None
        self.polyline_points = []
        self.page_render_scale = 1.0
        self.current_page_height_pt = 1.0
        self.base_render_width = 1500
        self.background_item = None
        self.background_render_factor = 1.0
        self.detail_render_timer = QTimer(self)
        self.detail_render_timer.setSingleShot(True)
        self.detail_render_timer.setInterval(160)
        self.detail_render_timer.timeout.connect(self._refresh_background_detail)

        self.report_title = str(report_title or "PDF Report Studio")
        self.show_report_controls = bool(show_report_controls)
        self.setWindowTitle(self.report_title)
        self.resize(1500, 900)
        self.setMinimumSize(1100, 700)
        root = QVBoxLayout(self)
        intro = QLabel(
            "Review the generated pages before export. Add and position text, point "
            "callouts, revision clouds, rectangles, and polylines."
            if not self.show_report_controls
            else "Review the generated pages before export. Drag callouts on the sheet, "
            "edit their text, hide unwanted information, and control report layers."
        )
        intro.setWordWrap(True)
        root.addWidget(intro)

        splitter = QSplitter(Qt.Horizontal)
        root.addWidget(splitter, 1)

        page_panel = QWidget()
        page_layout = QVBoxLayout(page_panel)
        page_layout.addWidget(QLabel("Pages"))
        self.page_list = QListWidget()
        self.page_list.currentRowChanged.connect(self._show_page)
        page_layout.addWidget(self.page_list, 1)
        splitter.addWidget(page_panel)

        preview_panel = QWidget()
        preview_layout = QVBoxLayout(preview_panel)
        preview_layout.setContentsMargins(0, 0, 0, 0)
        zoom_row = QHBoxLayout()
        self.zoom_out_button = QPushButton("Zoom out")
        self.zoom_in_button = QPushButton("Zoom in")
        self.fit_page_button = QPushButton("Fit page")
        self.zoom_combo = QComboBox()
        self.zoom_combo.addItem("Fit", None)
        for percentage in (50, 75, 100, 125, 150, 200, 300, 400):
            self.zoom_combo.addItem(f"{percentage}%", percentage)
        self.zoom_status = QLabel("Fit (100%)")
        self.zoom_out_button.clicked.connect(lambda: self._zoom_by(1.0 / 1.25))
        self.zoom_in_button.clicked.connect(lambda: self._zoom_by(1.25))
        self.fit_page_button.clicked.connect(self._fit_page)
        self.zoom_combo.currentIndexChanged.connect(self._zoom_preset_changed)
        zoom_row.addWidget(self.zoom_out_button)
        zoom_row.addWidget(self.zoom_combo)
        zoom_row.addWidget(self.zoom_in_button)
        zoom_row.addWidget(self.fit_page_button)
        zoom_row.addWidget(self.zoom_status)
        zoom_row.addStretch(1)
        zoom_row.addWidget(QLabel("Ctrl+mouse wheel also zooms"))
        preview_layout.addLayout(zoom_row)

        annotation_row = QHBoxLayout()
        annotation_row.addWidget(QLabel("Markup tools:"))
        self.annotation_tool_buttons = {}
        for tool, label in (
            ("select", "Select / move"),
            ("text", "Text"),
            ("callout", "Point callout"),
            ("rectangle", "Rectangle"),
            ("revision_cloud", "Revision cloud"),
            ("polyline", "Polyline"),
        ):
            button = QPushButton(label)
            button.setCheckable(True)
            button.setChecked(tool == "select")
            button.clicked.connect(
                lambda checked=False, value=tool: self._set_annotation_tool(value)
            )
            self.annotation_tool_buttons[tool] = button
            annotation_row.addWidget(button)
        self.finish_polyline_button = QPushButton("Finish polyline")
        self.finish_polyline_button.clicked.connect(self._finish_polyline)
        self.finish_polyline_button.setEnabled(False)
        annotation_row.addWidget(self.finish_polyline_button)
        self.delete_annotation_button = QPushButton("Delete selected markup")
        self.delete_annotation_button.clicked.connect(self._delete_selected_annotations)
        annotation_row.addWidget(self.delete_annotation_button)
        annotation_row.addStretch(1)
        preview_layout.addLayout(annotation_row)
        self.annotation_status = QLabel(
            "Select / move: choose callouts or markups and drag them into position."
        )
        preview_layout.addWidget(self.annotation_status)

        self.view = _StudioGraphicsView(
            self.scene, self._zoom_by, annotation_controller=self
        )
        self.view.setRenderHints(self.view.renderHints())
        self.view.setDragMode(QGraphicsView.ScrollHandDrag)
        self.view.setBackgroundBrush(QColor("#c7ccd1"))
        self.view.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.view.setResizeAnchor(QGraphicsView.AnchorViewCenter)
        self.scene.selectionChanged.connect(self._selection_changed)
        preview_layout.addWidget(self.view, 1)
        splitter.addWidget(preview_panel)

        controls_scroll = QScrollArea()
        controls_scroll.setWidgetResizable(True)
        controls = QWidget()
        controls_layout = QVBoxLayout(controls)

        layers = QGroupBox("Report contents")
        layers_layout = QVBoxLayout(layers)
        self.layer_checks = {}
        for key, label in (
            ("show_cover", "Cover comparison page"),
            ("show_zone_boundaries", "Placement-zone boundaries"),
            ("show_zone_callouts", "Zone callouts"),
            ("show_room_callouts", "Room capacity callouts"),
            ("show_max_distance_callouts", "Maximum-distance callouts"),
            ("show_routing_graph", "Routing graph"),
            ("show_other_data_points", "Other data points"),
            ("show_title_block", "Sheet title block"),
        ):
            check = QCheckBox(label)
            check.setChecked(bool(self.settings.get(key, True)))
            self.layer_checks[key] = check
            layers_layout.addWidget(check)
        controls_layout.addWidget(layers)

        formatting = QGroupBox("Formatting")
        format_form = QFormLayout(formatting)
        self.font_scale_spin = QDoubleSpinBox()
        self.font_scale_spin.setRange(0.5, 2.5)
        self.font_scale_spin.setSingleStep(0.1)
        self.font_scale_spin.setDecimals(1)
        self.font_scale_spin.setValue(float(self.settings.get("font_scale", 1.0)))
        format_form.addRow("Callout text scale", self.font_scale_spin)
        controls_layout.addWidget(formatting)

        callout_group = QGroupBox("Selected callout")
        callout_layout = QVBoxLayout(callout_group)
        self.callout_name = QLabel("Select a callout on the page")
        self.callout_name.setWordWrap(True)
        callout_layout.addWidget(self.callout_name)
        self.callout_text = QPlainTextEdit()
        self.callout_text.setMaximumHeight(95)
        callout_layout.addWidget(self.callout_text)
        self.callout_visible = QCheckBox("Include in final PDF")
        callout_layout.addWidget(self.callout_visible)
        size_form = QFormLayout()
        self.callout_width = QDoubleSpinBox()
        self.callout_width.setRange(18.0, 250.0)
        self.callout_width.setSuffix(" pt")
        self.callout_height = QDoubleSpinBox()
        self.callout_height.setRange(5.0, 100.0)
        self.callout_height.setSuffix(" pt")
        size_form.addRow("Width", self.callout_width)
        size_form.addRow("Height", self.callout_height)
        callout_layout.addLayout(size_form)
        self.apply_callout_button = QPushButton("Apply callout changes")
        self.apply_callout_button.clicked.connect(self._apply_callout_changes)
        callout_layout.addWidget(self.apply_callout_button)
        controls_layout.addWidget(callout_group)

        markup_group = QGroupBox("Selected custom markup")
        markup_layout = QVBoxLayout(markup_group)
        self.markup_name = QLabel("Select a custom markup on the page")
        self.markup_name.setWordWrap(True)
        markup_layout.addWidget(self.markup_name)
        markup_form = QFormLayout()
        self.markup_width = QDoubleSpinBox()
        self.markup_width.setRange(3.0, 2000.0)
        self.markup_width.setSuffix(" pt")
        self.markup_height = QDoubleSpinBox()
        self.markup_height.setRange(3.0, 2000.0)
        self.markup_height.setSuffix(" pt")
        self.markup_line_width = QDoubleSpinBox()
        self.markup_line_width.setRange(0.25, 12.0)
        self.markup_line_width.setSingleStep(0.25)
        self.markup_line_width.setSuffix(" pt")
        self.markup_font_size = QDoubleSpinBox()
        self.markup_font_size.setRange(4.0, 72.0)
        self.markup_font_size.setSuffix(" pt")
        markup_form.addRow("Width", self.markup_width)
        markup_form.addRow("Height", self.markup_height)
        markup_form.addRow("Line width", self.markup_line_width)
        markup_form.addRow("Text size", self.markup_font_size)
        markup_layout.addLayout(markup_form)
        self.markup_colour_button = QPushButton("Choose colour")
        self.markup_colour_button.clicked.connect(self._choose_annotation_colour)
        markup_layout.addWidget(self.markup_colour_button)
        self.edit_markup_text_button = QPushButton("Edit text")
        self.edit_markup_text_button.clicked.connect(self._edit_selected_annotation)
        markup_layout.addWidget(self.edit_markup_text_button)
        self.apply_markup_button = QPushButton("Apply markup formatting")
        self.apply_markup_button.clicked.connect(self._apply_annotation_changes)
        markup_layout.addWidget(self.apply_markup_button)
        self.markup_controls = (
            self.markup_width,
            self.markup_height,
            self.markup_line_width,
            self.markup_font_size,
            self.markup_colour_button,
            self.edit_markup_text_button,
            self.apply_markup_button,
        )
        for widget in self.markup_controls:
            widget.setEnabled(False)
        controls_layout.addWidget(markup_group)

        self.overlap_label = QLabel("Callout overlap check: not run")
        self.overlap_label.setWordWrap(True)
        controls_layout.addWidget(self.overlap_label)
        self.refresh_button = QPushButton("Refresh PDF preview")
        self.refresh_button.clicked.connect(self.refresh_preview)
        controls_layout.addWidget(self.refresh_button)
        self.reset_button = QPushButton("Reset automatic callout layout")
        self.reset_button.clicked.connect(self._reset_layout)
        controls_layout.addWidget(self.reset_button)
        if not self.show_report_controls:
            layers.hide()
            formatting.hide()
            callout_group.hide()
            self.overlap_label.hide()
            self.reset_button.hide()
        controls_layout.addStretch(1)
        controls_scroll.setWidget(controls)
        splitter.addWidget(controls_scroll)
        splitter.setSizes([180, 980, 320])

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Save).setText("Use Layout and Export PDF")
        buttons.accepted.connect(self._accept_layout)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)
        self.refresh_preview()

    def _collect_settings(self):
        self._save_current_page_callouts(commit_editor=True)
        for key, check in self.layer_checks.items():
            self.settings[key] = bool(check.isChecked())
        self.settings["font_scale"] = float(self.font_scale_spin.value())
        return deepcopy(self.settings)

    def _save_current_page_callouts(self, commit_editor=False):
        if commit_editor:
            self._commit_selected_editor()
        for item in self.callout_items:
            record = item.sync_record()
            self.settings.setdefault("callouts", {})[record["key"]] = {
                "x_pt": record["x_pt"],
                "y_pt": record["y_pt"],
                "width_pt": record["width_pt"],
                "height_pt": record["height_pt"],
                "rail": record.get("rail", "above"),
                "text": record.get("text", ""),
                "visible": bool(record.get("visible", True)),
            }
        annotations_by_id = {
            str(row.get("id")): deepcopy(row)
            for row in self.settings.get("annotations", []) or []
            if isinstance(row, dict) and row.get("id")
        }
        for item in self.annotation_items:
            record = item.sync_record()
            annotations_by_id[str(record["id"])] = deepcopy(record)
        self.settings["annotations"] = list(annotations_by_id.values())

    def _commit_selected_editor(self):
        if self._updating_properties:
            return
        item = self._property_item or self._selected_callout()
        if item is None:
            return
        item.set_text(self.callout_text.toPlainText().strip())
        item.set_callout_visible(self.callout_visible.isChecked())
        item.set_size_points(self.callout_width.value(), self.callout_height.value())
        item.sync_record()

    def refresh_preview(self):
        settings = self._collect_settings()
        try:
            path, manifest = self.preview_builder(settings)
        except Exception as exc:
            QMessageBox.critical(self, "Report preview failed", str(exc))
            return
        self.preview_path = str(path)
        self.manifest = list(manifest or [])
        self.document.close()
        error = self.document.load(self.preview_path)
        if error != QPdfDocument.Error.None_:
            QMessageBox.critical(
                self, "Report preview failed", f"Could not load preview PDF: {error}"
            )
            return
        previous = max(0, self.page_list.currentRow())
        self.page_list.clear()
        page_details = {}
        for row in self.manifest:
            if row.get("option_number") is not None and row.get("floor") is not None:
                label = (
                    f"Option {int(row['option_number'])} - "
                    f"Floor {int(row['floor'])}"
                )
            else:
                label = str(row.get("page_label", "") or "").strip()
            if label:
                page_details.setdefault(int(row["page"]), label)
        for page in range(self.document.pageCount()):
            label = page_details.get(page)
            if label is None:
                label = f"Page {page + 1}"
            self.page_list.addItem(f"{page + 1}. {label}")
        if self.page_list.count():
            self.page_list.setCurrentRow(min(previous, self.page_list.count() - 1))
        self._show_page(self.page_list.currentRow())

    def _fit_page(self):
        if self.scene.sceneRect().isEmpty():
            return
        self.view.fitInView(self.scene.sceneRect(), Qt.KeepAspectRatio)
        self.zoom_factor = 1.0
        self.zoom_combo.blockSignals(True)
        self.zoom_combo.setCurrentIndex(0)
        self.zoom_combo.blockSignals(False)
        self.zoom_status.setText("Fit (100%)")
        self.detail_render_timer.start()

    def _zoom_by(self, multiplier):
        target = max(0.25, min(8.0, self.zoom_factor * float(multiplier)))
        actual = target / max(0.001, self.zoom_factor)
        if abs(actual - 1.0) < 0.001:
            return
        self.view.scale(actual, actual)
        self.zoom_factor = target
        percentage = int(round(self.zoom_factor * 100.0))
        self.zoom_combo.blockSignals(True)
        preset_index = self.zoom_combo.findData(percentage)
        self.zoom_combo.setCurrentIndex(preset_index)
        self.zoom_combo.blockSignals(False)
        self.zoom_status.setText(f"{percentage}% of fit")
        self.detail_render_timer.start()

    def _refresh_background_detail(self):
        if (
            self.background_item is None
            or self.document is None
            or self.document.pageCount() <= 0
        ):
            return
        point_size = self.document.pagePointSize(self.current_page)
        base_width = int(self.base_render_width)
        base_height = max(
            1,
            int(
                float(point_size.height())
                * base_width
                / max(1.0, float(point_size.width()))
            ),
        )
        requested = 1.0 if self.zoom_factor <= 1.05 else self.zoom_factor * 1.12
        max_side_factor = 9000.0 / max(base_width, base_height)
        max_pixel_factor = math.sqrt(
            36_000_000.0 / max(1.0, float(base_width * base_height))
        )
        detail_factor = max(
            1.0, min(float(requested), max_side_factor, max_pixel_factor)
        )
        if (
            abs(detail_factor - self.background_render_factor) < 0.2
            or (
                detail_factor > 1.0
                and self.background_render_factor >= detail_factor
                and self.zoom_factor > 1.05
            )
        ):
            return
        render_width = max(base_width, int(round(base_width * detail_factor)))
        render_height = max(
            1,
            int(
                round(
                    float(point_size.height())
                    * render_width
                    / max(1.0, float(point_size.width()))
                )
            ),
        )
        image = self.document.render(
            self.current_page, QSize(render_width, render_height)
        )
        if image.isNull():
            return
        self.background_item.setPixmap(QPixmap.fromImage(image))
        self.background_item.setScale(base_width / float(render_width))
        self.background_render_factor = render_width / float(base_width)
        if self.zoom_factor > 1.05:
            percentage = int(round(self.zoom_factor * 100.0))
            self.zoom_status.setText(
                f"{percentage}% of fit - {self.background_render_factor:.1f}x detail"
            )

    def _zoom_preset_changed(self, index):
        percentage = self.zoom_combo.itemData(index)
        if percentage is None:
            self._fit_page()
            return
        target = max(0.25, min(8.0, float(percentage) / 100.0))
        self._zoom_by(target / max(0.001, self.zoom_factor))

    def _set_annotation_tool(self, tool):
        self.active_annotation_tool = str(tool)
        self.annotation_start = None
        self.callout_anchor = None
        if tool != "polyline":
            self.polyline_points = []
        for name, button in self.annotation_tool_buttons.items():
            button.setChecked(name == tool)
        self.finish_polyline_button.setEnabled(tool == "polyline")
        self.view.setDragMode(
            QGraphicsView.ScrollHandDrag
            if tool == "select"
            else QGraphicsView.NoDrag
        )
        instructions = {
            "select": "Select / move: choose callouts or markups and drag them into position.",
            "text": "Text: click the page, then enter the text to place.",
            "callout": "Point callout: click the target point, then click the label position.",
            "rectangle": "Rectangle: drag around the area to mark.",
            "revision_cloud": "Revision cloud: drag around the revised area.",
            "polyline": "Polyline: click each vertex, then double-click or press Finish polyline.",
        }
        self.annotation_status.setText(instructions.get(tool, ""))

    def _scene_to_pdf(self, point):
        scale = max(0.001, float(self.page_render_scale))
        return (
            max(0.0, float(point.x()) / scale),
            max(0.0, self.current_page_height_pt - float(point.y()) / scale),
        )

    def _add_annotation(self, record):
        record = deepcopy(record)
        record.setdefault("id", f"annotation-{uuid4().hex}")
        record.setdefault("page", int(self.current_page))
        record.setdefault("colour", "#d92d20")
        record.setdefault("line_width_pt", 1.5)
        record.setdefault("font_size_pt", 9.0)
        record.setdefault("visible", True)
        self.settings.setdefault("annotations", []).append(record)
        item = _AnnotationItem(
            record,
            self.page_render_scale,
            self.current_page_height_pt,
            edit_callback=self._edit_annotation,
        )
        self.scene.addItem(item)
        self.annotation_items.append(item)
        item.setSelected(True)
        self._set_annotation_tool("select")
        return item

    def annotation_mouse_press(self, event, scene_point):
        tool = self.active_annotation_tool
        if tool == "select":
            return False
        if event.button() == Qt.RightButton and tool == "polyline":
            self._finish_polyline()
            return True
        if event.button() != Qt.LeftButton:
            return True
        if tool == "text":
            text, ok = QInputDialog.getMultiLineText(
                self, "Add report text", "Text to place:", "Text"
            )
            if ok and str(text).strip():
                x, top_y = self._scene_to_pdf(scene_point)
                self._add_annotation(
                    {
                        "type": "text",
                        "x_pt": x,
                        "y_pt": max(0.0, top_y - 28.0),
                        "width_pt": 180.0,
                        "height_pt": 28.0,
                        "text": str(text).strip(),
                    }
                )
            return True
        if tool == "callout":
            point = self._scene_to_pdf(scene_point)
            if self.callout_anchor is None:
                self.callout_anchor = point
                self.annotation_status.setText(
                    "Point callout: target selected; now click the label position."
                )
            else:
                text, ok = QInputDialog.getMultiLineText(
                    self, "Add custom callout", "Callout text:", "Callout"
                )
                if ok and str(text).strip():
                    x, top_y = point
                    self._add_annotation(
                        {
                            "type": "callout",
                            "x_pt": x,
                            "y_pt": max(0.0, top_y - 34.0),
                            "width_pt": 190.0,
                            "height_pt": 34.0,
                            "anchor_x_pt": self.callout_anchor[0],
                            "anchor_y_pt": self.callout_anchor[1],
                            "text": str(text).strip(),
                        }
                    )
                self.callout_anchor = None
            return True
        if tool in {"rectangle", "revision_cloud"}:
            self.annotation_start = scene_point
            return True
        if tool == "polyline":
            self.polyline_points.append(self._scene_to_pdf(scene_point))
            self.annotation_status.setText(
                f"Polyline: {len(self.polyline_points)} point(s); double-click or finish when complete."
            )
            return True
        return False

    def annotation_mouse_move(self, event, scene_point):
        return bool(
            self.active_annotation_tool in {"rectangle", "revision_cloud"}
            and self.annotation_start is not None
        )

    def annotation_mouse_release(self, event, scene_point):
        tool = self.active_annotation_tool
        if (
            tool not in {"rectangle", "revision_cloud"}
            or self.annotation_start is None
        ):
            return tool != "select"
        start_x, start_y = self._scene_to_pdf(self.annotation_start)
        end_x, end_y = self._scene_to_pdf(scene_point)
        self.annotation_start = None
        left, right = sorted((start_x, end_x))
        bottom, top = sorted((start_y, end_y))
        if right - left >= 3.0 and top - bottom >= 3.0:
            self._add_annotation(
                {
                    "type": tool,
                    "x_pt": left,
                    "y_pt": bottom,
                    "width_pt": right - left,
                    "height_pt": top - bottom,
                }
            )
        return True

    def annotation_mouse_double_click(self, event, scene_point):
        if self.active_annotation_tool == "polyline":
            self._finish_polyline()
            return True
        return False

    def _finish_polyline(self):
        if len(self.polyline_points) < 2:
            self.polyline_points = []
            self._set_annotation_tool("select")
            return
        xs = [point[0] for point in self.polyline_points]
        ys = [point[1] for point in self.polyline_points]
        self._add_annotation(
            {
                "type": "polyline",
                "x_pt": min(xs),
                "y_pt": min(ys),
                "width_pt": max(3.0, max(xs) - min(xs)),
                "height_pt": max(3.0, max(ys) - min(ys)),
                "points_pt": [list(point) for point in self.polyline_points],
            }
        )
        self.polyline_points = []

    def _edit_annotation(self, item):
        if item.record.get("type") not in {"text", "callout"}:
            return
        value, ok = QInputDialog.getMultiLineText(
            self,
            "Edit report markup",
            "Text:",
            str(item.record.get("text", "")),
        )
        if ok:
            item.record["text"] = str(value).strip()
            item._redraw()
            item.sync_record()

    def _delete_selected_annotations(self):
        selected = [
            item
            for item in self.scene.selectedItems()
            if isinstance(item, _AnnotationItem)
        ]
        if not selected:
            return
        removed_ids = {str(item.record.get("id")) for item in selected}
        self.settings["annotations"] = [
            row
            for row in self.settings.get("annotations", []) or []
            if str(row.get("id")) not in removed_ids
        ]
        for item in selected:
            if item in self.annotation_items:
                self.annotation_items.remove(item)
            self.scene.removeItem(item)

    def _show_page(self, page):
        self.detail_render_timer.stop()
        self._save_current_page_callouts(commit_editor=True)
        if page < 0 or page >= self.document.pageCount():
            return
        self.current_page = int(page)
        point_size = self.document.pagePointSize(page)
        render_width = int(self.base_render_width)
        scale = render_width / max(1.0, float(point_size.width()))
        self.page_render_scale = float(scale)
        self.current_page_height_pt = float(point_size.height())
        image_size = QSize(render_width, max(1, int(point_size.height() * scale)))
        image = self.document.render(page, image_size)
        self._property_item = None
        self.scene.clear()
        self.callout_items = []
        self.annotation_items = []
        self.background_item = self.scene.addPixmap(QPixmap.fromImage(image))
        self.background_item.setZValue(-10)
        self.background_render_factor = 1.0
        for source in self.manifest:
            if int(source.get("page", -1)) != page:
                continue
            record = deepcopy(source)
            override = self.settings.get("callouts", {}).get(record["key"], {})
            record.update(override)
            item = _CalloutItem(
                record,
                scale,
                float(point_size.height()),
                self._update_overlap_status,
            )
            self.scene.addItem(item)
            self.callout_items.append(item)
        for source in self.settings.get("annotations", []) or []:
            if int(source.get("page", -1)) != page:
                continue
            item = _AnnotationItem(
                deepcopy(source),
                scale,
                float(point_size.height()),
                edit_callback=self._edit_annotation,
            )
            self.scene.addItem(item)
            self.annotation_items.append(item)
        self.scene.setSceneRect(0, 0, image.width(), image.height())
        self._fit_page()
        self._update_overlap_status()

    def _selected_callout(self):
        return next(
            (item for item in self.scene.selectedItems() if isinstance(item, _CalloutItem)),
            None,
        )

    def _selected_annotation(self):
        return next(
            (
                item
                for item in self.scene.selectedItems()
                if isinstance(item, _AnnotationItem)
            ),
            None,
        )

    def _selection_changed(self):
        item = self._selected_callout()
        if (
            self._property_item is not None
            and self._property_item is not item
            and not self._updating_properties
        ):
            self._commit_selected_editor()
            self._save_current_page_callouts()
        self._property_item = item
        self._updating_properties = True
        enabled = item is not None
        for widget in (
            self.callout_text,
            self.callout_visible,
            self.callout_width,
            self.callout_height,
            self.apply_callout_button,
        ):
            widget.setEnabled(enabled)
        if item is None:
            self.callout_name.setText("Select a callout on the page")
            self.callout_text.clear()
        else:
            display_name = str(item.record.get("name", "") or "").strip()
            if display_name:
                self.callout_name.setText(display_name)
            elif item.record.get("option_number") is not None:
                self.callout_name.setText(
                    f"{str(item.record.get('kind', 'callout')).replace('_', ' ').title()} - "
                    f"Option {item.record.get('option_number')} / Floor {item.record.get('floor')}"
                )
            else:
                self.callout_name.setText(
                    str(item.record.get("kind", "callout")).replace("_", " ").title()
                )
            self.callout_text.setPlainText(str(item.record.get("text", "")))
            self.callout_visible.setChecked(bool(item.record.get("visible", True)))
            self.callout_width.setValue(float(item.record["width_pt"]))
            self.callout_height.setValue(float(item.record["height_pt"]))

        annotation = self._selected_annotation()
        markup_enabled = annotation is not None
        for widget in self.markup_controls:
            widget.setEnabled(markup_enabled)
        if annotation is None:
            self.markup_name.setText("Select a custom markup on the page")
            self.markup_colour_button.setStyleSheet("")
        else:
            kind = str(annotation.record.get("type", "markup")).replace("_", " ")
            self.markup_name.setText(kind.title())
            self.markup_width.setValue(float(annotation.record.get("width_pt", 8.0)))
            self.markup_height.setValue(float(annotation.record.get("height_pt", 8.0)))
            self.markup_line_width.setValue(
                float(annotation.record.get("line_width_pt", 1.5))
            )
            self.markup_font_size.setValue(
                float(annotation.record.get("font_size_pt", 9.0))
            )
            colour = str(annotation.record.get("colour", "#d92d20"))
            self.markup_colour_button.setStyleSheet(
                f"background-color: {colour}; color: "
                f"{'#ffffff' if QColor(colour).lightness() < 128 else '#101828'};"
            )
            self.edit_markup_text_button.setEnabled(
                annotation.record.get("type") in {"text", "callout"}
            )
        self._updating_properties = False

    def _apply_annotation_changes(self):
        item = self._selected_annotation()
        if item is None:
            return
        old_width = max(0.001, float(item.record.get("width_pt", 8.0)))
        old_height = max(0.001, float(item.record.get("height_pt", 8.0)))
        new_width = float(self.markup_width.value())
        new_height = float(self.markup_height.value())
        if item.record.get("type") == "polyline":
            origin_x = float(item.record.get("x_pt", 0.0))
            origin_y = float(item.record.get("y_pt", 0.0))
            scale_x = new_width / old_width
            scale_y = new_height / old_height
            item.record["points_pt"] = [
                [
                    origin_x + (float(point[0]) - origin_x) * scale_x,
                    origin_y + (float(point[1]) - origin_y) * scale_y,
                ]
                for point in item.record.get("points_pt", []) or []
            ]
        item.record["width_pt"] = new_width
        item.record["height_pt"] = new_height
        item.record["line_width_pt"] = float(self.markup_line_width.value())
        item.record["font_size_pt"] = float(self.markup_font_size.value())
        item.setRect(
            0.0,
            0.0,
            new_width * item.scale,
            new_height * item.scale,
        )
        item._redraw()
        item.sync_record()
        self._save_current_page_callouts()

    def _choose_annotation_colour(self):
        item = self._selected_annotation()
        if item is None:
            return
        current = QColor(str(item.record.get("colour", "#d92d20")))
        colour = QColorDialog.getColor(current, self, "Choose markup colour")
        if not colour.isValid():
            return
        item.record["colour"] = colour.name()
        item._redraw()
        item.sync_record()
        self._save_current_page_callouts()
        self._selection_changed()

    def _edit_selected_annotation(self):
        item = self._selected_annotation()
        if item is None:
            return
        self._edit_annotation(item)
        self._save_current_page_callouts()

    def _apply_callout_changes(self):
        item = self._selected_callout()
        if item is None:
            return
        self._commit_selected_editor()
        self._save_current_page_callouts()
        self._update_overlap_status()

    def _update_overlap_status(self):
        visible = [item for item in self.callout_items if item.record.get("visible", True)]
        overlaps = 0
        for index, first in enumerate(visible):
            for second in visible[index + 1 :]:
                if first.sceneBoundingRect().intersects(second.sceneBoundingRect()):
                    overlaps += 1
        if overlaps:
            self.overlap_label.setText(
                f"Callout overlap check: {overlaps} overlap(s) remain on this page."
            )
            self.overlap_label.setStyleSheet("color: #b42318; font-weight: bold;")
        else:
            self.overlap_label.setText("Callout overlap check: no overlaps on this page.")
            self.overlap_label.setStyleSheet("color: #067647; font-weight: bold;")

    def _reset_layout(self):
        self.settings["callouts"] = {}
        self.refresh_preview()

    def _accept_layout(self):
        self._collect_settings()
        overlap_pages = []
        for page in range(self.document.pageCount()):
            rows = [
                row
                for row in self.manifest
                if int(row.get("page", -1)) == page
                and bool(
                    self.settings.get("callouts", {})
                    .get(row["key"], {})
                    .get("visible", row.get("visible", True))
                )
            ]
            rectangles = []
            for row in rows:
                effective = dict(row)
                effective.update(self.settings.get("callouts", {}).get(row["key"], {}))
                rect = (
                    float(effective["x_pt"]),
                    float(effective["y_pt"]),
                    float(effective["width_pt"]),
                    float(effective["height_pt"]),
                )
                if any(
                    not (
                        rect[0] + rect[2] <= other[0]
                        or other[0] + other[2] <= rect[0]
                        or rect[1] + rect[3] <= other[1]
                        or other[1] + other[3] <= rect[1]
                    )
                    for other in rectangles
                ):
                    overlap_pages.append(page + 1)
                    break
                rectangles.append(rect)
        if overlap_pages and QMessageBox.question(
            self,
            "Callouts still overlap",
            "Overlapping visible callouts remain on page(s) "
            + ", ".join(str(page) for page in sorted(set(overlap_pages)))
            + ". Export anyway?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        ) != QMessageBox.Yes:
            return
        self.accept()

    def export_settings(self):
        return self._collect_settings()

    def release_preview(self):
        """Release Qt's file handle before the caller removes preview files."""
        self.detail_render_timer.stop()
        self._property_item = None
        self.scene.blockSignals(True)
        self.scene.clear()
        self.scene.blockSignals(False)
        self.callout_items = []
        self.annotation_items = []
        self.background_item = None
        document = self.document
        self.document = None
        if document is not None:
            document.close()
            document.setParent(None)
            document.deleteLater()
            QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)
            QCoreApplication.processEvents()
        self.preview_path = ""


ZoneDesignReportStudioDialog = PdfReportStudioDialog
