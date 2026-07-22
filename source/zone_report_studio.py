"""Interactive layout studio for zone-design option PDF reports."""

from __future__ import annotations

import base64
import math
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

from PySide6.QtCore import QEventLoop, QPointF, QRectF, QSize, Qt, QTimer
from PySide6.QtGui import (
    QColor,
    QBrush,
    QFont,
    QKeySequence,
    QImage,
    QPainter,
    QPainterPath,
    QPen,
    QPixmap,
    QShortcut,
    QTextOption,
)
from PySide6.QtPdf import QPdfDocument
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QColorDialog,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFileDialog,
    QFormLayout,
    QGraphicsItem,
    QGraphicsEllipseItem,
    QGraphicsPathItem,
    QGraphicsPixmapItem,
    QGraphicsRectItem,
    QGraphicsScene,
    QGraphicsSimpleTextItem,
    QGraphicsTextItem,
    QGraphicsView,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QMessageBox,
    QInputDialog,
    QPlainTextEdit,
    QProgressDialog,
    QPushButton,
    QScrollArea,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

from pdf_report_annotations import (
    append_pdf_studio_blank_pages,
    fitted_generated_callout_size,
    resolve_callout_override,
)
from network_report_snippets import (
    DEFAULT_NETWORK_SNIPPET_TEMPLATES,
    NETWORK_ROOM_LAYOUT_VARIANTS,
    NETWORK_SNIPPET_VARIABLE_NAMES,
    apply_network_room_layout_variant,
    auto_arrange_network_room_cabinets,
    cabinet_snippet_physical_size_mm,
    format_network_snippet_template,
    network_report_snippet_catalog,
    network_report_snippet_variables,
    render_network_room_layout_png,
    render_network_report_snippet_png,
    render_network_report_snippets_png,
    room_layout_compliance,
    suggested_network_room_layout,
)


DEFAULT_REPORT_PAGE_TEMPLATES = [
    {"id": "page-a4-portrait", "name": "Blank A4 portrait", "width_pt": 595.276, "height_pt": 841.89, "annotations": [], "builtin": True},
    {"id": "page-a4-landscape", "name": "Blank A4 landscape", "width_pt": 841.89, "height_pt": 595.276, "annotations": [], "builtin": True},
    {"id": "page-a3-landscape-room", "name": "A3 landscape - room breakdown", "width_pt": 1190.551, "height_pt": 841.89, "builtin": True, "annotations": [
        {"type": "text", "x_pt": 32.0, "y_pt": 790.0, "width_pt": 1125.0, "height_pt": 26.0, "text": "ROOM BREAKDOWN", "font_size_pt": 16.0, "colour": "#176b87"},
        {"type": "rectangle", "x_pt": 32.0, "y_pt": 405.0, "width_pt": 550.0, "height_pt": 360.0, "line_width_pt": 1.0, "colour": "#94a3b8"},
        {"type": "rectangle", "x_pt": 608.0, "y_pt": 405.0, "width_pt": 550.0, "height_pt": 360.0, "line_width_pt": 1.0, "colour": "#94a3b8"},
        {"type": "rectangle", "x_pt": 32.0, "y_pt": 42.0, "width_pt": 1126.0, "height_pt": 330.0, "line_width_pt": 1.0, "colour": "#94a3b8"},
    ]},
    {"id": "page-a3-landscape-columns", "name": "A3 landscape - two-column technical", "width_pt": 1190.551, "height_pt": 841.89, "builtin": True, "annotations": [
        {"type": "text", "x_pt": 32.0, "y_pt": 790.0, "width_pt": 1125.0, "height_pt": 26.0, "text": "TECHNICAL DETAILS", "font_size_pt": 16.0, "colour": "#176b87"},
        {"type": "rectangle", "x_pt": 32.0, "y_pt": 45.0, "width_pt": 548.0, "height_pt": 715.0, "line_width_pt": 1.0, "colour": "#94a3b8"},
        {"type": "rectangle", "x_pt": 610.0, "y_pt": 45.0, "width_pt": 548.0, "height_pt": 715.0, "line_width_pt": 1.0, "colour": "#94a3b8"},
    ]},
    {"id": "page-a2-landscape-board", "name": "A2 landscape - proposal board", "width_pt": 1683.78, "height_pt": 1190.551, "builtin": True, "annotations": [
        {"type": "text", "x_pt": 42.0, "y_pt": 1128.0, "width_pt": 1600.0, "height_pt": 32.0, "text": "ROOM PROPOSAL", "font_size_pt": 20.0, "colour": "#176b87"},
        {"type": "rectangle", "x_pt": 42.0, "y_pt": 410.0, "width_pt": 980.0, "height_pt": 680.0, "line_width_pt": 1.2, "colour": "#94a3b8"},
        {"type": "rectangle", "x_pt": 1052.0, "y_pt": 410.0, "width_pt": 590.0, "height_pt": 680.0, "line_width_pt": 1.2, "colour": "#94a3b8"},
        {"type": "rectangle", "x_pt": 42.0, "y_pt": 48.0, "width_pt": 1600.0, "height_pt": 320.0, "line_width_pt": 1.2, "colour": "#94a3b8"},
    ]},
]

MAX_REPORT_ZOOM_FACTOR = 16.0


def _scaled_snippet_image_size(png_payload, physical_width_mm, physical_height_mm, denominator):
    """Fit the rendered artwork inside its true-scale physical envelope."""
    denominator = int(denominator or 0)
    if denominator <= 0:
        return 0.0, 0.0
    envelope_width = max(0.0, float(physical_width_mm or 0.0)) / denominator * 72.0 / 25.4
    envelope_height = max(0.0, float(physical_height_mm or 0.0)) / denominator * 72.0 / 25.4
    if envelope_width <= 0.0 or envelope_height <= 0.0:
        return envelope_width, envelope_height
    image = QImage.fromData(bytes(png_payload or b""), "PNG")
    if image.isNull() or image.width() <= 0 or image.height() <= 0:
        return envelope_width, envelope_height
    factor = min(
        envelope_width / float(image.width()),
        envelope_height / float(image.height()),
    )
    return float(image.width()) * factor, float(image.height()) * factor


class _LeaderBendHandle(QGraphicsEllipseItem):
    """Draggable page-space vertex belonging to a generated callout leader."""

    def __init__(self, owner, leader_index, point_index):
        radius = 5.0
        super().__init__(-radius, -radius, radius * 2.0, radius * 2.0, owner)
        self.owner = owner
        self.leader_index = int(leader_index)
        self.point_index = int(point_index)
        self.setBrush(QBrush(QColor("#ffffff")))
        self.setPen(QPen(QColor("#2474a8"), 1.5))
        self.setFlags(
            QGraphicsItem.ItemIsMovable
            | QGraphicsItem.ItemSendsGeometryChanges
        )
        self.setZValue(5)
        self.setCursor(Qt.SizeAllCursor)
        self.setToolTip("Drag this leader bend through clear space")

    def itemChange(self, change, value):
        result = super().itemChange(change, value)
        if (
            change == QGraphicsItem.ItemPositionHasChanged
            and hasattr(self, "owner")
            and not self.owner._positioning_leader_handles
        ):
            self.owner._leader_handle_moved(
                self.leader_index, self.point_index, self
            )
        return result

    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        if hasattr(self, "owner"):
            self.owner._notify_changed()


class _LeaderAnchorHandle(QGraphicsEllipseItem):
    """Draggable page-space endpoint for one generated callout leader."""

    def __init__(self, owner, leader_index):
        radius = 5.5
        super().__init__(-radius, -radius, radius * 2.0, radius * 2.0, owner)
        self.owner = owner
        self.leader_index = int(leader_index)
        colour = QColor(str(owner.record.get("colour", "#2474a8")))
        self.setBrush(QBrush(colour))
        self.setPen(QPen(QColor("#ffffff"), 1.5))
        self.setFlags(
            QGraphicsItem.ItemIsMovable
            | QGraphicsItem.ItemSendsGeometryChanges
        )
        self.setZValue(6)
        self.setCursor(Qt.SizeAllCursor)
        self.setToolTip("Drag this leader endpoint to the required dot or position")

    def itemChange(self, change, value):
        result = super().itemChange(change, value)
        if (
            change == QGraphicsItem.ItemPositionHasChanged
            and hasattr(self, "owner")
            and not self.owner._positioning_leader_handles
        ):
            self.owner._leader_anchor_moved(self.leader_index, self)
        return result

    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        if hasattr(self, "owner"):
            self.owner._notify_changed()


class _SnippetResizeHandle(QGraphicsRectItem):
    """One of the eight drag handles around a selected network snippet."""

    CURSORS = {
        "n": Qt.SizeVerCursor, "s": Qt.SizeVerCursor,
        "e": Qt.SizeHorCursor, "w": Qt.SizeHorCursor,
        "nw": Qt.SizeFDiagCursor, "se": Qt.SizeFDiagCursor,
        "ne": Qt.SizeBDiagCursor, "sw": Qt.SizeBDiagCursor,
    }

    def __init__(self, owner, edge):
        size = 8.0
        super().__init__(-size / 2.0, -size / 2.0, size, size, owner)
        self.owner = owner
        self.edge = str(edge)
        self.setBrush(QBrush(QColor("#ffffff")))
        self.setPen(QPen(QColor("#176b87"), 1.4))
        self.setFlag(QGraphicsItem.ItemSendsGeometryChanges, True)
        self.setZValue(20)
        self.setAcceptedMouseButtons(Qt.LeftButton)
        self.setCursor(self.CURSORS[self.edge])
        self.setToolTip("Drag to resize")

    def mousePressEvent(self, event):
        if self.owner.is_locked():
            event.ignore()
            return
        # A resize handle is a movable child of a movable snippet. Temporarily
        # disable parent movement so Qt cannot promote this drag into a panel move.
        self.owner.setFlag(QGraphicsItem.ItemIsMovable, False)
        self.owner._resizing_from_handle = True
        self._drag_scene_start = QPointF(event.scenePos())
        self._owner_start_pos = QPointF(self.owner.pos())
        self._owner_start_size = (
            float(self.owner.rect().width()),
            float(self.owner.rect().height()),
        )
        super().mousePressEvent(event)
        event.accept()

    def mouseMoveEvent(self, event):
        if not hasattr(self, "_drag_scene_start") or self.owner.is_locked():
            event.ignore()
            return
        delta = event.scenePos() - self._drag_scene_start
        self.owner._resize_snippet_from_drag(
            self.edge,
            self._owner_start_pos,
            self._owner_start_size,
            delta,
        )
        event.accept()

    def itemChange(self, change, value):
        result = super().itemChange(change, value)
        if (
            change == QGraphicsItem.ItemPositionHasChanged
            and hasattr(self, "owner")
            and not self.owner._positioning_resize_handles
        ):
            self.owner._snippet_resize_handle_moved(self.edge, self.pos())
        return result

    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        self.owner._resizing_from_handle = False
        self.owner.setFlag(QGraphicsItem.ItemIsMovable, not self.owner.is_locked())
        self.owner._notify_changed()
        event.accept()


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
        if self.record.get("joined_callout_keys") and "auto_fit_text" not in self.record:
            self.record["auto_fit_text"] = True
        fitted_width, fitted_height = fitted_generated_callout_size(
            self.record.get("text", ""), self.record.get("font_size_pt", 7.0)
        )
        if bool(self.record.get("auto_fit_text", False)):
            previous_width = float(self.record.get("width_pt", 1.0) or 1.0)
            previous_height = float(self.record.get("height_pt", 1.0) or 1.0)
            if str(self.record.get("rail", "")) == "left":
                self.record["x_pt"] = max(0.0, float(
                    self.record.get("x_pt", 0.0) or 0.0
                ) + (previous_width - fitted_width))
            self.record["y_pt"] = max(0.0, float(
                self.record.get("y_pt", 0.0) or 0.0
            ) + (previous_height - fitted_height))
            self.record["width_pt"] = fitted_width
            self.record["height_pt"] = fitted_height
        else:
            # User-sized callouts are allowed to be narrower than their original
            # unwrapped text. The editor and PDF renderer wrap within this box.
            self.record["width_pt"] = max(
                18.0, float(self.record.get("width_pt", fitted_width) or fitted_width)
            )
            self.record["height_pt"] = max(
                5.0, float(self.record.get("height_pt", fitted_height) or fitted_height)
            )
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
        self.leader_handles = []
        self.leader_anchor_handles = []
        self._positioning_leader_handles = False
        self.resize_handles = []
        self._positioning_resize_handles = False
        self._resizing_from_handle = False
        self.text_item = QGraphicsTextItem(self)
        self.text_item.setDefaultTextColor(colour)
        self.text_item.document().setDocumentMargin(0.0)
        self._update_text_font()
        self.set_text(str(record.get("text", "")))
        self.set_callout_visible(bool(record.get("visible", True)))
        self._rebuild_leader_handles()
        self._rebuild_resize_handles()
        self._update_leader()
        self.set_locked(bool(record.get("locked", False)))

    def is_locked(self):
        return bool(self.record.get("locked", False))

    def set_locked(self, locked):
        self.record["locked"] = bool(locked)
        self.setFlag(QGraphicsItem.ItemIsMovable, not bool(locked))
        self.setCursor(Qt.ArrowCursor if locked else Qt.SizeAllCursor)
        for handle in self.leader_handles + self.leader_anchor_handles:
            handle.setFlag(QGraphicsItem.ItemIsMovable, not bool(locked))
            handle.setVisible(self.isSelected() and not bool(locked))
        for handle in self.resize_handles:
            handle.setFlag(QGraphicsItem.ItemIsMovable, False)
            handle.setVisible(self.isSelected() and not bool(locked))
        self._update_tooltip()

    def _update_tooltip(self):
        visibility = (
            "Included in the PDF"
            if bool(self.record.get("visible", True))
            else "Hidden from the final PDF"
        )
        self.setToolTip(
            f"Locked - {visibility.lower()}" if self.is_locked() else visibility
        )

    def _notify_changed(self):
        if bool(self.record.get("wrap_text", True)):
            width_pt = self.rect().width() / max(0.01, self.scale)
            height_pt = self.rect().height() / max(0.01, self.scale)
            required_height = self.required_wrapped_height_points()
            if required_height > height_pt + 0.1:
                self.set_size_points(width_pt, required_height)
        self.sync_record()
        if self.moved_callback is not None:
            self.moved_callback()

    def set_text(self, value):
        self.record["text"] = str(value)
        self.text_item.setPlainText(str(value))
        self._fit_text()

    def _update_text_font(self):
        font = QFont("Arial")
        font.setPixelSize(max(4, round(
            float(self.record.get("font_size_pt", 9.0) or 9.0) * self.scale
        )))
        font.setBold(True)
        self.text_item.setFont(font)

    def set_font_size(self, font_size_pt):
        self.record["font_size_pt"] = max(4.0, float(font_size_pt))
        self._update_text_font()
        self._fit_text()

    def set_wrap_text(self, enabled):
        self.record["wrap_text"] = bool(enabled)
        self._fit_text()

    def _fit_text(self):
        self.text_item.setScale(1.0)
        available = max(10.0, self.rect().width() - 12.0)
        available_height = max(6.0, self.rect().height() - 4.0)
        option = self.text_item.document().defaultTextOption()
        wrap_text = bool(self.record.get("wrap_text", True))
        option.setWrapMode(
            QTextOption.WrapAtWordBoundaryOrAnywhere
            if wrap_text
            else QTextOption.NoWrap
        )
        self.text_item.document().setDefaultTextOption(option)
        self.text_item.setTextWidth(available if wrap_text else -1.0)
        bounds = self.text_item.boundingRect()
        factor = (
            1.0
            if wrap_text
            else min(
                1.0,
                available / max(1.0, bounds.width()),
                available_height / max(1.0, bounds.height()),
            )
        )
        self.text_item.setScale(factor)
        fitted_height = bounds.height() * factor
        self.text_item.setPos(6.0, max(2.0, (self.rect().height() - fitted_height) / 2.0))

    def required_wrapped_height_points(self):
        if not bool(self.record.get("wrap_text", True)):
            return 5.0
        self._fit_text()
        return max(
            5.0,
            (self.text_item.boundingRect().height() + 4.0) / max(0.01, self.scale),
        )

    def set_size_points(self, width_pt, height_pt):
        self.setRect(
            0.0,
            0.0,
            max(18.0, float(width_pt)) * self.scale,
            max(5.0, float(height_pt)) * self.scale,
        )
        self._fit_text()
        self._update_leader()
        self._position_resize_handles()

    def _rebuild_resize_handles(self):
        for handle in self.resize_handles:
            if handle.scene() is not None:
                handle.scene().removeItem(handle)
            handle.setParentItem(None)
        self.resize_handles = [
            _SnippetResizeHandle(self, edge)
            for edge in ("nw", "n", "ne", "e", "se", "s", "sw", "w")
        ]
        self._position_resize_handles()

    def _position_resize_handles(self):
        if not hasattr(self, "resize_handles"):
            return
        self._positioning_resize_handles = True
        try:
            width, height = self.rect().width(), self.rect().height()
            points = {
                "nw": (0.0, 0.0), "n": (width / 2.0, 0.0), "ne": (width, 0.0),
                "e": (width, height / 2.0), "se": (width, height),
                "s": (width / 2.0, height), "sw": (0.0, height),
                "w": (0.0, height / 2.0),
            }
            for handle in self.resize_handles:
                handle.setPos(*points[handle.edge])
                handle.setVisible(self.isSelected() and not self.is_locked())
        finally:
            self._positioning_resize_handles = False

    def _snippet_resize_handle_moved(self, _edge, _point):
        # Resize handles use immutable press-time geometry in
        # _resize_snippet_from_drag; their own child position is only visual.
        return

    def _resize_snippet_from_drag(self, edge, start_pos, start_size, delta):
        if self.is_locked():
            return
        minimum_width = max(36.0, 18.0 * self.scale)
        minimum_height = max(18.0, 5.0 * self.scale)
        start_width, start_height = start_size
        left, top = float(start_pos.x()), float(start_pos.y())
        right, bottom = left + start_width, top + start_height
        dx, dy = float(delta.x()), float(delta.y())
        if "w" in edge:
            left = min(left + dx, right - minimum_width)
        if "e" in edge:
            right = max(right + dx, left + minimum_width)
        if "n" in edge:
            top = min(top + dy, bottom - minimum_height)
        if "s" in edge:
            bottom = max(bottom + dy, top + minimum_height)
        self.setPos(left, top)
        self.setRect(0.0, 0.0, right - left, bottom - top)
        self.record["auto_fit_text"] = False
        self.record["x_pt"] = left / self.scale
        self.record["y_pt"] = self.page_height_pt - bottom / self.scale
        self.record["width_pt"] = (right - left) / self.scale
        self.record["height_pt"] = (bottom - top) / self.scale
        self._fit_text()
        self._update_leader()
        self._position_resize_handles()

    def set_callout_visible(self, visible):
        self.record["visible"] = bool(visible)
        self.setOpacity(1.0 if visible else 0.22)
        self._update_tooltip()

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

    def _pdf_to_local(self, point):
        scene_x = float(point[0]) * self.scale
        scene_y = (self.page_height_pt - float(point[1])) * self.scale
        return scene_x - self.pos().x(), scene_y - self.pos().y()

    def _local_to_pdf(self, point):
        scene_point = self.mapToScene(point)
        return [
            float(scene_point.x() / self.scale),
            float(self.page_height_pt - scene_point.y() / self.scale),
        ]

    def _rebuild_leader_handles(self):
        for handle in self.leader_handles + self.leader_anchor_handles:
            if handle.scene() is not None:
                handle.scene().removeItem(handle)
            handle.setParentItem(None)
        self.leader_handles = []
        self.leader_anchor_handles = []
        for leader_index, leader in enumerate(self._leader_routes()):
            self.leader_anchor_handles.append(
                _LeaderAnchorHandle(self, leader_index)
            )
            for point_index, _point in enumerate(leader.get("points_pt", []) or []):
                self.leader_handles.append(
                    _LeaderBendHandle(self, leader_index, point_index)
                )
        self._position_leader_handles()

    def _position_leader_handles(self):
        self._positioning_leader_handles = True
        try:
            routes = self._leader_routes()
            for handle in self.leader_anchor_handles:
                route = routes[handle.leader_index]
                handle.setPos(
                    *self._pdf_to_local(
                        (route["anchor_x_pt"], route["anchor_y_pt"])
                    )
                )
                handle.setVisible(self.isSelected() and not self.is_locked())
            for handle in self.leader_handles:
                point = routes[handle.leader_index]["points_pt"][handle.point_index]
                handle.setPos(*self._pdf_to_local(point))
                handle.setVisible(self.isSelected() and not self.is_locked())
        finally:
            self._positioning_leader_handles = False

    def _leader_routes(self):
        routes = self.record.get("leaders_pt", []) or []
        if routes:
            return [
                {
                    "anchor_x_pt": float(route.get("anchor_x_pt", 0.0)),
                    "anchor_y_pt": float(route.get("anchor_y_pt", 0.0)),
                    "source_anchor_x_pt": float(
                        route.get(
                            "source_anchor_x_pt", route.get("anchor_x_pt", 0.0)
                        )
                    ),
                    "source_anchor_y_pt": float(
                        route.get(
                            "source_anchor_y_pt", route.get("anchor_y_pt", 0.0)
                        )
                    ),
                    "points_pt": [
                        [float(point[0]), float(point[1])]
                        for point in route.get("points_pt", []) or []
                        if isinstance(point, (list, tuple)) and len(point) >= 2
                    ],
                }
                for route in routes
                if isinstance(route, dict)
            ]
        return [
            {
                "anchor_x_pt": float(self.record.get("anchor_x_pt", 0.0)),
                "anchor_y_pt": float(self.record.get("anchor_y_pt", 0.0)),
                "source_anchor_x_pt": float(
                    self.record.get("anchor_x_pt", 0.0)
                ),
                "source_anchor_y_pt": float(
                    self.record.get("anchor_y_pt", 0.0)
                ),
                "points_pt": [
                    [float(point[0]), float(point[1])]
                    for point in self.record.get("leader_points_pt", []) or []
                    if isinstance(point, (list, tuple)) and len(point) >= 2
                ],
            }
        ]

    def _store_leader_routes(self, routes):
        default_anchor = (
            float(self.record.get("anchor_x_pt", 0.0)),
            float(self.record.get("anchor_y_pt", 0.0)),
        )
        route_anchor = (
            float(routes[0].get("anchor_x_pt", 0.0)),
            float(routes[0].get("anchor_y_pt", 0.0)),
        ) if len(routes) == 1 else None
        if (
            len(routes) == 1
            and not self.record.get("leaders_pt")
            and route_anchor == default_anchor
        ):
            points = routes[0].get("points_pt", []) or []
            if points:
                self.record["leader_points_pt"] = points
            else:
                self.record.pop("leader_points_pt", None)
            return
        self.record["leaders_pt"] = routes
        self.record.pop("leader_points_pt", None)

    def _leader_anchor_moved(self, leader_index, handle):
        if self.is_locked():
            return
        routes = self._leader_routes()
        if not 0 <= int(leader_index) < len(routes):
            return
        anchor_x, anchor_y = self._local_to_pdf(handle.pos())
        routes[int(leader_index)]["anchor_x_pt"] = float(anchor_x)
        routes[int(leader_index)]["anchor_y_pt"] = float(anchor_y)
        self._store_leader_routes(routes)
        self._update_leader()

    def _leader_handle_moved(self, leader_index, point_index, handle):
        if self.is_locked():
            return
        routes = self._leader_routes()
        if not 0 <= int(leader_index) < len(routes):
            return
        points = routes[int(leader_index)].get("points_pt", []) or []
        if not 0 <= int(point_index) < len(points):
            return
        points[int(point_index)] = self._local_to_pdf(handle.pos())
        routes[int(leader_index)]["points_pt"] = points
        self._store_leader_routes(routes)
        self._update_leader()

    def add_leader_bend(self):
        if self.is_locked():
            return
        routes = self._leader_routes()
        for route in routes:
            anchor_x = float(route["anchor_x_pt"])
            anchor_y = float(route["anchor_y_pt"])
            points = [list(point) for point in route.get("points_pt", []) or []]
            reference = points[-1] if points else (anchor_x, anchor_y)
            attach_x, attach_y, _rail = self._attachment_point_pdf(reference)
            start_x, start_y = reference
            if not points:
                # The first bend creates the common vertical-then-horizontal route.
                if abs(attach_x - anchor_x) >= abs(attach_y - anchor_y):
                    point = [anchor_x, attach_y]
                else:
                    point = [attach_x, anchor_y]
            else:
                point = [(start_x + attach_x) / 2.0, (start_y + attach_y) / 2.0]
            points.append(point)
            route["points_pt"] = points
        self._store_leader_routes(routes)
        self._rebuild_leader_handles()
        self._update_leader()

    def remove_last_leader_bend(self):
        if self.is_locked():
            return
        routes = self._leader_routes()
        for route in routes:
            points = route.get("points_pt", []) or []
            if points:
                points.pop()
            route["points_pt"] = points
        self._store_leader_routes(routes)
        self._rebuild_leader_handles()
        self._update_leader()

    def reset_leader_route(self):
        if self.is_locked():
            return
        routes = self._leader_routes()
        for route in routes:
            route["points_pt"] = []
            route["anchor_x_pt"] = float(
                route.get("source_anchor_x_pt", route.get("anchor_x_pt", 0.0))
            )
            route["anchor_y_pt"] = float(
                route.get("source_anchor_y_pt", route.get("anchor_y_pt", 0.0))
            )
        self._store_leader_routes(routes)
        self._rebuild_leader_handles()
        self._update_leader()

    def _attachment_point_pdf(self, reference=None):
        x = float(self.pos().x() / self.scale)
        y = float(
            self.page_height_pt
            - (self.pos().y() + self.rect().height()) / self.scale
        )
        width = float(self.rect().width() / self.scale)
        height = float(self.rect().height() / self.scale)
        reference_x = float(reference[0]) if reference else x
        reference_y = float(reference[1]) if reference else y
        centre_x, centre_y = x + width / 2.0, y + height / 2.0
        dx, dy = centre_x - reference_x, centre_y - reference_y
        if abs(dx) > abs(dy):
            rail = "right" if dx >= 0 else "left"
            attach_x = x if dx >= 0 else x + width
            return attach_x, min(max(reference_y, y), y + height), rail
        rail = "above" if dy >= 0 else "below"
        return (
            min(max(reference_x, x), x + width),
            y if dy >= 0 else y + height,
            rail,
        )

    def _update_leader(self):
        if not hasattr(self, "leader_item"):
            return
        path = QPainterPath()
        for route_index, route in enumerate(self._leader_routes()):
            anchor = (float(route["anchor_x_pt"]), float(route["anchor_y_pt"]))
            relative_anchor_x, relative_anchor_y = self._pdf_to_local(anchor)
            leader_points = route.get("points_pt", []) or []
            reference = leader_points[-1] if leader_points else anchor
            attach_pdf_x, attach_pdf_y, rail = self._attachment_point_pdf(reference)
            if route_index == 0:
                self.record["rail"] = rail
            attach_x, attach_y = self._pdf_to_local((attach_pdf_x, attach_pdf_y))
            path.moveTo(relative_anchor_x, relative_anchor_y)
            if leader_points:
                for point in leader_points:
                    path.lineTo(*self._pdf_to_local(point))
            elif rail in {"above", "below"}:
                path.lineTo(relative_anchor_x, attach_y)
            else:
                path.lineTo(attach_x, relative_anchor_y)
            path.lineTo(attach_x, attach_y)
        self.leader_item.setPath(path)
        self._position_leader_handles()

    def itemChange(self, change, value):
        result = super().itemChange(change, value)
        if change == QGraphicsItem.ItemPositionHasChanged:
            self._update_leader()
        elif change == QGraphicsItem.ItemSelectedHasChanged and hasattr(
            self, "leader_handles"
        ):
            self._position_leader_handles()
            self._position_resize_handles()
        return result

    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        self._notify_changed()


class _AnnotationItem(QGraphicsRectItem):
    def __init__(
        self,
        record,
        scale,
        page_height_pt,
        edit_callback=None,
        change_callback=None,
    ):
        self.record = record
        if self.record.get("type") == "network_snippet":
            points = [
                list(point)
                for point in self.record.get("leader_points_pt", []) or []
                if isinstance(point, (list, tuple)) and len(point) >= 2
            ]
            if "leader_route_auto" not in self.record:
                anchor_x = float(self.record.get("anchor_x_pt", 0.0) or 0.0)
                legacy_auto_bend = (
                    len(points) == 1
                    and abs(float(points[0][0]) - anchor_x) <= 0.01
                )
                if legacy_auto_bend:
                    points = []
                    self.record["leader_points_pt"] = []
                self.record["leader_route_auto"] = not points
        self.scale = float(scale)
        self.page_height_pt = float(page_height_pt)
        self.edit_callback = edit_callback
        self.change_callback = change_callback
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
        self.panel_item = QGraphicsPathItem(self)
        self.panel_item.setPen(QPen(Qt.NoPen))
        self.panel_item.setZValue(-2)
        self.shape_item = QGraphicsPathItem(self)
        self.image_item = QGraphicsPixmapItem(self)
        self.text_item = QGraphicsSimpleTextItem(self)
        self.wrapped_text_item = QGraphicsTextItem(self)
        self.wrapped_text_item.document().setDocumentMargin(0.0)
        self.scale_item = QGraphicsSimpleTextItem(self)
        self.details_item = QGraphicsSimpleTextItem(self)
        self.leader_handles = []
        self.leader_anchor_handles = []
        self._positioning_leader_handles = False
        self.resize_handles = []
        self._positioning_resize_handles = False
        self._resizing_from_handle = False
        self._snippet_pixmap = None
        self._redraw()
        self._rebuild_annotation_leader_handles()
        self._rebuild_resize_handles()
        self.set_locked(bool(record.get("locked", False)))

    def is_locked(self):
        return bool(self.record.get("locked", False))

    def set_locked(self, locked):
        self.record["locked"] = bool(locked)
        self.setFlag(QGraphicsItem.ItemIsMovable, not bool(locked))
        self.setCursor(Qt.ArrowCursor if locked else Qt.SizeAllCursor)
        for handle in self.leader_handles + self.leader_anchor_handles:
            handle.setFlag(QGraphicsItem.ItemIsMovable, not bool(locked))
            handle.setVisible(self.isSelected() and not bool(locked))
        for handle in self.resize_handles:
            handle.setFlag(QGraphicsItem.ItemIsMovable, False)
            handle.setVisible(self.isSelected() and not bool(locked))
        self.setToolTip("Locked" if locked else "")

    def _notify_changed(self):
        self.sync_record()
        if self.change_callback is not None:
            self.change_callback()

    def _redraw(self):
        kind = str(self.record.get("type", "text"))
        colour = QColor(str(self.record.get("colour", "#d92d20")))
        pen = QPen(
            colour,
            max(1.0, float(self.record.get("line_width_pt", 1.5)) * self.scale),
        )
        self.shape_item.setPen(pen)
        self.shape_item.setBrush(QBrush(Qt.NoBrush))
        self.panel_item.setVisible(False)
        self.panel_item.setPath(QPainterPath())
        self.text_item.setBrush(QBrush(colour))
        self.text_item.setVisible(True)
        self.wrapped_text_item.setDefaultTextColor(colour)
        self.wrapped_text_item.setVisible(False)
        self.scale_item.setBrush(QBrush(colour))
        self.details_item.setBrush(QBrush(QColor("#334155")))
        font = QFont("Arial", max(6, int(float(self.record.get("font_size_pt", 9)))))
        self.text_item.setFont(font)
        self.wrapped_text_item.setFont(font)
        scale_font = QFont("Arial", max(5, int(float(self.record.get("font_size_pt", 9))) - 2))
        self.scale_item.setFont(scale_font)
        detail_font = QFont("Arial", max(5, int(float(self.record.get("font_size_pt", 9)) - 2)))
        self.details_item.setFont(detail_font)
        self.image_item.setVisible(False)
        self.details_item.setText("")
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
            self._set_wrapped_text()
        elif kind == "page_reference":
            path.addRoundedRect(0.0, 0.0, width, height, 5.0, 5.0)
            self.shape_item.setBrush(QBrush(QColor("#eff6ff")))
            target_number = int(self.record.get("target_page", 0) or 0) + 1
            target_title = str(self.record.get("target_page_title", "") or "").strip()
            self.text_item.setText(f"Go to page {target_number}  >\n{target_title}".strip())
            self.text_item.setPos(max(4.0, 5.0 * self.scale), max(3.0, 4.0 * self.scale))
        elif kind == "network_snippet":
            path.addRoundedRect(0.0, 0.0, width, height, 5.0, 5.0)
            panel_path = QPainterPath()
            panel_path.addRoundedRect(0.0, 0.0, width, height, 5.0, 5.0)
            self.panel_item.setPath(panel_path)
            self.panel_item.setBrush(QBrush(QColor("#ffffff")))
            self.panel_item.setVisible(True)
            self._draw_network_snippet_contents(width, height)
            if not bool(self.record.get("show_leader", True)):
                self.setPen(QPen(Qt.NoPen))
                self.shape_item.setPath(path)
                self._position_annotation_leader_handles()
                return
            anchor = (
                float(self.record.get("anchor_x_pt", 0.0)),
                float(self.record.get("anchor_y_pt", 0.0)),
            )
            points = [
                (float(point[0]), float(point[1]))
                for point in self.record.get("leader_points_pt", []) or []
                if isinstance(point, (list, tuple)) and len(point) >= 2
            ]
            attach = self._annotation_attachment_point(points[-1] if points else anchor)
            local_anchor = self._annotation_pdf_to_local(anchor)
            path.moveTo(*local_anchor)
            if points:
                for point in points:
                    path.lineTo(*self._annotation_pdf_to_local(point))
            else:
                if abs(attach[0] - anchor[0]) >= abs(attach[1] - anchor[1]):
                    path.lineTo(*self._annotation_pdf_to_local((anchor[0], attach[1])))
                else:
                    path.lineTo(*self._annotation_pdf_to_local((attach[0], anchor[1])))
            path.lineTo(*self._annotation_pdf_to_local(attach))
        else:
            self._set_text()
            editor_pen = QPen(QColor("#667085"), 1.0, Qt.DashLine)
            self.setPen(editor_pen)
        if kind != "text":
            self.setPen(QPen(Qt.NoPen))
        self.shape_item.setPath(path)
        self._position_annotation_leader_handles()

    def _draw_network_snippet_contents(self, width, height):
        if self._snippet_pixmap is None:
            try:
                payload = base64.b64decode(str(self.record.get("image_png_base64", "")))
                image = QImage.fromData(payload, "PNG")
                self._snippet_pixmap = QPixmap.fromImage(image) if not image.isNull() else QPixmap()
            except Exception:
                self._snippet_pixmap = QPixmap()
        padding = max(4.0, 3.0 * self.scale)
        title_height = max(15.0, 13.0 * self.scale)
        details = str(self.record.get("resolved_callouts", "") or "").strip()
        detail_lines = len(details.splitlines()) if details else 0
        details_height = max(0.0, detail_lines * max(9.0, 7.5 * self.scale) + padding)
        image_width = max(1.0, width - padding * 2.0)
        image_height = max(1.0, height - title_height - details_height - padding * 2.0)
        requested_width = float(self.record.get("drawing_content_width_pt", 0.0) or 0.0) * self.scale
        requested_height = float(self.record.get("drawing_content_height_pt", 0.0) or 0.0) * self.scale
        target_width = min(image_width, requested_width) if requested_width > 0.0 else image_width
        target_height = min(image_height, requested_height) if requested_height > 0.0 else image_height
        source_width = max(1, self._snippet_pixmap.width())
        source_height = max(1, self._snippet_pixmap.height())
        image_scale = min(
            target_width / float(source_width),
            target_height / float(source_height),
        )
        drawn_width = float(source_width) * image_scale
        drawn_height = float(source_height) * image_scale
        self.image_item.setPixmap(self._snippet_pixmap)
        self.image_item.setScale(image_scale)
        self.image_item.setPos(
            padding + max(0.0, (image_width - drawn_width) / 2.0),
            title_height + padding + max(0.0, (image_height - drawn_height) / 2.0),
        )
        self.image_item.setVisible(not self._snippet_pixmap.isNull())
        self.text_item.setScale(1.0)
        self.text_item.setText(str(self.record.get("title", "Network view")))
        denominator = int(self.record.get("scale_denominator", 0) or 0)
        self.scale_item.setText(f"Scale 1:{denominator}" if denominator > 0 else "")
        self.scale_item.setScale(1.0)
        scale_bounds = self.scale_item.boundingRect()
        self.scale_item.setPos(max(padding, width - padding - scale_bounds.width()), max(1.0, padding / 2.0))
        title_bounds = self.text_item.boundingRect()
        title_available = width - padding * 2.0 - (scale_bounds.width() + padding if denominator > 0 else 0.0)
        self.text_item.setScale(
            min(1.0, max(0.2, title_available / max(1.0, title_bounds.width())))
        )
        self.text_item.setPos(padding, max(1.0, padding / 2.0))
        self.details_item.setScale(1.0)
        self.details_item.setText(details)
        detail_bounds = self.details_item.boundingRect()
        self.details_item.setScale(
            min(1.0, max(0.2, (width - padding * 2.0) / max(1.0, detail_bounds.width())))
        )
        self.details_item.setPos(padding, max(title_height, height - details_height))

    def _annotation_pdf_to_local(self, point):
        return (
            float(point[0]) * self.scale - self.pos().x(),
            (self.page_height_pt - float(point[1])) * self.scale - self.pos().y(),
        )

    def _annotation_local_to_pdf(self, point):
        scene_point = self.mapToScene(point)
        return [
            float(scene_point.x() / self.scale),
            float(self.page_height_pt - scene_point.y() / self.scale),
        ]

    def _annotation_attachment_point(self, reference):
        x = float(self.pos().x() / self.scale)
        y = float(self.page_height_pt - (self.pos().y() + self.rect().height()) / self.scale)
        width = float(self.rect().width() / self.scale)
        height = float(self.rect().height() / self.scale)
        centre_x, centre_y = x + width / 2.0, y + height / 2.0
        dx, dy = centre_x - float(reference[0]), centre_y - float(reference[1])
        if abs(dx) > abs(dy):
            attach_x = x if dx >= 0 else x + width
            return attach_x, min(max(float(reference[1]), y), y + height)
        attach_y = y if dy >= 0 else y + height
        return min(max(float(reference[0]), x), x + width), attach_y

    def _rebuild_resize_handles(self):
        for handle in self.resize_handles:
            if handle.scene() is not None:
                handle.scene().removeItem(handle)
            handle.setParentItem(None)
        self.resize_handles = []
        if self.record.get("type") not in {"network_snippet", "callout"}:
            return
        self.resize_handles = [
            _SnippetResizeHandle(self, edge)
            for edge in ("nw", "n", "ne", "e", "se", "s", "sw", "w")
        ]
        self._position_resize_handles()

    def _position_resize_handles(self):
        if not hasattr(self, "resize_handles"):
            return
        self._positioning_resize_handles = True
        try:
            width, height = self.rect().width(), self.rect().height()
            points = {
                "nw": (0.0, 0.0), "n": (width / 2.0, 0.0), "ne": (width, 0.0),
                "e": (width, height / 2.0), "se": (width, height),
                "s": (width / 2.0, height), "sw": (0.0, height),
                "w": (0.0, height / 2.0),
            }
            for handle in self.resize_handles:
                handle.setPos(*points[handle.edge])
                handle.setVisible(self.isSelected() and not self.is_locked())
        finally:
            self._positioning_resize_handles = False

    def _snippet_resize_handle_moved(self, edge, point):
        if self.is_locked():
            return
        minimum = 32.0
        old_width, old_height = self.rect().width(), self.rect().height()
        left, top, right, bottom = 0.0, 0.0, old_width, old_height
        if "w" in edge:
            left = min(float(point.x()), right - minimum)
        if "e" in edge:
            right = max(float(point.x()), left + minimum)
        if "n" in edge:
            top = min(float(point.y()), bottom - minimum)
        if "s" in edge:
            bottom = max(float(point.y()), top + minimum)
        new_width, new_height = right - left, bottom - top
        if left or top:
            self.setPos(self.pos() + QPointF(left, top))
        self.setRect(0.0, 0.0, new_width, new_height)
        self.record["width_pt"] = new_width / self.scale
        self.record["height_pt"] = new_height / self.scale
        self.record["x_pt"] = self.pos().x() / self.scale
        self.record["y_pt"] = self.page_height_pt - (
            self.pos().y() + new_height
        ) / self.scale
        self._redraw()
        self._position_resize_handles()

    def _resize_snippet_from_drag(self, edge, start_pos, start_size, delta):
        """Resize from an immutable press-time frame to avoid top/left drag feedback."""
        if self.is_locked():
            return
        minimum = 32.0
        start_width, start_height = start_size
        left, top = float(start_pos.x()), float(start_pos.y())
        right, bottom = left + start_width, top + start_height
        dx, dy = float(delta.x()), float(delta.y())
        if "w" in edge:
            left = min(left + dx, right - minimum)
        if "e" in edge:
            right = max(right + dx, left + minimum)
        if "n" in edge:
            top = min(top + dy, bottom - minimum)
        if "s" in edge:
            bottom = max(bottom + dy, top + minimum)
        new_width, new_height = right - left, bottom - top
        self.setPos(left, top)
        self.setRect(0.0, 0.0, new_width, new_height)
        self.record["x_pt"] = left / self.scale
        self.record["y_pt"] = self.page_height_pt - bottom / self.scale
        self.record["width_pt"] = new_width / self.scale
        self.record["height_pt"] = new_height / self.scale
        self._redraw()
        self._position_resize_handles()

    def _rebuild_annotation_leader_handles(self):
        for handle in self.leader_handles + self.leader_anchor_handles:
            if handle.scene() is not None:
                handle.scene().removeItem(handle)
            handle.setParentItem(None)
        self.leader_handles = []
        self.leader_anchor_handles = []
        if self.record.get("type") != "network_snippet" or not bool(self.record.get("show_leader", True)):
            return
        self.leader_anchor_handles.append(_LeaderAnchorHandle(self, 0))
        for point_index, _point in enumerate(self.record.get("leader_points_pt", []) or []):
            self.leader_handles.append(_LeaderBendHandle(self, 0, point_index))
        self._position_annotation_leader_handles()

    def _position_annotation_leader_handles(self):
        if self.record.get("type") != "network_snippet" or not bool(self.record.get("show_leader", True)):
            return
        self._positioning_leader_handles = True
        try:
            for handle in self.leader_anchor_handles:
                handle.setPos(*self._annotation_pdf_to_local((
                    self.record.get("anchor_x_pt", 0.0),
                    self.record.get("anchor_y_pt", 0.0),
                )))
                handle.setVisible(self.isSelected() and not self.is_locked())
            points = self.record.get("leader_points_pt", []) or []
            for handle in self.leader_handles:
                if handle.point_index < len(points):
                    handle.setPos(*self._annotation_pdf_to_local(points[handle.point_index]))
                    handle.setVisible(self.isSelected() and not self.is_locked())
        finally:
            self._positioning_leader_handles = False

    def _leader_anchor_moved(self, _leader_index, handle):
        if self.is_locked():
            return
        self.record["anchor_x_pt"], self.record["anchor_y_pt"] = self._annotation_local_to_pdf(handle.pos())
        self._redraw()

    def _leader_handle_moved(self, _leader_index, point_index, handle):
        if self.is_locked():
            return
        points = [list(point) for point in self.record.get("leader_points_pt", []) or []]
        if 0 <= int(point_index) < len(points):
            points[int(point_index)] = self._annotation_local_to_pdf(handle.pos())
            self.record["leader_points_pt"] = points
            self.record["leader_route_auto"] = False
            self._redraw()

    def add_leader_bend(self):
        if self.is_locked() or self.record.get("type") != "network_snippet":
            return
        anchor = [
            float(self.record.get("anchor_x_pt", 0.0)),
            float(self.record.get("anchor_y_pt", 0.0)),
        ]
        points = [list(point) for point in self.record.get("leader_points_pt", []) or []]
        reference = points[-1] if points else anchor
        attach = self._annotation_attachment_point(reference)
        if not points:
            point = [anchor[0], attach[1]] if abs(attach[0] - anchor[0]) >= abs(attach[1] - anchor[1]) else [attach[0], anchor[1]]
        else:
            point = [(reference[0] + attach[0]) / 2.0, (reference[1] + attach[1]) / 2.0]
        points.append(point)
        self.record["leader_points_pt"] = points
        self.record["leader_route_auto"] = False
        self._rebuild_annotation_leader_handles()
        self._redraw()

    def remove_last_leader_bend(self):
        if self.is_locked():
            return
        points = [list(point) for point in self.record.get("leader_points_pt", []) or []]
        if points:
            points.pop()
        self.record["leader_points_pt"] = points
        self.record["leader_route_auto"] = not points
        self._rebuild_annotation_leader_handles()
        self._redraw()

    def reset_leader_route(self):
        if self.is_locked():
            return
        self.record["leader_points_pt"] = []
        self.record["leader_route_auto"] = True
        if "source_anchor_x_pt" in self.record:
            self.record["anchor_x_pt"] = float(self.record["source_anchor_x_pt"])
            self.record["anchor_y_pt"] = float(self.record["source_anchor_y_pt"])
        self._rebuild_annotation_leader_handles()
        self._redraw()

    def _set_text(self):
        self.wrapped_text_item.setVisible(False)
        self.text_item.setVisible(True)
        self.text_item.setScale(1.0)
        self.text_item.setText(str(self.record.get("text", "")))
        self.text_item.setPos(4.0, 3.0)
        bounds = self.text_item.boundingRect()
        available = max(10.0, self.rect().width() - 8.0)
        self.text_item.setScale(min(1.0, available / max(1.0, bounds.width())))

    def _set_wrapped_text(self):
        self.text_item.setVisible(False)
        self.wrapped_text_item.setVisible(True)
        self.wrapped_text_item.setScale(1.0)
        self.wrapped_text_item.setPlainText(str(self.record.get("text", "")))
        available_width = max(10.0, self.rect().width() - 8.0)
        available_height = max(8.0, self.rect().height() - 6.0)
        self.wrapped_text_item.setTextWidth(available_width)
        bounds = self.wrapped_text_item.boundingRect()
        factor = min(1.0, available_height / max(1.0, bounds.height()))
        self.wrapped_text_item.setScale(factor)
        self.wrapped_text_item.setPos(4.0, 3.0)

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
        elif change == QGraphicsItem.ItemSelectedHasChanged and hasattr(self, "leader_handles"):
            self._position_annotation_leader_handles()
            self._position_resize_handles()
        return result

    def mouseDoubleClickEvent(self, event):
        if self.edit_callback is not None and not self.is_locked():
            self.edit_callback(self)
            event.accept()
            return
        super().mouseDoubleClickEvent(event)

    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        self._notify_changed()


class _RoomCabinetEditorItem(QGraphicsRectItem):
    def __init__(self, editor, row, scale):
        self.editor = editor
        self.row = row
        self.mm_scale = float(scale)
        width_mm = float(row.get("width_mm", editor.layout_data.get("cabinet_width_mm", 600.0)) or 600.0)
        depth_mm = float(row.get("depth_mm", editor.layout_data.get("cabinet_depth_mm", 1070.0)) or 1070.0)
        rotation = int(row.get("rotation_deg", 0) or 0) % 360
        if rotation in {90, 270}:
            width_mm, depth_mm = depth_mm, width_mm
        width, depth = width_mm * scale, depth_mm * scale
        super().__init__(0.0, 0.0, width, depth)
        self.setBrush(QBrush(QColor("#64748b")))
        self.setPen(QPen(QColor("#0f172a"), 1.5))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)
        self.setCursor(Qt.SizeAllCursor)
        label = QGraphicsSimpleTextItem(str(row.get("name", "Cabinet")), self)
        label.setBrush(QBrush(QColor("#ffffff")))
        label.setFont(QFont("Arial", 9, QFont.Bold))
        bounds = label.boundingRect()
        label.setPos(max(2.0, (width - bounds.width()) / 2.0), max(2.0, (depth - bounds.height()) / 2.0))
        face_maps = {
            0: {"front": "front", "rear": "rear", "left": "left", "right": "right"},
            90: {"front": "left", "rear": "right", "left": "rear", "right": "front"},
            180: {"front": "rear", "rear": "front", "left": "right", "right": "left"},
            270: {"front": "right", "rear": "left", "left": "front", "right": "rear"},
        }
        face_lines = {
            "front": ((0.0, depth), (width, depth)),
            "rear": ((0.0, 0.0), (width, 0.0)),
            "left": ((0.0, 0.0), (0.0, depth)),
            "right": ((width, 0.0), (width, depth)),
        }
        faces = [str(face).lower() for face in row.get("accessible_faces", ["front", "rear"])]
        for local_face in faces:
            face = face_maps.get(rotation, face_maps[0]).get(local_face, local_face)
            if face not in face_lines:
                continue
            path = QPainterPath(); path.moveTo(*face_lines[face][0]); path.lineTo(*face_lines[face][1])
            marker = QGraphicsPathItem(path, self)
            marker.setPen(QPen(QColor("#fbbf24"), 4.0))
            marker.setZValue(2)
        face_label = ", ".join(face.title() for face in faces) or "None"
        self.setToolTip(
            f"Rotation: {rotation} degrees. Accessible faces: {face_label}. Drag to rearrange this cabinet; "
            "edges snap to nearby walls and cabinets. Non-conformant positions are allowed but will be warned."
        )

    def mouseMoveEvent(self, event):
        super().mouseMoveEvent(event)
        self.editor._snap_cabinet_item(self)

    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        self.editor._cabinet_item_moved(self)


class _RoomDoorEditorItem(QGraphicsRectItem):
    def __init__(self, editor, scale):
        self.editor = editor
        self.mm_scale = float(scale)
        wall = str(editor.layout_data.get("door_wall", "south"))
        width = float(editor.layout_data.get("door_width_mm", 900.0)) * scale
        if wall in {"north", "south"}:
            rect = QRectF(0.0, -5.0, width, 10.0)
        else:
            rect = QRectF(-5.0, 0.0, 10.0, width)
        super().__init__(rect)
        self.setBrush(QBrush(QColor("#2563eb")))
        self.setPen(QPen(QColor("#ffffff"), 1.0))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)
        self.setCursor(Qt.SizeAllCursor)
        self.setZValue(5)
        self.setToolTip("Drag the door along its selected wall, or enter an exact offset.")

    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        self.editor._door_item_moved(self)


class _RoomLayoutEditorDialog(QDialog):
    """Millimetre-based cabinet and door editor with warning-only validation."""

    def __init__(self, parent, layout_data):
        super().__init__(parent)
        self.layout_data = deepcopy(layout_data)
        self._updating = False
        self.setWindowTitle("Edit cabinet room proposal")
        self.resize(1040, 720)
        root = QVBoxLayout(self)
        help_label = QLabel(
            "Drag cabinets to rearrange them and drag the blue door along its wall. "
            "The editor allows non-conformant proposals, but clearly warns when an accessible face has less than the configured clearance."
        )
        help_label.setWordWrap(True); root.addWidget(help_label)
        content = QHBoxLayout(); root.addLayout(content, 1)
        self.scene = QGraphicsScene(self)
        self.view = QGraphicsView(self.scene)
        self.view.setRenderHint(QPainter.Antialiasing, True)
        self.view.setDragMode(QGraphicsView.RubberBandDrag)
        self.scene.selectionChanged.connect(self._cabinet_selection_changed)
        content.addWidget(self.view, 1)
        controls = QWidget(); form = QFormLayout(controls)
        self.variant_combo = QComboBox()
        self.variant_combo.addItem("Custom arrangement", "custom")
        for value, label in NETWORK_ROOM_LAYOUT_VARIANTS:
            self.variant_combo.addItem(label, value)
        self.variant_combo.setCurrentIndex(max(0, self.variant_combo.findData(str(self.layout_data.get("design_variant", "custom")))))
        self.room_width = QDoubleSpinBox(); self.room_width.setRange(1800, 30000); self.room_width.setSuffix(" mm"); self.room_width.setValue(float(self.layout_data.get("room_width_mm", 3600)))
        self.room_depth = QDoubleSpinBox(); self.room_depth.setRange(1800, 30000); self.room_depth.setSuffix(" mm"); self.room_depth.setValue(float(self.layout_data.get("room_depth_mm", 3470)))
        self.clearance = QDoubleSpinBox(); self.clearance.setRange(300, 3000); self.clearance.setSuffix(" mm"); self.clearance.setValue(float(self.layout_data.get("access_clearance_mm", 1200)))
        self.cabinet_door_swing = QDoubleSpinBox(); self.cabinet_door_swing.setRange(0, 1500); self.cabinet_door_swing.setSuffix(" mm"); self.cabinet_door_swing.setValue(float(self.layout_data.get("cabinet_door_swing_mm", self.layout_data.get("cabinet_width_mm", 600))))
        self.person_width = QDoubleSpinBox(); self.person_width.setRange(300, 1000); self.person_width.setSuffix(" mm"); self.person_width.setValue(float(self.layout_data.get("person_width_mm", 600)))
        self.person_depth = QDoubleSpinBox(); self.person_depth.setRange(200, 1000); self.person_depth.setSuffix(" mm"); self.person_depth.setValue(float(self.layout_data.get("person_depth_mm", 400)))
        self.door_wall = QComboBox()
        for label, value in (("South / bottom", "south"), ("North / top", "north"), ("West / left", "west"), ("East / right", "east")):
            self.door_wall.addItem(label, value)
        self.door_wall.setCurrentIndex(max(0, self.door_wall.findData(str(self.layout_data.get("door_wall", "south")))))
        self.door_offset = QDoubleSpinBox(); self.door_offset.setRange(0, 30000); self.door_offset.setSuffix(" mm"); self.door_offset.setValue(float(self.layout_data.get("door_offset_mm", 150)))
        self.door_width = QDoubleSpinBox(); self.door_width.setRange(600, 3000); self.door_width.setSuffix(" mm clear"); self.door_width.setValue(float(self.layout_data.get("door_width_mm", 900)))
        self.view_angle = QDoubleSpinBox(); self.view_angle.setRange(0, 359); self.view_angle.setSuffix(" deg"); self.view_angle.setValue(float(self.layout_data.get("view_angle_deg", 35)))
        form.addRow("Design variant", self.variant_combo)
        self.auto_arrange_check = QCheckBox("Auto-arrange after room size or cabinet rotation changes")
        self.auto_arrange_check.setChecked(bool(self.layout_data.get("auto_arrange_cabinets", True)))
        form.addRow(self.auto_arrange_check)
        form.addRow("Room width", self.room_width); form.addRow("Room depth", self.room_depth)
        form.addRow("Required clearance", self.clearance)
        form.addRow("Cabinet front-door swing", self.cabinet_door_swing)
        form.addRow("Person plan width", self.person_width)
        form.addRow("Person plan depth", self.person_depth)
        form.addRow("Door wall", self.door_wall)
        form.addRow("Door offset from left/bottom", self.door_offset); form.addRow("Door clear width", self.door_width)
        angle_row = QWidget(); angle_layout = QHBoxLayout(angle_row); angle_layout.setContentsMargins(0, 0, 0, 0)
        left = QPushButton("-15"); right = QPushButton("+15")
        left.clicked.connect(lambda: self.view_angle.setValue((self.view_angle.value() - 15) % 360))
        right.clicked.connect(lambda: self.view_angle.setValue((self.view_angle.value() + 15) % 360))
        angle_layout.addWidget(left); angle_layout.addWidget(self.view_angle); angle_layout.addWidget(right)
        form.addRow("3D viewing angle", angle_row)
        face_box = QGroupBox("Selected cabinet accessible faces")
        face_layout = QVBoxLayout(face_box)
        self.face_checks = {}
        for face in ("front", "rear", "left", "right"):
            check = QCheckBox(face.title())
            check.setEnabled(False)
            check.toggled.connect(self._accessible_faces_changed)
            self.face_checks[face] = check
            face_layout.addWidget(check)
        form.addRow(face_box)
        self.cabinet_x = QDoubleSpinBox(); self.cabinet_x.setRange(0, 30000); self.cabinet_x.setSuffix(" mm from west wall"); self.cabinet_x.setEnabled(False)
        self.cabinet_y = QDoubleSpinBox(); self.cabinet_y.setRange(0, 30000); self.cabinet_y.setSuffix(" mm from south wall"); self.cabinet_y.setEnabled(False)
        self.cabinet_rotation = QComboBox()
        for angle in (0, 90, 180, 270): self.cabinet_rotation.addItem(f"{angle} degrees", angle)
        self.cabinet_rotation.setEnabled(False)
        form.addRow("Cabinet west offset", self.cabinet_x)
        form.addRow("Cabinet south offset", self.cabinet_y)
        form.addRow("Cabinet rotation", self.cabinet_rotation)
        reset_button = QPushButton("Restore suggested arrangement")
        reset_button.clicked.connect(self._restore_suggestion)
        form.addRow(reset_button)
        note = QLabel("1200 mm is the project planning rule. Final design must still follow the selected cabinet manufacturer's service, cooling, fire and local access requirements.")
        note.setWordWrap(True); form.addRow(note)
        content.addWidget(controls)
        self.status = QLabel(); self.status.setWordWrap(True); root.addWidget(self.status)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Ok).setText("Use this arrangement")
        buttons.accepted.connect(self._accept_layout); buttons.rejected.connect(self.reject)
        root.addWidget(buttons)
        for widget in (self.room_width, self.room_depth, self.clearance, self.cabinet_door_swing, self.person_width, self.person_depth, self.door_wall, self.door_offset, self.door_width):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._controls_changed)
            else:
                widget.valueChanged.connect(self._controls_changed)
        self.view_angle.valueChanged.connect(self._angle_changed)
        self.variant_combo.currentIndexChanged.connect(self._variant_changed)
        self.auto_arrange_check.toggled.connect(self._auto_arrange_toggled)
        self.cabinet_x.valueChanged.connect(self._selected_cabinet_geometry_changed)
        self.cabinet_y.valueChanged.connect(self._selected_cabinet_geometry_changed)
        self.cabinet_rotation.currentIndexChanged.connect(self._selected_cabinet_geometry_changed)
        self._rebuild_scene()

    def _sync_controls(self):
        self.layout_data["room_width_mm"] = float(self.room_width.value())
        self.layout_data["room_depth_mm"] = float(self.room_depth.value())
        self.layout_data["access_clearance_mm"] = float(self.clearance.value())
        self.layout_data["cabinet_door_swing_mm"] = float(self.cabinet_door_swing.value())
        self.layout_data["person_width_mm"] = float(self.person_width.value())
        self.layout_data["person_depth_mm"] = float(self.person_depth.value())
        self.layout_data["door_wall"] = str(self.door_wall.currentData())
        self.layout_data["door_offset_mm"] = float(self.door_offset.value())
        self.layout_data["door_width_mm"] = float(self.door_width.value())
        self.layout_data["view_angle_deg"] = float(self.view_angle.value())
        self.layout_data["auto_arrange_cabinets"] = bool(self.auto_arrange_check.isChecked())

    def _controls_changed(self, *_args):
        if self._updating:
            return
        changed_control = self.sender()
        self._sync_controls()
        if self.auto_arrange_check.isChecked() and (
            changed_control is self.room_width or changed_control is self.room_depth
        ):
            self.layout_data = auto_arrange_network_room_cabinets(self.layout_data)
            self._updating = True; self.variant_combo.setCurrentIndex(self.variant_combo.findData("custom")); self._updating = False
        self._rebuild_scene()

    def _auto_arrange_toggled(self, enabled):
        self.layout_data["auto_arrange_cabinets"] = bool(enabled)
        if not enabled or self._updating:
            return
        self._sync_controls()
        self.layout_data = auto_arrange_network_room_cabinets(self.layout_data)
        self._updating = True; self.variant_combo.setCurrentIndex(self.variant_combo.findData("custom")); self._updating = False
        self._rebuild_scene()

    def _angle_changed(self, *_args):
        self.layout_data["view_angle_deg"] = float(self.view_angle.value())

    def _variant_changed(self, *_args):
        if self._updating:
            return
        variant = str(self.variant_combo.currentData() or "custom")
        if variant == "custom":
            self.layout_data["design_variant"] = "custom"
            return
        self._sync_controls()
        self.layout_data = apply_network_room_layout_variant(self.layout_data, variant)
        self._updating = True
        self.room_width.setValue(float(self.layout_data.get("room_width_mm", 3600.0)))
        self.room_depth.setValue(float(self.layout_data.get("room_depth_mm", 3470.0)))
        self._updating = False
        self._rebuild_scene()

    def _selected_cabinet_item(self):
        return next(
            (item for item in self.scene.selectedItems() if isinstance(item, _RoomCabinetEditorItem)),
            None,
        )

    def _cabinet_selection_changed(self):
        item = self._selected_cabinet_item()
        faces = set(item.row.get("accessible_faces", [])) if item is not None else set()
        self._updating = True
        for face, check in self.face_checks.items():
            check.setEnabled(item is not None)
            check.setChecked(face in faces)
        for control in (self.cabinet_x, self.cabinet_y, self.cabinet_rotation):
            control.setEnabled(item is not None)
        if item is not None:
            self.cabinet_x.setValue(float(item.row.get("x_mm", 0.0)))
            self.cabinet_y.setValue(float(item.row.get("y_mm", 0.0)))
            self.cabinet_rotation.setCurrentIndex(
                max(0, self.cabinet_rotation.findData(int(item.row.get("rotation_deg", 0) or 0) % 360))
            )
        self._updating = False

    def _selected_cabinet_geometry_changed(self, *_args):
        if self._updating:
            return
        item = self._selected_cabinet_item()
        if item is None:
            return
        selected_id = str(item.row.get("id", ""))
        item.row["x_mm"] = float(self.cabinet_x.value())
        item.row["y_mm"] = float(self.cabinet_y.value())
        item.row["rotation_deg"] = int(self.cabinet_rotation.currentData() or 0)
        self.layout_data["design_variant"] = "custom"
        if self.auto_arrange_check.isChecked():
            self.layout_data = auto_arrange_network_room_cabinets(self.layout_data)
        self._updating = True; self.variant_combo.setCurrentIndex(self.variant_combo.findData("custom")); self._updating = False
        self._rebuild_scene()
        replacement = next((candidate for candidate in self.scene.items() if isinstance(candidate, _RoomCabinetEditorItem) and str(candidate.row.get("id", "")) == selected_id), None)
        if replacement is not None: replacement.setSelected(True)

    def _accessible_faces_changed(self, *_args):
        if self._updating:
            return
        item = self._selected_cabinet_item()
        if item is None:
            return
        selected_id = str(item.row.get("id", ""))
        item.row["accessible_faces"] = [
            face for face, check in self.face_checks.items() if check.isChecked()
        ]
        self.layout_data["design_variant"] = "custom"
        self._updating = True
        self.variant_combo.setCurrentIndex(self.variant_combo.findData("custom"))
        self._updating = False
        self._rebuild_scene()
        replacement = next(
            (
                candidate
                for candidate in self.scene.items()
                if isinstance(candidate, _RoomCabinetEditorItem)
                and str(candidate.row.get("id", "")) == selected_id
            ),
            None,
        )
        if replacement is not None:
            replacement.setSelected(True)

    def _restore_suggestion(self):
        width = float(self.layout_data.get("cabinet_width_mm", 600.0))
        depth = float(self.layout_data.get("cabinet_depth_mm", 1070.0))
        clearance = float(self.clearance.value())
        cabinets = self.layout_data.get("cabinets", []) or []
        self._updating = True
        self.room_width.setValue(clearance * 2 + width * max(1, len(cabinets)))
        self.room_depth.setValue(clearance * 2 + depth)
        self.door_wall.setCurrentIndex(self.door_wall.findData("south"))
        self.door_offset.setValue(150.0)
        self._updating = False
        self._sync_controls()
        self.layout_data = apply_network_room_layout_variant(self.layout_data, "single_row")
        self._updating = True
        self.variant_combo.setCurrentIndex(self.variant_combo.findData("single_row"))
        self._updating = False
        self._rebuild_scene()

    def _rebuild_scene(self):
        self.scene.clear()
        room_w = float(self.layout_data.get("room_width_mm", 3600.0))
        room_d = float(self.layout_data.get("room_depth_mm", 3470.0))
        scale = min(760.0 / max(1.0, room_w), 540.0 / max(1.0, room_d))
        self._scene_scale = scale; self._origin_x = 30.0; self._origin_y = 30.0
        rect = QRectF(self._origin_x, self._origin_y, room_w * scale, room_d * scale)
        self.scene.addRect(rect, QPen(QColor("#111827"), 4.0), QBrush(QColor("#f8fafc")))
        for row in self.layout_data.get("cabinets", []) or []:
            item = _RoomCabinetEditorItem(self, row, scale)
            item_depth_mm = item.rect().height() / scale
            item.setPos(self._origin_x + float(row.get("x_mm", 0.0)) * scale, self._origin_y + (room_d - float(row.get("y_mm", 0.0)) - item_depth_mm) * scale)
            self.scene.addItem(item)
        self.door_item = _RoomDoorEditorItem(self, scale); self._position_door_item(); self.scene.addItem(self.door_item)
        self.scene.setSceneRect(rect.adjusted(-25, -25, 25, 25)); self.view.fitInView(self.scene.sceneRect(), Qt.KeepAspectRatio)
        self._update_status()

    def _position_door_item(self):
        wall = str(self.layout_data.get("door_wall", "south")); offset = float(self.layout_data.get("door_offset_mm", 0.0)); door = float(self.layout_data.get("door_width_mm", 900.0))
        room_w = float(self.layout_data.get("room_width_mm", 0.0)); room_d = float(self.layout_data.get("room_depth_mm", 0.0)); s = self._scene_scale
        if wall == "south": self.door_item.setPos(self._origin_x + offset*s, self._origin_y + room_d*s)
        elif wall == "north": self.door_item.setPos(self._origin_x + offset*s, self._origin_y)
        elif wall == "west": self.door_item.setPos(self._origin_x, self._origin_y + (room_d-offset-door)*s)
        else: self.door_item.setPos(self._origin_x + room_w*s, self._origin_y + (room_d-offset-door)*s)

    def _cabinet_item_moved(self, item):
        room_d = float(self.layout_data.get("room_depth_mm", 0.0)); s = self._scene_scale
        depth = item.rect().height() / s
        item.row["x_mm"] = round((item.pos().x() - self._origin_x) / s / 25.0) * 25.0
        item.row["y_mm"] = round((room_d - (item.pos().y() - self._origin_y) / s - depth) / 25.0) * 25.0
        item.setPos(self._origin_x + item.row["x_mm"]*s, self._origin_y + (room_d-item.row["y_mm"]-depth)*s)
        self.layout_data["design_variant"] = "custom"
        self._updating = True; self.variant_combo.setCurrentIndex(self.variant_combo.findData("custom")); self._updating = False
        self._cabinet_selection_changed()
        self._update_status()

    def _snap_cabinet_item(self, item):
        """Magnetically align a dragged cabinet with walls and neighbouring cabinets."""
        if self._updating or self._scene_scale <= 0.0:
            return
        s = self._scene_scale
        room_w = float(self.layout_data.get("room_width_mm", 0.0) or 0.0)
        room_d = float(self.layout_data.get("room_depth_mm", 0.0) or 0.0)
        width = item.rect().width() / s
        depth = item.rect().height() / s
        raw_x = (item.pos().x() - self._origin_x) / s
        raw_y = room_d - (item.pos().y() - self._origin_y) / s - depth
        x_targets = [0.0, room_w - width]
        y_targets = [0.0, room_d - depth]
        for row in self.layout_data.get("cabinets", []) or []:
            if row is item.row or not isinstance(row, dict):
                continue
            other_rotation = int(row.get("rotation_deg", 0) or 0) % 360
            row_width = float(row.get("width_mm", self.layout_data.get("cabinet_width_mm", 600.0)) or 600.0)
            row_depth = float(row.get("depth_mm", self.layout_data.get("cabinet_depth_mm", 1070.0)) or 1070.0)
            other_w, other_d = (
                (row_depth, row_width)
                if other_rotation in {90, 270}
                else (row_width, row_depth)
            )
            other_x = float(row.get("x_mm", 0.0) or 0.0)
            other_y = float(row.get("y_mm", 0.0) or 0.0)
            # Align matching edges or place either edge directly beside the other cabinet.
            x_targets.extend((other_x, other_x + other_w - width, other_x - width, other_x + other_w))
            y_targets.extend((other_y, other_y + other_d - depth, other_y - depth, other_y + other_d))

        tolerance = max(50.0, float(self.layout_data.get("snap_tolerance_mm", 125.0) or 125.0))

        def nearest(value, targets):
            target = min(targets, key=lambda candidate: abs(candidate - value))
            return target if abs(target - value) <= tolerance else value

        snapped_x = nearest(raw_x, x_targets)
        snapped_y = nearest(raw_y, y_targets)
        snapped_pos = QPointF(
            self._origin_x + snapped_x * s,
            self._origin_y + (room_d - snapped_y - depth) * s,
        )
        if (snapped_pos - item.pos()).manhattanLength() > 0.01:
            item.setPos(snapped_pos)

    def _door_item_moved(self, item):
        wall = str(self.layout_data.get("door_wall", "south")); room_d = float(self.layout_data.get("room_depth_mm", 0.0)); door = float(self.layout_data.get("door_width_mm", 900.0)); s = self._scene_scale
        if wall in {"north", "south"}: offset = (item.pos().x() - self._origin_x) / s
        else: offset = room_d - (item.pos().y() - self._origin_y) / s - door
        wall_length = float(self.layout_data.get("room_width_mm" if wall in {"north", "south"} else "room_depth_mm", 0.0))
        offset = max(0.0, min(offset, max(0.0, wall_length-door)))
        self.layout_data["door_offset_mm"] = round(offset / 25.0) * 25.0
        self._updating = True; self.door_offset.setValue(self.layout_data["door_offset_mm"]); self._updating = False
        self._position_door_item(); self._update_status()

    def _update_status(self):
        warnings = room_layout_compliance(self.layout_data)
        if warnings:
            self.status.setText("Warning - non-conformant proposal:\n" + "\n".join("- " + warning for warning in warnings))
            self.status.setStyleSheet("color:#991b1b; background:#fef2f2; padding:7px; font-weight:600;")
        else:
            self.status.setText("Conforms to the configured 1200 mm access rule and has no cabinet/door conflicts.")
            self.status.setStyleSheet("color:#166534; background:#f0fdf4; padding:7px; font-weight:600;")

    def _accept_layout(self):
        self._sync_controls(); self.accept()


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
        "snippet_templates": [],
        "extra_pages": [],
        "page_templates": [],
        "saved_report_configurations": {},
    }

    def __init__(
        self,
        preview_builder,
        initial_settings=None,
        parent=None,
        report_title="PDF Report Studio",
        show_report_controls=True,
        network_data=None,
        report_option_groups=None,
    ):
        super().__init__(parent)
        self.setWindowFlags(
            Qt.Window
            | Qt.WindowSystemMenuHint
            | Qt.WindowMinimizeButtonHint
            | Qt.WindowMaximizeButtonHint
            | Qt.WindowCloseButtonHint
        )
        self.setModal(False)
        self.setSizeGripEnabled(True)
        self.preview_builder = preview_builder
        self.settings = deepcopy(self.DEFAULTS)
        self.settings.update(deepcopy(initial_settings or {}))
        self.settings["callouts"] = deepcopy(
            (initial_settings or {}).get("callouts", {}) or {}
        )
        self.settings["annotations"] = deepcopy(
            (initial_settings or {}).get("annotations", []) or []
        )
        self.settings["extra_pages"] = deepcopy(
            (initial_settings or {}).get("extra_pages", []) or []
        )
        template_by_id = {
            str(row.get("id")): deepcopy(row)
            for row in DEFAULT_NETWORK_SNIPPET_TEMPLATES
        }
        for row in (initial_settings or {}).get("snippet_templates", []) or []:
            if isinstance(row, dict) and str(row.get("id", "")).strip():
                template_by_id[str(row["id"])] = deepcopy(row)
        self.settings["snippet_templates"] = list(template_by_id.values())
        page_template_by_id = {
            str(row.get("id")): deepcopy(row) for row in DEFAULT_REPORT_PAGE_TEMPLATES
        }
        for row in (initial_settings or {}).get("page_templates", []) or []:
            if isinstance(row, dict) and str(row.get("id", "")).strip():
                page_template_by_id[str(row["id"])] = deepcopy(row)
        self.settings["page_templates"] = list(page_template_by_id.values())
        self.settings["saved_report_configurations"] = deepcopy(
            (initial_settings or {}).get("saved_report_configurations", {}) or {}
        )
        self.network_data = deepcopy(network_data or {})
        self.report_option_groups = deepcopy(report_option_groups or [])
        self.report_option_checks = {}
        for group in self.report_option_groups:
            setting_key = str(group.get("setting_key", "") or "").strip()
            option_ids = [
                str(option.get("id", "") or "").strip()
                for option in group.get("options", []) or []
                if str(option.get("id", "") or "").strip()
            ]
            if setting_key and setting_key not in self.settings:
                self.settings[setting_key] = option_ids
        self._network_snippet_png_cache = {}
        self.preview_path = ""
        self._composed_preview_path = ""
        self.base_page_count = 0
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
        self.current_page_width_pt = 1.0
        self.base_render_width = 1500
        self.background_item = None
        self.background_render_factor = 1.0
        self._background_render_in_progress = False
        self._releasing_preview = False
        self._undo_stack = []
        self._redo_stack = []
        self._history_current = None
        self._history_suspended = True
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
            "callouts, network-view snippets, revision clouds, rectangles, and polylines."
            if not self.show_report_controls
            else "Review the generated pages before export. Drag callouts on the sheet, "
            "add linked topology or cabinet snippets, hide unwanted information, and "
            "control report layers."
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
        self.add_page_button = QPushButton("Add page...")
        self.add_page_button.clicked.connect(self._add_report_page)
        page_layout.addWidget(self.add_page_button)
        self.save_page_template_button = QPushButton("Save page as template...")
        self.save_page_template_button.clicked.connect(self._save_current_page_as_template)
        page_layout.addWidget(self.save_page_template_button)
        self.manage_page_templates_button = QPushButton("Manage page templates...")
        self.manage_page_templates_button.clicked.connect(self._manage_page_templates)
        page_layout.addWidget(self.manage_page_templates_button)
        self.delete_page_button = QPushButton("Delete added page")
        self.delete_page_button.clicked.connect(self._delete_current_extra_page)
        page_layout.addWidget(self.delete_page_button)
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
        for percentage in (50, 75, 100, 125, 150, 200, 300, 400, 600, 800, 1200, 1600):
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
        self.undo_button = QPushButton("Undo")
        self.undo_button.setToolTip("Undo the last Report Studio change (Ctrl+Z)")
        self.undo_button.clicked.connect(self.undo)
        annotation_row.addWidget(self.undo_button)
        self.redo_button = QPushButton("Redo")
        self.redo_button.setToolTip("Redo the last undone change (Ctrl+Y or Ctrl+Shift+Z)")
        self.redo_button.clicked.connect(self.redo)
        annotation_row.addWidget(self.redo_button)
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
        self.network_snippet_button = QPushButton("Network snippets...")
        self.network_snippet_button.setToolTip(
            "Add saved topology or cabinet views linked to a MER, DER, or comms room"
        )
        self.network_snippet_button.clicked.connect(self._add_network_snippets)
        self.network_snippet_button.setEnabled(False)
        annotation_row.addWidget(self.network_snippet_button)
        self.page_reference_button = QPushButton("Page reference...")
        self.page_reference_button.setToolTip("Add a clickable link to another report page")
        self.page_reference_button.clicked.connect(self._add_page_reference)
        annotation_row.addWidget(self.page_reference_button)
        self.delete_annotation_button = QPushButton("Delete selected items")
        self.delete_annotation_button.setToolTip(
            "Delete custom markup or hide selected generated callouts (Delete)"
        )
        self.delete_annotation_button.clicked.connect(self._delete_selected_items)
        annotation_row.addWidget(self.delete_annotation_button)
        self.lock_button = QPushButton("Lock selected")
        self.lock_button.setToolTip("Prevent selected elements from being changed or moved")
        self.lock_button.setEnabled(False)
        self.lock_button.clicked.connect(self._toggle_selected_items_locked)
        annotation_row.addWidget(self.lock_button)
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
        self.view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.view.customContextMenuRequested.connect(
            self._show_page_context_menu
        )
        self.scene.selectionChanged.connect(self._selection_changed)
        preview_layout.addWidget(self.view, 1)
        splitter.addWidget(preview_panel)

        self.delete_shortcut = QShortcut(QKeySequence.Delete, self.view)
        self.delete_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.delete_shortcut.activated.connect(self._delete_selected_items)
        self.join_shortcut = QShortcut(QKeySequence("Ctrl+J"), self.view)
        self.join_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.join_shortcut.activated.connect(self._join_selected_callouts)
        self.split_shortcut = QShortcut(QKeySequence("Ctrl+Shift+J"), self.view)
        self.split_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.split_shortcut.activated.connect(self._split_selected_callout)
        self.undo_shortcut = QShortcut(QKeySequence.Undo, self)
        self.undo_shortcut.activated.connect(self.undo)
        self.redo_shortcut = QShortcut(QKeySequence.Redo, self)
        self.redo_shortcut.activated.connect(self.redo)
        self.alternate_redo_shortcut = QShortcut(QKeySequence("Ctrl+Shift+Z"), self)
        self.alternate_redo_shortcut.activated.connect(self.redo)

        controls_scroll = QScrollArea()
        controls_scroll.setWidgetResizable(True)
        controls = QWidget()
        controls_layout = QVBoxLayout(controls)

        for group in self.report_option_groups:
            setting_key = str(group.get("setting_key", "") or "").strip()
            options = list(group.get("options", []) or [])
            if not setting_key or not options:
                continue
            option_group = QGroupBox(
                str(group.get("title", "Report fields") or "Report fields")
            )
            option_layout = QVBoxLayout(option_group)
            help_text = str(group.get("help", "") or "").strip()
            if help_text:
                help_label = QLabel(help_text)
                help_label.setWordWrap(True)
                option_layout.addWidget(help_label)
            selected = {
                str(value)
                for value in self.settings.get(setting_key, []) or []
            }
            checks = {}
            for option in options:
                option_id = str(option.get("id", "") or "").strip()
                if not option_id:
                    continue
                check = QCheckBox(
                    str(option.get("label", option_id) or option_id)
                )
                check.setChecked(option_id in selected)
                check.toggled.connect(
                    lambda value, key=setting_key, field=option_id: self._report_option_toggled(
                        key, field, bool(value)
                    )
                )
                checks[option_id] = check
                option_layout.addWidget(check)
            self.report_option_checks[setting_key] = checks
            controls_layout.addWidget(option_group)

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
            check.toggled.connect(
                lambda value, setting_key=key: self._report_setting_changed(
                    setting_key, bool(value)
                )
            )
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
        self.font_scale_spin.valueChanged.connect(
            lambda value: self._report_setting_changed("font_scale", float(value))
        )
        format_form.addRow("Callout text scale", self.font_scale_spin)
        controls_layout.addWidget(formatting)

        configurations = QGroupBox("Saved report configurations")
        configuration_layout = QVBoxLayout(configurations)
        self.report_configuration_combo = QComboBox()
        configuration_layout.addWidget(self.report_configuration_combo)
        configuration_buttons = QHBoxLayout()
        self.load_report_configuration_button = QPushButton("Load")
        self.load_report_configuration_button.clicked.connect(
            self._load_report_configuration
        )
        configuration_buttons.addWidget(self.load_report_configuration_button)
        self.save_report_configuration_button = QPushButton("Save current...")
        self.save_report_configuration_button.clicked.connect(
            self._save_report_configuration
        )
        configuration_buttons.addWidget(self.save_report_configuration_button)
        self.delete_report_configuration_button = QPushButton("Delete")
        self.delete_report_configuration_button.clicked.connect(
            self._delete_report_configuration
        )
        configuration_buttons.addWidget(self.delete_report_configuration_button)
        configuration_layout.addLayout(configuration_buttons)
        controls_layout.addWidget(configurations)
        self._refresh_report_configuration_combo()

        snippet_group = QGroupBox("Network view snippets")
        snippet_layout = QVBoxLayout(snippet_group)
        snippet_help = QLabel(
            "Insert topology or cabinet snapshots linked to room markers. "
            "Saved templates can use live model variables."
        )
        snippet_help.setWordWrap(True)
        snippet_layout.addWidget(snippet_help)
        add_snippet_button = QPushButton("Add snippets to this page...")
        add_snippet_button.clicked.connect(self._add_network_snippets)
        snippet_layout.addWidget(add_snippet_button)
        manage_templates_button = QPushButton("Create / manage snippet templates...")
        manage_templates_button.clicked.connect(self._manage_snippet_templates)
        snippet_layout.addWidget(manage_templates_button)
        controls_layout.addWidget(snippet_group)

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
        self.callout_width.setRange(18.0, 2000.0)
        self.callout_width.setSuffix(" pt")
        self.callout_height = QDoubleSpinBox()
        self.callout_height.setRange(5.0, 2000.0)
        self.callout_height.setSuffix(" pt")
        self.callout_font_size = QDoubleSpinBox()
        self.callout_font_size.setRange(4.0, 36.0)
        self.callout_font_size.setDecimals(1)
        self.callout_font_size.setSuffix(" pt")
        self.callout_wrap_text = QCheckBox("Wrap text to callout width")
        self.callout_wrap_text.setToolTip(
            "Break long generated-callout lines when they no longer fit the callout width."
        )
        size_form.addRow("Width", self.callout_width)
        size_form.addRow("Height", self.callout_height)
        size_form.addRow("Text size", self.callout_font_size)
        callout_layout.addLayout(size_form)
        callout_layout.addWidget(self.callout_wrap_text)
        self.apply_callout_button = QPushButton("Apply callout changes")
        self.apply_callout_button.clicked.connect(self._apply_callout_changes)
        callout_layout.addWidget(self.apply_callout_button)
        leader_help = QLabel(
            "Leader route: drag the coloured endpoint dot to its required position. "
            "Add bends, then drag the white handles through clear space."
        )
        leader_help.setWordWrap(True)
        callout_layout.addWidget(leader_help)
        leader_buttons = QHBoxLayout()
        self.add_leader_bend_button = QPushButton("Add bend")
        self.add_leader_bend_button.clicked.connect(self._add_selected_leader_bend)
        leader_buttons.addWidget(self.add_leader_bend_button)
        self.remove_leader_bend_button = QPushButton("Remove last")
        self.remove_leader_bend_button.clicked.connect(
            self._remove_selected_leader_bend
        )
        leader_buttons.addWidget(self.remove_leader_bend_button)
        callout_layout.addLayout(leader_buttons)
        self.reset_leader_button = QPushButton("Reset automatic leader")
        self.reset_leader_button.clicked.connect(self._reset_selected_leader)
        callout_layout.addWidget(self.reset_leader_button)
        join_help = QLabel(
            "Ctrl-click two or more callouts to combine their text and keep one leader per dot."
        )
        join_help.setWordWrap(True)
        callout_layout.addWidget(join_help)
        self.join_callouts_button = QPushButton("Join selected callouts")
        self.join_callouts_button.clicked.connect(self._join_selected_callouts)
        callout_layout.addWidget(self.join_callouts_button)
        self.split_callout_button = QPushButton("Split joined callout")
        self.split_callout_button.clicked.connect(self._split_selected_callout)
        callout_layout.addWidget(self.split_callout_button)
        self.hide_callout_button = QPushButton("Hide selected callout(s)")
        self.hide_callout_button.clicked.connect(self._hide_selected_callouts)
        callout_layout.addWidget(self.hide_callout_button)
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
        self.snippet_scale_combo = QComboBox()
        self.snippet_scale_combo.addItem("Free size", 0)
        for denominator in (10, 20, 25, 50, 100):
            self.snippet_scale_combo.addItem(f"1:{denominator}", denominator)
        markup_form.addRow("Width", self.markup_width)
        markup_form.addRow("Height", self.markup_height)
        markup_form.addRow("Line width", self.markup_line_width)
        markup_form.addRow("Text size", self.markup_font_size)
        markup_form.addRow("Drawing scale", self.snippet_scale_combo)
        markup_layout.addLayout(markup_form)
        self.snippet_show_leader_check = QCheckBox("Show leader to room marker")
        self.snippet_show_leader_check.hide()
        markup_layout.addWidget(self.snippet_show_leader_check)
        self.markup_colour_button = QPushButton("Choose colour")
        self.markup_colour_button.clicked.connect(self._choose_annotation_colour)
        markup_layout.addWidget(self.markup_colour_button)
        self.edit_markup_text_button = QPushButton("Edit text")
        self.edit_markup_text_button.clicked.connect(self._edit_selected_annotation)
        markup_layout.addWidget(self.edit_markup_text_button)
        self.edit_room_layout_button = QPushButton("Edit cabinet / door layout...")
        self.edit_room_layout_button.clicked.connect(self._edit_selected_room_layout)
        self.edit_room_layout_button.hide()
        markup_layout.addWidget(self.edit_room_layout_button)
        self.export_snippet_png_button = QPushButton("Export placed snippet as PNG...")
        self.export_snippet_png_button.clicked.connect(self._export_selected_snippet_png)
        self.export_snippet_png_button.hide()
        markup_layout.addWidget(self.export_snippet_png_button)
        self.snippet_leader_widget = QWidget()
        snippet_leader_layout = QHBoxLayout(self.snippet_leader_widget)
        snippet_leader_layout.setContentsMargins(0, 0, 0, 0)
        self.snippet_add_bend_button = QPushButton("Add bend")
        self.snippet_add_bend_button.clicked.connect(self._add_selected_leader_bend)
        snippet_leader_layout.addWidget(self.snippet_add_bend_button)
        self.snippet_remove_bend_button = QPushButton("Remove last")
        self.snippet_remove_bend_button.clicked.connect(self._remove_selected_leader_bend)
        snippet_leader_layout.addWidget(self.snippet_remove_bend_button)
        self.snippet_reset_leader_button = QPushButton("Reset leader")
        self.snippet_reset_leader_button.clicked.connect(self._reset_selected_leader)
        snippet_leader_layout.addWidget(self.snippet_reset_leader_button)
        self.snippet_leader_widget.hide()
        markup_layout.addWidget(self.snippet_leader_widget)
        self.apply_markup_button = QPushButton("Apply markup formatting")
        self.apply_markup_button.clicked.connect(self._apply_annotation_changes)
        markup_layout.addWidget(self.apply_markup_button)
        self.markup_controls = (
            self.markup_width,
            self.markup_height,
            self.markup_line_width,
            self.markup_font_size,
            self.snippet_scale_combo,
            self.snippet_show_leader_check,
            self.markup_colour_button,
            self.edit_markup_text_button,
            self.edit_room_layout_button,
            self.export_snippet_png_button,
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
        self._history_suspended = False
        self._history_current = deepcopy(self.settings)
        self._update_history_buttons()

    def _collect_settings(self):
        self._save_current_page_callouts(commit_editor=True)
        for key, check in self.layer_checks.items():
            self.settings[key] = bool(check.isChecked())
        self.settings["font_scale"] = float(self.font_scale_spin.value())
        self._record_history_state()
        return deepcopy(self.settings)

    def _report_setting_changed(self, key, value):
        self.settings[str(key)] = value
        self._record_history_state()

    def _report_option_toggled(self, setting_key, option_id, checked):
        checks = self.report_option_checks.get(str(setting_key), {})
        selected = [
            field_id for field_id, check in checks.items() if check.isChecked()
        ]
        if not selected:
            check = checks.get(str(option_id))
            if check is not None:
                check.blockSignals(True)
                check.setChecked(True)
                check.blockSignals(False)
            QMessageBox.information(
                self,
                "Report fields",
                "Select at least one field for the report.",
            )
            return
        self.settings[str(setting_key)] = selected
        self._record_history_state()
        self.refresh_preview(collect_current=False)

    def _record_history_state(self):
        if self._history_suspended:
            return
        snapshot = deepcopy(self.settings)
        if self._history_current is None:
            self._history_current = snapshot
        elif snapshot != self._history_current:
            self._undo_stack.append(deepcopy(self._history_current))
            del self._undo_stack[:-75]
            self._history_current = snapshot
            self._redo_stack.clear()
        self._update_history_buttons()

    def _update_history_buttons(self):
        if hasattr(self, "undo_button"):
            self.undo_button.setEnabled(bool(self._undo_stack))
            self.redo_button.setEnabled(bool(self._redo_stack))

    def _apply_settings_to_controls(self):
        for key, check in self.layer_checks.items():
            check.blockSignals(True)
            check.setChecked(bool(self.settings.get(key, True)))
            check.blockSignals(False)
        self.font_scale_spin.blockSignals(True)
        self.font_scale_spin.setValue(float(self.settings.get("font_scale", 1.0)))
        self.font_scale_spin.blockSignals(False)
        for setting_key, checks in self.report_option_checks.items():
            selected = {
                str(value)
                for value in self.settings.get(setting_key, []) or []
            }
            if not selected:
                selected = set(checks)
                self.settings[setting_key] = list(checks)
            for option_id, check in checks.items():
                check.blockSignals(True)
                check.setChecked(option_id in selected)
                check.blockSignals(False)
        self._refresh_report_configuration_combo()

    def _restore_history_snapshot(self, snapshot):
        self._history_suspended = True
        try:
            self.settings = deepcopy(snapshot)
            self._history_current = deepcopy(snapshot)
            self._apply_settings_to_controls()
            self.refresh_preview(collect_current=False)
        finally:
            self._history_suspended = False
        self._update_history_buttons()

    def undo(self):
        if not self._undo_stack:
            return
        self._save_current_page_callouts(commit_editor=True)
        if not self._undo_stack:
            return
        target = self._undo_stack.pop()
        self._redo_stack.append(deepcopy(self._history_current))
        self._restore_history_snapshot(target)

    def redo(self):
        if not self._redo_stack:
            return
        target = self._redo_stack.pop()
        self._undo_stack.append(deepcopy(self._history_current))
        self._restore_history_snapshot(target)

    def _refresh_report_configuration_combo(self, selected_name=""):
        if not hasattr(self, "report_configuration_combo"):
            return
        combo = self.report_configuration_combo
        previous = str(selected_name or combo.currentData() or "")
        combo.blockSignals(True)
        combo.clear()
        for name in sorted(
            (self.settings.get("saved_report_configurations", {}) or {}),
            key=str.casefold,
        ):
            combo.addItem(name, name)
        index = combo.findData(previous)
        combo.setCurrentIndex(index if index >= 0 else (0 if combo.count() else -1))
        combo.blockSignals(False)
        enabled = combo.count() > 0
        self.load_report_configuration_button.setEnabled(enabled)
        self.delete_report_configuration_button.setEnabled(enabled)

    def _save_report_configuration(self):
        name, ok = QInputDialog.getText(
            self, "Save report configuration", "Configuration name:"
        )
        name = str(name or "").strip()
        if not ok or not name:
            return
        settings = self._collect_settings()
        settings.pop("saved_report_configurations", None)
        configurations = self.settings.setdefault("saved_report_configurations", {})
        if name in configurations and QMessageBox.question(
            self,
            "Replace report configuration",
            f"Replace the saved configuration '{name}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        ) != QMessageBox.Yes:
            return
        configurations[name] = settings
        self._refresh_report_configuration_combo(name)
        self._record_history_state()

    def _load_report_configuration(self):
        name = str(self.report_configuration_combo.currentData() or "")
        configurations = deepcopy(
            self.settings.get("saved_report_configurations", {}) or {}
        )
        payload = deepcopy(configurations.get(name, {}))
        if not payload:
            return
        self._collect_settings()
        self.settings = deepcopy(self.DEFAULTS)
        self.settings.update(payload)
        self.settings["saved_report_configurations"] = configurations
        template_by_id = {
            str(row.get("id")): deepcopy(row)
            for row in DEFAULT_NETWORK_SNIPPET_TEMPLATES
        }
        for row in self.settings.get("snippet_templates", []) or []:
            if isinstance(row, dict) and str(row.get("id", "")).strip():
                template_by_id[str(row["id"])] = deepcopy(row)
        self.settings["snippet_templates"] = list(template_by_id.values())
        page_template_by_id = {
            str(row.get("id")): deepcopy(row) for row in DEFAULT_REPORT_PAGE_TEMPLATES
        }
        for row in self.settings.get("page_templates", []) or []:
            if isinstance(row, dict) and str(row.get("id", "")).strip():
                page_template_by_id[str(row["id"])] = deepcopy(row)
        self.settings["page_templates"] = list(page_template_by_id.values())
        self._apply_settings_to_controls()
        self._record_history_state()
        self.refresh_preview(collect_current=False)

    def _delete_report_configuration(self):
        name = str(self.report_configuration_combo.currentData() or "")
        if not name:
            return
        if QMessageBox.question(
            self,
            "Delete report configuration",
            f"Delete the saved configuration '{name}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        ) != QMessageBox.Yes:
            return
        self.settings.setdefault("saved_report_configurations", {}).pop(name, None)
        self._refresh_report_configuration_combo()
        self._record_history_state()

    def _edit_network_snippet_template(self, source=None):
        source = deepcopy(source or {})
        dialog = QDialog(self)
        dialog.setWindowTitle(
            "Edit network snippet template" if source else "Create network snippet template"
        )
        dialog.resize(620, 520)
        layout = QVBoxLayout(dialog)
        form = QFormLayout()
        name_edit = QLineEdit(str(source.get("name", "Custom network snippet")))
        view_combo = QComboBox()
        view_combo.addItem("Room topology", "topology")
        view_combo.addItem("Comms room power requirements", "power_summary")
        view_combo.addItem("Cabinet elevation", "cabinet")
        view_combo.addItem("All cabinets elevation", "cabinet_all")
        view_combo.addItem("Scaled room floor layout", "room_layout")
        view_combo.addItem("3D room cutaway", "room_cutaway")
        view_index = view_combo.findData(str(source.get("view_type", "topology")))
        view_combo.setCurrentIndex(max(0, view_index))
        title_edit = QLineEdit(
            str(source.get("title_template", "{room_type} - {location_name} - Floor {floor}"))
        )
        callout_edit = QPlainTextEdit(
            "\n".join(source.get("callout_templates", []) or [])
        )
        callout_edit.setPlaceholderText("One callout per line; variables use {variable_name}")
        width_spin = QDoubleSpinBox(); width_spin.setRange(40.0, 1200.0); width_spin.setSuffix(" pt")
        width_spin.setValue(float(source.get("width_pt", 220.0) or 220.0))
        height_spin = QDoubleSpinBox(); height_spin.setRange(40.0, 1600.0); height_spin.setSuffix(" pt")
        height_spin.setValue(float(source.get("height_pt", 160.0) or 160.0))
        scale_combo = QComboBox(); scale_combo.addItem("Free size", 0)
        for denominator in (10, 20, 25, 50, 100):
            scale_combo.addItem(f"1:{denominator}", denominator)
        scale_index = scale_combo.findData(int(source.get("scale_denominator", 0) or 0))
        scale_combo.setCurrentIndex(max(0, scale_index))
        form.addRow("Template name", name_edit)
        form.addRow("View", view_combo)
        form.addRow("Title", title_edit)
        form.addRow("Common callouts", callout_edit)
        form.addRow("Free-size width", width_spin)
        form.addRow("Free-size height", height_spin)
        form.addRow("Drawing scale", scale_combo)
        layout.addLayout(form)
        variables = QLabel(
            "Available model variables:\n"
            + ", ".join("{" + name + "}" for name in NETWORK_SNIPPET_VARIABLE_NAMES)
        )
        variables.setWordWrap(True)
        variables.setTextInteractionFlags(Qt.TextSelectableByMouse)
        layout.addWidget(variables)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        if dialog.exec() != QDialog.Accepted:
            return None
        name = name_edit.text().strip()
        if not name:
            return None
        return {
            "id": str(source.get("id") or f"custom-snippet-{uuid4().hex}"),
            "name": name,
            "view_type": str(view_combo.currentData()),
            "title_template": title_edit.text().strip(),
            "callout_templates": [
                line.strip() for line in callout_edit.toPlainText().splitlines() if line.strip()
            ],
            "width_pt": float(width_spin.value()),
            "height_pt": float(height_spin.value()),
            "scale_denominator": int(scale_combo.currentData() or 0),
            "builtin": False,
        }

    def _manage_snippet_templates(self):
        working = deepcopy(self.settings.get("snippet_templates", []) or [])
        dialog = QDialog(self)
        dialog.setWindowTitle("Network snippet templates")
        dialog.resize(650, 470)
        layout = QVBoxLayout(dialog)
        template_list = QListWidget()
        layout.addWidget(template_list, 1)
        row = QHBoxLayout()
        new_button = QPushButton("New...")
        edit_button = QPushButton("Edit...")
        delete_button = QPushButton("Delete")
        row.addWidget(new_button); row.addWidget(edit_button); row.addWidget(delete_button); row.addStretch(1)
        layout.addLayout(row)

        def refresh_list(selected_id=""):
            template_list.clear()
            for template in working:
                label = f"{template.get('name', 'Snippet')} - {str(template.get('view_type', '')).title()}"
                if template.get("builtin"):
                    label += " (built in)"
                item = QListWidgetItem(label)
                item.setData(Qt.UserRole, str(template.get("id", "")))
                template_list.addItem(item)
                if str(template.get("id", "")) == selected_id:
                    template_list.setCurrentItem(item)

        def selected_index():
            item = template_list.currentItem()
            if item is None:
                return -1
            template_id = str(item.data(Qt.UserRole) or "")
            return next((index for index, row in enumerate(working) if str(row.get("id", "")) == template_id), -1)

        def add_template():
            value = self._edit_network_snippet_template()
            if value:
                working.append(value); refresh_list(value["id"])

        def edit_template():
            index = selected_index()
            if index < 0:
                return
            source = working[index]
            if source.get("builtin"):
                source = {**source, "id": "", "name": f"{source.get('name')} - custom", "builtin": False}
            value = self._edit_network_snippet_template(source)
            if value:
                if index >= 0 and not working[index].get("builtin"):
                    working[index] = value
                else:
                    working.append(value)
                refresh_list(value["id"])

        def delete_template():
            index = selected_index()
            if index < 0:
                return
            if working[index].get("builtin"):
                QMessageBox.information(dialog, "Built-in template", "Built-in templates cannot be deleted; edit one to create a custom copy.")
                return
            working.pop(index); refresh_list()

        new_button.clicked.connect(add_template)
        edit_button.clicked.connect(edit_template)
        delete_button.clicked.connect(delete_template)
        template_list.itemDoubleClicked.connect(lambda _item: edit_template())
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept); buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        refresh_list()
        if dialog.exec() == QDialog.Accepted:
            self.settings["snippet_templates"] = working

    def _add_report_page(self):
        dialog = QDialog(self); dialog.setWindowTitle("Add report page"); dialog.resize(520, 260)
        layout = QVBoxLayout(dialog); form = QFormLayout()
        title_edit = QLineEdit("Room breakdown")
        template_combo = QComboBox()
        for template in self.settings.get("page_templates", []) or []:
            template_combo.addItem(str(template.get("name", "Page template")), str(template.get("id", "")))
        width_spin = QDoubleSpinBox(); width_spin.setRange(100, 1500); width_spin.setSuffix(" mm")
        height_spin = QDoubleSpinBox(); height_spin.setRange(100, 1500); height_spin.setSuffix(" mm")
        form.addRow("Page title", title_edit); form.addRow("Start from", template_combo)
        form.addRow("Page width", width_spin); form.addRow("Page height", height_spin)
        layout.addLayout(form)
        note = QLabel("You can select a default/custom template and then override its page dimensions.")
        note.setWordWrap(True); layout.addWidget(note)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Ok).setText("Add page")
        buttons.accepted.connect(dialog.accept); buttons.rejected.connect(dialog.reject); layout.addWidget(buttons)
        templates = {str(row.get("id", "")): row for row in self.settings.get("page_templates", []) or []}

        def load_template(*_args):
            template = templates.get(str(template_combo.currentData() or ""), {})
            width_spin.setValue(float(template.get("width_pt", 595.276)) * 25.4 / 72.0)
            height_spin.setValue(float(template.get("height_pt", 841.89)) * 25.4 / 72.0)

        template_combo.currentIndexChanged.connect(load_template); load_template()
        if dialog.exec() != QDialog.Accepted:
            return
        template = deepcopy(templates.get(str(template_combo.currentData() or ""), {}))
        page_index = int(self.base_page_count + len(self.settings.get("extra_pages", []) or []))
        page_id = f"extra-page-{uuid4().hex}"
        self.settings.setdefault("extra_pages", []).append({
            "id": page_id,
            "title": title_edit.text().strip() or f"Added page {page_index + 1}",
            "width_pt": float(width_spin.value()) * 72.0 / 25.4,
            "height_pt": float(height_spin.value()) * 72.0 / 25.4,
            "template_id": str(template.get("id", "")),
        })
        for source in template.get("annotations", []) or []:
            if not isinstance(source, dict):
                continue
            record = deepcopy(source); record["id"] = f"annotation-{uuid4().hex}"; record["page"] = page_index; record.setdefault("visible", True)
            self.settings.setdefault("annotations", []).append(record)
        self.refresh_preview(); self.page_list.setCurrentRow(page_index)

    def _save_current_page_as_template(self):
        if self.current_page < 0 or self.document is None:
            return
        name, accepted = QInputDialog.getText(self, "Save page template", "Template name:", text="Custom room page")
        if not accepted or not name.strip():
            return
        point_size = self.document.pagePointSize(self.current_page)
        annotations = []
        for source in self.settings.get("annotations", []) or []:
            if not isinstance(source, dict) or int(source.get("page", -1)) != self.current_page or source.get("type") == "page_reference":
                continue
            record = deepcopy(source); record.pop("id", None); record.pop("page", None); annotations.append(record)
            if record.get("type") == "network_snippet":
                record["show_leader"] = False
        self.settings.setdefault("page_templates", []).append({
            "id": f"custom-page-template-{uuid4().hex}", "name": name.strip(),
            "width_pt": float(point_size.width()), "height_pt": float(point_size.height()),
            "annotations": annotations, "builtin": False,
        })
        self._record_history_state()
        QMessageBox.information(self, "Page template saved", f"'{name.strip()}' is now available in Add page.")

    def _manage_page_templates(self):
        dialog = QDialog(self); dialog.setWindowTitle("Page templates"); dialog.resize(600, 430)
        layout = QVBoxLayout(dialog); listing = QListWidget(); layout.addWidget(listing, 1)
        remove_button = QPushButton("Delete selected custom template"); layout.addWidget(remove_button)
        close_button = QDialogButtonBox(QDialogButtonBox.Close); close_button.rejected.connect(dialog.reject); layout.addWidget(close_button)

        def refresh():
            listing.clear()
            for template in self.settings.get("page_templates", []) or []:
                width_mm = float(template.get("width_pt", 0.0)) * 25.4 / 72.0; height_mm = float(template.get("height_pt", 0.0)) * 25.4 / 72.0
                item = QListWidgetItem(f"{template.get('name', 'Page')} - {width_mm:.0f} x {height_mm:.0f} mm" + (" (built in)" if template.get("builtin") else ""))
                item.setData(Qt.UserRole, str(template.get("id", ""))); listing.addItem(item)

        def remove():
            item = listing.currentItem()
            if item is None: return
            template_id = str(item.data(Qt.UserRole) or "")
            template = next((row for row in self.settings.get("page_templates", []) or [] if str(row.get("id", "")) == template_id), None)
            if not template or template.get("builtin"):
                QMessageBox.information(dialog, "Built-in template", "Built-in page templates cannot be deleted."); return
            self.settings["page_templates"] = [row for row in self.settings.get("page_templates", []) or [] if str(row.get("id", "")) != template_id]
            self._record_history_state()
            refresh()

        remove_button.clicked.connect(remove); refresh(); dialog.exec()

    def _delete_current_extra_page(self):
        extra_index = self.current_page - int(self.base_page_count)
        pages = self.settings.get("extra_pages", []) or []
        if not (0 <= extra_index < len(pages)):
            return
        title = str(pages[extra_index].get("title", f"Page {self.current_page + 1}"))
        if QMessageBox.question(self, "Delete added page", f"Delete '{title}' and all markup on it?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No) != QMessageBox.Yes:
            return
        self._save_current_page_callouts()
        deleted_page = self.current_page; pages.pop(extra_index); self.settings["extra_pages"] = pages
        adjusted = []
        for row in self.settings.get("annotations", []) or []:
            if not isinstance(row, dict): continue
            source_page = int(row.get("page", -1))
            if source_page == deleted_page: continue
            record = deepcopy(row)
            if source_page > deleted_page: record["page"] = source_page - 1
            if record.get("type") == "page_reference":
                target = int(record.get("target_page", -1))
                if target == deleted_page: continue
                if target > deleted_page: record["target_page"] = target - 1
            adjusted.append(record)
        self.settings["annotations"] = adjusted
        self._record_history_state()
        self.refresh_preview(collect_current=False); self.page_list.setCurrentRow(max(0, deleted_page - 1))

    def _page_display_title(self, page):
        if page >= self.base_page_count:
            index = page - self.base_page_count
            extras = self.settings.get("extra_pages", []) or []
            if 0 <= index < len(extras): return str(extras[index].get("title", f"Added page {page + 1}"))
        item = self.page_list.item(page) if 0 <= page < self.page_list.count() else None
        return str(item.text()).split(". ", 1)[-1] if item else f"Page {page + 1}"

    def _add_page_reference(self):
        if self.document is None or self.document.pageCount() < 2:
            QMessageBox.information(self, "Page reference", "Add at least one more page before creating a page reference."); return
        labels = [f"Page {index + 1} - {self._page_display_title(index)}" for index in range(self.document.pageCount())]
        label, accepted = QInputDialog.getItem(self, "Add page reference", "Link to:", labels, 0, False)
        if not accepted: return
        target = labels.index(label)
        record = {
            "type": "page_reference", "page": int(self.current_page),
            "x_pt": 24.0, "y_pt": 24.0, "width_pt": 150.0, "height_pt": 44.0,
            "colour": "#176b87", "line_width_pt": 1.0, "font_size_pt": 8.0,
            "target_page": target, "target_page_title": self._page_display_title(target), "visible": True,
        }
        item = self._add_annotation(record); self._set_annotation_tool("select")
        if item is not None: item.setSelected(True)

    def _network_room_targets_on_page(self):
        result = {}
        for row in self.manifest:
            if not isinstance(row, dict) or int(row.get("page", -1)) != int(self.current_page):
                continue
            if str(row.get("kind", "")).strip() != "equipment_room":
                continue
            floor = int(row.get("floor", 0) or 0)
            location = str(row.get("location_name", "") or "").strip()
            if not location:
                key = str(row.get("key", ""))
                prefix = f"floor-plan-room:{floor}:"
                location = key[len(prefix):] if key.startswith(prefix) else str(row.get("name", "")).split(" - Floor", 1)[0]
            effective = resolve_callout_override(
                row, self.settings.get("callouts", {}).get(str(row.get("key", "")), {})
            )
            result[(floor, location)] = effective
        return result

    def _update_network_snippet_button(self):
        if hasattr(self, "network_snippet_button"):
            self.network_snippet_button.setEnabled(
                bool(self.network_data and (self._network_room_targets_on_page() or self.current_page >= self.base_page_count))
            )

    def _add_network_snippets(self):
        targets = self._network_room_targets_on_page()
        catalog = network_report_snippet_catalog(
            self.network_data,
            allowed_locations=None if self.current_page >= self.base_page_count else list(targets),
        )
        if not catalog:
            QMessageBox.information(
                self,
                "Network snippets",
                "This page has no MER, DER, or comms-room marker with an available network or cabinet view.",
            )
            return
        dialog = QDialog(self)
        dialog.setWindowTitle("Add network view snippets")
        dialog.resize(720, 560)
        layout = QVBoxLayout(dialog)
        help_label = QLabel(
            "Select one or more room plans, rotatable cutaways, topology, power summaries, or cabinet views. Each snippet is linked to its room marker and can be moved, resized, exported as PNG, or given movable leader bends after placement."
        )
        help_label.setWordWrap(True); layout.addWidget(help_label)
        source_list = QListWidget(); source_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        for spec in catalog:
            item = QListWidgetItem(str(spec.get("title", "Network view")))
            item.setData(Qt.UserRole, spec)
            source_list.addItem(item)
        if source_list.count():
            source_list.item(0).setSelected(True)
        layout.addWidget(source_list, 1)
        form = QFormLayout()
        template_combo = QComboBox(); template_combo.addItem("Automatic built-in template", "")
        for template in self.settings.get("snippet_templates", []) or []:
            template_combo.addItem(
                f"{template.get('name', 'Snippet')} - {str(template.get('view_type', '')).title()}",
                str(template.get("id", "")),
            )
        form.addRow("Snippet template", template_combo)
        leader_check = QCheckBox("Connect snippets to their room markers with leaders")
        leader_check.setChecked(True)
        form.addRow("Leader lines", leader_check)
        layout.addLayout(form)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Ok).setText("Add selected snippets")
        buttons.accepted.connect(dialog.accept); buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        if dialog.exec() != QDialog.Accepted:
            return
        selected_specs = [item.data(Qt.UserRole) for item in source_list.selectedItems()]
        if not selected_specs:
            return
        templates = {
            str(row.get("id", "")): row
            for row in self.settings.get("snippet_templates", []) or []
            if isinstance(row, dict)
        }
        selected_template_id = str(template_combo.currentData() or "")
        if selected_template_id:
            selected_template = templates.get(selected_template_id, {})
            mismatch = next(
                (
                    spec
                    for spec in selected_specs
                    if str(selected_template.get("view_type", ""))
                    != str(spec.get("view_type", ""))
                ),
                None,
            )
            if mismatch is not None:
                QMessageBox.warning(
                    self,
                    "Template does not match",
                    f"The template '{selected_template.get('name', '')}' is for "
                    f"{selected_template.get('view_type', '')} views and cannot be "
                    f"applied to {mismatch.get('view_type', '')}.",
                )
                return
        existing_count = sum(
            1 for row in self.settings.get("annotations", []) or []
            if isinstance(row, dict) and int(row.get("page", -1)) == self.current_page and row.get("type") == "network_snippet"
        )
        topology_specs = [
            spec
            for spec in selected_specs
            if str(spec.get("view_type", "")) not in {"room_layout", "room_cutaway"}
            and str(spec.get("id", "")) not in self._network_snippet_png_cache
        ]
        progress = QProgressDialog(
            "Preparing the shared network view renderer...",
            "Cancel",
            0,
            len(topology_specs) + len(selected_specs),
            self,
        )
        progress.setWindowTitle("Add network snippets")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.show()
        QApplication.processEvents()
        rendered_by_id = {
            str(spec.get("id", "")): self._network_snippet_png_cache[
                str(spec.get("id", ""))
            ]
            for spec in selected_specs
            if str(spec.get("id", "")) in self._network_snippet_png_cache
        }
        if topology_specs:
            try:
                rendered = render_network_report_snippets_png(
                    self.network_data,
                    topology_specs,
                    progress_callback=lambda index, row: self._snippet_render_progress(
                        progress, index, row
                    ),
                )
            except Exception as exc:
                progress.close()
                QMessageBox.critical(self, "Network snippet failed", str(exc))
                return
            if progress.wasCanceled():
                progress.close()
                return
            for spec, payload in zip(topology_specs, rendered):
                snippet_id = str(spec.get("id", ""))
                rendered_by_id[snippet_id] = payload
                self._network_snippet_png_cache[snippet_id] = payload
        for offset, spec in enumerate(selected_specs):
            progress.setValue(len(topology_specs) + offset)
            progress.setLabelText(f"Rendering {spec.get('title', 'network view')}...")
            QApplication.processEvents()
            if progress.wasCanceled():
                break
            if selected_template_id:
                template = templates.get(selected_template_id, {})
            else:
                builtin_id = {
                    "cabinet": "builtin-cabinet-elevation",
                    "cabinet_all": "builtin-all-cabinets-elevation",
                    "room_layout": "builtin-room-floor-layout",
                    "room_cutaway": "builtin-room-cutaway",
                    "power_summary": "builtin-room-power-summary",
                }.get(str(spec.get("view_type", "")), "builtin-room-topology")
                template = templates.get(builtin_id, {})
            self._place_network_snippet(
                spec,
                template,
                existing_count + offset,
                payload=rendered_by_id.get(str(spec.get("id", ""))),
                show_leader=leader_check.isChecked(),
            )
        progress.setValue(len(topology_specs) + len(selected_specs))
        self._save_current_page_callouts()

    @staticmethod
    def _snippet_render_progress(progress, index, spec):
        progress.setValue(int(index))
        progress.setLabelText(f"Rendering {spec.get('title', 'network view')}...")
        QApplication.processEvents()
        return not progress.wasCanceled()

    def _place_network_snippet(
        self, spec, template, sequence, payload=None, show_leader=True
    ):
        target = self._network_room_targets_on_page().get(
            (int(spec.get("floor", 0) or 0), str(spec.get("location_name", "")))
        )
        if target is None and self.current_page < self.base_page_count:
            return None
        view_type = str(spec.get("view_type", ""))
        layout_data = None
        if view_type in {"room_layout", "room_cutaway"}:
            layout_data = suggested_network_room_layout(self.network_data, spec)
            spec = {**spec, "room_layout": layout_data}
        try:
            if payload is None:
                payload = (
                    render_network_room_layout_png(layout_data, view_type)
                    if layout_data is not None
                    else render_network_report_snippet_png(self.network_data, spec)
                )
        except Exception as exc:
            QMessageBox.critical(self, "Network snippet failed", str(exc))
            return None
        snippet_id = str(spec.get("id", ""))
        if snippet_id:
            self._network_snippet_png_cache[snippet_id] = payload
        variables = network_report_snippet_variables(self.network_data, spec)
        title = format_network_snippet_template(template.get("title_template", spec.get("title", "Network view")), variables)
        callouts = [
            format_network_snippet_template(line, variables)
            for line in template.get("callout_templates", []) or []
            if str(line).strip()
        ]
        denominator = int(template.get("scale_denominator", 0) or 0) if view_type in {"cabinet", "cabinet_all", "room_layout"} else 0
        if view_type in {"cabinet", "cabinet_all"}:
            physical_width_mm, physical_height_mm = cabinet_snippet_physical_size_mm(self.network_data, spec)
        elif view_type == "room_layout":
            physical_width_mm = float(layout_data.get("room_width_mm", 0.0))
            physical_height_mm = float(layout_data.get("room_depth_mm", 0.0))
        else:
            physical_width_mm, physical_height_mm = 0.0, 0.0
        if denominator > 0:
            content_width, content_height = _scaled_snippet_image_size(
                payload,
                physical_width_mm,
                physical_height_mm,
                denominator,
            )
            width = max(float(template.get("width_pt", 180.0) or 180.0), content_width + 10.0)
            height = content_height + 24.0 + max(0, len(callouts)) * 14.0
        else:
            width = float(template.get("width_pt", 240.0) or 240.0)
            height = float(template.get("height_pt", 165.0) or 165.0)
        anchor_x = float(target.get("anchor_x_pt", 0.0) or 0.0) if target else self.current_page_width_pt / 2.0
        anchor_y = float(target.get("anchor_y_pt", 0.0) or 0.0) if target else self.current_page_height_pt / 2.0
        x = max(12.0, self.current_page_width_pt - width - 16.0) if anchor_x < self.current_page_width_pt / 2.0 else 16.0
        y = max(12.0, self.current_page_height_pt - height - 18.0 - (sequence % 5) * 18.0)
        record = {
            "type": "network_snippet",
            "page": int(self.current_page),
            "x_pt": x,
            "y_pt": y,
            "width_pt": width,
            "height_pt": height,
            "anchor_x_pt": anchor_x,
            "anchor_y_pt": anchor_y,
            "source_anchor_x_pt": anchor_x,
            "source_anchor_y_pt": anchor_y,
            "leader_points_pt": [],
            "leader_route_auto": True,
            "show_leader": bool(show_leader and target is not None),
            "leader_available": target is not None,
            "colour": "#176b87",
            "line_width_pt": 1.0,
            "font_size_pt": 8.0,
            "title": title,
            "resolved_callouts": "\n".join(callouts),
            "image_png_base64": base64.b64encode(payload).decode("ascii"),
            "snippet_id": str(spec.get("id", "")),
            "snippet_view_type": str(spec.get("view_type", "")),
            "template_id": str(template.get("id", "")),
            "location_name": str(spec.get("location_name", "")),
            "rack_name": str(spec.get("rack_name", "")),
            "floor": int(spec.get("floor", 0) or 0),
            "model_variables": variables,
            "room_layout": deepcopy(layout_data) if layout_data is not None else None,
            "callout_templates": list(template.get("callout_templates", []) or []),
            "physical_width_mm": physical_width_mm,
            "physical_height_mm": physical_height_mm,
            "scale_denominator": denominator,
            "drawing_content_width_pt": content_width if denominator > 0 else 0.0,
            "drawing_content_height_pt": content_height if denominator > 0 else 0.0,
            "visible": True,
        }
        item = self._add_annotation(record)
        self._set_annotation_tool("select")
        return item

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
                "locked": bool(record.get("locked", False)),
                "auto_fit_text": bool(record.get("auto_fit_text", False)),
                "font_size_pt": float(record.get("font_size_pt", 9.0) or 9.0),
                "wrap_text": bool(record.get("wrap_text", True)),
            }
            leader_points = record.get("leader_points_pt", []) or []
            if leader_points:
                self.settings["callouts"][record["key"]]["leader_points_pt"] = [
                    [float(point[0]), float(point[1])]
                    for point in leader_points
                    if isinstance(point, (list, tuple)) and len(point) >= 2
                ]
            leaders = record.get("leaders_pt", []) or []
            if leaders:
                self.settings["callouts"][record["key"]]["leaders_pt"] = deepcopy(
                    leaders
                )
            joined_keys = record.get("joined_callout_keys", []) or []
            if joined_keys:
                self.settings["callouts"][record["key"]]["joined_callout_keys"] = [
                    str(key) for key in joined_keys
                ]
            if record.get("joined_into_key"):
                self.settings["callouts"][record["key"]]["joined_into_key"] = str(
                    record["joined_into_key"]
                )
        annotations_by_id = {
            str(row.get("id")): deepcopy(row)
            for row in self.settings.get("annotations", []) or []
            if isinstance(row, dict) and row.get("id")
        }
        for item in self.annotation_items:
            record = item.sync_record()
            annotations_by_id[str(record["id"])] = deepcopy(record)
        self.settings["annotations"] = list(annotations_by_id.values())
        self._record_history_state()

    def _commit_selected_editor(self):
        if self._updating_properties:
            return
        item = self._property_item or self._selected_callout()
        if item is None or item.is_locked():
            return
        item.set_text(self.callout_text.toPlainText().strip())
        item.set_callout_visible(self.callout_visible.isChecked())
        item.record["auto_fit_text"] = False
        item.set_font_size(self.callout_font_size.value())
        item.set_wrap_text(self.callout_wrap_text.isChecked())
        width = self.callout_width.value()
        height = self.callout_height.value()
        item.set_size_points(width, height)
        if self.callout_wrap_text.isChecked():
            height = max(height, item.required_wrapped_height_points())
            item.set_size_points(width, height)
            self.callout_height.setValue(height)
        item.sync_record()

    def refresh_preview(self, *_signal_args, collect_current=True):
        self.detail_render_timer.stop()
        if collect_current:
            settings = self._collect_settings()
        else:
            self._property_item = None
            self.scene.clear()
            self.callout_items = []
            self.annotation_items = []
            settings = deepcopy(self.settings)
        try:
            base_path, manifest = self.preview_builder(settings)
        except Exception as exc:
            QMessageBox.critical(self, "Report preview failed", str(exc))
            return
        self.base_preview_path = str(base_path)
        self.manifest = list(manifest or [])
        self.document.close()
        if self._composed_preview_path:
            try:
                Path(self._composed_preview_path).unlink(missing_ok=True)
            except OSError:
                pass
            self._composed_preview_path = ""
        try:
            from pypdf import PdfReader
            self.base_page_count = len(PdfReader(str(base_path)).pages)
            if self.settings.get("extra_pages", []) or []:
                preview_dir = Path.cwd() / "tmp" / "pdfs"
                preview_dir.mkdir(parents=True, exist_ok=True)
                composed = preview_dir / f"report_studio_pages_{uuid4().hex}.pdf"
                append_pdf_studio_blank_pages(base_path, composed, self.settings.get("extra_pages", []))
                self._composed_preview_path = str(composed)
                self.preview_path = str(composed)
            else:
                self.preview_path = str(base_path)
        except Exception as exc:
            QMessageBox.critical(self, "Report page composition failed", str(exc))
            return
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
            if page >= self.base_page_count:
                extra_index = page - self.base_page_count
                extras = self.settings.get("extra_pages", []) or []
                if 0 <= extra_index < len(extras):
                    label = str(extras[extra_index].get("title", f"Added page {page + 1}"))
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
        target = max(
            0.25,
            min(MAX_REPORT_ZOOM_FACTOR, self.zoom_factor * float(multiplier)),
        )
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
            self._releasing_preview
            or self._background_render_in_progress
            or self.background_item is None
            or self.document is None
            or self.document.pageCount() <= 0
            or self.current_page < 0
            or self.current_page >= self.document.pageCount()
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
        # QPdfDocument.render and QPixmap briefly coexist in memory. Keeping the
        # detail image below about 64 MB avoids native Qt allocation failures
        # while 1600% view zoom remains available.
        max_side_factor = 6000.0 / max(base_width, base_height)
        max_pixel_factor = math.sqrt(
            16_000_000.0 / max(1.0, float(base_width * base_height))
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
        page = int(self.current_page)
        background_item = self.background_item
        self._background_render_in_progress = True
        try:
            image = self.document.render(page, QSize(render_width, render_height))
            if image.isNull() or self._releasing_preview:
                return
            if page != self.current_page or background_item is not self.background_item:
                return
            background_item.setPixmap(QPixmap.fromImage(image))
            background_item.setScale(base_width / float(render_width))
            self.background_render_factor = render_width / float(base_width)
        finally:
            self._background_render_in_progress = False
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
        target = max(
            0.25,
            min(MAX_REPORT_ZOOM_FACTOR, float(percentage) / 100.0),
        )
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
            change_callback=self._annotation_item_changed,
        )
        self.scene.addItem(item)
        self.annotation_items.append(item)
        item.setSelected(True)
        self._set_annotation_tool("select")
        self._save_current_page_callouts()
        return item

    def _annotation_item_changed(self):
        self._save_current_page_callouts()
        # Keep the property editor in step with handle drags. Otherwise a later
        # refresh/export can reapply the width and height from before the resize.
        self._selection_changed()
        self._update_overlap_status()

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
        if item.is_locked() or item.record.get("type") not in {"text", "callout"}:
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
            item._notify_changed()

    def _delete_selected_annotations(self):
        selected = [
            item
            for item in self.scene.selectedItems()
            if isinstance(item, _AnnotationItem) and not item.is_locked()
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
        self._save_current_page_callouts()
        self._selection_changed()

    @staticmethod
    def _editable_scene_item(item):
        current = item
        while current is not None:
            if isinstance(current, (_CalloutItem, _AnnotationItem)):
                return current
            current = current.parentItem()
        return None

    def _scene_item_at_view_position(self, position):
        scene_position = self.view.mapToScene(position)
        for item in self.scene.items(scene_position):
            editable = self._editable_scene_item(item)
            if editable is not None:
                return editable
        return None

    def _delete_selected_items(self):
        """Delete custom markup and hide generated callouts from the PDF."""
        selected_annotations = [
            item
            for item in self.scene.selectedItems()
            if isinstance(item, _AnnotationItem) and not item.is_locked()
        ]
        selected_callouts = [
            item for item in self._selected_callouts() if not item.is_locked()
        ]
        if selected_annotations:
            self._delete_selected_annotations()
        if selected_callouts:
            for item in selected_callouts:
                item.set_callout_visible(False)
            self._save_current_page_callouts()
            self._selection_changed()
            self._update_overlap_status()

    def _set_selected_callouts_visible(self, visible):
        selected = [item for item in self._selected_callouts() if not item.is_locked()]
        if not selected:
            return
        for item in selected:
            item.set_callout_visible(bool(visible))
        self._save_current_page_callouts()
        self._selection_changed()
        self._update_overlap_status()

    def _edit_selected_callout_text(self):
        item = self._selected_callout()
        if item is None or item.is_locked():
            return
        value, ok = QInputDialog.getMultiLineText(
            self,
            "Edit generated callout",
            "Callout text:",
            str(item.record.get("text", "")),
        )
        if not ok:
            return
        item.set_text(str(value).strip())
        item.sync_record()
        self._save_current_page_callouts()
        self._selection_changed()

    def _select_all_page_callouts(self):
        self.scene.clearSelection()
        for item in self.callout_items:
            item.setSelected(True)

    def _show_page_context_menu(self, position):
        item = self._scene_item_at_view_position(position)
        if item is not None and not item.isSelected():
            self.scene.clearSelection()
            item.setSelected(True)
        menu = QMenu(self)
        if item is not None:
            lock_action = menu.addAction(
                "Unlock selected" if item.is_locked() else "Lock selected"
            )
            lock_action.triggered.connect(
                lambda _checked=False, locked=not item.is_locked():
                self._set_selected_items_locked(locked)
            )
            menu.addSeparator()
        if isinstance(item, _CalloutItem):
            editable = not item.is_locked()
            edit_action = menu.addAction("Edit callout text...")
            edit_action.setEnabled(editable)
            edit_action.triggered.connect(self._edit_selected_callout_text)
            menu.addSeparator()
            add_bend_action = menu.addAction("Add leader bend")
            add_bend_action.setEnabled(editable)
            add_bend_action.triggered.connect(self._add_selected_leader_bend)
            remove_bend_action = menu.addAction("Remove last leader bend")
            remove_bend_action.setEnabled(
                editable
                and any(route.get("points_pt", []) for route in item._leader_routes())
            )
            remove_bend_action.triggered.connect(
                self._remove_selected_leader_bend
            )
            reset_action = menu.addAction("Reset leader and endpoint")
            reset_action.setEnabled(editable)
            reset_action.triggered.connect(self._reset_selected_leader)
            menu.addSeparator()
            join_action = menu.addAction("Join selected callouts\tCtrl+J")
            join_action.setEnabled(
                editable
                and len(self._selected_callouts()) >= 2
                and not any(row.is_locked() for row in self._selected_callouts())
            )
            join_action.triggered.connect(self._join_selected_callouts)
            split_action = menu.addAction("Split joined callout\tCtrl+Shift+J")
            split_action.setEnabled(
                editable and bool(item.record.get("joined_callout_keys", []))
            )
            split_action.triggered.connect(self._split_selected_callout)
            menu.addSeparator()
            if bool(item.record.get("visible", True)):
                visibility_action = menu.addAction("Hide from final PDF\tDelete")
                visibility_action.setEnabled(editable)
                visibility_action.triggered.connect(
                    lambda: self._set_selected_callouts_visible(False)
                )
            else:
                visibility_action = menu.addAction("Include in final PDF")
                visibility_action.setEnabled(editable)
                visibility_action.triggered.connect(
                    lambda: self._set_selected_callouts_visible(True)
                )
        elif isinstance(item, _AnnotationItem):
            editable = not item.is_locked()
            if item.record.get("type") == "network_snippet":
                if item.record.get("snippet_view_type") in {"room_layout", "room_cutaway"}:
                    room_layout_action = menu.addAction("Edit cabinet / door layout...")
                    room_layout_action.setEnabled(editable)
                    room_layout_action.triggered.connect(self._edit_selected_room_layout)
                export_png_action = menu.addAction("Export placed snippet as PNG...")
                export_png_action.triggered.connect(self._export_selected_snippet_png)
                leader_available = bool(
                    item.record.get(
                        "leader_available", item.record.get("show_leader", False)
                    )
                )
                leader_action = menu.addAction(
                    "Remove leader"
                    if bool(item.record.get("show_leader", True))
                    else "Add leader to room marker"
                )
                leader_action.setEnabled(editable and leader_available)
                leader_action.triggered.connect(
                    lambda _checked=False, visible=not bool(
                        item.record.get("show_leader", True)
                    ): self._set_selected_snippet_leader(visible)
                )
                menu.addSeparator()
                add_bend_action = menu.addAction("Add leader bend")
                add_bend_action.setEnabled(editable)
                add_bend_action.triggered.connect(self._add_selected_leader_bend)
                remove_bend_action = menu.addAction("Remove last leader bend")
                remove_bend_action.setEnabled(
                    editable and bool(item.record.get("leader_points_pt", []) or [])
                )
                remove_bend_action.triggered.connect(self._remove_selected_leader_bend)
                reset_action = menu.addAction("Reset leader and endpoint")
                reset_action.setEnabled(editable)
                reset_action.triggered.connect(self._reset_selected_leader)
                menu.addSeparator()
            edit_action = menu.addAction("Edit text...")
            edit_action.setEnabled(
                editable and item.record.get("type") in {"text", "callout"}
            )
            edit_action.triggered.connect(lambda: self._edit_annotation(item))
            colour_action = menu.addAction("Choose colour...")
            colour_action.setEnabled(editable)
            colour_action.triggered.connect(self._choose_annotation_colour)
            menu.addSeparator()
            delete_action = menu.addAction("Delete selected markup\tDelete")
            delete_action.setEnabled(editable)
            delete_action.triggered.connect(self._delete_selected_annotations)
        else:
            if self.network_data and (self._network_room_targets_on_page() or self.current_page >= self.base_page_count):
                snippet_action = menu.addAction("Add network view snippets...")
                snippet_action.triggered.connect(self._add_network_snippets)
                menu.addSeparator()
            if self.document is not None and self.document.pageCount() > 1:
                page_reference_action = menu.addAction("Add page reference...")
                page_reference_action.triggered.connect(self._add_page_reference)
                menu.addSeparator()
            for tool, label in (
                ("text", "Add text"),
                ("callout", "Add point callout"),
                ("rectangle", "Add rectangle"),
                ("revision_cloud", "Add revision cloud"),
                ("polyline", "Add polyline"),
            ):
                action = menu.addAction(label)
                action.triggered.connect(
                    lambda checked=False, value=tool: self._set_annotation_tool(value)
                )
            if self.callout_items:
                menu.addSeparator()
                select_all_action = menu.addAction("Select all callouts on page")
                select_all_action.triggered.connect(self._select_all_page_callouts)
        menu.addSeparator()
        undo_action = menu.addAction("Undo\tCtrl+Z")
        undo_action.setEnabled(bool(self._undo_stack))
        undo_action.triggered.connect(self.undo)
        redo_action = menu.addAction("Redo\tCtrl+Y")
        redo_action.setEnabled(bool(self._redo_stack))
        redo_action.triggered.connect(self.redo)
        menu.addSeparator()
        fit_action = menu.addAction("Fit page")
        fit_action.triggered.connect(self._fit_page)
        refresh_action = menu.addAction("Refresh PDF preview")
        refresh_action.triggered.connect(self.refresh_preview)
        menu.exec(self.view.viewport().mapToGlobal(position))

    def _show_page(self, page):
        self.detail_render_timer.stop()
        self._save_current_page_callouts(commit_editor=True)
        if page < 0 or page >= self.document.pageCount():
            return
        self.current_page = int(page)
        self.delete_page_button.setEnabled(self.current_page >= self.base_page_count)
        self.page_reference_button.setEnabled(self.document.pageCount() > 1)
        point_size = self.document.pagePointSize(page)
        render_width = int(self.base_render_width)
        scale = render_width / max(1.0, float(point_size.width()))
        self.page_render_scale = float(scale)
        self.current_page_width_pt = float(point_size.width())
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
            record = resolve_callout_override(record, override)
            if record.get("joined_into_key"):
                continue
            item = _CalloutItem(
                record,
                scale,
                float(point_size.height()),
                self._annotation_item_changed,
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
                change_callback=self._annotation_item_changed,
            )
            self.scene.addItem(item)
            self.annotation_items.append(item)
        self.scene.setSceneRect(0, 0, image.width(), image.height())
        self._fit_page()
        self._update_overlap_status()
        self._update_network_snippet_button()

    def _selected_callout(self):
        selected = self._selected_callouts()
        return selected[0] if selected else None

    def _selected_callouts(self):
        return [
            item
            for item in self.scene.selectedItems()
            if isinstance(item, _CalloutItem)
        ]

    def _selected_annotation(self):
        return next(
            (
                item
                for item in self.scene.selectedItems()
                if isinstance(item, _AnnotationItem)
            ),
            None,
        )

    def _selected_editable_items(self):
        return [
            item
            for item in self.scene.selectedItems()
            if isinstance(item, (_CalloutItem, _AnnotationItem))
        ]

    def _toggle_selected_items_locked(self):
        selected = self._selected_editable_items()
        if not selected:
            return
        lock = any(not item.is_locked() for item in selected)
        for item in selected:
            item.set_locked(lock)
            item.sync_record()
        self._save_current_page_callouts()
        self._selection_changed()

    def _set_selected_snippet_leader(self, visible):
        item = self._selected_annotation()
        if (
            item is None
            or item.is_locked()
            or item.record.get("type") != "network_snippet"
        ):
            return
        available = bool(
            item.record.get(
                "leader_available", item.record.get("show_leader", False)
            )
        )
        item.record["show_leader"] = bool(visible and available)
        item._rebuild_annotation_leader_handles()
        item._redraw()
        item.sync_record()
        self._save_current_page_callouts()
        self._selection_changed()

    def _set_selected_items_locked(self, locked):
        selected = self._selected_editable_items()
        if not selected:
            return
        for item in selected:
            item.set_locked(bool(locked))
            item.sync_record()
        self._save_current_page_callouts()
        self._selection_changed()

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
        enabled = item is not None and not item.is_locked()
        for widget in (
            self.callout_text,
            self.callout_visible,
            self.callout_width,
            self.callout_height,
            self.callout_font_size,
            self.callout_wrap_text,
            self.apply_callout_button,
            self.add_leader_bend_button,
            self.remove_leader_bend_button,
            self.reset_leader_button,
            self.split_callout_button,
            self.hide_callout_button,
        ):
            widget.setEnabled(enabled)
        if item is None:
            self.callout_name.setText("Select a callout on the page")
            self.callout_text.clear()
        else:
            display_name = str(item.record.get("name", "") or "").strip()
            if display_name:
                self.callout_name.setText(
                    f"{display_name}{' (locked)' if item.is_locked() else ''}"
                )
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
            self.callout_font_size.setValue(
                float(item.record.get("font_size_pt", 9.0) or 9.0)
            )
            self.callout_wrap_text.setChecked(
                bool(item.record.get("wrap_text", True))
            )
            if item.is_locked() and not self.callout_name.text().endswith("(locked)"):
                self.callout_name.setText(self.callout_name.text() + " (locked)")
        has_leader_bends = bool(
            item
            and any(route.get("points_pt", []) for route in item._leader_routes())
        )
        self.remove_leader_bend_button.setEnabled(enabled and has_leader_bends)
        self.reset_leader_button.setEnabled(enabled and has_leader_bends)
        selected_callouts = self._selected_callouts()
        self.join_callouts_button.setEnabled(
            len(selected_callouts) >= 2
            and not any(row.is_locked() for row in selected_callouts)
        )
        self.split_callout_button.setEnabled(
            bool(enabled and (item.record.get("joined_callout_keys", []) or []))
        )

        annotation = self._selected_annotation()
        markup_enabled = annotation is not None and not annotation.is_locked()
        for widget in self.markup_controls:
            widget.setEnabled(markup_enabled)
        if annotation is None:
            self.markup_name.setText("Select a custom markup on the page")
            self.markup_colour_button.setStyleSheet("")
            self.snippet_leader_widget.hide()
            self.snippet_show_leader_check.hide()
            self.edit_room_layout_button.hide()
            self.export_snippet_png_button.hide()
        else:
            kind = str(annotation.record.get("type", "markup")).replace("_", " ")
            self.markup_name.setText(
                kind.title() + (" (locked)" if annotation.is_locked() else "")
            )
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
                markup_enabled
                and annotation.record.get("type") in {"text", "callout"}
            )
            is_snippet = annotation.record.get("type") == "network_snippet"
            is_room_proposal = is_snippet and annotation.record.get("snippet_view_type") in {"room_layout", "room_cutaway"}
            scale_index = self.snippet_scale_combo.findData(
                int(annotation.record.get("scale_denominator", 0) or 0)
            )
            self.snippet_scale_combo.setCurrentIndex(max(0, scale_index))
            self.snippet_scale_combo.setEnabled(markup_enabled and is_snippet)
            leader_available = bool(
                annotation.record.get(
                    "leader_available", annotation.record.get("show_leader", False)
                )
            )
            show_leader = bool(annotation.record.get("show_leader", True))
            self.snippet_show_leader_check.blockSignals(True)
            self.snippet_show_leader_check.setChecked(show_leader)
            self.snippet_show_leader_check.blockSignals(False)
            self.snippet_show_leader_check.setVisible(is_snippet)
            self.snippet_show_leader_check.setEnabled(
                markup_enabled and is_snippet and leader_available
            )
            self.snippet_show_leader_check.setToolTip(
                "Connect this snippet to its original room marker"
                if leader_available
                else "This snippet was placed on a page without a matching room marker"
            )
            self.snippet_leader_widget.setVisible(
                is_snippet and leader_available and show_leader
            )
            self.edit_room_layout_button.setVisible(is_room_proposal)
            self.edit_room_layout_button.setEnabled(markup_enabled and is_room_proposal)
            self.export_snippet_png_button.setVisible(is_snippet)
            self.export_snippet_png_button.setEnabled(is_snippet)
            snippet_has_bends = bool(annotation.record.get("leader_points_pt", []) or [])
            self.snippet_add_bend_button.setEnabled(markup_enabled and is_snippet)
            self.snippet_remove_bend_button.setEnabled(markup_enabled and snippet_has_bends)
            self.snippet_reset_leader_button.setEnabled(markup_enabled and is_snippet)
        selected_elements = self._selected_editable_items()
        self.lock_button.setEnabled(bool(selected_elements))
        if selected_elements and all(row.is_locked() for row in selected_elements):
            self.lock_button.setText("Unlock selected")
            self.lock_button.setToolTip("Allow the selected elements to be edited again")
        else:
            self.lock_button.setText("Lock selected")
            self.lock_button.setToolTip("Prevent selected elements from being changed or moved")
        self.delete_annotation_button.setEnabled(
            any(not row.is_locked() for row in selected_elements)
        )
        self._updating_properties = False

    def _apply_annotation_changes(self):
        item = self._selected_annotation()
        if item is None or item.is_locked():
            return
        old_width = max(0.001, float(item.record.get("width_pt", 8.0)))
        old_height = max(0.001, float(item.record.get("height_pt", 8.0)))
        new_width = float(self.markup_width.value())
        new_height = float(self.markup_height.value())
        if item.record.get("type") == "network_snippet":
            denominator = int(self.snippet_scale_combo.currentData() or 0)
            item.record["scale_denominator"] = denominator
            leader_available = bool(
                item.record.get(
                    "leader_available", item.record.get("show_leader", False)
                )
            )
            item.record["show_leader"] = bool(
                leader_available and self.snippet_show_leader_check.isChecked()
            )
            if denominator > 0 and float(item.record.get("physical_width_mm", 0.0) or 0.0) > 0.0:
                callout_lines = len(str(item.record.get("resolved_callouts", "") or "").splitlines())
                try:
                    payload = base64.b64decode(
                        str(item.record.get("image_png_base64", ""))
                    )
                except Exception:
                    payload = b""
                content_width, content_height = _scaled_snippet_image_size(
                    payload,
                    item.record["physical_width_mm"],
                    item.record["physical_height_mm"],
                    denominator,
                )
                item.record["drawing_content_width_pt"] = content_width
                item.record["drawing_content_height_pt"] = content_height
                new_width = max(180.0, content_width + 10.0)
                new_height = content_height + 24.0 + callout_lines * 14.0
                self.markup_width.setValue(new_width)
                self.markup_height.setValue(new_height)
            elif denominator <= 0:
                item.record["drawing_content_width_pt"] = 0.0
                item.record["drawing_content_height_pt"] = 0.0
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
        if item.record.get("type") == "network_snippet":
            item._rebuild_annotation_leader_handles()
        item._redraw()
        item.sync_record()
        self._save_current_page_callouts()

    def _choose_annotation_colour(self):
        item = self._selected_annotation()
        if item is None or item.is_locked():
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
        if item is None or item.is_locked():
            return
        self._edit_annotation(item)
        self._save_current_page_callouts()

    def _edit_selected_room_layout(self):
        item = self._selected_annotation()
        if item is None or item.is_locked() or item.record.get("snippet_view_type") not in {"room_layout", "room_cutaway"}:
            return
        layout_data = deepcopy(item.record.get("room_layout") or {})
        if not layout_data:
            return
        dialog = _RoomLayoutEditorDialog(self, layout_data)
        if dialog.exec() != QDialog.Accepted:
            return
        updated = deepcopy(dialog.layout_data)
        floor = int(item.record.get("floor", 0) or 0)
        location = str(item.record.get("location_name", ""))
        for candidate in self.annotation_items:
            record = candidate.record
            if (
                record.get("type") != "network_snippet"
                or record.get("snippet_view_type") not in {"room_layout", "room_cutaway"}
                or int(record.get("floor", 0) or 0) != floor
                or str(record.get("location_name", "")) != location
            ):
                continue
            record["room_layout"] = deepcopy(updated)
            view_type = str(record.get("snippet_view_type", "room_layout"))
            payload = render_network_room_layout_png(updated, view_type)
            record["image_png_base64"] = base64.b64encode(payload).decode("ascii")
            snippet = {
                "floor": floor,
                "location_name": location,
                "room_type": str((record.get("model_variables") or {}).get("room_type", "Comms room")),
                "room_layout": updated,
            }
            variables = network_report_snippet_variables(self.network_data, snippet)
            record["model_variables"] = variables
            record["resolved_callouts"] = "\n".join(
                format_network_snippet_template(line, variables)
                for line in record.get("callout_templates", []) or []
                if str(line).strip()
            )
            if view_type == "room_layout":
                record["physical_width_mm"] = float(updated.get("room_width_mm", 0.0))
                record["physical_height_mm"] = float(updated.get("room_depth_mm", 0.0))
                denominator = int(record.get("scale_denominator", 50) or 50)
                if denominator > 0:
                    content_width, content_height = _scaled_snippet_image_size(
                        payload,
                        record["physical_width_mm"],
                        record["physical_height_mm"],
                        denominator,
                    )
                    record["drawing_content_width_pt"] = content_width
                    record["drawing_content_height_pt"] = content_height
                    record["width_pt"] = max(180.0, content_width + 10.0)
                    record["height_pt"] = content_height + 24.0 + len(record["resolved_callouts"].splitlines()) * 14.0
                    candidate.setRect(0.0, 0.0, record["width_pt"] * candidate.scale, record["height_pt"] * candidate.scale)
            candidate._snippet_pixmap = None
            candidate._redraw()
            candidate.sync_record()
        self._save_current_page_callouts()
        self._selection_changed()

    def _export_selected_snippet_png(self):
        item = self._selected_annotation()
        if item is None or item.record.get("type") != "network_snippet":
            return
        title = str(item.record.get("title", "network-snippet") or "network-snippet")
        safe_name = "".join(character if character.isalnum() or character in "-_" else "-" for character in title).strip("-")[:80] or "network-snippet"
        path, _selected_filter = QFileDialog.getSaveFileName(
            self,
            "Export placed snippet as PNG",
            f"{safe_name}.png",
            "PNG image (*.png)",
        )
        if not path:
            return
        if not str(path).lower().endswith(".png"):
            path += ".png"
        try:
            payload = base64.b64decode(str(item.record.get("image_png_base64", "")))
            image = QImage.fromData(payload, "PNG")
            if image.isNull() or not image.save(path, "PNG"):
                raise ValueError("The placed snippet image could not be written.")
        except Exception as exc:
            QMessageBox.critical(self, "PNG export failed", str(exc))
            return
        QMessageBox.information(self, "PNG exported", f"The placed snippet was exported to:\n{path}")

    def _apply_callout_changes(self):
        item = self._selected_callout()
        if item is None or item.is_locked():
            return
        item.record["auto_fit_text"] = False
        self._commit_selected_editor()
        self._save_current_page_callouts()

    def _add_selected_leader_bend(self):
        item = self._selected_callout()
        if item is None or item.is_locked():
            candidate = self._selected_annotation()
            item = candidate if candidate and candidate.record.get("type") == "network_snippet" else None
        if item is None:
            return
        item.add_leader_bend()
        item.sync_record()
        self._save_current_page_callouts()
        self._selection_changed()

    def _remove_selected_leader_bend(self):
        item = self._selected_callout()
        if item is None or item.is_locked():
            candidate = self._selected_annotation()
            item = candidate if candidate and candidate.record.get("type") == "network_snippet" else None
        if item is None:
            return
        item.remove_last_leader_bend()
        item.sync_record()
        self._save_current_page_callouts()
        self._selection_changed()

    def _reset_selected_leader(self):
        item = self._selected_callout()
        if item is None or item.is_locked():
            candidate = self._selected_annotation()
            item = candidate if candidate and candidate.record.get("type") == "network_snippet" else None
        if item is None:
            return
        item.reset_leader_route()
        item.sync_record()
        self._save_current_page_callouts()
        self._selection_changed()

    def _join_selected_callouts(self):
        selected = self._selected_callouts()
        if len(selected) < 2 or any(item.is_locked() for item in selected):
            return
        primary = self._property_item if self._property_item in selected else selected[0]
        ordered = [primary] + [item for item in selected if item is not primary]
        routes = []
        member_keys = []
        text_lines = []
        for item in ordered:
            routes.extend(item._leader_routes())
            key = str(item.record.get("key", ""))
            if key and key != str(primary.record.get("key", "")):
                member_keys.append(key)
            member_keys.extend(
                str(value)
                for value in item.record.get("joined_callout_keys", []) or []
                if str(value) != str(primary.record.get("key", ""))
            )
            for line in str(item.record.get("text", "")).splitlines():
                line = line.strip()
                if line:
                    text_lines.append(line)
        primary.record["leaders_pt"] = routes
        primary.record["joined_callout_keys"] = list(dict.fromkeys(member_keys))
        primary.record["auto_fit_text"] = True
        primary.record.pop("leader_points_pt", None)
        primary.set_text("\n".join(text_lines))
        fitted_width, fitted_height = fitted_generated_callout_size(
            primary.record.get("text", ""), primary.record.get("font_size_pt", 7.0)
        )
        primary.set_size_points(fitted_width, fitted_height)
        primary._rebuild_leader_handles()
        primary._update_leader()
        primary_key = str(primary.record.get("key", ""))
        for item in ordered[1:]:
            item.record["joined_into_key"] = primary_key
            item.set_callout_visible(False)
            item.setVisible(False)
        primary.setSelected(True)
        self._save_current_page_callouts()
        self._selection_changed()

    def _split_selected_callout(self):
        item = self._selected_callout()
        if item is None or item.is_locked():
            return
        joined_keys = [
            str(key) for key in item.record.get("joined_callout_keys", []) or []
        ]
        if not joined_keys:
            return
        self._save_current_page_callouts()
        primary_key = str(item.record.get("key", ""))
        manifest_by_key = {
            str(row.get("key", "")): row
            for row in self.manifest
            if isinstance(row, dict)
        }
        for key in [primary_key] + joined_keys:
            override = self.settings.setdefault("callouts", {}).setdefault(key, {})
            override.pop("joined_callout_keys", None)
            override.pop("joined_into_key", None)
            override.pop("leaders_pt", None)
            override.pop("leader_points_pt", None)
            override.pop("auto_fit_text", None)
            override["visible"] = True
            source = manifest_by_key.get(key)
            if source is not None:
                override["text"] = str(source.get("text", ""))
        self._record_history_state()
        self.refresh_preview(collect_current=False)

    def _hide_selected_callouts(self):
        selected = [item for item in self._selected_callouts() if not item.is_locked()]
        if not selected:
            return
        for item in selected:
            item.set_callout_visible(False)
        self._save_current_page_callouts()
        self._selection_changed()
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
        self._save_current_page_callouts()
        self.settings["callouts"] = {}
        self._record_history_state()
        self.refresh_preview(collect_current=False)

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

    def run_as_window(self):
        """Show as a modeless top-level window while retaining a result for export callers."""
        self.setWindowModality(Qt.NonModal)
        self.setModal(False)
        event_loop = QEventLoop(self)
        self.finished.connect(event_loop.quit)
        self.show()
        self.raise_()
        self.activateWindow()
        event_loop.exec()
        return self.result()

    def release_preview(self):
        """Release Qt's file handle before the caller removes preview files."""
        if self._releasing_preview:
            return
        self._releasing_preview = True
        self.detail_render_timer.stop()
        self._property_item = None
        self.hide()
        # Do not manually clear this QGraphicsScene. Snippet resize and leader
        # handles are C++ child items with Python owner references; destroying
        # that mixed ownership tree piecemeal can corrupt the Windows heap. Drop
        # only the large raster allocations here and let QObject deferred
        # deletion destroy the complete dialog tree in its native ownership order.
        if self.background_item is not None:
            self.background_item.setPixmap(QPixmap())
        for item in self.annotation_items:
            if getattr(item, "record", {}).get("type") != "network_snippet":
                continue
            item.image_item.setPixmap(QPixmap())
            item._snippet_pixmap = QPixmap()
        self._network_snippet_png_cache.clear()
        self.network_data = {}
        self._undo_stack.clear()
        self._redo_stack.clear()
        self._history_current = None
        document = self.document
        if document is not None:
            # close() releases the Windows PDF file handle. Do not force a
            # DeferredDelete and then pump the global event queue here: snippet
            # children may still have live Python wrappers, and that ordering can
            # terminate the process inside Qt without a Python exception.
            document.close()
        self.preview_path = ""
        if self._composed_preview_path:
            try:
                Path(self._composed_preview_path).unlink(missing_ok=True)
            except OSError:
                pass
            self._composed_preview_path = ""
        self.setParent(None)
        self.deleteLater()


ZoneDesignReportStudioDialog = PdfReportStudioDialog
