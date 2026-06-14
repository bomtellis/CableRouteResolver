"""
OpenGL-backed DXF and cable graph renderer for CableRouteResolver.

"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from PySide6.QtCore import QPoint, QPointF, QRect, QRectF, Qt, Signal
from PySide6.QtGui import (
    QColor,
    QFont,
    QPainter,
    QPainterPath,
    QPen,
    QBrush,
    QPolygonF,
    QTransform,
)
from PySide6.QtOpenGLWidgets import QOpenGLWidget
from PySide6.QtWidgets import QRubberBand

Bounds = Tuple[float, float, float, float]
PointTuple = Tuple[float, float]


@dataclass
class DxfRenderCache:
    """CPU-side geometry cache for fast OpenGL-backed QPainter draws."""

    source_key: Optional[Tuple[Optional[str], int]] = None
    line_path: QPainterPath = field(default_factory=QPainterPath)
    poly_path: QPainterPath = field(default_factory=QPainterPath)
    arc_path: QPainterPath = field(default_factory=QPainterPath)
    text_entities: List[Dict[str, Any]] = field(default_factory=list)

    def clear(self) -> None:
        self.source_key = None
        self.line_path = QPainterPath()
        self.poly_path = QPainterPath()
        self.arc_path = QPainterPath()
        self.text_entities = []


class GpuDxfGraphView(QOpenGLWidget):
    """
    GPU-backed viewport for DXF background plus CableRouteResolver graph overlay.

    This is deliberately not a QGraphicsView. It avoids creating thousands of
    QGraphicsItems, which is the main cost in the existing `DXFScene.populate_graphics_scene()`
    path. Qt paints this widget through an OpenGL framebuffer, while this class
    keeps the draw model simple enough to use from your existing app.
    """

    leftClicked = Signal(object, float, float)
    leftDoubleClicked = Signal(object, float, float)
    leftReleased = Signal(object)
    rightClicked = Signal(object, float, float)
    middleClicked = Signal(object)
    middleDragged = Signal(object)
    middleReleased = Signal(object)
    mouseWheelScrolled = Signal(object)
    mouseDragged = Signal(object, float, float)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMouseTracking(True)
        self.setFocusPolicy(Qt.StrongFocus)
        self.setAutoFillBackground(False)

        self.store = None
        self.dxf_scene = None
        self.floor = 0

        self.show_dxf = True
        self.show_labels = True
        self.show_graph = True
        self.show_overlay = True

        # Individual graph layers
        self.show_edges = True
        self.show_nodes = True
        self.show_data_points = True
        self.show_locations = True
        self.show_comms_rooms = True
        self.show_departments = True
        self.show_network = True

        self.selected_point_name: Optional[str] = None
        self.selected_template_names: set[str] = set()
        self.edge_chain_start: Optional[str] = None

        self._scale = 1.0
        self._offset = QPointF(0.0, 0.0)
        self._last_middle_pos: Optional[QPoint] = None
        self._overlay_provider = None
        self._dxf_cache = DxfRenderCache()
        self._rubber_band = QRubberBand(QRubberBand.Rectangle, self)

        self._background = QColor("#111111")
        self._dxf_line_pen = QPen(QColor("#858585"), 0.0)
        self._dxf_poly_pen = QPen(QColor("#bebebe"), 0.0)
        self._dxf_arc_pen = QPen(QColor("#2e2e2e"), 0.0)
        self._same_floor_edge_pen = QPen(QColor("#6aa9ff"), 0.0)
        self._cross_floor_edge_pen = QPen(QColor("#ff4d4f"), 0.0)

    # ------------------------------------------------------------------
    # Public API used by app.py
    # ------------------------------------------------------------------
    def set_store(self, store: Any) -> None:
        self.store = store
        self.update()

    def set_dxf_scene(self, dxf_scene: Any) -> None:
        self.dxf_scene = dxf_scene
        self._dxf_cache.clear()
        self.update()

    def set_floor(self, floor: int) -> None:
        self.floor = int(floor)
        self.update()

    def set_visible_layers(
        self,
        *,
        show_dxf: Optional[bool] = None,
        show_labels: Optional[bool] = None,
        show_graph: Optional[bool] = None,
        show_overlay: Optional[bool] = None,
        show_edges: Optional[bool] = None,
        show_nodes: Optional[bool] = None,
        show_data_points: Optional[bool] = None,
        show_locations: Optional[bool] = None,
        show_comms_rooms: Optional[bool] = None,
        show_departments: Optional[bool] = None,
        show_network: Optional[bool] = None,
    ) -> None:
        if show_dxf is not None:
            self.show_dxf = bool(show_dxf)

        if show_labels is not None:
            self.show_labels = bool(show_labels)

        if show_graph is not None:
            self.show_graph = bool(show_graph)

        if show_overlay is not None:
            self.show_overlay = bool(show_overlay)

        if show_edges is not None:
            self.show_edges = bool(show_edges)

        if show_nodes is not None:
            self.show_nodes = bool(show_nodes)

        if show_data_points is not None:
            self.show_data_points = bool(show_data_points)

        if show_locations is not None:
            self.show_locations = bool(show_locations)

        if show_comms_rooms is not None:
            self.show_comms_rooms = bool(show_comms_rooms)

        if show_departments is not None:
            self.show_departments = bool(show_departments)

        if show_network is not None:
            self.show_network = bool(show_network)

        self.update()

    def set_selection(
        self,
        selected_point_name: Optional[str],
        selected_template_names: Optional[Iterable[str]] = None,
        edge_chain_start: Optional[str] = None,
    ) -> None:
        self.selected_point_name = selected_point_name
        self.selected_template_names = set(selected_template_names or [])
        self.edge_chain_start = edge_chain_start
        self.update()

    def set_overlay_provider(self, overlay_provider) -> None:
        self._overlay_provider = overlay_provider
        self.update()

    def invalidate_dxf_cache(self) -> None:
        self._dxf_cache.clear()
        self.update()

    def transform(self) -> QTransform:
        """Compatibility helper for app code that reads canvas.transform().m11()."""
        return QTransform().scale(self._scale, self._scale)

    def scale(self, sx: float, sy: float) -> None:  # QGraphicsView-like helper
        factor = float(sx)
        centre = QPointF(self.width() / 2.0, self.height() / 2.0)
        before = self.screen_to_world(centre)
        self._scale = max(0.001, min(5000.0, self._scale * factor))
        after_screen = self.world_to_screen(before[0], before[1])
        self._offset += centre - after_screen
        self.update()

    def resetTransform(self) -> None:  # QGraphicsView-like helper
        self._scale = 1.0
        self._offset = QPointF(0.0, 0.0)
        self.update()

    def fitInView(
        self, rect: QRectF, aspect_mode: Qt.AspectRatioMode = Qt.KeepAspectRatio
    ) -> None:
        self.fit_to_rect(rect, aspect_mode)

    def fit_to_content(self, padding: float = 8.0) -> None:
        rect = self.content_scene_rect(padding=padding)
        if rect is not None:
            self.fit_to_rect(rect, Qt.KeepAspectRatio)

    def fit_to_rect(
        self, rect: QRectF, aspect_mode: Qt.AspectRatioMode = Qt.KeepAspectRatio
    ) -> None:
        if rect.isNull() or rect.width() <= 0 or rect.height() <= 0:
            return
        margin = 30.0
        available_w = max(1.0, self.width() - margin * 2.0)
        available_h = max(1.0, self.height() - margin * 2.0)
        sx = available_w / rect.width()
        sy = available_h / rect.height()
        self._scale = min(sx, sy) if aspect_mode == Qt.KeepAspectRatio else max(sx, sy)
        self._scale = max(0.001, min(5000.0, self._scale))
        centre_scene = rect.center()
        centre_screen = QPointF(self.width() / 2.0, self.height() / 2.0)
        self._offset = centre_screen - QPointF(
            centre_scene.x() * self._scale, centre_scene.y() * self._scale
        )
        self.update()

    def content_scene_rect(self, padding: float = 8.0) -> Optional[QRectF]:
        bounds = self._content_bounds()
        if bounds is None:
            return None
        min_x, min_y, max_x, max_y = bounds
        return QRectF(
            min_x - padding,
            -(max_y + padding),
            max(1.0, (max_x - min_x) + padding * 2.0),
            max(1.0, (max_y - min_y) + padding * 2.0),
        )

    def world_to_scene(self, x: float, y: float) -> QPointF:
        return QPointF(float(x), -float(y))

    def scene_to_world(self, sx: float, sy: float) -> Tuple[float, float]:
        return float(sx), -float(sy)

    def world_to_screen(self, x: float, y: float) -> QPointF:
        scene = self.world_to_scene(x, y)
        return QPointF(
            scene.x() * self._scale + self._offset.x(),
            scene.y() * self._scale + self._offset.y(),
        )

    def screen_to_scene(self, pos: QPointF | QPoint) -> QPointF:
        return QPointF(
            (float(pos.x()) - self._offset.x()) / self._scale,
            (float(pos.y()) - self._offset.y()) / self._scale,
        )

    def screen_to_world(self, pos: QPointF | QPoint) -> Tuple[float, float]:
        scene = self.screen_to_scene(pos)
        return self.scene_to_world(scene.x(), scene.y())

    def find_nearest_selectable_name(
        self, x: float, y: float, radius_px: float = 12.0
    ) -> Optional[str]:
        if self.store is None:
            return None
        radius_world = max(0.2, radius_px / max(self._scale, 0.001))
        best_name = None
        best_dist = radius_world

        for name, point in self._points_for_floor().items():
            d = math.hypot(
                float(point.get("x", 0.0)) - x, float(point.get("y", 0.0)) - y
            )
            if d <= best_dist:
                best_dist = d
                best_name = str(name)

        for department_id, dept in self._departments_for_floor().items():
            d = math.hypot(float(dept.get("x", 0.0)) - x, float(dept.get("y", 0.0)) - y)
            if d <= best_dist:
                best_dist = d
                best_name = str(department_id)

        return best_name

    # ------------------------------------------------------------------
    # Qt events
    # ------------------------------------------------------------------
    def paintGL(self) -> None:
        painter = QPainter(self)
        try:
            painter.setRenderHint(QPainter.Antialiasing, False)
            painter.fillRect(self.rect(), self._background)
            painter.translate(self._offset)
            painter.scale(self._scale, self._scale)

            if self.show_dxf:
                self._draw_dxf(painter)
            if self.show_graph:
                self._draw_edges(painter)
                self._draw_departments(painter)
                self._draw_points(painter)

            painter.resetTransform()
            if self.show_overlay and self._overlay_provider is not None:
                self._overlay_provider(painter, self.rect())
        finally:
            painter.end()

    def mousePressEvent(self, event) -> None:
        x, y = self.screen_to_world(event.position())
        if event.button() == Qt.LeftButton:
            self.leftClicked.emit(event, x, y)
        elif event.button() == Qt.RightButton:
            self.rightClicked.emit(event, x, y)
        elif event.button() == Qt.MiddleButton:
            self._last_middle_pos = event.position().toPoint()
            self.middleClicked.emit(event)

    def mouseDoubleClickEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            x, y = self.screen_to_world(event.position())
            self.leftDoubleClicked.emit(event, x, y)

    def mouseReleaseEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            self.leftReleased.emit(event)
        elif event.button() == Qt.MiddleButton:
            self._last_middle_pos = None
            self.middleReleased.emit(event)

    def mouseMoveEvent(self, event) -> None:
        if event.buttons() & Qt.MiddleButton and self._last_middle_pos is not None:
            current = event.position().toPoint()
            delta = current - self._last_middle_pos
            self._offset += QPointF(delta.x(), delta.y())
            self._last_middle_pos = current
            self.middleDragged.emit(event)
            self.update()
            return

        if event.buttons() & Qt.LeftButton:
            x, y = self.screen_to_world(event.position())
            self.mouseDragged.emit(event, x, y)

    def wheelEvent(self, event) -> None:
        old_world = self.screen_to_world(event.position())
        factor = 1.1 if event.angleDelta().y() > 0 else 0.9
        self._scale = max(0.001, min(5000.0, self._scale * factor))
        new_screen = self.world_to_screen(old_world[0], old_world[1])
        self._offset += QPointF(event.position().x(), event.position().y()) - new_screen
        self.mouseWheelScrolled.emit(event)
        self.update()
        event.accept()

    # ------------------------------------------------------------------
    # Cached DXF drawing
    # ------------------------------------------------------------------
    def _draw_dxf(self, painter: QPainter) -> None:
        if self.dxf_scene is None or not getattr(self.dxf_scene, "entities", None):
            return
        self._ensure_dxf_cache()

        if not self._dxf_cache.line_path.isEmpty():
            painter.setPen(self._dxf_line_pen)
            painter.setBrush(Qt.NoBrush)
            painter.drawPath(self._dxf_cache.line_path)
        if not self._dxf_cache.poly_path.isEmpty():
            painter.setPen(self._dxf_poly_pen)
            painter.setBrush(Qt.NoBrush)
            painter.drawPath(self._dxf_cache.poly_path)
        if not self._dxf_cache.arc_path.isEmpty():
            painter.setPen(self._dxf_arc_pen)
            painter.setBrush(Qt.NoBrush)
            painter.drawPath(self._dxf_cache.arc_path)

        if self.show_labels and self._scale >= 6.0:
            self._draw_dxf_text(painter)

    def _ensure_dxf_cache(self) -> None:
        entities = list(getattr(self.dxf_scene, "entities", []) or [])
        source_key = (
            getattr(self.dxf_scene, "path", None),
            id(entities),
            len(entities),
        )
        # id(entities) changes because of list() above, so use path + actual scene list id.
        source_key = (
            getattr(self.dxf_scene, "path", None),
            id(getattr(self.dxf_scene, "entities", None)),
            len(entities),
        )
        if self._dxf_cache.source_key == source_key:
            return

        self._dxf_cache.clear()
        self._dxf_cache.source_key = source_key

        for entity in entities:
            etype = entity.get("type")
            if etype == "LINE":
                x1, y1 = entity["start"]
                x2, y2 = entity["end"]
                self._dxf_cache.line_path.moveTo(float(x1), -float(y1))
                self._dxf_cache.line_path.lineTo(float(x2), -float(y2))
            elif etype == "POLYLINE":
                pts = entity.get("points", [])
                if len(pts) < 2:
                    continue
                self._dxf_cache.poly_path.moveTo(float(pts[0][0]), -float(pts[0][1]))
                for x, y in pts[1:]:
                    self._dxf_cache.poly_path.lineTo(float(x), -float(y))
                if entity.get("closed"):
                    self._dxf_cache.poly_path.closeSubpath()
            elif etype == "CIRCLE":
                cx, cy = entity["center"]
                r = float(entity["radius"])
                self._dxf_cache.arc_path.addEllipse(
                    QRectF(float(cx) - r, -(float(cy) + r), r * 2.0, r * 2.0)
                )
            elif etype == "ARC":
                cx, cy = entity["center"]
                r = float(entity["radius"])
                start_angle = float(entity.get("start_angle", 0.0))
                end_angle = float(entity.get("end_angle", 0.0))
                span_angle = end_angle - start_angle
                if span_angle <= 0:
                    span_angle += 360.0
                rect = QRectF(float(cx) - r, -(float(cy) + r), r * 2.0, r * 2.0)
                self._dxf_cache.arc_path.arcMoveTo(rect, -start_angle)
                self._dxf_cache.arc_path.arcTo(rect, -start_angle, -span_angle)
            elif etype == "TEXT":
                text = str(entity.get("text") or "").strip()
                if not text:
                    continue
                if float(entity.get("height") or 0.0) > 40.0:
                    continue
                self._dxf_cache.text_entities.append(entity)

    def _draw_dxf_text(self, painter: QPainter) -> None:
        """Draw DXF text in world space so it scales naturally with camera zoom."""
        for entity in self._dxf_cache.text_entities:
            text = str(entity.get("text") or "").strip()
            if not text:
                continue

            x, y = entity.get("insert", (0.0, 0.0))
            scene_pos = self.world_to_scene(float(x), float(y))
            screen = self.world_to_screen(float(x), float(y))

            if not self.rect().adjusted(-120, -120, 120, 120).contains(screen.toPoint()):
                continue

            raw_height = float(entity.get("height") or 0.0)
            world_height = raw_height if raw_height > 0.0 else 0.8
            world_height = max(0.45, min(3.0, world_height))

            self._draw_world_label(
                painter,
                scene_pos,
                text,
                QColor("#C0C0C0"),
                world_height=world_height,
                rotation_degrees=-float(entity.get("rotation", 0.0)),
                offset=QPointF(0.0, 0.0),
            )

    # ------------------------------------------------------------------
    # Cable graph drawing
    # ------------------------------------------------------------------


    def _draw_edges(self, painter: QPainter) -> None:
        if self.store is None or not self.show_edges:
            return
        points = self._all_points()
        for edge in self.store.data.get("corridors", {}).get("edges", []):
            a = points.get(edge.get("from"))
            b = points.get(edge.get("to"))
            if not a or not b:
                continue
            a_floor = int(a.get("floor", 0))
            b_floor = int(b.get("floor", 0))
            if self.floor not in {a_floor, b_floor}:
                continue
            painter.setPen(
                self._cross_floor_edge_pen
                if a_floor != b_floor
                else self._same_floor_edge_pen
            )
            pa = self.world_to_scene(float(a.get("x", 0.0)), float(a.get("y", 0.0)))
            pb = self.world_to_scene(float(b.get("x", 0.0)), float(b.get("y", 0.0)))
            painter.drawLine(pa, pb)

    def _draw_departments(self, painter: QPainter) -> None:
        if not self.show_departments:
            return

        for department_id, dept in self._departments_for_floor().items():

            pos = self.world_to_scene(
                float(dept.get("x", 0.0)), float(dept.get("y", 0.0))
            )
            selected = str(department_id) == str(self.selected_point_name)
            poly = QPolygonF(
                [
                    QPointF(pos.x(), pos.y() - 0.7),
                    QPointF(pos.x() + 0.7, pos.y()),
                    QPointF(pos.x(), pos.y() + 0.7),
                    QPointF(pos.x() - 0.7, pos.y()),
                ]
            )
            painter.setBrush(QBrush(QColor("#1abc9c")))
            painter.setPen(
                QPen(QColor("#ffffff") if selected else QColor("#8ef3df"), 0.08)
            )
            painter.drawPolygon(poly)
            if self.show_labels:
                self._draw_screen_label(
                    painter,
                    pos,
                    str(dept.get("name") or department_id),
                    QColor("#aaf7ea"),
                )

    def _draw_points(self, painter: QPainter) -> None:
        for name, point in self._points_for_floor().items():
            pos = self.world_to_scene(
                float(point.get("x", 0.0)), float(point.get("y", 0.0))
            )
            selected = (
                str(name) == str(self.selected_point_name)
                or str(name) in self.selected_template_names
            )

            kind = str(point.get("kind", "")).strip()

            # Apply individual graph-layer visibility.
            if kind == "corridor_node" and not self.show_nodes:
                continue

            if kind == "data_point" and not self.show_data_points:
                continue

            if kind == "comms_room" and not self.show_comms_rooms:
                continue

            if kind in {
                "location",
                "distributed_equipment_room",
                "mer",
                "main_equipment_room",
                "polan",
                "polan_location",
            } and not self.show_locations:
                continue

            outline = QPen(
                QColor("#ffffff") if selected else QColor("transparent"), 0.0
            )

            if kind in {"location", "comms_room"}:
                painter.setPen(outline)
                painter.setBrush(QBrush(QColor("#18c37e")))
                r = 0.3
                painter.drawEllipse(QRectF(pos.x() - r, pos.y() - r, r * 2.0, r * 2.0))
                label_color = QColor("#9bf0cd")
            elif kind == "corridor_node":
                painter.setPen(outline)
                painter.setBrush(QBrush(QColor("#f2c94c")))
                r = 0.3
                painter.drawRect(QRectF(pos.x() - r, pos.y() - r, r * 2.0, r * 2.0))
                label_color = QColor("#ffe8a3")
            elif kind == "data_point":
                self._draw_diamond(
                    painter,
                    pos,
                    0.45,
                    QColor("#b07cff"),
                    QColor("#ffffff") if selected else QColor("#d5bbff"),
                )
                label_color = QColor("#eadcff")
            else:
                self._draw_diamond(
                    painter,
                    pos,
                    0.5,
                    QColor("#ff7b72"),
                    QColor("#ffffff") if selected else QColor("#ffb3ae"),
                )
                label_color = QColor("#ffb3ae")

            if self.show_labels:
                self._draw_screen_label(painter, pos, str(name), label_color)

    @staticmethod
    def _draw_diamond(
        painter: QPainter, pos: QPointF, r: float, fill: QColor, outline: QColor
    ) -> None:
        poly = QPolygonF(
            [
                QPointF(pos.x(), pos.y() - r),
                QPointF(pos.x() + r, pos.y()),
                QPointF(pos.x(), pos.y() + r),
                QPointF(pos.x() - r, pos.y()),
            ]
        )
        painter.setBrush(QBrush(fill))
        painter.setPen(QPen(outline, 0.08))
        painter.drawPolygon(poly)

    def _draw_screen_label(
        self, painter: QPainter, scene_pos: QPointF, text: str, color: QColor
    ) -> None:
        """Compatibility wrapper for graph labels, now drawn in world space."""
        if not text or self._scale < 2.5:
            return

        screen = QPointF(
            scene_pos.x() * self._scale + self._offset.x(),
            scene_pos.y() * self._scale + self._offset.y(),
        )
        if not self.rect().adjusted(-120, -120, 120, 120).contains(screen.toPoint()):
            return

        self._draw_world_label(
            painter,
            scene_pos,
            text,
            color,
            world_height=0.25,
            offset=QPointF(0.42, -0.42),
        )

    @staticmethod
    def _draw_world_label(
        painter: QPainter,
        scene_pos: QPointF,
        text: str,
        color: QColor,
        *,
        world_height: float,
        rotation_degrees: float = 0.0,
        offset: QPointF = QPointF(0.0, 0.0),
    ) -> None:
        """
        Draw text at a fixed model-space size.

        The camera transform remains active, so zooming in makes the text appear
        larger and zooming out makes it appear smaller. The text origin remains
        attached to the same world coordinate and therefore does not drift.
        """
        if not text:
            return

        reference_pixels = 32.0
        local_scale = max(0.001, float(world_height) / reference_pixels)

        painter.save()
        painter.translate(scene_pos + offset)
        painter.rotate(float(rotation_degrees))
        painter.scale(local_scale, local_scale)
        painter.setPen(color)

        font = QFont("Arial")
        font.setPixelSize(int(reference_pixels))
        font.setHintingPreference(QFont.PreferFullHinting)
        painter.setFont(font)
        painter.drawText(QPointF(0.0, 0.0), text)
        painter.restore()

    # ------------------------------------------------------------------
    # Store helpers
    # ------------------------------------------------------------------
    def _all_points(self) -> Dict[str, dict]:
        if self.store is None:
            return {}
        return self.store.all_points()

    def _points_for_floor(self) -> Dict[str, dict]:
        if self.store is None:
            return {}
        return self.store.points_for_floor(self.floor)

    def _departments_for_floor(self) -> Dict[str, dict]:
        if self.store is None:
            return {}
        if hasattr(self.store, "departments_for_floor"):
            return self.store.departments_for_floor(self.floor)
        return {}

    def _content_bounds(self) -> Optional[Bounds]:
        bounds: List[Bounds] = []
        if self.dxf_scene is not None and getattr(self.dxf_scene, "bounds", None):
            bounds.append(tuple(float(x) for x in self.dxf_scene.bounds))

        points = self._points_for_floor()
        if points:
            xs = [float(p.get("x", 0.0)) for p in points.values()]
            ys = [float(p.get("y", 0.0)) for p in points.values()]
            bounds.append((min(xs), min(ys), max(xs), max(ys)))

        departments = self._departments_for_floor()
        if departments:
            xs = [float(p.get("x", 0.0)) for p in departments.values()]
            ys = [float(p.get("y", 0.0)) for p in departments.values()]
            bounds.append((min(xs), min(ys), max(xs), max(ys)))

        if not bounds:
            return None
        return (
            min(b[0] for b in bounds),
            min(b[1] for b in bounds),
            max(b[2] for b in bounds),
            max(b[3] for b in bounds),
        )
