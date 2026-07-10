"""Editable physical-fibre floor topology, separate from the logical network view."""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import time
from typing import Callable, Dict, List, Optional, Sequence

from PySide6.QtCore import QPoint, QPointF, QRectF, Qt, QTimer, Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import (
    QApplication, QComboBox, QDialog, QFrame, QGraphicsItem, QGraphicsObject,
    QGraphicsScene, QGraphicsSimpleTextItem, QGraphicsView, QHBoxLayout, QLabel, QMenu,
    QMessageBox, QPushButton, QScrollArea, QSizePolicy, QVBoxLayout, QWidget,
)

from network_fibre_dialogs import (
    FibreCableEditorDialog,
    FibreCableTypeLibraryDialog,
    FibreNodeEditorDialog,
    FibreSpliceEditorDialog,
    OpticalPropertiesDialog,
    PhysicalFibrePlanningDialog,
    SpliceCassetteViewDialog,
)
from network_schema import NETWORK_SCHEMA_VERSION, ensure_network_schema, next_network_id
from network_services import (
    cable_core_statistics,
    calculate_optical_budgets,
    circuit_trace,
    set_core_status_from_splices,
)


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _int(value, default: int = 0) -> int:
    try: return int(value)
    except (TypeError, ValueError): return default


def _float(value, default: float = 0.0) -> float:
    try: return float(value)
    except (TypeError, ValueError): return default


class FibreMapView(QGraphicsView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setRenderHints(QPainter.Antialiasing | QPainter.TextAntialiasing)
        self.setFrameShape(QFrame.NoFrame)
        self.setBackgroundBrush(QColor("#10161c"))
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorViewCenter)
        self.setViewportUpdateMode(QGraphicsView.MinimalViewportUpdate)
        self.setOptimizationFlags(QGraphicsView.DontSavePainterState | QGraphicsView.DontAdjustForAntialiasing)

    def wheelEvent(self, event):
        delta = event.angleDelta().y()
        if not delta: return super().wheelEvent(event)
        factor = 1.18 if delta > 0 else 1 / 1.18
        scale = abs(self.transform().m11())
        if 0.02 <= scale * factor <= 50:
            self.scale(factor, factor)
        event.accept()

    def fit_content(self):
        if not self.scene(): return
        # ``rebuild`` already calculates a tight scene rectangle. Reusing it
        # avoids a second full scene traversal when a large floor first opens.
        rect = self.scene().sceneRect().adjusted(-8, -8, 8, 8)
        if not rect.isEmpty():
            self.resetTransform(); self.fitInView(rect, Qt.KeepAspectRatio)


class FibreMapLabelItem(QGraphicsSimpleTextItem):
    """Screen-stable label for map annotations.

    The physical-fibre map uses project/DXF world coordinates, so text painted
    directly into the scene can become stretched or widely spaced after view
    transforms. These labels stay in device text coordinates while their anchor
    remains tied to the floor-plan position.
    """
    def __init__(
        self,
        text: str,
        colour: QColor,
        offset: QPointF = QPointF(0.0, 0.0),
        font_px: int = 12,
        bold: bool = False,
        minimum_lod: float = 0.0,
        background: Optional[QColor] = None,
        z_value: float = 1.0,
    ):
        label_text = _text(text)
        super().__init__(label_text)
        self.label_text = label_text
        self.offset = QPointF(offset)
        self.font_px = max(8, int(font_px))
        self.bold = bool(bold)
        self.minimum_lod = max(0.0, float(minimum_lod or 0.0))
        self.background = QColor(background) if background is not None else None
        font = QFont("Arial")
        font.setPixelSize(self.font_px)
        font.setBold(self.bold)
        self.setFont(font)
        self.setBrush(QColor(colour))
        self.setFlag(QGraphicsItem.ItemIgnoresTransformations, True)
        self.setAcceptedMouseButtons(Qt.NoButton)
        self.setZValue(z_value)

    def boundingRect(self):
        return super().boundingRect().translated(self.offset).adjusted(-4.0, -3.0, 4.0, 3.0)

    def _view_lod(self) -> float:
        views = self.scene().views() if self.scene() else []
        return abs(views[0].transform().m11()) if views else 1.0

    def paint(self, painter, option, widget=None):
        if not self.label_text or self._view_lod() < self.minimum_lod:
            return
        painter.setRenderHint(QPainter.TextAntialiasing, True)
        text_rect = super().boundingRect().translated(self.offset)
        if self.background is not None:
            painter.setPen(Qt.NoPen)
            painter.setBrush(self.background)
            painter.drawRoundedRect(text_rect.adjusted(-4.0, -2.0, 4.0, 2.0), 4.0, 4.0)
        painter.save()
        painter.translate(self.offset)
        super().paint(painter, option, widget)
        painter.restore()


class FibreFloorContextItem(QGraphicsObject):
    """Low-contrast building context drawn behind the fibre overlay."""
    def __init__(
        self,
        route_edges: Sequence[tuple[QPointF, QPointF]],
        room_points: Sequence[QPointF],
        departments: Sequence[dict],
    ):
        super().__init__()
        self.room_points = list(room_points)
        self.departments = list(departments)
        self.route_path = QPainterPath()
        self._bounds = QRectF()
        first = True
        for start, end in route_edges:
            self.route_path.moveTo(start)
            self.route_path.lineTo(end)
            edge_bounds = QRectF(start, end).normalized()
            self._bounds = edge_bounds if first else self._bounds.united(edge_bounds)
            first = False
        for point in self.room_points:
            marker = QRectF(point.x() - 0.6, point.y() - 0.6, 1.2, 1.2)
            self._bounds = marker if first else self._bounds.united(marker)
            first = False
        for department in self.departments:
            point = department.get("point", QPointF())
            marker = QRectF(point.x() - 4.5, point.y() - 4.5, 9.0, 9.0)
            self._bounds = marker if first else self._bounds.united(marker)
            first = False
        self.setAcceptedMouseButtons(Qt.NoButton)
        self.setZValue(-4.0)

    def boundingRect(self):
        return self._bounds.adjusted(-6.0, -6.0, 6.0, 6.0) if not self._bounds.isNull() else QRectF()

    def paint(self, painter, option, widget=None):
        painter.setRenderHint(QPainter.Antialiasing, True)
        if self.room_points:
            painter.setPen(Qt.NoPen)
            painter.setBrush(QColor(105, 121, 132, 72))
            lod = max(0.001, abs(painter.worldTransform().m11()))
            radius = 0.75 if lod >= 0.75 else 1.05
            exposed = option.exposedRect.adjusted(-2.0, -2.0, 2.0, 2.0)
            for point in self.room_points:
                if exposed.contains(point):
                    painter.drawEllipse(point, radius, radius)
        if not self.route_path.isEmpty():
            painter.setBrush(Qt.NoBrush)
            painter.setPen(QPen(QColor("#40505b"), 0.32, Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
            painter.drawPath(self.route_path)
        for department in self.departments:
            point = department.get("point", QPointF())
            rect = QRectF(point.x() - 3.2, point.y() - 3.2, 6.4, 6.4)
            painter.setPen(QPen(QColor("#7ae1d0"), 0.38))
            painter.setBrush(QColor(47, 155, 139, 62))
            painter.drawRoundedRect(rect, 1.5, 1.5)


class FibreNodeItem(QGraphicsObject):
    contextRequested = Signal(str, object)
    moved = Signal(str, float, float)
    activated = Signal(str)

    def __init__(self, node: dict, traced: bool = False, symbol_scale: float = 0.075):
        super().__init__()
        self.node = node
        self.traced = traced
        self.symbol_scale = max(0.035, float(symbol_scale or 0.075))
        self._size = max(20.0, min(44.0, 26.0 * self.symbol_scale / 0.075))
        self.setFlags(
            QGraphicsItem.ItemIsSelectable
            | QGraphicsItem.ItemIsMovable
            | QGraphicsItem.ItemSendsGeometryChanges
            | QGraphicsItem.ItemIgnoresTransformations
        )
        self.setCacheMode(QGraphicsItem.NoCache)
        self.setAcceptHoverEvents(True)
        self.setToolTip(self._tooltip())
        self.setZValue(2.0)

    def boundingRect(self):
        return QRectF(-self._size / 2.0, -self._size / 2.0, self._size, self._size)

    def _tooltip(self):
        return f"{_text(self.node.get('name'))}\n{_text(self.node.get('node_type')).replace('_',' ').title()}\n{_text(self.node.get('location_name'))} · Floor {_int(self.node.get('floor'))}"

    def paint(self, painter, option, widget=None):
        painter.setRenderHint(QPainter.Antialiasing, True)
        node_type = _text(self.node.get("node_type"))
        colour = {
            "splice_enclosure": QColor("#9167c6"), "splice_cassette": QColor("#b77bdc"),
            "fibre_joint": QColor("#e19b52"), "termination": QColor("#5f9ee3"),
            "handhole": QColor("#77838d"), "chamber": QColor("#53616d"),
        }.get(node_type, QColor("#77838d"))
        border = QColor("#ff8a24") if self.traced else (QColor("#8fc7ff") if self.isSelected() else QColor("#d5dce1"))
        pen_width = 2.8 if self.traced or self.isSelected() else 1.7
        painter.setPen(QPen(border, pen_width))
        painter.setBrush(colour)
        inset = 2.8
        rect = self.boundingRect().adjusted(inset, inset, -inset, -inset)
        if node_type in {"splice_enclosure", "fibre_joint"}: painter.drawEllipse(rect)
        elif node_type == "splice_cassette":
            painter.drawRoundedRect(rect.adjusted(0, 3, 0, -3), 4, 4)
            painter.drawLine(QPointF(rect.left()+4, -3), QPointF(rect.right()-4, -3)); painter.drawLine(QPointF(rect.left()+4, 3), QPointF(rect.right()-4, 3))
        elif node_type in {"handhole", "chamber"}: painter.drawRect(rect)
        else: painter.drawRoundedRect(rect, 5, 5)
        painter.setPen(QColor("#ffffff"))
        font = QFont("Arial")
        font.setPixelSize(max(9, int(round(self._size * 0.30))))
        font.setBold(True)
        painter.setFont(font)
        initials = {"splice_enclosure":"SE","splice_cassette":"SC","fibre_joint":"J","termination":"T","handhole":"HH","chamber":"CH"}.get(node_type,"F")
        painter.drawText(self.boundingRect(), Qt.AlignCenter, initials)

    def mouseDoubleClickEvent(self, event): self.activated.emit(_text(self.node.get("id"))); event.accept()
    def contextMenuEvent(self, event): self.contextRequested.emit(_text(self.node.get("id")), event.screenPos()); event.accept()
    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        pos = self.pos(); self.moved.emit(_text(self.node.get("id")), pos.x(), -pos.y())


class FibreCableBatchItem(QGraphicsObject):
    """One graphics item for a batch of static fibre paths on a floor."""
    def __init__(self, records: Sequence[dict], context_callback: Callable[[str, QPoint], None], width_scale: float = 0.18):
        super().__init__()
        self.records = list(records)
        self.context_callback = context_callback
        self.width_scale = max(0.05, float(width_scale or 0.18))
        self.normal_path = QPainterPath(); self.traced_path = QPainterPath(); self._bounds = QRectF()
        first = True
        for record in self.records:
            points = record.get("points", [])
            if len(points) < 2:
                continue
            target_path = self.traced_path if record.get("traced") else self.normal_path
            target_path.moveTo(points[0])
            for point in points[1:]:
                target_path.lineTo(point)
            bounds = record.get("bounds", QRectF())
            self._bounds = bounds if first else self._bounds.united(bounds); first = False
        self.setZValue(-0.2)
        # A device-coordinate pixmap cache is expensive for floor-sized paths
        # and is rebuilt whenever the view scale changes. Batched vector paths
        # are faster and use substantially less memory without that cache.
        self.setCacheMode(QGraphicsItem.NoCache)
        self.setAcceptHoverEvents(False)

    def boundingRect(self):
        return self._bounds.adjusted(-4, -4, 4, 4) if not self._bounds.isNull() else QRectF(-1, -1, 2, 2)

    def paint(self, painter, option, widget=None):
        # Cable widths are scene dimensions rather than cosmetic screen-pixel
        # widths so routed plant remains proportional to the floor geometry.
        normal_pen = QPen(
            QColor("#6f8dff"),
            max(0.22, 0.48 * self.width_scale / 0.18),
            Qt.SolidLine,
            Qt.RoundCap,
            Qt.RoundJoin,
        )
        traced_pen = QPen(
            QColor("#ff8a24"),
            max(0.42, 0.90 * self.width_scale / 0.18),
            Qt.SolidLine,
            Qt.RoundCap,
            Qt.RoundJoin,
        )
        painter.setBrush(Qt.NoBrush)
        painter.setPen(normal_pen)
        painter.drawPath(self.normal_path)
        painter.setPen(traced_pen)
        painter.drawPath(self.traced_path)

    @staticmethod
    def _distance_to_segment(point: QPointF, a: QPointF, b: QPointF) -> float:
        dx = b.x() - a.x(); dy = b.y() - a.y()
        if abs(dx) + abs(dy) < 1e-12:
            return ((point.x()-a.x())**2 + (point.y()-a.y())**2) ** 0.5
        t = max(0.0, min(1.0, ((point.x()-a.x())*dx + (point.y()-a.y())*dy) / (dx*dx + dy*dy)))
        px = a.x() + t*dx; py = a.y() + t*dy
        return ((point.x()-px)**2 + (point.y()-py)**2) ** 0.5

    def contextMenuEvent(self, event):
        views = self.scene().views() if self.scene() else []
        scale = abs(views[0].transform().m11()) if views else 1.0
        tolerance = 8.0 / max(0.001, scale)
        point = event.pos(); best = None
        for record in self.records:
            if not record.get("bounds", QRectF()).adjusted(-tolerance, -tolerance, tolerance, tolerance).contains(point):
                continue
            points = record.get("points", [])
            distance = min((self._distance_to_segment(point, points[index-1], points[index]) for index in range(1, len(points))), default=float("inf"))
            if distance <= tolerance and (best is None or distance < best[0]):
                best = (distance, _text(record.get("cable_id")))
        if best is not None:
            self.context_callback(best[1], event.screenPos()); event.accept(); return
        super().contextMenuEvent(event)


class PhysicalFibreTopologyDialog(QDialog):
    """Editable floor map for physical fibre, splices and terminations."""
    def __init__(self, parent, data: dict, on_change: Optional[Callable[[dict], None]] = None, initial_trace_connection_id: str = ""):
        super().__init__(parent)
        self.data = data
        self.on_change = on_change
        # Project loading already applies the network schema. Avoid another
        # whole-project normalisation pass when opening this view; on large
        # projects that scan was one of the dominant startup costs.
        settings = self.data.get("network_settings")
        if (
            _int(self.data.get("network_schema_version"), 0) < NETWORK_SCHEMA_VERSION
            or not isinstance(settings, dict)
            or not isinstance(settings.get("physical_fibre_layer"), dict)
        ):
            ensure_network_schema(self.data)
        self.trace = circuit_trace(self.data, initial_trace_connection_id) if initial_trace_connection_id else {}
        self._route_points_cache: Optional[Dict[str, dict]] = None
        self._cable_geometry_cache: Dict[int, List[dict]] = {}
        self._base_graph_cache: Dict[int, dict] = {}
        self._instances_cache: Optional[Dict[str, dict]] = None
        self._building = False
        self._initial_build_pending = True
        self.setWindowTitle("Physical Fibre Topology")
        self.setWindowFlag(Qt.Window, True)
        screen = (
            parent.screen()
            if parent is not None and hasattr(parent, "screen")
            else QApplication.primaryScreen()
        )
        available = screen.availableGeometry() if screen is not None else None
        target_width = (
            min(1500, max(620, int(available.width() * 0.96)), available.width())
            if available is not None
            else 1500
        )
        target_height = (
            min(900, max(440, int(available.height() * 0.92)), available.height())
            if available is not None
            else 900
        )
        self.setMinimumSize(min(620, target_width), min(440, target_height))
        self.resize(target_width, target_height)
        self.setStyleSheet(
            "QDialog{background:#f8f9fa;color:#212529}"
            "QPushButton,QComboBox{background:#ffffff;color:#212529;border:1px solid #ced4da;padding:6px 10px;border-radius:6px;font-weight:600}"
            "QPushButton:hover{background:#f1f5ff;border-color:#9ec5fe;color:#084298}"
            "QLabel{color:#212529}"
        )
        layout = QVBoxLayout(self)

        toolbar_widget = QWidget()
        toolbar_widget.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Preferred)
        toolbar = QHBoxLayout(toolbar_widget)
        toolbar.setContentsMargins(4, 2, 4, 2)
        toolbar_scroll = QScrollArea()
        toolbar_scroll.setObjectName("PhysicalFibreToolbarScrollArea")
        toolbar_scroll.setFrameShape(QFrame.NoFrame)
        toolbar_scroll.setWidgetResizable(True)
        toolbar_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        toolbar_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        toolbar_scroll.setWidget(toolbar_widget)
        layout.addWidget(toolbar_scroll)

        title = QLabel("Physical fibre overlay"); title.setFont(QFont("Arial", 13, QFont.Bold)); toolbar.addWidget(title)
        self.floor_combo = QComboBox(); toolbar.addWidget(self.floor_combo)
        for label, handler in (
            ("Add enclosure / cassette", self.add_node),
            ("Add fibre cable", self.add_cable),
            ("Add splice", self.add_splice),
            ("Cable library", self.edit_cable_types),
            ("Fibre planning", self.edit_fibre_planning),
            ("Optical properties", self.edit_optical_properties),
            ("Cassette view", self.view_cassettes),
            ("Clear trace", self.clear_trace),
            ("Fit", self._fit),
            ("Refresh", self.refresh),
        ):
            button = QPushButton(label); button.clicked.connect(handler); toolbar.addWidget(button)
        toolbar.addStretch(1)
        self.layer_label = QLabel(); toolbar.addWidget(self.layer_label)
        toolbar_widget.setMinimumWidth(toolbar_widget.sizeHint().width())
        toolbar_scroll.setMinimumHeight(max(58, toolbar_widget.sizeHint().height() + 18))
        toolbar_scroll.setMaximumHeight(max(76, toolbar_widget.sizeHint().height() + 24))
        self.scene = QGraphicsScene(self); self.scene.setItemIndexMethod(QGraphicsScene.NoIndex); self.view = FibreMapView(self); self.view.setScene(self.scene); layout.addWidget(self.view, 1)
        self.status = QLabel("Loading physical fibre…"); self.status.setStyleSheet("padding:7px;background:#ffffff;color:#6c757d;border:1px solid #dee2e6;border-radius:6px"); layout.addWidget(self.status)
        self.floor_combo.currentIndexChanged.connect(self.rebuild)
        self._populate_floors()
        # Let the dialog become visible before the first potentially large
        # geometry build. This removes the apparent application freeze while
        # the window is opening.
        QTimer.singleShot(0, self._initial_rebuild)

    def _populate_floors(self):
        current = self.floor_combo.currentData(); self.floor_combo.blockSignals(True); self.floor_combo.clear()
        floors = set()
        for collection in (self.data.get("locations", []), self.data.get("corridors", {}).get("nodes", []), self.data.get("network_fibre_nodes", [])):
            floors.update(_int(row.get("floor")) for row in collection if isinstance(row, dict))
        for floor in sorted(floors or {0}): self.floor_combo.addItem(f"Floor {floor}", floor)
        idx=self.floor_combo.findData(current); self.floor_combo.setCurrentIndex(idx if idx>=0 else 0); self.floor_combo.blockSignals(False)

    def _floor(self): return _int(self.floor_combo.currentData())

    @staticmethod
    def _compact_scale(value, legacy_defaults, new_default: float, minimum: float, maximum: float) -> float:
        resolved = _float(value, new_default)
        for legacy in legacy_defaults:
            if abs(resolved - float(legacy)) <= 1e-9:
                resolved = new_default
                break
        return max(minimum, min(maximum, resolved))

    def _initial_rebuild(self):
        if not self._initial_build_pending:
            return
        self._initial_build_pending = False
        self.rebuild()
        QTimer.singleShot(0, self._fit)

    def _invalidate_geometry(self, changed: Optional[Sequence[str]] = None):
        keys = set(changed or ())
        if not keys or keys.intersection({
            "corridors", "locations", "data_points", "transitions", "departments",
            "network_asset_instances", "network_fibre_cables",
        }):
            self._route_points_cache = None
            self._instances_cache = None
            self._cable_geometry_cache.clear()
            self._base_graph_cache.clear()

    def _instances(self) -> Dict[str, dict]:
        if self._instances_cache is None:
            self._instances_cache = {
                _text(row.get("id")): row
                for row in self.data.get("network_asset_instances", [])
                if isinstance(row, dict) and _text(row.get("id"))
            }
        return self._instances_cache

    def _layer_settings(self): return self.data.get("network_settings",{}).get("physical_fibre_layer",{})
    def _commit(
        self,
        label="Update physical fibre",
        changed: Optional[Sequence[str]] = None,
        rebuild: bool = True,
        notify: bool = True,
    ):
        self._invalidate_geometry(changed)
        if self.on_change and notify:
            keys = tuple(changed or ("network_fibre_nodes",))
            payload = {key: deepcopy(self.data.get(key)) for key in keys if key in self.data}
            self.on_change(payload)
        if rebuild:
            self.rebuild()

    def _all_route_points(self):
        if self._route_points_cache is not None:
            return self._route_points_cache
        points: Dict[str, dict] = {}
        # Corridor nodes and locations are needed for the base graph and make
        # up nearly all physical-fibre route paths.
        for collection in (self.data.get("corridors", {}).get("nodes", []), self.data.get("locations", [])):
            for row in collection:
                if isinstance(row, dict) and _text(row.get("name")):
                    points[_text(row.get("name"))] = row
        for transition in self.data.get("transitions", []):
            if not isinstance(transition, dict): continue
            transition_id = _text(transition.get("id"))
            for floor_text, coordinates in (transition.get("floor_locations") or {}).items():
                if not isinstance(coordinates, dict): continue
                row = dict(coordinates); row["floor"] = _int(floor_text); row["name"] = f"{transition_id}-F{floor_text}"; points[row["name"]] = row

        missing_names = {
            _text(name)
            for cable in self.data.get("network_fibre_cables", [])
            if isinstance(cable, dict)
            for name in cable.get("route_path", [])
            if _text(name) and _text(name) not in points
        }
        # Data points can dominate a large project. Do not scan or duplicate
        # them unless a physical-fibre route actually references one that was
        # not already resolved as a corridor, location or transition node.
        if missing_names:
            for row in self.data.get("data_points", []):
                name = _text(row.get("name")) if isinstance(row, dict) else ""
                if name in missing_names:
                    points[name] = row
                    missing_names.discard(name)
                    if not missing_names:
                        break
        self._route_points_cache = points
        return points

    @staticmethod
    def _polyline_midpoint(points: Sequence[QPointF]) -> QPointF:
        if not points:
            return QPointF()
        if len(points) == 1:
            return QPointF(points[0])
        lengths = []
        total = 0.0
        for index in range(1, len(points)):
            dx = points[index].x() - points[index - 1].x()
            dy = points[index].y() - points[index - 1].y()
            length = (dx * dx + dy * dy) ** 0.5
            lengths.append(length)
            total += length
        if total <= 1e-12:
            return QPointF(points[len(points) // 2])
        target = total / 2.0
        travelled = 0.0
        for index, length in enumerate(lengths, start=1):
            if travelled + length >= target and length > 1e-12:
                ratio = (target - travelled) / length
                a = points[index - 1]; b = points[index]
                return QPointF(
                    a.x() + (b.x() - a.x()) * ratio,
                    a.y() + (b.y() - a.y()) * ratio,
                )
            travelled += length
        return QPointF(points[-1])

    @staticmethod
    def _polyline_bounds(points: Sequence[QPointF]) -> QRectF:
        if not points:
            return QRectF()
        min_x = min(point.x() for point in points)
        max_x = max(point.x() for point in points)
        min_y = min(point.y() for point in points)
        max_y = max(point.y() for point in points)
        return QRectF(min_x, min_y, max(0.01, max_x - min_x), max(0.01, max_y - min_y))

    def _cable_geometry_for_floor(self, floor: int, route_points: Dict[str, dict]) -> List[dict]:
        cached = self._cable_geometry_cache.get(floor)
        if cached is not None:
            return cached
        instances = self._instances()
        records: List[dict] = []
        for cable in self.data.get("network_fibre_cables", []):
            if not isinstance(cable, dict):
                continue
            points: List[QPointF] = []
            for name in cable.get("route_path", []):
                point = route_points.get(_text(name))
                if point is not None and _int(point.get("floor")) == floor:
                    points.append(QPointF(_float(point.get("x")), -_float(point.get("y"))))
            if not points:
                for key in ("from_instance_id", "to_instance_id"):
                    instance = instances.get(_text(cable.get(key)), {})
                    if instance and _int(instance.get("floor")) == floor:
                        points.append(QPointF(_float(instance.get("x")), -_float(instance.get("y"))))
            if len(points) < 2:
                continue
            records.append({
                "cable": cable,
                "cable_id": _text(cable.get("id")),
                "points": points,
                "midpoint": self._polyline_midpoint(points),
                "bounds": self._polyline_bounds(points),
            })
        self._cable_geometry_cache[floor] = records
        return records

    def rebuild(self, *_args):
        if self._building or self._initial_build_pending:
            return
        self._building = True
        started = time.perf_counter()
        self.view.setUpdatesEnabled(False)
        try:
            self.scene.clear()
            floor = self._floor(); layer = self._layer_settings()
            self.layer_label.setText(f"DXF layers: {layer.get('cable_layer','NET-FIBRE-CABLE')} / {layer.get('splice_layer','NET-FIBRE-SPLICE')}")
            symbol_scale = self._compact_scale(layer.get("symbol_scale"), (0.32, 0.18, 0.12), 0.075, 0.035, 0.24)
            width_scale = self._compact_scale(layer.get("cable_width_scale"), (0.55, 0.30, 0.22), 0.18, 0.05, 0.32)
            route_points = self._all_route_points()
            context_rect = QRectF()
            if layer.get("show_base_graph", True):
                context_rect = self._draw_base_graph(floor, route_points)
            traced_cables = set(self.trace.get("fibre_cable_ids", [])); traced_splices = set(self.trace.get("splice_ids", []))

            floor_nodes = [
                node for node in self.data.get("network_fibre_nodes", [])
                if isinstance(node, dict) and _int(node.get("floor")) == floor
            ]
            floor_node_ids = {_text(node.get("id")) for node in floor_nodes if _text(node.get("id"))}
            traced_nodes = set(); splice_counts: Dict[str, int] = defaultdict(int); floor_splices = 0
            for splice in self.data.get("network_fibre_splices", []):
                if not isinstance(splice, dict):
                    continue
                node_id = _text(splice.get("node_id"))
                if _text(splice.get("id")) in traced_splices:
                    traced_nodes.add(node_id)
                if node_id in floor_node_ids:
                    splice_counts[node_id] += 1
                    floor_splices += 1

            cable_records = []; labels = []
            geometry = self._cable_geometry_for_floor(floor, route_points)
            for record in geometry:
                cable = record["cable"]
                cable_id = record["cable_id"]
                traced = cable_id in traced_cables
                compact_label = f"{cable_id} · {_int(cable.get('core_count'))}F"
                label_text = compact_label
                minimum_lod = 1.25
                if traced:
                    role = _text(cable.get("routing_role")) or "direct"
                    label_text += f" · {role.replace('_', ' ')}"
                    if layer.get("show_dark_fibre", True):
                        stats = cable_core_statistics(cable)
                        label_text += f" · {stats['dark']} dark"
                    if _float(cable.get("estimated_total_loss_db")) > 0:
                        label_text += f" · {_float(cable.get('estimated_total_loss_db')):.2f} dB"
                    minimum_lod = 0.78
                cable_records.append({
                    "cable_id": cable_id,
                    "points": record["points"],
                    "bounds": record["bounds"],
                    "traced": traced,
                })
                labels.append({
                    "point": record["midpoint"],
                    "text": label_text,
                    "colour": QColor("#ffb25f") if traced else QColor("#b8c8ff"),
                    "minimum_lod": minimum_lod,
                    "offset": QPointF(9.0, -7.0),
                    "font_px": 12,
                    "bold": traced,
                    "background": QColor(13, 20, 27, 190),
                })

            for node in floor_nodes:
                item = FibreNodeItem(node, _text(node.get("id")) in traced_nodes, symbol_scale)
                item.setPos(_float(node.get("x")), -_float(node.get("y")))
                item.contextRequested.connect(self._node_context); item.moved.connect(self._node_moved); item.activated.connect(self.edit_node)
                self.scene.addItem(item)
                labels.append({
                    "point": item.pos(),
                    "text": _text(node.get("label")) or _text(node.get("name")),
                    "colour": QColor("#dce5ea"),
                    "minimum_lod": 0.90,
                    "offset": QPointF(item._size * 0.56, -item._size * 0.18),
                    "font_px": 12,
                    "bold": True,
                    "background": QColor(13, 20, 27, 180),
                })
                count = splice_counts.get(_text(node.get("id")), 0)
                if layer.get("show_splice_labels", True) and count:
                    labels.append({
                        "point": item.pos(),
                        "text": f"{count} splice{'s' if count != 1 else ''}",
                        "colour": QColor("#d5a5f1"),
                        "minimum_lod": 1.35,
                        "offset": QPointF(item._size * 0.56, item._size * 0.32),
                        "font_px": 11,
                        "background": QColor(13, 20, 27, 170),
                    })

            if cable_records or labels:
                all_points = [record["midpoint"] for record in geometry]
                all_points.extend(label.get("point", QPointF()) for label in labels)
                min_x = min((point.x() for point in all_points), default=0.0)
                max_x = max((point.x() for point in all_points), default=1.0)
                min_y = min((point.y() for point in all_points), default=0.0)
                max_y = max((point.y() for point in all_points), default=1.0)
                tile_count = 8
                tile_w = max(1.0, (max_x - min_x) / tile_count)
                tile_h = max(1.0, (max_y - min_y) / tile_count)

                def tile_key(point: QPointF) -> tuple[int, int]:
                    return (
                        max(0, min(tile_count - 1, int((point.x() - min_x) / tile_w))),
                        max(0, min(tile_count - 1, int((point.y() - min_y) / tile_h))),
                    )

                tiled_records: Dict[tuple[int, int], List[dict]] = defaultdict(list)
                geometry_midpoints = {record["cable_id"]: record["midpoint"] for record in geometry}
                for record in cable_records:
                    tiled_records[tile_key(geometry_midpoints.get(record["cable_id"], QPointF()))].append(record)
                for key in sorted(tiled_records):
                    self.scene.addItem(FibreCableBatchItem(
                        tiled_records.get(key, []),
                        self._cable_context,
                        width_scale,
                    ))
                for label in labels:
                    label_item = FibreMapLabelItem(
                        _text(label.get("text")),
                        label.get("colour", QColor("#dce5ea")),
                        label.get("offset", QPointF(9.0, -7.0)),
                        _int(label.get("font_px"), 12),
                        bool(label.get("bold", False)),
                        float(label.get("minimum_lod", 0.0)),
                        label.get("background"),
                        3.0,
                    )
                    label_item.setPos(label.get("point", QPointF()))
                    self.scene.addItem(label_item)

            content_rect = QRectF()
            have_bounds = False
            if not context_rect.isEmpty():
                content_rect = context_rect; have_bounds = True
            for record in geometry:
                bounds = record.get("bounds", QRectF())
                if bounds.isEmpty():
                    continue
                content_rect = bounds if not have_bounds else content_rect.united(bounds)
                have_bounds = True
            node_pad = 8.0
            for node in floor_nodes:
                point_rect = QRectF(
                    _float(node.get("x")) - node_pad,
                    -_float(node.get("y")) - node_pad,
                    node_pad * 2.0,
                    node_pad * 2.0,
                )
                content_rect = point_rect if not have_bounds else content_rect.united(point_rect)
                have_bounds = True
            margin = 10.0
            rect = content_rect.adjusted(-margin,-margin,margin,margin) if have_bounds else QRectF(-100,-100,200,200)
            self.scene.setSceneRect(rect)
            failed_paths = sum(1 for row in self.data.get("network_optical_paths", []) if _text(row.get("status")).lower() not in {"pass", "ok"})
            elapsed = time.perf_counter() - started
            self.status.setText(
                f"Floor {floor}: {len(cable_records)} routed fibre cables, {len(floor_nodes)} fibre nodes, {floor_splices} core splices · "
                f"{failed_paths} optical path{'s' if failed_paths != 1 else ''} outside budget · "
                f"loaded in {elapsed:.2f} s · crisp symbols, labels and floor context"
            )
        finally:
            self._building = False
            self.view.setUpdatesEnabled(True)
            self.view.viewport().update()

    def _draw_base_graph(self, floor, points) -> QRectF:
        context = self._base_graph_cache.get(floor)
        if context is None:
            edges = []
            room_points = []
            departments = []
            for edge in self.data.get("corridors", {}).get("edges", []):
                if not isinstance(edge, dict): continue
                a = points.get(_text(edge.get("from"))); b = points.get(_text(edge.get("to")))
                if not a or not b or _int(a.get("floor")) != floor or _int(b.get("floor")) != floor: continue
                edges.append((QPointF(_float(a.get("x")), -_float(a.get("y"))), QPointF(_float(b.get("x")), -_float(b.get("y")))))
            for row in self.data.get("locations", []):
                if isinstance(row, dict) and _int(row.get("floor")) == floor:
                    room_points.append(QPointF(_float(row.get("x")), -_float(row.get("y"))))
            for row in self.data.get("departments", []):
                if not isinstance(row, dict) or _int(row.get("floor")) != floor:
                    continue
                departments.append({
                    "id": _text(row.get("id")),
                    "name": _text(row.get("name")) or _text(row.get("id")),
                    "point": QPointF(_float(row.get("x")), -_float(row.get("y"))),
                })
            context = {"edges": edges, "room_points": room_points, "departments": departments}
            self._base_graph_cache[floor] = context
        item = FibreFloorContextItem(context.get("edges", []), context.get("room_points", []), context.get("departments", []))
        if not item.boundingRect().isEmpty():
            self.scene.addItem(item)
        for department in context.get("departments", []):
            label_item = FibreMapLabelItem(
                department.get("name", ""),
                QColor("#9af2e4"),
                QPointF(8.0, -8.0),
                12,
                True,
                0.36,
                QColor(10, 25, 29, 160),
                -3.0,
            )
            label_item.setPos(department.get("point", QPointF()))
            self.scene.addItem(label_item)
        return item.boundingRect()

    def _fit(self): self.view.fit_content()
    def refresh(self):
        if _int(self.data.get("network_schema_version"), 0) < NETWORK_SCHEMA_VERSION:
            ensure_network_schema(self.data)
        self._invalidate_geometry()
        self._populate_floors(); self.rebuild()
    def clear_trace(self): self.trace={}; self.rebuild()
    def trace_cable(self,cable_id):
        cable=next((c for c in self.data.get("network_fibre_cables",[]) if _text(c.get("id"))==_text(cable_id)),None)
        if cable:
            logical=next((_text(v) for v in cable.get("logical_connection_ids",[]) if _text(v)),"")
            self.trace=circuit_trace(self.data,logical) if logical else {"fibre_cable_ids":[cable_id],"splice_ids":list(cable.get("splice_ids",[]))}
            self.rebuild()

    def add_node(self):
        dialog=FibreNodeEditorDialog(self,nodes=self.data.get("network_fibre_nodes",[]),instances=self.data.get("network_asset_instances",[]),locations=self.data.get("locations",[]),suggested_id=next_network_id(self.data.get("network_fibre_nodes",[]),"FN"),default_floor=self._floor())
        if dialog.exec()==QDialog.Accepted and dialog.result: self.data.setdefault("network_fibre_nodes",[]).append(dialog.result); self._commit(changed=("network_fibre_nodes",))
    def edit_node(self,node_id):
        nodes=self.data.get("network_fibre_nodes",[]); index=next((i for i,n in enumerate(nodes) if _text(n.get("id"))==_text(node_id)),-1)
        if index<0:return
        dialog=FibreNodeEditorDialog(self,nodes[index],nodes,self.data.get("network_asset_instances",[]),self.data.get("locations",[]),node_id)
        if dialog.exec()==QDialog.Accepted and dialog.result: nodes[index]=dialog.result; self._commit(changed=("network_fibre_nodes",))
    def delete_node(self,node_id):
        if QMessageBox.question(self,"Delete fibre node",f"Delete fibre node {node_id} and its splice records?")!=QMessageBox.Yes:return
        self.data["network_fibre_nodes"]=[n for n in self.data.get("network_fibre_nodes",[]) if _text(n.get("id"))!=node_id and _text(n.get("parent_node_id"))!=node_id]
        self.data["network_fibre_splices"]=[s for s in self.data.get("network_fibre_splices",[]) if _text(s.get("node_id"))!=node_id and _text(s.get("cassette_id"))!=node_id]; self._commit(changed=("network_fibre_nodes", "network_fibre_splices"))
    def _node_moved(self,node_id,x,y):
        node=next((n for n in self.data.get("network_fibre_nodes",[]) if _text(n.get("id"))==node_id),None)
        if node:
            node["x"]=round(x,3); node["y"]=round(y,3); node["floor"]=self._floor()
            # The dialog edits the live project object. Avoid an expensive
            # whole-project callback and canvas refresh for a coordinate-only
            # move; the next explicit save already sees the updated values.
            self._commit(changed=("network_fibre_nodes",), rebuild=False, notify=False)

    def add_cable(self):
        dialog=FibreCableEditorDialog(
            self,
            instances=self.data.get("network_asset_instances",[]),
            suggested_id=next_network_id(self.data.get("network_fibre_cables",[]),"FOC"),
            cable_types=self.data.get("network_fibre_cable_types",[]),
        )
        if dialog.exec()==QDialog.Accepted and dialog.result:
            self.data.setdefault("network_fibre_cables",[]).append(dialog.result)
            calculate_optical_budgets(self.data)
            self._commit(changed=("network_fibre_cables", "network_optical_paths", "network_connections"))

    def edit_cable(self,cable_id):
        cables=self.data.get("network_fibre_cables",[])
        index=next((i for i,c in enumerate(cables) if _text(c.get("id"))==cable_id),-1)
        if index<0:return
        dialog=FibreCableEditorDialog(
            self,
            cables[index],
            self.data.get("network_asset_instances",[]),
            cable_id,
            self.data.get("network_fibre_cable_types",[]),
        )
        if dialog.exec()==QDialog.Accepted and dialog.result:
            cables[index]=dialog.result
            calculate_optical_budgets(self.data)
            self._commit(changed=("network_fibre_cables", "network_optical_paths", "network_connections"))

    def edit_cable_types(self):
        dialog=FibreCableTypeLibraryDialog(self,self.data.get("network_fibre_cable_types",[]))
        if dialog.exec()==QDialog.Accepted and dialog.result is not None:
            self.data["network_fibre_cable_types"]=dialog.result
            ensure_network_schema(self.data)
            calculate_optical_budgets(self.data)
            self._commit(changed=("network_fibre_cable_types", "network_fibre_cables", "network_optical_paths", "network_connections"))

    def edit_fibre_planning(self):
        settings=self.data.setdefault("network_settings",{}).get("physical_fibre_planning",{})
        dialog=PhysicalFibrePlanningDialog(self,settings,self.data.get("network_fibre_cable_types",[]))
        if dialog.exec()==QDialog.Accepted and dialog.result is not None:
            self.data.setdefault("network_settings",{})["physical_fibre_planning"]=dialog.result
            ensure_network_schema(self.data)
            self._commit(changed=("network_settings",))

    def edit_optical_properties(self):
        dialog=OpticalPropertiesDialog(self,self.data.get("network_assets",[]))
        if dialog.exec()==QDialog.Accepted and dialog.result is not None:
            self.data["network_assets"]=dialog.result
            ensure_network_schema(self.data)
            calculate_optical_budgets(self.data)
            self._commit(changed=("network_assets", "network_optical_paths", "network_connections"))

    def view_cassettes(self,enclosure_id=""):
        if not enclosure_id:
            enclosure_id=next(
                (
                    _text(node.get("id"))
                    for node in self.data.get("network_fibre_nodes",[])
                    if isinstance(node,dict)
                    and _int(node.get("floor"))==self._floor()
                    and _text(node.get("node_type")) in {"splice_enclosure","fibre_joint","termination"}
                ),
                "",
            )
        dialog=SpliceCassetteViewDialog(
            self,
            self.data.get("network_fibre_nodes",[]),
            self.data.get("network_fibre_cables",[]),
            self.data.get("network_fibre_splices",[]),
            enclosure_id,
        )
        dialog.exec()
    def delete_cable(self,cable_id):
        if QMessageBox.question(self,"Delete fibre cable",f"Delete fibre cable {cable_id} and its splice records?")!=QMessageBox.Yes:return
        self.data["network_fibre_cables"]=[c for c in self.data.get("network_fibre_cables",[]) if _text(c.get("id"))!=cable_id]
        self.data["network_fibre_splices"]=[s for s in self.data.get("network_fibre_splices",[]) if _text(s.get("incoming_cable_id"))!=cable_id and _text(s.get("outgoing_cable_id"))!=cable_id]; calculate_optical_budgets(self.data); self._commit(changed=("network_fibre_cables", "network_fibre_splices", "network_optical_paths", "network_connections"))

    def add_splice(self,node_id="",incoming_cable_id=""):
        seed={"node_id":node_id,"incoming_cable_id":incoming_cable_id}
        dialog=FibreSpliceEditorDialog(self,seed,self.data.get("network_fibre_nodes",[]),self.data.get("network_fibre_cables",[]),next_network_id(self.data.get("network_fibre_splices",[]),"FS"))
        if dialog.exec()==QDialog.Accepted and dialog.result:
            self.data.setdefault("network_fibre_splices",[]).append(dialog.result)
            for cable in self.data.get("network_fibre_cables",[]):
                if _text(cable.get("id")) in {_text(dialog.result.get("incoming_cable_id")),_text(dialog.result.get("outgoing_cable_id"))}:
                    ids=[_text(v) for v in cable.get("splice_ids",[]) if _text(v)]
                    if _text(dialog.result.get("id")) not in ids: ids.append(_text(dialog.result.get("id")))
                    cable["splice_ids"]=ids
            set_core_status_from_splices(self.data); calculate_optical_budgets(self.data); self._commit(changed=("network_fibre_splices", "network_fibre_cables", "network_optical_paths", "network_connections"))
    def edit_splice(self,splice_id):
        splices=self.data.get("network_fibre_splices",[]); index=next((i for i,s in enumerate(splices) if _text(s.get("id"))==splice_id),-1)
        if index<0:return
        dialog=FibreSpliceEditorDialog(self,splices[index],self.data.get("network_fibre_nodes",[]),self.data.get("network_fibre_cables",[]),splice_id)
        if dialog.exec()==QDialog.Accepted and dialog.result: splices[index]=dialog.result; set_core_status_from_splices(self.data); calculate_optical_budgets(self.data); self._commit(changed=("network_fibre_splices", "network_fibre_cables", "network_optical_paths", "network_connections"))
    def delete_splice(self,splice_id):
        if QMessageBox.question(self,"Delete splice",f"Delete splice {splice_id}?")!=QMessageBox.Yes:return
        self.data["network_fibre_splices"]=[s for s in self.data.get("network_fibre_splices",[]) if _text(s.get("id"))!=splice_id]
        for cable in self.data.get("network_fibre_cables",[]): cable["splice_ids"]=[v for v in cable.get("splice_ids",[]) if _text(v)!=splice_id]
        calculate_optical_budgets(self.data); self._commit(changed=("network_fibre_splices", "network_fibre_cables", "network_optical_paths", "network_connections"))

    def _node_context(self,node_id,screen_pos):
        node=next((n for n in self.data.get("network_fibre_nodes",[]) if _text(n.get("id"))==node_id),{})
        menu=QMenu(self); edit=menu.addAction("Edit fibre node"); cassette=menu.addAction("Add splice cassette inside"); splice=menu.addAction("Add splice at this node")
        view_trays=menu.addAction("View splice cassettes") if _text(node.get("node_type")) in {"splice_enclosure","fibre_joint","termination"} else None
        menu.addSeparator(); delete=menu.addAction("Delete fibre node")
        action=menu.exec(screen_pos)
        if action==edit:self.edit_node(node_id)
        elif view_trays is not None and action==view_trays:self.view_cassettes(node_id)
        elif action==cassette:
            parent=next((n for n in self.data.get("network_fibre_nodes",[]) if _text(n.get("id"))==node_id),{})
            seed={"node_type":"splice_cassette","parent_node_id":node_id,"location_name":parent.get("location_name", ""),"floor":parent.get("floor",0),"x":parent.get("x",0.0),"y":parent.get("y",0.0)}
            dialog=FibreNodeEditorDialog(self,seed,self.data.get("network_fibre_nodes",[]),self.data.get("network_asset_instances",[]),self.data.get("locations",[]),next_network_id(self.data.get("network_fibre_nodes",[]),"FSC"),self._floor())
            if dialog.exec()==QDialog.Accepted and dialog.result:self.data["network_fibre_nodes"].append(dialog.result);self._commit(changed=("network_fibre_nodes",))
        elif action==splice:self.add_splice(node_id)
        elif action==delete:self.delete_node(node_id)

    def _cable_context(self,cable_id,screen_pos):
        menu=QMenu(self); trace=menu.addAction("Trace circuit"); edit=menu.addAction("Edit fibre cable and cores"); splice=menu.addAction("Add splice using this cable"); menu.addSeparator(); delete=menu.addAction("Delete fibre cable")
        action=menu.exec(screen_pos)
        if action==trace:self.trace_cable(cable_id)
        elif action==edit:self.edit_cable(cable_id)
        elif action==splice:self.add_splice("",cable_id)
        elif action==delete:self.delete_cable(cable_id)


__all__=["PhysicalFibreTopologyDialog"]
