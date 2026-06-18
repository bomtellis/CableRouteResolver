"""Editable physical-fibre floor topology, separate from the logical network view."""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import time
from typing import Callable, Dict, List, Optional, Sequence

from PySide6.QtCore import QPoint, QPointF, QRectF, Qt, QTimer, Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import (
    QComboBox, QDialog, QFrame, QGraphicsItem, QGraphicsObject, QGraphicsPathItem,
    QGraphicsScene, QGraphicsView, QHBoxLayout, QLabel, QMenu, QMessageBox,
    QPushButton, QVBoxLayout,
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
from network_schema import ensure_network_schema, next_network_id
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


class FibreNodeItem(QGraphicsObject):
    contextRequested = Signal(str, object)
    moved = Signal(str, float, float)
    activated = Signal(str)

    def __init__(self, node: dict, traced: bool = False, symbol_scale: float = 0.12):
        super().__init__()
        self.node = node
        self.traced = traced
        self.symbol_scale = max(0.05, float(symbol_scale or 0.12))
        self._size = max(6.0, min(12.0, 8.0 * self.symbol_scale / 0.12))
        self.setFlags(QGraphicsItem.ItemIsSelectable | QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemSendsGeometryChanges | QGraphicsItem.ItemIgnoresTransformations)
        self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
        self.setAcceptHoverEvents(True)
        self.setToolTip(self._tooltip())

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
        pen = QPen(border, 1.8 if self.traced or self.isSelected() else 1.0); pen.setCosmetic(True)
        painter.setPen(pen); painter.setBrush(colour)
        rect = self.boundingRect().adjusted(1.0, 1.0, -1.0, -1.0)
        if node_type in {"splice_enclosure", "fibre_joint"}: painter.drawEllipse(rect)
        elif node_type == "splice_cassette":
            painter.drawRoundedRect(rect.adjusted(0, 2, 0, -2), 2, 2)
            painter.drawLine(QPointF(rect.left()+2, -1), QPointF(rect.right()-2, -1)); painter.drawLine(QPointF(rect.left()+2, 2), QPointF(rect.right()-2, 2))
        elif node_type in {"handhole", "chamber"}: painter.drawRect(rect)
        else: painter.drawRoundedRect(rect, 3, 3)
        painter.setPen(QColor("#ffffff")); font = QFont("Arial"); font.setPixelSize(max(5, int(self._size * 0.34))); font.setBold(True); painter.setFont(font)
        initials = {"splice_enclosure":"SE","splice_cassette":"SC","fibre_joint":"J","termination":"T","handhole":"HH","chamber":"CH"}.get(node_type,"F")
        painter.drawText(self.boundingRect(), Qt.AlignCenter, initials)

    def mouseDoubleClickEvent(self, event): self.activated.emit(_text(self.node.get("id"))); event.accept()
    def contextMenuEvent(self, event): self.contextRequested.emit(_text(self.node.get("id")), event.screenPos()); event.accept()
    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        pos = self.pos(); self.moved.emit(_text(self.node.get("id")), pos.x(), -pos.y())


class FibreCableBatchItem(QGraphicsObject):
    """One graphics item for all static fibre paths and labels on a floor."""
    def __init__(self, records: Sequence[dict], labels: Sequence[dict], context_callback: Callable[[str, QPoint], None], width_scale: float = 0.18, label_scale: float = 0.14):
        super().__init__()
        self.records = list(records)
        self.labels = list(labels)
        self.context_callback = context_callback
        self.width_scale = max(0.05, float(width_scale or 0.18))
        self.label_scale = max(0.06, float(label_scale or 0.14))
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
        for label in self.labels:
            point = label.get("point", QPointF())
            marker = QRectF(point.x()-1, point.y()-1, 2, 2)
            self._bounds = marker if first else self._bounds.united(marker); first = False
        self.setZValue(-0.2)
        # A device-coordinate pixmap cache is expensive for floor-sized paths
        # and is rebuilt whenever the view scale changes. Batched vector paths
        # are faster and use substantially less memory without that cache.
        self.setCacheMode(QGraphicsItem.NoCache)
        self.setAcceptHoverEvents(False)

    def boundingRect(self):
        return self._bounds.adjusted(-4, -4, 4, 4) if not self._bounds.isNull() else QRectF(-1, -1, 2, 2)

    def paint(self, painter, option, widget=None):
        lod = max(0.001, abs(painter.worldTransform().m11()))
        normal_pen = QPen(QColor("#6f8dff"), max(0.40, 0.72 * self.width_scale / 0.18), Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin); normal_pen.setCosmetic(True)
        traced_pen = QPen(QColor("#ff8a24"), max(0.80, 1.45 * self.width_scale / 0.18), Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin); traced_pen.setCosmetic(True)
        painter.setBrush(Qt.NoBrush); painter.setPen(normal_pen); painter.drawPath(self.normal_path); painter.setPen(traced_pen); painter.drawPath(self.traced_path)
        # Keep labels hidden at floor-plan zoom. When visible they remain a
        # compact, near-constant screen size and cannot expand enough to obscure
        # the underlying DXF. Legacy projects commonly store label_scale=0.30;
        # the renderer maps that old default to the new compact presentation.
        if lod < 0.72:
            return
        requested_screen_px = 3.8 * self.label_scale / 0.14
        screen_px = max(3.0, min(4.8, requested_screen_px))
        font = QFont("Arial"); font.setBold(True); font.setPixelSize(max(1, int(round(screen_px / lod))))
        painter.setFont(font)
        offset_x = 2.0 / lod; offset_y = -5.0 / lod
        exposed = option.exposedRect.adjusted(-20.0 / lod, -20.0 / lod, 20.0 / lod, 20.0 / lod)
        for label in self.labels:
            minimum_lod = float(label.get("minimum_lod", 0.30))
            if lod < minimum_lod:
                continue
            point = label.get("point", QPointF())
            if not exposed.contains(point):
                continue
            painter.setPen(label.get("colour", QColor("#b8c8ff")))
            painter.drawText(point + QPointF(offset_x, offset_y), _text(label.get("text")))

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
        if not isinstance(settings, dict) or not isinstance(settings.get("physical_fibre_layer"), dict):
            ensure_network_schema(self.data)
        self.trace = circuit_trace(self.data, initial_trace_connection_id) if initial_trace_connection_id else {}
        self._route_points_cache: Optional[Dict[str, dict]] = None
        self._cable_geometry_cache: Dict[int, List[dict]] = {}
        self._base_graph_cache: Dict[int, QPainterPath] = {}
        self._instances_cache: Optional[Dict[str, dict]] = None
        self._building = False
        self._initial_build_pending = True
        self.setWindowTitle("Physical Fibre Topology"); self.setWindowFlag(Qt.Window, True); self.resize(1500, 900)
        self.setStyleSheet("QDialog{background:#10161c;color:#e8edf1} QPushButton,QComboBox{background:#25303b;color:#e8edf1;border:1px solid #46535e;padding:5px;border-radius:4px} QLabel{color:#d8e0e6}")
        layout = QVBoxLayout(self); toolbar = QHBoxLayout(); layout.addLayout(toolbar)
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
        self.scene = QGraphicsScene(self); self.scene.setItemIndexMethod(QGraphicsScene.NoIndex); self.view = FibreMapView(self); self.view.setScene(self.scene); layout.addWidget(self.view, 1)
        self.status = QLabel("Loading physical fibre…"); self.status.setStyleSheet("padding:7px;background:#182028;color:#aeb9c2"); layout.addWidget(self.status)
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
            "corridors", "locations", "data_points", "transitions",
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
            keys = tuple(changed or (
                "network_settings", "network_assets", "network_asset_instances",
                "network_connections", "network_optic_modules", "network_optical_paths",
                "network_fibre_cable_types", "network_fibre_cables",
                "network_fibre_nodes", "network_fibre_splices",
            ))
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
            symbol_scale = self._compact_scale(layer.get("symbol_scale"), (0.32, 0.18), 0.12, 0.05, 0.32)
            label_scale = self._compact_scale(layer.get("label_scale"), (0.55, 0.30, 0.18), 0.14, 0.06, 0.24)
            width_scale = self._compact_scale(layer.get("cable_width_scale"), (0.55, 0.30, 0.22), 0.18, 0.05, 0.32)
            route_points = self._all_route_points()
            if layer.get("show_base_graph", True): self._draw_base_graph(floor, route_points)
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
                    "minimum_lod": 1.10,
                })
                count = splice_counts.get(_text(node.get("id")), 0)
                if layer.get("show_splice_labels", True) and count:
                    labels.append({
                        "point": item.pos(),
                        "text": f"{count} splice{'s' if count != 1 else ''}",
                        "colour": QColor("#d5a5f1"),
                        "minimum_lod": 1.65,
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
                tiled_labels: Dict[tuple[int, int], List[dict]] = defaultdict(list)
                geometry_midpoints = {record["cable_id"]: record["midpoint"] for record in geometry}
                for record in cable_records:
                    tiled_records[tile_key(geometry_midpoints.get(record["cable_id"], QPointF()))].append(record)
                for label in labels:
                    tiled_labels[tile_key(label.get("point", QPointF()))].append(label)
                for key in sorted(set(tiled_records) | set(tiled_labels)):
                    self.scene.addItem(FibreCableBatchItem(
                        tiled_records.get(key, []),
                        tiled_labels.get(key, []),
                        self._cable_context,
                        width_scale,
                        label_scale,
                    ))

            content_rect = QRectF()
            have_bounds = False
            base_path = self._base_graph_cache.get(floor)
            if base_path is not None and not base_path.isEmpty():
                content_rect = base_path.boundingRect(); have_bounds = True
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
                f"loaded in {elapsed:.2f} s · labels appear only at close zoom"
            )
        finally:
            self._building = False
            self.view.setUpdatesEnabled(True)
            self.view.viewport().update()

    def _draw_base_graph(self, floor, points):
        path = self._base_graph_cache.get(floor)
        if path is None:
            path = QPainterPath()
            for edge in self.data.get("corridors", {}).get("edges", []):
                if not isinstance(edge, dict): continue
                a = points.get(_text(edge.get("from"))); b = points.get(_text(edge.get("to")))
                if not a or not b or _int(a.get("floor")) != floor or _int(b.get("floor")) != floor: continue
                path.moveTo(QPointF(_float(a.get("x")), -_float(a.get("y")))); path.lineTo(QPointF(_float(b.get("x")), -_float(b.get("y"))))
            self._base_graph_cache[floor] = path
        if not path.isEmpty():
            item = QGraphicsPathItem(path); pen = QPen(QColor("#2d3740"), 0.55); pen.setCosmetic(True); item.setPen(pen); item.setZValue(-2); self.scene.addItem(item)

    def _fit(self): self.view.fit_content()
    def refresh(self):
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
        if dialog.exec()==QDialog.Accepted and dialog.result: self.data.setdefault("network_fibre_nodes",[]).append(dialog.result); self._commit()
    def edit_node(self,node_id):
        nodes=self.data.get("network_fibre_nodes",[]); index=next((i for i,n in enumerate(nodes) if _text(n.get("id"))==_text(node_id)),-1)
        if index<0:return
        dialog=FibreNodeEditorDialog(self,nodes[index],nodes,self.data.get("network_asset_instances",[]),self.data.get("locations",[]),node_id)
        if dialog.exec()==QDialog.Accepted and dialog.result: nodes[index]=dialog.result; self._commit()
    def delete_node(self,node_id):
        if QMessageBox.question(self,"Delete fibre node",f"Delete fibre node {node_id} and its splice records?")!=QMessageBox.Yes:return
        self.data["network_fibre_nodes"]=[n for n in self.data.get("network_fibre_nodes",[]) if _text(n.get("id"))!=node_id and _text(n.get("parent_node_id"))!=node_id]
        self.data["network_fibre_splices"]=[s for s in self.data.get("network_fibre_splices",[]) if _text(s.get("node_id"))!=node_id and _text(s.get("cassette_id"))!=node_id]; self._commit()
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
            self._commit()

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
            self._commit()

    def edit_cable_types(self):
        dialog=FibreCableTypeLibraryDialog(self,self.data.get("network_fibre_cable_types",[]))
        if dialog.exec()==QDialog.Accepted and dialog.result is not None:
            self.data["network_fibre_cable_types"]=dialog.result
            ensure_network_schema(self.data)
            calculate_optical_budgets(self.data)
            self._commit()

    def edit_fibre_planning(self):
        settings=self.data.setdefault("network_settings",{}).get("physical_fibre_planning",{})
        dialog=PhysicalFibrePlanningDialog(self,settings,self.data.get("network_fibre_cable_types",[]))
        if dialog.exec()==QDialog.Accepted and dialog.result is not None:
            self.data.setdefault("network_settings",{})["physical_fibre_planning"]=dialog.result
            ensure_network_schema(self.data)
            self._commit()

    def edit_optical_properties(self):
        dialog=OpticalPropertiesDialog(self,self.data.get("network_assets",[]))
        if dialog.exec()==QDialog.Accepted and dialog.result is not None:
            self.data["network_assets"]=dialog.result
            ensure_network_schema(self.data)
            calculate_optical_budgets(self.data)
            self._commit()

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
        self.data["network_fibre_splices"]=[s for s in self.data.get("network_fibre_splices",[]) if _text(s.get("incoming_cable_id"))!=cable_id and _text(s.get("outgoing_cable_id"))!=cable_id]; calculate_optical_budgets(self.data); self._commit()

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
            set_core_status_from_splices(self.data); calculate_optical_budgets(self.data); self._commit()
    def edit_splice(self,splice_id):
        splices=self.data.get("network_fibre_splices",[]); index=next((i for i,s in enumerate(splices) if _text(s.get("id"))==splice_id),-1)
        if index<0:return
        dialog=FibreSpliceEditorDialog(self,splices[index],self.data.get("network_fibre_nodes",[]),self.data.get("network_fibre_cables",[]),splice_id)
        if dialog.exec()==QDialog.Accepted and dialog.result: splices[index]=dialog.result; set_core_status_from_splices(self.data); calculate_optical_budgets(self.data); self._commit()
    def delete_splice(self,splice_id):
        if QMessageBox.question(self,"Delete splice",f"Delete splice {splice_id}?")!=QMessageBox.Yes:return
        self.data["network_fibre_splices"]=[s for s in self.data.get("network_fibre_splices",[]) if _text(s.get("id"))!=splice_id]
        for cable in self.data.get("network_fibre_cables",[]): cable["splice_ids"]=[v for v in cable.get("splice_ids",[]) if _text(v)!=splice_id]
        calculate_optical_budgets(self.data); self._commit()

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
            if dialog.exec()==QDialog.Accepted and dialog.result:self.data["network_fibre_nodes"].append(dialog.result);self._commit()
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
