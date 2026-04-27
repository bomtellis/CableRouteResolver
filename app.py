import os
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED, as_completed
from itertools import combinations
from copy import deepcopy
import re

import heapq
import math
import sys
from pathlib import Path
from itertools import combinations

from PySide6.QtCore import (
    QObject,
    QPoint,
    QPointF,
    Qt,
    Signal,
    QRect,
    QRectF,
    QThread,
    Slot,
)
from PySide6.QtGui import (
    QColor,
    QBrush,
    QPainter,
    QPen,
    QFont,
    QPolygonF,
    QShortcut,
    QKeySequence,
)
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QGraphicsItem,
    QGraphicsPolygonItem,
    QGraphicsScene,
    QGraphicsView,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QSpinBox,
    QVBoxLayout,
    QWidget,
    QInputDialog,
    QDoubleSpinBox,
    QRubberBand,
    QScrollArea,
    QListWidget,
    QLineEdit,
)

from PySide6.QtOpenGLWidgets import QOpenGLWidget

from dxf_scene import DXFScene
from dialogs import (
    BulkDataPointPlacementDialog,
    BulkLocationPlacementDialog,
    DataPointEditorDialog,
    DepartmentEditorDialog,
    EdgeConnectionsDialog,
    LocationEditorDialog,
    PointEditorDialog,
    TableListEditor,
    TransitionEditorDialog,
    SuggestCommsRoomDialog,
    CommsRoomOptimisationProgressDialog,
    RoomTypesEditorWindow,
    AssetsEditorWindow,
)
from advanced_dialogs import (
    ConnectionEditorWindow,
    DataPointDepartmentsBulkDialog,
    FloorTemplateCopyDialog,
    LocationDepartmentsBulkDialog,
    RouteProfilesEditorV2,
)
from models import JsonStore

try:
    import pulp
except Exception:
    pulp = None

_COMMS_CANDIDATES = None
_COMMS_ALL_MASK = None
_COMMS_POINT_COUNT = None


def _init_comms_process(candidate_payload, all_mask, point_count):
    global _COMMS_CANDIDATES, _COMMS_ALL_MASK, _COMMS_POINT_COUNT
    _COMMS_CANDIDATES = candidate_payload
    _COMMS_ALL_MASK = int(all_mask)
    _COMMS_POINT_COUNT = int(point_count)


def _evaluate_comms_batch(batch):
    candidates = _COMMS_CANDIDATES
    all_mask = _COMMS_ALL_MASK
    point_count = _COMMS_POINT_COUNT

    best_combo = None
    best_score = None
    processed = 0

    for combo in batch:
        processed += 1

        covered = 0
        for candidate_idx in combo:
            covered |= candidates[candidate_idx]["mask"]

        if covered != all_mask:
            continue

        total = 0.0
        worst = 0.0

        for point_idx in range(point_count):
            best_length = None

            for candidate_idx in combo:
                value = candidates[candidate_idx]["lengths"][point_idx]
                if value is None:
                    continue
                if best_length is None or value < best_length:
                    best_length = value

            if best_length is None:
                total = None
                break

            total += best_length
            worst = max(worst, best_length)

        if total is None:
            continue

        score = (len(combo), total, worst)

        if best_score is None or score < best_score:
            best_score = score
            best_combo = combo

    return processed, best_combo, best_score


def _batched_combinations(iterable, batch_size):
    batch = []
    for item in iterable:
        batch.append(tuple(item))
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

_AUTOROUTE_GRAPH = None
_AUTOROUTE_POINTS = None
_AUTOROUTE_COMMS_ROOMS = None


def _init_autoroute_process(graph, points, comms_rooms):
    global _AUTOROUTE_GRAPH, _AUTOROUTE_POINTS, _AUTOROUTE_COMMS_ROOMS
    _AUTOROUTE_GRAPH = graph
    _AUTOROUTE_POINTS = points
    _AUTOROUTE_COMMS_ROOMS = comms_rooms


def _autoroute_shortest_path(graph, start, end):
    if start not in graph or end not in graph:
        return None, []

    heap = [(0.0, start)]
    dist = {start: 0.0}

    while heap:
        cost, node = heapq.heappop(heap)

        if cost > dist.get(node, math.inf):
            continue

        if node == end:
            return cost, []

        for next_node, weight in graph.get(node, []):
            new_cost = cost + float(weight)
            if new_cost < dist.get(next_node, math.inf):
                dist[next_node] = new_cost
                heapq.heappush(heap, (new_cost, next_node))

    return None, []


def _autoroute_data_point_worker(data_point):
    graph = _AUTOROUTE_GRAPH
    points = _AUTOROUTE_POINTS
    comms_rooms = _AUTOROUTE_COMMS_ROOMS

    point_name = str(data_point.get("name", "")).strip()
    if not point_name:
        return {"status": "skip_empty"}

    if point_name not in points:
        return {"status": "unreachable", "point_name": point_name}

    best_room = None
    best_cost = None

    for comms_room in comms_rooms:
        if comms_room not in points:
            continue

        route_cost, _route_path = _autoroute_shortest_path(
            graph, point_name, comms_room
        )
        if route_cost is None:
            continue

        total_cost = float(route_cost) + float(
            data_point.get("extension_distance_m", 0.0) or 0.0
        )

        if best_cost is None or total_cost < best_cost:
            best_cost = total_cost
            best_room = comms_room

    if best_room is None:
        return {"status": "unreachable", "point_name": point_name}

    return {
        "status": "created",
        "point_name": point_name,
        "from": best_room,
        "to": point_name,
        "qty": int(data_point.get("qty", 1) or 1),
        "route_profile": "",
        "cost": best_cost,
    }


def _load_dxf_floor_process(args):
    floor, path = args
    try:
        payload = DXFScene.load_content(path)
        return {
            "ok": True,
            "floor": int(floor),
            "path": str(path),
            "entities": payload["entities"],
            "bounds": payload["bounds"],
            "error": "",
        }
    except Exception as exc:
        return {
            "ok": False,
            "floor": int(floor),
            "path": str(path),
            "entities": None,
            "bounds": None,
            "error": str(exc),
        }


class DXFLoadWorker(QObject):
    loaded = Signal(int, str, object, object)
    failed = Signal(int, str, str)
    finished_batch = Signal()

    @Slot(object)
    def load_floors(self, jobs):
        jobs = list(jobs or [])
        if not jobs:
            self.finished_batch.emit()
            return

        worker_count = min(len(jobs), max(1, (os.cpu_count() or 2) - 1))

        try:
            with ProcessPoolExecutor(max_workers=worker_count) as pool:
                futures = [pool.submit(_load_dxf_floor_process, job) for job in jobs]

                for future in as_completed(futures):
                    result = future.result()
                    floor = int(result["floor"])
                    path = str(result["path"])

                    if result.get("ok"):
                        self.loaded.emit(
                            floor,
                            path,
                            result["entities"],
                            result["bounds"],
                        )
                    else:
                        self.failed.emit(
                            floor,
                            path,
                            str(result.get("error", "Unknown DXF load error")),
                        )
        finally:
            self.finished_batch.emit()


class DXFLoadingDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._completed = False
        self.setWindowTitle("Loading DXFs")
        self.setWindowModality(Qt.ApplicationModal)
        self.setWindowFlag(Qt.WindowCloseButtonHint, False)
        self.setWindowFlag(Qt.WindowContextHelpButtonHint, False)
        self.setMinimumWidth(420)

        layout = QVBoxLayout(self)
        self.message_label = QLabel("Loading DXF files...")
        self.message_label.setWordWrap(True)
        layout.addWidget(self.message_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.detail_label = QLabel("0 / 0")
        layout.addWidget(self.detail_label)

    def update_progress(self, current, total, message, failed_count=0):
        total = max(1, int(total))
        current = max(0, min(int(current), total))
        self.progress_bar.setRange(0, total)
        self.progress_bar.setValue(current)
        self.message_label.setText(message)
        detail = f"{current} / {total} loaded"
        if failed_count:
            detail += f" ({failed_count} failed)"
        self.detail_label.setText(detail)

    def mark_complete(self):
        self._completed = True
        self.accept()

    def reject(self):
        if self._completed:
            super().reject()

    def closeEvent(self, event):
        if self._completed:
            super().closeEvent(event)
        else:
            event.ignore()


class EditorGraphicsView(QGraphicsView):
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
        self.setViewport(QOpenGLWidget())
        self.setRenderHint(QPainter.Antialiasing, False)
        self.setBackgroundBrush(QBrush(QColor("#111111")))
        self._overlay_provider = None
        self.setDragMode(QGraphicsView.NoDrag)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorUnderMouse)
        self._middle_panning = False
        self._last_middle_pos = None

    def mousePressEvent(self, event):
        scene_pos = self.mapToScene(event.position().toPoint())
        if event.button() == Qt.LeftButton:
            self.leftClicked.emit(event, scene_pos.x(), scene_pos.y())
        elif event.button() == Qt.RightButton:
            self.rightClicked.emit(event, scene_pos.x(), scene_pos.y())
        elif event.button() == Qt.MiddleButton:
            self._middle_panning = True
            self._last_middle_pos = event.position().toPoint()
            self.middleClicked.emit(event)
        super().mousePressEvent(event)

    def mouseDoubleClickEvent(self, event):
        scene_pos = self.mapToScene(event.position().toPoint())
        if event.button() == Qt.LeftButton:
            self.leftDoubleClicked.emit(event, scene_pos.x(), scene_pos.y())
        super().mouseDoubleClickEvent(event)

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.leftReleased.emit(event)
        elif event.button() == Qt.MiddleButton:
            self._middle_panning = False
            self._last_middle_pos = None
            self.middleReleased.emit(event)
        super().mouseReleaseEvent(event)

    def mouseMoveEvent(self, event):
        scene_pos = self.mapToScene(event.position().toPoint())
        if self._middle_panning and self._last_middle_pos is not None:
            self.middleDragged.emit(event)
        if event.buttons() & Qt.LeftButton:
            self.mouseDragged.emit(event, scene_pos.x(), scene_pos.y())
        super().mouseMoveEvent(event)

    def wheelEvent(self, event):
        self.mouseWheelScrolled.emit(event)
        event.accept()

    def set_overlay_provider(self, overlay_provider):
        self._overlay_provider = overlay_provider
        self.viewport().update()

    def drawForeground(self, painter, rect):
        super().drawForeground(painter, rect)
        if self._overlay_provider:
            painter.save()
            painter.resetTransform()
            self._overlay_provider(painter, self.viewport().rect())
            painter.restore()


class UnassignedDataPointNavigatorDialog(QDialog):
    nextRequested = Signal()
    previousRequested = Signal()
    closeRequested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Unassigned Data Points")
        self.setModal(False)
        self.resize(360, 180)

        layout = QVBoxLayout(self)

        self.status_label = QLabel("Ready")
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        row = QHBoxLayout()
        layout.addLayout(row)

        prev_btn = QPushButton("Previous")
        next_btn = QPushButton("Next")
        close_btn = QPushButton("Close")

        prev_btn.clicked.connect(self.previousRequested.emit)
        next_btn.clicked.connect(self.nextRequested.emit)
        close_btn.clicked.connect(self.close)

        row.addWidget(prev_btn)
        row.addWidget(next_btn)
        row.addWidget(close_btn)

    def set_status(self, text):
        self.status_label.setText(text)


class FindDataPointDialog(QDialog):
    findRequested = Signal(str)
    nextRequested = Signal()
    previousRequested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Find Data Point")
        self.setModal(False)
        self.resize(360, 150)

        layout = QVBoxLayout(self)

        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Enter data point name or part name...")
        layout.addWidget(self.search_box)

        self.status_label = QLabel("Ready")
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        row = QHBoxLayout()
        layout.addLayout(row)

        find_btn = QPushButton("Find")
        prev_btn = QPushButton("Previous")
        next_btn = QPushButton("Next")
        close_btn = QPushButton("Close")

        row.addWidget(find_btn)
        row.addWidget(prev_btn)
        row.addWidget(next_btn)
        row.addWidget(close_btn)

        find_btn.clicked.connect(self._emit_find)
        self.search_box.returnPressed.connect(self._emit_find)
        prev_btn.clicked.connect(self.previousRequested.emit)
        next_btn.clicked.connect(self.nextRequested.emit)
        close_btn.clicked.connect(self.close)

    def _emit_find(self):
        self.findRequested.emit(self.search_box.text().strip())

    def set_status(self, text):
        self.status_label.setText(text)


class CableRouteEditor(QMainWindow):
    _request_dxf_batch_load = Signal(object)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cable Routing Graph Editor")
        self.resize(1500, 920)

        self.undo_stack = []
        self.redo_stack = []
        self.max_undo_steps = 50

        self.store = JsonStore()
        self.current_json_path = None
        self.current_dxf_path = None
        self.loaded_dxf_floor = None
        self.dxf_scene = DXFScene()
        self._dxf_cache = {}
        self._dxf_loading_floors = set()
        self._pending_fit_after_load = False
        self._loading_dialog = None
        self._loading_batch_floors = set()
        self._loading_batch_failed = set()
        self._loading_batch_active = False

        self._dxf_thread = QThread(self)
        self._dxf_worker = DXFLoadWorker()
        self._dxf_worker.moveToThread(self._dxf_thread)
        self._dxf_worker.loaded.connect(self._on_dxf_loaded)
        self._dxf_worker.failed.connect(self._on_dxf_failed)
        self._request_dxf_batch_load.connect(self._dxf_worker.load_floors)
        self._dxf_thread.start()

        self.last_pan = None
        self.selected_for_edge = None
        self.selected_point_name = None
        self.selected_template_names = set()
        self.dragging_point_name = None
        self.drag_mode_active = False
        self.alt_move_locked = False
        self.selection_rect_active = False
        self.selection_rect_origin = None
        self.selection_rect_current = None
        self._rubber_band = None
        self.edge_delete_start = None
        self._item_lookup = {}
        self._point_item_lookup = {}
        self.bulk_location_session = None
        self.bulk_data_point_session = None
        self._comms_optimisation_dialog = None
        self._clear_canvas_multi_selection()

        self._unassigned_dp_dialog = None
        self._unassigned_dp_names = []
        self._unassigned_dp_index = -1

        self._find_dp_dialog = None
        self._find_dp_matches = []
        self._find_dp_index = -1

        self._build_ui()
        self.refresh_canvas()

    def push_undo_state(self, label="Change"):
        self.undo_stack.append(
            {
                "label": label,
                "data": deepcopy(self.store.data),
            }
        )

        if len(self.undo_stack) > self.max_undo_steps:
            self.undo_stack.pop(0)

        self.redo_stack.clear()

    def undo(self):
        if not self.undo_stack:
            self.set_status("Nothing to undo")
            return

        self.redo_stack.append(
            {
                "label": "Redo",
                "data": deepcopy(self.store.data),
            }
        )

        state = self.undo_stack.pop()
        self.store.data = deepcopy(state["data"])
        self.selected_point_name = None
        self.selected_template_names.clear()
        self.selected_for_edge = None
        self.edge_delete_start = None
        self.refresh_canvas()
        self.set_status(f"Undid: {state.get('label', 'Change')}")

    def redo(self):
        if not self.redo_stack:
            self.set_status("Nothing to redo")
            return

        self.undo_stack.append(
            {
                "label": "Undo",
                "data": deepcopy(self.store.data),
            }
        )

        state = self.redo_stack.pop()
        self.store.data = deepcopy(state["data"])
        self.selected_point_name = None
        self.selected_template_names.clear()
        self.selected_for_edge = None
        self.edge_delete_start = None
        self.refresh_canvas()
        self.set_status("Redid change")

    def _build_ui(self):
        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)

        self.sidebar_scroll = QScrollArea()
        self.sidebar_scroll.setWidgetResizable(True)
        self.sidebar_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.sidebar_scroll.setMinimumWidth(280)
        self.sidebar_scroll.setMaximumWidth(320)

        self.sidebar = QWidget()
        self.sidebar.setMinimumWidth(260)
        self.sidebar.setMaximumWidth(260)
        sidebar_layout = QVBoxLayout(self.sidebar)

        self.sidebar_scroll.setWidget(self.sidebar)
        layout.addWidget(self.sidebar_scroll)

        self.scene = QGraphicsScene(self)
        self.canvas = EditorGraphicsView(self)
        self.canvas.setScene(self.scene)
        self.canvas.set_overlay_provider(self.draw_overlay_panels)
        self._rubber_band = QRubberBand(QRubberBand.Rectangle, self.canvas.viewport())
        layout.addWidget(self.canvas, 1)

        self.canvas.leftClicked.connect(self.on_left_click)
        self.canvas.leftDoubleClicked.connect(self.on_double_click)
        self.canvas.leftReleased.connect(self.on_left_release)
        self.canvas.rightClicked.connect(self.on_right_click)
        self.canvas.middleClicked.connect(self.on_middle_click)
        self.canvas.middleDragged.connect(self.on_middle_drag)
        self.canvas.middleReleased.connect(self.on_middle_release)
        self.canvas.mouseWheelScrolled.connect(self.on_mousewheel)
        self.canvas.mouseDragged.connect(self.on_drag)

        self.mode_combo = QComboBox()
        self.mode_combo.addItems(
            [
                "select_move",
                "corridor_node",
                "location",
                "department",
                "data_point",
                "transition",
                "edge",
                "pan",
                "delete",
            ]
        )
        self.floor_spin = QSpinBox()
        self.floor_spin.setRange(0, 99)
        self.floor_spin.valueChanged.connect(self.on_floor_changed)
        self.snap_check = QCheckBox("Snap to 1.0")
        self.snap_check.setChecked(True)
        self.bidirectional_check = QCheckBox("Bidirectional edges")
        self.bidirectional_check.setChecked(True)
        self.chain_edges_check = QCheckBox("Chain edges")
        self.chain_edges_check.setChecked(True)

        self.show_dxf_check = QCheckBox("Show DXF")
        self.show_dxf_check.setChecked(True)

        self.show_labels_check = QCheckBox("Show labels")
        self.show_labels_check.setChecked(True)

        self.show_edges_check = QCheckBox("Show edges")
        self.show_edges_check.setChecked(True)

        self.show_nodes_check = QCheckBox("Show corridor nodes")
        self.show_nodes_check.setChecked(True)

        self.show_data_points_check = QCheckBox("Show data points")
        self.show_data_points_check.setChecked(True)

        self.hide_connected_data_points_check = QCheckBox("Hide connected data points")
        self.hide_connected_data_points_check.setChecked(False)

        self.show_locations_check = QCheckBox("Show locations")
        self.show_locations_check.setChecked(True)

        self.show_comms_rooms_check = QCheckBox("Show comms rooms")
        self.show_comms_rooms_check.setChecked(True)

        for check in [
            self.show_dxf_check,
            self.show_labels_check,
            self.show_edges_check,
            self.show_nodes_check,
            self.show_data_points_check,
            self.show_locations_check,
            self.show_comms_rooms_check,
            self.hide_connected_data_points_check,
        ]:
            check.toggled.connect(self.refresh_canvas)

        self.quick_add_corridor_check = QCheckBox("Quick add corridor nodes")
        self.quick_add_corridor_check.setChecked(False)

        self.default_corridor_height_spin = QDoubleSpinBox()
        self.default_corridor_height_spin.setRange(0.0, 100.0)
        self.default_corridor_height_spin.setDecimals(2)
        self.default_corridor_height_spin.setSingleStep(0.1)
        self.default_corridor_height_spin.setValue(0.0)

        self.default_corridor_cable_limit_spin = QSpinBox()
        self.default_corridor_cable_limit_spin.setRange(0, 1000000)
        self.default_corridor_cable_limit_spin.setValue(0)

        sidebar_layout.addWidget(QLabel("Mode"))
        sidebar_layout.addWidget(self.mode_combo)
        sidebar_layout.addSpacing(10)
        sidebar_layout.addWidget(QLabel("Floor"))
        floor_row = QHBoxLayout()
        floor_row.addWidget(self.floor_spin)
        go_btn = QPushButton("Go")
        go_btn.clicked.connect(self.refresh_canvas)
        floor_row.addWidget(go_btn)
        sidebar_layout.addLayout(floor_row)
        sidebar_layout.addSpacing(10)
        sidebar_layout.addWidget(self.snap_check)
        sidebar_layout.addWidget(self.bidirectional_check)
        sidebar_layout.addWidget(self.chain_edges_check)
        sidebar_layout.addWidget(self.show_dxf_check)
        sidebar_layout.addWidget(self.show_labels_check)
        sidebar_layout.addWidget(self.show_edges_check)
        sidebar_layout.addWidget(self.show_nodes_check)
        sidebar_layout.addWidget(self.show_data_points_check)
        sidebar_layout.addWidget(self.hide_connected_data_points_check)
        sidebar_layout.addWidget(self.show_locations_check)
        sidebar_layout.addWidget(self.show_comms_rooms_check)
        sidebar_layout.addSpacing(10)

        sidebar_layout.addWidget(self.quick_add_corridor_check)
        sidebar_layout.addWidget(QLabel("Default corridor height AFFL (m)"))
        sidebar_layout.addWidget(self.default_corridor_height_spin)
        sidebar_layout.addWidget(QLabel("Default corridor cable limit"))
        sidebar_layout.addWidget(self.default_corridor_cable_limit_spin)

        sidebar_layout.addSpacing(10)

        for text, handler in [
            ("Open JSON", self.open_json),
            ("Save JSON", self.save_json),
            ("Save JSON As", self.save_json_as),
            ("Undo", self.undo),
            ("Redo", self.redo),
            ("Map DXF to Floor", self.load_dxf),
            ("Clear Floor DXF", self.clear_floor_dxf),
            ("Fit View", self.fit_view),
            ("Validate", self.validate_json),
            ("Departments", self.manage_departments),
            ("Assets", self.manage_assets),
            ("Room Types", self.manage_room_types),
            ("Locations", self.manage_locations),
            ("Location Departments", self.manage_location_departments),
            ("Mass Create Locations", self.start_bulk_location_placement),
            ("Mass Create Data Points", self.start_bulk_data_point_placement),
            ("Copy Template Between Floors", self.copy_template_between_floors),
            ("Data Point Departments", self.manage_data_point_departments),
            ("Data Points", self.manage_data_points),
            ("Find Unconnected Data Points", self.find_unconnected_data_points),
            ("Find Unassigned Data Points", self.show_unassigned_data_point_navigator),
            ("Find Data Point", self.show_find_data_point_dialog),
            ("Transitions", self.manage_transitions),
            ("Connections", self.manage_connections),
            ("Optimise Comms Rooms", self.optimise_comms_rooms_for_model),
            ("Autoroute Data Points", self.autoroute_data_points),
            ("Suggest Comms Room", self.suggest_comms_room_for_department),
            ("Route Profiles", self.manage_route_profiles),
            ("Export Floor DXFs", self.export_floor_dxfs),
        ]:
            btn = QPushButton(text)
            btn.clicked.connect(handler)
            sidebar_layout.addWidget(btn)
            if text in {"Validate", "Connections"}:
                sidebar_layout.addSpacing(10)

        cancel_bulk_locations_btn = QPushButton("Cancel Mass Create")
        cancel_bulk_locations_btn.clicked.connect(self.cancel_bulk_location_placement)
        sidebar_layout.addWidget(cancel_bulk_locations_btn)

        cancel_bulk_data_points_btn = QPushButton("Cancel Mass Create Data Points")
        cancel_bulk_data_points_btn.clicked.connect(
            self.cancel_bulk_data_point_placement
        )
        sidebar_layout.addWidget(cancel_bulk_data_points_btn)

        sidebar_layout.addWidget(QLabel("Current file"))
        self.file_label = QLabel("New file")
        self.file_label.setWordWrap(True)
        sidebar_layout.addWidget(self.file_label)
        sidebar_layout.addWidget(QLabel("Status"))
        self.status_label = QLabel("Ready")
        self.status_label.setWordWrap(True)
        sidebar_layout.addWidget(self.status_label)
        sidebar_layout.addStretch(1)

        QShortcut(QKeySequence("Ctrl+Z"), self, activated=self.undo)
        QShortcut(QKeySequence("Ctrl+Y"), self, activated=self.redo)

    def cancel_bulk_location_placement(self):
        if self.bulk_location_session:
            self.bulk_location_session = None
            self.set_status("Mass create cancelled")

    def cancel_bulk_data_point_placement(self):
        if self.bulk_data_point_session:
            self.bulk_data_point_session = None
            self.set_status("Mass create data points cancelled")

    def _format_bulk_data_point_name(self, prefix, number):
        return f"{prefix}{int(number)}"

    def _next_available_bulk_data_point_name(self, prefix, start_number):
        number = int(start_number)
        used = self.store.names_in_use()
        while self._format_bulk_data_point_name(prefix, number) in used:
            number += 1
        return self._format_bulk_data_point_name(prefix, number), number

    def start_bulk_data_point_placement(self):
        floor = self.floor_spin.value()
        dialog = BulkDataPointPlacementDialog(
            self,
            default_floor=floor,
            default_prefix=f"DP{floor}-",
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            prefix = dialog.result["prefix"]
            count = int(dialog.result["count"])
            qty = int(dialog.result["qty"])
            extension_distance_m = float(dialog.result["extension_distance_m"])

            next_name, next_number = self._next_available_bulk_data_point_name(
                prefix, 1
            )

            self.bulk_data_point_session = {
                "prefix": prefix,
                "next_number": next_number,
                "remaining": count,
                "qty": qty,
                "extension_distance_m": extension_distance_m,
            }

            self.mode_combo.setCurrentText("data_point")
            self.set_status(
                f"Mass create active: {count} data point(s) starting at {next_name}. Click to place."
            )

    def _connected_data_point_names(self):
        return {
            str(connection.get("to", "")).strip()
            for connection in self.store.data.get("connections", [])
            if str(connection.get("to", "")).strip()
        }

    def show_find_data_point_dialog(self):
        if self._find_dp_dialog is None:
            self._find_dp_dialog = FindDataPointDialog(self)
            self._find_dp_dialog.findRequested.connect(self.find_data_point_matches)
            self._find_dp_dialog.nextRequested.connect(self.goto_next_found_data_point)
            self._find_dp_dialog.previousRequested.connect(
                self.goto_previous_found_data_point
            )

        self._find_dp_dialog.show()
        self._find_dp_dialog.raise_()
        self._find_dp_dialog.activateWindow()

    def find_data_point_matches(self, search_text):
        search_text = str(search_text).strip().lower()

        if not search_text:
            self._find_dp_matches = []
            self._find_dp_index = -1
            self._find_dp_dialog.set_status("Enter a data point name or part name.")
            return

        matches = []

        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            if search_text in name.lower():
                matches.append(name)

        matches.sort(
            key=lambda name: (
                int(self.store.all_points().get(name, {}).get("floor", 0)),
                name,
            )
        )

        self._find_dp_matches = matches
        self._find_dp_index = -1

        if not matches:
            self._find_dp_dialog.set_status("No matching data points found.")
            self.set_status("No matching data points found")
            return

        self.goto_next_found_data_point()

    def goto_next_found_data_point(self):
        if not self._find_dp_matches:
            if self._find_dp_dialog:
                self._find_dp_dialog.set_status("No active search results.")
            return

        self._find_dp_index = (self._find_dp_index + 1) % len(self._find_dp_matches)
        self._centre_on_found_data_point()

    def goto_previous_found_data_point(self):
        if not self._find_dp_matches:
            if self._find_dp_dialog:
                self._find_dp_dialog.set_status("No active search results.")
            return

        self._find_dp_index = (self._find_dp_index - 1) % len(self._find_dp_matches)
        self._centre_on_found_data_point()

    def _centre_on_found_data_point(self):
        if self._find_dp_index < 0:
            return

        name = self._find_dp_matches[self._find_dp_index]
        point = self.store.all_points().get(name)

        if not point:
            return

        floor = int(point.get("floor", 0))

        if self.floor_spin.value() != floor:
            self.floor_spin.setValue(floor)

        self.selected_point_name = name
        self._set_canvas_multi_selection([name], append=False)
        self.refresh_canvas()

        scene_pos = self.world_to_scene(point["x"], point["y"])
        self.canvas.centerOn(scene_pos)

        status = (
            f"{self._find_dp_index + 1} / {len(self._find_dp_matches)}\n"
            f"{name}\n"
            f"Floor {floor}"
        )

        if self._find_dp_dialog:
            self._find_dp_dialog.set_status(status)

        self.set_status(f"Centred on data point {name}")

    def find_unconnected_data_points(self):
        edge_connected = set()

        for edge in self.store.data.get("corridors", {}).get("edges", []):
            a = str(edge.get("from", "")).strip()
            b = str(edge.get("to", "")).strip()

            if a:
                edge_connected.add(a)
            if b:
                edge_connected.add(b)

        unconnected = []

        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            if name not in edge_connected:
                unconnected.append(
                    {
                        "name": name,
                        "floor": int(item.get("floor", 0)),
                        "qty": int(item.get("qty", 1) or 1),
                    }
                )

        unconnected.sort(key=lambda x: (x["floor"], x["name"]))

        if not unconnected:
            QMessageBox.information(
                self,
                "Unconnected Data Points",
                "All data points have at least one edge connection.",
            )
            self.set_status("All data points have edge connections")
            return

        lines = [
            f"Found {len(unconnected)} data point(s) with no edge connection:",
            "",
        ]

        for row in unconnected[:100]:
            lines.append(f"{row['name']} | Floor {row['floor']} | Qty {row['qty']}")

        if len(unconnected) > 100:
            lines.append(f"... and {len(unconnected) - 100} more")

        QMessageBox.information(
            self,
            "Unconnected Data Points",
            "\n".join(lines),
        )

        self.set_status(
            f"Found {len(unconnected)} data point(s) with no edge connection"
        )

    def _unconnected_data_point_names(self):
        edge_connected = set()

        for edge in self.store.data.get("corridors", {}).get("edges", []):
            a = str(edge.get("from", "")).strip()
            b = str(edge.get("to", "")).strip()

            if a:
                edge_connected.add(a)
            if b:
                edge_connected.add(b)

        result = set()

        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            if name not in edge_connected:
                result.add(name)

        return result

    def set_status(self, text):
        self.status_label.setText(text)

    def on_floor_changed(self, *_):
        self._clear_canvas_multi_selection()
        self.refresh_canvas()
        self._queue_all_floor_dxf_loads(
            active_floor=self.floor_spin.value(), force_reload=False
        )

    def floor_dxf_entries(self):
        self.push_undo_state("Set default dxf floor entries")
        return self.store.data.setdefault("floor_dxf_files", [])

    def get_floor_dxf_path(self, floor):
        return self.store.floor_dxf_path(floor)

    def set_floor_dxf_path(self, floor, filepath):
        self.store.set_floor_dxf_path(floor, filepath)

    def clear_floor_dxf_mapping(self, floor):
        self.store.clear_floor_dxf_path(floor)

    def _all_mapped_floors(self):
        floors = []
        for entry in self.floor_dxf_entries():
            try:
                floor = int(entry.get("floor"))
            except Exception:
                continue
            if self.get_floor_dxf_path(floor):
                floors.append(floor)
        return sorted(set(floors))

    def _ensure_loading_dialog(self):
        if self._loading_dialog is None:
            self._loading_dialog = DXFLoadingDialog(self)
        return self._loading_dialog

    def _update_loading_dialog(self):
        if not self._loading_batch_active:
            return
        dialog = self._ensure_loading_dialog()
        total = len(self._loading_batch_floors)
        completed = 0
        for floor in self._loading_batch_floors:
            path = self.get_floor_dxf_path(floor)
            cached = self._dxf_cache.get(floor)
            if cached and path and cached.get("path") == path:
                completed += 1
            elif floor in self._loading_batch_failed:
                completed += 1
        failed_count = len(self._loading_batch_failed)
        pending = max(0, total - completed)
        message = f"Loading {total} DXF file(s)..."
        if pending:
            message = f"Loading {pending} remaining DXF file(s)..."
        elif failed_count:
            message = "Finished loading DXFs with some failures."
        else:
            message = "Finished loading all DXFs."
        dialog.update_progress(completed, total, message, failed_count=failed_count)
        if total > 0 and not dialog.isVisible():
            dialog.show()
            dialog.raise_()
            dialog.activateWindow()
        QApplication.processEvents()
        if total > 0 and completed >= total:
            dialog.mark_complete()
            self._loading_batch_active = False

    def _start_loading_batch(self, floors):
        target_floors = []
        for floor in floors:
            floor = int(floor)
            path = self.get_floor_dxf_path(floor)
            if path:
                target_floors.append(floor)
        target_floors = sorted(set(target_floors))
        if not target_floors:
            return
        self._loading_batch_floors = set(target_floors)
        self._loading_batch_failed = set()
        self._loading_batch_active = True
        dialog = self._ensure_loading_dialog()
        dialog._completed = False
        self._update_loading_dialog()

    def _queue_all_floor_dxf_loads(self, active_floor=None, force_reload=False):
        floors = self._all_mapped_floors()
        if not floors:
            return

        if active_floor is not None:
            active_floor = int(active_floor)
            floors = [active_floor] + [
                int(floor) for floor in floors if int(floor) != active_floor
            ]

        jobs = []

        for floor in floors:
            floor = int(floor)
            path = self.get_floor_dxf_path(floor)
            if not path:
                continue

            cached = self._dxf_cache.get(floor)
            force_this = bool(
                force_reload and active_floor is not None and floor == int(active_floor)
            )

            if (not force_this) and cached and cached.get("path") == path:
                if active_floor is not None and floor == int(active_floor):
                    self._set_active_dxf_floor(floor)
                continue

            if floor in self._dxf_loading_floors:
                continue

            self._dxf_loading_floors.add(floor)
            jobs.append((floor, path))

        if not jobs:
            self._update_loading_dialog()
            return

        self._start_loading_batch([floor for floor, _path in jobs])

        if active_floor is not None:
            self.set_status(f"Loading DXFs using multiple processes...")

        self._request_dxf_batch_load.emit(jobs)
        self._update_loading_dialog()

    def _clear_dxf_cache(self):
        self._dxf_cache.clear()
        self._dxf_loading_floors.clear()
        self._loading_batch_floors.clear()
        self._loading_batch_failed.clear()
        self._loading_batch_active = False
        if self._loading_dialog is not None and self._loading_dialog.isVisible():
            self._loading_dialog.mark_complete()
        self.current_dxf_path = None
        self.loaded_dxf_floor = None
        self.dxf_scene.clear()

    def _set_active_dxf_floor(self, floor):
        floor = int(floor)
        cached = self._dxf_cache.get(floor)
        if not cached:
            self.current_dxf_path = None
            self.loaded_dxf_floor = None
            self.dxf_scene.clear()
            return False
        self.current_dxf_path = cached["path"]
        self.loaded_dxf_floor = floor
        self.dxf_scene.set_content(cached["path"], cached["entities"], cached["bounds"])
        return True

    def request_floor_dxf_load(self, floor, force_reload=False, prefetch=False):
        floor = int(floor)
        path = self.get_floor_dxf_path(floor)

        if not path:
            if not prefetch and floor == self.floor_spin.value():
                self.dxf_scene.clear()
                self.current_dxf_path = None
                self.loaded_dxf_floor = None
            return False

        cached = self._dxf_cache.get(floor)
        if (not force_reload) and cached and cached.get("path") == path:
            if not prefetch and floor == self.floor_spin.value():
                self._set_active_dxf_floor(floor)
            return True

        if floor in self._dxf_loading_floors:
            self._update_loading_dialog()
            return False

        self._dxf_loading_floors.add(floor)
        self._start_loading_batch([floor])

        if not prefetch and floor == self.floor_spin.value():
            self.set_status(f"Loading DXF for floor {floor}...")

        self._request_dxf_batch_load.emit([(floor, path)])
        self._update_loading_dialog()
        return False

    def ensure_floor_dxf_loaded(self, floor, force_reload=False):
        floor = int(floor)
        path = self.get_floor_dxf_path(floor)
        if not path:
            if floor == self.floor_spin.value():
                self.dxf_scene.clear()
                self.current_dxf_path = None
                self.loaded_dxf_floor = None
            return False
        cached = self._dxf_cache.get(floor)
        if (not force_reload) and cached and cached.get("path") == path:
            return self._set_active_dxf_floor(floor)
        self.request_floor_dxf_load(floor, force_reload=force_reload, prefetch=False)
        return False

    @Slot(int, str, object, object)
    def _on_dxf_loaded(self, floor, path, entities, bounds):
        floor = int(floor)
        self._dxf_loading_floors.discard(floor)
        self._loading_batch_failed.discard(floor)
        self._dxf_cache[floor] = {"path": path, "entities": entities, "bounds": bounds}
        if floor == self.floor_spin.value():
            self._set_active_dxf_floor(floor)
            self.refresh_canvas()
            if self._pending_fit_after_load:
                self._pending_fit_after_load = False
                self.fit_view()
        self._update_loading_dialog()

    @Slot(int, str, str)
    def _on_dxf_failed(self, floor, path, message):
        self._dxf_loading_floors.discard(int(floor))
        self._loading_batch_failed.add(int(floor))
        if int(floor) == self.floor_spin.value():
            self.set_status(f"Failed to load DXF for floor {floor}: {message}")
        self._update_loading_dialog()

    def world_to_scene(self, x, y):
        return QPointF(float(x), -float(y))

    def scene_to_world(self, sx, sy):
        return float(sx), -float(sy)

    def snap(self, x, y):
        if self.snap_check.isChecked():
            return round(x), round(y)
        return round(x, 3), round(y, 3)

    def _content_bounds(self, floor):
        bounds = []
        if self.dxf_scene.bounds and self.loaded_dxf_floor == int(floor):
            bounds.append(self.dxf_scene.bounds)
        floor_points = self.store.points_for_floor(floor)
        floor_departments = self.store.departments_for_floor(floor)
        if floor_departments:
            xs = [float(p["x"]) for p in floor_departments.values()]
            ys = [float(p["y"]) for p in floor_departments.values()]
            bounds.append((min(xs), min(ys), max(xs), max(ys)))
        if floor_points:
            xs = [float(p["x"]) for p in floor_points.values()]
            ys = [float(p["y"]) for p in floor_points.values()]
            bounds.append((min(xs), min(ys), max(xs), max(ys)))
        if not bounds:
            return None
        min_x = min(b[0] for b in bounds)
        min_y = min(b[1] for b in bounds)
        max_x = max(b[2] for b in bounds)
        max_y = max(b[3] for b in bounds)
        return min_x, min_y, max_x, max_y

    def _scene_rect_for_floor(self, floor, padding=8.0):
        bounds = self._content_bounds(floor)
        if not bounds:
            return None
        min_x, min_y, max_x, max_y = bounds
        return QRectF(
            min_x - padding,
            -(max_y + padding),
            max(1.0, (max_x - min_x) + (padding * 2)),
            max(1.0, (max_y - min_y) + (padding * 2)),
        )

    def fit_view(self):
        floor = self.floor_spin.value()
        ready = self.ensure_floor_dxf_loaded(floor)
        rect = self._scene_rect_for_floor(floor, padding=8.0)
        if rect is None and not ready and self.get_floor_dxf_path(floor):
            self._pending_fit_after_load = True
            return
        if (
            rect is not None
            and not rect.isNull()
            and rect.width() > 0
            and rect.height() > 0
        ):
            self.canvas.resetTransform()
            self.canvas.fitInView(rect, Qt.KeepAspectRatio)
            self.scene.setSceneRect(rect.adjusted(-40, -40, 40, 40))
            self.canvas.viewport().update()
        self.refresh_canvas()

    def _clear_canvas_multi_selection(self):
        self.selected_template_names.clear()

    def _eligible_template_name_set(self, floor):
        return set(self.template_copy_candidate_names(floor))

    def _set_canvas_multi_selection(self, names, append=False, toggle=False):
        floor = self.floor_spin.value()
        eligible = self._eligible_template_name_set(floor)
        filtered = {
            str(name).strip() for name in (names or []) if str(name).strip() in eligible
        }

        if toggle:
            for name in filtered:
                if name in self.selected_template_names:
                    self.selected_template_names.discard(name)
                else:
                    self.selected_template_names.add(name)
        elif append:
            self.selected_template_names.update(filtered)
        else:
            self.selected_template_names = set(filtered)

        if self.selected_template_names:
            self.selected_point_name = next(iter(sorted(self.selected_template_names)))
        elif self.selected_point_name in eligible:
            self.selected_point_name = None

    def _begin_selection_rect(self, event):
        origin = event.position().toPoint()
        self.selection_rect_active = True
        self.selection_rect_origin = origin
        self.selection_rect_current = origin
        if self._rubber_band is not None:
            self._rubber_band.setGeometry(QRect(origin, origin))
            self._rubber_band.show()

    def _update_selection_rect(self, event):
        if not self.selection_rect_active or self.selection_rect_origin is None:
            return
        self.selection_rect_current = event.position().toPoint()
        if self._rubber_band is not None:
            rect = QRect(
                self.selection_rect_origin, self.selection_rect_current
            ).normalized()
            self._rubber_band.setGeometry(rect)

    def _finish_selection_rect(self, event):
        if not self.selection_rect_active or self.selection_rect_origin is None:
            return False

        end_point = event.position().toPoint()
        rect = QRect(self.selection_rect_origin, end_point).normalized()
        if self._rubber_band is not None:
            self._rubber_band.hide()

        self.selection_rect_active = False
        self.selection_rect_current = None
        self.selection_rect_origin = None

        if rect.width() < 4 and rect.height() < 4:
            return False

        scene_rect = self.canvas.mapToScene(rect).boundingRect()
        floor = self.floor_spin.value()
        selected = []

        for name, point in self.store.points_for_floor(floor).items():
            if point.get("kind") not in {"corridor_node", "data_point"}:
                continue

            if not self._is_point_kind_visible(point):
                continue

            pos = self.world_to_scene(point["x"], point["y"])
            if scene_rect.contains(pos):
                selected.append(name)

        modifiers = Qt.KeyboardModifiers(QApplication.keyboardModifiers())
        append = bool(modifiers & Qt.ShiftModifier)
        toggle = bool(modifiers & Qt.ControlModifier)
        self._set_canvas_multi_selection(selected, append=append, toggle=toggle)
        self.refresh_canvas()
        if selected:
            self.set_status(f"Selected {len(selected)} template item(s)")
        else:
            self.set_status("No template items found in selection box")
        return True

    def refresh_canvas(self):
        self._unconnected_cache = self._unconnected_data_point_names()
        self._scene_label_positions = []
        self.scene.clear()
        self._item_lookup = {}
        self._point_item_lookup = {}
        floor = self.floor_spin.value()
        self.ensure_floor_dxf_loaded(floor)
        self.scene.setBackgroundBrush(QBrush(QColor("#111111")))
        rect = self._scene_rect_for_floor(floor, padding=8.0)
        if rect is not None:
            self.scene.setSceneRect(rect.adjusted(-40, -40, 40, 40))
        if (
            self.show_dxf_check.isChecked()
            and self.loaded_dxf_floor == int(floor)
            and self.dxf_scene.entities
        ):
            self.dxf_scene.populate_graphics_scene(
                self.scene, self.canvas.transform().m11()
            )
        self.draw_edges(floor)
        self.draw_departments(floor)
        self.draw_points(floor)
        self.file_label.setText(self.current_json_path or "New file")
        self.canvas.viewport().update()

    def draw_edges(self, floor):
        if not self.show_edges_check.isChecked():
            return
        points = self.store.all_points()
        pen_same_floor = QPen(QColor("#6aa9ff"), 0)
        pen_cross_floor = QPen(QColor("#ff4d4f"), 0)
        for edge in self.store.data.get("corridors", {}).get("edges", []):
            a = points.get(edge["from"])
            b = points.get(edge["to"])
            if not a or not b:
                continue
            a_floor = int(a["floor"])
            b_floor = int(b["floor"])
            if int(floor) not in {a_floor, b_floor}:
                continue
            pa = self.world_to_scene(a["x"], a["y"])
            pb = self.world_to_scene(b["x"], b["y"])
            pen = pen_cross_floor if a_floor != b_floor else pen_same_floor
            item = self.scene.addLine(pa.x(), pa.y(), pb.x(), pb.y(), pen)
            self._item_lookup[item] = ("edge", edge)

    def _next_scene_label_pos(self, label, base_x, base_y):
        if not hasattr(self, "_scene_label_positions"):
            self._scene_label_positions = []

        scale = float(label.scale() or 1.0)
        text_rect = label.boundingRect()
        text_w = text_rect.width() * scale
        text_h = text_rect.height() * scale

        gap_x = 0.45
        gap_y = 0.35

        candidates = [
            (base_x + gap_x, base_y - gap_y),
            (base_x + gap_x, base_y + gap_y),
            (base_x - text_w - gap_x, base_y + gap_y),
            (base_x - text_w - gap_x, base_y - gap_y),
            (base_x + gap_x * 2, base_y - gap_y * 2),
            (base_x + gap_x * 2, base_y + gap_y * 2),
            (base_x - text_w - gap_x * 2, base_y + gap_y * 2),
            (base_x - text_w - gap_x * 2, base_y - gap_y * 2),
            (base_x - text_w / 2, base_y - text_h - gap_y),
            (base_x - text_w / 2, base_y + gap_y),
        ]

        min_dist = 3

        for x, y in candidates:
            if all(
                math.hypot(x - px, y - py) >= min_dist
                for px, py in self._scene_label_positions
            ):
                self._scene_label_positions.append((x, y))
                return x, y

        x = base_x + gap_x
        y = base_y - gap_y
        self._scene_label_positions.append((x, y))
        return x, y

    def draw_departments(self, floor):
        for department_id, dept in self.store.departments_for_floor(floor).items():
            pos = self.world_to_scene(dept["x"], dept["y"])
            selected = department_id == self.selected_point_name

            poly = QPolygonF(
                [
                    QPointF(pos.x(), pos.y() - 0.7),
                    QPointF(pos.x() + 0.7, pos.y()),
                    QPointF(pos.x(), pos.y() + 0.7),
                    QPointF(pos.x() - 0.7, pos.y()),
                ]
            )
            item = QGraphicsPolygonItem(poly)
            item.setBrush(QBrush(QColor("#1abc9c")))
            item.setPen(
                QPen(QColor("#ffffff") if selected else QColor("#8ef3df"), 0.08)
            )
            self.scene.addItem(item)
            self._item_lookup[item] = ("department", department_id)

            if self.show_labels_check.isChecked():
                label = self.scene.addText(str(dept.get("name") or department_id))
                label.setDefaultTextColor(QColor("#aaf7ea"))
                label.setScale(0.08)
                lx, ly = self._next_scene_label_pos(label, pos.x(), pos.y())
                label.setPos(lx, ly)
                self._item_lookup[label] = ("department_label", department_id)

    def draw_points(self, floor):
        connected_data_points = self._connected_data_point_names()
        hide_connected = self.hide_connected_data_points_check.isChecked()
        for name, point in self.store.points_for_floor(floor).items():
            pos = self.world_to_scene(point["x"], point["y"])
            selected = (name == self.selected_point_name) or (
                name in self.selected_template_names
            )
            kind = point.get("kind")

            if kind == "corridor_node" and not self.show_nodes_check.isChecked():
                continue

            if kind == "data_point" and not self.show_data_points_check.isChecked():
                continue

            if (
                kind == "data_point"
                and hide_connected
                and name in connected_data_points
            ):
                continue

            if kind == "location" and not self.show_locations_check.isChecked():
                continue

            if kind == "comms_room" and not self.show_comms_rooms_check.isChecked():
                continue

            outline = QPen(QColor("#ffffff") if selected else QColor("transparent"), 0)

            if kind in {"location", "comms_room"}:
                r = 0.3
                item = self.scene.addEllipse(
                    pos.x() - r,
                    pos.y() - r,
                    2 * r,
                    2 * r,
                    outline,
                    QBrush(QColor("#18c37e")),
                )
                label_color = QColor("#9bf0cd")
            elif kind == "corridor_node":
                r = 0.3
                item = self.scene.addRect(
                    pos.x() - r,
                    pos.y() - r,
                    2 * r,
                    2 * r,
                    outline,
                    QBrush(
                        QColor("#ff6b6b")
                        if point.get("restricted", False)
                        else QColor("#f2c94c")
                    ),
                )
                label_color = QColor("#ffe8a3")
            elif kind == "data_point":
                r = 0.45
                poly = QPolygonF(
                    [
                        QPointF(pos.x(), pos.y() - r),
                        QPointF(pos.x() + r, pos.y()),
                        QPointF(pos.x(), pos.y() + r),
                        QPointF(pos.x() - r, pos.y()),
                    ]
                )

                unconnected = getattr(self, "_unconnected_cache", set())
                item = QGraphicsPolygonItem(poly)
                brush = QBrush(QColor("#b07cff"))
                if name in unconnected:
                    brush = QBrush(QColor("#FFC561"))  # orange

                else:
                    brush = QBrush(QColor("#b07cff"))

                item.setBrush(brush)
                item.setPen(
                    QPen(QColor("#ffffff") if selected else QColor("#d5bbff"), 0.08)
                )
                self.scene.addItem(item)
                label_color = QColor("#eadcff")
            else:
                poly = QPolygonF(
                    [
                        QPointF(pos.x(), pos.y() - 0.5),
                        QPointF(pos.x() + 0.5, pos.y()),
                        QPointF(pos.x(), pos.y() + 0.5),
                        QPointF(pos.x() - 0.5, pos.y()),
                    ]
                )
                item = QGraphicsPolygonItem(poly)
                item.setBrush(QBrush(QColor("#ff7b72")))
                item.setPen(
                    QPen(QColor("#ffffff") if selected else QColor("#ffb3ae"), 0.08)
                )
                self.scene.addItem(item)
                label_color = QColor("#ffb3ae")

            item.setFlag(QGraphicsItem.ItemIgnoresTransformations, False)
            self._item_lookup[item] = ("point", name)
            self._point_item_lookup[name] = item

            if self.show_labels_check.isChecked():
                label = self.scene.addText(name)
                label.setDefaultTextColor(label_color)
                label.setScale(0.05)
                lx, ly = self._next_scene_label_pos(label, pos.x(), pos.y())
                label.setPos(lx, ly)
                self._item_lookup[label] = ("label", name)

    def _edge_rows_for_point(self, point_name):
        points = self.store.all_points()
        results = []
        for edge in self.store.data.get("corridors", {}).get("edges", []):
            if edge.get("from") != point_name and edge.get("to") != point_name:
                continue
            from_name = edge.get("from", "")
            to_name = edge.get("to", "")
            from_point = points.get(from_name)
            to_point = points.get(to_name)
            from_floor = from_point.get("floor", "") if from_point else ""
            to_floor = to_point.get("floor", "") if to_point else ""
            results.append(
                {
                    "from": from_name,
                    "from_floor": from_floor,
                    "to": to_name,
                    "to_floor": to_floor,
                    "cross_floor": (
                        from_point is not None
                        and to_point is not None
                        and int(from_floor) != int(to_floor)
                    ),
                }
            )
        return results

    def _show_edge_connections_dialog(self, point_name):
        rows = self._edge_rows_for_point(point_name)

        def on_delete(selected_edges):
            for edge in selected_edges:
                self.store.remove_edge(edge.get("from"), edge.get("to"))
            self.refresh_canvas()

        dialog = EdgeConnectionsDialog(self, point_name, rows, on_delete)
        dialog.exec()

    def draw_overlay_panels(self, painter, viewport_rect):
        floor = self.floor_spin.value()
        mapped_path = self.get_floor_dxf_path(floor)
        dxf_name = Path(mapped_path).name if mapped_path else "None"
        active_edge_start = self.selected_for_edge or "-"
        lines = [
            "Legend",
            "Green circle = location / comms room",
            "Yellow square = corridor node",
            "Teal diamond = department",
            "Purple diamond = data point",
            "Red diamond = transition",
            f"Mode: {self.mode_combo.currentText()} | Floor: {floor}",
            f"DXF: {dxf_name}",
            f"Edge chain start: {active_edge_start}",
            f"Template selection: {len(self.selected_template_names)}",
            "Drag in select_move to multi-select template items",
            "Double-click a point to edit",
        ]
        self._draw_overlay_box(painter, 12, 12, 330, lines, "#333333", "white")

    def _draw_overlay_box(self, painter, x, y, w, lines, border_color, title_color):
        margin_x = 10
        margin_y = 8
        line_h = 18
        box_h = (margin_y * 2) + (len(lines) * line_h)
        painter.save()
        painter.setPen(QPen(QColor(border_color), 1))
        painter.setBrush(QBrush(QColor("#151515")))
        painter.drawRect(x, y, w, box_h)
        font = QFont()
        font.setPixelSize(12)
        painter.setFont(font)
        for i, line in enumerate(lines):
            painter.setPen(QColor(title_color if i == 0 else "white"))
            painter.drawText(x + margin_x, y + margin_y + 12 + (i * line_h), line)
        painter.restore()

    def _is_point_kind_visible(self, point):
        if not point:
            return False

        kind = str(point.get("kind", "")).strip()

        if kind == "corridor_node":
            return self.show_nodes_check.isChecked()

        if kind == "data_point":
            return self.show_data_points_check.isChecked()

        if kind == "location":
            return self.show_locations_check.isChecked()

        if kind == "comms_room":
            return self.show_comms_rooms_check.isChecked()

        # Keep transitions visible/selectable unless you add a transition checkbox.
        if kind == "transition_node":
            return True

        return True

    def _is_department_visible(self):
        # Change this later if you add a department checkbox.
        return True

    def find_nearest_point_name(self, x, y, floor, radius_world=0.5):
        best = None
        best_dist = radius_world
        for name, point in self.store.points_for_floor(floor).items():
            d = math.hypot(point["x"] - x, point["y"] - y)
            if d <= best_dist:
                best = name
                best_dist = d
        return best

    def _is_alt_pressed(self):
        return bool(QApplication.keyboardModifiers() & Qt.AltModifier)

    def _select_pick_radius(self):
        if self._is_alt_pressed():
            return 0.35
        return 3.0

    def find_nearest_selectable_name(self, x, y, floor, radius_world=3.0):
        best = None
        best_dist = radius_world

        for name, point in self.store.points_for_floor(floor).items():
            if not self._is_point_kind_visible(point):
                continue

            d = math.hypot(point["x"] - x, point["y"] - y)
            if d <= best_dist:
                best = name
                best_dist = d

        if self._is_department_visible():
            for department_id, dept in self.store.departments_for_floor(floor).items():
                d = math.hypot(dept["x"] - x, dept["y"] - y)
                if d <= best_dist:
                    best = department_id
                    best_dist = d

        return best

    def build_floor_map(self, store):
        floor_map = {}
        for item in store.get("locations", []):
            floor_map[item["name"]] = int(item["floor"])
        for item in store.get("data_points", []):
            floor_map[item["name"]] = int(item["floor"])
        for item in store.get("corridors", {}).get("nodes", []):
            floor_map[item["name"]] = int(item["floor"])
        for transition in store.get("transitions", []):
            for floor_str in transition.get("floor_locations", {}).keys():
                floor_map[f"{transition['id']}-F{floor_str}"] = int(floor_str)
        return floor_map

    def department_options(self):
        options = []
        for item in self.store.data.get("departments", []):
            department_id = str(item.get("id", "")).strip()
            if not department_id:
                continue
            options.append(
                (department_id, str(item.get("name", department_id)).strip())
            )
        return sorted(options, key=lambda x: x[0])

    def _data_point_department_ids(self, item):
        if isinstance(item.get("department_ids"), list):
            return [
                str(x).strip() for x in item.get("department_ids", []) if str(x).strip()
            ]
        legacy = str(item.get("department_id", "")).strip()
        return [legacy] if legacy else []

    def department_data_point_names(self, department_id):
        department_id = str(department_id).strip()
        result = []
        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            if department_id in self._data_point_department_ids(item):
                result.append(name)
        return sorted(set(result))

    def _comms_prefix_for_kind(self, kind):
            kind = str(kind or "").strip()
            if kind == "distributed_equipment_room":
                return "DER"
            return "CR"

    def suggest_next_comms_room_name(self, floor, kind="comms_room"):
        prefix = self._comms_prefix_for_kind(kind)
        floor = int(floor)
        pattern = re.compile(rf"^{re.escape(prefix)}(\d+)-F{floor}$", re.IGNORECASE)

        nums = []
        for item in self.store.data.get("locations", []):
            if str(item.get("kind", "")) not in {"comms_room", "distributed_equipment_room"}:
                continue

            name = str(item.get("name", "")).strip()
            match = pattern.match(name)
            if match:
                nums.append(int(match.group(1)))

        return f"{prefix}{max(nums, default=0) + 1}-F{floor}"

    def _distance_between_points(self, a, b):
        if int(a["floor"]) == int(b["floor"]):
            return math.hypot(
                float(a["x"]) - float(b["x"]), float(a["y"]) - float(b["y"])
            )

        floor_height_m = float(
            self.store.data.get("building", {}).get("floor_height_m", 4.0)
        )
        horizontal = math.hypot(
            float(a["x"]) - float(b["x"]), float(a["y"]) - float(b["y"])
        )
        vertical = abs(int(a["floor"]) - int(b["floor"])) * floor_height_m
        return horizontal + vertical

    def _build_routing_graph(self):
        points = self.store.all_points()
        graph = {name: [] for name in points.keys()}

        for edge in self.store.data.get("corridors", {}).get("edges", []):
            a_name = edge.get("from")
            b_name = edge.get("to")
            a = points.get(a_name)
            b = points.get(b_name)
            if not a or not b:
                continue

            weight = self._distance_between_points(a, b)

            graph.setdefault(a_name, []).append((b_name, weight))
            graph.setdefault(b_name, []).append((a_name, weight))

        floor_height_m = float(
            self.store.data.get("building", {}).get("floor_height_m", 4.0)
        )
        for transition in self.store.data.get("transitions", []):
            floor_locations = transition.get("floor_locations", {})
            names = []
            for floor_str in floor_locations.keys():
                node_name = f"{transition['id']}-F{floor_str}"
                if node_name in points:
                    names.append(node_name)

            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    a_name = names[i]
                    b_name = names[j]
                    a = points[a_name]
                    b = points[b_name]

                    vertical = abs(int(a["floor"]) - int(b["floor"])) * floor_height_m
                    graph.setdefault(a_name, []).append((b_name, vertical))
                    graph.setdefault(b_name, []).append((a_name, vertical))

        return graph, points

    def _routing_anchor_names(self):
        points = self.store.all_points()
        connected_names = set()

        for edge in self.store.data.get("corridors", {}).get("edges", []):
            a_name = str(edge.get("from", "")).strip()
            b_name = str(edge.get("to", "")).strip()
            if a_name in points:
                connected_names.add(a_name)
            if b_name in points:
                connected_names.add(b_name)

        result = []
        for name in connected_names:
            point = points.get(name)
            if not point:
                continue
            kind = str(point.get("kind", "")).strip()
            if kind in {"corridor_node", "transition_node"}:
                result.append(name)

        return sorted(set(result))

    def _nearest_routing_anchor_for_point(self, point_name):
        points = self.store.all_points()
        source = points.get(point_name)
        if not source:
            return None, None

        best_name = None
        best_dist = None

        for anchor_name in self._routing_anchor_names():
            if anchor_name == point_name:
                continue
            anchor = points.get(anchor_name)
            if not anchor:
                continue
            if int(anchor["floor"]) != int(source["floor"]):
                continue

            dist = math.hypot(
                float(source["x"]) - float(anchor["x"]),
                float(source["y"]) - float(anchor["y"]),
            )
            if best_dist is None or dist < best_dist:
                best_name = anchor_name
                best_dist = dist

        return best_name, best_dist

    def _shortest_path_length(self, graph, start, end):
        if start not in graph or end not in graph:
            return None, []

        heap = [(0.0, start, [start])]
        best = {}

        while heap:
            cost, node, path = heapq.heappop(heap)

            if node in best and cost >= best[node]:
                continue
            best[node] = cost

            if node == end:
                return cost, path

            for next_node, weight in graph.get(node, []):
                new_cost = cost + float(weight)
                if next_node not in best or new_cost < best[next_node]:
                    heapq.heappush(heap, (new_cost, next_node, path + [next_node]))

        return None, []

    def data_point_options(self):
        result = []

        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            result.append(
                {
                    "name": name,
                    "floor": int(item.get("floor", 0)),
                    "qty": int(item.get("qty", 1) or 1),
                }
            )

        return sorted(result, key=lambda x: (x["floor"], x["name"]))

    def suggest_comms_room_for_department(self):
        if not self.store.data.get("data_points"):
            QMessageBox.critical(self, "Suggest Comms Room", "No data points found.")
            return

        graph, points = self._build_routing_graph()

        data_point_names_set = {
            str(item.get("name", "")).strip()
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
        }

        preselected = sorted(
            name
            for name in self.selected_template_names
            if name in data_point_names_set
        )

        dialog = SuggestCommsRoomDialog(
            self,
            self.data_point_options(),
            default_name=self.suggest_next_comms_room_name(self.floor_spin.value()),
            selected_data_points=preselected,
        )

        if dialog.exec() != QDialog.Accepted or not dialog.result:
            return

        data_point_names = [
            str(x).strip()
            for x in dialog.result.get("data_point_names", [])
            if str(x).strip()
        ]

        max_cable_length_m = float(dialog.result["max_cable_length_m"])
        room_name = dialog.result["room_name"].strip()
        search_mode = str(
            dialog.result.get("search_mode", "Graph route length")
        ).strip()
        use_xy_distance = search_mode == "XY straight-line distance"

        if room_name in self.store.names_in_use():
            QMessageBox.critical(self, "Suggest Comms Room", "Name already exists.")
            return

        if not data_point_names:
            QMessageBox.critical(
                self,
                "Suggest Comms Room",
                "No data points selected.",
            )
            return

        candidate_nodes = self._candidate_comms_room_nodes()
        if not candidate_nodes:
            QMessageBox.critical(
                self,
                "Suggest Comms Room",
                "No corridor nodes available.",
            )
            return

        best_candidate = None
        best_total = None
        best_max = None

        rejected_no_anchor = set()
        rejected_no_path = set()
        rejected_over_limit = {}

        for candidate_name in candidate_nodes:
            if candidate_name not in points:
                continue

            total_length = 0.0
            max_length = 0.0
            valid = True

            for point_name in data_point_names:
                if point_name not in points:
                    valid = False
                    rejected_no_anchor.add(point_name)
                    break

                if use_xy_distance:
                    candidate_point = points.get(candidate_name)
                    data_point = points.get(point_name)

                    if not candidate_point or not data_point:
                        valid = False
                        rejected_no_anchor.add(point_name)
                        break

                    if int(candidate_point.get("floor", 0)) != int(
                        data_point.get("floor", 0)
                    ):
                        valid = False
                        rejected_no_path.add(f"{point_name} -> {candidate_name}")
                        break

                    route_length = 0.0
                    spur_length = math.hypot(
                        float(data_point["x"]) - float(candidate_point["x"]),
                        float(data_point["y"]) - float(candidate_point["y"]),
                    )

                else:
                    anchor_name, spur_length = self._nearest_routing_anchor_for_point(
                        point_name
                    )

                    if anchor_name is None or spur_length is None:
                        valid = False
                        rejected_no_anchor.add(point_name)
                        break

                    if anchor_name == candidate_name:
                        route_length = 0.0
                    else:
                        route_length, _route_path = self._shortest_path_length(
                            graph,
                            anchor_name,
                            candidate_name,
                        )

                        if route_length is None:
                            valid = False
                            rejected_no_path.add(f"{point_name} -> {candidate_name}")
                            break

                extension = float(
                    points[point_name].get("extension_distance_m", 0.0) or 0.0
                )
                cable_length = float(spur_length) + float(route_length) + extension

                if cable_length > max_cable_length_m:
                    valid = False
                    rejected_over_limit.setdefault(candidate_name, []).append(
                        (point_name, cable_length)
                    )
                    break

                total_length += cable_length
                max_length = max(max_length, cable_length)

            if not valid:
                continue

            if (
                best_candidate is None
                or total_length < best_total
                or (math.isclose(total_length, best_total) and max_length < best_max)
            ):
                best_candidate = candidate_name
                best_total = total_length
                best_max = max_length

        if best_candidate is None:
            debug_lines = [
                "No valid corridor node found.",
                f"Data points considered: {len(data_point_names)}",
                f"Candidate nodes considered: {len(candidate_nodes)}",
            ]

            if rejected_no_anchor:
                debug_lines.append(
                    "No routing anchor for: "
                    + ", ".join(sorted(rejected_no_anchor)[:10])
                )

            if rejected_no_path:
                debug_lines.append(
                    "No path examples: " + ", ".join(sorted(rejected_no_path)[:5])
                )

            if rejected_over_limit:
                sample_candidate = next(iter(rejected_over_limit.keys()))
                samples = rejected_over_limit[sample_candidate][:3]
                debug_lines.append(
                    f"Over limit at {sample_candidate}: "
                    + ", ".join(f"{name} ({length:.2f} m)" for name, length in samples)
                )

            QMessageBox.information(
                self,
                "Suggest Comms Room",
                "\n".join(debug_lines),
            )
            return

        candidate_point = points[best_candidate]

        self.push_undo_state("Suggest comms room")

        offset_radius = 1.5
        angle = math.radians(45)

        room_x = float(candidate_point["x"]) + (math.cos(angle) * offset_radius)
        room_y = float(candidate_point["y"]) + (math.sin(angle) * offset_radius)

        self.store.add_location(
            room_name,
            int(candidate_point["floor"]),
            room_x,
            room_y,
            kind="comms_room",
        )

        self._safe_add_same_floor_edge(room_name, best_candidate)

        self.selected_point_name = room_name
        self.refresh_canvas()

        QMessageBox.information(
            self,
            "Suggest Comms Room",
            "\n".join(
                [
                    f"Placed comms room {room_name}",
                    f"Candidate node: {best_candidate}",
                    f"Data points: {len(data_point_names)}",
                    f"Floor: {candidate_point['floor']}",
                    f"Total cable length: {best_total:.2f} m",
                    f"Longest single cable: {best_max:.2f} m",
                    f"Search mode: {search_mode}",
                ]
            ),
        )

        self.set_status(
            f"Placed suggested comms room {room_name} at {best_candidate} "
            f"for {len(data_point_names)} data point(s)"
        )

    def departments_data_point_names(self, department_ids):
        wanted = {
            str(department_id).strip()
            for department_id in department_ids
            if str(department_id).strip()
        }
        result = []

        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            point_departments = set(self._data_point_department_ids(item))
            if wanted & point_departments:
                result.append(name)

        return sorted(set(result))

    def _candidate_comms_room_nodes(self, include_restricted=False):
        result = []

        for item in self.store.data.get("corridors", {}).get("nodes", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            if bool(item.get("restricted", False)) and not include_restricted:
                continue

            result.append(name)

        return sorted(result)

    def comms_room_names(self):
        result = []
        for item in self.store.data.get("locations", []):
            if str(item.get("kind", "location")) in {"comms_room", "distributed_equipment_room"}:
                name = str(item.get("name", "")).strip()
                if name:
                    result.append(name)
        return sorted(set(result))

    def data_point_names(self):
        result = []
        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if name:
                result.append(name)
        return sorted(set(result))

    def _distance_between_points(self, a, b):
        if int(a["floor"]) == int(b["floor"]):
            return math.hypot(
                float(a["x"]) - float(b["x"]), float(a["y"]) - float(b["y"])
            )

        floor_height_m = float(
            self.store.data.get("building", {}).get("floor_height_m", 4.0)
        )
        horizontal = math.hypot(
            float(a["x"]) - float(b["x"]), float(a["y"]) - float(b["y"])
        )
        vertical = abs(int(a["floor"]) - int(b["floor"])) * floor_height_m
        return horizontal + vertical

    def _build_routing_graph(self):
        points = self.store.all_points()
        graph = {name: [] for name in points.keys()}

        # Existing corridor edges
        for edge in self.store.data.get("corridors", {}).get("edges", []):
            a_name = edge.get("from")
            b_name = edge.get("to")
            a = points.get(a_name)
            b = points.get(b_name)
            if not a or not b:
                continue

            weight = self._distance_between_points(a, b)

            # Treat as undirected for cable routing.
            graph.setdefault(a_name, []).append((b_name, weight))
            graph.setdefault(b_name, []).append((a_name, weight))

        # Add vertical travel links between floors of the same transition
        floor_height_m = float(
            self.store.data.get("building", {}).get("floor_height_m", 4.0)
        )
        for transition in self.store.data.get("transitions", []):
            floor_locations = transition.get("floor_locations", {})
            names = []
            for floor_str in floor_locations.keys():
                node_name = f"{transition['id']}-F{floor_str}"
                if node_name in points:
                    names.append(node_name)

            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    a_name = names[i]
                    b_name = names[j]
                    a = points[a_name]
                    b = points[b_name]

                    vertical = abs(int(a["floor"]) - int(b["floor"])) * floor_height_m
                    graph.setdefault(a_name, []).append((b_name, vertical))
                    graph.setdefault(b_name, []).append((a_name, vertical))

        return graph, points

    def _shortest_path_length(self, graph, start, end):
        if start not in graph or end not in graph:
            return None, []

        heap = [(0.0, start, [start])]
        best = {}

        while heap:
            cost, node, path = heapq.heappop(heap)

            if node in best and cost >= best[node]:
                continue
            best[node] = cost

            if node == end:
                return cost, path

            for next_node, weight in graph.get(node, []):
                new_cost = cost + float(weight)
                if next_node not in best or new_cost < best[next_node]:
                    heapq.heappush(heap, (new_cost, next_node, path + [next_node]))

        return None, []

    def _existing_connection_targets(self):
        return {
            str(item.get("to", "")).strip()
            for item in self.store.data.get("connections", [])
            if str(item.get("to", "")).strip()
        }

    def autoroute_data_points(self):
        comms_rooms = self.comms_room_names()
        if not comms_rooms:
            QMessageBox.critical(self, "Autoroute", "No comms rooms found.")
            return

        graph, points = self._build_routing_graph()
        existing_targets = self._existing_connection_targets()

        data_points = [
            item
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
            and str(item.get("name", "")).strip() not in existing_targets
        ]

        skipped_existing = len(self.store.data.get("data_points", [])) - len(data_points)

        if not data_points:
            QMessageBox.information(
                self,
                "Autoroute",
                f"No data points to autoroute. {skipped_existing} already had connections.",
            )
            return

        progress = QDialog(self)
        progress.setWindowTitle("Autorouting Data Points")
        progress.setWindowModality(Qt.ApplicationModal)
        progress.setMinimumWidth(460)

        layout = QVBoxLayout(progress)

        message_label = QLabel("Preparing multiprocessing autoroute...")
        message_label.setWordWrap(True)
        layout.addWidget(message_label)

        bar = QProgressBar()
        bar.setRange(0, len(data_points))
        bar.setValue(0)
        layout.addWidget(bar)

        detail_label = QLabel(f"0 / {len(data_points)}")
        layout.addWidget(detail_label)

        cancel_btn = QPushButton("Cancel")
        layout.addWidget(cancel_btn)

        cancelled = {"value": False}

        def cancel_autoroute():
            cancelled["value"] = True
            cancel_btn.setEnabled(False)
            message_label.setText("Cancelling...")

        cancel_btn.clicked.connect(cancel_autoroute)

        progress.show()
        QApplication.processEvents()

        created_results = []
        skipped_unreachable = []

        worker_count = max(1, min(os.cpu_count() or 1, len(data_points)))

        try:
            with ProcessPoolExecutor(
                max_workers=worker_count,
                initializer=_init_autoroute_process,
                initargs=(graph, points, comms_rooms),
            ) as pool:
                futures = [
                    pool.submit(_autoroute_data_point_worker, data_point)
                    for data_point in data_points
                ]

                completed = 0

                for future in as_completed(futures):
                    completed += 1

                    if cancelled["value"]:
                        pool.shutdown(cancel_futures=True)
                        break

                    result = future.result()

                    if result.get("status") == "created":
                        created_results.append(result)
                    elif result.get("status") == "unreachable":
                        skipped_unreachable.append(result.get("point_name", ""))

                    bar.setValue(completed)
                    detail_label.setText(f"{completed} / {len(data_points)}")
                    message_label.setText(
                        f"Autorouting with {worker_count} process(es)..."
                    )
                    QApplication.processEvents()

        except Exception as exc:
            progress.accept()
            QMessageBox.critical(self, "Autoroute failed", str(exc))
            return

        progress.accept()

        if cancelled["value"]:
            QMessageBox.information(
                self,
                "Autoroute Cancelled",
                "Autoroute cancelled. No changes were applied.",
            )
            self.set_status("Autoroute cancelled")
            return

        existing_connection_ids = {
            str(item.get("id", "")).strip()
            for item in self.store.data.get("connections", [])
            if str(item.get("id", "")).strip()
        }

        def next_connection_id():
            n = 1
            while f"C{n}" in existing_connection_ids:
                n += 1
            new_id = f"C{n}"
            existing_connection_ids.add(new_id)
            return new_id

        created_rows = []
        for result in created_results:
            created_rows.append(
                {
                    "id": next_connection_id(),
                    "from": result["from"],
                    "to": result["to"],
                    "qty": result["qty"],
                    "route_profile": result.get("route_profile", ""),
                }
            )

        if created_rows:
            self.push_undo_state("Autoroute data points")
            self.store.data.setdefault("connections", []).extend(created_rows)
            self.set_status(
                f"Autorouted {len(created_rows)} data point(s) using {worker_count} process(es)"
            )

        message_lines = [f"Created {len(created_rows)} connection(s)."]

        if skipped_existing:
            message_lines.append(
                f"Skipped {skipped_existing} already-connected data point(s)."
            )

        if skipped_unreachable:
            message_lines.append(
                "Unreachable data point(s): "
                + ", ".join(skipped_unreachable[:15])
                + (" ..." if len(skipped_unreachable) > 15 else "")
            )

        QMessageBox.information(self, "Autoroute", "\n".join(message_lines))

    def department_options(self):
        options = []
        for item in self.store.data.get("departments", []):
            department_id = str(item.get("id", "")).strip()
            if not department_id:
                continue
            options.append((department_id, str(item.get("name", "")).strip()))
        return sorted(options, key=lambda x: x[0])

    def connection_candidate_names(self):
        used_targets = {
            str(item.get("to", "")).strip()
            for item in self.store.data.get("connections", [])
            if str(item.get("to", "")).strip()
        }

        result = []

        for item in self.store.data.get("locations", []):
            name = str(item.get("name", "")).strip()
            kind = str(item.get("kind", "location") or "location").strip()
            if not name:
                continue
            if kind == "comms_room":
                result.append(name)

        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            if name in used_targets:
                continue
            result.append(name)

        return sorted(set(result))

    def connection_candidate_group_map(self):
        department_name_by_id = {}
        for department in self.store.data.get("departments", []):
            department_id = str(department.get("id", "")).strip()
            if not department_id:
                continue
            department_name_by_id[department_id] = (
                str(department.get("name", department_id)).strip() or department_id
            )

        group_map = {}

        for item in self.store.data.get("locations", []):
            name = str(item.get("name", "")).strip()
            kind = str(item.get("kind", "location") or "location").strip()
            if not name or kind != "comms_room":
                continue
            floor = item.get("floor", "")
            group_map[name] = f"Comms rooms / Floor {floor}"

        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            department_ids = [
                str(x).strip() for x in item.get("department_ids", []) if str(x).strip()
            ]

            if department_ids:
                labels = []
                for department_id in department_ids:
                    labels.append(
                        department_name_by_id.get(department_id, department_id)
                    )
                group_map[name] = "Data points / " + ", ".join(sorted(set(labels)))
            else:
                floor = item.get("floor", "")
                group_map[name] = f"Data points / Unassigned / Floor {floor}"

        return group_map

    def template_copy_candidate_names(self, floor):
        floor = int(floor)
        result = []

        for item in self.store.data.get("corridors", {}).get("nodes", []):
            if int(item.get("floor", 0)) == floor:
                name = str(item.get("name", "")).strip()
                if name:
                    result.append(name)

        for item in self.store.data.get("locations", []):
            if int(item.get("floor", 0)) != floor:
                continue
            kind = str(item.get("kind", "location") or "location").strip()
            if kind in {"comms_room", "distributed_equipment_room"}:
                name = str(item.get("name", "")).strip()
                if name:
                    result.append(name)

        for item in self.store.data.get("data_points", []):
            if int(item.get("floor", 0)) == floor:
                name = str(item.get("name", "")).strip()
                if name:
                    result.append(name)

        return sorted(set(result))

    def template_copy_group_map(self, floor):
        floor = int(floor)
        group_map = {}

        for item in self.store.data.get("corridors", {}).get("nodes", []):
            if int(item.get("floor", 0)) != floor:
                continue
            name = str(item.get("name", "")).strip()
            if name:
                group_map[name] = f"Corridor nodes / Floor {floor}"

        department_name_by_id = {}
        for department in self.store.data.get("departments", []):
            department_id = str(department.get("id", "")).strip()
            if not department_id:
                continue
            department_name_by_id[department_id] = (
                str(department.get("name", department_id)).strip() or department_id
            )

        for item in self.store.data.get("locations", []):
            if int(item.get("floor", 0)) != floor:
                continue

            name = str(item.get("name", "")).strip()
            kind = str(item.get("kind", "location") or "location").strip()

            if not name:
                continue

            if kind == "distributed_equipment_room":
                group_map[name] = f"Distributed equipment rooms / Floor {floor}"
            elif kind == "comms_room":
                group_map[name] = f"Comms rooms / Floor {floor}"

        for item in self.store.data.get("data_points", []):
            if int(item.get("floor", 0)) != floor:
                continue
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            department_ids = [
                str(x).strip() for x in item.get("department_ids", []) if str(x).strip()
            ]
            if department_ids:
                labels = [
                    department_name_by_id.get(dept_id, dept_id)
                    for dept_id in department_ids
                ]
                group_map[name] = "Data points / " + ", ".join(sorted(set(labels)))
            else:
                group_map[name] = f"Data points / Unassigned / Floor {floor}"

        return group_map

    def _format_bulk_location_name(self, prefix, number):
        return f"{prefix}{int(number)}"

    def _next_available_bulk_location_name(self, prefix, start_number):
        number = int(start_number)
        used = self.store.names_in_use()
        while self._format_bulk_location_name(prefix, number) in used:
            number += 1
        return self._format_bulk_location_name(prefix, number), number

    def open_json(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open JSON", "", "JSON files (*.json)"
        )
        if not path:
            return
        self.store = JsonStore.from_file(path)
        self.bulk_location_session = None
        self.bulk_data_point_session = None
        self.current_json_path = path
        self._clear_dxf_cache()
        current_floor = self.floor_spin.value()
        self._pending_fit_after_load = bool(self.get_floor_dxf_path(current_floor))
        self._queue_all_floor_dxf_loads(active_floor=current_floor, force_reload=False)
        self.set_status(f"Opened {Path(path).name}")
        self.refresh_canvas()
        self.fit_view()

    def save_json(self):
        path = self.current_json_path
        if not path:
            path, _ = QFileDialog.getSaveFileName(
                self, "Save JSON", "", "JSON files (*.json)"
            )
        if not path:
            return
        self.store.save(path)
        self.current_json_path = path
        self.set_status(f"Saved {Path(path).name}")
        self.refresh_canvas()

    def save_json_as(self):
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save JSON As",
            self.current_json_path or "",
            "JSON files (*.json)",
        )

        if not path:
            return

        if not path.lower().endswith(".json"):
            path += ".json"

        self.store.save(path)
        self.current_json_path = path
        self.set_status(f"Saved as {Path(path).name}")
        self.refresh_canvas()

    def load_dxf(self):
        floor = self.floor_spin.value()
        initialdir = ""
        existing = self.get_floor_dxf_path(floor)
        if existing:
            try:
                initialdir = str(Path(existing).expanduser().resolve().parent)
            except Exception:
                initialdir = str(Path(existing).expanduser().parent)
        path, _ = QFileDialog.getOpenFileName(
            self, "Select DXF", initialdir, "DXF files (*.dxf)"
        )
        if not path:
            return
        self.set_floor_dxf_path(floor, path)
        self._dxf_cache.pop(int(floor), None)
        if self.loaded_dxf_floor == int(floor):
            self.dxf_scene.clear()
            self.current_dxf_path = None
            self.loaded_dxf_floor = None
        self._pending_fit_after_load = True
        self._queue_all_floor_dxf_loads(active_floor=floor, force_reload=True)
        self.refresh_canvas()
        self.set_status(f"Mapped DXF {Path(path).name} to floor {floor}")

    def clear_floor_dxf(self):
        floor = self.floor_spin.value()
        existing = self.get_floor_dxf_path(floor)
        if not existing:
            self.set_status(f"No DXF mapped to floor {floor}")
            return
        if (
            QMessageBox.question(
                self, "Clear floor DXF", f"Remove DXF mapping for floor {floor}?"
            )
            != QMessageBox.Yes
        ):
            return
        self.clear_floor_dxf_mapping(floor)
        self._dxf_cache.pop(int(floor), None)
        self._dxf_loading_floors.discard(int(floor))
        if self.loaded_dxf_floor == int(floor):
            self.dxf_scene.clear()
            self.current_dxf_path = None
            self.loaded_dxf_floor = None
        self.set_status(f"Removed DXF mapping from floor {floor}")
        self.refresh_canvas()

    def validate_json(self):
        errors = self.store.validate()

        points = self.store.all_points()
        invalid_edges = []

        for edge in self.store.data.get("corridors", {}).get("edges", []):
            from_name = str(edge.get("from", "")).strip()
            to_name = str(edge.get("to", "")).strip()

            from_point = points.get(from_name)
            to_point = points.get(to_name)

            if not from_point or not to_point:
                invalid_edges.append(edge)
                continue

            if int(from_point.get("floor", 0)) != int(to_point.get("floor", 0)):
                invalid_edges.append(edge)

        if invalid_edges:
            if (
                QMessageBox.question(
                    self,
                    "Invalid Edges",
                    (
                        f"Found {len(invalid_edges)} invalid edge(s).\n\n"
                        "Invalid edges include missing endpoints or cross-floor edges.\n"
                        "Cross-floor travel must use transitions.\n\n"
                        "Remove invalid edges now?"
                    ),
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.Yes,
                )
                == QMessageBox.Yes
            ):
                self.push_undo_state("Remove invalid edges")
                self.store.data["corridors"]["edges"] = [
                    edge
                    for edge in self.store.data.get("corridors", {}).get("edges", [])
                    if edge not in invalid_edges
                ]
                self.refresh_canvas()

        errors = self.store.validate()

        if errors:
            QMessageBox.critical(self, "Validation errors", "\n".join(errors[:100]))
            self.set_status(f"Validation failed with {len(errors)} error(s)")
        else:
            QMessageBox.information(
                self, "Validation", "JSON structure is internally consistent."
            )
            self.set_status("Validation passed")

    def manage_locations(self):
        columns = [
            ("name", "Name", 180),
            ("kind", "Kind", 120),
            ("department_ids", "Department IDs", 180),
            ("floor", "Floor", 70),
            ("x", "X", 80),
            ("y", "Y", 80),
        ]
        TableListEditor(
            self,
            "Locations",
            columns,
            self.store.data.get("locations", []),
            self._save_locations,
        )

    def _save_locations(self, items):
        self.push_undo_state("Save locations")
        self.store.data["locations"] = items
        self.set_status("Locations updated")
        self.refresh_canvas()

    def manage_departments(self):
        columns = [
            ("id", "ID", 120),
            ("name", "Name", 180),
            ("floor", "Floor", 70),
            ("x", "X", 80),
            ("y", "Y", 80),
        ]
        TableListEditor(
            self,
            "Departments",
            columns,
            self.store.data.get("departments", []),
            self._save_departments,
        )

    def _save_departments(self, items):
        self.push_undo_state("Save departments")
        self.store.data["departments"] = items
        self.set_status("Departments updated")
        self.refresh_canvas()

    def manage_room_types(self):
        assets_by_id = {
            str(asset.get("id", "")).strip(): asset
            for asset in self.store.data.get("assets", [])
            if str(asset.get("id", "")).strip()
        }

        RoomTypesEditorWindow(
            self,
            self.store.data.get("room_types", []),
            self._save_room_types,
            asset_options=self.store.asset_options(),
            assets_by_id=assets_by_id,
        )

    def _save_room_types(self, items):
        self.push_undo_state("Save room types")
        self.store.data["room_types"] = items

        for point in self.store.data.get("data_points", []):
            name = str(point.get("name", "")).strip()
            if name:
                self.store.sync_connection_qty_for_data_point(name)

        self.set_status("Room types updated and data point quantities recalculated")
        self.refresh_canvas()

    def manage_data_points(self):
        columns = [
            ("name", "Name", 180),
            ("floor", "Floor", 70),
            ("x", "X", 80),
            ("y", "Y", 80),
            ("qty", "Qty", 70),
            ("extension_distance_m", "Extension m", 100),
            ("room_type_id", "Room Type", 120),
        ]
        TableListEditor(
            self,
            "Data Points",
            columns,
            self.store.data.get("data_points", []),
            self._save_data_points,
        )

    def _save_data_points(self, items):
        self.push_undo_state("Save data points")
        self.store.data["data_points"] = items

        for point in self.store.data.get("data_points", []):
            name = str(point.get("name", "")).strip()
            if name:
                self.store.sync_connection_qty_for_data_point(name)

        self.set_status("Data points updated")
        self.refresh_canvas()

    def manage_transitions(self):
        columns = [
            ("id", "ID", 120),
            ("floors", "Floors", 160),
            ("cable_limit", "Cable limit", 100),
            ("floor_locations", "Floor locations", 300),
        ]
        TableListEditor(
            self,
            "Transitions",
            columns,
            self.store.data.get("transitions", []),
            self._save_transitions,
        )

    def _save_transitions(self, items):
        self.push_undo_state("Save transistions")
        self.store.data["transitions"] = items
        self.set_status("Transitions updated")
        self.refresh_canvas()

    def manage_connections(self):
        point_names = self.connection_candidate_names()
        profile_names = [""] + sorted(self.store.data.get("route_profiles", {}).keys())
        floor_map = self.build_floor_map(self.store.data)
        group_map = self.connection_candidate_group_map()
        ConnectionEditorWindow(
            self,
            self.store.data.get("connections", []),
            point_names,
            profile_names,
            self.store.suggest_next_connection_id,
            self._save_connections,
            floor_map=floor_map,
            group_map=group_map,
        )

    def _save_connections(self, items):
        self.push_undo_state("Save connections")
        self.store.data["connections"] = items
        self.set_status("Connections updated")

    def manage_route_profiles(self):
        point_names = set(self.store.names_in_use())
        transition_ids = {x["id"] for x in self.store.data.get("transitions", [])}
        floor_map = self.build_floor_map(self.store.data)
        dialog = RouteProfilesEditorV2(
            self,
            self.store.data.get("route_profiles", {}),
            point_names,
            transition_ids,
            self.store.data.get("corridors", {}).get("edges", []),
            self._save_route_profiles,
            floor_map=floor_map,
        )
        dialog.exec()

    def manage_location_departments(self):
        def group_resolver(name):
            for item in self.store.data.get("locations", []):
                if item.get("name") == name:
                    return f"Floor {item.get('floor', 0)}"
            return "Other"

        dialog = LocationDepartmentsBulkDialog(
            self,
            self.store.data.get("locations", []),
            self.department_options(),
            self._save_locations,
            group_resolver=group_resolver,
        )
        dialog.exec()

    def manage_data_point_departments(self):
        department_name_by_id = {
            str(item.get("id", ""))
            .strip(): str(item.get("name", item.get("id", "")))
            .strip()
            for item in self.store.data.get("departments", [])
            if str(item.get("id", "")).strip()
        }

        def group_resolver(name):
            for item in self.store.data.get("data_points", []):
                if item.get("name") != name:
                    continue

                qty = int(item.get("qty", 1) or 1)
                floor = int(item.get("floor", 0))

                department_ids = [
                    str(x).strip()
                    for x in item.get("department_ids", [])
                    if str(x).strip()
                ]

                if department_ids:
                    labels = [
                        department_name_by_id.get(dept_id, dept_id)
                        for dept_id in department_ids
                    ]
                    return (
                        "Assigned / "
                        + " / ".join(sorted(set(labels), key=lambda x: x.lower()))
                        + f" / Floor {floor} / Qty {qty}"
                    )

                return f"Unassigned / Floor {floor} / Qty {qty}"

            return "Other"

        data_point_names = {
            str(item.get("name", "")).strip()
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
        }

        preselected = sorted(
            name for name in self.selected_template_names if name in data_point_names
        )

        dialog = DataPointDepartmentsBulkDialog(
            self,
            self.store.data.get("data_points", []),
            self.department_options(),
            self._save_data_points,
            group_resolver=group_resolver,
            selected_data_points=preselected,
        )
        dialog.exec()

    def _selected_data_point_names(self):
        data_point_names = {
            str(item.get("name", "")).strip()
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
        }

        return sorted(
            name for name in self.selected_template_names if name in data_point_names
        )

    def assign_room_type_to_selected_data_points(self):
        selected = self._selected_data_point_names()

        if not selected:
            QMessageBox.information(
                self,
                "Assign Room Type",
                "No selected data points found.",
            )
            return

        room_types = self.store.room_type_options()
        if not room_types:
            QMessageBox.information(
                self,
                "Assign Room Type",
                "No room types have been created.",
            )
            return

        labels = ["Manual / no room type"]
        values = [""]

        for room_type_id, room_type_name in room_types:
            labels.append(
                f"{room_type_id} - {room_type_name}"
                if room_type_name
                else room_type_id
            )
            values.append(room_type_id)

        label, ok = QInputDialog.getItem(
            self,
            "Assign Room Type",
            f"Room type for {len(selected)} selected data point(s):",
            labels,
            0,
            False,
        )

        if not ok:
            return

        room_type_id = values[labels.index(label)]
        selected_set = set(selected)

        self.push_undo_state("Assign room type to data points")

        updated = 0
        for point in self.store.data.get("data_points", []):
            name = str(point.get("name", "")).strip()
            if name not in selected_set:
                continue

            point["room_type_id"] = room_type_id

            if room_type_id:
                point["qty"] = self.store.room_type_cable_qty(room_type_id)

            self.store.sync_connection_qty_for_data_point(name)
            updated += 1

        self.set_status(
            f"Assigned room type {room_type_id or 'Manual'} to {updated} data point(s)"
        )
        self.refresh_canvas()

    def update_selected_data_point_qty(self):
        selected = self._selected_data_point_names()

        if not selected:
            QMessageBox.information(
                self,
                "Update Data Point Qty",
                "No selected data points found.",
            )
            return

        qty, ok = QInputDialog.getInt(
            self,
            "Update Data Point Qty",
            f"Set qty for {len(selected)} selected data point(s):",
            1,
            1,
            1000000,
        )

        if not ok:
            return

        selected_set = set(selected)
        updated = 0
        updated_connections = 0

        self.push_undo_state("Update selected data point qty")

        for item in self.store.data.get("data_points", []):
            if str(item.get("name", "")).strip() in selected_set:
                item["qty"] = int(qty)
                updated += 1
        for connection in self.store.data.get("connections", []):
            target = str(connection.get("to", "")).strip()
            if target in selected_set:
                connection["qty"] = int(qty)
                updated_connections += 1

        self.set_status(f"Updated qty to {qty} for {updated} selected data point(s)")
        self.refresh_canvas()

    def start_bulk_location_placement(self):
        floor = self.floor_spin.value()
        dialog = BulkLocationPlacementDialog(
            self,
            default_floor=floor,
            default_prefix=f"L{floor}-",
            default_start_number=1,
            department_options=self.department_options_with_floor(),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            kind = dialog.result["kind"]
            count = int(dialog.result["count"])
            department_ids = list(dialog.result.get("department_ids", []))

            if kind in {"comms_room", "distributed_equipment_room"}:
                prefix = self._comms_prefix_for_kind(kind)
                next_name = self.suggest_next_comms_room_name(floor, kind)
                match = re.match(rf"^{prefix}(\d+)-F{int(floor)}$", next_name)
                next_number = int(match.group(1)) if match else 1
            else:
                prefix = dialog.result["prefix"]
                start_number = int(dialog.result["start_number"])
                next_name, next_number = self._next_available_bulk_location_name(
                    prefix, start_number
                )

            self.bulk_location_session = {
                "prefix": prefix,
                "next_number": next_number,
                "remaining": count,
                "kind": kind,
                "department_ids": department_ids,
            }

            self.mode_combo.setCurrentText("location")
            self.set_status(
                f"Mass create active: {count} {kind} item(s) starting at {next_name}. Click to place."
            )
    def copy_template_between_floors(self):
        source_floor = self.floor_spin.value()
        point_names = self.template_copy_candidate_names(source_floor)
        if not point_names:
            QMessageBox.information(
                self,
                "Copy Template Between Floors",
                "No corridor nodes, comms rooms, or data points found on the current floor.",
            )
            return

        group_map = self.template_copy_group_map(source_floor)

        def group_resolver(name):
            return group_map.get(name, "Other")

        preselected = sorted(
            name for name in self.selected_template_names if name in set(point_names)
        )
        dialog = FloorTemplateCopyDialog(
            self,
            source_floor=source_floor,
            point_names=point_names,
            selected_points=preselected,
            group_resolver=group_resolver,
        )
        if dialog.exec() != QDialog.Accepted or not dialog.result:
            return

        try:
            payload = dialog.result
            result = self.store.clone_template_between_floors(
                source_names=payload["source_names"],
                target_floor=payload["target_floor"],
                include_internal_edges=payload["include_internal_edges"],
                offset_x=payload["offset_x"],
                offset_y=payload["offset_y"],
            )

            created_count = len(result.get("created_corridors", [])) + len(
                result.get("created_data_points", [])
            )
            created_edges = len(result.get("created_edges", []))
            created_count = (
                len(result.get("created_corridors", []))
                + len(result.get("created_locations", []))
                + len(result.get("created_data_points", []))
            )

            self.selected_point_name = None
            self._clear_canvas_multi_selection()
            self.refresh_canvas()

            lines = [
                f"Copied {created_count} item(s) to floor {payload['target_floor']}.",
                f"Corridor nodes created: {len(result.get('created_corridors', []))}",
                f"Data points created: {len(result.get('created_data_points', []))}",
                f"Comms rooms created: {len(result.get('created_locations', []))}",
                f"Edges recreated: {created_edges}",
            ]
            skipped = result.get("skipped", [])
            if skipped:
                lines.append("Skipped: " + ", ".join(skipped[:10]))

            QMessageBox.information(
                self,
                "Copy Template Between Floors",
                "\n".join(lines),
            )
            self.set_status(
                f"Copied template to floor {payload['target_floor']} "
                f"({created_count} items, {created_edges} edges)"
            )
        except Exception as exc:
            QMessageBox.critical(
                self,
                "Copy Template Between Floors",
                str(exc),
            )

    def _save_route_profiles(self, profiles):
        self.push_undo_state("Set route_profiles")
        self.store.data["route_profiles"] = profiles
        self.set_status("Route profiles updated")

    def on_left_click(self, event, sx, sy):
        mode = self.mode_combo.currentText()
        floor = self.floor_spin.value()
        x, y = self.scene_to_world(sx, sy)
        x, y = self.snap(x, y)

        if mode == "pan":
            self.last_pan = event.position().toPoint()
            return

        if mode == "select_move":
            picked = self.find_nearest_selectable_name(
                x,
                y,
                floor,
                radius_world=self._select_pick_radius(),
            )
        else:
            picked = self.find_nearest_selectable_name(x, y, floor)

        if mode == "select_move":
            modifiers = Qt.KeyboardModifiers(QApplication.keyboardModifiers())
            template_eligible = (
                picked in self._eligible_template_name_set(floor) if picked else False
            )

            if picked:
                if template_eligible:
                    if modifiers & Qt.ControlModifier:
                        self._set_canvas_multi_selection([picked], toggle=True)
                    elif modifiers & Qt.ShiftModifier:
                        self._set_canvas_multi_selection([picked], append=True)
                    else:
                        self._set_canvas_multi_selection([picked], append=False)
                else:
                    if not (modifiers & (Qt.ControlModifier | Qt.ShiftModifier)):
                        self._clear_canvas_multi_selection()

                self.dragging_point_name = picked
                self.alt_move_locked = self._is_alt_pressed()
                self.drag_mode_active = True

                if self.alt_move_locked:
                    self.set_status(
                        f"ALT safe select: selected {picked}, movement locked"
                    )
                else:
                    self.set_status(f"Selected {picked}")
                    self.push_undo_state("Move selected point(s)")

                self.refresh_canvas()
                return

            if not (modifiers & (Qt.ControlModifier | Qt.ShiftModifier)):
                self._clear_canvas_multi_selection()
            self._begin_selection_rect(event)
            self.refresh_canvas()
            return

        if mode == "delete":
            if picked:
                department_ids = self.store.department_ids()
                if picked in department_ids:
                    if (
                        QMessageBox.question(
                            self, "Delete department", f"Delete department {picked}?"
                        )
                        == QMessageBox.Yes
                    ):
                        self.push_undo_state("Delete department")
                        self.store.delete_department(picked)
                        self.selected_point_name = None
                        self.set_status(f"Deleted department {picked}")
                elif "-F" in picked and picked.rsplit("-F", 1)[0] in {
                    t["id"] for t in self.store.data.get("transitions", [])
                }:
                    transition_id = picked.rsplit("-F", 1)[0]
                    if (
                        QMessageBox.question(
                            self, "Delete transition", f"Delete entire {transition_id}?"
                        )
                        == QMessageBox.Yes
                    ):
                        self.push_undo_state("Delete transistion")
                        self.store.delete_transition(transition_id)
                        self.selected_point_name = None
                        self.set_status(f"Deleted {transition_id}")
                else:
                    if (
                        QMessageBox.question(self, "Delete point", f"Delete {picked}?")
                        == QMessageBox.Yes
                    ):
                        self.push_undo_state("Delete point")
                        self.store.delete_point(picked)
                        self.selected_point_name = None
                        self.set_status(f"Deleted {picked}")
                self.refresh_canvas()
            return

        if mode == "corridor_node":
            if self.quick_add_corridor_check.isChecked():
                name = self.store.suggest_next_corridor_name(floor)
            else:
                name, ok = QInputDialog.getText(
                    self,
                    "Corridor node",
                    "Node name:",
                    text=self.store.suggest_next_corridor_name(floor),
                )
                if not ok or not name:
                    return
                name = name.strip()

            if name in self.store.names_in_use():
                QMessageBox.critical(self, "Duplicate name", "Name already exists")
                return

            if self.quick_add_corridor_check.isChecked():
                height = float(self.default_corridor_height_spin.value())
                limit = int(self.default_corridor_cable_limit_spin.value())
            else:
                height, ok = QInputDialog.getDouble(
                    self,
                    "Corridor node",
                    "Height AFFL (m):",
                    float(self.default_corridor_height_spin.value()),
                    0.0,
                    100.0,
                    2,
                )
                if not ok:
                    return
                limit, ok = QInputDialog.getInt(
                    self,
                    "Corridor node",
                    "Cable limit:",
                    int(self.default_corridor_cable_limit_spin.value()),
                    0,
                    1000000,
                )
                if not ok:
                    return
            self.push_undo_state("Add corridor node")
            self.store.add_corridor_node(name, floor, x, y, height, limit)

            for item in self.store.data.get("corridors", {}).get("nodes", []):
                if item.get("name") == name:
                    item["restricted"] = False
                    break
            self.set_status(f"Added corridor node {name}")
            self.refresh_canvas()
            return

        if mode == "location":
            if self.bulk_location_session:
                session = self.bulk_location_session
                if session["kind"] in {"comms_room", "distributed_equipment_room"}:
                    name, number = self._next_comms_room_name(
                        self.store.names_in_use(),
                        floor,
                        session["next_number"],
                        session["kind"],
                    )
                else:
                    name, number = self._next_available_bulk_location_name(
                        session["prefix"],
                        session["next_number"],
                    )
                self.push_undo_state("Add location")
                self.store.add_location(
                    name,
                    floor,
                    x,
                    y,
                    kind=session["kind"],
                    department_ids=list(session.get("department_ids", [])),
                )

                session["next_number"] = number
                session["remaining"] -= 1
                self.selected_point_name = name
                self.refresh_canvas()

                if session["remaining"] <= 0:
                    self.bulk_location_session = None
                    self.set_status(f"Mass create complete. Added {name}")
                else:
                    upcoming_name, _ = self._next_available_bulk_location_name(
                        session["prefix"],
                        session["next_number"],
                    )
                    self.set_status(
                        f"Added {session['kind']} {name}. "
                        f"{session['remaining']} remaining. Next: {upcoming_name}"
                    )
                return

            name, ok = QInputDialog.getText(self, "Location", "Location name:")
            if not ok or not name:
                return
            name = name.strip()
            if name in self.store.names_in_use():
                QMessageBox.critical(self, "Duplicate name", "Name already exists")
                return
            kind, ok = QInputDialog.getItem(
                self, "Location kind", "Kind:", ["location", "comms_room"], 0, False
            )
            if not ok:
                return
            self.store.add_location(name, floor, x, y, kind=kind, department_ids=[])
            self.set_status(f"Added {kind} {name}")
            self.refresh_canvas()
            return

        if mode == "department":
            suggested_id = self.store.suggest_next_department_id()
            department_name, ok = QInputDialog.getText(
                self,
                "Department",
                "Department name:",
                text=f"Department {suggested_id}",
            )
            if not ok or not department_name.strip():
                return

            department_id = self.store.add_department(
                department_name.strip(),
                floor,
                x,
                y,
            )
            self.selected_point_name = department_id
            self.set_status(f"Added department {department_id}")
            self.refresh_canvas()
            return

        if mode == "data_point":
            if self.bulk_data_point_session:
                session = self.bulk_data_point_session
                name, number = self._next_available_bulk_data_point_name(
                    session["prefix"],
                    session["next_number"],
                )
                self.push_undo_state("Add data point")
                self.store.add_data_point(
                    name,
                    floor,
                    x,
                    y,
                    session["qty"],
                    session["extension_distance_m"],
                    dialog.result["room_type_id"],
                )

                session["next_number"] = number + 1
                session["remaining"] -= 1
                self.selected_point_name = name
                self.refresh_canvas()

                if session["remaining"] <= 0:
                    self.bulk_data_point_session = None
                    self.set_status(f"Mass create complete. Added data point {name}")
                else:
                    upcoming_name, _ = self._next_available_bulk_data_point_name(
                        session["prefix"],
                        session["next_number"],
                    )
                    self.set_status(
                        f"Added data point {name}. "
                        f"{session['remaining']} remaining. Next: {upcoming_name}"
                    )
                return

            dialog = DataPointEditorDialog(
                self,
                seed={"floor": floor, "department_ids": []},
                default_floor=floor,
                default_x=x,
                default_y=y,
                default_name=self.store.suggest_next_data_point_name(floor),
                department_options=self.department_options(),
                room_type_options=self.store.room_type_options(),
            )
            if dialog.exec() == QDialog.Accepted and dialog.result:
                if dialog.result["name"] in self.store.names_in_use():
                    QMessageBox.critical(self, "Duplicate name", "Name already exists")
                    return
                self.store.add_data_point(
                    dialog.result["name"],
                    floor,
                    dialog.result["x"],
                    dialog.result["y"],
                    dialog.result["qty"],
                    dialog.result["extension_distance_m"],
                    department_ids=dialog.result.get("department_ids", []),
                    room_type_id=dialog.result["room_type_id"],
                )
                self.set_status(f"Added data point {dialog.result['name']}")
                self.refresh_canvas()
            return

        if mode == "edge":
            if not picked:
                self.set_status("No nearby point found")
                return

            if self.selected_for_edge is None:
                self.selected_for_edge = picked
                self.selected_point_name = picked
                self.set_status(f"Edge start selected: {picked}")
                self.refresh_canvas()
                return

            if picked == self.selected_for_edge:
                self.set_status("Pick a different point for the edge end")
                return

            start_name = self.selected_for_edge
            end_name = picked

            ok, message = self._can_create_corridor_edge(start_name, end_name)
            if not ok:
                QMessageBox.warning(self, "Invalid edge", message)
                self.selected_for_edge = None
                self.selected_point_name = None
                self.refresh_canvas()
                return

            self.push_undo_state("Add edge")

            self.store.add_edge(start_name, end_name)
            if self.bidirectional_check.isChecked():
                self.store.add_edge(end_name, start_name)

            self.selected_point_name = end_name

            if self.chain_edges_check.isChecked():
                self.selected_for_edge = end_name
                self.set_status(
                    f"Connected {start_name} -> {end_name}. Chain continues from {end_name}"
                )
            else:
                self.selected_for_edge = None
                self.set_status(f"Connected {start_name} -> {end_name}")

            self.refresh_canvas()
            return

        if mode == "transition":
            transition_id = None
            existing_transition = None

            if picked and "-F" in picked:
                transition_id = picked.rsplit("-F", 1)[0]
                for item in self.store.data.get("transitions", []):
                    if item["id"] == transition_id:
                        existing_transition = item
                        break

            dialog = TransitionEditorDialog(
                self,
                existing_transition,
                default_floor=floor,
                default_x=x,
                default_y=y,
                default_id=transition_id or self.store.suggest_next_transition_id(),
            )

            if dialog.exec() == QDialog.Accepted and dialog.result:
                self.push_undo_state("Upsert transition")

                old_id = transition_id
                new_id = dialog.result["id"]

                self.store.upsert_transition(
                    new_id,
                    dialog.result["floors"],
                    dialog.result["floor_locations"],
                    dialog.result["cable_limit"],
                )

                if old_id and old_id != new_id:
                    self._rename_transition_references(old_id, new_id)

                self.set_status(f"Saved transition {new_id}")
                self.refresh_canvas()

            return

    def on_double_click(self, event, sx, sy):
        floor = self.floor_spin.value()
        x, y = self.scene_to_world(sx, sy)
        picked = self.find_nearest_selectable_name(x, y, floor)
        if not picked:
            return

        department = self.store.departments_for_floor(floor).get(picked)
        if department is not None:
            dialog = DepartmentEditorDialog(self, department)
            if dialog.exec() == QDialog.Accepted and dialog.result:
                self.store.set_department_position(
                    picked, dialog.result["x"], dialog.result["y"]
                )
                self.push_undo_state("Rename department")
                self.store.rename_department(picked, picked, dialog.result["name"])
                self.selected_point_name = picked
                self.set_status(f"Edited department {picked}")
                self.refresh_canvas()
            return
        point = self.store.all_points()[picked]

        if point.get("kind") == "transition_node":
            transition_id = point["transition_id"]
            existing_transition = next(
                (
                    x
                    for x in self.store.data.get("transitions", [])
                    if x["id"] == transition_id
                ),
                None,
            )
            dialog = TransitionEditorDialog(
                self,
                existing_transition,
                default_floor=floor,
                default_x=point["x"],
                default_y=point["y"],
                default_id=transition_id,
            )
            if dialog.exec() == QDialog.Accepted and dialog.result:
                self.store.upsert_transition(
                    dialog.result["id"],
                    dialog.result["floors"],
                    dialog.result["floor_locations"],
                    dialog.result["cable_limit"],
                )
                self.set_status(f"Edited {dialog.result['id']}")
                self.refresh_canvas()
            return

        if point.get("kind") == "data_point":
            seed = dict(point)
            dialog = DataPointEditorDialog(
                self,
                seed=seed,
                default_floor=floor,
                default_x=point["x"],
                default_y=point["y"],
                default_name=picked,
                department_options=self.department_options(),
                room_type_options=self.store.room_type_options(),
            )
            if dialog.exec() == QDialog.Accepted and dialog.result:
                self.push_undo_state("Set point position")
                self.store.set_point_position(
                    picked, dialog.result["x"], dialog.result["y"]
                )
                self.store.rename_point(picked, dialog.result["name"])

                for item in self.store.data.get("corridors", {}).get("nodes", []):
                    if item.get("name") == dialog.result["name"]:
                        item["restricted"] = bool(
                            dialog.result.get("restricted", False)
                        )
                        break
                for item in self.store.data.get("data_points", []):
                    if item["name"] == dialog.result["name"]:
                        item["extension_distance_m"] = dialog.result[
                            "extension_distance_m"
                        ]
                        item["room_type_id"] = dialog.result["room_type_id"]
                        if item["room_type_id"]:
                            item["qty"] = self.store.room_type_cable_qty(item["room_type_id"])
                        else:
                            item["qty"] = dialog.result["qty"]

                        self.store.sync_connection_qty_for_data_point(dialog.result["name"])
                        item["department_ids"] = list(
                            dialog.result.get("department_ids", [])
                        )
                        break
                self.selected_point_name = dialog.result["name"]
                self.set_status(f"Edited {dialog.result['name']}")
                self.refresh_canvas()
            return

        if point.get("kind") in {"location", "comms_room"}:
            location_item = next(
                (
                    item
                    for item in self.store.data.get("locations", [])
                    if item["name"] == picked
                ),
                None,
            )
            if location_item is None:
                return

            dialog = LocationEditorDialog(
                self, picked, dict(location_item), self.department_options()
            )
            if dialog.exec() == QDialog.Accepted and dialog.result:
                new_name = dialog.result["name"]
                if new_name != picked and new_name in self.store.names_in_use():
                    QMessageBox.critical(self, "Duplicate name", "Name already exists")
                    return
                self.push_undo_state("Set point position")
                self.store.set_point_position(
                    picked, dialog.result["x"], dialog.result["y"]
                )
                self.store.rename_point(picked, new_name)

                for item in self.store.data.get("locations", []):
                    if item["name"] == new_name:
                        item["kind"] = dialog.result["kind"]
                        item["department_ids"] = list(
                            dialog.result.get("department_ids", [])
                        )
                        break

                self.selected_point_name = new_name
                self.set_status(f"Edited {new_name}")
                self.refresh_canvas()
            return

        dialog = PointEditorDialog(self, f"Edit {picked}", picked, point)
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self.push_undo_state("Set point position")
            self.store.set_point_position(
                picked, dialog.result["x"], dialog.result["y"]
            )
            self.store.rename_point(picked, dialog.result["name"])
            self.selected_point_name = dialog.result["name"]
            self.set_status(f"Edited {dialog.result['name']}")
            self.refresh_canvas()

    def on_left_release(self, event):
        if self.selection_rect_active:
            handled = self._finish_selection_rect(event)
            self.dragging_point_name = None
            self.drag_mode_active = False
            self.last_pan = None
            if handled:
                return
        self.dragging_point_name = None
        self.drag_mode_active = False
        self.alt_move_locked = False
        self.last_pan = None

    def on_right_click(self, event, sx, sy):
        mode = self.mode_combo.currentText()
        floor = self.floor_spin.value()
        x, y = self.scene_to_world(sx, sy)
        picked = self.find_nearest_selectable_name(x, y, floor)

        # In edge mode, right click is ONLY for deleting edges.
        # Never fall through to the normal context menu.
        if mode == "edge":
            if not picked:
                self.set_status("No nearby point found for edge delete")
                return

            if self.edge_delete_start is None:
                self.edge_delete_start = picked
                self.selected_for_edge = None
                self.selected_point_name = picked
                self.set_status(f"Edge delete start selected: {picked}")
                self.refresh_canvas()
                return

            removed = False

            before = len(self.store.data.get("corridors", {}).get("edges", []))
            self.store.remove_edge(self.edge_delete_start, picked)
            after = len(self.store.data.get("corridors", {}).get("edges", []))
            removed = removed or (after < before)

            if self.bidirectional_check.isChecked():
                before = len(self.store.data.get("corridors", {}).get("edges", []))
                self.store.remove_edge(picked, self.edge_delete_start)
                after = len(self.store.data.get("corridors", {}).get("edges", []))
                removed = removed or (after < before)

            self.edge_delete_start = None
            self.selected_for_edge = None
            self.selected_point_name = picked
            self.set_status("Edge removed" if removed else "No matching edge to remove")
            self.refresh_canvas()
            return

        if picked:
            self.selected_point_name = picked
            self.refresh_canvas()

            point = self.store.all_points().get(picked)
            kind = str(point.get("kind", "")).strip() if point else ""

            menu = QMenu(self)
            show_edges_action = menu.addAction("Show all edge connections")
            estimate_cables_action = menu.addAction("Show estimated cables passing")

            selected_data_points = self._selected_data_point_names()
            update_selected_dp_qty_action = None

            create_selected_dp_connections_action = None
            disconnect_selected_dp_connections_action = None

            assign_selected_dp_departments_action = None
            assign_selected_dp_room_type_action = None

            if selected_data_points:
                menu.addSeparator()
                update_selected_dp_qty_action = menu.addAction(
                    f"Update qty for {len(selected_data_points)} selected data points"
                )
                menu.addSeparator()
                create_selected_dp_connections_action = menu.addAction(
                    f"Create connection(s) for {len(selected_data_points)} selected data point(s)"
                )
                disconnect_selected_dp_connections_action = menu.addAction(
                    f"Disconnect connection(s) for {len(selected_data_points)} selected data point(s)"
                )
                assign_selected_dp_departments_action = menu.addAction(
                    f"Assign department(s) for {len(selected_data_points)} selected data point(s)"
                )
                assign_selected_dp_room_type_action = menu.addAction(
                    f"Assign room type for {len(selected_data_points)} selected data point(s)"
                )

            selected_corridor_nodes = self._selected_corridor_node_names()
            restrict_nodes_action = None
            unrestrict_nodes_action = None

            if selected_corridor_nodes:
                menu.addSeparator()
                restrict_nodes_action = menu.addAction(
                    f"Restrict {len(selected_corridor_nodes)} selected corridor node(s)"
                )
                unrestrict_nodes_action = menu.addAction(
                    f"Unrestrict {len(selected_corridor_nodes)} selected corridor node(s)"
                )

            if kind not in {
                "corridor_node",
                "transition_node",
                "location",
                "comms_room",
                "data_point",
            }:
                estimate_cables_action.setEnabled(False)

            action = menu.exec(event.globalPosition().toPoint())
            if action == show_edges_action:
                self._show_edge_connections_dialog(picked)
            elif action == estimate_cables_action:
                self.show_cable_count_for_node(picked)
            elif (
                update_selected_dp_qty_action is not None
                and action == update_selected_dp_qty_action
            ):
                self.update_selected_data_point_qty()
            elif restrict_nodes_action is not None and action == restrict_nodes_action:
                self.set_selected_corridor_restricted(True)
            elif (
                unrestrict_nodes_action is not None
                and action == unrestrict_nodes_action
            ):
                self.set_selected_corridor_restricted(False)
            elif (
                create_selected_dp_connections_action is not None
                and action == create_selected_dp_connections_action
            ):
                self.create_connections_for_selected_data_points()
            elif (
                disconnect_selected_dp_connections_action is not None
                and action == disconnect_selected_dp_connections_action
            ):
                self.disconnect_selected_data_point_connections()
            elif (
                assign_selected_dp_departments_action is not None
                and action == assign_selected_dp_departments_action
            ):
                self.manage_data_point_departments()
            elif (
                assign_selected_dp_room_type_action is not None
                and action == assign_selected_dp_room_type_action
            ):
                self.assign_room_type_to_selected_data_points()

    def on_drag(self, event, sx, sy):
        mode = self.mode_combo.currentText()
        if mode == "pan":
            current = event.position().toPoint()
            if self.last_pan is None:
                self.last_pan = current
                return
            dx = current.x() - self.last_pan.x()
            dy = current.y() - self.last_pan.y()
            self.canvas.horizontalScrollBar().setValue(
                self.canvas.horizontalScrollBar().value() - dx
            )
            self.canvas.verticalScrollBar().setValue(
                self.canvas.verticalScrollBar().value() - dy
            )
            self.last_pan = current
            self.canvas.viewport().update()
            return
        if mode == "select_move":
            if self.selection_rect_active:
                self._update_selection_rect(event)
                return

            if self.alt_move_locked:
                return

            if self.drag_mode_active and self.dragging_point_name:
                point = self.store.all_points().get(self.dragging_point_name)
                if point and not self._is_point_kind_visible(point):
                    self.dragging_point_name = None
                    self.drag_mode_active = False
                    return

                x, y = self.scene_to_world(sx, sy)
                x, y = self.snap(x, y)
                self._move_point_or_transition(self.dragging_point_name, x, y)
                self.refresh_canvas()

    def on_middle_click(self, event):
        self.last_pan = event.position().toPoint()

    def on_middle_drag(self, event):
        current = event.position().toPoint()
        if self.last_pan is None:
            self.last_pan = current
            return
        dx = current.x() - self.last_pan.x()
        dy = current.y() - self.last_pan.y()
        self.canvas.horizontalScrollBar().setValue(
            self.canvas.horizontalScrollBar().value() - dx
        )
        self.canvas.verticalScrollBar().setValue(
            self.canvas.verticalScrollBar().value() - dy
        )
        self.last_pan = current
        self.canvas.viewport().update()

    def on_middle_release(self, event):
        self.last_pan = None
        self.refresh_canvas()

    def on_mousewheel(self, event):
        factor = 1.1 if event.angleDelta().y() > 0 else 0.9
        self.canvas.scale(factor, factor)
        self.canvas.viewport().update()

    def closeEvent(self, event):
        try:
            self._dxf_thread.quit()
            self._dxf_thread.wait(2000)
        except Exception:
            pass
        super().closeEvent(event)

    def _safe_add_same_floor_edge(self, from_name, to_name):
        ok, _message = self._can_create_corridor_edge(from_name, to_name)
        if not ok:
            return False

        self.store.add_edge(from_name, to_name)
        self.store.add_edge(to_name, from_name)
        return True

    def department_options_with_floor(self):
        options = []
        for item in self.store.data.get("departments", []):
            department_id = str(item.get("id", "")).strip()
            if not department_id:
                continue

            department_name = str(item.get("name", department_id)).strip()
            department_floor = int(item.get("floor", 0))

            options.append((department_id, department_name, department_floor))

        return sorted(
            options,
            key=lambda item: (
                item[1].lower(),
                int(item[2]),
                item[0].lower(),
            ),
        )

    def estimate_cables_through_node(self, node_name):
        graph, points = self._build_routing_graph()
        if node_name not in points:
            return 0, 0, []

        data_point_qty_by_name = {
            str(item.get("name", "")).strip(): int(item.get("qty", 1) or 1)
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
        }

        total_cables = 0
        matching_connections = 0
        details = []

        for connection in self.store.data.get("connections", []):
            start = str(connection.get("from", "")).strip()
            end = str(connection.get("to", "")).strip()
            if not start or not end:
                continue
            if start not in points or end not in points:
                continue

            route_length, route_path = self._shortest_path_length(graph, start, end)
            if route_length is None or not route_path:
                continue

            if node_name in route_path:
                connection_qty = int(connection.get("qty", 1) or 1)

                # If the connection terminates at a data point, use the data point qty.
                # This is normally the number of outlets/cables represented by that point.
                end_data_point_qty = data_point_qty_by_name.get(end)

                if end_data_point_qty is not None:
                    qty = end_data_point_qty
                else:
                    qty = connection_qty

                total_cables += qty
                matching_connections += 1
                details.append(
                    {
                        "id": str(connection.get("id", "")).strip(),
                        "from": start,
                        "to": end,
                        "qty": qty,
                        "connection_qty": connection_qty,
                        "data_point_qty": end_data_point_qty,
                        "route_path": route_path,
                    }
                )

        return total_cables, matching_connections, details

    def show_cable_count_for_node(self, node_name):
        total_cables, matching_connections, details = self.estimate_cables_through_node(
            node_name
        )

        point = self.store.all_points().get(node_name, {})
        kind = str(point.get("kind", "")).strip()

        title = "Estimated cables through node"
        lines = [
            f"Node: {node_name}",
            f"Kind: {kind or 'unknown'}",
            f"Estimated cables passing: {total_cables}",
            f"Connections contributing: {matching_connections}",
        ]

        if details:
            sample = details[:8]
            lines.append("")
            lines.append("Examples:")
            for item in sample:
                lines.append(
                    f"{item['id'] or '-'}: {item['from']} -> {item['to']} (qty {item['qty']})"
                )
            if len(details) > len(sample):
                lines.append(f"... and {len(details) - len(sample)} more")

        QMessageBox.information(self, title, "\n".join(lines))

    def _next_connection_id(self, existing_ids=None):
        existing_ids = set(existing_ids or [])
        for item in self.store.data.get("connections", []):
            value = str(item.get("id", "")).strip()
            if value:
                existing_ids.add(value)

        n = 1
        while f"C{n}" in existing_ids:
            n += 1

        new_id = f"C{n}"
        existing_ids.add(new_id)
        return new_id, existing_ids

    def _next_comms_room_name(self, used_names, floor, start_number=1, kind="comms_room"):
            prefix = self._comms_prefix_for_kind(kind)
            floor = int(floor)
            n = int(start_number)

            while True:
                name = f"{prefix}{n}-F{floor}"
                if name not in used_names:
                    used_names.add(name)
                    return name, n + 1
                n += 1

    def _single_source_distances(self, graph, start):
        if start not in graph:
            return {}

        distances = {start: 0.0}
        heap = [(0.0, start)]

        while heap:
            current_dist, node = heapq.heappop(heap)

            if current_dist > distances.get(node, math.inf):
                continue

            for neighbour, weight in graph.get(node, []):
                new_dist = current_dist + float(weight)

                if new_dist < distances.get(neighbour, math.inf):
                    distances[neighbour] = new_dist
                    heapq.heappush(heap, (new_dist, neighbour))

        return distances

    def _candidate_cover_map(
        self,
        graph,
        points,
        data_point_names,
        candidate_nodes,
        max_cable_length_m,
    ):
        cover_map = {}

        candidate_nodes = [name for name in candidate_nodes if name in points]

        # Cache routing anchors once.
        routing_anchors = self._routing_anchor_names()

        anchor_cache = {}
        unique_anchors = set()

        for point_name in data_point_names:
            point = points.get(point_name)
            if not point:
                anchor_cache[point_name] = (None, None)
                continue

            best_name = None
            best_dist = None

            for anchor_name in routing_anchors:
                if anchor_name == point_name:
                    continue

                anchor = points.get(anchor_name)
                if not anchor:
                    continue

                if int(anchor["floor"]) != int(point["floor"]):
                    continue

                dist = math.hypot(
                    float(point["x"]) - float(anchor["x"]),
                    float(point["y"]) - float(anchor["y"]),
                )

                if best_dist is None or dist < best_dist:
                    best_name = anchor_name
                    best_dist = dist

            anchor_cache[point_name] = (best_name, best_dist)

            if best_name is not None:
                unique_anchors.add(best_name)

        # Run Dijkstra once per unique anchor.
        anchor_distances = {}
        total_anchor_steps = max(1, len(unique_anchors))

        for index, anchor_name in enumerate(sorted(unique_anchors), start=1):
            self._update_comms_optimisation_progress(
                current=index,
                total=total_anchor_steps,
                message=("Precalculating shortest paths from data point anchors..."),
                rooms_to_place=None,
            )

            anchor_distances[anchor_name] = self._single_source_distances(
                graph,
                anchor_name,
            )

        # Build cover map using cached distances.
        total_steps = max(1, len(candidate_nodes))

        for current_step, candidate_name in enumerate(candidate_nodes, start=1):
            self._update_comms_optimisation_progress(
                current=current_step,
                total=total_steps,
                message="Building candidate comms room coverage map...",
                rooms_to_place=None,
            )

            candidate_cover = {}

            for point_name in data_point_names:
                point = points.get(point_name)
                if not point:
                    continue

                anchor_name, spur_length = anchor_cache.get(point_name, (None, None))

                if anchor_name is None or spur_length is None:
                    continue

                route_length = anchor_distances.get(anchor_name, {}).get(candidate_name)

                if route_length is None:
                    continue

                extension = float(point.get("extension_distance_m", 0.0) or 0.0)
                cable_length = float(spur_length) + float(route_length) + extension

                if cable_length <= max_cable_length_m:
                    candidate_cover[point_name] = {
                        "cable_length": float(cable_length),
                        "route_length": float(route_length),
                        "spur_length": float(spur_length),
                        "anchor_name": anchor_name,
                    }

            if candidate_cover:
                cover_map[candidate_name] = candidate_cover

        return cover_map

    def _score_candidate_set(self, room_nodes, cover_map, data_point_names):
        """
        Lower is better.
        Sort order:
          1. fewer rooms
          2. lower total assigned cable
          3. lower worst assigned cable
        """
        assigned_total = 0.0
        assigned_worst = 0.0

        for point_name in data_point_names:
            best_length = None
            for candidate_name in room_nodes:
                info = cover_map.get(candidate_name, {}).get(point_name)
                if not info:
                    continue
                length = float(info["cable_length"])
                if best_length is None or length < best_length:
                    best_length = length

            if best_length is None:
                return None

            assigned_total += best_length
            assigned_worst = max(assigned_worst, best_length)

        return (len(room_nodes), assigned_total, assigned_worst)

    def _remove_dominated_comms_candidates(self, candidate_payload):
        """
        Remove candidate B if candidate A covers every point B covers and
        A is no longer for every covered point.

        This reduces n before n-choose-k search, which is the biggest win.
        """
        keep = [True] * len(candidate_payload)

        for i, a in enumerate(candidate_payload):
            if not keep[i]:
                continue

            a_mask = int(a["mask"])
            a_lengths = a["lengths"]

            for j, b in enumerate(candidate_payload):
                if i == j or not keep[j]:
                    continue

                b_mask = int(b["mask"])

                # A must cover all points that B covers.
                if (a_mask | b_mask) != a_mask:
                    continue

                no_worse = True
                strictly_better = False

                for point_idx, b_len in enumerate(b["lengths"]):
                    if b_len is None:
                        continue

                    a_len = a_lengths[point_idx]
                    if a_len is None:
                        no_worse = False
                        break

                    if a_len > b_len:
                        no_worse = False
                        break

                    if a_len < b_len:
                        strictly_better = True

                if no_worse and strictly_better:
                    keep[j] = False

        return [
            candidate
            for candidate, should_keep in zip(candidate_payload, keep)
            if should_keep
        ]

    def _solve_minimum_comms_room_nodes_ilp(
        self,
        cover_map,
        data_point_names,
        time_limit_sec=120,
    ):
        if pulp is None:
            return None, "PuLP is not installed. Install with: pip install pulp"

        all_points = list(data_point_names)
        candidate_names = sorted(cover_map.keys())

        if not candidate_names:
            return None, "No candidate corridor nodes can reach any data points."

        missing = []
        for point_name in all_points:
            if not any(
                point_name in cover_map.get(candidate, {})
                for candidate in candidate_names
            ):
                missing.append(point_name)

        if missing:
            return None, (
                "Some data points cannot be covered within the limit: "
                + ", ".join(missing[:15])
                + (" ..." if len(missing) > 15 else "")
            )

        # Phase 1: minimise number of comms rooms.
        model = pulp.LpProblem("Minimum_Comms_Rooms", pulp.LpMinimize)

        x = {
            candidate: pulp.LpVariable(f"use_{i}", cat="Binary")
            for i, candidate in enumerate(candidate_names)
        }

        model += pulp.lpSum(x[candidate] for candidate in candidate_names)

        for point_name in all_points:
            covering_candidates = [
                candidate
                for candidate in candidate_names
                if point_name in cover_map.get(candidate, {})
            ]

            model += (
                pulp.lpSum(x[candidate] for candidate in covering_candidates) >= 1,
                f"cover_{point_name}",
            )

        solver = pulp.PULP_CBC_CMD(
            msg=False,
            timeLimit=int(time_limit_sec),
        )

        status = model.solve(solver)

        if pulp.LpStatus.get(status) not in {"Optimal", "Feasible"}:
            return None, f"ILP solver failed: {pulp.LpStatus.get(status, status)}"

        minimum_room_count = int(round(sum(pulp.value(x[c]) for c in candidate_names)))

        # Phase 2: keep minimum room count, then minimise total cable assignment.
        model2 = pulp.LpProblem("Minimum_Comms_Rooms_Cable_Length", pulp.LpMinimize)

        x2 = {
            candidate: pulp.LpVariable(f"use_{i}", cat="Binary")
            for i, candidate in enumerate(candidate_names)
        }

        y = {}

        for candidate in candidate_names:
            for point_name in all_points:
                if point_name not in cover_map.get(candidate, {}):
                    continue
                y[(candidate, point_name)] = pulp.LpVariable(
                    f"assign_{candidate}_{point_name}",
                    cat="Binary",
                )

        model2 += pulp.lpSum(
            float(cover_map[candidate][point_name]["cable_length"]) * var
            for (candidate, point_name), var in y.items()
        )

        model2 += (
            pulp.lpSum(x2[candidate] for candidate in candidate_names)
            == minimum_room_count
        )

        for point_name in all_points:
            assign_vars = [
                y[(candidate, point_name)]
                for candidate in candidate_names
                if (candidate, point_name) in y
            ]

            model2 += (
                pulp.lpSum(assign_vars) == 1,
                f"assign_once_{point_name}",
            )

        for (candidate, point_name), var in y.items():
            model2 += var <= x2[candidate]

        status2 = model2.solve(solver)

        if pulp.LpStatus.get(status2) not in {"Optimal", "Feasible"}:
            selected = [
                candidate
                for candidate in candidate_names
                if pulp.value(x[candidate]) and pulp.value(x[candidate]) > 0.5
            ]
            return selected, None

        selected = [
            candidate
            for candidate in candidate_names
            if pulp.value(x2[candidate]) and pulp.value(x2[candidate]) > 0.5
        ]

        return selected, None

    def _solve_minimum_comms_room_nodes(
        self,
        cover_map,
        data_point_names,
        exact_combo_limit=5_000_000,
        batch_size=20000,
        max_workers=None,
    ):
        all_points = list(data_point_names)
        point_index = {name: idx for idx, name in enumerate(all_points)}
        point_count = len(all_points)
        all_mask = (1 << point_count) - 1

        candidate_names = sorted(cover_map.keys())

        if not candidate_names:
            return None, "No candidate corridor nodes can reach any data points."

        union_cover = set()
        for candidate_name in candidate_names:
            union_cover.update(cover_map[candidate_name].keys())

        missing = sorted(set(all_points) - union_cover)
        if missing:
            return None, (
                "Some data points cannot be covered within the limit: "
                + ", ".join(missing[:15])
                + (" ..." if len(missing) > 15 else "")
            )

        candidate_payload = []

        for candidate_name in candidate_names:
            mask = 0
            lengths = [None] * point_count

            for point_name, info in cover_map[candidate_name].items():
                idx = point_index.get(point_name)
                if idx is None:
                    continue

                mask |= 1 << idx
                lengths[idx] = float(info["cable_length"])

            if mask:
                candidate_payload.append(
                    {
                        "name": candidate_name,
                        "mask": mask,
                        "lengths": lengths,
                    }
                )

        before_count = len(candidate_payload)
        candidate_payload = self._remove_dominated_comms_candidates(candidate_payload)
        after_count = len(candidate_payload)

        self._update_comms_optimisation_progress(
            current=0,
            total=1,
            message=(
                f"Reduced candidates from {before_count} to {after_count}. "
                "Starting exact search..."
            ),
            rooms_to_place=None,
        )

        worker_count = max_workers or max(1, (os.cpu_count() or 2) - 1)
        max_pending = max(1, worker_count * 3)

        for room_count in range(1, len(candidate_payload) + 1):
            combo_count = math.comb(len(candidate_payload), room_count)

            if combo_count > exact_combo_limit:
                break

            processed = 0
            best_combo = None
            best_score = None
            last_progress_emit = 0

            self._update_comms_optimisation_progress(
                current=0,
                total=max(1, combo_count),
                message=(
                    f"Testing {room_count} room combination(s) "
                    f"using {worker_count} process(es)..."
                ),
                rooms_to_place=room_count,
            )

            combo_iter = combinations(range(len(candidate_payload)), room_count)
            batch_iter = iter(_batched_combinations(combo_iter, batch_size))

            with ProcessPoolExecutor(
                max_workers=worker_count,
                initializer=_init_comms_process,
                initargs=(candidate_payload, all_mask, point_count),
            ) as pool:
                pending = set()

                def submit_next_batch():
                    try:
                        batch = next(batch_iter)
                    except StopIteration:
                        return False

                    pending.add(pool.submit(_evaluate_comms_batch, batch))
                    return True

                for _ in range(max_pending):
                    if not submit_next_batch():
                        break

                while pending:
                    done, pending = wait(
                        pending,
                        return_when=FIRST_COMPLETED,
                    )

                    for future in done:
                        batch_processed, batch_combo, batch_score = future.result()
                        processed += int(batch_processed)

                        if batch_combo is not None and (
                            best_score is None or batch_score < best_score
                        ):
                            best_combo = batch_combo
                            best_score = batch_score

                        submit_next_batch()

                    # Throttle UI updates. Updating every batch slows the solve.
                    if (
                        processed - last_progress_emit >= batch_size * max_pending
                        or processed >= combo_count
                    ):
                        last_progress_emit = processed
                        self._update_comms_optimisation_progress(
                            current=processed,
                            total=max(1, combo_count),
                            message=(
                                f"Testing {room_count} room combination(s) "
                                f"using {worker_count} process(es)..."
                            ),
                            rooms_to_place=room_count,
                        )

            if best_combo is not None:
                return [candidate_payload[idx]["name"] for idx in best_combo], None

        return self._greedy_minimum_comms_room_nodes(
            candidate_payload,
            all_mask,
        )

    def _can_create_corridor_edge(self, start_name, end_name):
        points = self.store.all_points()
        start = points.get(start_name)
        end = points.get(end_name)

        if not start or not end:
            return False, "Both edge endpoints must exist."

        if int(start.get("floor", 0)) != int(end.get("floor", 0)):
            return (
                False,
                "Cross-floor corridor edges are not allowed. Use a transition instead.",
            )

        return True, ""

    def _greedy_minimum_comms_room_nodes(self, candidate_payload, all_mask):
        uncovered = all_mask
        selected = []

        while uncovered:
            best_idx = None
            best_key = None

            for idx, candidate in enumerate(candidate_payload):
                if idx in selected:
                    continue

                newly_covered = uncovered & candidate["mask"]
                gain = newly_covered.bit_count()

                if gain <= 0:
                    continue

                lengths = [
                    length
                    for point_idx, length in enumerate(candidate["lengths"])
                    if length is not None and newly_covered & (1 << point_idx)
                ]

                key = (
                    -gain,
                    sum(lengths),
                    max(lengths) if lengths else 0.0,
                    candidate["name"],
                )

                if best_key is None or key < best_key:
                    best_key = key
                    best_idx = idx

            if best_idx is None:
                return None, "Greedy fallback failed to cover all data points."

            selected.append(best_idx)
            uncovered &= ~candidate_payload[best_idx]["mask"]

            self._update_comms_optimisation_progress(
                current=len(selected),
                total=len(candidate_payload),
                message="Large search space detected. Using greedy fallback...",
                rooms_to_place=len(selected),
            )

        return [candidate_payload[idx]["name"] for idx in selected], None

    def remove_invalid_cross_floor_edges(self):
        points = self.store.all_points()
        valid_edges = []
        removed_edges = []

        for edge in self.store.data.get("corridors", {}).get("edges", []):
            from_name = str(edge.get("from", "")).strip()
            to_name = str(edge.get("to", "")).strip()

            from_point = points.get(from_name)
            to_point = points.get(to_name)

            if not from_point or not to_point:
                removed_edges.append(edge)
                continue

            if int(from_point.get("floor", 0)) != int(to_point.get("floor", 0)):
                removed_edges.append(edge)
                continue

            valid_edges.append(edge)

        if not removed_edges:
            QMessageBox.information(
                self,
                "Remove Invalid Edges",
                "No invalid edges found.",
            )
            self.set_status("No invalid edges found")
            return

        self.push_undo_state("Remove invalid edges")
        self.store.data["corridors"]["edges"] = valid_edges
        self.refresh_canvas()

        lines = [
            f"Removed {len(removed_edges)} invalid edge(s):",
            "",
        ]

        for edge in removed_edges[:50]:
            lines.append(f"{edge.get('from', '')} -> {edge.get('to', '')}")

        if len(removed_edges) > 50:
            lines.append(f"... and {len(removed_edges) - 50} more")

        QMessageBox.information(
            self,
            "Remove Invalid Edges",
            "\n".join(lines),
        )

        self.set_status(f"Removed {len(removed_edges)} invalid edge(s)")

    def _highest_comms_room_number(self, prefix="CR"):
        highest = 0
        pattern = re.compile(rf"^{re.escape(prefix)}(\d+)-F\d+$", re.IGNORECASE)

        for item in self.store.data.get("locations", []):
            if str(item.get("kind", "")) not in {"comms_room", "distributed_equipment_room"}:
                continue

            name = str(item.get("name", "")).strip()
            match = pattern.match(name)
            if match:
                highest = max(highest, int(match.group(1)))

        return highest

    def optimise_comms_rooms_for_model(self):
        if not self.store.data.get("data_points"):
            QMessageBox.critical(
                self,
                "Optimise Comms Rooms",
                "No data points found in the model.",
            )
            return

        max_cable_length_m, ok = QInputDialog.getDouble(
            self,
            "Optimise Comms Rooms",
            "Maximum cable length per data point (m)",
            90.0,
            0.0,
            100000.0,
            2,
        )
        if not ok:
            return

        comms_room_cable_limit, ok = QInputDialog.getInt(
            self,
            "Optimise Comms Rooms",
            "Maximum cables per comms room:",
            48,
            1,
            1000000,
        )
        if not ok:
            return

        replace_existing = (
            QMessageBox.question(
                self,
                "Optimise Comms Rooms",
                "Replace existing comms rooms and their comms-room connections?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes,
            )
            == QMessageBox.Yes
        )

        graph, points = self._build_routing_graph()
        data_point_names = self.data_point_names()
        candidate_nodes = self._candidate_comms_room_nodes()

        if not candidate_nodes:
            QMessageBox.critical(
                self,
                "Optimise Comms Rooms",
                "No corridor nodes are available as candidate comms room positions.",
            )
            return

        self._update_comms_optimisation_progress(
            current=0,
            total=max(1, len(candidate_nodes)),
            message="Preparing optimisation...",
            rooms_to_place=None,
        )

        try:
            cover_map = self._candidate_cover_map(
                graph=graph,
                points=points,
                data_point_names=data_point_names,
                candidate_nodes=candidate_nodes,
                max_cable_length_m=float(max_cable_length_m),
            )

            selected_nodes = None
            error_message = None

            if not selected_nodes:
                selected_nodes, error_message = self._solve_minimum_comms_room_nodes(
                    cover_map=cover_map,
                    data_point_names=data_point_names,
                )

            if selected_nodes:
                self._update_comms_optimisation_progress(
                    current=1,
                    total=1,
                    message="Applying solution to the model...",
                    rooms_to_place=len(selected_nodes),
                )
        finally:
            self._finish_comms_optimisation_progress("Optimisation complete")

        if not selected_nodes:
            QMessageBox.information(
                self,
                "Optimise Comms Rooms",
                error_message or "No valid solution found.",
            )
            return

        existing_comms_rooms = set(self.comms_room_names())

        self.push_undo_state("Optimise comms rooms")

        if replace_existing:
            self.store.data["locations"] = [
                item
                for item in self.store.data.get("locations", [])
                if str(item.get("kind", "location")) != "comms_room"
            ]
            self.store.data["connections"] = [
                item
                for item in self.store.data.get("connections", [])
                if str(item.get("from", "")).strip() not in existing_comms_rooms
            ]

        used_names = set(self.store.names_in_use())
        next_comms_room_number = self._highest_comms_room_number() + 1

        rooms_by_candidate = {}
        room_loads = {}

        def create_comms_room_for_candidate(candidate_name):
            nonlocal next_comms_room_number

            candidate_point = points[candidate_name]
            floor = int(candidate_point["floor"])

            room_name, next_comms_room_number = self._next_comms_room_name(
                used_names,
                floor,
                next_comms_room_number,
            )

            rooms_by_candidate.setdefault(candidate_name, []).append(room_name)
            room_loads[room_name] = 0

            room_index = len(rooms_by_candidate.get(candidate_name, [])) - 1

            offset_radius = 1.5
            angle_step = math.radians(60)

            if room_index == 0:
                angle = math.radians(45)
            else:
                angle = math.radians(45) + (room_index * angle_step)

            room_x = float(candidate_point["x"]) + (math.cos(angle) * offset_radius)
            room_y = float(candidate_point["y"]) + (math.sin(angle) * offset_radius)

            self.store.add_location(
                room_name,
                floor,
                room_x,
                room_y,
                kind="comms_room",
            )

            for location in self.store.data.get("locations", []):
                if location.get("name") == room_name:
                    location["cable_limit"] = int(comms_room_cable_limit)
                    break

            self._safe_add_same_floor_edge(room_name, candidate_name)
            return room_name

        for candidate_name in selected_nodes:
            create_comms_room_for_candidate(candidate_name)

        data_point_qty = {
            str(item.get("name", "")).strip(): int(item.get("qty", 1) or 1)
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
        }

        assignments = {}
        total_length = 0.0
        worst_length = 0.0

        ordered_points = sorted(
            data_point_names,
            key=lambda point_name: data_point_qty.get(point_name, 1),
            reverse=True,
        )

        for point_name in ordered_points:
            qty = data_point_qty.get(point_name, 1)

            best_candidate = None
            best_length = None

            for candidate_name in selected_nodes:
                info = cover_map.get(candidate_name, {}).get(point_name)
                if not info:
                    continue

                cable_length = float(info["cable_length"])
                if best_length is None or cable_length < best_length:
                    best_candidate = candidate_name
                    best_length = cable_length

            if best_candidate is None:
                QMessageBox.critical(
                    self,
                    "Optimise Comms Rooms",
                    f"No selected comms room location can serve {point_name}.",
                )
                return

            nearby_rooms = self._existing_comms_rooms_near_candidate(best_candidate)

            if nearby_rooms:
                options = nearby_rooms + ["<Create new comms room>"]

                choice, ok = QInputDialog.getItem(
                    self,
                    "Use Existing Comms Room?",
                    "Nearby comms rooms found. Select one or create new:",
                    options,
                    0,
                    False,
                )

                if not ok:
                    return

                if choice != "<Create new comms room>":
                    # Reassign connections instead of creating new room
                    room_name = choice

                    self.push_undo_state("Reassign to existing comms room")

                    for connection in self.store.data.get("connections", []):
                        if connection.get("to") in data_point_names:
                            connection["from"] = room_name

                    self.refresh_canvas()
                    self.set_status(
                        f"Assigned {len(data_point_names)} data point(s) to existing comms room {room_name}"
                    )
                    return

            target_room = None

            for room_name in rooms_by_candidate.get(best_candidate, []):
                if room_loads.get(room_name, 0) + qty <= int(comms_room_cable_limit):
                    target_room = room_name
                    break

            if target_room is None:
                target_room = create_comms_room_for_candidate(best_candidate)

            assignments[point_name] = {
                "candidate_node": best_candidate,
                "room_name": target_room,
                "cable_length": best_length,
            }

            room_loads[target_room] += qty
            total_length += best_length
            worst_length = max(worst_length, best_length)

        existing_connection_ids = {
            str(item.get("id", "")).strip()
            for item in self.store.data.get("connections", [])
            if str(item.get("id", "")).strip()
        }

        existing_targets = self._existing_connection_targets()
        created_connections = 0

        for item in self.store.data.get("data_points", []):
            point_name = str(item.get("name", "")).strip()
            if not point_name or point_name not in assignments:
                continue

            if point_name in existing_targets:
                continue

            room_name = assignments[point_name]["room_name"]

            connection_id, existing_connection_ids = self._next_connection_id(
                existing_connection_ids
            )

            self.store.data.setdefault("connections", []).append(
                {
                    "id": connection_id,
                    "from": room_name,
                    "to": point_name,
                    "qty": int(item.get("qty", 1) or 1),
                    "route_profile": "",
                }
            )
            created_connections += 1

        first_candidate = selected_nodes[0]
        self.selected_point_name = rooms_by_candidate[first_candidate][0]
        self.refresh_canvas()

        placed_lines = []
        for candidate_name, room_names in rooms_by_candidate.items():
            floor = points[candidate_name]["floor"]
            for room_name in room_names:
                placed_lines.append(
                    f"{room_name} -> {candidate_name} "
                    f"(floor {floor}, load {room_loads.get(room_name, 0)}/{comms_room_cable_limit})"
                )

        total_rooms = sum(len(room_names) for room_names in rooms_by_candidate.values())

        QMessageBox.information(
            self,
            "Optimise Comms Rooms",
            "\n".join(
                [
                    f"Placed {total_rooms} comms room(s).",
                    f"Created {created_connections} connection(s).",
                    f"Total assigned cable length: {total_length:.2f} m",
                    f"Longest assigned cable: {worst_length:.2f} m",
                    "",
                    "Rooms:",
                    *placed_lines[:20],
                    *(
                        [f"... and {len(placed_lines) - 20} more"]
                        if len(placed_lines) > 20
                        else []
                    ),
                ]
            ),
        )

        self.set_status(
            f"Optimised {total_rooms} comms room(s) for {len(data_point_names)} data point(s)"
        )

    def _ensure_comms_optimisation_dialog(self):
        if self._comms_optimisation_dialog is None:
            self._comms_optimisation_dialog = CommsRoomOptimisationProgressDialog(self)
        return self._comms_optimisation_dialog

    def _update_comms_optimisation_progress(
        self,
        current,
        total,
        message,
        rooms_to_place=None,
    ):
        dialog = self._ensure_comms_optimisation_dialog()
        dialog.update_progress(
            current=current,
            total=total,
            message=message,
            rooms_to_place=rooms_to_place,
        )
        if not dialog.isVisible():
            dialog.show()
            dialog.raise_()
            dialog.activateWindow()
        QApplication.processEvents()

    def _finish_comms_optimisation_progress(self, message="Finished"):
        if self._comms_optimisation_dialog is not None:
            self._comms_optimisation_dialog.mark_complete(message)

    def _move_point_or_transition(self, point_name, x, y):
        point = self.store.all_points().get(point_name)

        if point and point.get("kind") == "transition_node":
            transition_id = str(point.get("transition_id", "")).strip()
            from_floor = int(point.get("floor", 0))

            moved_count = self.store.move_transition_from_floor_up(
                transition_id,
                from_floor,
                x,
                y,
            )

            self.set_status(
                f"Moved {transition_id} on floor {from_floor} and "
                f"{max(0, moved_count - 1)} floor(s) above"
            )
            return
        self.store.set_point_position(point_name, x, y)

    def _unassigned_data_point_names(self):
        result = []

        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            department_ids = [
                str(x).strip() for x in item.get("department_ids", []) if str(x).strip()
            ]

            if not department_ids:
                result.append(
                    {
                        "name": name,
                        "floor": int(item.get("floor", 0)),
                        "x": float(item.get("x", 0.0)),
                        "y": float(item.get("y", 0.0)),
                    }
                )

        result.sort(key=lambda row: (row["floor"], row["name"]))
        return [row["name"] for row in result]

    def show_unassigned_data_point_navigator(self):
        self._unassigned_dp_names = self._unassigned_data_point_names()
        self._unassigned_dp_index = -1

        if self._unassigned_dp_dialog is None:
            self._unassigned_dp_dialog = UnassignedDataPointNavigatorDialog(self)
            self._unassigned_dp_dialog.nextRequested.connect(
                self.goto_next_unassigned_data_point
            )
            self._unassigned_dp_dialog.previousRequested.connect(
                self.goto_previous_unassigned_data_point
            )

        self._unassigned_dp_dialog.show()
        self._unassigned_dp_dialog.raise_()
        self._unassigned_dp_dialog.activateWindow()

        if not self._unassigned_dp_names:
            self._unassigned_dp_dialog.set_status("No unassigned data points found.")
            self.set_status("No unassigned data points found")
            return

        self.goto_next_unassigned_data_point()

    def goto_next_unassigned_data_point(self):
        if not self._unassigned_dp_names:
            self._unassigned_dp_names = self._unassigned_data_point_names()

        if not self._unassigned_dp_names:
            if self._unassigned_dp_dialog:
                self._unassigned_dp_dialog.set_status(
                    "No unassigned data points found."
                )
            return

        self._unassigned_dp_index = (self._unassigned_dp_index + 1) % len(
            self._unassigned_dp_names
        )

        self._centre_on_unassigned_data_point()

    def goto_previous_unassigned_data_point(self):
        if not self._unassigned_dp_names:
            self._unassigned_dp_names = self._unassigned_data_point_names()

        if not self._unassigned_dp_names:
            if self._unassigned_dp_dialog:
                self._unassigned_dp_dialog.set_status(
                    "No unassigned data points found."
                )
            return

        self._unassigned_dp_index = (self._unassigned_dp_index - 1) % len(
            self._unassigned_dp_names
        )

        self._centre_on_unassigned_data_point()

    def _centre_on_unassigned_data_point(self):
        if self._unassigned_dp_index < 0:
            return

        name = self._unassigned_dp_names[self._unassigned_dp_index]
        point = self.store.all_points().get(name)

        if not point:
            return

        floor = int(point.get("floor", 0))

        if self.floor_spin.value() != floor:
            self.floor_spin.setValue(floor)

        self.selected_point_name = name
        self._set_canvas_multi_selection([name], append=False)
        self.refresh_canvas()

        scene_pos = self.world_to_scene(point["x"], point["y"])
        self.canvas.centerOn(scene_pos)

        text = (
            f"{self._unassigned_dp_index + 1} / {len(self._unassigned_dp_names)}\n"
            f"{name}\n"
            f"Floor {floor}"
        )

        if self._unassigned_dp_dialog:
            self._unassigned_dp_dialog.set_status(text)

        self.set_status(f"Centred on unassigned data point {name}")

    def _all_model_floors(self):
        floors = set()

        for item in self.store.data.get("corridors", {}).get("nodes", []):
            floors.add(int(item.get("floor", 0)))

        for item in self.store.data.get("data_points", []):
            floors.add(int(item.get("floor", 0)))

        for item in self.store.data.get("locations", []):
            floors.add(int(item.get("floor", 0)))

        for item in self.store.data.get("departments", []):
            floors.add(int(item.get("floor", 0)))

        for transition in self.store.data.get("transitions", []):
            for floor_key in transition.get("floor_locations", {}).keys():
                try:
                    floors.add(int(floor_key))
                except Exception:
                    pass

        return sorted(floors)

    def _cable_counts_by_node(self):
        graph, points = self._build_routing_graph()
        counts = {name: 0 for name in points.keys()}

        data_point_qty_by_name = {
            str(item.get("name", "")).strip(): int(item.get("qty", 1) or 1)
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
        }

        route_cache = {}

        for connection in self.store.data.get("connections", []):
            start = str(connection.get("from", "")).strip()
            end = str(connection.get("to", "")).strip()

            if not start or not end:
                continue
            if start not in points or end not in points:
                continue

            cache_key = tuple(sorted((start, end)))
            if cache_key in route_cache:
                route_path = route_cache[cache_key]
            else:
                _route_length, route_path = self._shortest_path_length(
                    graph,
                    start,
                    end,
                )
                route_cache[cache_key] = route_path

            if not route_path:
                continue

            qty = data_point_qty_by_name.get(
                end,
                int(connection.get("qty", 1) or 1),
            )

            for node_name in route_path:
                counts[node_name] = counts.get(node_name, 0) + qty

        return counts

    def _export_dxf_background_entities(self, msp, entities):
        for entity in entities or []:
            etype = entity.get("type")

            try:
                if etype == "LINE":
                    msp.add_line(
                        entity["start"],
                        entity["end"],
                        dxfattribs={"layer": "00_DXF_BACKGROUND"},
                    )

                elif etype == "POLYLINE":
                    points = entity.get("points", [])
                    if len(points) >= 2:
                        msp.add_lwpolyline(
                            points,
                            close=bool(entity.get("closed", False)),
                            dxfattribs={"layer": "00_DXF_BACKGROUND"},
                        )

                elif etype == "CIRCLE":
                    msp.add_circle(
                        entity["center"],
                        float(entity["radius"]),
                        dxfattribs={"layer": "00_DXF_BACKGROUND"},
                    )

                elif etype == "ARC":
                    msp.add_arc(
                        center=entity["center"],
                        radius=float(entity["radius"]),
                        start_angle=float(entity.get("start_angle", 0.0)),
                        end_angle=float(entity.get("end_angle", 0.0)),
                        dxfattribs={"layer": "00_DXF_BACKGROUND"},
                    )

                elif etype == "TEXT":
                    text = str(entity.get("text", "")).strip()
                    if not text:
                        continue

                    msp.add_text(
                        text,
                        dxfattribs={
                            "layer": "00_DXF_BACKGROUND",
                            "height": float(entity.get("height", 2.5) or 2.5),
                            "rotation": float(entity.get("rotation", 0.0) or 0.0),
                        },
                    ).set_placement(entity.get("insert", (0.0, 0.0)))

            except Exception:
                continue

    def _add_export_layers(self, doc):
        layers = {
            "00_DXF_BACKGROUND": 8,
            "10_CORRIDOR_EDGES": 5,
            "20_CORRIDOR_NODES": 2,
            "30_TRANSITIONS": 1,
            "40_COMMS_ROOMS": 3,
            "45_LOCATIONS": 4,
            "50_DATA_POINTS": 6,
            "60_CABLE_COUNTS": 7,
        }
        for name, colour in layers.items():
            if name not in doc.layers:
                doc.layers.add(name, color=colour)

    def _add_diamond(self, msp, x, y, size, layer):
        half = float(size) / 2.0
        points = [
            (x, y + half),
            (x + half, y),
            (x, y - half),
            (x - half, y),
            (x, y + half),
        ]
        msp.add_lwpolyline(points, close=True, dxfattribs={"layer": layer})

    def _add_square(self, msp, x, y, size, layer):
        half = float(size) / 2.0
        points = [
            (x - half, y - half),
            (x + half, y - half),
            (x + half, y + half),
            (x - half, y + half),
            (x - half, y - half),
        ]
        msp.add_lwpolyline(points, close=True, dxfattribs={"layer": layer})

    def _add_label(self, msp, text, x, y, layer, height=0.7, scale=1.0):
        msp.add_text(
            str(text),
            dxfattribs={
                "layer": layer,
                "height": float(height) * float(scale),
            },
        ).set_placement((float(x), float(y)))

    def _export_one_floor_dxf(self, floor, out_path, cable_counts):
        try:
            import ezdxf
            from ezdxf import units
        except Exception:
            raise RuntimeError(
                "ezdxf is not installed. Install with: pip install ezdxf"
            )

        # Model coordinates are metres. Export DXF in millimetres.
        EXPORT_SCALE = 1000.0
        SYMBOL_SCALE = EXPORT_SCALE
        TEXT_SCALE = EXPORT_SCALE

        doc = ezdxf.new("R2010")
        doc.units = units.MM
        doc.header["$INSUNITS"] = 4  # millimetres
        doc.header["$MEASUREMENT"] = 1  # metric
        doc.header["$LUNITS"] = 2  # decimal
        doc.header["$LUPREC"] = 3

        self._add_export_layers(doc)
        msp = doc.modelspace()

        points = self.store.all_points()

        # Edges
        for edge in self.store.data.get("corridors", {}).get("edges", []):
            a_name = str(edge.get("from", "")).strip()
            b_name = str(edge.get("to", "")).strip()
            a = points.get(a_name)
            b = points.get(b_name)

            if not a or not b:
                continue
            if int(a.get("floor", 0)) != int(floor):
                continue
            if int(b.get("floor", 0)) != int(floor):
                continue

            msp.add_line(
                (
                    float(a["x"]) * EXPORT_SCALE,
                    float(a["y"]) * EXPORT_SCALE,
                ),
                (
                    float(b["x"]) * EXPORT_SCALE,
                    float(b["y"]) * EXPORT_SCALE,
                ),
                dxfattribs={"layer": "10_CORRIDOR_EDGES"},
            )

        # Corridor nodes
        for item in self.store.data.get("corridors", {}).get("nodes", []):
            if int(item.get("floor", 0)) != int(floor):
                continue

            name = str(item.get("name", "")).strip()
            x = float(item.get("x", 0.0)) * EXPORT_SCALE
            y = float(item.get("y", 0.0)) * EXPORT_SCALE
            count = cable_counts.get(name, 0)

            self._add_square(msp, x, y, 0.8 * SYMBOL_SCALE, "20_CORRIDOR_NODES")
            self._add_label(
                msp,
                name,
                x + 0.6 * EXPORT_SCALE,
                y + 0.4 * EXPORT_SCALE,
                "20_CORRIDOR_NODES",
                0.45 * TEXT_SCALE,
            )
            self._add_label(
                msp,
                f"Cables: {count}",
                x + 0.6 * EXPORT_SCALE,
                y - 0.4 * EXPORT_SCALE,
                "60_CABLE_COUNTS",
                0.35 * TEXT_SCALE,
            )

        # Transitions
        for transition in self.store.data.get("transitions", []):
            transition_id = str(transition.get("id", "")).strip()
            pos = transition.get("floor_locations", {}).get(str(floor))
            if pos is None:
                pos = transition.get("floor_locations", {}).get(int(floor))

            if not pos:
                continue

            if isinstance(pos, dict):
                x = float(pos.get("x", 0.0)) * EXPORT_SCALE
                y = float(pos.get("y", 0.0)) * EXPORT_SCALE
            else:
                x = float(pos[0]) * EXPORT_SCALE
                y = float(pos[1]) * EXPORT_SCALE

            node_name = f"{transition_id}-F{floor}"
            count = cable_counts.get(node_name, 0)

            self._add_diamond(msp, x, y, 1.0 * SYMBOL_SCALE, "30_TRANSITIONS")
            self._add_label(
                msp,
                node_name,
                x + 0.7 * EXPORT_SCALE,
                y + 0.4 * EXPORT_SCALE,
                "30_TRANSITIONS",
                0.45 * TEXT_SCALE,
            )
            self._add_label(
                msp,
                f"Cables: {count}",
                x + 0.7 * EXPORT_SCALE,
                y - 0.4 * EXPORT_SCALE,
                "60_CABLE_COUNTS",
                0.35 * TEXT_SCALE,
            )

        # Locations and comms rooms
        for item in self.store.data.get("locations", []):
            if int(item.get("floor", 0)) != int(floor):
                continue

            name = str(item.get("name", "")).strip()
            kind = str(item.get("kind", "location")).strip()
            x = float(item.get("x", 0.0)) * EXPORT_SCALE
            y = float(item.get("y", 0.0)) * EXPORT_SCALE
            count = cable_counts.get(name, 0)

            if kind == "comms_room":
                layer = "40_COMMS_ROOMS"

                msp.add_circle(
                    (x, y),
                    0.45 * SYMBOL_SCALE,
                    dxfattribs={"layer": layer},
                )

                self._add_label(
                    msp,
                    name,
                    x + 0.7 * EXPORT_SCALE,
                    y + 0.4 * EXPORT_SCALE,
                    layer,
                    0.45 * TEXT_SCALE,
                )

                limit = item.get("cable_limit", "")
                count_text = f"Cables: {count}"
                if limit != "":
                    count_text += f" / {limit}"

                self._add_label(
                    msp,
                    count_text,
                    x + 0.7 * EXPORT_SCALE,
                    y - 0.4 * EXPORT_SCALE,
                    "60_CABLE_COUNTS",
                    0.35 * TEXT_SCALE,
                )

            else:
                layer = "45_LOCATIONS"

                msp.add_circle(
                    (x, y),
                    0.3 * SYMBOL_SCALE,
                    dxfattribs={"layer": layer},
                )

                self._add_label(
                    msp,
                    name,
                    x + 0.6 * EXPORT_SCALE,
                    y + 0.3 * EXPORT_SCALE,
                    layer,
                    0.45 * TEXT_SCALE,
                )

        # Data points
        for item in self.store.data.get("data_points", []):
            if int(item.get("floor", 0)) != int(floor):
                continue

            name = str(item.get("name", "")).strip()
            x = float(item.get("x", 0.0)) * EXPORT_SCALE
            y = float(item.get("y", 0.0)) * EXPORT_SCALE
            qty = int(item.get("qty", 1) or 1)
            count = cable_counts.get(name, 0)

            self._add_diamond(msp, x, y, 0.8 * SYMBOL_SCALE, "50_DATA_POINTS")
            self._add_label(
                msp,
                name,
                x + 0.6 * EXPORT_SCALE,
                y + 0.4 * EXPORT_SCALE,
                "50_DATA_POINTS",
                0.45 * TEXT_SCALE,
            )
            self._add_label(
                msp,
                f"Qty: {qty} | Cables: {count}",
                x + 0.6 * EXPORT_SCALE,
                y - 0.4 * EXPORT_SCALE,
                "60_CABLE_COUNTS",
                0.35 * TEXT_SCALE,
            )

        doc.saveas(str(out_path))

    def export_floor_dxfs(self):
        folder = QFileDialog.getExistingDirectory(
            self,
            "Select folder for exported DXFs",
            "",
        )
        if not folder:
            return

        floors = self._all_model_floors()
        if not floors:
            QMessageBox.information(
                self,
                "Export Floor DXFs",
                "No floors found in the model.",
            )
            return

        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            cable_counts = self._cable_counts_by_node()
        finally:
            QApplication.restoreOverrideCursor()
        base_name = (
            Path(self.current_json_path).stem
            if self.current_json_path
            else "cable_routes"
        )
        output_dir = Path(folder)

        exported = []
        failed = []

        for floor in floors:
            out_path = output_dir / f"{base_name}_floor_{floor}.dxf"
            try:
                self._export_one_floor_dxf(floor, out_path, cable_counts)
                exported.append(out_path)
            except Exception as exc:
                failed.append((floor, str(exc)))

        lines = [f"Exported {len(exported)} floor DXF file(s)."]

        if exported:
            lines.append("")
            lines.extend(str(path.name) for path in exported[:20])
            if len(exported) > 20:
                lines.append(f"... and {len(exported) - 20} more")

        if failed:
            lines.append("")
            lines.append("Failures:")
            for floor, message in failed[:10]:
                lines.append(f"Floor {floor}: {message}")

        QMessageBox.information(
            self,
            "Export Floor DXFs",
            "\n".join(lines),
        )

        self.set_status(f"Exported {len(exported)} floor DXF file(s)")

    def _selected_corridor_node_names(self):
        corridor_names = {
            str(item.get("name", "")).strip()
            for item in self.store.data.get("corridors", {}).get("nodes", [])
            if str(item.get("name", "")).strip()
        }

        return sorted(
            name for name in self.selected_template_names if name in corridor_names
        )

    def set_selected_corridor_restricted(self, restricted):
        selected = set(self._selected_corridor_node_names())

        if not selected:
            QMessageBox.information(
                self,
                "Restricted Corridor Nodes",
                "No selected corridor nodes found.",
            )
            return

        self.push_undo_state("Set corridor restricted flag")

        updated = 0
        for item in self.store.data.get("corridors", {}).get("nodes", []):
            if str(item.get("name", "")).strip() in selected:
                item["restricted"] = bool(restricted)
                updated += 1

        self.refresh_canvas()
        self.set_status(
            f"Set {updated} corridor node(s) to restricted={bool(restricted)}"
        )

    def create_connections_for_selected_data_points(self):
        selected = self._selected_data_point_names()

        if not selected:
            QMessageBox.information(
                self,
                "Create Connections",
                "No selected data points found.",
            )
            return

        comms_rooms = self.comms_room_names()
        if not comms_rooms:
            QMessageBox.critical(
                self,
                "Create Connections",
                "No comms rooms found.",
            )
            return

        room_name, ok = QInputDialog.getItem(
            self,
            "Create Connections",
            "Connect selected data points from comms room:",
            comms_rooms,
            0,
            False,
        )

        if not ok or not room_name:
            return

        existing_targets = self._existing_connection_targets()

        skipped_existing = [name for name in selected if name in existing_targets]

        targets = [name for name in selected if name not in existing_targets]

        if not targets:
            QMessageBox.information(
                self,
                "Create Connections",
                "All selected data points already have connections.",
            )
            return

        data_point_qty = {
            str(item.get("name", "")).strip(): int(item.get("qty", 1) or 1)
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
        }

        existing_connection_ids = {
            str(item.get("id", "")).strip()
            for item in self.store.data.get("connections", [])
            if str(item.get("id", "")).strip()
        }

        self.push_undo_state("Create selected data point connections")

        created = 0

        for point_name in targets:
            connection_id, existing_connection_ids = self._next_connection_id(
                existing_connection_ids
            )

            self.store.data.setdefault("connections", []).append(
                {
                    "id": connection_id,
                    "from": str(room_name).strip(),
                    "to": point_name,
                    "qty": int(data_point_qty.get(point_name, 1)),
                    "route_profile": "",
                }
            )

            created += 1

        self.refresh_canvas()

        lines = [
            f"Created {created} connection(s).",
            f"From: {room_name}",
        ]

        if skipped_existing:
            lines.append(
                f"Skipped {len(skipped_existing)} already-connected data point(s)."
            )

        QMessageBox.information(
            self,
            "Create Connections",
            "\n".join(lines),
        )

        self.set_status(f"Created {created} connection(s) from {room_name}")

    def disconnect_selected_data_point_connections(self):
        selected = self._selected_data_point_names()

        if not selected:
            QMessageBox.information(
                self,
                "Disconnect Connections",
                "No selected data points found.",
            )
            return

        selected_set = set(selected)

        matching = [
            connection
            for connection in self.store.data.get("connections", [])
            if str(connection.get("to", "")).strip() in selected_set
        ]

        if not matching:
            QMessageBox.information(
                self,
                "Disconnect Connections",
                "No connections found for the selected data point(s).",
            )
            return

        if (
            QMessageBox.question(
                self,
                "Disconnect Connections",
                f"Remove {len(matching)} connection(s) from selected data point(s)?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            != QMessageBox.Yes
        ):
            return

        self.push_undo_state("Disconnect selected data point connections")

        self.store.data["connections"] = [
            connection
            for connection in self.store.data.get("connections", [])
            if str(connection.get("to", "")).strip() not in selected_set
        ]

        self.refresh_canvas()
        self.set_status(
            f"Disconnected {len(matching)} connection(s) from {len(selected)} data point(s)"
        )

    def _existing_comms_rooms_near_candidate(self, candidate_name, radius=5.0):
        points = self.store.data.get("points", {})
        candidate = points.get(candidate_name)
        if not candidate:
            return []

        cx = float(candidate.get("x", 0.0))
        cy = float(candidate.get("y", 0.0))
        cf = int(candidate.get("floor", 0))

        nearby = []

        for item in self.store.data.get("locations", []):
            if item.get("kind") != "comms_room":
                continue

            if int(item.get("floor", 0)) != cf:
                continue

            dx = float(item.get("x", 0.0)) - cx
            dy = float(item.get("y", 0.0)) - cy
            dist = math.hypot(dx, dy)

            if dist <= radius:
                nearby.append((item.get("name"), dist))

        nearby.sort(key=lambda x: x[1])
        return [n for n, _ in nearby]
    
    def manage_assets(self):
        AssetsEditorWindow(
            self,
            self.store.data.get("assets", []),
            self._save_assets,
        )


    def _save_assets(self, items):
        self.push_undo_state("Save assets")
        self.store.data["assets"] = items

        for point in self.store.data.get("data_points", []):
            name = str(point.get("name", "")).strip()
            if name:
                self.store.sync_connection_qty_for_data_point(name)

        self.set_status("Assets updated and room type quantities recalculated")
        self.refresh_canvas()


def main():
    app = QApplication.instance() or QApplication(sys.argv)
    window = CableRouteEditor()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
