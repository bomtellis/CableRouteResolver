import os
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED, as_completed
from copy import deepcopy
import re
import subprocess
import csv
import pickle
import shutil
import time
import zlib

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
    QSize,
    QTimer,
)
from PySide6.QtGui import (
    QAction,
    QColor,
    QBrush,
    QPainter,
    QPen,
    QFont,
    QPolygonF,
    QShortcut,
    QKeySequence,
    QIcon,
    QPixmap,
    QPainterPath,
)
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QHeaderView,
    QTableWidget,
    QTableWidgetItem,
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
    QProgressDialog,
    QSpinBox,
    QVBoxLayout,
    QWidget,
    QInputDialog,
    QDoubleSpinBox,
    QRubberBand,
    QScrollArea,
    QListWidget,
    QLineEdit,
    QDialogButtonBox,
    QCompleter,
    QFrame,
    QTabWidget,
    QGridLayout,
    QLayout,
    QSizePolicy,
    QToolButton,
    QStyle,
    QButtonGroup,
    QGraphicsPathItem,
    QDockWidget,
    QListWidgetItem,
)

from PySide6.QtOpenGLWidgets import QOpenGLWidget

from dxf_scene import DXFScene
from dxf_gpu_renderer import GpuDxfGraphView
from ui_theme import (
    BOOTSTRAP_GREEN,
    BOOTSTRAP_RIBBON_STYLESHEET,
    apply_bootstrap_theme,
    bootstrap_icon,
    bootstrap_icon_for,
    set_action_icon,
)

from dialogs import (
    BulkDataPointPlacementDialog,
    BulkLocationPlacementDialog,
    DataPointEditorDialog,
    DepartmentEditorDialog,
    EdgeConnectionsDialog,
    LocationEditorDialog,
    PointEditorDialog,
    TableListEditor,
    DataPointsTableEditor,
    LocationsTableEditor,
    PlacementZoneEditorDialog,
    PlacementZonesTableEditor,
    SuggestPlacementZonesDialog,
    TransitionEditorDialog,
    SuggestCommsRoomDialog,
    SuggestRoomsFromZonesDialog,
    CommsRoomOptimisationProgressDialog,
    RoomTypesEditorWindow,
    RoomTypeAssetReviewWizard,
    AssetsEditorWindow,
    AssetCategoriesEditorWindow,
    ScenarioGroupManagerDialog,
    RoomTypeAssetScenarioDialog,
    AssetCapabilityOverlapDialog,
)
from advanced_dialogs import (
    ConnectionEditorWindow,
    DataPointDepartmentsBulkDialog,
    FloorTemplateCopyDialog,
    LocationDepartmentsBulkDialog,
    RouteProfilesEditorV2,
)
from models import JsonStore
from asset_condensation import (
    condense_assets as apply_asset_condensation,
    create_condensation_rfis,
    expand_asset as apply_asset_expansion,
)
from room_type_condensation import condense_room_types as apply_room_type_condensation
from room_type_asset_staging import (
    build_commit as build_room_type_asset_commit,
    clean_assignment_rows,
    staged_changes as room_type_asset_staged_changes,
    update_staging as update_room_type_asset_staging,
    resolve_rfis_with_commit,
    remember_room_type_revision_change,
    should_mirror_rfi_audit_to_revision,
)

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
_AUTOROUTE_SAME_FLOOR = False
_AUTOROUTE_PREFERRED_EDGES = None
_AUTOROUTE_ROOM_CAPACITIES = None


def _init_autoroute_process(
    graph,
    points,
    comms_rooms,
    same_floor=False,
    preferred_edges=None,
    room_capacities=None,
):
    global _AUTOROUTE_GRAPH, _AUTOROUTE_POINTS, _AUTOROUTE_COMMS_ROOMS
    global _AUTOROUTE_SAME_FLOOR, _AUTOROUTE_PREFERRED_EDGES
    global _AUTOROUTE_ROOM_CAPACITIES
    _AUTOROUTE_GRAPH = graph
    _AUTOROUTE_POINTS = points
    _AUTOROUTE_COMMS_ROOMS = comms_rooms
    _AUTOROUTE_SAME_FLOOR = bool(same_floor)
    _AUTOROUTE_PREFERRED_EDGES = set(preferred_edges or ())
    _AUTOROUTE_ROOM_CAPACITIES = dict(room_capacities or {})


def _autoroute_edge_key(first, second):
    return tuple(sorted((str(first), str(second))))


def _autoroute_shortest_path(
    graph,
    start,
    end,
    points=None,
    allowed_floor=None,
    preferred_edges=None,
):
    if start not in graph or end not in graph:
        return None, []

    preferred_edges = set(preferred_edges or ())
    heap = [(0.0, 0.0, start)]
    distances = {start: (0.0, 0.0)}
    previous = {}

    while heap:
        score, cost, node = heapq.heappop(heap)

        if (score, cost) != distances.get(node):
            continue

        if node == end:
            path = [node]
            while path[-1] != start:
                path.append(previous[path[-1]])
            path.reverse()
            return cost, path

        for next_node, weight in graph.get(node, []):
            if allowed_floor is not None and points is not None:
                next_point = points.get(next_node)
                if next_point is None or int(next_point.get("floor", 0)) != int(
                    allowed_floor
                ):
                    continue
            weight = float(weight)
            new_cost = cost + weight
            reuse_factor = (
                0.25
                if _autoroute_edge_key(node, next_node) in preferred_edges
                else 1.0
            )
            new_score = score + weight * reuse_factor
            if (new_score, new_cost) < distances.get(
                next_node, (math.inf, math.inf)
            ):
                distances[next_node] = (new_score, new_cost)
                previous[next_node] = node
                heapq.heappush(heap, (new_score, new_cost, next_node))

    return None, []


def _autoroute_existing_route_edges(graph, comms_rooms, connections):
    """Return graph edges already used by room-to-data-point connections."""
    room_names = set(comms_rooms)
    result = set()
    unresolved = {}

    for connection in connections:
        if not isinstance(connection, dict):
            continue
        left = str(connection.get("from", "") or "").strip()
        right = str(connection.get("to", "") or "").strip()
        if left in room_names:
            room_name, target_name = left, right
        elif right in room_names:
            room_name, target_name = right, left
        else:
            continue

        route_path = [
            str(value).strip()
            for value in connection.get("route_path", [])
            if str(value).strip()
        ]
        route_path_is_valid = len(route_path) >= 2 and all(
            first in graph
            and any(next_node == second for next_node, _weight in graph[first])
            for first, second in zip(route_path, route_path[1:])
        )
        if route_path_is_valid:
            result.update(
                _autoroute_edge_key(first, second)
                for first, second in zip(route_path, route_path[1:])
            )
        elif room_name in graph and target_name in graph:
            unresolved.setdefault(room_name, set()).add(target_name)

    # Older autoroutes did not save route_path. Build one shortest-path tree per
    # room so their paths can still guide new routes without running a search
    # separately for every connected data point.
    for room_name, targets in unresolved.items():
        heap = [(0.0, room_name)]
        distances = {room_name: 0.0}
        previous = {}
        remaining = set(targets)
        while heap and remaining:
            cost, node = heapq.heappop(heap)
            if cost > distances.get(node, math.inf):
                continue
            remaining.discard(node)
            for next_node, weight in graph.get(node, []):
                new_cost = cost + float(weight)
                if new_cost < distances.get(next_node, math.inf):
                    distances[next_node] = new_cost
                    previous[next_node] = node
                    heapq.heappush(heap, (new_cost, next_node))

        for target_name in targets - remaining:
            node = target_name
            while node != room_name and node in previous:
                parent = previous[node]
                result.add(_autoroute_edge_key(parent, node))
                node = parent

    return result


def _autoroute_route_score(graph, route_path, preferred_edges):
    """Score a route with already-used edges discounted, preserving real length."""
    preferred_edges = set(preferred_edges or ())
    score = 0.0
    for first, second in zip(route_path, route_path[1:]):
        weight = next(
            (
                float(value)
                for next_node, value in graph.get(first, [])
                if next_node == second
            ),
            0.0,
        )
        if _autoroute_edge_key(first, second) in preferred_edges:
            weight *= 0.25
        score += weight
    return score


def _autoroute_data_point_worker(data_point):
    graph = _AUTOROUTE_GRAPH
    points = _AUTOROUTE_POINTS
    comms_rooms = _AUTOROUTE_COMMS_ROOMS
    same_floor = bool(_AUTOROUTE_SAME_FLOOR)
    preferred_edges = set(_AUTOROUTE_PREFERRED_EDGES or ())
    room_capacities = dict(_AUTOROUTE_ROOM_CAPACITIES or {})

    point_name = str(data_point.get("name", "")).strip()
    if not point_name:
        return {"status": "skip_empty"}

    if point_name not in points:
        return {"status": "unreachable", "point_name": point_name}

    best_room = None
    best_cost = None
    best_priority = None
    best_selection_score = None
    best_route_path = []
    room_candidates = []
    point_floor = int(points[point_name].get("floor", 0))

    for comms_room in comms_rooms:
        if comms_room not in points:
            continue
        if same_floor and int(points[comms_room].get("floor", 0)) != point_floor:
            continue

        route_cost, _route_path = _autoroute_shortest_path(
            graph,
            point_name,
            comms_room,
            points=points if same_floor else None,
            allowed_floor=point_floor if same_floor else None,
            preferred_edges=preferred_edges,
        )
        if route_cost is None:
            continue

        total_cost = float(route_cost) + float(
            data_point.get("extension_distance_m", 0.0) or 0.0
        )
        selection_score = _autoroute_route_score(
            graph, _route_path, preferred_edges
        ) + float(data_point.get("extension_distance_m", 0.0) or 0.0)
        room_distance_limit = max(
            0.1,
            float(points[comms_room].get("max_cable_length_m", 90.0) or 90.0),
        )
        if total_cost > room_distance_limit + 1e-9 and preferred_edges:
            # Reuse is a preference, not permission to make an otherwise valid
            # endpoint fail its real cable-length limit.
            route_cost, _route_path = _autoroute_shortest_path(
                graph,
                point_name,
                comms_room,
                points=points if same_floor else None,
                allowed_floor=point_floor if same_floor else None,
            )
            if route_cost is not None:
                total_cost = float(route_cost) + float(
                    data_point.get("extension_distance_m", 0.0) or 0.0
                )
                selection_score = _autoroute_route_score(
                    graph, _route_path, preferred_edges
                ) + float(data_point.get("extension_distance_m", 0.0) or 0.0)
        if total_cost > room_distance_limit + 1e-9:
            continue

        # A full comms room is the preferred termination point. Distributed
        # equipment rooms are the fallback for endpoints that cannot be served
        # by a comms room under the active floor/routing constraints.
        room_kind = str(points[comms_room].get("kind", "") or "").strip().lower()
        legacy_der_name = comms_room.upper().startswith("DER")
        room_priority = (
            0 if room_kind == "comms_room" and not legacy_der_name else 1
        )
        candidate = {
            "room": comms_room,
            "cost": total_cost,
            "priority": room_priority,
            "selection_score": selection_score,
            # Connections are stored room -> data point, while this search runs
            # data point -> room.
            "route_path": list(reversed(_route_path)),
        }
        room_candidates.append(candidate)

        if (
            best_cost is None
            or (room_priority, selection_score, total_cost, comms_room)
            < (best_priority, best_selection_score, best_cost, best_room)
        ):
            best_cost = total_cost
            best_room = comms_room
            best_priority = room_priority
            best_selection_score = selection_score
            best_route_path = list(candidate["route_path"])

    if best_room is None:
        return {"status": "unreachable", "point_name": point_name}

    room_candidates.sort(
        key=lambda row: (
            int(row["priority"]),
            float(row["selection_score"]),
            float(row["cost"]),
            str(row["room"]),
        )
    )
    capacity_candidates = []
    for candidate in room_candidates:
        capacity_candidates.append(candidate)
        # Once an unlimited room is reached, later choices can never be needed
        # to resolve a capacity conflict and would only inflate worker results.
        if room_capacities.get(str(candidate["room"])) is None:
            break

    return {
        "status": "created",
        "point_name": point_name,
        "from": best_room,
        "to": point_name,
        "qty": max(
            0,
            int(
                data_point.get(
                    "_required_ports", data_point.get("qty", 1)
                )
                or 0
            ),
        ),
        "route_profile": "",
        "cost": best_cost,
        "route_path": best_route_path,
        "room_candidates": capacity_candidates,
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

        # DXF parsing produces a large Python entity graph and returning it from
        # a child process briefly holds both the worker copy and the unpickled
        # main-process copy.  Starting one worker per floor exhausted memory on
        # multi-floor projects (typically one floor completed and the rest of
        # the process pool died).  Two concurrent parsers retain useful I/O/CPU
        # overlap without multiplying that peak across every mapped drawing.
        worker_count = min(len(jobs), 2)

        try:
            with ProcessPoolExecutor(max_workers=worker_count) as pool:
                futures = {
                    pool.submit(_load_dxf_floor_process, job): job for job in jobs
                }

                for future in as_completed(futures):
                    job_floor, job_path = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        # A failed child process used to abort the result loop,
                        # leaving the progress dialog permanently at e.g. 5/6.
                        self.failed.emit(int(job_floor), str(job_path), str(exc))
                        continue
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


class LayerVisibilityDialog(QDialog):
    """Batch layer visibility editor.

    The checkboxes in this dialog are deliberately disconnected from the live
    ribbon controls until Apply/OK is pressed.  This lets several layers be
    changed with one renderer invalidation instead of causing a full refresh for
    every click.
    """

    LAYER_SPECS = [
        ("Drawing", "DXF background", "show_dxf_check"),
        ("Drawing", "Labels", "show_labels_check"),
        ("Routing graph", "Edges", "show_edges_check"),
        ("Routing graph", "Corridor nodes", "show_nodes_check"),
        ("Routing graph", "Data points", "show_data_points_check"),
        ("Routing graph", "Locations", "show_locations_check"),
        ("Routing graph", "Comms rooms", "show_comms_rooms_check"),
        ("Routing graph", "Equipment room placement zones", "show_placement_zones_check"),
        ("Routing graph", "Departments", "show_departments_check"),
        ("Network", "Network planning", "show_network_check"),
        ("Network", "Network assets", "show_network_assets_check"),
        ("Network", "Network links", "show_network_connections_check"),
        ("Network", "Wireless devices", "show_wireless_devices_check"),
        ("Network", "Physical fibre", "show_physical_fibre_check"),
    ]

    def __init__(self, editor, parent=None):
        super().__init__(parent or editor)
        self.editor = editor
        self.setWindowTitle("Drawing Layers")
        self.setModal(True)
        self.resize(440, 520)
        self._rows = []

        root = QVBoxLayout(self)
        intro = QLabel(
            "Select all required layer changes, then press Apply. "
            "The main viewer redraws once for the whole batch."
        )
        intro.setWordWrap(True)
        root.addWidget(intro)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(8, 8, 8, 8)
        container_layout.setSpacing(8)

        current_group = None
        group_grid = None
        row_index = 0
        for group, label, attr_name in self.LAYER_SPECS:
            target = getattr(editor, attr_name, None)
            if target is None or not hasattr(target, "isChecked"):
                continue
            if group != current_group:
                current_group = group
                title = QLabel(group)
                title_font = title.font()
                title_font.setBold(True)
                title.setFont(title_font)
                container_layout.addWidget(title)
                group_widget = QWidget()
                group_grid = QGridLayout(group_widget)
                group_grid.setContentsMargins(12, 0, 0, 0)
                group_grid.setHorizontalSpacing(12)
                group_grid.setVerticalSpacing(6)
                container_layout.addWidget(group_widget)
                row_index = 0

            check = QCheckBox(label)
            check.setChecked(bool(target.isChecked()))
            group_grid.addWidget(check, row_index // 2, row_index % 2)
            row_index += 1
            self._rows.append((target, check))

        container_layout.addStretch(1)
        scroll.setWidget(container)
        root.addWidget(scroll, 1)

        batch_row = QHBoxLayout()
        show_all = QPushButton("Show all")
        hide_all = QPushButton("Hide all")
        invert = QPushButton("Invert")
        show_all.clicked.connect(lambda: self._set_all(True))
        hide_all.clicked.connect(lambda: self._set_all(False))
        invert.clicked.connect(self._invert)
        batch_row.addWidget(show_all)
        batch_row.addWidget(hide_all)
        batch_row.addWidget(invert)
        batch_row.addStretch(1)
        root.addLayout(batch_row)

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Apply | QDialogButtonBox.Cancel
        )
        buttons.button(QDialogButtonBox.Apply).clicked.connect(self.apply_changes)
        buttons.accepted.connect(self._accept_changes)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

    def _set_all(self, checked):
        for _target, check in self._rows:
            check.setChecked(bool(checked))

    def _invert(self):
        for _target, check in self._rows:
            check.setChecked(not check.isChecked())

    def apply_changes(self):
        changed = False
        for target, check in self._rows:
            value = bool(check.isChecked())
            if bool(target.isChecked()) == value:
                continue
            target.blockSignals(True)
            target.setChecked(value)
            target.blockSignals(False)
            changed = True
        if changed:
            self.editor.refresh_canvas()
            self.editor.set_status("Layer visibility updated")

    def _accept_changes(self):
        self.apply_changes()
        self.accept()


class RendererPerformanceDialog(QDialog):
    def __init__(self, editor, parent=None):
        super().__init__(parent or editor)
        self.editor = editor
        self.setWindowTitle("Main Viewer Performance")
        self.setModal(True)
        self.resize(420, 220)

        root = QVBoxLayout(self)
        form = QFormLayout()
        root.addLayout(form)

        stats = editor.canvas.render_stats() if hasattr(editor.canvas, "render_stats") else {}
        backend = QLabel(str(stats.get("backend", "Unknown")))
        backend.setWordWrap(True)
        form.addRow("Graphics backend", backend)

        self.fps_spin = QSpinBox()
        self.fps_spin.setRange(5, 120)
        self.fps_spin.setSuffix(" FPS")
        current_fps = (
            editor.canvas.target_fps()
            if hasattr(editor.canvas, "target_fps")
            else 30
        )
        self.fps_spin.setValue(int(current_fps))
        form.addRow("Maximum interactive frame rate", self.fps_spin)

        self.stats_check = QCheckBox("Show renderer statistics in the legend")
        self.stats_check.setChecked(bool(getattr(editor, "_show_render_stats", True)))
        root.addWidget(self.stats_check)

        note = QLabel(
            "The viewer is event-driven: once no layer is dirty, frame production "
            "stops instead of continuously using the GPU."
        )
        note.setWordWrap(True)
        root.addWidget(note)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._apply_and_accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

    def _apply_and_accept(self):
        if hasattr(self.editor.canvas, "set_target_fps"):
            self.editor.canvas.set_target_fps(self.fps_spin.value())
        self.editor._show_render_stats = bool(self.stats_check.isChecked())
        self.editor.refresh_canvas()
        self.editor.set_status(
            f"Viewer limited to {self.fps_spin.value()} FPS"
        )
        self.accept()


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
    mouseMoved = Signal(object, float, float)

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

        self.setOptimizationFlag(QGraphicsView.DontSavePainterState, True)
        self.setOptimizationFlag(QGraphicsView.DontAdjustForAntialiasing, True)
        self.setViewportUpdateMode(QGraphicsView.BoundingRectViewportUpdate)
        self.setCacheMode(QGraphicsView.CacheBackground)

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
        self.mouseMoved.emit(event, scene_pos.x(), scene_pos.y())
        if self._middle_panning and self._last_middle_pos is not None:
            self.middleDragged.emit(event)
        if event.buttons() & Qt.LeftButton:
            self.mouseDragged.emit(event, scene_pos.x(), scene_pos.y())
        super().mouseMoveEvent(event)

    def wheelEvent(self, event):
        super().wheelEvent(event)
        self.mouseWheelScrolled.emit(event)

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


class RoomTypeCountsDialog(QDialog):
    """Show placed-room counts for every room type in the current model."""

    navigateRequested = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Room Type Counts")
        self.setModal(False)
        self.resize(760, 560)
        self._rows = []

        layout = QVBoxLayout(self)

        description = QLabel(
            "Counts are based on placed data points that have a room_type_id. "
            "Double-click a populated room type to navigate to the nearest matching room."
        )
        description.setWordWrap(True)
        layout.addWidget(description)

        search_row = QHBoxLayout()
        layout.addLayout(search_row)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText(
            "Type to filter room type ID, name, count or floor..."
        )
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.textChanged.connect(self._apply_filter)
        search_row.addWidget(QLabel("Search"))
        search_row.addWidget(self.search_edit, 1)

        self.match_label = QLabel("0 room types")
        search_row.addWidget(self.match_label)

        self.summary_label = QLabel()
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(
            ["Room type ID", "Room type", "Placed rooms", "Floors"]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        self.table.itemDoubleClicked.connect(self._navigate_selected_row)
        layout.addWidget(self.table, 1)

        button_row = QHBoxLayout()
        layout.addLayout(button_row)

        refresh_btn = QPushButton("Refresh")
        navigate_btn = QPushButton("Navigate to nearest")
        close_btn = QPushButton("Close")

        refresh_btn.clicked.connect(self.refresh_from_parent)
        navigate_btn.clicked.connect(self._navigate_selected_row)
        close_btn.clicked.connect(self.close)

        button_row.addWidget(refresh_btn)
        button_row.addWidget(navigate_btn)
        button_row.addStretch(1)
        button_row.addWidget(close_btn)

    @staticmethod
    def _floor_summary(floor_counts):
        if not floor_counts:
            return "-"
        return ", ".join(
            f"F{floor}: {count}"
            for floor, count in sorted(floor_counts.items(), key=lambda item: item[0])
        )

    def set_model_data(self, data):
        data = data if isinstance(data, dict) else {}

        room_types = {}
        for room_type in data.get("room_types", []):
            if not isinstance(room_type, dict):
                continue
            room_type_id = str(room_type.get("id", "")).strip()
            if not room_type_id:
                continue
            room_types[room_type_id] = (
                str(room_type.get("name", room_type_id)).strip() or room_type_id
            )

        counts = {
            room_type_id: {"count": 0, "floors": {}}
            for room_type_id in room_types
        }
        unassigned_count = 0
        total_rooms = 0

        for point in data.get("data_points", []):
            if not isinstance(point, dict):
                continue
            total_rooms += 1
            room_type_id = str(point.get("room_type_id", "") or "").strip()
            if not room_type_id:
                unassigned_count += 1
                continue

            record = counts.setdefault(room_type_id, {"count": 0, "floors": {}})
            record["count"] += 1
            try:
                floor = int(point.get("floor", 0))
            except (TypeError, ValueError):
                floor = 0
            record["floors"][floor] = record["floors"].get(floor, 0) + 1

        rows = []
        all_ids = sorted(
            set(room_types) | set(counts),
            key=lambda room_type_id: (
                room_types.get(room_type_id, room_type_id).lower(),
                room_type_id.lower(),
            ),
        )
        for room_type_id in all_ids:
            record = counts.get(room_type_id, {"count": 0, "floors": {}})
            rows.append(
                {
                    "id": room_type_id,
                    "name": room_types.get(
                        room_type_id,
                        "(missing room type definition)",
                    ),
                    "count": int(record.get("count", 0)),
                    "floors": dict(record.get("floors", {})),
                }
            )

        self._rows = rows
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)

        for record in rows:
            row = self.table.rowCount()
            self.table.insertRow(row)

            id_item = QTableWidgetItem(record["id"])
            id_item.setData(Qt.UserRole, record["id"])

            name_item = QTableWidgetItem(record["name"])
            count_item = QTableWidgetItem()
            count_item.setData(Qt.DisplayRole, record["count"])
            count_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            floor_item = QTableWidgetItem(self._floor_summary(record["floors"]))

            search_text = " ".join(
                [
                    record["id"],
                    record["name"],
                    str(record["count"]),
                    floor_item.text(),
                ]
            ).lower()
            id_item.setData(Qt.UserRole + 1, search_text)

            self.table.setItem(row, 0, id_item)
            self.table.setItem(row, 1, name_item)
            self.table.setItem(row, 2, count_item)
            self.table.setItem(row, 3, floor_item)

        self.table.setSortingEnabled(True)
        self.table.sortItems(1, Qt.AscendingOrder)

        assigned_count = total_rooms - unassigned_count
        used_defined_count = sum(
            1
            for room_type_id in room_types
            if counts.get(room_type_id, {}).get("count", 0) > 0
        )
        unknown_assignment_count = sum(
            int(record.get("count", 0))
            for room_type_id, record in counts.items()
            if room_type_id not in room_types
        )
        defined_count = len(room_types)
        summary = (
            f"Total placed rooms/data points: {total_rooms} | "
            f"Assigned to a room type: {assigned_count} | "
            f"Unassigned/manual: {unassigned_count} | "
            f"Defined room types used: {used_defined_count} of {defined_count}"
        )
        if unknown_assignment_count:
            summary += f" | Missing room-type definitions: {unknown_assignment_count}"
        self.summary_label.setText(summary)
        self._apply_filter()

    def refresh_from_parent(self):
        parent = self.parent()
        store = getattr(parent, "store", None)
        data = getattr(store, "data", {}) if store is not None else {}
        self.set_model_data(data)

    def _apply_filter(self, *_):
        terms = [
            term
            for term in self.search_edit.text().strip().lower().split()
            if term
        ]
        visible_count = 0

        for row in range(self.table.rowCount()):
            id_item = self.table.item(row, 0)
            haystack = str(id_item.data(Qt.UserRole + 1) or "") if id_item else ""
            visible = all(term in haystack for term in terms)
            self.table.setRowHidden(row, not visible)
            if visible:
                visible_count += 1

        self.match_label.setText(
            f"{visible_count} room type{'s' if visible_count != 1 else ''}"
        )

    def _navigate_selected_row(self, *_):
        row = self.table.currentRow()
        if row < 0 or self.table.isRowHidden(row):
            return

        id_item = self.table.item(row, 0)
        count_item = self.table.item(row, 2)
        if id_item is None or count_item is None:
            return

        room_type_id = str(id_item.data(Qt.UserRole) or "").strip()
        try:
            count = int(count_item.data(Qt.DisplayRole) or count_item.text() or 0)
        except (TypeError, ValueError):
            count = 0

        if not room_type_id or count <= 0:
            return

        self.navigateRequested.emit(room_type_id)


class RevisionHistoryDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Revision History")
        self.setModal(False)
        self.resize(980, 520)

        layout = QVBoxLayout(self)

        self.summary_label = QLabel()
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(
            ["Revision", "Saved", "Notes", "Changed", "Deleted", "Indexed records"]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.setWordWrap(True)
        self.table.verticalHeader().setVisible(False)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        layout.addWidget(self.table, 1)

        button_row = QHBoxLayout()
        layout.addLayout(button_row)
        refresh_btn = QPushButton("Refresh")
        export_btn = QPushButton("Export PDF")
        close_btn = QPushButton("Close")
        refresh_btn.clicked.connect(self.refresh_from_parent)
        export_btn.clicked.connect(self._export_pdf)
        close_btn.clicked.connect(self.close)
        button_row.addWidget(refresh_btn)
        button_row.addWidget(export_btn)
        button_row.addStretch(1)
        button_row.addWidget(close_btn)

    def refresh_from_parent(self):
        parent = self.parent()
        revisions = []
        if parent is not None and hasattr(parent, "store"):
            try:
                revisions = parent.store.revision_history()
            except Exception as exc:
                QMessageBox.critical(self, "Revision history failed", str(exc))
                return
        self.set_revisions(revisions)

    def set_revisions(self, revisions):
        revisions = list(revisions or [])
        self.summary_label.setText(
            f"{len(revisions)} saved revision{'s' if len(revisions) != 1 else ''}"
        )
        self.table.setRowCount(len(revisions))
        for row, revision in enumerate(revisions):
            values = [
                revision.get("revision_number", ""),
                revision.get("created_utc", ""),
                revision.get("notes", ""),
                revision.get("changed_chunks", 0),
                revision.get("deleted_chunks", 0),
                revision.get("indexed_records", 0),
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value))
                if column in {0, 3, 4, 5}:
                    item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.table.setItem(row, column, item)
        self.table.resizeRowsToContents()

    def _export_pdf(self):
        parent = self.parent()
        if parent is not None and hasattr(parent, "export_revision_history_pdf"):
            parent.export_revision_history_pdf()
            self.refresh_from_parent()


class ProjectSummaryPdfOptionsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Project Summary PDF Sections")
        self.resize(560, 480)
        self._checks = {}

        layout = QVBoxLayout(self)

        form = QFormLayout()
        layout.addLayout(form)
        default_name = "Cable Routing Project"
        if parent is not None and hasattr(parent, "store"):
            default_name = str(
                parent.store.data.get("project", {}).get("name", default_name)
                or default_name
            ).strip()
        self.project_name_edit = QLineEdit(default_name)
        form.addRow("Project name", self.project_name_edit)

        self.paper_size_combo = QComboBox()
        self.orientation_combo = QComboBox()
        try:
            from project_summary_report import (
                PROJECT_SUMMARY_PAPER_SIZES,
                PROJECT_SUMMARY_SECTIONS,
            )
            paper_sizes = list(PROJECT_SUMMARY_PAPER_SIZES.keys())
        except Exception:
            PROJECT_SUMMARY_SECTIONS = [
                ("overall_summary", "Overall summary"),
                ("room_summary", "Room type summary"),
                ("room_details", "Room asset details"),
                ("use_cases", "Use cases"),
                ("network_summary", "Network summary, topology and layers"),
                ("network_equipment", "Network equipment required"),
                ("power_draw", "Power draw and theoretical kWh"),
                ("rack_power_fibre", "Rack, power and fibre requirements"),
            ]
            paper_sizes = ["A4", "A3", "Letter", "Legal"]
        self.paper_size_combo.addItems(paper_sizes)
        if "A4" in paper_sizes:
            self.paper_size_combo.setCurrentText("A4")
        self.orientation_combo.addItems(["Landscape", "Portrait"])
        self.orientation_combo.setCurrentText("Landscape")
        form.addRow("Paper size", self.paper_size_combo)
        form.addRow("Orientation", self.orientation_combo)

        label = QLabel("Select the sections to include.")
        label.setWordWrap(True)
        layout.addWidget(label)

        for section_id, section_label in PROJECT_SUMMARY_SECTIONS:
            check = QCheckBox(section_label)
            check.setChecked(True)
            check.stateChanged.connect(self._update_buttons)
            layout.addWidget(check)
            self._checks[section_id] = check

        button_row = QHBoxLayout()
        layout.addLayout(button_row)
        all_btn = QPushButton("Select All")
        none_btn = QPushButton("Clear")
        all_btn.clicked.connect(self._select_all)
        none_btn.clicked.connect(self._clear_all)
        button_row.addWidget(all_btn)
        button_row.addWidget(none_btn)
        button_row.addStretch(1)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)
        self._update_buttons()

    def selected_sections(self):
        return [
            section_id
            for section_id, check in self._checks.items()
            if check.isChecked()
        ]

    def report_options(self):
        options = {
            "project_name": self.project_name_edit.text().strip(),
            "paper_size": self.paper_size_combo.currentText(),
            "orientation": self.orientation_combo.currentText(),
        }
        parent = self.parent()
        if parent is not None and hasattr(parent, "latest_project_revision_number"):
            revision_number = parent.latest_project_revision_number()
            if revision_number:
                options["revision_number"] = revision_number
        return options

    def _select_all(self):
        for check in self._checks.values():
            check.setChecked(True)
        self._update_buttons()

    def _clear_all(self):
        for check in self._checks.values():
            check.setChecked(False)
        self._update_buttons()

    def _update_buttons(self):
        ok_button = self.buttons.button(QDialogButtonBox.Ok)
        if ok_button is not None:
            ok_button.setEnabled(bool(self.selected_sections()))


class FloorPlanPdfOptionsDialog(QDialog):
    """Collect floor selection plus exact sheet and scale settings."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Export Floor Plans PDF")
        self.resize(500, 460)
        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.paper_size_combo = QComboBox()
        self.paper_size_combo.addItems(["A0", "A1", "A2"])
        self.scale_spin = QSpinBox()
        self.scale_spin.setRange(10, 5000)
        self.scale_spin.setPrefix("1:")
        self.scale_spin.setSingleStep(50)

        self.current_floor = 0
        self.available_floors = []
        if parent is not None:
            if hasattr(parent, "floor_spin"):
                self.current_floor = int(parent.floor_spin.value())
            if hasattr(parent, "_all_model_floors"):
                self.available_floors = [
                    int(floor) for floor in parent._all_model_floors()
                ]
        if not self.available_floors:
            self.available_floors = [self.current_floor]

        self.floor_scope_combo = QComboBox()
        self.floor_scope_combo.addItem("All model floors", "all")
        self.floor_scope_combo.addItem(
            f"Current floor ({self.current_floor})", "current"
        )
        self.floor_scope_combo.addItem("Selected floors", "selected")
        self.floor_list = QListWidget()
        self.floor_list.setMaximumHeight(170)

        saved = {}
        if parent is not None and hasattr(parent, "store"):
            saved = parent.store.data.get("floor_plan_pdf_settings", {}) or {}
        paper_size = str(saved.get("paper_size", "A1") or "A1").upper()
        paper_index = self.paper_size_combo.findText(paper_size)
        if paper_index >= 0:
            self.paper_size_combo.setCurrentIndex(paper_index)
        self.scale_spin.setValue(max(10, int(saved.get("scale", 100) or 100)))
        saved_scope = str(saved.get("floor_scope", "all") or "all")
        scope_index = self.floor_scope_combo.findData(saved_scope)
        self.floor_scope_combo.setCurrentIndex(max(0, scope_index))
        saved_floors = {
            int(floor) for floor in saved.get("selected_floors", []) or []
        }
        for floor in self.available_floors:
            item = QListWidgetItem(f"Floor {floor}")
            item.setData(Qt.UserRole, int(floor))
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(
                Qt.Checked
                if not saved_floors or floor in saved_floors
                else Qt.Unchecked
            )
            self.floor_list.addItem(item)

        form.addRow("Floor scope", self.floor_scope_combo)
        form.addRow("Floors", self.floor_list)
        form.addRow("Paper size (landscape)", self.paper_size_combo)
        form.addRow("Printed scale", self.scale_spin)

        note = QLabel(
            "Each chosen floor is exported to a separate page at the exact selected "
            "scale. If a floor cannot fit, the exporter reports the minimum scale "
            "needed instead of clipping the drawing. Model coordinates are treated "
            "as metres."
        )
        note.setWordWrap(True)
        layout.addWidget(note)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Ok).setText("Export PDF")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.floor_scope_combo.currentIndexChanged.connect(
            self._update_floor_selection_state
        )
        self._update_floor_selection_state()

    def _update_floor_selection_state(self):
        self.floor_list.setEnabled(
            self.floor_scope_combo.currentData() == "selected"
        )

    def _checked_floors(self):
        return [
            int(self.floor_list.item(index).data(Qt.UserRole))
            for index in range(self.floor_list.count())
            if self.floor_list.item(index).checkState() == Qt.Checked
        ]

    def selected_floors(self):
        scope = str(self.floor_scope_combo.currentData() or "all")
        if scope == "current":
            return [self.current_floor]
        if scope == "selected":
            return self._checked_floors()
        return list(self.available_floors)

    def accept(self):
        if not self.selected_floors():
            QMessageBox.warning(
                self,
                "Select floors",
                "Select at least one floor to include in the PDF.",
            )
            return
        super().accept()

    def export_options(self):
        return {
            "paper_size": self.paper_size_combo.currentText(),
            "scale": int(self.scale_spin.value()),
            "floor_scope": str(self.floor_scope_combo.currentData() or "all"),
            "selected_floors": self._checked_floors(),
            "floors": self.selected_floors(),
        }


class EquipmentRoomExtentsPdfOptionsDialog(QDialog):
    """Collect page and exact DXF scale settings for room extent sheets."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Export Equipment Room Extents PDF")
        self.resize(500, 240)
        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)
        self.paper_size_combo = QComboBox()
        self.paper_size_combo.addItems(["A0", "A1", "A2", "A3", "A4"])
        self.scale_spin = QSpinBox()
        self.scale_spin.setRange(10, 5000)
        self.scale_spin.setPrefix("1:")
        self.scale_spin.setSingleStep(50)
        saved = {}
        if parent is not None and hasattr(parent, "store"):
            saved = parent.store.data.get("equipment_room_extents_pdf_settings", {}) or {}
        paper = str(saved.get("paper_size", "A1") or "A1").upper()
        index = self.paper_size_combo.findText(paper)
        if index >= 0:
            self.paper_size_combo.setCurrentIndex(index)
        self.scale_spin.setValue(max(10, int(saved.get("scale", 100) or 100)))
        form.addRow("Paper size (landscape)", self.paper_size_combo)
        form.addRow("DXF printed scale", self.scale_spin)
        note = QLabel(
            "Creates one scaled sheet per comms room or DER. Each sheet shows the "
            "DXF background, maximum reachable graph, current served routes, extent "
            "boundary, drawing key, title block and furthest served data-point tag."
        )
        note.setWordWrap(True)
        layout.addWidget(note)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Ok).setText("Export PDF")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def export_options(self):
        return {
            "paper_size": self.paper_size_combo.currentText(),
            "scale": int(self.scale_spin.value()),
        }


class ZoneDesignOptionsPdfOptionsDialog(QDialog):
    """Collect sheet settings for the zone design comparison report."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Export Zone Design Options PDF")
        self.resize(490, 220)
        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)
        self.paper_size_combo = QComboBox()
        self.paper_size_combo.addItems(["A0", "A1", "A2", "A3", "A4"])
        self.scale_spin = QSpinBox()
        self.scale_spin.setRange(10, 5000)
        self.scale_spin.setPrefix("1:")
        self.scale_spin.setSingleStep(50)
        self.floor_scope_combo = QComboBox()
        self.floor_scope_combo.addItem("All floors", "all")
        floors = []
        if parent is not None and hasattr(parent, "_all_model_floors"):
            floors = list(parent._all_model_floors())
        for floor in floors:
            self.floor_scope_combo.addItem(f"Floor {int(floor)} only", int(floor))
        saved = {}
        if parent is not None and hasattr(parent, "store"):
            saved = parent.store.data.get("zone_design_options_pdf_settings", {}) or {}
        paper = str(saved.get("paper_size", "A1") or "A1").upper()
        index = self.paper_size_combo.findText(paper)
        if index >= 0:
            self.paper_size_combo.setCurrentIndex(index)
        self.scale_spin.setValue(max(10, int(saved.get("scale", 100) or 100)))
        saved_scope = saved.get("floor_scope", "all")
        scope_index = self.floor_scope_combo.findData(saved_scope)
        if scope_index < 0 and str(saved_scope).lstrip("-").isdigit():
            scope_index = self.floor_scope_combo.findData(int(saved_scope))
        if scope_index >= 0:
            self.floor_scope_combo.setCurrentIndex(scope_index)
        form.addRow("Paper size (landscape)", self.paper_size_combo)
        form.addRow("DXF printed scale", self.scale_spin)
        form.addRow("Floors to export", self.floor_scope_combo)
        note = QLabel(
            "Exports every generated zone-placement option without applying it. "
            "The first page compares the options; following sheets show each "
            "option and floor against the mapped DXF at the selected scale."
        )
        note.setWordWrap(True)
        layout.addWidget(note)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Ok).setText("Open Report Studio")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def export_options(self):
        return {
            "paper_size": self.paper_size_combo.currentText(),
            "scale": int(self.scale_spin.value()),
            "floor_scope": self.floor_scope_combo.currentData(),
        }


class ZoneDesignOptionsSelectionDialog(QDialog):
    """Choose a generated design or export every option for comparison."""

    def __init__(self, option_labels, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Zone-based Design Options")
        self.resize(1120, 720)
        self.setMinimumSize(920, 600)
        self.result_action = ""
        layout = QVBoxLayout(self)
        intro = QLabel(
            "Select the zone-placement design to apply, or open Report Studio to "
            "review and customise all option pages before exporting the PDF."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)
        self.options_list = QListWidget()
        self.options_list.setWordWrap(True)
        self.options_list.setSpacing(8)
        self.options_list.setTextElideMode(Qt.ElideNone)
        self.options_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.options_list.setSelectionMode(QAbstractItemView.SingleSelection)
        self.options_list.setStyleSheet(
            "QListWidget::item { padding: 12px; border: 1px solid #c9d2d8; "
            "border-radius: 5px; color: #17212b; background: #ffffff; }"
            "QListWidget::item:selected { border: 2px solid #0d6efd; "
            "color: #102a3a; background: #e7f1ff; }"
        )
        for label in option_labels:
            text = str(label)
            item = QListWidgetItem(text)
            item.setToolTip(text)
            item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
            line_count = max(6, text.count("\n") + 1)
            item.setSizeHint(QSize(0, 32 + line_count * 22))
            self.options_list.addItem(item)
        if self.options_list.count():
            self.options_list.setCurrentRow(0)
        layout.addWidget(self.options_list, 1)
        button_row = QHBoxLayout()
        self.export_button = QPushButton("Open Report Studio")
        self.apply_button = QPushButton("Apply selected option")
        self.cancel_button = QPushButton("Cancel")
        button_row.addWidget(self.export_button)
        button_row.addStretch(1)
        button_row.addWidget(self.apply_button)
        button_row.addWidget(self.cancel_button)
        layout.addLayout(button_row)
        self.export_button.clicked.connect(self._export)
        self.apply_button.clicked.connect(self._apply)
        self.cancel_button.clicked.connect(self.reject)

    def _export(self):
        self.result_action = "export"
        self.accept()

    def _apply(self):
        if self.options_list.currentRow() < 0:
            return
        self.result_action = "apply"
        self.accept()

    def selected_index(self):
        return max(0, int(self.options_list.currentRow()))


_NETWORK_UNDO_KEYS = (
    "network_settings", "network_assets", "network_asset_instances",
    "network_racks", "network_connections", "network_endpoint_assignments",
    "network_patch_leads", "network_redundancy_groups",
    "network_power_connections", "network_vlans", "network_routes",
    "network_ip_allocations", "network_external_networks",
    "network_optic_modules", "network_optical_paths",
    "network_fibre_cable_types", "network_fibre_cables",
    "network_fibre_nodes", "network_fibre_splices",
    "network_design_summary", "locations", "assets",
)


class CableRouteEditor(QMainWindow):
    _request_dxf_batch_load = Signal(object)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cable Routing Graph Editor")
        self.setMinimumSize(600, 420)

        self._render_data_revision = 0
        self._last_overlay_signature = None
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
        self._measure_data_point_name = None
        self.selected_point_name = None
        self.selected_template_names = set()
        self.selection_clipboard = None
        self.dragging_point_name = None
        self.drag_mode_active = False
        self.multi_drag_names = []
        self.multi_drag_start_positions = {}
        self.multi_drag_anchor_name = None
        self.multi_drag_anchor_start = None
        self.alt_move_locked = False
        self.selection_rect_active = False
        self.selection_rect_origin = None
        self.selection_rect_current = None
        self._rubber_band = None
        self.edge_delete_start = None
        self._item_lookup = {}
        self._point_item_lookup = {}
        self._static_scene_items = []
        self._static_scene_key = None
        self._static_scene_visible_rect = None
        self.bulk_location_session = None
        self.bulk_data_point_session = None
        self.placement_zone_start = None
        self.selected_placement_zone_id = None
        self.dragging_placement_zone_id = None
        self.dragging_placement_zone_handle = None
        self.placement_zone_drag_start = None
        self.placement_zone_drag_original = None
        self._pinned_equipment_room_extent_name = None
        self._comms_optimisation_dialog = None
        self._clear_canvas_multi_selection()

        self._unassigned_dp_dialog = None
        self._unassigned_dp_names = []
        self._unassigned_dp_index = -1

        self._manual_room_type_dp_dialog = None
        self._manual_room_type_dp_names = []
        self._manual_room_type_dp_index = -1

        self._find_dp_dialog = None
        self._find_dp_matches = []
        self._find_dp_index = -1
        self._room_type_counts_dialog = None
        self._room_type_asset_review_dialog = None

        self._viewport_refresh_timer = QTimer(self)
        self._viewport_refresh_timer.setSingleShot(True)
        self._viewport_refresh_timer.timeout.connect(self.refresh_canvas)
        self._show_render_stats = True

        self._ribbon_buttons = []
        self._ribbon_groups = []
        self._ribbon_scroll_areas = []
        self._responsive_compact = None
        self._responsive_layout_timer = QTimer(self)
        self._responsive_layout_timer.setSingleShot(True)
        self._responsive_layout_timer.setInterval(60)
        self._responsive_layout_timer.timeout.connect(self._apply_responsive_layout)

        self._build_ui()
        self._fit_initial_window_to_screen()
        self._apply_responsive_layout(force=True)
        self.refresh_canvas()

    def _invalidate_static_scene_cache(self):
        if hasattr(self, "canvas") and hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()

    def _selected_visible_drag_names(self):
        floor = self.floor_spin.value()
        points = self.store.points_for_floor(floor)
        result = []

        for name in sorted(self.selected_template_names):
            point = points.get(name)
            if not point:
                continue

            point = {**point, "name": name}
            if not self._is_point_kind_visible(point):
                continue

            if str(point.get("kind", "")).strip() not in {
                "corridor_node",
                "data_point",
            }:
                continue

            result.append(name)

        return result

    def _begin_multi_drag(self, picked):
        points = self.store.all_points()

        if picked in self.selected_template_names:
            names = self._selected_visible_drag_names()
        else:
            names = [picked]

        if picked not in names:
            names = [picked]

        self.multi_drag_names = names
        self.multi_drag_anchor_name = picked

        anchor = points.get(picked)
        if not anchor:
            self.multi_drag_anchor_start = None
            self.multi_drag_start_positions = {}
            return

        self.multi_drag_anchor_start = (
            float(anchor.get("x", 0.0)),
            float(anchor.get("y", 0.0)),
        )

        self.multi_drag_start_positions = {
            name: (
                float(points[name].get("x", 0.0)),
                float(points[name].get("y", 0.0)),
            )
            for name in names
            if name in points
        }

    def _clear_multi_drag(self):
        self.multi_drag_names = []
        self.multi_drag_start_positions = {}
        self.multi_drag_anchor_name = None
        self.multi_drag_anchor_start = None

    def push_undo_state(self, label="Change"):
        self._render_data_revision += 1
        self.undo_stack.append(
            {
                "label": label,
                "scope": "project",
                "data": deepcopy(self.store.data),
            }
        )
        if len(self.undo_stack) > self.max_undo_steps:
            self.undo_stack.pop(0)
        self.redo_stack.clear()

    def _network_undo_snapshot(self) -> bytes:
        payload = {
            key: self.store.data.get(key)
            for key in _NETWORK_UNDO_KEYS
            if key in self.store.data
        }
        return zlib.compress(
            pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL),
            level=1,
        )

    def _restore_network_undo_snapshot(self, snapshot: bytes) -> None:
        payload = pickle.loads(zlib.decompress(snapshot))
        for key in _NETWORK_UNDO_KEYS:
            if key in payload:
                self.store.data[key] = payload[key]
            elif key in self.store.data:
                self.store.data.pop(key, None)

    def push_network_undo_state(self, label="Network change"):
        """Store a compact network-only history state.

        Network dialogs previously deep-copied the complete project, including
        corridor geometry and tens of thousands of room/data-point records,
        before every edit. A compressed network snapshot keeps undo support
        without copying the unrelated 90 MB project model.
        """
        self._render_data_revision += 1
        self.undo_stack.append(
            {
                "label": label,
                "scope": "network",
                "data": self._network_undo_snapshot(),
            }
        )
        if len(self.undo_stack) > self.max_undo_steps:
            self.undo_stack.pop(0)
        self.redo_stack.clear()

    def _after_history_restore(self) -> None:
        self._render_data_revision += 1
        self.selected_point_name = None
        self.selected_template_names.clear()
        self.selected_for_edge = None
        self._measure_data_point_name = None
        self._clear_data_room_measurement_overlay()
        self.edge_delete_start = None
        self.selected_placement_zone_id = None
        self.dragging_placement_zone_id = None
        self.dragging_placement_zone_handle = None
        self.placement_zone_drag_start = None
        self.placement_zone_drag_original = None
        if hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()
        self.refresh_canvas()

    def undo(self):
        if not self.undo_stack:
            self.set_status("Nothing to undo")
            return
        state = self.undo_stack.pop()
        if state.get("scope") == "network":
            self.redo_stack.append(
                {
                    "label": state.get("label", "Network change"),
                    "scope": "network",
                    "data": self._network_undo_snapshot(),
                }
            )
            self._restore_network_undo_snapshot(state["data"])
        else:
            self.redo_stack.append(
                {
                    "label": state.get("label", "Change"),
                    "scope": "project",
                    "data": deepcopy(self.store.data),
                }
            )
            self.store.data = deepcopy(state["data"])
        self._after_history_restore()
        self.set_status(f"Undid: {state.get('label', 'Change')}")

    def redo(self):
        if not self.redo_stack:
            self.set_status("Nothing to redo")
            return
        state = self.redo_stack.pop()
        if state.get("scope") == "network":
            self.undo_stack.append(
                {
                    "label": state.get("label", "Network change"),
                    "scope": "network",
                    "data": self._network_undo_snapshot(),
                }
            )
            self._restore_network_undo_snapshot(state["data"])
        else:
            self.undo_stack.append(
                {
                    "label": state.get("label", "Change"),
                    "scope": "project",
                    "data": deepcopy(self.store.data),
                }
            )
            self.store.data = deepcopy(state["data"])
        self._after_history_restore()
        self.set_status("Redid change")

    def _mode_definitions(self):
        return [
            ("select_move", "Select", "select"),
            ("corridor_node", "Node", "corridor_node"),
            ("location", "Location", "location"),
            ("department", "Dept", "department"),
            ("data_point", "Data Point", "data_point"),
            ("transition", "Transition", "transition_node"),
            ("placement_zone", "Room Zone", "placement_zone"),
            ("edge", "Edge", "edge"),
            ("measure_data_room", "Measure", "measure"),
            ("pan", "Pan", "pan"),
            ("delete", "Delete", "delete"),
        ]

    def _make_mode_icon(self, icon_key, size=22):
        bootstrap_modes = {
            "select": "cursor",
            "corridor_node": "plus-square",
            "location": "geo-alt",
            "department": "building",
            "data_point": "hdd-network",
            "transition_node": "arrow-up",
            "placement_zone": "bounding-box",
            "edge": "diagram-3",
            "pan": "arrows-fullscreen",
            "delete": "trash3",
        }
        if icon_key in bootstrap_modes:
            return bootstrap_icon(bootstrap_modes[icon_key], size=size)

        pixmap = QPixmap(size, size)
        pixmap.fill(Qt.transparent)

        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing, True)

        cx = size / 2
        cy = size / 2
        r = size * 0.32

        pen = QPen(QColor("#222222"), 2)
        painter.setPen(pen)

        if icon_key == "data_point":
            painter.setBrush(QBrush(QColor("#b07cff")))
            painter.drawEllipse(QPointF(cx, cy), r, r)
            painter.drawLine(QPointF(cx - r, cy), QPointF(cx + r, cy))
            painter.drawLine(QPointF(cx, cy - r), QPointF(cx, cy + r))

        elif icon_key == "transition_node":
            painter.setBrush(QBrush(QColor("#ff7b72")))
            path = QPainterPath()
            path.moveTo(cx, cy - r)
            path.lineTo(cx + r, cy)
            path.lineTo(cx + r * 0.35, cy)
            path.lineTo(cx + r * 0.35, cy + r)
            path.lineTo(cx - r * 0.35, cy + r)
            path.lineTo(cx - r * 0.35, cy)
            path.lineTo(cx - r, cy)
            path.closeSubpath()
            painter.drawPath(path)

        elif icon_key == "location":
            painter.setBrush(QBrush(QColor("#18c37e")))
            painter.drawRoundedRect(
                int(cx - r),
                int(cy - r),
                int(r * 2),
                int(r * 2),
                5,
                5,
            )

        elif icon_key == "department":
            painter.setBrush(QBrush(QColor("#1abc9c")))
            poly = QPolygonF([
                QPointF(cx, cy - r),
                QPointF(cx + r, cy - r * 0.2),
                QPointF(cx + r * 0.65, cy + r),
                QPointF(cx - r * 0.65, cy + r),
                QPointF(cx - r, cy - r * 0.2),
            ])
            painter.drawPolygon(poly)

        elif icon_key == "corridor_node":
            painter.setBrush(QBrush(QColor("#f2c94c")))
            painter.drawRect(int(cx - r), int(cy - r), int(r * 2), int(r * 2))
            painter.drawLine(QPointF(cx - r, cy), QPointF(cx + r, cy))
            painter.drawLine(QPointF(cx, cy - r), QPointF(cx, cy + r))

        elif icon_key == "edge":
            painter.setBrush(Qt.NoBrush)
            painter.setPen(QPen(QColor("#4da3ff"), 3))
            painter.drawLine(QPointF(4, cy), QPointF(size - 5, cy))
            painter.drawLine(QPointF(size - 9, cy - 4), QPointF(size - 5, cy))
            painter.drawLine(QPointF(size - 9, cy + 4), QPointF(size - 5, cy))

        elif icon_key == "measure":
            painter.setBrush(Qt.NoBrush)
            painter.setPen(QPen(QColor("#0d6efd"), 2))
            painter.drawLine(QPointF(5, size - 6), QPointF(size - 5, 5))
            for offset in (0.28, 0.5, 0.72):
                px = 5 + (size - 10) * offset
                py = size - 6 - (size - 11) * offset
                painter.drawLine(
                    QPointF(px - 2, py - 2), QPointF(px + 2, py + 2)
                )

        elif icon_key == "select":
            painter.setBrush(QBrush(QColor("#ffffff")))
            painter.drawPolygon(QPolygonF([
                QPointF(5, 4),
                QPointF(5, size - 5),
                QPointF(11, size - 11),
                QPointF(15, size - 4),
                QPointF(18, size - 6),
                QPointF(14, size - 13),
                QPointF(size - 5, size - 13),
            ]))

        elif icon_key == "pan":
            painter.setBrush(QBrush(QColor("#cccccc")))
            painter.drawEllipse(QPointF(cx, cy), r, r)
            painter.drawLine(QPointF(cx - r, cy), QPointF(cx + r, cy))
            painter.drawLine(QPointF(cx, cy - r), QPointF(cx, cy + r))

        elif icon_key == "delete":
            painter.setPen(QPen(QColor("#ff4d4d"), 3))
            painter.drawLine(QPointF(6, 6), QPointF(size - 6, size - 6))
            painter.drawLine(QPointF(size - 6, 6), QPointF(6, size - 6))

        painter.end()
        return QIcon(pixmap)

    def _set_editor_mode(self, mode):
        previous_mode = self.mode_combo.currentText()
        if mode == "placement_zone" and hasattr(self, "show_placement_zones_check"):
            self.show_placement_zones_check.setChecked(True)
        if mode != "placement_zone":
            self.placement_zone_start = None
            self._clear_placement_zone_preview()
        if mode != "select_move":
            self.dragging_placement_zone_id = None
            self.dragging_placement_zone_handle = None
            self.placement_zone_drag_start = None
            self.placement_zone_drag_original = None
        if mode != "measure_data_room" or previous_mode != mode:
            self._measure_data_point_name = None
            self._clear_data_room_measurement_overlay()
        self.mode_combo.setCurrentText(mode)

        for button_mode, button in getattr(self, "_mode_buttons", {}).items():
            button.blockSignals(True)
            button.setChecked(button_mode == mode)
            button.blockSignals(False)

        if hasattr(self, "status_label"):
            if mode == "measure_data_room":
                self.set_status("Measure mode: click a data point first")
            else:
                self.set_status(f"Mode: {mode}")

    def _mode_icon_button(self, mode, text, icon_key):
        btn = QToolButton()
        btn.setCheckable(True)
        btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        btn.setIcon(self._make_mode_icon(icon_key))
        btn.setIconSize(QSize(18, 18))
        btn.setText(text)
        btn.setToolTip(
            "Measure routed cable distance: click a data point, then a comms "
            "room or DER"
            if mode == "measure_data_room"
            else f"Set mode: {text}"
        )
        self._configure_ribbon_button(btn)
        btn.clicked.connect(lambda checked=False, m=mode: self._set_editor_mode(m))
        return btn

    def _build_mode_buttons(self):
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(
            [mode for mode, _text, _icon in self._mode_definitions()]
        )
        self.mode_combo.setVisible(False)

        self._mode_buttons = {}
        buttons = []

        for mode, text, icon_enum in self._mode_definitions():
            btn = self._mode_icon_button(mode, text, icon_enum)
            self._mode_buttons[mode] = btn
            buttons.append(btn)

        self._set_editor_mode("select_move")
        return buttons

    def _fit_initial_window_to_screen(self):
        """Choose a useful initial size without exceeding the active desktop."""
        screen = self.screen() if hasattr(self, "screen") else None
        if screen is None:
            screen = QApplication.primaryScreen()
        available = screen.availableGeometry() if screen is not None else None
        if available is None or available.width() <= 0 or available.height() <= 0:
            self.resize(1500, 920)
            return

        target_width = min(1500, max(600, int(available.width() * 0.94)))
        target_height = min(920, max(420, int(available.height() * 0.92)))
        self.resize(
            min(target_width, available.width()),
            min(target_height, available.height()),
        )

        # On genuinely narrow logical desktops, start with the dock collapsed so
        # the drawing area remains usable. It remains available from View.
        if hasattr(self, "search_dock") and available.width() < 1100:
            self.search_dock.hide()

    def _configure_ribbon_button(self, button):
        button.setMinimumSize(96, 30)
        button.setMaximumSize(144, 34)
        button.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self._ribbon_buttons.append(button)
        return button

    def _add_scrollable_ribbon_tab(self, ribbon, content, title):
        content_layout = content.layout()
        if content_layout is not None:
            content_layout.setSizeConstraint(QLayout.SetMinimumSize)
        content.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Preferred)

        scroll = QScrollArea()
        scroll.setObjectName("RibbonTabScrollArea")
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        scroll.setWidget(content)
        self._ribbon_scroll_areas.append(scroll)
        ribbon.addTab(scroll, title)
        return scroll

    def _apply_responsive_layout(self, force=False):
        compact = self.width() < 1500 or self.height() < 850
        if not force and compact == self._responsive_compact:
            return
        self._responsive_compact = compact

        minimum_width = 96 if compact else 104
        maximum_width = 142 if compact else 154
        button_height = 30 if compact else 32
        for button in self._ribbon_buttons:
            try:
                button.setMinimumSize(minimum_width, button_height)
                button.setMaximumSize(maximum_width, button_height + 2)
                button.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
                button.setIconSize(QSize(17 if compact else 18, 17 if compact else 18))
            except RuntimeError:
                continue

        ribbon = getattr(self, "ribbon", None)
        if ribbon is not None:
            ribbon.setMinimumHeight(142 if compact else 150)
            ribbon.setMaximumHeight(158 if compact else 168)

        dock = getattr(self, "search_dock", None)
        if dock is not None:
            dock.setMinimumWidth(205 if compact else 230)
            if dock.isVisible():
                target = max(220, min(360, int(self.width() * (0.19 if compact else 0.22))))
                try:
                    self.resizeDocks([dock], [target], Qt.Horizontal)
                except RuntimeError:
                    pass

    def resizeEvent(self, event):
        super().resizeEvent(event)
        timer = getattr(self, "_responsive_layout_timer", None)
        if timer is not None:
            timer.start()

    def _build_ui(self):
        self.setObjectName("CableRouteEditor")
        central = QWidget(self)
        self.setCentralWidget(central)

        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(10, 8, 10, 8)
        main_layout.setSpacing(8)

        self._build_ribbon(main_layout)
        self._build_menu_bar()

        self.scene = None

        self.canvas = GpuDxfGraphView(self)
        self.canvas.set_store(self.store)
        self.canvas.set_dxf_scene(self.dxf_scene)
        self.canvas.set_overlay_provider(self.draw_overlay_panels)

        self._rubber_band = QRubberBand(QRubberBand.Rectangle, self.canvas)
        main_layout.addWidget(self.canvas, 1)

        self._build_rhs_search_sidebar()

        self.canvas.leftClicked.connect(self.on_left_click)
        self.canvas.leftDoubleClicked.connect(self.on_double_click)
        self.canvas.leftReleased.connect(self.on_left_release)
        self.canvas.rightClicked.connect(self.on_right_click)
        self.canvas.middleClicked.connect(self.on_middle_click)
        self.canvas.middleDragged.connect(self.on_middle_drag)
        self.canvas.middleReleased.connect(self.on_middle_release)
        self.canvas.mouseWheelScrolled.connect(self.on_mousewheel)
        self.canvas.mouseDragged.connect(self.on_drag)
        self.canvas.mouseMoved.connect(self.on_mouse_move)

        status_row = QHBoxLayout()
        status_row.setContentsMargins(2, 0, 2, 0)
        status_row.setSpacing(8)

        file_caption = QLabel("Current file")
        file_caption.setObjectName("StatusCaption")
        status_row.addWidget(file_caption)
        self.file_label = QLabel("New file")
        self.file_label.setObjectName("StatusValue")
        self.file_label.setWordWrap(True)
        status_row.addWidget(self.file_label, 1)

        status_caption = QLabel("Status")
        status_caption.setObjectName("StatusCaption")
        status_row.addWidget(status_caption)
        self.status_label = QLabel("Ready")
        self.status_label.setObjectName("StatusValue")
        self.status_label.setWordWrap(True)
        status_row.addWidget(self.status_label, 2)

        main_layout.addLayout(status_row)

    def _build_rhs_search_sidebar(self):
        self.search_dock = QDockWidget("Search", self)
        self.search_dock.setAllowedAreas(Qt.RightDockWidgetArea | Qt.LeftDockWidgetArea)
        self.search_dock.setMinimumWidth(230)

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        self.sidebar_search_edit = QLineEdit()
        self.sidebar_search_edit.setPlaceholderText("Search current module...")
        self.sidebar_search_edit.textChanged.connect(self.refresh_rhs_search_sidebar)
        layout.addWidget(self.sidebar_search_edit)

        self.search_tabs = QTabWidget()
        self.search_tabs.setDocumentMode(True)
        self.search_tabs.tabBar().setUsesScrollButtons(True)
        self.search_tabs.tabBar().setElideMode(Qt.ElideRight)
        self.search_tabs.tabBar().setExpanding(False)
        self.search_tabs.currentChanged.connect(self.refresh_rhs_search_sidebar)
        layout.addWidget(self.search_tabs, 1)

        self.search_lists = {}

        sidebar_tabs = [
            ("Data Points", "Data"),
            ("Departments", "Depts"),
            ("Locations", "Rooms"),
            ("Corridor Nodes", "Nodes"),
            ("Transitions", "Trans"),
            ("Connections", "Links"),
            ("Room Types", "Types"),
            ("Assets", "Assets"),
        ]
        for module_name, tab_label in sidebar_tabs:
            list_widget = QListWidget()
            list_widget.itemDoubleClicked.connect(self._rhs_search_item_activated)
            self.search_tabs.addTab(list_widget, tab_label)
            self.search_tabs.setTabToolTip(self.search_tabs.count() - 1, module_name)
            self.search_lists[module_name] = list_widget

        sidebar_button_row = QGridLayout()
        sidebar_button_row.setHorizontalSpacing(6)
        sidebar_button_row.setVerticalSpacing(6)
        layout.addLayout(sidebar_button_row)

        refresh_btn = QPushButton("Refresh")
        refresh_btn.setIcon(bootstrap_icon("arrow-clockwise"))
        refresh_btn.clicked.connect(self.refresh_rhs_search_sidebar)
        sidebar_button_row.addWidget(refresh_btn, 0, 0)

        room_counts_btn = QPushButton("Room Counts")
        room_counts_btn.setIcon(bootstrap_icon("list-task"))
        room_counts_btn.setToolTip(
            "Show the number of placed rooms assigned to each room type"
        )
        room_counts_btn.clicked.connect(self.show_room_type_counts_dialog)
        sidebar_button_row.addWidget(room_counts_btn, 0, 1)

        scenario_btn = QPushButton("Scenarios")
        scenario_btn.setIcon(bootstrap_icon("diagram-3"))
        scenario_btn.setToolTip(
            "Preview and permanently apply grouped room/asset scenario sets"
        )
        scenario_btn.clicked.connect(self.show_room_type_asset_scenario_dialog)
        sidebar_button_row.addWidget(scenario_btn, 1, 0)

        capability_btn = QPushButton("Capabilities")
        capability_btn.setIcon(bootstrap_icon("boxes"))
        capability_btn.setToolTip("Show asset capability keyword overlap and deployed overlap locations")
        capability_btn.clicked.connect(self.show_asset_capability_overlap_dialog)
        sidebar_button_row.addWidget(capability_btn, 1, 1)

        for button in (refresh_btn, room_counts_btn, scenario_btn, capability_btn):
            button.setMinimumHeight(32)
            button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        self.search_dock.setWidget(container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.search_dock)

        toggle_action = self.search_dock.toggleViewAction()
        toggle_action.setText("Search panel")
        if hasattr(self, "_view_menu"):
            self._view_menu.addSeparator()
            self._view_menu.addAction(toggle_action)

        self.refresh_rhs_search_sidebar()

    def refresh_rhs_search_sidebar(self):
        if not hasattr(self, "search_lists"):
            return

        search_text = self.sidebar_search_edit.text().strip().lower()

        def visible(text):
            return not search_text or search_text in text.lower()

        room_group_labels = {}
        for group in self.store.data.get("room_type_scenario_groups", []) or []:
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("name", "") or "").strip()
            if not group_name:
                continue
            for room_type_id in group.get("room_type_ids", []) or []:
                room_type_id = str(room_type_id).strip()
                if room_type_id:
                    room_group_labels.setdefault(room_type_id, []).append(group_name)

        asset_group_labels = {}
        for group in self.store.data.get("asset_scenario_groups", []) or []:
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("name", "") or "").strip()
            if not group_name:
                continue
            for asset_id in group.get("asset_ids", []) or []:
                asset_id = str(asset_id).strip()
                if asset_id:
                    asset_group_labels.setdefault(asset_id, []).append(group_name)

        rows = {
            "Data Points": [
                item.get("name", "")
                for item in self.store.data.get("data_points", [])
            ],
            "Departments": [
                f"{item.get('id', '')} - {item.get('name', '')}"
                for item in self.store.data.get("departments", [])
            ],
            "Locations": [
                item.get("name", "")
                for item in self.store.data.get("locations", [])
            ],
            "Corridor Nodes": [
                item.get("name", "")
                for item in self.store.data.get("corridors", {}).get("nodes", [])
            ],
            "Transitions": [
                item.get("id", "")
                for item in self.store.data.get("transitions", [])
            ],
            "Connections": [
                f"{item.get('id', '')}: {item.get('from', '')} -> {item.get('to', '')}"
                for item in self.store.data.get("connections", [])
            ],
            "Room Types": [
                (
                    f"{item.get('id', '')} - {item.get('name', '')}"
                    + (
                        f" [{'; '.join(sorted(room_group_labels.get(str(item.get('id', '') or '').strip(), []), key=str.casefold))}]"
                        if room_group_labels.get(str(item.get('id', '') or '').strip())
                        else ""
                    )
                )
                for item in self.store.data.get("room_types", [])
            ],
            "Assets": [
                (
                    f"{item.get('id', '')} - {item.get('name', '')}"
                    + (
                        f" [{'; '.join(sorted(asset_group_labels.get(str(item.get('id', '') or '').strip(), []), key=str.casefold))}]"
                        if asset_group_labels.get(str(item.get('id', '') or '').strip())
                        else ""
                    )
                )
                for item in self.store.data.get("assets", [])
            ],
        }

        for module_name, list_widget in self.search_lists.items():
            list_widget.clear()

            for text in sorted(rows.get(module_name, []), key=str.lower):
                text = str(text).strip()
                if not text or not visible(text):
                    continue

                item = QListWidgetItem(text)
                item.setData(Qt.UserRole, module_name)
                list_widget.addItem(item)

    def _rhs_search_item_activated(self, item):
        module_name = item.data(Qt.UserRole)
        text = item.text()

        if module_name in {"Data Points", "Locations", "Corridor Nodes"}:
            name = text.strip()
            self._centre_on_named_point(name)
            return

        if module_name == "Departments":
            department_id = text.split(" - ", 1)[0].strip()
            self._centre_on_department(department_id)
            return

        if module_name == "Transitions":
            transition_id = text.strip()
            self._centre_on_transition(transition_id)
            return

        if module_name == "Room Types":
            room_type_id = text.split(" - ", 1)[0].strip()
            self._centre_on_nearest_room_type(room_type_id)
            return

    def _current_view_world_centre(self):
        """Return the graph position currently at the centre of the viewport."""
        canvas = getattr(self, "canvas", None)
        if canvas is None:
            return 0.0, 0.0

        try:
            viewport = canvas.viewport() if hasattr(canvas, "viewport") else canvas
            centre = viewport.rect().center()

            if hasattr(canvas, "screen_to_world"):
                x, y = canvas.screen_to_world(centre)
                return float(x), float(y)

            if hasattr(canvas, "mapToScene"):
                scene_pos = canvas.mapToScene(centre)
                return self.scene_to_world(scene_pos.x(), scene_pos.y())
        except (AttributeError, RuntimeError, TypeError, ValueError):
            pass

        return 0.0, 0.0

    def _centre_on_nearest_room_type(self, room_type_id):
        """Navigate to the nearest placed room/data point using a room type."""
        room_type_id = str(room_type_id or "").strip()
        if not room_type_id:
            self.set_status("Could not identify the selected room type")
            return

        room_type_name = room_type_id
        for room_type in self.store.data.get("room_types", []):
            if str(room_type.get("id", "")).strip() == room_type_id:
                room_type_name = str(room_type.get("name", room_type_id)).strip() or room_type_id
                break

        candidates = []
        for point in self.store.data.get("data_points", []):
            if str(point.get("room_type_id", "") or "").strip() != room_type_id:
                continue

            name = str(point.get("name", "")).strip()
            if not name:
                continue

            try:
                candidates.append(
                    {
                        "name": name,
                        "x": float(point.get("x", 0.0)),
                        "y": float(point.get("y", 0.0)),
                        "floor": int(point.get("floor", 0)),
                    }
                )
            except (TypeError, ValueError):
                continue

        if not candidates:
            self.set_status(
                f"No placed rooms/data points use room type {room_type_id} - {room_type_name}"
            )
            return

        centre_x, centre_y = self._current_view_world_centre()
        current_floor = int(self.floor_spin.value())
        try:
            floor_height = float(
                self.store.data.get("building", {}).get("floor_height_m", 4.0) or 4.0
            )
        except (TypeError, ValueError):
            floor_height = 4.0
        floor_height = max(0.0, floor_height)

        def distance_key(candidate):
            horizontal = math.hypot(candidate["x"] - centre_x, candidate["y"] - centre_y)
            vertical = abs(candidate["floor"] - current_floor) * floor_height
            spatial_distance = math.hypot(horizontal, vertical)
            return (
                spatial_distance,
                abs(candidate["floor"] - current_floor),
                horizontal,
                candidate["name"].lower(),
            )

        nearest = min(candidates, key=distance_key)
        name = nearest["name"]
        floor = nearest["floor"]

        if self.floor_spin.value() != floor:
            self.floor_spin.setValue(floor)

        self.selected_point_name = name
        self._set_canvas_multi_selection([name], append=False)
        self.refresh_canvas()

        self.canvas.fit_to_rect(
            QRectF(nearest["x"] - 10.0, -nearest["y"] - 10.0, 20.0, 20.0),
            Qt.KeepAspectRatio,
        )

        self.set_status(
            f"Centred on nearest {room_type_name} room: {name} "
            f"(floor {floor}, {len(candidates)} matching room(s))"
        )

    def show_room_type_counts_dialog(self):
        """Open or refresh the model-wide room-type count summary."""
        if self._room_type_counts_dialog is None:
            self._room_type_counts_dialog = RoomTypeCountsDialog(self)
            self._room_type_counts_dialog.navigateRequested.connect(
                self._centre_on_nearest_room_type
            )

        self._room_type_counts_dialog.set_model_data(self.store.data)
        self._room_type_counts_dialog.show()
        self._room_type_counts_dialog.raise_()
        self._room_type_counts_dialog.activateWindow()

    def show_room_type_asset_review_wizard(self):
        room_types = self.store.data.get("room_types", []) or []
        if not room_types:
            QMessageBox.information(
                self,
                "Room Type Asset Review",
                "Create room types before reviewing their asset assignments.",
            )
            return

        assets_by_id = {
            str(asset.get("id", "") or "").strip(): asset
            for asset in self.store.data.get("assets", []) or []
            if str(asset.get("id", "") or "").strip()
        }
        asset_categories_by_id = {
            str(category.get("id", "") or "").strip(): str(
                category.get("name", category.get("id", "")) or ""
            ).strip()
            for category in self.store.data.get("asset_categories", []) or []
            if str(category.get("id", "") or "").strip()
        }

        self._room_type_asset_review_dialog = RoomTypeAssetReviewWizard(
            self,
            room_types,
            assets_by_id=assets_by_id,
            asset_categories_by_id=asset_categories_by_id,
            review_state=self.store.data.get("room_type_asset_review", {}),
            staging_state=self.store.data.get("room_type_asset_staging", {}),
            asset_commits=self.store.data.get("room_type_asset_commits", []),
            rfi_state=self.store.data.get("room_type_asset_rfi", {}),
            on_state_changed=self._save_room_type_asset_review_state,
            on_assignments_changed=self._save_room_type_asset_review_assignments,
            on_commit_staging=self._commit_room_type_asset_staging,
            on_reset_staging=self._reset_room_type_asset_staging,
            on_rollback_commit=self._rollback_room_type_asset_commit,
            on_rfi_changed=self._save_room_type_asset_rfi_state,
            on_export_rfi=self.export_room_type_asset_rfi_pdf,
        )
        self._room_type_asset_review_dialog.show()
        self._room_type_asset_review_dialog.raise_()
        self._room_type_asset_review_dialog.activateWindow()

    def _room_type_asset_quantities(self, room_type):
        quantities = {}
        if not isinstance(room_type, dict):
            return quantities
        rows = room_type.get("assets", []) or []
        if rows:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                asset_id = str(row.get("asset_id", row.get("id", "")) or "").strip()
                if not asset_id:
                    continue
                try:
                    quantity = max(1, int(row.get("qty", 1) or 1))
                except (TypeError, ValueError):
                    quantity = 1
                quantities[asset_id] = quantity
            return quantities
        for asset_id in room_type.get("asset_ids", []) or []:
            asset_id = str(asset_id or "").strip()
            if asset_id:
                quantities[asset_id] = 1
        return quantities

    def _asset_audit_label(self, asset_id):
        asset_id = str(asset_id or "").strip()
        asset = next(
            (
                item
                for item in self.store.data.get("assets", []) or []
                if isinstance(item, dict)
                and str(item.get("id", "") or "").strip() == asset_id
            ),
            {},
        )
        name = str(asset.get("name", "") or "").strip()
        return f"{asset_id} ({name})" if name else asset_id

    def _room_type_asset_requesters(self, room_type):
        requesters = {}
        if not isinstance(room_type, dict):
            return requesters
        for row in room_type.get("assets", []) or []:
            if not isinstance(row, dict):
                continue
            asset_id = str(row.get("asset_id", row.get("id", "")) or "").strip()
            if asset_id:
                requesters[asset_id] = str(row.get("requested_by", "") or "").strip()
        return requesters

    def _room_assignment_change_details(
        self,
        before_quantities,
        after_quantities,
        before_ports=None,
        after_ports=None,
        before_requesters=None,
        after_requesters=None,
    ):
        details = []
        before_ports = dict(before_ports or {})
        after_ports = dict(after_ports or {})
        before_requesters = dict(before_requesters or {})
        after_requesters = dict(after_requesters or {})
        asset_ids = sorted(
            set(before_quantities) | set(after_quantities), key=str.casefold
        )
        for asset_id in asset_ids:
            label = self._asset_audit_label(asset_id)
            if asset_id not in before_quantities:
                requester = after_requesters.get(asset_id, "")
                detail = f"added asset {label} with quantity {after_quantities[asset_id]}"
                if requester:
                    detail += f", requested by '{requester}'"
                details.append(detail)
            elif asset_id not in after_quantities:
                requester = before_requesters.get(asset_id, "")
                detail = f"removed asset {label} (previous quantity {before_quantities[asset_id]}"
                if requester:
                    detail += f", requested by '{requester}'"
                details.append(detail + ")")
            elif before_quantities[asset_id] != after_quantities[asset_id]:
                details.append(
                    f"changed {label} quantity from {before_quantities[asset_id]} "
                    f"to {after_quantities[asset_id]}"
                )
            if (
                asset_id in after_ports
                and asset_id in before_ports
                and before_ports[asset_id] != after_ports[asset_id]
            ):
                details.append(
                    f"changed {label} data points each from {before_ports[asset_id]} "
                    f"to {after_ports[asset_id]}"
                )
            if (
                asset_id in before_quantities
                and asset_id in after_quantities
                and before_requesters.get(asset_id, "")
                != after_requesters.get(asset_id, "")
            ):
                details.append(
                    f"changed {label} requested by from "
                    f"'{before_requesters.get(asset_id, '') or '(blank)'}' to "
                    f"'{after_requesters.get(asset_id, '') or '(blank)'}'"
                )
        return details

    def _record_room_type_change(self, source, room_type_id, room_type_name, details):
        details = [str(detail).strip() for detail in details if str(detail).strip()]
        if not details:
            return
        identity = str(room_type_id or "").strip() or "(no ID)"
        fingerprints = getattr(
            self, "_room_type_revision_change_fingerprints", None
        )
        if fingerprints is None:
            fingerprints = {}
            self._room_type_revision_change_fingerprints = fingerprints
        if not remember_room_type_revision_change(
            fingerprints, source, identity, details
        ):
            return
        name = str(room_type_name or "").strip()
        label = f"{identity} - {name}" if name and name != identity else identity
        summary = f"Room type {label}: " + "; ".join(details)
        self.store.record_revision_change(
            source,
            summary,
            room_type_id=identity,
            details=details,
        )

    def _append_room_type_audit_history(
        self, action, room_type_id, room_type_name, details
    ):
        details = [str(detail).strip() for detail in details if str(detail).strip()]
        if not details:
            return
        state = self.store.data.setdefault(
            "room_type_asset_rfi", {"queries": [], "history": []}
        )
        history = state.setdefault("history", [])
        entry = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "action": str(action or "room_type_modified").strip(),
            "rfi_id": "",
            "room_type_id": str(room_type_id or "").strip(),
            "room_type_name": str(room_type_name or "").strip(),
            "asset_id": "",
            "asset_name": "",
            "note": "; ".join(details),
        }
        history.append(entry)
        dialog = getattr(self, "_room_type_asset_review_dialog", None)
        if dialog is not None and isinstance(getattr(dialog, "rfi_state", None), dict):
            dialog.rfi_state.setdefault("history", []).append(deepcopy(entry))

    def _save_room_type_asset_review_state(self, review_state):
        before = deepcopy(self.store.data.get("room_type_asset_review", {}))
        self.push_undo_state("Update room type asset review")
        valid_ids = {
            str(room_type.get("id", "") or "").strip()
            for room_type in self.store.data.get("room_types", []) or []
            if str(room_type.get("id", "") or "").strip()
        }
        self.store.data["room_type_asset_review"] = {
            str(room_type_id).strip(): dict(record)
            for room_type_id, record in (review_state or {}).items()
            if str(room_type_id).strip() in valid_ids and isinstance(record, dict)
        }
        after = self.store.data["room_type_asset_review"]
        room_types_by_id = {
            str(item.get("id", "") or "").strip(): item
            for item in self.store.data.get("room_types", []) or []
            if isinstance(item, dict) and str(item.get("id", "") or "").strip()
        }
        for room_type_id in sorted(set(before) | set(after), key=str.casefold):
            was_complete = bool(
                before.get(room_type_id, {}).get("complete", False)
                if isinstance(before.get(room_type_id), dict)
                else False
            )
            is_complete = bool(
                after.get(room_type_id, {}).get("complete", False)
                if isinstance(after.get(room_type_id), dict)
                else False
            )
            if was_complete == is_complete:
                continue
            room_type = room_types_by_id.get(room_type_id, {})
            detail = "marked asset review complete" if is_complete else "cleared asset review completion"
            self._record_room_type_change(
                "Room Type Asset Review",
                room_type_id,
                room_type.get("name", ""),
                [detail],
            )
        completed = sum(
            1
            for record in self.store.data["room_type_asset_review"].values()
            if isinstance(record, dict) and bool(record.get("complete", False))
        )
        self.set_status(f"Reviewed {completed} of {len(valid_ids)} room type asset assignment(s)")

    def _save_room_type_asset_review_assignments(self, room_type_id, asset_rows, data_ports_by_asset_id):
        room_type_id = str(room_type_id or "").strip()
        if not room_type_id:
            return
        room_type = next(
            (
                item
                for item in self.store.data.get("room_types", []) or []
                if str(item.get("id", "") or "").strip() == room_type_id
            ),
            None,
        )
        before_assignment_rows = clean_assignment_rows(
            (
                room_type.get("assets", [])
                or [
                    {"asset_id": asset_id, "qty": 1}
                    for asset_id in room_type.get("asset_ids", []) or []
                ]
            )
            if isinstance(room_type, dict)
            else []
        )
        before_quantities = self._room_type_asset_quantities(room_type)
        before_requesters = self._room_type_asset_requesters(room_type)
        before_ports = {}
        for asset in self.store.data.get("assets", []) or []:
            if not isinstance(asset, dict):
                continue
            asset_id = str(asset.get("id", "") or "").strip()
            if not asset_id:
                continue
            try:
                before_ports[asset_id] = max(
                    0, int(asset.get("data_points", 0) or 0)
                )
            except (TypeError, ValueError):
                before_ports[asset_id] = 0
        self.push_undo_state("Update room type asset assignments")
        cleaned_rows = []
        if isinstance(room_type, dict):
            for row in asset_rows or []:
                if not isinstance(row, dict):
                    continue
                asset_id = str(row.get("asset_id", "") or "").strip()
                if not asset_id:
                    continue
                try:
                    qty = int(row.get("qty", 1) or 1)
                except (TypeError, ValueError):
                    qty = 1
                cleaned_row = {"asset_id": asset_id, "qty": max(1, qty)}
                requested_by = str(row.get("requested_by", "") or "").strip()
                if requested_by:
                    cleaned_row["requested_by"] = requested_by
                cleaned_rows.append(cleaned_row)
            room_type["assets"] = cleaned_rows
            room_type["asset_ids"] = [row["asset_id"] for row in cleaned_rows]

        ports_by_asset = {
            str(asset_id or "").strip(): ports
            for asset_id, ports in (data_ports_by_asset_id or {}).items()
            if str(asset_id or "").strip()
        }
        for asset in self.store.data.get("assets", []) or []:
            if not isinstance(asset, dict):
                continue
            asset_id = str(asset.get("id", "") or "").strip()
            if asset_id not in ports_by_asset:
                continue
            try:
                ports = int(ports_by_asset[asset_id] or 0)
            except (TypeError, ValueError):
                ports = 0
            asset["data_points"] = max(0, ports)

        after_quantities = self._room_type_asset_quantities(room_type)
        after_requesters = self._room_type_asset_requesters(room_type)
        after_ports = dict(before_ports)
        for asset_id, ports in ports_by_asset.items():
            try:
                after_ports[asset_id] = max(0, int(ports or 0))
            except (TypeError, ValueError):
                after_ports[asset_id] = 0
        asset_names = {
            str(asset.get("id", "") or "").strip(): str(asset.get("name", "") or "").strip()
            for asset in self.store.data.get("assets", []) or []
            if isinstance(asset, dict) and str(asset.get("id", "") or "").strip()
        }
        staging = update_room_type_asset_staging(
            self.store.data.get("room_type_asset_staging", {}),
            room_type_id=room_type_id,
            room_type_name=(room_type.get("name", "") if isinstance(room_type, dict) else ""),
            before_rows=before_assignment_rows,
            after_rows=cleaned_rows,
            before_ports=before_ports,
            after_ports=ports_by_asset,
            asset_names=asset_names,
        )
        self.store.data["room_type_asset_staging"] = staging
        details = self._room_assignment_change_details(
            before_quantities,
            after_quantities,
            before_ports,
            after_ports,
            before_requesters,
            after_requesters,
        )
        if isinstance(room_type, dict):
            self._record_room_type_change(
                "Room Type Asset Review",
                room_type_id,
                room_type.get("name", ""),
                details,
            )

        review_state = self.store.data.get("room_type_asset_review", {})
        if isinstance(review_state, dict):
            review_state.pop(room_type_id, None)
        self.store.sync_all_room_type_quantities()
        self.set_status(f"Updated asset quantities and data ports for room type {room_type_id}")
        return deepcopy(staging)

    def _commit_room_type_asset_staging(self, message, resolve_rfi_ids=None):
        staging = self.store.data.get("room_type_asset_staging", {})
        changes = staging.get("changes", []) if isinstance(staging, dict) else []
        if not changes:
            return {}
        commits = self.store.data.setdefault("room_type_asset_commits", [])
        highest = 0
        for item in commits:
            if not isinstance(item, dict):
                continue
            match = re.search(r"(\d+)$", str(item.get("id", "") or ""))
            if match:
                highest = max(highest, int(match.group(1)))
        commit_id = f"RTAC-{highest + 1:06d}"
        try:
            commit = build_room_type_asset_commit(
                staging, message, commit_id=commit_id
            )
        except ValueError as exc:
            QMessageBox.information(self, "Commit Asset Changes", str(exc))
            return deepcopy(staging)

        self.push_undo_state("Commit staged room type asset changes")
        commits.append(commit)
        self.store.data["room_type_asset_staging"] = {}
        rfi_state, resolved_rfi_ids = resolve_rfis_with_commit(
            self.store.data.get("room_type_asset_rfi", {}),
            resolve_rfi_ids,
            commit_id=commit_id,
            message=message,
        )
        self.store.data["room_type_asset_rfi"] = rfi_state
        if resolved_rfi_ids:
            commit["resolved_rfi_ids"] = resolved_rfi_ids
        details = []
        for change in commit["changes"]:
            action = str(change.get("change_type", "changed") or "changed")
            room_id = str(change.get("room_type_id", "") or "").strip()
            asset_id = str(change.get("asset_id", "") or "").strip()
            scope = str(change.get("scope", "assignment") or "assignment")
            if scope == "asset":
                before = change.get("before", {}) or {}
                after = change.get("after", {}) or {}
                details.append(
                    f"changed asset {asset_id} data points from "
                    f"{before.get('data_points', 0)} to {after.get('data_points', 0)}"
                )
            else:
                details.append(f"{action} asset {asset_id} for room type {room_id}")
        details.extend(
            f"resolved RFI {rfi_id} with commit {commit_id}"
            for rfi_id in resolved_rfi_ids
        )
        self.store.record_revision_change(
            "Room Type Asset Commit",
            f"{commit_id}: {str(message or '').strip()}",
            details=details,
        )
        self.set_status(
            f"Committed {len(commit['changes'])} staged room type asset change(s) as "
            f"{commit_id}; resolved {len(resolved_rfi_ids)} RFI(s)"
        )
        return self._room_type_asset_staging_result({})

    def _rollback_room_type_asset_commit(self, commit_id):
        identity = str(commit_id or "").strip()
        commit = next(
            (
                item
                for item in self.store.data.get("room_type_asset_commits", []) or []
                if isinstance(item, dict)
                and str(item.get("id", "") or "").strip() == identity
            ),
            None,
        )
        if not isinstance(commit, dict):
            QMessageBox.information(
                self, "Rollback Asset Commit", f"Asset commit {identity} was not found."
            )
            return self._room_type_asset_staging_result(
                self.store.data.get("room_type_asset_staging", {})
            )

        self.push_undo_state(f"Rollback room type asset commit {identity}")
        staging = deepcopy(self.store.data.get("room_type_asset_staging", {}))
        room_types_by_id = {
            str(row.get("id", "") or "").strip(): row
            for row in self.store.data.get("room_types", []) or []
            if isinstance(row, dict) and str(row.get("id", "") or "").strip()
        }
        assets_by_id = {
            str(row.get("id", "") or "").strip(): row
            for row in self.store.data.get("assets", []) or []
            if isinstance(row, dict) and str(row.get("id", "") or "").strip()
        }
        asset_names = {
            asset_id: str(asset.get("name", "") or "").strip()
            for asset_id, asset in assets_by_id.items()
        }
        assignment_changes = {}
        asset_changes = []
        for change in commit.get("changes", []) or []:
            if not isinstance(change, dict):
                continue
            if str(change.get("scope", "assignment") or "assignment") == "asset":
                asset_changes.append(change)
                continue
            room_id = str(change.get("room_type_id", "") or "").strip()
            if room_id:
                assignment_changes.setdefault(room_id, []).append(change)

        for room_id, changes in assignment_changes.items():
            room_type = room_types_by_id.get(room_id)
            if not isinstance(room_type, dict):
                continue
            before_rows = clean_assignment_rows(
                room_type.get("assets", [])
                or [
                    {"asset_id": value, "qty": 1}
                    for value in room_type.get("asset_ids", []) or []
                ]
            )
            target_by_id = {row["asset_id"]: row for row in before_rows}
            for change in changes:
                asset_id = str(change.get("asset_id", "") or "").strip()
                original = change.get("before")
                if isinstance(original, dict):
                    restored = clean_assignment_rows([original])
                    if restored:
                        target_by_id[asset_id] = restored[0]
                else:
                    target_by_id.pop(asset_id, None)
            after_rows = sorted(target_by_id.values(), key=lambda row: row["asset_id"].casefold())
            staging = update_room_type_asset_staging(
                staging,
                room_type_id=room_id,
                room_type_name=room_type.get("name", ""),
                before_rows=before_rows,
                after_rows=after_rows,
                before_ports={},
                after_ports={},
                asset_names=asset_names,
            )
            room_type["assets"] = after_rows
            room_type["asset_ids"] = [row["asset_id"] for row in after_rows]

        for change in asset_changes:
            asset_id = str(change.get("asset_id", "") or "").strip()
            asset = assets_by_id.get(asset_id)
            before = change.get("before", {}) or {}
            if not isinstance(asset, dict) or "data_points" not in before:
                continue
            try:
                current_value = max(0, int(asset.get("data_points", 0) or 0))
                target_value = max(0, int(before.get("data_points", 0) or 0))
            except (TypeError, ValueError):
                continue
            room_id = str(change.get("room_type_id", "") or "").strip()
            room_type = room_types_by_id.get(room_id, {})
            current_rows = clean_assignment_rows(
                room_type.get("assets", []) if isinstance(room_type, dict) else []
            )
            staging = update_room_type_asset_staging(
                staging,
                room_type_id=room_id,
                room_type_name=(room_type.get("name", "") if isinstance(room_type, dict) else ""),
                before_rows=current_rows,
                after_rows=current_rows,
                before_ports={asset_id: current_value},
                after_ports={asset_id: target_value},
                asset_names=asset_names,
            )
            asset["data_points"] = target_value

        if staging:
            rollback_ids = [
                str(value).strip()
                for value in staging.get("rollback_of", []) or []
                if str(value).strip()
            ]
            if identity not in rollback_ids:
                rollback_ids.append(identity)
            staging["rollback_of"] = rollback_ids
            staging["changes"] = room_type_asset_staged_changes(staging)
        self.store.data["room_type_asset_staging"] = staging
        self.store.sync_all_room_type_quantities()
        self.set_status(
            f"Staged rollback of {identity}: {len(staging.get('changes', [])) if staging else 0} change(s)"
        )
        return self._room_type_asset_staging_result(staging)

    def _reset_room_type_asset_staging(self, room_type_id=""):
        staging = deepcopy(self.store.data.get("room_type_asset_staging", {}))
        if not isinstance(staging, dict):
            staging = {}
        target_id = str(room_type_id or "").strip()
        rooms = staging.get("rooms", {})
        assets = staging.get("assets", {})
        if not isinstance(rooms, dict):
            rooms = {}
        if not isinstance(assets, dict):
            assets = {}
        selected_room_ids = (
            {target_id} if target_id else set(str(key) for key in rooms)
        )
        selected_asset_ids = {
            str(asset_id)
            for asset_id, record in assets.items()
            if not target_id
            or (
                isinstance(record, dict)
                and str(record.get("room_type_id", "") or "").strip() == target_id
            )
        }
        if not selected_room_ids and not selected_asset_ids:
            return self._room_type_asset_staging_result(staging)

        self.push_undo_state(
            "Reset current room asset changes"
            if target_id
            else "Clear all staged room type asset changes"
        )
        room_types_by_id = {
            str(row.get("id", "") or "").strip(): row
            for row in self.store.data.get("room_types", []) or []
            if isinstance(row, dict) and str(row.get("id", "") or "").strip()
        }
        for reset_room_id in selected_room_ids:
            record = rooms.get(reset_room_id)
            room_type = room_types_by_id.get(reset_room_id)
            if not isinstance(record, dict) or not isinstance(room_type, dict):
                continue
            restored_rows = clean_assignment_rows(record.get("before", []))
            room_type["assets"] = restored_rows
            room_type["asset_ids"] = [row["asset_id"] for row in restored_rows]
            rooms.pop(reset_room_id, None)

        assets_by_id = {
            str(row.get("id", "") or "").strip(): row
            for row in self.store.data.get("assets", []) or []
            if isinstance(row, dict) and str(row.get("id", "") or "").strip()
        }
        for asset_id in selected_asset_ids:
            record = assets.get(asset_id)
            asset = assets_by_id.get(asset_id)
            if isinstance(record, dict) and isinstance(asset, dict):
                try:
                    value = max(0, int(record.get("before_data_points", 0) or 0))
                except (TypeError, ValueError):
                    value = 0
                asset["data_points"] = value
            assets.pop(asset_id, None)

        staging["rooms"] = rooms
        staging["assets"] = assets
        staging["changes"] = room_type_asset_staged_changes(staging)
        if not staging["changes"]:
            staging = {}
        self.store.data["room_type_asset_staging"] = staging
        self.store.sync_all_room_type_quantities()
        self.set_status(
            f"Reset staged asset changes for room type {target_id}"
            if target_id
            else "Cleared all staged room type asset changes"
        )
        return self._room_type_asset_staging_result(staging)

    def _room_type_asset_staging_result(self, staging):
        return {
            "staging": deepcopy(staging if isinstance(staging, dict) else {}),
            "room_types": deepcopy(self.store.data.get("room_types", []) or []),
            "assets_by_id": {
                str(asset.get("id", "") or "").strip(): deepcopy(asset)
                for asset in self.store.data.get("assets", []) or []
                if isinstance(asset, dict) and str(asset.get("id", "") or "").strip()
            },
            "commits": deepcopy(
                self.store.data.get("room_type_asset_commits", []) or []
            ),
            "rfi_state": deepcopy(
                self.store.data.get("room_type_asset_rfi", {}) or {}
            ),
        }

    def _save_room_type_asset_rfi_state(self, rfi_state):
        old_state = self.store.data.get("room_type_asset_rfi", {})
        old_history = (
            list(old_state.get("history", []) or [])
            if isinstance(old_state, dict)
            else []
        )
        self.push_undo_state("Update room type asset RFI list")
        state = rfi_state if isinstance(rfi_state, dict) else {}
        self.store.data["room_type_asset_rfi"] = {
            "queries": [
                dict(item)
                for item in state.get("queries", []) or []
                if isinstance(item, dict)
            ],
            "history": [
                dict(item)
                for item in state.get("history", []) or []
                if isinstance(item, dict)
            ],
        }
        new_history = self.store.data["room_type_asset_rfi"]["history"]
        appended = new_history[len(old_history):] if new_history[:len(old_history)] == old_history else []
        for item in appended:
            if not should_mirror_rfi_audit_to_revision(item):
                continue
            action = str(item.get("action", "audit updated") or "audit updated").replace("_", " ")
            asset_id = str(item.get("asset_id", "") or "").strip()
            asset_name = str(item.get("asset_name", "") or "").strip()
            subject = f" for asset {asset_id} ({asset_name})" if asset_id and asset_name else (f" for asset {asset_id}" if asset_id else "")
            rfi_id = str(item.get("rfi_id", "") or "").strip()
            reason = str(item.get("note", "") or "").strip()
            detail = action + subject
            if rfi_id:
                detail += f" [{rfi_id}]"
            if reason:
                detail += f": {reason}"
            self._record_room_type_change(
                "Room Type Asset Review Audit",
                item.get("room_type_id", ""),
                item.get("room_type_name", ""),
                [detail],
            )
        outstanding = sum(
            1
            for item in self.store.data["room_type_asset_rfi"]["queries"]
            if str(item.get("status", "outstanding") or "outstanding")
            .strip()
            .casefold()
            != "resolved"
        )
        noun = "query" if outstanding == 1 else "queries"
        self.set_status(
            f"Room type asset RFI list updated - {outstanding} outstanding {noun}"
        )

    def _centre_on_named_point(self, name):
        point = self.store.all_points().get(name)
        if not point:
            self.set_status(f"Could not find {name}")
            return

        floor = int(point.get("floor", 0))

        if self.floor_spin.value() != floor:
            self.floor_spin.setValue(floor)

        self.selected_point_name = name
        self._set_canvas_multi_selection([name], append=False)
        self.refresh_canvas()

        self.canvas.fit_to_rect(QRectF(point["x"] - 10, -point["y"] - 10, 20, 20), Qt.KeepAspectRatio)

        if hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()
        self.refresh_canvas()

        self.set_status(f"Centred on {name}")

    def _centre_on_department(self, department_id):
        for item in self.store.data.get("departments", []):
            if str(item.get("id", "")).strip() != str(department_id).strip():
                continue

            floor = int(item.get("floor", 0))

            if self.floor_spin.value() != floor:
                self.floor_spin.setValue(floor)

            self.refresh_canvas()

            scene_pos = self.world_to_scene(item.get("x", 0.0), item.get("y", 0.0))
            self.canvas.centerOn(scene_pos)

            if hasattr(self.canvas, "invalidate_dxf_cache"):
                self.canvas.invalidate_dxf_cache()
            self.refresh_canvas()

            self.set_status(f"Centred on department {department_id}")
            return

        self.set_status(f"Could not find department {department_id}")

    def _centre_on_transition(self, transition_id):
        transition_id = str(transition_id).strip()

        for transition in self.store.data.get("transitions", []):
            if str(transition.get("id", "")).strip() != transition_id:
                continue

            floor_locations = transition.get("floor_locations", {})
            if not floor_locations:
                self.set_status(f"Transition {transition_id} has no floor locations")
                return

            floor_text = sorted(floor_locations.keys(), key=lambda x: int(x))[0]
            pos = floor_locations[floor_text]
            floor = int(floor_text)

            if self.floor_spin.value() != floor:
                self.floor_spin.setValue(floor)

            self.refresh_canvas()

            scene_pos = self.world_to_scene(pos.get("x", 0.0), pos.get("y", 0.0))
            self.canvas.centerOn(scene_pos)

            if hasattr(self.canvas, "invalidate_dxf_cache"):
                self.canvas.invalidate_dxf_cache()
            self.refresh_canvas()

            self.set_status(f"Centred on transition {transition_id}")
            return

        self.set_status(f"Could not find transition {transition_id}")

    def _build_menu_bar(self):
        file_menu = self.menuBar().addMenu("File")

        for text, icon_name, handler in [
            ("Open Project", "folder2-open", self.open_json),
            ("Import Legacy JSON", "box-arrow-right", self.import_json),
            (
                "Import Locations from Project...",
                "geo-alt",
                self.import_locations_from_project,
            ),
            ("Save Project", "database", self.save_json),
            ("Save Project As", "file-earmark-plus", self.save_json_as),
            ("Revision History...", "clock-history", self.show_revision_history),
            ("Export Revision History PDF", "filetype-pdf", self.export_revision_history_pdf),
            ("Export Asset Register PDF", "filetype-pdf", self.export_asset_register_pdf),
            ("Export Room Type Asset RFI PDF", "filetype-pdf", self.export_room_type_asset_rfi_pdf),
            ("Export Project Summary PDF", "filetype-pdf", self.export_project_summary_pdf),
            ("Export Floor Plans PDF", "filetype-pdf", self.export_all_floors_pdf),
            ("Export Placement Zones PDF", "filetype-pdf", self.export_all_floor_zones_pdf),
            ("Open PDF in Report Studio...", "pencil-square", self.open_pdf_in_report_studio),
            ("Export Project JSON", "box-arrow-right", self.export_json),
            ("Map DXF to Floor", "geo-alt", self.load_dxf),
            ("Clear Floor DXF", "trash3", self.clear_floor_dxf),
            ("Export Floor DXFs", "database", self.export_floor_dxfs),
        ]:
            action = file_menu.addAction(text)
            set_action_icon(action, icon_name)
            action.triggered.connect(handler)

        edit_menu = self.menuBar().addMenu("Edit")

        undo_action = edit_menu.addAction("Undo")
        set_action_icon(undo_action, "arrow-clockwise")
        undo_action.setShortcut(QKeySequence("Ctrl+Z"))
        undo_action.triggered.connect(self.undo)

        redo_action = edit_menu.addAction("Redo")
        set_action_icon(redo_action, "arrow-right")
        redo_action.setShortcut(QKeySequence("Ctrl+Y"))
        redo_action.triggered.connect(self.redo)

        view_menu = self.menuBar().addMenu("View")
        self._view_menu = view_menu

        fit_action = view_menu.addAction("Fit View")
        set_action_icon(fit_action, "arrows-fullscreen")
        fit_action.triggered.connect(self.fit_view)

        layers_action = view_menu.addAction("Drawing Layers...")
        set_action_icon(layers_action, "list-task")
        layers_action.triggered.connect(self.show_layer_visibility_dialog)

        performance_action = view_menu.addAction("Viewer Performance...")
        set_action_icon(performance_action, "pc-display")
        performance_action.triggered.connect(self.show_renderer_performance_dialog)

        tools_menu = self.menuBar().addMenu("Tools")

        room_type_counts_action = tools_menu.addAction("Room Type Counts")
        set_action_icon(room_type_counts_action, "list-task")
        room_type_counts_action.triggered.connect(self.show_room_type_counts_dialog)

        room_type_review_action = tools_menu.addAction("Room Type Asset Review Wizard")
        set_action_icon(room_type_review_action, "check-circle", BOOTSTRAP_GREEN)
        room_type_review_action.triggered.connect(self.show_room_type_asset_review_wizard)

        room_type_rfi_action = tools_menu.addAction("Export Room Type Asset RFI PDF")
        set_action_icon(room_type_rfi_action, "filetype-pdf")
        room_type_rfi_action.triggered.connect(self.export_room_type_asset_rfi_pdf)

        scenario_action = tools_menu.addAction("Room/Asset Scenario Test")
        set_action_icon(scenario_action, "diagram-3")
        scenario_action.triggered.connect(self.show_room_type_asset_scenario_dialog)

        room_groups_action = tools_menu.addAction("Room Scenario Groups")
        set_action_icon(room_groups_action, "collection")
        room_groups_action.triggered.connect(self.manage_room_type_scenario_groups)

        asset_groups_action = tools_menu.addAction("Asset Scenario Groups")
        set_action_icon(asset_groups_action, "boxes")
        asset_groups_action.triggered.connect(self.manage_asset_scenario_groups)

        capability_overlap_action = tools_menu.addAction("Asset Capability Overlap Matrix")
        set_action_icon(capability_overlap_action, "tags")
        capability_overlap_action.triggered.connect(self.show_asset_capability_overlap_dialog)

        tools_menu.addSeparator()

        export_room_type_matrix_action = tools_menu.addAction("Export Room Type Asset Matrix")
        set_action_icon(export_room_type_matrix_action, "database")
        export_room_type_matrix_action.triggered.connect(self.export_room_type_asset_matrix)

        import_room_type_matrix_action = tools_menu.addAction("Import Room Type Asset Matrix")
        set_action_icon(import_room_type_matrix_action, "box-arrow-right")
        import_room_type_matrix_action.triggered.connect(self.import_room_type_asset_matrix)

        remove_invalid_routes_action = tools_menu.addAction(
            "Remove Invalid Connections and Routes…"
        )
        set_action_icon(remove_invalid_routes_action, "trash", "#dc3545")
        remove_invalid_routes_action.triggered.connect(
            self.remove_invalid_connections_and_routes
        )

        validate_action = tools_menu.addAction("Validate")
        set_action_icon(validate_action, "check-circle", BOOTSTRAP_GREEN)
        validate_action.triggered.connect(self.validate_json)

    def _ribbon_button(self, text, handler):
        btn = QPushButton(text)
        btn.setIcon(bootstrap_icon("arrow-right"))
        btn.setMinimumSize(124, 34)
        btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        btn.clicked.connect(handler)
        return btn

    def show_layer_visibility_dialog(self):
        LayerVisibilityDialog(self, self).exec()

    def show_renderer_performance_dialog(self):
        RendererPerformanceDialog(self, self).exec()

    def _ribbon_layers_button(self):
        btn = QToolButton()
        btn.setText("Layers")
        btn.setToolTip("Open the batch drawing-layer visibility dialog")
        btn.setIcon(bootstrap_icon("list-task"))
        btn.setIconSize(QSize(18, 18))
        btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self._configure_ribbon_button(btn)
        btn.clicked.connect(self.show_layer_visibility_dialog)
        return btn

    def _add_ribbon_group(self, parent_layout, title, widgets, columns=3):
        frame = QFrame()
        frame.setObjectName("RibbonGroup")
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Minimum)
        self._ribbon_groups.append(frame)

        layout = QVBoxLayout(frame)
        layout.setContentsMargins(6, 5, 6, 4)
        layout.setSpacing(2)

        grid = QGridLayout()
        grid.setHorizontalSpacing(4)
        grid.setVerticalSpacing(2)

        # Supports:
        #   [btn1, btn2, btn3]
        #   [[btn1, btn2], [btn3, btn4]]
        if widgets and all(isinstance(row, list) for row in widgets):
            rows_layout = QVBoxLayout()
            rows_layout.setContentsMargins(0, 0, 0, 0)
            rows_layout.setSpacing(4)

            for row_widgets in widgets:
                row_container = QWidget()

                row_layout = QHBoxLayout(row_container)

                row_layout.setContentsMargins(2, 1, 2, 1)
                row_layout.setSpacing(6)
                row_layout.setAlignment(Qt.AlignLeft)
                for widget in row_widgets:
                    row_layout.addWidget(widget)

                rows_layout.addWidget(row_container)

            layout.addLayout(rows_layout)
        else:
            grid = QGridLayout()
            grid.setContentsMargins(2, 2, 2, 2)
            grid.setHorizontalSpacing(6)
            grid.setVerticalSpacing(5)

            for index, widget in enumerate(widgets):
                row = index // columns
                col = index % columns
                grid.addWidget(widget, row, col)

            layout.addLayout(grid)

        title_label = QLabel(title)

        title_label.setObjectName("RibbonGroupTitle")
        title_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(title_label)

        parent_layout.addWidget(frame)

    def _ribbon_icon_button(self, text, tooltip, icon_enum, handler):
        btn = QToolButton()
        btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        btn.setIcon(bootstrap_icon_for(icon_enum))
        btn.setIconSize(QSize(18, 18))
        btn.setText(text)
        btn.setToolTip(tooltip)

        self._configure_ribbon_button(btn)

        btn.clicked.connect(handler)
        return btn

    def _ribbon_toggle_button(self, text, icon_enum, checked=True):
        btn = QToolButton()
        btn.setCheckable(True)
        btn.setChecked(checked)
        btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        btn.setIcon(bootstrap_icon_for(icon_enum))
        btn.setIconSize(QSize(18, 18))
        btn.setText(text)
        btn.setToolTip(text)

        self._configure_ribbon_button(btn)

        return btn

    def _build_ribbon(self, main_layout):
        mode_buttons = self._build_mode_buttons()

        self.floor_spin = QSpinBox()
        self.floor_spin.setRange(0, 99)
        self.floor_spin.valueChanged.connect(self.on_floor_changed)

        go_btn = self._ribbon_icon_button(
            "Go",
            "Go to floor",
            QStyle.SP_ArrowForward,
            self.refresh_canvas,
        )

        floor_row = QWidget()
        floor_layout = QHBoxLayout(floor_row)
        floor_layout.setContentsMargins(0, 0, 0, 0)
        floor_layout.setSpacing(6)
        floor_layout.addWidget(self.floor_spin)
        floor_layout.addWidget(go_btn)

        self.snap_check = self._ribbon_toggle_button(
            "Snap",
            QStyle.SP_DialogApplyButton,
            True,
        )

        self.bidirectional_check = self._ribbon_toggle_button(
            "2-Way",
            QStyle.SP_BrowserReload,
            True,
        )

        self.chain_edges_check = self._ribbon_toggle_button(
            "Chain",
            QStyle.SP_CommandLink,
            True,
        )

        self.show_dxf_check = self._ribbon_toggle_button(
            "DXF",
            QStyle.SP_FileDialogDetailedView,
            True,
        )

        self.show_labels_check = self._ribbon_toggle_button(
            "Labels",
            QStyle.SP_FileDialogContentsView,
            True,
        )

        self.show_edges_check = self._ribbon_toggle_button(
            "Edges",
            QStyle.SP_ArrowRight,
            True,
        )

        self.show_nodes_check = self._ribbon_toggle_button(
            "Nodes",
            QStyle.SP_DirIcon,
            True,
        )

        self.show_data_points_check = self._ribbon_toggle_button(
            "Data",
            QStyle.SP_DriveNetIcon,
            True,
        )

        self.show_unassigned_room_types_only_check = self._ribbon_toggle_button(
            "Unassigned",
            QStyle.SP_MessageBoxWarning,
            False,
        )

        self.hide_connected_data_points_check = self._ribbon_toggle_button(
            "Hide Used",
            QStyle.SP_BrowserStop,
            False,
        )

        self.show_unconnected_data_points_only_check = self._ribbon_toggle_button(
            "No Graph Edge",
            QStyle.SP_MessageBoxWarning,
            False,
        )
        self.show_unconnected_data_points_only_check.setToolTip(
            "Show only data points that do not have a routing-graph edge connection."
        )

        self.show_routing_unconnected_data_points_only_check = (
            self._ribbon_toggle_button(
                "No Routing Link",
                QStyle.SP_MessageBoxWarning,
                False,
            )
        )
        self.show_routing_unconnected_data_points_only_check.setToolTip(
            "Show only data points that are not referenced by a connection in the Routing tab."
        )

        self.show_locations_check = self._ribbon_toggle_button(
            "Locations",
            QStyle.SP_DialogOpenButton,
            True,
        )

        self.show_comms_rooms_check = self._ribbon_toggle_button(
            "Comms",
            QStyle.SP_ComputerIcon,
            True,
        )

        self.show_departments_check = self._ribbon_toggle_button(
            "Departments",
            QStyle.SP_DirHomeIcon,
            True,
        )

        self.show_placement_zones_check = self._ribbon_toggle_button(
            "Room Zones",
            QStyle.SP_FileDialogDetailedView,
            True,
        )

        for check in [
            self.show_dxf_check,
            self.show_labels_check,
            self.show_edges_check,
            self.show_nodes_check,
            self.show_data_points_check,
            self.show_unassigned_room_types_only_check,
            self.hide_connected_data_points_check,
            self.show_unconnected_data_points_only_check,
            self.show_routing_unconnected_data_points_only_check,
            self.show_locations_check,
            self.show_comms_rooms_check,
            self.show_departments_check,
            self.show_placement_zones_check,
        ]:
            check.toggled.connect(self.refresh_canvas)

        self.quick_add_corridor_check = self._ribbon_toggle_button(
            "Quick Add",
            QStyle.SP_FileDialogNewFolder,
            False,
        )

        self.default_corridor_height_spin = QDoubleSpinBox()
        self.default_corridor_height_spin.setRange(0.0, 100.0)
        self.default_corridor_height_spin.setDecimals(2)
        self.default_corridor_height_spin.setSingleStep(0.1)
        self.default_corridor_height_spin.setValue(0.0)

        self.default_corridor_cable_limit_spin = QSpinBox()
        self.default_corridor_cable_limit_spin.setRange(0, 1000000)
        self.default_corridor_cable_limit_spin.setValue(0)

        ribbon = QTabWidget()
        self.ribbon = ribbon
        ribbon.setObjectName("AeroRibbon")
        ribbon.setDocumentMode(True)
        ribbon.tabBar().setUsesScrollButtons(True)
        ribbon.tabBar().setElideMode(Qt.ElideRight)
        ribbon.tabBar().setExpanding(False)
        ribbon.setMinimumHeight(150)
        ribbon.setMaximumHeight(168)
        main_layout.addWidget(ribbon)

        # ---------------- Home tab ----------------
        home_tab = QWidget()
        home_layout = QHBoxLayout(home_tab)
        home_layout.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        home_layout.setContentsMargins(10, 1, 10, 1)
        home_layout.setSpacing(10)

        self._add_ribbon_group(
            home_layout,
            "Layers",
            [
                self._ribbon_layers_button(),
                self.show_placement_zones_check,
                self.show_unassigned_room_types_only_check,
                self.hide_connected_data_points_check,
                self.show_unconnected_data_points_only_check,
                self.show_routing_unconnected_data_points_only_check,
            ],
        )

        self._add_ribbon_group(
            home_layout,
            "Placement",
            [
                mode_buttons,
                [
                    QLabel("Floor"),
                    floor_row,
                    self.snap_check,
                    self.bidirectional_check,
                    self.chain_edges_check,
                ],
            ],
        )
        self._add_scrollable_ribbon_tab(ribbon, home_tab, "Home")

        # ---------------- Data tab ----------------
        data_tab = QWidget()
        data_layout = QHBoxLayout(data_tab)

        self._add_ribbon_group(
            data_layout,
            "Master Data",
            [
                self._ribbon_icon_button(
                    "Departments",
                    "Manage departments",
                    QStyle.SP_DirHomeIcon,
                    self.manage_departments,
                ),
                self._ribbon_icon_button(
                    "Categories",
                    "Asset categories",
                    QStyle.SP_FileDialogListView,
                    self.manage_asset_categories,
                ),
                self._ribbon_icon_button(
                    "Assets",
                    "Manage assets",
                    QStyle.SP_ComputerIcon,
                    self.manage_assets,
                ),
                self._ribbon_icon_button(
                    "Rooms", "Room types", QStyle.SP_DirIcon, self.manage_room_types
                ),
                self._ribbon_icon_button(
                    "Review",
                    "Review room type asset assignments",
                    QStyle.SP_DialogApplyButton,
                    self.show_room_type_asset_review_wizard,
                ),
                self._ribbon_icon_button(
                    "Scenario",
                    "Test adding assets to grouped room types",
                    QStyle.SP_FileDialogDetailedView,
                    self.show_room_type_asset_scenario_dialog,
                ),
                self._ribbon_icon_button(
                    "Room Groups",
                    "Manage reusable room scenario groups",
                    QStyle.SP_DirLinkIcon,
                    self.manage_room_type_scenario_groups,
                ),
                self._ribbon_icon_button(
                    "Asset Groups",
                    "Manage reusable asset scenario groups",
                    QStyle.SP_FileLinkIcon,
                    self.manage_asset_scenario_groups,
                ),
                self._ribbon_icon_button(
                    "Locations",
                    "Manage locations",
                    QStyle.SP_DialogOpenButton,
                    self.manage_locations,
                ),
                self._ribbon_icon_button(
                    "Data Points",
                    "Manage data points",
                    QStyle.SP_DriveNetIcon,
                    self.manage_data_points,
                ),
            ],
            columns=3,
        )

        self._add_ribbon_group(
            data_layout,
            "Assign / Bulk",
            [
                self._ribbon_icon_button(
                    "Loc Dept",
                    "Location departments",
                    QStyle.SP_DialogYesButton,
                    self.manage_location_departments,
                ),
                self._ribbon_icon_button(
                    "DP Dept",
                    "Data point departments",
                    QStyle.SP_DialogYesButton,
                    self.manage_data_point_departments,
                ),
                self._ribbon_icon_button(
                    "Mass Loc",
                    "Mass create locations",
                    QStyle.SP_FileDialogNewFolder,
                    self.start_bulk_location_placement,
                ),
                self._ribbon_icon_button(
                    "Mass DP",
                    "Mass create data points",
                    QStyle.SP_FileIcon,
                    self.start_bulk_data_point_placement,
                ),
                self._ribbon_icon_button(
                    "Cancel Loc",
                    "Cancel mass create locations",
                    QStyle.SP_DialogCancelButton,
                    self.cancel_bulk_location_placement,
                ),
                self._ribbon_icon_button(
                    "Cancel DP",
                    "Cancel mass create data points",
                    QStyle.SP_DialogCancelButton,
                    self.cancel_bulk_data_point_placement,
                ),
            ],
            columns=3,
        )

        self._add_ribbon_group(
            data_layout,
            "Find",
            [
                self._ribbon_icon_button(
                    "Unconnected",
                    "Find unconnected data points",
                    QStyle.SP_MessageBoxQuestion,
                    self.find_unconnected_data_points,
                ),
                self._ribbon_icon_button(
                    "Unassigned",
                    "Find unassigned data points",
                    QStyle.SP_MessageBoxWarning,
                    self.show_unassigned_data_point_navigator,
                ),
                self._ribbon_icon_button(
                    "Manual RT",
                    "Find data points with manual or no room type",
                    QStyle.SP_MessageBoxWarning,
                    self.show_manual_room_type_data_point_navigator,
                ),
                self._ribbon_icon_button(
                    "Find DP",
                    "Find data point",
                    QStyle.SP_FileDialogContentsView,
                    self.show_find_data_point_dialog,
                ),
            ],
            columns=3,
        )
        data_layout.setAlignment(Qt.AlignLeft | Qt.AlignTop)

        self._add_scrollable_ribbon_tab(ribbon, data_tab, "Data")

        # ---------------- Routing tab ----------------
        routing_tab = QWidget()
        routing_layout = QHBoxLayout(routing_tab)
        routing_layout.setAlignment(Qt.AlignLeft | Qt.AlignTop)

        self.autoroute_same_floor_check = self._ribbon_toggle_button(
            "Same Floor",
            QStyle.SP_DialogApplyButton,
            False,
        )
        self.autoroute_same_floor_check.setToolTip(
            "When enabled, autoroute only considers comms rooms and graph paths "
            "on the data point's own floor."
        )
        self.autoroute_follow_existing_check = self._ribbon_toggle_button(
            "Follow Existing",
            QStyle.SP_ArrowForward,
            False,
        )
        self.autoroute_follow_existing_check.setToolTip(
            "Prefer graph segments already used by autorouted data-point "
            "connections while still enforcing the true cable-distance limit."
        )
        self.autoroute_ignore_unconnected_check = self._ribbon_toggle_button(
            "Ignore Unconnected",
            QStyle.SP_DialogCancelButton,
            False,
        )
        self.autoroute_ignore_unconnected_check.setToolTip(
            "Skip data points that have no connection to the routing graph. "
            "This option is off by default."
        )

        self._add_ribbon_group(
            routing_layout,
            "Routing",
            [
                self._ribbon_icon_button(
                    "Transitions",
                    "Manage transitions",
                    QStyle.SP_ArrowUp,
                    self.manage_transitions,
                ),
                self._ribbon_icon_button(
                    "Connections",
                    "Manage connections",
                    QStyle.SP_ArrowRight,
                    self.manage_connections,
                ),
                self._ribbon_icon_button(
                    "Optimise",
                    "Optimise comms rooms",
                    QStyle.SP_ComputerIcon,
                    self.optimise_comms_rooms_for_model,
                ),
                self._ribbon_icon_button(
                    "Autoroute",
                    "Autoroute data points",
                    QStyle.SP_BrowserReload,
                    self.autoroute_data_points,
                ),
                self.autoroute_same_floor_check,
                self.autoroute_follow_existing_check,
                self.autoroute_ignore_unconnected_check,
                self._ribbon_icon_button(
                    "Suggest",
                    "Suggest comms room",
                    QStyle.SP_MessageBoxInformation,
                    self.suggest_comms_room_for_department,
                ),
                self._ribbon_icon_button(
                    "Zone Suggest",
                    "Suggest comms rooms and DERs from placement zones",
                    QStyle.SP_DialogApplyButton,
                    self.suggest_equipment_rooms_from_zones,
                ),
                self._ribbon_icon_button(
                    "Suggest Zones",
                    "Suggest placement zones from unconnected port demand",
                    QStyle.SP_DialogApplyButton,
                    self.suggest_equipment_room_placement_zones,
                ),
                self._ribbon_icon_button(
                    "Profiles",
                    "Route profiles",
                    QStyle.SP_FileDialogDetailedView,
                    self.manage_route_profiles,
                ),
                self._ribbon_icon_button(
                    "Copy Floors",
                    "Copy template between floors",
                    QStyle.SP_DialogOpenButton,
                    self.copy_template_between_floors,
                ),
                self._ribbon_icon_button(
                    "Room Zones",
                    "Manage allowed comms-room and DER placement zones",
                    QStyle.SP_FileDialogDetailedView,
                    self.manage_equipment_room_placement_zones,
                ),
            ],
            columns=4,
        )

        self._add_ribbon_group(
            routing_layout,
            "Corridor Defaults",
            [
                self.quick_add_corridor_check,
                QLabel("Height AFFL"),
                self.default_corridor_height_spin,
                QLabel("Cable Limit"),
                self.default_corridor_cable_limit_spin,
            ],
            columns=2,
        )

        self._add_scrollable_ribbon_tab(ribbon, routing_tab, "Routing")

        # ---------------- Map tab ----------------
        map_tab = QWidget()
        map_layout = QHBoxLayout(map_tab)
        map_layout.setAlignment(Qt.AlignLeft | Qt.AlignTop)

        self._add_ribbon_group(
            map_layout,
            "DXF / View",
            [
                self._ribbon_icon_button(
                    "Map DXF", "Map DXF to floor", QStyle.SP_DirOpenIcon, self.load_dxf
                ),
                self._ribbon_icon_button(
                    "Clear DXF",
                    "Clear floor DXF",
                    QStyle.SP_TrashIcon,
                    self.clear_floor_dxf,
                ),
                self._ribbon_icon_button(
                    "Fit", "Fit view", QStyle.SP_TitleBarMaxButton, self.fit_view
                ),
                self._ribbon_icon_button(
                    "Export",
                    "Export floor DXFs",
                    QStyle.SP_DriveHDIcon,
                    self.export_floor_dxfs,
                ),
                self._ribbon_icon_button(
                    "Performance",
                    "Configure the Vulkan/RHI viewer and frame-rate limit",
                    QStyle.SP_ComputerIcon,
                    self.show_renderer_performance_dialog,
                ),
            ],
            columns=2,
        )        

        self._add_scrollable_ribbon_tab(ribbon, map_tab, "Maps")

        # ---------------- Output tab ----------------
        output_tab = QWidget()
        output_layout = QHBoxLayout(output_tab)
        output_layout.setAlignment(Qt.AlignLeft | Qt.AlignTop)

        self._add_ribbon_group(
            output_layout,
            "Reports",
            [
                self._ribbon_icon_button(
                    "Cable CSV",
                    "Generate cable length CSV",
                    QStyle.SP_FileDialogDetailedView,
                    self.generate_cable_report,
                ),
                self._ribbon_icon_button(
                    "Project PDF",
                    "Export room, use-case and network summary PDF",
                    QStyle.SP_FileIcon,
                    self.export_project_summary_pdf,
                ),
                self._ribbon_icon_button(
                    "Asset Register",
                    "Export the project asset library and deployment totals",
                    QStyle.SP_FileIcon,
                    self.export_asset_register_pdf,
                ),
                self._ribbon_icon_button(
                    "Floor PDF",
                    "Export one, selected, or all floors as scaled drawing sheets",
                    QStyle.SP_FileIcon,
                    self.export_all_floors_pdf,
                ),
                self._ribbon_icon_button(
                    "Zone PDF",
                    "Export placement zones on one, selected, or all floors",
                    QStyle.SP_FileIcon,
                    self.export_all_floor_zones_pdf,
                ),
                self._ribbon_icon_button(
                    "Room Extents PDF",
                    "Export current and maximum graph extents for every comms room and DER",
                    QStyle.SP_FileIcon,
                    self.export_equipment_room_extents_pdf,
                ),
                self._ribbon_icon_button(
                    "Room RFI PDF",
                    "Export outstanding room type asset queries and audit history",
                    QStyle.SP_FileIcon,
                    self.export_room_type_asset_rfi_pdf,
                ),
            ],
            columns=5,
        )

        self._add_scrollable_ribbon_tab(ribbon, output_tab, "Output")

        ribbon.setStyleSheet(BOOTSTRAP_RIBBON_STYLESHEET)

    def delete_right_clicked_items(self, picked):
        if not picked:
            return

        names_to_delete = []

        if picked in self.selected_template_names:
            names_to_delete = sorted(self.selected_template_names)
        else:
            names_to_delete = [picked]

        names_to_delete = [
            name for name in names_to_delete if name in self.store.names_in_use()
        ]

        if not names_to_delete:
            return

        if len(names_to_delete) == 1:
            message = f"Delete {names_to_delete[0]}?"
        else:
            message = f"Delete {len(names_to_delete)} selected item(s)?"

        if QMessageBox.question(self, "Delete", message) != QMessageBox.Yes:
            return

        self.push_undo_state("Delete selected item(s)")

        transition_ids = {
            str(t.get("id", "")).strip()
            for t in self.store.data.get("transitions", [])
            if str(t.get("id", "")).strip()
        }

        deleted = 0

        for name in names_to_delete:
            if "-F" in name and name.rsplit("-F", 1)[0] in transition_ids:
                self.store.delete_transition(name.rsplit("-F", 1)[0])
            else:
                self.store.delete_point(name)
            deleted += 1

        self.selected_point_name = None
        self._clear_canvas_multi_selection()
        if hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()
        self.refresh_canvas()
        self.set_status(f"Deleted {deleted} item(s)")

    def delete_placement_zone(self, zone_id, confirm=True):
        zone = self._placement_zone_by_id(zone_id)
        if zone is None:
            return False
        zone_id = str(zone.get("id", "")).strip()
        zone_name = str(zone.get("name", zone_id) or zone_id).strip()
        if confirm and QMessageBox.question(
            self,
            "Delete placement zone",
            f"Delete placement zone {zone_name} ({zone_id})?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        ) != QMessageBox.Yes:
            return False
        self.push_undo_state("Delete equipment room placement zone")
        self.store.data["equipment_room_placement_zones"] = [
            item
            for item in self.store.data.get(
                "equipment_room_placement_zones", []
            )
            if str(item.get("id", "")).strip() != zone_id
        ]
        if self.selected_placement_zone_id == zone_id:
            self.selected_placement_zone_id = None
        self.dragging_placement_zone_id = None
        self.dragging_placement_zone_handle = None
        self.placement_zone_drag_start = None
        self.placement_zone_drag_original = None
        self._invalidate_static_scene_cache()
        self.refresh_canvas()
        self.set_status(f"Deleted placement zone {zone_id}")
        return True

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Escape and self.placement_zone_start is not None:
            self.placement_zone_start = None
            self._clear_placement_zone_preview()
            self.set_status("Placement zone cancelled")
            event.accept()
            return
        if (
            event.key() in {Qt.Key_Delete, Qt.Key_Backspace}
            and self.selected_placement_zone_id
        ):
            if self.delete_placement_zone(self.selected_placement_zone_id):
                event.accept()
                return
        super().keyPressEvent(event)

    def _selected_copyable_names(self):
        floor = self.floor_spin.value()
        eligible = self._eligible_template_name_set(floor)
        return sorted(name for name in self.selected_template_names if name in eligible)

    def copy_selected_template_items(self):
        selected = self._selected_copyable_names()
        if not selected:
            self.set_status("No selected items to copy")
            return

        points = self.store.all_points()
        valid = [name for name in selected if name in points]
        if not valid:
            self.set_status("No valid selected items to copy")
            return

        base_x = min(float(points[name]["x"]) for name in valid)
        base_y = min(float(points[name]["y"]) for name in valid)
        source_floor = int(self.floor_spin.value())

        items = []
        for name in valid:
            point = points[name]
            kind = str(point.get("kind", "")).strip()

            if kind not in {
                "corridor_node",
                "data_point",
                "location",
                "comms_room",
                "distributed_equipment_room",
            }:
                continue

            items.append(
                {
                    "name": name,
                    "kind": kind,
                    "offset_x": float(point["x"]) - base_x,
                    "offset_y": float(point["y"]) - base_y,
                }
            )

        if not items:
            self.set_status("No copyable items selected")
            return

        selected_set = {item["name"] for item in items}
        internal_edges = []
        for edge in self.store.data.get("corridors", {}).get("edges", []):
            start = str(edge.get("from", "")).strip()
            end = str(edge.get("to", "")).strip()
            if start in selected_set and end in selected_set:
                internal_edges.append({"from": start, "to": end})

        self.selection_clipboard = {
            "source_floor": source_floor,
            "base_x": base_x,
            "base_y": base_y,
            "items": items,
            "internal_edges": internal_edges,
        }

        self.set_status(
            f"Copied {len(items)} item(s) with offsets from base point ({base_x:.3f}, {base_y:.3f})"
        )

    def paste_selected_template_items_at_view_centre(self):
        centre = self.canvas.mapToScene(self.canvas.viewport().rect().center())
        x, y = self.scene_to_world(centre.x(), centre.y())
        x, y = self.snap(x, y)
        self.paste_selected_template_items_at(x, y, self.floor_spin.value())

    def paste_selected_template_items_at(self, base_x, base_y, target_floor):
        if not self.selection_clipboard:
            self.set_status("Nothing copied")
            return

        items = list(self.selection_clipboard.get("items", []))
        if not items:
            self.set_status("Copied selection is empty")
            return

        self.push_undo_state("Paste selected items")

        used_names = set(self.store.names_in_use())
        id_map = {}
        created_names = []

        for copied in items:
            old_name = str(copied.get("name", "")).strip()
            kind, record = self.store._point_record_by_name(old_name)

            if kind is None or record is None:
                continue

            x = round(float(base_x) + float(copied.get("offset_x", 0.0)), 3)
            y = round(float(base_y) + float(copied.get("offset_y", 0.0)), 3)

            if kind == "corridor_node":
                new_name = self.store._suggest_next_corridor_name_for_floor(
                    target_floor,
                    used_names,
                )
                used_names.add(new_name)

                new_record = deepcopy(record)
                new_record["name"] = new_name
                new_record["floor"] = int(target_floor)
                new_record["x"] = x
                new_record["y"] = y

                self.store.data.setdefault("corridors", {}).setdefault(
                    "nodes", []
                ).append(new_record)

            elif kind == "data_point":
                new_name = self.store._suggest_next_data_point_name_for_floor(
                    target_floor,
                    used_names,
                )
                used_names.add(new_name)

                new_record = deepcopy(record)
                new_record["name"] = new_name
                new_record["floor"] = int(target_floor)
                new_record["x"] = x
                new_record["y"] = y

                self.store.data.setdefault("data_points", []).append(new_record)

            elif kind in {"location", "comms_room", "distributed_equipment_room"}:
                if kind in {"comms_room", "distributed_equipment_room"}:
                    new_name = self.store._suggest_next_comms_room_name_for_floor(
                        target_floor,
                        used_names,
                        kind,
                    )
                else:
                    new_name = self._next_available_bulk_location_name("LOC", 1)[0]

                while new_name in used_names:
                    new_name = f"{new_name}_copy"

                used_names.add(new_name)

                new_record = deepcopy(record)
                new_record["name"] = new_name
                new_record["floor"] = int(target_floor)
                new_record["x"] = x
                new_record["y"] = y

                self.store.data.setdefault("locations", []).append(new_record)

            else:
                continue

            id_map[old_name] = new_name
            created_names.append(new_name)

        created_edges = 0
        for edge in self.selection_clipboard.get("internal_edges", []):
            old_start = str(edge.get("from", "")).strip()
            old_end = str(edge.get("to", "")).strip()

            if old_start not in id_map or old_end not in id_map:
                continue

            self.store.add_edge(id_map[old_start], id_map[old_end])
            created_edges += 1

        if created_edges:
            self._mark_routing_graph_changed()

        self.selected_point_name = None
        self._set_canvas_multi_selection(created_names, append=False)
        if hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()
        self.refresh_canvas()

        self.set_status(
            f"Pasted {len(created_names)} item(s) at cursor position; recreated {created_edges} internal edge(s)"
        )

    def cancel_bulk_location_placement(self):
        if self.bulk_location_session:
            self.bulk_location_session = None
            self._clear_equipment_room_extent_overlay()
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
                "department_ids": [],
                "room_type_id": "",
            }

            self.mode_combo.setCurrentText("data_point")
            self.set_status(
                f"Mass create active: {count} data point(s) starting at {next_name}. Click to place."
            )

    def _connected_data_point_names(self):
        data_point_names = {
            str(point.get("name", "")).strip()
            for point in self.store.data.get("data_points", [])
            if str(point.get("name", "")).strip()
        }
        connected = set()
        for connection in self.store.data.get("connections", []):
            for field in ("from", "to"):
                name = str(connection.get(field, "")).strip()
                if name in data_point_names:
                    connected.add(name)
        return connected

    def _routing_unconnected_data_point_names(self):
        data_point_names = {
            str(point.get("name", "")).strip()
            for point in self.store.data.get("data_points", [])
            if str(point.get("name", "")).strip()
        }
        return data_point_names - self._connected_data_point_names()

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

        self.canvas.fit_to_rect(QRectF(point["x"] - 10, -point["y"] - 10, 20, 20), Qt.KeepAspectRatio)

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
        self.placement_zone_start = None
        self._clear_placement_zone_preview()
        self._clear_canvas_multi_selection()
        if hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()
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
            if hasattr(self.canvas, "invalidate_dxf_cache"):
                self.canvas.invalidate_dxf_cache()
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

    def _viewport_rect_to_scene_rect(self, rect):
        canvas = self.canvas
        if hasattr(canvas, "mapToScene"):
            return canvas.mapToScene(rect).boundingRect()
        if hasattr(canvas, "screen_to_scene"):
            top_left = canvas.screen_to_scene(rect.topLeft())
            bottom_right = canvas.screen_to_scene(rect.bottomRight())
            return QRectF(top_left, bottom_right).normalized()
        if hasattr(canvas, "screen_to_world"):
            top_left_world = canvas.screen_to_world(rect.topLeft())
            bottom_right_world = canvas.screen_to_world(rect.bottomRight())
            top_left = self.world_to_scene(*top_left_world)
            bottom_right = self.world_to_scene(*bottom_right_world)
            return QRectF(top_left, bottom_right).normalized()
        return QRectF()

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

    def _current_visible_scene_rect(self, padding=180.0):
        if not hasattr(self, "canvas") or self.canvas is None:
            return None

        viewport_rect = self.canvas.viewport().rect()
        if viewport_rect.isNull():
            return None

        visible_rect = self.canvas.mapToScene(viewport_rect).boundingRect()
        return visible_rect.adjusted(-padding, -padding, padding, padding)

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

        if rect is not None and not rect.isNull():
            self.canvas.resetTransform()
            self.canvas.fitInView(rect, Qt.KeepAspectRatio)

        self.refresh_canvas()

    def _clear_canvas_multi_selection(self):
        self.selected_template_names.clear()

    def _eligible_template_name_set(self, floor):
        result = set()

        for name, point in self.store.points_for_floor(floor).items():
            if point.get("kind") not in {"corridor_node", "data_point"}:
                continue
            point = {**point, "name": name}
            if not self._is_point_kind_visible(point):
                continue
            result.add(name)

        return result

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

        scene_rect = self._viewport_rect_to_scene_rect(rect)
        if scene_rect.isEmpty():
            return False
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

    def _clear_static_scene_items(self):
        for item in getattr(self, "_static_scene_items", []):
            try:
                self.scene.removeItem(item)
            except RuntimeError:
                pass

        self._static_scene_items = []

    def _static_scene_cache_key(self, floor, visible_rect):
        scale = self.canvas.transform().m11()
        label_tier = 0 if scale < 6 else 1 if scale < 12 else 2

        return (
            int(floor),
            bool(self.show_dxf_check.isChecked()),
            bool(self.show_edges_check.isChecked()),
            self.loaded_dxf_floor,
            self.current_dxf_path,
            len(self.dxf_scene.entities),
            label_tier,
        )

    def _rebuild_static_scene_items(self, floor, visible_rect):
        self._clear_static_scene_items()

        created_items = []

        if (
            self.show_dxf_check.isChecked()
            and self.loaded_dxf_floor == int(floor)
            and self.dxf_scene.entities
        ):
            created_items.extend(
                self.dxf_scene.populate_graphics_scene(
                    self.scene,
                    self.canvas.transform().m11(),
                    visible_rect=visible_rect,
                )
            )

        if self.show_edges_check.isChecked():
            before_items = set(self.scene.items())
            self.draw_edges(floor, visible_rect)
            after_items = set(self.scene.items())
            created_items.extend(list(after_items - before_items))

        self._static_scene_items = created_items
        self._static_scene_key = self._static_scene_cache_key(floor, visible_rect)
        self._static_scene_loaded_rect = (
            QRectF(visible_rect) if visible_rect is not None else None
        )

    def _ensure_static_scene_items(self, floor, visible_rect):
        key = self._static_scene_cache_key(floor, visible_rect)

        if (
            key == self._static_scene_key
            and getattr(self, "_static_scene_loaded_rect", None) is not None
            and visible_rect is not None
            and self._static_scene_loaded_rect.contains(visible_rect)
        ):
            return

        self._rebuild_static_scene_items(floor, visible_rect)

    def refresh_canvas_geometry_only(self):
        """Invalidate graph geometry without re-running the full UI refresh path.

        This is used for high-frequency drag updates. Layer controls, DXF
        bindings, search lists, file labels and overlay signatures are
        unchanged while an item is moving.
        """
        canvas = getattr(self, "canvas", None)
        if canvas is None:
            return

        # Mouse-move updates happen many times per second.  On large projects
        # redrawing the complete edge/routing layer every time is slow enough
        # that the dragged marker can appear not to move until release.  Prefer
        # the renderer's lightweight live-drag path, then let on_left_release()
        # perform the full edge/object redraw once the final position is known.
        if (
            getattr(self, "drag_mode_active", False)
            and getattr(self, "dragging_point_name", None)
            and hasattr(canvas, "notify_moving_object_changed")
        ):
            canvas.notify_moving_object_changed()
            return

        if hasattr(canvas, "notify_store_changed"):
            canvas.notify_store_changed()
            return
        if hasattr(canvas, "invalidate_store_cache"):
            canvas.invalidate_store_cache()
            return
        self.refresh_canvas()

    def _mark_routing_graph_changed(self):
        """Invalidate retained graph geometry after an edge-list mutation."""
        self._render_data_revision += 1
        canvas = getattr(self, "canvas", None)
        if canvas is None:
            return
        if hasattr(canvas, "notify_store_changed"):
            canvas.notify_store_changed()
        elif hasattr(canvas, "invalidate_store_cache"):
            canvas.invalidate_store_cache()

    def refresh_canvas(self):
        self._unconnected_cache = self._unconnected_data_point_names()
        self._routing_unconnected_cache = (
            self._routing_unconnected_data_point_names()
        )

        floor = self.floor_spin.value()
        self.ensure_floor_dxf_loaded(floor)

        try:
            self.canvas.set_store(self.store, self._render_data_revision)
        except TypeError:
            self.canvas.set_store(self.store)
        self.canvas.set_dxf_scene(self.dxf_scene)
        self.canvas.set_floor(floor)
        self.canvas.set_visible_layers(
            show_dxf=self.show_dxf_check.isChecked(),
            show_labels=self.show_labels_check.isChecked(),
            show_graph=True,
            show_overlay=True,
            show_edges=self.show_edges_check.isChecked(),
            show_nodes=self.show_nodes_check.isChecked(),
            show_data_points=self.show_data_points_check.isChecked(),
            show_unconnected_data_points_only=(
                self.show_unconnected_data_points_only_check.isChecked()
            ),
            unconnected_data_point_names=self._unconnected_cache,
            show_routing_unconnected_data_points_only=(
                self.show_routing_unconnected_data_points_only_check.isChecked()
            ),
            routing_unconnected_data_point_names=self._routing_unconnected_cache,
            hide_connected_data_points=self.hide_connected_data_points_check.isChecked(),
            connected_data_point_names=self._connected_data_point_names(),
            show_locations=self.show_locations_check.isChecked(),
            show_comms_rooms=self.show_comms_rooms_check.isChecked(),
            show_placement_zones=self.show_placement_zones_check.isChecked(),
            show_departments=self.show_departments_check.isChecked(),
        )
        self.canvas.set_selection(
            self.selected_point_name,
            self.selected_template_names,
            self.selected_for_edge,
        )
        if hasattr(self.canvas, "set_placement_zone_selection"):
            self.canvas.set_placement_zone_selection(
                self.selected_placement_zone_id
            )

        self.file_label.setText(self.current_json_path or "New file")
        overlay_signature = (
            self.current_json_path,
            int(floor),
            self.mode_combo.currentText(),
            self.selected_for_edge,
            tuple(sorted(self.selected_template_names)),
            self.get_floor_dxf_path(floor),
            self.canvas.target_fps() if hasattr(self.canvas, "target_fps") else 0,
            bool(self._show_render_stats),
        )
        if overlay_signature != self._last_overlay_signature:
            self._last_overlay_signature = overlay_signature
            if hasattr(self.canvas, "invalidate_overlay"):
                self.canvas.invalidate_overlay()

    def draw_edges(self, floor, visible_rect=None):
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
            edge_rect = QRectF(pa, pb).normalized().adjusted(-2.0, -2.0, 2.0, 2.0)
            if visible_rect is not None and not visible_rect.intersects(edge_rect):
                continue
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

    def draw_departments(self, floor, visible_rect=None):
        for department_id, dept in self.store.departments_for_floor(floor).items():
            pos = self.world_to_scene(dept["x"], dept["y"])
            item_rect = QRectF(pos.x() - 1.2, pos.y() - 1.2, 2.4, 2.4)
            if visible_rect is not None and not visible_rect.intersects(item_rect):
                continue
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

    def _draw_data_point_assignment_marks(self, pos, point, visible_rect=None):
        if visible_rect is not None:
            marker_rect = QRectF(pos.x() + 0.4, pos.y() - 0.6, 0.8, 1.0)
            if not visible_rect.intersects(marker_rect):
                return

        green = QColor("#00ff66")
        pen = QPen(QColor("#003d1f"), 0.05)
        brush = QBrush(green)

        has_location = bool(
            [str(x).strip() for x in point.get("department_ids", []) if str(x).strip()]
        )
        has_room_type = bool(str(point.get("room_type_id", "") or "").strip())

        if has_location:
            r = 0.12
            self.scene.addEllipse(
                pos.x() + 0.55,
                pos.y() - 0.36,
                r * 2,
                r * 2,
                pen,
                brush,
            )

        if has_room_type:
            s = 0.16
            triangle = QPolygonF(
                [
                    QPointF(pos.x() + 0.67, pos.y() + 0.18 - s),
                    QPointF(pos.x() + 0.67 + s, pos.y() + 0.18 + s),
                    QPointF(pos.x() + 0.67 - s, pos.y() + 0.18 + s),
                ]
            )
            marker = QGraphicsPolygonItem(triangle)
            marker.setBrush(brush)
            marker.setPen(pen)
            self.scene.addItem(marker)

    def _data_point_tooltip(self, name, point):
        department_lookup = {
            str(item.get("id", "")).strip(): str(item.get("name", "")).strip()
            for item in self.store.data.get("departments", [])
            if str(item.get("id", "")).strip()
        }

        room_type_lookup = {
            str(item.get("id", "")).strip(): str(item.get("name", "")).strip()
            for item in self.store.data.get("room_types", [])
            if str(item.get("id", "")).strip()
        }

        department_ids = [
            str(x).strip() for x in point.get("department_ids", []) if str(x).strip()
        ]

        departments = [
            f"{dept_id} - {department_lookup.get(dept_id, '')}".strip(" -")
            for dept_id in department_ids
        ]

        room_type_id = str(point.get("room_type_id", "") or "").strip()
        room_type_name = room_type_lookup.get(room_type_id, "")

        return "\n".join(
            [
                f"Data point: {name}",
                f"Department: {', '.join(departments) if departments else 'Unassigned'}",
                f"Room type: {(room_type_id + ' - ' + room_type_name).strip(' -') if room_type_id else 'Manual / no room type'}",
                f"Qty: {int(point.get('qty', 1) or 1)}",
            ]
        )

    def draw_points(self, floor, visible_rect=None):
        connected_data_points = self._connected_data_point_names()
        hide_connected = self.hide_connected_data_points_check.isChecked()
        unconnected = getattr(self, "_unconnected_cache", set())
        show_unconnected_only = (
            self.show_unconnected_data_points_only_check.isChecked()
        )
        routing_unconnected = getattr(self, "_routing_unconnected_cache", set())
        show_routing_unconnected_only = (
            self.show_routing_unconnected_data_points_only_check.isChecked()
        )
        for name, point in self.store.points_for_floor(floor).items():
            pos = self.world_to_scene(point["x"], point["y"])
            item_rect = QRectF(pos.x() - 1.2, pos.y() - 1.2, 2.4, 2.4)
            if visible_rect is not None and not visible_rect.intersects(item_rect):
                continue
            selected = (name == self.selected_point_name) or (
                name in self.selected_template_names
            )
            kind = point.get("kind")

            if kind == "corridor_node" and not self.show_nodes_check.isChecked():
                continue

            if kind == "data_point" and not self.show_data_points_check.isChecked():
                continue

            if (
                self.show_unassigned_room_types_only_check.isChecked()
                and point.get("kind") == "data_point"
                and str(point.get("room_type_id", "")).strip()
            ):
                continue

            if (
                point.get("kind") == "data_point"
                and self.show_unassigned_room_types_only_check.isChecked()
                and str(point.get("room_type_id", "") or "").strip()
            ):
                continue

            if (
                kind == "data_point"
                and hide_connected
                and name in connected_data_points
            ):
                continue

            if (
                kind == "data_point"
                and show_unconnected_only
                and name not in unconnected
            ):
                continue

            if (
                kind == "data_point"
                and show_routing_unconnected_only
                and name not in routing_unconnected
            ):
                continue

            if kind == "location" and not self.show_locations_check.isChecked():
                continue

            if kind == "comms_room" and not self.show_comms_rooms_check.isChecked():
                continue

            outline = QPen(QColor("#ffffff") if selected else QColor("transparent"), 0)
            is_der = kind == "distributed_equipment_room" or (
                kind == "location" and str(name).upper().startswith("DER")
            )

            if is_der:
                half_width = 0.48
                half_height = 0.38
                path = QPainterPath()
                path.addRoundedRect(
                    QRectF(
                        pos.x() - half_width,
                        pos.y() - half_height,
                        2 * half_width,
                        2 * half_height,
                    ),
                    0.1,
                    0.1,
                )
                path.moveTo(pos.x() - half_width + 0.1, pos.y())
                path.lineTo(pos.x() + half_width - 0.1, pos.y())
                item = QGraphicsPathItem(path)
                item.setBrush(QBrush(QColor("#35a7ff")))
                item.setPen(
                    QPen(QColor("#ffffff") if selected else QColor("#9edcff"), 0.08)
                )
                self.scene.addItem(item)
                label_color = QColor("#bfe8ff")
            elif kind in {"location", "comms_room"}:
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

                item = QGraphicsPolygonItem(poly)
                brush = QBrush(QColor("#b07cff"))
                item.setBrush(brush)
                item.setPen(
                    QPen(QColor("#ffffff") if selected else QColor("#d5bbff"), 0.08)
                )
                self.scene.addItem(item)
                label_color = QColor("#eadcff")

                self._draw_data_point_assignment_marks(pos, point, visible_rect)
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

            if kind == "data_point":
                item.setAcceptHoverEvents(True)
                item.setToolTip(self._data_point_tooltip(name, point))

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
            before = len(self.store.data.get("corridors", {}).get("edges", []))
            for edge in selected_edges:
                self.store.remove_edge(edge.get("from"), edge.get("to"))
            after = len(self.store.data.get("corridors", {}).get("edges", []))
            if after != before:
                self._mark_routing_graph_changed()
            self.refresh_canvas()

        dialog = EdgeConnectionsDialog(self, point_name, rows, on_delete)
        dialog.exec()

    def draw_overlay_panels(self, painter, viewport_rect):
        # The canvas legend was visually competing with the drawing area.
        # Keep the overlay hook in place, but do not paint the legend panel.
        return
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
            "Blue split rectangle = DER",
            "Red diamond = transition",
            "Green dot by data point = location / department assigned",
            "Green triangle by data point = room type assigned",
            f"Mode: {self.mode_combo.currentText()} | Floor: {floor}",
            f"DXF: {dxf_name}",
            f"Edge chain start: {active_edge_start}",
            f"Template selection: {len(self.selected_template_names)}",
            "Drag in select_move to multi-select template items",
            "Double-click a point to edit",
        ]
        if self._show_render_stats and hasattr(self.canvas, "render_stats"):
            stats = self.canvas.render_stats()
            lines.extend(
                [
                    f"Renderer: {stats.get('backend', 'Unknown')}",
                    f"Frame cap: {stats.get('target_fps', 0)} FPS | "
                    f"Last: {stats.get('actual_fps', 0):.1f} FPS / "
                    f"{stats.get('last_frame_ms', 0):.2f} ms",
                ]
            )
        self._draw_overlay_box(painter, 12, 12, 390, lines, "#333333", "white")

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
        name = str(point.get("name", "")).strip()

        if kind == "corridor_node":
            return self.show_nodes_check.isChecked()

        if kind == "data_point":
            if not self.show_data_points_check.isChecked():
                return False
            if (
                self.hide_connected_data_points_check.isChecked()
                and name in self._connected_data_point_names()
            ):
                return False

            if (
                self.show_unconnected_data_points_only_check.isChecked()
                and name not in getattr(self, "_unconnected_cache", set())
            ):
                return False

            if (
                self.show_routing_unconnected_data_points_only_check.isChecked()
                and name not in getattr(self, "_routing_unconnected_cache", set())
            ):
                return False

            if (
                point.get("kind") == "data_point"
                and self.show_unassigned_room_types_only_check.isChecked()
                and str(point.get("room_type_id", "") or "").strip()
            ):
                return False

            return True

        if kind == "location":
            return self.show_locations_check.isChecked()

        if kind == "comms_room":
            return self.show_comms_rooms_check.isChecked()

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

    def find_nearest_selectable_name(
        self,
        x,
        y,
        floor,
        radius_world=1.0,
    ):
        best_name = None
        best_dist = None

        for name, point in self.store.points_for_floor(floor).items():
            point = {**point, "name": name}

            if not self._is_point_kind_visible(point):
                continue

            dist = math.hypot(
                float(point["x"]) - float(x),
                float(point["y"]) - float(y),
            )

            if dist <= float(radius_world) and (
                best_dist is None or dist < best_dist
            ):
                best_name = name
                best_dist = dist

        if self._is_department_visible():
            for department_id, dept in self.store.departments_for_floor(floor).items():
                dist = math.hypot(
                    float(dept["x"]) - float(x),
                    float(dept["y"]) - float(y),
                )

                if dist <= float(radius_world) and (
                    best_dist is None or dist < best_dist
                ):
                    best_name = department_id
                    best_dist = dist

        return best_name

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
            if str(item.get("kind", "")) not in {
                "comms_room",
                "distributed_equipment_room",
            }:
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
                    "qty": self.store.data_point_required_port_count(item),
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
        required_ports_by_name = {
            str(point.get("name", "")).strip(): int(
                self.store.data_point_required_port_count(point)
            )
            for point in self.store.data.get("data_points", [])
            if str(point.get("name", "")).strip()
        }
        selected_port_count = sum(
            required_ports_by_name.get(name, 0) for name in data_point_names
        )

        max_cable_length_m = float(dialog.result["max_cable_length_m"])
        room_name = dialog.result["room_name"].strip()
        location_kind = str(
            dialog.result.get("location_kind", "comms_room") or "comms_room"
        ).strip()
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

        candidate_nodes = self._candidate_comms_room_nodes(
            location_kind=location_kind
        )
        if not candidate_nodes:
            QMessageBox.critical(
                self,
                "Suggest Comms Room",
                "No permitted corridor nodes are available for the selected "
                "location type. Review the placement zones on the relevant floors.",
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

                total_length += (
                    cable_length * required_ports_by_name.get(point_name, 0)
                )
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

        room_x, room_y = self._placement_coordinates_near_candidate(
            candidate_point, location_kind
        )

        self.store.add_location(
            room_name,
            int(candidate_point["floor"]),
            room_x,
            room_y,
            kind=location_kind,
        )

        self._connect_equipment_room_to_corridor_anchor(
            room_name, best_candidate
        )

        self.selected_point_name = room_name
        self.refresh_canvas()

        QMessageBox.information(
            self,
            "Suggest Comms Room",
            "\n".join(
                [
                    f"Placed {location_kind.replace('_', ' ')} {room_name}",
                    f"Candidate node: {best_candidate}",
                    f"Data points: {len(data_point_names)}",
                    f"Required ports: {selected_port_count}",
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

    @staticmethod
    def _zone_allows_location_kind(zone, location_kind):
        if location_kind == "distributed_equipment_room":
            return bool(zone.get("allow_distributed_equipment_room", False))
        return bool(zone.get("allow_comms_room", False))

    @staticmethod
    def _point_inside_placement_zone(x, y, zone):
        return (
            float(zone.get("min_x", 0.0)) <= float(x) <= float(zone.get("max_x", 0.0))
            and float(zone.get("min_y", 0.0)) <= float(y) <= float(zone.get("max_y", 0.0))
        )

    def _equipment_room_placement_zones(self, floor=None):
        zones = [
            zone
            for zone in self.store.data.get("equipment_room_placement_zones", [])
            if isinstance(zone, dict)
        ]
        if floor is None:
            return zones
        return [zone for zone in zones if int(zone.get("floor", 0)) == int(floor)]

    @staticmethod
    def _placement_zone_handles(zone):
        min_x = float(zone.get("min_x", 0.0))
        max_x = float(zone.get("max_x", 0.0))
        min_y = float(zone.get("min_y", 0.0))
        max_y = float(zone.get("max_y", 0.0))
        mid_x = (min_x + max_x) / 2.0
        mid_y = (min_y + max_y) / 2.0
        return {
            "nw": (min_x, max_y),
            "n": (mid_x, max_y),
            "ne": (max_x, max_y),
            "e": (max_x, mid_y),
            "se": (max_x, min_y),
            "s": (mid_x, min_y),
            "sw": (min_x, min_y),
            "w": (min_x, mid_y),
        }

    def _find_placement_zone_hit(self, x, y, floor):
        if (
            hasattr(self, "show_placement_zones_check")
            and not self.show_placement_zones_check.isChecked()
        ):
            return None, None
        zones = list(reversed(self._equipment_room_placement_zones(floor)))
        scale = max(0.001, float(getattr(self.canvas, "_scale", 1.0) or 1.0))
        radius = max(0.18, 9.0 / scale)
        selected_zone = next(
            (
                zone
                for zone in zones
                if str(zone.get("id", "")).strip()
                == str(self.selected_placement_zone_id or "").strip()
            ),
            None,
        )
        if selected_zone is not None:
            selected_handle = None
            selected_distance = None
            for handle, (hx, hy) in self._placement_zone_handles(
                selected_zone
            ).items():
                distance = math.hypot(float(x) - hx, float(y) - hy)
                if distance <= radius and (
                    selected_distance is None or distance < selected_distance
                ):
                    selected_handle = handle
                    selected_distance = distance
            if selected_handle is not None:
                return selected_zone, selected_handle
            if self._point_inside_placement_zone(x, y, selected_zone):
                return selected_zone, "body"

        if selected_zone is not None:
            zones = [zone for zone in zones if zone is not selected_zone]
        best = None
        for zone in zones:
            for handle, (hx, hy) in self._placement_zone_handles(zone).items():
                distance = math.hypot(float(x) - hx, float(y) - hy)
                if distance <= radius and (best is None or distance < best[0]):
                    best = (distance, zone, handle)
        if best is not None:
            return best[1], best[2]
        containing = [
            zone
            for zone in zones
            if self._point_inside_placement_zone(x, y, zone)
        ]
        if not containing:
            return None, None
        zone = min(
            containing,
            key=lambda item: (
                (float(item.get("max_x", 0.0)) - float(item.get("min_x", 0.0)))
                * (float(item.get("max_y", 0.0)) - float(item.get("min_y", 0.0)))
            ),
        )
        return zone, "body"

    def _placement_zone_by_id(self, zone_id):
        return next(
            (
                zone
                for zone in self.store.data.get(
                    "equipment_room_placement_zones", []
                )
                if str(zone.get("id", "")).strip() == str(zone_id).strip()
            ),
            None,
        )

    @staticmethod
    def _zone_room_limit(zone, location_kind):
        key = (
            "max_distributed_equipment_rooms"
            if location_kind == "distributed_equipment_room"
            else "max_comms_rooms"
        )
        return max(0, int(zone.get(key, 0) or 0))

    @staticmethod
    def _equipment_room_location_kind(room):
        """Return the planner room type, including legacy DER records."""
        kind = str(room.get("kind", "") or "").strip()
        name = str(room.get("name", "") or "").strip().upper()
        cabinet_type = str(
            room.get("cabinet_type", "") or ""
        ).strip().lower()
        if (
            kind == "distributed_equipment_room"
            or name.startswith("DER")
            or cabinet_type == "slim_wall"
        ):
            return "distributed_equipment_room"
        if kind == "comms_room":
            return "comms_room"
        return ""

    def _existing_room_count_for_zone(self, zone, location_kind):
        zone_id = str(zone.get("id", "")).strip()
        floor = int(zone.get("floor", 0))
        count = 0
        for room in self.store.data.get("locations", []):
            if self._equipment_room_location_kind(room) != location_kind:
                continue
            if int(room.get("floor", 0)) != floor:
                continue
            assigned_zone = str(room.get("placement_zone_id", "")).strip()
            if assigned_zone:
                if assigned_zone == zone_id:
                    count += 1
                continue
            if self._point_inside_placement_zone(
                float(room.get("x", 0.0)), float(room.get("y", 0.0)), zone
            ):
                count += 1
        return count

    def _zone_has_room_availability(self, zone, location_kind):
        limit = self._zone_room_limit(zone, location_kind)
        return limit <= 0 or self._existing_room_count_for_zone(
            zone, location_kind
        ) < limit

    def _placement_allowed_at(self, floor, x, y, location_kind):
        floor_zones = self._equipment_room_placement_zones(floor)
        if not floor_zones:
            return True
        return any(
            self._zone_allows_location_kind(zone, location_kind)
            and self._point_inside_placement_zone(x, y, zone)
            and self._zone_has_room_availability(zone, location_kind)
            for zone in floor_zones
        )

    def _placement_coordinates_near_candidate(
        self, candidate_point, location_kind, room_index=0
    ):
        base_x = float(candidate_point.get("x", 0.0))
        base_y = float(candidate_point.get("y", 0.0))
        floor = int(candidate_point.get("floor", 0))
        angle = math.radians(45 + int(room_index) * 60)
        for radius in (1.5, 1.0, 0.5, 0.0):
            x = base_x + math.cos(angle) * radius
            y = base_y + math.sin(angle) * radius
            if self._placement_allowed_at(floor, x, y, location_kind):
                return x, y
        return base_x, base_y

    @staticmethod
    def _distance_from_point_to_zone(point, zone, floor_height_m=4.0):
        x = float(point.get("x", 0.0))
        y = float(point.get("y", 0.0))
        min_x = float(zone.get("min_x", 0.0))
        max_x = float(zone.get("max_x", 0.0))
        min_y = float(zone.get("min_y", 0.0))
        max_y = float(zone.get("max_y", 0.0))
        dx = max(min_x - x, 0.0, x - max_x)
        dy = max(min_y - y, 0.0, y - max_y)
        horizontal = math.hypot(dx, dy)
        vertical = abs(
            int(point.get("floor", 0)) - int(zone.get("floor", 0))
        ) * float(floor_height_m)
        return horizontal + vertical

    @staticmethod
    def _position_inside_zone_near_points(zone, points, room_index=0):
        min_x = float(zone.get("min_x", 0.0))
        max_x = float(zone.get("max_x", 0.0))
        min_y = float(zone.get("min_y", 0.0))
        max_y = float(zone.get("max_y", 0.0))
        weights = [
            max(
                1,
                int(
                    point.get(
                        "_required_ports", point.get("qty", 1)
                    )
                    or 1
                ),
            )
            for point in points
        ]
        total_weight = max(1, sum(weights))
        target_x = sum(
            float(point.get("x", 0.0)) * weight
            for point, weight in zip(points, weights)
        ) / total_weight
        target_y = sum(
            float(point.get("y", 0.0)) * weight
            for point, weight in zip(points, weights)
        ) / total_weight

        width = max(0.0, max_x - min_x)
        height = max(0.0, max_y - min_y)
        if room_index:
            angle = math.radians(45.0 + (room_index - 1) * 72.0)
            radius = min(max(width, 0.5), max(height, 0.5)) * 0.08
            target_x += math.cos(angle) * radius
            target_y += math.sin(angle) * radius

        margin_x = min(0.5, width / 4.0)
        margin_y = min(0.5, height / 4.0)
        low_x, high_x = min_x + margin_x, max_x - margin_x
        low_y, high_y = min_y + margin_y, max_y - margin_y
        x = (min_x + max_x) / 2.0 if low_x > high_x else min(max(target_x, low_x), high_x)
        y = (min_y + max_y) / 2.0 if low_y > high_y else min(max(target_y, low_y), high_y)
        return float(x), float(y)

    def _build_zone_room_suggestion_plan(
        self,
        max_distance_m,
        room_port_limits,
        same_floor_only=True,
        current_floor_only=False,
        design_strategy="shortest_routes",
        room_limit_overrides=None,
        planning_cache=None,
    ):
        room_limit_overrides = dict(room_limit_overrides or {})
        shared_cache = planning_cache if isinstance(planning_cache, dict) else {}
        scope_floor = int(self.floor_spin.value())
        context_key = (
            round(float(max_distance_m), 6),
            bool(same_floor_only),
            bool(current_floor_only),
            scope_floor,
        )
        context = shared_cache.setdefault(("zone-room-context", context_key), {})
        if isinstance(room_port_limits, dict):
            port_limits = {
                "comms_room": max(
                    1, int(room_port_limits.get("comms_room", 1) or 1)
                ),
                "distributed_equipment_room": max(
                    1,
                    int(
                        room_port_limits.get("distributed_equipment_room", 1)
                        or 1
                    ),
                ),
            }
        else:
            shared_limit = max(1, int(room_port_limits or 1))
            port_limits = {
                "comms_room": shared_limit,
                "distributed_equipment_room": shared_limit,
            }
        plan_key = (
            str(design_strategy),
            int(port_limits["comms_room"]),
            int(port_limits["distributed_equipment_room"]),
            tuple(
                sorted(
                    (
                        str(zone_key),
                        str(kind),
                        int(limit),
                    )
                    for (zone_key, kind), limit in room_limit_overrides.items()
                )
            ),
        )
        plan_results = context.setdefault("plan_results", {})
        if plan_key in plan_results:
            return deepcopy(plan_results[plan_key])
        if "zones" not in context:
            zones = []
            for index, source in enumerate(self._equipment_room_placement_zones()):
                if not (
                    bool(source.get("allow_comms_room", False))
                    or bool(source.get("allow_distributed_equipment_room", False))
                ):
                    continue
                zone = dict(source)
                zone["_suggestion_key"] = (
                    str(zone.get("id", "")).strip() or f"zone-{index + 1}"
                )
                zones.append(zone)
            context["zones"] = zones
        zones = context["zones"]

        if "data_points" not in context:
            connected_targets = self._existing_connection_targets()
            data_points = []
            for source in self.store.data.get("data_points", []):
                name = str(source.get("name", "")).strip()
                if (
                    not name
                    or name in connected_targets
                    or (
                        current_floor_only
                        and int(source.get("floor", 0)) != scope_floor
                    )
                ):
                    continue
                point = dict(source)
                if hasattr(self.store, "data_point_required_port_count"):
                    required_ports = self.store.data_point_required_port_count(point)
                else:
                    required_ports = max(0, int(point.get("qty", 1) or 0))
                point["_required_ports"] = int(required_ports)
                if required_ports > 0:
                    point["_zone_distance_cache_token"] = (
                        name,
                        int(point.get("floor", 0)),
                        float(point.get("x", 0.0) or 0.0),
                        float(point.get("y", 0.0) or 0.0),
                        float(
                            point.get("extension_distance_m", 0.0) or 0.0
                        ),
                    )
                    data_points.append(point)
            context["data_points"] = data_points
        data_points = context["data_points"]

        if "routing" not in context:
            routing_graph, routing_points = self._build_routing_graph()
            routing_anchors = [
                name
                for name, record in routing_points.items()
                if str(record.get("kind", "")).strip() == "corridor_node"
                and routing_graph.get(name)
            ]
            placement_anchors = [
                name
                for name in routing_anchors
                if not bool(routing_points[name].get("restricted", False))
            ]
            context["routing"] = (
                routing_graph,
                routing_points,
                routing_anchors,
                placement_anchors,
            )
        (
            routing_graph,
            routing_points,
            routing_anchors,
            placement_anchors,
        ) = context["routing"]

        if "components" not in context:
            component_by_name = {}
            component_number = 0
            for source_name in routing_graph:
                if source_name in component_by_name:
                    continue
                component_number += 1
                pending = [source_name]
                component_by_name[source_name] = component_number
                while pending:
                    current_name = pending.pop()
                    current_floor = int(routing_points[current_name].get("floor", 0))
                    for neighbour, _weight in routing_graph.get(current_name, []):
                        if (
                            same_floor_only
                            and int(routing_points[neighbour].get("floor", 0))
                            != current_floor
                        ):
                            continue
                        if neighbour in component_by_name:
                            continue
                        component_by_name[neighbour] = component_number
                        pending.append(neighbour)

            anchors_by_floor_component = {}
            for anchor_name in routing_anchors:
                anchor = routing_points[anchor_name]
                key = (
                    int(anchor.get("floor", 0)),
                    component_by_name.get(anchor_name),
                )
                anchors_by_floor_component.setdefault(key, []).append(anchor_name)
            context["components"] = (
                component_by_name,
                anchors_by_floor_component,
            )
        component_by_name, anchors_by_floor_component = context["components"]

        start_anchor_cache = context.setdefault("start_anchor_cache", {})

        def candidate_start_anchors(room_floor, room_x, room_y):
            cache_key = (
                int(room_floor),
                round(float(room_x), 4),
                round(float(room_y), 4),
            )
            if cache_key in start_anchor_cache:
                return start_anchor_cache[cache_key]
            result = []
            for (floor, _component), names in anchors_by_floor_component.items():
                if floor != int(room_floor):
                    continue
                result.append(
                    min(
                        names,
                        key=lambda name: math.hypot(
                            float(routing_points[name].get("x", 0.0)) - room_x,
                            float(routing_points[name].get("y", 0.0)) - room_y,
                        ),
                    )
                )
            start_anchor_cache[cache_key] = result
            return result

        distance_maps = context.setdefault("distance_maps", {})
        room_point_distance_cache = context.setdefault(
            "room_point_distance_cache", {}
        )

        def shortest_graph_distance(start, end, allowed_floor=None):
            if start not in routing_graph or end not in routing_graph:
                return None
            cache_key = (start, allowed_floor)
            if cache_key in distance_maps:
                return distance_maps[cache_key].get(end)
            pending = [(0.0, start)]
            best = {start: 0.0}
            while pending:
                cost, name = heapq.heappop(pending)
                if cost > best.get(name, math.inf):
                    continue
                for neighbour, weight in routing_graph.get(name, []):
                    if (
                        allowed_floor is not None
                        and int(routing_points[neighbour].get("floor", 0))
                        != int(allowed_floor)
                    ):
                        continue
                    candidate = cost + float(weight)
                    if candidate < best.get(neighbour, math.inf):
                        best[neighbour] = candidate
                        heapq.heappush(pending, (candidate, neighbour))
            distance_maps[cache_key] = best
            return best.get(end)

        def graph_distance_from_room(
            room_floor, room_x, room_y, point, start_anchor
        ):
            point_floor = int(point.get("floor", 0))
            if start_anchor not in routing_graph:
                return None
            point_name = str(point.get("name", "") or "").strip()
            cache_key = (
                int(room_floor),
                float(room_x),
                float(room_y),
                point.get(
                    "_zone_distance_cache_token",
                    (
                        point_name,
                        point_floor,
                        float(point.get("x", 0.0) or 0.0),
                        float(point.get("y", 0.0) or 0.0),
                        float(
                            point.get("extension_distance_m", 0.0) or 0.0
                        ),
                    ),
                ),
                str(start_anchor),
            )
            if cache_key in room_point_distance_cache:
                return room_point_distance_cache[cache_key]
            if point_name in routing_graph and routing_graph.get(point_name):
                end_candidates = [point_name]
            else:
                end_candidates = [
                    name
                    for name in routing_anchors
                    if int(routing_points[name].get("floor", 0)) == point_floor
                    and component_by_name.get(name)
                    == component_by_name.get(start_anchor)
                ]
                if not end_candidates:
                    room_point_distance_cache[cache_key] = None
                    return None
            best_end = None
            best_end_name = None
            for end in end_candidates:
                graph_distance = shortest_graph_distance(
                    start_anchor,
                    end,
                    int(room_floor) if same_floor_only else None,
                )
                if graph_distance is None:
                    continue
                end_spur = 0.0
                if end != point_name:
                    end_spur = math.hypot(
                        float(point.get("x", 0.0))
                        - float(routing_points[end].get("x", 0.0)),
                        float(point.get("y", 0.0))
                        - float(routing_points[end].get("y", 0.0)),
                    )
                total = float(graph_distance) + end_spur
                if best_end is None or total < best_end:
                    best_end = total
                    best_end_name = end
            if best_end is None:
                room_point_distance_cache[cache_key] = None
                return None
            start_spur = math.hypot(
                float(routing_points[start_anchor].get("x", 0.0)) - room_x,
                float(routing_points[start_anchor].get("y", 0.0)) - room_y,
            )
            result = (
                best_end
                + start_spur
                + max(0.0, float(point.get("extension_distance_m", 0.0) or 0.0)),
                best_end_name,
            )
            room_point_distance_cache[cache_key] = result
            return result

        zone_position_cache = context.setdefault("zone_position_cache", {})
        best_room_position_cache = context.setdefault(
            "best_room_position_cache", {}
        )

        def zone_candidate_positions(zone):
            zone_key = str(zone.get("_suggestion_key", "") or "")
            if zone_key in zone_position_cache:
                return zone_position_cache[zone_key]
            floor = int(zone.get("floor", 0))
            positions = []
            centre = {
                "x": (
                    float(zone.get("min_x", 0.0))
                    + float(zone.get("max_x", 0.0))
                )
                / 2.0,
                "y": (
                    float(zone.get("min_y", 0.0))
                    + float(zone.get("max_y", 0.0))
                )
                / 2.0,
                "_required_ports": 1,
            }
            positions.append(
                self._position_inside_zone_near_points(zone, [centre], 0)
            )
            for anchor_name in placement_anchors:
                anchor = routing_points[anchor_name]
                if int(anchor.get("floor", 0)) != floor:
                    continue
                if self._distance_from_point_to_zone(anchor, zone, 0.0) > float(
                    max_distance_m
                ):
                    continue
                positions.append(
                    self._position_inside_zone_near_points(
                        zone,
                        [{**anchor, "_required_ports": 1}],
                        0,
                    )
                )
            unique = []
            seen = set()
            for x, y in positions:
                key = (round(float(x), 4), round(float(y), 4))
                if key in seen:
                    continue
                seen.add(key)
                unique.append((float(x), float(y)))
            zone_position_cache[zone_key] = unique
            return unique

        def best_room_position(
            zone,
            members,
            preferred_positions=(),
            fixed_position=False,
            preferred_start_anchors=(),
        ):
            cache_key = (
                str(zone.get("_suggestion_key", "") or ""),
                tuple(str(member.get("name", "") or "") for member in members),
                tuple(
                    (float(position[0]), float(position[1]))
                    for position in preferred_positions
                ),
                bool(fixed_position),
                tuple(str(anchor or "") for anchor in preferred_start_anchors),
            )
            if cache_key in best_room_position_cache:
                return best_room_position_cache[cache_key]
            positions = list(preferred_positions)
            if not fixed_position:
                positions.append(
                    self._position_inside_zone_near_points(zone, members, 0)
                )
                positions.extend(zone_candidate_positions(zone))
            best_result = None
            seen = set()
            for room_x, room_y in positions:
                key = (round(float(room_x), 4), round(float(room_y), 4))
                if key in seen:
                    continue
                seen.add(key)
                start_anchors = list(preferred_start_anchors)
                if not start_anchors:
                    start_anchors = candidate_start_anchors(
                        int(zone.get("floor", 0)), room_x, room_y
                    )
                for start_anchor in start_anchors:
                    distances = []
                    end_anchors = []
                    valid = True
                    for member in members:
                        value = graph_distance_from_room(
                            int(zone.get("floor", 0)),
                            room_x,
                            room_y,
                            member,
                            start_anchor,
                        )
                        if value is None:
                            valid = False
                            break
                        member_distance, end_anchor = value
                        if member_distance > float(max_distance_m):
                            valid = False
                            break
                        distances.append(float(member_distance))
                        end_anchors.append(str(end_anchor or ""))
                    if not valid:
                        continue
                    weighted_distance = sum(
                        distance
                        * max(0, int(member.get("_required_ports", 0) or 0))
                        for member, distance in zip(members, distances)
                    )
                    score = (max(distances, default=0.0), weighted_distance)
                    if best_result is None or score < best_result[0]:
                        best_result = (
                            score,
                            float(room_x),
                            float(room_y),
                            start_anchor,
                            distances,
                            end_anchors,
                        )
            if best_result is None:
                best_room_position_cache[cache_key] = None
                return None
            (
                _score,
                room_x,
                room_y,
                start_anchor,
                distances,
                end_anchors,
            ) = best_result
            result = room_x, room_y, start_anchor, distances, end_anchors
            best_room_position_cache[cache_key] = result
            return result

        unassigned = []
        unassigned_details = []

        pools = {}
        for zone in zones:
            for kind, allow_key in (
                ("comms_room", "allow_comms_room"),
                (
                    "distributed_equipment_room",
                    "allow_distributed_equipment_room",
                ),
            ):
                if not bool(zone.get(allow_key, False)):
                    continue
                override_key = (zone["_suggestion_key"], kind)
                limit = max(
                    0,
                    int(
                        room_limit_overrides.get(
                            override_key,
                            self._zone_room_limit(zone, kind),
                        )
                        or 0
                    ),
                )
                existing = self._existing_room_count_for_zone(zone, kind)
                max_new = None if limit <= 0 else max(0, limit - existing)
                pools[(zone["_suggestion_key"], kind)] = {
                    "zone": zone,
                    "kind": kind,
                    "bins": [],
                    "limit": limit,
                    "existing": existing,
                    "max_new": max_new,
                }

        if "existing_room_context" not in context:
            all_required_ports = {}
            for data_point in self.store.data.get("data_points", []):
                point_name = str(data_point.get("name", "") or "").strip()
                if not point_name:
                    continue
                if hasattr(self.store, "data_point_required_port_count"):
                    point_ports = self.store.data_point_required_port_count(data_point)
                else:
                    point_ports = max(0, int(data_point.get("qty", 1) or 0))
                all_required_ports[point_name] = max(0, int(point_ports or 0))

            existing_rooms = [
                room
                for room in self.store.data.get("locations", [])
                if self._equipment_room_location_kind(room)
                and str(room.get("name", "") or "").strip()
            ]
            existing_room_names = {
                str(room.get("name", "") or "").strip()
                for room in existing_rooms
            }
            existing_usage = self._autoroute_existing_room_port_usage(
                existing_room_names, all_required_ports
            )
            existing_capacities = self._autoroute_room_capacity_limits(
                existing_room_names
            )
            context["existing_room_context"] = (
                existing_rooms,
                existing_usage,
                existing_capacities,
            )
        (
            existing_rooms,
            existing_usage,
            existing_capacities,
        ) = context["existing_room_context"]
        floors_with_comms = set()
        zones_by_id = {
            str(zone.get("id", "") or "").strip(): zone
            for zone in zones
            if str(zone.get("id", "") or "").strip()
        }
        for room in existing_rooms:
            room_name = str(room.get("name", "") or "").strip()
            kind = self._equipment_room_location_kind(room)
            floor = int(room.get("floor", 0) or 0)
            room_x = float(room.get("x", 0.0) or 0.0)
            room_y = float(room.get("y", 0.0) or 0.0)
            assigned_zone_id = str(
                room.get("placement_zone_id", "") or ""
            ).strip()
            candidate_zones = []
            explicit_zone = zones_by_id.get(assigned_zone_id)
            if (
                explicit_zone is not None
                and int(explicit_zone.get("floor", 0)) == floor
                and self._zone_allows_location_kind(explicit_zone, kind)
            ):
                candidate_zones.append(explicit_zone)
            candidate_zones.extend(
                zone
                for zone in zones
                if zone is not explicit_zone
                and int(zone.get("floor", 0)) == floor
                and self._zone_allows_location_kind(zone, kind)
                and self._point_inside_placement_zone(room_x, room_y, zone)
            )
            if not candidate_zones:
                zone = {
                    "id": f"existing-room:{room_name}",
                    "_suggestion_key": f"existing-room:{room_name}",
                    "name": f"Existing {room_name}",
                    "floor": floor,
                    "min_x": room_x,
                    "max_x": room_x,
                    "min_y": room_y,
                    "max_y": room_y,
                    "allow_comms_room": kind == "comms_room",
                    "allow_distributed_equipment_room": (
                        kind == "distributed_equipment_room"
                    ),
                }
                pools[(zone["_suggestion_key"], kind)] = {
                    "zone": zone,
                    "kind": kind,
                    "bins": [],
                    "limit": 0,
                    "existing": 1,
                    "max_new": 0,
                }
                candidate_zones.append(zone)
            zone = min(
                candidate_zones,
                key=lambda item: (
                    0 if item is explicit_zone else 1,
                    (
                        float(item.get("max_x", 0.0))
                        - float(item.get("min_x", 0.0))
                    )
                    * (
                        float(item.get("max_y", 0.0))
                        - float(item.get("min_y", 0.0))
                    ),
                ),
            )
            pool = pools.get((zone["_suggestion_key"], kind))
            if pool is None:
                continue
            start_anchor = (
                room_name
                if room_name in routing_graph and routing_graph.get(room_name)
                else ""
            )
            if not start_anchor:
                anchors = candidate_start_anchors(floor, room_x, room_y)
                start_anchor = str(anchors[0] if anchors else "")
            pool["bins"].append(
                {
                    "ports": int(existing_usage.get(room_name, 0) or 0),
                    "existing_ports": int(
                        existing_usage.get(room_name, 0) or 0
                    ),
                    "port_limit": existing_capacities.get(room_name),
                    "points": [],
                    "x": room_x,
                    "y": room_y,
                    "anchor_name": start_anchor,
                    "data_point_anchor_names": {},
                    "max_route_distance_m": 0.0,
                    "existing": True,
                    "existing_name": room_name,
                    "fixed_position": True,
                }
            )
            if kind == "comms_room":
                floors_with_comms.add(floor)

        for point in sorted(
            data_points,
            key=lambda item: (
                -int(item.get("_required_ports", 0) or 0),
                str(item.get("name", "")),
            ),
        ):
            qty = max(0, int(point.get("_required_ports", 0) or 0))
            candidates = []
            matching_floor_pool = False
            supported_by_room_capacity = False
            zone_limit_blocked = False
            route_or_distance_blocked = False
            reachable_limit_blocks = {}
            nearest_capacity_block = None
            for pool in pools.values():
                zone = pool["zone"]
                kind = pool["kind"]
                if same_floor_only and int(point.get("floor", 0)) != int(
                    zone.get("floor", 0)
                ):
                    continue
                matching_floor_pool = True
                if qty > port_limits[kind]:
                    continue
                supported_by_room_capacity = True

                target_index = None
                best_remaining = None
                distance = None
                route_span = None
                selected_position = None
                selected_anchor = None
                selected_distances = None
                selected_end_anchors = None
                selected_incremental_cable = None
                best_existing_key = None
                for index, room_bin in enumerate(pool["bins"]):
                    room_port_limit = room_bin.get(
                        "port_limit", port_limits[kind]
                    )
                    remaining = (
                        None
                        if room_port_limit is None
                        else int(room_port_limit) - int(room_bin["ports"]) - qty
                    )
                    if remaining is not None and remaining < 0:
                        direct_route = graph_distance_from_room(
                            int(zone.get("floor", 0)),
                            float(room_bin["x"]),
                            float(room_bin["y"]),
                            point,
                            str(room_bin.get("anchor_name", "") or ""),
                        )
                        if (
                            direct_route is not None
                            and float(direct_route[0]) <= float(max_distance_m)
                        ):
                            used_ports = int(room_bin["ports"])
                            port_limit = int(room_port_limit)
                            capacity_block = {
                                "distance": float(direct_route[0]),
                                "zone_name": (
                                    str(zone.get("name", "")).strip()
                                    or str(zone.get("_suggestion_key", "")).strip()
                                ),
                                "kind": kind,
                                "room_bin": room_bin,
                                "used_ports": used_ports,
                                "port_limit": port_limit,
                                "free_ports": max(0, port_limit - used_ports),
                            }
                            if (
                                nearest_capacity_block is None
                                or capacity_block["distance"]
                                < nearest_capacity_block["distance"]
                            ):
                                nearest_capacity_block = capacity_block
                        continue
                    prospective_points = list(room_bin["points"]) + [point]
                    position_result = best_room_position(
                        zone,
                        prospective_points,
                        preferred_positions=[
                            (float(room_bin["x"]), float(room_bin["y"]))
                        ],
                        fixed_position=bool(
                            room_bin.get("fixed_position", False)
                        ),
                        preferred_start_anchors=(
                            [str(room_bin.get("anchor_name", "") or "")]
                            if room_bin.get("anchor_name")
                            else []
                        ),
                    )
                    if position_result is None:
                        route_or_distance_blocked = True
                        continue
                    (
                        room_x,
                        room_y,
                        start_anchor,
                        route_distances,
                        end_anchors,
                    ) = position_result
                    point_distance = float(route_distances[-1])
                    candidate_span = max(route_distances, default=0.0)
                    current_cable = sum(
                        float(member.get("_zone_cable_length_m", 0.0) or 0.0)
                        * max(0, int(member.get("_required_ports", 0) or 0))
                        for member in room_bin["points"]
                    )
                    prospective_cable = sum(
                        float(member_distance)
                        * max(0, int(member.get("_required_ports", 0) or 0))
                        for member, member_distance in zip(
                            prospective_points, route_distances
                        )
                    )
                    incremental_cable = prospective_cable - current_cable
                    existing_key = (
                        float(incremental_cable),
                        float(candidate_span),
                        int(remaining) if remaining is not None else 10**12,
                    )
                    if best_existing_key is None or existing_key < best_existing_key:
                        best_existing_key = existing_key
                        target_index = index
                        best_remaining = remaining
                        distance = float(point_distance)
                        route_span = candidate_span
                        selected_incremental_cable = float(incremental_cable)
                        selected_position = (room_x, room_y)
                        selected_anchor = start_anchor
                        selected_distances = route_distances
                        selected_end_anchors = end_anchors
                creates_room = target_index is None
                if creates_room:
                    position_result = best_room_position(zone, [point])
                    if position_result is None:
                        route_or_distance_blocked = True
                        continue
                    proposed_count = sum(
                        1
                        for room_bin in pool["bins"]
                        if not room_bin.get("existing", False)
                    )
                    if (
                        pool["max_new"] is not None
                        and proposed_count >= int(pool["max_new"])
                    ):
                        zone_limit_blocked = True
                        zone_name = (
                            str(zone.get("name", "")).strip()
                            or str(zone.get("_suggestion_key", "")).strip()
                        )
                        reachable_limit_blocks[
                            (zone_name, kind)
                        ] = {
                            "zone_name": zone_name,
                            "kind": kind,
                            "room_count": int(pool["existing"])
                            + proposed_count,
                            "limit": int(pool["limit"]),
                        }
                        continue
                    (
                        room_x,
                        room_y,
                        start_anchor,
                        route_distances,
                        end_anchors,
                    ) = position_result
                    distance = float(route_distances[0])
                    selected_incremental_cable = float(distance) * qty
                    selected_position = (room_x, room_y)
                    selected_anchor = start_anchor
                    selected_distances = route_distances
                    selected_end_anchors = end_anchors
                    if distance > float(max_distance_m):
                        continue
                    route_span = float(distance)
                remaining_after = (
                    port_limits[kind] - qty
                    if creates_room
                    else (
                        int(best_remaining)
                        if best_remaining is not None
                        else 10**12
                    )
                )
                preferred_kind = {
                    "comms_utilisation": "comms_room",
                    "comms_limit_then_der": "comms_room",
                    "der_utilisation": "distributed_equipment_room",
                }.get(design_strategy)
                type_priority = 0 if kind == preferred_kind else 1
                if design_strategy == "comms_limit_then_der":
                    point_floor = int(point.get("floor", 0) or 0)
                    needs_floor_comms = point_floor not in floors_with_comms
                    if needs_floor_comms:
                        type_priority = (
                            0
                            if kind == "comms_room"
                            and int(zone.get("floor", 0) or 0) == point_floor
                            else 1
                        )
                    else:
                        type_priority = (
                            0
                            if kind == "distributed_equipment_room"
                            else 1
                        )
                    # Establish one local comms room per served floor, counting
                    # rooms already placed there. Then reuse DER capacity before
                    # proposing further rooms.
                    score = (
                        type_priority,
                        1 if creates_room else 0,
                        float(selected_incremental_cable or 0.0),
                        route_span,
                        remaining_after,
                    )
                elif preferred_kind is not None:
                    # Utilisation designs first fill the selected room type,
                    # then favour its fullest reachable room before opening a
                    # new one. Route length remains the final tie-breaker.
                    score = (
                        type_priority,
                        float(selected_incremental_cable or 0.0),
                        route_span,
                        1 if creates_room else 0,
                        remaining_after,
                    )
                else:
                    # The shortest-route design is deliberately distance-led.
                    # Room reuse and spare capacity only break equal routes.
                    score = (
                        float(selected_incremental_cable or 0.0),
                        route_span,
                        1 if creates_room else 0,
                        remaining_after,
                    )
                candidates.append(
                    (
                        score,
                        pool,
                        target_index,
                        distance,
                        selected_position,
                        selected_anchor,
                        selected_distances,
                        selected_end_anchors,
                    )
                )

            if not candidates:
                point_name = str(point.get("name", "")).strip()
                unassigned.append(point_name)
                reason_parts = []
                reason_codes = []
                if not matching_floor_pool:
                    reason_parts.append(
                        "no permitted comms-room or DER placement zone is available on this floor"
                    )
                    reason_codes.append("no_same_floor_zone")
                elif not supported_by_room_capacity:
                    reason_parts.append(
                        f"the {qty}-port demand exceeds the capacity of a single permitted room "
                        f"(comms room {port_limits['comms_room']} ports; "
                        f"DER {port_limits['distributed_equipment_room']} ports)"
                    )
                    reason_codes.append("room_port_capacity")
                else:
                    if reachable_limit_blocks:
                        blocked_zone_text = ", ".join(
                            f"{row['zone_name']} "
                            f"({'DER' if row['kind'] == 'distributed_equipment_room' else 'comms room'} "
                            f"{row['room_count']}/{row['limit']})"
                            for row in sorted(
                                reachable_limit_blocks.values(),
                                key=lambda item: (
                                    str(item["zone_name"]),
                                    str(item["kind"]),
                                ),
                            )
                        )
                        reason_parts.append(
                            f"the placement zones reachable within the "
                            f"{float(max_distance_m):.2f} m cable limit have reached "
                            f"their configured room limits: {blocked_zone_text}"
                            + (
                                "; other same-floor permitted zones are outside the routing limit"
                                if route_or_distance_blocked
                                else ""
                            )
                        )
                        reason_codes.append("reachable_zone_room_limit")
                    elif zone_limit_blocked:
                        reason_parts.append(
                            "the permitted zones have reached their configured room limits"
                        )
                        reason_codes.append("zone_room_limit")
                    elif route_or_distance_blocked:
                        reason_parts.append(
                            f"no permitted zone has a usable routing path within the "
                            f"{float(max_distance_m):.2f} m cable limit"
                        )
                        reason_codes.append("route_or_cable_limit")
                    if nearest_capacity_block is not None:
                        reason_codes.append("nearest_room_capacity")
                    if not reason_parts:
                        reason_parts.append(
                            "no permitted room has enough remaining port capacity and no additional room can be placed"
                        )
                        reason_codes.append("remaining_room_capacity")
                unassigned_details.append(
                    {
                        "name": point_name,
                        "floor": int(point.get("floor", 0)),
                        "ports": qty,
                        "reason_code": "+".join(reason_codes),
                        "_reason_parts": reason_parts,
                        "_nearest_capacity_block": nearest_capacity_block,
                    }
                )
                continue
            (
                _score,
                selected_pool,
                target_index,
                distance,
                selected_position,
                selected_anchor,
                selected_distances,
                selected_end_anchors,
            ) = min(
                candidates, key=lambda item: item[0]
            )
            assigned = dict(point)
            assigned["_zone_cable_length_m"] = float(distance)
            if target_index is None:
                room_x, room_y = selected_position
                selected_pool["bins"].append(
                    {
                        "ports": 0,
                        "existing_ports": 0,
                        "port_limit": int(
                            port_limits[selected_pool["kind"]]
                        ),
                        "points": [],
                        "x": float(room_x),
                        "y": float(room_y),
                        "anchor_name": str(selected_anchor or ""),
                        "data_point_anchor_names": {},
                        "max_route_distance_m": 0.0,
                        "existing": False,
                    }
                )
                target_index = len(selected_pool["bins"]) - 1
            target_bin = selected_pool["bins"][target_index]
            room_x, room_y = selected_position
            target_bin["x"] = float(room_x)
            target_bin["y"] = float(room_y)
            target_bin["anchor_name"] = str(selected_anchor or "")
            target_bin["points"].append(assigned)
            target_bin["ports"] += qty
            for member, member_distance in zip(
                target_bin["points"], selected_distances
            ):
                member["_zone_cable_length_m"] = float(member_distance)
            target_bin["data_point_anchor_names"] = {
                str(member.get("name", "")).strip(): str(end_anchor or "")
                for member, end_anchor in zip(
                    target_bin["points"], selected_end_anchors
                )
            }
            target_bin["max_route_distance_m"] = max(
                selected_distances, default=0.0
            )
            if selected_pool["kind"] == "comms_room":
                floors_with_comms.add(
                    int(selected_pool["zone"].get("floor", 0) or 0)
                )

        for detail in unassigned_details:
            reason_parts = list(detail.pop("_reason_parts", []) or [])
            capacity_block = detail.pop("_nearest_capacity_block", None)
            if capacity_block is not None:
                room_label = (
                    "DER"
                    if capacity_block["kind"]
                    == "distributed_equipment_room"
                    else "comms room"
                )
                room_bin = capacity_block["room_bin"]
                used_ports = int(room_bin["ports"])
                port_limit = int(capacity_block["port_limit"])
                free_ports = max(0, port_limit - used_ports)
                fill_percentage = (
                    100.0 * used_ports / port_limit
                    if port_limit > 0
                    else 0.0
                )
                reason_parts.append(
                    f"the nearest reachable suggested {room_label} in "
                    f"{capacity_block['zone_name']} is "
                    f"{fill_percentage:.1f}% filled "
                    f"({used_ports}/{port_limit} ports; {free_ports} free), "
                    f"which cannot accept this point's "
                    f"{int(detail['ports'])}-port demand"
                )
            detail["reason"] = "; and ".join(reason_parts)

        suggestions = []

        for pool in pools.values():
            zone = pool["zone"]
            kind = pool["kind"]
            port_limit = port_limits[kind]
            for room_index, room_bin in enumerate(pool["bins"]):
                if room_bin.get("existing", False) and not room_bin["points"]:
                    continue
                x = float(room_bin["x"])
                y = float(room_bin["y"])
                floor = int(zone.get("floor", 0))
                assigned_ports = sum(
                    max(0, int(point.get("_required_ports", 0) or 0))
                    for point in room_bin["points"]
                )
                effective_port_limit = room_bin.get("port_limit", port_limit)
                suggestions.append(
                    {
                        "zone_id": zone["_suggestion_key"],
                        "zone_name": str(zone.get("name", "")).strip()
                        or zone["_suggestion_key"],
                        "kind": kind,
                        "floor": floor,
                        "x": x,
                        "y": y,
                        "ports": int(assigned_ports),
                        "existing_ports": int(
                            room_bin.get("existing_ports", 0) or 0
                        ),
                        "port_limit": (
                            0
                            if effective_port_limit is None
                            else int(effective_port_limit)
                        ),
                        "existing_room_name": str(
                            room_bin.get("existing_name", "") or ""
                        ).strip(),
                        "total_cable_length_m": sum(
                            float(
                                point.get("_zone_cable_length_m", 0.0) or 0.0
                            )
                            * max(
                                0,
                                int(point.get("_required_ports", 0) or 0),
                            )
                            for point in room_bin["points"]
                        ),
                        "data_point_names": [
                            str(point.get("name", "")).strip()
                            for point in room_bin["points"]
                        ],
                        "anchor_name": str(
                            room_bin.get("anchor_name", "") or ""
                        ).strip(),
                        "data_point_anchor_names": dict(
                            room_bin.get("data_point_anchor_names", {}) or {}
                        ),
                    }
                )

        unassigned_names = sorted(set(unassigned))
        unassigned_name_set = set(unassigned_names)
        considered_port_count = sum(
            max(0, int(point.get("_required_ports", 0) or 0))
            for point in data_points
        )
        unassigned_port_count = sum(
            max(0, int(point.get("_required_ports", 0) or 0))
            for point in data_points
            if str(point.get("name", "")).strip() in unassigned_name_set
        )
        assigned_port_count = max(
            0, considered_port_count - unassigned_port_count
        )
        grouped_shortfalls = {}
        for detail in unassigned_details:
            key = (int(detail["floor"]), str(detail["reason"]))
            record = grouped_shortfalls.setdefault(
                key,
                {
                    "floor": int(detail["floor"]),
                    "reason": str(detail["reason"]),
                    "point_count": 0,
                    "port_count": 0,
                    "examples": [],
                },
            )
            record["point_count"] += 1
            record["port_count"] += int(detail["ports"])
            if len(record["examples"]) < 5:
                record["examples"].append(str(detail["name"]))
        shortfall_reasons = sorted(
            grouped_shortfalls.values(),
            key=lambda row: (int(row["floor"]), str(row["reason"])),
        )
        floor_satisfaction = []
        for floor in sorted(
            {int(point.get("floor", 0)) for point in data_points}
        ):
            floor_points = [
                point
                for point in data_points
                if int(point.get("floor", 0)) == floor
            ]
            floor_unassigned = [
                point
                for point in floor_points
                if str(point.get("name", "")).strip() in unassigned_name_set
            ]
            floor_considered_ports = sum(
                max(0, int(point.get("_required_ports", 0) or 0))
                for point in floor_points
            )
            floor_unassigned_ports = sum(
                max(0, int(point.get("_required_ports", 0) or 0))
                for point in floor_unassigned
            )
            floor_satisfaction.append(
                {
                    "floor": floor,
                    "considered_points": len(floor_points),
                    "considered_ports": floor_considered_ports,
                    "assigned_points": len(floor_points) - len(floor_unassigned),
                    "assigned_ports": max(
                        0, floor_considered_ports - floor_unassigned_ports
                    ),
                    "unassigned_points": len(floor_unassigned),
                    "unassigned_ports": floor_unassigned_ports,
                    "satisfied": floor_unassigned_ports == 0,
                }
            )

        result = {
            "suggestions": suggestions,
            "unassigned": unassigned_names,
            "unassigned_details": unassigned_details,
            "considered": len(data_points),
            "considered_port_count": considered_port_count,
            "assigned_point_count": max(
                0, len(data_points) - len(unassigned_names)
            ),
            "assigned_port_count": assigned_port_count,
            "unassigned_port_count": unassigned_port_count,
            "total_cable_length_m": sum(
                float(item.get("total_cable_length_m", 0.0) or 0.0)
                for item in suggestions
            ),
            "same_floor_only": bool(same_floor_only),
            "current_floor_only": bool(current_floor_only),
            "scope_floor": scope_floor,
            "floor_satisfaction": floor_satisfaction,
            "shortfall_reasons": shortfall_reasons,
            "strategy": design_strategy,
            "zone_usage": [
                {
                    "zone_id": pool["zone"]["_suggestion_key"],
                    "zone_name": str(pool["zone"].get("name", "")).strip()
                    or pool["zone"]["_suggestion_key"],
                    "kind": pool["kind"],
                    "existing": int(pool["existing"]),
                    "proposed": sum(
                        1
                        for room_bin in pool["bins"]
                        if not room_bin.get("existing", False)
                    ),
                    "limit": int(pool["limit"]),
                }
                for pool in pools.values()
            ],
        }
        plan_results[plan_key] = deepcopy(result)
        return result

    def _verified_zone_limit_changes_for_plan(
        self,
        base_plan,
        max_distance_m,
        room_port_limits,
        same_floor_only,
        current_floor_only,
        design_strategy,
        progress_callback=None,
        enforce_comms_room_limits=False,
        enforce_der_limits=False,
        planning_cache=None,
    ):
        if int(base_plan.get("unassigned_port_count", 0) or 0) <= 0:
            return []

        unassigned_floors = {
            int(detail.get("floor", 0))
            for detail in base_plan.get("unassigned_details", []) or []
        }
        candidates = []
        for index, source in enumerate(self._equipment_room_placement_zones()):
            zone = dict(source)
            zone_key = str(zone.get("id", "")).strip() or f"zone-{index + 1}"
            floor = int(zone.get("floor", 0))
            if unassigned_floors and floor not in unassigned_floors:
                continue
            for kind, allow_key in (
                ("comms_room", "allow_comms_room"),
                (
                    "distributed_equipment_room",
                    "allow_distributed_equipment_room",
                ),
            ):
                if not bool(zone.get(allow_key, False)):
                    continue
                if (
                    kind == "comms_room"
                    and bool(enforce_comms_room_limits)
                ) or (
                    kind == "distributed_equipment_room"
                    and bool(enforce_der_limits)
                ):
                    continue
                limit = self._zone_room_limit(zone, kind)
                if limit <= 0:
                    continue
                candidates.append(
                    {
                        "key": (zone_key, kind),
                        "zone_name": str(zone.get("name", "")).strip()
                        or zone_key,
                        "kind": kind,
                        "original_limit": int(limit),
                    }
                )

        if not candidates:
            return []

        # Prefer high-capacity comms-room changes first. If they cannot produce
        # a verified complete plan, include DER-zone limit changes as well.
        comms_candidates = [
            row for row in candidates if row["kind"] == "comms_room"
        ]
        der_candidates = [
            row
            for row in candidates
            if row["kind"] == "distributed_equipment_room"
        ]
        candidate_sets = []
        if design_strategy == "comms_limit_then_der" and der_candidates:
            candidate_sets.append(der_candidates)
        elif comms_candidates:
            candidate_sets.append(comms_candidates)
        if not candidate_sets or len(candidate_sets[0]) != len(candidates):
            candidate_sets.append(candidates)

        overrides = {}
        working_plan = base_plan
        for active_candidates in candidate_sets:
            for _step in range(12):
                trials = []
                for candidate in active_candidates:
                    trial_overrides = dict(overrides)
                    current_limit = int(
                        trial_overrides.get(
                            candidate["key"], candidate["original_limit"]
                        )
                    )
                    trial_overrides[candidate["key"]] = current_limit + 1
                    if progress_callback is not None and not progress_callback(
                        f"Testing {candidate['zone_name']}: "
                        f"{current_limit + 1} permitted "
                        f"{'DERs' if candidate['kind'] == 'distributed_equipment_room' else 'comms rooms'}"
                    ):
                        raise InterruptedError("Zone suggestion cancelled")
                    trial_plan = self._build_zone_room_suggestion_plan(
                        max_distance_m=max_distance_m,
                        room_port_limits=room_port_limits,
                        same_floor_only=same_floor_only,
                        current_floor_only=current_floor_only,
                        design_strategy=design_strategy,
                        room_limit_overrides=trial_overrides,
                        planning_cache=planning_cache,
                    )
                    trials.append(
                        (
                            int(trial_plan.get("unassigned_port_count", 0) or 0),
                            len(trial_plan.get("unassigned", []) or []),
                            0 if candidate["kind"] == "comms_room" else 1,
                            str(candidate["zone_name"]),
                            candidate,
                            trial_overrides,
                            trial_plan,
                        )
                    )
                if not trials:
                    break
                (
                    _unassigned_ports,
                    _unassigned_points,
                    _kind_priority,
                    _zone_name,
                    _candidate,
                    overrides,
                    working_plan,
                ) = min(trials, key=lambda row: row[:4])
                if int(working_plan.get("unassigned_port_count", 0) or 0) <= 0:
                    changes = []
                    candidate_by_key = {
                        row["key"]: row for row in candidates
                    }
                    for key, new_limit in sorted(
                        overrides.items(),
                        key=lambda item: (
                            str(candidate_by_key[item[0]]["zone_name"]),
                            str(item[0][1]),
                        ),
                    ):
                        row = candidate_by_key[key]
                        original_limit = int(row["original_limit"])
                        if int(new_limit) <= original_limit:
                            continue
                        changes.append(
                            {
                                "zone_id": key[0],
                                "zone_name": row["zone_name"],
                                "kind": row["kind"],
                                "current_limit": original_limit,
                                "suggested_limit": int(new_limit),
                            }
                        )
                    return changes
        return []

    def _export_zone_design_options_pdf(
        self,
        design_options,
        strategy_names,
        planning_options,
        room_port_limits,
    ):
        settings_dialog = ZoneDesignOptionsPdfOptionsDialog(self)
        if settings_dialog.exec() != QDialog.Accepted:
            return False
        pdf_options = settings_dialog.export_options()
        source_path = (
            getattr(self.store, "storage_path", "") or self.current_json_path or ""
        )
        try:
            from tempfile import TemporaryDirectory

            from zone_design_options_pdf import export_zone_design_options_pdf
            from zone_report_studio import ZoneDesignReportStudioDialog
        except ImportError as exc:
            QMessageBox.critical(
                self,
                "Zone Design Report Studio failed",
                f"The report studio requires the PDF components.\n\n{exc}",
            )
            return False

        saved_pdf_settings = self.store.data.get(
            "zone_design_options_pdf_settings", {}
        ) or {}
        initial_studio_settings = dict(
            saved_pdf_settings.get("studio_settings", {}) or {}
        )
        initial_studio_settings.setdefault(
            "snippet_templates",
            deepcopy(self.store.data.get("network_pdf_snippet_templates", []) or []),
        )
        initial_studio_settings.setdefault(
            "page_templates",
            deepcopy(self.store.data.get("pdf_report_page_templates", []) or []),
        )
        preview_counter = [0]
        preview_directory = TemporaryDirectory(
            prefix="cable_route_report_studio_", ignore_cleanup_errors=True
        )

        def build_studio_preview(studio_settings):
            preview_counter[0] += 1
            preview_path = str(
                Path(preview_directory.name)
                / f"zone_design_preview_{preview_counter[0]}.pdf"
            )
            manifest = []
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                export_zone_design_options_pdf(
                    self.store.data,
                    design_options,
                    preview_path,
                    strategy_names=strategy_names,
                    planning_options=planning_options,
                    room_port_limits=room_port_limits,
                    source_path=source_path,
                    paper_size=pdf_options["paper_size"],
                    scale=pdf_options["scale"],
                    floor_scope=pdf_options["floor_scope"],
                    revision_number=self.latest_project_revision_number(),
                    studio_settings=studio_settings,
                    layout_manifest=manifest,
                    preview_background=True,
                )
            finally:
                QApplication.restoreOverrideCursor()
            return preview_path, manifest

        studio = None
        try:
            studio = ZoneDesignReportStudioDialog(
                build_studio_preview,
                initial_settings=initial_studio_settings,
                parent=self,
                network_data=self.store.data,
            )
            if studio.run_as_window() != QDialog.Accepted:
                return False
            studio_settings = studio.export_settings()
            self.store.data["network_pdf_snippet_templates"] = deepcopy(
                studio_settings.get("snippet_templates", []) or []
            )
            self.store.data["pdf_report_page_templates"] = deepcopy(
                studio_settings.get("page_templates", []) or []
            )
        finally:
            if studio is not None:
                studio.release_preview()
            preview_directory.cleanup()

        base_path = Path(source_path) if source_path else Path("cable_routes.crsdb")
        initial = str(
            base_path.with_suffix("").with_name(
                base_path.stem + "_zone_design_options.pdf"
            )
        )
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Zone Design Options PDF",
            initial,
            "PDF files (*.pdf)",
        )
        if not path:
            return False
        if not path.lower().endswith(".pdf"):
            path += ".pdf"

        from uuid import uuid4

        destination_path = Path(path)
        temporary_output_path = destination_path.with_name(
            f".{destination_path.stem}_zone_export_{uuid4().hex}.pdf"
        )
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            export_zone_design_options_pdf(
                self.store.data,
                design_options,
                str(temporary_output_path),
                strategy_names=strategy_names,
                planning_options=planning_options,
                room_port_limits=room_port_limits,
                source_path=source_path,
                paper_size=pdf_options["paper_size"],
                scale=pdf_options["scale"],
                floor_scope=pdf_options["floor_scope"],
                revision_number=self.latest_project_revision_number(),
                studio_settings=studio_settings,
            )
            os.replace(str(temporary_output_path), str(destination_path))
            output_path = str(destination_path)
        except ImportError as exc:
            QMessageBox.critical(
                self,
                "Zone Design Options PDF failed",
                f"PDF export requires reportlab.\n\n{exc}",
            )
            return False
        except Exception as exc:
            QMessageBox.critical(self, "Zone Design Options PDF failed", str(exc))
            return False
        finally:
            QApplication.restoreOverrideCursor()
            if temporary_output_path.exists():
                try:
                    temporary_output_path.unlink()
                except OSError:
                    pass

        self.store.data["zone_design_options_pdf_settings"] = {
            **dict(pdf_options),
            "studio_settings": dict(studio_settings),
        }
        self.set_status(f"Exported zone design options PDF: {Path(output_path).name}")
        QMessageBox.information(
            self,
            "Zone Design Options PDF complete",
            f"Exported {len(design_options)} unapplied design option(s) to:\n\n"
            f"{output_path}",
        )
        return True

    def suggest_equipment_rooms_from_zones(self):
        if not self._equipment_room_placement_zones():
            QMessageBox.warning(
                self,
                "Zone-based Equipment Room Suggestion",
                "No equipment-room placement zones have been supplied.",
            )
            return
        if not self.store.data.get("data_points"):
            QMessageBox.warning(
                self,
                "Zone-based Equipment Room Suggestion",
                "No data ports are available to assess.",
            )
            return

        switch_capacity = (
            self.store.access_switch_capacity_profile()
            if hasattr(self.store, "access_switch_capacity_profile")
            else {
                "name": "Default 48-port access switch",
                "ports": 48,
                "rack_size_u": 42,
                "switches_per_full_cabinet": 42,
            }
        )
        dialog = SuggestRoomsFromZonesDialog(
            self,
            current_floor=self.floor_spin.value(),
            switch_capacity=switch_capacity,
        )
        if dialog.exec() != QDialog.Accepted or not dialog.result:
            return
        options = dialog.result
        room_port_limits = {
            "comms_room": (
                int(options["access_ports_per_switch"])
                * int(options["comms_switches_per_cabinet"])
                * int(options["comms_cabinet_count"])
            ),
            "distributed_equipment_room": (
                int(options["access_ports_per_switch"])
                * int(options["der_max_switches"])
            ),
        }
        strategy_names = {
            "shortest_routes": "Shortest routes",
            "comms_utilisation": "Maximise comms-room utilisation",
            "comms_limit_then_der": "Comms-room limit, then DER overflow",
            "der_utilisation": "Maximise DER utilisation",
        }
        design_options = []
        progress = QProgressDialog(
            "Preparing zone design options...",
            "Cancel",
            0,
            0,
            self,
        )
        progress.setWindowTitle("Generating Zone-based Design Options")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.setAutoClose(False)
        progress.setAutoReset(False)
        progress.show()

        def update_progress(message):
            progress.setLabelText(str(message))
            QApplication.processEvents()
            return not progress.wasCanceled()

        try:
            planning_cache = {}
            for option_index, strategy in enumerate(strategy_names, start=1):
                strategy_label = strategy_names[strategy]
                if not update_progress(
                    f"Option {option_index} of {len(strategy_names)}: "
                    f"building {strategy_label.lower()}..."
                ):
                    raise InterruptedError("Zone suggestion cancelled")
                base_plan = self._build_zone_room_suggestion_plan(
                    max_distance_m=options["max_distance_m"],
                    room_port_limits=room_port_limits,
                    same_floor_only=options["same_floor_only"],
                    current_floor_only=bool(
                        options.get("ignore_other_floors", False)
                    ),
                    design_strategy=strategy,
                    planning_cache=planning_cache,
                )
                if not update_progress(
                    f"Option {option_index} of {len(strategy_names)}: "
                    f"checking {strategy_label.lower()} for full coverage..."
                ):
                    raise InterruptedError("Zone suggestion cancelled")
                recommended_changes = self._verified_zone_limit_changes_for_plan(
                    base_plan,
                    max_distance_m=options["max_distance_m"],
                    room_port_limits=room_port_limits,
                    same_floor_only=options["same_floor_only"],
                    current_floor_only=bool(
                        options.get("ignore_other_floors", False)
                    ),
                    design_strategy=strategy,
                    enforce_comms_room_limits=bool(
                        options.get("enforce_comms_room_limits", True)
                    ),
                    enforce_der_limits=bool(
                        options.get("enforce_der_limits", True)
                    ),
                    planning_cache=planning_cache,
                    progress_callback=lambda detail, index=option_index, label=strategy_label: update_progress(
                        f"Option {index} of {len(strategy_names)}: "
                        f"{label} - {detail}"
                    ),
                )
                if recommended_changes:
                    room_limit_overrides = {
                        (change["zone_id"], change["kind"]): int(
                            change["suggested_limit"]
                        )
                        for change in recommended_changes
                    }
                    if not update_progress(
                        f"Option {option_index} of {len(strategy_names)}: "
                        f"finalising satisfactory {strategy_label.lower()} plan..."
                    ):
                        raise InterruptedError("Zone suggestion cancelled")
                    candidate_plan = self._build_zone_room_suggestion_plan(
                        max_distance_m=options["max_distance_m"],
                        room_port_limits=room_port_limits,
                        same_floor_only=options["same_floor_only"],
                        current_floor_only=bool(
                            options.get("ignore_other_floors", False)
                        ),
                        design_strategy=strategy,
                        room_limit_overrides=room_limit_overrides,
                        planning_cache=planning_cache,
                    )
                else:
                    candidate_plan = base_plan
                candidate_plan["recommended_zone_changes"] = recommended_changes
                design_options.append(candidate_plan)
            update_progress("Design options ready. Opening comparison...")
        except InterruptedError:
            self.set_status("Zone-based design option generation cancelled")
            return
        finally:
            progress.close()

        if not design_options:
            design_options = [
                self._build_zone_room_suggestion_plan(
                    max_distance_m=options["max_distance_m"],
                    room_port_limits=room_port_limits,
                    same_floor_only=options["same_floor_only"],
                    current_floor_only=bool(options.get("ignore_other_floors", False)),
                )
            ]

        # These caches can be sizeable on multi-floor projects and are only
        # useful while the alternatives are being generated.
        planning_cache.clear()

        option_labels = []
        for index, candidate_plan in enumerate(design_options, start=1):
            candidate_suggestions = candidate_plan["suggestions"]
            candidate_comms = sum(
                1
                for item in candidate_suggestions
                if item["kind"] == "comms_room"
            )
            candidate_ders = len(candidate_suggestions) - candidate_comms
            assigned = int(
                candidate_plan.get(
                    "assigned_point_count",
                    sum(
                        len(item["data_point_names"])
                        for item in candidate_suggestions
                    ),
                )
            )
            assigned_ports = int(
                candidate_plan.get(
                    "assigned_port_count",
                    sum(int(item.get("ports", 0) or 0) for item in candidate_suggestions),
                )
            )
            unassigned_ports = int(
                candidate_plan.get("unassigned_port_count", 0) or 0
            )
            total_cable_length_m = float(
                candidate_plan.get("total_cable_length_m", 0.0) or 0.0
            )
            comms_ports = sum(
                int(item.get("ports", 0) or 0)
                for item in candidate_suggestions
                if item["kind"] == "comms_room"
            )
            der_ports = sum(
                int(item.get("ports", 0) or 0)
                for item in candidate_suggestions
                if item["kind"] == "distributed_equipment_room"
            )
            comms_capacity = candidate_comms * int(room_port_limits["comms_room"])
            der_capacity = candidate_ders * int(
                room_port_limits["distributed_equipment_room"]
            )
            comms_utilisation = (
                100.0 * comms_ports / comms_capacity if comms_capacity else 0.0
            )
            der_utilisation = (
                100.0 * der_ports / der_capacity if der_capacity else 0.0
            )
            floor_rows = list(candidate_plan.get("floor_satisfaction", []) or [])
            if candidate_plan.get("current_floor_only", False):
                scope_floor = int(candidate_plan.get("scope_floor", 0) or 0)
                scope_status = (
                    f"Floor {scope_floor} scope only (not whole model): "
                    f"{assigned_ports}/{int(candidate_plan.get('considered_port_count', 0) or 0)} "
                    "ports satisfied"
                )
            elif candidate_plan.get("same_floor_only", False):
                failed_floors = [
                    row for row in floor_rows if not bool(row.get("satisfied", False))
                ]
                if failed_floors:
                    failures = ", ".join(
                        f"F{int(row['floor'])} ({int(row['unassigned_ports'])} ports)"
                        for row in failed_floors
                    )
                    scope_status = f"Same-floor check: unsatisfied {failures}"
                else:
                    scope_status = (
                        "Same-floor check: all in-scope ports are satisfied on every floor"
                    )
            else:
                scope_status = (
                    f"Selected planning scope: {assigned_ports}/"
                    f"{int(candidate_plan.get('considered_port_count', 0) or 0)} ports satisfied"
                )
            zone_usage_lines = [
                f"{usage['zone_name']}: "
                f"{'DER' if usage['kind'] == 'distributed_equipment_room' else 'CR'} "
                f"{usage['proposed']}/{usage['limit'] or 'unlimited'}"
                for usage in candidate_plan.get("zone_usage", [])
                if usage["proposed"]
            ]
            option_lines = [
                f"Option {index} - "
                f"{strategy_names.get(candidate_plan['strategy'], candidate_plan['strategy'])}",
                f"  • Rooms: {candidate_comms} comms room(s), {candidate_ders} DER(s)",
                f"  • Assigned demand: {assigned} data point(s), {assigned_ports} port(s)",
                f"  • Unassigned demand: {len(candidate_plan['unassigned'])} data point(s), "
                f"{unassigned_ports} port(s)",
                f"  • Utilisation: comms rooms {comms_utilisation:.0f}%, "
                f"DERs {der_utilisation:.0f}%",
                f"  • Estimated cable: {total_cable_length_m:,.1f} m total "
                "across assigned ports",
                "  - Zone limits: comms rooms "
                + (
                    "enforced"
                    if options.get("enforce_comms_room_limits", True)
                    else "may be increased"
                )
                + "; DERs "
                + (
                    "enforced"
                    if options.get("enforce_der_limits", True)
                    else "may be increased"
                ),
                f"  • Floor coverage: {scope_status}",
            ]
            if unassigned_ports > 0:
                option_lines.append("  • Coverage shortfall reasons:")
                for reason_row in candidate_plan.get("shortfall_reasons", []) or []:
                    examples = ", ".join(reason_row.get("examples", []) or [])
                    option_lines.append(
                        f"      ◦ Floor {int(reason_row['floor'])}: "
                        f"{int(reason_row['point_count'])} point(s) / "
                        f"{int(reason_row['port_count'])} port(s) - "
                        f"{reason_row['reason']}"
                        + (f" (for example: {examples})" if examples else "")
                    )
                recommended_changes = list(
                    candidate_plan.get("recommended_zone_changes", []) or []
                )
                if recommended_changes:
                    option_lines.append(
                        "  • Verified zone alterations for 100% coverage:"
                    )
                    for change in recommended_changes:
                        room_label = (
                            "DERs"
                            if change["kind"]
                            == "distributed_equipment_room"
                            else "comms rooms"
                        )
                        option_lines.append(
                            f"      ◦ {change['zone_name']}: increase maximum "
                            f"{room_label} from {int(change['current_limit'])} "
                            f"to {int(change['suggested_limit'])}"
                        )
            recommended_changes = list(
                candidate_plan.get("recommended_zone_changes", []) or []
            )
            if recommended_changes and unassigned_ports <= 0:
                option_lines.append(
                    "  • Zone alterations included in this 100% coverage plan:"
                )
                for change in recommended_changes:
                    room_label = (
                        "DERs"
                        if change["kind"] == "distributed_equipment_room"
                        else "comms rooms"
                    )
                    option_lines.append(
                        f"      ◦ {change['zone_name']}: increase maximum "
                        f"{room_label} from {int(change['current_limit'])} "
                        f"to {int(change['suggested_limit'])}"
                    )
            if zone_usage_lines:
                option_lines.append("  • Zone usage:")
                option_lines.extend(
                    f"      ◦ {zone_text}" for zone_text in zone_usage_lines[:8]
                )
                if len(zone_usage_lines) > 8:
                    option_lines.append(
                        f"      ◦ ... and {len(zone_usage_lines) - 8} more zone(s)"
                    )
            option_labels.append("\n".join(option_lines))
        while True:
            selection_dialog = ZoneDesignOptionsSelectionDialog(
                option_labels, self
            )
            if selection_dialog.exec() != QDialog.Accepted:
                return
            if selection_dialog.result_action == "export":
                self._export_zone_design_options_pdf(
                    design_options,
                    strategy_names,
                    options,
                    room_port_limits,
                )
                continue
            plan = design_options[selection_dialog.selected_index()]
            break
        suggestions = plan["suggestions"]
        if not suggestions:
            shortfall_text = "\n".join(
                f"- Floor {int(row['floor'])}: {int(row['point_count'])} point(s) / "
                f"{int(row['port_count'])} port(s) - {row['reason']}"
                for row in plan.get("shortfall_reasons", []) or []
            )
            QMessageBox.warning(
                self,
                "No Equipment Rooms Suggested",
                "None of the unconnected data ports are within the permitted "
                "placement zones under the selected options.\n\n"
                f"Unconnected data-point locations: {len(plan['unassigned'])}\n"
                f"Unconnected data ports: {int(plan.get('unassigned_port_count', 0) or 0)}"
                + (f"\n\nReasons:\n{shortfall_text}" if shortfall_text else ""),
            )
            return

        proposed_suggestions = [
            item for item in suggestions if not item.get("existing_room_name")
        ]
        existing_assignment_rooms = {
            str(item.get("existing_room_name", "") or "").strip()
            for item in suggestions
            if str(item.get("existing_room_name", "") or "").strip()
        }
        comms_count = sum(
            1 for item in proposed_suggestions if item["kind"] == "comms_room"
        )
        der_count = len(proposed_suggestions) - comms_count
        assigned_count = sum(len(item["data_point_names"]) for item in suggestions)
        assigned_ports = sum(int(item["ports"]) for item in suggestions)
        confirmation = [
            f"Design option: {strategy_names.get(plan['strategy'], plan['strategy'])}",
            "Comms-room zone limits: "
            + (
                "enforced unchanged"
                if options.get("enforce_comms_room_limits", True)
                else "verified increases allowed"
            ),
            "DER zone limits: "
            + (
                "enforced unchanged"
                if options.get("enforce_der_limits", True)
                else "verified increases allowed"
            ),
            f"Suggested comms rooms: {comms_count}",
            f"Suggested DERs: {der_count}",
            f"Existing rooms receiving assignments: {len(existing_assignment_rooms)}",
            f"Assigned data-port locations: {assigned_count}",
            f"Assigned data ports: {assigned_ports}",
            f"Estimated cable across assigned ports: "
            f"{float(plan.get('total_cable_length_m', 0.0) or 0.0):,.1f} m",
            f"Comms-room capacity: {room_port_limits['comms_room']} ports "
            f"({options['comms_cabinet_count']} full-size cabinet(s))",
            f"DER capacity: {room_port_limits['distributed_equipment_room']} ports "
            f"({options['der_max_switches']} switches)",
            f"Currently unassigned: {len(plan['unassigned'])} data-point "
            f"location(s), {int(plan.get('unassigned_port_count', 0) or 0)} port(s)",
            (
                f"Planning scope: floor {int(plan.get('scope_floor', 0) or 0)} only; "
                "other floors are excluded from these counts and are not included "
                "in a whole-model total."
                if plan.get("current_floor_only", False)
                else "Planning scope: all selected floors; counts show unconnected planning demand, not all model ports."
            ),
            "",
            "Zone usage (existing + proposed / limit):",
        ]
        for usage in plan.get("zone_usage", []):
            if not usage["proposed"] and not usage["existing"]:
                continue
            kind_label = (
                "DER"
                if usage["kind"] == "distributed_equipment_room"
                else "CR"
            )
            limit_label = str(usage["limit"]) if usage["limit"] else "unlimited"
            confirmation.append(
                f"  {usage['zone_name']} - {kind_label}: "
                f"{usage['existing']} + {usage['proposed']} / {limit_label}"
            )
        if plan.get("same_floor_only", False):
            confirmation.extend(["", "Same-floor port satisfaction:"])
            for floor_row in plan.get("floor_satisfaction", []) or []:
                confirmation.append(
                    f"  Floor {int(floor_row['floor'])}: "
                    f"{int(floor_row['assigned_ports'])}/"
                    f"{int(floor_row['considered_ports'])} ports satisfied"
                    + (
                        " - all satisfied"
                        if floor_row.get("satisfied", False)
                        else f" - {int(floor_row['unassigned_ports'])} unsatisfied"
                    )
                )
        if int(plan.get("unassigned_port_count", 0) or 0) > 0:
            confirmation.extend(["", "Coverage shortfall reasons:"])
            for reason_row in plan.get("shortfall_reasons", []) or []:
                confirmation.append(
                    f"  Floor {int(reason_row['floor'])}: "
                    f"{int(reason_row['point_count'])} point(s) / "
                    f"{int(reason_row['port_count'])} port(s) - "
                    f"{reason_row['reason']}"
                )
        recommended_changes = list(
            plan.get("recommended_zone_changes", []) or []
        )
        if recommended_changes:
            confirmation.extend(
                ["", "Zone-limit alterations included in this plan:"]
            )
            for change in recommended_changes:
                room_label = (
                    "DERs"
                    if change["kind"] == "distributed_equipment_room"
                    else "comms rooms"
                )
                confirmation.append(
                    f"  {change['zone_name']}: maximum {room_label} "
                    f"{int(change['current_limit'])} -> "
                    f"{int(change['suggested_limit'])}"
                )
        confirmation.extend(
            [
            "",
            (
                "Data ports will be kept on the same floor."
                if options["same_floor_only"]
                else "Cross-floor room assignments are permitted."
            ),
            (
                "Connections will be created."
                if options["create_connections"]
                else "Only the equipment rooms will be placed."
            ),
            "",
            "Apply these suggestions?",
            ]
        )
        if QMessageBox.question(
            self,
            "Apply Zone-based Equipment Room Suggestions",
            "\n".join(confirmation),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes,
        ) != QMessageBox.Yes:
            return

        self.push_undo_state("Suggest equipment rooms from placement zones")
        # Carry the zone planner's routing contract into automatic network
        # design so a later network pass cannot silently move endpoints across
        # floors or route through a transition on another level.
        self.store.data.setdefault("network_settings", {})[
            "auto_planner_same_floor_only"
        ] = bool(options["same_floor_only"])
        for change in recommended_changes:
            zone = self._placement_zone_by_id(change["zone_id"])
            if zone is None:
                continue
            limit_key = (
                "max_distributed_equipment_rooms"
                if change["kind"] == "distributed_equipment_room"
                else "max_comms_rooms"
            )
            zone[limit_key] = int(change["suggested_limit"])
        used_names = set(self.store.names_in_use())
        next_numbers = {
            "comms_room": self._highest_comms_room_number("CR") + 1,
            "distributed_equipment_room": self._highest_comms_room_number("DER") + 1,
        }
        existing_connection_ids = {
            str(item.get("id", "")).strip()
            for item in self.store.data.get("connections", [])
            if str(item.get("id", "")).strip()
        }
        connected_targets = self._existing_connection_targets()
        created_rooms = []
        created_connections = 0
        created_connection_ports = 0
        missing_anchor = []

        for suggestion in suggestions:
            kind = suggestion["kind"]
            existing_room_name = str(
                suggestion.get("existing_room_name", "") or ""
            ).strip()
            if existing_room_name:
                room_name = existing_room_name
            else:
                room_name, next_numbers[kind] = self._next_comms_room_name(
                    used_names,
                    suggestion["floor"],
                    next_numbers[kind],
                    kind,
                )
                self.store.add_location(
                    room_name,
                    suggestion["floor"],
                    suggestion["x"],
                    suggestion["y"],
                    kind=kind,
                    max_cable_length_m=float(options["max_distance_m"]),
                )
                for location in self.store.data.get("locations", []):
                    if str(location.get("name", "")).strip() == room_name:
                        location["cable_limit"] = int(suggestion["port_limit"])
                        location["placement_zone_id"] = suggestion["zone_id"]
                        if kind == "distributed_equipment_room":
                            location["cabinet_type"] = "slim_wall"
                            location["max_network_cabinets"] = 1
                        else:
                            location["cabinet_type"] = "standard"
                            location["max_network_cabinets"] = int(
                                options["comms_cabinet_count"]
                            )
                        break

            anchor_name = suggestion.get("anchor_name", "")
            anchor_connected = bool(anchor_name == room_name)
            if not anchor_connected:
                anchor_connected = bool(
                    anchor_name
                    and self._connect_equipment_room_to_corridor_anchor(
                        room_name, anchor_name
                    )
                )
            if not anchor_connected:
                missing_anchor.extend(suggestion["data_point_names"])

            if options["create_connections"] and anchor_connected:
                for point_name in suggestion["data_point_names"]:
                    if point_name in connected_targets:
                        continue
                    point = next(
                        (
                            item
                            for item in self.store.data.get("data_points", [])
                            if str(item.get("name", "")).strip() == point_name
                        ),
                        None,
                    )
                    if point is None:
                        continue
                    point_anchor_name = str(
                        (
                            suggestion.get("data_point_anchor_names", {}) or {}
                        ).get(point_name, "")
                        or ""
                    ).strip()
                    if (
                        point_anchor_name
                        and point_anchor_name != point_name
                        and not self._safe_add_same_floor_edge(
                            point_name, point_anchor_name
                        )
                    ):
                        missing_anchor.append(point_name)
                        continue
                    if hasattr(self.store, "data_point_required_port_count"):
                        connection_ports = self.store.data_point_required_port_count(
                            point
                        )
                    else:
                        connection_ports = max(
                            0, int(point.get("qty", 1) or 0)
                        )
                    connection_id, existing_connection_ids = self._next_connection_id(
                        existing_connection_ids
                    )
                    self.store.data.setdefault("connections", []).append(
                        {
                            "id": connection_id,
                            "from": room_name,
                            "to": point_name,
                            "qty": int(connection_ports),
                            "route_profile": "",
                        }
                    )
                    connected_targets.add(point_name)
                    created_connections += 1
                    created_connection_ports += int(connection_ports)
            if not existing_room_name:
                created_rooms.append(room_name)

        if created_rooms:
            self.selected_point_name = created_rooms[0]
        self.refresh_canvas()

        all_data_point_names = set()
        for item in self.store.data.get("data_points", []):
            point_name = str(item.get("name", "")).strip()
            if not point_name or (
                options["scope"] == "current"
                and int(item.get("floor", 0)) != int(self.floor_spin.value())
            ):
                continue
            required_ports = (
                self.store.data_point_required_port_count(item)
                if hasattr(self.store, "data_point_required_port_count")
                else max(0, int(item.get("qty", 1) or 0))
            )
            if int(required_ports) > 0:
                all_data_point_names.add(point_name)
        final_connected_targets = self._existing_connection_targets()
        unconnected = sorted(all_data_point_names - final_connected_targets)
        unconnected_name_set = set(unconnected)
        unconnected_ports = 0
        for point in self.store.data.get("data_points", []):
            point_name = str(point.get("name", "")).strip()
            if point_name not in unconnected_name_set:
                continue
            if hasattr(self.store, "data_point_required_port_count"):
                unconnected_ports += int(
                    self.store.data_point_required_port_count(point)
                )
            else:
                unconnected_ports += max(0, int(point.get("qty", 1) or 0))
        summary = (
            f"Placed {len(created_rooms)} equipment room(s) and created "
            f"{created_connections} data-point connection(s) covering "
            f"{created_connection_ports} port(s)."
        )
        self.set_status(summary)
        if unconnected:
            examples = "\n".join(f"- {name}" for name in unconnected[:40])
            extra = (
                f"\n- ... and {len(unconnected) - 40} more"
                if len(unconnected) > 40
                else ""
            )
            anchor_note = (
                "\n\nSome suggested rooms had no usable same-floor corridor node."
                if missing_anchor
                else ""
            )
            QMessageBox.warning(
                self,
                "Unconnected Data Ports",
                summary
                + f"\n\n{len(unconnected)} data-point location(s), requiring "
                + f"{unconnected_ports} port(s), remain unconnected:\n"
                + examples
                + extra
                + anchor_note,
            )
        else:
            QMessageBox.information(
                self,
                "Zone-based Equipment Room Suggestion",
                summary + "\n\nAll data ports in the selected scope are connected.",
            )

    def _candidate_comms_room_nodes(
        self, include_restricted=False, location_kind="comms_room"
    ):
        result = []

        for item in self.store.data.get("corridors", {}).get("nodes", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            if bool(item.get("restricted", False)) and not include_restricted:
                continue

            if not self._placement_allowed_at(
                int(item.get("floor", 0)),
                float(item.get("x", 0.0)),
                float(item.get("y", 0.0)),
                location_kind,
            ):
                continue

            result.append(name)

        return sorted(result)

    def comms_room_names(self):
        result = []
        for item in self.store.data.get("locations", []):
            if str(item.get("kind", "location")) in {
                "comms_room",
                "distributed_equipment_room",
            }:
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
        return self._connected_data_point_names()

    def _autoroute_room_capacity_limits(self, room_names):
        """Return maximum endpoint ports per room, or ``None`` when unlimited."""
        switch_profile = self.store.access_switch_capacity_profile()
        ports_per_switch = max(1, int(switch_profile.get("ports", 48) or 48))
        full_cabinet_switches = max(
            1,
            int(switch_profile.get("switches_per_full_cabinet", 1) or 1),
        )
        locations = {
            str(item.get("name", "")).strip(): item
            for item in self.store.data.get("locations", [])
            if str(item.get("name", "")).strip()
        }
        result = {}
        for room_name in room_names:
            room = locations.get(str(room_name), {})
            maximum_cabinets = max(
                0, int(room.get("max_network_cabinets", 0) or 0)
            )
            if maximum_cabinets <= 0:
                result[str(room_name)] = None
                continue
            cabinet_type = str(
                room.get("cabinet_type", "standard") or "standard"
            ).strip().lower()
            switches_per_cabinet = (
                2 if cabinet_type == "slim_wall" else full_cabinet_switches
            )
            result[str(room_name)] = (
                ports_per_switch * switches_per_cabinet * maximum_cabinets
            )
        return result

    def _autoroute_existing_room_port_usage(
        self, room_names, required_ports_by_name
    ):
        """Count existing Routing-tab endpoint demand against each room."""
        room_names = {str(name) for name in room_names}
        usage = {name: 0 for name in room_names}
        for connection in self.store.data.get("connections", []):
            if not isinstance(connection, dict):
                continue
            left = str(connection.get("from", "") or "").strip()
            right = str(connection.get("to", "") or "").strip()
            room_name = None
            point_name = None
            if left in room_names and right in required_ports_by_name:
                room_name, point_name = left, right
            elif right in room_names and left in required_ports_by_name:
                room_name, point_name = right, left
            if room_name is not None:
                usage[room_name] += max(
                    0, int(required_ports_by_name.get(point_name, 0) or 0)
                )
        return usage

    def autoroute_data_points(self):
        comms_rooms = self.comms_room_names()
        if not comms_rooms:
            QMessageBox.critical(self, "Autoroute", "No comms rooms found.")
            return

        # Room-type asset edits can change the number of physical ports needed
        # at every placed data point. Synchronise both the point quantities and
        # any existing graph connections before selecting autoroute candidates.
        required_ports_by_name = {}
        quantity_sync_required = False
        for point in self.store.data.get("data_points", []):
            if not isinstance(point, dict):
                continue
            point_name = str(point.get("name", "") or "").strip()
            if not point_name:
                continue
            required_ports = self.store.data_point_required_port_count(point)
            required_ports_by_name[point_name] = int(required_ports)
            try:
                stored_ports = int(point.get("qty", 1) or 0)
            except (TypeError, ValueError):
                stored_ports = 1
            if stored_ports != int(required_ports):
                quantity_sync_required = True

        for connection in self.store.data.get("connections", []):
            if not isinstance(connection, dict):
                continue
            point_name = str(connection.get("to", "") or "").strip()
            if point_name not in required_ports_by_name:
                point_name = str(connection.get("from", "") or "").strip()
            if point_name not in required_ports_by_name:
                continue
            try:
                connection_ports = int(connection.get("qty", 1) or 0)
            except (TypeError, ValueError):
                connection_ports = 1
            if connection_ports != required_ports_by_name[point_name]:
                quantity_sync_required = True
                break

        undo_pushed = False
        if quantity_sync_required:
            self.push_undo_state("Update data point quantities for autoroute")
            undo_pushed = True
        self.store.sync_all_room_type_quantities()

        graph, points = self._build_routing_graph()
        same_floor_only = bool(self.autoroute_same_floor_check.isChecked())
        follow_existing = bool(
            self.autoroute_follow_existing_check.isChecked()
        )
        ignore_unconnected = bool(
            self.autoroute_ignore_unconnected_check.isChecked()
        )
        existing_targets = self._existing_connection_targets()
        room_capacity_limits = self._autoroute_room_capacity_limits(comms_rooms)
        room_port_usage = self._autoroute_existing_room_port_usage(
            comms_rooms, required_ports_by_name
        )
        graph_unconnected = (
            self._unconnected_data_point_names() if ignore_unconnected else set()
        )
        preferred_edges = (
            _autoroute_existing_route_edges(
                graph,
                comms_rooms,
                self.store.data.get("connections", []),
            )
            if follow_existing
            else set()
        )

        data_points = [
            {
                **item,
                "_required_ports": self.store.data_point_required_port_count(item),
            }
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
            and str(item.get("name", "")).strip() not in existing_targets
            and str(item.get("name", "")).strip() not in graph_unconnected
        ]

        # A placed data point still needs a Routing-tab path even when its
        # current room-type/manual demand resolves to zero ports.  Keep the
        # connection quantity at zero so it does not consume switch capacity,
        # but do not silently remove the point before the distance check runs.
        zero_demand_candidates = sum(
            1
            for item in data_points
            if int(item.get("_required_ports", 0) or 0) == 0
        )

        skipped_existing = sum(
            1
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip() in existing_targets
        )
        skipped_graph_unconnected = sum(
            1
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip() in graph_unconnected
            and str(item.get("name", "")).strip() not in existing_targets
        )

        if not data_points:
            QMessageBox.information(
                self,
                "Autoroute",
                "No data points to autoroute. "
                f"{skipped_existing} already had connections and "
                f"{skipped_graph_unconnected} were ignored because they are "
                "not connected to the routing graph.",
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
                initargs=(
                    graph,
                    points,
                    comms_rooms,
                    same_floor_only,
                    preferred_edges,
                    room_capacity_limits,
                ),
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
                        f"Autorouting with {worker_count} process(es)"
                        + (" on the same floor only..." if same_floor_only else "...")
                        + (" Following existing routes." if follow_existing else "")
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

        capacity_skipped = []
        assigned_results = []

        def result_candidates(result):
            candidates = list(result.get("room_candidates", []))
            if candidates:
                return candidates
            return [
                {
                    "room": result.get("from", ""),
                    "cost": result.get("cost", 0.0),
                    "route_path": list(result.get("route_path", [])),
                }
            ]

        def capacity_assignment_order(result):
            candidates = result_candidates(result)
            has_unlimited_room = any(
                room_capacity_limits.get(str(candidate.get("room", ""))) is None
                for candidate in candidates
            )
            return (
                1 if has_unlimited_room else 0,
                len(candidates),
                -max(0, int(result.get("qty", 0) or 0)),
                str(result.get("point_name", "")),
            )

        for result in sorted(created_results, key=capacity_assignment_order):
            required_ports = max(0, int(result.get("qty", 0) or 0))
            candidates = result_candidates(result)
            selected_candidate = None
            for candidate in candidates:
                room_name = str(candidate.get("room", "")).strip()
                capacity = room_capacity_limits.get(room_name)
                used_ports = int(room_port_usage.get(room_name, 0) or 0)
                if (
                    required_ports == 0
                    or capacity is None
                    or used_ports + required_ports <= int(capacity)
                ):
                    selected_candidate = candidate
                    break

            if selected_candidate is None:
                capacity_skipped.append(
                    {
                        "point_name": str(result.get("point_name", "")),
                        "required_ports": required_ports,
                        "rooms": [
                            {
                                "name": str(candidate.get("room", "")),
                                "used": int(
                                    room_port_usage.get(
                                        str(candidate.get("room", "")), 0
                                    )
                                    or 0
                                ),
                                "capacity": room_capacity_limits.get(
                                    str(candidate.get("room", ""))
                                ),
                            }
                            for candidate in candidates
                        ],
                    }
                )
                continue

            selected_room = str(selected_candidate.get("room", "")).strip()
            result["from"] = selected_room
            result["cost"] = float(selected_candidate.get("cost", 0.0) or 0.0)
            result["route_path"] = list(
                selected_candidate.get("route_path", [])
            )
            room_port_usage[selected_room] = int(
                room_port_usage.get(selected_room, 0) or 0
            ) + required_ports
            assigned_results.append(result)

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
        for result in assigned_results:
            created_rows.append(
                {
                    "id": next_connection_id(),
                    "from": result["from"],
                    "to": result["to"],
                    "qty": result["qty"],
                    "route_profile": result.get("route_profile", ""),
                    "route_path": list(result.get("route_path", [])),
                    "length_m": round(float(result.get("cost", 0.0) or 0.0), 3),
                }
            )

        if created_rows:
            if not undo_pushed:
                self.push_undo_state("Autoroute data points")
            self.store.data.setdefault("connections", []).extend(created_rows)
            self.refresh_canvas()
            self.set_status(
                f"Autorouted {len(created_rows)} data point(s) using {worker_count} process(es)"
                + (" on the same floor only" if same_floor_only else "")
                + (" while following existing routes" if follow_existing else "")
            )

        message_lines = [f"Created {len(created_rows)} connection(s)."]
        message_lines.append(
            "Floor restriction: same floor only."
            if same_floor_only
            else "Floor restriction: cross-floor routing permitted."
        )
        message_lines.append(
            f"Existing-route preference: {'enabled' if follow_existing else 'disabled'}."
        )

        if zero_demand_candidates:
            message_lines.append(
                f"Included {zero_demand_candidates} zero-demand data point(s) in "
                "distance routing; their connection quantity remains 0."
            )

        if skipped_existing:
            message_lines.append(
                f"Skipped {skipped_existing} already-connected data point(s)."
            )

        if skipped_graph_unconnected:
            message_lines.append(
                f"Ignored {skipped_graph_unconnected} data point(s) not connected "
                "to the routing graph."
            )

        if skipped_unreachable:
            message_lines.append(
                "Unreachable data point(s): "
                + ", ".join(skipped_unreachable[:15])
                + (" ..." if len(skipped_unreachable) > 15 else "")
            )

        if capacity_skipped:
            message_lines.append(
                f"Capacity-limited data point(s): {len(capacity_skipped)}."
            )
            for row in capacity_skipped[:10]:
                room_text = ", ".join(
                    f"{room['name']} {room['used']}/{room['capacity']} ports"
                    for room in row["rooms"]
                    if room["capacity"] is not None
                )
                message_lines.append(
                    f"{row['point_name']} needs {row['required_ports']} ports"
                    + (f"; {room_text}" if room_text else "")
                )
            if len(capacity_skipped) > 10:
                message_lines.append(
                    f"... and {len(capacity_skipped) - 10} more capacity-limited point(s)."
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

    def generate_cable_report(self):
        if not self.current_json_path:
            QMessageBox.warning(
                self,
                "No project",
                "Open or save a project database first.",
            )
            return

        try:
            self.save_json()
            script_path = Path(__file__).with_name("cable_length_report.py")

            output_path = Path(self.current_json_path).with_suffix("")
            output_path = output_path.with_name(output_path.name + "_cable_lengths.csv")

            subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    str(self.current_json_path),
                    "-o",
                    str(output_path),
                ],
                check=True,
            )

            QMessageBox.information(
                self,
                "Report complete",
                f"Cable report written to:\n\n{output_path}",
            )

        except subprocess.CalledProcessError as exc:
            QMessageBox.critical(
                self,
                "Report failed",
                f"Report generation failed:\n\n{exc}",
            )

        except Exception as exc:
            QMessageBox.critical(
                self,
                "Error",
                str(exc),
            )

    @staticmethod
    def _next_import_database_path(json_path):
        source = Path(json_path)
        candidate = source.with_suffix(".crsdb")
        if not candidate.exists():
            return candidate
        number = 2
        while True:
            candidate = source.with_name(f"{source.stem}_imported_{number}.crsdb")
            if not candidate.exists():
                return candidate
            number += 1

    def _activate_loaded_project(self, store, path, status_message):
        self.store = store
        self._render_data_revision += 1
        self._measure_data_point_name = None
        self._clear_data_room_measurement_overlay()
        self.bulk_location_session = None
        self.bulk_data_point_session = None
        self.current_json_path = str(path)
        self._clear_dxf_cache()
        current_floor = self.floor_spin.value()
        self._pending_fit_after_load = bool(self.get_floor_dxf_path(current_floor))
        self._queue_all_floor_dxf_loads(active_floor=current_floor, force_reload=False)
        self.set_status(status_message)
        self.refresh_canvas()
        self.fit_view()

    def _open_project_path(self, path):
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            store = JsonStore.from_file(path)
            opened_path = Path(path)
            if store.storage_format == "json":
                database_path = self._next_import_database_path(opened_path)
                store.save(str(database_path))
                opened_path = database_path
                status = f"Imported {Path(path).name} as {database_path.name}"
            else:
                database_path = None
                status = f"Opened {opened_path.name}"
        finally:
            QApplication.restoreOverrideCursor()

        if database_path is not None:
            QMessageBox.information(
                self,
                "JSON project imported",
                "The legacy JSON project was imported into SQLite.\n\n"
                f"Database: {database_path}\n"
                f"Original JSON retained: {path}",
            )
        self._activate_loaded_project(store, opened_path, status)

    def open_json(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Open Cable Routing Project",
            "",
            "Cable Routing projects (*.crsdb *.sqlite *.db);;"
            "Legacy JSON projects (*.json);;All files (*)",
        )
        if not path:
            return
        try:
            self._open_project_path(path)
        except Exception as exc:
            QMessageBox.critical(self, "Open project failed", str(exc))

    def import_json(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Import Legacy JSON", "", "JSON files (*.json)"
        )
        if not path:
            return
        try:
            self._open_project_path(path)
        except Exception as exc:
            QMessageBox.critical(self, "JSON import failed", str(exc))

    def import_locations_from_project(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Import Locations from Project",
            "",
            "Cable Routing projects (*.crsdb *.sqlite *.db);;"
            "Legacy JSON projects (*.json);;All files (*)",
        )
        if not path:
            return

        if self.current_json_path and os.path.normcase(os.path.abspath(path)) == os.path.normcase(
            os.path.abspath(self.current_json_path)
        ):
            QMessageBox.information(
                self,
                "Import Locations",
                "The selected file is the project currently open.",
            )
            return

        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            source_store = JsonStore.from_file(path)
        except Exception as exc:
            QMessageBox.critical(
                self,
                "Location import failed",
                f"The source project could not be opened:\n\n{exc}",
            )
            return
        finally:
            QApplication.restoreOverrideCursor()

        source_locations = [
            deepcopy(item)
            for item in source_store.data.get("locations", [])
            if isinstance(item, dict) and str(item.get("name", "")).strip()
        ]
        if not source_locations:
            QMessageBox.information(
                self,
                "Import Locations",
                f"{Path(path).name} does not contain any locations.",
            )
            return

        selection_dialog = QDialog(self)
        selection_dialog.setWindowTitle("Import Locations from Project")
        layout = QVBoxLayout(selection_dialog)
        intro = QLabel(
            f"Select locations to import from {Path(path).name}. "
            "Data points, departments, and network connections are not imported."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        location_list = QListWidget()
        location_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        for index, location in enumerate(source_locations):
            name = str(location.get("name", "")).strip()
            kind = str(location.get("kind", "location") or "location")
            floor = int(location.get("floor", 0) or 0)
            item = QListWidgetItem(f"{name}  •  Floor {floor}  •  {kind}")
            item.setData(Qt.UserRole, index)
            location_list.addItem(item)
            item.setSelected(True)
        layout.addWidget(location_list, 1)

        selection_buttons = QHBoxLayout()
        select_all_button = QPushButton("Select all")
        clear_button = QPushButton("Clear selection")
        select_all_button.clicked.connect(location_list.selectAll)
        clear_button.clicked.connect(location_list.clearSelection)
        selection_buttons.addWidget(select_all_button)
        selection_buttons.addWidget(clear_button)
        selection_buttons.addStretch(1)
        layout.addLayout(selection_buttons)

        conflict_combo = QComboBox()
        conflict_combo.addItem("Skip names already used in this project", "skip")
        conflict_combo.addItem("Rename imported locations when names conflict", "rename")
        form = QFormLayout()
        form.addRow("Duplicate names", conflict_combo)
        layout.addLayout(form)

        attach_check = QCheckBox(
            "Attach each imported location to one matching corridor node when available"
        )
        attach_check.setChecked(True)
        attach_check.setToolTip(
            "Only corridor nodes already present in the current project are used. "
            "Location-to-location edges and network connections are not imported."
        )
        layout.addWidget(attach_check)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(selection_dialog.accept)
        buttons.rejected.connect(selection_dialog.reject)
        layout.addWidget(buttons)
        selection_dialog.resize(720, 540)
        if selection_dialog.exec() != QDialog.Accepted:
            return

        selected_indexes = sorted(
            int(item.data(Qt.UserRole)) for item in location_list.selectedItems()
        )
        if not selected_indexes:
            QMessageBox.information(
                self,
                "Import Locations",
                "No locations were selected.",
            )
            return

        conflict_mode = str(conflict_combo.currentData() or "skip")
        used_names = set(self.store.names_in_use())
        destination_departments = set(self.store.department_ids())
        destination_zone_ids = {
            str(zone.get("id", "")).strip()
            for zone in self.store.data.get("equipment_room_placement_zones", [])
            if str(zone.get("id", "")).strip()
        }
        imported_rows = []
        source_to_imported = {}
        skipped_names = []
        removed_department_links = 0
        removed_zone_links = 0

        for index in selected_indexes:
            source = deepcopy(source_locations[index])
            source_name = str(source.get("name", "")).strip()
            imported_name = source_name
            if imported_name in used_names:
                if conflict_mode == "skip":
                    skipped_names.append(source_name)
                    continue
                suffix = 1
                imported_name = f"{source_name} (imported)"
                while imported_name in used_names:
                    suffix += 1
                    imported_name = f"{source_name} (imported {suffix})"

            source["name"] = imported_name
            source["floor"] = int(source.get("floor", 0) or 0)
            source["x"] = round(float(source.get("x", 0.0) or 0.0), 3)
            source["y"] = round(float(source.get("y", 0.0) or 0.0), 3)
            source["kind"] = str(source.get("kind", "location") or "location")
            department_ids = [
                str(value).strip()
                for value in source.get("department_ids", []) or []
                if str(value).strip()
            ]
            valid_department_ids = [
                value for value in department_ids if value in destination_departments
            ]
            removed_department_links += len(department_ids) - len(valid_department_ids)
            source["department_ids"] = valid_department_ids
            placement_zone_id = str(source.get("placement_zone_id", "")).strip()
            if placement_zone_id and placement_zone_id not in destination_zone_ids:
                source.pop("placement_zone_id", None)
                removed_zone_links += 1

            imported_rows.append(source)
            source_to_imported[source_name] = imported_name
            used_names.add(imported_name)

        if not imported_rows:
            QMessageBox.information(
                self,
                "Import Locations",
                "No locations were imported because every selected name already exists.",
            )
            return

        self.push_undo_state("Import locations from project")
        self.store.data.setdefault("locations", []).extend(imported_rows)

        attached_locations = 0
        if attach_check.isChecked():
            destination_points = self.store.all_points()
            source_edges = source_store.data.get("corridors", {}).get("edges", [])
            for source_name, imported_name in source_to_imported.items():
                imported_point = destination_points.get(imported_name)
                if imported_point is None:
                    continue
                corridor_candidates = set()
                for edge in source_edges:
                    edge_from = str(edge.get("from", "")).strip()
                    edge_to = str(edge.get("to", "")).strip()
                    if edge_from == source_name:
                        corridor_candidates.add(edge_to)
                    elif edge_to == source_name:
                        corridor_candidates.add(edge_from)
                valid_candidates = [
                    name
                    for name in corridor_candidates
                    if name in destination_points
                    and str(destination_points[name].get("kind", "")).strip()
                    == "corridor_node"
                    and int(destination_points[name].get("floor", 0))
                    == int(imported_point.get("floor", 0))
                ]
                if not valid_candidates:
                    continue
                anchor_name = min(
                    valid_candidates,
                    key=lambda name: math.hypot(
                        float(destination_points[name].get("x", 0.0))
                        - float(imported_point.get("x", 0.0)),
                        float(destination_points[name].get("y", 0.0))
                        - float(imported_point.get("y", 0.0)),
                    ),
                )
                if self._connect_equipment_room_to_corridor_anchor(
                    imported_name, anchor_name
                ):
                    attached_locations += 1

        self.selected_point_name = imported_rows[0]["name"]
        self.refresh_canvas()
        summary = (
            f"Imported {len(imported_rows)} location(s) from {Path(path).name}.\n\n"
            f"Attached to matching corridor nodes: {attached_locations}\n"
            f"Skipped duplicate names: {len(skipped_names)}"
        )
        if removed_department_links:
            summary += (
                f"\nRemoved unavailable department links: {removed_department_links}"
            )
        if removed_zone_links:
            summary += f"\nRemoved unavailable placement-zone links: {removed_zone_links}"
        QMessageBox.information(self, "Location import complete", summary)
        self.set_status(
            f"Imported {len(imported_rows)} location(s) from {Path(path).name}"
        )

    def _project_save_path(self, title, initial_path=""):
        path, _ = QFileDialog.getSaveFileName(
            self,
            title,
            initial_path,
            "Cable Routing project (*.crsdb)",
        )
        if not path:
            return ""
        if not path.lower().endswith(".crsdb"):
            path += ".crsdb"
        return path

    def _save_project_to_path(self, path, status_prefix="Saved"):
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            self.store.save(path)
        finally:
            QApplication.restoreOverrideCursor()
        self.current_json_path = self.store.storage_path or str(path)
        statistics = getattr(self.store, "last_save_statistics", None)
        detail = ""
        if statistics is not None:
            if getattr(statistics, "revision_created", True):
                detail = (
                    f" - revision {statistics.revision_number} - "
                    f"{statistics.changed_chunks} changed chunk(s), "
                    f"{statistics.unchanged_chunks} unchanged"
                )
            else:
                detail = " - no project data changes"
                if getattr(statistics, "revision_number", 0):
                    detail += f" - latest revision {statistics.revision_number}"
            if getattr(statistics, "compacted", False):
                reclaimed_mb = getattr(statistics, "reclaimed_bytes", 0) / (1024 * 1024)
                detail += f" - compacted ({reclaimed_mb:.1f} MiB reclaimed)"
            elif getattr(statistics, "compaction_error", ""):
                detail += " - compaction skipped"
        self.set_status(f"{status_prefix} {Path(self.current_json_path).name}{detail}")
        self.refresh_canvas()

    def show_revision_history(self):
        if getattr(self.store, "storage_format", "") != "sqlite" or not getattr(
            self.store, "storage_path", ""
        ):
            QMessageBox.information(
                self,
                "Revision History",
                "Save this project as a .crsdb database before viewing revision history.",
            )
            return
        dialog = getattr(self, "_revision_history_dialog", None)
        if dialog is None or not dialog.isVisible():
            dialog = RevisionHistoryDialog(self)
            self._revision_history_dialog = dialog
        dialog.refresh_from_parent()
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()

    def _review_pdf_in_report_studio(
        self,
        pdf_path,
        settings_key="general",
        report_title="PDF Report Studio",
        output_path=None,
        layout_manifest=None,
        preview_builder=None,
        report_option_groups=None,
    ):
        pdf_path = str(pdf_path or "").strip()
        if not pdf_path or not Path(pdf_path).exists():
            return False
        destination_path = str(output_path or pdf_path).strip()
        if not destination_path:
            return False
        try:
            from zone_report_studio import PdfReportStudioDialog
        except ImportError as exc:
            QMessageBox.critical(
                self, "Report Studio failed", f"Could not open Report Studio.\n\n{exc}"
            )
            return False
        saved_by_report = self.store.data.get("pdf_report_studio_settings", {}) or {}
        initial_settings = dict(saved_by_report.get(str(settings_key), {}) or {})
        initial_settings.setdefault(
            "snippet_templates",
            deepcopy(self.store.data.get("network_pdf_snippet_templates", []) or []),
        )
        initial_settings.setdefault(
            "page_templates",
            deepcopy(self.store.data.get("pdf_report_page_templates", []) or []),
        )

        if preview_builder is None:
            def studio_preview_builder(_settings):
                return pdf_path, list(layout_manifest or [])
        else:
            studio_preview_builder = preview_builder

        studio = PdfReportStudioDialog(
            studio_preview_builder,
            initial_settings=initial_settings,
            parent=self,
            report_title=report_title,
            show_report_controls=bool(layout_manifest),
            network_data=self.store.data,
            report_option_groups=report_option_groups,
        )
        try:
            if studio.run_as_window() != QDialog.Accepted:
                return False
            settings = studio.export_settings()
            selected_pdf_path = str(
                getattr(studio, "base_preview_path", "") or pdf_path
            )
            selected_manifest = list(studio.manifest or layout_manifest or [])
        finally:
            studio.release_preview()

        from uuid import uuid4

        destination = Path(destination_path)
        temporary_output = str(
            destination.with_name(
                f".{destination.stem}_report_studio_{uuid4().hex}.pdf"
            )
        )
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            annotations = list(settings.get("annotations", []) or [])
            generated_callouts = selected_manifest
            if annotations or generated_callouts or settings.get("extra_pages"):
                from pdf_report_annotations import apply_pdf_studio_annotations

                apply_pdf_studio_annotations(
                    selected_pdf_path,
                    temporary_output,
                    settings,
                    callout_manifest=generated_callouts,
                )
            else:
                shutil.copy2(selected_pdf_path, temporary_output)
            os.replace(temporary_output, destination_path)
        except Exception as exc:
            if Path(temporary_output).exists():
                try:
                    Path(temporary_output).unlink()
                except OSError:
                    pass
            QMessageBox.critical(self, "Report Studio export failed", str(exc))
            return False
        finally:
            QApplication.restoreOverrideCursor()
        saved_by_report = dict(saved_by_report)
        saved_by_report[str(settings_key)] = dict(settings)
        self.store.data["pdf_report_studio_settings"] = saved_by_report
        self.store.data["network_pdf_snippet_templates"] = deepcopy(
            settings.get("snippet_templates", []) or []
        )
        self.store.data["pdf_report_page_templates"] = deepcopy(
            settings.get("page_templates", []) or []
        )
        return True

    def _export_pdf_through_report_studio(
        self,
        destination_path,
        build_preview,
        settings_key,
        report_title,
        build_preview_with_settings=None,
        report_option_groups=None,
    ):
        """Build a temporary PDF and only publish it after studio approval."""
        from uuid import uuid4

        destination_path = str(destination_path)
        destination_parent = Path(destination_path).expanduser().resolve().parent
        preview_directory = destination_parent / (
            f".cable_route_pdf_preview_{uuid4().hex}"
        )
        preview_directory.mkdir()
        try:
            preview_path = str(
                preview_directory / Path(destination_path).name
            )
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                generated = build_preview(preview_path)
            finally:
                QApplication.restoreOverrideCursor()
            if isinstance(generated, tuple):
                generated_path = str(generated[0] or preview_path)
                layout_manifest = list(generated[1] or [])
            else:
                generated_path = str(generated or preview_path)
                layout_manifest = []
            studio_preview_builder = None
            if build_preview_with_settings is not None:
                def studio_preview_builder(settings):
                    candidate = preview_directory / (
                        f"{Path(destination_path).stem}_{uuid4().hex}.pdf"
                    )
                    rebuilt = build_preview_with_settings(str(candidate), settings)
                    if isinstance(rebuilt, tuple):
                        return str(rebuilt[0] or candidate), list(rebuilt[1] or [])
                    return str(rebuilt or candidate), []
            if not self._review_pdf_in_report_studio(
                generated_path,
                settings_key=settings_key,
                report_title=report_title,
                output_path=destination_path,
                layout_manifest=layout_manifest,
                preview_builder=studio_preview_builder,
                report_option_groups=report_option_groups,
            ):
                return ""
            return destination_path
        finally:
            shutil.rmtree(preview_directory, ignore_errors=True)

    def open_pdf_in_report_studio(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Open PDF in Report Studio",
            "",
            "PDF files (*.pdf)",
        )
        if not path:
            return
        if self._review_pdf_in_report_studio(
            path,
            settings_key="external_pdf",
            report_title="PDF Report Studio",
        ):
            self.set_status(f"Updated PDF in Report Studio: {Path(path).name}")
            QMessageBox.information(
                self,
                "Report Studio complete",
                f"Updated PDF written to:\n\n{path}",
            )

    def export_revision_history_pdf(self):
        if getattr(self.store, "storage_format", "") != "sqlite" or not getattr(
            self.store, "storage_path", ""
        ):
            QMessageBox.information(
                self,
                "Revision PDF",
                "Save this project as a .crsdb database before exporting revision history.",
            )
            return

        revisions = self.store.revision_history()
        if not revisions:
            QMessageBox.information(
                self,
                "Revision PDF",
                "No saved revision history is available to export.",
            )
            return

        initial = str(Path(self.store.storage_path).with_suffix(""))
        initial = str(Path(initial).with_name(Path(initial).name + "_revision_history.pdf"))
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Revision History PDF",
            initial,
            "PDF files (*.pdf)",
        )
        if not path:
            return
        if not path.lower().endswith(".pdf"):
            path += ".pdf"

        try:
            from revision_report import export_revision_history_pdf

            output_path = self._export_pdf_through_report_studio(
                path,
                lambda preview_path: export_revision_history_pdf(
                    revisions,
                    preview_path,
                    project_data=self.store.data,
                    source_path=self.store.storage_path
                    or self.current_json_path
                    or "",
                ),
                settings_key="revision_history",
                report_title="Revision History Report Studio",
            )
        except ImportError as exc:
            QMessageBox.critical(
                self,
                "Revision PDF failed",
                f"PDF export requires reportlab. Install project requirements and try again.\n\n{exc}",
            )
            return
        except Exception as exc:
            QMessageBox.critical(self, "Revision PDF failed", str(exc))
            return
        if not output_path:
            return
        self.set_status(f"Exported revision history PDF: {Path(output_path).name}")
        QMessageBox.information(
            self,
            "Revision PDF complete",
            f"Revision history PDF written to:\n\n{output_path}",
        )

    def export_asset_register_pdf(self):
        assets = self.store.data.get("assets", []) or []
        if not assets:
            QMessageBox.information(
                self,
                "Asset Register PDF",
                "No project assets are available to export.",
            )
            return

        source_path = (
            getattr(self.store, "storage_path", "") or self.current_json_path or ""
        )
        base_path = Path(source_path) if source_path else Path("cable_routes.crsdb")
        initial = str(
            base_path.with_suffix("").with_name(base_path.stem + "_asset_register.pdf")
        )
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Asset Register PDF",
            initial,
            "PDF files (*.pdf)",
        )
        if not path:
            return
        if not path.lower().endswith(".pdf"):
            path += ".pdf"

        try:
            from asset_register_report import (
                ASSET_REGISTER_COLUMNS,
                asset_register_column_ids,
                export_asset_register_pdf,
            )

            def build_asset_register(preview_path, settings=None):
                settings = settings or {}
                return export_asset_register_pdf(
                    self.store.data,
                    preview_path,
                    source_path=source_path,
                    revision_number=self.latest_project_revision_number(),
                    columns=settings.get(
                        "asset_register_columns", asset_register_column_ids()
                    ),
                )

            output_path = self._export_pdf_through_report_studio(
                path,
                lambda preview_path: build_asset_register(preview_path),
                settings_key="asset_register",
                report_title="Asset Register Report Studio",
                build_preview_with_settings=build_asset_register,
                report_option_groups=[
                    {
                        "title": "Asset register columns",
                        "setting_key": "asset_register_columns",
                        "help": "Choose the columns included in the exported register.",
                        "options": [
                            {"id": column["id"], "label": column["label"]}
                            for column in ASSET_REGISTER_COLUMNS
                        ],
                    }
                ],
            )
        except ImportError as exc:
            QMessageBox.critical(
                self,
                "Asset Register PDF failed",
                "PDF export requires reportlab. Install project requirements and "
                f"try again.\n\n{exc}",
            )
            return
        except Exception as exc:
            QMessageBox.critical(self, "Asset Register PDF failed", str(exc))
            return
        if not output_path:
            return
        self.set_status(f"Exported asset register PDF: {Path(output_path).name}")
        QMessageBox.information(
            self,
            "Asset Register PDF complete",
            f"Asset register PDF written to:\n\n{output_path}",
        )

    def export_room_type_asset_rfi_pdf(self):
        rfi_state = self.store.data.get("room_type_asset_rfi", {})
        queries = rfi_state.get("queries", []) if isinstance(rfi_state, dict) else []
        history = rfi_state.get("history", []) if isinstance(rfi_state, dict) else []
        if not queries and not history:
            QMessageBox.information(
                self,
                "Room Type Asset RFI PDF",
                "No room type asset queries or audit history are available to export.",
            )
            return

        source_path = (
            getattr(self.store, "storage_path", "") or self.current_json_path or ""
        )
        base_path = Path(source_path) if source_path else Path("cable_routes.crsdb")
        initial = str(
            base_path.with_suffix("").with_name(
                base_path.stem + "_room_type_asset_rfi.pdf"
            )
        )
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Room Type Asset RFI PDF",
            initial,
            "PDF files (*.pdf)",
        )
        if not path:
            return
        if not path.lower().endswith(".pdf"):
            path += ".pdf"

        try:
            from room_type_rfi_report import export_room_type_asset_rfi_pdf

            output_path = self._export_pdf_through_report_studio(
                path,
                lambda preview_path: export_room_type_asset_rfi_pdf(
                    self.store.data,
                    preview_path,
                    source_path=source_path,
                    revision_number=self.latest_project_revision_number(),
                ),
                settings_key="room_type_asset_rfi",
                report_title="Room Type Asset RFI Report Studio",
            )
        except ImportError as exc:
            QMessageBox.critical(
                self,
                "Room Type Asset RFI PDF failed",
                f"PDF export requires reportlab. Install project requirements and try again.\n\n{exc}",
            )
            return
        except Exception as exc:
            QMessageBox.critical(self, "Room Type Asset RFI PDF failed", str(exc))
            return
        if not output_path:
            return
        self.set_status(f"Exported room type asset RFI PDF: {Path(output_path).name}")
        QMessageBox.information(
            self,
            "Room Type Asset RFI PDF complete",
            f"RFI list and audit history written to:\n\n{output_path}",
        )

    def latest_project_revision_number(self):
        try:
            revisions = self.store.revision_history(limit=1)
        except Exception:
            return 0
        if not revisions:
            return 0
        try:
            return int(revisions[0].get("revision_number", 0) or 0)
        except (TypeError, ValueError):
            return 0

    def export_all_floors_pdf(self):
        options_dialog = FloorPlanPdfOptionsDialog(self)
        if options_dialog.exec() != QDialog.Accepted:
            return
        options = options_dialog.export_options()
        selected_floors = [int(floor) for floor in options["floors"]]
        all_floors = [int(floor) for floor in self._all_model_floors()]
        if len(selected_floors) == 1:
            floor_suffix = f"floor_{selected_floors[0]}"
        elif selected_floors == all_floors:
            floor_suffix = "all_floors"
        else:
            floor_suffix = "floors_" + "_".join(
                str(floor) for floor in selected_floors
            )
        source_path = (
            getattr(self.store, "storage_path", "")
            or self.current_json_path
            or ""
        )
        base_path = Path(source_path) if source_path else Path("cable_routes.crsdb")
        initial = str(
            base_path.with_suffix("").with_name(
                base_path.stem + f"_{floor_suffix}.pdf"
            )
        )
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Floor Plans PDF",
            initial,
            "PDF files (*.pdf)",
        )
        if not path:
            return
        if not path.lower().endswith(".pdf"):
            path += ".pdf"

        try:
            from floor_plan_pdf import export_floor_plans_pdf

            layout_manifest = []
            output_path = self._export_pdf_through_report_studio(
                path,
                lambda preview_path: (
                    export_floor_plans_pdf(
                        self.store.data,
                        preview_path,
                        source_path=source_path,
                        paper_size=options["paper_size"],
                        scale=options["scale"],
                        revision_number=self.latest_project_revision_number(),
                        layout_manifest=layout_manifest,
                        preview_background=True,
                        floors=selected_floors,
                    ),
                    layout_manifest,
                ),
                settings_key=f"floor_plans:{','.join(map(str, selected_floors))}",
                report_title="Floor Plans Report Studio",
            )
        except ImportError as exc:
            QMessageBox.critical(
                self,
                "Floor PDF failed",
                "PDF export requires reportlab and ezdxf. Install project "
                f"requirements and try again.\n\n{exc}",
            )
            return
        except Exception as exc:
            QMessageBox.critical(self, "Floor PDF failed", str(exc))
            return
        if not output_path:
            return
        self.store.data["floor_plan_pdf_settings"] = dict(options)
        self.set_status(f"Exported floor PDF: {Path(output_path).name}")
        QMessageBox.information(
            self,
            "Floor PDF complete",
            f"Exported {len(selected_floors)} floor sheet(s) to:\n\n"
            f"{output_path}",
        )

    def export_all_floor_zones_pdf(self):
        zones = self.store.data.get("equipment_room_placement_zones", [])
        if not zones:
            QMessageBox.information(
                self,
                "Placement Zones PDF",
                "No equipment-room placement zones are available to export.",
            )
            return
        options_dialog = FloorPlanPdfOptionsDialog(self)
        options_dialog.setWindowTitle("Export Placement Zones PDF")
        if options_dialog.exec() != QDialog.Accepted:
            return
        options = options_dialog.export_options()
        selected_floors = [int(floor) for floor in options["floors"]]
        all_floors = [int(floor) for floor in self._all_model_floors()]
        if len(selected_floors) == 1:
            floor_suffix = f"floor_{selected_floors[0]}"
        elif selected_floors == all_floors:
            floor_suffix = "all_floors"
        else:
            floor_suffix = "floors_" + "_".join(
                str(floor) for floor in selected_floors
            )
        source_path = (
            getattr(self.store, "storage_path", "")
            or self.current_json_path
            or ""
        )
        base_path = Path(source_path) if source_path else Path("cable_routes.crsdb")
        initial = str(
            base_path.with_suffix("").with_name(
                base_path.stem + f"_zones_{floor_suffix}.pdf"
            )
        )
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Placement Zones PDF",
            initial,
            "PDF files (*.pdf)",
        )
        if not path:
            return
        if not path.lower().endswith(".pdf"):
            path += ".pdf"

        try:
            from floor_plan_pdf import export_floor_plans_pdf

            layout_manifest = []
            output_path = self._export_pdf_through_report_studio(
                path,
                lambda preview_path: (
                    export_floor_plans_pdf(
                        self.store.data,
                        preview_path,
                        source_path=source_path,
                        paper_size=options["paper_size"],
                        scale=options["scale"],
                        revision_number=self.latest_project_revision_number(),
                        layout_manifest=layout_manifest,
                        preview_background=True,
                        include_placement_zones=True,
                        floors=selected_floors,
                    ),
                    layout_manifest,
                ),
                settings_key=(
                    "placement_zone_floor_plans:"
                    + ",".join(map(str, selected_floors))
                ),
                report_title="Placement Zones Report Studio",
            )
        except ImportError as exc:
            QMessageBox.critical(
                self,
                "Zones PDF failed",
                "PDF export requires reportlab and ezdxf. Install project "
                f"requirements and try again.\n\n{exc}",
            )
            return
        except Exception as exc:
            QMessageBox.critical(self, "Zones PDF failed", str(exc))
            return
        if not output_path:
            return
        self.store.data["floor_plan_pdf_settings"] = dict(options)
        self.set_status(f"Exported zones PDF: {Path(output_path).name}")
        QMessageBox.information(
            self,
            "Zones PDF complete",
            f"Exported placement zones across {len(selected_floors)} "
            f"floor sheet(s) to:\n\n{output_path}",
        )

    def export_equipment_room_extents_pdf(self):
        rooms = [
            row
            for row in self.store.data.get("locations", [])
            if isinstance(row, dict)
            and str(row.get("kind", "")).strip()
            in {"comms_room", "distributed_equipment_room"}
        ]
        if not rooms:
            QMessageBox.information(
                self,
                "Equipment Room Extents PDF",
                "No comms rooms or distributed equipment rooms are present.",
            )
            return

        options_dialog = EquipmentRoomExtentsPdfOptionsDialog(self)
        if options_dialog.exec() != QDialog.Accepted:
            return
        options = options_dialog.export_options()
        source_path = (
            getattr(self.store, "storage_path", "") or self.current_json_path or ""
        )
        base_path = Path(source_path) if source_path else Path("cable_routes.crsdb")
        initial = str(
            base_path.with_suffix("").with_name(
                base_path.stem + "_equipment_room_extents.pdf"
            )
        )
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Equipment Room Extents PDF",
            initial,
            "PDF files (*.pdf)",
        )
        if not path:
            return
        if not path.lower().endswith(".pdf"):
            path += ".pdf"

        try:
            payloads = []
            for room in sorted(
                rooms,
                key=lambda row: (
                    int(row.get("floor", 0) or 0),
                    str(row.get("name", "")),
                ),
            ):
                payloads.append(
                    self._equipment_room_graph_extent_overlay(
                        name=str(room.get("name", "")).strip(),
                        floor=int(room.get("floor", 0) or 0),
                        x=float(room.get("x", 0.0)),
                        y=float(room.get("y", 0.0)),
                        distance_limit=max(
                            0.1,
                            float(room.get("max_cable_length_m", 90.0) or 90.0),
                        ),
                        source_name=str(room.get("name", "")).strip(),
                        include_current=True,
                    )
                )
            from equipment_room_extents_pdf import export_equipment_room_extents_pdf

            layout_manifest = []
            output_path = self._export_pdf_through_report_studio(
                path,
                lambda preview_path: (
                    export_equipment_room_extents_pdf(
                        self.store.data,
                        payloads,
                        preview_path,
                        source_path=source_path,
                        paper_size=options["paper_size"],
                        scale=options["scale"],
                        revision_number=self.latest_project_revision_number(),
                        layout_manifest=layout_manifest,
                        preview_background=True,
                    ),
                    layout_manifest,
                ),
                settings_key="equipment_room_extents",
                report_title="Equipment Room Extents Report Studio",
            )
        except ImportError as exc:
            QMessageBox.critical(
                self,
                "Equipment Room Extents PDF failed",
                f"PDF export requires reportlab.\n\n{exc}",
            )
            return
        except Exception as exc:
            QMessageBox.critical(
                self, "Equipment Room Extents PDF failed", str(exc)
            )
            return
        if not output_path:
            return
        self.store.data["equipment_room_extents_pdf_settings"] = dict(options)
        self.set_status(
            f"Exported equipment room extents PDF: {Path(output_path).name}"
        )
        QMessageBox.information(
            self,
            "Equipment Room Extents PDF complete",
            f"Exported {len(payloads)} room extent sheet(s) to:\n\n{output_path}",
        )

    def export_project_summary_pdf(self):
        options = ProjectSummaryPdfOptionsDialog(self)
        if options.exec() != QDialog.Accepted:
            return
        selected_sections = options.selected_sections()
        if not selected_sections:
            return
        report_options = options.report_options()

        project_name = str(
            report_options.get(
                "project_name",
                self.store.data.get("project", {}).get("name", "cable_routing_project"),
            )
            or "cable_routing_project"
        ).strip()
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", project_name).strip("_")
        if not safe_name:
            safe_name = "cable_routing_project"
        if self.current_json_path:
            initial = str(Path(self.current_json_path).with_suffix(""))
            initial = str(Path(initial).with_name(Path(initial).name + "_project_summary.pdf"))
        else:
            initial = f"{safe_name}_project_summary.pdf"

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Project Summary PDF",
            initial,
            "PDF files (*.pdf)",
        )
        if not path:
            return
        if not path.lower().endswith(".pdf"):
            path += ".pdf"

        try:
            from project_summary_report import export_project_summary_pdf

            output_path = self._export_pdf_through_report_studio(
                path,
                lambda preview_path: export_project_summary_pdf(
                    self.store.data,
                    preview_path,
                    source_path=self.current_json_path
                    or getattr(self.store, "storage_path", "")
                    or "",
                    sections=selected_sections,
                    report_options=report_options,
                ),
                settings_key="project_summary",
                report_title="Project Summary Report Studio",
            )
        except ImportError as exc:
            QMessageBox.critical(
                self,
                "Project PDF failed",
                f"PDF export requires reportlab. Install project requirements and try again.\n\n{exc}",
            )
            return
        except Exception as exc:
            QMessageBox.critical(self, "Project PDF failed", str(exc))
            return
        if not output_path:
            return
        self.set_status(f"Exported project summary PDF: {Path(output_path).name}")
        QMessageBox.information(
            self,
            "Project PDF complete",
            f"Project summary PDF written to:\n\n{output_path}",
        )

    def save_json(self):
        path = self.current_json_path
        if not path or Path(path).suffix.lower() == ".json":
            initial = str(Path(path).with_suffix(".crsdb")) if path else ""
            path = self._project_save_path("Save Cable Routing Project", initial)
        if not path:
            return
        try:
            self._save_project_to_path(path)
        except Exception as exc:
            QMessageBox.critical(self, "Save project failed", str(exc))

    def save_json_as(self):
        initial = self.current_json_path or ""
        if initial:
            initial = str(Path(initial).with_suffix(".crsdb"))
        path = self._project_save_path("Save Cable Routing Project As", initial)
        if not path:
            return
        try:
            self._save_project_to_path(path, "Saved as")
        except Exception as exc:
            QMessageBox.critical(self, "Save project failed", str(exc))

    def export_json(self):
        initial = (
            str(Path(self.current_json_path).with_suffix(".json"))
            if self.current_json_path
            else "cable_routing_project.json"
        )
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Project JSON", initial, "JSON files (*.json)"
        )
        if not path:
            return
        if not path.lower().endswith(".json"):
            path += ".json"
        try:
            self.store.save(path)
            self.set_status(f"Exported {Path(path).name}")
        except Exception as exc:
            QMessageBox.critical(self, "JSON export failed", str(exc))

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
        if hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()
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
        if hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()
        self.refresh_canvas()

    def remove_invalid_connections_and_routes(self):
        invalid = self.store.invalid_connections_and_routes()
        invalid_connections = invalid["connections"]
        invalid_routes = invalid["routes"]
        connection_count = len(invalid_connections)
        route_count = len(invalid_routes)
        if not connection_count and not route_count:
            QMessageBox.information(
                self,
                "Remove Invalid Connections and Routes",
                "No invalid connections or routing edges were found.",
            )
            self.set_status("No invalid connections or routes found")
            return

        examples = []
        for row in invalid_connections[:4]:
            examples.append(f"Connection {row['id']}: {row['reason']}")
        for row in invalid_routes[:4]:
            label = f"{row['from']} -> {row['to']}".strip(" ->") or "unnamed route"
            examples.append(f"Route {label}: {row['reason']}")
        detail = "\n".join(f"• {value}" for value in examples)
        if connection_count + route_count > len(examples):
            detail += f"\n• …and {connection_count + route_count - len(examples)} more"
        message = (
            f"Remove {connection_count} invalid connection(s) and "
            f"{route_count} invalid routing edge(s)?\n\n"
            "Only those connection and route records will be removed. Points, "
            "rooms, transitions and valid graph edges will not be changed."
        )
        if detail:
            message += f"\n\n{detail}"
        if (
            QMessageBox.question(
                self,
                "Remove Invalid Connections and Routes",
                message,
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            != QMessageBox.Yes
        ):
            return

        self.push_undo_state("Remove invalid connections and routes")
        removed = self.store.remove_invalid_connections_and_routes()
        removed_connections = len(removed["connections"])
        removed_routes = len(removed["routes"])
        self.refresh_canvas()
        self.set_status(
            f"Removed {removed_connections} invalid connection(s) and "
            f"{removed_routes} invalid route(s)"
        )
        QMessageBox.information(
            self,
            "Invalid Connections and Routes Removed",
            f"Removed {removed_connections} invalid connection(s) and "
            f"{removed_routes} invalid routing edge(s).",
        )

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
                self._mark_routing_graph_changed()
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
            ("cabinet_type", "Cabinet type", 150),
            ("max_network_cabinets", "Maximum cabinets", 130),
            ("department_ids", "Department IDs", 180),
            ("floor", "Floor", 70),
            ("x", "X", 80),
            ("y", "Y", 80),
        ]
        LocationsTableEditor(
            self,
            "Locations",
            columns,
            self.store.data.get("locations", []),
            self._save_locations,
        )

    def _next_placement_zone_id(self):
        used = {
            str(zone.get("id", "")).strip()
            for zone in self.store.data.get("equipment_room_placement_zones", [])
            if isinstance(zone, dict)
        }
        number = 1
        while f"ZONE-{number}" in used:
            number += 1
        return f"ZONE-{number}"

    def _build_demand_based_zone_suggestions(
        self, options, progress_callback=None
    ):
        """Return corridor-centred zones that cover all selected unconnected demand."""
        options = dict(options or {})
        max_distance_m = max(0.1, float(options.get("max_distance_m", 90.0)))
        scope_floor = int(options.get("scope_floor", self.floor_spin.value()))
        current_floor_only = bool(options.get("current_floor_only", False))
        zone_width = max(0.5, float(options.get("zone_width_m", 4.0)))
        zone_depth = max(0.5, float(options.get("zone_depth_m", 4.0)))
        comms_capacity = max(
            1, int(options.get("comms_room_port_capacity", 1) or 1)
        )
        der_capacity = max(1, int(options.get("der_port_capacity", 1) or 1))

        connected_targets = self._existing_connection_targets()
        demands_by_floor = {}
        for source in self.store.data.get("data_points", []):
            if not isinstance(source, dict):
                continue
            name = str(source.get("name", "") or "").strip()
            floor = int(source.get("floor", 0) or 0)
            if (
                not name
                or name in connected_targets
                or (current_floor_only and floor != scope_floor)
            ):
                continue
            required_ports = (
                self.store.data_point_required_port_count(source)
                if hasattr(self.store, "data_point_required_port_count")
                else max(0, int(source.get("qty", 1) or 0))
            )
            if int(required_ports) <= 0:
                continue
            point = dict(source)
            point["_required_ports"] = int(required_ports)
            demands_by_floor.setdefault(floor, []).append(point)

        if not demands_by_floor:
            return {
                "zones": [],
                "uncovered": [],
                "considered_points": 0,
                "considered_ports": 0,
            }

        routing_graph, routing_points = self._build_routing_graph()
        anchors_by_floor = {}
        for name, point in routing_points.items():
            if (
                str(point.get("kind", "") or "").strip() != "corridor_node"
                or bool(point.get("restricted", False))
                or not routing_graph.get(name)
            ):
                continue
            anchors_by_floor.setdefault(int(point.get("floor", 0) or 0), []).append(
                name
            )

        total_candidates = sum(
            len(anchors_by_floor.get(floor, [])) for floor in demands_by_floor
        )
        progress_step = 0

        def report(message):
            if progress_callback is None:
                return True
            return bool(progress_callback(progress_step, total_candidates, message))

        def floor_distances(start_name, floor):
            pending = [(0.0, start_name)]
            best = {start_name: 0.0}
            while pending:
                cost, name = heapq.heappop(pending)
                if cost > best.get(name, math.inf):
                    continue
                if cost > max_distance_m:
                    continue
                for neighbour, weight in routing_graph.get(name, []):
                    if int(routing_points[neighbour].get("floor", 0) or 0) != floor:
                        continue
                    candidate = cost + float(weight)
                    if (
                        candidate <= max_distance_m
                        and candidate < best.get(neighbour, math.inf)
                    ):
                        best[neighbour] = candidate
                        heapq.heappush(pending, (candidate, neighbour))
            return best

        proposed = []
        uncovered = []
        considered_points = 0
        considered_ports = 0
        for floor in sorted(demands_by_floor):
            floor_demands = demands_by_floor[floor]
            considered_points += len(floor_demands)
            considered_ports += sum(
                int(point.get("_required_ports", 0) or 0)
                for point in floor_demands
            )
            anchors = anchors_by_floor.get(floor, [])
            if not anchors:
                uncovered.extend(
                    {
                        "name": str(point.get("name", "") or ""),
                        "floor": floor,
                        "ports": int(point.get("_required_ports", 0) or 0),
                        "reason": "No unrestricted corridor node is available on this floor.",
                    }
                    for point in floor_demands
                )
                continue

            endpoint_by_name = {}
            for point in floor_demands:
                point_name = str(point.get("name", "") or "").strip()
                extension = max(
                    0.0, float(point.get("extension_distance_m", 0.0) or 0.0)
                )
                if point_name in routing_graph and routing_graph.get(point_name):
                    endpoint_by_name[point_name] = (point_name, extension)
                    continue
                nearest = min(
                    anchors,
                    key=lambda anchor_name: math.hypot(
                        float(routing_points[anchor_name].get("x", 0.0))
                        - float(point.get("x", 0.0)),
                        float(routing_points[anchor_name].get("y", 0.0))
                        - float(point.get("y", 0.0)),
                    ),
                )
                spur = math.hypot(
                    float(routing_points[nearest].get("x", 0.0))
                    - float(point.get("x", 0.0)),
                    float(routing_points[nearest].get("y", 0.0))
                    - float(point.get("y", 0.0)),
                )
                endpoint_by_name[point_name] = (nearest, spur + extension)

            coverage = {}
            for anchor_name in anchors:
                progress_step += 1
                if not report(
                    f"Floor {floor}: assessing corridor position {progress_step} "
                    f"of {max(1, total_candidates)}"
                ):
                    raise InterruptedError("Zone suggestion cancelled")
                distances = floor_distances(anchor_name, floor)
                covered = {}
                for point in floor_demands:
                    point_name = str(point.get("name", "") or "").strip()
                    endpoint, spur = endpoint_by_name[point_name]
                    route_distance = distances.get(endpoint)
                    if route_distance is None:
                        continue
                    cable_length = float(route_distance) + float(spur)
                    if cable_length <= max_distance_m:
                        covered[point_name] = cable_length
                if covered:
                    coverage[anchor_name] = covered

            demand_by_name = {
                str(point.get("name", "") or "").strip(): point
                for point in floor_demands
            }
            remaining = set(demand_by_name)
            selected = []
            while remaining:
                best_anchor = None
                best_key = None
                for anchor_name, covered in coverage.items():
                    names = remaining.intersection(covered)
                    if not names:
                        continue
                    port_count = sum(
                        int(demand_by_name[name].get("_required_ports", 0) or 0)
                        for name in names
                    )
                    weighted_cable = sum(
                        float(covered[name])
                        * int(demand_by_name[name].get("_required_ports", 0) or 0)
                        for name in names
                    )
                    key = (port_count, len(names), -weighted_cable)
                    if best_key is None or key > best_key:
                        best_key = key
                        best_anchor = anchor_name
                if best_anchor is None:
                    break
                selected.append(best_anchor)
                remaining.difference_update(coverage[best_anchor])

            for point_name in sorted(remaining):
                point = demand_by_name[point_name]
                uncovered.append(
                    {
                        "name": point_name,
                        "floor": floor,
                        "ports": int(point.get("_required_ports", 0) or 0),
                        "reason": (
                            f"No corridor position can serve this demand within "
                            f"{max_distance_m:.2f} m."
                        ),
                    }
                )

            assignments = {anchor_name: [] for anchor_name in selected}
            for point_name, point in demand_by_name.items():
                choices = [
                    (coverage[anchor_name][point_name], anchor_name)
                    for anchor_name in selected
                    if point_name in coverage.get(anchor_name, {})
                ]
                if choices:
                    _distance, anchor_name = min(choices)
                    assignments[anchor_name].append(point)

            for floor_index, anchor_name in enumerate(selected, start=1):
                members = assignments.get(anchor_name, [])
                if not members:
                    continue
                required_ports = sum(
                    int(point.get("_required_ports", 0) or 0) for point in members
                )
                longest = max(
                    coverage[anchor_name][str(point.get("name", "") or "").strip()]
                    for point in members
                )
                anchor = routing_points[anchor_name]
                x = float(anchor.get("x", 0.0) or 0.0)
                y = float(anchor.get("y", 0.0) or 0.0)
                proposed.append(
                    {
                        "name": f"Suggested Zone F{floor}-{floor_index}",
                        "floor": floor,
                        "min_x": round(x - zone_width / 2.0, 3),
                        "max_x": round(x + zone_width / 2.0, 3),
                        "min_y": round(y - zone_depth / 2.0, 3),
                        "max_y": round(y + zone_depth / 2.0, 3),
                        "allow_comms_room": True,
                        "allow_distributed_equipment_room": True,
                        "max_comms_rooms": max(
                            1, int(math.ceil(required_ports / comms_capacity))
                        ),
                        "max_distributed_equipment_rooms": max(
                            1, int(math.ceil(required_ports / der_capacity))
                        ),
                        "required_ports": int(required_ports),
                        "covered_data_point_count": len(members),
                        "longest_cable_length_m": round(float(longest), 2),
                        "suggested_anchor_name": anchor_name,
                        "auto_suggested": True,
                    }
                )

        used_ids = {
            str(zone.get("id", "") or "").strip()
            for zone in self.store.data.get("equipment_room_placement_zones", [])
            if isinstance(zone, dict)
        }
        next_number = 1
        for zone in proposed:
            while f"ZONE-{next_number}" in used_ids:
                next_number += 1
            zone["id"] = f"ZONE-{next_number}"
            used_ids.add(zone["id"])
            next_number += 1

        return {
            "zones": proposed,
            "uncovered": uncovered,
            "considered_points": considered_points,
            "considered_ports": considered_ports,
        }

    def suggest_equipment_room_placement_zones(self):
        if not self.store.data.get("data_points"):
            QMessageBox.information(
                self,
                "Suggest Placement Zones",
                "No data points are available to assess.",
            )
            return
        switch_capacity = (
            self.store.access_switch_capacity_profile()
            if hasattr(self.store, "access_switch_capacity_profile")
            else {"ports": 48, "switches_per_full_cabinet": 42}
        )
        dialog = SuggestPlacementZonesDialog(
            self,
            current_floor=self.floor_spin.value(),
            switch_capacity=switch_capacity,
        )
        if dialog.exec() != QDialog.Accepted or not dialog.result:
            return

        progress = QProgressDialog(
            "Preparing zone suggestions...", "Cancel", 0, 0, self
        )
        progress.setWindowTitle("Suggesting Placement Zones")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.show()

        def update_progress(current, total, message):
            progress.setRange(0, max(1, int(total)))
            progress.setValue(min(int(current), max(1, int(total))))
            progress.setLabelText(str(message))
            QApplication.processEvents()
            return not progress.wasCanceled()

        try:
            result = self._build_demand_based_zone_suggestions(
                dialog.result, progress_callback=update_progress
            )
        except InterruptedError:
            self.set_status("Placement-zone suggestion cancelled")
            return
        finally:
            progress.close()

        if result["uncovered"]:
            examples = "\n".join(
                f"- {row['name']} (floor {row['floor']}, {row['ports']} ports): "
                f"{row['reason']}"
                for row in result["uncovered"][:12]
            )
            extra = max(0, len(result["uncovered"]) - 12)
            QMessageBox.warning(
                self,
                "No satisfactory zone set",
                "The proposed zones were not applied because some demand cannot "
                "be served under the selected cable limit.\n\n"
                + examples
                + (f"\n... and {extra} more" if extra else ""),
            )
            return
        if not result["zones"]:
            QMessageBox.information(
                self,
                "Suggest Placement Zones",
                "All positive port demand is already connected; no new zones are required.",
            )
            return

        existing = deepcopy(
            self.store.data.get("equipment_room_placement_zones", [])
        )
        if dialog.result.get("replace_previous", True):
            existing = [zone for zone in existing if not zone.get("auto_suggested")]
        combined = existing + result["zones"]
        columns = [
            ("id", "Zone ID", 100),
            ("name", "Name", 170),
            ("floor", "Floor", 60),
            ("required_ports", "Required ports", 105),
            ("covered_data_point_count", "Data points", 90),
            ("longest_cable_length_m", "Longest cable", 105),
            ("max_comms_rooms", "Max comms", 90),
            ("max_distributed_equipment_rooms", "Max DERs", 80),
            ("min_x", "Minimum X", 85),
            ("min_y", "Minimum Y", 85),
            ("max_x", "Maximum X", 85),
            ("max_y", "Maximum Y", 85),
        ]
        PlacementZonesTableEditor(
            self,
            (
                f"Review Suggested Placement Zones - {result['considered_ports']} "
                f"ports across {result['considered_points']} data points"
            ),
            columns,
            combined,
            self._save_equipment_room_placement_zones,
        )
        self.set_status(
            f"Prepared {len(result['zones'])} satisfactory placement-zone suggestion(s)"
        )

    def manage_equipment_room_placement_zones(self):
        columns = [
            ("id", "Zone ID", 110),
            ("name", "Name", 180),
            ("floor", "Floor", 70),
            ("min_x", "Minimum X", 95),
            ("min_y", "Minimum Y", 95),
            ("max_x", "Maximum X", 95),
            ("max_y", "Maximum Y", 95),
            ("allow_comms_room", "Comms rooms", 105),
            (
                "allow_distributed_equipment_room",
                "Distributed rooms",
                130,
            ),
            ("max_comms_rooms", "Maximum comms rooms", 130),
            (
                "max_distributed_equipment_rooms",
                "Maximum DERs",
                105,
            ),
            ("required_ports", "Required ports", 100),
        ]
        PlacementZonesTableEditor(
            self,
            "Equipment Room Placement Zones",
            columns,
            deepcopy(
                self.store.data.get("equipment_room_placement_zones", [])
            ),
            self._save_equipment_room_placement_zones,
        )

    def _save_equipment_room_placement_zones(self, zones):
        self.push_undo_state("Update equipment room placement zones")
        self.store.data["equipment_room_placement_zones"] = deepcopy(zones)
        self.placement_zone_start = None
        self._clear_placement_zone_preview()
        self.selected_placement_zone_id = None
        self._invalidate_static_scene_cache()
        self.refresh_canvas()
        self.set_status(f"Saved {len(zones)} equipment room placement zone(s)")

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

        asset_categories_by_id = {
            str(category.get("id", ""))
            .strip(): str(category.get("name", category.get("id", "")))
            .strip()
            for category in self.store.data.get("asset_categories", [])
            if str(category.get("id", "")).strip()
        }

        RoomTypesEditorWindow(
            self,
            self.store.data.get("room_types", []),
            self._save_room_types,
            asset_options=self.store.asset_options(),
            assets_by_id=assets_by_id,
            asset_categories_by_id=asset_categories_by_id,
            on_condense_room_types=self._condense_room_types,
        )

    def _room_type_editor_change_details(self, before, after):
        details = []
        before = before if isinstance(before, dict) else {}
        after = after if isinstance(after, dict) else {}
        ignored = {"id", "assets", "asset_ids"}
        labels = {"name": "name", "scenario_group": "scenario group"}
        for key in sorted((set(before) | set(after)) - ignored, key=str.casefold):
            old_value = before.get(key, "")
            new_value = after.get(key, "")
            if old_value == new_value:
                continue
            field = labels.get(key, str(key).replace("_", " "))
            old_text = str(old_value).strip() or "(blank)"
            new_text = str(new_value).strip() or "(blank)"
            details.append(f"changed {field} from '{old_text}' to '{new_text}'")
        details.extend(
            self._room_assignment_change_details(
                self._room_type_asset_quantities(before),
                self._room_type_asset_quantities(after),
                before_requesters=self._room_type_asset_requesters(before),
                after_requesters=self._room_type_asset_requesters(after),
            )
        )
        return details

    def _save_room_types(self, items):
        before_items = [
            deepcopy(item)
            for item in self.store.data.get("room_types", []) or []
            if isinstance(item, dict)
        ]
        self.push_undo_state("Save room types")
        self.store.data["room_types"] = items
        valid_room_type_ids = {str(room_type.get("id", "") or "").strip() for room_type in items if str(room_type.get("id", "") or "").strip()}
        review_state = self.store.data.get("room_type_asset_review", {})
        if isinstance(review_state, dict):
            self.store.data["room_type_asset_review"] = {
                str(room_type_id).strip(): dict(record)
                for room_type_id, record in review_state.items()
                if str(room_type_id).strip() in valid_room_type_ids
                and isinstance(record, dict)
            }
        for group in self.store.data.get("room_type_scenario_groups", []) or []:
            if isinstance(group, dict):
                group["room_type_ids"] = [
                    str(room_type_id).strip()
                    for room_type_id in group.get("room_type_ids", []) or []
                    if str(room_type_id).strip() in valid_room_type_ids
                ]

        self.store.sync_all_room_type_quantities()

        before_by_id = {
            str(item.get("id", "") or "").strip(): item
            for item in before_items
            if str(item.get("id", "") or "").strip()
        }
        after_by_id = {
            str(item.get("id", "") or "").strip(): item
            for item in items
            if isinstance(item, dict) and str(item.get("id", "") or "").strip()
        }
        for room_type_id in sorted(set(before_by_id) | set(after_by_id), key=str.casefold):
            before = before_by_id.get(room_type_id)
            after = after_by_id.get(room_type_id)
            if before is None:
                details = ["added room type"]
                details.extend(
                    self._room_assignment_change_details(
                        {},
                        self._room_type_asset_quantities(after),
                        before_requesters={},
                        after_requesters=self._room_type_asset_requesters(after),
                    )
                )
                action = "room_type_added"
                subject = after
            elif after is None:
                details = ["deleted room type"]
                details.extend(
                    self._room_assignment_change_details(
                        self._room_type_asset_quantities(before),
                        {},
                        before_requesters=self._room_type_asset_requesters(before),
                        after_requesters={},
                    )
                )
                action = "room_type_deleted"
                subject = before
            else:
                details = self._room_type_editor_change_details(before, after)
                action = "room_type_modified"
                subject = after
            if not details:
                continue
            room_type_name = str(subject.get("name", "") or "").strip()
            self._record_room_type_change(
                "Room Type Editor",
                room_type_id,
                room_type_name,
                details,
            )
            self._append_room_type_audit_history(
                action,
                room_type_id,
                room_type_name,
                details,
            )

        self.set_status("Room types updated and data point quantities recalculated")
        self.refresh_canvas()

    def _condense_room_types(
        self, items, main_room_type_id, condensed_room_type_ids, reason
    ):
        reason = str(reason or "").strip()
        if not reason:
            raise ValueError("A database commit note is required.")
        staging = self.store.data.get("room_type_asset_staging", {})
        if isinstance(staging, dict) and staging.get("changes"):
            raise ValueError(
                "Commit or reset the staged room-type asset changes before "
                "condensing room types."
            )

        updated_data = deepcopy(self.store.data)
        updated_data["room_types"] = deepcopy(items)
        result = apply_room_type_condensation(
            updated_data,
            main_room_type_id,
            condensed_room_type_ids,
            reason,
        )
        self.push_undo_state("Condense room types")
        self.store.data = updated_data
        self.store.sync_all_room_type_quantities()

        removed_labels = []
        for room_type in result["removed_room_types"]:
            room_type_id = str(room_type.get("id", "") or "").strip()
            name = str(room_type.get("name", room_type_id) or room_type_id).strip()
            removed_labels.append(
                f"{room_type_id} ({name})"
                if name and name != room_type_id
                else room_type_id
            )
        main_label = result["main_room_type_id"]
        if result["main_room_type_name"] != main_label:
            main_label += f" ({result['main_room_type_name']})"
        details = [
            f"reassigned {count} placed room(s)/data point(s) from "
            f"{room_type_id} to {result['main_room_type_id']}"
            for room_type_id, count in result["placement_counts"].items()
        ]
        details.extend(
            f"raised {rfi['id']} to verify the retained room type asset and port count"
            for rfi in result["created_rfis"]
        )
        self.store.record_revision_change(
            "Room Type Condensation",
            f"Condensed {', '.join(removed_labels)} into {main_label}. Reason: {reason}",
            room_type_id=result["main_room_type_id"],
            details=details,
        )
        self.set_status(
            f"Condensed {len(removed_labels)} room type(s) into "
            f"{result['main_room_type_id']}; reassigned "
            f"{sum(result['placement_counts'].values())} placement(s) and raised "
            f"{len(result['created_rfis'])} RFI(s)"
        )
        self.refresh_canvas()
        return {
            "placement_count": sum(result["placement_counts"].values()),
            "rfi_count": len(result["created_rfis"]),
        }

    def manage_room_type_scenario_groups(self):
        room_options = [
            (
                str(room_type.get("id", "") or "").strip(),
                str(room_type.get("name", room_type.get("id", "")) or "").strip(),
            )
            for room_type in self.store.data.get("room_types", []) or []
            if str(room_type.get("id", "") or "").strip()
        ]
        if not room_options:
            QMessageBox.information(
                self,
                "Room Scenario Groups",
                "Create room types before defining room scenario groups.",
            )
            return

        dialog = ScenarioGroupManagerDialog(
            self,
            "Room Scenario Groups",
            self.store.data.get("room_type_scenario_groups", []),
            room_options,
            "room_type_ids",
            "Room type",
        )
        if dialog.exec() == QDialog.Accepted and dialog.result is not None:
            self.push_undo_state("Save room scenario groups")
            self.store.data["room_type_scenario_groups"] = dialog.result
            self.set_status(f"Saved {len(dialog.result)} room scenario group(s)")
            self.refresh_rhs_search_sidebar()

    def manage_asset_scenario_groups(self):
        asset_options = [
            (
                str(asset.get("id", "") or "").strip(),
                str(asset.get("name", asset.get("id", "")) or "").strip(),
            )
            for asset in self.store.data.get("assets", []) or []
            if str(asset.get("id", "") or "").strip()
        ]
        if not asset_options:
            QMessageBox.information(
                self,
                "Asset Scenario Groups",
                "Create endpoint assets before defining asset scenario groups.",
            )
            return

        dialog = ScenarioGroupManagerDialog(
            self,
            "Asset Scenario Groups",
            self.store.data.get("asset_scenario_groups", []),
            asset_options,
            "asset_ids",
            "Asset",
        )
        if dialog.exec() == QDialog.Accepted and dialog.result is not None:
            self.push_undo_state("Save asset scenario groups")
            self.store.data["asset_scenario_groups"] = dialog.result
            self.set_status(f"Saved {len(dialog.result)} asset scenario group(s)")
            self.refresh_rhs_search_sidebar()

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
        DataPointsTableEditor(
            self,
            "Data Points",
            columns,
            self.store.data.get("data_points", []),
            self._save_data_points,
        )

    def _save_data_points(self, items):
        self.push_undo_state("Save data points")
        old_names = {
            str(point.get("name", "")).strip()
            for point in self.store.data.get("data_points", [])
            if str(point.get("name", "")).strip()
        }
        clean_items = []
        for source in items:
            point = dict(source)
            point["name"] = str(point.get("name", "")).strip()
            point["qty"] = max(1, int(point.get("qty", 1) or 1))
            point["extension_distance_m"] = max(
                0.0,
                float(point.get("extension_distance_m", 0.0) or 0.0),
            )
            point["department_ids"] = [
                str(value).strip()
                for value in point.get("department_ids", [])
                if str(value).strip()
            ]
            point["room_type_id"] = str(
                point.get("room_type_id", "") or ""
            ).strip()
            if point["room_type_id"]:
                point["qty"] = self.store.room_type_cable_qty(
                    point["room_type_id"]
                )
            clean_items.append(point)

        new_names = {
            point["name"] for point in clean_items if point.get("name")
        }
        for removed_name in sorted(old_names - new_names):
            self.store.delete_point(removed_name)
        self.store.data["data_points"] = clean_items

        for point in self.store.data.get("data_points", []):
            name = str(point.get("name", "")).strip()
            if name:
                self.store.sync_connection_qty_for_data_point(name)

        self.set_status(f"Updated {len(clean_items)} data point(s)")
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

    def _connection_port_estimate_for_data_points(
        self, data_point_names, exclude_connected=True
    ):
        records = {
            str(item.get("name", "") or "").strip(): item
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "") or "").strip()
        }
        connected = (
            self._existing_connection_targets() if exclude_connected else set()
        )
        eligible_names = [
            str(name)
            for name in data_point_names
            if str(name) in records and str(name) not in connected
        ]
        quantities = {
            name: max(
                0,
                int(self.store.data_point_required_port_count(records[name]) or 0),
            )
            for name in eligible_names
        }
        return eligible_names, sum(quantities.values()), quantities

    def _similar_data_point_seed_names(self, picked=None):
        """Return the data points that define a Select Similar search.

        When the right-clicked data point is already part of the current
        multi-selection, every selected data point contributes criteria. When
        it is not selected, the clicked data point is used on its own so an
        unrelated previous selection cannot broaden the search unexpectedly.
        """
        picked = str(picked or "").strip()
        selected = self._selected_data_point_names()

        data_point_names = {
            str(item.get("name", "")).strip()
            for item in self.store.data.get("data_points", [])
            if str(item.get("name", "")).strip()
        }

        if picked and picked in data_point_names:
            if picked in selected:
                return selected
            return [picked]

        return selected

    def _matching_data_point_names_for_similarity(self, seed_names, floor=None):
        """Find visible data points sharing a seed room-type/department pair.

        A multi-selection expands each room type across every department paired
        with that room type in the seed selection. Pairing is retained per room
        type, preventing a selection containing different room types and
        departments from creating an unintended room-type/department
        cross-product.
        """
        seed_names = {
            str(name).strip() for name in (seed_names or []) if str(name).strip()
        }
        if not seed_names:
            return []

        if floor is None:
            floor = int(self.floor_spin.value())
        else:
            floor = int(floor)

        records_by_name = {
            str(item.get("name", "")).strip(): item
            for item in self.store.data.get("data_points", [])
            if isinstance(item, dict) and str(item.get("name", "")).strip()
        }

        criteria_by_room_type = {}
        for name in seed_names:
            point = records_by_name.get(name)
            if point is None:
                continue

            room_type_id = str(point.get("room_type_id", "") or "").strip()
            department_ids = set(self._data_point_department_ids(point))
            if not department_ids:
                department_ids = {""}

            criteria_by_room_type.setdefault(room_type_id, set()).update(
                department_ids
            )

        if not criteria_by_room_type:
            return []

        eligible_names = self._eligible_template_name_set(floor)
        matches = []

        for point in self.store.data.get("data_points", []):
            if not isinstance(point, dict):
                continue

            name = str(point.get("name", "")).strip()
            if not name or name not in eligible_names:
                continue

            try:
                point_floor = int(point.get("floor", 0))
            except (TypeError, ValueError):
                point_floor = 0
            if point_floor != floor:
                continue

            room_type_id = str(point.get("room_type_id", "") or "").strip()
            allowed_departments = criteria_by_room_type.get(room_type_id)
            if allowed_departments is None:
                continue

            department_ids = set(self._data_point_department_ids(point))
            if not department_ids:
                department_ids = {""}

            if department_ids & allowed_departments:
                matches.append(name)

        return sorted(set(matches))

    def select_similar_data_points(self, picked=None):
        """Select current-floor data points with matching room/dept criteria."""
        seed_names = self._similar_data_point_seed_names(picked)
        if not seed_names:
            self.set_status("Select Similar requires at least one data point")
            return

        matches = self._matching_data_point_names_for_similarity(seed_names)
        if not matches:
            self.set_status(
                "No visible data points match the selected room type and department"
            )
            return

        self._set_canvas_multi_selection(matches, append=False)
        self.refresh_canvas()

        seed_lookup = {
            str(item.get("name", "")).strip(): item
            for item in self.store.data.get("data_points", [])
            if isinstance(item, dict) and str(item.get("name", "")).strip()
        }
        room_types = set()
        departments = set()
        includes_unassigned_department = False

        for name in seed_names:
            point = seed_lookup.get(name)
            if point is None:
                continue
            room_types.add(str(point.get("room_type_id", "") or "").strip())
            point_departments = set(self._data_point_department_ids(point))
            if point_departments:
                departments.update(point_departments)
            else:
                includes_unassigned_department = True

        department_scope_count = len(departments) + int(includes_unassigned_department)
        self.set_status(
            f"Selected {len(matches)} similar data point(s) on floor "
            f"{self.floor_spin.value()} using {len(room_types)} room type(s) and "
            f"{department_scope_count} department scope(s)"
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

        dialog = QDialog(self)
        dialog.setWindowTitle("Assign Room Type")

        layout = QVBoxLayout(dialog)
        form = QFormLayout()
        layout.addLayout(form)

        room_type_combo = QComboBox()
        room_type_combo.setEditable(True)
        room_type_combo.setInsertPolicy(QComboBox.NoInsert)
        room_type_combo.setMaxVisibleItems(20)

        room_type_combo.addItem("Manual / no room type", "")

        for room_type_id, room_type_name in room_types:
            room_type_id = str(room_type_id).strip()
            room_type_name = str(room_type_name).strip()
            label = (
                f"{room_type_id} - {room_type_name}" if room_type_name else room_type_id
            )
            room_type_combo.addItem(label, room_type_id)

        completer = QCompleter(room_type_combo)
        completer.setModel(room_type_combo.model())
        completer.setCompletionMode(QCompleter.PopupCompletion)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        room_type_combo.setCompleter(completer)

        form.addRow(
            f"Room type for {len(selected)} selected data point(s)",
            room_type_combo,
        )

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        def selected_room_type_id():
            text = room_type_combo.currentText().strip()

            if not text or text == "Manual / no room type":
                return ""

            current_data = room_type_combo.currentData()
            if current_data is not None:
                return str(current_data).strip()

            for idx in range(room_type_combo.count()):
                label = room_type_combo.itemText(idx).strip()
                room_type_id = str(room_type_combo.itemData(idx) or "").strip()

                if text == label:
                    return room_type_id

                if room_type_id and text.lower() == room_type_id.lower():
                    return room_type_id

                if room_type_id and text.lower().startswith(
                    room_type_id.lower() + " -"
                ):
                    return room_type_id

            return ""

        if dialog.exec() != QDialog.Accepted:
            return

        room_type_id = selected_room_type_id()
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
                "max_cable_length_m": float(
                    dialog.result.get("max_cable_length_m", 90.0) or 90.0
                ),
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

    def _clear_placement_zone_preview(self):
        if hasattr(self, "canvas") and hasattr(
            self.canvas, "set_placement_zone_preview"
        ):
            self.canvas.set_placement_zone_preview(None)

    def _set_equipment_room_extent_overlay(self, overlay):
        if hasattr(self, "canvas") and hasattr(
            self.canvas, "set_equipment_room_extent_overlay"
        ):
            self.canvas.set_equipment_room_extent_overlay(overlay)

    def _clear_equipment_room_extent_overlay(self):
        self._set_equipment_room_extent_overlay(None)

    def _set_data_room_measurement_overlay(self, overlay):
        if hasattr(self, "canvas") and hasattr(
            self.canvas, "set_data_room_measurement_overlay"
        ):
            self.canvas.set_data_room_measurement_overlay(overlay)

    def _clear_data_room_measurement_overlay(self):
        self._set_data_room_measurement_overlay(None)

    def _data_room_measurement(self, data_point_name, room_name):
        data_point_name = str(data_point_name or "").strip()
        room_name = str(room_name or "").strip()
        points = self.store.all_points()
        data_point = points.get(data_point_name)
        room = points.get(room_name)
        if not data_point or not room:
            return None

        graph, points = self._build_routing_graph()
        routing_anchors = self._routing_anchor_names()

        def endpoint_candidates(name, record):
            if graph.get(name):
                return [(name, 0.0)]
            result = []
            endpoint_floor = int(record.get("floor", 0))
            for anchor_name in routing_anchors:
                anchor = points.get(anchor_name)
                if not anchor or int(anchor.get("floor", 0)) != endpoint_floor:
                    continue
                result.append(
                    (
                        anchor_name,
                        math.hypot(
                            float(record.get("x", 0.0))
                            - float(anchor.get("x", 0.0)),
                            float(record.get("y", 0.0))
                            - float(anchor.get("y", 0.0)),
                        ),
                    )
                )
            return result

        point_candidates = endpoint_candidates(data_point_name, data_point)
        room_candidates = endpoint_candidates(room_name, room)
        best = None
        for point_anchor, point_spur in point_candidates:
            for room_anchor, room_spur in room_candidates:
                graph_distance, route_path = self._shortest_path_length(
                    graph, point_anchor, room_anchor
                )
                if graph_distance is None or not route_path:
                    continue
                routed_distance = (
                    float(point_spur)
                    + float(graph_distance)
                    + float(room_spur)
                )
                score = (routed_distance, float(point_spur) + float(room_spur))
                if best is None or score < best[0]:
                    best = (
                        score,
                        routed_distance,
                        route_path,
                        point_anchor,
                        room_anchor,
                    )
        if best is None:
            return None

        _score, routed_distance, route_path, point_anchor, room_anchor = best
        extension = max(
            0.0,
            float(data_point.get("extension_distance_m", 0.0) or 0.0),
        )
        total_distance = routed_distance + extension
        distance_limit = max(
            0.1, float(room.get("max_cable_length_m", 90.0) or 90.0)
        )

        ordered_names = [data_point_name]
        if point_anchor != data_point_name:
            ordered_names.append(point_anchor)
        ordered_names.extend(route_path)
        if room_anchor != room_name:
            ordered_names.append(room_name)
        elif not ordered_names or ordered_names[-1] != room_name:
            ordered_names.append(room_name)
        deduplicated_names = []
        for name in ordered_names:
            if not deduplicated_names or name != deduplicated_names[-1]:
                deduplicated_names.append(name)

        path_points = []
        for name in deduplicated_names:
            record = points.get(name)
            if not record:
                continue
            path_points.append(
                {
                    "name": name,
                    "floor": int(record.get("floor", 0)),
                    "x": float(record.get("x", 0.0)),
                    "y": float(record.get("y", 0.0)),
                }
            )

        return {
            "data_point_name": data_point_name,
            "room_name": room_name,
            "path_points": path_points,
            "routed_distance_m": float(routed_distance),
            "extension_distance_m": float(extension),
            "total_distance_m": float(total_distance),
            "distance_limit_m": float(distance_limit),
            "within_limit": bool(total_distance <= distance_limit + 1e-9),
        }

    def _nearest_measurement_target(self, x, y, floor, names):
        points = self.store.all_points()
        best_name = None
        best_distance = None
        radius = float(self._select_pick_radius())
        for name in names:
            point = points.get(name)
            if not point or int(point.get("floor", 0)) != int(floor):
                continue
            distance = math.hypot(
                float(point.get("x", 0.0)) - float(x),
                float(point.get("y", 0.0)) - float(y),
            )
            if distance <= radius and (
                best_distance is None or distance < best_distance
            ):
                best_name = name
                best_distance = distance
        return best_name

    def _handle_data_room_measurement_click(self, x, y, floor):
        if not self._measure_data_point_name:
            point_name = self._nearest_measurement_target(
                x, y, floor, set(self.data_point_names())
            )
            if not point_name:
                self.set_status("Measure mode: click a data point first")
                return
            self._measure_data_point_name = point_name
            self.selected_point_name = point_name
            self._clear_data_room_measurement_overlay()
            self.refresh_canvas()
            self.set_status(
                f"Measure start: {point_name}. Now click a comms room or DER"
            )
            return

        room_names = set(self.comms_room_names())
        for location in self.store.data.get("locations", []):
            name = str(location.get("name", "")).strip()
            if name.upper().startswith(("CR", "DER")):
                room_names.add(name)
        room_name = self._nearest_measurement_target(x, y, floor, room_names)
        if not room_name:
            self.set_status(
                f"Measure start: {self._measure_data_point_name}. "
                "Click a comms room or DER"
            )
            return

        point_name = self._measure_data_point_name
        result = self._data_room_measurement(point_name, room_name)
        if result is None:
            QMessageBox.warning(
                self,
                "No Graph Route",
                f"No routing-graph path connects {point_name} to {room_name}.",
            )
            self.set_status(
                f"No graph route from {point_name} to {room_name}; choose another room"
            )
            return

        self._measure_data_point_name = None
        self.selected_point_name = room_name
        self._set_data_room_measurement_overlay(result)
        self.refresh_canvas()
        difference = abs(
            result["distance_limit_m"] - result["total_distance_m"]
        )
        outcome = (
            f"within limit by {difference:.2f} m"
            if result["within_limit"]
            else f"exceeds limit by {difference:.2f} m"
        )
        self.set_status(
            f"{point_name} to {room_name}: {result['total_distance_m']:.2f} m "
            f"({outcome}). Click another data point to measure again"
        )

    @staticmethod
    def _clip_extent_polyline(points, distance_limit):
        points = [(float(x), float(y)) for x, y in points]
        remaining = max(0.0, float(distance_limit))
        if len(points) < 2 or remaining <= 0.0:
            return points[:1]
        result = [points[0]]
        for start, end in zip(points, points[1:]):
            length = math.hypot(end[0] - start[0], end[1] - start[1])
            if length <= 1e-9:
                continue
            if length <= remaining + 1e-9:
                result.append(end)
                remaining -= length
                continue
            ratio = remaining / length
            result.append(
                (
                    start[0] + (end[0] - start[0]) * ratio,
                    start[1] + (end[1] - start[1]) * ratio,
                )
            )
            break
        return result

    @staticmethod
    def _extent_boundary_polyline(polylines):
        points = sorted(
            {
                (round(float(point[0]), 6), round(float(point[1]), 6))
                for polyline in polylines
                for point in polyline
                if isinstance(point, (list, tuple)) and len(point) >= 2
            }
        )
        if len(points) < 3:
            return points

        def cross(origin, a, b):
            return (a[0] - origin[0]) * (b[1] - origin[1]) - (
                a[1] - origin[1]
            ) * (b[0] - origin[0])

        lower = []
        for point in points:
            while len(lower) >= 2 and cross(lower[-2], lower[-1], point) <= 0:
                lower.pop()
            lower.append(point)
        upper = []
        for point in reversed(points):
            while len(upper) >= 2 and cross(upper[-2], upper[-1], point) <= 0:
                upper.pop()
            upper.append(point)
        hull = lower[:-1] + upper[:-1]
        return hull + [hull[0]] if len(hull) >= 3 else hull

    def _equipment_room_graph_extent_overlay(
        self,
        *,
        name,
        floor,
        x,
        y,
        distance_limit,
        source_name="",
        include_current=False,
        preview=False,
    ):
        floor = int(floor)
        x = float(x)
        y = float(y)
        distance_limit = max(0.1, float(distance_limit))
        graph, points = self._build_routing_graph()

        source_name = str(source_name or "").strip()
        source_anchor = None
        source_spur = 0.0
        if source_name in graph and graph.get(source_name):
            source_anchor = source_name
        else:
            best = None
            for anchor_name in self._routing_anchor_names():
                anchor = points.get(anchor_name)
                if not anchor or int(anchor.get("floor", 0)) != floor:
                    continue
                distance = math.hypot(
                    float(anchor.get("x", 0.0)) - x,
                    float(anchor.get("y", 0.0)) - y,
                )
                candidate = (distance, anchor_name)
                if best is None or candidate < best:
                    best = candidate
            if best is not None:
                source_spur, source_anchor = best

        possible_polylines = []
        current_polylines = []
        current_max_target = ""
        current_max_distance = 0.0
        current_max_point = None
        distances = {}

        if source_anchor is not None:
            anchor = points[source_anchor]
            anchor_xy = (
                float(anchor.get("x", 0.0)),
                float(anchor.get("y", 0.0)),
            )
            if source_anchor != source_name or source_spur > 1e-9:
                clipped_spur = self._clip_extent_polyline(
                    [(x, y), anchor_xy], distance_limit
                )
                if len(clipped_spur) >= 2:
                    possible_polylines.append(clipped_spur)

            if source_spur <= distance_limit + 1e-9:
                distances[source_anchor] = source_spur
                heap = [(source_spur, source_anchor)]
                while heap:
                    cost, node = heapq.heappop(heap)
                    if cost > distance_limit + 1e-9:
                        continue
                    if cost > distances.get(node, float("inf")) + 1e-9:
                        continue
                    for next_node, weight in graph.get(node, []):
                        next_cost = cost + float(weight)
                        if next_cost < distances.get(next_node, float("inf")) - 1e-9:
                            distances[next_node] = next_cost
                            heapq.heappush(heap, (next_cost, next_node))

                for edge in self.store.data.get("corridors", {}).get("edges", []):
                    a_name = str(edge.get("from", "")).strip()
                    b_name = str(edge.get("to", "")).strip()
                    a = points.get(a_name)
                    b = points.get(b_name)
                    if not a or not b:
                        continue
                    if int(a.get("floor", 0)) != floor or int(b.get("floor", 0)) != floor:
                        continue
                    a_xy = (float(a.get("x", 0.0)), float(a.get("y", 0.0)))
                    b_xy = (float(b.get("x", 0.0)), float(b.get("y", 0.0)))
                    edge_length = math.hypot(b_xy[0] - a_xy[0], b_xy[1] - a_xy[1])
                    if edge_length <= 1e-9:
                        continue
                    da = distances.get(a_name, float("inf"))
                    db = distances.get(b_name, float("inf"))
                    reach_a = max(0.0, distance_limit - da)
                    reach_b = max(0.0, distance_limit - db)
                    if reach_a <= 0.0 and reach_b <= 0.0:
                        continue
                    if reach_a + reach_b >= edge_length - 1e-9:
                        possible_polylines.append([a_xy, b_xy])
                        continue
                    if reach_a > 0.0:
                        ratio = min(1.0, reach_a / edge_length)
                        possible_polylines.append(
                            [
                                a_xy,
                                (
                                    a_xy[0] + (b_xy[0] - a_xy[0]) * ratio,
                                    a_xy[1] + (b_xy[1] - a_xy[1]) * ratio,
                                ),
                            ]
                        )
                    if reach_b > 0.0:
                        ratio = min(1.0, reach_b / edge_length)
                        possible_polylines.append(
                            [
                                b_xy,
                                (
                                    b_xy[0] + (a_xy[0] - b_xy[0]) * ratio,
                                    b_xy[1] + (a_xy[1] - b_xy[1]) * ratio,
                                ),
                            ]
                        )

        if include_current and source_anchor is not None:
            for connection in self.store.data.get("connections", []):
                if str(connection.get("from", "")).strip() != source_name:
                    continue
                target_name = str(connection.get("to", "")).strip()
                target = points.get(target_name)
                if not target or int(target.get("floor", 0)) != floor:
                    continue
                target_anchor = target_name if graph.get(target_name) else None
                target_spur = 0.0
                if target_anchor is None:
                    target_anchor, target_spur = self._nearest_routing_anchor_for_point(
                        target_name
                    )
                if target_anchor is None:
                    continue
                route_length, node_path = self._shortest_path_length(
                    graph, source_anchor, target_anchor
                )
                if not node_path:
                    continue
                route = [(x, y)]
                route.extend(
                    (
                        float(points[node_name].get("x", 0.0)),
                        float(points[node_name].get("y", 0.0)),
                    )
                    for node_name in node_path
                    if int(points[node_name].get("floor", 0)) == floor
                )
                target_xy = (
                    float(target.get("x", 0.0)),
                    float(target.get("y", 0.0)),
                )
                if not route or route[-1] != target_xy:
                    route.append(target_xy)
                extension = max(
                    0.0, float(target.get("extension_distance_m", 0.0) or 0.0)
                )
                served_distance = (
                    float(source_spur)
                    + float(route_length or 0.0)
                    + float(target_spur or 0.0)
                    + extension
                )
                if len(route) >= 2:
                    current_polylines.append(route)
                if served_distance > current_max_distance:
                    current_max_distance = served_distance
                    current_max_target = target_name
                    current_max_point = target_xy

        return {
            "name": str(name),
            "floor": floor,
            "x": x,
            "y": y,
            "distance_limit_m": distance_limit,
            "possible_polylines": possible_polylines,
            "current_polylines": current_polylines,
            "boundary_polyline": self._extent_boundary_polyline(
                possible_polylines
            ),
            "current_max_target": current_max_target,
            "current_max_distance_m": current_max_distance,
            "current_max_point": current_max_point,
            "preview": bool(preview),
        }

    def show_equipment_room_extents(self, location_name):
        location_name = str(location_name or "").strip()
        location = next(
            (
                row
                for row in self.store.data.get("locations", [])
                if str(row.get("name", "")).strip() == location_name
            ),
            None,
        )
        if not location:
            return

        kind = str(location.get("kind", "")).strip()
        if kind not in {"comms_room", "distributed_equipment_room"} and not (
            location_name.upper().startswith("DER") and kind == "location"
        ):
            return

        floor = int(location.get("floor", 0))
        x = float(location.get("x", 0.0))
        y = float(location.get("y", 0.0))
        distance_limit = max(
            0.1, float(location.get("max_cable_length_m", 90.0) or 90.0)
        )
        self._pinned_equipment_room_extent_name = location_name
        self._set_equipment_room_extent_overlay(
            self._equipment_room_graph_extent_overlay(
                name=location_name,
                floor=floor,
                x=x,
                y=y,
                distance_limit=distance_limit,
                source_name=location_name,
                include_current=True,
            )
        )
        self.set_status(
            f"Showing {location_name} graph extents within {distance_limit:.1f} m"
        )

    def _update_placement_zone_preview(self, x, y):
        if self.placement_zone_start is None or not hasattr(
            self.canvas, "set_placement_zone_preview"
        ):
            return
        start_floor, start_x, start_y = self.placement_zone_start
        self.canvas.set_placement_zone_preview(
            {
                "id": "__placement_zone_preview__",
                "name": "Indicative placement area",
                "floor": int(start_floor),
                "min_x": min(float(start_x), float(x)),
                "min_y": min(float(start_y), float(y)),
                "max_x": max(float(start_x), float(x)),
                "max_y": max(float(start_y), float(y)),
                "allow_comms_room": True,
                "allow_distributed_equipment_room": True,
                "preview": True,
            }
        )

    def on_mouse_move(self, _event, sx, sy):
        mode = self.mode_combo.currentText()
        if mode == "location":
            session = self.bulk_location_session or {}
            kind = str(session.get("kind", "")).strip()
            if kind in {"comms_room", "distributed_equipment_room"} or not session:
                x, y = self.snap(sx, sy)
                self._set_equipment_room_extent_overlay(
                    self._equipment_room_graph_extent_overlay(
                        name="Proposed comms room / DER",
                        floor=int(self.floor_spin.value()),
                        x=float(x),
                        y=float(y),
                        distance_limit=max(
                            0.1,
                            float(session.get("max_cable_length_m", 90.0) or 90.0),
                        ),
                        preview=True,
                    )
                )
            else:
                self._clear_equipment_room_extent_overlay()
            return
        if mode != "placement_zone" or self.placement_zone_start is None:
            if not self._pinned_equipment_room_extent_name:
                self._clear_equipment_room_extent_overlay()
            return
        floor, _, _ = self.placement_zone_start
        if int(floor) != int(self.floor_spin.value()):
            self.placement_zone_start = None
            self._clear_placement_zone_preview()
            return
        x, y = self.snap(sx, sy)
        self._update_placement_zone_preview(x, y)

    def on_left_click(self, event, sx, sy):
        mode = self.mode_combo.currentText()
        floor = self.floor_spin.value()
        x, y = sx, sy
        x, y = self.snap(x, y)

        if mode == "pan":
            self.last_pan = event.position().toPoint()
            return

        if mode == "measure_data_room":
            self._handle_data_room_measurement_click(x, y, floor)
            return

        if mode == "placement_zone":
            if self.placement_zone_start is None:
                self.placement_zone_start = (int(floor), float(x), float(y))
                self._update_placement_zone_preview(x, y)
                self.set_status(
                    "Placement zone first corner set. Click the opposite corner."
                )
                return

            start_floor, start_x, start_y = self.placement_zone_start
            if int(start_floor) != int(floor):
                self.placement_zone_start = (int(floor), float(x), float(y))
                self._update_placement_zone_preview(x, y)
                self.set_status(
                    "Floor changed; placement zone first corner reset on this floor."
                )
                return

            self.placement_zone_start = None
            self._clear_placement_zone_preview()
            zone_id = self._next_placement_zone_id()
            seed = {
                "id": zone_id,
                "name": f"Placement Zone {zone_id.split('-')[-1]}",
                "floor": int(floor),
                "min_x": min(start_x, x),
                "min_y": min(start_y, y),
                "max_x": max(start_x, x),
                "max_y": max(start_y, y),
                "allow_comms_room": True,
                "allow_distributed_equipment_room": True,
            }
            dialog = PlacementZoneEditorDialog(self, seed)
            if dialog.exec() != QDialog.Accepted or not dialog.result:
                self.set_status("Placement zone cancelled")
                return
            if any(
                str(zone.get("id", "")).strip() == dialog.result["id"]
                for zone in self.store.data.get(
                    "equipment_room_placement_zones", []
                )
            ):
                QMessageBox.critical(
                    self, "Duplicate zone", "Zone ID already exists."
                )
                return
            self.push_undo_state("Add equipment room placement zone")
            self.store.data.setdefault(
                "equipment_room_placement_zones", []
            ).append(dialog.result)
            self.selected_placement_zone_id = dialog.result["id"]
            self._invalidate_static_scene_cache()
            self.refresh_canvas()
            self.set_status(
                f"Added placement zone {dialog.result['id']} on floor {floor}"
            )
            return

        if mode == "select_move":
            zone, zone_handle = self._find_placement_zone_hit(x, y, floor)
            point_under_body = (
                self.find_nearest_selectable_name(
                    x, y, floor, radius_world=self._select_pick_radius()
                )
                if zone_handle == "body"
                and (
                    zone is None
                    or str(zone.get("id", "")).strip()
                    != str(self.selected_placement_zone_id or "").strip()
                )
                else None
            )
            if zone is not None and point_under_body is None:
                self.selected_placement_zone_id = str(zone.get("id", "")).strip()
                self.selected_point_name = None
                self._clear_canvas_multi_selection()
                self.dragging_placement_zone_id = self.selected_placement_zone_id
                self.dragging_placement_zone_handle = zone_handle
                self.placement_zone_drag_start = (float(x), float(y))
                self.placement_zone_drag_original = deepcopy(zone)
                self.push_undo_state(
                    "Move equipment room placement zone"
                    if zone_handle == "body"
                    else "Resize equipment room placement zone"
                )
                self.set_status(
                    f"Selected placement zone {self.selected_placement_zone_id}: "
                    + (
                        "drag to move"
                        if zone_handle == "body"
                        else f"drag {zone_handle} handle to resize"
                    )
                )
                self.refresh_canvas()
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
            self.selected_placement_zone_id = None
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
                        if picked not in self.selected_template_names:
                            self._set_canvas_multi_selection([picked], append=False)
                else:
                    if not (modifiers & (Qt.ControlModifier | Qt.ShiftModifier)):
                        self._clear_canvas_multi_selection()

                self.dragging_point_name = picked
                self.alt_move_locked = self._is_alt_pressed()
                self.drag_mode_active = True

                self._begin_multi_drag(picked)

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
            zone, _zone_handle = self._find_placement_zone_hit(x, y, floor)
            if zone is not None:
                self.delete_placement_zone(zone.get("id", ""))
                return
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
                if (
                    session["kind"]
                    in {"comms_room", "distributed_equipment_room"}
                    and not self._placement_allowed_at(
                        floor, x, y, session["kind"]
                    )
                ):
                    QMessageBox.warning(
                        self,
                        "Placement outside allowed zone",
                        "This location type is not allowed at the selected point.",
                    )
                    return
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
                    max_cable_length_m=float(
                        session.get("max_cable_length_m", 90.0) or 90.0
                    ),
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
                self,
                "Location kind",
                "Kind:",
                ["location", "comms_room", "distributed_equipment_room"],
                0,
                False,
            )
            if not ok:
                return
            if kind in {"comms_room", "distributed_equipment_room"} and not self._placement_allowed_at(
                floor, x, y, kind
            ):
                QMessageBox.warning(
                    self,
                    "Placement outside allowed zone",
                    "This location type is not allowed at the selected point.",
                )
                return
            max_cable_length_m = 90.0
            if kind in {"comms_room", "distributed_equipment_room"}:
                max_cable_length_m, ok = QInputDialog.getDouble(
                    self,
                    "Equipment room distance limit",
                    "Maximum cable distance (m):",
                    90.0,
                    0.1,
                    100000.0,
                    2,
                )
                if not ok:
                    return
            self.store.add_location(
                name,
                floor,
                x,
                y,
                kind=kind,
                department_ids=[],
                max_cable_length_m=max_cable_length_m,
            )
            self.set_status(f"Added {kind} {name}")
            self.refresh_canvas()
            if kind in {"comms_room", "distributed_equipment_room"}:
                self.show_equipment_room_extents(name)
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
                    department_ids=session.get("department_ids", []),
                    room_type_id=session.get("room_type_id", ""),
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

            self._mark_routing_graph_changed()

            if hasattr(self.canvas, "invalidate_dxf_cache"):
                self.canvas.invalidate_dxf_cache()
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
        x, y = sx, sy
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
                            item["qty"] = self.store.room_type_cable_qty(
                                item["room_type_id"]
                            )
                        else:
                            item["qty"] = dialog.result["qty"]

                        self.store.sync_connection_qty_for_data_point(
                            dialog.result["name"]
                        )
                        item["department_ids"] = list(
                            dialog.result.get("department_ids", [])
                        )
                        item["cabinet_type"] = dialog.result.get(
                            "cabinet_type", "standard"
                        )
                        item["max_network_cabinets"] = max(
                            0,
                            int(dialog.result.get("max_network_cabinets", 0) or 0),
                        )
                        break
                self.selected_point_name = dialog.result["name"]
                self.set_status(f"Edited {dialog.result['name']}")
                self.refresh_canvas()
            return

        if point.get("kind") in {
            "location",
            "comms_room",
            "distributed_equipment_room",
        }:
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
                        item["cabinet_type"] = dialog.result.get(
                            "cabinet_type", "standard"
                        )
                        item["max_network_cabinets"] = max(
                            0,
                            int(dialog.result.get("max_network_cabinets", 0) or 0),
                        )
                        item["max_cable_length_m"] = max(
                            0.1,
                            float(dialog.result.get("max_cable_length_m", 90.0) or 90.0),
                        )
                        break

                self.selected_point_name = new_name
                if self._pinned_equipment_room_extent_name == picked:
                    self._pinned_equipment_room_extent_name = new_name
                    self.show_equipment_room_extents(new_name)
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
        if self.dragging_placement_zone_id:
            zone_id = self.dragging_placement_zone_id
            self.dragging_placement_zone_id = None
            self.dragging_placement_zone_handle = None
            self.placement_zone_drag_start = None
            self.placement_zone_drag_original = None
            self._invalidate_static_scene_cache()
            self.refresh_canvas()
            self.set_status(f"Updated placement zone {zone_id}")
            return

        moved = bool(self.drag_mode_active and self.dragging_point_name)

        if self.selection_rect_active:
            handled = self._finish_selection_rect(event)
            self.dragging_point_name = None
            self.drag_mode_active = False
            self.last_pan = None
            self._clear_multi_drag()

            if handled:
                return

        self.dragging_point_name = None
        self.drag_mode_active = False
        self.alt_move_locked = False
        self.last_pan = None
        self._clear_multi_drag()

        if moved:
            if hasattr(self.canvas, "invalidate_dxf_cache"):
                self.canvas.invalidate_dxf_cache()
            self.refresh_canvas()

    def on_right_click(self, event, sx, sy):
        mode = self.mode_combo.currentText()
        if mode == "placement_zone" and self.placement_zone_start is not None:
            self.placement_zone_start = None
            self._clear_placement_zone_preview()
            self.set_status("Placement zone cancelled")
            return
        floor = self.floor_spin.value()
        x, y = sx, sy
        picked = self.find_nearest_selectable_name(x, y, floor)

        x, y = self.snap(x, y)

        # In edge mode, right click is ONLY for deleting edges.
        # Never fall through to the normal context menu.
        if mode == "edge":

            if not picked:
                if self.selected_for_edge:
                    self.selected_for_edge = None
                    self.set_status("Edge chaining cancelled")
                    self.refresh_canvas()
                    return

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

            if removed:
                self._mark_routing_graph_changed()

            self.refresh_canvas()
            return

        zone, _zone_handle = self._find_placement_zone_hit(x, y, floor)
        if zone is not None and (
            not picked
            or str(zone.get("id", "")).strip()
            == str(self.selected_placement_zone_id or "").strip()
        ):
            zone_id = str(zone.get("id", "")).strip()
            self.selected_placement_zone_id = zone_id
            self.selected_point_name = None
            self._clear_canvas_multi_selection()
            self.refresh_canvas()
            menu = QMenu(self)
            edit_zone_action = menu.addAction("Edit placement zone")
            delete_zone_action = menu.addAction("Delete placement zone")
            action = menu.exec(event.globalPosition().toPoint())
            if action == edit_zone_action:
                dialog = PlacementZoneEditorDialog(self, dict(zone))
                if dialog.exec() == QDialog.Accepted and dialog.result:
                    self.push_undo_state("Edit equipment room placement zone")
                    zone.update(dialog.result)
                    self.refresh_canvas()
            elif action == delete_zone_action:
                self.delete_placement_zone(zone_id)
            return

        if picked:
            # A context menu is not a model or camera change. Keep the current
            # visual selection untouched so opening the menu does not trigger a
            # graph/DXF repaint. Menu actions receive ``picked`` explicitly and
            # refresh only when they actually modify data.
            point = self.store.all_points().get(picked)
            kind = str(point.get("kind", "")).strip() if point else ""

            menu = QMenu(self)
            show_edges_action = menu.addAction("Show all edge connections")
            estimate_cables_action = menu.addAction("Show estimated cables passing")
            find_topology_action = None
            show_extents_action = None
            if (
                kind in {"comms_room", "distributed_equipment_room"}
                or (picked.upper().startswith("DER") and kind == "location")
            ) and callable(
                getattr(self, "find_network_location_in_topology", None)
            ):
                find_topology_action = menu.addAction("Find in topology map")
            if kind in {"comms_room", "distributed_equipment_room"} or (
                picked.upper().startswith("DER") and kind == "location"
            ):
                show_extents_action = menu.addAction(
                    "Show current and possible extents"
                )

            menu.addSeparator()
            copy_selected_action = menu.addAction(
                f"Copy selected ({len(self._selected_copyable_names())})"
            )
            paste_here_action = menu.addAction("Paste copied selection here")
            paste_here_action.setEnabled(bool(self.selection_clipboard))

            rotate_90_action = menu.addAction("Rotate selected 90° clockwise")
            delete_action = menu.addAction("Delete")

            selected_data_points = self._selected_data_point_names()
            similar_seed_names = self._similar_data_point_seed_names(picked)
            select_similar_dp_action = None
            update_selected_dp_qty_action = None

            create_selected_dp_connections_action = None
            disconnect_selected_dp_connections_action = None

            assign_selected_dp_departments_action = None
            assign_selected_dp_room_type_action = None

            if kind == "data_point" and similar_seed_names:
                menu.addSeparator()
                seed_count = len(similar_seed_names)
                select_similar_dp_action = menu.addAction(
                    "Select similar data points "
                    f"(room type + department, {seed_count} seed"
                    f"{'s' if seed_count != 1 else ''})"
                )
                select_similar_dp_action.setToolTip(
                    "Select visible data points on this floor that share the same "
                    "room-type and department combinations as the current selection"
                )

            if selected_data_points:
                menu.addSeparator()
                update_selected_dp_qty_action = menu.addAction(
                    f"Update qty for {len(selected_data_points)} selected data points"
                )
                menu.addSeparator()
                (
                    pending_connection_names,
                    estimated_connection_ports,
                    _estimated_quantities,
                ) = self._connection_port_estimate_for_data_points(
                    selected_data_points
                )
                create_selected_dp_connections_action = menu.addAction(
                    f"Create connection(s) for {len(selected_data_points)} selected "
                    f"data point(s) - estimated {estimated_connection_ports} new port(s)"
                )
                create_selected_dp_connections_action.setToolTip(
                    f"{len(pending_connection_names)} unconnected data point(s) require "
                    f"an estimated {estimated_connection_ports} port(s). The estimate "
                    "includes manual quantities and room-type asset demand."
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
            align_corridor_x_action = None
            align_corridor_y_action = None
            restrict_nodes_action = None
            unrestrict_nodes_action = None

            if kind == "corridor_node":
                alignment_nodes = (
                    selected_corridor_nodes
                    if picked in selected_corridor_nodes
                    else [picked]
                )
                menu.addSeparator()
                align_corridor_x_action = menu.addAction(
                    f"Align {len(alignment_nodes)} corridor node(s) along X axis"
                )
                align_corridor_x_action.setToolTip(
                    "Keep each X position and align the selected nodes horizontally "
                    "to the clicked node's Y position."
                )
                align_corridor_y_action = menu.addAction(
                    f"Align {len(alignment_nodes)} corridor node(s) along Y axis"
                )
                align_corridor_y_action.setToolTip(
                    "Keep each Y position and align the selected nodes vertically "
                    "to the clicked node's X position."
                )
                can_align = len(alignment_nodes) >= 2
                align_corridor_x_action.setEnabled(can_align)
                align_corridor_y_action.setEnabled(can_align)

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
                find_topology_action is not None
                and action == find_topology_action
            ):
                self.find_network_location_in_topology(picked)
            elif show_extents_action is not None and action == show_extents_action:
                self.show_equipment_room_extents(picked)
            elif action == copy_selected_action:
                self.copy_selected_template_items()
            elif action == paste_here_action:
                self.paste_selected_template_items_at(x, y, floor)
            elif action == rotate_90_action:
                self.rotate_right_clicked_selection_90(picked)
            elif (
                align_corridor_x_action is not None
                and action == align_corridor_x_action
            ):
                self.align_right_clicked_corridor_nodes(picked, "x")
            elif (
                align_corridor_y_action is not None
                and action == align_corridor_y_action
            ):
                self.align_right_clicked_corridor_nodes(picked, "y")
            elif action == delete_action:
                self.delete_right_clicked_items(picked)
            elif (
                select_similar_dp_action is not None
                and action == select_similar_dp_action
            ):
                self.select_similar_data_points(picked)
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

        elif self.selection_clipboard:
            menu = QMenu(self)
            paste_here_action = menu.addAction("Paste copied selection here")
            action = menu.exec(event.globalPosition().toPoint())

            if action == paste_here_action:
                self.paste_selected_template_items_at(x, y, floor)

    def on_drag(self, event, sx, sy):
        mode = self.mode_combo.currentText()
        if mode == "pan":
            current = event.position().toPoint()
            if self.last_pan is None:
                self.last_pan = current
                return
            dx = current.x() - self.last_pan.x()
            dy = current.y() - self.last_pan.y()
            self.last_pan = current
            if hasattr(self.canvas, "pan_by"):
                self.canvas.pan_by(dx, dy)
            else:
                self.canvas.update()
            return
        if mode == "select_move":
            if (
                self.dragging_placement_zone_id
                and self.dragging_placement_zone_handle
                and self.placement_zone_drag_start
                and self.placement_zone_drag_original
            ):
                zone = self._placement_zone_by_id(
                    self.dragging_placement_zone_id
                )
                if zone is None:
                    return
                x, y = self.snap(sx, sy)
                original = self.placement_zone_drag_original
                handle = self.dragging_placement_zone_handle
                min_x = float(original.get("min_x", 0.0))
                max_x = float(original.get("max_x", 0.0))
                min_y = float(original.get("min_y", 0.0))
                max_y = float(original.get("max_y", 0.0))
                if handle == "body":
                    start_x, start_y = self.placement_zone_drag_start
                    dx = float(x) - float(start_x)
                    dy = float(y) - float(start_y)
                    min_x += dx
                    max_x += dx
                    min_y += dy
                    max_y += dy
                else:
                    minimum_span = 0.1
                    if "w" in handle:
                        min_x = min(float(x), max_x - minimum_span)
                    if "e" in handle:
                        max_x = max(float(x), min_x + minimum_span)
                    if "s" in handle:
                        min_y = min(float(y), max_y - minimum_span)
                    if "n" in handle:
                        max_y = max(float(y), min_y + minimum_span)
                zone.update(
                    {
                        "min_x": round(min_x, 3),
                        "max_x": round(max_x, 3),
                        "min_y": round(min_y, 3),
                        "max_y": round(max_y, 3),
                    }
                )
                if hasattr(self.canvas, "notify_moving_object_changed"):
                    self.canvas.notify_moving_object_changed()
                else:
                    self.refresh_canvas()
                return

            if self.selection_rect_active:
                self._update_selection_rect(event)
                return

            if self.alt_move_locked:
                return

            if self.drag_mode_active and self.dragging_point_name:
                point = self.store.all_points().get(self.dragging_point_name)
                if point and not self._is_point_kind_visible(
                    {**point, "name": self.dragging_point_name}
                ):
                    self.dragging_point_name = None
                    self.drag_mode_active = False
                    self._clear_multi_drag()
                    return

                x, y = sx, sy
                x, y = self.snap(x, y)

                if (
                    self.multi_drag_anchor_start is not None
                    and self.multi_drag_start_positions
                    and self.multi_drag_names
                ):
                    start_x, start_y = self.multi_drag_anchor_start
                    dx = float(x) - float(start_x)
                    dy = float(y) - float(start_y)

                    for name in self.multi_drag_names:
                        if name not in self.multi_drag_start_positions:
                            continue
                        original_x, original_y = self.multi_drag_start_positions[name]
                        self._move_point_or_transition(
                            name,
                            round(original_x + dx, 3),
                            round(original_y + dy, 3),
                        )
                else:
                    self._move_point_or_transition(self.dragging_point_name, x, y)

                self.refresh_canvas_geometry_only()

    def on_middle_click(self, event):
        self.last_pan = event.position().toPoint()

    def on_middle_drag(self, event):
        current = event.position().toPoint()
        if self.last_pan is None:
            self.last_pan = current
            return
        dx = current.x() - self.last_pan.x()
        dy = current.y() - self.last_pan.y()

        self.last_pan = current
        # GpuDxfGraphView has already applied the middle-button delta to its
        # retained scene-graph layers before emitting this signal. Calling
        # update() here used to invalidate every layer a second time.
        if not isinstance(self.canvas, GpuDxfGraphView):
            self.canvas.update()

    def on_middle_release(self, event):
        self.last_pan = None

    def on_mousewheel(self, event):
        if isinstance(self.canvas, GpuDxfGraphView):
            return

        delta = event.angleDelta().y()
        if delta == 0:
            return

        factor = 1.15 if delta > 0 else 1 / 1.15
        self.canvas.scale(factor, factor)
        self._viewport_refresh_timer.start(120)

    def closeEvent(self, event):
        if not getattr(self, "_close_database_prompt_handled", False):
            storage_path = getattr(self.store, "storage_path", "") or self.current_json_path or ""
            is_database = (
                getattr(self.store, "storage_format", "") == "sqlite"
                and bool(storage_path)
                and Path(storage_path).suffix.lower() == ".crsdb"
            )
            if is_database:
                usage = {}
                try:
                    usage = self.store.database_space_usage()
                except Exception:
                    usage = {}
                file_mb = float(usage.get("file_size_bytes", 0) or 0) / (1024 * 1024)
                reclaimable_mb = float(usage.get("reclaimable_bytes", 0) or 0) / (1024 * 1024)
                free_percent = float(usage.get("free_ratio", 0.0) or 0.0) * 100.0

                message = QMessageBox(self)
                message.setWindowTitle("Close Cable Routing Solver")
                message.setIcon(QMessageBox.Question)
                message.setText("Save and compact the project database before closing?")
                message.setInformativeText(
                    f"Current database size: {file_mb:.1f} MiB. "
                    f"Currently reusable space: {reclaimable_mb:.1f} MiB ({free_percent:.1f}%).\n\n"
                    "Save and Compact writes the current project and runs SQLite VACUUM. "
                    "This can take longer for large projects."
                )
                compact_button = message.addButton("Save and Compact", QMessageBox.ButtonRole.AcceptRole)
                close_button = message.addButton("Close Without Compacting", QMessageBox.ButtonRole.DestructiveRole)
                cancel_button = message.addButton(QMessageBox.Cancel)
                message.setDefaultButton(compact_button)
                message.exec()
                clicked = message.clickedButton()
                if clicked == cancel_button:
                    event.ignore()
                    return
                if clicked == compact_button:
                    QApplication.setOverrideCursor(Qt.WaitCursor)
                    try:
                        self.store.save_sqlite(storage_path, auto_compact=False)
                        compaction = self.store.compact_database(force=True)
                        reclaimed = getattr(compaction, "reclaimed_bytes", 0) / (1024 * 1024) if compaction else 0.0
                        statistics = getattr(self.store, "last_save_statistics", None)
                        revision_detail = ""
                        if statistics is not None:
                            if getattr(statistics, "revision_created", True):
                                revision_detail = f" - revision {statistics.revision_number}"
                            elif getattr(statistics, "revision_number", 0):
                                revision_detail = (
                                    f" - no project data changes - latest revision {statistics.revision_number}"
                                )
                            else:
                                revision_detail = " - no project data changes"
                        self.set_status(
                            f"Saved and compacted {Path(storage_path).name}{revision_detail} - {reclaimed:.1f} MiB reclaimed"
                        )
                    except Exception as exc:
                        QMessageBox.critical(
                            self,
                            "Database compaction failed",
                            f"The project could not be saved and compacted:\n{exc}",
                        )
                        event.ignore()
                        return
                    finally:
                        QApplication.restoreOverrideCursor()
                elif clicked != close_button:
                    event.ignore()
                    return
            self._close_database_prompt_handled = True

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

        before = len(self.store.data.get("corridors", {}).get("edges", []))
        self.store.add_edge(from_name, to_name)
        self.store.add_edge(to_name, from_name)
        if len(self.store.data.get("corridors", {}).get("edges", [])) != before:
            self._mark_routing_graph_changed()
        return True

    def _remove_corridor_edges_touching(self, point_names):
        names = {
            str(name).strip() for name in point_names if str(name).strip()
        }
        if not names:
            return 0
        edges = self.store.data.setdefault("corridors", {}).setdefault(
            "edges", []
        )
        retained = [
            edge
            for edge in edges
            if str(edge.get("from", "")).strip() not in names
            and str(edge.get("to", "")).strip() not in names
        ]
        removed = len(edges) - len(retained)
        if removed:
            self.store.data["corridors"]["edges"] = retained
            self._mark_routing_graph_changed()
        return removed

    def _connect_equipment_room_to_corridor_anchor(
        self, room_name, anchor_name
    ):
        room_name = str(room_name or "").strip()
        anchor_name = str(anchor_name or "").strip()
        if not room_name or not anchor_name or room_name == anchor_name:
            return False
        anchor = self.store.all_points().get(anchor_name, {})
        if str(anchor.get("kind", "")).strip() != "corridor_node":
            return False

        # A room may reuse a name after an earlier optimisation replaced it.
        # Remove every stale attachment before adding its one direct corridor
        # anchor, so new DERs cannot inherit distant edges or chain together.
        self._remove_corridor_edges_touching({room_name})
        return self._safe_add_same_floor_edge(room_name, anchor_name)

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
            str(item.get("name", "")).strip(): int(
                self.store.data_point_required_port_count(item)
            )
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

    def _next_comms_room_name(
        self, used_names, floor, start_number=1, kind="comms_room"
    ):
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
        self._mark_routing_graph_changed()
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
            if str(item.get("kind", "")) not in {
                "comms_room",
                "distributed_equipment_room",
            }:
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

        location_type_label, ok = QInputDialog.getItem(
            self,
            "Optimise Equipment Rooms",
            "Location type to place:",
            ["Comms room", "Distributed equipment room"],
            0,
            False,
        )
        if not ok:
            return
        location_kind = (
            "distributed_equipment_room"
            if location_type_label == "Distributed equipment room"
            else "comms_room"
        )

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
        candidate_nodes = self._candidate_comms_room_nodes(
            location_kind=location_kind
        )

        if not candidate_nodes:
            QMessageBox.critical(
                self,
                "Optimise Comms Rooms",
                "No permitted corridor nodes are available for the selected "
                "location type. Review the placement zones on the relevant floors.",
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

        existing_comms_rooms = {
            str(item.get("name", "")).strip()
            for item in self.store.data.get("locations", [])
            if str(item.get("kind", "")).strip() == location_kind
            and str(item.get("name", "")).strip()
        }

        self.push_undo_state("Optimise comms rooms")

        if replace_existing:
            self.store.data["locations"] = [
                item
                for item in self.store.data.get("locations", [])
                if str(item.get("kind", "location")) != location_kind
            ]
            self.store.data["connections"] = [
                item
                for item in self.store.data.get("connections", [])
                if str(item.get("from", "")).strip() not in existing_comms_rooms
            ]
            self._remove_corridor_edges_touching(existing_comms_rooms)

        used_names = set(self.store.names_in_use())
        location_prefix = self._comms_prefix_for_kind(location_kind)
        next_comms_room_number = self._highest_comms_room_number(location_prefix) + 1

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
                location_kind,
            )

            rooms_by_candidate.setdefault(candidate_name, []).append(room_name)
            room_loads[room_name] = 0

            room_index = len(rooms_by_candidate.get(candidate_name, [])) - 1

            room_x, room_y = self._placement_coordinates_near_candidate(
                candidate_point, location_kind, room_index
            )

            self.store.add_location(
                room_name,
                floor,
                room_x,
                room_y,
                kind=location_kind,
            )

            for location in self.store.data.get("locations", []):
                if location.get("name") == room_name:
                    location["cable_limit"] = int(comms_room_cable_limit)
                    break

            self._connect_equipment_room_to_corridor_anchor(
                room_name, candidate_name
            )
            return room_name

        for candidate_name in selected_nodes:
            create_comms_room_for_candidate(candidate_name)

        data_point_qty = {
            str(item.get("name", "")).strip(): int(
                self.store.data_point_required_port_count(item)
            )
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

            nearby_rooms = self._existing_comms_rooms_near_candidate(
                best_candidate, location_kind=location_kind
            )
            nearby_rooms = [
                name for name in nearby_rooms if name in existing_comms_rooms
            ]

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
                    "qty": int(data_point_qty.get(point_name, 0)),
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
        self._render_data_revision += 1
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

    def _manual_room_type_data_point_names(self):
        result = []

        for item in self.store.data.get("data_points", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue

            room_type_id = str(item.get("room_type_id", "") or "").strip()

            # Manual / no room type means blank room_type_id.
            if not room_type_id:
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

    def show_manual_room_type_data_point_navigator(self):
        self._manual_room_type_dp_names = self._manual_room_type_data_point_names()
        self._manual_room_type_dp_index = -1

        if self._manual_room_type_dp_dialog is None:
            self._manual_room_type_dp_dialog = UnassignedDataPointNavigatorDialog(self)
            self._manual_room_type_dp_dialog.setWindowTitle("Manual / No Room Type Data Points")
            self._manual_room_type_dp_dialog.nextRequested.connect(
                self.goto_next_manual_room_type_data_point
            )
            self._manual_room_type_dp_dialog.previousRequested.connect(
                self.goto_previous_manual_room_type_data_point
            )

        self._manual_room_type_dp_dialog.show()
        self._manual_room_type_dp_dialog.raise_()
        self._manual_room_type_dp_dialog.activateWindow()

        if not self._manual_room_type_dp_names:
            self._manual_room_type_dp_dialog.set_status(
                "No data points with manual / no room type found."
            )
            self.set_status("No data points with manual / no room type found")
            return

        self.goto_next_manual_room_type_data_point()

    def goto_next_manual_room_type_data_point(self):
        if not self._manual_room_type_dp_names:
            self._manual_room_type_dp_names = self._manual_room_type_data_point_names()

        if not self._manual_room_type_dp_names:
            if self._manual_room_type_dp_dialog:
                self._manual_room_type_dp_dialog.set_status(
                    "No data points with manual / no room type found."
                )
            return

        self._manual_room_type_dp_index = (
            self._manual_room_type_dp_index + 1
        ) % len(self._manual_room_type_dp_names)

        self._centre_on_manual_room_type_data_point()

    def goto_previous_manual_room_type_data_point(self):
        if not self._manual_room_type_dp_names:
            self._manual_room_type_dp_names = self._manual_room_type_data_point_names()

        if not self._manual_room_type_dp_names:
            if self._manual_room_type_dp_dialog:
                self._manual_room_type_dp_dialog.set_status(
                    "No data points with manual / no room type found."
                )
            return

        self._manual_room_type_dp_index = (
            self._manual_room_type_dp_index - 1
        ) % len(self._manual_room_type_dp_names)

        self._centre_on_manual_room_type_data_point()

    def _centre_on_manual_room_type_data_point(self):
        if self._manual_room_type_dp_index < 0:
            return

        name = self._manual_room_type_dp_names[self._manual_room_type_dp_index]
        point = self.store.all_points().get(name)

        if not point:
            return

        floor = int(point.get("floor", 0))

        if self.floor_spin.value() != floor:
            self.floor_spin.setValue(floor)

        self.selected_point_name = name
        self._set_canvas_multi_selection([name], append=False)
        self.refresh_canvas()

        self.canvas.fit_to_rect(QRectF(point["x"] - 10, -point["y"] - 10, 20, 20), Qt.KeepAspectRatio)

        if hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()
        self.refresh_canvas()

        text = (
            f"{self._manual_room_type_dp_index + 1} / "
            f"{len(self._manual_room_type_dp_names)}\n"
            f"{name}\n"
            f"Floor {floor}"
        )

        if self._manual_room_type_dp_dialog:
            self._manual_room_type_dp_dialog.set_status(text)

        self.set_status(f"Centred on manual / no room type data point {name}")

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

        self.canvas.fit_to_rect(QRectF(point["x"] - 10, -point["y"] - 10, 20, 20), Qt.KeepAspectRatio)

        if hasattr(self.canvas, "invalidate_dxf_cache"):
            self.canvas.invalidate_dxf_cache()
        self.refresh_canvas()

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

        for entry in self.store.data.get("floor_dxf_files", []):
            try:
                floors.add(int(entry.get("floor", 0)))
            except (TypeError, ValueError, AttributeError):
                pass

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
            str(item.get("name", "")).strip(): int(
                self.store.data_point_required_port_count(item)
            )
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
            qty = int(self.store.data_point_required_port_count(item))
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

        targets, estimated_ports, data_point_qty = (
            self._connection_port_estimate_for_data_points(selected)
        )
        target_set = set(targets)
        skipped_existing = [name for name in selected if name not in target_set]

        if not targets:
            QMessageBox.information(
                self,
                "Create Connections",
                "All selected data points already have connections.",
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
            "Connect selected data points from comms room:\n\n"
            f"Estimated ports required: {estimated_ports} across "
            f"{len(targets)} unconnected data point(s).",
            comms_rooms,
            0,
            False,
        )

        if not ok or not room_name:
            return

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
            f"Estimated ports required: {estimated_ports}",
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

    def _existing_comms_rooms_near_candidate(
        self, candidate_name, radius=5.0, location_kind="comms_room"
    ):
        points = self.store.all_points()
        candidate = points.get(candidate_name)
        if not candidate:
            return []

        cx = float(candidate.get("x", 0.0))
        cy = float(candidate.get("y", 0.0))
        cf = int(candidate.get("floor", 0))

        nearby = []

        for item in self.store.data.get("locations", []):
            if item.get("kind") != location_kind:
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
            category_options=self.store.asset_category_options(),
            category_items=self.store.data.get("asset_categories", []),
            on_save_categories=self._save_asset_categories,
            deployment_summary=self.store.asset_deployment_summary(),
            deployment_locations=self.store.asset_deployment_locations(),
            on_navigate_to_room=self._centre_on_named_point,
            on_show_capability_overlap=self.show_asset_capability_overlap_dialog,
            on_condense_assets=self._condense_assets,
            on_expand_asset=self._expand_asset,
        )

    def manage_asset_categories(self):
        AssetCategoriesEditorWindow(
            self,
            self.store.data.get("asset_categories", []),
            self._save_asset_categories,
        )

    def _save_asset_categories(self, items):
        self.push_undo_state("Save asset categories")
        self.store.data["asset_categories"] = items
        self.set_status("Asset categories updated")

    def _save_assets(self, items):
        self.push_undo_state("Save assets")
        self.store.data["assets"] = items
        valid_asset_ids = {str(asset.get("id", "") or "").strip() for asset in items if str(asset.get("id", "") or "").strip()}
        for group in self.store.data.get("asset_scenario_groups", []) or []:
            if isinstance(group, dict):
                group["asset_ids"] = [
                    str(asset_id).strip()
                    for asset_id in group.get("asset_ids", []) or []
                    if str(asset_id).strip() in valid_asset_ids
                ]

        self.store.sync_all_room_type_quantities()

        self.set_status("Assets updated and room type quantities recalculated")
        self.refresh_canvas()

    def _condense_assets(self, items, main_asset_id, condensed_asset_ids, reason):
        reason = str(reason or "").strip()
        if not reason:
            raise ValueError("A database commit note is required.")
        staging = self.store.data.get("room_type_asset_staging", {})
        if isinstance(staging, dict) and staging.get("changes"):
            raise ValueError(
                "Commit or reset the staged room-type asset changes before "
                "condensing assets."
            )

        updated_data = deepcopy(self.store.data)
        updated_data["assets"] = deepcopy(items)
        result = apply_asset_condensation(
            updated_data, main_asset_id, condensed_asset_ids
        )
        created_rfis = create_condensation_rfis(updated_data, result, reason)
        self.push_undo_state("Condense assets")
        self.store.data = updated_data
        removed_labels = []
        for asset in result["removed_assets"]:
            asset_id = str(asset.get("id", "") or "").strip()
            name = str(asset.get("name", asset_id) or asset_id).strip()
            removed_labels.append(
                f"{asset_id} ({name})" if name and name != asset_id else asset_id
            )
        main_label = result["main_asset_id"]
        if result["main_asset_name"] != main_label:
            main_label += f" ({result['main_asset_name']})"
        details = [
            f"{change['room_type_id'] or change['room_type_name']}: replaced condensed "
            f"asset assignments with {result['main_asset_id']}"
            for change in result["room_changes"]
        ]
        details.extend(f"Removed asset {label}" for label in removed_labels)
        details.extend(
            f"Raised {rfi['id']} for room type {rfi['room_type_id']} to verify "
            f"{rfi['asset_id']} and its port count"
            for rfi in created_rfis
        )
        self.store.record_revision_change(
            "Asset Condensation",
            f"Condensed {', '.join(removed_labels)} into {main_label}. Reason: {reason}",
            details=details,
        )
        self.store.sync_all_room_type_quantities()
        self.set_status(
            f"Condensed {len(removed_labels)} asset(s) into {result['main_asset_id']} "
            f"across {len(result['room_changes'])} room type(s)"
        )
        self.refresh_canvas()
        return {
            "room_type_count": len(result["room_changes"]),
            "rfi_count": len(created_rfis),
            "deployment_summary": self.store.asset_deployment_summary(),
            "deployment_locations": self.store.asset_deployment_locations(),
        }

    def _expand_asset(self, items, source_asset_id, replacement_assets, reason):
        reason = str(reason or "").strip()
        if not reason:
            raise ValueError("A database commit note is required.")
        staging = self.store.data.get("room_type_asset_staging", {})
        if isinstance(staging, dict) and staging.get("changes"):
            raise ValueError(
                "Commit or reset the staged room-type asset changes before "
                "expanding an asset."
            )

        updated_data = deepcopy(self.store.data)
        updated_data["assets"] = deepcopy(items)
        result = apply_asset_expansion(
            updated_data, source_asset_id, replacement_assets
        )
        self.push_undo_state("Expand asset")
        self.store.data = updated_data

        source = result["source_asset"]
        source_id = str(source.get("id", "") or "").strip()
        source_name = str(source.get("name", source_id) or source_id).strip()
        source_label = (
            f"{source_id} ({source_name})"
            if source_name and source_name != source_id
            else source_id
        )
        replacement_labels = []
        for asset in result["replacement_assets"]:
            asset_id = str(asset.get("id", "") or "").strip()
            name = str(asset.get("name", asset_id) or asset_id).strip()
            replacement_labels.append(
                f"{asset_id} ({name})" if name and name != asset_id else asset_id
            )

        replacement_ids = [
            str(asset.get("id", "") or "").strip()
            for asset in result["replacement_assets"]
        ]
        details = [
            f"{change['room_type_id'] or change['room_type_name']}: replaced "
            f"{source_id} with {', '.join(replacement_ids)}, retaining the "
            "assigned quantity for each replacement"
            for change in result["room_changes"]
        ]
        details.append(f"Removed expanded asset {source_label}")
        details.extend(f"Created replacement asset {label}" for label in replacement_labels)
        self.store.record_revision_change(
            "Asset Expansion",
            f"Expanded {source_label} into {', '.join(replacement_labels)}. "
            f"Reason: {reason}",
            details=details,
        )
        self.store.sync_all_room_type_quantities()
        self.set_status(
            f"Expanded {source_id} into {', '.join(replacement_ids)} across "
            f"{len(result['room_changes'])} room type(s)"
        )
        self.refresh_canvas()
        return {
            "assets": deepcopy(self.store.data.get("assets", [])),
            "room_type_count": len(result["room_changes"]),
            "deployment_summary": self.store.asset_deployment_summary(),
            "deployment_locations": self.store.asset_deployment_locations(),
        }

    def show_asset_capability_overlap_dialog(self):
        assets = list(self.store.data.get("assets", []) or [])
        if not assets:
            QMessageBox.information(
                self,
                "Asset Capability Overlap",
                "Create endpoint assets before reviewing capability overlaps.",
            )
            return

        rows = self.store.asset_capability_overlap_rows()
        if not rows:
            QMessageBox.information(
                self,
                "Asset Capability Overlap",
                "No capability keywords have been entered on endpoint assets yet. Edit assets and populate the Capability / function keywords field first.",
            )
            return

        dialog = AssetCapabilityOverlapDialog(self, rows, assets)
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()

    def show_room_type_asset_scenario_dialog(self):
        assets_by_id = {
            str(asset.get("id", "")).strip(): asset
            for asset in self.store.data.get("assets", [])
            if str(asset.get("id", "")).strip()
        }
        if not assets_by_id:
            QMessageBox.information(
                self,
                "Room/Asset Scenario",
                "Create endpoint assets before running a room/asset scenario.",
            )
            return

        if not self.store.room_type_scenario_groups():
            QMessageBox.information(
                self,
                "Room/Asset Scenario",
                "No room scenario groups have been defined yet. Use Tools > Room Scenario Groups to create reusable room selections.",
            )

        if not self.store.asset_scenario_groups():
            QMessageBox.information(
                self,
                "Room/Asset Scenario",
                "No asset scenario groups have been defined yet. Use Tools > Asset Scenario Groups to create reusable asset selections.",
            )

        asset_categories_by_id = {
            str(category.get("id", "")).strip(): str(
                category.get("name", category.get("id", ""))
            ).strip()
            for category in self.store.data.get("asset_categories", [])
            if str(category.get("id", "")).strip()
        }

        dialog = RoomTypeAssetScenarioDialog(
            self,
            self.store.data,
            asset_options=self.store.asset_options(),
            assets_by_id=assets_by_id,
            asset_categories_by_id=asset_categories_by_id,
            scenario_definitions=self.store.scenario_definitions(),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            action = str(dialog.result.get("action", "") or "").strip().lower()
            if action == "apply":
                self.apply_room_type_asset_scenarios(dialog.result)
            else:
                self.save_room_type_asset_scenarios(dialog.result.get("scenarios", []))
                summary = str(dialog.result.get("summary", "") or "").strip()
                if summary:
                    self.set_status(summary)

    def save_room_type_asset_scenarios(self, scenarios):
        self.push_undo_state("Save room/asset scenarios")
        self.store.data["room_type_asset_scenarios"] = [dict(item) for item in scenarios]
        self.set_status(f"Saved {len(scenarios)} room/asset scenario definition(s)")

    def _asset_scenario_group_key(self, asset):
        explicit = str(
            asset.get("scenario_group", asset.get("asset_scenario_group", "")) or ""
        ).strip()
        if explicit:
            return explicit
        return str(asset.get("Group", asset.get("group", "")) or "").strip()

    def _scenario_name_list(self, value):
        if isinstance(value, (list, tuple, set)):
            raw_values = list(value)
        elif value in (None, ""):
            raw_values = []
        else:
            text = str(value or "").strip()
            raw_values = [part.strip() for part in text.split(";")] if ";" in text else [text]
        names = []
        seen = set()
        for item in raw_values:
            name = str(item or "").strip()
            if name and name.casefold() not in seen:
                names.append(name)
                seen.add(name.casefold())
        return names

    def _scenario_group_label(self, names):
        return "; ".join(self._scenario_name_list(names))

    def _scenario_type(self, scenario):
        value = str(
            scenario.get("scenario_type", scenario.get("type", scenario.get("kind", "standard")))
            if isinstance(scenario, dict)
            else "standard"
        ).strip().lower()
        if value.startswith("rep") or value in {"replace_asset", "asset_replacement", "replacement"}:
            return "replacement"
        return "standard"

    def _scenario_replacement_asset_groups(self, scenario):
        replacement_groups = self._scenario_name_list(scenario.get("replacement_asset_groups"))
        if not replacement_groups:
            replacement_groups = self._scenario_name_list(
                scenario.get(
                    "replacement_asset_group",
                    scenario.get("replacement_group", scenario.get("target_asset_group", "")),
                )
            )
        return replacement_groups

    def _scenario_matching_room_types(self, room_groups):
        wanted = set()
        for room_group in self._scenario_name_list(room_groups):
            room_type_ids = self.store.room_type_ids_for_scenario_group(room_group)
            wanted.update(str(room_type_id).strip() for room_type_id in room_type_ids if str(room_type_id).strip())
        return [
            room_type
            for room_type in self.store.data.get("room_types", []) or []
            if str(room_type.get("id", "") or "").strip() in wanted
        ]

    def _scenario_matching_asset_ids(self, asset_groups):
        asset_ids = []
        seen = set()
        known_asset_ids = {
            str(asset.get("id", "") or "").strip()
            for asset in self.store.data.get("assets", []) or []
            if str(asset.get("id", "") or "").strip()
        }
        for asset_group in self._scenario_name_list(asset_groups):
            assets = self.store.asset_ids_for_scenario_group(asset_group)
            if assets:
                candidates = sorted(assets, key=str.casefold)
            elif asset_group in known_asset_ids:
                candidates = [asset_group]
            else:
                candidates = []
            for asset_id in candidates:
                if asset_id not in seen:
                    asset_ids.append(asset_id)
                    seen.add(asset_id)
        return asset_ids

    def _scenario_new_qty(self, current_qty, qty, mode):
        current_qty = max(0, int(current_qty or 0))
        qty = max(1, int(qty or 1))
        mode = str(mode or "add").strip().lower()
        if mode == "minimum":
            return max(current_qty, qty)
        if mode == "replace":
            return qty
        return current_qty + qty

    def _normalise_room_asset_rows_for_save(self, rows_by_asset_id, requested_by_asset_id=None):
        requested_by_asset_id = dict(requested_by_asset_id or {})
        rows = []
        for asset_id, qty in sorted(rows_by_asset_id.items(), key=lambda item: item[0].casefold()):
            if not str(asset_id).strip() or int(qty or 0) <= 0:
                continue
            row = {"asset_id": asset_id, "qty": max(1, int(qty or 1))}
            requested_by = str(requested_by_asset_id.get(asset_id, "") or "").strip()
            if requested_by:
                row["requested_by"] = requested_by
            rows.append(row)
        return rows

    def apply_room_type_asset_scenarios(self, scenario_result):
        scenarios = [dict(item) for item in scenario_result.get("scenarios", []) or []]
        enabled_scenarios = [item for item in scenarios if item.get("enabled")]
        if not enabled_scenarios:
            return

        self.push_undo_state("Apply room/asset scenarios")
        self.store.data["room_type_asset_scenarios"] = scenarios

        updated_room_type_ids = set()
        added = 0
        changed_existing = 0
        removed = 0
        skipped = []

        for scenario in enabled_scenarios:
            scenario_name = str(scenario.get("name", "Scenario") or "Scenario").strip()
            scenario_type = self._scenario_type(scenario)
            room_groups = self._scenario_name_list(scenario.get("room_groups"))
            if not room_groups:
                room_groups = self._scenario_name_list(scenario.get("room_group", ""))
            asset_groups = self._scenario_name_list(scenario.get("asset_groups"))
            if not asset_groups:
                asset_groups = self._scenario_name_list(scenario.get("asset_group", ""))
            replacement_asset_groups = self._scenario_replacement_asset_groups(scenario)
            room_group_label = self._scenario_group_label(room_groups)
            asset_group_label = self._scenario_group_label(asset_groups)
            replacement_group_label = self._scenario_group_label(replacement_asset_groups)
            qty = max(1, int(scenario.get("qty", 1) or 1))
            mode = str(scenario.get("mode", "add") or "add").strip().lower()

            room_types = self._scenario_matching_room_types(room_groups)
            asset_ids = self._scenario_matching_asset_ids(asset_groups)
            replacement_asset_ids = (
                self._scenario_matching_asset_ids(replacement_asset_groups)
                if scenario_type == "replacement"
                else []
            )
            if not room_types:
                skipped.append(f"{scenario_name}: no room types for '{room_group_label}'")
                continue
            if not asset_ids:
                skipped.append(f"{scenario_name}: no assets for '{asset_group_label}'")
                continue
            if scenario_type == "replacement" and not replacement_asset_ids:
                skipped.append(f"{scenario_name}: no replacement assets for '{replacement_group_label}'")
                continue

            for room_type in room_types:
                room_type_id = str(room_type.get("id", "") or "").strip()
                rows_by_asset_id = {
                    str(row.get("asset_id", "") or "").strip(): int(row.get("qty", 1) or 1)
                    for row in self.store.room_type_asset_rows(room_type)
                    if str(row.get("asset_id", "") or "").strip()
                }
                requested_by_asset_id = {
                    str(row.get("asset_id", "") or "").strip(): str(
                        row.get("requested_by", "") or ""
                    ).strip()
                    for row in self.store.room_type_asset_rows(room_type)
                    if str(row.get("asset_id", "") or "").strip()
                }

                if scenario_type == "replacement":
                    for asset_id in asset_ids:
                        current_qty = int(rows_by_asset_id.get(asset_id, 0) or 0)
                        if current_qty > 0:
                            rows_by_asset_id.pop(asset_id, None)
                            requested_by_asset_id.pop(asset_id, None)
                            removed += 1

                    for replacement_asset_id in replacement_asset_ids:
                        current_qty = int(rows_by_asset_id.get(replacement_asset_id, 0) or 0)
                        new_qty = self._scenario_new_qty(current_qty, qty, mode)
                        if current_qty <= 0:
                            added += 1
                        elif new_qty != current_qty:
                            changed_existing += 1
                        rows_by_asset_id[replacement_asset_id] = new_qty
                else:
                    for asset_id in asset_ids:
                        current_qty = int(rows_by_asset_id.get(asset_id, 0) or 0)
                        new_qty = self._scenario_new_qty(current_qty, qty, mode)
                        if current_qty <= 0:
                            added += 1
                        elif new_qty != current_qty:
                            changed_existing += 1
                        rows_by_asset_id[asset_id] = new_qty

                rows = self._normalise_room_asset_rows_for_save(
                    rows_by_asset_id, requested_by_asset_id
                )
                room_type["assets"] = rows
                room_type["asset_ids"] = [row["asset_id"] for row in rows]
                updated_room_type_ids.add(room_type_id)

        self.store.sync_all_room_type_quantities()
        self.refresh_canvas()
        self.refresh_rhs_search_sidebar()

        summary = (
            f"Applied {len(enabled_scenarios)} enabled scenario(s) to "
            f"{len(updated_room_type_ids)} room type(s). Added {added} asset assignment(s); "
            f"updated {changed_existing} existing quantity value(s); removed {removed} replaced asset assignment(s)."
        )
        if skipped:
            summary += f" Skipped {len(skipped)} empty scenario target(s)."

        self.set_status(summary)
        QMessageBox.information(self, "Scenarios applied", summary)

    def apply_room_type_asset_scenario(self, scenario_result):
        """Backward-compatible wrapper for older single-scenario dialog payloads."""
        if "scenarios" in scenario_result:
            self.apply_room_type_asset_scenarios(scenario_result)
            return
        group_name = str(scenario_result.get("group_name", "") or "").strip()
        asset_id = str(scenario_result.get("asset_id", "") or "").strip()
        qty = max(1, int(scenario_result.get("qty", 1) or 1))
        mode = str(scenario_result.get("mode", "add") or "add").strip().lower()
        if not group_name or not asset_id:
            return
        self.apply_room_type_asset_scenarios(
            {
                "scenarios": [
                    {
                        "name": f"{group_name} / {asset_id}",
                        "enabled": True,
                        "room_group": group_name,
                        "asset_group": asset_id,
                        "qty": qty,
                        "mode": mode,
                    }
                ]
            }
        )

    def rotate_right_clicked_selection_90(self, picked):
        if not picked:
            return

        points = self.store.all_points()
        pivot = points.get(picked)

        if not pivot:
            return

        if picked in self.selected_template_names:
            names = sorted(self.selected_template_names)
        else:
            names = [picked]

        names = [
            name
            for name in names
            if name in points
            and str(points[name].get("kind", "")).strip()
            in {
                "corridor_node",
                "data_point",
                "location",
                "comms_room",
                "distributed_equipment_room",
            }
        ]

        if not names:
            self.set_status("No rotatable selected items")
            return

        pivot_x = float(pivot["x"])
        pivot_y = float(pivot["y"])

        self.push_undo_state("Rotate selected items 90 degrees")

        for name in names:
            point = points[name]

            rel_x = float(point["x"]) - pivot_x
            rel_y = float(point["y"]) - pivot_y

            # 90 degrees clockwise in world coordinates
            new_x = pivot_x + rel_y
            new_y = pivot_y - rel_x

            self._move_point_or_transition(
                name,
                round(new_x, 3),
                round(new_y, 3),
            )

        self.refresh_canvas()
        self.set_status(f"Rotated {len(names)} item(s) 90° clockwise around {picked}")

    def align_right_clicked_corridor_nodes(self, picked, axis):
        """Align selected corridor nodes to the clicked node on one world axis."""
        axis = str(axis or "").strip().lower()
        if axis not in {"x", "y"} or not picked:
            return

        points = self.store.all_points()
        pivot = points.get(picked)
        if not pivot or str(pivot.get("kind", "")).strip() != "corridor_node":
            return

        if picked in self.selected_template_names:
            names = self._selected_corridor_node_names()
        else:
            names = [picked]

        if len(names) < 2:
            self.set_status("Select at least two corridor nodes to align")
            return

        self.push_undo_state(f"Align corridor nodes along {axis.upper()} axis")
        pivot_x = float(pivot.get("x", 0.0))
        pivot_y = float(pivot.get("y", 0.0))

        for name in names:
            point = points.get(name)
            if not point:
                continue
            if axis == "x":
                new_x = float(point.get("x", 0.0))
                new_y = pivot_y
            else:
                new_x = pivot_x
                new_y = float(point.get("y", 0.0))
            self._move_point_or_transition(
                name,
                round(new_x, 3),
                round(new_y, 3),
            )

        self._mark_routing_graph_changed()
        self.refresh_canvas()
        self.set_status(
            f"Aligned {len(names)} corridor node(s) along {axis.upper()} axis "
            f"using {picked} as the reference"
        )

    def _asset_matrix_header_label(self, asset):
        asset_id = str(asset.get("id", "")).strip()
        asset_name = str(asset.get("name", asset_id)).strip()
        return f"{asset_id} - {asset_name}" if asset_name and asset_name != asset_id else asset_id

    def _asset_id_from_matrix_header(self, header):
        return str(header or "").split(" - ", 1)[0].strip()

    def _room_type_asset_qty_map(self, room_type):
        result = {}

        for row in room_type.get("assets", []) or []:
            if not isinstance(row, dict):
                continue

            asset_id = str(row.get("asset_id", row.get("id", ""))).strip()
            if not asset_id:
                continue

            result[asset_id] = int(row.get("qty", 1) or 1)

        # Backwards compatibility with old asset_ids-only room types.
        for asset_id in room_type.get("asset_ids", []) or []:
            asset_id = str(asset_id).strip()
            if asset_id and asset_id not in result:
                result[asset_id] = 1

        return result

    def _room_group_names_for_room_type(self, room_type_id):
        room_type_id = str(room_type_id or "").strip()
        if not room_type_id:
            return []
        groups = []
        for group in self.store.data.get("room_type_scenario_groups", []) or []:
            if not isinstance(group, dict):
                continue
            name = str(group.get("name", "") or "").strip()
            members = {str(member_id).strip() for member_id in group.get("room_type_ids", []) or []}
            if name and room_type_id in members:
                groups.append(name)
        return sorted(groups, key=str.casefold)

    def _split_group_names_cell(self, value):
        text = str(value or "").strip()
        if not text:
            return []
        parts = []
        for chunk in re.split(r"[;|]", text):
            chunk = chunk.strip()
            if chunk:
                parts.append(chunk)
        if len(parts) <= 1 and "," in text:
            parts = [chunk.strip() for chunk in text.split(",") if chunk.strip()]
        seen = set()
        result = []
        for name in parts:
            key = name.casefold()
            if key not in seen:
                result.append(name)
                seen.add(key)
        return result

    def export_room_type_asset_matrix(self):
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Room Type Asset Matrix",
            str(
                Path(self.current_json_path).with_suffix("").with_name(
                    Path(self.current_json_path).stem + "_room_type_asset_matrix.csv"
                )
            )
            if self.current_json_path
            else "room_type_asset_matrix.csv",
            "CSV files (*.csv)",
        )

        if not path:
            return

        if not path.lower().endswith(".csv"):
            path += ".csv"

        assets = [
            asset
            for asset in self.store.data.get("assets", [])
            if str(asset.get("id", "")).strip()
        ]

        room_types = [
            room_type
            for room_type in self.store.data.get("room_types", [])
            if str(room_type.get("id", "")).strip()
        ]

        headers = ["room_type_id", "room_type_name", "room_groups"] + [
            self._asset_matrix_header_label(asset) for asset in assets
        ]

        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for room_type in room_types:
                qty_by_asset_id = self._room_type_asset_qty_map(room_type)

                row = {
                    "room_type_id": str(room_type.get("id", "")).strip(),
                    "room_type_name": str(room_type.get("name", "")).strip(),
                    "room_groups": "; ".join(
                        self._room_group_names_for_room_type(str(room_type.get("id", "") or "").strip())
                    ),
                }

                for asset in assets:
                    asset_id = str(asset.get("id", "")).strip()
                    header = self._asset_matrix_header_label(asset)
                    qty = qty_by_asset_id.get(asset_id, 0)
                    row[header] = qty if qty else ""

                writer.writerow(row)

        self.set_status(f"Exported room type asset matrix to {Path(path).name}")

    def import_room_type_asset_matrix(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Import Room Type Asset Matrix",
            "",
            "CSV files (*.csv)",
        )

        if not path:
            return

        assets_by_id = {
            str(asset.get("id", "")).strip(): asset
            for asset in self.store.data.get("assets", [])
            if str(asset.get("id", "")).strip()
        }

        room_types_by_id = {
            str(room_type.get("id", "")).strip(): room_type
            for room_type in self.store.data.get("room_types", [])
            if str(room_type.get("id", "")).strip()
        }

        updated = 0
        created = 0
        ignored_assets = set()
        imported_group_members = {}

        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                QMessageBox.critical(self, "Import failed", "CSV has no header row.")
                return

            asset_columns = [
                header
                for header in reader.fieldnames
                if header not in {"room_type_id", "room_type_name", "room_groups", "scenario_group"}
            ]

            self.push_undo_state("Import room type asset matrix")

            for csv_row in reader:
                room_type_id = str(csv_row.get("room_type_id", "")).strip()
                room_type_name = str(csv_row.get("room_type_name", "")).strip()
                room_group_names = self._split_group_names_cell(
                    csv_row.get("room_groups", csv_row.get("scenario_group", ""))
                )

                if not room_type_id:
                    continue

                room_type = room_types_by_id.get(room_type_id)
                existing_requesters = self._room_type_asset_requesters(room_type)
                asset_rows = []

                for header in asset_columns:
                    asset_id = self._asset_id_from_matrix_header(header)
                    if not asset_id:
                        continue

                    if asset_id not in assets_by_id:
                        ignored_assets.add(asset_id)
                        continue

                    raw_qty = str(csv_row.get(header, "")).strip()

                    if not raw_qty:
                        continue

                    try:
                        qty = int(float(raw_qty))
                    except Exception:
                        continue

                    if qty <= 0:
                        continue

                    asset_row = {"asset_id": asset_id, "qty": qty}
                    requested_by = existing_requesters.get(asset_id, "")
                    if requested_by:
                        asset_row["requested_by"] = requested_by
                    asset_rows.append(asset_row)

                if room_type is None:
                    room_type = {
                        "id": room_type_id,
                        "name": room_type_name or room_type_id,
                        "scenario_group": room_group_names[0] if room_group_names else "",
                        "assets": [],
                        "asset_ids": [],
                    }
                    self.store.data.setdefault("room_types", []).append(room_type)
                    room_types_by_id[room_type_id] = room_type
                    created += 1
                else:
                    updated += 1

                if room_type_name:
                    room_type["name"] = room_type_name
                if room_group_names:
                    room_type["scenario_group"] = room_group_names[0]
                imported_group_members[room_type_id] = room_group_names

                room_type["assets"] = asset_rows
                room_type["asset_ids"] = [row["asset_id"] for row in asset_rows]

        if imported_group_members:
            all_imported_room_ids = set(imported_group_members)
            groups_by_key = {}
            for group in self.store.data.get("room_type_scenario_groups", []) or []:
                if not isinstance(group, dict):
                    continue
                name = str(group.get("name", "") or "").strip()
                if not name:
                    continue
                key = name.casefold()
                members = [
                    str(member_id).strip()
                    for member_id in group.get("room_type_ids", []) or []
                    if str(member_id).strip() and str(member_id).strip() not in all_imported_room_ids
                ]
                groups_by_key[key] = {
                    "name": name,
                    "room_type_ids": members,
                    "notes": str(group.get("notes", "") or "").strip(),
                }
            for room_type_id, group_names in imported_group_members.items():
                for group_name in group_names:
                    key = group_name.casefold()
                    if key not in groups_by_key:
                        groups_by_key[key] = {"name": group_name, "room_type_ids": [], "notes": ""}
                    if room_type_id not in groups_by_key[key]["room_type_ids"]:
                        groups_by_key[key]["room_type_ids"].append(room_type_id)
            self.store.data["room_type_scenario_groups"] = list(groups_by_key.values())

        self.store.sync_all_room_type_quantities()

        self.refresh_canvas()

        message = f"Imported matrix. Updated {updated}, created {created} room type(s)."

        if ignored_assets:
            message += f"\n\nIgnored unknown asset ID(s): {', '.join(sorted(ignored_assets))}"

        QMessageBox.information(self, "Import complete", message)
        self.set_status(message.replace("\n", " "))


# NETWORK_PLANNING_EXTENSION_START
from network_integration import install_network_planning
install_network_planning(CableRouteEditor)
# NETWORK_PLANNING_EXTENSION_END


def main():
    app = QApplication.instance() or QApplication(sys.argv)
    apply_bootstrap_theme(app)
    window = CableRouteEditor()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
