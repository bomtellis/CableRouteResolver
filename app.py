import heapq
import math
import sys
from pathlib import Path

from PySide6.QtCore import QObject, QPointF, Qt, Signal, QRectF, QThread, Slot
from PySide6.QtGui import QColor, QBrush, QPainter, QPen, QFont, QPolygonF
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
)

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
)
from advanced_dialogs import (
    ConnectionEditorWindow,
    DataPointDepartmentsBulkDialog,
    LocationDepartmentsBulkDialog,
    RouteProfilesEditorV2,
)
from models import JsonStore


class DXFLoadWorker(QObject):
    loaded = Signal(int, str, object, object)
    failed = Signal(int, str, str)

    @Slot(int, str)
    def load_floor(self, floor, path):
        try:
            payload = DXFScene.load_content(path)
            self.loaded.emit(
                int(floor), str(path), payload["entities"], payload["bounds"]
            )
        except Exception as exc:
            self.failed.emit(int(floor), str(path), str(exc))


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


class CableRouteEditor(QMainWindow):
    _request_dxf_load = Signal(int, str)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cable Routing Graph Editor")
        self.resize(1500, 920)

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
        self._request_dxf_load.connect(self._dxf_worker.load_floor)
        self._dxf_thread.start()

        self.last_pan = None
        self.selected_for_edge = None
        self.selected_point_name = None
        self.dragging_point_name = None
        self.drag_mode_active = False
        self.edge_delete_start = None
        self._item_lookup = {}
        self._point_item_lookup = {}
        self.bulk_location_session = None
        self.bulk_data_point_session = None

        self._build_ui()
        self.refresh_canvas()

    def _build_ui(self):
        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)

        self.sidebar = QWidget()
        self.sidebar.setFixedWidth(260)
        sidebar_layout = QVBoxLayout(self.sidebar)
        layout.addWidget(self.sidebar)

        self.scene = QGraphicsScene(self)
        self.canvas = EditorGraphicsView(self)
        self.canvas.setScene(self.scene)
        self.canvas.set_overlay_provider(self.draw_overlay_panels)
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
        self.show_dxf_check.toggled.connect(self.refresh_canvas)
        self.show_labels_check.toggled.connect(self.refresh_canvas)

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
            ("Map DXF to Floor", self.load_dxf),
            ("Clear Floor DXF", self.clear_floor_dxf),
            ("Fit View", self.fit_view),
            ("Validate", self.validate_json),
            ("Departments", self.manage_departments),
            ("Locations", self.manage_locations),
            ("Location Departments", self.manage_location_departments),
            ("Mass Create Locations", self.start_bulk_location_placement),
            ("Mass Create Data Points", self.start_bulk_data_point_placement),
            ("Data Point Departments", self.manage_data_point_departments),
            ("Data Points", self.manage_data_points),
            ("Transitions", self.manage_transitions),
            ("Connections", self.manage_connections),
            ("Autoroute Data Points", self.autoroute_data_points),
            ("Suggest Comms Room", self.suggest_comms_room_for_department),
            ("Route Profiles", self.manage_route_profiles),
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

    def set_status(self, text):
        self.status_label.setText(text)

    def on_floor_changed(self, *_):
        self.refresh_canvas()
        self._queue_all_floor_dxf_loads(
            active_floor=self.floor_spin.value(), force_reload=False
        )

    def floor_dxf_entries(self):
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
            floors = [int(active_floor)] + [
                f for f in floors if int(f) != int(active_floor)
            ]
        self._start_loading_batch(floors)
        for floor in floors:
            self.request_floor_dxf_load(
                floor,
                force_reload=force_reload and int(floor) == int(active_floor),
                prefetch=int(floor)
                != int(active_floor if active_floor is not None else floor),
            )
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
        if not prefetch and floor == self.floor_spin.value():
            self.set_status(f"Loading DXF for floor {floor}...")
        self._request_dxf_load.emit(floor, path)
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

    def refresh_canvas(self):
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
                label.setPos(pos.x() + 0.4, pos.y() - 0.35)
                label.setScale(0.08)
                self._item_lookup[label] = ("department_label", department_id)

    def draw_points(self, floor):
        for name, point in self.store.points_for_floor(floor).items():
            pos = self.world_to_scene(point["x"], point["y"])
            selected = name == self.selected_point_name
            kind = point.get("kind")
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
                    QBrush(QColor("#f2c94c")),
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
                item.setBrush(QBrush(QColor("#b07cff")))
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
                label.setPos(pos.x() + 0.35, pos.y() - 0.35)
                label.setScale(0.08)
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

    def find_nearest_point_name(self, x, y, floor, radius_world=3.0):
        best = None
        best_dist = radius_world
        for name, point in self.store.points_for_floor(floor).items():
            d = math.hypot(point["x"] - x, point["y"] - y)
            if d <= best_dist:
                best = name
                best_dist = d
        return best

    def find_nearest_selectable_name(self, x, y, floor, radius_world=3.0):
        best = None
        best_dist = radius_world

        for name, point in self.store.points_for_floor(floor).items():
            d = math.hypot(point["x"] - x, point["y"] - y)
            if d <= best_dist:
                best = name
                best_dist = d

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

    def suggest_next_comms_room_name(self, floor):
        prefix = f"CR{int(floor)}-"
        nums = []
        for item in self.store.data.get("locations", []):
            if str(item.get("kind", "location")) != "comms_room":
                continue
            name = str(item.get("name", ""))
            if name.startswith(prefix):
                tail = name[len(prefix) :]
                if tail.isdigit():
                    nums.append(int(tail))
        return f"{prefix}{max(nums, default=0) + 1}"

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

    def suggest_comms_room_for_department(self):
        if not self.store.data.get("departments"):
            QMessageBox.critical(self, "Suggest Comms Room", "No departments found.")
            return

        graph, points = self._build_routing_graph()

        dialog = SuggestCommsRoomDialog(
            self,
            self.department_options(),
            default_name=self.suggest_next_comms_room_name(self.floor_spin.value()),
        )
        if dialog.exec() != QDialog.Accepted or not dialog.result:
            return

        department_ids = [
            str(x).strip()
            for x in dialog.result.get("department_ids", [])
            if str(x).strip()
        ]
        max_cable_length_m = float(dialog.result["max_cable_length_m"])
        room_name = dialog.result["room_name"].strip()

        if room_name in self.store.names_in_use():
            QMessageBox.critical(self, "Suggest Comms Room", "Name already exists.")
            return

        data_point_names = self.departments_data_point_names(department_ids)
        if not data_point_names:
            QMessageBox.critical(
                self,
                "Suggest Comms Room",
                "No data points found for the selected departments.",
            )
            return

        candidate_floors = {
            int(points[name]["floor"]) for name in data_point_names if name in points
        }

        candidate_nodes = self._candidate_comms_room_nodes()
        if not candidate_nodes:
            QMessageBox.critical(
                self,
                "Suggest Comms Room",
                "No corridor nodes available on the same floor(s) as the selected data points.",
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
                    route_length, route_path = self._shortest_path_length(
                        graph, anchor_name, candidate_name
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
                f"Departments: {', '.join(department_ids)}",
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
        self.store.add_location(
            room_name,
            int(candidate_point["floor"]),
            float(candidate_point["x"]),
            float(candidate_point["y"]),
            kind="comms_room",
        )
        self.selected_point_name = room_name
        self.refresh_canvas()

        QMessageBox.information(
            self,
            "Suggest Comms Room",
            "\n".join(
                [
                    f"Placed comms room {room_name}",
                    f"Candidate node: {best_candidate}",
                    f"Departments: {', '.join(department_ids)}",
                    f"Floor: {candidate_point['floor']}",
                    f"Total cable length: {best_total:.2f} m",
                    f"Longest single cable: {best_max:.2f} m",
                    "You can now edit or rename the comms room as needed.",
                ]
            ),
        )
        self.set_status(
            f"Placed suggested comms room {room_name} at {best_candidate} "
            f"for {len(department_ids)} department(s) "
            f"(total {best_total:.2f} m, max {best_max:.2f} m)"
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

    def _candidate_comms_room_nodes(self):
        result = []
        for item in self.store.data.get("corridors", {}).get("nodes", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            result.append(name)
        return sorted(result)

    def comms_room_names(self):
        result = []
        for item in self.store.data.get("locations", []):
            if str(item.get("kind", "location")) == "comms_room":
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

        created = 0
        skipped_existing = 0
        skipped_unreachable = []
        created_rows = []

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

        for data_point in self.store.data.get("data_points", []):
            point_name = str(data_point.get("name", "")).strip()
            if not point_name:
                continue

            if point_name in existing_targets:
                skipped_existing += 1
                continue

            if point_name not in points:
                skipped_unreachable.append(point_name)
                continue

            best_room = None
            best_cost = None
            best_path = []

            for comms_room in comms_rooms:
                if comms_room not in points:
                    continue

                route_cost, route_path = self._shortest_path_length(
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
                    best_path = route_path

            if best_room is None:
                skipped_unreachable.append(point_name)
                continue

            created_rows.append(
                {
                    "id": next_connection_id(),
                    "from": best_room,
                    "to": point_name,
                    "qty": int(data_point.get("qty", 1) or 1),
                    "route_profile": "",
                }
            )

            created += 1

        if not created_rows and not skipped_unreachable and skipped_existing:
            QMessageBox.information(
                self,
                "Autoroute",
                f"No new routes created. {skipped_existing} data point(s) already had connections.",
            )
            return

        if created_rows:
            self.store.data.setdefault("connections", []).extend(created_rows)
            self.set_status(f"Autorouted {created} data point(s) to best comms rooms")

        message_lines = [f"Created {created} connection(s)."]
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
        self.store.data["departments"] = items
        self.set_status("Departments updated")
        self.refresh_canvas()

    def manage_data_points(self):
        columns = [
            ("name", "Name", 180),
            ("floor", "Floor", 70),
            ("x", "X", 80),
            ("y", "Y", 80),
            ("qty", "Qty", 70),
            ("extension_distance_m", "Extension m", 100),
        ]
        TableListEditor(
            self,
            "Data Points",
            columns,
            self.store.data.get("data_points", []),
            self._save_data_points,
        )

    def _save_data_points(self, items):
        self.store.data["data_points"] = items
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
                    return " / ".join(sorted(set(labels)))
                return f"Floor {item.get('floor', 0)} / Unassigned"
            return "Other"

        dialog = DataPointDepartmentsBulkDialog(
            self,
            self.store.data.get("data_points", []),
            self.department_options(),
            self._save_data_points,
            group_resolver=group_resolver,
        )
        dialog.exec()

    def start_bulk_location_placement(self):
        floor = self.floor_spin.value()
        dialog = BulkLocationPlacementDialog(
            self,
            default_floor=floor,
            default_prefix=f"L{floor}-",
            default_start_number=1,
            department_options=self.department_options(),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            prefix = dialog.result["prefix"]
            start_number = int(dialog.result["start_number"])
            count = int(dialog.result["count"])
            kind = dialog.result["kind"]
            department_ids = list(dialog.result.get("department_ids", []))

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

    def _save_route_profiles(self, profiles):
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

        picked = self.find_nearest_selectable_name(x, y, floor)
        self.selected_point_name = picked

        if mode == "select_move":
            if picked:
                self.dragging_point_name = picked
                self.drag_mode_active = True
                self.set_status(f"Selected {picked}")
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
                        self.store.delete_transition(transition_id)
                        self.selected_point_name = None
                        self.set_status(f"Deleted {transition_id}")
                else:
                    if (
                        QMessageBox.question(self, "Delete point", f"Delete {picked}?")
                        == QMessageBox.Yes
                    ):
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

            self.store.add_corridor_node(name, floor, x, y, height, limit)
            self.set_status(f"Added corridor node {name}")
            self.refresh_canvas()
            return

        if mode == "location":
            if self.bulk_location_session:
                session = self.bulk_location_session
                name, number = self._next_available_bulk_location_name(
                    session["prefix"],
                    session["next_number"],
                )

                self.store.add_location(
                    name,
                    floor,
                    x,
                    y,
                    kind=session["kind"],
                    department_ids=list(session.get("department_ids", [])),
                )

                session["next_number"] = number + 1
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

                self.store.add_data_point(
                    name,
                    floor,
                    x,
                    y,
                    session["qty"],
                    session["extension_distance_m"],
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
                seed={"floor": floor},
                default_floor=floor,
                default_x=x,
                default_y=y,
                default_name=self.store.suggest_next_data_point_name(floor),
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
                default_id=self.store.suggest_next_transition_id(),
            )
            if dialog.exec() == QDialog.Accepted and dialog.result:
                self.store.upsert_transition(
                    dialog.result["id"],
                    dialog.result["floors"],
                    dialog.result["floor_locations"],
                    dialog.result["cable_limit"],
                )
                self.set_status(f"Saved {dialog.result['id']}")
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
            )
            if dialog.exec() == QDialog.Accepted and dialog.result:
                self.store.set_point_position(
                    picked, dialog.result["x"], dialog.result["y"]
                )
                self.store.rename_point(picked, dialog.result["name"])
                for item in self.store.data.get("data_points", []):
                    if item["name"] == dialog.result["name"]:
                        item["qty"] = dialog.result["qty"]
                        item["extension_distance_m"] = dialog.result[
                            "extension_distance_m"
                        ]
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
            self.store.set_point_position(
                picked, dialog.result["x"], dialog.result["y"]
            )
            self.store.rename_point(picked, dialog.result["name"])
            self.selected_point_name = dialog.result["name"]
            self.set_status(f"Edited {dialog.result['name']}")
            self.refresh_canvas()

    def on_left_release(self, event):
        self.dragging_point_name = None
        self.drag_mode_active = False
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
            return

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
        if mode == "select_move" and self.drag_mode_active and self.dragging_point_name:
            x, y = self.scene_to_world(sx, sy)
            x, y = self.snap(x, y)
            self.store.set_point_position(self.dragging_point_name, x, y)
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

    def estimate_cables_through_node(self, node_name):
        graph, points = self._build_routing_graph()
        if node_name not in points:
            return 0, 0, []

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
                qty = int(connection.get("qty", 1) or 1)
                total_cables += qty
                matching_connections += 1
                details.append(
                    {
                        "id": str(connection.get("id", "")).strip(),
                        "from": start,
                        "to": end,
                        "qty": qty,
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


def main():
    app = QApplication.instance() or QApplication(sys.argv)
    window = CableRouteEditor()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
