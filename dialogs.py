import json
from typing import Any
import time

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QMainWindow,
    QListWidget,
    QListWidgetItem,
    QCheckBox,
    QDoubleSpinBox,
    QProgressBar,
    QSpinBox,
)


class PointEditorDialog(QDialog):
    def __init__(self, parent, title, point_name, point):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.point_name = point_name
        self.point = point
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.name_edit = QLineEdit(str(point_name))
        self.x_edit = QLineEdit(str(point["x"]))
        self.y_edit = QLineEdit(str(point["y"]))
        form.addRow("Name", self.name_edit)
        form.addRow("X", self.x_edit)
        form.addRow("Y", self.y_edit)
        form.addRow("Floor", QLabel(str(point["floor"])))
        form.addRow("Kind", QLabel(str(point.get("kind", ""))))
        self.restricted_check = QCheckBox("Restricted - cannot host comms room")
        self.restricted_check.setChecked(bool(point.get("restricted", False)))
        form.addRow("", self.restricted_check)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self):
        try:
            x = float(self.x_edit.text())
            y = float(self.y_edit.text())
            name = self.name_edit.text().strip()
            if not name:
                raise ValueError("Name is required")
            self.result = {
                "name": name,
                "x": x,
                "y": y,
                "restricted": self.restricted_check.isChecked(),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid value", str(exc))


class LocationEditorDialog(QDialog):
    def __init__(self, parent, location_name, location, department_options):
        super().__init__(parent)
        self.setWindowTitle(f"Edit {location_name}")
        self.location_name = location_name
        self.location = location
        self.department_options = list(department_options)
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.name_edit = QLineEdit(str(location_name))
        self.x_edit = QLineEdit(str(location.get("x", 0.0)))
        self.y_edit = QLineEdit(str(location.get("y", 0.0)))

        self.kind_combo = QComboBox()
        self.kind_combo.addItems(["location", "comms_room"])
        self.kind_combo.setCurrentText(str(location.get("kind", "location")))

        self.departments_list = QListWidget()
        self.departments_list.setSelectionMode(QAbstractItemView.NoSelection)
        selected = {
            str(x).strip() for x in location.get("department_ids", []) if str(x).strip()
        }

        for department_id, department_name in self.department_options:
            text = (
                f"{department_id} - {department_name}"
                if department_name
                else department_id
            )
            item = QListWidgetItem(text)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setData(Qt.UserRole, department_id)
            item.setCheckState(
                Qt.Checked if department_id in selected else Qt.Unchecked
            )
            self.departments_list.addItem(item)

        form.addRow("Name", self.name_edit)
        form.addRow("X", self.x_edit)
        form.addRow("Y", self.y_edit)
        form.addRow("Floor", QLabel(str(location.get("floor", 0))))
        form.addRow("Kind", self.kind_combo)
        form.addRow("Departments", self.departments_list)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.resize(520, 500)

    def accept(self):
        try:
            name = self.name_edit.text().strip()
            if not name:
                raise ValueError("Name is required")

            department_ids = []
            for i in range(self.departments_list.count()):
                item = self.departments_list.item(i)
                if item.checkState() == Qt.Checked:
                    department_ids.append(str(item.data(Qt.UserRole)).strip())

            self.result = {
                "name": name,
                "x": float(self.x_edit.text()),
                "y": float(self.y_edit.text()),
                "floor": int(self.location.get("floor", 0)),
                "kind": self.kind_combo.currentText().strip(),
                "department_ids": department_ids,
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid location", str(exc))


class BulkLocationPlacementDialog(QDialog):
    def __init__(
        self,
        parent,
        default_floor=0,
        default_prefix="LOC",
        default_start_number=1,
        department_options=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Mass Create Locations")
        self.result = None
        self.department_options = list(department_options or [])

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.prefix_edit = QLineEdit(str(default_prefix))
        self.start_number_edit = QLineEdit(str(default_start_number))
        self.count_edit = QLineEdit("1")

        self.kind_combo = QComboBox()
        self.kind_combo.addItems(["location", "comms_room", "distributed_equipment_room"])

        self.departments_list = QListWidget()
        self.departments_list.setSelectionMode(QAbstractItemView.NoSelection)

        for row in self.department_options:
            department_id = row[0]
            department_name = row[1]
            department_floor = row[2] if len(row) > 2 else None
            text = (
                f"{department_id} - {department_name} - Floor {department_floor}"
                if department_floor is not None and department_name
                else f"{department_id} - Floor {department_floor}"
                if department_floor is not None
                else f"{department_id} - {department_name}"
                if department_name
                else department_id
            )

            item = QListWidgetItem(text)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setData(Qt.UserRole, department_id)
            item.setCheckState(Qt.Unchecked)
            self.departments_list.addItem(item)

        form.addRow("Prefix", self.prefix_edit)
        form.addRow("Starting number", self.start_number_edit)
        form.addRow("Number to place", self.count_edit)
        form.addRow("Kind", self.kind_combo)
        form.addRow("Departments", self.departments_list)
        form.addRow("Floor", QLabel(str(default_floor)))

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.resize(420, 420)

    def _checked_department_ids(self):
        result = []
        for i in range(self.departments_list.count()):
            item = self.departments_list.item(i)
            if item.checkState() == Qt.Checked:
                result.append(str(item.data(Qt.UserRole)).strip())
        return result

    def accept(self):
        try:
            prefix = self.prefix_edit.text().strip()
            if not prefix:
                raise ValueError("Prefix is required")

            start_number = int(self.start_number_edit.text())
            count = int(self.count_edit.text())

            if count <= 0:
                raise ValueError("Number to place must be greater than 0")

            self.result = {
                "prefix": prefix,
                "start_number": start_number,
                "count": count,
                "kind": self.kind_combo.currentText().strip(),
                "department_ids": self._checked_department_ids(),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid bulk placement", str(exc))


class BulkDataPointPlacementDialog(QDialog):
    def __init__(self, parent, default_floor=0, default_prefix="DP"):
        super().__init__(parent)
        self.setWindowTitle("Mass Create Data Points")
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.prefix_edit = QLineEdit(str(default_prefix))
        self.count_edit = QLineEdit("1")
        self.qty_edit = QLineEdit("1")
        self.extension_edit = QLineEdit("0.0")

        form.addRow("Prefix", self.prefix_edit)
        form.addRow("Number to place", self.count_edit)
        form.addRow("Qty", self.qty_edit)
        form.addRow("Extension distance (m)", self.extension_edit)
        form.addRow("Floor", QLabel(str(default_floor)))
        form.addRow("", QLabel("Starting number is automatic."))

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.resize(360, 220)

    def accept(self):
        try:
            prefix = self.prefix_edit.text().strip()
            if not prefix:
                raise ValueError("Prefix is required")

            count = int(self.count_edit.text())
            qty = int(self.qty_edit.text())
            extension_distance_m = float(self.extension_edit.text())

            if count <= 0:
                raise ValueError("Number to place must be greater than 0")
            if qty <= 0:
                raise ValueError("Qty must be greater than 0")

            self.result = {
                "prefix": prefix,
                "count": count,
                "qty": qty,
                "extension_distance_m": extension_distance_m,
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid bulk placement", str(exc))


class DepartmentEditorDialog(QDialog):
    def __init__(self, parent, department):
        super().__init__(parent)
        self.setWindowTitle("Department")
        self.department = department or {}
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        department_id = str(self.department.get("id", "")).strip()
        self.id_label = QLabel(department_id or "(auto)")
        self.name_edit = QLineEdit(
            str(self.department.get("name", self.department.get("id", "")))
        )
        self.x_edit = QLineEdit(str(self.department.get("x", 0.0)))
        self.y_edit = QLineEdit(str(self.department.get("y", 0.0)))

        form.addRow("Department ID", self.id_label)
        form.addRow("Department name", self.name_edit)
        form.addRow("X", self.x_edit)
        form.addRow("Y", self.y_edit)
        form.addRow("Floor", QLabel(str(self.department.get("floor", 0))))

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self):
        try:
            department_id = str(self.department.get("id", "")).strip()
            department_name = self.name_edit.text().strip()
            if not department_name:
                raise ValueError("Department name is required")

            self.result = {
                "id": department_id,
                "name": department_name,
                "x": float(self.x_edit.text()),
                "y": float(self.y_edit.text()),
                "floor": int(self.department.get("floor", 0)),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid department", str(exc))


class DataPointEditorDialog(QDialog):
    def __init__(
        self,
        parent,
        seed=None,
        default_floor=0,
        default_x=0.0,
        default_y=0.0,
        default_name="",
        department_options=None,
        room_type_options=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Data Point")
        self.seed = seed or {}
        self.default_floor = default_floor
        self.default_x = default_x
        self.default_y = default_y
        self.room_type_options = list(room_type_options or [])
        self.department_options = list(department_options or [])
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.name_edit = QLineEdit(self.seed.get("name", default_name))
        self.x_edit = QLineEdit(str(self.seed.get("x", default_x)))
        self.y_edit = QLineEdit(str(self.seed.get("y", default_y)))
        self.qty_edit = QLineEdit(str(self.seed.get("qty", 1)))
        self.room_type_combo = QComboBox()
        self.room_type_combo.addItem("Manual / no room type", "")
        for room_type_id, room_type_name in self.room_type_options:
            label = f"{room_type_id} - {room_type_name}" if room_type_name else room_type_id
            self.room_type_combo.addItem(label, room_type_id)

        current_room_type = str(self.seed.get("room_type_id", "") or "").strip()
        idx = self.room_type_combo.findData(current_room_type)
        if idx >= 0:
            self.room_type_combo.setCurrentIndex(idx)
        self.extension_edit = QLineEdit(
            str(
                self.seed.get(
                    "extension_distance_m",
                    self.seed.get("distance_from_ceiling_m", 0.0),
                )
            )
        )

        self.departments_list = QListWidget()
        self.departments_list.setSelectionMode(QAbstractItemView.NoSelection)

        selected = {
            str(x).strip()
            for x in self.seed.get("department_ids", [])
            if str(x).strip()
        }

        for department_id, department_name in self.department_options:
            text = (
                f"{department_id} - {department_name}"
                if department_name
                else department_id
            )
            item = QListWidgetItem(text)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setData(Qt.UserRole, department_id)
            item.setCheckState(
                Qt.Checked if str(department_id).strip() in selected else Qt.Unchecked
            )
            self.departments_list.addItem(item)

        form.addRow("Name", self.name_edit)
        form.addRow("X", self.x_edit)
        form.addRow("Y", self.y_edit)
        form.addRow("Floor", QLabel(str(self.seed.get("floor", default_floor))))
        form.addRow("Qty", self.qty_edit)
        form.addRow("Room type", self.room_type_combo)
        form.addRow("Extension distance (m)", self.extension_edit)
        form.addRow("Departments", self.departments_list)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.resize(520, 500)

    def _checked_department_ids(self):
        result = []
        for i in range(self.departments_list.count()):
            item = self.departments_list.item(i)
            if item.checkState() == Qt.Checked:
                result.append(str(item.data(Qt.UserRole)).strip())
        return result

    def accept(self):
        try:
            name = self.name_edit.text().strip()
            if not name:
                raise ValueError("Name is required")
            self.result = {
                "name": name,
                "x": float(self.x_edit.text()),
                "y": float(self.y_edit.text()),
                "floor": int(self.seed.get("floor", self.default_floor)),
                "qty": int(self.qty_edit.text()),
                "extension_distance_m": float(self.extension_edit.text()),
                "department_ids": self._checked_department_ids(),
                "room_type_id": str(self.room_type_combo.currentData() or "").strip(),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid data point", str(exc))


class SuggestCommsRoomDialog(QDialog):
    def __init__(
        self, parent, data_point_options, default_name="", selected_data_points=None
    ):
        super().__init__(parent)
        self.setWindowTitle("Suggest Comms Room")
        self.result = None
        self.data_point_options = list(data_point_options)
        selected_data_points = set(selected_data_points or [])

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.data_points_list = QListWidget()
        self.data_points_list.setSelectionMode(QAbstractItemView.NoSelection)

        for item in self.data_point_options:
            name = item["name"]
            floor = item.get("floor", "")
            qty = item.get("qty", 1)

            label = f"{name} | Floor {floor} | Qty {qty}"

            row = QListWidgetItem(label)
            row.setFlags(row.flags() | Qt.ItemIsUserCheckable)
            row.setData(Qt.UserRole, name)
            row.setCheckState(
                Qt.Checked if name in selected_data_points else Qt.Unchecked
            )
            self.data_points_list.addItem(row)

        self.max_length_spin = QDoubleSpinBox()
        self.max_length_spin.setRange(0.0, 100000.0)
        self.max_length_spin.setDecimals(2)
        self.max_length_spin.setSingleStep(1.0)
        self.max_length_spin.setValue(90.0)

        self.search_mode_combo = QComboBox()
        self.search_mode_combo.addItems(
            [
                "Graph route length",
                "XY straight-line distance",
            ]
        )

        self.name_edit = QLineEdit(default_name)

        form.addRow("Data points", self.data_points_list)
        form.addRow("Search mode", self.search_mode_combo)
        form.addRow("Max cable length (m)", self.max_length_spin)
        form.addRow("New comms room name", self.name_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.resize(520, 500)

    def _checked_data_point_names(self):
        result = []
        for i in range(self.data_points_list.count()):
            item = self.data_points_list.item(i)
            if item.checkState() == Qt.Checked:
                result.append(str(item.data(Qt.UserRole)).strip())
        return result

    def accept(self):
        try:
            data_point_names = self._checked_data_point_names()
            if not data_point_names:
                raise ValueError("Select one or more data points")

            room_name = self.name_edit.text().strip()
            if not room_name:
                raise ValueError("New comms room name is required")

            self.result = {
                "data_point_names": data_point_names,
                "max_cable_length_m": float(self.max_length_spin.value()),
                "room_name": room_name,
                "search_mode": self.search_mode_combo.currentText(),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid input", str(exc))


class CommsRoomOptimisationProgressDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Optimising Comms Rooms")
        self.setWindowModality(Qt.ApplicationModal)
        self.setWindowFlag(Qt.WindowCloseButtonHint, False)
        self.setWindowFlag(Qt.WindowContextHelpButtonHint, False)
        self.setMinimumWidth(460)

        self._completed = False
        self._started_at = time.monotonic()

        layout = QVBoxLayout(self)

        self.message_label = QLabel("Preparing optimisation...")
        self.message_label.setWordWrap(True)
        layout.addWidget(self.message_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.detail_label = QLabel("0 / 0")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)

        self.rooms_label = QLabel("Rooms to place: calculating...")
        self.rooms_label.setWordWrap(True)
        layout.addWidget(self.rooms_label)

        self.eta_label = QLabel("ETA: calculating...")
        self.eta_label.setWordWrap(True)
        layout.addWidget(self.eta_label)

    def update_progress(
        self,
        current,
        total,
        message,
        rooms_to_place=None,
    ):
        total = max(1, int(total))
        current = max(0, min(int(current), total))

        self.progress_bar.setRange(0, total)
        self.progress_bar.setValue(current)
        self.message_label.setText(str(message))
        self.detail_label.setText(f"{current} / {total}")

        if rooms_to_place is None:
            self.rooms_label.setText("Rooms to place: calculating...")
        else:
            self.rooms_label.setText(f"Rooms to place: {int(rooms_to_place)}")

        if current <= 0:
            eta_text = "ETA: calculating..."
        else:
            elapsed = max(0.001, time.monotonic() - self._started_at)
            rate = elapsed / float(current)
            remaining = max(0.0, (total - current) * rate)

            mins = int(remaining // 60)
            secs = int(round(remaining % 60))
            if mins > 0:
                eta_text = f"ETA: {mins}m {secs:02d}s"
            else:
                eta_text = f"ETA: {secs}s"

        self.eta_label.setText(eta_text)

    def mark_complete(self, message="Finished"):
        self._completed = True
        self.message_label.setText(message)
        self.eta_label.setText("ETA: complete")
        self.accept()

    def reject(self):
        if self._completed:
            super().reject()

    def closeEvent(self, event):
        if self._completed:
            super().closeEvent(event)
        else:
            event.ignore()


class EdgeConnectionsDialog(QDialog):
    columns = [
        ("from", "From", 180),
        ("from_floor", "From floor", 90),
        ("to", "To", 180),
        ("to_floor", "To floor", 90),
        ("cross_floor", "Cross-floor", 90),
    ]

    def __init__(self, parent, point_name, edges, on_delete):
        super().__init__(parent)
        self.setWindowTitle(f"Edge Connections - {point_name}")
        self.resize(760, 420)
        self.point_name = point_name
        self.edges = list(edges)
        self.on_delete = on_delete

        layout = QVBoxLayout(self)
        self.summary_label = QLabel()
        layout.addWidget(self.summary_label)

        self.table = QTableWidget(0, len(self.columns))
        self.table.setHorizontalHeaderLabels(
            [heading for _key, heading, _width in self.columns]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        for idx, (_key, _heading, width) in enumerate(self.columns):
            self.table.setColumnWidth(idx, width)
        layout.addWidget(self.table, 1)

        button_row = QHBoxLayout()
        layout.addLayout(button_row)
        self.delete_btn = QPushButton("Delete selected")
        close_btn = QPushButton("Close")
        button_row.addWidget(self.delete_btn)
        button_row.addStretch(1)
        button_row.addWidget(close_btn)

        self.delete_btn.clicked.connect(self.delete_selected)
        close_btn.clicked.connect(self.accept)
        self._refresh_table()

    def _refresh_table(self):
        self.table.setRowCount(0)
        for edge in self.edges:
            row = self.table.rowCount()
            self.table.insertRow(row)
            values = [
                edge.get("from", ""),
                edge.get("from_floor", ""),
                edge.get("to", ""),
                edge.get("to_floor", ""),
                "Yes" if edge.get("cross_floor") else "No",
            ]
            for col, value in enumerate(values):
                self.table.setItem(row, col, QTableWidgetItem(str(value)))
        count = len(self.edges)
        if count == 0:
            self.summary_label.setText(f"No edge connections for {self.point_name}")
            self.delete_btn.setEnabled(False)
        else:
            cross_count = sum(1 for edge in self.edges if edge.get("cross_floor"))
            self.summary_label.setText(
                f"{count} connection(s) for {self.point_name} ({cross_count} cross-floor)"
            )
            self.delete_btn.setEnabled(True)

    def delete_selected(self):
        rows = sorted(
            {index.row() for index in self.table.selectionModel().selectedRows()}
        )
        if not rows:
            QMessageBox.information(
                self, "Delete edges", "Select one or more edge connections first."
            )
            return
        selected_edges = [self.edges[row] for row in rows]
        if (
            QMessageBox.question(
                self,
                "Delete edges",
                f"Delete {len(selected_edges)} selected edge connection(s)?",
            )
            != QMessageBox.Yes
        ):
            return
        self.on_delete(selected_edges)
        for row in reversed(rows):
            del self.edges[row]
        self._refresh_table()


class TransitionEditorDialog(QDialog):
    def __init__(
        self,
        parent,
        transition=None,
        default_floor=0,
        default_x=0.0,
        default_y=0.0,
        default_id="TR-1",
    ):
        super().__init__(parent)
        self.setWindowTitle("Transition Editor")
        self.result = None
        self.transition = transition or {}
        self.default_floor = int(default_floor)
        self.default_x = float(default_x)
        self.default_y = float(default_y)

        floors = self.transition.get("floors", [self.default_floor])
        floor_locations = self.transition.get("floor_locations", {})

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_edit = QLineEdit(self.transition.get("id", default_id))
        self.floors_edit = QLineEdit(", ".join(str(x) for x in floors))
        self.cable_limit_edit = QLineEdit(str(self.transition.get("cable_limit", 0)))

        self.base_position_label = QLabel(
            f"Floor {self.default_floor}: X={self.default_x:.3f}, Y={self.default_y:.3f}"
        )
        self.base_position_label.setWordWrap(True)

        self.positions_preview = QPlainTextEdit()
        self.positions_preview.setReadOnly(True)

        form.addRow("Transition ID", self.id_edit)
        form.addRow("Floors", self.floors_edit)
        form.addRow("Cable limit", self.cable_limit_edit)
        form.addRow("Base position", self.base_position_label)
        form.addRow("Generated positions", self.positions_preview)
        form.addRow(
            "",
            QLabel(
                "Per-floor position JSON is generated automatically.\n"
                "For new floors, the clicked position is reused.\n"
                "When editing, any existing saved floor positions are preserved."
            ),
        )

        self.floors_edit.textChanged.connect(self._refresh_positions_preview)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.resize(560, 420)

        self._refresh_positions_preview()

    def _parsed_floors(self):
        floors = [
            int(x.strip()) for x in self.floors_edit.text().split(",") if x.strip()
        ]
        deduped = []
        seen = set()
        for floor in floors:
            if floor not in seen:
                deduped.append(floor)
                seen.add(floor)
        return deduped

    def _build_floor_locations(self, floors):
        existing_locations = self.transition.get("floor_locations", {}) or {}
        payload = {}

        for floor in floors:
            existing = existing_locations.get(str(floor))
            if existing is None:
                existing = existing_locations.get(floor)

            if isinstance(existing, dict):
                x = float(existing.get("x", self.default_x))
                y = float(existing.get("y", self.default_y))
            elif isinstance(existing, (list, tuple)) and len(existing) >= 2:
                x = float(existing[0])
                y = float(existing[1])
            else:
                x = self.default_x
                y = self.default_y

            payload[int(floor)] = (round(x, 3), round(y, 3))

        return payload

    def _refresh_positions_preview(self):
        try:
            floors = self._parsed_floors()
            if not floors:
                self.positions_preview.setPlainText("")
                return

            payload = {
                int(floor): [x, y]
                for floor, (x, y) in self._build_floor_locations(floors).items()
            }
            self.positions_preview.setPlainText(json.dumps(payload, indent=2))
        except Exception:
            self.positions_preview.setPlainText("")

    def accept(self):
        try:
            transition_id = self.id_edit.text().strip()
            if not transition_id:
                raise ValueError("Transition ID is required")

            floors = self._parsed_floors()
            if not floors:
                raise ValueError("At least one floor is required")

            self.result = {
                "id": transition_id,
                "floors": floors,
                "cable_limit": int(self.cable_limit_edit.text()),
                "floor_locations": self._build_floor_locations(floors),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid transition", str(exc))

class RoomItemDialog(QDialog):
    def __init__(self, parent, seed=None):
        super().__init__(parent)
        self.setWindowTitle("Room Item")
        self.seed = seed or {}
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.name_edit = QLineEdit(str(self.seed.get("name", "")))

        self.qty_spin = QSpinBox()
        self.qty_spin.setRange(1, 100000)
        self.qty_spin.setValue(int(self.seed.get("qty", 1) or 1))

        self.data_points_spin = QSpinBox()
        self.data_points_spin.setRange(1, 100000)
        self.data_points_spin.setValue(
            int(
                self.seed.get(
                    "data_points",
                    self.seed.get("data_points_each", self.seed.get("cables", 1)),
                )
                or 1
            )
        )

        form.addRow("Item name", self.name_edit)
        form.addRow("Quantity", self.qty_spin)
        form.addRow("Data points per item", self.data_points_spin)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self):
        try:
            name = self.name_edit.text().strip()
            if not name:
                raise ValueError("Item name is required")

            self.result = {
                "name": name,
                "qty": int(self.qty_spin.value()),
                "data_points": int(self.data_points_spin.value()),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid room item", str(exc))
class AssetEditorDialog(QDialog):
    def __init__(self, parent, seed=None):
        super().__init__(parent)
        self.setWindowTitle("Asset")
        self.seed = seed or {}
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_edit = QLineEdit(str(self.seed.get("id", "")))
        self.name_edit = QLineEdit(str(self.seed.get("name", "")))

        self.qty_spin = QSpinBox()
        self.qty_spin.setRange(1, 100000)
        self.qty_spin.setValue(int(self.seed.get("qty", 1) or 1))

        self.data_points_spin = QSpinBox()
        self.data_points_spin.setRange(1, 100000)
        self.data_points_spin.setValue(
            int(
                self.seed.get(
                    "data_points",
                    self.seed.get("data_points_each", self.seed.get("cables", 1)),
                )
                or 1
            )
        )

        form.addRow("Asset ID", self.id_edit)
        form.addRow("Asset name", self.name_edit)
        form.addRow("Quantity", self.qty_spin)
        form.addRow("Data points per item", self.data_points_spin)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self):
        try:
            asset_id = self.id_edit.text().strip()
            name = self.name_edit.text().strip()
            if not asset_id:
                raise ValueError("Asset ID is required")
            if not name:
                raise ValueError("Asset name is required")

            self.result = {
                "id": asset_id,
                "name": name,
                "qty": int(self.qty_spin.value()),
                "data_points": int(self.data_points_spin.value()),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid asset", str(exc))


class AssetsEditorWindow(QMainWindow):
    def __init__(self, master, items, on_save):
        super().__init__(master)
        self.setWindowTitle("Assets")
        self.resize(820, 520)
        self.items = [dict(item) for item in items]
        self.on_save = on_save

        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(
            ["ID", "Name", "Qty", "Data points each", "Total"]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.doubleClicked.connect(self.edit_asset)
        layout.addWidget(self.table, 1)

        row = QHBoxLayout()
        layout.addLayout(row)
        add_btn = QPushButton("Add")
        edit_btn = QPushButton("Edit")
        delete_btn = QPushButton("Delete selected")
        save_btn = QPushButton("Save")

        add_btn.clicked.connect(self.add_asset)
        edit_btn.clicked.connect(self.edit_asset)
        delete_btn.clicked.connect(self.delete_assets)
        save_btn.clicked.connect(self.save)

        row.addWidget(add_btn)
        row.addWidget(edit_btn)
        row.addWidget(delete_btn)
        row.addStretch(1)
        row.addWidget(save_btn)

        self._refresh_table()
        self.show()

    def _refresh_table(self):
        self.table.setRowCount(0)
        for asset in self.items:
            qty = int(asset.get("qty", 1) or 1)
            dp = int(asset.get("data_points", 1) or 1)
            values = [asset.get("id", ""), asset.get("name", ""), qty, dp, qty * dp]

            row = self.table.rowCount()
            self.table.insertRow(row)
            for col, value in enumerate(values):
                self.table.setItem(row, col, QTableWidgetItem(str(value)))

    def add_asset(self):
        dialog = AssetEditorDialog(self)
        if dialog.exec() == QDialog.Accepted and dialog.result:
            if any(x.get("id") == dialog.result["id"] for x in self.items):
                QMessageBox.critical(self, "Duplicate", "Asset ID already exists.")
                return
            self.items.append(dialog.result)
            self._refresh_table()

    def edit_asset(self):
        rows = sorted({x.row() for x in self.table.selectionModel().selectedRows()})
        if not rows:
            return
        row = rows[0]
        dialog = AssetEditorDialog(self, self.items[row])
        if dialog.exec() == QDialog.Accepted and dialog.result:
            new_id = dialog.result["id"]
            for idx, asset in enumerate(self.items):
                if idx != row and asset.get("id") == new_id:
                    QMessageBox.critical(self, "Duplicate", "Asset ID already exists.")
                    return
            self.items[row] = dialog.result
            self._refresh_table()
            self.table.selectRow(row)

    def delete_assets(self):
        rows = sorted(
            {x.row() for x in self.table.selectionModel().selectedRows()},
            reverse=True,
        )
        if not rows:
            return
        if QMessageBox.question(
            self, "Delete assets", f"Delete {len(rows)} selected asset(s)?"
        ) != QMessageBox.Yes:
            return
        for row in rows:
            del self.items[row]
        self._refresh_table()

    def save(self):
        self.on_save(self.items)
        self.close()

class RoomTypeEditorDialog(QDialog):
    def __init__(self, parent, seed=None, asset_options=None, assets_by_id=None):
        super().__init__(parent)
        self.setWindowTitle("Room Type")
        self.resize(680, 520)
        self.seed = seed or {}
        self.asset_options = list(asset_options or [])
        self.assets_by_id = dict(assets_by_id or {})
        self.selected_asset_ids = {
            str(x).strip()
            for x in self.seed.get("asset_ids", [])
            if str(x).strip()
        }
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_edit = QLineEdit(str(self.seed.get("id", "")))
        self.name_edit = QLineEdit(str(self.seed.get("name", "")))

        form.addRow("Room type ID", self.id_edit)
        form.addRow("Room type name", self.name_edit)

        self.total_label = QLabel()
        layout.addWidget(self.total_label)

        self.assets_list = QListWidget()
        self.assets_list.setSelectionMode(QAbstractItemView.NoSelection)

        for asset_id, asset_name in self.asset_options:
            asset = self.assets_by_id.get(asset_id, {})
            qty = int(asset.get("qty", 1) or 1)
            dp = int(asset.get("data_points", 1) or 1)
            label = f"{asset_id} - {asset_name} | Qty {qty} | DP each {dp} | Total {qty * dp}"

            item = QListWidgetItem(label)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setData(Qt.UserRole, asset_id)
            item.setCheckState(
                Qt.Checked if asset_id in self.selected_asset_ids else Qt.Unchecked
            )
            self.assets_list.addItem(item)

        self.assets_list.itemChanged.connect(self._refresh_total)

        layout.addWidget(QLabel("Select assets required in this room type"))
        layout.addWidget(self.assets_list, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._refresh_total()

    def _checked_asset_ids(self):
        result = []
        for i in range(self.assets_list.count()):
            item = self.assets_list.item(i)
            if item.checkState() == Qt.Checked:
                result.append(str(item.data(Qt.UserRole)).strip())
        return result

    def _refresh_total(self):
        total = 0
        for asset_id in self._checked_asset_ids():
            asset = self.assets_by_id.get(asset_id, {})
            qty = int(asset.get("qty", 1) or 1)
            dp = int(asset.get("data_points", 1) or 1)
            total += qty * dp
        self.total_label.setText(f"Total data points / cables: {total}")

    def accept(self):
        try:
            room_type_id = self.id_edit.text().strip()
            name = self.name_edit.text().strip()
            if not room_type_id:
                raise ValueError("Room type ID is required")
            if not name:
                raise ValueError("Room type name is required")

            self.result = {
                "id": room_type_id,
                "name": name,
                "asset_ids": self._checked_asset_ids(),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid room type", str(exc))

class RoomTypesEditorWindow(QMainWindow):
    def __init__(self, master, items, on_save, asset_options=None, assets_by_id=None):
        super().__init__(master)
        self.setWindowTitle("Room Types")
        self.resize(900, 520)
        self.items = [dict(item) for item in items]
        self.on_save = on_save

        self.asset_options = list(asset_options or [])
        self.assets_by_id = dict(assets_by_id or {})

        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(
            ["ID", "Name", "Items", "Total data points"]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.doubleClicked.connect(self.edit_room_type)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.setColumnWidth(0, 160)
        self.table.setColumnWidth(1, 260)
        self.table.setColumnWidth(2, 100)
        self.table.setColumnWidth(3, 150)
        layout.addWidget(self.table, 1)

        buttons = QHBoxLayout()
        layout.addLayout(buttons)

        add_btn = QPushButton("Add")
        edit_btn = QPushButton("Edit")
        delete_btn = QPushButton("Delete selected")
        save_btn = QPushButton("Save")

        add_btn.clicked.connect(self.add_room_type)
        edit_btn.clicked.connect(self.edit_room_type)
        delete_btn.clicked.connect(self.delete_room_types)
        save_btn.clicked.connect(self.save)

        buttons.addWidget(add_btn)
        buttons.addWidget(edit_btn)
        buttons.addWidget(delete_btn)
        buttons.addStretch(1)
        buttons.addWidget(save_btn)

        self._refresh_table()
        self.show()

    def _room_total(self, room_type):
        total = 0
        for asset_id in room_type.get("asset_ids", []) or []:
            asset = self.assets_by_id.get(str(asset_id).strip())
            if not asset:
                continue
            qty = int(asset.get("qty", 1) or 1)
            dp = int(asset.get("data_points", 1) or 1)
            total += qty * dp
        return total

    def _refresh_table(self):
        self.table.setRowCount(0)

        for room_type in self.items:
            row = self.table.rowCount()
            self.table.insertRow(row)

            assets = room_type.get("asset_ids", []) or []
            values = [
                room_type.get("id", ""),
                room_type.get("name", ""),
                len(assets),
                self._room_total(room_type),
            ]

            for col, value in enumerate(values):
                self.table.setItem(row, col, QTableWidgetItem(str(value)))

    def add_room_type(self):
        dialog = RoomTypeEditorDialog(
            self,
            asset_options=self.asset_options,
            assets_by_id=self.assets_by_id,
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            new_id = dialog.result["id"]
            if any(str(item.get("id", "")).strip() == new_id for item in self.items):
                QMessageBox.critical(self, "Duplicate", "Room type ID already exists.")
                return
            self.items.append(dialog.result)
            self._refresh_table()

    def edit_room_type(self):
        rows = sorted(
            {index.row() for index in self.table.selectionModel().selectedRows()}
        )
        if not rows:
            return

        row = rows[0]
        old_id = str(self.items[row].get("id", "")).strip()
        dialog = RoomTypeEditorDialog(
            self,
            asset_options=self.asset_options,
            assets_by_id=self.assets_by_id,
        )

        if dialog.exec() == QDialog.Accepted and dialog.result:
            new_id = str(dialog.result["id"]).strip()

            for idx, item in enumerate(self.items):
                if idx == row:
                    continue
                if str(item.get("id", "")).strip() == new_id:
                    QMessageBox.critical(
                        self,
                        "Duplicate",
                        "Room type ID already exists.",
                    )
                    return

            self.items[row] = dialog.result
            self._refresh_table()
            self.table.selectRow(row)

    def delete_room_types(self):
        rows = sorted(
            {index.row() for index in self.table.selectionModel().selectedRows()},
            reverse=True,
        )
        if not rows:
            QMessageBox.information(
                self,
                "Delete room types",
                "Select one or more room types.",
            )
            return

        if (
            QMessageBox.question(
                self,
                "Delete room types",
                f"Delete {len(rows)} selected room type(s)?",
            )
            != QMessageBox.Yes
        ):
            return

        for row in rows:
            del self.items[row]

        self._refresh_table()

    def save(self):
        self.on_save(self.items)
        self.close()

class TableListEditor(QMainWindow):
    def __init__(self, master, title, columns, items, on_save):
        super().__init__(master)
        self.setWindowTitle(title)
        self.resize(1100, 500)
        self.columns = columns
        self.items = items
        self.on_save = on_save

        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        self.table = QTableWidget(0, len(columns))
        self.table.setHorizontalHeaderLabels([c[1] for c in columns])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        for idx, (_, _, width) in enumerate(columns):
            self.table.setColumnWidth(idx, width)
        layout.addWidget(self.table)

        button_row = QHBoxLayout()
        layout.addLayout(button_row)
        add_btn = QPushButton("Add")
        edit_btn = QPushButton("Edit")
        delete_btn = QPushButton("Delete")
        save_btn = QPushButton("Save")
        button_row.addWidget(add_btn)
        button_row.addWidget(edit_btn)
        button_row.addWidget(delete_btn)
        button_row.addStretch(1)
        button_row.addWidget(save_btn)

        add_btn.clicked.connect(self.add_item)
        edit_btn.clicked.connect(self.edit_item)
        delete_btn.clicked.connect(self.delete_item)
        save_btn.clicked.connect(self.save)

        self._refresh_table()
        self.show()

    @staticmethod
    def stringify(value: Any) -> str:
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        return str(value)

    def parse_value(self, value: str):
        value = value.strip()
        if value.startswith("[") or value.startswith("{"):
            return json.loads(value)
        if value == "":
            return ""
        try:
            if "." in value:
                return float(value)
            return int(value)
        except Exception:
            return value

    def prompt_item(self, seed=None):
        seed = seed or {}
        result = {}
        for key, heading, _ in self.columns:
            value, ok = QInputDialog.getText(
                self,
                self.windowTitle(),
                heading,
                text=self.stringify(seed.get(key, "")),
            )
            if not ok:
                return None
            result[key] = self.parse_value(value)
        return result

    def _refresh_table(self):
        self.table.setRowCount(0)
        for item in self.items:
            row = self.table.rowCount()
            self.table.insertRow(row)
            for col, (key, _heading, _width) in enumerate(self.columns):
                self.table.setItem(
                    row, col, QTableWidgetItem(self.stringify(item.get(key, "")))
                )

    def add_item(self):
        item = self.prompt_item()
        if item is None:
            return
        self.items.append(item)
        self._refresh_table()

    def edit_item(self):
        row = self.table.currentRow()
        if row < 0:
            return
        updated = self.prompt_item(self.items[row])
        if updated is None:
            return
        self.items[row] = updated
        self._refresh_table()
        self.table.selectRow(row)

    def delete_item(self):
        rows = sorted(
            {index.row() for index in self.table.selectionModel().selectedRows()},
            reverse=True,
        )

        if not rows:
            row = self.table.currentRow()
            if row < 0:
                return
            rows = [row]

        count = len(rows)

        if (
            QMessageBox.question(
                self,
                "Delete selected rows",
                f"Delete {count} selected row(s)?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            != QMessageBox.Yes
        ):
            return

        for row in rows:
            if 0 <= row < len(self.items):
                del self.items[row]

        self._refresh_table()

    def save(self):
        self.on_save(self.items)
        self.close()
