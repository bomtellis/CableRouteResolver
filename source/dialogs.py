import csv
import json
import re
from typing import Any
import time
from copy import deepcopy

from PySide6.QtCore import Qt, QSortFilterProxyModel
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
    QMenu,
    QCompleter,
    QFileDialog,
)


def suggest_next_id(items, prefix):
    used = {
        str(item.get("id", "")).strip()
        for item in items
        if str(item.get("id", "")).strip()
    }

    number = 1
    while f"{prefix}{number}" in used:
        number += 1

    return f"{prefix}{number}"


def _normalise_capability_keywords(value):
    if isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    elif value in (None, ""):
        raw_values = []
    else:
        text = str(value or "")
        for sep in ["\r", "\n", ",", "|", "/"]:
            text = text.replace(sep, ";")
        raw_values = text.split(";")

    keywords = []
    seen = set()
    for item in raw_values:
        keyword = re.sub(r"\s+", " ", str(item or "").strip()).lower()
        if keyword and keyword not in seen:
            keywords.append(keyword)
            seen.add(keyword)
    return keywords


def _capability_keywords_text(value):
    return "; ".join(_normalise_capability_keywords(value))


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
        self.kind_combo.addItems(
            ["location", "comms_room", "distributed_equipment_room"]
        )

        self.departments_list = QListWidget()
        self.departments_list.setSelectionMode(QAbstractItemView.NoSelection)

        for row in self.department_options:
            department_id = row[0]
            department_name = row[1]
            department_floor = row[2] if len(row) > 2 else None
            text = (
                f"{department_id} - {department_name} - Floor {department_floor}"
                if department_floor is not None and department_name
                else (
                    f"{department_id} - Floor {department_floor}"
                    if department_floor is not None
                    else (
                        f"{department_id} - {department_name}"
                        if department_name
                        else department_id
                    )
                )
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
        self.room_type_combo.setEditable(True)
        self.room_type_combo.setInsertPolicy(QComboBox.NoInsert)
        self.room_type_combo.setMaxVisibleItems(20)

        self.room_type_combo.addItem("Manual / no room type", "")

        for room_type_id, room_type_name in self.room_type_options:
            room_type_id = str(room_type_id).strip()
            room_type_name = str(room_type_name).strip()

            label = (
                f"{room_type_id} - {room_type_name}"
                if room_type_name
                else room_type_id
            )

            self.room_type_combo.addItem(label, room_type_id)

        proxy_model = QSortFilterProxyModel(self.room_type_combo)
        proxy_model.setSourceModel(self.room_type_combo.model())
        proxy_model.setFilterCaseSensitivity(Qt.CaseInsensitive)
        proxy_model.setFilterKeyColumn(0)

        completer = QCompleter(proxy_model, self.room_type_combo)
        completer.setCompletionMode(QCompleter.PopupCompletion)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)

        self.room_type_combo.setCompleter(completer)

        self.room_type_combo.lineEdit().textEdited.connect(
            proxy_model.setFilterFixedString
        )

        def _apply_room_type_completion(index):
            source_index = proxy_model.mapToSource(index)
            if not source_index.isValid():
                return

            self.room_type_combo.setCurrentIndex(source_index.row())

        completer.activated.connect(
            lambda _text: self.room_type_combo.setCurrentText(_text)
        )
        completer.highlighted.connect(
            lambda _text: self.room_type_combo.setCurrentText(_text)
        )

        current_room_type = str(self.seed.get("room_type_id", "") or "").strip()
        idx = self.room_type_combo.findData(current_room_type)
        if idx >= 0:
            self.room_type_combo.setCurrentIndex(idx)
        else:
            self.room_type_combo.setCurrentIndex(0)
        
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

    def _selected_room_type_id(self):
        text = self.room_type_combo.currentText().strip()

        if not text or text == "Manual / no room type":
            return ""

        current_data = self.room_type_combo.currentData()
        if current_data is not None:
            return str(current_data).strip()

        for idx in range(self.room_type_combo.count()):
            label = self.room_type_combo.itemText(idx).strip()
            room_type_id = str(self.room_type_combo.itemData(idx) or "").strip()

            if text == label:
                return room_type_id

            if room_type_id and text.lower() == room_type_id.lower():
                return room_type_id

            if room_type_id and text.lower().startswith(room_type_id.lower() + " -"):
                return room_type_id

        return ""

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
                "room_type_id": self._selected_room_type_id(),
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
        self.data_points_spin.setRange(0, 100000)
        self.data_points_spin.setValue(
            int(
                self.seed.get(
                    "data_points",
                    self.seed.get("data_points_each", self.seed.get("cables", 1)),
                )
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

class AssetCategoryEditorDialog(QDialog):
    def __init__(self, parent, seed=None, default_id="AC1"):
        super().__init__(parent)
        self.setWindowTitle("Asset Category")
        self.seed = seed or {}
        self.default_id = default_id
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_label = QLabel(str(self.seed.get("id", "") or self.default_id))
        self.name_edit = QLineEdit(str(self.seed.get("name", "")))

        form.addRow("Category ID", self.id_label)
        form.addRow("Category name", self.name_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self):
        try:
            category_id = str(self.seed.get("id", "") or self.default_id).strip()
            name = self.name_edit.text().strip()
            if not category_id:
                raise ValueError("Category ID could not be generated")
            if not name:
                raise ValueError("Category name is required")

            self.result = {
                "id": category_id,
                "name": name,
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid asset category", str(exc))

class AssetCategoriesEditorWindow(QMainWindow):
    def __init__(self, master, items, on_save):
        super().__init__(master)
        self.setWindowTitle("Asset Categories")
        self.resize(620, 420)
        self.items = [dict(item) for item in items]
        self.on_save = on_save

        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        self.table = QTableWidget(0, 2)
        self.table.setHorizontalHeaderLabels(["ID", "Name"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.doubleClicked.connect(self.edit_category)
        layout.addWidget(self.table, 1)

        row = QHBoxLayout()
        layout.addLayout(row)

        add_btn = QPushButton("Add")
        edit_btn = QPushButton("Edit")
        copy_btn = QPushButton("Copy")
        delete_btn = QPushButton("Delete selected")
        save_btn = QPushButton("Save")

        add_btn.clicked.connect(self.add_category)
        edit_btn.clicked.connect(self.edit_category)
        copy_btn.clicked.connect(self.copy_category)
        delete_btn.clicked.connect(self.delete_categories)
        save_btn.clicked.connect(self.save)

        row.addWidget(add_btn)
        row.addWidget(edit_btn)
        row.addWidget(copy_btn)
        row.addWidget(delete_btn)
        row.addStretch(1)
        row.addWidget(save_btn)

        self._refresh_table()
        self.show()

    def _refresh_table(self):
        self.table.setRowCount(0)
        for item in self.items:
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, QTableWidgetItem(str(item.get("id", ""))))
            self.table.setItem(row, 1, QTableWidgetItem(str(item.get("name", ""))))

    def add_category(self):
        dialog = AssetCategoryEditorDialog(self, default_id=suggest_next_id(self.items, "AC"))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self.items.append(dialog.result)
            self._refresh_table()

    def edit_category(self):
        rows = sorted({x.row() for x in self.table.selectionModel().selectedRows()})
        if not rows:
            return
        row = rows[0]
        dialog = AssetCategoryEditorDialog(self, self.items[row])
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self.items[row] = dialog.result
            self._refresh_table()
            self.table.selectRow(row)

    def copy_category(self):
        rows = sorted({x.row() for x in self.table.selectionModel().selectedRows()})
        if not rows:
            return
        source = dict(self.items[rows[0]])
        source["id"] = suggest_next_id(self.items, "AC")
        source["name"] = f"{source.get('name', '')} Copy".strip()
        dialog = AssetCategoryEditorDialog(self, source)
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self.items.append(dialog.result)
            self._refresh_table()

    def delete_categories(self):
        rows = sorted({x.row() for x in self.table.selectionModel().selectedRows()}, reverse=True)
        if not rows:
            return
        if QMessageBox.question(self, "Delete asset categories", f"Delete {len(rows)} selected categor(ies)?") != QMessageBox.Yes:
            return
        for row in rows:
            del self.items[row]
        self._refresh_table()

    def save(self):
        self.on_save(self.items)
        self.close()

class AssetEditorDialog(QDialog):
    def __init__(self, parent, seed=None, default_id="A1", category_options=None):
        super().__init__(parent)
        self.setWindowTitle("Asset")
        self.seed = seed or {}
        self.default_id = default_id
        self.category_options = list(category_options or [])
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_label = QLabel(str(self.seed.get("id", "") or self.default_id))
        self.name_edit = QLineEdit(str(self.seed.get("name", "")))
        self.adb_code_edit = QLineEdit(
            str(self.seed.get("ADB_Code", self.seed.get("adb_code", "")) or "")
        )
        self.group_edit = QLineEdit(
            str(self.seed.get("Group", self.seed.get("group", "")) or "")
        )
        self.capability_keywords_edit = QPlainTextEdit(
            _capability_keywords_text(
                self.seed.get(
                    "capability_keywords",
                    self.seed.get("capabilities", self.seed.get("function_keywords", "")),
                )
            )
        )
        self.capability_keywords_edit.setPlaceholderText(
            "Enter capability/function keywords separated by semicolons, commas or new lines. "
            "Example: patient entertainment; nurse call; wayfinding"
        )
        self.capability_keywords_edit.setFixedHeight(72)
        self.qty_spin = QSpinBox()
        self.qty_spin.setRange(1, 100000)
        self.qty_spin.setValue(int(self.seed.get("qty", 1) or 1))

        self.data_points_spin = QSpinBox()
        self.data_points_spin.setRange(0, 100000)

        data_points = self.seed.get(
            "data_points",
            self.seed.get("data_points_each", self.seed.get("cables", 1)),
        )

        try:
            data_points = int(data_points)
        except (TypeError, ValueError):
            data_points = 1

        self.data_points_spin.setValue(data_points)

        self.connection_type_combo = QComboBox()
        self.connection_type_combo.addItems(["wired", "wireless"])
        self.connection_type_combo.setCurrentText(
            str(self.seed.get("connection_type", self.seed.get("type_of_connection", "wired")) or "wired")
        )

        self.category_combo = QComboBox()
        self.category_combo.addItem("Uncategorised", "")
        for category_id, category_name in self.category_options:
            label = f"{category_id} - {category_name}" if category_name else category_id
            self.category_combo.addItem(label, category_id)

        current_category = str(self.seed.get("category_id", self.seed.get("category", "")) or "").strip()
        idx = self.category_combo.findData(current_category)
        if idx >= 0:
            self.category_combo.setCurrentIndex(idx)

        form.addRow("Asset ID", self.id_label)
        form.addRow("Asset name", self.name_edit)
        form.addRow("ADB_Code", self.adb_code_edit)
        form.addRow("Group", self.group_edit)
        form.addRow("Capability / function keywords", self.capability_keywords_edit)
        form.addRow("Connection type", self.connection_type_combo)
        form.addRow("Category", self.category_combo)
        form.addRow("Quantity", self.qty_spin)
        form.addRow("Data points per item", self.data_points_spin)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self):
        try:
            asset_id = str(self.seed.get("id", "") or self.default_id).strip()
            name = self.name_edit.text().strip()

            if not asset_id:
                raise ValueError("Asset ID could not be generated")
            if not name:
                raise ValueError("Asset name is required")

            capability_keywords = _capability_keywords_text(self.capability_keywords_edit.toPlainText())
            self.result = {
                "id": asset_id,
                "name": name,
                "ADB_Code": self.adb_code_edit.text().strip(),
                "Group": self.group_edit.text().strip(),
                "scenario_group": str(self.seed.get("scenario_group", self.seed.get("asset_scenario_group", "")) or "").strip(),
                "capability_keywords": capability_keywords,
                "capabilities": _normalise_capability_keywords(capability_keywords),
                "connection_type": self.connection_type_combo.currentText().strip(),
                "category_id": str(self.category_combo.currentData() or "").strip(),
                "qty": int(self.qty_spin.value()),
                "data_points": int(self.data_points_spin.value()),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid asset", str(exc))


class AssetDeploymentLocationsDialog(QDialog):
    def __init__(self, parent, asset, rows, on_navigate=None):
        super().__init__(parent)
        self.asset = dict(asset or {})
        self.rows = [dict(row) for row in rows or []]
        self.on_navigate = on_navigate
        asset_name = str(self.asset.get("name", self.asset.get("id", "Asset")) or "Asset")
        self.setWindowTitle(f"Rooms using asset - {asset_name}")
        self.resize(1080, 600)

        layout = QVBoxLayout(self)

        asset_id = str(self.asset.get("id", "") or "").strip()
        room_type_keys = {
            (
                str(row.get("room_type_id", "") or "").strip(),
                str(row.get("room_type_name", "") or "").strip(),
            )
            for row in self.rows
        }
        self.summary = QLabel(
            f"{asset_name}"
            + (f" ({asset_id})" if asset_id else "")
            + f" is deployed in {len(self.rows)} placed room/data-point instance(s)"
            + f" across {len(room_type_keys)} room type(s)."
        )
        self.summary.setWordWrap(True)
        layout.addWidget(self.summary)

        search_row = QHBoxLayout()
        layout.addLayout(search_row)
        search_row.addWidget(QLabel("Search"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText(
            "Filter by room, room type, floor, quantity, data points or department..."
        )
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.textChanged.connect(self._apply_filter)
        search_row.addWidget(self.search_edit, 1)
        self.combine_room_types_check = QCheckBox("Combine room types")
        self.combine_room_types_check.setToolTip(
            "Show one summary row per room type instead of every placed room/data point."
        )
        self.combine_room_types_check.toggled.connect(self._populate)
        search_row.addWidget(self.combine_room_types_check)
        self.match_label = QLabel()
        search_row.addWidget(self.match_label)

        self.table = QTableWidget(0, 9)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.itemDoubleClicked.connect(self._navigate_selected_row)
        layout.addWidget(self.table, 1)

        button_row = QHBoxLayout()
        layout.addLayout(button_row)
        self.navigate_btn = QPushButton("Navigate to selected room")
        close_btn = QPushButton("Close")
        self.navigate_btn.clicked.connect(self._navigate_selected_row)
        close_btn.clicked.connect(self.close)
        button_row.addWidget(self.navigate_btn)
        button_row.addStretch(1)
        button_row.addWidget(close_btn)

        self._populate()

    @staticmethod
    def _fmt_coord(value):
        try:
            return f"{float(value):.3f}"
        except (TypeError, ValueError):
            return str(value or "")

    @staticmethod
    def _safe_int(value, default=0):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _natural_join(values):
        cleaned = []
        seen = set()
        for value in values:
            text = str(value or "").strip()
            if not text:
                continue
            key = text.casefold()
            if key in seen:
                continue
            cleaned.append(text)
            seen.add(key)
        return ", ".join(sorted(cleaned, key=str.casefold))

    def _room_type_label(self, room_type_id, room_type_name):
        room_type_id = str(room_type_id or "").strip()
        room_type_name = str(room_type_name or "").strip()
        if room_type_id and room_type_name:
            return f"{room_type_id} - {room_type_name}"
        return room_type_id or room_type_name

    def _configure_table_for_detail_mode(self):
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels(
            [
                "Room / data point",
                "Floor",
                "Room type",
                "Qty per room",
                "Data points",
                "Data points each",
                "X",
                "Y",
                "Departments",
            ]
        )
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.Stretch)
        self.navigate_btn.setText("Navigate to selected room")

    def _configure_table_for_combined_mode(self):
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels(
            [
                "Room type",
                "Placed rooms",
                "Floors",
                "Qty per room",
                "Deployed items",
                "Data points",
                "Data points each",
                "Departments",
                "Example room",
            ]
        )
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.Stretch)
        header.setSectionResizeMode(8, QHeaderView.Stretch)
        self.navigate_btn.setText("Navigate to example room")

    def _make_item(self, value, search_text=None, navigate_room=None, numeric=False, align_right=False):
        item = QTableWidgetItem(str(value))
        if search_text is not None:
            item.setData(Qt.UserRole + 1, search_text)
        if navigate_room is not None:
            item.setData(Qt.UserRole, navigate_room)
        if numeric:
            try:
                item.setData(Qt.DisplayRole, int(value))
            except (TypeError, ValueError):
                pass
            align_right = True
        if align_right:
            item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        return item

    def _populate(self, *_):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)

        if self.combine_room_types_check.isChecked():
            self._configure_table_for_combined_mode()
            self._populate_combined_rows()
        else:
            self._configure_table_for_detail_mode()
            self._populate_detail_rows()

        self.table.setSortingEnabled(True)
        self.table.sortItems(0, Qt.AscendingOrder)
        self._apply_filter()

    def _populate_detail_rows(self):
        for record in self.rows:
            row = self.table.rowCount()
            self.table.insertRow(row)

            room_name = str(record.get("room_name", "") or "").strip()
            room_type_id = str(record.get("room_type_id", "") or "").strip()
            room_type_name = str(record.get("room_type_name", room_type_id) or room_type_id).strip()
            room_type_label = self._room_type_label(room_type_id, room_type_name)
            departments = self._natural_join(record.get("department_ids", []) or [])
            values = [
                room_name,
                str(record.get("floor", "") or ""),
                room_type_label,
                str(record.get("deployed_items", record.get("qty_per_room", 0)) or 0),
                str(record.get("deployed_data_points", 0) or 0),
                str(record.get("data_points_each", 0) or 0),
                self._fmt_coord(record.get("x", "")),
                self._fmt_coord(record.get("y", "")),
                departments,
            ]
            search_text = " ".join(values + [room_type_id, room_type_name]).casefold()

            for col, value in enumerate(values):
                item = self._make_item(
                    value,
                    search_text=search_text if col == 0 else None,
                    navigate_room=room_name if col == 0 else None,
                    numeric=col in (1, 3, 4, 5),
                )
                self.table.setItem(row, col, item)

    def _combined_records(self):
        grouped = {}
        for record in self.rows:
            room_type_id = str(record.get("room_type_id", "") or "").strip()
            room_type_name = str(record.get("room_type_name", room_type_id) or room_type_id).strip()
            key = (room_type_id, room_type_name)
            entry = grouped.setdefault(
                key,
                {
                    "room_type_id": room_type_id,
                    "room_type_name": room_type_name,
                    "room_names": [],
                    "floors": [],
                    "qty_values": [],
                    "deployed_items": 0,
                    "deployed_data_points": 0,
                    "data_points_each_values": [],
                    "departments": [],
                    "example_room": "",
                },
            )

            room_name = str(record.get("room_name", "") or "").strip()
            if room_name:
                entry["room_names"].append(room_name)
                if not entry["example_room"]:
                    entry["example_room"] = room_name

            floor = record.get("floor", "")
            if str(floor).strip() != "":
                entry["floors"].append(floor)

            qty_per_room = record.get("deployed_items", record.get("qty_per_room", 0))
            entry["qty_values"].append(qty_per_room)
            entry["deployed_items"] += self._safe_int(qty_per_room, 0)
            entry["deployed_data_points"] += self._safe_int(record.get("deployed_data_points", 0), 0)
            entry["data_points_each_values"].append(record.get("data_points_each", 0))
            entry["departments"].extend(record.get("department_ids", []) or [])

        return list(grouped.values())

    def _populate_combined_rows(self):
        for record in self._combined_records():
            row = self.table.rowCount()
            self.table.insertRow(row)

            room_type_label = self._room_type_label(
                record.get("room_type_id", ""),
                record.get("room_type_name", ""),
            )
            placed_rooms = len(record.get("room_names", []) or [])
            floors = self._natural_join(record.get("floors", []) or [])
            qty_values = self._natural_join(record.get("qty_values", []) or [])
            data_points_each = self._natural_join(record.get("data_points_each_values", []) or [])
            departments = self._natural_join(record.get("departments", []) or [])
            example_room = str(record.get("example_room", "") or "").strip()
            values = [
                room_type_label,
                str(placed_rooms),
                floors,
                qty_values,
                str(record.get("deployed_items", 0) or 0),
                str(record.get("deployed_data_points", 0) or 0),
                data_points_each,
                departments,
                example_room,
            ]
            search_text = " ".join(
                values
                + [
                    str(record.get("room_type_id", "") or ""),
                    str(record.get("room_type_name", "") or ""),
                    " ".join(record.get("room_names", []) or []),
                ]
            ).casefold()

            for col, value in enumerate(values):
                item = self._make_item(
                    value,
                    search_text=search_text if col == 0 else None,
                    navigate_room=example_room if col == 0 else None,
                    numeric=col in (1, 4, 5),
                    align_right=col in (3, 6),
                )
                self.table.setItem(row, col, item)

    def _apply_filter(self, *_):
        terms = [term for term in self.search_edit.text().casefold().split() if term]
        visible_count = 0
        for row in range(self.table.rowCount()):
            first_item = self.table.item(row, 0)
            haystack = str(first_item.data(Qt.UserRole + 1) or "") if first_item else ""
            visible = all(term in haystack for term in terms)
            self.table.setRowHidden(row, not visible)
            if visible:
                visible_count += 1

        total = self.table.rowCount()
        unit = "room types" if self.combine_room_types_check.isChecked() else "rooms"
        self.match_label.setText(f"{visible_count} of {total} {unit}")

    def _navigate_selected_row(self, *_):
        row = self.table.currentRow()
        if row < 0 or self.table.isRowHidden(row):
            return
        item = self.table.item(row, 0)
        room_name = str(item.data(Qt.UserRole) or "").strip() if item else ""
        if not room_name and not self.combine_room_types_check.isChecked():
            room_name = str(item.text() or "").strip() if item else ""
        if not room_name:
            return
        if callable(self.on_navigate):
            self.on_navigate(room_name)


class AssetCapabilityOverlapDialog(QDialog):
    """Show a capability matrix and where overlapping asset functions occur."""

    def __init__(self, parent, rows, assets):
        super().__init__(parent)
        self.setWindowTitle("Asset Capability Overlap Matrix")
        self.resize(1380, 720)
        self.rows = [dict(row) for row in rows or []]
        self.assets = [dict(asset) for asset in assets or []]
        self.asset_lookup = {
            str(asset.get("id", "") or "").strip(): str(asset.get("name", asset.get("id", "")) or "").strip()
            for asset in self.assets
            if str(asset.get("id", "") or "").strip()
        }
        self.matrix_asset_ids = self._matrix_asset_ids()

        layout = QVBoxLayout(self)
        self.summary_label = QLabel()
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        filter_row = QHBoxLayout()
        layout.addLayout(filter_row)
        filter_row.addWidget(QLabel("Search"))
        self.search_edit = QLineEdit()
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.setPlaceholderText("Filter by capability, asset, room type or room name...")
        self.search_edit.textChanged.connect(self._apply_filter)
        filter_row.addWidget(self.search_edit, 1)
        self.only_overlaps_check = QCheckBox("Only shared capabilities")
        self.only_overlaps_check.setChecked(True)
        self.only_overlaps_check.toggled.connect(self._populate)
        filter_row.addWidget(self.only_overlaps_check)
        self.only_deployed_overlap_check = QCheckBox("Only overlaps deployed in same room type")
        self.only_deployed_overlap_check.toggled.connect(self._populate)
        filter_row.addWidget(self.only_deployed_overlap_check)
        self.count_label = QLabel()
        self.count_label.setMinimumWidth(150)
        self.count_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        filter_row.addWidget(self.count_label)

        self.table = QTableWidget(0, 0)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSortingEnabled(True)
        layout.addWidget(self.table, 1)

        note = QLabel(
            "Capability keywords are edited on each endpoint asset. A shared capability means more than one asset has the same keyword. "
            "A deployed overlap means two or more of those assets are assigned to the same room type; placed rooms are then listed as overlap locations."
        )
        note.setWordWrap(True)
        layout.addWidget(note)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(self.reject)
        buttons.accepted.connect(self.accept)
        layout.addWidget(buttons)
        self._populate()

    def _matrix_asset_ids(self):
        asset_ids = []
        seen = set()
        for row in self.rows:
            for asset_id in row.get("asset_ids", []) or []:
                asset_id = str(asset_id or "").strip()
                if asset_id and asset_id not in seen:
                    asset_ids.append(asset_id)
                    seen.add(asset_id)
        return sorted(asset_ids, key=lambda aid: (self.asset_lookup.get(aid, aid).casefold(), aid.casefold()))

    @staticmethod
    def _join(values, limit=16):
        cleaned = []
        seen = set()
        for value in values or []:
            text = str(value or "").strip()
            if text and text.casefold() not in seen:
                cleaned.append(text)
                seen.add(text.casefold())
        suffix = "" if len(cleaned) <= limit else f" + {len(cleaned) - limit} more"
        return "; ".join(cleaned[:limit]) + suffix

    def _row_matches_mode(self, row):
        if self.only_overlaps_check.isChecked() and int(row.get("asset_count", 0) or 0) < 2:
            return False
        if self.only_deployed_overlap_check.isChecked() and not row.get("overlap_room_types"):
            return False
        return True

    def _populate(self, *_):
        self.table.setSortingEnabled(False)
        base_headers = [
            "Capability / function keyword",
            "Asset count",
            "Deployed assets",
            "Assets providing capability",
            "Overlap room types",
            "Overlap placed rooms",
        ]
        asset_headers = [
            f"{asset_id}\n{self.asset_lookup.get(asset_id, asset_id)}" for asset_id in self.matrix_asset_ids
        ]
        self.table.setColumnCount(len(base_headers) + len(asset_headers))
        self.table.setHorizontalHeaderLabels(base_headers + asset_headers)
        self.table.setRowCount(0)

        rows_to_show = [row for row in self.rows if self._row_matches_mode(row)]
        for row_data in rows_to_show:
            row = self.table.rowCount()
            self.table.insertRow(row)
            asset_ids = {str(asset_id) for asset_id in row_data.get("asset_ids", []) or []}
            deployed_asset_ids = {str(asset_id) for asset_id in row_data.get("deployed_asset_ids", []) or []}
            asset_labels = row_data.get("asset_labels", []) or []
            overlap_room_types = row_data.get("overlap_room_types", []) or []
            overlap_rooms = row_data.get("overlap_rooms", []) or []
            values = [
                str(row_data.get("capability", "") or ""),
                str(row_data.get("asset_count", 0) or 0),
                str(row_data.get("deployed_asset_count", 0) or 0),
                self._join(asset_labels),
                self._join(overlap_room_types),
                self._join(overlap_rooms, limit=24),
            ]
            search_text = " ".join(values + list(asset_ids)).casefold()
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col == 0:
                    item.setData(Qt.UserRole, search_text)
                if col in (1, 2):
                    try:
                        item.setData(Qt.DisplayRole, int(value))
                    except (TypeError, ValueError):
                        pass
                    item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.table.setItem(row, col, item)

            for offset, asset_id in enumerate(self.matrix_asset_ids, start=len(base_headers)):
                value = ""
                if asset_id in asset_ids:
                    value = "✓ deployed" if asset_id in deployed_asset_ids else "✓"
                item = QTableWidgetItem(value)
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(row, offset, item)

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeToContents)
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        header.setSectionResizeMode(4, QHeaderView.Stretch)
        header.setSectionResizeMode(5, QHeaderView.Stretch)
        self.table.setSortingEnabled(True)
        self.table.sortItems(0, Qt.AscendingOrder)
        shared_count = sum(1 for row in self.rows if int(row.get("asset_count", 0) or 0) >= 2)
        deployed_overlap_count = sum(1 for row in self.rows if row.get("overlap_room_types"))
        self.summary_label.setText(
            f"Capabilities found: {len(self.rows)} | Shared capabilities: {shared_count} | "
            f"Capabilities overlapping within deployed room types: {deployed_overlap_count}."
        )
        self._apply_filter()

    def _apply_filter(self, *_):
        terms = [term for term in self.search_edit.text().casefold().split() if term]
        visible = 0
        for row in range(self.table.rowCount()):
            first = self.table.item(row, 0)
            haystack = str(first.data(Qt.UserRole) or "") if first else ""
            is_visible = all(term in haystack for term in terms)
            self.table.setRowHidden(row, not is_visible)
            if is_visible:
                visible += 1
        self.count_label.setText(f"{visible} of {self.table.rowCount()} rows")


class AssetsEditorWindow(QMainWindow):
    def __init__(
        self,
        master,
        items,
        on_save,
        category_options=None,
        deployment_summary=None,
        deployment_locations=None,
        on_navigate_to_room=None,
        on_show_capability_overlap=None,
    ):
        super().__init__(master)
        self.setWindowTitle("Assets")
        self.resize(1380, 560)
        self.items = [dict(item) for item in items]
        self.on_save = on_save
        self.deployment_summary = dict(deployment_summary or {})
        self.deployment_locations = dict(deployment_locations or {})
        self.on_navigate_to_room = on_navigate_to_room
        self.on_show_capability_overlap = on_show_capability_overlap

        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        self.category_options = list(category_options or [])
        self.categories_by_id = {category_id: name for category_id, name in self.category_options}

        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("Search assets"))

        self.asset_search_edit = QLineEdit()
        self.asset_search_edit.setPlaceholderText(
            "Type to filter by asset name, ID, ADB code, group, capability, category, connection type, or deployed count..."
        )
        self.asset_search_edit.setClearButtonEnabled(True)
        search_row.addWidget(self.asset_search_edit, 1)

        self.asset_filter_count_label = QLabel()
        self.asset_filter_count_label.setMinimumWidth(110)
        self.asset_filter_count_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        search_row.addWidget(self.asset_filter_count_label)
        layout.addLayout(search_row)

        self.table = QTableWidget(0, 12)
        self.table.setHorizontalHeaderLabels(
            [
                "Name",
                "ADB_Code",
                "Group",
                "Connection",
                "Category",
                "Library Qty",
                "Data points each",
                "Library total",
                "Capabilities",
                "Deployed rooms",
                "Deployed items",
                "Deployed data points",
            ]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.doubleClicked.connect(self.edit_asset)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)
        layout.addWidget(self.table, 1)
        self.asset_search_edit.textChanged.connect(self._apply_asset_filter)

        row = QHBoxLayout()
        layout.addLayout(row)
        add_btn = QPushButton("Add")
        edit_btn = QPushButton("Edit")
        delete_btn = QPushButton("Delete selected")
        capability_btn = QPushButton("Capability overlap")
        save_btn = QPushButton("Save")

        add_btn.clicked.connect(self.add_asset)
        edit_btn.clicked.connect(self.edit_asset)
        delete_btn.clicked.connect(self.delete_assets)
        capability_btn.clicked.connect(self.show_capability_overlap)
        save_btn.clicked.connect(self.save)

        row.addWidget(add_btn)
        row.addWidget(edit_btn)
        row.addWidget(delete_btn)
        row.addStretch(1)
        row.addWidget(capability_btn)
        row.addWidget(save_btn)

        self._refresh_table()
        self.show()

    def _refresh_table(self):
        self.table.setRowCount(0)

        for asset in self.items:
            row = self.table.rowCount()
            self.table.insertRow(row)

            qty = int(asset.get("qty", 1) or 1)
            dp_value = asset.get(
                "data_points",
                asset.get(
                    "data_points_each",
                    asset.get("cables", 1),
                ),
            )

            try:
                dp = int(dp_value)
            except (TypeError, ValueError):
                dp = 1

            category_id = str(
                asset.get(
                    "category_id",
                    asset.get("category", ""),
                )
                or ""
            ).strip()

            category_name = self.categories_by_id.get(
                category_id,
                category_id,
            )

            asset_id = str(asset.get("id", "") or "").strip()
            deployed = self.deployment_summary.get(asset_id, {})
            capabilities = _capability_keywords_text(
                asset.get(
                    "capability_keywords",
                    asset.get("capabilities", asset.get("function_keywords", "")),
                )
            )
            deployed_rooms = int(deployed.get("deployed_rooms", 0) or 0)
            deployed_items = int(deployed.get("deployed_items", 0) or 0)
            deployed_data_points = int(deployed.get("deployed_data_points", 0) or 0)

            values = [
                str(asset.get("name", "")),
                str(asset.get("ADB_Code", asset.get("adb_code", "")) or ""),
                str(asset.get("Group", asset.get("group", "")) or ""),
                str(
                    asset.get(
                        "connection_type",
                        asset.get("type_of_connection", "wired"),
                    )
                ),
                str(category_name),
                str(qty),
                str(dp),
                str(qty * dp),
                capabilities,
                str(deployed_rooms),
                str(deployed_items),
                str(deployed_data_points),
            ]

            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col in (9, 10, 11):
                    item.setToolTip(
                        "Calculated from placed data points with a room type. "
                        "Deployed items = placed rooms using this asset × quantity in the room type."
                    )
                self.table.setItem(row, col, item)

        self.table.resizeColumnsToContents()
        self._apply_asset_filter()

    def _asset_search_text(self, asset):
        asset_id = str(asset.get("id", "") or "").strip()
        asset_name = str(asset.get("name", "") or "").strip()
        connection_type = str(
            asset.get(
                "connection_type",
                asset.get("type_of_connection", "wired"),
            )
            or ""
        ).strip()
        category_id = str(
            asset.get("category_id", asset.get("category", "")) or ""
        ).strip()
        category_name = str(
            self.categories_by_id.get(category_id, category_id) or ""
        ).strip()
        adb_code = str(
            asset.get("ADB_Code", asset.get("adb_code", "")) or ""
        ).strip()
        group = str(
            asset.get("Group", asset.get("group", "")) or ""
        ).strip()
        capabilities = _capability_keywords_text(
            asset.get(
                "capability_keywords",
                asset.get("capabilities", asset.get("function_keywords", "")),
            )
        )
        deployed = self.deployment_summary.get(asset_id, {})
        deployed_terms = [
            str(deployed.get("deployed_rooms", 0) or 0),
            str(deployed.get("deployed_items", 0) or 0),
            str(deployed.get("deployed_data_points", 0) or 0),
        ]
        deployed_terms.extend(
            str(room_type_id) for room_type_id in deployed.get("room_type_ids", [])
        )
        return " ".join(
            (
                asset_id,
                asset_name,
                adb_code,
                group,
                capabilities,
                connection_type,
                category_id,
                category_name,
                *deployed_terms,
            )
        ).casefold()

    def _apply_asset_filter(self, *_):
        if _:
            self.table.clearSelection()

        terms = [
            term
            for term in self.asset_search_edit.text().casefold().split()
            if term
        ]
        visible_count = 0

        for row, asset in enumerate(self.items):
            searchable = self._asset_search_text(asset)
            matches = all(term in searchable for term in terms)
            self.table.setRowHidden(row, not matches)
            if matches:
                visible_count += 1

        total = len(self.items)
        if terms:
            self.asset_filter_count_label.setText(f"{visible_count} of {total} assets")
        else:
            self.asset_filter_count_label.setText(f"{total} assets")

    def add_asset(self):
        dialog = AssetEditorDialog(
            self,
            default_id=suggest_next_id(self.items, "A"),
            category_options=self.category_options,
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self.items.append(dialog.result)
            self._refresh_table()

    def edit_asset(self):
        rows = sorted({x.row() for x in self.table.selectionModel().selectedRows()})
        if not rows:
            return
        row = rows[0]
        dialog = AssetEditorDialog(
            self,
            self.items[row],
            category_options=self.category_options,
        )
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
        if (
            QMessageBox.question(
                self, "Delete assets", f"Delete {len(rows)} selected asset(s)?"
            )
            != QMessageBox.Yes
        ):
            return
        for row in rows:
            del self.items[row]
        self._refresh_table()

    def _selected_asset_row(self):
        rows = sorted({x.row() for x in self.table.selectionModel().selectedRows()})
        if not rows:
            return -1
        return rows[0]

    def _selected_asset(self):
        row = self._selected_asset_row()
        if row < 0 or row >= len(self.items):
            return None
        return self.items[row]

    def show_selected_asset_rooms(self):
        asset = self._selected_asset()
        if not asset:
            return

        asset_id = str(asset.get("id", "") or "").strip()
        rows = list(self.deployment_locations.get(asset_id, []) or [])
        if not rows:
            QMessageBox.information(
                self,
                "Find rooms",
                "No placed rooms/data points currently use this asset through a room type assignment.",
            )
            return

        dialog = AssetDeploymentLocationsDialog(
            self,
            asset,
            rows,
            on_navigate=self.on_navigate_to_room,
        )
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()

    def show_capability_overlap(self):
        if callable(self.on_show_capability_overlap):
            self.on_show_capability_overlap()

    def _show_context_menu(self, pos):
        row = self.table.rowAt(pos.y())
        if row >= 0:
            self.table.selectRow(row)

        menu = QMenu(self)
        find_rooms_action = menu.addAction("Find rooms using this asset")
        capability_overlap_action = menu.addAction("Show capability overlap matrix")
        menu.addSeparator()
        edit_action = menu.addAction("Edit asset")
        delete_action = menu.addAction("Delete selected")

        selected_asset = self._selected_asset()
        selected_asset_id = str(selected_asset.get("id", "") or "").strip() if selected_asset else ""
        find_rooms_action.setEnabled(bool(selected_asset_id))
        capability_overlap_action.setEnabled(callable(self.on_show_capability_overlap))

        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        if action == find_rooms_action:
            self.show_selected_asset_rooms()
        elif action == capability_overlap_action:
            self.show_capability_overlap()
        elif action == edit_action:
            self.edit_asset()
        elif action == delete_action:
            self.delete_assets()

    def save(self):
        self.on_save(self.items)
        self.close()


class RoomTypeEditorDialog(QDialog):
    def __init__(
        self,
        parent,
        seed=None,
        asset_options=None,
        assets_by_id=None,
        asset_categories_by_id=None,
        default_id="RT1",
    ):
        super().__init__(parent)
        self.setWindowTitle("Room Type")
        self.resize(1120, 560)
        self.seed = seed or {}
        self.default_id = default_id
        self.asset_options = list(asset_options or [])
        self.assets_by_id = dict(assets_by_id or {})
        self.asset_categories_by_id = dict(asset_categories_by_id or {})
        self.result = None

        self.asset_rows_by_id = self._seed_asset_rows_by_id()

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.name_edit = QLineEdit(str(self.seed.get("name", "")))
        form.addRow("Room type name", self.name_edit)

        self.total_label = QLabel()
        layout.addWidget(self.total_label)

        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("Search assets"))

        self.asset_search_edit = QLineEdit()
        self.asset_search_edit.setPlaceholderText(
            "Type to filter by asset name, asset ID, ADB code, group, or category..."
        )
        self.asset_search_edit.setClearButtonEnabled(True)
        search_row.addWidget(self.asset_search_edit, 1)

        self.asset_filter_count_label = QLabel()
        self.asset_filter_count_label.setMinimumWidth(110)
        self.asset_filter_count_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        search_row.addWidget(self.asset_filter_count_label)

        layout.addLayout(search_row)

        self.assets_table = QTableWidget(0, 8)
        self.assets_table.setHorizontalHeaderLabels(
            [
                "Use",
                "Category",
                "Group",
                "ADB_Code",
                "Asset",
                "Data points each",
                "Qty in room",
                "Total",
            ]
        )

        self.assets_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.assets_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.assets_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)

        self.assets_table.setColumnWidth(0, 55)
        self.assets_table.setColumnWidth(1, 150)
        self.assets_table.setColumnWidth(2, 140)
        self.assets_table.setColumnWidth(3, 130)
        self.assets_table.setColumnWidth(4, 240)
        self.assets_table.setColumnWidth(5, 120)
        self.assets_table.setColumnWidth(6, 110)
        self.assets_table.setColumnWidth(7, 100)

        grouped_assets = []
        for asset_id, asset_name in self.asset_options:
            asset = self.assets_by_id.get(asset_id, {})
            category_id = str(asset.get("category_id", asset.get("category", "")) or "").strip()
            category_name = self.asset_categories_by_id.get(category_id, "Uncategorised")
            grouped_assets.append((category_name, asset_name, asset_id))

        grouped_assets.sort(
            key=lambda row: (
                row[0].lower(),
                row[1].lower(),
                row[2].lower(),
            )
        )

        self._asset_table_rows = []
        self._asset_category_header_rows = {}

        last_category = None

        for category_name, asset_name, asset_id in grouped_assets:
            if category_name != last_category:
                row = self.assets_table.rowCount()
                self.assets_table.insertRow(row)

                category_item = QTableWidgetItem(str(category_name))
                category_item.setFlags(Qt.ItemIsEnabled)
                self.assets_table.setSpan(row, 0, 1, 8)
                self.assets_table.setItem(row, 0, category_item)
                self._asset_category_header_rows[str(category_name)] = row

                last_category = category_name

            asset = self.assets_by_id.get(asset_id, {})
            adb_code = str(
                asset.get("ADB_Code", asset.get("adb_code", "")) or ""
            ).strip()
            group = str(
                asset.get("Group", asset.get("group", "")) or ""
            ).strip()
            dp = int(asset.get("data_points", 1))
            room_qty = int(self.asset_rows_by_id.get(asset_id, {}).get("qty", 1) or 1)
            checked = asset_id in self.asset_rows_by_id

            row = self.assets_table.rowCount()
            self.assets_table.insertRow(row)

            use_item = QTableWidgetItem("")
            use_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable)
            use_item.setCheckState(Qt.Checked if checked else Qt.Unchecked)
            use_item.setData(Qt.UserRole, asset_id)

            self.assets_table.setItem(row, 0, use_item)
            self.assets_table.setItem(row, 1, QTableWidgetItem(str(category_name)))
            self.assets_table.setItem(row, 2, QTableWidgetItem(group))
            self.assets_table.setItem(row, 3, QTableWidgetItem(adb_code))
            self.assets_table.setItem(row, 4, QTableWidgetItem(str(asset_name)))
            self.assets_table.setItem(row, 5, QTableWidgetItem(str(dp)))

            qty_spin = QSpinBox()
            qty_spin.setRange(1, 100000)
            qty_spin.setValue(room_qty)
            qty_spin.valueChanged.connect(self._refresh_total)
            self.assets_table.setCellWidget(row, 6, qty_spin)

            self.assets_table.setItem(row, 7, QTableWidgetItem(str(room_qty * dp)))

            self._asset_table_rows.append(
                {
                    "row": row,
                    "asset_id": str(asset_id),
                    "asset_name": str(asset_name),
                    "category_name": str(category_name),
                    "search_text": " ".join(
                        (
                            str(asset_id),
                            str(asset_name),
                            str(adb_code),
                            str(group),
                            str(category_name),
                        )
                    ).casefold(),
                }
            )

        self.assets_table.itemChanged.connect(self._refresh_total)
        self.asset_search_edit.textChanged.connect(self._filter_asset_rows)

        layout.addWidget(QLabel("Select assets required in this room type"))
        layout.addWidget(self.assets_table, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._refresh_total()
        self._filter_asset_rows("")
        self.asset_search_edit.setFocus()

    def _filter_asset_rows(self, search_text):
        """Filter room assets without rebuilding the table or losing selections."""
        terms = [
            term.casefold()
            for term in str(search_text or "").split()
            if term.strip()
        ]

        visible_by_category = {
            category_name: 0
            for category_name in self._asset_category_header_rows
        }
        visible_count = 0

        for metadata in self._asset_table_rows:
            searchable = metadata["search_text"]
            matches = all(term in searchable for term in terms)
            row = int(metadata["row"])
            self.assets_table.setRowHidden(row, not matches)

            if matches:
                visible_count += 1
                category_name = metadata["category_name"]
                visible_by_category[category_name] = (
                    visible_by_category.get(category_name, 0) + 1
                )

        for category_name, header_row in self._asset_category_header_rows.items():
            self.assets_table.setRowHidden(
                int(header_row),
                visible_by_category.get(category_name, 0) == 0,
            )

        total_count = len(self._asset_table_rows)
        if terms:
            self.asset_filter_count_label.setText(
                f"{visible_count} of {total_count}"
            )
        else:
            self.asset_filter_count_label.setText(f"{total_count} assets")

    def _seed_asset_rows_by_id(self):
        result = {}

        for row in self.seed.get("assets", []) or []:
            if not isinstance(row, dict):
                continue
            asset_id = str(row.get("asset_id", row.get("id", ""))).strip()
            if asset_id:
                result[asset_id] = {
                    "asset_id": asset_id,
                    "qty": int(row.get("qty", 1) or 1),
                }

        for asset_id in self.seed.get("asset_ids", []) or []:
            asset_id = str(asset_id).strip()
            if asset_id and asset_id not in result:
                result[asset_id] = {
                    "asset_id": asset_id,
                    "qty": 1,
                }

        return result

    def _checked_asset_rows(self):
        result = []

        for row in range(self.assets_table.rowCount()):
            use_item = self.assets_table.item(row, 0)

            # Skip category header rows
            if use_item is None or use_item.data(Qt.UserRole) is None:
                continue

            if use_item.checkState() != Qt.Checked:
                continue

            asset_id = str(use_item.data(Qt.UserRole)).strip()
            if not asset_id:
                continue

            qty_spin = self.assets_table.cellWidget(row, 6)
            qty = int(qty_spin.value()) if qty_spin is not None else 1

            result.append(
                {
                    "asset_id": asset_id,
                    "qty": qty,
                }
            )

        return result

    def _refresh_total(self, *_):
        total = 0

        self.assets_table.blockSignals(True)
        try:
            for row in range(self.assets_table.rowCount()):
                use_item = self.assets_table.item(row, 0)

                # Skip category header rows
                if use_item is None or use_item.data(Qt.UserRole) is None:
                    continue

                dp_item = self.assets_table.item(row, 5)
                qty_widget = self.assets_table.cellWidget(row, 6)

                if dp_item is None or qty_widget is None:
                    continue

                dp = int(dp_item.text())
                qty = int(qty_widget.value())

                row_total = 0
                if use_item.checkState() == Qt.Checked:
                    row_total = dp * qty
                    total += row_total

                total_item = self.assets_table.item(row, 7)
                if total_item is None:
                    total_item = QTableWidgetItem("0")
                    self.assets_table.setItem(row, 7, total_item)

                total_item.setText(str(row_total))
        finally:
            self.assets_table.blockSignals(False)

        self.total_label.setText(f"Total data points / cables: {total}")

    def accept(self):
        try:
            room_type_id = str(self.seed.get("id", "") or self.default_id).strip()
            name = self.name_edit.text().strip()
            if not room_type_id:
                raise ValueError("Room type ID is required")
            if not name:
                raise ValueError("Room type name is required")

            asset_rows = self._checked_asset_rows()

            self.result = {
                "id": room_type_id,
                "name": name,
                "scenario_group": str(self.seed.get("scenario_group", self.seed.get("deployment_group", self.seed.get("room_type_group", ""))) or "").strip(),
                "assets": asset_rows,
                "asset_ids": [row["asset_id"] for row in asset_rows],
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid room type", str(exc))


class RoomTypesEditorWindow(QMainWindow):
    def __init__(
        self,
        master,
        items,
        on_save,
        asset_options=None,
        assets_by_id=None,
        asset_categories_by_id=None,
    ):
        super().__init__(master)
        self.setWindowTitle("Room Types")
        self.resize(1080, 560)
        self.items = [dict(item) for item in items]
        self.on_save = on_save

        self.asset_options = list(asset_options or [])
        self.assets_by_id = dict(assets_by_id or {})
        self.asset_categories_by_id = dict(asset_categories_by_id or {})

        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("Search room types"))

        self.room_type_search_edit = QLineEdit()
        self.room_type_search_edit.setPlaceholderText(
            "Type to filter by room name, room ID, asset, or category..."
        )
        self.room_type_search_edit.setClearButtonEnabled(True)
        search_row.addWidget(self.room_type_search_edit, 1)

        self.room_type_filter_count_label = QLabel()
        self.room_type_filter_count_label.setMinimumWidth(125)
        self.room_type_filter_count_label.setAlignment(
            Qt.AlignRight | Qt.AlignVCenter
        )
        search_row.addWidget(self.room_type_filter_count_label)
        layout.addLayout(search_row)

        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(
            ["Name", "Items", "Total data points"]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.doubleClicked.connect(self.edit_room_type)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.setColumnWidth(0, 260)
        self.table.setColumnWidth(1, 100)
        self.table.setColumnWidth(2, 140)
        layout.addWidget(self.table, 1)
        self.room_type_search_edit.textChanged.connect(
            self._apply_room_type_filter
        )

        buttons = QHBoxLayout()
        layout.addLayout(buttons)

        add_btn = QPushButton("Add")
        edit_btn = QPushButton("Edit")
        copy_btn = QPushButton("Copy")
        delete_btn = QPushButton("Delete selected")
        save_btn = QPushButton("Save")

        add_btn.clicked.connect(self.add_room_type)
        edit_btn.clicked.connect(self.edit_room_type)
        copy_btn.clicked.connect(self.copy_room_type)
        delete_btn.clicked.connect(self.delete_room_types)
        save_btn.clicked.connect(self.save)

        buttons.addWidget(add_btn)
        buttons.addWidget(edit_btn)
        buttons.addWidget(copy_btn)
        buttons.addWidget(delete_btn)
        buttons.addStretch(1)
        buttons.addWidget(save_btn)

        self._refresh_table()
        self.show()

    def _room_asset_rows(self, room_type):
        rows = []

        for row in room_type.get("assets", []) or []:
            if not isinstance(row, dict):
                continue
            asset_id = str(row.get("asset_id", row.get("id", ""))).strip()
            if asset_id:
                rows.append(
                    {
                        "asset_id": asset_id,
                        "qty": int(row.get("qty", 1) or 1),
                    }
                )

        if rows:
            return rows

        return [
            {
                "asset_id": str(asset_id).strip(),
                "qty": 1,
            }
            for asset_id in room_type.get("asset_ids", []) or []
            if str(asset_id).strip()
        ]

    def _room_total(self, room_type):
        total = 0
        for row in self._room_asset_rows(room_type):
            asset = self.assets_by_id.get(row["asset_id"])
            if not asset:
                continue
            room_qty = int(row.get("qty", 1) or 1)
            dp = int(asset.get("data_points", 1) or 1)
            total += room_qty * dp
        return total

    def _refresh_table(self):
        self.table.setRowCount(0)

        for room_type in self.items:
            row = self.table.rowCount()
            self.table.insertRow(row)

            assets = self._room_asset_rows(room_type)
            values = [
                room_type.get("name", ""),
                len(assets),
                self._room_total(room_type),
            ]

            for col, value in enumerate(values):
                self.table.setItem(row, col, QTableWidgetItem(str(value)))

        self._apply_room_type_filter()

    def _room_type_search_text(self, room_type):
        room_type_id = str(room_type.get("id", "") or "").strip()
        room_name = str(room_type.get("name", "") or "").strip()
        tokens = [room_type_id, room_name]

        for room_asset in self._room_asset_rows(room_type):
            asset_id = str(room_asset.get("asset_id", "") or "").strip()
            asset = self.assets_by_id.get(asset_id, {})
            asset_name = str(asset.get("name", "") or "").strip()
            category_id = str(
                asset.get("category_id", asset.get("category", "")) or ""
            ).strip()
            category_name = str(
                self.asset_categories_by_id.get(category_id, category_id) or ""
            ).strip()
            connection_type = str(
                asset.get(
                    "connection_type",
                    asset.get("type_of_connection", ""),
                )
                or ""
            ).strip()
            adb_code = str(
                asset.get("ADB_Code", asset.get("adb_code", "")) or ""
            ).strip()
            group = str(
                asset.get("Group", asset.get("group", "")) or ""
            ).strip()
            tokens.extend(
                (
                    asset_id,
                    asset_name,
                    adb_code,
                    group,
                    category_id,
                    category_name,
                    connection_type,
                )
            )

        return " ".join(tokens).casefold()

    def _apply_room_type_filter(self, *_):
        if _:
            self.table.clearSelection()

        terms = [
            term
            for term in self.room_type_search_edit.text().casefold().split()
            if term
        ]
        visible_count = 0

        for row, room_type in enumerate(self.items):
            searchable = self._room_type_search_text(room_type)
            matches = all(term in searchable for term in terms)
            self.table.setRowHidden(row, not matches)
            if matches:
                visible_count += 1

        total = len(self.items)
        if terms:
            self.room_type_filter_count_label.setText(
                f"{visible_count} of {total} room types"
            )
        else:
            self.room_type_filter_count_label.setText(f"{total} room types")

    def add_room_type(self):
        dialog = RoomTypeEditorDialog(
            self,
            asset_options=self.asset_options,
            assets_by_id=self.assets_by_id,
            default_id=suggest_next_id(self.items, "RT"),
            asset_categories_by_id=self.asset_categories_by_id,
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
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
            seed=deepcopy(self.items[row]),
            asset_options=self.asset_options,
            assets_by_id=self.assets_by_id,
            asset_categories_by_id=self.asset_categories_by_id,
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

    def _show_context_menu(self, pos):
        row = self.table.rowAt(pos.y())
        if row >= 0:
            self.table.selectRow(row)

        menu = QMenu(self)
        copy_action = menu.addAction("Copy room type")
        delete_action = menu.addAction("Delete selected")

        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        if action == copy_action:
            self.copy_room_type()
        elif action == delete_action:
            self.delete_room_types()

    def copy_room_type(self):
        rows = sorted({x.row() for x in self.table.selectionModel().selectedRows()})
        if not rows:
            return

        source = deepcopy(self.items[rows[0]])
        source["id"] = suggest_next_id(self.items, "RT")
        source["name"] = f"{source.get('name', '')} Copy".strip()

        dialog = RoomTypeEditorDialog(
            self,
            source,
            asset_options=self.asset_options,
            assets_by_id=self.assets_by_id,
            default_id=source["id"],
            asset_categories_by_id=self.asset_categories_by_id,
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self.items.append(dialog.result)
            self._refresh_table()
            self.table.selectRow(len(self.items) - 1)



class ScenarioGroupManagerDialog(QDialog):
    """Manage many-to-many room or asset memberships for scenario testing."""

    def __init__(
        self,
        parent,
        title,
        groups,
        item_options,
        member_key,
        item_kind_label,
    ):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(1120, 680)
        self.member_key = str(member_key)
        self.item_kind_label = str(item_kind_label)
        self.item_options = [
            (str(item_id).strip(), str(label or item_id).strip())
            for item_id, label in (item_options or [])
            if str(item_id).strip()
        ]
        self.item_options.sort(key=lambda row: (row[1].casefold(), row[0].casefold()))
        self.groups = self._normalise_groups(groups)
        self.result = None
        self._current_group_index = None
        self._loading_group = False

        layout = QVBoxLayout(self)
        intro = QLabel(
            f"Create scenario groups and tick the {self.item_kind_label.lower()} that belong to each group. "
            f"A {self.item_kind_label.lower()} can be included in multiple groups by ticking it in more than one group."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        body = QHBoxLayout()
        layout.addLayout(body, 1)

        left = QVBoxLayout()
        body.addLayout(left, 1)
        left.addWidget(QLabel("Groups"))

        self.group_table = QTableWidget(0, 3)
        self.group_table.setHorizontalHeaderLabels(["Group name", "Members", "Notes"])
        self.group_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.group_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.group_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.group_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.group_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.group_table.itemSelectionChanged.connect(self._group_selection_changed)
        left.addWidget(self.group_table, 1)

        group_buttons = QHBoxLayout()
        left.addLayout(group_buttons)
        add_btn = QPushButton("Add group")
        duplicate_btn = QPushButton("Duplicate")
        delete_btn = QPushButton("Delete")
        add_btn.clicked.connect(self.add_group)
        duplicate_btn.clicked.connect(self.duplicate_group)
        delete_btn.clicked.connect(self.delete_selected_group)
        group_buttons.addWidget(add_btn)
        group_buttons.addWidget(duplicate_btn)
        group_buttons.addWidget(delete_btn)

        right = QVBoxLayout()
        body.addLayout(right, 2)

        form = QFormLayout()
        right.addLayout(form)
        self.name_edit = QLineEdit()
        self.name_edit.textEdited.connect(self._selected_group_edited)
        self.notes_edit = QLineEdit()
        self.notes_edit.textEdited.connect(self._selected_group_edited)
        form.addRow("Group name", self.name_edit)
        form.addRow("Notes", self.notes_edit)

        search_row = QHBoxLayout()
        right.addLayout(search_row)
        search_row.addWidget(QLabel(f"Search {self.item_kind_label.lower()}"))
        self.member_search_edit = QLineEdit()
        self.member_search_edit.setClearButtonEnabled(True)
        self.member_search_edit.setPlaceholderText("Type to filter the selectable list...")
        self.member_search_edit.textChanged.connect(self._filter_members)
        search_row.addWidget(self.member_search_edit, 1)
        self.member_count_label = QLabel()
        self.member_count_label.setMinimumWidth(120)
        self.member_count_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        search_row.addWidget(self.member_count_label)

        self.member_list = QListWidget()
        self.member_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.member_list.itemChanged.connect(self._member_checked_changed)
        right.addWidget(self.member_list, 1)

        member_buttons = QHBoxLayout()
        right.addLayout(member_buttons)
        select_visible_btn = QPushButton("Select visible")
        clear_visible_btn = QPushButton("Clear visible")
        select_all_btn = QPushButton("Select all")
        clear_all_btn = QPushButton("Clear all")
        select_visible_btn.clicked.connect(lambda: self._set_visible_members(Qt.Checked))
        clear_visible_btn.clicked.connect(lambda: self._set_visible_members(Qt.Unchecked))
        select_all_btn.clicked.connect(lambda: self._set_all_members(Qt.Checked))
        clear_all_btn.clicked.connect(lambda: self._set_all_members(Qt.Unchecked))
        member_buttons.addWidget(select_visible_btn)
        member_buttons.addWidget(clear_visible_btn)
        member_buttons.addWidget(select_all_btn)
        member_buttons.addWidget(clear_all_btn)
        member_buttons.addStretch(1)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._populate_members()
        self._refresh_group_table()
        if self.groups:
            self.group_table.selectRow(0)
        else:
            self.groups.append({"name": "New Group", self.member_key: [], "notes": ""})
            self._refresh_group_table()
            self.group_table.selectRow(0)
            self._load_group(0)

    @staticmethod
    def _text(value):
        return str(value or "").strip()

    def _normalise_groups(self, groups):
        result = []
        seen_names = set()
        for idx, group in enumerate(groups or [], start=1):
            if not isinstance(group, dict):
                group = {"name": str(group or "").strip()}
            name = self._text(group.get("name", group.get("id", f"Group {idx}"))) or f"Group {idx}"
            base_name = name
            suffix = 2
            while name.casefold() in seen_names:
                name = f"{base_name} {suffix}"
                suffix += 1
            seen_names.add(name.casefold())
            members = []
            seen_members = set()
            raw_members = group.get(self.member_key)
            if raw_members is None:
                raw_members = group.get("member_ids", group.get("members", group.get("ids", [])))
            if not isinstance(raw_members, (list, tuple, set)):
                raw_members = [raw_members]
            for member_id in raw_members:
                member_id = self._text(member_id)
                if member_id and member_id not in seen_members:
                    members.append(member_id)
                    seen_members.add(member_id)
            result.append(
                {
                    "name": name,
                    self.member_key: members,
                    "notes": self._text(group.get("notes", group.get("description", ""))),
                }
            )
        return result

    def _populate_members(self):
        self.member_list.blockSignals(True)
        try:
            self.member_list.clear()
            for item_id, label in self.item_options:
                display = f"{item_id} - {label}" if label and label != item_id else item_id
                item = QListWidgetItem(display)
                item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                item.setCheckState(Qt.Unchecked)
                item.setData(Qt.UserRole, item_id)
                item.setData(Qt.UserRole + 1, f"{item_id} {label}".casefold())
                self.member_list.addItem(item)
        finally:
            self.member_list.blockSignals(False)
        self._filter_members(self.member_search_edit.text())

    def _refresh_group_table(self):
        selected = self._current_group_index
        self.group_table.blockSignals(True)
        try:
            self.group_table.setRowCount(0)
            for group in self.groups:
                row = self.group_table.rowCount()
                self.group_table.insertRow(row)
                members = group.get(self.member_key, []) or []
                values = [group.get("name", ""), len(members), group.get("notes", "")]
                for col, value in enumerate(values):
                    self.group_table.setItem(row, col, QTableWidgetItem(str(value)))
        finally:
            self.group_table.blockSignals(False)
        if selected is not None and 0 <= selected < self.group_table.rowCount():
            self.group_table.selectRow(selected)

    def _save_current_group(self):
        if self._current_group_index is None:
            return
        if not (0 <= self._current_group_index < len(self.groups)):
            return
        group = self.groups[self._current_group_index]
        group["name"] = self.name_edit.text().strip()
        group["notes"] = self.notes_edit.text().strip()
        members = []
        seen = set()
        for row in range(self.member_list.count()):
            item = self.member_list.item(row)
            member_id = self._text(item.data(Qt.UserRole))
            if item.checkState() == Qt.Checked and member_id and member_id not in seen:
                members.append(member_id)
                seen.add(member_id)
        group[self.member_key] = members

    def _load_group(self, index):
        self._loading_group = True
        try:
            if index is None or not (0 <= index < len(self.groups)):
                self._current_group_index = None
                self.name_edit.clear()
                self.notes_edit.clear()
                for row in range(self.member_list.count()):
                    self.member_list.item(row).setCheckState(Qt.Unchecked)
                return
            self._current_group_index = index
            group = self.groups[index]
            self.name_edit.setText(self._text(group.get("name")))
            self.notes_edit.setText(self._text(group.get("notes")))
            members = {self._text(member_id) for member_id in group.get(self.member_key, []) or []}
            self.member_list.blockSignals(True)
            try:
                for row in range(self.member_list.count()):
                    item = self.member_list.item(row)
                    member_id = self._text(item.data(Qt.UserRole))
                    item.setCheckState(Qt.Checked if member_id in members else Qt.Unchecked)
            finally:
                self.member_list.blockSignals(False)
            self._filter_members(self.member_search_edit.text())
        finally:
            self._loading_group = False

    def _group_selection_changed(self):
        if self._loading_group:
            return
        previous = self._current_group_index
        new_rows = sorted({idx.row() for idx in self.group_table.selectionModel().selectedRows()})
        new_index = new_rows[0] if new_rows else None
        if previous == new_index:
            return
        self._save_current_group()
        self._refresh_group_table()
        self._load_group(new_index)

    def _selected_group_edited(self, *_):
        if self._loading_group:
            return
        self._save_current_group()
        self._refresh_group_table()

    def _member_checked_changed(self, *_):
        if self._loading_group:
            return
        self._save_current_group()
        self._refresh_group_table()
        self._filter_members(self.member_search_edit.text())

    def _filter_members(self, search_text=""):
        terms = [term.casefold() for term in str(search_text or "").split() if term.strip()]
        visible = 0
        checked = 0
        for row in range(self.member_list.count()):
            item = self.member_list.item(row)
            searchable = self._text(item.data(Qt.UserRole + 1)).casefold()
            matches = all(term in searchable for term in terms)
            item.setHidden(not matches)
            if matches:
                visible += 1
            if item.checkState() == Qt.Checked:
                checked += 1
        total = self.member_list.count()
        if terms:
            self.member_count_label.setText(f"{visible} of {total} shown | {checked} selected")
        else:
            self.member_count_label.setText(f"{total} total | {checked} selected")

    def _set_visible_members(self, state):
        for row in range(self.member_list.count()):
            item = self.member_list.item(row)
            if not item.isHidden():
                item.setCheckState(state)
        self._member_checked_changed()

    def _set_all_members(self, state):
        for row in range(self.member_list.count()):
            self.member_list.item(row).setCheckState(state)
        self._member_checked_changed()

    def add_group(self, default_name=""):
        if not default_name:
            default_name = f"Group {len(self.groups) + 1}"
        name, ok = QInputDialog.getText(self, "Add group", "Group name:", text=default_name)
        if not ok:
            return
        name = self._text(name)
        if not name:
            return
        existing = {self._text(group.get("name")).casefold() for group in self.groups}
        if name.casefold() in existing:
            QMessageBox.critical(self, "Duplicate group", "A group with that name already exists.")
            return
        self._save_current_group()
        self.groups.append({"name": name, self.member_key: [], "notes": ""})
        self._current_group_index = len(self.groups) - 1
        self._refresh_group_table()
        self.group_table.selectRow(self._current_group_index)
        self._load_group(self._current_group_index)

    def duplicate_group(self):
        rows = sorted({idx.row() for idx in self.group_table.selectionModel().selectedRows()})
        if not rows:
            QMessageBox.information(self, "Duplicate group", "Select a group to duplicate.")
            return
        self._save_current_group()
        source = dict(self.groups[rows[0]])
        base_name = f"{source.get('name', 'Group')} Copy"
        name = base_name
        existing = {self._text(group.get("name")).casefold() for group in self.groups}
        suffix = 2
        while name.casefold() in existing:
            name = f"{base_name} {suffix}"
            suffix += 1
        source["name"] = name
        source[self.member_key] = list(source.get(self.member_key, []) or [])
        self.groups.append(source)
        self._current_group_index = len(self.groups) - 1
        self._refresh_group_table()
        self.group_table.selectRow(self._current_group_index)
        self._load_group(self._current_group_index)

    def delete_selected_group(self):
        rows = sorted({idx.row() for idx in self.group_table.selectionModel().selectedRows()}, reverse=True)
        if not rows:
            QMessageBox.information(self, "Delete group", "Select one or more groups to delete.")
            return
        if QMessageBox.question(self, "Delete groups", f"Delete {len(rows)} selected group(s)?") != QMessageBox.Yes:
            return
        for row in rows:
            if 0 <= row < len(self.groups):
                del self.groups[row]
        self._current_group_index = None
        self._refresh_group_table()
        if self.groups:
            self.group_table.selectRow(min(rows[-1], len(self.groups) - 1))
        else:
            self._load_group(None)

    def accept(self):
        self._save_current_group()
        cleaned = []
        seen = set()
        for group in self.groups:
            name = self._text(group.get("name"))
            if not name:
                continue
            key = name.casefold()
            if key in seen:
                QMessageBox.critical(self, "Duplicate group", f"The group name '{name}' is used more than once.")
                return
            seen.add(key)
            members = []
            seen_members = set()
            for member_id in group.get(self.member_key, []) or []:
                member_id = self._text(member_id)
                if member_id and member_id not in seen_members:
                    members.append(member_id)
                    seen_members.add(member_id)
            cleaned.append({"name": name, self.member_key: members, "notes": self._text(group.get("notes"))})
        self.result = cleaned
        super().accept()


class ScenarioGroupSelectionDialog(QDialog):
    """Select one or more scenario groups with membership context."""

    def __init__(
        self,
        parent,
        title,
        groups,
        selected_names=None,
        member_lookup=None,
        allow_multiple=True,
        extra_hint="",
    ):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(920, 600)
        self.allow_multiple = bool(allow_multiple)
        self.member_lookup = dict(member_lookup or {})
        self.groups = self._normalise_groups(groups)
        self.selected_names = {
            self._text(name).casefold()
            for name in (selected_names or [])
            if self._text(name)
        }
        self.result = []

        layout = QVBoxLayout(self)
        intro = QLabel(
            "Tick the group or groups to use for this scenario row. "
            "The table shows how much each group contains so you can choose the right target quickly."
        )
        if extra_hint:
            intro.setText(f"{intro.text()} {extra_hint}")
        intro.setWordWrap(True)
        layout.addWidget(intro)

        search_row = QHBoxLayout()
        layout.addLayout(search_row)
        search_row.addWidget(QLabel("Search groups"))
        self.search_edit = QLineEdit()
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.setPlaceholderText("Filter by group name, notes or members...")
        self.search_edit.textChanged.connect(self._filter_rows)
        search_row.addWidget(self.search_edit, 1)
        self.count_label = QLabel()
        self.count_label.setMinimumWidth(180)
        self.count_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        search_row.addWidget(self.count_label)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Use", "Group name", "Members", "Notes / examples"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection if not self.allow_multiple else QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        self.table.itemChanged.connect(self._item_changed)
        self.table.itemDoubleClicked.connect(self._row_double_clicked)
        layout.addWidget(self.table, 1)

        helper_row = QHBoxLayout()
        layout.addLayout(helper_row)
        select_visible_btn = QPushButton("Select visible")
        clear_visible_btn = QPushButton("Clear visible")
        select_all_btn = QPushButton("Select all")
        clear_all_btn = QPushButton("Clear all")
        select_visible_btn.clicked.connect(lambda: self._set_visible(Qt.Checked))
        clear_visible_btn.clicked.connect(lambda: self._set_visible(Qt.Unchecked))
        select_all_btn.clicked.connect(lambda: self._set_all(Qt.Checked))
        clear_all_btn.clicked.connect(lambda: self._set_all(Qt.Unchecked))
        helper_row.addWidget(select_visible_btn)
        helper_row.addWidget(clear_visible_btn)
        helper_row.addWidget(select_all_btn)
        helper_row.addWidget(clear_all_btn)
        helper_row.addStretch(1)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._populate_table()
        self._filter_rows()

    @staticmethod
    def _text(value):
        return str(value or "").strip()

    def _normalise_groups(self, groups):
        rows = []
        for group in groups or []:
            if isinstance(group, dict):
                name = self._text(group.get("name", group.get("id", "")))
                notes = self._text(group.get("notes", group.get("description", "")))
                members = list(group.get("members", group.get("member_ids", [])) or [])
            else:
                name = self._text(group)
                notes = ""
                members = list(self.member_lookup.get(name, []) or [])
            if not name:
                continue
            if name in self.member_lookup:
                members = list(self.member_lookup.get(name, []) or [])
            example_members = [self._text(member) for member in members if self._text(member)]
            rows.append(
                {
                    "name": name,
                    "notes": notes,
                    "members": example_members,
                    "count": len(example_members),
                    "examples": ", ".join(example_members[:8]) + ("..." if len(example_members) > 8 else ""),
                }
            )
        rows.sort(key=lambda row: row["name"].casefold())
        return rows

    def _populate_table(self):
        self.table.blockSignals(True)
        try:
            self.table.setRowCount(0)
            for group in self.groups:
                row = self.table.rowCount()
                self.table.insertRow(row)
                check_item = QTableWidgetItem("")
                check_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
                check_item.setCheckState(Qt.Checked if group["name"].casefold() in self.selected_names else Qt.Unchecked)
                check_item.setData(Qt.UserRole, group["name"])
                self.table.setItem(row, 0, check_item)

                name_item = QTableWidgetItem(group["name"])
                name_item.setData(Qt.UserRole, group["name"])
                self.table.setItem(row, 1, name_item)
                self.table.setItem(row, 2, QTableWidgetItem(str(group["count"])))
                notes_text = group["notes"]
                if group["examples"]:
                    notes_text = f"{notes_text} | {group['examples']}" if notes_text else group["examples"]
                self.table.setItem(row, 3, QTableWidgetItem(notes_text))
        finally:
            self.table.blockSignals(False)

    def _row_group_name(self, row):
        item = self.table.item(row, 0) or self.table.item(row, 1)
        return self._text(item.data(Qt.UserRole) if item else "")

    def _item_changed(self, item):
        if item.column() != 0:
            return
        if not self.allow_multiple and item.checkState() == Qt.Checked:
            self.table.blockSignals(True)
            try:
                for row in range(self.table.rowCount()):
                    other = self.table.item(row, 0)
                    if other is not item and other:
                        other.setCheckState(Qt.Unchecked)
            finally:
                self.table.blockSignals(False)
        self._update_count_label()

    def _row_double_clicked(self, item):
        if not item:
            return
        check_item = self.table.item(item.row(), 0)
        if check_item:
            check_item.setCheckState(Qt.Unchecked if check_item.checkState() == Qt.Checked else Qt.Checked)

    def _filter_rows(self):
        needle = self.search_edit.text().strip().casefold()
        visible = 0
        for row in range(self.table.rowCount()):
            haystack = " ".join(
                self._text(self.table.item(row, col).text() if self.table.item(row, col) else "")
                for col in range(1, self.table.columnCount())
            ).casefold()
            hidden = bool(needle and needle not in haystack)
            self.table.setRowHidden(row, hidden)
            if not hidden:
                visible += 1
        self._update_count_label(visible)

    def _checked_names(self):
        names = []
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if item and item.checkState() == Qt.Checked:
                name = self._row_group_name(row)
                if name:
                    names.append(name)
        return names

    def _update_count_label(self, visible=None):
        if visible is None:
            visible = sum(1 for row in range(self.table.rowCount()) if not self.table.isRowHidden(row))
        self.count_label.setText(f"{len(self._checked_names())} selected | {visible} visible")

    def _set_visible(self, state):
        self.table.blockSignals(True)
        try:
            if not self.allow_multiple and state == Qt.Checked:
                for row in range(self.table.rowCount()):
                    item = self.table.item(row, 0)
                    if item:
                        item.setCheckState(Qt.Unchecked)
                for row in range(self.table.rowCount()):
                    if not self.table.isRowHidden(row):
                        item = self.table.item(row, 0)
                        if item:
                            item.setCheckState(Qt.Checked)
                        break
            else:
                for row in range(self.table.rowCount()):
                    if not self.table.isRowHidden(row):
                        item = self.table.item(row, 0)
                        if item:
                            item.setCheckState(state)
        finally:
            self.table.blockSignals(False)
        self._update_count_label()

    def _set_all(self, state):
        self.table.blockSignals(True)
        try:
            if not self.allow_multiple and state == Qt.Checked:
                for row in range(self.table.rowCount()):
                    item = self.table.item(row, 0)
                    if item:
                        item.setCheckState(Qt.Checked if row == 0 else Qt.Unchecked)
            else:
                for row in range(self.table.rowCount()):
                    item = self.table.item(row, 0)
                    if item:
                        item.setCheckState(state)
        finally:
            self.table.blockSignals(False)
        self._update_count_label()

    def accept(self):
        names = self._checked_names()
        if not names:
            QMessageBox.information(self, "Select groups", "Select at least one group.")
            return
        self.result = names
        super().accept()


class GuidedScenarioEditorDialog(QDialog):
    """Guided one-scenario editor that writes the same saved scenario format."""

    MODE_LABELS = {
        "add": "Add to existing quantity",
        "minimum": "Set to at least this quantity",
        "replace": "Replace with this quantity",
    }
    TYPE_LABELS = {
        "standard": "Standard asset quantity change",
        "replacement": "Replacement: remove source functions and add replacement",
    }

    def __init__(self, parent, scenario=None, room_group_lookup=None, asset_group_lookup=None):
        super().__init__(parent)
        self.setWindowTitle("Guided Scenario Editor")
        self.resize(860, 620)
        self.room_group_lookup = dict(room_group_lookup or {})
        self.asset_group_lookup = dict(asset_group_lookup or {})
        self.result = None
        seed = dict(scenario or {})

        layout = QVBoxLayout(self)
        intro = QLabel(
            "Use this guided editor to create one scenario at a time. Choose whether you are adding/updating assets "
            "or replacing several source functions with a replacement device, then select the room groups and asset groups."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        form = QFormLayout()
        layout.addLayout(form)

        self.name_edit = QLineEdit(str(seed.get("name", "") or "Scenario"))
        form.addRow("1. Scenario name", self.name_edit)

        self.type_combo = QComboBox()
        for key, label in self.TYPE_LABELS.items():
            self.type_combo.addItem(label, key)
        scenario_type = str(seed.get("scenario_type", seed.get("type", "standard")) or "standard").strip().lower()
        if scenario_type.startswith("rep"):
            scenario_type = "replacement"
        type_idx = self.type_combo.findData(scenario_type)
        self.type_combo.setCurrentIndex(type_idx if type_idx >= 0 else 0)
        self.type_combo.currentIndexChanged.connect(self._refresh_state)
        form.addRow("2. Scenario type", self.type_combo)

        self.room_groups_edit = QLineEdit(self._format_names(seed.get("room_groups", seed.get("room_group", ""))))
        self.room_groups_edit.setReadOnly(True)
        room_row = QHBoxLayout()
        room_row.addWidget(self.room_groups_edit, 1)
        pick_room_btn = QPushButton("Choose room group(s)")
        pick_room_btn.clicked.connect(lambda: self._pick_groups("room"))
        room_row.addWidget(pick_room_btn)
        form.addRow("3. Target room groups", room_row)

        self.source_groups_edit = QLineEdit(self._format_names(seed.get("asset_groups", seed.get("asset_group", ""))))
        self.source_groups_edit.setReadOnly(True)
        source_row = QHBoxLayout()
        source_row.addWidget(self.source_groups_edit, 1)
        pick_source_btn = QPushButton("Choose source/function group(s)")
        pick_source_btn.clicked.connect(lambda: self._pick_groups("source"))
        source_row.addWidget(pick_source_btn)
        form.addRow("4. Asset/function groups", source_row)

        replacement_seed = seed.get("replacement_asset_groups", seed.get("replacement_asset_group", ""))
        self.replacement_groups_edit = QLineEdit(self._format_names(replacement_seed))
        self.replacement_groups_edit.setReadOnly(True)
        replacement_row = QHBoxLayout()
        replacement_row.addWidget(self.replacement_groups_edit, 1)
        pick_replacement_btn = QPushButton("Choose replacement group(s)")
        pick_replacement_btn.clicked.connect(lambda: self._pick_groups("replacement"))
        replacement_row.addWidget(pick_replacement_btn)
        form.addRow("5. Replacement asset/group", replacement_row)

        self.qty_spin = QSpinBox()
        self.qty_spin.setRange(1, 100000)
        try:
            qty = int(seed.get("qty", 1) or 1)
        except (TypeError, ValueError):
            qty = 1
        self.qty_spin.setValue(max(1, qty))
        self.qty_spin.valueChanged.connect(self._refresh_summary)
        form.addRow("6. Quantity per room type", self.qty_spin)

        self.mode_combo = QComboBox()
        for key, label in self.MODE_LABELS.items():
            self.mode_combo.addItem(label, key)
        mode = str(seed.get("mode", "add") or "add").strip().lower()
        mode_idx = self.mode_combo.findData(mode)
        self.mode_combo.setCurrentIndex(mode_idx if mode_idx >= 0 else 0)
        self.mode_combo.currentIndexChanged.connect(self._refresh_summary)
        form.addRow("7. Quantity action", self.mode_combo)

        self.notes_edit = QPlainTextEdit(str(seed.get("notes", "") or ""))
        self.notes_edit.setFixedHeight(80)
        form.addRow("8. Notes", self.notes_edit)

        self.summary_label = QLabel()
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self._refresh_state()

    @staticmethod
    def _text(value):
        return str(value or "").strip()

    def _normalise_names(self, value):
        if isinstance(value, (list, tuple, set)):
            raw_values = list(value)
        else:
            text = self._text(value)
            raw_values = text.split(";") if text else []
        names = []
        seen = set()
        for item in raw_values:
            name = self._text(item)
            if name and name.casefold() not in seen:
                names.append(name)
                seen.add(name.casefold())
        return names

    def _format_names(self, value):
        return "; ".join(self._normalise_names(value))

    def _current_type(self):
        return str(self.type_combo.currentData() or "standard")

    def _pick_groups(self, kind):
        if kind == "room":
            lookup = self.room_group_lookup
            title = "Choose target room group(s)"
            edit = self.room_groups_edit
            hint = "These room groups define which room types the scenario will affect."
        elif kind == "replacement":
            lookup = self.asset_group_lookup
            title = "Choose replacement asset/group(s)"
            edit = self.replacement_groups_edit
            hint = "These groups contain the new device or devices that will provide the replaced functions."
        else:
            lookup = self.asset_group_lookup
            title = "Choose source asset/function group(s)"
            edit = self.source_groups_edit
            hint = "For standard scenarios these assets are added/updated. For replacement scenarios these existing functions are removed."

        if not lookup:
            QMessageBox.information(self, "No groups available", "Create scenario groups before using the guided editor.")
            return
        dialog = ScenarioGroupSelectionDialog(
            self,
            title,
            groups=sorted(lookup.keys(), key=str.casefold),
            selected_names=self._normalise_names(edit.text()),
            member_lookup=lookup,
            allow_multiple=True,
            extra_hint=hint,
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            edit.setText(self._format_names(dialog.result))
            self._refresh_summary()

    def _refresh_state(self, *_):
        is_replacement = self._current_type() == "replacement"
        self.replacement_groups_edit.setEnabled(is_replacement)
        if is_replacement:
            mode_idx = self.mode_combo.findData("replace")
            if mode_idx >= 0:
                self.mode_combo.setCurrentIndex(mode_idx)
        self._refresh_summary()

    def _refresh_summary(self, *_):
        room_count = sum(len(self.room_group_lookup.get(name, []) or []) for name in self._normalise_names(self.room_groups_edit.text()))
        source_count = sum(len(self.asset_group_lookup.get(name, []) or []) for name in self._normalise_names(self.source_groups_edit.text()))
        replacement_count = sum(len(self.asset_group_lookup.get(name, []) or []) for name in self._normalise_names(self.replacement_groups_edit.text()))
        scenario_type = self._current_type()
        if scenario_type == "replacement":
            text = (
                f"Replacement scenario preview target: {room_count} room type membership(s), "
                f"{source_count} source asset/function membership(s) to remove, and "
                f"{replacement_count} replacement asset membership(s) to add/update."
            )
        else:
            text = (
                f"Standard scenario preview target: {room_count} room type membership(s) and "
                f"{source_count} asset membership(s) to add/update."
            )
        self.summary_label.setText(text)

    def accept(self):
        name = self.name_edit.text().strip() or "Scenario"
        scenario_type = self._current_type()
        room_groups = self._normalise_names(self.room_groups_edit.text())
        source_groups = self._normalise_names(self.source_groups_edit.text())
        replacement_groups = self._normalise_names(self.replacement_groups_edit.text())
        errors = []
        if not room_groups:
            errors.append("Choose at least one target room group.")
        if not source_groups:
            errors.append("Choose at least one asset/function group.")
        if scenario_type == "replacement" and not replacement_groups:
            errors.append("Choose at least one replacement asset/group for a replacement scenario.")
        if errors:
            QMessageBox.critical(self, "Scenario setup incomplete", "\n".join(errors))
            return
        self.result = {
            "name": name,
            "enabled": True,
            "scenario_type": scenario_type,
            "room_group": room_groups[0] if room_groups else "",
            "room_groups": room_groups,
            "asset_group": source_groups[0] if source_groups else "",
            "asset_groups": source_groups,
            "replacement_asset_group": replacement_groups[0] if replacement_groups else "",
            "replacement_asset_groups": replacement_groups,
            "qty": int(self.qty_spin.value()),
            "mode": str(self.mode_combo.currentData() or "add"),
            "notes": self.notes_edit.toPlainText().strip(),
        }
        super().accept()

class RoomTypeAssetScenarioDialog(QDialog):
    """Preview and permanently apply grouped room/asset scenario sets."""

    MODE_LABELS = {
        "add": "Add to existing quantity",
        "minimum": "Set to at least this quantity",
        "replace": "Replace with this quantity",
    }
    SCENARIO_TYPE_LABELS = {
        "standard": "Standard asset quantity change",
        "replacement": "Replacement: remove source assets and add replacement",
    }
    COL_RUN = 0
    COL_NAME = 1
    COL_TYPE = 2
    COL_ROOM_GROUP = 3
    COL_ASSET_GROUP = 4
    COL_REPLACEMENT_GROUP = 5
    COL_QTY = 6
    COL_MODE = 7
    COL_NOTES = 8

    def __init__(
        self,
        parent,
        data,
        asset_options=None,
        assets_by_id=None,
        asset_categories_by_id=None,
        scenario_definitions=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Room/Asset Scenario Test")
        self.resize(1320, 760)
        self.data = data or {}
        self.asset_options = list(asset_options or [])
        self.assets_by_id = dict(assets_by_id or {})
        self.asset_categories_by_id = dict(asset_categories_by_id or {})
        self.result = None

        layout = QVBoxLayout(self)

        intro = QLabel(
            "Build one or more named scenarios using separately managed room groups and "
            "asset groups. Tick the scenarios you want to run; the preview combines them "
            "in table order so you can test them individually or simultaneously. Use a "
            "replacement scenario when several existing devices/functions can be removed "
            "and provided by one replacement device."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        self.available_label = QLabel(self._available_groups_text())
        self.available_label.setWordWrap(True)
        layout.addWidget(self.available_label)

        scenario_header = QHBoxLayout()
        scenario_header.addWidget(QLabel("Scenario definitions"))
        scenario_header.addStretch(1)
        guided_btn = QPushButton("Guided editor")
        add_btn = QPushButton("Add scenario")
        add_replacement_btn = QPushButton("Add replacement scenario")
        duplicate_btn = QPushButton("Duplicate selected")
        delete_btn = QPushButton("Delete selected")
        room_group_picker_btn = QPushButton("Select room group(s)")
        asset_group_picker_btn = QPushButton("Select source asset group(s)")
        replacement_group_picker_btn = QPushButton("Select replacement group(s)")
        preview_btn = QPushButton("Refresh preview")
        guided_btn.clicked.connect(self.open_guided_scenario_editor)
        add_btn.clicked.connect(self.add_scenario)
        add_replacement_btn.clicked.connect(self.add_replacement_scenario)
        duplicate_btn.clicked.connect(self.duplicate_selected_scenario)
        delete_btn.clicked.connect(self.delete_selected_scenarios)
        room_group_picker_btn.clicked.connect(lambda: self.pick_groups_for_selected_row("room"))
        asset_group_picker_btn.clicked.connect(lambda: self.pick_groups_for_selected_row("asset"))
        replacement_group_picker_btn.clicked.connect(lambda: self.pick_groups_for_selected_row("replacement"))
        preview_btn.clicked.connect(self.refresh_preview)
        scenario_header.addWidget(guided_btn)
        scenario_header.addWidget(add_btn)
        scenario_header.addWidget(add_replacement_btn)
        scenario_header.addWidget(duplicate_btn)
        scenario_header.addWidget(delete_btn)
        scenario_header.addSpacing(18)
        scenario_header.addWidget(room_group_picker_btn)
        scenario_header.addWidget(asset_group_picker_btn)
        scenario_header.addWidget(replacement_group_picker_btn)
        scenario_header.addWidget(preview_btn)
        layout.addLayout(scenario_header)

        self.scenario_table = QTableWidget(0, 9)
        self.scenario_table.setHorizontalHeaderLabels(
            [
                "Run",
                "Scenario name",
                "Type",
                "Room group",
                "Asset / function group",
                "Replacement asset/group",
                "Qty per room type",
                "Action",
                "Notes",
            ]
        )
        self.scenario_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.scenario_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.scenario_table.setEditTriggers(QAbstractItemView.AllEditTriggers)
        self.scenario_table.itemDoubleClicked.connect(self._scenario_cell_double_clicked)
        self.scenario_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.scenario_table.horizontalHeader().setSectionResizeMode(self.COL_NOTES, QHeaderView.Stretch)
        layout.addWidget(self.scenario_table, 1)

        for scenario in list(scenario_definitions or []):
            self._append_scenario_row(scenario)

        if self.scenario_table.rowCount() == 0:
            self._append_scenario_row(self._default_scenario())

        self.summary_label = QLabel()
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.preview_table = QTableWidget(0, 12)
        self.preview_table.setHorizontalHeaderLabels(
            [
                "Scenario",
                "Type",
                "Room group",
                "Room type",
                "Placed rooms",
                "Asset / function group",
                "Asset / function",
                "Replacement group",
                "Replacement asset",
                "Current qty",
                "Scenario qty",
                "Delta deployed data points",
            ]
        )
        self.preview_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.preview_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.preview_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.preview_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        self.preview_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.Stretch)
        self.preview_table.horizontalHeader().setSectionResizeMode(8, QHeaderView.Stretch)
        layout.addWidget(self.preview_table, 2)

        button_row = QHBoxLayout()
        layout.addLayout(button_row)
        export_btn = QPushButton("Export enabled scenario CSV")
        save_btn = QPushButton("Save scenario definitions")
        apply_btn = QPushButton("Permanently apply enabled scenarios")
        close_btn = QPushButton("Close")
        export_btn.clicked.connect(self.export_enabled_scenario_csv)
        save_btn.clicked.connect(self.save_scenarios)
        apply_btn.clicked.connect(self.apply_enabled_scenarios)
        close_btn.clicked.connect(self.reject)
        button_row.addStretch(1)
        button_row.addWidget(export_btn)
        button_row.addWidget(save_btn)
        button_row.addWidget(apply_btn)
        button_row.addWidget(close_btn)

        self.refresh_preview()

    @staticmethod
    def _text(value):
        return str(value or "").strip()

    @staticmethod
    def _safe_int(value, default=0):
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return int(default)

    def _group_lookup(self, collection_key, member_key):
        lookup = {}
        for group in self.data.get(collection_key, []) or []:
            if not isinstance(group, dict):
                continue
            name = self._text(group.get("name", group.get("id", "")))
            if not name:
                continue
            members = []
            seen = set()
            for member_id in group.get(member_key, []) or []:
                member_id = self._text(member_id)
                if member_id and member_id not in seen:
                    members.append(member_id)
                    seen.add(member_id)
            lookup[name] = members
        return lookup

    def _room_group_lookup(self):
        lookup = self._group_lookup("room_type_scenario_groups", "room_type_ids")
        if lookup:
            return lookup
        legacy = {}
        for room_type in self.data.get("room_types", []) or []:
            if not isinstance(room_type, dict):
                continue
            group = self._text(room_type.get("scenario_group"))
            room_type_id = self._text(room_type.get("id"))
            if group and room_type_id:
                legacy.setdefault(group, []).append(room_type_id)
        return legacy

    def _asset_group_lookup(self):
        lookup = self._group_lookup("asset_scenario_groups", "asset_ids")
        if lookup:
            return lookup
        legacy = {}
        for asset_id, asset in self.assets_by_id.items():
            group = self._text(asset.get("scenario_group", asset.get("asset_scenario_group", "")))
            if group and asset_id:
                legacy.setdefault(group, []).append(asset_id)
        return legacy

    def _room_groups(self):
        return sorted(self._room_group_lookup().keys(), key=str.casefold)

    def _asset_groups(self):
        return sorted(self._asset_group_lookup().keys(), key=str.casefold)

    def _available_groups_text(self):
        room_groups = self._room_group_lookup()
        asset_groups = self._asset_group_lookup()
        room_text = ", ".join(f"{name} ({len(members)})" for name, members in sorted(room_groups.items(), key=lambda item: item[0].casefold())) or "none yet"
        asset_text = ", ".join(f"{name} ({len(members)})" for name, members in sorted(asset_groups.items(), key=lambda item: item[0].casefold())) or "none yet"
        return (
            f"Available room groups: {room_text}.  Available asset groups: {asset_text}. "
            "Use the group selection buttons, or double-click a group cell, to choose one or more groups "
            "with search and membership counts. For replacement scenarios, the Asset / function group column "
            "is the existing devices/functions to remove, and the Replacement asset/group column is the device "
            "that will provide those functions. You can still type a single asset ID into an asset-group column "
            "for one-off testing."
        )

    def _sorted_asset_options(self):
        return sorted(
            [
                (self._text(asset_id), self._text(asset_name) or self._text(asset_id))
                for asset_id, asset_name in self.asset_options
                if self._text(asset_id)
            ],
            key=lambda row: (row[1].casefold(), row[0].casefold()),
        )

    def _normalise_name_list(self, value):
        if isinstance(value, (list, tuple, set)):
            raw_values = list(value)
        elif value in (None, ""):
            raw_values = []
        else:
            text = self._text(value)
            if not text:
                raw_values = []
            elif ";" in text:
                raw_values = [part.strip() for part in text.split(";")]
            else:
                raw_values = [text]
        names = []
        seen = set()
        for item in raw_values:
            name = self._text(item)
            if name and name.casefold() not in seen:
                names.append(name)
                seen.add(name.casefold())
        return names

    def _scenario_type(self, scenario):
        value = self._text(
            scenario.get("scenario_type", scenario.get("type", scenario.get("kind", "standard")))
            if isinstance(scenario, dict)
            else "standard"
        ).casefold()
        if value.startswith("rep") or value in {"replace_asset", "asset_replacement", "replacement"}:
            return "replacement"
        return "standard"

    def _scenario_type_label(self, scenario_type):
        return self.SCENARIO_TYPE_LABELS.get(scenario_type, self.SCENARIO_TYPE_LABELS["standard"])

    def _scenario_room_groups(self, scenario):
        names = self._normalise_name_list(scenario.get("room_groups"))
        if not names:
            names = self._normalise_name_list(scenario.get("room_group", scenario.get("scenario_group", "")))
        return names

    def _scenario_asset_groups(self, scenario):
        names = self._normalise_name_list(scenario.get("asset_groups"))
        if not names:
            names = self._normalise_name_list(scenario.get("asset_group", scenario.get("asset_scenario_group", "")))
        return names

    def _scenario_replacement_asset_groups(self, scenario):
        names = self._normalise_name_list(scenario.get("replacement_asset_groups"))
        if not names:
            names = self._normalise_name_list(
                scenario.get(
                    "replacement_asset_group",
                    scenario.get("replacement_group", scenario.get("target_asset_group", "")),
                )
            )
        return names

    def _format_group_names(self, names):
        return "; ".join(self._normalise_name_list(names))

    def _default_scenario(self):
        room_groups = self._room_groups()
        asset_groups = self._asset_groups()
        first_asset_id = next(iter(sorted(self.assets_by_id.keys(), key=str.casefold)), "")
        room_group_list = [room_groups[0]] if room_groups else []
        asset_group_list = [asset_groups[0]] if asset_groups else ([first_asset_id] if first_asset_id else [])
        return {
            "name": "Scenario 1",
            "enabled": True,
            "scenario_type": "standard",
            "room_group": room_group_list[0] if room_group_list else "",
            "room_groups": room_group_list,
            "asset_group": asset_group_list[0] if asset_group_list else "",
            "asset_groups": asset_group_list,
            "replacement_asset_group": "",
            "replacement_asset_groups": [],
            "qty": 1,
            "mode": "add",
            "notes": "",
        }

    def _make_item(self, value="", editable=True):
        item = QTableWidgetItem(str(value or ""))
        flags = Qt.ItemIsEnabled | Qt.ItemIsSelectable
        if editable:
            flags |= Qt.ItemIsEditable
        item.setFlags(flags)
        return item

    def _append_scenario_row(self, scenario=None):
        scenario = dict(scenario or {})
        row = self.scenario_table.rowCount()
        self.scenario_table.insertRow(row)

        enabled_item = QTableWidgetItem("")
        enabled_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
        enabled_item.setCheckState(Qt.Checked if scenario.get("enabled", True) else Qt.Unchecked)
        self.scenario_table.setItem(row, 0, enabled_item)

        mode = self._text(scenario.get("mode", "add")).lower()
        if mode not in self.MODE_LABELS:
            mode = "add"

        scenario_type = self._scenario_type(scenario)
        values = [
            self._text(scenario.get("name")) or f"Scenario {row + 1}",
            scenario_type,
            self._format_group_names(self._scenario_room_groups(scenario)),
            self._format_group_names(self._scenario_asset_groups(scenario)),
            self._format_group_names(self._scenario_replacement_asset_groups(scenario)),
            str(max(1, self._safe_int(scenario.get("qty", 1), 1))),
            mode,
            self._text(scenario.get("notes")),
        ]
        for col, value in enumerate(values, start=1):
            item = self._make_item(value, editable=True)
            if col == self.COL_TYPE:
                item.setToolTip("Type 'standard' for asset quantity changes or 'replacement' to remove source assets and add replacement assets.")
            if col in (self.COL_ROOM_GROUP, self.COL_ASSET_GROUP, self.COL_REPLACEMENT_GROUP):
                item.setToolTip("Double-click to open the searchable group selector. Multiple groups are separated with semicolons.")
            if col == self.COL_MODE:
                item.setToolTip("Use add, minimum, or replace. For replacement scenarios this controls the replacement asset quantity; source assets are removed.")
            self.scenario_table.setItem(row, col, item)

    def _scenario_from_table_row(self, row):
        if row < 0 or row >= self.scenario_table.rowCount():
            return self._default_scenario()
        enabled_item = self.scenario_table.item(row, self.COL_RUN)
        return {
            "name": self._text(self.scenario_table.item(row, self.COL_NAME).text() if self.scenario_table.item(row, self.COL_NAME) else "") or f"Scenario {row + 1}",
            "enabled": enabled_item.checkState() == Qt.Checked if enabled_item else True,
            "scenario_type": self._parse_scenario_type(self.scenario_table.item(row, self.COL_TYPE).text() if self.scenario_table.item(row, self.COL_TYPE) else "standard"),
            "room_groups": self._normalise_name_list(self.scenario_table.item(row, self.COL_ROOM_GROUP).text() if self.scenario_table.item(row, self.COL_ROOM_GROUP) else ""),
            "asset_groups": self._normalise_name_list(self.scenario_table.item(row, self.COL_ASSET_GROUP).text() if self.scenario_table.item(row, self.COL_ASSET_GROUP) else ""),
            "replacement_asset_groups": self._normalise_name_list(self.scenario_table.item(row, self.COL_REPLACEMENT_GROUP).text() if self.scenario_table.item(row, self.COL_REPLACEMENT_GROUP) else ""),
            "qty": max(1, self._safe_int(self.scenario_table.item(row, self.COL_QTY).text() if self.scenario_table.item(row, self.COL_QTY) else "1", 1)),
            "mode": self._parse_mode(self.scenario_table.item(row, self.COL_MODE).text() if self.scenario_table.item(row, self.COL_MODE) else "add"),
            "notes": self._text(self.scenario_table.item(row, self.COL_NOTES).text() if self.scenario_table.item(row, self.COL_NOTES) else ""),
        }

    def _replace_scenario_row(self, row, scenario):
        if row < 0 or row >= self.scenario_table.rowCount():
            self._append_scenario_row(scenario)
            self.scenario_table.selectRow(self.scenario_table.rowCount() - 1)
            return
        self.scenario_table.removeRow(row)
        self.scenario_table.insertRow(row)
        self.scenario_table.removeRow(row)
        insert_at_end = row >= self.scenario_table.rowCount()
        if insert_at_end:
            self._append_scenario_row(scenario)
            self.scenario_table.selectRow(self.scenario_table.rowCount() - 1)
        else:
            # Rebuild by appending then moving values into the selected position.
            existing = []
            scenarios, _ = self._scenarios_from_table()
            scenarios.insert(row, scenario)
            self.scenario_table.setRowCount(0)
            for item in scenarios:
                self._append_scenario_row(item)
            self.scenario_table.selectRow(row)

    def open_guided_scenario_editor(self):
        current_row = self.scenario_table.currentRow()
        seed = self._scenario_from_table_row(current_row) if current_row >= 0 else self._default_scenario()
        dialog = GuidedScenarioEditorDialog(
            self,
            seed,
            room_group_lookup=self._room_group_lookup(),
            asset_group_lookup=self._asset_group_lookup(),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            if current_row >= 0:
                # Replace the selected row with the guided result while preserving table order.
                scenarios, _ = self._scenarios_from_table()
                if current_row < len(scenarios):
                    scenarios[current_row] = dialog.result
                else:
                    scenarios.append(dialog.result)
                self.scenario_table.setRowCount(0)
                for scenario in scenarios:
                    self._append_scenario_row(scenario)
                self.scenario_table.selectRow(min(current_row, self.scenario_table.rowCount() - 1))
            else:
                self._append_scenario_row(dialog.result)
                self.scenario_table.selectRow(self.scenario_table.rowCount() - 1)
            self.refresh_preview()

    def _scenario_cell_double_clicked(self, item):
        if not item:
            return
        if item.column() == self.COL_ROOM_GROUP:
            self.pick_groups_for_row(item.row(), "room")
        elif item.column() == self.COL_ASSET_GROUP:
            self.pick_groups_for_row(item.row(), "asset")
        elif item.column() == self.COL_REPLACEMENT_GROUP:
            self.pick_groups_for_row(item.row(), "replacement")

    def pick_groups_for_selected_row(self, group_kind):
        rows = sorted({idx.row() for idx in self.scenario_table.selectionModel().selectedRows()})
        if not rows:
            current = self.scenario_table.currentRow()
            if current >= 0:
                rows = [current]
        if not rows:
            QMessageBox.information(self, "Select groups", "Select a scenario row first.")
            return
        self.pick_groups_for_row(rows[0], group_kind)

    def pick_groups_for_row(self, row, group_kind):
        if row < 0 or row >= self.scenario_table.rowCount():
            return
        if group_kind == "room":
            lookup = self._room_group_lookup()
            title = "Select room scenario group(s)"
            col = self.COL_ROOM_GROUP
            hint = "Room groups define which room types receive the scenario asset changes."
        elif group_kind == "replacement":
            lookup = self._asset_group_lookup()
            title = "Select replacement asset/group(s)"
            col = self.COL_REPLACEMENT_GROUP
            hint = "For replacement scenarios, these are the new devices that will provide the removed functions."
        else:
            lookup = self._asset_group_lookup()
            title = "Select asset/function scenario group(s)"
            col = self.COL_ASSET_GROUP
            hint = "For standard scenarios these are the assets to add or update. For replacement scenarios these are the existing devices/functions to remove."

        if not lookup:
            QMessageBox.information(
                self,
                "No groups available",
                "Create groups from the Room Scenario Groups or Asset Scenario Groups tool before selecting them here.",
            )
            return

        current_text = self._text(self.scenario_table.item(row, col).text() if self.scenario_table.item(row, col) else "")
        dialog = ScenarioGroupSelectionDialog(
            self,
            title,
            groups=sorted(lookup.keys(), key=str.casefold),
            selected_names=self._normalise_name_list(current_text),
            member_lookup=lookup,
            allow_multiple=True,
            extra_hint=hint,
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            item = self.scenario_table.item(row, col)
            if item is None:
                item = self._make_item("", editable=True)
                self.scenario_table.setItem(row, col, item)
            item.setText(self._format_group_names(dialog.result))
            self.refresh_preview()

    def add_scenario(self):
        scenario = self._default_scenario()
        scenario["name"] = f"Scenario {self.scenario_table.rowCount() + 1}"
        self._append_scenario_row(scenario)
        self.scenario_table.selectRow(self.scenario_table.rowCount() - 1)
        self.refresh_preview()

    def add_replacement_scenario(self):
        scenario = self._default_scenario()
        scenario["name"] = f"Replacement {self.scenario_table.rowCount() + 1}"
        scenario["scenario_type"] = "replacement"
        scenario["replacement_asset_group"] = ""
        scenario["replacement_asset_groups"] = []
        scenario["mode"] = "replace"
        scenario["notes"] = "Remove source asset/function group and add replacement device."
        self._append_scenario_row(scenario)
        self.scenario_table.selectRow(self.scenario_table.rowCount() - 1)
        self.refresh_preview()

    def duplicate_selected_scenario(self):
        rows = sorted({idx.row() for idx in self.scenario_table.selectionModel().selectedRows()})
        if not rows:
            QMessageBox.information(self, "Duplicate scenario", "Select a scenario row to duplicate.")
            return
        scenarios, errors = self._scenarios_from_table()
        if errors:
            QMessageBox.critical(self, "Scenario error", "\n".join(errors))
            return
        source = dict(scenarios[rows[0]])
        source["name"] = f"{source.get('name', 'Scenario')} Copy"
        self._append_scenario_row(source)
        self.scenario_table.selectRow(self.scenario_table.rowCount() - 1)
        self.refresh_preview()

    def delete_selected_scenarios(self):
        rows = sorted({idx.row() for idx in self.scenario_table.selectionModel().selectedRows()}, reverse=True)
        if not rows:
            QMessageBox.information(self, "Delete scenarios", "Select one or more scenario rows.")
            return
        for row in rows:
            self.scenario_table.removeRow(row)
        if self.scenario_table.rowCount() == 0:
            self._append_scenario_row(self._default_scenario())
        self.refresh_preview()

    def _parse_scenario_type(self, value):
        text = self._text(value).casefold()
        if text.startswith("rep") or "replacement" in text or "single device" in text:
            return "replacement"
        return "standard"

    def _parse_mode(self, value):
        text = self._text(value).casefold()
        if text.startswith("min") or "least" in text:
            return "minimum"
        if text.startswith("rep") or text.startswith("set"):
            return "replace"
        return "add"

    def _scenarios_from_table(self):
        scenarios = []
        errors = []
        for row in range(self.scenario_table.rowCount()):
            enabled_item = self.scenario_table.item(row, self.COL_RUN)
            enabled = enabled_item.checkState() == Qt.Checked if enabled_item else True
            name = self._text(self.scenario_table.item(row, self.COL_NAME).text() if self.scenario_table.item(row, self.COL_NAME) else "")
            scenario_type = self._parse_scenario_type(
                self.scenario_table.item(row, self.COL_TYPE).text() if self.scenario_table.item(row, self.COL_TYPE) else "standard"
            )
            room_group_text = self._text(
                self.scenario_table.item(row, self.COL_ROOM_GROUP).text() if self.scenario_table.item(row, self.COL_ROOM_GROUP) else ""
            )
            asset_group_text = self._text(
                self.scenario_table.item(row, self.COL_ASSET_GROUP).text() if self.scenario_table.item(row, self.COL_ASSET_GROUP) else ""
            )
            replacement_group_text = self._text(
                self.scenario_table.item(row, self.COL_REPLACEMENT_GROUP).text() if self.scenario_table.item(row, self.COL_REPLACEMENT_GROUP) else ""
            )
            room_groups = self._normalise_name_list(room_group_text)
            asset_groups = self._normalise_name_list(asset_group_text)
            replacement_asset_groups = self._normalise_name_list(replacement_group_text)
            qty_text = self._text(self.scenario_table.item(row, self.COL_QTY).text() if self.scenario_table.item(row, self.COL_QTY) else "")
            mode = self._parse_mode(self.scenario_table.item(row, self.COL_MODE).text() if self.scenario_table.item(row, self.COL_MODE) else "add")
            notes = self._text(self.scenario_table.item(row, self.COL_NOTES).text() if self.scenario_table.item(row, self.COL_NOTES) else "")

            qty = self._safe_int(qty_text, 0)
            if not name:
                name = f"Scenario {row + 1}"
            if enabled and not room_groups:
                errors.append(f"Row {row + 1}: at least one room group is required for enabled scenarios.")
            if enabled and not asset_groups:
                if scenario_type == "replacement":
                    errors.append(f"Row {row + 1}: at least one source asset/function group is required for replacement scenarios.")
                else:
                    errors.append(f"Row {row + 1}: at least one asset group is required for enabled scenarios.")
            if enabled and scenario_type == "replacement" and not replacement_asset_groups:
                errors.append(f"Row {row + 1}: at least one replacement asset/group is required for replacement scenarios.")
            if qty <= 0:
                errors.append(f"Row {row + 1}: quantity must be a positive number.")
                qty = 1

            scenarios.append(
                {
                    "name": name,
                    "enabled": enabled,
                    "scenario_type": scenario_type,
                    "room_group": room_groups[0] if room_groups else "",
                    "room_groups": room_groups,
                    "asset_group": asset_groups[0] if asset_groups else "",
                    "asset_groups": asset_groups,
                    "replacement_asset_group": replacement_asset_groups[0] if replacement_asset_groups else "",
                    "replacement_asset_groups": replacement_asset_groups,
                    "qty": max(1, qty),
                    "mode": mode,
                    "notes": notes,
                }
            )
        return scenarios, errors

    def _room_asset_rows(self, room_type):
        rows = []
        seen = set()
        for row in room_type.get("assets", []) or []:
            if not isinstance(row, dict):
                continue
            asset_id = self._text(row.get("asset_id", row.get("id", "")))
            if not asset_id:
                continue
            rows.append({"asset_id": asset_id, "qty": max(1, self._safe_int(row.get("qty", 1), 1))})
            seen.add(asset_id)
        for asset_id in room_type.get("asset_ids", []) or []:
            asset_id = self._text(asset_id)
            if asset_id and asset_id not in seen:
                rows.append({"asset_id": asset_id, "qty": 1})
                seen.add(asset_id)
        return rows

    def _placed_room_counts(self):
        counts = {}
        for point in self.data.get("data_points", []) or []:
            room_type_id = self._text(point.get("room_type_id"))
            if room_type_id:
                counts[room_type_id] = counts.get(room_type_id, 0) + 1
        return counts

    def _asset_data_points_each(self, asset_id):
        asset = self.assets_by_id.get(asset_id, {})
        return max(
            0,
            self._safe_int(asset.get("data_points", asset.get("data_points_each", asset.get("cables", 1))), 1),
        )

    def _scenario_qty(self, current_qty, qty, mode):
        current_qty = max(0, self._safe_int(current_qty, 0))
        qty = max(1, self._safe_int(qty, 1))
        if mode == "minimum":
            return max(current_qty, qty)
        if mode == "replace":
            return qty
        return current_qty + qty

    def _matching_room_types(self, group_names):
        names = self._normalise_name_list(group_names)
        lookup = self._room_group_lookup()
        wanted = set()
        for group_name in names:
            for name, members in lookup.items():
                if name.casefold() == group_name.casefold():
                    wanted.update(self._text(room_type_id) for room_type_id in members if self._text(room_type_id))
                    break
        if not wanted:
            return []
        return [
            room_type
            for room_type in self.data.get("room_types", []) or []
            if isinstance(room_type, dict)
            and self._text(room_type.get("id")) in wanted
        ]

    def _matching_asset_ids(self, group_names):
        names = self._normalise_name_list(group_names)
        lookup = self._asset_group_lookup()
        asset_ids = []
        seen = set()
        for group_name in names:
            matched_group = False
            for name, members in lookup.items():
                if name.casefold() == group_name.casefold():
                    matched_group = True
                    for asset_id in members:
                        if asset_id in self.assets_by_id and asset_id not in seen:
                            asset_ids.append(asset_id)
                            seen.add(asset_id)
                    break
            if not matched_group and group_name in self.assets_by_id and group_name not in seen:
                asset_ids.append(group_name)
                seen.add(group_name)
        return asset_ids

    def _preview_rows(self):
        scenarios, errors = self._scenarios_from_table()
        messages = list(errors)
        if errors:
            return [], scenarios, messages

        enabled_scenarios = [scenario for scenario in scenarios if scenario.get("enabled")]
        if not enabled_scenarios:
            return [], scenarios, ["No scenarios are enabled. Tick one or more rows in the Run column."]

        room_type_state = {}
        room_types_by_id = {}
        for room_type in self.data.get("room_types", []) or []:
            if not isinstance(room_type, dict):
                continue
            room_type_id = self._text(room_type.get("id"))
            if not room_type_id:
                continue
            room_types_by_id[room_type_id] = room_type
            room_type_state[room_type_id] = {
                self._text(row.get("asset_id")): max(1, self._safe_int(row.get("qty", 1), 1))
                for row in self._room_asset_rows(room_type)
                if self._text(row.get("asset_id"))
            }

        placed_counts = self._placed_room_counts()
        rows = []

        for scenario in enabled_scenarios:
            scenario_type = self._scenario_type(scenario)
            scenario_type_label = "Replacement" if scenario_type == "replacement" else "Standard"
            room_groups = self._scenario_room_groups(scenario)
            asset_groups = self._scenario_asset_groups(scenario)
            replacement_groups = self._scenario_replacement_asset_groups(scenario)
            room_group_label = self._format_group_names(room_groups)
            asset_group_label = self._format_group_names(asset_groups)
            replacement_group_label = self._format_group_names(replacement_groups)
            matching_room_types = self._matching_room_types(room_groups)
            matching_asset_ids = self._matching_asset_ids(asset_groups)
            replacement_asset_ids = self._matching_asset_ids(replacement_groups) if scenario_type == "replacement" else []
            if not matching_room_types:
                messages.append(f"{scenario['name']}: no room types match room group(s) '{room_group_label}'.")
                continue
            if not matching_asset_ids:
                messages.append(f"{scenario['name']}: no source assets/functions match group(s) '{asset_group_label}'.")
                continue
            if scenario_type == "replacement" and not replacement_asset_ids:
                messages.append(f"{scenario['name']}: no replacement assets match group(s) '{replacement_group_label}'.")
                continue

            source_assets_seen = False
            for room_type in matching_room_types:
                room_type_id = self._text(room_type.get("id"))
                room_type_name = self._text(room_type.get("name")) or room_type_id
                placed_rooms = int(placed_counts.get(room_type_id, 0) or 0)
                state = room_type_state.setdefault(room_type_id, {})

                if scenario_type == "replacement":
                    for asset_id in matching_asset_ids:
                        current_qty = int(state.get(asset_id, 0) or 0)
                        if current_qty <= 0:
                            continue
                        source_assets_seen = True
                        asset = self.assets_by_id.get(asset_id, {})
                        asset_name = self._text(asset.get("name")) or asset_id
                        del state[asset_id]
                        rows.append(
                            {
                                "scenario": scenario["name"],
                                "scenario_type": scenario_type_label,
                                "room_group": room_group_label,
                                "room_type": f"{room_type_id} - {room_type_name}",
                                "room_type_id": room_type_id,
                                "room_type_name": room_type_name,
                                "placed_rooms": placed_rooms,
                                "asset_group": asset_group_label,
                                "asset": f"{asset_id} - {asset_name}",
                                "asset_id": asset_id,
                                "asset_name": asset_name,
                                "replacement_group": replacement_group_label,
                                "replacement_asset": "",
                                "replacement_asset_id": "",
                                "replacement_asset_name": "",
                                "change_asset_id": asset_id,
                                "change_asset_name": asset_name,
                                "change_direction": "remove",
                                "ports_per_asset": self._asset_data_points_each(asset_id),
                                "current_qty": current_qty,
                                "scenario_qty": 0,
                                "delta_qty_per_location": -current_qty,
                                "delta_items": -current_qty * placed_rooms,
                                "delta_data_points": -current_qty * placed_rooms * self._asset_data_points_each(asset_id),
                            }
                        )

                    for replacement_asset_id in replacement_asset_ids:
                        replacement_asset = self.assets_by_id.get(replacement_asset_id, {})
                        replacement_asset_name = self._text(replacement_asset.get("name")) or replacement_asset_id
                        current_qty = int(state.get(replacement_asset_id, 0) or 0)
                        scenario_qty = self._scenario_qty(current_qty, scenario["qty"], scenario["mode"])
                        delta_qty_per_room = scenario_qty - current_qty
                        state[replacement_asset_id] = scenario_qty
                        rows.append(
                            {
                                "scenario": scenario["name"],
                                "scenario_type": scenario_type_label,
                                "room_group": room_group_label,
                                "room_type": f"{room_type_id} - {room_type_name}",
                                "room_type_id": room_type_id,
                                "room_type_name": room_type_name,
                                "placed_rooms": placed_rooms,
                                "asset_group": asset_group_label,
                                "asset": "",
                                "asset_id": "",
                                "asset_name": "",
                                "replacement_group": replacement_group_label,
                                "replacement_asset": f"{replacement_asset_id} - {replacement_asset_name}",
                                "replacement_asset_id": replacement_asset_id,
                                "replacement_asset_name": replacement_asset_name,
                                "change_asset_id": replacement_asset_id,
                                "change_asset_name": replacement_asset_name,
                                "change_direction": "add/update replacement",
                                "ports_per_asset": self._asset_data_points_each(replacement_asset_id),
                                "current_qty": current_qty,
                                "scenario_qty": scenario_qty,
                                "delta_qty_per_location": delta_qty_per_room,
                                "delta_items": delta_qty_per_room * placed_rooms,
                                "delta_data_points": delta_qty_per_room * placed_rooms * self._asset_data_points_each(replacement_asset_id),
                            }
                        )
                    continue

                for asset_id in matching_asset_ids:
                    asset = self.assets_by_id.get(asset_id, {})
                    asset_name = self._text(asset.get("name")) or asset_id
                    current_qty = int(state.get(asset_id, 0) or 0)
                    scenario_qty = self._scenario_qty(current_qty, scenario["qty"], scenario["mode"])
                    delta_qty_per_room = scenario_qty - current_qty
                    state[asset_id] = scenario_qty
                    rows.append(
                        {
                            "scenario": scenario["name"],
                            "scenario_type": scenario_type_label,
                            "room_group": room_group_label,
                            "room_type": f"{room_type_id} - {room_type_name}",
                            "room_type_id": room_type_id,
                            "room_type_name": room_type_name,
                            "placed_rooms": placed_rooms,
                            "asset_group": asset_group_label,
                            "asset": f"{asset_id} - {asset_name}",
                            "asset_id": asset_id,
                            "asset_name": asset_name,
                            "replacement_group": "",
                            "replacement_asset": "",
                            "replacement_asset_id": "",
                            "replacement_asset_name": "",
                            "change_asset_id": asset_id,
                            "change_asset_name": asset_name,
                            "change_direction": "add/update",
                            "ports_per_asset": self._asset_data_points_each(asset_id),
                            "current_qty": current_qty,
                            "scenario_qty": scenario_qty,
                            "delta_qty_per_location": delta_qty_per_room,
                            "delta_items": delta_qty_per_room * placed_rooms,
                            "delta_data_points": delta_qty_per_room * placed_rooms * self._asset_data_points_each(asset_id),
                        }
                    )

            if scenario_type == "replacement" and not source_assets_seen:
                messages.append(
                    f"{scenario['name']}: source asset/function group(s) '{asset_group_label}' are not currently assigned to the matching room types."
                )

        return rows, scenarios, messages

    def refresh_preview(self, *_):
        rows, scenarios, messages = self._preview_rows()
        self.preview_table.setRowCount(0)
        total_delta_items = 0
        total_delta_data_points = 0
        touched_room_types = set()
        touched_assets = set()

        for row_data in rows:
            row = self.preview_table.rowCount()
            self.preview_table.insertRow(row)
            values = [
                row_data["scenario"],
                row_data["scenario_type"],
                row_data["room_group"],
                row_data["room_type"],
                row_data["placed_rooms"],
                row_data["asset_group"],
                row_data["asset"],
                row_data["replacement_group"],
                row_data["replacement_asset"],
                row_data["current_qty"],
                row_data["scenario_qty"],
                row_data["delta_data_points"],
            ]
            for col, value in enumerate(values):
                self.preview_table.setItem(row, col, QTableWidgetItem(str(value)))
            total_delta_items += int(row_data.get("delta_items", 0) or 0)
            total_delta_data_points += int(row_data.get("delta_data_points", 0) or 0)
            touched_room_types.add(row_data["room_type"])
            if row_data.get("asset"):
                touched_assets.add(row_data["asset"])
            if row_data.get("replacement_asset"):
                touched_assets.add(row_data["replacement_asset"])

        enabled_count = sum(1 for scenario in scenarios if scenario.get("enabled"))
        message_text = " ".join(messages[:3])
        if len(messages) > 3:
            message_text += f" + {len(messages) - 3} more message(s)."
        if message_text:
            message_text = "  " + message_text

        self.summary_label.setText(
            f"Enabled scenarios: {enabled_count} | Preview rows: {len(rows)} | "
            f"Room types touched: {len(touched_room_types)} | Assets touched: {len(touched_assets)} | "
            f"Delta deployed items: {total_delta_items} | Delta deployed data points: {total_delta_data_points}."
            f"{message_text}"
        )
        self.preview_table.resizeColumnsToContents()


    def _normalise_id_list(self, value):
        if isinstance(value, (list, tuple, set)):
            raw_values = list(value)
        elif value in (None, ""):
            raw_values = []
        else:
            text = self._text(value)
            for sep in [";", "|", ",", "\n", "\r"]:
                text = text.replace(sep, ";")
            raw_values = [part.strip() for part in text.split(";")]
        ids = []
        seen = set()
        for item in raw_values:
            item = self._text(item)
            if item and item.casefold() not in seen:
                ids.append(item)
                seen.add(item.casefold())
        return ids

    def _department_label_lookup(self):
        return {
            self._text(department.get("id")): self._text(department.get("name", department.get("id", "")))
            for department in self.data.get("departments", []) or []
            if isinstance(department, dict) and self._text(department.get("id"))
        }

    def _data_points_by_room_type(self):
        grouped = {}
        for point in self.data.get("data_points", []) or []:
            if not isinstance(point, dict):
                continue
            room_type_id = self._text(point.get("room_type_id"))
            if room_type_id:
                grouped.setdefault(room_type_id, []).append(point)
        for points in grouped.values():
            points.sort(
                key=lambda point: (
                    self._safe_int(point.get("floor", 0), 0),
                    self._text(point.get("name")).casefold(),
                    self._text(point.get("x")),
                    self._text(point.get("y")),
                )
            )
        return grouped

    def _scenario_delta_location_rows(self, preview_rows):
        points_by_room_type = self._data_points_by_room_type()
        departments_by_id = self._department_label_lookup()
        export_rows = []
        for row_data in preview_rows:
            room_type_id = self._text(row_data.get("room_type_id"))
            points = points_by_room_type.get(room_type_id, [])
            if not points:
                continue
            delta_assets = self._safe_int(row_data.get("delta_qty_per_location", 0), 0)
            ports_per_asset = self._safe_int(row_data.get("ports_per_asset", 0), 0)
            delta_ports = delta_assets * ports_per_asset
            total_delta_assets = self._safe_int(row_data.get("delta_items", 0), 0)
            total_delta_ports = self._safe_int(row_data.get("delta_data_points", 0), 0)
            for point in points:
                department_ids = self._normalise_id_list(
                    point.get("department_ids", point.get("department_id", point.get("departments", [])))
                )
                department_names = [departments_by_id.get(department_id, department_id) for department_id in department_ids]
                export_rows.append(
                    {
                        "scenario": row_data.get("scenario", ""),
                        "scenario_type": row_data.get("scenario_type", ""),
                        "room_group": row_data.get("room_group", ""),
                        "asset_group": row_data.get("asset_group", ""),
                        "replacement_group": row_data.get("replacement_group", ""),
                        "change_direction": row_data.get("change_direction", ""),
                        "floor": point.get("floor", ""),
                        "location": point.get("name", ""),
                        "x": point.get("x", ""),
                        "y": point.get("y", ""),
                        "room_type_id": room_type_id,
                        "room_type_name": row_data.get("room_type_name", ""),
                        "department_ids": "; ".join(department_ids),
                        "departments": "; ".join(department_names),
                        "asset_id": row_data.get("change_asset_id", ""),
                        "asset_name": row_data.get("change_asset_name", ""),
                        "current_qty_per_location": row_data.get("current_qty", 0),
                        "scenario_qty_per_location": row_data.get("scenario_qty", 0),
                        "delta_assets_per_location": delta_assets,
                        "ports_per_asset": ports_per_asset,
                        "delta_ports_per_location": delta_ports,
                        "placed_locations_for_room_type": row_data.get("placed_rooms", 0),
                        "room_type_total_delta_assets": total_delta_assets,
                        "room_type_total_delta_ports": total_delta_ports,
                    }
                )
        return export_rows

    def export_enabled_scenario_csv(self):
        rows, _, messages = self._preview_rows()
        blocking_messages = [
            message
            for message in messages
            if "Row " in message or "required" in message or "quantity must" in message
        ]
        if blocking_messages and not rows:
            QMessageBox.critical(self, "Scenario export error", "\n".join(blocking_messages))
            return
        if not rows:
            QMessageBox.information(
                self,
                "No scenario delta to export",
                "Refresh the preview and tick one or more scenarios that change deployed assets before exporting.",
            )
            return

        export_rows = self._scenario_delta_location_rows(rows)
        if not export_rows:
            QMessageBox.information(
                self,
                "No placed locations to export",
                "The enabled scenarios affect room types that do not currently have placed rooms/data points.",
            )
            return

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export enabled scenario delta CSV",
            "scenario_asset_port_delta_by_location.csv",
            "CSV files (*.csv);;All files (*.*)",
        )
        if not path:
            return
        if not path.lower().endswith(".csv"):
            path += ".csv"

        fieldnames = [
            "scenario",
            "scenario_type",
            "room_group",
            "asset_group",
            "replacement_group",
            "change_direction",
            "floor",
            "location",
            "x",
            "y",
            "room_type_id",
            "room_type_name",
            "department_ids",
            "departments",
            "asset_id",
            "asset_name",
            "current_qty_per_location",
            "scenario_qty_per_location",
            "delta_assets_per_location",
            "ports_per_asset",
            "delta_ports_per_location",
            "placed_locations_for_room_type",
            "room_type_total_delta_assets",
            "room_type_total_delta_ports",
        ]

        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                for export_row in export_rows:
                    writer.writerow({field: export_row.get(field, "") for field in fieldnames})
        except OSError as exc:
            QMessageBox.critical(self, "Export failed", f"Could not write CSV file:\n{exc}")
            return

        QMessageBox.information(
            self,
            "Scenario delta exported",
            f"Exported {len(export_rows)} location-level scenario delta row(s) to:\n{path}",
        )

    def save_scenarios(self):
        scenarios, errors = self._scenarios_from_table()
        if errors:
            QMessageBox.critical(self, "Scenario error", "\n".join(errors))
            return
        self.result = {
            "action": "save",
            "scenarios": scenarios,
            "summary": f"Saved {len(scenarios)} room/asset scenario definition(s).",
        }
        super().accept()

    def apply_enabled_scenarios(self):
        scenarios, errors = self._scenarios_from_table()
        if errors:
            QMessageBox.critical(self, "Scenario error", "\n".join(errors))
            return
        if not any(scenario.get("enabled") for scenario in scenarios):
            QMessageBox.information(
                self,
                "No enabled scenarios",
                "Tick one or more scenarios in the Run column before applying.",
            )
            return

        rows, _, messages = self._preview_rows()
        blocking_messages = [
            message
            for message in messages
            if "no room types match" in message
            or "no source assets/functions match" in message
            or "no replacement assets match" in message
        ]
        if blocking_messages and not rows:
            QMessageBox.critical(self, "Nothing to apply", "\n".join(blocking_messages))
            return

        if (
            QMessageBox.question(
                self,
                "Permanently apply scenarios",
                "Apply the enabled scenarios to the standard room type asset assignments? "
                "This updates the room type asset matrix and can be undone from the main window.",
            )
            != QMessageBox.Yes
        ):
            return

        self.result = {
            "action": "apply",
            "scenarios": scenarios,
            "summary": self.summary_label.text(),
        }
        super().accept()


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
