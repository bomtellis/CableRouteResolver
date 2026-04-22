import json
from copy import deepcopy
import re

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QScrollArea,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QToolButton,
    QVBoxLayout,
    QWidget,
    QInputDialog,
    QListWidgetItem,
    QSpinBox,
    QDoubleSpinBox,
    QFrame,
)


def ask_string(parent, title, prompt, text=""):
    value, ok = QInputDialog.getText(parent, title, prompt, text=text)
    return value if ok else None


class TaskCellButton(QToolButton):
    doubleClicked = Signal()

    def mouseDoubleClickEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.doubleClicked.emit()
            event.accept()
            return
        super().mouseDoubleClickEvent(event)


class MultiSelectPicker(QDialog):
    def __init__(self, parent, title, options, selected=None, group_resolver=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(520, 620)
        self.result = None
        self.options = list(options)
        self.selected = set(selected or [])
        self.group_resolver = group_resolver or (lambda item: "Other")
        self.checkboxes = {}
        self.visible = []
        self.checkbox_order = []
        self.last_clicked_item = None
        self._applying_shift_range = False

        self.group_collapsed = {}
        self.group_rows = {}
        self.group_headers = {}

        layout = QVBoxLayout(self)
        self.filter_edit = QLineEdit()
        self.filter_edit.textChanged.connect(self.refresh)
        layout.addWidget(self.filter_edit)

        tools = QHBoxLayout()
        layout.addLayout(tools)
        for text, handler in [
            ("All", self.select_all),
            ("None", self.clear_all),
            ("Select visible", self.select_visible),
            ("Clear visible", self.clear_visible),
        ]:
            btn = QPushButton(text)
            btn.clicked.connect(handler)
            tools.addWidget(btn)
        tools.addStretch(1)

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.container = QWidget()
        self.container_layout = QVBoxLayout(self.container)
        self.container_layout.setContentsMargins(0, 0, 0, 0)
        self.scroll.setWidget(self.container)
        layout.addWidget(self.scroll, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.finish)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.refresh()

    @staticmethod
    def _natural_sort_key(value):
        text = str(value)
        return [
            int(part) if part.isdigit() else part.lower()
            for part in re.split(r"(\d+)", text)
        ]

    def _sorted_values(self, values):
        return sorted(values, key=self._natural_sort_key)

    def _group_sort_key(self, group_name):
        text = str(group_name).strip()
        lower = text.lower()

        # Put unassigned groups first
        if "unassigned" in lower:
            return (0, self._natural_sort_key(text))

        return (1, self._natural_sort_key(text))

    def _set_group_collapsed(self, group_name, collapsed):
        self.group_collapsed[group_name] = bool(collapsed)

        for widget in self.group_rows.get(group_name, []):
            if widget is not None:
                widget.setVisible(not collapsed)

        header_btn = self.group_headers.get(group_name)
        if header_btn is not None:
            header_btn.setText("▶" if collapsed else "▼")

    def _toggle_group(self, group_name):
        collapsed = bool(self.group_collapsed.get(group_name, False))
        self._set_group_collapsed(group_name, not collapsed)

    def refresh(self):
        self._sync_visible_state()
        while self.container_layout.count():
            item = self.container_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

        filter_text = self.filter_edit.text().strip().lower()
        self.visible = []
        grouped = {}
        self.checkbox_order = []
        for item in self.options:
            if filter_text and filter_text not in item.lower():
                continue
            grouped.setdefault(self.group_resolver(item), []).append(item)

        self.group_rows = {}
        self.group_headers = {}

        for group_name in sorted(grouped.keys(), key=self._group_sort_key):
            self.group_rows[group_name] = []
            items = self._sorted_values(grouped[group_name])
            header = QHBoxLayout()

            toggle_btn = QToolButton()
            toggle_btn.setText(
                "▶" if self.group_collapsed.get(group_name, False) else "▼"
            )
            toggle_btn.setAutoRaise(True)
            toggle_btn.clicked.connect(
                lambda _=False, g=group_name: self._toggle_group(g)
            )
            header.addWidget(toggle_btn)
            self.group_headers[group_name] = toggle_btn

            label = QLabel(f"{group_name} ({len(items)})")
            header.addWidget(label)
            header.addStretch(1)

            btn_all = QPushButton("All")
            btn_none = QPushButton("None")
            btn_all.setFixedWidth(52)
            btn_none.setFixedWidth(52)
            btn_all.clicked.connect(
                lambda _, its=list(items): self._set_items(its, True)
            )
            btn_none.clicked.connect(
                lambda _, its=list(items): self._set_items(its, False)
            )
            header.addWidget(btn_all)
            header.addWidget(btn_none)

            header_widget = QWidget()
            header_widget.setLayout(header)
            self.container_layout.addWidget(header_widget)

            divider = QFrame()
            divider.setFrameShape(QFrame.HLine)
            divider.setFrameShadow(QFrame.Sunken)
            self.container_layout.addWidget(divider)
            self.group_rows[group_name].append(divider)

            for item in items:
                checked = item in self.selected
                existing = self.checkboxes.get(item)
                if existing is not None:
                    try:
                        checked = existing.isChecked()
                    except RuntimeError:
                        pass

                chk = QCheckBox(item)
                chk.setChecked(checked)
                chk.clicked.connect(
                    lambda checked=False, name=item: self._on_checkbox_clicked(name)
                )
                self.checkboxes[item] = chk
                self.checkbox_order.append(item)

                row = QWidget()
                row_layout = QHBoxLayout(row)
                row_layout.setContentsMargins(18, 0, 0, 0)
                row_layout.addWidget(chk)
                row_layout.addStretch(1)

                self.container_layout.addWidget(row)
                self.group_rows[group_name].append(row)
                self.visible.append(item)

            self._set_group_collapsed(
                group_name,
                self.group_collapsed.get(group_name, False),
            )

        self.container_layout.addStretch(1)

    def _set_items(self, items, value):
        for name in items:
            if value:
                self.selected.add(name)
            else:
                self.selected.discard(name)
            chk = self.checkboxes.get(name)
            if chk is not None:
                try:
                    chk.setChecked(value)
                except RuntimeError:
                    pass

    def _on_checkbox_clicked(self, item_name):
        if self._applying_shift_range:
            return

        chk = self.checkboxes.get(item_name)
        if chk is None:
            self.last_clicked_item = item_name
            return

        modifiers = Qt.KeyboardModifiers(QApplication.keyboardModifiers())
        current_value = chk.isChecked()

        if (
            modifiers & Qt.ShiftModifier
            and self.last_clicked_item in self.checkbox_order
        ):
            try:
                start = self.checkbox_order.index(self.last_clicked_item)
                end = self.checkbox_order.index(item_name)
            except ValueError:
                start = end = -1

            if start >= 0 and end >= 0:
                lo = min(start, end)
                hi = max(start, end)
                self._applying_shift_range = True
                try:
                    for name in self.checkbox_order[lo : hi + 1]:
                        other_chk = self.checkboxes.get(name)
                        if other_chk is None:
                            continue
                        other_chk.setChecked(current_value)
                        if current_value:
                            self.selected.add(name)
                        else:
                            self.selected.discard(name)
                finally:
                    self._applying_shift_range = False
        else:
            if current_value:
                self.selected.add(item_name)
            else:
                self.selected.discard(item_name)

        self.last_clicked_item = item_name

    def select_all(self):
        self._set_items(self.options, True)

    def clear_all(self):
        self._set_items(self.options, False)

    def select_visible(self):
        self._set_items(self.visible, True)

    def clear_visible(self):
        self._set_items(self.visible, False)

    def finish(self):
        result = []
        for item in self.options:
            chk = self.checkboxes.get(item)
            checked = item in self.selected
            if chk is not None:
                try:
                    checked = chk.isChecked()
                except RuntimeError:
                    pass
            if checked:
                result.append(item)
        self.result = result
        self.selected = set(result)
        self.accept()

    def _sync_visible_state(self):
        for item, chk in self.checkboxes.items():
            try:
                if chk.isChecked():
                    self.selected.add(item)
                else:
                    self.selected.discard(item)
            except RuntimeError:
                continue


class RouteProfilesEditorV2(QDialog):
    def __init__(
        self,
        master,
        profiles,
        point_names,
        transition_ids,
        corridor_edges,
        on_save,
        floor_map=None,
    ):
        super().__init__(master)
        self.setWindowTitle("Route Profiles")
        self.resize(1200, 720)
        self.profiles = json.loads(json.dumps(profiles))
        self.point_names = sorted(point_names)
        self.transition_ids = sorted(transition_ids)
        self.corridor_edges = corridor_edges
        self.floor_map = floor_map or {}
        self.on_save = on_save
        self.current_profile = None
        self.allowed_transitions = []
        self.allowed_nodes = []

        layout = QHBoxLayout(self)
        splitter = QSplitter()
        layout.addWidget(splitter)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        self.profile_list = QListWidget()
        self.profile_list.currentTextChanged.connect(self.on_profile_select)
        left_layout.addWidget(self.profile_list)
        btn_row = QHBoxLayout()
        left_layout.addLayout(btn_row)
        add_btn = QPushButton("Add")
        del_btn = QPushButton("Delete")
        add_btn.clicked.connect(self.add_profile)
        del_btn.clicked.connect(self.delete_profile)
        btn_row.addWidget(add_btn)
        btn_row.addWidget(del_btn)
        btn_row.addStretch(1)

        right = QWidget()
        form = QVBoxLayout(right)
        profile_form = QFormLayout()
        form.addLayout(profile_form)
        self.name_edit = QLineEdit()
        profile_form.addRow("Profile name", self.name_edit)

        transitions_row = QHBoxLayout()
        self.transitions_summary = QLabel("None")
        pick_transitions = QPushButton("Pick")
        pick_transitions.clicked.connect(self.pick_transitions)
        transitions_row.addWidget(self.transitions_summary, 1)
        transitions_row.addWidget(pick_transitions)
        profile_form.addRow("Allowed transitions", transitions_row)

        nodes_row = QHBoxLayout()
        self.nodes_summary = QLabel("None")
        self.nodes_summary.setWordWrap(True)
        pick_nodes = QPushButton("Pick")
        pick_nodes.clicked.connect(self.pick_nodes)
        nodes_row.addWidget(self.nodes_summary, 1)
        nodes_row.addWidget(pick_nodes)
        profile_form.addRow("Allowed nodes", nodes_row)

        form.addWidget(QLabel("Allowed edges as JSON array pairs"))
        self.edges_text = QPlainTextEdit()
        form.addWidget(self.edges_text, 1)

        edge_row = QHBoxLayout()
        gen_btn = QPushButton("Generate from selected nodes")
        clr_btn = QPushButton("Clear edges")
        gen_btn.clicked.connect(self.fill_edges_from_nodes)
        clr_btn.clicked.connect(lambda: self.edges_text.setPlainText(""))
        edge_row.addWidget(gen_btn)
        edge_row.addWidget(clr_btn)
        edge_row.addStretch(1)
        form.addLayout(edge_row)

        lower = QHBoxLayout()
        apply_btn = QPushButton("Apply Changes")
        save_btn = QPushButton("Save All")
        apply_btn.clicked.connect(self.apply_profile_changes)
        save_btn.clicked.connect(self.save_all)
        lower.addWidget(apply_btn)
        lower.addStretch(1)
        lower.addWidget(save_btn)
        form.addLayout(lower)

        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setStretchFactor(1, 1)

        for name in self.profiles.keys():
            self.profile_list.addItem(name)
        if self.profiles:
            self.profile_list.setCurrentRow(0)

    def summarize(self, values):
        if not values:
            return "None"
        if len(values) <= 6:
            return ", ".join(values)
        return f"{len(values)} selected"

    def _group_for_item(self, item):
        floor = self.floor_map.get(item)
        return f"Floor {floor}" if floor is not None else "Other"

    def add_profile(self):
        name = ask_string(self, "New profile", "Profile name:")
        if not name:
            return
        if name in self.profiles:
            QMessageBox.critical(self, "Duplicate", "Profile already exists")
            return
        self.profiles[name] = {
            "allowed_transitions": [],
            "allowed_nodes": [],
            "allowed_edges": [],
        }
        self.profile_list.addItem(name)
        items = self.profile_list.findItems(name, Qt.MatchExactly)
        if items:
            self.profile_list.setCurrentItem(items[0])

    def delete_profile(self):
        item = self.profile_list.currentItem()
        if item is None:
            return
        name = item.text()
        if name == "default":
            QMessageBox.critical(self, "Not allowed", "Cannot delete default profile")
            return
        del self.profiles[name]
        row = self.profile_list.row(item)
        self.profile_list.takeItem(row)
        self.current_profile = None
        self.name_edit.clear()
        self.transitions_summary.setText("None")
        self.nodes_summary.setText("None")
        self.edges_text.setPlainText("")

    def pick_transitions(self):
        picker = MultiSelectPicker(
            self,
            "Pick transitions",
            self.transition_ids,
            self.allowed_transitions,
            group_resolver=self._group_for_item,
        )
        if picker.exec() == QDialog.Accepted and picker.result is not None:
            self.allowed_transitions = sorted(picker.result)
            self.transitions_summary.setText(self.summarize(self.allowed_transitions))

    def pick_nodes(self):
        picker = MultiSelectPicker(
            self,
            "Pick nodes",
            self.point_names,
            self.allowed_nodes,
            group_resolver=self._group_for_item,
        )
        if picker.exec() == QDialog.Accepted and picker.result is not None:
            self.allowed_nodes = sorted(picker.result)
            self.nodes_summary.setText(self.summarize(self.allowed_nodes))

    def fill_edges_from_nodes(self):
        allowed = set(self.allowed_nodes)
        profile_edges = [
            [e["from"], e["to"]]
            for e in self.corridor_edges
            if e["from"] in allowed and e["to"] in allowed
        ]
        self.edges_text.setPlainText(json.dumps(profile_edges, indent=2))

    def on_profile_select(self, name):
        if not name:
            return
        self.current_profile = name
        profile = self.profiles[name]
        self.name_edit.setText(name)
        self.allowed_transitions = list(profile.get("allowed_transitions", []))
        self.allowed_nodes = list(profile.get("allowed_nodes", []))
        self.transitions_summary.setText(self.summarize(self.allowed_transitions))
        self.nodes_summary.setText(self.summarize(self.allowed_nodes))
        self.edges_text.setPlainText(
            json.dumps(profile.get("allowed_edges", []), indent=2)
        )

    def apply_profile_changes(self):
        if not self.current_profile:
            return
        try:
            new_name = self.name_edit.text().strip()
            if not new_name:
                raise ValueError("Profile name is required")
            edges = json.loads(self.edges_text.toPlainText().strip() or "[]")
            if not isinstance(edges, list):
                raise ValueError("Allowed edges must be a JSON list")
            payload = {
                "allowed_transitions": list(self.allowed_transitions),
                "allowed_nodes": list(self.allowed_nodes),
                "allowed_edges": edges,
            }
            if new_name != self.current_profile:
                self.profiles[new_name] = payload
                del self.profiles[self.current_profile]
                self.profile_list.currentItem().setText(new_name)
                self.current_profile = new_name
            else:
                self.profiles[self.current_profile] = payload
        except Exception as exc:
            QMessageBox.critical(self, "Invalid profile", str(exc))

    def save_all(self):
        self.apply_profile_changes()
        self.on_save(self.profiles)
        self.accept()


class ConnectionFormDialog(QDialog):
    def __init__(
        self,
        parent,
        point_names,
        profile_names,
        seed=None,
        default_connection_id="C1",
        group_resolver=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Connection")
        self.point_names = list(point_names)
        self.profile_names = list(profile_names)
        self.seed = seed or {}
        self.default_connection_id = default_connection_id
        self.group_resolver = group_resolver or (lambda item: "Other")
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_edit = QLineEdit(self.seed.get("id", self.default_connection_id))

        from_row = QHBoxLayout()
        self.from_edit = QLineEdit(self.seed.get("from", ""))
        self.from_edit.setReadOnly(True)
        from_btn = QPushButton("Select...")
        from_btn.clicked.connect(self._pick_from)
        from_row.addWidget(self.from_edit)
        from_row.addWidget(from_btn)

        to_row = QHBoxLayout()
        self.to_edit = QLineEdit(self.seed.get("to", ""))
        self.to_edit.setReadOnly(True)
        to_btn = QPushButton("Select...")
        to_btn.clicked.connect(self._pick_to)
        to_row.addWidget(self.to_edit)
        to_row.addWidget(to_btn)

        self.qty_edit = QLineEdit(str(self.seed.get("qty", 1)))
        self.route_profile_combo = QComboBox()
        self.route_profile_combo.addItems(self.profile_names)
        self.route_profile_combo.setCurrentText(self.seed.get("route_profile", ""))

        form.addRow("ID", self.id_edit)
        form.addRow("From", from_row)
        form.addRow("To", to_row)
        form.addRow("Qty", self.qty_edit)
        form.addRow("Route profile", self.route_profile_combo)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.resize(560, 260)

    def _pick_single_point(self, title, current_text):
        picker = MultiSelectPicker(
            self,
            title,
            self.point_names,
            selected=[current_text] if current_text else [],
            group_resolver=self.group_resolver,
        )
        if picker.exec() == QDialog.Accepted and picker.result:
            return picker.result[0]
        return None

    def _pick_from(self):
        value = self._pick_single_point("Select start point", self.from_edit.text())
        if value:
            self.from_edit.setText(value)

    def _pick_to(self):
        value = self._pick_single_point("Select end point", self.to_edit.text())
        if value:
            self.to_edit.setText(value)

    def accept(self):
        try:
            if not self.id_edit.text().strip():
                raise ValueError("ID is required")
            if not self.from_edit.text().strip():
                raise ValueError("From is required")
            if not self.to_edit.text().strip():
                raise ValueError("To is required")
            self.result = {
                "id": self.id_edit.text().strip(),
                "from": self.from_edit.text().strip(),
                "to": self.to_edit.text().strip(),
                "qty": int(self.qty_edit.text()),
                "route_profile": self.route_profile_combo.currentText().strip(),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid connection", str(exc))


class BulkConnectionDialog(QDialog):
    def __init__(
        self,
        parent,
        point_names,
        profile_names,
        suggest_connection_id,
        group_resolver=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Add Multiple Connections")
        self.point_names = list(point_names)
        self.profile_names = list(profile_names)
        self.suggest_connection_id = suggest_connection_id
        self.group_resolver = group_resolver or (lambda item: "Other")
        self.result = None

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        from_row = QHBoxLayout()
        self.from_summary = QLabel("None")
        self.from_summary.setWordWrap(True)
        from_btn = QPushButton("Pick...")
        from_btn.clicked.connect(self._pick_from_points)
        from_row.addWidget(self.from_summary, 1)
        from_row.addWidget(from_btn)

        to_row = QHBoxLayout()
        self.to_summary = QLabel("None")
        self.to_summary.setWordWrap(True)
        to_btn = QPushButton("Pick...")
        to_btn.clicked.connect(self._pick_to_points)
        to_row.addWidget(self.to_summary, 1)
        to_row.addWidget(to_btn)

        self.qty_edit = QLineEdit("1")

        self.route_profile_combo = QComboBox()
        self.route_profile_combo.addItems(self.profile_names)

        self.mode_combo = QComboBox()
        self.mode_combo.addItems(
            [
                "All combinations",
                "Pair by order",
            ]
        )

        self.id_prefix_edit = QLineEdit("C")
        self.start_number_spin = QSpinBox()
        self.start_number_spin.setRange(1, 1000000)
        self.start_number_spin.setValue(self._suggest_start_number())

        form.addRow("From points", from_row)
        form.addRow("To points", to_row)
        form.addRow("Qty", self.qty_edit)
        form.addRow("Route profile", self.route_profile_combo)
        form.addRow("Create mode", self.mode_combo)
        form.addRow("ID prefix", self.id_prefix_edit)
        form.addRow("Start number", self.start_number_spin)

        self.preview_label = QLabel("0 connection(s) will be created")
        self.preview_label.setWordWrap(True)
        layout.addWidget(self.preview_label)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.selected_from = []
        self.selected_to = []
        self.resize(680, 360)
        self._refresh_preview()

    def _suggest_start_number(self):
        suggested = self.suggest_connection_id() or "C1"
        digits = "".join(ch for ch in str(suggested) if ch.isdigit())
        return int(digits) if digits else 1

    def _summarize(self, values):
        if not values:
            return "None"
        if len(values) <= 5:
            return ", ".join(values)
        return f"{len(values)} selected"

    def _pick_from_points(self):
        picker = MultiSelectPicker(
            self,
            "Select start points",
            self.point_names,
            selected=self.selected_from,
            group_resolver=self.group_resolver,
        )
        if picker.exec() == QDialog.Accepted and picker.result is not None:
            self.selected_from = sorted(picker.result)
            self.from_summary.setText(self._summarize(self.selected_from))
            self._refresh_preview()

    def _pick_to_points(self):
        picker = MultiSelectPicker(
            self,
            "Select end points",
            self.point_names,
            selected=self.selected_to,
            group_resolver=self.group_resolver,
        )
        if picker.exec() == QDialog.Accepted and picker.result is not None:
            self.selected_to = sorted(picker.result)
            self.to_summary.setText(self._summarize(self.selected_to))
            self._refresh_preview()

    def _refresh_preview(self):
        count = 0
        if self.mode_combo.currentText() == "All combinations":
            count = len(self.selected_from) * len(self.selected_to)
        else:
            count = min(len(self.selected_from), len(self.selected_to))
        self.preview_label.setText(f"{count} connection(s) will be created")

    def _build_rows(self):
        qty = int(self.qty_edit.text())
        route_profile = self.route_profile_combo.currentText().strip()
        prefix = self.id_prefix_edit.text().strip() or "C"
        start_number = int(self.start_number_spin.value())

        pairs = []
        if self.mode_combo.currentText() == "All combinations":
            for from_name in self.selected_from:
                for to_name in self.selected_to:
                    pairs.append((from_name, to_name))
        else:
            for from_name, to_name in zip(self.selected_from, self.selected_to):
                pairs.append((from_name, to_name))

        rows = []
        next_number = start_number
        for from_name, to_name in pairs:
            rows.append(
                {
                    "id": f"{prefix}{next_number}",
                    "from": from_name,
                    "to": to_name,
                    "qty": qty,
                    "route_profile": route_profile,
                }
            )
            next_number += 1
        return rows

    def accept(self):
        try:
            if not self.selected_from:
                raise ValueError("Select at least one start point")
            if not self.selected_to:
                raise ValueError("Select at least one end point")
            if not self.id_prefix_edit.text().strip():
                raise ValueError("ID prefix is required")

            qty = int(self.qty_edit.text())
            if qty <= 0:
                raise ValueError("Qty must be greater than 0")

            rows = self._build_rows()
            if not rows:
                raise ValueError("No connections to create")

            duplicate_self = [row for row in rows if row["from"] == row["to"]]
            if duplicate_self:
                raise ValueError("From and To cannot be the same for a connection")

            ids = [row["id"] for row in rows]
            if len(ids) != len(set(ids)):
                raise ValueError("Generated IDs are duplicated")

            self.result = rows
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid bulk connection set", str(exc))


class FloorTemplateCopyDialog(QDialog):
    def __init__(
        self,
        parent,
        source_floor,
        point_names,
        selected_points=None,
        group_resolver=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Copy Template Between Floors")
        self.resize(720, 420)
        self.result = None
        self.source_floor = int(source_floor)
        self.point_names = list(point_names)
        self.group_resolver = group_resolver or (lambda item: "Other")
        self.selected_points = sorted(selected_points or [])

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        points_row = QHBoxLayout()
        self.points_summary = QLabel("None")
        self.points_summary.setWordWrap(True)
        pick_points_btn = QPushButton("Pick...")
        pick_points_btn.clicked.connect(self.pick_points)
        points_row.addWidget(self.points_summary, 1)
        points_row.addWidget(pick_points_btn)

        self.target_floor_spin = QSpinBox()
        self.target_floor_spin.setRange(0, 999)
        self.target_floor_spin.setValue(self.source_floor)

        self.include_edges_check = QCheckBox(
            "Recreate internal edges between copied items"
        )
        self.include_edges_check.setChecked(True)

        self.offset_x_spin = QDoubleSpinBox()
        self.offset_x_spin.setRange(-100000.0, 100000.0)
        self.offset_x_spin.setDecimals(3)
        self.offset_x_spin.setSingleStep(0.5)
        self.offset_x_spin.setValue(0.0)

        self.offset_y_spin = QDoubleSpinBox()
        self.offset_y_spin.setRange(-100000.0, 100000.0)
        self.offset_y_spin.setDecimals(3)
        self.offset_y_spin.setSingleStep(0.5)
        self.offset_y_spin.setValue(0.0)

        form.addRow("Selected corridor nodes / data points", points_row)
        form.addRow("Source floor", QLabel(str(self.source_floor)))
        form.addRow("Target floor", self.target_floor_spin)
        form.addRow("", self.include_edges_check)
        form.addRow("Offset X", self.offset_x_spin)
        form.addRow("Offset Y", self.offset_y_spin)

        if self.selected_points:
            self.points_summary.setText(self._summarize(self.selected_points))

        info = QLabel(
            "Only corridor nodes and data points are copied.\n"
            "New names are created automatically.\n"
            "Edges are only recreated where both original endpoints are in the selected set."
        )
        info.setWordWrap(True)
        layout.addWidget(info)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _summarize(self, values):
        if not values:
            return "None"
        if len(values) <= 6:
            return ", ".join(values)
        return f"{len(values)} selected"

    def pick_points(self):
        picker = MultiSelectPicker(
            self,
            "Select template items",
            self.point_names,
            selected=self.selected_points,
            group_resolver=self.group_resolver,
        )
        if picker.exec() == QDialog.Accepted and picker.result is not None:
            self.selected_points = sorted(picker.result)
            self.points_summary.setText(self._summarize(self.selected_points))

    def accept(self):
        try:
            if not self.selected_points:
                raise ValueError("Select one or more corridor nodes or data points")

            self.result = {
                "source_names": list(self.selected_points),
                "target_floor": int(self.target_floor_spin.value()),
                "include_internal_edges": bool(self.include_edges_check.isChecked()),
                "offset_x": float(self.offset_x_spin.value()),
                "offset_y": float(self.offset_y_spin.value()),
            }
            super().accept()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid template copy", str(exc))


class DataPointDepartmentsBulkDialog(QDialog):
    def __init__(
        self,
        parent,
        data_points,
        department_options,
        on_apply,
        group_resolver=None,
        selected_data_points=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Data Point Departments")
        self.resize(760, 640)
        self.data_points = data_points
        self.department_options = list(department_options)
        self.on_apply = on_apply
        self.group_resolver = group_resolver or (lambda item: "Other")
        self.selected_data_points = sorted(selected_data_points or [])

        layout = QVBoxLayout(self)

        points_row = QHBoxLayout()
        self.points_summary = QLabel("None")
        self.points_summary.setWordWrap(True)
        pick_points_btn = QPushButton("Pick data points...")
        pick_points_btn.clicked.connect(self.pick_data_points)
        points_row.addWidget(self.points_summary, 1)
        points_row.addWidget(pick_points_btn)
        layout.addLayout(points_row)

        if self.selected_data_points:
            if len(self.selected_data_points) <= 6:
                self.points_summary.setText(", ".join(self.selected_data_points))
            else:
                self.points_summary.setText(
                    f"{len(self.selected_data_points)} selected"
                )

        form = QFormLayout()
        layout.addLayout(form)

        self.mode_combo = QComboBox()
        self.mode_combo.addItems(
            [
                "Replace selected data points with checked departments",
                "Add checked departments to selected data points",
                "Remove checked departments from selected data points",
                "Clear all departments from selected data points",
            ]
        )

        self.departments_list = QListWidget()
        self.departments_list.setSelectionMode(QAbstractItemView.NoSelection)

        for department_id, department_name in self.department_options:
            text = (
                f"{department_id} - {department_name}"
                if department_name
                else department_id
            )
            item = QListWidgetItem(text)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setData(Qt.UserRole, department_id)
            item.setCheckState(Qt.Unchecked)
            self.departments_list.addItem(item)

        form.addRow("Apply mode", self.mode_combo)
        form.addRow("Departments", self.departments_list)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.apply_changes)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def pick_data_points(self):
        point_names = [
            item.get("name", "") for item in self.data_points if item.get("name")
        ]
        picker = MultiSelectPicker(
            self,
            "Select data points",
            point_names,
            selected=self.selected_data_points,
            group_resolver=self.group_resolver,
        )
        if picker.exec() == QDialog.Accepted and picker.result is not None:
            self.selected_data_points = sorted(picker.result)
            if len(self.selected_data_points) <= 6:
                self.points_summary.setText(", ".join(self.selected_data_points))
            else:
                self.points_summary.setText(
                    f"{len(self.selected_data_points)} selected"
                )

    def checked_department_ids(self):
        result = []
        for i in range(self.departments_list.count()):
            item = self.departments_list.item(i)
            if item.checkState() == Qt.Checked:
                result.append(str(item.data(Qt.UserRole)).strip())
        return result

    def apply_changes(self):
        try:
            if not self.selected_data_points:
                raise ValueError("Select one or more data points")

            mode = self.mode_combo.currentText()
            checked = self.checked_department_ids()

            if (
                mode != "Clear all departments from selected data points"
                and not checked
            ):
                raise ValueError("Select one or more departments")

            selected_set = set(self.selected_data_points)

            for data_point in self.data_points:
                if data_point.get("name") not in selected_set:
                    continue

                current = [
                    str(x).strip()
                    for x in data_point.get("department_ids", [])
                    if str(x).strip()
                ]

                if mode == "Replace selected data points with checked departments":
                    data_point["department_ids"] = list(checked)
                elif mode == "Add checked departments to selected data points":
                    merged = list(current)
                    for department_id in checked:
                        if department_id not in merged:
                            merged.append(department_id)
                    data_point["department_ids"] = merged
                elif mode == "Remove checked departments from selected data points":
                    data_point["department_ids"] = [
                        x for x in current if x not in checked
                    ]
                elif mode == "Clear all departments from selected data points":
                    data_point["department_ids"] = []

            self.on_apply(self.data_points)
            self.accept()
        except Exception as exc:
            QMessageBox.critical(self, "Bulk update failed", str(exc))


class ConnectionEditorWindow(QMainWindow):
    columns = [
        ("id", "ID", 90),
        ("from", "From", 180),
        ("to", "To", 180),
        ("qty", "Qty", 70),
        ("route_profile", "Route profile", 120),
    ]

    def __init__(
        self,
        master,
        items,
        point_names,
        profile_names,
        suggest_connection_id,
        on_save,
        floor_map=None,
        group_map=None,
    ):
        super().__init__(master)
        self.setWindowTitle("Connections")
        self.resize(900, 460)
        self.items = items
        self.point_names = point_names
        self.profile_names = profile_names
        self.suggest_connection_id = suggest_connection_id
        self.on_save = on_save
        self.floor_map = floor_map or {}
        self.group_map = group_map or {}

        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        self.table = QTableWidget(0, len(self.columns))
        self.table.setHorizontalHeaderLabels([c[1] for c in self.columns])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        for idx, (_key, _heading, width) in enumerate(self.columns):
            self.table.setColumnWidth(idx, width)
        layout.addWidget(self.table)

        buttons = QHBoxLayout()
        layout.addLayout(buttons)
        for text, handler in [
            ("Add", self.add_item),
            ("Add Multiple", self.add_multiple_items),
            ("Edit", self.edit_item),
            ("Delete", self.delete_item),
        ]:
            btn = QPushButton(text)
            btn.clicked.connect(handler)
            buttons.addWidget(btn)

        self._refresh_table()
        self.show()

    def _group_for_item(self, item):
        if item in self.group_map:
            return self.group_map[item]
        floor = self.floor_map.get(item)
        return f"Other / Floor {floor}" if floor is not None else "Other"

    def _refresh_table(self):
        self.table.setRowCount(0)
        for item in self.items:
            row = self.table.rowCount()
            self.table.insertRow(row)
            values = [
                item.get("id", ""),
                item.get("from", ""),
                item.get("to", ""),
                item.get("qty", 1),
                item.get("route_profile", ""),
            ]
            for col, value in enumerate(values):
                self.table.setItem(row, col, QTableWidgetItem(str(value)))

    def add_item(self):
        dialog = ConnectionFormDialog(
            self,
            self.point_names,
            self.profile_names,
            default_connection_id=self.suggest_connection_id(),
            group_resolver=self._group_for_item,
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self.items.append(dialog.result)
            self._refresh_table()

    def add_multiple_items(self):
        dialog = BulkConnectionDialog(
            self,
            self.point_names,
            self.profile_names,
            self.suggest_connection_id,
            group_resolver=self._group_for_item,
        )
        dialog.mode_combo.currentTextChanged.connect(dialog._refresh_preview)

        if dialog.exec() == QDialog.Accepted and dialog.result:
            existing_ids = {item.get("id", "") for item in self.items}
            duplicates = [
                row["id"] for row in dialog.result if row["id"] in existing_ids
            ]
            if duplicates:
                QMessageBox.critical(
                    self,
                    "Duplicate IDs",
                    "These IDs already exist:\n" + "\n".join(duplicates[:20]),
                )
                return

            self.items.extend(dialog.result)
            self._refresh_table()

    def edit_item(self):
        row = self.table.currentRow()
        if row < 0:
            return
        dialog = ConnectionFormDialog(
            self,
            self.point_names,
            self.profile_names,
            seed=deepcopy(self.items[row]),
            default_connection_id=self.items[row].get(
                "id", self.suggest_connection_id()
            ),
            group_resolver=self._group_for_item,
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self.items[row] = dialog.result
            self._refresh_table()
            self.table.selectRow(row)

    def delete_item(self):
        rows = sorted(
            {index.row() for index in self.table.selectionModel().selectedRows()},
            reverse=True,
        )
        if not rows:
            return
        for row in rows:
            del self.items[row]
        self._refresh_table()

    def save(self):
        self.on_save(self.items)
        self.close()


class LocationDepartmentsBulkDialog(QDialog):
    def __init__(
        self, parent, locations, department_options, on_apply, group_resolver=None
    ):
        super().__init__(parent)
        self.setWindowTitle("Location Departments")
        self.resize(760, 640)
        self.locations = locations
        self.department_options = list(department_options)
        self.on_apply = on_apply
        self.group_resolver = group_resolver or (lambda item: "Other")
        self.selected_locations = []

        layout = QVBoxLayout(self)

        locations_row = QHBoxLayout()
        self.locations_summary = QLabel("None")
        self.locations_summary.setWordWrap(True)
        pick_locations_btn = QPushButton("Pick locations...")
        pick_locations_btn.clicked.connect(self.pick_locations)
        locations_row.addWidget(self.locations_summary, 1)
        locations_row.addWidget(pick_locations_btn)
        layout.addLayout(locations_row)

        form = QFormLayout()
        layout.addLayout(form)

        self.mode_combo = QComboBox()
        self.mode_combo.addItems(
            [
                "Replace selected locations with checked departments",
                "Add checked departments to selected locations",
                "Remove checked departments from selected locations",
                "Clear all departments from selected locations",
            ]
        )

        self.departments_list = QListWidget()
        self.departments_list.setSelectionMode(QAbstractItemView.NoSelection)

        for department_id, department_name in self.department_options:
            text = (
                f"{department_id} - {department_name}"
                if department_name
                else department_id
            )
            item = QListWidgetItem(text)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setData(Qt.UserRole, department_id)
            item.setCheckState(Qt.Unchecked)
            self.departments_list.addItem(item)

        form.addRow("Apply mode", self.mode_combo)
        form.addRow("Departments", self.departments_list)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.apply_changes)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def pick_locations(self):
        location_names = [
            item.get("name", "") for item in self.locations if item.get("name")
        ]
        picker = MultiSelectPicker(
            self,
            "Select locations",
            location_names,
            selected=self.selected_locations,
            group_resolver=self.group_resolver,
        )
        if picker.exec() == QDialog.Accepted and picker.result is not None:
            self.selected_locations = sorted(picker.result)
            if len(self.selected_locations) <= 6:
                self.locations_summary.setText(", ".join(self.selected_locations))
            else:
                self.locations_summary.setText(
                    f"{len(self.selected_locations)} selected"
                )

    def checked_department_ids(self):
        result = []
        for i in range(self.departments_list.count()):
            item = self.departments_list.item(i)
            if item.checkState() == Qt.Checked:
                result.append(str(item.data(Qt.UserRole)).strip())
        return result

    def apply_changes(self):
        try:
            if not self.selected_locations:
                raise ValueError("Select one or more locations")

            mode = self.mode_combo.currentText()
            checked = self.checked_department_ids()

            if mode != "Clear all departments from selected locations" and not checked:
                raise ValueError("Select one or more departments")

            selected_set = set(self.selected_locations)

            for location in self.locations:
                if location.get("name") not in selected_set:
                    continue

                current = [
                    str(x).strip()
                    for x in location.get("department_ids", [])
                    if str(x).strip()
                ]

                if mode == "Replace selected locations with checked departments":
                    location["department_ids"] = list(checked)
                elif mode == "Add checked departments to selected locations":
                    merged = list(current)
                    for department_id in checked:
                        if department_id not in merged:
                            merged.append(department_id)
                    location["department_ids"] = merged
                elif mode == "Remove checked departments from selected locations":
                    location["department_ids"] = [
                        x for x in current if x not in checked
                    ]
                elif mode == "Clear all departments from selected locations":
                    location["department_ids"] = []

            self.on_apply(self.locations)
            self.accept()
        except Exception as exc:
            QMessageBox.critical(self, "Bulk update failed", str(exc))
