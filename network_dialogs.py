from __future__ import annotations

from copy import deepcopy
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

NETWORK_ASSET_TYPES = [
    ("patch_panel", "Patch panel"),
    ("fibre_splitter", "Fibre splitter"),
    ("network_switch", "Network switch"),
    ("network_router", "Network router"),
    ("firewall", "Firewall"),
    ("wireless_access_point", "Wireless access point"),
    ("optical_line_terminal", "Optical line terminal (OLT)"),
    ("optical_network_terminal", "Optical network terminal (ONT)"),
    ("other", "Other"),
]

CONNECTION_MEDIA = ["copper", "fibre", "wireless", "virtual", "stacking", "none"]
CONNECTION_ROLES = ["input", "output", "uplink"]
PATCH_PANEL_TYPES = ["", "copper", "fibre"]
SPLIT_RATIOS = ["", "1:2", "1:4", "1:8", "1:16", "1:32", "1:64", "1:128"]
FREQUENCY_OPTIONS = ["2.4 GHz", "5 GHz", "6 GHz", "60 GHz", "868 MHz", "433 MHz"]
NETWORK_TECHNOLOGIES = ["Traditional", "PoLAN"]
PORT_TYPE_OPTIONS = ["rj45", "sfp", "sfp+", "qsfp", "qsfp28", "pon", "lc", "sc", "mpo", "usb", "console", "power", "other"]
PORT_USE_OPTIONS = ["input", "output", "uplink", "downlink", "management", "console", "pon", "client", "patch", "stacking", "power", "spare", "other"]

from network_auto_planner import NetworkPlanningError, generate_network_design


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _csv_list(text: str) -> List[str]:
    result = []
    for part in str(text or "").replace(";", ",").split(","):
        value = part.strip()
        if value and value not in result:
            result.append(value)
    return result


def _next_id(items: Iterable[dict], prefix: str) -> str:
    used = {_text(item.get("id")) for item in items}
    number = 1
    while f"{prefix}{number}" in used:
        number += 1
    return f"{prefix}{number}"


def _combo_with_data(
    options: Sequence[Tuple[str, str]], current: str = ""
) -> QComboBox:
    combo = QComboBox()
    for value, label in options:
        combo.addItem(label, value)
    index = combo.findData(current)
    if index >= 0:
        combo.setCurrentIndex(index)
    return combo


def _set_combo_text(combo: QComboBox, value: str) -> None:
    index = combo.findText(value)
    if index >= 0:
        combo.setCurrentIndex(index)
    elif combo.isEditable():
        combo.setEditText(value)


class NetworkAssetEditorDialog(QDialog):
    def __init__(
        self, parent=None, asset: Optional[dict] = None, suggested_id: str = "NA1"
    ):
        super().__init__(parent)
        self.setWindowTitle("Network Asset")
        self.asset = deepcopy(asset or {})
        self.result: Optional[dict] = None
        self.resize(760, 860)

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_edit = QLineEdit(_text(self.asset.get("id")) or suggested_id)
        self.name_edit = QLineEdit(_text(self.asset.get("name")))
        self.asset_type_combo = _combo_with_data(
            NETWORK_ASSET_TYPES, _text(self.asset.get("asset_type")) or "network_switch"
        )
        self.manufacturer_edit = QLineEdit(_text(self.asset.get("manufacturer")))
        self.model_edit = QLineEdit(_text(self.asset.get("model")))

        self.patch_panel_type_combo = QComboBox()
        self.patch_panel_type_combo.addItems(PATCH_PANEL_TYPES)
        _set_combo_text(
            self.patch_panel_type_combo, _text(self.asset.get("patch_panel_type"))
        )

        self.split_ratio_combo = QComboBox()
        self.split_ratio_combo.setEditable(True)
        self.split_ratio_combo.addItems(SPLIT_RATIOS)
        _set_combo_text(self.split_ratio_combo, _text(self.asset.get("split_ratio")))

        self.olt_max_split_ratio_combo = QComboBox()
        self.olt_max_split_ratio_combo.setEditable(True)
        self.olt_max_split_ratio_combo.addItems(SPLIT_RATIOS)
        _set_combo_text(
            self.olt_max_split_ratio_combo,
            _text(self.asset.get("max_split_ratio")),
        )
        self.olt_max_split_ratio_combo.setToolTip(
            "Maximum passive optical split supported per OLT PON port, for example 1:32. "
            "Leave blank when the manufacturer limit is not known."
        )

        self.frequencies_list = QListWidget()
        self.frequencies_list.setSelectionMode(QAbstractItemView.NoSelection)
        selected_frequencies = {
            _text(value) for value in self.asset.get("frequencies", []) if _text(value)
        }
        for frequency in FREQUENCY_OPTIONS:
            item = QListWidgetItem(frequency)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(
                Qt.Checked if frequency in selected_frequencies else Qt.Unchecked
            )
            self.frequencies_list.addItem(item)

        self.additional_frequencies_edit = QLineEdit(
            ", ".join(
                value
                for value in selected_frequencies
                if value not in FREQUENCY_OPTIONS
            )
        )
        self.additional_frequencies_edit.setPlaceholderText(
            "Other frequencies, comma separated"
        )

        self.power_input_spin = QDoubleSpinBox()
        self.power_input_spin.setRange(0.0, 1_000_000.0)
        self.power_input_spin.setDecimals(2)
        self.power_input_spin.setSuffix(" W")
        self.power_input_spin.setValue(
            float(self.asset.get("power_input_w", 0.0) or 0.0)
        )

        self.poe_budget_spin = QDoubleSpinBox()
        self.poe_budget_spin.setRange(0.0, 1_000_000.0)
        self.poe_budget_spin.setDecimals(2)
        self.poe_budget_spin.setSuffix(" W")
        self.poe_budget_spin.setValue(float(self.asset.get("poe_budget_w", 0.0) or 0.0))

        self.number_of_ports_spin = QSpinBox()
        self.number_of_ports_spin.setRange(0, 100_000)
        self.number_of_ports_spin.setValue(
            int(self.asset.get("number_of_ports", 0) or 0)
        )

        self.connections_in_spin = QSpinBox()
        self.connections_in_spin.setRange(0, 100_000)
        self.connections_in_spin.setValue(int(self.asset.get("connections_in", 0) or 0))

        self.connections_out_spin = QSpinBox()
        self.connections_out_spin.setRange(0, 100_000)
        self.connections_out_spin.setValue(
            int(self.asset.get("connections_out", 0) or 0)
        )

        self.uplink_ports_spin = QSpinBox()
        self.uplink_ports_spin.setRange(0, 100_000)
        self.uplink_ports_spin.setValue(int(self.asset.get("uplink_ports", 0) or 0))

        self.port_table = QTableWidget(0, 3)
        self.port_table.setHorizontalHeaderLabels(["Port type", "Port count", "Port use"])
        self.port_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.port_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.port_table.setMinimumHeight(170)
        port_rows = self.asset.get("port_definitions", [])
        if not isinstance(port_rows, list) or not port_rows:
            count = int(self.asset.get("number_of_ports", 0) or 0)
            if count:
                asset_type = _text(self.asset.get("asset_type"))
                connector = "pon" if asset_type == "fibre_splitter" else ("lc" if _text(self.asset.get("patch_panel_type")) == "fibre" else "rj45")
                use = "patch" if asset_type == "patch_panel" else ("pon" if asset_type in {"fibre_splitter", "optical_line_terminal"} else "client")
                port_rows = [{"port_type": connector, "port_count": count, "port_use": use}]
        for row in port_rows:
            if isinstance(row, dict):
                self._append_port_row(row)
        port_buttons = QWidget()
        port_buttons_layout = QHBoxLayout(port_buttons)
        port_buttons_layout.setContentsMargins(0, 0, 0, 0)
        add_port_button = QPushButton("Add port row")
        remove_port_button = QPushButton("Remove selected row")
        add_port_button.clicked.connect(lambda: self._append_port_row({}))
        remove_port_button.clicked.connect(self._remove_selected_port_rows)
        port_buttons_layout.addWidget(add_port_button)
        port_buttons_layout.addWidget(remove_port_button)
        port_buttons_layout.addStretch(1)
        self.port_table.itemChanged.connect(self._sync_legacy_port_counts)

        self.supports_stacking_check = QCheckBox("Can form a logical stack")
        self.supports_stacking_check.setChecked(
            bool(self.asset.get("supports_stacking", False))
        )

        self.max_stack_members_spin = QSpinBox()
        self.max_stack_members_spin.setRange(1, 64)
        self.max_stack_members_spin.setValue(
            max(1, int(self.asset.get("max_stack_members", 1) or 1))
        )

        self.input_type_combo = QComboBox()
        self.input_type_combo.addItems(CONNECTION_MEDIA)
        _set_combo_text(
            self.input_type_combo,
            _text(self.asset.get("input_connection_type")) or "copper",
        )

        self.output_type_combo = QComboBox()
        self.output_type_combo.addItems(CONNECTION_MEDIA)
        _set_combo_text(
            self.output_type_combo,
            _text(self.asset.get("output_connection_type")) or "copper",
        )

        self.uplink_type_combo = QComboBox()
        self.uplink_type_combo.addItems(CONNECTION_MEDIA)
        _set_combo_text(
            self.uplink_type_combo,
            _text(self.asset.get("uplink_connection_type")) or "fibre",
        )

        self.rack_units_spin = QSpinBox()
        self.rack_units_spin.setRange(0, 100)
        self.rack_units_spin.setSuffix("U")
        self.rack_units_spin.setValue(int(self.asset.get("rack_units", 1) or 0))

        self.switch_rack_allowance_spin = QSpinBox()
        self.switch_rack_allowance_spin.setRange(0, 100)
        self.switch_rack_allowance_spin.setSuffix("U")
        self.switch_rack_allowance_spin.setValue(
            int(
                self.asset.get(
                    "switch_rack_unit_allowance", self.asset.get("rack_units", 1)
                )
                or 0
            )
        )

        self.olt_units_per_u_spin = QSpinBox()
        self.olt_units_per_u_spin.setRange(1, 32)
        self.olt_units_per_u_spin.setValue(
            max(1, int(self.asset.get("olt_units_per_rack_unit", 1) or 1))
        )
        self.olt_units_per_u_spin.setToolTip(
            "Number of separate functional OLT units that this model can mount within one physical rack unit."
        )

        self.notes_edit = QTextEdit(_text(self.asset.get("notes")))
        self.notes_edit.setMinimumHeight(90)

        form.addRow("Asset ID", self.id_edit)
        form.addRow("Name", self.name_edit)
        form.addRow("Asset type", self.asset_type_combo)
        form.addRow("Manufacturer", self.manufacturer_edit)
        form.addRow("Model", self.model_edit)
        form.addRow("Patch panel medium", self.patch_panel_type_combo)
        form.addRow("Fibre split ratio", self.split_ratio_combo)
        form.addRow("OLT maximum split ratio", self.olt_max_split_ratio_combo)
        form.addRow("Wireless frequencies", self.frequencies_list)
        form.addRow("Additional frequencies", self.additional_frequencies_edit)
        form.addRow("Power input", self.power_input_spin)
        form.addRow("PoE budget", self.poe_budget_spin)
        form.addRow("Physical ports", self.port_table)
        form.addRow("Port rows", port_buttons)
        form.addRow("Stacking", self.supports_stacking_check)
        form.addRow("Maximum stack members", self.max_stack_members_spin)
        form.addRow("Input connection type", self.input_type_combo)
        form.addRow("Output connection type", self.output_type_combo)
        form.addRow("Uplink connection type", self.uplink_type_combo)
        form.addRow("Rack spaces", self.rack_units_spin)
        form.addRow("Switch rack allowance", self.switch_rack_allowance_spin)
        form.addRow("OLT units per rack unit", self.olt_units_per_u_spin)
        form.addRow("Notes", self.notes_edit)

        self.asset_type_combo.currentIndexChanged.connect(self._update_visibility)
        self.supports_stacking_check.toggled.connect(self._update_visibility)
        self._update_visibility()

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _append_port_row(self, row: dict) -> None:
        table_row = self.port_table.rowCount()
        self.port_table.insertRow(table_row)
        type_combo = QComboBox()
        type_combo.addItems(PORT_TYPE_OPTIONS)
        _set_combo_text(type_combo, _text(row.get("port_type")) or "rj45")
        use_combo = QComboBox()
        use_combo.addItems(PORT_USE_OPTIONS)
        _set_combo_text(use_combo, _text(row.get("port_use")) or "client")
        count_item = QTableWidgetItem(str(max(1, int(row.get("port_count", 1) or 1))))
        count_item.setTextAlignment(Qt.AlignCenter)
        self.port_table.setCellWidget(table_row, 0, type_combo)
        self.port_table.setItem(table_row, 1, count_item)
        self.port_table.setCellWidget(table_row, 2, use_combo)
        type_combo.currentTextChanged.connect(self._sync_legacy_port_counts)
        use_combo.currentTextChanged.connect(self._sync_legacy_port_counts)
        self._sync_legacy_port_counts()

    def _remove_selected_port_rows(self) -> None:
        rows = sorted(
            {index.row() for index in self.port_table.selectionModel().selectedRows()},
            reverse=True,
        )
        for row in rows:
            self.port_table.removeRow(row)
        self._sync_legacy_port_counts()

    def _port_definitions(self) -> List[dict]:
        result: List[dict] = []
        for row in range(self.port_table.rowCount()):
            type_combo = self.port_table.cellWidget(row, 0)
            use_combo = self.port_table.cellWidget(row, 2)
            item = self.port_table.item(row, 1)
            try:
                count = max(0, int(item.text() if item else 0))
            except (TypeError, ValueError):
                count = 0
            if count <= 0:
                continue
            result.append(
                {
                    "port_type": _text(type_combo.currentText() if type_combo else "other").lower(),
                    "port_count": count,
                    "port_use": _text(use_combo.currentText() if use_combo else "other").lower(),
                    "name_prefix": "",
                }
            )
        return result

    def _sync_legacy_port_counts(self, *_args) -> None:
        if not hasattr(self, "port_table"):
            return
        rows = self._port_definitions()
        self.number_of_ports_spin.setValue(sum(row["port_count"] for row in rows))
        self.connections_in_spin.setValue(
            sum(row["port_count"] for row in rows if row["port_use"] == "input")
        )
        self.connections_out_spin.setValue(
            sum(
                row["port_count"]
                for row in rows
                if row["port_use"] in {"output", "downlink", "client", "pon", "patch"}
            )
        )
        self.uplink_ports_spin.setValue(
            sum(row["port_count"] for row in rows if row["port_use"] == "uplink")
        )

    def _update_visibility(self) -> None:
        asset_type = _text(self.asset_type_combo.currentData())
        self.patch_panel_type_combo.setEnabled(asset_type == "patch_panel")
        self.split_ratio_combo.setEnabled(asset_type == "fibre_splitter")
        self.olt_max_split_ratio_combo.setEnabled(
            asset_type == "optical_line_terminal"
        )
        enabled_frequencies = asset_type == "wireless_access_point"
        self.frequencies_list.setEnabled(enabled_frequencies)
        self.additional_frequencies_edit.setEnabled(enabled_frequencies)
        stacking_enabled = (
            asset_type == "network_switch" and self.supports_stacking_check.isChecked()
        )
        self.supports_stacking_check.setEnabled(asset_type == "network_switch")
        self.max_stack_members_spin.setEnabled(stacking_enabled)
        self.switch_rack_allowance_spin.setEnabled(asset_type == "network_switch")
        self.olt_units_per_u_spin.setEnabled(asset_type == "optical_line_terminal")

    def _frequencies(self) -> List[str]:
        result = []
        for index in range(self.frequencies_list.count()):
            item = self.frequencies_list.item(index)
            if item.checkState() == Qt.Checked:
                result.append(item.text())
        for value in _csv_list(self.additional_frequencies_edit.text()):
            if value not in result:
                result.append(value)
        return result

    def accept(self) -> None:
        asset_id = self.id_edit.text().strip()
        name = self.name_edit.text().strip()
        asset_type = _text(self.asset_type_combo.currentData())
        if not asset_id:
            QMessageBox.critical(self, "Invalid asset", "Asset ID is required.")
            return
        if not name:
            QMessageBox.critical(self, "Invalid asset", "Asset name is required.")
            return
        if (
            asset_type == "fibre_splitter"
            and not self.split_ratio_combo.currentText().strip()
        ):
            QMessageBox.critical(
                self, "Invalid asset", "A fibre splitter requires a split ratio."
            )
            return
        port_definitions = self._port_definitions()
        if not port_definitions:
            QMessageBox.critical(
                self, "Invalid asset", "At least one physical port row is required."
            )
            return
        frequencies = self._frequencies()
        if asset_type == "wireless_access_point" and not frequencies:
            QMessageBox.critical(
                self,
                "Invalid asset",
                "A wireless access point requires at least one frequency.",
            )
            return

        self.result = {
            "id": asset_id,
            "name": name,
            "asset_type": asset_type,
            "manufacturer": self.manufacturer_edit.text().strip(),
            "model": self.model_edit.text().strip(),
            "patch_panel_type": (
                self.patch_panel_type_combo.currentText().strip()
                if asset_type == "patch_panel"
                else ""
            ),
            "split_ratio": (
                self.split_ratio_combo.currentText().strip()
                if asset_type == "fibre_splitter"
                else ""
            ),
            "max_split_ratio": (
                self.olt_max_split_ratio_combo.currentText().strip()
                if asset_type == "optical_line_terminal"
                else ""
            ),
            "frequencies": frequencies if asset_type == "wireless_access_point" else [],
            "power_input_w": float(self.power_input_spin.value()),
            "poe_budget_w": float(self.poe_budget_spin.value()),
            "port_definitions": port_definitions,
            "number_of_ports": sum(row["port_count"] for row in port_definitions),
            "connections_in": int(self.connections_in_spin.value()),
            "connections_out": int(self.connections_out_spin.value()),
            "uplink_ports": int(self.uplink_ports_spin.value()),
            "supports_stacking": bool(
                asset_type == "network_switch"
                and self.supports_stacking_check.isChecked()
            ),
            "max_stack_members": (
                int(self.max_stack_members_spin.value())
                if asset_type == "network_switch"
                and self.supports_stacking_check.isChecked()
                else 1
            ),
            "input_connection_type": self.input_type_combo.currentText().strip(),
            "output_connection_type": self.output_type_combo.currentText().strip(),
            "uplink_connection_type": self.uplink_type_combo.currentText().strip(),
            "rack_units": int(self.rack_units_spin.value()),
            "switch_rack_unit_allowance": (
                int(self.switch_rack_allowance_spin.value())
                if asset_type == "network_switch"
                else 0
            ),
            "olt_units_per_rack_unit": (
                int(self.olt_units_per_u_spin.value())
                if asset_type == "optical_line_terminal"
                else 1
            ),
            "notes": self.notes_edit.toPlainText().strip(),
        }
        super().accept()


class NetworkInstanceEditorDialog(QDialog):
    def __init__(
        self,
        parent=None,
        instance: Optional[dict] = None,
        assets: Optional[Sequence[dict]] = None,
        locations: Optional[Sequence[dict]] = None,
        suggested_id: str = "NI1",
        default_floor: int = 0,
        default_x: float = 0.0,
        default_y: float = 0.0,
    ):
        super().__init__(parent)
        self.setWindowTitle("Installed Network Asset")
        self.instance = deepcopy(instance or {})
        self.assets = list(assets or [])
        self.locations = list(locations or [])
        self.result: Optional[dict] = None
        self.resize(560, 620)

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_edit = QLineEdit(_text(self.instance.get("id")) or suggested_id)
        self.name_edit = QLineEdit(_text(self.instance.get("name")))
        self.asset_combo = QComboBox()
        for asset in sorted(
            self.assets,
            key=lambda item: (_text(item.get("asset_type")), _text(item.get("name"))),
        ):
            asset_id = _text(asset.get("id"))
            self.asset_combo.addItem(
                f"{asset_id} - {_text(asset.get('name'))} [{_text(asset.get('asset_type'))}]",
                asset_id,
            )
        asset_index = self.asset_combo.findData(_text(self.instance.get("asset_id")))
        if asset_index >= 0:
            self.asset_combo.setCurrentIndex(asset_index)

        self.location_combo = QComboBox()
        self.location_combo.addItem("No linked location", "")
        for location in sorted(
            self.locations,
            key=lambda item: (int(item.get("floor", 0)), _text(item.get("name"))),
        ):
            name = _text(location.get("name"))
            kind = _text(location.get("kind"))
            self.location_combo.addItem(
                f"{name} - Floor {location.get('floor', 0)} [{kind}]",
                name,
            )
        location_index = self.location_combo.findData(
            _text(self.instance.get("location_name"))
        )
        if location_index >= 0:
            self.location_combo.setCurrentIndex(location_index)

        self.floor_spin = QSpinBox()
        self.floor_spin.setRange(-20, 200)
        self.floor_spin.setValue(int(self.instance.get("floor", default_floor) or 0))
        self.x_spin = QDoubleSpinBox()
        self.x_spin.setRange(-1_000_000.0, 1_000_000.0)
        self.x_spin.setDecimals(3)
        self.x_spin.setValue(float(self.instance.get("x", default_x) or 0.0))
        self.y_spin = QDoubleSpinBox()
        self.y_spin.setRange(-1_000_000.0, 1_000_000.0)
        self.y_spin.setDecimals(3)
        self.y_spin.setValue(float(self.instance.get("y", default_y) or 0.0))

        self.rack_name_edit = QLineEdit(_text(self.instance.get("rack_name")))
        self.rack_start_spin = QSpinBox()
        self.rack_start_spin.setRange(0, 1000)
        self.rack_start_spin.setSuffix("U")
        self.rack_start_spin.setValue(int(self.instance.get("rack_start_u", 0) or 0))
        self.rack_size_spin = QSpinBox()
        self.rack_size_spin.setRange(0, 200)
        self.rack_size_spin.setSuffix("U")
        self.rack_size_spin.setSpecialValueText("Default")
        self.rack_size_spin.setValue(int(self.instance.get("rack_size_u", 0) or 0))
        self.management_ip_edit = QLineEdit(_text(self.instance.get("management_ip")))
        self.management_vlan_edit = QLineEdit(
            _text(self.instance.get("management_vlan"))
        )
        self.power_feed_edit = QLineEdit(_text(self.instance.get("power_feed")))
        self.ups_source_edit = QLineEdit(_text(self.instance.get("ups_source")))
        self.notes_edit = QTextEdit(_text(self.instance.get("notes")))
        self.notes_edit.setMinimumHeight(100)

        form.addRow("Instance ID", self.id_edit)
        form.addRow("Instance name", self.name_edit)
        form.addRow("Network asset", self.asset_combo)
        form.addRow("Location", self.location_combo)
        form.addRow("Floor", self.floor_spin)
        form.addRow("X", self.x_spin)
        form.addRow("Y", self.y_spin)
        form.addRow("Rack", self.rack_name_edit)
        form.addRow("Rack start", self.rack_start_spin)
        form.addRow("Rack cabinet size", self.rack_size_spin)
        form.addRow("Management IP", self.management_ip_edit)
        form.addRow("Management VLAN", self.management_vlan_edit)
        form.addRow("Power feed", self.power_feed_edit)
        form.addRow("UPS source", self.ups_source_edit)
        form.addRow("Notes", self.notes_edit)

        self.location_combo.currentIndexChanged.connect(self._location_changed)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _location_changed(self) -> None:
        name = _text(self.location_combo.currentData())
        if not name:
            return
        location = next(
            (item for item in self.locations if _text(item.get("name")) == name), None
        )
        if location is None:
            return
        self.floor_spin.setValue(int(location.get("floor", 0) or 0))
        self.x_spin.setValue(float(location.get("x", 0.0) or 0.0))
        self.y_spin.setValue(float(location.get("y", 0.0) or 0.0))

    def accept(self) -> None:
        instance_id = self.id_edit.text().strip()
        name = self.name_edit.text().strip()
        asset_id = _text(self.asset_combo.currentData())
        if not instance_id:
            QMessageBox.critical(self, "Invalid instance", "Instance ID is required.")
            return
        if not name:
            name = instance_id
        if not asset_id:
            QMessageBox.critical(self, "Invalid instance", "Select a network asset.")
            return
        self.result = {
            "id": instance_id,
            "name": name,
            "asset_id": asset_id,
            "location_name": _text(self.location_combo.currentData()),
            "floor": int(self.floor_spin.value()),
            "x": float(self.x_spin.value()),
            "y": float(self.y_spin.value()),
            "rack_name": self.rack_name_edit.text().strip(),
            "rack_start_u": int(self.rack_start_spin.value()),
            "rack_size_u": int(self.rack_size_spin.value()),
            "management_ip": self.management_ip_edit.text().strip(),
            "management_vlan": self.management_vlan_edit.text().strip(),
            "power_feed": self.power_feed_edit.text().strip(),
            "ups_source": self.ups_source_edit.text().strip(),
            "notes": self.notes_edit.toPlainText().strip(),
        }
        super().accept()


class NetworkConnectionEditorDialog(QDialog):
    def __init__(
        self,
        parent=None,
        connection: Optional[dict] = None,
        instances: Optional[Sequence[dict]] = None,
        vlans: Optional[Sequence[dict]] = None,
        route_profiles: Optional[Sequence[str]] = None,
        suggested_id: str = "NC1",
        default_from: str = "",
        default_to: str = "",
    ):
        super().__init__(parent)
        self.setWindowTitle("Network Connection")
        self.connection = deepcopy(connection or {})
        self.instances = list(instances or [])
        self.vlans = list(vlans or [])
        self.result: Optional[dict] = None
        self.resize(580, 600)

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_edit = QLineEdit(_text(self.connection.get("id")) or suggested_id)
        self.from_combo = QComboBox()
        self.to_combo = QComboBox()
        for instance in sorted(self.instances, key=lambda item: _text(item.get("id"))):
            instance_id = _text(instance.get("id"))
            label = f"{instance_id} - {_text(instance.get('name')) or instance_id}"
            self.from_combo.addItem(label, instance_id)
            self.to_combo.addItem(label, instance_id)
        from_value = _text(self.connection.get("from_instance_id")) or default_from
        to_value = _text(self.connection.get("to_instance_id")) or default_to
        from_index = self.from_combo.findData(from_value)
        to_index = self.to_combo.findData(to_value)
        if from_index >= 0:
            self.from_combo.setCurrentIndex(from_index)
        if to_index >= 0:
            self.to_combo.setCurrentIndex(to_index)

        self.from_port_edit = QLineEdit(_text(self.connection.get("from_port")))
        self.to_port_edit = QLineEdit(_text(self.connection.get("to_port")))
        self.role_combo = QComboBox()
        self.role_combo.addItems(CONNECTION_ROLES)
        _set_combo_text(
            self.role_combo, _text(self.connection.get("connection_role")) or "output"
        )
        self.medium_combo = QComboBox()
        self.medium_combo.addItems(CONNECTION_MEDIA[:-1])
        _set_combo_text(
            self.medium_combo, _text(self.connection.get("medium")) or "copper"
        )
        self.cable_spec_edit = QLineEdit(
            _text(self.connection.get("cable_specification"))
        )
        self.fibre_count_spin = QSpinBox()
        self.fibre_count_spin.setRange(0, 100_000)
        self.fibre_count_spin.setValue(int(self.connection.get("fibre_count", 0) or 0))

        self.vlan_list = QListWidget()
        self.vlan_list.setSelectionMode(QAbstractItemView.NoSelection)
        selected_vlans = {
            _text(value)
            for value in self.connection.get("vlan_ids", [])
            if _text(value)
        }
        for vlan in sorted(
            self.vlans, key=lambda item: int(item.get("vlan_id", 0) or 0)
        ):
            row_id = _text(vlan.get("id")) or str(vlan.get("vlan_id", ""))
            label = f"{vlan.get('vlan_id', '')} - {_text(vlan.get('name'))}"
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, row_id)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if row_id in selected_vlans else Qt.Unchecked)
            self.vlan_list.addItem(item)

        self.route_profile_combo = QComboBox()
        self.route_profile_combo.addItem("")
        self.route_profile_combo.addItems(sorted(set(route_profiles or [])))
        _set_combo_text(
            self.route_profile_combo, _text(self.connection.get("route_profile"))
        )
        self.route_path_edit = QLineEdit(
            " -> ".join(
                str(value)
                for value in self.connection.get("route_path", [])
                if _text(value)
            )
        )
        self.route_path_edit.setPlaceholderText(
            "Optional graph node path, separated by ->"
        )
        self.notes_edit = QTextEdit(_text(self.connection.get("notes")))
        self.notes_edit.setMinimumHeight(90)

        form.addRow("Connection ID", self.id_edit)
        form.addRow("From asset", self.from_combo)
        form.addRow("From port", self.from_port_edit)
        form.addRow("To asset", self.to_combo)
        form.addRow("To port", self.to_port_edit)
        form.addRow("Connection role", self.role_combo)
        form.addRow("Medium", self.medium_combo)
        form.addRow("Cable specification", self.cable_spec_edit)
        form.addRow("Fibre count", self.fibre_count_spin)
        form.addRow("VLANs", self.vlan_list)
        form.addRow("Route profile", self.route_profile_combo)
        form.addRow("Route path", self.route_path_edit)
        form.addRow("Notes", self.notes_edit)

        self.medium_combo.currentTextChanged.connect(
            lambda value: self.fibre_count_spin.setEnabled(value == "fibre")
        )
        self.fibre_count_spin.setEnabled(self.medium_combo.currentText() == "fibre")

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _selected_vlans(self) -> List[str]:
        result = []
        for index in range(self.vlan_list.count()):
            item = self.vlan_list.item(index)
            if item.checkState() == Qt.Checked:
                result.append(_text(item.data(Qt.UserRole)))
        return result

    def accept(self) -> None:
        connection_id = self.id_edit.text().strip()
        from_id = _text(self.from_combo.currentData())
        to_id = _text(self.to_combo.currentData())
        if not connection_id:
            QMessageBox.critical(
                self, "Invalid connection", "Connection ID is required."
            )
            return
        if not from_id or not to_id:
            QMessageBox.critical(
                self, "Invalid connection", "Select both connection endpoints."
            )
            return
        if from_id == to_id:
            QMessageBox.critical(
                self, "Invalid connection", "Connection endpoints must be different."
            )
            return
        if (
            not self.from_port_edit.text().strip()
            or not self.to_port_edit.text().strip()
        ):
            QMessageBox.critical(
                self, "Invalid connection", "Both port identifiers are required."
            )
            return
        self.result = {
            "id": connection_id,
            "from_instance_id": from_id,
            "from_port": self.from_port_edit.text().strip(),
            "to_instance_id": to_id,
            "to_port": self.to_port_edit.text().strip(),
            "connection_role": self.role_combo.currentText().strip(),
            "medium": self.medium_combo.currentText().strip(),
            "cable_specification": self.cable_spec_edit.text().strip(),
            "fibre_count": (
                int(self.fibre_count_spin.value())
                if self.medium_combo.currentText() == "fibre"
                else 0
            ),
            "vlan_ids": self._selected_vlans(),
            "route_profile": self.route_profile_combo.currentText().strip(),
            "route_path": [
                part.strip()
                for part in self.route_path_edit.text().split("->")
                if part.strip()
            ],
            "notes": self.notes_edit.toPlainText().strip(),
        }
        super().accept()


class VlanEditorDialog(QDialog):
    def __init__(
        self, parent=None, vlan: Optional[dict] = None, suggested_id: str = "VLAN1"
    ):
        super().__init__(parent)
        self.setWindowTitle("VLAN")
        self.vlan = deepcopy(vlan or {})
        self.result: Optional[dict] = None
        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)
        self.id_edit = QLineEdit(_text(self.vlan.get("id")) or suggested_id)
        self.vlan_id_spin = QSpinBox()
        self.vlan_id_spin.setRange(1, 4094)
        self.vlan_id_spin.setValue(int(self.vlan.get("vlan_id", 1) or 1))
        self.name_edit = QLineEdit(_text(self.vlan.get("name")))
        self.purpose_edit = QLineEdit(_text(self.vlan.get("purpose")))
        self.subnet_edit = QLineEdit(_text(self.vlan.get("subnet")))
        self.gateway_edit = QLineEdit(_text(self.vlan.get("gateway")))
        self.dhcp_scope_edit = QLineEdit(_text(self.vlan.get("dhcp_scope")))
        self.security_zone_edit = QLineEdit(_text(self.vlan.get("security_zone")))
        self.notes_edit = QTextEdit(_text(self.vlan.get("notes")))
        for label, widget in [
            ("Record ID", self.id_edit),
            ("VLAN ID", self.vlan_id_spin),
            ("Name", self.name_edit),
            ("Purpose", self.purpose_edit),
            ("Subnet", self.subnet_edit),
            ("Gateway", self.gateway_edit),
            ("DHCP scope", self.dhcp_scope_edit),
            ("Security zone", self.security_zone_edit),
            ("Notes", self.notes_edit),
        ]:
            form.addRow(label, widget)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self) -> None:
        if not self.id_edit.text().strip() or not self.name_edit.text().strip():
            QMessageBox.critical(
                self, "Invalid VLAN", "Record ID and name are required."
            )
            return
        self.result = {
            "id": self.id_edit.text().strip(),
            "vlan_id": int(self.vlan_id_spin.value()),
            "name": self.name_edit.text().strip(),
            "purpose": self.purpose_edit.text().strip(),
            "subnet": self.subnet_edit.text().strip(),
            "gateway": self.gateway_edit.text().strip(),
            "dhcp_scope": self.dhcp_scope_edit.text().strip(),
            "security_zone": self.security_zone_edit.text().strip(),
            "notes": self.notes_edit.toPlainText().strip(),
        }
        super().accept()


class RouteEditorDialog(QDialog):
    def __init__(
        self,
        parent=None,
        route: Optional[dict] = None,
        suggested_id: str = "NR1",
        vlans: Optional[Sequence[dict]] = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Network Route")
        self.route = deepcopy(route or {})
        self.result: Optional[dict] = None
        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)
        self.id_edit = QLineEdit(_text(self.route.get("id")) or suggested_id)
        self.source_edit = QLineEdit(_text(self.route.get("source")))
        self.destination_edit = QLineEdit(_text(self.route.get("destination")))
        self.vlan_combo = QComboBox()
        self.vlan_combo.setEditable(True)
        self.vlan_combo.addItem("")
        for vlan in sorted(
            vlans or [], key=lambda item: int(item.get("vlan_id", 0) or 0)
        ):
            self.vlan_combo.addItem(
                f"{vlan.get('vlan_id', '')} - {_text(vlan.get('name'))}",
                _text(vlan.get("id")),
            )
        index = self.vlan_combo.findData(_text(self.route.get("vlan_id")))
        if index >= 0:
            self.vlan_combo.setCurrentIndex(index)
        elif _text(self.route.get("vlan_id")):
            self.vlan_combo.setEditText(_text(self.route.get("vlan_id")))
        self.protocol_combo = QComboBox()
        self.protocol_combo.setEditable(True)
        self.protocol_combo.addItems(["static", "OSPF", "BGP", "connected", "default"])
        _set_combo_text(
            self.protocol_combo, _text(self.route.get("protocol")) or "static"
        )
        self.next_hop_edit = QLineEdit(_text(self.route.get("next_hop")))
        self.metric_spin = QSpinBox()
        self.metric_spin.setRange(0, 1_000_000)
        self.metric_spin.setValue(int(self.route.get("metric", 0) or 0))
        self.firewall_policy_edit = QLineEdit(_text(self.route.get("firewall_policy")))
        self.notes_edit = QTextEdit(_text(self.route.get("notes")))
        for label, widget in [
            ("Route ID", self.id_edit),
            ("Source", self.source_edit),
            ("Destination", self.destination_edit),
            ("VLAN", self.vlan_combo),
            ("Protocol", self.protocol_combo),
            ("Next hop", self.next_hop_edit),
            ("Metric", self.metric_spin),
            ("Firewall policy", self.firewall_policy_edit),
            ("Notes", self.notes_edit),
        ]:
            form.addRow(label, widget)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self) -> None:
        if not self.id_edit.text().strip() or not self.destination_edit.text().strip():
            QMessageBox.critical(
                self, "Invalid route", "Route ID and destination are required."
            )
            return
        vlan_value = (
            _text(self.vlan_combo.currentData())
            or self.vlan_combo.currentText().strip()
        )
        self.result = {
            "id": self.id_edit.text().strip(),
            "source": self.source_edit.text().strip(),
            "destination": self.destination_edit.text().strip(),
            "vlan_id": vlan_value,
            "protocol": self.protocol_combo.currentText().strip(),
            "next_hop": self.next_hop_edit.text().strip(),
            "metric": int(self.metric_spin.value()),
            "firewall_policy": self.firewall_policy_edit.text().strip(),
            "notes": self.notes_edit.toPlainText().strip(),
        }
        super().accept()


class _CrudTab(QWidget):
    def __init__(self, headers: Sequence[str], parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, len(headers))
        self.table.setHorizontalHeaderLabels(list(headers))
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.doubleClicked.connect(lambda _index: self.edit_requested())
        layout.addWidget(self.table, 1)
        row = QHBoxLayout()
        layout.addLayout(row)
        self.add_button = QPushButton("Add")
        self.edit_button = QPushButton("Edit")
        self.delete_button = QPushButton("Delete")
        row.addWidget(self.add_button)
        row.addWidget(self.edit_button)
        row.addWidget(self.delete_button)
        row.addStretch(1)
        self.edit_requested: Callable[[], None] = lambda: None

    def selected_row(self) -> int:
        rows = self.table.selectionModel().selectedRows()
        return rows[0].row() if rows else -1

    def set_rows(self, rows: Sequence[Sequence[object]]) -> None:
        self.table.setRowCount(len(rows))
        for row_index, values in enumerate(rows):
            for column_index, value in enumerate(values):
                self.table.setItem(
                    row_index,
                    column_index,
                    QTableWidgetItem(str(value if value is not None else "")),
                )


class NetworkPlannerDialog(QDialog):
    def __init__(self, parent, data: dict, on_save: Callable[[dict], None]):
        super().__init__(parent)
        self.setWindowTitle("Network Planning")
        self.resize(1250, 780)
        self.data = deepcopy(data)
        self.on_save = on_save

        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs, 1)

        settings_tab = QWidget()
        settings_layout = QFormLayout(settings_tab)
        self.technology_combo = QComboBox()
        self.technology_combo.addItems(NETWORK_TECHNOLOGIES)
        _set_combo_text(
            self.technology_combo,
            _text(self.data.get("network_settings", {}).get("technology"))
            or "Traditional",
        )
        self.expected_mer_spin = QSpinBox()
        self.expected_mer_spin.setRange(1, 20)
        self.expected_mer_spin.setValue(
            int(self.data.get("network_settings", {}).get("expected_mer_count", 2) or 2)
        )
        self.redundant_core_check = QCheckBox("Use redundant core / MER design")
        self.redundant_core_check.setChecked(
            bool(self.data.get("network_settings", {}).get("redundant_core", True))
        )

        settings = self.data.get("network_settings", {})
        self.spare_capacity_spin = QDoubleSpinBox()
        self.spare_capacity_spin.setRange(0.0, 100.0)
        self.spare_capacity_spin.setDecimals(1)
        self.spare_capacity_spin.setSuffix(" %")
        self.spare_capacity_spin.setValue(
            float(settings.get("spare_capacity_percent", 15.0) or 0.0)
        )

        self.traditional_max_copper_spin = QDoubleSpinBox()
        self.traditional_max_copper_spin.setRange(1.0, 1000.0)
        self.traditional_max_copper_spin.setDecimals(1)
        self.traditional_max_copper_spin.setSuffix(" m")
        self.traditional_max_copper_spin.setValue(
            float(settings.get("traditional_max_copper_m", 90.0) or 90.0)
        )

        self.polan_max_copper_spin = QDoubleSpinBox()
        self.polan_max_copper_spin.setRange(1.0, 500.0)
        self.polan_max_copper_spin.setDecimals(1)
        self.polan_max_copper_spin.setSuffix(" m")
        self.polan_max_copper_spin.setValue(
            float(settings.get("polan_max_ont_copper_m", 30.0) or 30.0)
        )

        self.polan_max_onts_per_splitter_spin = QSpinBox()
        self.polan_max_onts_per_splitter_spin.setRange(1, 512)
        self.polan_max_onts_per_splitter_spin.setValue(
            int(settings.get("polan_max_onts_per_splitter", 8) or 8)
        )
        self.polan_max_onts_per_splitter_spin.setToolTip(
            "Maximum ONT/device connections that the auto-planner may allocate to one passive splitter before starting another local splitter group."
        )

        self.polan_max_splitter_route_spin = QDoubleSpinBox()
        self.polan_max_splitter_route_spin.setRange(0.0, 5000.0)
        self.polan_max_splitter_route_spin.setDecimals(1)
        self.polan_max_splitter_route_spin.setSuffix(" m")
        self.polan_max_splitter_route_spin.setValue(
            float(settings.get("polan_max_splitter_ont_route_m", 120.0) or 120.0)
        )
        self.polan_max_splitter_route_spin.setToolTip(
            "Maximum graph-route distance from a splitter to any connected ONT. Set to 0 to disable the distance gate."
        )

        self.default_rack_size_spin = QSpinBox()
        self.default_rack_size_spin.setRange(1, 200)
        self.default_rack_size_spin.setSuffix("U")
        self.default_rack_size_spin.setValue(
            int(settings.get("default_rack_size_u", 42) or 42)
        )

        self.olt_failover_check = QCheckBox(
            "Provide standby OLT failover for each protected splitter"
        )
        self.olt_failover_check.setChecked(
            bool(settings.get("polan_olt_failover", True))
        )

        self.auto_design_button = QPushButton("Generate Minimum-Component Network")
        self.auto_design_button.clicked.connect(self.generate_automatic_design)

        self.clear_installed_button = QPushButton(
            "Clear Installed Assets and Connections"
        )
        self.clear_installed_button.setToolTip(
            "Remove installed network asset instances, physical connections, "
            "endpoint assignments and generated redundancy records. The network "
            "asset library, settings, VLANs and routing records are preserved."
        )
        self.clear_installed_button.clicked.connect(
            self.clear_installed_assets_and_connections
        )

        self.visual_edit_button = QPushButton("Edit visually on plan")
        self.visual_edit_button.setToolTip(
            "Save this generated network and return to the drawing canvas for drag/drop editing"
        )
        self.visual_edit_button.clicked.connect(self.edit_visually_on_plan)

        info = QLabel(
            "MER locations are treated as roots of the network tree. PoLAN locations can be used for optical distribution, OLT, splitter or ONT placement."
        )
        info.setWordWrap(True)
        settings_layout.addRow("Network technology", self.technology_combo)
        settings_layout.addRow("Expected MER locations", self.expected_mer_spin)
        settings_layout.addRow("Spare port and PoE capacity", self.spare_capacity_spin)
        settings_layout.addRow(
            "Traditional maximum copper route", self.traditional_max_copper_spin
        )
        settings_layout.addRow(
            "PoLAN maximum ONT copper distance", self.polan_max_copper_spin
        )
        settings_layout.addRow(
            "PoLAN maximum ONTs per splitter", self.polan_max_onts_per_splitter_spin
        )
        settings_layout.addRow(
            "PoLAN maximum splitter-to-ONT route", self.polan_max_splitter_route_spin
        )
        settings_layout.addRow("Default rack cabinet size", self.default_rack_size_spin)
        settings_layout.addRow("", self.redundant_core_check)
        settings_layout.addRow("", self.olt_failover_check)
        settings_layout.addRow("", self.auto_design_button)
        settings_layout.addRow("", self.clear_installed_button)
        settings_layout.addRow("", self.visual_edit_button)
        settings_layout.addRow("", info)
        self.tabs.addTab(settings_tab, "Settings")

        self.assets_tab = _CrudTab(
            [
                "ID",
                "Name",
                "Type",
                "Ports",
                "In",
                "Out",
                "Uplinks",
                "Stack",
                "Max stack",
                "Power W",
                "PoE W",
                "Rack U",
                "Switch U",
                "OLT units/U",
            ]
        )
        self.instances_tab = _CrudTab(
            [
                "ID",
                "Name",
                "Asset",
                "Location",
                "Floor",
                "Rack",
                "Start U",
                "Rack U",
                "Management IP",
            ]
        )
        self.connections_tab = _CrudTab(
            ["ID", "From", "Port", "To", "Port", "Role", "Medium", "Cable", "VLANs"]
        )
        self.vlans_tab = _CrudTab(
            ["Record ID", "VLAN", "Name", "Purpose", "Subnet", "Gateway", "Zone"]
        )
        self.routes_tab = _CrudTab(
            [
                "ID",
                "Source",
                "Destination",
                "VLAN",
                "Protocol",
                "Next hop",
                "Metric",
                "Policy",
            ]
        )

        self.tabs.addTab(self.assets_tab, "Asset Library")
        self.tabs.addTab(self.instances_tab, "Installed Assets")
        self.tabs.addTab(self.connections_tab, "Connections")
        self.tabs.addTab(self.vlans_tab, "VLANs")
        self.tabs.addTab(self.routes_tab, "Routing")

        self.summary_text = QTextEdit()
        self.summary_text.setReadOnly(True)
        self.tabs.addTab(self.summary_text, "Generated Design")

        self.assets_tab.add_button.clicked.connect(self.add_asset)
        self.assets_tab.edit_button.clicked.connect(self.edit_asset)
        self.assets_tab.delete_button.clicked.connect(self.delete_asset)
        self.assets_tab.edit_requested = self.edit_asset

        self.instances_tab.add_button.clicked.connect(self.add_instance)
        self.instances_tab.edit_button.clicked.connect(self.edit_instance)
        self.instances_tab.delete_button.clicked.connect(self.delete_instance)
        self.instances_tab.edit_requested = self.edit_instance

        self.connections_tab.add_button.clicked.connect(self.add_connection)
        self.connections_tab.edit_button.clicked.connect(self.edit_connection)
        self.connections_tab.delete_button.clicked.connect(self.delete_connection)
        self.connections_tab.edit_requested = self.edit_connection

        self.vlans_tab.add_button.clicked.connect(self.add_vlan)
        self.vlans_tab.edit_button.clicked.connect(self.edit_vlan)
        self.vlans_tab.delete_button.clicked.connect(self.delete_vlan)
        self.vlans_tab.edit_requested = self.edit_vlan

        self.routes_tab.add_button.clicked.connect(self.add_route)
        self.routes_tab.edit_button.clicked.connect(self.edit_route)
        self.routes_tab.delete_button.clicked.connect(self.delete_route)
        self.routes_tab.edit_requested = self.edit_route

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Close)
        buttons.button(QDialogButtonBox.Save).clicked.connect(self.save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.refresh_tables()

    def _items(self, key: str) -> List[dict]:
        return self.data.setdefault(key, [])

    def _selected(self, tab: _CrudTab, key: str) -> Tuple[int, Optional[dict]]:
        row = tab.selected_row()
        items = self._items(key)
        if row < 0 or row >= len(items):
            return -1, None
        return row, items[row]

    def _replace_or_append(self, key: str, old_index: int, value: dict) -> bool:
        items = self._items(key)
        value_id = _text(value.get("id"))
        for index, item in enumerate(items):
            if index != old_index and _text(item.get("id")) == value_id:
                QMessageBox.critical(
                    self, "Duplicate ID", f"ID {value_id} already exists."
                )
                return False
        if old_index < 0:
            items.append(value)
        else:
            old_id = _text(items[old_index].get("id"))
            items[old_index] = value
            if old_id != value_id:
                self._rename_reference(key, old_id, value_id)
        self.refresh_tables()
        return True

    def _rename_reference(self, key: str, old_id: str, new_id: str) -> None:
        if key == "network_assets":
            for instance in self._items("network_asset_instances"):
                if _text(instance.get("asset_id")) == old_id:
                    instance["asset_id"] = new_id
        elif key == "network_asset_instances":
            for connection in self._items("network_connections"):
                if _text(connection.get("from_instance_id")) == old_id:
                    connection["from_instance_id"] = new_id
                if _text(connection.get("to_instance_id")) == old_id:
                    connection["to_instance_id"] = new_id
            for assignment in self._items("network_endpoint_assignments"):
                if _text(assignment.get("network_instance_id")) == old_id:
                    assignment["network_instance_id"] = new_id
        elif key == "network_vlans":
            for connection in self._items("network_connections"):
                connection["vlan_ids"] = [
                    new_id if _text(value) == old_id else value
                    for value in connection.get("vlan_ids", [])
                ]
            for route in self._items("network_routes"):
                if _text(route.get("vlan_id")) == old_id:
                    route["vlan_id"] = new_id

    def refresh_tables(self) -> None:
        self.assets_tab.set_rows(
            [
                [
                    item.get("id", ""),
                    item.get("name", ""),
                    item.get("asset_type", ""),
                    item.get("number_of_ports", 0),
                    item.get("connections_in", 0),
                    item.get("connections_out", 0),
                    item.get("uplink_ports", 0),
                    "Yes" if item.get("supports_stacking", False) else "No",
                    item.get("max_stack_members", 1),
                    item.get("power_input_w", 0),
                    item.get("poe_budget_w", 0),
                    item.get("rack_units", 0),
                    item.get("switch_rack_unit_allowance", 0),
                    item.get("olt_units_per_rack_unit", 1),
                ]
                for item in self._items("network_assets")
            ]
        )
        self.instances_tab.set_rows(
            [
                [
                    item.get("id", ""),
                    item.get("name", ""),
                    item.get("asset_id", ""),
                    item.get("location_name", ""),
                    item.get("floor", 0),
                    item.get("rack_name", ""),
                    item.get("rack_start_u", 0),
                    item.get("rack_size_u", 0) or "",
                    item.get("management_ip", ""),
                ]
                for item in self._items("network_asset_instances")
            ]
        )
        self.connections_tab.set_rows(
            [
                [
                    item.get("id", ""),
                    item.get("from_instance_id", ""),
                    item.get("from_port", ""),
                    item.get("to_instance_id", ""),
                    item.get("to_port", ""),
                    item.get("connection_role", ""),
                    item.get("medium", ""),
                    item.get("cable_specification", ""),
                    ", ".join(item.get("vlan_ids", [])),
                ]
                for item in self._items("network_connections")
            ]
        )
        self.vlans_tab.set_rows(
            [
                [
                    item.get("id", ""),
                    item.get("vlan_id", ""),
                    item.get("name", ""),
                    item.get("purpose", ""),
                    item.get("subnet", ""),
                    item.get("gateway", ""),
                    item.get("security_zone", ""),
                ]
                for item in self._items("network_vlans")
            ]
        )
        self.routes_tab.set_rows(
            [
                [
                    item.get("id", ""),
                    item.get("source", ""),
                    item.get("destination", ""),
                    item.get("vlan_id", ""),
                    item.get("protocol", ""),
                    item.get("next_hop", ""),
                    item.get("metric", 0),
                    item.get("firewall_policy", ""),
                ]
                for item in self._items("network_routes")
            ]
        )
        self._refresh_design_summary()

    def _sync_planner_settings(self) -> None:
        settings = self.data.setdefault("network_settings", {})
        settings["technology"] = self.technology_combo.currentText().strip()
        settings["expected_mer_count"] = int(self.expected_mer_spin.value())
        settings["redundant_core"] = self.redundant_core_check.isChecked()
        settings["spare_capacity_percent"] = float(self.spare_capacity_spin.value())
        settings["traditional_max_copper_m"] = float(
            self.traditional_max_copper_spin.value()
        )
        settings["polan_max_ont_copper_m"] = float(self.polan_max_copper_spin.value())
        settings["polan_max_onts_per_splitter"] = int(
            self.polan_max_onts_per_splitter_spin.value()
        )
        settings["polan_max_splitter_ont_route_m"] = float(
            self.polan_max_splitter_route_spin.value()
        )
        settings["default_rack_size_u"] = int(self.default_rack_size_spin.value())
        settings["polan_olt_failover"] = self.olt_failover_check.isChecked()

    def _refresh_design_summary(self) -> None:
        summary = self.data.get("network_design_summary", {})
        if not isinstance(summary, dict) or not summary:
            self.summary_text.setPlainText(
                "No automatic network design has been generated."
            )
            return
        lines = [
            f"Technology: {summary.get('technology', '')}",
            f"Objective: {summary.get('objective', '')}",
            "",
            f"Endpoint locations: {summary.get('endpoint_locations', 0)}",
            f"Required ports: {summary.get('required_ports', 0)}",
            f"Required PoE: {summary.get('required_poe_w', 0)} W",
            f"Installed endpoint ports: {summary.get('installed_endpoint_ports', 0)}",
            f"Installed PoE budget: {summary.get('installed_poe_budget_w', 0)} W",
            f"Spare capacity: {summary.get('spare_capacity_percent', 0)}%",
            f"Estimated copper: {summary.get('estimated_copper_length_m', 0)} m",
            f"Estimated fibre: {summary.get('estimated_fibre_length_m', 0)} m",
            f"PoLAN ONTs per splitter limit: {summary.get('polan_max_onts_per_splitter', '')}",
            f"PoLAN splitter-to-ONT route limit: {summary.get('polan_max_splitter_ont_route_m', '')} m",
            "",
            "Components:",
        ]
        for role, quantity in (summary.get("component_counts", {}) or {}).items():
            lines.append(f"  {role}: {quantity}")
        warnings = summary.get("warnings", []) or []
        if warnings:
            lines.extend(["", "Warnings:"])
            lines.extend(f"  • {warning}" for warning in warnings)
        self.summary_text.setPlainText("\n".join(lines))

    def generate_automatic_design(self) -> None:
        self._sync_planner_settings()
        technology = self.technology_combo.currentText().strip()
        existing_auto = any(
            bool(item.get("auto_generated"))
            for item in self.data.get("network_asset_instances", [])
            if isinstance(item, dict)
        )
        if (
            existing_auto
            and QMessageBox.question(
                self,
                "Regenerate network",
                "Replace the previous automatically generated network configuration? "
                "Manually placed network assets will be preserved.",
            )
            != QMessageBox.Yes
        ):
            return
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            summary = generate_network_design(self.data, technology)
        except NetworkPlanningError as exc:
            QMessageBox.critical(self, "Automatic network planning", str(exc))
            return
        except Exception as exc:
            QMessageBox.critical(
                self, "Automatic network planning", f"Unexpected planning error:\n{exc}"
            )
            return
        finally:
            QApplication.restoreOverrideCursor()
        self.refresh_tables()
        self.on_save(deepcopy(self.data))
        self.tabs.setCurrentWidget(self.summary_text)
        QMessageBox.information(
            self,
            "Automatic network planning",
            f"Generated {summary.get('technology', technology)} network configuration.\n\n"
            f"Endpoint ports: {summary.get('required_ports', 0)}\n"
            f"Generated components: {summary.get('auto_generated_instances', 0)}\n"
            f"Copper: {summary.get('estimated_copper_length_m', 0)} m\n"
            f"Fibre: {summary.get('estimated_fibre_length_m', 0)} m",
        )

    def clear_installed_assets_and_connections(self) -> None:
        instance_count = len(self.data.get("network_asset_instances", []))
        connection_count = len(self.data.get("network_connections", []))
        assignment_count = len(self.data.get("network_endpoint_assignments", []))
        redundancy_count = len(self.data.get("network_redundancy_groups", []))

        if not any(
            (instance_count, connection_count, assignment_count, redundancy_count)
        ):
            QMessageBox.information(
                self,
                "Clear installed networking",
                "There are no installed network assets or connections to clear.",
            )
            return

        message = (
            "Clear all installed network assets and physical connections?\n\n"
            f"Installed assets: {instance_count}\n"
            f"Connections: {connection_count}\n"
            f"Endpoint assignments: {assignment_count}\n"
            f"Redundancy groups: {redundancy_count}\n\n"
            "The network asset library, planner settings, VLANs and routing "
            "records will be preserved."
        )
        if (
            QMessageBox.question(
                self,
                "Clear installed networking",
                message,
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            != QMessageBox.Yes
        ):
            return

        self.data["network_asset_instances"] = []
        self.data["network_connections"] = []
        self.data["network_endpoint_assignments"] = []
        self.data["network_redundancy_groups"] = []
        self.data["network_design_summary"] = {}

        self.refresh_tables()
        self.on_save(deepcopy(self.data))

        QMessageBox.information(
            self,
            "Clear installed networking",
            "Installed network assets and connections have been cleared.",
        )

    def edit_visually_on_plan(self) -> None:
        self._sync_planner_settings()
        self.on_save(deepcopy(self.data))
        parent = self.parent()
        setter = getattr(parent, "_set_editor_mode", None)
        if callable(setter):
            setter("network_asset")
        else:
            mode_combo = getattr(parent, "mode_combo", None)
            if mode_combo is not None:
                index = mode_combo.findText("network_asset")
                if index < 0:
                    index = mode_combo.findData("network_asset")
                if index >= 0:
                    mode_combo.setCurrentIndex(index)
        self.accept()

    def add_asset(self) -> None:
        dialog = NetworkAssetEditorDialog(
            self, suggested_id=_next_id(self._items("network_assets"), "NA")
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_assets", -1, dialog.result)

    def edit_asset(self) -> None:
        index, item = self._selected(self.assets_tab, "network_assets")
        if item is None:
            return
        dialog = NetworkAssetEditorDialog(
            self, item, suggested_id=_text(item.get("id"))
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_assets", index, dialog.result)

    def delete_asset(self) -> None:
        index, item = self._selected(self.assets_tab, "network_assets")
        if item is None:
            return
        asset_id = _text(item.get("id"))
        if any(
            _text(instance.get("asset_id")) == asset_id
            for instance in self._items("network_asset_instances")
        ):
            QMessageBox.critical(
                self,
                "Asset in use",
                "Delete installed instances that use this asset first.",
            )
            return
        if (
            QMessageBox.question(self, "Delete network asset", f"Delete {asset_id}?")
            == QMessageBox.Yes
        ):
            self._items("network_assets").pop(index)
            self.refresh_tables()

    def add_instance(self) -> None:
        if not self._items("network_assets"):
            QMessageBox.information(
                self, "No network assets", "Create a network asset definition first."
            )
            return
        dialog = NetworkInstanceEditorDialog(
            self,
            assets=self._items("network_assets"),
            locations=self.data.get("locations", []),
            suggested_id=_next_id(self._items("network_asset_instances"), "NI"),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_asset_instances", -1, dialog.result)

    def edit_instance(self) -> None:
        index, item = self._selected(self.instances_tab, "network_asset_instances")
        if item is None:
            return
        dialog = NetworkInstanceEditorDialog(
            self,
            item,
            self._items("network_assets"),
            self.data.get("locations", []),
            suggested_id=_text(item.get("id")),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_asset_instances", index, dialog.result)

    def delete_instance(self) -> None:
        index, item = self._selected(self.instances_tab, "network_asset_instances")
        if item is None:
            return
        instance_id = _text(item.get("id"))
        if (
            QMessageBox.question(
                self,
                "Delete installed asset",
                f"Delete {instance_id} and all of its network connections?",
            )
            != QMessageBox.Yes
        ):
            return
        self._items("network_asset_instances").pop(index)
        self.data["network_connections"] = [
            connection
            for connection in self._items("network_connections")
            if _text(connection.get("from_instance_id")) != instance_id
            and _text(connection.get("to_instance_id")) != instance_id
        ]
        self.data["network_endpoint_assignments"] = [
            assignment
            for assignment in self._items("network_endpoint_assignments")
            if _text(assignment.get("network_instance_id")) != instance_id
        ]
        self.refresh_tables()

    def add_connection(self) -> None:
        if len(self._items("network_asset_instances")) < 2:
            QMessageBox.information(
                self, "Not enough assets", "Install at least two network assets first."
            )
            return
        dialog = NetworkConnectionEditorDialog(
            self,
            instances=self._items("network_asset_instances"),
            vlans=self._items("network_vlans"),
            route_profiles=list(self.data.get("route_profiles", {}).keys()),
            suggested_id=_next_id(self._items("network_connections"), "NC"),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_connections", -1, dialog.result)

    def edit_connection(self) -> None:
        index, item = self._selected(self.connections_tab, "network_connections")
        if item is None:
            return
        dialog = NetworkConnectionEditorDialog(
            self,
            item,
            self._items("network_asset_instances"),
            self._items("network_vlans"),
            list(self.data.get("route_profiles", {}).keys()),
            suggested_id=_text(item.get("id")),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_connections", index, dialog.result)

    def delete_connection(self) -> None:
        index, item = self._selected(self.connections_tab, "network_connections")
        if (
            item is not None
            and QMessageBox.question(
                self, "Delete connection", f"Delete {_text(item.get('id'))}?"
            )
            == QMessageBox.Yes
        ):
            self._items("network_connections").pop(index)
            self.refresh_tables()

    def add_vlan(self) -> None:
        dialog = VlanEditorDialog(
            self, suggested_id=_next_id(self._items("network_vlans"), "VLAN")
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_vlans", -1, dialog.result)

    def edit_vlan(self) -> None:
        index, item = self._selected(self.vlans_tab, "network_vlans")
        if item is None:
            return
        dialog = VlanEditorDialog(self, item, suggested_id=_text(item.get("id")))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_vlans", index, dialog.result)

    def delete_vlan(self) -> None:
        index, item = self._selected(self.vlans_tab, "network_vlans")
        if item is None:
            return
        vlan_id = _text(item.get("id"))
        if (
            QMessageBox.question(
                self, "Delete VLAN", f"Delete {vlan_id} and remove its references?"
            )
            != QMessageBox.Yes
        ):
            return
        self._items("network_vlans").pop(index)
        for connection in self._items("network_connections"):
            connection["vlan_ids"] = [
                value
                for value in connection.get("vlan_ids", [])
                if _text(value) != vlan_id
            ]
        for route in self._items("network_routes"):
            if _text(route.get("vlan_id")) == vlan_id:
                route["vlan_id"] = ""
        self.refresh_tables()

    def add_route(self) -> None:
        dialog = RouteEditorDialog(
            self,
            suggested_id=_next_id(self._items("network_routes"), "NR"),
            vlans=self._items("network_vlans"),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_routes", -1, dialog.result)

    def edit_route(self) -> None:
        index, item = self._selected(self.routes_tab, "network_routes")
        if item is None:
            return
        dialog = RouteEditorDialog(
            self, item, _text(item.get("id")), self._items("network_vlans")
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_routes", index, dialog.result)

    def delete_route(self) -> None:
        index, item = self._selected(self.routes_tab, "network_routes")
        if (
            item is not None
            and QMessageBox.question(
                self, "Delete route", f"Delete {_text(item.get('id'))}?"
            )
            == QMessageBox.Yes
        ):
            self._items("network_routes").pop(index)
            self.refresh_tables()

    def save(self) -> None:
        self._sync_planner_settings()
        self.on_save(deepcopy(self.data))
        QMessageBox.information(
            self, "Network planning", "Network planning data saved."
        )
