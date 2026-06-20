"""Editors for physical fibre, patching, peering and IP planning records."""
from __future__ import annotations

from copy import deepcopy
from typing import List, Optional, Sequence

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from network_services import FIBRE_COLOURS, build_fibre_cores, cable_core_statistics


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _set_combo(combo: QComboBox, value: str) -> None:
    index = combo.findData(value)
    if index < 0:
        index = combo.findText(value)
    if index >= 0:
        combo.setCurrentIndex(index)
    elif combo.isEditable():
        combo.setEditText(value)


class FibreNodeEditorDialog(QDialog):
    NODE_TYPES = [
        ("splice_enclosure", "Splice enclosure"),
        ("splice_cassette", "Splice cassette"),
        ("fibre_joint", "Fibre joint"),
        ("termination", "Termination"),
        ("handhole", "Handhole"),
        ("chamber", "Chamber"),
    ]

    def __init__(
        self,
        parent=None,
        node: Optional[dict] = None,
        nodes: Optional[Sequence[dict]] = None,
        instances: Optional[Sequence[dict]] = None,
        locations: Optional[Sequence[dict]] = None,
        suggested_id: str = "FN1",
        default_floor: int = 0,
        default_x: float = 0.0,
        default_y: float = 0.0,
    ):
        super().__init__(parent)
        self.setWindowTitle("Physical Fibre Node")
        self.node = deepcopy(node or {})
        self.result: Optional[dict] = None
        self.resize(620, 680)
        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.id_edit = QLineEdit(_text(self.node.get("id")) or suggested_id)
        self.name_edit = QLineEdit(_text(self.node.get("name")))
        self.type_combo = QComboBox()
        for value, label in self.NODE_TYPES:
            self.type_combo.addItem(label, value)
        _set_combo(self.type_combo, _text(self.node.get("node_type")) or "splice_enclosure")

        self.location_combo = QComboBox()
        self.location_combo.addItem("No linked location", "")
        for location in sorted(locations or [], key=lambda row: (_int(row.get("floor")), _text(row.get("name")))):
            name = _text(location.get("name"))
            if name:
                self.location_combo.addItem(f"{name} · Floor {_int(location.get('floor'))}", name)
        _set_combo(self.location_combo, _text(self.node.get("location_name")))

        self.instance_combo = QComboBox()
        self.instance_combo.addItem("No linked network instance", "")
        for instance in sorted(instances or [], key=lambda row: _text(row.get("name") or row.get("id"))):
            iid = _text(instance.get("id"))
            self.instance_combo.addItem(f"{_text(instance.get('name')) or iid} [{iid}]", iid)
        _set_combo(self.instance_combo, _text(self.node.get("linked_instance_id")))

        self.parent_combo = QComboBox()
        self.parent_combo.addItem("No parent", "")
        own_id = _text(self.node.get("id"))
        for parent_node in sorted(nodes or [], key=lambda row: _text(row.get("name") or row.get("id"))):
            pid = _text(parent_node.get("id"))
            if pid and pid != own_id:
                self.parent_combo.addItem(f"{_text(parent_node.get('name')) or pid} [{pid}]", pid)
        _set_combo(self.parent_combo, _text(self.node.get("parent_node_id")))

        self.floor_spin = QSpinBox(); self.floor_spin.setRange(-20, 200); self.floor_spin.setValue(_int(self.node.get("floor"), default_floor))
        self.x_spin = QDoubleSpinBox(); self.x_spin.setRange(-1e9, 1e9); self.x_spin.setDecimals(3); self.x_spin.setValue(float(self.node.get("x", default_x) or 0.0))
        self.y_spin = QDoubleSpinBox(); self.y_spin.setRange(-1e9, 1e9); self.y_spin.setDecimals(3); self.y_spin.setValue(float(self.node.get("y", default_y) or 0.0))
        self.rack_edit = QLineEdit(_text(self.node.get("rack_name")))
        self.rack_u_spin = QSpinBox(); self.rack_u_spin.setRange(0, 1000); self.rack_u_spin.setValue(_int(self.node.get("rack_start_u")))
        self.rack_units_spin = QSpinBox(); self.rack_units_spin.setRange(0, 100); self.rack_units_spin.setValue(_int(self.node.get("rack_units"), 1))
        self.capacity_spin = QSpinBox(); self.capacity_spin.setRange(0, 100000); self.capacity_spin.setValue(_int(self.node.get("splice_capacity", self.node.get("cassette_capacity", 12)), 12))
        self.cassette_position_spin = QSpinBox(); self.cassette_position_spin.setRange(1, 4); self.cassette_position_spin.setValue(max(1, min(4, _int(self.node.get("cassette_position", self.node.get("tray_number")), 1))))
        self.cassette_front_combo = QComboBox(); self.cassette_front_combo.addItem("LC duplex", "lc_duplex"); self.cassette_front_combo.addItem("SC simplex", "sc_simplex"); self.cassette_front_combo.addItem("SC duplex", "sc_duplex"); _set_combo(self.cassette_front_combo, _text(self.node.get("front_connector")) or "lc_duplex")
        self.cassette_mode_combo = QComboBox(); self.cassette_mode_combo.addItem("Spliced / pigtail", "spliced"); self.cassette_mode_combo.addItem("Connectorised MPO/MTP", "connectorised"); _set_combo(self.cassette_mode_combo, _text(self.node.get("termination_mode")) or "spliced")
        self.cassette_rear_combo = QComboBox(); self.cassette_rear_combo.addItem("Splice", "splice"); self.cassette_rear_combo.addItem("MPO-12", "mpo-12"); self.cassette_rear_combo.addItem("MTP-12", "mtp-12"); self.cassette_rear_combo.addItem("MPO-24", "mpo-24"); self.cassette_rear_combo.addItem("MTP-24", "mtp-24"); _set_combo(self.cassette_rear_combo, _text(self.node.get("rear_connector")) or "splice")
        self.cassette_rear_count_spin = QSpinBox(); self.cassette_rear_count_spin.setRange(0, 4); self.cassette_rear_count_spin.setValue(max(0, min(4, _int(self.node.get("rear_connector_count"), 0))))
        self.cassette_used_front_spin = QSpinBox(); self.cassette_used_front_spin.setRange(0, 12); self.cassette_used_front_spin.setValue(max(0, min(12, _int(self.node.get("used_front_connectors"), 0))))
        self.cassette_used_fibres_spin = QSpinBox(); self.cassette_used_fibres_spin.setRange(0, 96); self.cassette_used_fibres_spin.setValue(max(0, _int(self.node.get("used_fibres"), 0)))
        self.layer_edit = QLineEdit(_text(self.node.get("drawing_layer")) or "NET-FIBRE-NODE")
        self.symbol_edit = QLineEdit(_text(self.node.get("symbol")))
        self.label_edit = QLineEdit(_text(self.node.get("label")))
        self.notes_edit = QTextEdit(_text(self.node.get("notes"))); self.notes_edit.setMinimumHeight(100)

        for label, widget in [
            ("Node ID", self.id_edit), ("Name", self.name_edit), ("Type", self.type_combo),
            ("Location", self.location_combo), ("Linked network instance", self.instance_combo),
            ("Parent enclosure/node", self.parent_combo), ("Floor", self.floor_spin),
            ("X", self.x_spin), ("Y", self.y_spin), ("Rack", self.rack_edit),
            ("Rack start", self.rack_u_spin), ("Rack units", self.rack_units_spin),
            ("Splice/cassette capacity", self.capacity_spin),
            ("Cassette position", self.cassette_position_spin),
            ("Cassette front connector", self.cassette_front_combo),
            ("Cassette termination", self.cassette_mode_combo),
            ("Cassette rear interface", self.cassette_rear_combo),
            ("Rear MPO/MTP connectors", self.cassette_rear_count_spin),
            ("Used front connectors", self.cassette_used_front_spin),
            ("Used fibres", self.cassette_used_fibres_spin),
            ("Drawing layer", self.layer_edit),
            ("Symbol", self.symbol_edit), ("Drawing label", self.label_edit), ("Notes", self.notes_edit),
        ]:
            form.addRow(label, widget)

        self.location_combo.currentIndexChanged.connect(self._location_changed)
        self.instance_combo.currentIndexChanged.connect(self._instance_changed)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)
        self._update_type_fields()
        self.type_combo.currentIndexChanged.connect(self._update_type_fields)
        self.cassette_mode_combo.currentIndexChanged.connect(self._update_type_fields)

        self._locations = list(locations or [])
        self._instances = list(instances or [])

    def _location_changed(self) -> None:
        name = _text(self.location_combo.currentData())
        location = next((row for row in self._locations if _text(row.get("name")) == name), None)
        if location:
            self.floor_spin.setValue(_int(location.get("floor")))
            self.x_spin.setValue(float(location.get("x", 0.0) or 0.0))
            self.y_spin.setValue(float(location.get("y", 0.0) or 0.0))

    def _instance_changed(self) -> None:
        iid = _text(self.instance_combo.currentData())
        instance = next((row for row in self._instances if _text(row.get("id")) == iid), None)
        if instance:
            self.floor_spin.setValue(_int(instance.get("floor")))
            self.x_spin.setValue(float(instance.get("x", 0.0) or 0.0))
            self.y_spin.setValue(float(instance.get("y", 0.0) or 0.0))
            self.rack_edit.setText(_text(instance.get("rack_name")))
            self.rack_u_spin.setValue(_int(instance.get("rack_start_u")))

    def _update_type_fields(self) -> None:
        node_type = _text(self.type_combo.currentData())
        is_cassette = node_type == "splice_cassette"
        self.parent_combo.setEnabled(is_cassette)
        self.capacity_spin.setEnabled(node_type in {"splice_enclosure", "splice_cassette", "fibre_joint"})
        for widget in (self.cassette_position_spin, self.cassette_front_combo, self.cassette_mode_combo, self.cassette_used_front_spin, self.cassette_used_fibres_spin):
            widget.setEnabled(is_cassette)
        connectorised = is_cassette and _text(self.cassette_mode_combo.currentData()) == "connectorised"
        self.cassette_rear_combo.setEnabled(connectorised)
        self.cassette_rear_count_spin.setEnabled(connectorised)

    def accept(self) -> None:
        node_id = self.id_edit.text().strip()
        name = self.name_edit.text().strip() or node_id
        if not node_id:
            QMessageBox.critical(self, "Invalid fibre node", "Node ID is required.")
            return
        self.result = {
            **self.node,
            "id": node_id,
            "name": name,
            "node_type": _text(self.type_combo.currentData()),
            "location_name": _text(self.location_combo.currentData()),
            "linked_instance_id": _text(self.instance_combo.currentData()),
            "parent_node_id": _text(self.parent_combo.currentData()),
            "floor": int(self.floor_spin.value()), "x": float(self.x_spin.value()), "y": float(self.y_spin.value()),
            "rack_name": self.rack_edit.text().strip(), "rack_start_u": int(self.rack_u_spin.value()),
            "rack_units": int(self.rack_units_spin.value()), "cassette_capacity": int(self.capacity_spin.value()),
            "splice_capacity": int(self.capacity_spin.value()),
            "cassette_position": int(self.cassette_position_spin.value()) if _text(self.type_combo.currentData()) == "splice_cassette" else 0,
            "tray_number": int(self.cassette_position_spin.value()) if _text(self.type_combo.currentData()) == "splice_cassette" else int(self.node.get("tray_number", 0) or 0),
            "front_connector": _text(self.cassette_front_combo.currentData()) if _text(self.type_combo.currentData()) == "splice_cassette" else "",
            "front_connector_capacity": 12 if _text(self.type_combo.currentData()) == "splice_cassette" else 0,
            "termination_mode": _text(self.cassette_mode_combo.currentData()) if _text(self.type_combo.currentData()) == "splice_cassette" else "",
            "rear_connector": (_text(self.cassette_rear_combo.currentData()) if _text(self.cassette_mode_combo.currentData()) == "connectorised" else "splice") if _text(self.type_combo.currentData()) == "splice_cassette" else "",
            "rear_connector_count": int(self.cassette_rear_count_spin.value()) if _text(self.type_combo.currentData()) == "splice_cassette" and _text(self.cassette_mode_combo.currentData()) == "connectorised" else 0,
            "used_front_connectors": int(self.cassette_used_front_spin.value()) if _text(self.type_combo.currentData()) == "splice_cassette" else 0,
            "used_fibres": int(self.cassette_used_fibres_spin.value()) if _text(self.type_combo.currentData()) == "splice_cassette" else 0,
            "drawing_layer": self.layer_edit.text().strip() or "NET-FIBRE-NODE",
            "symbol": self.symbol_edit.text().strip() or _text(self.type_combo.currentData()),
            "label": self.label_edit.text().strip() or name, "notes": self.notes_edit.toPlainText().strip(),
            "auto_generated": bool(self.node.get("auto_generated", False)),
        }
        super().accept()


class FibreCableEditorDialog(QDialog):
    def __init__(
        self,
        parent=None,
        cable: Optional[dict] = None,
        instances: Optional[Sequence[dict]] = None,
        suggested_id: str = "FOC1",
        cable_types: Optional[Sequence[dict]] = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Physical Fibre Cable")
        self.cable = deepcopy(cable or {})
        self.cable_types = [row for row in (cable_types or []) if isinstance(row, dict)]
        self.result: Optional[dict] = None
        self.resize(820, 900)
        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)
        self.id_edit = QLineEdit(_text(self.cable.get("id")) or suggested_id)
        self.name_edit = QLineEdit(_text(self.cable.get("name")))
        self.type_combo = QComboBox()
        self.type_combo.addItem("Project-specific / unclassified", "")
        for cable_type in sorted(self.cable_types, key=lambda row: (_int(row.get("core_count")), _text(row.get("name")))):
            type_id = _text(cable_type.get("id"))
            self.type_combo.addItem(
                f"{_text(cable_type.get('name')) or type_id} · {_int(cable_type.get('core_count'))}F",
                type_id,
            )
        _set_combo(self.type_combo, _text(self.cable.get("cable_type_id")))
        self.from_combo = QComboBox(); self.to_combo = QComboBox()
        for combo in (self.from_combo, self.to_combo):
            combo.addItem("No active-device termination", "")
        for instance in sorted(instances or [], key=lambda row: _text(row.get("name") or row.get("id"))):
            iid = _text(instance.get("id")); label = f"{_text(instance.get('name')) or iid} [{iid}]"
            self.from_combo.addItem(label, iid); self.to_combo.addItem(label, iid)
        _set_combo(self.from_combo, _text(self.cable.get("from_instance_id")))
        _set_combo(self.to_combo, _text(self.cable.get("to_instance_id")))
        self.from_port_edit = QLineEdit(_text(self.cable.get("from_port")))
        self.to_port_edit = QLineEdit(_text(self.cable.get("to_port")))
        self.core_count_spin = QSpinBox(); self.core_count_spin.setRange(1, 6912); self.core_count_spin.setValue(max(1, _int(self.cable.get("core_count"), 12)))
        self.attenuation_spin = QDoubleSpinBox(); self.attenuation_spin.setRange(0, 10); self.attenuation_spin.setDecimals(6); self.attenuation_spin.setSuffix(" dB/m"); self.attenuation_spin.setValue(float(self.cable.get("attenuation_db_per_m", 0.00035) or 0.0))
        self.connector_loss_spin = QDoubleSpinBox(); self.connector_loss_spin.setRange(0, 20); self.connector_loss_spin.setDecimals(3); self.connector_loss_spin.setSuffix(" dB"); self.connector_loss_spin.setValue(float(self.cable.get("connector_loss_db", 0.5) or 0.0))
        self.reflection_loss_spin = QDoubleSpinBox(); self.reflection_loss_spin.setRange(0, 100); self.reflection_loss_spin.setDecimals(2); self.reflection_loss_spin.setSuffix(" dB"); self.reflection_loss_spin.setValue(float(self.cable.get("reflection_loss_db", 55.0) or 0.0))
        self.splice_loss_spin = QDoubleSpinBox(); self.splice_loss_spin.setRange(0, 10); self.splice_loss_spin.setDecimals(3); self.splice_loss_spin.setSuffix(" dB"); self.splice_loss_spin.setValue(float(self.cable.get("splice_loss_db", 0.1) or 0.0))
        self.wavelength_spin = QSpinBox(); self.wavelength_spin.setRange(0, 10000); self.wavelength_spin.setSuffix(" nm"); self.wavelength_spin.setValue(_int(self.cable.get("wavelength_nm"), 1310))
        self.routing_role_combo = QComboBox(); self.routing_role_combo.addItems(["direct", "spine", "spur"]); _set_combo(self.routing_role_combo, _text(self.cable.get("routing_role")) or "direct")
        self.from_method_combo = QComboBox(); self.from_method_combo.addItems(["connectorised", "spliced"]); _set_combo(self.from_method_combo, _text(self.cable.get("from_termination_method")) or "connectorised")
        self.to_method_combo = QComboBox(); self.to_method_combo.addItems(["connectorised", "spliced"]); _set_combo(self.to_method_combo, _text(self.cable.get("to_termination_method")) or "connectorised")
        self.length_spin = QDoubleSpinBox(); self.length_spin.setRange(0, 1e9); self.length_spin.setDecimals(3); self.length_spin.setSuffix(" m"); self.length_spin.setValue(float(self.cable.get("length_m", 0.0) or 0.0))
        self.slack_spin = QDoubleSpinBox(); self.slack_spin.setRange(0, 1e6); self.slack_spin.setDecimals(2); self.slack_spin.setSuffix(" m"); self.slack_spin.setValue(float(self.cable.get("slack_length_m", 0.0) or 0.0))
        self.route_edit = QLineEdit(" -> ".join(_text(v) for v in self.cable.get("route_path", []) if _text(v)))
        self.route_edit.setPlaceholderText("Graph node path separated by ->")
        self.layer_edit = QLineEdit(_text(self.cable.get("drawing_layer")) or "NET-FIBRE-CABLE")
        self.sheath_combo = QComboBox(); self.sheath_combo.setEditable(True); self.sheath_combo.addItems(["Black", "Yellow", "Orange", "Blue", "Green", "White", "Violet"]); _set_combo(self.sheath_combo, _text(self.cable.get("sheath_colour")) or "Black")
        self.status_combo = QComboBox(); self.status_combo.addItems(["planned", "installed", "commissioned", "decommissioned"]); _set_combo(self.status_combo, _text(self.cable.get("installation_status")) or "planned")
        self.owner_edit = QLineEdit(_text(self.cable.get("owner")))
        self.logical_edit = QLineEdit(", ".join(_text(v) for v in self.cable.get("logical_connection_ids", []) if _text(v)))
        self.notes_edit = QTextEdit(_text(self.cable.get("notes"))); self.notes_edit.setMinimumHeight(70)
        for label, widget in [
            ("Cable ID", self.id_edit), ("Name", self.name_edit), ("Defined fixed cable", self.type_combo),
            ("Routing role", self.routing_role_combo), ("From device", self.from_combo), ("From port", self.from_port_edit),
            ("To device", self.to_combo), ("To port", self.to_port_edit), ("From termination", self.from_method_combo),
            ("To termination", self.to_method_combo), ("Fibre core count", self.core_count_spin),
            ("Attenuation", self.attenuation_spin), ("Connector loss", self.connector_loss_spin),
            ("Reflection / return loss", self.reflection_loss_spin), ("Splice loss", self.splice_loss_spin),
            ("Wavelength", self.wavelength_spin), ("Route length", self.length_spin), ("Slack allowance", self.slack_spin),
            ("Graph route", self.route_edit), ("Logical connection IDs", self.logical_edit), ("Drawing layer", self.layer_edit),
            ("Sheath colour", self.sheath_combo), ("Installation status", self.status_combo), ("Owner", self.owner_edit),
            ("Notes", self.notes_edit),
        ]:
            form.addRow(label, widget)
        self.stats_label = QLabel(); self.stats_label.setWordWrap(True); layout.addWidget(self.stats_label)
        self.core_table = QTableWidget(0, 8)
        self.core_table.setHorizontalHeaderLabels(["Core", "Colour", "Tube", "Tube colour", "Status", "Circuit", "From termination", "To termination"])
        layout.addWidget(self.core_table, 1)
        self.core_count_spin.valueChanged.connect(self._rebuild_core_table)
        self.type_combo.currentIndexChanged.connect(self._type_changed)
        self._rebuild_core_table()
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)

    def _selected_type(self) -> dict:
        type_id = _text(self.type_combo.currentData())
        return next((row for row in self.cable_types if _text(row.get("id")) == type_id), {})

    def _type_changed(self) -> None:
        row = self._selected_type()
        if not row:
            return
        self.core_count_spin.setValue(max(1, _int(row.get("core_count"), 12)))
        self.attenuation_spin.setValue(float(row.get("attenuation_db_per_m", 0.00035) or 0.0))
        self.connector_loss_spin.setValue(float(row.get("connector_loss_db", 0.5) or 0.0))
        self.reflection_loss_spin.setValue(float(row.get("reflection_loss_db", 55.0) or 0.0))
        self.splice_loss_spin.setValue(float(row.get("splice_loss_db", 0.1) or 0.0))
        self.wavelength_spin.setValue(_int(row.get("wavelength_nm"), 1310))

    def _table_cores(self) -> List[dict]:
        rows = []
        for r in range(self.core_table.rowCount()):
            status = self.core_table.cellWidget(r, 4)
            rows.append({
                "number": _int(self.core_table.item(r, 0).text()), "colour": _text(self.core_table.item(r, 1).text()),
                "tube_number": _int(self.core_table.item(r, 2).text()), "tube_colour": _text(self.core_table.item(r, 3).text()),
                "status": _text(status.currentText() if status else "dark"),
                "circuit_id": _text(self.core_table.item(r, 5).text()), "from_termination": _text(self.core_table.item(r, 6).text()),
                "to_termination": _text(self.core_table.item(r, 7).text()), "notes": "",
            })
        return rows

    def _rebuild_core_table(self) -> None:
        existing = self._table_cores() if self.core_table.rowCount() else self.cable.get("cores", [])
        cores = build_fibre_cores(self.core_count_spin.value(), 0, "", existing)
        self.core_table.setRowCount(0)
        for core in cores:
            row = self.core_table.rowCount(); self.core_table.insertRow(row)
            for col, value in enumerate([core["number"], core["colour"], core["tube_number"], core["tube_colour"]]):
                self.core_table.setItem(row, col, QTableWidgetItem(str(value)))
            status = QComboBox(); status.addItems(["dark", "reserved", "allocated", "spliced", "fault"]); _set_combo(status, _text(core.get("status")) or "dark"); self.core_table.setCellWidget(row, 4, status)
            for col, key in ((5, "circuit_id"), (6, "from_termination"), (7, "to_termination")):
                self.core_table.setItem(row, col, QTableWidgetItem(_text(core.get(key))))
        stats = cable_core_statistics({"core_count": len(cores), "cores": cores})
        length = float(self.length_spin.value()) if hasattr(self, "length_spin") else float(self.cable.get("length_m", 0.0) or 0.0)
        estimated = length * float(self.attenuation_spin.value()) if hasattr(self, "attenuation_spin") else 0.0
        self.stats_label.setText(
            f"Core allocation: {stats['used']} used/reserved, {stats['dark']} dark, {stats['fault']} fault, {stats['total']} total. "
            f"Cable attenuation at entered length: {estimated:.3f} dB before connectors/splices."
        )

    def accept(self) -> None:
        cable_id = self.id_edit.text().strip(); name = self.name_edit.text().strip() or cable_id
        if not cable_id:
            QMessageBox.critical(self, "Invalid fibre cable", "Cable ID is required."); return
        logical_ids = [v.strip() for v in self.logical_edit.text().replace(";", ",").split(",") if v.strip()]
        route = [v.strip() for v in self.route_edit.text().split("->") if v.strip()]
        selected_type = self._selected_type()
        from_method = self.from_method_combo.currentText().strip(); to_method = self.to_method_combo.currentText().strip()
        connector_count = int(from_method == "connectorised") + int(to_method == "connectorised")
        splice_count = max(_int(self.cable.get("splice_count")), int(from_method == "spliced") + int(to_method == "spliced"))
        attenuation = float(self.attenuation_spin.value()); connector_loss = float(self.connector_loss_spin.value()); splice_loss = float(self.splice_loss_spin.value())
        length = float(self.length_spin.value())
        self.result = {
            **self.cable, "id": cable_id, "name": name,
            "cable_type_id": _text(self.type_combo.currentData()),
            "cable_type": _text(selected_type.get("name")) or _text(self.cable.get("cable_type")) or "Project-specific fibre cable",
            "routing_role": self.routing_role_combo.currentText().strip(),
            "from_instance_id": _text(self.from_combo.currentData()), "from_port": self.from_port_edit.text().strip(),
            "to_instance_id": _text(self.to_combo.currentData()), "to_port": self.to_port_edit.text().strip(),
            "from_location": _text(self.cable.get("from_location")), "to_location": _text(self.cable.get("to_location")),
            "from_termination_method": from_method, "to_termination_method": to_method,
            "route_path": route, "length_m": length, "slack_length_m": float(self.slack_spin.value()),
            "core_count": int(self.core_count_spin.value()), "cores": self._table_cores(), "logical_connection_ids": logical_ids,
            "attenuation_db_per_m": attenuation, "connector_loss_db": connector_loss,
            "reflection_loss_db": float(self.reflection_loss_spin.value()), "minimum_return_loss_db": float(self.reflection_loss_spin.value()),
            "splice_loss_db": splice_loss, "wavelength_nm": int(self.wavelength_spin.value()),
            "connector_count": connector_count, "splice_count": splice_count,
            "estimated_attenuation_db": round(length * attenuation, 6),
            "estimated_connector_loss_db": round(connector_count * connector_loss, 6),
            "estimated_splice_loss_db": round(splice_count * splice_loss, 6),
            "estimated_total_loss_db": round(length * attenuation + connector_count * connector_loss + splice_count * splice_loss, 6),
            "splice_ids": list(self.cable.get("splice_ids", [])), "drawing_layer": self.layer_edit.text().strip() or "NET-FIBRE-CABLE",
            "sheath_colour": self.sheath_combo.currentText().strip(), "label": name,
            "installation_status": self.status_combo.currentText().strip(), "owner": self.owner_edit.text().strip(),
            "notes": self.notes_edit.toPlainText().strip(), "auto_generated": bool(self.cable.get("auto_generated", False)),
        }
        super().accept()


class FibreSpliceEditorDialog(QDialog):
    def __init__(self, parent=None, splice: Optional[dict] = None, nodes: Optional[Sequence[dict]] = None, cables: Optional[Sequence[dict]] = None, suggested_id: str = "FS1"):
        super().__init__(parent); self.setWindowTitle("Fibre Splice / Joint"); self.splice = deepcopy(splice or {}); self.result: Optional[dict] = None
        self.cables = list(cables or []); self.resize(600, 600); layout = QVBoxLayout(self); form = QFormLayout(); layout.addLayout(form)
        self.id_edit = QLineEdit(_text(self.splice.get("id")) or suggested_id)
        self.node_combo = QComboBox(); self.node_combo.addItem("Select splice node", "")
        for node in sorted(nodes or [], key=lambda row: _text(row.get("name") or row.get("id"))):
            nid=_text(node.get("id")); self.node_combo.addItem(f"{_text(node.get('name')) or nid} [{nid}]", nid)
        _set_combo(self.node_combo, _text(self.splice.get("node_id")))
        self.cassette_combo = QComboBox(); self.cassette_combo.addItem("No separate cassette", "")
        for node in sorted(nodes or [], key=lambda row: _text(row.get("name") or row.get("id"))):
            if _text(node.get("node_type")) == "splice_cassette":
                nid=_text(node.get("id")); self.cassette_combo.addItem(f"{_text(node.get('name')) or nid} [{nid}]", nid)
        _set_combo(self.cassette_combo, _text(self.splice.get("cassette_id")))
        self.in_cable_combo = QComboBox(); self.out_cable_combo = QComboBox()
        for cable in sorted(self.cables, key=lambda row: _text(row.get("name") or row.get("id"))):
            cid=_text(cable.get("id")); label=f"{_text(cable.get('name')) or cid} [{cid}]"
            self.in_cable_combo.addItem(label, cid); self.out_cable_combo.addItem(label, cid)
        _set_combo(self.in_cable_combo, _text(self.splice.get("incoming_cable_id"))); _set_combo(self.out_cable_combo, _text(self.splice.get("outgoing_cable_id")))
        self.in_core_spin=QSpinBox(); self.in_core_spin.setRange(1,6912); self.in_core_spin.setValue(max(1,_int(self.splice.get("incoming_core"),1)))
        self.out_core_spin=QSpinBox(); self.out_core_spin.setRange(1,6912); self.out_core_spin.setValue(max(1,_int(self.splice.get("outgoing_core"),1)))
        self.type_combo=QComboBox(); self.type_combo.addItems(["fusion","mechanical","connectorised","expressed","mid-span","pigtail"]); _set_combo(self.type_combo,_text(self.splice.get("splice_type")) or "fusion")
        self.pigtail_check=QCheckBox("Pigtail to active/passive optic"); self.pigtail_check.setChecked(bool(self.splice.get("pigtail",False)))
        self.termination_instance_edit=QLineEdit(_text(self.splice.get("termination_instance_id")))
        self.termination_port_edit=QLineEdit(_text(self.splice.get("termination_port")))
        self.circuit_edit=QLineEdit(_text(self.splice.get("circuit_id")))
        self.loss_spin=QDoubleSpinBox(); self.loss_spin.setRange(0,10); self.loss_spin.setDecimals(3); self.loss_spin.setSuffix(" dB"); self.loss_spin.setValue(float(self.splice.get("loss_db",0.1) or 0.1))
        self.layer_edit=QLineEdit(_text(self.splice.get("drawing_layer")) or "NET-FIBRE-SPLICE")
        self.notes_edit=QTextEdit(_text(self.splice.get("notes")))
        self.core_info=QLabel(); self.core_info.setWordWrap(True)
        for label,widget in [("Splice ID",self.id_edit),("Enclosure / joint",self.node_combo),("Cassette",self.cassette_combo),("Incoming cable",self.in_cable_combo),("Incoming core",self.in_core_spin),("Outgoing cable",self.out_cable_combo),("Outgoing core",self.out_core_spin),("Splice type",self.type_combo),("Pigtail",self.pigtail_check),("Termination instance",self.termination_instance_edit),("Termination port",self.termination_port_edit),("Circuit ID",self.circuit_edit),("Estimated loss",self.loss_spin),("Drawing layer",self.layer_edit),("Core colours",self.core_info),("Notes",self.notes_edit)]: form.addRow(label,widget)
        for control in (self.in_cable_combo,self.out_cable_combo,self.in_core_spin,self.out_core_spin):
            if isinstance(control,QComboBox): control.currentIndexChanged.connect(self._update_core_info)
            else: control.valueChanged.connect(self._update_core_info)
        self._update_core_info()
        buttons=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)

    def _core(self,cable_id,number):
        cable=next((c for c in self.cables if _text(c.get("id"))==_text(cable_id)),{})
        return next((c for c in cable.get("cores",[]) if _int(c.get("number"))==number),{})
    def _update_core_info(self):
        inc=self._core(self.in_cable_combo.currentData(),self.in_core_spin.value()); out=self._core(self.out_cable_combo.currentData(),self.out_core_spin.value())
        self.core_info.setText(f"Incoming: tube {_int(inc.get('tube_number'))} {_text(inc.get('tube_colour'))}, core {_text(inc.get('colour'))}. Outgoing: tube {_int(out.get('tube_number'))} {_text(out.get('tube_colour'))}, core {_text(out.get('colour'))}.")
    def accept(self):
        sid=self.id_edit.text().strip()
        if not sid or not _text(self.node_combo.currentData()): QMessageBox.critical(self,"Invalid splice","Splice ID and enclosure/joint are required."); return
        if _text(self.in_cable_combo.currentData())==_text(self.out_cable_combo.currentData()) and self.in_core_spin.value()==self.out_core_spin.value(): QMessageBox.critical(self,"Invalid splice","A fibre cannot be spliced to itself."); return
        self.result={**self.splice,"id":sid,"node_id":_text(self.node_combo.currentData()),"cassette_id":_text(self.cassette_combo.currentData()),"incoming_cable_id":_text(self.in_cable_combo.currentData()),"incoming_core":int(self.in_core_spin.value()),"outgoing_cable_id":_text(self.out_cable_combo.currentData()),"outgoing_core":int(self.out_core_spin.value()),"splice_type":self.type_combo.currentText().strip(),"pigtail":self.pigtail_check.isChecked(),"connectorised":self.type_combo.currentText().strip()=="connectorised","termination_instance_id":self.termination_instance_edit.text().strip(),"termination_port":self.termination_port_edit.text().strip(),"circuit_id":self.circuit_edit.text().strip(),"loss_db":float(self.loss_spin.value()),"drawing_layer":self.layer_edit.text().strip() or "NET-FIBRE-SPLICE","label":sid,"notes":self.notes_edit.toPlainText().strip(),"auto_generated":bool(self.splice.get("auto_generated",False))}
        super().accept()


class FibreCableTypeEditorDialog(QDialog):
    def __init__(self, parent=None, record: Optional[dict] = None, suggested_id: str = "FCT1"):
        super().__init__(parent); self.setWindowTitle("Fixed Installation Fibre Cable Type"); self.record=deepcopy(record or {}); self.result=None
        layout=QVBoxLayout(self); form=QFormLayout(); layout.addLayout(form)
        self.id_edit=QLineEdit(_text(self.record.get("id")) or suggested_id); self.name_edit=QLineEdit(_text(self.record.get("name")))
        self.standard_edit=QLineEdit(_text(self.record.get("fibre_standard")) or "OS2"); self.manufacturer_edit=QLineEdit(_text(self.record.get("manufacturer"))); self.model_edit=QLineEdit(_text(self.record.get("model")))
        self.cores=QSpinBox(); self.cores.setRange(1,6912); self.cores.setValue(max(1,_int(self.record.get("core_count"),12)))
        self.attenuation=QDoubleSpinBox(); self.attenuation.setRange(0,10); self.attenuation.setDecimals(6); self.attenuation.setSuffix(" dB/m"); self.attenuation.setValue(float(self.record.get("attenuation_db_per_m",0.00035) or 0.0))
        self.connector=QDoubleSpinBox(); self.connector.setRange(0,20); self.connector.setDecimals(3); self.connector.setSuffix(" dB"); self.connector.setValue(float(self.record.get("connector_loss_db",0.5) or 0.0))
        self.reflection=QDoubleSpinBox(); self.reflection.setRange(0,100); self.reflection.setDecimals(2); self.reflection.setSuffix(" dB"); self.reflection.setValue(float(self.record.get("reflection_loss_db",55.0) or 0.0))
        self.splice=QDoubleSpinBox(); self.splice.setRange(0,10); self.splice.setDecimals(3); self.splice.setSuffix(" dB"); self.splice.setValue(float(self.record.get("splice_loss_db",0.1) or 0.0))
        self.wavelength=QSpinBox(); self.wavelength.setRange(0,10000); self.wavelength.setSuffix(" nm"); self.wavelength.setValue(_int(self.record.get("wavelength_nm"),1310))
        self.construction=QLineEdit(_text(self.record.get("construction"))); self.sheath=QLineEdit(_text(self.record.get("sheath_type"))); self.notes=QTextEdit(_text(self.record.get("notes")))
        for label,w in [("Type ID",self.id_edit),("Name",self.name_edit),("Fibre standard",self.standard_edit),("Manufacturer",self.manufacturer_edit),("Model",self.model_edit),("Core count",self.cores),("Attenuation",self.attenuation),("Connector loss",self.connector),("Reflection / return loss",self.reflection),("Splice loss",self.splice),("Wavelength",self.wavelength),("Construction",self.construction),("Sheath type",self.sheath),("Notes",self.notes)]: form.addRow(label,w)
        buttons=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)
    def accept(self):
        rid=self.id_edit.text().strip(); name=self.name_edit.text().strip()
        if not rid or not name: QMessageBox.critical(self,"Invalid cable type","Type ID and name are required."); return
        self.result={**self.record,"id":rid,"name":name,"fibre_standard":self.standard_edit.text().strip() or "OS2","manufacturer":self.manufacturer_edit.text().strip(),"model":self.model_edit.text().strip(),"core_count":int(self.cores.value()),"attenuation_db_per_m":float(self.attenuation.value()),"connector_loss_db":float(self.connector.value()),"reflection_loss_db":float(self.reflection.value()),"splice_loss_db":float(self.splice.value()),"wavelength_nm":int(self.wavelength.value()),"construction":self.construction.text().strip(),"sheath_type":self.sheath.text().strip(),"notes":self.notes.toPlainText().strip()}; super().accept()


class FibreCableTypeLibraryDialog(QDialog):
    def __init__(self,parent=None,cable_types:Optional[Sequence[dict]]=None):
        super().__init__(parent); self.setWindowTitle("Fixed Installation Fibre Cable Library"); self.records=deepcopy(list(cable_types or [])); self.result=None; self.resize(1050,600)
        layout=QVBoxLayout(self); self.table=QTableWidget(0,9); self.table.setHorizontalHeaderLabels(["ID","Name","Standard","Cores","dB/m","Connector dB","Return dB","Splice dB","nm"]); layout.addWidget(self.table,1)
        row=QHBoxLayout(); add=QPushButton("Add"); edit=QPushButton("Edit"); delete=QPushButton("Delete"); row.addWidget(add); row.addWidget(edit); row.addWidget(delete); row.addStretch(1); layout.addLayout(row)
        add.clicked.connect(self._add); edit.clicked.connect(self._edit); delete.clicked.connect(self._delete); self.table.doubleClicked.connect(self._edit)
        buttons=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons); self._refresh()
    def _refresh(self):
        self.table.setRowCount(0)
        for record in self.records:
            r=self.table.rowCount(); self.table.insertRow(r)
            vals=[record.get("id",""),record.get("name",""),record.get("fibre_standard",""),record.get("core_count",0),record.get("attenuation_db_per_m",0),record.get("connector_loss_db",0),record.get("reflection_loss_db",0),record.get("splice_loss_db",0),record.get("wavelength_nm",0)]
            for c,v in enumerate(vals): self.table.setItem(r,c,QTableWidgetItem(str(v)))
    def _add(self):
        dialog=FibreCableTypeEditorDialog(self,suggested_id=f"FCT{len(self.records)+1}")
        if dialog.exec()==QDialog.Accepted and dialog.result: self.records.append(dialog.result); self._refresh()
    def _edit(self,*_args):
        row=self.table.currentRow()
        if row<0 or row>=len(self.records): return
        dialog=FibreCableTypeEditorDialog(self,self.records[row],_text(self.records[row].get("id")))
        if dialog.exec()==QDialog.Accepted and dialog.result: self.records[row]=dialog.result; self._refresh(); self.table.selectRow(row)
    def _delete(self):
        row=self.table.currentRow()
        if row>=0 and row<len(self.records): self.records.pop(row); self._refresh()
    def accept(self): self.result=self.records; super().accept()


class PhysicalFibrePlanningDialog(QDialog):
    def __init__(self,parent=None,settings:Optional[dict]=None,cable_types:Optional[Sequence[dict]]=None):
        super().__init__(parent); self.setWindowTitle("Physical Fibre Auto-Planning"); self.settings=deepcopy(settings or {}); self.result=None
        layout=QVBoxLayout(self); form=QFormLayout(); layout.addLayout(form)
        self.mode=QComboBox(); self.mode.addItem("Direct home-run cables","direct"); self.mode.addItem("Spine and spur with designated splices","spine_and_spur"); _set_combo(self.mode,_text(self.settings.get("routing_mode")) or "direct")
        def cable_combo(value):
            combo=QComboBox()
            for record in sorted(cable_types or [],key=lambda r:(_int(r.get("core_count")),_text(r.get("name")))): combo.addItem(f"{_text(record.get('name'))} · {_int(record.get('core_count'))}F",_text(record.get("id")))
            _set_combo(combo,_text(value)); return combo
        self.default_type=cable_combo(self.settings.get("default_cable_type_id")); self.spine_type=cable_combo(self.settings.get("spine_cable_type_id")); self.spur_type=cable_combo(self.settings.get("spur_cable_type_id"))
        self.branch=QComboBox(); self.branch.addItems(["spliced","connectorised"]); _set_combo(self.branch,_text(self.settings.get("branch_termination_method")) or "spliced")
        self.splitter=QComboBox(); self.splitter.addItems(["connectorised","spliced"]); _set_combo(self.splitter,_text(self.settings.get("splitter_termination_method")) or "connectorised")
        self.pigtail=QCheckBox("Use a spliced pigtail when splitter termination is spliced"); self.pigtail.setChecked(bool(self.settings.get("splitter_pigtail",True)))
        self.reduced_spine=QCheckBox("Continue the dominant route with reduced-count spine cables"); self.reduced_spine.setChecked(bool(self.settings.get("extend_reduced_count_spine",True))); self.reduced_spine.setToolTip("After each branch, keep one dominant route as a smaller-count spine extension instead of classifying every remaining single-circuit leg as a spur.")
        self.tray=QSpinBox(); self.tray.setRange(1,24); self.tray.setValue(max(1,min(24,_int(self.settings.get("max_splices_per_cassette"),24))))
        self.spare=QDoubleSpinBox(); self.spare.setRange(0,200); self.spare.setDecimals(1); self.spare.setSuffix(" %"); self.spare.setValue(float(self.settings.get("spare_core_percent",15.0) or 0.0))
        self.optical_margin=QDoubleSpinBox(); self.optical_margin.setRange(0,30); self.optical_margin.setDecimals(2); self.optical_margin.setSuffix(" dB"); self.optical_margin.setValue(float(self.settings.get("minimum_optical_margin_db",3.0) or 0.0))
        self.mpo_threshold=QSpinBox(); self.mpo_threshold.setRange(12,6912); self.mpo_threshold.setSuffix(" fibre cores"); self.mpo_threshold.setValue(max(12,_int(self.settings.get("mpo_breakout_minimum_cores"),48))); self.mpo_threshold.setToolTip("Cables at or above this core count use MPO/MTP rear connections into modular LC/SC breakout cassettes.")
        self.mpo_connector=QComboBox(); self.mpo_connector.addItem("MPO-12","mpo-12"); self.mpo_connector.addItem("MTP-12","mtp-12"); self.mpo_connector.addItem("MPO-24","mpo-24"); self.mpo_connector.addItem("MTP-24","mtp-24"); _set_combo(self.mpo_connector,_text(self.settings.get("mpo_breakout_connector")) or "mpo-24")
        for label,w in [("Routing arrangement",self.mode),("Direct cable type",self.default_type),("Spine cable type",self.spine_type),("Spur cable type",self.spur_type),("Reduced-count spine continuation",self.reduced_spine),("Branch termination",self.branch),("Splitter termination",self.splitter),("Splitter pigtail",self.pigtail),("Maximum splices per tray",self.tray),("Spare core allowance",self.spare),("MPO/MTP breakout threshold",self.mpo_threshold),("MPO/MTP breakout connector",self.mpo_connector),("Minimum optical design margin",self.optical_margin)]: form.addRow(label,w)
        buttons=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)
    def accept(self):
        self.result={**self.settings,"routing_mode":_text(self.mode.currentData()),"default_cable_type_id":_text(self.default_type.currentData()),"spine_cable_type_id":_text(self.spine_type.currentData()),"spur_cable_type_id":_text(self.spur_type.currentData()),"extend_reduced_count_spine":self.reduced_spine.isChecked(),"branch_termination_method":self.branch.currentText(),"splitter_termination_method":self.splitter.currentText(),"splitter_pigtail":self.pigtail.isChecked(),"max_splices_per_cassette":int(self.tray.value()),"spare_core_percent":float(self.spare.value()),"mpo_breakout_minimum_cores":int(self.mpo_threshold.value()),"mpo_breakout_connector":_text(self.mpo_connector.currentData()),"minimum_optical_margin_db":float(self.optical_margin.value())}; super().accept()


class OpticalPropertiesDialog(QDialog):
    def __init__(self,parent=None,assets:Optional[Sequence[dict]]=None):
        super().__init__(parent); self.setWindowTitle("Optical Transmit, Receive and Passive Loss Properties"); self.assets=deepcopy(list(assets or [])); self.result=None; self.resize(1200,650)
        layout=QVBoxLayout(self); info=QLabel("Configure active optic transmit power and receiver sensitivity. Passive devices use insertion loss. Return loss is reported separately and is not added to forward attenuation."); info.setWordWrap(True); layout.addWidget(info)
        self.table=QTableWidget(0,8); self.table.setHorizontalHeaderLabels(["Asset ID","Name","Type","Tx dBm","Receiver sensitivity dBm","Insertion loss dB","Return loss dB","Wavelength nm"]); layout.addWidget(self.table,1)
        for asset in self.assets:
            r=self.table.rowCount(); self.table.insertRow(r)
            vals=[asset.get("id",""),asset.get("name",""),asset.get("asset_type",""),asset.get("optical_tx_power_dbm",""),asset.get("optical_receiver_sensitivity_dbm",""),asset.get("optical_insertion_loss_db",""),asset.get("optical_return_loss_db",""),asset.get("optical_wavelength_nm","")]
            for c,v in enumerate(vals):
                item=QTableWidgetItem(str(v));
                if c<3: item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                self.table.setItem(r,c,item)
        buttons=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)
    def accept(self):
        for r,asset in enumerate(self.assets):
            for c,key in ((3,"optical_tx_power_dbm"),(4,"optical_receiver_sensitivity_dbm"),(5,"optical_insertion_loss_db"),(6,"optical_return_loss_db")):
                text=_text(self.table.item(r,c).text())
                asset[key]="" if not text else float(text)
            text=_text(self.table.item(r,7).text()); asset["optical_wavelength_nm"]=0 if not text else int(float(text))
        self.result=self.assets; super().accept()


class SpliceCassetteViewDialog(QDialog):
    def __init__(self,parent=None,nodes:Optional[Sequence[dict]]=None,cables:Optional[Sequence[dict]]=None,splices:Optional[Sequence[dict]]=None,initial_enclosure_id=""):
        super().__init__(parent); self.setWindowTitle("Splice Cassette Field View"); self.nodes=list(nodes or []); self.cables={_text(c.get("id")):c for c in (cables or []) if isinstance(c,dict)}; self.splices=list(splices or []); self.resize(1150,760)
        layout=QVBoxLayout(self); self.enclosure_combo=QComboBox(); layout.addWidget(self.enclosure_combo)
        for node in sorted(self.nodes,key=lambda r:(_int(r.get("floor")),_text(r.get("name")))):
            if _text(node.get("node_type")) in {"splice_enclosure","fibre_joint","termination"}: self.enclosure_combo.addItem(f"{_text(node.get('name'))} [{_text(node.get('id'))}]",_text(node.get("id")))
        _set_combo(self.enclosure_combo,initial_enclosure_id); self.tabs=QTabWidget(); layout.addWidget(self.tabs,1); close=QDialogButtonBox(QDialogButtonBox.Close); close.rejected.connect(self.reject); close.accepted.connect(self.accept); layout.addWidget(close); self.enclosure_combo.currentIndexChanged.connect(self._rebuild); self._rebuild()
    def _core_label(self,cable_id,number):
        cable=self.cables.get(_text(cable_id),{}); core=next((row for row in cable.get("cores",[]) if _int(row.get("number"))==_int(number)),{}); return f"T{_int(core.get('tube_number'))} {_text(core.get('tube_colour'))} / {_text(core.get('colour'))}"
    def _rebuild(self):
        self.tabs.clear(); enclosure_id=_text(self.enclosure_combo.currentData()); cassettes=[row for row in self.nodes if _text(row.get("node_type"))=="splice_cassette" and _text(row.get("parent_node_id"))==enclosure_id]
        cassettes.sort(key=lambda r:(_int(r.get("tray_number")),_text(r.get("id"))))
        if not cassettes:
            cassettes=[{"id":"","name":"Unassigned tray","tray_number":1,"max_splices_per_tray":24}]
        for cassette in cassettes:
            rows=[row for row in self.splices if _text(row.get("node_id"))==enclosure_id and (_text(row.get("cassette_id"))==_text(cassette.get("id")) or not _text(cassette.get("id")))]
            rows.sort(key=lambda r:(_text(r.get("incoming_cable_id")),_int(r.get("incoming_core")),_text(r.get("id"))))
            capacity=max(1,min(24,_int(cassette.get("max_splices_per_tray"),cassette.get("splice_capacity",24))))
            table=QTableWidget(capacity,8); table.setHorizontalHeaderLabels(["Position","Incoming cable/core","Incoming colour","Outgoing / pigtail","Outgoing colour","Circuit","Method","Loss dB"])
            for pos in range(capacity):
                table.setItem(pos,0,QTableWidgetItem(str(pos+1)))
                if pos>=len(rows): continue
                splice=rows[pos]; incoming=f"{_text(splice.get('incoming_cable_id'))}:{_int(splice.get('incoming_core'))}"; outgoing=(f"Pigtail → {_text(splice.get('termination_instance_id'))}:{_text(splice.get('termination_port'))}" if bool(splice.get("pigtail")) else f"{_text(splice.get('outgoing_cable_id'))}:{_int(splice.get('outgoing_core'))}")
                vals=[incoming,self._core_label(splice.get("incoming_cable_id"),splice.get("incoming_core")),outgoing,self._core_label(splice.get("outgoing_cable_id"),splice.get("outgoing_core")),_text(splice.get("circuit_id")),_text(splice.get("splice_type")),f"{float(splice.get('loss_db',0.0) or 0.0):.3f}"]
                for col,val in enumerate(vals,start=1): table.setItem(pos,col,QTableWidgetItem(str(val)))
            self.tabs.addTab(table,f"Tray {_int(cassette.get('tray_number'),1)} · {len(rows)}/{capacity}")


class PatchLeadEditorDialog(QDialog):
    def __init__(self,parent=None,lead:Optional[dict]=None,instances:Optional[Sequence[dict]]=None,suggested_id="PL1",default_instance_id="",default_port=""):
        super().__init__(parent); self.setWindowTitle("Patch Cable"); self.lead=deepcopy(lead or {}); self.result=None
        layout=QVBoxLayout(self); form=QFormLayout(); layout.addLayout(form)
        self.id_edit=QLineEdit(_text(self.lead.get("id")) or suggested_id)
        self.instance_combo=QComboBox(); self.peer_combo=QComboBox(); self.peer_combo.addItem("Endpoint / no peer device","")
        for instance in sorted(instances or [],key=lambda r:_text(r.get("name") or r.get("id"))):
            iid=_text(instance.get("id")); label=f"{_text(instance.get('name')) or iid} [{iid}]"; self.instance_combo.addItem(label,iid); self.peer_combo.addItem(label,iid)
        _set_combo(self.instance_combo,_text(self.lead.get("instance_id")) or default_instance_id); _set_combo(self.peer_combo,_text(self.lead.get("peer_instance_id")))
        self.port_edit=QLineEdit(_text(self.lead.get("port")) or default_port); self.peer_port_edit=QLineEdit(_text(self.lead.get("peer_port")))
        self.endpoint_edit=QLineEdit(_text(self.lead.get("endpoint_name"))); self.medium_combo=QComboBox(); self.medium_combo.addItems(["copper","fibre"]); _set_combo(self.medium_combo,_text(self.lead.get("medium")) or "copper")
        self.spec_edit=QLineEdit(_text(self.lead.get("cable_specification"))); self.length_spin=QDoubleSpinBox(); self.length_spin.setRange(0,10000); self.length_spin.setDecimals(2); self.length_spin.setSuffix(" m"); self.length_spin.setValue(float(self.lead.get("length_m",2.0) or 0.0))
        self.connection_edit=QLineEdit(_text(self.lead.get("connection_id"))); self.assignment_edit=QLineEdit(_text(self.lead.get("assignment_id")))
        for label,w in [("Patch lead ID",self.id_edit),("Device",self.instance_combo),("Port",self.port_edit),("Peer device",self.peer_combo),("Peer port",self.peer_port_edit),("Endpoint",self.endpoint_edit),("Medium",self.medium_combo),("Cable specification",self.spec_edit),("Length",self.length_spin),("Logical connection ID",self.connection_edit),("Endpoint assignment ID",self.assignment_edit)]: form.addRow(label,w)
        buttons=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)
    def accept(self):
        if not self.id_edit.text().strip() or not _text(self.instance_combo.currentData()) or not self.port_edit.text().strip(): QMessageBox.critical(self,"Invalid patch cable","ID, device and port are required."); return
        medium=self.medium_combo.currentText().strip(); self.result={**self.lead,"id":self.id_edit.text().strip(),"connection_id":self.connection_edit.text().strip(),"assignment_id":self.assignment_edit.text().strip(),"instance_id":_text(self.instance_combo.currentData()),"port":self.port_edit.text().strip(),"peer_instance_id":_text(self.peer_combo.currentData()),"peer_port":self.peer_port_edit.text().strip(),"endpoint_name":self.endpoint_edit.text().strip(),"port_type":"lc" if medium=="fibre" else "rj45","port_use":"patch","medium":medium,"cable_specification":self.spec_edit.text().strip() or ("OS2 fibre patch lead" if medium=="fibre" else "Category 6A copper patch lead"),"length_m":float(self.length_spin.value()),"auto_generated":bool(self.lead.get("auto_generated",False))}; super().accept()


class ExternalNetworkEditorDialog(QDialog):
    def __init__(self,parent=None,record:Optional[dict]=None,instances:Optional[Sequence[dict]]=None,locations:Optional[Sequence[dict]]=None,suggested_id="EXT1"):
        super().__init__(parent); self.setWindowTitle("External Network / Telco PoP"); self.record=deepcopy(record or {}); self.result=None; layout=QVBoxLayout(self); form=QFormLayout(); layout.addLayout(form)
        self.id_edit=QLineEdit(_text(self.record.get("id")) or suggested_id); self.name_edit=QLineEdit(_text(self.record.get("name")))
        self.type_combo=QComboBox(); self.type_combo.addItems(["telco_pop","carrier","partner_network","internet_exchange","cloud","other"]); _set_combo(self.type_combo,_text(self.record.get("network_type")) or "telco_pop")
        self.provider_edit=QLineEdit(_text(self.record.get("provider"))); self.asn_edit=QLineEdit(_text(self.record.get("asn"))); self.prefixes_edit=QLineEdit(", ".join(self.record.get("prefixes",[])))
        self.service_edit=QLineEdit(_text(self.record.get("service_type")) or "Internet access")
        self.redundant_check=QCheckBox("Require independent redundant connections"); self.redundant_check.setChecked(bool(self.record.get("redundant",True)))
        self.links_spin=QSpinBox(); self.links_spin.setRange(1,8); self.links_spin.setValue(max(1,_int(self.record.get("required_links"),2)))
        self.medium_combo=QComboBox(); self.medium_combo.addItems(["fibre","copper","wireless"]); _set_combo(self.medium_combo,_text(self.record.get("medium")) or "fibre")
        self.bandwidth_spin=QDoubleSpinBox(); self.bandwidth_spin.setRange(0,1e9); self.bandwidth_spin.setDecimals(3); self.bandwidth_spin.setSuffix(" Mbps"); self.bandwidth_spin.setValue(float(self.record.get("bandwidth_mbps",0.0) or 0.0))
        self.location_combo=QComboBox(); self.location_combo.addItem("No location","")
        for loc in locations or []: self.location_combo.addItem(_text(loc.get("name")),_text(loc.get("name")))
        _set_combo(self.location_combo,_text(self.record.get("location_name")))
        self.demarc_combo=QComboBox(); self.demarc_combo.addItem("No demarcation device","")
        self.peers=QListWidget()
        selected=set(_text(v) for v in self.record.get("peer_instance_ids",[]))
        for inst in instances or []:
            iid=_text(inst.get("id")); self.demarc_combo.addItem(f"{_text(inst.get('name')) or iid} [{iid}]",iid)
            item=QListWidgetItem(f"{_text(inst.get('name')) or iid} [{iid}]"); item.setData(Qt.UserRole,iid); item.setFlags(item.flags()|Qt.ItemIsUserCheckable); item.setCheckState(Qt.Checked if iid in selected else Qt.Unchecked); self.peers.addItem(item)
        _set_combo(self.demarc_combo,_text(self.record.get("demarcation_instance_id")))
        self.notes=QTextEdit(_text(self.record.get("notes")))
        for label,w in [("Record ID",self.id_edit),("Name",self.name_edit),("Type",self.type_combo),("Provider",self.provider_edit),("ASN",self.asn_edit),("Service",self.service_edit),("Advertised prefixes",self.prefixes_edit),("Redundancy",self.redundant_check),("Required links",self.links_spin),("Medium",self.medium_combo),("Bandwidth",self.bandwidth_spin),("Location",self.location_combo),("Demarcation device",self.demarc_combo),("Peering devices",self.peers),("Notes",self.notes)]: form.addRow(label,w)
        buttons=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)
    def accept(self):
        rid=self.id_edit.text().strip(); name=self.name_edit.text().strip()
        if not rid or not name: QMessageBox.critical(self,"Invalid external network","ID and name are required."); return
        peers=[]
        for i in range(self.peers.count()):
            item=self.peers.item(i)
            if item.checkState()==Qt.Checked: peers.append(_text(item.data(Qt.UserRole)))
        self.result={**self.record,"id":rid,"name":name,"network_type":self.type_combo.currentText().strip(),"provider":self.provider_edit.text().strip(),"asn":self.asn_edit.text().strip(),"service_type":self.service_edit.text().strip(),"redundant":self.redundant_check.isChecked(),"required_links":int(self.links_spin.value()),"medium":self.medium_combo.currentText().strip(),"bandwidth_mbps":float(self.bandwidth_spin.value()),"prefixes":[v.strip() for v in self.prefixes_edit.text().replace(";",",").split(",") if v.strip()],"location_name":_text(self.location_combo.currentData()),"demarcation_instance_id":_text(self.demarc_combo.currentData()),"peer_instance_ids":peers,"notes":self.notes.toPlainText().strip()}; super().accept()
