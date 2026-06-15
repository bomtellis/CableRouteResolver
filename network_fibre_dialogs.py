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
            ("Splice/cassette capacity", self.capacity_spin), ("Drawing layer", self.layer_edit),
            ("Symbol", self.symbol_edit), ("Drawing label", self.label_edit), ("Notes", self.notes_edit),
        ]:
            form.addRow(label, widget)

        self.location_combo.currentIndexChanged.connect(self._location_changed)
        self.instance_combo.currentIndexChanged.connect(self._instance_changed)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)
        self._update_type_fields()
        self.type_combo.currentIndexChanged.connect(self._update_type_fields)

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
        self.parent_combo.setEnabled(node_type == "splice_cassette")
        self.capacity_spin.setEnabled(node_type in {"splice_enclosure", "splice_cassette", "fibre_joint"})

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
            "splice_capacity": int(self.capacity_spin.value()), "drawing_layer": self.layer_edit.text().strip() or "NET-FIBRE-NODE",
            "symbol": self.symbol_edit.text().strip() or _text(self.type_combo.currentData()),
            "label": self.label_edit.text().strip() or name, "notes": self.notes_edit.toPlainText().strip(),
            "auto_generated": bool(self.node.get("auto_generated", False)),
        }
        super().accept()


class FibreCableEditorDialog(QDialog):
    def __init__(self, parent=None, cable: Optional[dict] = None, instances: Optional[Sequence[dict]] = None, suggested_id: str = "FOC1"):
        super().__init__(parent)
        self.setWindowTitle("Physical Fibre Cable")
        self.cable = deepcopy(cable or {})
        self.result: Optional[dict] = None
        self.resize(760, 820)
        layout = QVBoxLayout(self); form = QFormLayout(); layout.addLayout(form)
        self.id_edit = QLineEdit(_text(self.cable.get("id")) or suggested_id)
        self.name_edit = QLineEdit(_text(self.cable.get("name")))
        self.type_edit = QLineEdit(_text(self.cable.get("cable_type")) or "OS2 single-mode fibre")
        self.from_combo = QComboBox(); self.to_combo = QComboBox()
        for combo in (self.from_combo, self.to_combo): combo.addItem("No active-device termination", "")
        for instance in sorted(instances or [], key=lambda row: _text(row.get("name") or row.get("id"))):
            iid = _text(instance.get("id")); label = f"{_text(instance.get('name')) or iid} [{iid}]"
            self.from_combo.addItem(label, iid); self.to_combo.addItem(label, iid)
        _set_combo(self.from_combo, _text(self.cable.get("from_instance_id"))); _set_combo(self.to_combo, _text(self.cable.get("to_instance_id")))
        self.from_port_edit = QLineEdit(_text(self.cable.get("from_port"))); self.to_port_edit = QLineEdit(_text(self.cable.get("to_port")))
        self.core_count_spin = QSpinBox(); self.core_count_spin.setRange(1, 6912); self.core_count_spin.setValue(max(1, _int(self.cable.get("core_count"), 12)))
        self.length_spin = QDoubleSpinBox(); self.length_spin.setRange(0, 1e9); self.length_spin.setDecimals(3); self.length_spin.setSuffix(" m"); self.length_spin.setValue(float(self.cable.get("length_m", 0.0) or 0.0))
        self.slack_spin = QDoubleSpinBox(); self.slack_spin.setRange(0, 1e6); self.slack_spin.setDecimals(2); self.slack_spin.setSuffix(" m"); self.slack_spin.setValue(float(self.cable.get("slack_length_m", 0.0) or 0.0))
        self.route_edit = QLineEdit(" -> ".join(_text(v) for v in self.cable.get("route_path", []) if _text(v)))
        self.route_edit.setPlaceholderText("Graph node path separated by ->")
        self.layer_edit = QLineEdit(_text(self.cable.get("drawing_layer")) or "NET-FIBRE-CABLE")
        self.sheath_combo = QComboBox(); self.sheath_combo.setEditable(True); self.sheath_combo.addItems(["Black", "Yellow", "Orange", "Blue", "Green", "White", "Violet"]); _set_combo(self.sheath_combo, _text(self.cable.get("sheath_colour")) or "Black")
        self.status_combo = QComboBox(); self.status_combo.addItems(["planned", "installed", "commissioned", "decommissioned"]); _set_combo(self.status_combo, _text(self.cable.get("installation_status")) or "planned")
        self.owner_edit = QLineEdit(_text(self.cable.get("owner")))
        self.logical_edit = QLineEdit(", ".join(_text(v) for v in self.cable.get("logical_connection_ids", []) if _text(v)))
        self.notes_edit = QTextEdit(_text(self.cable.get("notes"))); self.notes_edit.setMinimumHeight(80)
        for label, widget in [
            ("Cable ID", self.id_edit), ("Name", self.name_edit), ("Cable type", self.type_edit),
            ("From device", self.from_combo), ("From port", self.from_port_edit), ("To device", self.to_combo), ("To port", self.to_port_edit),
            ("Fibre core count", self.core_count_spin), ("Route length", self.length_spin), ("Slack allowance", self.slack_spin),
            ("Graph route", self.route_edit), ("Logical connection IDs", self.logical_edit), ("Drawing layer", self.layer_edit),
            ("Sheath colour", self.sheath_combo), ("Installation status", self.status_combo), ("Owner", self.owner_edit), ("Notes", self.notes_edit),
        ]: form.addRow(label, widget)
        self.stats_label = QLabel(); self.stats_label.setWordWrap(True); layout.addWidget(self.stats_label)
        self.core_table = QTableWidget(0, 8)
        self.core_table.setHorizontalHeaderLabels(["Core", "Colour", "Tube", "Tube colour", "Status", "Circuit", "From termination", "To termination"])
        layout.addWidget(self.core_table, 1)
        self.core_count_spin.valueChanged.connect(self._rebuild_core_table)
        self._rebuild_core_table()
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)

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
            values = [core["number"], core["colour"], core["tube_number"], core["tube_colour"]]
            for col, value in enumerate(values): self.core_table.setItem(row, col, QTableWidgetItem(str(value)))
            status = QComboBox(); status.addItems(["dark", "reserved", "allocated", "spliced", "fault"]); _set_combo(status, _text(core.get("status")) or "dark"); self.core_table.setCellWidget(row, 4, status)
            for col, key in ((5, "circuit_id"), (6, "from_termination"), (7, "to_termination")):
                self.core_table.setItem(row, col, QTableWidgetItem(_text(core.get(key))))
        stats = cable_core_statistics({"core_count": len(cores), "cores": cores})
        self.stats_label.setText(f"Core allocation: {stats['used']} used/reserved, {stats['dark']} dark, {stats['fault']} fault, {stats['total']} total")

    def accept(self) -> None:
        cable_id = self.id_edit.text().strip(); name = self.name_edit.text().strip() or cable_id
        if not cable_id:
            QMessageBox.critical(self, "Invalid fibre cable", "Cable ID is required."); return
        logical_ids = [v.strip() for v in self.logical_edit.text().replace(";", ",").split(",") if v.strip()]
        route = [v.strip() for v in self.route_edit.text().split("->") if v.strip()]
        self.result = {
            **self.cable, "id": cable_id, "name": name, "cable_type": self.type_edit.text().strip() or "OS2 single-mode fibre",
            "from_instance_id": _text(self.from_combo.currentData()), "from_port": self.from_port_edit.text().strip(),
            "to_instance_id": _text(self.to_combo.currentData()), "to_port": self.to_port_edit.text().strip(),
            "from_location": _text(self.cable.get("from_location")), "to_location": _text(self.cable.get("to_location")),
            "route_path": route, "length_m": float(self.length_spin.value()), "slack_length_m": float(self.slack_spin.value()),
            "core_count": int(self.core_count_spin.value()), "cores": self._table_cores(), "logical_connection_ids": logical_ids,
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
        self.type_combo=QComboBox(); self.type_combo.addItems(["fusion","mechanical","connectorised","expressed","mid-span"]); _set_combo(self.type_combo,_text(self.splice.get("splice_type")) or "fusion")
        self.circuit_edit=QLineEdit(_text(self.splice.get("circuit_id")))
        self.loss_spin=QDoubleSpinBox(); self.loss_spin.setRange(0,10); self.loss_spin.setDecimals(3); self.loss_spin.setSuffix(" dB"); self.loss_spin.setValue(float(self.splice.get("loss_db",0.1) or 0.1))
        self.layer_edit=QLineEdit(_text(self.splice.get("drawing_layer")) or "NET-FIBRE-SPLICE")
        self.notes_edit=QTextEdit(_text(self.splice.get("notes")))
        self.core_info=QLabel(); self.core_info.setWordWrap(True)
        for label,widget in [("Splice ID",self.id_edit),("Enclosure / joint",self.node_combo),("Cassette",self.cassette_combo),("Incoming cable",self.in_cable_combo),("Incoming core",self.in_core_spin),("Outgoing cable",self.out_cable_combo),("Outgoing core",self.out_core_spin),("Splice type",self.type_combo),("Circuit ID",self.circuit_edit),("Estimated loss",self.loss_spin),("Drawing layer",self.layer_edit),("Core colours",self.core_info),("Notes",self.notes_edit)]: form.addRow(label,widget)
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
        self.result={**self.splice,"id":sid,"node_id":_text(self.node_combo.currentData()),"cassette_id":_text(self.cassette_combo.currentData()),"incoming_cable_id":_text(self.in_cable_combo.currentData()),"incoming_core":int(self.in_core_spin.value()),"outgoing_cable_id":_text(self.out_cable_combo.currentData()),"outgoing_core":int(self.out_core_spin.value()),"splice_type":self.type_combo.currentText().strip(),"circuit_id":self.circuit_edit.text().strip(),"loss_db":float(self.loss_spin.value()),"drawing_layer":self.layer_edit.text().strip() or "NET-FIBRE-SPLICE","label":sid,"notes":self.notes_edit.toPlainText().strip(),"auto_generated":bool(self.splice.get("auto_generated",False))}
        super().accept()


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
        for label,w in [("Record ID",self.id_edit),("Name",self.name_edit),("Type",self.type_combo),("Provider",self.provider_edit),("ASN",self.asn_edit),("Advertised prefixes",self.prefixes_edit),("Location",self.location_combo),("Demarcation device",self.demarc_combo),("Peering devices",self.peers),("Notes",self.notes)]: form.addRow(label,w)
        buttons=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)
    def accept(self):
        rid=self.id_edit.text().strip(); name=self.name_edit.text().strip()
        if not rid or not name: QMessageBox.critical(self,"Invalid external network","ID and name are required."); return
        peers=[]
        for i in range(self.peers.count()):
            item=self.peers.item(i)
            if item.checkState()==Qt.Checked: peers.append(_text(item.data(Qt.UserRole)))
        self.result={**self.record,"id":rid,"name":name,"network_type":self.type_combo.currentText().strip(),"provider":self.provider_edit.text().strip(),"asn":self.asn_edit.text().strip(),"prefixes":[v.strip() for v in self.prefixes_edit.text().replace(";",",").split(",") if v.strip()],"location_name":_text(self.location_combo.currentData()),"demarcation_instance_id":_text(self.demarc_combo.currentData()),"peer_instance_ids":peers,"notes":self.notes.toPlainText().strip()}; super().accept()
