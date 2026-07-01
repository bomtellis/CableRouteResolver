from __future__ import annotations

from copy import deepcopy
import pickle
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
    QProgressDialog,
    QPushButton,
    QSpinBox,
    QScrollArea,
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
    ("optical_transceiver", "Pluggable optical transceiver"),
    ("ups", "Uninterruptible power supply (UPS)"),
    ("pdu", "Power distribution unit (PDU)"),
    ("power_device", "Power device"),
    ("cable_management", "Cable management"),
    ("telco_pop", "Telecommunications point of presence"),
    ("external_network", "External / partner network"),
    ("other", "Other"),
]

CONNECTION_MEDIA = ["copper", "fibre", "wireless", "virtual", "stacking", "none"]
CONNECTION_ROLES = ["input", "output", "uplink"]
PATCH_PANEL_TYPES = ["", "copper", "fibre"]
SPLIT_RATIOS = [
    "",
    "1:2", "1:4", "1:8", "1:16", "1:32", "1:64", "1:128",
    "2:2", "2:4", "2:8", "2:16", "2:32", "2:64", "2:128",
]
FREQUENCY_OPTIONS = ["2.4 GHz", "5 GHz", "6 GHz", "60 GHz", "868 MHz", "433 MHz"]
NETWORK_TECHNOLOGIES = ["Traditional", "PoLAN"]
PORT_TYPE_OPTIONS = ["rj45", "sfp", "sfp+", "sfp28", "qsfp", "qsfp+", "qsfp28", "qsfp56", "qsfpdd", "osfp", "pon", "lc", "sc", "mpo", "usb", "console", "power", "other"]
PORT_USE_OPTIONS = ["input", "output", "uplink", "downlink", "management", "console", "pon", "client", "patch", "stacking", "power", "spare", "other"]

from network_auto_planner import (
    NetworkPlanningError,
    auto_connect_manual_devices,
    generate_network_design,
)
from network_services import cable_core_statistics, ensure_physical_fibre_for_design, generate_ip_address_plan, set_core_status_from_splices
from network_fibre_dialogs import (
    ExternalNetworkEditorDialog, FibreCableEditorDialog, FibreNodeEditorDialog,
    FibreSpliceEditorDialog, PatchLeadEditorDialog,
)
from network_schema import (
    MANUFACTURER_PREFERENCE_COMPONENTS,
    NETWORK_PORT_SPEED_OPTIONS,
    PLUGGABLE_OPTIC_PORT_TYPES,
    compatible_port_speeds,
    default_layer_connection_rules,
    default_port_speeds,
    normalise_layer_connection_rules,
    normalise_manufacturer_preferences,
    normalise_port_speeds,
    port_speed_label,
)


def _text(value) -> str:
    return str(value if value is not None else "").strip()




def _normalise_split_ratio(value: str) -> tuple[str, int, int]:
    """Normalise 1:x and 2:x splitter ratios entered as :, x or ×."""
    text = _text(value).lower().replace("×", ":").replace("x", ":")
    text = "".join(text.split())
    if ":" not in text:
        return "", 0, 0
    left, right = text.split(":", 1)
    try:
        inputs = int(left)
        outputs = int(right)
    except (TypeError, ValueError):
        return "", 0, 0
    if inputs not in {1, 2} or outputs < 1:
        return "", 0, 0
    return f"{inputs}:{outputs}", inputs, outputs

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


class PortSpeedSelectionDialog(QDialog):
    """Canonical speed picker used by port, optic and connection editors."""

    def __init__(self, parent=None, selected: Optional[Sequence[int]] = None, title: str = "Supported Port Speeds"):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.result: Optional[List[int]] = None
        selected_values = set(normalise_port_speeds(selected or []))
        layout = QVBoxLayout(self)
        info = QLabel("Select every line rate supported by this port or optic. Values are stored as canonical Mbps numbers.")
        info.setWordWrap(True)
        layout.addWidget(info)
        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QAbstractItemView.NoSelection)
        for speed, label in NETWORK_PORT_SPEED_OPTIONS:
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, speed)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if speed in selected_values else Qt.Unchecked)
            self.list_widget.addItem(item)
        layout.addWidget(self.list_widget)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self) -> None:
        self.result = [
            int(self.list_widget.item(index).data(Qt.UserRole))
            for index in range(self.list_widget.count())
            if self.list_widget.item(index).checkState() == Qt.Checked
        ]
        super().accept()


class PortSpeedButton(QPushButton):
    def __init__(self, parent=None, speeds: Optional[Sequence[int]] = None):
        super().__init__(parent)
        self._speeds = normalise_port_speeds(speeds or [])
        self.clicked.connect(self._choose)
        self._refresh_text()

    def speeds(self) -> List[int]:
        return list(self._speeds)

    def set_speeds(self, speeds: Sequence[int]) -> None:
        self._speeds = normalise_port_speeds(speeds)
        self._refresh_text()

    def _refresh_text(self) -> None:
        labels = [port_speed_label(value) for value in self._speeds]
        full_text = ", ".join(labels) if labels else "Transparent / not declared"
        if len(labels) > 3:
            display_text = ", ".join(labels[:3]) + f" +{len(labels) - 3}"
        else:
            display_text = full_text
        self.setText(display_text)
        self.setToolTip(full_text)

    def _choose(self) -> None:
        dialog = PortSpeedSelectionDialog(self, self._speeds)
        if dialog.exec() == QDialog.Accepted and dialog.result is not None:
            self.set_speeds(dialog.result)


def _expanded_instance_ports(instance: dict, asset: dict) -> List[dict]:
    rows = [row for row in asset.get("port_definitions", []) if isinstance(row, dict) and int(row.get("port_count", 0) or 0) > 0]
    result: List[dict] = []
    counters: Dict[str, int] = {}
    for row in rows:
        port_type = _text(row.get("port_type")).lower() or "other"
        port_use = _text(row.get("port_use")).lower() or "other"
        speeds = normalise_port_speeds(row.get("supported_speeds_mbps")) or default_port_speeds(port_type)
        explicit = [_text(value) for value in row.get("explicit_names", []) if _text(value)] if isinstance(row.get("explicit_names", []), list) else []
        count = max(0, int(row.get("port_count", 0) or 0))
        for name in explicit[:count]:
            result.append({"name": name, "port_type": port_type, "port_use": port_use, "speeds": speeds})
        prefix = _text(row.get("name_prefix")) or {
            "pon": "PON", "sfp": "SFP", "sfp+": "SFP+", "sfp28": "SFP28",
            "qsfp": "QSFP", "qsfp+": "QSFP+", "qsfp28": "QSFP28",
            "qsfp56": "QSFP56", "qsfpdd": "QSFP-DD", "osfp": "OSFP",
            "lc": "LC", "sc": "SC", "mpo": "MPO", "rj45": "",
        }.get(port_type, port_type.upper())
        remaining = count - min(count, len(explicit))
        counters.setdefault(prefix, 0)
        for _ in range(remaining):
            counters[prefix] += 1
            name = f"{prefix}-{counters[prefix]}" if prefix else str(counters[prefix])
            result.append({"name": name, "port_type": port_type, "port_use": port_use, "speeds": speeds})
    members = max(1, int(instance.get("stack_member_count", 1) or 1)) if bool(instance.get("logical_stack")) else 1
    if members > 1:
        result = [{**row, "name": f"{member}/{row['name']}"} for member in range(1, members + 1) for row in result]
    return result


class NetworkAssetEditorDialog(QDialog):
    def __init__(
        self, parent=None, asset: Optional[dict] = None, suggested_id: str = "NA1"
    ):
        super().__init__(parent)
        self.setWindowTitle("Network Asset")
        self.asset = deepcopy(asset or {})
        self.result: Optional[dict] = None
        screen = parent.screen() if parent is not None and hasattr(parent, "screen") else QApplication.primaryScreen()
        available = screen.availableGeometry() if screen is not None else None
        target_width = max(480, min(900, available.width() - 80)) if available is not None else 820
        target_height = max(420, min(760, available.height() - 100)) if available is not None else 720
        self.resize(target_width, target_height)
        self.setMinimumSize(min(520, target_width), min(420, target_height))

        layout = QVBoxLayout(self)

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

        self.patch_panel_format_combo = QComboBox()
        self.patch_panel_format_combo.addItem("Fixed connector panel", "fixed")
        self.patch_panel_format_combo.addItem("Modular cassette panel", "modular_cassette")
        panel_format = _text(self.asset.get("patch_panel_format")).lower()
        if bool(self.asset.get("modular_patch_panel")) or panel_format in {"modular", "cassette", "modular_cassette"}:
            panel_format = "modular_cassette"
        else:
            panel_format = "fixed"
        format_index = self.patch_panel_format_combo.findData(panel_format)
        if format_index >= 0:
            self.patch_panel_format_combo.setCurrentIndex(format_index)

        self.patch_panel_cassette_count_spin = QSpinBox()
        self.patch_panel_cassette_count_spin.setRange(1, 4)
        self.patch_panel_cassette_count_spin.setValue(
            max(1, min(4, int(self.asset.get("patch_panel_cassette_count", 4) or 4)))
        )
        self.patch_panel_cassette_count_spin.setSuffix(" cassettes")
        self.patch_panel_cassette_count_spin.setToolTip(
            "Number of cassette positions fitted in the modular panel. Maximum four."
        )
        self.patch_panel_cassette_capacity_label = QLabel("12 front connector positions per cassette")
        self.patch_panel_cassette_capacity_label.setWordWrap(True)

        self.patch_panel_cassette_front_combo = QComboBox()
        self.patch_panel_cassette_front_combo.addItem("12 duplex LC connectors", "lc_duplex")
        self.patch_panel_cassette_front_combo.addItem("12 simplex SC connectors", "sc_simplex")
        self.patch_panel_cassette_front_combo.addItem("12 duplex SC connectors", "sc_duplex")
        front_value = _text(self.asset.get("patch_panel_cassette_front_connector")).lower() or "lc_duplex"
        front_index = self.patch_panel_cassette_front_combo.findData(front_value)
        self.patch_panel_cassette_front_combo.setCurrentIndex(max(0, front_index))

        self.patch_panel_cassette_mode_combo = QComboBox()
        self.patch_panel_cassette_mode_combo.addItem("Spliced / pigtail cassette", "spliced")
        self.patch_panel_cassette_mode_combo.addItem("Connectorised MPO/MTP breakout cassette", "connectorised")
        mode_value = _text(self.asset.get("patch_panel_cassette_termination_mode")).lower() or "spliced"
        mode_index = self.patch_panel_cassette_mode_combo.findData(mode_value)
        self.patch_panel_cassette_mode_combo.setCurrentIndex(max(0, mode_index))

        self.patch_panel_cassette_rear_combo = QComboBox()
        for label, value in (("MPO-12", "mpo-12"), ("MTP-12", "mtp-12"), ("MPO-24", "mpo-24"), ("MTP-24", "mtp-24")):
            self.patch_panel_cassette_rear_combo.addItem(label, value)
        rear_value = _text(self.asset.get("patch_panel_cassette_rear_connector")).lower() or "mpo-24"
        rear_index = self.patch_panel_cassette_rear_combo.findData(rear_value)
        self.patch_panel_cassette_rear_combo.setCurrentIndex(max(0, rear_index))

        self.patch_panel_cassette_rear_count_spin = QSpinBox()
        self.patch_panel_cassette_rear_count_spin.setRange(1, 4)
        self.patch_panel_cassette_rear_count_spin.setValue(
            max(1, min(4, int(self.asset.get("patch_panel_cassette_rear_connector_count", 1) or 1)))
        )
        self.patch_panel_cassette_rear_count_spin.setSuffix(" rear connectors")

        self.patch_panel_mpo_threshold_spin = QSpinBox()
        self.patch_panel_mpo_threshold_spin.setRange(12, 6912)
        self.patch_panel_mpo_threshold_spin.setValue(
            max(12, int(self.asset.get("patch_panel_mpo_breakout_minimum_cores", 48) or 48))
        )
        self.patch_panel_mpo_threshold_spin.setSuffix(" fibre cores")
        self.patch_panel_mpo_threshold_spin.setToolTip(
            "Incoming cables at or above this size are allocated connectorised MPO/MTP breakout cassettes automatically."
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

        self.power_capacity_spin = QDoubleSpinBox()
        self.power_capacity_spin.setRange(0.0, 100_000_000.0)
        self.power_capacity_spin.setDecimals(1)
        self.power_capacity_spin.setSuffix(" W")
        self.power_capacity_spin.setSpecialValueText("Not declared")
        self.power_capacity_spin.setValue(float(self.asset.get("power_capacity_w", 0.0) or 0.0))

        self.power_outlet_count_spin = QSpinBox()
        self.power_outlet_count_spin.setRange(0, 10000)
        self.power_outlet_count_spin.setValue(int(self.asset.get("power_outlet_count", 0) or 0))

        self.power_feed_count_spin = QSpinBox()
        self.power_feed_count_spin.setRange(1, 8)
        self.power_feed_count_spin.setValue(max(1, int(self.asset.get("power_feed_count", 1) or 1)))

        self.redundant_power_check = QCheckBox("Device has independent/redundant power supplies")
        self.redundant_power_check.setChecked(bool(self.asset.get("redundant_power_supplies", False)))

        self.rack_mount_style_combo = QComboBox()
        self.rack_mount_style_combo.addItem("Rack-unit mounted", "rack_units")
        self.rack_mount_style_combo.addItem("Vertical side-mounted PDU", "vertical_side")
        mount_index = self.rack_mount_style_combo.findData(_text(self.asset.get("rack_mount_style")) or "rack_units")
        if mount_index >= 0:
            self.rack_mount_style_combo.setCurrentIndex(mount_index)

        self.ups_backed_source_check = QCheckBox("This power source is UPS backed")
        self.ups_backed_source_check.setChecked(bool(self.asset.get("ups_backed_source", False)))

        self.poe_budget_spin = QDoubleSpinBox()
        self.poe_budget_spin.setRange(0.0, 1_000_000.0)
        self.poe_budget_spin.setDecimals(2)
        self.poe_budget_spin.setSuffix(" W")
        self.poe_budget_spin.setValue(float(self.asset.get("poe_budget_w", 0.0) or 0.0))

        self.bandwidth_capacity_spin = QDoubleSpinBox()
        self.bandwidth_capacity_spin.setRange(0.0, 10_000_000.0)
        self.bandwidth_capacity_spin.setDecimals(3)
        self.bandwidth_capacity_spin.setSuffix(" Gbps")
        self.bandwidth_capacity_spin.setSpecialValueText("Not declared")
        self.bandwidth_capacity_spin.setValue(float(self.asset.get("bandwidth_capacity_gbps", 0.0) or 0.0))
        self.bandwidth_capacity_spin.setToolTip("Aggregate switching or routed throughput declared by the manufacturer. Zero means not declared.")

        self.packet_throughput_spin = QDoubleSpinBox()
        self.packet_throughput_spin.setRange(0.0, 10_000_000.0)
        self.packet_throughput_spin.setDecimals(3)
        self.packet_throughput_spin.setSuffix(" Mpps")
        self.packet_throughput_spin.setSpecialValueText("Not declared")
        self.packet_throughput_spin.setValue(float(self.asset.get("packet_throughput_mpps", 0.0) or 0.0))
        self.packet_throughput_spin.setToolTip("Maximum packet forwarding rate in millions of packets per second. Zero means not declared.")

        self.expected_bandwidth_spin = QDoubleSpinBox()
        self.expected_bandwidth_spin.setRange(0.0, 1_000_000_000.0)
        self.expected_bandwidth_spin.setDecimals(3)
        self.expected_bandwidth_spin.setSuffix(" Mbps")
        self.expected_bandwidth_spin.setValue(float(self.asset.get("expected_bandwidth_mbps", 0.0) or 0.0))
        self.expected_bandwidth_spin.setToolTip("Expected traffic generated by this installed device, excluding traffic merely forwarded through it.")

        self.expected_packet_rate_spin = QDoubleSpinBox()
        self.expected_packet_rate_spin.setRange(0.0, 1_000_000_000_000.0)
        self.expected_packet_rate_spin.setDecimals(1)
        self.expected_packet_rate_spin.setSuffix(" pps")
        self.expected_packet_rate_spin.setValue(float(self.asset.get("expected_packet_rate_pps", 0.0) or 0.0))
        self.expected_packet_rate_spin.setToolTip("Expected packet generation rate for this installed device.")

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

        self.port_table = QTableWidget(0, 4)
        self.port_table.setHorizontalHeaderLabels(["Port type", "Port count", "Port use", "Supported speeds"])
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

        self.optic_form_factor_combo = QComboBox()
        self.optic_form_factor_combo.addItems(sorted(PLUGGABLE_OPTIC_PORT_TYPES))
        _set_combo_text(self.optic_form_factor_combo, _text(self.asset.get("optic_form_factor")) or "sfp")
        self.optic_speeds_button = PortSpeedButton(self, self.asset.get("supported_speeds_mbps", []))
        self.optic_connector_combo = QComboBox()
        self.optic_connector_combo.addItems(["lc", "sc", "mpo"])
        _set_combo_text(self.optic_connector_combo, _text(self.asset.get("optic_connector_type")) or "lc")
        self.optic_standard_edit = QLineEdit(_text(self.asset.get("optic_fibre_standard")) or _text(self.asset.get("optical_standard")) or "OS2")
        self.optic_reach_spin = QDoubleSpinBox()
        self.optic_reach_spin.setRange(0.0, 10_000_000.0)
        self.optic_reach_spin.setDecimals(1)
        self.optic_reach_spin.setSuffix(" m")
        self.optic_reach_spin.setValue(float(self.asset.get("optic_reach_m", 0.0) or 0.0))
        self.optic_tx_edit = QLineEdit(_text(self.asset.get("optical_tx_power_dbm")))
        self.optic_rx_edit = QLineEdit(_text(self.asset.get("optical_receiver_sensitivity_dbm")))
        self.optic_insertion_edit = QLineEdit(_text(self.asset.get("optical_insertion_loss_db")))
        self.optic_return_edit = QLineEdit(_text(self.asset.get("optical_return_loss_db")))
        self.optic_wavelength_spin = QSpinBox()
        self.optic_wavelength_spin.setRange(0, 100_000)
        self.optic_wavelength_spin.setSuffix(" nm")
        self.optic_wavelength_spin.setValue(int(self.asset.get("optical_wavelength_nm", 0) or 0))

        self.notes_edit = QTextEdit(_text(self.asset.get("notes")))
        self.notes_edit.setMinimumHeight(90)

        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        layout.addWidget(self.tabs, 1)

        def add_scroll_form_tab(title: str) -> tuple[QWidget, QFormLayout]:
            page = QWidget()
            page_layout = QVBoxLayout(page)
            page_layout.setContentsMargins(0, 0, 0, 0)
            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            content = QWidget()
            form_layout = QFormLayout(content)
            form_layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
            form_layout.setRowWrapPolicy(QFormLayout.WrapLongRows)
            scroll.setWidget(content)
            page_layout.addWidget(scroll)
            self.tabs.addTab(page, title)
            return page, form_layout

        _general_page, general_form = add_scroll_form_tab("General")
        general_form.addRow("Asset ID", self.id_edit)
        general_form.addRow("Name", self.name_edit)
        general_form.addRow("Asset type", self.asset_type_combo)
        general_form.addRow("Manufacturer", self.manufacturer_edit)
        general_form.addRow("Model", self.model_edit)
        general_form.addRow("Patch panel medium", self.patch_panel_type_combo)
        general_form.addRow("Patch panel construction", self.patch_panel_format_combo)
        general_form.addRow("Modular cassette quantity", self.patch_panel_cassette_count_spin)
        general_form.addRow("Cassette capacity", self.patch_panel_cassette_capacity_label)
        general_form.addRow("Cassette front connector", self.patch_panel_cassette_front_combo)
        general_form.addRow("Cassette termination", self.patch_panel_cassette_mode_combo)
        general_form.addRow("Rear MPO/MTP connector", self.patch_panel_cassette_rear_combo)
        general_form.addRow("Rear connectors per cassette", self.patch_panel_cassette_rear_count_spin)
        general_form.addRow("Automatic MPO breakout threshold", self.patch_panel_mpo_threshold_spin)
        general_form.addRow("Fibre split ratio", self.split_ratio_combo)
        general_form.addRow("OLT maximum split ratio", self.olt_max_split_ratio_combo)
        general_form.addRow("Wireless frequencies", self.frequencies_list)
        general_form.addRow("Additional frequencies", self.additional_frequencies_edit)

        _capacity_page, capacity_form = add_scroll_form_tab("Capacity and rack")
        capacity_form.addRow("Power input", self.power_input_spin)
        capacity_form.addRow("Power output capacity", self.power_capacity_spin)
        capacity_form.addRow("PDU outlet count", self.power_outlet_count_spin)
        capacity_form.addRow("Required power feeds", self.power_feed_count_spin)
        capacity_form.addRow("Redundant supplies", self.redundant_power_check)
        capacity_form.addRow("Rack mounting", self.rack_mount_style_combo)
        capacity_form.addRow("UPS-backed source", self.ups_backed_source_check)
        capacity_form.addRow("PoE budget", self.poe_budget_spin)
        capacity_form.addRow("Switching / routing bandwidth", self.bandwidth_capacity_spin)
        capacity_form.addRow("Packet forwarding throughput", self.packet_throughput_spin)
        capacity_form.addRow("Expected device bandwidth", self.expected_bandwidth_spin)
        capacity_form.addRow("Expected device packet rate", self.expected_packet_rate_spin)
        capacity_form.addRow("Stacking", self.supports_stacking_check)
        capacity_form.addRow("Maximum stack members", self.max_stack_members_spin)
        capacity_form.addRow("Rack spaces", self.rack_units_spin)
        capacity_form.addRow("Switch rack allowance", self.switch_rack_allowance_spin)
        capacity_form.addRow("OLT units per rack unit", self.olt_units_per_u_spin)

        ports_page = QWidget()
        ports_layout = QVBoxLayout(ports_page)
        ports_layout.addWidget(self.port_table, 1)
        ports_layout.addWidget(port_buttons)
        ports_form = QFormLayout()
        ports_form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        ports_form.setRowWrapPolicy(QFormLayout.WrapLongRows)
        ports_form.addRow("Input connection type", self.input_type_combo)
        ports_form.addRow("Output connection type", self.output_type_combo)
        ports_form.addRow("Uplink connection type", self.uplink_type_combo)
        ports_layout.addLayout(ports_form)
        self.tabs.addTab(ports_page, "Ports and connectivity")

        _optics_page, optics_form = add_scroll_form_tab("Optics")
        optics_form.addRow("Optic form factor", self.optic_form_factor_combo)
        optics_form.addRow("Optic supported speeds", self.optic_speeds_button)
        optics_form.addRow("Optic connector", self.optic_connector_combo)
        optics_form.addRow("Optic fibre standard", self.optic_standard_edit)
        optics_form.addRow("Optic maximum reach", self.optic_reach_spin)
        optics_form.addRow("Optic transmit power (dBm)", self.optic_tx_edit)
        optics_form.addRow("Optic receiver sensitivity (dBm)", self.optic_rx_edit)
        optics_form.addRow("Optic insertion loss (dB)", self.optic_insertion_edit)
        optics_form.addRow("Optic return loss (dB)", self.optic_return_edit)
        optics_form.addRow("Optic wavelength", self.optic_wavelength_spin)

        notes_page = QWidget()
        notes_layout = QVBoxLayout(notes_page)
        notes_layout.addWidget(self.notes_edit)
        self.tabs.addTab(notes_page, "Notes")

        self.asset_type_combo.currentIndexChanged.connect(self._update_visibility)
        self.patch_panel_type_combo.currentTextChanged.connect(self._update_visibility)
        self.patch_panel_format_combo.currentIndexChanged.connect(self._update_visibility)
        self.patch_panel_cassette_count_spin.valueChanged.connect(self._update_visibility)
        self.patch_panel_cassette_mode_combo.currentIndexChanged.connect(self._update_visibility)
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
        type_combo.setProperty("source_row", deepcopy(row))
        use_combo = QComboBox()
        use_combo.addItems(PORT_USE_OPTIONS)
        _set_combo_text(use_combo, _text(row.get("port_use")) or "client")
        count_item = QTableWidgetItem(str(max(1, int(row.get("port_count", 1) or 1))))
        count_item.setTextAlignment(Qt.AlignCenter)
        speeds = normalise_port_speeds(row.get("supported_speeds_mbps"))
        if not speeds:
            speeds = default_port_speeds(_text(row.get("port_type")) or "rj45")
        speed_button = PortSpeedButton(self.port_table, speeds)
        self.port_table.setCellWidget(table_row, 0, type_combo)
        self.port_table.setItem(table_row, 1, count_item)
        self.port_table.setCellWidget(table_row, 2, use_combo)
        self.port_table.setCellWidget(table_row, 3, speed_button)
        type_combo.currentTextChanged.connect(lambda value, button=speed_button: button.set_speeds(default_port_speeds(value)))
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
            speed_button = self.port_table.cellWidget(row, 3)
            item = self.port_table.item(row, 1)
            try:
                count = max(0, int(item.text() if item else 0))
            except (TypeError, ValueError):
                count = 0
            if count <= 0:
                continue
            source = deepcopy(type_combo.property("source_row") or {}) if type_combo else {}
            port_type = _text(type_combo.currentText() if type_combo else "other").lower()
            source.update(
                {
                    "port_type": port_type,
                    "port_count": count,
                    "port_use": _text(use_combo.currentText() if use_combo else "other").lower(),
                    "name_prefix": _text(source.get("name_prefix")),
                    "supported_speeds_mbps": speed_button.speeds() if isinstance(speed_button, PortSpeedButton) else default_port_speeds(port_type),
                }
            )
            result.append(source)
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
        is_patch_panel = asset_type == "patch_panel"
        is_fibre_patch_panel = is_patch_panel and _text(self.patch_panel_type_combo.currentText()).lower() == "fibre"
        is_modular_patch_panel = is_fibre_patch_panel and _text(self.patch_panel_format_combo.currentData()).lower() == "modular_cassette"
        self.patch_panel_type_combo.setEnabled(is_patch_panel)
        self.patch_panel_format_combo.setEnabled(is_fibre_patch_panel)
        self.patch_panel_cassette_count_spin.setEnabled(is_modular_patch_panel)
        self.patch_panel_cassette_capacity_label.setEnabled(is_modular_patch_panel)
        self.patch_panel_cassette_front_combo.setEnabled(is_modular_patch_panel)
        self.patch_panel_cassette_mode_combo.setEnabled(is_modular_patch_panel)
        connectorised_cassette = is_modular_patch_panel and _text(self.patch_panel_cassette_mode_combo.currentData()).lower() == "connectorised"
        self.patch_panel_cassette_rear_combo.setEnabled(connectorised_cassette)
        self.patch_panel_cassette_rear_count_spin.setEnabled(connectorised_cassette)
        self.patch_panel_mpo_threshold_spin.setEnabled(is_modular_patch_panel)
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
        optic_enabled = asset_type == "optical_transceiver"
        for widget in (self.optic_form_factor_combo, self.optic_speeds_button, self.optic_connector_combo, self.optic_standard_edit, self.optic_reach_spin, self.optic_tx_edit, self.optic_rx_edit, self.optic_insertion_edit, self.optic_return_edit, self.optic_wavelength_spin):
            widget.setEnabled(optic_enabled)
        self.port_table.setEnabled(not optic_enabled and not is_modular_patch_panel)
        capacity_enabled = asset_type in {
            "network_switch", "network_router", "firewall",
            "optical_line_terminal", "optical_network_terminal",
        }
        self.bandwidth_capacity_spin.setEnabled(capacity_enabled)
        self.packet_throughput_spin.setEnabled(capacity_enabled)
        is_pdu = asset_type == "pdu"
        is_power_source = asset_type in {"ups", "power_device"}
        self.power_capacity_spin.setEnabled(is_pdu or is_power_source)
        self.power_outlet_count_spin.setEnabled(is_pdu)
        self.rack_mount_style_combo.setEnabled(is_pdu)
        self.ups_backed_source_check.setEnabled(is_power_source)
        self.power_feed_count_spin.setEnabled(asset_type not in {"pdu", "ups", "power_device", "cable_management", "optical_transceiver"})
        self.redundant_power_check.setEnabled(self.power_feed_count_spin.isEnabled())

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
        split_ratio = ""
        split_inputs = 0
        split_outputs = 0
        if asset_type == "fibre_splitter":
            split_ratio, split_inputs, split_outputs = _normalise_split_ratio(
                self.split_ratio_combo.currentText()
            )
            if not split_ratio:
                QMessageBox.critical(
                    self,
                    "Invalid asset",
                    "A fibre splitter requires a valid 1:x or 2:x split ratio.",
                )
                return
            self.split_ratio_combo.setEditText(split_ratio)

        is_modular_patch_panel = bool(
            asset_type == "patch_panel"
            and self.patch_panel_type_combo.currentText().strip().lower() == "fibre"
            and _text(self.patch_panel_format_combo.currentData()).lower() == "modular_cassette"
        )
        port_definitions = self._port_definitions()
        if asset_type == "fibre_splitter":
            input_names = ["Input-1"] if split_inputs == 1 else ["Input-A", "Input-B"]
            port_definitions = [
                {
                    "port_type": "lc",
                    "port_count": split_inputs,
                    "port_use": "input",
                    "name_prefix": "Input",
                    "explicit_names": input_names,
                },
                {
                    "port_type": "lc",
                    "port_count": split_outputs,
                    "port_use": "output",
                    "name_prefix": "Output",
                },
            ]
        portless_asset_types = {"ups", "pdu", "power_device", "cable_management", "optical_transceiver"}
        if (
            not port_definitions
            and asset_type not in portless_asset_types
            and not is_modular_patch_panel
        ):
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

        def optional_float(edit: QLineEdit, label: str):
            text = edit.text().strip()
            if not text:
                return ""
            try:
                return float(text)
            except ValueError:
                QMessageBox.critical(self, "Invalid asset", f"{label} must be a number or blank.")
                raise

        try:
            optic_tx = optional_float(self.optic_tx_edit, "Transmit power")
            optic_rx = optional_float(self.optic_rx_edit, "Receiver sensitivity")
            optic_insertion = optional_float(self.optic_insertion_edit, "Insertion loss")
            optic_return = optional_float(self.optic_return_edit, "Return loss")
        except ValueError:
            return

        cassette_count = int(self.patch_panel_cassette_count_spin.value()) if is_modular_patch_panel else 0
        cassette_front = _text(self.patch_panel_cassette_front_combo.currentData()).lower() if is_modular_patch_panel else ""
        cassette_mode = _text(self.patch_panel_cassette_mode_combo.currentData()).lower() if is_modular_patch_panel else ""
        cassette_rear = _text(self.patch_panel_cassette_rear_combo.currentData()).lower() if is_modular_patch_panel else ""
        cassette_rear_count = int(self.patch_panel_cassette_rear_count_spin.value()) if is_modular_patch_panel else 0
        if is_modular_patch_panel:
            port_type = "sc" if cassette_front.startswith("sc_") else "lc"
            connector_label = "SC" if port_type == "sc" else "LC"
            explicit_names = [
                f"C{cassette}-{connector_label}-{position:02d}"
                for cassette in range(1, cassette_count + 1)
                for position in range(1, 13)
            ]
            port_definitions = [
                {
                    "port_type": port_type,
                    "port_count": cassette_count * 12,
                    "port_use": "patch",
                    "name_prefix": connector_label,
                    "explicit_names": explicit_names,
                    "supported_speeds_mbps": [],
                    "default_speed_mbps": 0,
                }
            ]

        self.result = {
            **self.asset,
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
            "patch_panel_format": "modular_cassette" if is_modular_patch_panel else "fixed",
            "modular_patch_panel": is_modular_patch_panel,
            "patch_panel_cassette_count": cassette_count,
            "patch_panel_cassette_capacity": 12 if is_modular_patch_panel else 0,
            "patch_panel_cassette_front_connector": cassette_front,
            "patch_panel_cassette_termination_mode": cassette_mode,
            "patch_panel_cassette_rear_connector": cassette_rear if cassette_mode == "connectorised" else "",
            "patch_panel_cassette_rear_connector_count": cassette_rear_count if cassette_mode == "connectorised" else 0,
            "patch_panel_mpo_breakout_minimum_cores": int(self.patch_panel_mpo_threshold_spin.value()) if is_modular_patch_panel else 0,
            "split_ratio": split_ratio if asset_type == "fibre_splitter" else "",
            "max_split_ratio": (
                self.olt_max_split_ratio_combo.currentText().strip()
                if asset_type == "optical_line_terminal"
                else ""
            ),
            "frequencies": frequencies if asset_type == "wireless_access_point" else [],
            "power_input_w": float(self.power_input_spin.value()),
            "power_capacity_w": float(self.power_capacity_spin.value()),
            "power_outlet_count": int(self.power_outlet_count_spin.value()) if asset_type == "pdu" else int(self.asset.get("power_outlet_count", 0) or 0),
            "power_feed_count": int(self.power_feed_count_spin.value()),
            "redundant_power_supplies": bool(self.redundant_power_check.isChecked()),
            "rack_mount_style": _text(self.rack_mount_style_combo.currentData()) if asset_type == "pdu" else _text(self.asset.get("rack_mount_style")),
            "ups_backed_source": bool(self.ups_backed_source_check.isChecked()) if asset_type in {"ups", "power_device"} else bool(self.asset.get("ups_backed_source", False)),
            "poe_budget_w": float(self.poe_budget_spin.value()),
            "bandwidth_capacity_gbps": float(self.bandwidth_capacity_spin.value()),
            "packet_throughput_mpps": float(self.packet_throughput_spin.value()),
            "expected_bandwidth_mbps": float(self.expected_bandwidth_spin.value()),
            "expected_packet_rate_pps": float(self.expected_packet_rate_spin.value()),
            "port_definitions": port_definitions,
            "number_of_ports": 0 if asset_type == "optical_transceiver" else sum(row["port_count"] for row in port_definitions),
            "connections_in": (
                split_inputs if asset_type == "fibre_splitter"
                else cassette_count * 12 if is_modular_patch_panel
                else int(self.connections_in_spin.value())
            ),
            "connections_out": (
                split_outputs if asset_type == "fibre_splitter"
                else cassette_count * 12 if is_modular_patch_panel
                else int(self.connections_out_spin.value())
            ),
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
            "optic_form_factor": self.optic_form_factor_combo.currentText().strip() if asset_type == "optical_transceiver" else _text(self.asset.get("optic_form_factor")),
            "supported_speeds_mbps": self.optic_speeds_button.speeds() if asset_type == "optical_transceiver" else normalise_port_speeds(self.asset.get("supported_speeds_mbps")),
            "optic_connector_type": self.optic_connector_combo.currentText().strip() if asset_type == "optical_transceiver" else _text(self.asset.get("optic_connector_type")),
            "optic_fibre_standard": self.optic_standard_edit.text().strip() if asset_type == "optical_transceiver" else _text(self.asset.get("optic_fibre_standard")),
            "optic_reach_m": float(self.optic_reach_spin.value()) if asset_type == "optical_transceiver" else float(self.asset.get("optic_reach_m", 0.0) or 0.0),
            "optical_tx_power_dbm": optic_tx if asset_type == "optical_transceiver" else self.asset.get("optical_tx_power_dbm", ""),
            "optical_receiver_sensitivity_dbm": optic_rx if asset_type == "optical_transceiver" else self.asset.get("optical_receiver_sensitivity_dbm", ""),
            "optical_insertion_loss_db": optic_insertion if asset_type == "optical_transceiver" else self.asset.get("optical_insertion_loss_db", ""),
            "optical_return_loss_db": optic_return if asset_type == "optical_transceiver" else self.asset.get("optical_return_loss_db", ""),
            "optical_wavelength_nm": int(self.optic_wavelength_spin.value()) if asset_type == "optical_transceiver" else int(self.asset.get("optical_wavelength_nm", 0) or 0),
            "notes": self.notes_edit.toPlainText().strip(),
        }
        super().accept()


def rack_selection_records(data: dict) -> List[dict]:
    """Return real rack choices, including racks inferred from installed assets.

    Auto-generated rack records with no installed equipment are intentionally
    omitted so stale planner records cannot reappear as ghost install targets.
    Manually created empty cabinets remain available.
    """
    installed_keys = {
        (
            int(row.get("floor", 0) or 0),
            _text(row.get("location_name")),
            _text(row.get("rack_name")),
        )
        for row in data.get("network_asset_instances", [])
        if isinstance(row, dict) and _text(row.get("rack_name"))
    }
    result: List[dict] = []
    seen = set()
    for rack in data.get("network_racks", []):
        if not isinstance(rack, dict):
            continue
        key = (
            int(rack.get("floor", 0) or 0),
            _text(rack.get("location_name")),
            _text(rack.get("name")),
        )
        if not key[2] or key in seen:
            continue
        occupied = key in installed_keys
        if bool(rack.get("auto_generated")) and not occupied:
            continue
        result.append({**rack, "has_installed_assets": occupied})
        seen.add(key)
    for floor, location, rack_name in sorted(installed_keys):
        key = (floor, location, rack_name)
        if key in seen:
            continue
        capacity = max(
            [
                int(row.get("rack_size_u", 0) or 0)
                for row in data.get("network_asset_instances", [])
                if isinstance(row, dict)
                and int(row.get("floor", 0) or 0) == floor
                and _text(row.get("location_name")) == location
                and _text(row.get("rack_name")) == rack_name
            ]
            or [0]
        )
        result.append(
            {
                "id": "",
                "name": rack_name,
                "location_name": location,
                "floor": floor,
                "capacity_u": capacity,
                "has_installed_assets": True,
                "inferred_from_assets": True,
            }
        )
        seen.add(key)
    return result


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
        default_auto_connect: bool = True,
        racks: Optional[Sequence[dict]] = None,
        default_location: str = "",
        default_rack: str = "",
        default_rack_start_u: int = 0,
    ):
        super().__init__(parent)
        self.setWindowTitle("Installed Network Asset")
        self.instance = deepcopy(instance or {})
        self.assets = list(assets or [])
        self.locations = list(locations or [])
        self.racks = [row for row in (racks or []) if isinstance(row, dict)]
        self.default_rack = _text(default_rack)
        self.result: Optional[dict] = None
        self.auto_connect_requested = False
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

        self.network_layer_combo = QComboBox()
        self.network_layer_combo.addItem("Automatic from asset", "")
        self.network_layer_combo.addItem("Edge / router", "edge")
        self.network_layer_combo.addItem("Core", "core")
        self.network_layer_combo.addItem("Aggregation / distribution", "aggregation")
        self.network_layer_combo.addItem("Access", "access")
        self.network_layer_combo.addItem("Endpoint", "endpoint")
        self.network_layer_combo.addItem("OLT", "olt")
        self.network_layer_combo.addItem("Fibre splitter", "splitter")
        self.network_layer_combo.addItem("ONT", "ont")
        current_layer = _text(
            self.instance.get("network_layer")
            or self.instance.get("design_layer")
        ).lower()
        layer_index = self.network_layer_combo.findData(current_layer)
        if layer_index >= 0:
            self.network_layer_combo.setCurrentIndex(layer_index)

        self.auto_connect_check = QCheckBox(
            "Auto-connect this device using the configured topology rules"
        )
        self.auto_connect_check.setChecked(
            bool(default_auto_connect) and not bool(self.instance)
        )
        self.auto_connect_check.setToolTip(
            "Find the nearest valid upstream device, allocate compatible free ports, "
            "and route the new link along the existing cable graph."
        )

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
            _text(self.instance.get("location_name")) or _text(default_location)
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

        self.rack_combo = QComboBox()
        self.rack_combo.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLengthWithIcon)
        self.rack_combo.setMinimumContentsLength(18)
        self.rack_start_spin = QSpinBox()
        self.rack_start_spin.setRange(0, 1000)
        self.rack_start_spin.setSuffix("U")
        self.rack_start_spin.setValue(int(self.instance.get("rack_start_u", default_rack_start_u) or default_rack_start_u or 0))
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
        form.addRow("Network layer", self.network_layer_combo)
        form.addRow("Automatic connection", self.auto_connect_check)
        form.addRow("Location", self.location_combo)
        form.addRow("Floor", self.floor_spin)
        form.addRow("X", self.x_spin)
        form.addRow("Y", self.y_spin)
        form.addRow("Rack", self.rack_combo)
        form.addRow("Rack start", self.rack_start_spin)
        form.addRow("Rack cabinet size", self.rack_size_spin)
        form.addRow("Management IP", self.management_ip_edit)
        form.addRow("Management VLAN", self.management_vlan_edit)
        form.addRow("Power feed", self.power_feed_edit)
        form.addRow("UPS source", self.ups_source_edit)
        form.addRow("Notes", self.notes_edit)

        self.location_combo.currentIndexChanged.connect(self._location_changed)
        self.floor_spin.valueChanged.connect(lambda _value: self._populate_rack_combo())
        self._populate_rack_combo(_text(self.instance.get("rack_name")) or self.default_rack)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _populate_rack_combo(self, wanted: str = "") -> None:
        wanted = _text(wanted) or _text(self.rack_combo.currentData())
        location_name = _text(self.location_combo.currentData())
        floor = int(self.floor_spin.value())
        self.rack_combo.blockSignals(True)
        self.rack_combo.clear()
        self.rack_combo.addItem("No rack cabinet", "")
        seen = set()
        for rack in sorted(
            self.racks,
            key=lambda row: (_text(row.get("location_name")).lower(), _text(row.get("name")).lower()),
        ):
            rack_name = _text(rack.get("name"))
            if not rack_name or rack_name in seen:
                continue
            if _text(rack.get("location_name")) != location_name or int(rack.get("floor", floor) or 0) != floor:
                continue
            if bool(rack.get("auto_generated")) and not bool(rack.get("has_installed_assets")):
                continue
            seen.add(rack_name)
            capacity = int(rack.get("capacity_u", 0) or 0)
            suffix = f" ({capacity}U)" if capacity else ""
            self.rack_combo.addItem(rack_name + suffix, rack_name)
        if wanted and wanted not in seen:
            current_location = _text(self.instance.get("location_name"))
            if wanted == _text(self.instance.get("rack_name")) and location_name == current_location:
                self.rack_combo.addItem(f"{wanted} (installed rack)", wanted)
        index = self.rack_combo.findData(wanted)
        self.rack_combo.setCurrentIndex(index if index >= 0 else 0)
        self.rack_combo.blockSignals(False)

    def _location_changed(self) -> None:
        name = _text(self.location_combo.currentData())
        if not name:
            self._populate_rack_combo("")
            return
        location = next(
            (item for item in self.locations if _text(item.get("name")) == name), None
        )
        if location is None:
            return
        self.floor_spin.setValue(int(location.get("floor", 0) or 0))
        self.x_spin.setValue(float(location.get("x", 0.0) or 0.0))
        self.y_spin.setValue(float(location.get("y", 0.0) or 0.0))
        self._populate_rack_combo(self.default_rack)

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
            "network_layer": _text(self.network_layer_combo.currentData()),
            "location_name": _text(self.location_combo.currentData()),
            "floor": int(self.floor_spin.value()),
            "x": float(self.x_spin.value()),
            "y": float(self.y_spin.value()),
            "rack_name": _text(self.rack_combo.currentData()),
            "rack_start_u": int(self.rack_start_spin.value()),
            "rack_size_u": int(self.rack_size_spin.value()),
            "management_ip": self.management_ip_edit.text().strip(),
            "management_vlan": self.management_vlan_edit.text().strip(),
            "power_feed": self.power_feed_edit.text().strip(),
            "ups_source": self.ups_source_edit.text().strip(),
            "notes": self.notes_edit.toPlainText().strip(),
        }
        self.auto_connect_requested = bool(self.auto_connect_check.isChecked())
        super().accept()


class NetworkRackEditorDialog(QDialog):
    """Create or edit an explicit rack cabinet, including empty cabinets."""

    def __init__(self, parent=None, rack: Optional[dict] = None, locations: Optional[Sequence[dict]] = None, suggested_id: str = "NR1", default_location: str = "", default_floor: int = 0, default_capacity_u: int = 42):
        super().__init__(parent)
        self.setWindowTitle("Rack Cabinet")
        self.rack = deepcopy(rack or {})
        self.locations = list(locations or [])
        self.result: Optional[dict] = None
        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)
        self.id_edit = QLineEdit(_text(self.rack.get("id")) or suggested_id)
        self.name_edit = QLineEdit(_text(self.rack.get("name")))
        self.location_combo = QComboBox()
        for location in sorted(self.locations, key=lambda row: (int(row.get("floor", 0) or 0), _text(row.get("name")))):
            name = _text(location.get("name"))
            if name:
                self.location_combo.addItem(f"{name} - Floor {int(location.get('floor', 0) or 0)}", name)
        wanted = _text(self.rack.get("location_name")) or default_location
        index = self.location_combo.findData(wanted)
        if index >= 0:
            self.location_combo.setCurrentIndex(index)
        self.floor_spin = QSpinBox(); self.floor_spin.setRange(-20, 200); self.floor_spin.setValue(int(self.rack.get("floor", default_floor) or 0))
        self.capacity_spin = QSpinBox(); self.capacity_spin.setRange(1, 200); self.capacity_spin.setSuffix("U"); self.capacity_spin.setValue(max(1, int(self.rack.get("capacity_u", default_capacity_u) or default_capacity_u)))
        self.manufacturer_edit = QLineEdit(_text(self.rack.get("manufacturer")))
        self.model_edit = QLineEdit(_text(self.rack.get("model")))
        self.notes_edit = QTextEdit(_text(self.rack.get("notes"))); self.notes_edit.setMinimumHeight(80)
        form.addRow("Cabinet ID", self.id_edit)
        form.addRow("Cabinet name", self.name_edit)
        form.addRow("Location", self.location_combo)
        form.addRow("Floor", self.floor_spin)
        form.addRow("Capacity", self.capacity_spin)
        form.addRow("Manufacturer", self.manufacturer_edit)
        form.addRow("Model", self.model_edit)
        form.addRow("Notes", self.notes_edit)
        self.location_combo.currentIndexChanged.connect(self._location_changed)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _location_changed(self) -> None:
        name = _text(self.location_combo.currentData())
        location = next((row for row in self.locations if _text(row.get("name")) == name), None)
        if location is not None:
            self.floor_spin.setValue(int(location.get("floor", 0) or 0))

    def accept(self) -> None:
        rack_id = self.id_edit.text().strip()
        name = self.name_edit.text().strip()
        location_name = _text(self.location_combo.currentData())
        if not rack_id or not name or not location_name:
            QMessageBox.critical(self, "Invalid cabinet", "Cabinet ID, name and location are required.")
            return
        self.result = {
            "id": rack_id, "name": name, "location_name": location_name,
            "floor": int(self.floor_spin.value()), "capacity_u": int(self.capacity_spin.value()),
            "manufacturer": self.manufacturer_edit.text().strip(), "model": self.model_edit.text().strip(),
            "notes": self.notes_edit.toPlainText().strip(), "auto_generated": bool(self.rack.get("auto_generated", False)),
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
        assets: Optional[Sequence[dict]] = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Network Connection")
        self.connection = deepcopy(connection or {})
        self.instances = list(instances or [])
        if assets is None and hasattr(parent, "data"):
            assets = getattr(parent, "data", {}).get("network_assets", [])
        self.assets = {_text(row.get("id")): row for row in (assets or []) if isinstance(row, dict)}
        self.instances_by_id = {_text(row.get("id")): row for row in self.instances if isinstance(row, dict)}
        self.vlans = list(vlans or [])
        self.result: Optional[dict] = None
        self.resize(650, 680)

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)
        self.id_edit = QLineEdit(_text(self.connection.get("id")) or suggested_id)
        self.from_combo = QComboBox(); self.to_combo = QComboBox()
        for instance in sorted(self.instances, key=lambda item: _text(item.get("id"))):
            instance_id = _text(instance.get("id")); label = f"{instance_id} - {_text(instance.get('name')) or instance_id}"
            self.from_combo.addItem(label, instance_id); self.to_combo.addItem(label, instance_id)
        for combo, value in ((self.from_combo, _text(self.connection.get("from_instance_id")) or default_from), (self.to_combo, _text(self.connection.get("to_instance_id")) or default_to)):
            index = combo.findData(value)
            if index >= 0: combo.setCurrentIndex(index)

        self.from_port_combo = QComboBox(); self.from_port_combo.setEditable(True)
        self.to_port_combo = QComboBox(); self.to_port_combo.setEditable(True)
        self.role_combo = QComboBox(); self.role_combo.addItems(CONNECTION_ROLES); _set_combo_text(self.role_combo, _text(self.connection.get("connection_role")) or "output")
        self.medium_combo = QComboBox(); self.medium_combo.addItems(CONNECTION_MEDIA[:-1]); _set_combo_text(self.medium_combo, _text(self.connection.get("medium")) or "copper")
        self.speed_combo = QComboBox()
        self.speed_status = QLabel(); self.speed_status.setWordWrap(True)
        self.cable_spec_edit = QLineEdit(_text(self.connection.get("cable_specification")))
        self.fibre_count_spin = QSpinBox(); self.fibre_count_spin.setRange(0, 100_000); self.fibre_count_spin.setValue(int(self.connection.get("fibre_count", 0) or 0))
        self.vlan_list = QListWidget(); self.vlan_list.setSelectionMode(QAbstractItemView.NoSelection)
        selected_vlans = {_text(value) for value in self.connection.get("vlan_ids", []) if _text(value)}
        for vlan in sorted(self.vlans, key=lambda item: int(item.get("vlan_id", 0) or 0)):
            row_id = _text(vlan.get("id")) or str(vlan.get("vlan_id", "")); item = QListWidgetItem(f"{vlan.get('vlan_id', '')} - {_text(vlan.get('name'))}")
            item.setData(Qt.UserRole, row_id); item.setFlags(item.flags() | Qt.ItemIsUserCheckable); item.setCheckState(Qt.Checked if row_id in selected_vlans else Qt.Unchecked); self.vlan_list.addItem(item)
        self.route_profile_combo = QComboBox(); self.route_profile_combo.addItem(""); self.route_profile_combo.addItems(sorted(set(route_profiles or []))); _set_combo_text(self.route_profile_combo, _text(self.connection.get("route_profile")))
        self.route_path_edit = QLineEdit(" -> ".join(str(value) for value in self.connection.get("route_path", []) if _text(value))); self.route_path_edit.setPlaceholderText("Optional graph node path, separated by ->")
        self.notes_edit = QTextEdit(_text(self.connection.get("notes"))); self.notes_edit.setMinimumHeight(90)

        for label, widget in [("Connection ID", self.id_edit), ("From asset", self.from_combo), ("From port", self.from_port_combo), ("To asset", self.to_combo), ("To port", self.to_port_combo), ("Connection role", self.role_combo), ("Medium", self.medium_combo), ("Link speed", self.speed_combo), ("Speed compatibility", self.speed_status), ("Cable specification", self.cable_spec_edit), ("Fibre count", self.fibre_count_spin), ("VLANs", self.vlan_list), ("Route profile", self.route_profile_combo), ("Route path", self.route_path_edit), ("Notes", self.notes_edit)]:
            form.addRow(label, widget)

        self.from_combo.currentIndexChanged.connect(self._populate_ports)
        self.to_combo.currentIndexChanged.connect(self._populate_ports)
        self.medium_combo.currentTextChanged.connect(self._populate_ports)
        self.from_port_combo.currentIndexChanged.connect(self._populate_speeds)
        self.to_port_combo.currentIndexChanged.connect(self._populate_speeds)
        self.from_port_combo.editTextChanged.connect(self._populate_speeds)
        self.to_port_combo.editTextChanged.connect(self._populate_speeds)
        self.medium_combo.currentTextChanged.connect(lambda value: self.fibre_count_spin.setEnabled(value == "fibre"))
        self._populate_ports()
        self.fibre_count_spin.setEnabled(self.medium_combo.currentText() == "fibre")
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel); buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)

    def _ports(self, instance_id: str) -> List[dict]:
        instance = self.instances_by_id.get(instance_id, {})
        return _expanded_instance_ports(instance, self.assets.get(_text(instance.get("asset_id")), {}))

    def _port_medium(self, row: dict) -> str:
        return "fibre" if _text(row.get("port_type")) in PLUGGABLE_OPTIC_PORT_TYPES | {"pon", "lc", "sc", "mpo"} else "copper"

    def _populate_one_port_combo(self, combo: QComboBox, instance_id: str, current: str) -> None:
        combo.blockSignals(True); combo.clear()
        medium = self.medium_combo.currentText().strip()
        for row in self._ports(instance_id):
            if medium in {"fibre", "copper"} and self._port_medium(row) != medium:
                continue
            speeds = normalise_port_speeds(row.get("speeds"))
            suffix = f" · {', '.join(port_speed_label(value) for value in speeds)}" if speeds else ""
            combo.addItem(f"{_text(row.get('name'))} ({_text(row.get('port_type'))}/{_text(row.get('port_use'))}){suffix}", row)
        index = next((i for i in range(combo.count()) if _text((combo.itemData(i) or {}).get("name")) == current), -1)
        if index >= 0: combo.setCurrentIndex(index)
        elif current: combo.setEditText(current)
        combo.blockSignals(False)

    def _populate_ports(self, *_args) -> None:
        self._populate_one_port_combo(self.from_port_combo, _text(self.from_combo.currentData()), _text(self.connection.get("from_port")) if self.from_port_combo.count() == 0 else self.from_port_combo.currentText().split(" (")[0])
        self._populate_one_port_combo(self.to_port_combo, _text(self.to_combo.currentData()), _text(self.connection.get("to_port")) if self.to_port_combo.count() == 0 else self.to_port_combo.currentText().split(" (")[0])
        self._populate_speeds()

    def _selected_port(self, combo: QComboBox) -> Tuple[str, dict]:
        row = combo.currentData() if isinstance(combo.currentData(), dict) else {}
        return (_text(row.get("name")) or combo.currentText().split(" (")[0].strip(), row)

    def _populate_speeds(self, *_args) -> None:
        current = int(self.connection.get("link_speed_mbps", 0) or 0) if self.speed_combo.count() == 0 else int(self.speed_combo.currentData() or 0)
        _from_name, left = self._selected_port(self.from_port_combo); _to_name, right = self._selected_port(self.to_port_combo)
        left_speeds = normalise_port_speeds(left.get("speeds")); right_speeds = normalise_port_speeds(right.get("speeds")); speeds = compatible_port_speeds(left_speeds, right_speeds)
        self.speed_combo.blockSignals(True); self.speed_combo.clear()
        if not speeds:
            self.speed_combo.addItem("Not declared / transparent", 0)
        else:
            for speed in speeds: self.speed_combo.addItem(port_speed_label(speed), speed)
        index = self.speed_combo.findData(current)
        if index >= 0: self.speed_combo.setCurrentIndex(index)
        self.speed_combo.blockSignals(False)
        incompatible = bool(left_speeds and right_speeds and not set(left_speeds) & set(right_speeds))
        self.speed_status.setText("No common supported speed. Select different ports." if incompatible else ("Common rates: " + ", ".join(port_speed_label(value) for value in speeds) if speeds else "One or both ports are speed-transparent or not declared."))
        self.speed_status.setStyleSheet("color:#d65c5c" if incompatible else "")

    def _selected_vlans(self) -> List[str]:
        return [_text(self.vlan_list.item(index).data(Qt.UserRole)) for index in range(self.vlan_list.count()) if self.vlan_list.item(index).checkState() == Qt.Checked]

    def accept(self) -> None:
        connection_id = self.id_edit.text().strip(); from_id = _text(self.from_combo.currentData()); to_id = _text(self.to_combo.currentData())
        from_port, from_row = self._selected_port(self.from_port_combo); to_port, to_row = self._selected_port(self.to_port_combo)
        if not connection_id:
            QMessageBox.critical(self, "Invalid connection", "Connection ID is required."); return
        if not from_id or not to_id or from_id == to_id:
            QMessageBox.critical(self, "Invalid connection", "Select two different connection endpoints."); return
        if not from_port or not to_port:
            QMessageBox.critical(self, "Invalid connection", "Both port identifiers are required."); return
        speed = int(self.speed_combo.currentData() or 0)
        left = normalise_port_speeds(from_row.get("speeds")); right = normalise_port_speeds(to_row.get("speeds"))
        if left and right and speed not in set(left) & set(right):
            QMessageBox.critical(self, "Incompatible port speeds", "The selected ports do not share the selected line rate."); return
        self.result = {
            **self.connection, "id": connection_id, "from_instance_id": from_id, "from_port": from_port, "to_instance_id": to_id, "to_port": to_port,
            "connection_role": self.role_combo.currentText().strip(), "medium": self.medium_combo.currentText().strip(), "link_speed_mbps": speed,
            "cable_specification": self.cable_spec_edit.text().strip(), "fibre_count": int(self.fibre_count_spin.value()) if self.medium_combo.currentText() == "fibre" else 0,
            "vlan_ids": self._selected_vlans(), "route_profile": self.route_profile_combo.currentText().strip(),
            "route_path": [part.strip() for part in self.route_path_edit.text().split("->") if part.strip()], "notes": self.notes_edit.toPlainText().strip(),
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
        self.requested_hosts_spin = QSpinBox()
        self.requested_hosts_spin.setRange(2, 16_777_214)
        self.requested_hosts_spin.setValue(max(2, int(self.vlan.get("requested_hosts", 254) or 254)))
        self.subnet_edit = QLineEdit(_text(self.vlan.get("subnet")))
        self.subnet_mask_edit = QLineEdit(_text(self.vlan.get("subnet_mask")))
        self.subnet_mask_edit.setReadOnly(True)
        self.gateway_edit = QLineEdit(_text(self.vlan.get("gateway")))
        self.dhcp_scope_edit = QLineEdit(_text(self.vlan.get("dhcp_scope")))
        self.security_zone_edit = QLineEdit(_text(self.vlan.get("security_zone")))
        self.notes_edit = QTextEdit(_text(self.vlan.get("notes")))
        for label, widget in [
            ("Record ID", self.id_edit),
            ("VLAN ID", self.vlan_id_spin),
            ("Name", self.name_edit),
            ("Purpose", self.purpose_edit),
            ("Requested hosts", self.requested_hosts_spin),
            ("Subnet", self.subnet_edit),
            ("Subnet mask", self.subnet_mask_edit),
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
            "requested_hosts": int(self.requested_hosts_spin.value()),
            "subnet": self.subnet_edit.text().strip(),
            "subnet_mask": self.subnet_mask_edit.text().strip(),
            "prefix_length": int(self.vlan.get("prefix_length", 0) or 0),
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


_PLANNER_MUTABLE_KEYS = (
    "network_settings",
    "network_assets",
    "network_asset_instances",
    "network_racks",
    "network_connections",
    "network_endpoint_assignments",
    "network_patch_leads",
    "network_redundancy_groups",
    "network_power_connections",
    "network_vlans",
    "network_routes",
    "network_ip_allocations",
    "network_external_networks",
    "network_optic_modules",
    "network_optical_paths",
    "network_fibre_cable_types",
    "network_fibre_cables",
    "network_fibre_nodes",
    "network_fibre_splices",
    "network_design_summary",
    "locations",
    "assets",
)


def _planner_working_copy(data: dict) -> dict:
    """Copy only sections the planner can edit.

    The corridor graph, data points, room definitions and drawing metadata are
    read-only while this dialog is open and can safely be shared. Copying the
    complete 90 MB project was a major part of the previous startup delay.
    """

    result = dict(data)
    mutable = {key: data.get(key) for key in _PLANNER_MUTABLE_KEYS if key in data}
    # Pickle performs one graph traversal and is materially faster than a long
    # series of recursive ``deepcopy`` calls for the large network collections.
    result.update(pickle.loads(pickle.dumps(mutable, protocol=pickle.HIGHEST_PROTOCOL)))
    return result


class PlanningResolutionDialog(QDialog):
    """Show a failed planner constraint and apply a selected retry override."""

    def __init__(self, parent, error: NetworkPlanningError, data: dict):
        super().__init__(parent)
        self.setWindowTitle("Resolve automatic planning issue")
        self.resize(760, 560)
        self.error = error
        self.data = data
        self.details = dict(getattr(error, "details", {}) or {})
        self.selected_action = ""

        layout = QVBoxLayout(self)
        heading = QLabel("The automatic planner could not satisfy a network constraint.")
        heading.setWordWrap(True)
        layout.addWidget(heading)

        message = QLabel(str(error))
        message.setWordWrap(True)
        layout.addWidget(message)

        diagnostics = QTextEdit()
        diagnostics.setReadOnly(True)
        diagnostics.setMaximumHeight(190)
        diagnostics.setPlainText(self._diagnostic_text())
        layout.addWidget(diagnostics)

        form = QFormLayout()
        self.action_combo = QComboBox()
        self._populate_actions()
        self.action_combo.currentIndexChanged.connect(self._action_changed)
        form.addRow("Resolution", self.action_combo)

        self.asset_combo = QComboBox()
        form.addRow("Selected asset", self.asset_combo)

        self.minimum_switch_count_spin = QSpinBox()
        self.minimum_switch_count_spin.setRange(1, 9999)
        current_switches = max(
            0, int(self.details.get("current_location_switch_count", 0) or 0)
        )
        suggested_switches = max(
            0, int(self.details.get("suggested_device_count", 0) or 0)
        )
        self.minimum_switch_count_spin.setValue(
            max(1, current_switches + 1, suggested_switches)
        )
        self.minimum_switch_count_spin.setToolTip(
            "Minimum number of access switches the retry must install at the "
            "affected location. Increase or reduce this value to override the "
            "planner recommendation."
        )
        form.addRow("Minimum access switches", self.minimum_switch_count_spin)
        layout.addLayout(form)

        note = QLabel(
            "The selected override is saved in Network Settings and the planner is "
            "run again. It can be cleared later from the planner settings."
        )
        note.setWordWrap(True)
        layout.addWidget(note)

        buttons = QDialogButtonBox(QDialogButtonBox.Apply | QDialogButtonBox.Cancel)
        apply_button = buttons.button(QDialogButtonBox.Apply)
        apply_button.setText("Apply and retry")
        apply_button.clicked.connect(self._accept_resolution)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self._action_changed()

    def _diagnostic_text(self) -> str:
        lines = []
        required = float(self.details.get("required_bandwidth_mbps", 0.0) or 0.0)
        maximum = float(
            self.details.get("max_compatible_capacity_mbps", 0.0) or 0.0
        )
        shortfall = float(self.details.get("shortfall_mbps", 0.0) or 0.0)
        if required > 0.0:
            lines.append(f"Required traffic: {required:.3f} Mbps")
        if maximum > 0.0:
            lines.append(f"Best compatible port bundle: {maximum:.3f} Mbps")
        if shortfall > 0.0:
            lines.append(f"Capacity shortfall: {shortfall:.3f} Mbps")
        if self.details.get("location_name"):
            lines.append(f"Location: {self.details.get('location_name')}")
        if self.details.get("from_name"):
            lines.append(f"Downstream device: {self.details.get('from_name')}")
        if self.details.get("to_name"):
            lines.append(f"Upstream device: {self.details.get('to_name')}")

        actual_ports = int(self.details.get("actual_port_count", 0) or 0)
        required_ports = int(
            self.details.get("required_port_count_with_spare", 0) or 0
        )
        available_ports = int(
            self.details.get("available_port_count", 0) or 0
        )
        spare_percent = float(
            self.details.get("spare_capacity_percent", 0.0) or 0.0
        )
        if actual_ports > 0:
            lines.append(f"Current port demand: {actual_ports}")
        if required_ports > 0:
            lines.append(
                f"Ports required with {spare_percent:.1f}% spare: {required_ports}"
            )
        if available_ports > 0:
            lines.append(f"Installed port capacity: {available_ports}")
        if self.details.get("switches_without_spare") is not None:
            lines.append(
                "Switches without spare: "
                f"{int(self.details.get('switches_without_spare', 0) or 0)}"
            )
        if self.details.get("switches_with_spare") is not None:
            lines.append(
                "Switches with spare: "
                f"{int(self.details.get('switches_with_spare', 0) or 0)}"
            )
        if self.details.get("stacks_without_spare") is not None:
            lines.append(
                "Logical stacks without spare: "
                f"{int(self.details.get('stacks_without_spare', 0) or 0)}"
            )
        if self.details.get("stacks_with_spare") is not None:
            lines.append(
                "Logical stacks with spare: "
                f"{int(self.details.get('stacks_with_spare', 0) or 0)}"
            )
        if self.details.get("max_stack_members") is not None:
            lines.append(
                "Maximum members per stack: "
                f"{int(self.details.get('max_stack_members', 0) or 0)}"
            )

        capabilities = self.details.get("capabilities", []) or []
        if capabilities:
            lines.extend(["", "Compatible same-speed bundles currently available:"])
            for row in capabilities:
                speed = int(row.get("speed_mbps", 0) or 0)
                members = int(row.get("max_members", 0) or 0)
                capacity = float(row.get("capacity_mbps", 0.0) or 0.0)
                lines.append(
                    f"  {members} × {port_speed_label(speed)} = {capacity:.3f} Mbps"
                )

        access = self.details.get("access_alternatives", []) or []
        upstream = self.details.get("upstream_alternatives", []) or []
        if access:
            lines.extend(["", "Access-switch alternatives:"])
            for row in access[:12]:
                status = "meets demand" if row.get("meets_required") else "insufficient alone"
                lines.append(
                    f"  {row.get('name')}: {float(row.get('capacity_mbps', 0)):.3f} Mbps ({status})"
                )
        if upstream:
            lines.extend(["", "Upstream-switch alternatives:"])
            for row in upstream[:12]:
                status = "meets demand" if row.get("meets_required") else "insufficient alone"
                lines.append(
                    f"  {row.get('name')}: {float(row.get('capacity_mbps', 0)):.3f} Mbps ({status})"
                )
        if not lines:
            lines.append(
                "No structured capacity data was available. Review the asset library, "
                "port definitions and planner settings."
            )
        return "\n".join(lines)

    def _populate_actions(self) -> None:
        location = _text(self.details.get("location_name"))
        from_role = _text(self.details.get("from_role")).lower()
        error_code = getattr(self.error, "code", "")
        if error_code == "access_stack_spare_capacity" and location:
            self.action_combo.addItem(
                "Install another rack and create an overflow stack for spare capacity (recommended)",
                "add_spare_capacity_rack",
            )
            self.action_combo.addItem(
                "Defer spare access-port capacity at this location",
                "defer_location_spare_capacity",
            )
        if location and error_code != "access_stack_spare_capacity" and (
            from_role in {"access", "access_switch"}
            or error_code == "access_uplink_capacity"
        ):
            self.action_combo.addItem(
                "Add an access switch and redistribute traffic (recommended)",
                "add_access_switch",
            )
        if self.details.get("access_alternatives"):
            self.action_combo.addItem(
                "Use a selected access-switch model at this location",
                "select_access_asset",
            )
        if self.details.get("upstream_alternatives"):
            self.action_combo.addItem(
                "Use a selected upstream core/aggregation switch model",
                "select_upstream_asset",
            )
        if getattr(self.error, "code", "") in {
            "link_capacity",
            "access_uplink_capacity",
        }:
            self.action_combo.addItem(
                "Create the best available link and ignore the bandwidth shortfall",
                "ignore_bandwidth",
            )
            self.action_combo.addItem(
                "Retry without spare traffic capacity", "remove_spare_capacity"
            )
        self.action_combo.addItem(
            "Open the asset library and review the design manually", "review_assets"
        )

    def _action_changed(self, *_args) -> None:
        action = _text(self.action_combo.currentData())
        self.asset_combo.clear()
        alternatives = []
        if action == "select_access_asset":
            alternatives = self.details.get("access_alternatives", []) or []
        elif action == "select_upstream_asset":
            alternatives = self.details.get("upstream_alternatives", []) or []
        for row in alternatives:
            capacity = float(row.get("capacity_mbps", 0.0) or 0.0)
            status = "meets demand" if row.get("meets_required") else "requires multiple devices"
            self.asset_combo.addItem(
                f"{row.get('name')} — {capacity:.3f} Mbps ({status})",
                _text(row.get("asset_id")),
            )
        self.asset_combo.setEnabled(bool(alternatives))
        self.minimum_switch_count_spin.setEnabled(action == "add_access_switch")

    def _accept_resolution(self) -> None:
        action = _text(self.action_combo.currentData())
        settings = self.data.setdefault("network_settings", {})
        overrides = settings.setdefault("auto_planner_resolution_overrides", {})
        if not isinstance(overrides, dict):
            overrides = {}
            settings["auto_planner_resolution_overrides"] = overrides
        access_assets = overrides.setdefault("access_asset_by_location", {})
        upstream_assets = overrides.setdefault("upstream_asset_by_layer", {})
        minimum_switches = overrides.setdefault(
            "minimum_access_switches_by_location", {}
        )
        spare_modes = overrides.setdefault(
            "spare_capacity_mode_by_location", {}
        )

        location = _text(self.details.get("location_name"))
        if action == "add_spare_capacity_rack":
            if not location:
                return
            spare_modes[location] = "new_rack"
        elif action == "defer_location_spare_capacity":
            if not location:
                return
            spare_modes[location] = "defer"
        elif action == "add_access_switch":
            requested = max(1, int(self.minimum_switch_count_spin.value()))
            minimum_switches[location] = max(
                int(minimum_switches.get(location, 0) or 0),
                requested,
            )
            settings["auto_add_switches_for_bandwidth"] = True
        elif action == "select_access_asset":
            asset_id = _text(self.asset_combo.currentData())
            if not asset_id or not location:
                return
            access_assets[location] = asset_id
        elif action == "select_upstream_asset":
            asset_id = _text(self.asset_combo.currentData())
            if not asset_id:
                return
            to_role = _text(self.details.get("to_role")).lower()
            layer = "aggregation" if "aggregation" in to_role else "core"
            upstream_assets[layer] = asset_id
        elif action == "ignore_bandwidth":
            settings["ignore_link_bandwidth_errors"] = True
        elif action == "remove_spare_capacity":
            settings["spare_capacity_percent"] = 0.0
        elif action == "review_assets":
            self.selected_action = action
            self.accept()
            return
        else:
            return

        self.selected_action = action
        self.accept()


class NetworkPlannerDialog(QDialog):
    def __init__(self, parent, data: dict, on_save: Callable[[dict], None]):
        super().__init__(parent)
        self.setWindowTitle("Network Planning")
        self.resize(1250, 780)
        self.data = _planner_working_copy(data)
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
        self.redundant_core_check = QCheckBox("Enable redundant layer design")
        self.redundant_core_check.setChecked(
            bool(self.data.get("network_settings", {}).get("redundant_core", True))
        )

        settings = self.data.get("network_settings", {})
        self.topology_model_combo = QComboBox()
        self.topology_model_combo.addItem("Collapsed core (core + access)", "collapsed_core")
        self.topology_model_combo.addItem("Three tier (core + aggregation + access)", "three_tier")
        topology_index = self.topology_model_combo.findData(
            _text(settings.get("topology_model")) or "collapsed_core"
        )
        if topology_index >= 0:
            self.topology_model_combo.setCurrentIndex(topology_index)

        self.independent_link_count_spin = QSpinBox()
        self.independent_link_count_spin.setRange(1, 16)
        self.independent_link_count_spin.setValue(
            max(1, int(settings.get("independent_link_count", 2) or 2))
        )
        self.independent_link_count_spin.setToolTip(
            "Default number of separate uplinks generated for each downstream device. "
            "Redundant designs require at least two distinct source devices and two different core paths."
        )
        self.spare_capacity_spin = QDoubleSpinBox()
        self.spare_capacity_spin.setRange(0.0, 100.0)
        self.spare_capacity_spin.setDecimals(1)
        self.spare_capacity_spin.setSuffix(" %")
        self.spare_capacity_spin.setValue(
            float(settings.get("spare_capacity_percent", 15.0) or 0.0)
        )

        self.default_expected_bandwidth_spin = QDoubleSpinBox()
        self.default_expected_bandwidth_spin.setRange(0.0, 1_000_000_000.0)
        self.default_expected_bandwidth_spin.setDecimals(3)
        self.default_expected_bandwidth_spin.setSuffix(" Mbps")
        self.default_expected_bandwidth_spin.setValue(float(settings.get("default_expected_bandwidth_mbps", 0.0) or 0.0))
        self.default_expected_bandwidth_spin.setToolTip("Traffic applied to a generic network port when its endpoint asset has no explicit expected bandwidth.")

        self.default_expected_packet_rate_spin = QDoubleSpinBox()
        self.default_expected_packet_rate_spin.setRange(0.0, 1_000_000_000_000.0)
        self.default_expected_packet_rate_spin.setDecimals(1)
        self.default_expected_packet_rate_spin.setSuffix(" pps")
        self.default_expected_packet_rate_spin.setValue(float(settings.get("default_expected_packet_rate_pps", 0.0) or 0.0))
        self.default_expected_packet_rate_spin.setToolTip("Packet rate applied to a generic network port when its endpoint asset has no explicit value.")

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

        self.default_fibre_core_count_spin = QSpinBox()
        self.default_fibre_core_count_spin.setRange(2, 6912)
        self.default_fibre_core_count_spin.setValue(
            int(settings.get("default_fibre_core_count", 12) or 12)
        )
        self.default_fibre_core_count_spin.setToolTip(
            "Default sheath core count used when logical fibre links are converted into physical routed cables."
        )

        self.ip_plan_base_edit = QLineEdit(
            _text(settings.get("ip_plan_base_cidr")) or "10.0.0.0/8"
        )
        self.ip_plan_base_edit.setToolTip(
            "IPv4 supernet used by the automatic VLAN, gateway and management-address generator."
        )

        self.sync_physical_fibre_button = QPushButton("Synchronise Physical Fibre Layer")
        self.sync_physical_fibre_button.setToolTip(
            "Create or refresh routed physical fibre cables, terminations and cassettes from logical fibre connections."
        )
        self.sync_physical_fibre_button.clicked.connect(self.sync_physical_fibre_layer)

        self.generate_ip_plan_button = QPushButton("Generate IP Address Plan")
        self.generate_ip_plan_button.setToolTip(
            "Allocate VLAN subnets, masks, gateways, router interfaces and device management addresses."
        )
        self.generate_ip_plan_button.clicked.connect(self.generate_ip_plan)

        self.olt_failover_check = QCheckBox(
            "Provide standby OLT failover for each protected splitter"
        )
        self.olt_failover_check.setChecked(
            bool(settings.get("polan_olt_failover", True))
        )

        self.auto_connect_manual_check = QCheckBox(
            "Auto-connect newly placed manual devices"
        )
        self.auto_connect_manual_check.setChecked(
            bool(settings.get("auto_connect_new_manual_devices", True))
        )
        self.auto_connect_manual_check.setToolTip(
            "Use the configured layer rules, nearest routed upstream device and "
            "compatible free ports after a device is placed manually."
        )

        self.auto_add_bandwidth_switches_check = QCheckBox(
            "Automatically add access switches when uplink bandwidth is exceeded"
        )
        self.auto_add_bandwidth_switches_check.setChecked(
            bool(settings.get("auto_add_switches_for_bandwidth", True))
        )
        self.auto_add_bandwidth_switches_check.setToolTip(
            "Split endpoint traffic across additional access switches when one "
            "switch cannot form a sufficiently large same-speed uplink bundle."
        )

        self.ignore_link_bandwidth_check = QCheckBox(
            "Allow best-effort links that do not meet calculated bandwidth"
        )
        self.ignore_link_bandwidth_check.setChecked(
            bool(settings.get("ignore_link_bandwidth_errors", False))
        )
        self.ignore_link_bandwidth_check.setToolTip(
            "Use only as a manual override. The generated design records a warning "
            "for every undersized link."
        )

        self.clear_planner_overrides_button = QPushButton(
            "Clear automatic planner resolution overrides"
        )
        self.clear_planner_overrides_button.setToolTip(
            "Remove manually selected switch models and minimum switch counts "
            "created by the planning-resolution dialog."
        )
        self.clear_planner_overrides_button.clicked.connect(
            self.clear_planner_resolution_overrides
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
        settings_layout.addRow("Traditional topology", self.topology_model_combo)
        settings_layout.addRow(
            "Independent links per target", self.independent_link_count_spin
        )
        settings_layout.addRow("Spare port, PoE and traffic capacity", self.spare_capacity_spin)
        settings_layout.addRow("Default endpoint bandwidth", self.default_expected_bandwidth_spin)
        settings_layout.addRow("Default endpoint packet rate", self.default_expected_packet_rate_spin)
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
        settings_layout.addRow("Default physical fibre core count", self.default_fibre_core_count_spin)
        settings_layout.addRow("IP planning base CIDR", self.ip_plan_base_edit)
        settings_layout.addRow("", self.redundant_core_check)
        settings_layout.addRow("", self.olt_failover_check)
        settings_layout.addRow("", self.auto_connect_manual_check)
        settings_layout.addRow("", self.auto_add_bandwidth_switches_check)
        settings_layout.addRow("", self.ignore_link_bandwidth_check)
        settings_layout.addRow("", self.clear_planner_overrides_button)
        settings_layout.addRow("", self.auto_design_button)
        settings_layout.addRow("", self.sync_physical_fibre_button)
        settings_layout.addRow("", self.generate_ip_plan_button)
        settings_layout.addRow("", self.clear_installed_button)
        settings_layout.addRow("", self.visual_edit_button)
        settings_layout.addRow("", info)
        self.tabs.addTab(settings_tab, "Settings")

        layer_rules_tab = QWidget()
        layer_rules_layout = QVBoxLayout(layer_rules_tab)
        layer_rules_info = QLabel(
            "Define how active network layers are connected. A peer rule links devices "
            "within the same layer; an uplink rule defines the number of links entering "
            "each downstream device. Minimum distinct sources prevents redundant links "
            "from terminating on the same upstream device. Changing the topology, "
            "redundancy or independent-link controls reloads the matching default rules; "
            "the table can then be edited for project-specific requirements."
        )
        layer_rules_info.setWordWrap(True)
        layer_rules_layout.addWidget(layer_rules_info)

        profile_buttons = QHBoxLayout()
        self.load_profile_rules_button = QPushButton("Load selected topology defaults")
        self.load_profile_rules_button.clicked.connect(self._load_layer_profile_defaults)
        self.add_layer_rule_button = QPushButton("Add rule")
        self.add_layer_rule_button.clicked.connect(lambda: self._append_layer_rule_row({}))
        self.remove_layer_rule_button = QPushButton("Remove selected rule")
        self.remove_layer_rule_button.clicked.connect(self._remove_selected_layer_rules)
        profile_buttons.addWidget(self.load_profile_rules_button)
        profile_buttons.addWidget(self.add_layer_rule_button)
        profile_buttons.addWidget(self.remove_layer_rule_button)
        profile_buttons.addStretch(1)
        layer_rules_layout.addLayout(profile_buttons)

        self.layer_rules_table = QTableWidget(0, 4)
        self.layer_rules_table.setHorizontalHeaderLabels(
            ["Enabled", "Layer connection", "Links", "Minimum distinct sources"]
        )
        self.layer_rules_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents
        )
        self.layer_rules_table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.Stretch
        )
        self.layer_rules_table.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeToContents
        )
        self.layer_rules_table.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeToContents
        )
        self.layer_rules_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.layer_rules_table.setMinimumHeight(260)
        layer_rules_layout.addWidget(self.layer_rules_table, 1)

        initial_rules = normalise_layer_connection_rules(
            settings.get("layer_connection_rules"),
            _text(settings.get("topology_model")) or "collapsed_core",
            bool(settings.get("redundant_core", True)),
            int(settings.get("independent_link_count", 2) or 2),
        )
        for rule in initial_rules:
            self._append_layer_rule_row(rule)
        self.tabs.addTab(layer_rules_tab, "Layer Rules")

        manufacturer_tab = QWidget()
        manufacturer_layout = QVBoxLayout(manufacturer_tab)
        manufacturer_info = QLabel(
            "Set an ordered manufacturer preference for each generated component. "
            "Non-strict preferences rank matching products first but allow another manufacturer when needed for capacity. "
            "Strict preferences prevent the planner from selecting an unlisted manufacturer."
        )
        manufacturer_info.setWordWrap(True)
        manufacturer_layout.addWidget(manufacturer_info)
        self.manufacturer_preferences_table = QTableWidget(0, 3)
        self.manufacturer_preferences_table.setHorizontalHeaderLabels(
            ["Component", "Preferred manufacturers (ordered, comma separated)", "Strict"]
        )
        self.manufacturer_preferences_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.manufacturer_preferences_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.manufacturer_preferences_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        preferences = normalise_manufacturer_preferences(settings.get("manufacturer_preferences"))
        for component, label in MANUFACTURER_PREFERENCE_COMPONENTS.items():
            row = self.manufacturer_preferences_table.rowCount()
            self.manufacturer_preferences_table.insertRow(row)
            component_item = QTableWidgetItem(label)
            component_item.setData(Qt.UserRole, component)
            component_item.setFlags(component_item.flags() & ~Qt.ItemIsEditable)
            self.manufacturer_preferences_table.setItem(row, 0, component_item)
            self.manufacturer_preferences_table.setItem(row, 1, QTableWidgetItem(", ".join(preferences[component]["preferred_manufacturers"])))
            strict_check = QCheckBox(); strict_check.setChecked(bool(preferences[component]["strict"])); strict_check.setToolTip("Require one of the preferred manufacturers")
            wrapper = QWidget(); wrapper_layout = QHBoxLayout(wrapper); wrapper_layout.setContentsMargins(0, 0, 0, 0); wrapper_layout.addWidget(strict_check, 0, Qt.AlignCenter)
            wrapper._strict_check = strict_check
            self.manufacturer_preferences_table.setCellWidget(row, 2, wrapper)
        manufacturer_layout.addWidget(self.manufacturer_preferences_table, 1)
        self.tabs.addTab(manufacturer_tab, "Manufacturer Preferences")

        self.topology_model_combo.currentIndexChanged.connect(
            self._load_layer_profile_defaults
        )
        self.redundant_core_check.toggled.connect(
            self._load_layer_profile_defaults
        )
        self.independent_link_count_spin.valueChanged.connect(
            self._load_layer_profile_defaults
        )
        self.technology_combo.currentTextChanged.connect(
            self._update_layer_rule_availability
        )
        self._update_layer_rule_availability()

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
                "Bandwidth Gbps",
                "Packet Mpps",
                "Expected Mbps",
                "Expected pps",
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
        self.patch_leads_tab = _CrudTab(
            ["ID", "Device", "Port", "Peer", "Peer port", "Endpoint", "Medium", "Length m", "Logical link"]
        )
        self.fibre_nodes_tab = _CrudTab(
            ["ID", "Name", "Type", "Location", "Floor", "Rack", "Parent", "Capacity", "Drawing layer"]
        )
        self.fibre_cables_tab = _CrudTab(
            ["ID", "Name", "From", "To", "Type", "Cores", "Used", "Dark", "Length m", "Route", "Layer"]
        )
        self.fibre_splices_tab = _CrudTab(
            ["ID", "Node", "Cassette", "Incoming cable", "Core", "Outgoing cable", "Core", "Type", "Circuit", "Loss dB"]
        )
        self.external_networks_tab = _CrudTab(
            ["ID", "Name", "Type", "Provider", "ASN", "Location", "Demarcation", "Prefixes", "Peers"]
        )
        self.ip_allocations_tab = _CrudTab(
            ["ID", "Instance", "VLAN", "Address", "Prefix", "Gateway", "Purpose"]
        )
        self.ip_allocations_tab.add_button.setText("Generate plan")
        self.ip_allocations_tab.edit_button.setEnabled(False)
        self.ip_allocations_tab.delete_button.setText("Clear generated")

        self.tabs.addTab(self.assets_tab, "Asset Library")

        self.endpoint_traffic_tab = QWidget()
        endpoint_traffic_layout = QVBoxLayout(self.endpoint_traffic_tab)
        endpoint_traffic_info = QLabel(
            "Expected traffic is configured against the project endpoint asset library. "
            "For assets with multiple data ports, the auto-planner apportions the total demand across those ports."
        )
        endpoint_traffic_info.setWordWrap(True)
        endpoint_traffic_layout.addWidget(endpoint_traffic_info)
        self.endpoint_traffic_table = QTableWidget(0, 5)
        self.endpoint_traffic_table.setHorizontalHeaderLabels(
            ["Asset ID", "Asset name", "Data ports", "Expected bandwidth Mbps", "Expected packet rate pps"]
        )
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.itemChanged.connect(self._endpoint_traffic_changed)
        endpoint_traffic_layout.addWidget(self.endpoint_traffic_table, 1)
        self.tabs.addTab(self.endpoint_traffic_tab, "Endpoint Traffic")

        self.tabs.addTab(self.instances_tab, "Installed Assets")
        self.tabs.addTab(self.connections_tab, "Connections")
        self.tabs.addTab(self.patch_leads_tab, "Patch Cables")
        self.tabs.addTab(self.fibre_nodes_tab, "Fibre Nodes")
        self.tabs.addTab(self.fibre_cables_tab, "Fibre Cables")
        self.tabs.addTab(self.fibre_splices_tab, "Fibre Splices")
        self.tabs.addTab(self.external_networks_tab, "External Networks")
        self.tabs.addTab(self.vlans_tab, "VLANs")
        self.tabs.addTab(self.routes_tab, "Routing")
        self.tabs.addTab(self.ip_allocations_tab, "IP Addresses")

        self.summary_text = QTextEdit()
        self.summary_text.setReadOnly(True)
        self.tabs.addTab(self.summary_text, "Generated Design")

        self._data_tabs = {
            self.assets_tab: "assets",
            self.endpoint_traffic_tab: "endpoint_traffic",
            self.instances_tab: "instances",
            self.connections_tab: "connections",
            self.patch_leads_tab: "patch_leads",
            self.fibre_nodes_tab: "fibre_nodes",
            self.fibre_cables_tab: "fibre_cables",
            self.fibre_splices_tab: "fibre_splices",
            self.external_networks_tab: "external_networks",
            self.vlans_tab: "vlans",
            self.routes_tab: "routes",
            self.ip_allocations_tab: "ip_allocations",
            self.summary_text: "summary",
        }
        self._dirty_tabs = set(self._data_tabs)
        self.tabs.currentChanged.connect(self._refresh_current_tab)

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

        self.patch_leads_tab.add_button.clicked.connect(self.add_patch_lead)
        self.patch_leads_tab.edit_button.clicked.connect(self.edit_patch_lead)
        self.patch_leads_tab.delete_button.clicked.connect(self.delete_patch_lead)
        self.patch_leads_tab.edit_requested = self.edit_patch_lead
        self.fibre_nodes_tab.add_button.clicked.connect(self.add_fibre_node)
        self.fibre_nodes_tab.edit_button.clicked.connect(self.edit_fibre_node)
        self.fibre_nodes_tab.delete_button.clicked.connect(self.delete_fibre_node)
        self.fibre_nodes_tab.edit_requested = self.edit_fibre_node
        self.fibre_cables_tab.add_button.clicked.connect(self.add_fibre_cable)
        self.fibre_cables_tab.edit_button.clicked.connect(self.edit_fibre_cable)
        self.fibre_cables_tab.delete_button.clicked.connect(self.delete_fibre_cable)
        self.fibre_cables_tab.edit_requested = self.edit_fibre_cable
        self.fibre_splices_tab.add_button.clicked.connect(self.add_fibre_splice)
        self.fibre_splices_tab.edit_button.clicked.connect(self.edit_fibre_splice)
        self.fibre_splices_tab.delete_button.clicked.connect(self.delete_fibre_splice)
        self.fibre_splices_tab.edit_requested = self.edit_fibre_splice
        self.external_networks_tab.add_button.clicked.connect(self.add_external_network)
        self.external_networks_tab.edit_button.clicked.connect(self.edit_external_network)
        self.external_networks_tab.delete_button.clicked.connect(self.delete_external_network)
        self.external_networks_tab.edit_requested = self.edit_external_network
        self.ip_allocations_tab.add_button.clicked.connect(self.generate_ip_plan)
        self.ip_allocations_tab.delete_button.clicked.connect(self.clear_ip_plan)

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
            for lead in self._items("network_patch_leads"):
                if _text(lead.get("instance_id")) == old_id:
                    lead["instance_id"] = new_id
                if _text(lead.get("peer_instance_id")) == old_id:
                    lead["peer_instance_id"] = new_id
            for cable in self._items("network_fibre_cables"):
                if _text(cable.get("from_instance_id")) == old_id:
                    cable["from_instance_id"] = new_id
                if _text(cable.get("to_instance_id")) == old_id:
                    cable["to_instance_id"] = new_id
            for node in self._items("network_fibre_nodes"):
                if _text(node.get("linked_instance_id")) == old_id:
                    node["linked_instance_id"] = new_id
            for allocation in self._items("network_ip_allocations"):
                if _text(allocation.get("instance_id")) == old_id:
                    allocation["instance_id"] = new_id
            for external in self._items("network_external_networks"):
                if _text(external.get("demarcation_instance_id")) == old_id:
                    external["demarcation_instance_id"] = new_id
                external["peer_instance_ids"] = [new_id if _text(v) == old_id else v for v in external.get("peer_instance_ids", [])]
        elif key == "network_connections":
            for lead in self._items("network_patch_leads"):
                if _text(lead.get("connection_id")) == old_id:
                    lead["connection_id"] = new_id
            for cable in self._items("network_fibre_cables"):
                cable["logical_connection_ids"] = [new_id if _text(v) == old_id else v for v in cable.get("logical_connection_ids", [])]
            for splice in self._items("network_fibre_splices"):
                if _text(splice.get("circuit_id")) == old_id:
                    splice["circuit_id"] = new_id
        elif key == "network_fibre_cables":
            for splice in self._items("network_fibre_splices"):
                if _text(splice.get("incoming_cable_id")) == old_id:
                    splice["incoming_cable_id"] = new_id
                if _text(splice.get("outgoing_cable_id")) == old_id:
                    splice["outgoing_cable_id"] = new_id
        elif key == "network_fibre_nodes":
            for node in self._items("network_fibre_nodes"):
                if _text(node.get("parent_node_id")) == old_id:
                    node["parent_node_id"] = new_id
            for splice in self._items("network_fibre_splices"):
                if _text(splice.get("node_id")) == old_id:
                    splice["node_id"] = new_id
                if _text(splice.get("cassette_id")) == old_id:
                    splice["cassette_id"] = new_id
        elif key == "network_vlans":
            for connection in self._items("network_connections"):
                connection["vlan_ids"] = [
                    new_id if _text(value) == old_id else value
                    for value in connection.get("vlan_ids", [])
                ]
            for route in self._items("network_routes"):
                if _text(route.get("vlan_id")) == old_id:
                    route["vlan_id"] = new_id
            for allocation in self._items("network_ip_allocations"):
                if _text(allocation.get("vlan_id")) == old_id:
                    allocation["vlan_id"] = new_id

    def refresh_tables(self, force_all: bool = False) -> None:
        """Mark planner tables dirty and materialise only the visible tab."""

        self._dirty_tabs.update(self._data_tabs)
        if force_all:
            for widget in tuple(self._data_tabs):
                self._refresh_tab(widget)
        else:
            self._refresh_current_tab()

    def _refresh_current_tab(self, *_args) -> None:
        widget = self.tabs.currentWidget()
        if widget in self._dirty_tabs:
            self._refresh_tab(widget)

    def _refresh_tab(self, widget) -> None:
        key = self._data_tabs.get(widget)
        if not key:
            return
        if key == "assets":
            self.assets_tab.set_rows([
                [item.get("id", ""), item.get("name", ""), item.get("asset_type", ""),
                 item.get("number_of_ports", 0), item.get("connections_in", 0),
                 item.get("connections_out", 0), item.get("uplink_ports", 0),
                 "Yes" if item.get("supports_stacking", False) else "No",
                 item.get("max_stack_members", 1), item.get("power_input_w", 0),
                 item.get("poe_budget_w", 0), item.get("bandwidth_capacity_gbps", 0),
                 item.get("packet_throughput_mpps", 0), item.get("expected_bandwidth_mbps", 0),
                 item.get("expected_packet_rate_pps", 0), item.get("rack_units", 0),
                 item.get("switch_rack_unit_allowance", 0), item.get("olt_units_per_rack_unit", 1)]
                for item in self._items("network_assets")
            ])
        elif key == "endpoint_traffic":
            self._refresh_endpoint_traffic_table()
        elif key == "instances":
            self.instances_tab.set_rows([
                [item.get("id", ""), item.get("name", ""), item.get("asset_id", ""),
                 item.get("location_name", ""), item.get("floor", 0), item.get("rack_name", ""),
                 item.get("rack_start_u", 0), item.get("rack_size_u", 0) or "",
                 item.get("management_ip", "")]
                for item in self._items("network_asset_instances")
            ])
        elif key == "connections":
            self.connections_tab.set_rows([
                [item.get("id", ""), item.get("from_instance_id", ""), item.get("from_port", ""),
                 item.get("to_instance_id", ""), item.get("to_port", ""),
                 item.get("connection_role", ""), item.get("medium", ""),
                 item.get("cable_specification", ""), ", ".join(item.get("vlan_ids", []))]
                for item in self._items("network_connections")
            ])
        elif key == "vlans":
            self.vlans_tab.set_rows([
                [item.get("id", ""), item.get("vlan_id", ""), item.get("name", ""),
                 item.get("purpose", ""), item.get("subnet", ""), item.get("gateway", ""),
                 item.get("security_zone", "")]
                for item in self._items("network_vlans")
            ])
        elif key == "routes":
            self.routes_tab.set_rows([
                [item.get("id", ""), item.get("source", ""), item.get("destination", ""),
                 item.get("vlan_id", ""), item.get("protocol", ""), item.get("next_hop", ""),
                 item.get("metric", 0), item.get("firewall_policy", "")]
                for item in self._items("network_routes")
            ])
        elif key == "patch_leads":
            self.patch_leads_tab.set_rows([
                [item.get("id", ""), item.get("instance_id", ""), item.get("port", ""),
                 item.get("peer_instance_id", ""), item.get("peer_port", ""),
                 item.get("endpoint_name", ""), item.get("medium", ""),
                 item.get("length_m", 0), item.get("connection_id", "")]
                for item in self._items("network_patch_leads")
            ])
        elif key == "fibre_nodes":
            self.fibre_nodes_tab.set_rows([
                [item.get("id", ""), item.get("name", ""), item.get("node_type", ""),
                 item.get("location_name", ""), item.get("floor", 0), item.get("rack_name", ""),
                 item.get("parent_node_id", ""),
                 item.get("splice_capacity", item.get("cassette_capacity", 0)),
                 item.get("drawing_layer", "")]
                for item in self._items("network_fibre_nodes")
            ])
        elif key == "fibre_cables":
            rows = []
            for item in self._items("network_fibre_cables"):
                stats = cable_core_statistics(item)
                rows.append([item.get("id", ""), item.get("name", ""),
                    item.get("from_instance_id", "") or item.get("from_location", ""),
                    item.get("to_instance_id", "") or item.get("to_location", ""),
                    item.get("cable_type", ""), item.get("core_count", 0), stats.get("used", 0),
                    stats.get("dark", 0), item.get("length_m", 0),
                    " -> ".join(str(v) for v in item.get("route_path", []) if _text(v)),
                    item.get("drawing_layer", "")])
            self.fibre_cables_tab.set_rows(rows)
        elif key == "fibre_splices":
            self.fibre_splices_tab.set_rows([
                [item.get("id", ""), item.get("node_id", ""), item.get("cassette_id", ""),
                 item.get("incoming_cable_id", ""), item.get("incoming_core", 1),
                 item.get("outgoing_cable_id", ""), item.get("outgoing_core", 1),
                 item.get("splice_type", ""), item.get("circuit_id", ""), item.get("loss_db", 0.0)]
                for item in self._items("network_fibre_splices")
            ])
        elif key == "external_networks":
            self.external_networks_tab.set_rows([
                [item.get("id", ""), item.get("name", ""), item.get("network_type", ""),
                 item.get("provider", ""), item.get("asn", ""), item.get("location_name", ""),
                 item.get("demarcation_instance_id", ""), ", ".join(item.get("prefixes", [])),
                 ", ".join(item.get("peer_instance_ids", []))]
                for item in self._items("network_external_networks")
            ])
        elif key == "ip_allocations":
            self.ip_allocations_tab.set_rows([
                [item.get("id", ""), item.get("instance_id", ""), item.get("vlan_id", ""),
                 item.get("address", ""), item.get("prefix_length", 0), item.get("gateway", ""),
                 item.get("purpose", "")]
                for item in self._items("network_ip_allocations")
            ])
        elif key == "summary":
            self._refresh_design_summary()
        self._dirty_tabs.discard(widget)

    def _refresh_endpoint_traffic_table(self) -> None:
        table = self.endpoint_traffic_table
        table.blockSignals(True)
        table.setRowCount(0)
        for asset in sorted(
            (item for item in self.data.get("assets", []) if isinstance(item, dict)),
            key=lambda item: (_text(item.get("name")).lower(), _text(item.get("id"))),
        ):
            row = table.rowCount()
            table.insertRow(row)
            values = [
                _text(asset.get("id")),
                _text(asset.get("name")) or _text(asset.get("id")),
                max(0, int(asset.get("data_points", 0) or 0)),
                max(0.0, float(asset.get("expected_bandwidth_mbps", 0.0) or 0.0)),
                max(0.0, float(asset.get("expected_packet_rate_pps", 0.0) or 0.0)),
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value))
                if column < 3:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                if column == 0:
                    item.setData(Qt.UserRole, values[0])
                table.setItem(row, column, item)
        table.blockSignals(False)

    def _endpoint_traffic_changed(self, item: QTableWidgetItem) -> None:
        if item.column() not in {3, 4}:
            return
        id_item = self.endpoint_traffic_table.item(item.row(), 0)
        asset_id = _text(id_item.data(Qt.UserRole) if id_item else "") or _text(id_item.text() if id_item else "")
        asset = next((row for row in self.data.get("assets", []) if isinstance(row, dict) and _text(row.get("id")) == asset_id), None)
        if asset is None:
            return
        try:
            value = max(0.0, float(item.text()))
        except (TypeError, ValueError):
            value = 0.0
        field = "expected_bandwidth_mbps" if item.column() == 3 else "expected_packet_rate_pps"
        asset[field] = value
        item.setText(str(value))

    @staticmethod
    def _layer_rule_pairs() -> List[Tuple[str, str, str]]:
        return [
            ("core", "core", "Core ↔ Core peer interconnection"),
            ("core", "aggregation", "Core → Aggregation"),
            ("aggregation", "aggregation", "Aggregation ↔ Aggregation peer interconnection"),
            ("aggregation", "access", "Aggregation → Access"),
            ("core", "access", "Core → Access"),
        ]

    def _append_layer_rule_row(self, rule: dict) -> None:
        row = self.layer_rules_table.rowCount()
        self.layer_rules_table.insertRow(row)

        enabled_check = QCheckBox()
        enabled_check.setChecked(bool(rule.get("enabled", True)))
        self.layer_rules_table.setCellWidget(row, 0, enabled_check)

        pair_combo = QComboBox()
        for source, target, label in self._layer_rule_pairs():
            pair_combo.addItem(label, (source, target))
        wanted_pair = (
            _text(rule.get("source_layer")).lower(),
            _text(rule.get("target_layer")).lower(),
        )
        for index in range(pair_combo.count()):
            if pair_combo.itemData(index) == wanted_pair:
                pair_combo.setCurrentIndex(index)
                break
        self.layer_rules_table.setCellWidget(row, 1, pair_combo)

        links_spin = QSpinBox()
        links_spin.setRange(1, 16)
        links_spin.setValue(max(1, int(rule.get("links_per_target", 1) or 1)))
        self.layer_rules_table.setCellWidget(row, 2, links_spin)

        distinct_spin = QSpinBox()
        distinct_spin.setRange(1, 16)
        distinct_spin.setValue(
            max(1, int(rule.get("minimum_distinct_sources", 1) or 1))
        )
        self.layer_rules_table.setCellWidget(row, 3, distinct_spin)

        def keep_distinct_valid(value: int) -> None:
            distinct_spin.setMaximum(max(1, int(value)))
            if distinct_spin.value() > value:
                distinct_spin.setValue(value)

        links_spin.valueChanged.connect(keep_distinct_valid)
        keep_distinct_valid(links_spin.value())

    def _layer_rules_from_table(self) -> List[dict]:
        rows: List[dict] = []
        for row in range(self.layer_rules_table.rowCount()):
            enabled_check = self.layer_rules_table.cellWidget(row, 0)
            pair_combo = self.layer_rules_table.cellWidget(row, 1)
            links_spin = self.layer_rules_table.cellWidget(row, 2)
            distinct_spin = self.layer_rules_table.cellWidget(row, 3)
            pair = pair_combo.currentData() if pair_combo is not None else None
            if not pair or len(pair) != 2:
                continue
            source, target = pair
            rows.append(
                {
                    "id": (
                        f"{source}_peer"
                        if source == target
                        else f"{source}_to_{target}"
                    ),
                    "source_layer": source,
                    "target_layer": target,
                    "links_per_target": int(links_spin.value()),
                    "minimum_distinct_sources": int(distinct_spin.value()),
                    "enabled": bool(enabled_check.isChecked()),
                }
            )
        return rows

    def _load_layer_profile_defaults(self, *_args) -> None:
        topology_model = _text(self.topology_model_combo.currentData()) or "collapsed_core"
        rules = default_layer_connection_rules(
            topology_model,
            self.redundant_core_check.isChecked(),
            self.independent_link_count_spin.value(),
        )
        self.layer_rules_table.setRowCount(0)
        for rule in rules:
            self._append_layer_rule_row(rule)

    def _remove_selected_layer_rules(self) -> None:
        rows = sorted(
            {index.row() for index in self.layer_rules_table.selectionModel().selectedRows()},
            reverse=True,
        )
        for row in rows:
            self.layer_rules_table.removeRow(row)

    def _update_layer_rule_availability(self, *_args) -> None:
        enabled = self.technology_combo.currentText().strip().lower() == "traditional"
        for widget in (
            self.topology_model_combo,
            self.independent_link_count_spin,
            self.redundant_core_check,
            self.layer_rules_table,
            self.load_profile_rules_button,
            self.add_layer_rule_button,
            self.remove_layer_rule_button,
        ):
            widget.setEnabled(enabled)

    def _manufacturer_preferences_from_table(self) -> Dict[str, dict]:
        result: Dict[str, dict] = {}
        for row in range(self.manufacturer_preferences_table.rowCount()):
            component_item = self.manufacturer_preferences_table.item(row, 0)
            names_item = self.manufacturer_preferences_table.item(row, 1)
            component = _text(component_item.data(Qt.UserRole) if component_item else "")
            wrapper = self.manufacturer_preferences_table.cellWidget(row, 2)
            strict_check = getattr(wrapper, "_strict_check", None) if wrapper is not None else None
            names = _csv_list(names_item.text() if names_item else "")
            if component:
                result[component] = {"preferred_manufacturers": names, "strict": bool(strict_check.isChecked()) if strict_check else False}
        return normalise_manufacturer_preferences(result)

    def clear_planner_resolution_overrides(self) -> None:
        settings = self.data.setdefault("network_settings", {})
        overrides = settings.get("auto_planner_resolution_overrides", {})
        has_overrides = isinstance(overrides, dict) and any(
            bool(value) for value in overrides.values()
        )
        if not has_overrides:
            QMessageBox.information(
                self,
                "Planner overrides",
                "There are no manual automatic-planner overrides to clear.",
            )
            return
        if (
            QMessageBox.question(
                self,
                "Clear planner overrides",
                "Clear selected switch models and minimum switch counts created "
                "by planning issue resolutions?",
            )
            != QMessageBox.Yes
        ):
            return
        settings["auto_planner_resolution_overrides"] = {}
        QMessageBox.information(
            self, "Planner overrides", "Automatic-planner overrides were cleared."
        )

    def _sync_planner_settings(self) -> None:
        settings = self.data.setdefault("network_settings", {})
        settings["technology"] = self.technology_combo.currentText().strip()
        settings["expected_mer_count"] = int(self.expected_mer_spin.value())
        settings["redundant_core"] = self.redundant_core_check.isChecked()
        settings["topology_model"] = (
            _text(self.topology_model_combo.currentData()) or "collapsed_core"
        )
        settings["independent_link_count"] = int(
            self.independent_link_count_spin.value()
        )
        settings["layer_connection_rules"] = normalise_layer_connection_rules(
            self._layer_rules_from_table(),
            settings["topology_model"],
            settings["redundant_core"],
            settings["independent_link_count"],
        )
        settings["manufacturer_preferences"] = self._manufacturer_preferences_from_table()
        settings["auto_connect_new_manual_devices"] = bool(
            self.auto_connect_manual_check.isChecked()
        )
        settings["auto_add_switches_for_bandwidth"] = bool(
            self.auto_add_bandwidth_switches_check.isChecked()
        )
        settings["ignore_link_bandwidth_errors"] = bool(
            self.ignore_link_bandwidth_check.isChecked()
        )
        settings["spare_capacity_percent"] = float(self.spare_capacity_spin.value())
        settings["default_expected_bandwidth_mbps"] = float(self.default_expected_bandwidth_spin.value())
        settings["default_expected_packet_rate_pps"] = float(self.default_expected_packet_rate_spin.value())
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
        settings["default_fibre_core_count"] = int(self.default_fibre_core_count_spin.value())
        settings["ip_plan_base_cidr"] = self.ip_plan_base_edit.text().strip() or "10.0.0.0/8"
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
            f"Topology: {summary.get('topology_model', '')}",
            f"Independent links: {summary.get('independent_link_count', '')}",
            f"Objective: {summary.get('objective', '')}",
            "",
            f"Endpoint locations: {summary.get('endpoint_locations', 0)}",
            f"Required ports: {summary.get('required_ports', 0)}",
            f"Required PoE: {summary.get('required_poe_w', 0)} W",
            f"Expected bandwidth: {summary.get('required_bandwidth_mbps', 0)} Mbps",
            f"Expected packet rate: {summary.get('required_packet_rate_pps', 0)} pps",
            f"Installed endpoint ports: {summary.get('installed_endpoint_ports', 0)}",
            f"Installed PoE budget: {summary.get('installed_poe_budget_w', 0)} W",
            f"Installed endpoint bandwidth capacity: {summary.get('installed_bandwidth_capacity_mbps', 0)} Mbps",
            f"Installed endpoint packet capacity: {summary.get('installed_packet_throughput_pps', 0)} pps",
            f"Spare capacity: {summary.get('spare_capacity_percent', 0)}%",
            f"Auto-add switches for bandwidth: {'Yes' if summary.get('auto_add_switches_for_bandwidth', True) else 'No'}",
            f"Bandwidth shortfall override: {'Enabled' if summary.get('ignore_link_bandwidth_errors', False) else 'Disabled'}",
            f"Estimated copper: {summary.get('estimated_copper_length_m', 0)} m",
            f"Estimated fibre: {summary.get('estimated_fibre_length_m', 0)} m",
            f"PoLAN ONTs per splitter limit: {summary.get('polan_max_onts_per_splitter', '')}",
            f"PoLAN splitter-to-ONT route limit: {summary.get('polan_max_splitter_ont_route_m', '')} m",
            "",
            "Components:",
        ]
        for role, quantity in (summary.get("component_counts", {}) or {}).items():
            lines.append(f"  {role}: {quantity}")
        layer_rules = summary.get("layer_connection_rules", []) or []
        if layer_rules:
            lines.extend(["", "Layer connection rules:"])
            for rule in layer_rules:
                if not rule.get("enabled", True):
                    continue
                source = _text(rule.get("source_layer")).title()
                target = _text(rule.get("target_layer")).title()
                arrow = "↔" if source == target else "→"
                lines.append(
                    f"  {source} {arrow} {target}: {rule.get('links_per_target', 1)} link(s), "
                    f"minimum {rule.get('minimum_distinct_sources', 1)} distinct source(s)"
                )
        overrides = summary.get("planner_resolution_overrides", {}) or {}
        if isinstance(overrides, dict) and any(bool(value) for value in overrides.values()):
            lines.extend(["", "Manual planner overrides:"])
            for location, asset_id in sorted(
                (overrides.get("access_asset_by_location", {}) or {}).items()
            ):
                lines.append(f"  Access switch at {location}: {asset_id}")
            for layer, asset_id in sorted(
                (overrides.get("upstream_asset_by_layer", {}) or {}).items()
            ):
                lines.append(f"  {layer.title()} switch model: {asset_id}")
            for location, count in sorted(
                (overrides.get("minimum_access_switches_by_location", {}) or {}).items()
            ):
                lines.append(f"  Minimum access switches at {location}: {count}")
            for location, mode in sorted(
                (overrides.get("spare_capacity_mode_by_location", {}) or {}).items()
            ):
                description = (
                    "defer access-port spare capacity"
                    if mode == "defer"
                    else "install a separate spare-capacity rack"
                )
                lines.append(f"  Spare capacity at {location}: {description}")
        deferred_locations = summary.get(
            "deferred_spare_capacity_locations", []
        ) or []
        if deferred_locations:
            lines.extend(["", "Deferred access-port spare capacity:"])
            lines.extend(f"  {location}" for location in deferred_locations)
        extra_rack_locations = summary.get(
            "additional_spare_capacity_rack_locations", []
        ) or []
        if extra_rack_locations:
            lines.extend(["", "Additional spare-capacity racks:"])
            lines.extend(f"  {location}" for location in extra_rack_locations)
        warnings = summary.get("warnings", []) or []
        if warnings:
            lines.extend(["", "Warnings:"])
            lines.extend(f"  • {warning}" for warning in warnings)
        self.summary_text.setPlainText("\n".join(lines))

    def _save_payload(self) -> dict:
        payload = {
            key: self.data.get(key)
            for key in _PLANNER_MUTABLE_KEYS
            if key in self.data
        }
        return pickle.loads(pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL))

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

        summary = None
        for _attempt in range(12):
            snapshot = self._save_payload()
            progress = QProgressDialog(
                "Preparing automatic network plan...", "", 0, 100, self
            )
            progress.setWindowTitle("Automatic network planning")
            progress.setWindowModality(Qt.WindowModal)
            progress.setCancelButton(None)
            progress.setMinimumDuration(0)
            progress.setAutoClose(False)
            progress.setAutoReset(False)
            progress.setValue(0)
            progress.show()

            def update_progress(value: int, message: str) -> None:
                progress.setLabelText(message)
                progress.setValue(max(0, min(100, int(value))))
                QApplication.processEvents()

            planning_error = None
            unexpected_error = None
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                summary = generate_network_design(
                    self.data,
                    technology,
                    progress_callback=update_progress,
                )
            except NetworkPlanningError as exc:
                planning_error = exc
            except Exception as exc:
                unexpected_error = exc
            finally:
                QApplication.restoreOverrideCursor()
                progress.setValue(100)
                progress.close()

            if summary is not None:
                break

            # A failed pass may have removed the previous generated design before
            # reaching the failing constraint. Restore the complete planner state
            # before presenting choices or returning to manual editing.
            for key in _PLANNER_MUTABLE_KEYS:
                if key in snapshot:
                    self.data[key] = snapshot[key]
                else:
                    self.data.pop(key, None)

            if unexpected_error is not None:
                QMessageBox.critical(
                    self,
                    "Automatic network planning",
                    f"Unexpected planning error:\n{unexpected_error}",
                )
                return

            resolution = PlanningResolutionDialog(self, planning_error, self.data)
            if resolution.exec() != QDialog.Accepted:
                return
            if resolution.selected_action == "review_assets":
                self.tabs.setCurrentWidget(self.assets_tab)
                self.refresh_tables()
                return

            settings = self.data.setdefault("network_settings", {})
            self.auto_add_bandwidth_switches_check.setChecked(
                bool(settings.get("auto_add_switches_for_bandwidth", True))
            )
            self.ignore_link_bandwidth_check.setChecked(
                bool(settings.get("ignore_link_bandwidth_errors", False))
            )
            self.spare_capacity_spin.setValue(
                float(settings.get("spare_capacity_percent", 0.0) or 0.0)
            )
        else:
            QMessageBox.critical(
                self,
                "Automatic network planning",
                "The planner could not resolve the design after 12 retry attempts. "
                "Review the asset library and the saved planner overrides.",
            )
            return

        self.refresh_tables()
        self.on_save(self._save_payload())
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
        power_connection_count = len(self.data.get("network_power_connections", []))
        patch_lead_count = len(self.data.get("network_patch_leads", []))
        fibre_cable_count = len(self.data.get("network_fibre_cables", []))
        fibre_node_count = len(self.data.get("network_fibre_nodes", []))
        fibre_splice_count = len(self.data.get("network_fibre_splices", []))

        if not any(
            (instance_count, connection_count, assignment_count, redundancy_count, power_connection_count, patch_lead_count, fibre_cable_count, fibre_node_count, fibre_splice_count)
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
            f"Redundancy groups: {redundancy_count}\n"
            f"Power connections: {power_connection_count}\n\n"
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
        self.data["network_power_connections"] = []
        self.data["network_patch_leads"] = []
        self.data["network_fibre_cables"] = []
        self.data["network_fibre_nodes"] = []
        self.data["network_fibre_splices"] = []
        self.data["network_ip_allocations"] = []
        self.data["network_design_summary"] = {}

        self.refresh_tables()
        self.on_save(self._save_payload())

        QMessageBox.information(
            self,
            "Clear installed networking",
            "Installed network assets and connections have been cleared.",
        )

    def edit_visually_on_plan(self) -> None:
        self._sync_planner_settings()
        self.on_save(self._save_payload())
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
            racks=rack_selection_records(self.data),
            default_auto_connect=bool(
                self.data.get("network_settings", {}).get(
                    "auto_connect_new_manual_devices", True
                )
            ),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_asset_instances", -1, dialog.result)
            if dialog.auto_connect_requested:
                result = auto_connect_manual_devices(
                    self.data, [_text(dialog.result.get("id"))]
                )
                self.refresh_tables()
                if result.get("warnings") and not result.get("created_connection_ids"):
                    QMessageBox.information(
                        self,
                        "Auto connect",
                        "\n".join(result.get("warnings", [])),
                    )

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
            racks=rack_selection_records(self.data),
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_asset_instances", index, dialog.result)
            if dialog.auto_connect_requested:
                result = auto_connect_manual_devices(
                    self.data, [_text(dialog.result.get("id"))]
                )
                self.refresh_tables()
                if result.get("warnings") and not result.get("created_connection_ids"):
                    QMessageBox.information(
                        self,
                        "Auto connect",
                        "\n".join(result.get("warnings", [])),
                    )

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
        self.data["network_power_connections"] = [
            link
            for link in self._items("network_power_connections")
            if _text(link.get("from_instance_id")) != instance_id
            and _text(link.get("to_instance_id")) != instance_id
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
            assets=self._items("network_assets"),
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
            assets=self._items("network_assets"),
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

    def sync_physical_fibre_layer(self) -> None:
        self._sync_planner_settings()
        summary = ensure_physical_fibre_for_design(self.data, replace_auto=False)
        self.refresh_tables()
        QMessageBox.information(
            self,
            "Physical fibre layer",
            f"Physical fibre synchronised. {summary.get('cable_count', 0)} cable records and "
            f"{summary.get('node_count', 0)} passive node records are now linked to the logical design.",
        )

    def generate_ip_plan(self) -> None:
        self._sync_planner_settings()
        try:
            result = generate_ip_address_plan(
                self.data, self.ip_plan_base_edit.text().strip()
            )
        except Exception as exc:
            QMessageBox.critical(self, "IP address plan", str(exc))
            return
        self.refresh_tables()
        QMessageBox.information(
            self,
            "IP address plan",
            f"Generated {result.get('vlan_count', 0)} VLAN subnet(s), "
            f"{result.get('management_allocations', 0)} management address(es), and router interfaces "
            f"on {result.get('router_instance_id') or 'the selected gateway device'}.",
        )

    def clear_ip_plan(self) -> None:
        if QMessageBox.question(self, "Clear generated IP plan", "Clear generated device IP allocations and router interfaces?") != QMessageBox.Yes:
            return
        self.data["network_ip_allocations"] = [
            row for row in self._items("network_ip_allocations")
            if not bool(row.get("auto_generated", False))
        ]
        for instance in self._items("network_asset_instances"):
            instance.pop("router_ip_addresses", None)
            if _text(instance.get("management_vlan")):
                instance["management_ip"] = ""
                instance["management_vlan"] = ""
        self.refresh_tables()

    def add_patch_lead(self) -> None:
        dialog = PatchLeadEditorDialog(
            self, instances=self._items("network_asset_instances"),
            suggested_id=_next_id(self._items("network_patch_leads"), "PL")
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_patch_leads", -1, dialog.result)

    def edit_patch_lead(self) -> None:
        index, item = self._selected(self.patch_leads_tab, "network_patch_leads")
        if item is None: return
        dialog = PatchLeadEditorDialog(self, item, self._items("network_asset_instances"), _text(item.get("id")))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_patch_leads", index, dialog.result)

    def delete_patch_lead(self) -> None:
        index, item = self._selected(self.patch_leads_tab, "network_patch_leads")
        if item is not None and QMessageBox.question(self, "Delete patch cable", f"Delete {_text(item.get('id'))}?") == QMessageBox.Yes:
            self._items("network_patch_leads").pop(index); self.refresh_tables()

    def add_fibre_node(self) -> None:
        dialog = FibreNodeEditorDialog(
            self, nodes=self._items("network_fibre_nodes"), instances=self._items("network_asset_instances"),
            locations=self.data.get("locations", []), suggested_id=_next_id(self._items("network_fibre_nodes"), "FN")
        )
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_fibre_nodes", -1, dialog.result)

    def edit_fibre_node(self) -> None:
        index, item = self._selected(self.fibre_nodes_tab, "network_fibre_nodes")
        if item is None: return
        dialog = FibreNodeEditorDialog(self, item, self._items("network_fibre_nodes"), self._items("network_asset_instances"), self.data.get("locations", []), _text(item.get("id")))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_fibre_nodes", index, dialog.result)

    def delete_fibre_node(self) -> None:
        index, item = self._selected(self.fibre_nodes_tab, "network_fibre_nodes")
        if item is None: return
        node_id = _text(item.get("id"))
        if QMessageBox.question(self, "Delete fibre node", f"Delete {node_id} and its splice records?") != QMessageBox.Yes: return
        self._items("network_fibre_nodes").pop(index)
        self.data["network_fibre_splices"] = [s for s in self._items("network_fibre_splices") if _text(s.get("node_id")) != node_id and _text(s.get("cassette_id")) != node_id]
        self.refresh_tables()

    def add_fibre_cable(self) -> None:
        dialog = FibreCableEditorDialog(self, instances=self._items("network_asset_instances"), suggested_id=_next_id(self._items("network_fibre_cables"), "FOC"))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_fibre_cables", -1, dialog.result)

    def edit_fibre_cable(self) -> None:
        index, item = self._selected(self.fibre_cables_tab, "network_fibre_cables")
        if item is None: return
        dialog = FibreCableEditorDialog(self, item, self._items("network_asset_instances"), _text(item.get("id")))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_fibre_cables", index, dialog.result)

    def delete_fibre_cable(self) -> None:
        index, item = self._selected(self.fibre_cables_tab, "network_fibre_cables")
        if item is None: return
        cable_id = _text(item.get("id"))
        if QMessageBox.question(self, "Delete fibre cable", f"Delete {cable_id} and all splices using it?") != QMessageBox.Yes: return
        self._items("network_fibre_cables").pop(index)
        self.data["network_fibre_splices"] = [s for s in self._items("network_fibre_splices") if _text(s.get("incoming_cable_id")) != cable_id and _text(s.get("outgoing_cable_id")) != cable_id]
        self.refresh_tables()

    def add_fibre_splice(self) -> None:
        if len(self._items("network_fibre_cables")) < 2:
            QMessageBox.information(self, "Fibre splice", "Create at least two physical fibre cables first.")
            return
        dialog = FibreSpliceEditorDialog(self, nodes=self._items("network_fibre_nodes"), cables=self._items("network_fibre_cables"), suggested_id=_next_id(self._items("network_fibre_splices"), "FS"))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_fibre_splices", -1, dialog.result); set_core_status_from_splices(self.data)

    def edit_fibre_splice(self) -> None:
        index, item = self._selected(self.fibre_splices_tab, "network_fibre_splices")
        if item is None: return
        dialog = FibreSpliceEditorDialog(self, item, self._items("network_fibre_nodes"), self._items("network_fibre_cables"), _text(item.get("id")))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_fibre_splices", index, dialog.result); set_core_status_from_splices(self.data)

    def delete_fibre_splice(self) -> None:
        index, item = self._selected(self.fibre_splices_tab, "network_fibre_splices")
        if item is not None and QMessageBox.question(self, "Delete fibre splice", f"Delete {_text(item.get('id'))}?") == QMessageBox.Yes:
            splice_id = _text(item.get("id")); self._items("network_fibre_splices").pop(index)
            for cable in self._items("network_fibre_cables"):
                cable["splice_ids"] = [v for v in cable.get("splice_ids", []) if _text(v) != splice_id]
            self.refresh_tables()

    def add_external_network(self) -> None:
        dialog = ExternalNetworkEditorDialog(self, instances=self._items("network_asset_instances"), locations=self.data.get("locations", []), suggested_id=_next_id(self._items("network_external_networks"), "EXT"))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_external_networks", -1, dialog.result)

    def edit_external_network(self) -> None:
        index, item = self._selected(self.external_networks_tab, "network_external_networks")
        if item is None: return
        dialog = ExternalNetworkEditorDialog(self, item, self._items("network_asset_instances"), self.data.get("locations", []), _text(item.get("id")))
        if dialog.exec() == QDialog.Accepted and dialog.result:
            self._replace_or_append("network_external_networks", index, dialog.result)

    def delete_external_network(self) -> None:
        index, item = self._selected(self.external_networks_tab, "network_external_networks")
        if item is not None and QMessageBox.question(self, "Delete external network", f"Delete {_text(item.get('id'))}?") == QMessageBox.Yes:
            self._items("network_external_networks").pop(index); self.refresh_tables()

    def save(self) -> None:
        self._sync_planner_settings()
        self.on_save(self._save_payload())
        QMessageBox.information(
            self, "Network planning", "Network planning data saved."
        )
