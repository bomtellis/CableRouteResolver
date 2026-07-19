from __future__ import annotations

from copy import deepcopy
import pickle
from pathlib import Path
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
    QFileDialog,
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
    QWizard,
    QWizardPage,
)

NETWORK_ASSET_TYPES = [
    ("patch_panel", "Patch panel"),
    ("fibre_splitter", "Fibre splitter"),
    ("network_switch", "Network switch"),
    ("network_router", "Network router"),
    ("firewall", "Firewall"),
    ("wireless_access_point", "Wireless access point"),
    ("wireless_device", "Wireless device"),
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

NETWORK_ASSET_GROUP_OPTIONS = [
    ("router", "Router / edge security"),
    ("core", "Core"),
    ("aggregation", "Aggregation / distribution"),
    ("access", "Access"),
    ("switching", "Other switching"),
    ("wireless", "Wireless devices"),
    ("optical", "Optical network"),
    ("patching", "Patch panels"),
    ("power", "Power"),
    ("cable_management", "Cable management"),
    ("external", "External networks"),
    ("other", "Other"),
]
NETWORK_ASSET_GROUP_LABELS = dict(NETWORK_ASSET_GROUP_OPTIONS)
NETWORK_ASSET_GROUP_ORDER = {
    value: index for index, (value, _label) in enumerate(NETWORK_ASSET_GROUP_OPTIONS)
}

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
PORT_TYPE_OPTIONS = ["rj45", "sfp", "sfp+", "sfp28", "sfp56", "qsfp", "qsfp+", "qsfp28", "qsfp56", "qsfpdd", "osfp", "pon", "lc", "sc", "mpo", "usb", "console", "power", "other"]
PORT_USE_OPTIONS = ["input", "output", "uplink", "downlink", "management", "console", "pon", "client", "patch", "stacking", "power", "spare", "other"]

from network_auto_planner import (
    NetworkPlanningError,
    auto_connect_manual_devices,
    auto_connect_pending_imported_wireless_devices,
    der_graph_inventory_signature,
    estimate_network_switch_counts,
    generate_network_design,
)
from network_services import cable_core_statistics, ensure_physical_fibre_for_design, generate_ip_address_plan, set_core_status_from_splices
from network_fibre_dialogs import (
    ExternalNetworkEditorDialog, FibreCableEditorDialog, FibreNodeEditorDialog,
    FibreSpliceEditorDialog, PatchLeadEditorDialog,
)
from network_schema import (
    ASSET_MODEL_PREFERENCE_COMPONENTS,
    CATALYST_9600_LINE_CARDS,
    MANUFACTURER_PREFERENCE_COMPONENTS,
    NETWORK_PORT_SPEED_OPTIONS,
    PLUGGABLE_OPTIC_PORT_TYPES,
    compatible_port_speeds,
    catalyst_9600_port_definitions,
    default_layer_connection_rules,
    default_port_speeds,
    normalise_asset_model_preferences,
    normalise_layer_connection_rules,
    normalise_manufacturer_preferences,
    normalise_port_speeds,
    network_asset_group,
    port_speed_label,
)
from asset_library_io import (
    AssetPackError,
    merge_asset_rows,
    read_asset_pack,
    write_asset_pack,
)


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _layer_switch_estimate_text(estimate: dict) -> str:
    if not bool(estimate.get("available", True)):
        return _text(estimate.get("note")) or "Layer estimate is unavailable."
    return (
        f"Core: {int(estimate.get('core_switches', 0) or 0)}   |   "
        f"Aggregation: {int(estimate.get('aggregation_switches', 0) or 0)}   |   "
        f"Access: {int(estimate.get('access_switches', 0) or 0)} physical "
        f"switches in {int(estimate.get('access_stacks', 0) or 0)} logical stacks\n"
        f"Based on {int(estimate.get('endpoint_ports', 0) or 0)} endpoint ports. "
        f"{_text(estimate.get('note'))}"
    )




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
            "pon": "PON", "sfp": "SFP", "sfp+": "SFP+", "sfp28": "SFP28", "sfp56": "SFP56",
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
        group_source = {
            **self.asset,
            "asset_type": _text(self.asset.get("asset_type")) or "network_switch",
        }
        self.asset_group_combo = _combo_with_data(
            NETWORK_ASSET_GROUP_OPTIONS, network_asset_group(group_source)
        )
        self.asset_group_combo.setToolTip(
            "Groups models in the Asset Library. Core, aggregation and access "
            "groups also identify the intended layer for switch models."
        )
        self.manufacturer_edit = QLineEdit(_text(self.asset.get("manufacturer")))
        self.model_edit = QLineEdit(_text(self.asset.get("model")))
        self.wireless_category_combo = QComboBox()
        for label, value in (
            ("Access point", "access_point"),
            ("IoT gateway", "iot_gateway"),
            ("Radio gateway", "radio_gateway"),
            ("Wireless bridge", "wireless_bridge"),
            ("Wireless sensor", "wireless_sensor"),
            ("Other wireless device", "other"),
        ):
            self.wireless_category_combo.addItem(label, value)
        current_wireless_category = _text(
            self.asset.get("wireless_device_category")
        ).lower() or (
            "access_point"
            if _text(self.asset.get("asset_type")).lower() == "wireless_access_point"
            else "other"
        )
        wireless_category_index = self.wireless_category_combo.findData(
            current_wireless_category
        )
        self.wireless_category_combo.setCurrentIndex(
            wireless_category_index if wireless_category_index >= 0 else 0
        )

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

        legacy_expected_bandwidth = float(
            self.asset.get("expected_bandwidth_mbps", 0.0) or 0.0
        )
        has_directional_traffic = (
            "expected_north_south_bandwidth_mbps" in self.asset
            or "expected_east_west_bandwidth_mbps" in self.asset
        )
        self.expected_north_south_spin = QDoubleSpinBox()
        self.expected_north_south_spin.setRange(0.0, 1_000_000_000.0)
        self.expected_north_south_spin.setDecimals(3)
        self.expected_north_south_spin.setSuffix(" Mbps")
        self.expected_north_south_spin.setValue(
            float(
                self.asset.get(
                    "expected_north_south_bandwidth_mbps",
                    legacy_expected_bandwidth if not has_directional_traffic else 0.0,
                )
                or 0.0
            )
        )
        self.expected_north_south_spin.setToolTip(
            "Traffic expected to enter or leave the site through routers, WAN, cloud or internet links."
        )

        self.expected_east_west_spin = QDoubleSpinBox()
        self.expected_east_west_spin.setRange(0.0, 1_000_000_000.0)
        self.expected_east_west_spin.setDecimals(3)
        self.expected_east_west_spin.setSuffix(" Mbps")
        self.expected_east_west_spin.setValue(
            float(self.asset.get("expected_east_west_bandwidth_mbps", 0.0) or 0.0)
        )
        self.expected_east_west_spin.setToolTip(
            "Traffic expected between internal devices, servers, controllers, NVRs and local services."
        )

        self.north_south_concurrency_spin = QDoubleSpinBox()
        self.north_south_concurrency_spin.setRange(0.0, 100.0)
        self.north_south_concurrency_spin.setDecimals(1)
        self.north_south_concurrency_spin.setSuffix(" %")
        self.north_south_concurrency_spin.setValue(
            100.0
            * min(
                1.0,
                max(
                    0.0,
                    float(
                        self.asset.get("north_south_concurrency_factor", 1.0)
                        or 0.0
                    ),
                ),
            )
        )
        self.north_south_concurrency_spin.setToolTip(
            "Percentage of north-south device traffic expected concurrently "
            "during the design busy hour."
        )

        self.east_west_concurrency_spin = QDoubleSpinBox()
        self.east_west_concurrency_spin.setRange(0.0, 100.0)
        self.east_west_concurrency_spin.setDecimals(1)
        self.east_west_concurrency_spin.setSuffix(" %")
        self.east_west_concurrency_spin.setValue(
            100.0
            * min(
                1.0,
                max(
                    0.0,
                    float(
                        self.asset.get("east_west_concurrency_factor", 1.0)
                        or 0.0
                    ),
                ),
            )
        )
        self.east_west_concurrency_spin.setToolTip(
            "Percentage of east-west device traffic expected concurrently "
            "during the design busy hour."
        )

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

        self.chassis_module_table = QTableWidget(6, 3)
        self.chassis_module_table.setHorizontalHeaderLabels(["Slot", "Module type", "Installed module"])
        self.chassis_module_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        configured_modules = {
            int(row.get("slot", 0) or 0): row
            for row in self.asset.get("chassis_modules", [])
            if isinstance(row, dict)
        }
        for table_row, slot in enumerate((1, 2, 3, 4, 5, 6)):
            self.chassis_module_table.setItem(table_row, 0, QTableWidgetItem(str(slot)))
            type_item = QTableWidgetItem("Supervisor" if slot in {3, 4} else "Line card")
            type_item.setFlags(type_item.flags() & ~Qt.ItemIsEditable)
            self.chassis_module_table.setItem(table_row, 1, type_item)
            module_combo = QComboBox()
            module_combo.addItem("Empty", "")
            choices = ("C9600-SUP-1", "C9600X-SUP-2") if slot in {3, 4} else tuple(CATALYST_9600_LINE_CARDS)
            for model in choices:
                module_combo.addItem(model, model)
            _set_combo_text(module_combo, _text(configured_modules.get(slot, {}).get("model")))
            self.chassis_module_table.setCellWidget(table_row, 2, module_combo)
            module_combo.currentIndexChanged.connect(self._sync_chassis_port_definitions)

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
        self.physical_width_spin = QDoubleSpinBox(); self.physical_width_spin.setRange(0, 5000); self.physical_width_spin.setDecimals(1); self.physical_width_spin.setSuffix(" mm"); self.physical_width_spin.setValue(float(self.asset.get("physical_width_mm", 0.0) or 0.0))
        self.physical_depth_spin = QDoubleSpinBox(); self.physical_depth_spin.setRange(0, 5000); self.physical_depth_spin.setDecimals(1); self.physical_depth_spin.setSuffix(" mm"); self.physical_depth_spin.setValue(float(self.asset.get("physical_depth_mm", 0.0) or 0.0))
        self.physical_height_spin = QDoubleSpinBox(); self.physical_height_spin.setRange(0, 5000); self.physical_height_spin.setDecimals(1); self.physical_height_spin.setSuffix(" mm"); self.physical_height_spin.setValue(float(self.asset.get("physical_height_mm", 0.0) or 0.0))

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
        general_form.addRow("Asset group", self.asset_group_combo)
        general_form.addRow("Manufacturer", self.manufacturer_edit)
        general_form.addRow("Model", self.model_edit)
        general_form.addRow("Wireless device category", self.wireless_category_combo)
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
        capacity_form.addRow("North-south device traffic", self.expected_north_south_spin)
        capacity_form.addRow("North-south concurrency", self.north_south_concurrency_spin)
        capacity_form.addRow("East-west device traffic", self.expected_east_west_spin)
        capacity_form.addRow("East-west concurrency", self.east_west_concurrency_spin)
        capacity_form.addRow("Expected device packet rate", self.expected_packet_rate_spin)
        capacity_form.addRow("Stacking", self.supports_stacking_check)
        capacity_form.addRow("Maximum stack members", self.max_stack_members_spin)
        capacity_form.addRow("Rack spaces", self.rack_units_spin)
        capacity_form.addRow("Physical width", self.physical_width_spin)
        capacity_form.addRow("Physical depth", self.physical_depth_spin)
        capacity_form.addRow("Physical height", self.physical_height_spin)
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

        chassis_page = QWidget()
        chassis_layout = QVBoxLayout(chassis_page)
        chassis_layout.addWidget(QLabel("Catalyst 9606R line-card slots: 1, 2, 5 and 6; supervisor slots: 3 and 4."))
        chassis_layout.addWidget(self.chassis_module_table, 1)
        self.tabs.addTab(chassis_page, "Chassis modules")

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
        if configured_modules:
            self._sync_chassis_port_definitions()
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

    def _sync_chassis_port_definitions(self, *_args) -> None:
        """Rebuild the visible port inventory from the installed line cards."""
        if not hasattr(self, "chassis_module_table"):
            return
        modules = []
        for row in range(self.chassis_module_table.rowCount()):
            combo = self.chassis_module_table.cellWidget(row, 2)
            model = _text(combo.currentData()) if isinstance(combo, QComboBox) else ""
            if not model:
                continue
            slot_item = self.chassis_module_table.item(row, 0)
            slot = int(slot_item.text()) if slot_item is not None else row + 1
            modules.append({
                "slot": slot,
                "module_type": "supervisor" if slot in {3, 4} else "line_card",
                "model": model,
            })
        definitions = catalyst_9600_port_definitions(modules)
        if not definitions and not modules:
            return
        self.port_table.blockSignals(True)
        try:
            self.port_table.setRowCount(0)
            for definition in definitions:
                self._append_port_row(definition)
        finally:
            self.port_table.blockSignals(False)
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
        enabled_frequencies = asset_type in {"wireless_access_point", "wireless_device"}
        self.wireless_category_combo.setEnabled(enabled_frequencies)
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
        asset_group = _text(self.asset_group_combo.currentData()) or "other"
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
        if asset_type in {"wireless_access_point", "wireless_device"} and not frequencies:
            QMessageBox.critical(
                self,
                "Invalid asset",
                "A wireless device requires at least one frequency.",
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

        chassis_modules = []
        for row in range(self.chassis_module_table.rowCount()):
            combo = self.chassis_module_table.cellWidget(row, 2)
            model = _text(combo.currentData()) if isinstance(combo, QComboBox) else ""
            if not model:
                continue
            slot = int(self.chassis_module_table.item(row, 0).text())
            chassis_modules.append({
                "slot": slot,
                "module_type": "supervisor" if slot in {3, 4} else "line_card",
                "model": model,
            })

        self.result = {
            **self.asset,
            "id": asset_id,
            "name": name,
            "asset_type": asset_type,
            "asset_group": asset_group,
            "network_layer": (
                asset_group
                if asset_type == "network_switch"
                and asset_group in {"core", "aggregation", "access"}
                else ""
                if asset_type == "network_switch" and asset_group == "switching"
                else _text(self.asset.get("network_layer"))
                or ("edge" if asset_type in {"network_router", "firewall"} else "")
            ),
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
            "wireless_device_category": (
                _text(self.wireless_category_combo.currentData())
                if asset_type in {"wireless_access_point", "wireless_device"}
                else ""
            ),
            "frequencies": frequencies if asset_type in {"wireless_access_point", "wireless_device"} else [],
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
            "expected_north_south_bandwidth_mbps": float(
                self.expected_north_south_spin.value()
            ),
            "expected_east_west_bandwidth_mbps": float(
                self.expected_east_west_spin.value()
            ),
            "north_south_concurrency_factor": round(
                float(self.north_south_concurrency_spin.value()) / 100.0, 6
            ),
            "east_west_concurrency_factor": round(
                float(self.east_west_concurrency_spin.value()) / 100.0, 6
            ),
            "expected_bandwidth_mbps": float(
                self.expected_north_south_spin.value()
                + self.expected_east_west_spin.value()
            ),
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
            "modular_chassis": bool(asset_type == "network_switch" and chassis_modules),
            "chassis_modules": chassis_modules if asset_type == "network_switch" else [],
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
            "physical_width_mm": float(self.physical_width_spin.value()),
            "physical_depth_mm": float(self.physical_depth_spin.value()),
            "physical_height_mm": float(self.physical_height_spin.value()),
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
        self.result = deepcopy(self.instance)
        self.result.update({
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
        })
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
        self.cabinet_type_combo = QComboBox()
        self.cabinet_type_combo.addItem("Standard rack cabinet", "standard")
        self.cabinet_type_combo.addItem("Slim wall cabinet (maximum two switches)", "slim_wall")
        cabinet_type_index = self.cabinet_type_combo.findData(
            _text(self.rack.get("cabinet_type")) or "standard"
        )
        if cabinet_type_index >= 0:
            self.cabinet_type_combo.setCurrentIndex(cabinet_type_index)
        self.manufacturer_edit = QLineEdit(_text(self.rack.get("manufacturer")))
        self.model_edit = QLineEdit(_text(self.rack.get("model")))
        self.notes_edit = QTextEdit(_text(self.rack.get("notes"))); self.notes_edit.setMinimumHeight(80)
        form.addRow("Cabinet ID", self.id_edit)
        form.addRow("Cabinet name", self.name_edit)
        form.addRow("Location", self.location_combo)
        form.addRow("Floor", self.floor_spin)
        form.addRow("Capacity", self.capacity_spin)
        form.addRow("Cabinet type", self.cabinet_type_combo)
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
            "cabinet_type": _text(self.cabinet_type_combo.currentData()) or "standard",
            "max_switches": 2 if self.cabinet_type_combo.currentData() == "slim_wall" else 0,
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
        self.notice_label = QLabel()
        self.notice_label.setWordWrap(True)
        self.notice_label.setStyleSheet(
            "QLabel { background: #fff4cc; border: 1px solid #d6a700; "
            "border-radius: 3px; color: #4d3a00; padding: 7px; }"
        )
        self.notice_label.hide()
        layout.addWidget(self.notice_label)
        self.filter_layout = QHBoxLayout()
        layout.addLayout(self.filter_layout)
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
        self.button_layout = row
        layout.addLayout(row)
        self.add_button = QPushButton("Add")
        self.edit_button = QPushButton("Edit")
        self.delete_button = QPushButton("Delete")
        row.addWidget(self.add_button)
        row.addWidget(self.edit_button)
        row.addWidget(self.delete_button)
        row.addStretch(1)
        self.edit_requested: Callable[[], None] = lambda: None

    def selected_rows(self) -> List[int]:
        selection_model = self.table.selectionModel()
        if selection_model is None:
            return []
        rows = set()
        for index in selection_model.selectedRows():
            item = self.table.item(index.row(), 0)
            source_row = item.data(Qt.UserRole) if item is not None else None
            rows.add(index.row() if source_row is None else int(source_row))
        return sorted(rows)

    def selected_row(self) -> int:
        rows = self.selected_rows()
        return rows[0] if rows else -1

    def set_rows(
        self,
        rows: Sequence[Sequence[object]],
        source_indices: Optional[Sequence[int]] = None,
    ) -> None:
        self.table.setRowCount(len(rows))
        for row_index, values in enumerate(rows):
            source_index = (
                int(source_indices[row_index])
                if source_indices is not None and row_index < len(source_indices)
                else row_index
            )
            for column_index, value in enumerate(values):
                item = QTableWidgetItem(str(value if value is not None else ""))
                item.setData(Qt.UserRole, source_index)
                self.table.setItem(row_index, column_index, item)

    def display_row_for_source_index(self, source_index: int) -> int:
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if item is not None and item.data(Qt.UserRole) == source_index:
                return row
        return -1


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
            "The selected override is saved in Network Settings. The guided setup "
            "is not reopened; if another comms room reaches a separate constraint, "
            "the next resolution is shown with all earlier choices retained. "
            "Overrides can be cleared later from the planner settings."
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
        if self.details.get("free_from_port_count") is not None:
            lines.append(
                "Free fibre ports: "
                f"downstream {int(self.details.get('free_from_port_count', 0) or 0)}, "
                f"upstream {int(self.details.get('free_to_port_count', 0) or 0)}"
            )
        if self.details.get("location_name"):
            lines.append(f"Location: {self.details.get('location_name')}")
        if self.details.get("parent_location_name"):
            lines.append(
                f"Parent DER: {self.details.get('parent_location_name')}"
            )
        if self.details.get("switch_asset_name"):
            lines.append(
                f"Access-switch model to clone: {self.details.get('switch_asset_name')}"
            )
        if self.details.get("from_name"):
            lines.append(f"Downstream device: {self.details.get('from_name')}")
        if self.details.get("to_name"):
            lines.append(f"Upstream device: {self.details.get('to_name')}")
        maximum_cabinets = int(self.details.get("maximum_cabinets", 0) or 0)
        required_cabinets = int(self.details.get("required_cabinets", 0) or 0)
        if maximum_cabinets > 0:
            lines.append(f"Maximum cabinets at location: {maximum_cabinets}")
        if required_cabinets > 0:
            lines.append(f"Cabinets required by design: {required_cabinets}")
        maximum_switches = int(
            self.details.get(
                "maximum_switches_per_cabinet",
                self.details.get("maximum_switches", 0),
            )
            or 0
        )
        required_switches = int(self.details.get("required_switches", 0) or 0)
        if maximum_switches > 0:
            lines.append(f"Maximum switches per cabinet: {maximum_switches}")
        if required_switches > 0:
            lines.append(f"Switches required by design: {required_switches}")

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
        upstream_ports_exhausted = (
            error_code == "link_capacity"
            and from_role in {"access", "access_switch"}
            and int(self.details.get("free_from_port_count", 0) or 0) > 0
            and int(self.details.get("free_to_port_count", 0) or 0) == 0
        )
        upstream_speed_pool_exhausted = (
            error_code == "link_capacity"
            and from_role in {"access", "access_switch"}
            and int(self.details.get("free_from_port_count", 0) or 0) > 0
            and int(self.details.get("free_to_port_count", 0) or 0) > 0
            and float(
                self.details.get("max_compatible_capacity_mbps", 0.0) or 0.0
            )
            <= 0.0
        )
        if error_code in {
            "location_cabinet_limit",
            "slim_wall_cabinet_capacity",
        }:
            self.action_combo.addItem(
                "Stop planning and edit the location cabinet constraint",
                "review_location",
            )
        if error_code == "access_stack_spare_capacity" and location:
            self.action_combo.addItem(
                "Install another rack and create an overflow stack for spare capacity (recommended)",
                "add_spare_capacity_rack",
            )
            self.action_combo.addItem(
                "Defer spare access-port capacity at this location",
                "defer_location_spare_capacity",
            )
        if error_code == "der_patch_panel_capacity" and location:
            self.action_combo.addItem(
                "Place another DER adjacent to the existing DER and connect it automatically (recommended)",
                "add_adjacent_der",
            )
        if (
            upstream_ports_exhausted or upstream_speed_pool_exhausted
        ) and self.details.get("upstream_alternatives"):
            self.action_combo.addItem(
                (
                    "Use a core/aggregation model with more compatible-speed ports (recommended)"
                    if upstream_speed_pool_exhausted
                    else "Use a core/aggregation model with more downlink ports (recommended)"
                ),
                "select_upstream_asset",
            )
        if (
            not upstream_ports_exhausted
            and not upstream_speed_pool_exhausted
            and location
            and error_code != "access_stack_spare_capacity"
            and (
                from_role in {"access", "access_switch"}
                or error_code == "access_uplink_capacity"
            )
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
        if self.details.get("upstream_alternatives") and not upstream_ports_exhausted:
            self.action_combo.addItem(
                "Use a selected upstream core/aggregation switch model",
                "select_upstream_asset",
            )
        if getattr(self.error, "code", "") in {
            "link_capacity",
            "access_uplink_capacity",
            "external_north_south_capacity",
        }:
            self.action_combo.addItem(
                (
                    "Use the available external internet capacity and record a warning"
                    if getattr(self.error, "code", "")
                    == "external_north_south_capacity"
                    else "Create the best available link and ignore the bandwidth shortfall"
                ),
                "ignore_bandwidth",
            )
            self.action_combo.addItem(
                "Retry without spare traffic capacity", "remove_spare_capacity"
            )
        if error_code not in {
            "location_cabinet_limit",
            "slim_wall_cabinet_capacity",
        }:
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
        additional_ders = overrides.setdefault("additional_der_by_location", {})
        additional_der_assets = overrides.setdefault(
            "additional_der_asset_by_location", {}
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
        elif action == "add_adjacent_der":
            parent_location = _text(
                self.details.get("parent_location_name")
            ) or location
            if not parent_location:
                return
            requested = max(
                1, int(self.details.get("remaining_panel_count", 1) or 1)
            )
            additional_ders[parent_location] = max(
                max(0, int(additional_ders.get(parent_location, 0) or 0)),
                requested,
            )
            switch_asset_id = _text(self.details.get("switch_asset_id"))
            if switch_asset_id:
                additional_der_assets[parent_location] = switch_asset_id
            minimum_switches[parent_location] = max(
                int(minimum_switches.get(parent_location, 0) or 0),
                int(self.details.get("suggested_device_count", 0) or 0),
            )
            settings["auto_add_switches_for_bandwidth"] = True
            settings["der_expansion_inventory_signature"] = (
                der_graph_inventory_signature(self.data)
            )
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
        elif action in {"review_assets", "review_location"}:
            self.selected_action = action
            self.accept()
            return
        else:
            return

        self.selected_action = action
        self.accept()



class PlanningResolutionSequenceDialog(QDialog):
    """Resolve repeated spare-capacity constraints without restarting setup."""

    def __init__(self, parent, error: NetworkPlanningError, data: dict):
        super().__init__(parent)
        self.setWindowTitle("Resolve repeated automatic planning issues")
        self.resize(820, 560)
        self.error = error
        self.data = data
        details = dict(getattr(error, "details", {}) or {})
        self.issue_mode = _text(getattr(error, "code", "")).lower()
        self.is_der_batch = self.issue_mode == "der_patch_panel_capacity_batch"
        self.issues = [
            dict(issue)
            for issue in details.get("issues", []) or []
            if isinstance(issue, dict) and _text(issue.get("location_name"))
        ]
        self.current_index = 0
        self.decisions: Dict[str, str] = {}
        self.selected_actions: Dict[str, str] = {}

        layout = QVBoxLayout(self)
        heading = QLabel(
            (
                "The planner found every DER that currently needs adjacent "
                "capacity. Work through the affected DERs below; all decisions "
                "are applied together before the network is recalculated once."
                if self.is_der_batch
                else
                "The same stack and cabinet constraint occurs in more than one "
                "comms room. Work through each affected room below. The guided "
                "planner setup remains active, every decision is retained, and the "
                "network is recalculated only after all currently known occurrences "
                "have been resolved."
            )
        )
        heading.setWordWrap(True)
        layout.addWidget(heading)

        self.issue_progress_label = QLabel()
        layout.addWidget(self.issue_progress_label)

        self.issue_message_label = QLabel()
        self.issue_message_label.setWordWrap(True)
        layout.addWidget(self.issue_message_label)

        self.diagnostics = QTextEdit()
        self.diagnostics.setReadOnly(True)
        self.diagnostics.setMaximumHeight(230)
        layout.addWidget(self.diagnostics)

        form = QFormLayout()
        self.action_combo = QComboBox()
        if self.is_der_batch:
            self.action_combo.addItem(
                "Place another DER adjacent to the parent and clone its access switch (recommended)",
                "add_adjacent_der",
            )
        else:
            self.action_combo.addItem(
                "Install another cabinet and the required overflow switch stack "
                "(recommended)",
                "new_rack",
            )
            self.action_combo.addItem(
                "Defer spare access-port capacity in this comms room",
                "defer",
            )
        form.addRow("Resolution", self.action_combo)
        layout.addLayout(form)

        self.apply_remaining_check = QCheckBox(
            "Use this resolution for all remaining matching comms rooms"
        )
        self.apply_remaining_check.setChecked(False)
        layout.addWidget(self.apply_remaining_check)

        note = QLabel(
            (
                "Each added DER is spaced away from its parent so it remains "
                "selectable, receives a direct graph edge to that parent, and "
                "uses the affected DER's access-switch model."
                if self.is_der_batch
                else
                "Installing another cabinet preserves the configured spare capacity "
                "and adds the required switches. Deferring spare capacity affects "
                "only the selected comms room; the global spare percentage remains "
                "active elsewhere."
            )
        )
        note.setWordWrap(True)
        layout.addWidget(note)

        buttons = QHBoxLayout()
        self.previous_button = QPushButton("Previous issue")
        self.previous_button.clicked.connect(self._previous_issue)
        buttons.addWidget(self.previous_button)
        buttons.addStretch(1)
        cancel_button = QPushButton("Cancel planning")
        cancel_button.clicked.connect(self.reject)
        buttons.addWidget(cancel_button)
        self.apply_button = QPushButton()
        self.apply_button.clicked.connect(self._apply_current_issue)
        buttons.addWidget(self.apply_button)
        layout.addLayout(buttons)

        if not self.issues:
            self.reject()
            return
        self._show_current_issue()

    def _current_issue(self) -> dict:
        return self.issues[self.current_index]

    def _issue_diagnostic_text(self, issue: dict) -> str:
        if self.is_der_batch:
            return "\n".join(
                [
                    f"Parent DER: {_text(issue.get('parent_location_name'))}",
                    f"Required patch positions: {int(issue.get('required_port_count_with_spare', 0) or 0)}",
                    f"Available patch positions: {int(issue.get('available_port_count', 0) or 0)}",
                    f"Additional panels required: {max(1, int(issue.get('remaining_panel_count', 1) or 1))}",
                    f"Access-switch model to clone: {_text(issue.get('switch_asset_name'))}",
                ]
            )
        actual_ports = int(issue.get("actual_port_count", 0) or 0)
        required_ports = int(
            issue.get("required_port_count_with_spare", 0) or 0
        )
        spare_percent = float(issue.get("spare_capacity_percent", 0.0) or 0.0)
        switches_without = int(issue.get("switches_without_spare", 0) or 0)
        switches_with = int(issue.get("switches_with_spare", 0) or 0)
        stacks_without = int(issue.get("stacks_without_spare", 0) or 0)
        stacks_with = int(issue.get("stacks_with_spare", 0) or 0)
        max_members = int(issue.get("max_stack_members", 0) or 0)
        extra_racks = int(issue.get("suggested_extra_rack_count", 1) or 1)
        rack_size = int(issue.get("rack_size_u", 0) or 0)
        return "\n".join(
            [
                f"Current endpoint port demand: {actual_ports}",
                f"Ports required with {spare_percent:.1f}% spare: {required_ports}",
                f"Access switches: {switches_without} without spare → "
                f"{switches_with} with spare",
                f"Logical stacks: {stacks_without} without spare → "
                f"{stacks_with} with spare",
                f"Maximum members per stack: {max_members}",
                f"Additional cabinet requirement: {max(1, extra_racks)}",
                f"Configured cabinet size: {rack_size}U" if rack_size else "",
            ]
        ).strip()

    def _show_current_issue(self) -> None:
        issue = self._current_issue()
        location = _text(issue.get("location_name"))
        count = len(self.issues)
        self.issue_progress_label.setText(
            f"Issue {self.current_index + 1} of {count}: {location}"
        )
        self.issue_message_label.setText(
            (
                f"{location} needs more local switch and patch-panel capacity. "
                "Confirm the adjacent DER, then continue to the next fault."
                if self.is_der_batch
                else
                f"Spare capacity at {location} requires another logical access "
                "switch stack and therefore another cabinet. Choose how this room "
                "should be handled, then continue to the next occurrence."
            )
        )
        self.diagnostics.setPlainText(self._issue_diagnostic_text(issue))

        saved_action = self.decisions.get(
            location, "add_adjacent_der" if self.is_der_batch else "new_rack"
        )
        index = self.action_combo.findData(saved_action)
        if index >= 0:
            self.action_combo.setCurrentIndex(index)

        self.previous_button.setEnabled(self.current_index > 0)
        is_last = self.current_index >= count - 1
        self.apply_button.setText(
            "Apply decisions and continue planning"
            if is_last
            else "Apply and show next issue"
        )
        self.apply_remaining_check.setVisible(not is_last)
        self.apply_remaining_check.setChecked(False)

    def _remember_current_selection(self) -> str:
        issue = self._current_issue()
        location = _text(issue.get("location_name"))
        action = _text(self.action_combo.currentData()).lower()
        allowed_actions = (
            {"add_adjacent_der"}
            if self.is_der_batch
            else {"new_rack", "defer"}
        )
        if action not in allowed_actions:
            return ""
        self.decisions[location] = action
        return action

    def _previous_issue(self) -> None:
        if self.current_index <= 0:
            return
        self._remember_current_selection()
        self.current_index -= 1
        self._show_current_issue()

    def _apply_current_issue(self) -> None:
        action = self._remember_current_selection()
        if not action:
            QMessageBox.warning(
                self,
                "Planning decision required",
                "Choose how the current comms room should be resolved.",
            )
            return

        if self.apply_remaining_check.isChecked():
            for issue in self.issues[self.current_index + 1 :]:
                location = _text(issue.get("location_name"))
                if location:
                    self.decisions[location] = action
            self._commit_and_accept()
            return

        if self.current_index < len(self.issues) - 1:
            self.current_index += 1
            self._show_current_issue()
            return

        self._commit_and_accept()

    def _commit_and_accept(self) -> None:
        missing = [
            _text(issue.get("location_name"))
            for issue in self.issues
            if _text(issue.get("location_name")) not in self.decisions
        ]
        if missing:
            QMessageBox.warning(
                self,
                "Incomplete planning decisions",
                "Resolve every affected comms room before continuing.",
            )
            return

        settings = self.data.setdefault("network_settings", {})
        overrides = settings.setdefault("auto_planner_resolution_overrides", {})
        if not isinstance(overrides, dict):
            overrides = {}
            settings["auto_planner_resolution_overrides"] = overrides
        if self.is_der_batch:
            additional_ders = overrides.setdefault("additional_der_by_location", {})
            additional_assets = overrides.setdefault(
                "additional_der_asset_by_location", {}
            )
            minimum_switches = overrides.setdefault(
                "minimum_access_switches_by_location", {}
            )
            for issue in self.issues:
                location = _text(issue.get("location_name"))
                if self.decisions.get(location) != "add_adjacent_der":
                    continue
                parent_location = _text(
                    issue.get("parent_location_name")
                ) or location
                requested = max(
                    1, int(issue.get("remaining_panel_count", 1) or 1)
                )
                additional_ders[parent_location] = max(
                    max(0, int(additional_ders.get(parent_location, 0) or 0)),
                    requested,
                )
                asset_id = _text(issue.get("switch_asset_id"))
                if asset_id:
                    additional_assets[parent_location] = asset_id
                minimum_switches[parent_location] = max(
                    int(minimum_switches.get(parent_location, 0) or 0),
                    int(issue.get("suggested_device_count", 0) or 0),
                )
            settings["auto_add_switches_for_bandwidth"] = True
            settings["der_expansion_inventory_signature"] = (
                der_graph_inventory_signature(self.data)
            )
        else:
            spare_modes = overrides.setdefault("spare_capacity_mode_by_location", {})
            if not isinstance(spare_modes, dict):
                spare_modes = {}
                overrides["spare_capacity_mode_by_location"] = spare_modes
            for location, action in self.decisions.items():
                spare_modes[location] = action
        self.selected_actions = dict(self.decisions)
        self.accept()


class AutoPlannerSetupWizard(QWizard):
    """Guided network hierarchy, cabinet and exact-model selection."""

    def __init__(self, parent, data: dict):
        super().__init__(parent)
        self.data = data
        self.settings = data.setdefault("network_settings", {})
        self.setWindowTitle("Automatic Network Planner Setup")
        self.resize(860, 650)
        self.setWizardStyle(QWizard.ModernStyle)
        self.setOption(QWizard.NoBackButtonOnStartPage, True)

        design_page = QWizardPage()
        design_page.setTitle("Choose the network architecture")
        design_page.setSubTitle(
            "Select the technology, hierarchy and resilience before choosing equipment models."
        )
        design_form = QFormLayout(design_page)

        self.technology_combo = QComboBox()
        self.technology_combo.addItems(NETWORK_TECHNOLOGIES)
        _set_combo_text(
            self.technology_combo,
            _text(self.settings.get("technology")) or "Traditional",
        )
        design_form.addRow("Network technology", self.technology_combo)

        self.topology_combo = QComboBox()
        self.topology_combo.addItem("Collapsed core (core + access)", "collapsed_core")
        self.topology_combo.addItem(
            "Three layer (core + aggregation + access)", "three_tier"
        )
        topology_index = self.topology_combo.findData(
            _text(self.settings.get("topology_model")) or "collapsed_core"
        )
        if topology_index >= 0:
            self.topology_combo.setCurrentIndex(topology_index)
        design_form.addRow("Traditional hierarchy", self.topology_combo)

        self.redundant_check = QCheckBox("Generate independent redundant paths")
        self.redundant_check.setChecked(
            bool(self.settings.get("redundant_core", True))
        )
        design_form.addRow("", self.redundant_check)

        self.link_count_spin = QSpinBox()
        self.link_count_spin.setRange(1, 16)
        self.link_count_spin.setValue(
            max(1, int(self.settings.get("independent_link_count", 2) or 2))
        )
        self.link_count_spin.setToolTip(
            "Number of separate uplinks to each downstream switch or stack."
        )
        design_form.addRow("Independent uplinks", self.link_count_spin)

        self.access_stacking_check = QCheckBox(
            "Stack compatible access-layer switches"
        )
        self.access_stacking_check.setChecked(
            bool(self.settings.get("access_stacking_enabled", True))
        )
        design_form.addRow("", self.access_stacking_check)
        self.access_stack_max_spin = QSpinBox()
        self.access_stack_max_spin.setRange(1, 64)
        self.access_stack_max_spin.setValue(
            max(1, int(self.settings.get("access_stack_max_members", 8) or 8))
        )
        self.access_stack_max_spin.setToolTip(
            "The selected switch model's lower manufacturer limit still applies."
        )
        design_form.addRow("Maximum access stack members", self.access_stack_max_spin)
        self.access_stack_topology_combo = QComboBox()
        self.access_stack_topology_combo.addItem("Ring (resilient)", "ring")
        self.access_stack_topology_combo.addItem("Chain", "chain")
        stack_topology_index = self.access_stack_topology_combo.findData(
            _text(self.settings.get("access_stack_topology")) or "ring"
        )
        if stack_topology_index >= 0:
            self.access_stack_topology_combo.setCurrentIndex(stack_topology_index)
        design_form.addRow("Access stack interconnect", self.access_stack_topology_combo)
        self.access_stacking_check.toggled.connect(self.access_stack_max_spin.setEnabled)
        self.access_stacking_check.toggled.connect(
            self.access_stack_topology_combo.setEnabled
        )
        self.access_stack_max_spin.setEnabled(self.access_stacking_check.isChecked())
        self.access_stack_topology_combo.setEnabled(self.access_stacking_check.isChecked())

        self.spare_spin = QDoubleSpinBox()
        self.spare_spin.setRange(0.0, 100.0)
        self.spare_spin.setDecimals(1)
        self.spare_spin.setSuffix(" %")
        self.spare_spin.setValue(
            float(self.settings.get("spare_capacity_percent", 15.0) or 0.0)
        )
        design_form.addRow("Spare port, PoE and traffic capacity", self.spare_spin)
        self.connected_data_points_only_check = QCheckBox(
            "Only account for data points with current connections"
        )
        self.connected_data_points_only_check.setChecked(
            bool(
                self.settings.get(
                    "auto_planner_connected_data_points_only", False
                )
            )
        )
        self.connected_data_points_only_check.setToolTip(
            "Size and generate the network using only data points that already have "
            "a connection on the Routing tab. Unconnected placed data points are ignored."
        )

        design_form.addRow("", self.connected_data_points_only_check)
        self.same_floor_only_check = QCheckBox(
            "Keep endpoint assignments and cable routes on the same floor"
        )
        self.same_floor_only_check.setChecked(
            bool(self.settings.get("auto_planner_same_floor_only", False))
        )
        self.same_floor_only_check.setToolTip(
            "Preserve the zone planner's same-floor rule. Existing cross-floor "
            "assignments are reported instead of being silently reassigned."
        )
        design_form.addRow("", self.same_floor_only_check)
        self.prevent_additional_rooms_check = QCheckBox(
            "Use existing DERs and comms rooms only"
        )
        self.prevent_additional_rooms_check.setChecked(
            bool(self.settings.get("prevent_additional_equipment_rooms", False))
        )
        self.prevent_additional_rooms_check.setToolTip(
            "Prevents the automatic planner from creating additional distributed "
            "equipment rooms or comms rooms. Planning stops with a capacity "
            "diagnostic if the existing rooms cannot satisfy demand."
        )
        design_form.addRow("", self.prevent_additional_rooms_check)
        self.layer_estimate_button = QPushButton("Estimate layer switch quantities")
        self.layer_estimate_button.clicked.connect(self._refresh_layer_estimate)
        design_form.addRow("", self.layer_estimate_button)
        self.layer_estimate_label = QLabel(
            "Select the planner rules, then calculate the estimated switch quantities."
        )
        self.layer_estimate_label.setWordWrap(True)
        design_form.addRow("Layer estimate", self.layer_estimate_label)
        self.addPage(design_page)

        rack_page = QWizardPage()
        rack_page.setTitle("Choose the cabinet deployment model")
        rack_page.setSubTitle(
            "Top of Rack keeps every final copper termination with its serving access stack. "
            "End of Row consolidates switching and permits patch-panel pairs to use adjacent cabinets."
        )
        rack_form = QFormLayout(rack_page)
        self.rack_model_combo = QComboBox()
        self.rack_model_combo.addItem(
            "Top of Rack — local access stack per cabinet", "top_of_rack"
        )
        self.rack_model_combo.addItem(
            "End of Row — consolidated access switching", "end_of_row"
        )
        rack_index = self.rack_model_combo.findData(
            _text(self.settings.get("rack_deployment_model")) or "end_of_row"
        )
        if rack_index >= 0:
            self.rack_model_combo.setCurrentIndex(rack_index)
        rack_form.addRow("Cabinet strategy", self.rack_model_combo)

        self.aggregation_mode_combo = QComboBox()
        self.aggregation_mode_combo.addItem(
            "Dedicated aggregation cabinet", "dedicated"
        )
        self.aggregation_mode_combo.addItem(
            "Share the End-of-Row cabinet", "shared_eor"
        )
        aggregation_index = self.aggregation_mode_combo.findData(
            _text(self.settings.get("aggregation_rack_mode")) or "dedicated"
        )
        if aggregation_index >= 0:
            self.aggregation_mode_combo.setCurrentIndex(aggregation_index)
        self.aggregation_mode_combo.setToolTip(
            "Used by three-layer Traditional designs. Dedicated mode isolates aggregation equipment; "
            "shared mode places it in the End-of-Row cabinet where capacity permits."
        )
        rack_form.addRow("Aggregation placement", self.aggregation_mode_combo)

        self.rack_size_spin = QSpinBox()
        self.rack_size_spin.setRange(1, 200)
        self.rack_size_spin.setSuffix("U")
        self.rack_size_spin.setValue(
            max(1, int(self.settings.get("default_rack_size_u", 42) or 42))
        )
        rack_form.addRow("Rack cabinet size", self.rack_size_spin)

        tor_note = QLabel(
            "With Top of Rack, the planner sizes each access stack together with its copper "
            "patch panels, cable managers and UPS allowance. Final endpoint connections cannot "
            "cross cabinets. Fibre uplinks and peer links may use the adjacent cabinet route."
        )
        tor_note.setWordWrap(True)
        rack_form.addRow("", tor_note)
        self.addPage(rack_page)

        external_page = QWizardPage()
        self.external_page = external_page
        external_page.setTitle("Define external links and edge hardware")
        external_page.setSubTitle(
            "Add internet, carrier, cloud or partner links. Enabled services cause the planner "
            "to install edge routers/firewalls and connect them to the generated core."
        )
        external_layout = QVBoxLayout(external_page)
        external_info = QLabel(
            "Bandwidth entered here is the service capacity. Edge hardware is sized against "
            "the calculated north-south endpoint demand plus the configured spare capacity; "
            "east-west traffic remains inside the switching fabric."
        )
        external_info.setWordWrap(True)
        external_layout.addWidget(external_info)
        self.external_services_table = QTableWidget(0, 9)
        self.external_services_table.setHorizontalHeaderLabels(
            [
                "Use", "Name", "Provider", "Service", "Capacity Mbps",
                "Redundant", "Links", "Medium", "Location",
            ]
        )
        self.external_services_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.external_services_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.external_services_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.external_services_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.external_services_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.external_services_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        for column in range(4, 9):
            self.external_services_table.horizontalHeader().setSectionResizeMode(
                column, QHeaderView.ResizeToContents
            )
        external_layout.addWidget(self.external_services_table, 1)
        external_buttons = QHBoxLayout()
        self.add_external_service_button = QPushButton("Add external service")
        self.remove_external_service_button = QPushButton("Remove selected")
        self.add_external_service_button.clicked.connect(
            lambda: self._append_external_service_row({})
        )
        self.remove_external_service_button.clicked.connect(
            self._remove_external_service_rows
        )
        external_buttons.addWidget(self.add_external_service_button)
        external_buttons.addWidget(self.remove_external_service_button)
        external_buttons.addStretch(1)
        external_layout.addLayout(external_buttons)

        hardware_form = QFormLayout()
        self.external_router_combo = QComboBox()
        self.external_router_combo.addItem("Automatic router/firewall selection", "")
        for asset in self._component_assets("edge_router"):
            asset_id = _text(asset.get("id"))
            self.external_router_combo.addItem(self._asset_label(asset), asset_id)
        existing_preferences = normalise_asset_model_preferences(
            self.settings.get("asset_model_preferences")
        )
        existing_edge_ids = existing_preferences.get("edge_router", {}).get(
            "preferred_asset_ids", []
        )
        if existing_edge_ids:
            selected_index = self.external_router_combo.findData(
                _text(existing_edge_ids[0])
            )
            if selected_index >= 0:
                self.external_router_combo.setCurrentIndex(selected_index)
        self.external_router_strict_check = QCheckBox(
            "Require this exact edge hardware model"
        )
        self.external_router_strict_check.setChecked(
            bool(existing_preferences.get("edge_router", {}).get("strict"))
        )
        self.external_router_strict_check.setEnabled(
            bool(_text(self.external_router_combo.currentData()))
        )
        self.external_router_combo.currentIndexChanged.connect(
            lambda _index: self.external_router_strict_check.setEnabled(
                bool(_text(self.external_router_combo.currentData()))
            )
        )
        hardware_form.addRow("Router / firewall hardware", self.external_router_combo)
        hardware_form.addRow("", self.external_router_strict_check)
        external_layout.addLayout(hardware_form)
        self.external_hardware_warning = QLabel()
        self.external_hardware_warning.setWordWrap(True)
        if not self._component_assets("edge_router"):
            self.external_hardware_warning.setText(
                "No router or firewall hardware exists in the network asset library. "
                "Create a network-router or firewall model before enabling an external service."
            )
        external_layout.addWidget(self.external_hardware_warning)

        existing_external_rows = [
            row
            for row in self.data.get("network_external_networks", [])
            if isinstance(row, dict)
        ]
        if existing_external_rows:
            for row in existing_external_rows:
                self._append_external_service_row(row)
        else:
            self._append_external_service_row(
                {
                    "id": "AUTO-EXT-1",
                    "name": "Internet / WAN",
                    "network_type": "carrier",
                    "service_type": "Internet access",
                    "planner_enabled": False,
                    "redundant": True,
                    "required_links": 2,
                    "medium": "fibre",
                    "bandwidth_mbps": 1000.0,
                }
            )
        self.addPage(external_page)

        models_page = QWizardPage()
        models_page.setTitle("Select preferred equipment models")
        models_page.setSubTitle(
            "Choose a model for any generated layer. Strict selections prohibit automatic fallback; "
            "non-strict selections are preferred but may be replaced when capacity or port compatibility requires it."
        )
        models_layout = QVBoxLayout(models_page)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        content = QWidget()
        models_form = QFormLayout(content)
        scroll.setWidget(content)
        models_layout.addWidget(scroll)

        preferences = normalise_asset_model_preferences(
            self.settings.get("asset_model_preferences")
        )
        self.model_controls: Dict[str, Tuple[QComboBox, QCheckBox]] = {
            "edge_router": (
                self.external_router_combo,
                self.external_router_strict_check,
            )
        }
        for component, label in ASSET_MODEL_PREFERENCE_COMPONENTS.items():
            if component == "edge_router":
                continue
            row_widget = QWidget()
            row_layout = QHBoxLayout(row_widget)
            row_layout.setContentsMargins(0, 0, 0, 0)
            combo = QComboBox()
            combo.addItem("Automatic selection", "")
            for asset in self._component_assets(component):
                asset_id = _text(asset.get("id"))
                combo.addItem(self._asset_label(asset), asset_id)
            selected_ids = preferences.get(component, {}).get(
                "preferred_asset_ids", []
            )
            if selected_ids:
                selected_index = combo.findData(_text(selected_ids[0]))
                if selected_index >= 0:
                    combo.setCurrentIndex(selected_index)
            strict = QCheckBox("Strict")
            strict.setChecked(bool(preferences.get(component, {}).get("strict")))
            strict.setToolTip(
                "Do not use another model when this selected model cannot satisfy the design."
            )
            strict.setEnabled(bool(_text(combo.currentData())))
            combo.currentIndexChanged.connect(
                lambda _index, c=combo, check=strict: check.setEnabled(
                    bool(_text(c.currentData()))
                )
            )
            row_layout.addWidget(combo, 1)
            row_layout.addWidget(strict)
            models_form.addRow(label, row_widget)
            self.model_controls[component] = (combo, strict)
        self.addPage(models_page)

        review_page = QWizardPage()
        review_page.setTitle("Review and generate")
        review_page.setSubTitle(
            "The selected preferences are saved with the project and used for this and later planner runs."
        )
        review_layout = QVBoxLayout(review_page)
        self.review_text = QLabel()
        self.review_text.setTextFormat(Qt.PlainText)
        self.review_text.setWordWrap(True)
        self.review_text.setTextInteractionFlags(Qt.TextSelectableByMouse)
        review_layout.addWidget(self.review_text)
        review_layout.addStretch(1)
        self.addPage(review_page)

        self.technology_combo.currentTextChanged.connect(self._update_availability)
        self.topology_combo.currentIndexChanged.connect(self._update_availability)
        self.rack_model_combo.currentIndexChanged.connect(self._update_availability)
        self.currentIdChanged.connect(self._update_review)
        self._update_availability()
        self._update_review()

    def validateCurrentPage(self) -> bool:
        if self.currentPage() is getattr(self, "external_page", None):
            enabled = [
                row
                for row in self._external_service_records()
                if bool(row.get("planner_enabled", True))
            ]
            if enabled and not self._component_assets("edge_router"):
                QMessageBox.warning(
                    self,
                    "External-link hardware required",
                    "At least one external service is enabled, but the network asset "
                    "library contains no router or firewall model. Add the hardware "
                    "to the asset library, or disable the external services before "
                    "continuing.",
                )
                return False
        return super().validateCurrentPage()

    def _append_external_service_row(self, record: dict) -> None:
        row = self.external_services_table.rowCount()
        self.external_services_table.insertRow(row)
        enabled = QCheckBox()
        enabled.setChecked(bool(record.get("planner_enabled", True)))
        enabled_wrapper = QWidget()
        enabled_layout = QHBoxLayout(enabled_wrapper)
        enabled_layout.setContentsMargins(0, 0, 0, 0)
        enabled_layout.addWidget(enabled, 0, Qt.AlignCenter)
        enabled_wrapper._check = enabled
        self.external_services_table.setCellWidget(row, 0, enabled_wrapper)

        name_item = QTableWidgetItem(
            _text(record.get("name")) or f"External service {row + 1}"
        )
        name_item.setData(Qt.UserRole, _text(record.get("id")))
        self.external_services_table.setItem(row, 1, name_item)
        self.external_services_table.setItem(
            row, 2, QTableWidgetItem(_text(record.get("provider")))
        )
        self.external_services_table.setItem(
            row,
            3,
            QTableWidgetItem(
                _text(record.get("service_type")) or "Internet access"
            ),
        )
        self.external_services_table.setItem(
            row,
            4,
            QTableWidgetItem(str(float(record.get("bandwidth_mbps", 0.0) or 0.0))),
        )

        redundant = QCheckBox()
        redundant.setChecked(bool(record.get("redundant", True)))
        redundant_wrapper = QWidget()
        redundant_layout = QHBoxLayout(redundant_wrapper)
        redundant_layout.setContentsMargins(0, 0, 0, 0)
        redundant_layout.addWidget(redundant, 0, Qt.AlignCenter)
        redundant_wrapper._check = redundant
        self.external_services_table.setCellWidget(row, 5, redundant_wrapper)

        links = QSpinBox()
        links.setRange(1, 8)
        links.setValue(max(1, int(record.get("required_links", 2) or 2)))
        self.external_services_table.setCellWidget(row, 6, links)
        medium = QComboBox()
        medium.addItems(["fibre", "copper", "wireless"])
        _set_combo_text(medium, _text(record.get("medium")) or "fibre")
        self.external_services_table.setCellWidget(row, 7, medium)
        location = QComboBox()
        location.addItem("Automatic MER / comms room", "")
        for candidate in sorted(
            (
                item
                for item in self.data.get("locations", [])
                if isinstance(item, dict)
                and _text(item.get("kind")).lower()
                in {"mer", "comms_room", "telco_pop"}
            ),
            key=lambda item: (int(item.get("floor", 0) or 0), _text(item.get("name"))),
        ):
            location.addItem(
                f"{_text(candidate.get('name'))} — Floor {candidate.get('floor', 0)}",
                _text(candidate.get("name")),
            )
        location_index = location.findData(_text(record.get("location_name")))
        if location_index >= 0:
            location.setCurrentIndex(location_index)
        self.external_services_table.setCellWidget(row, 8, location)

    def _remove_external_service_rows(self) -> None:
        rows = sorted(
            {
                index.row()
                for index in self.external_services_table.selectionModel().selectedRows()
            },
            reverse=True,
        )
        for row in rows:
            self.external_services_table.removeRow(row)

    def _external_service_records(self) -> List[dict]:
        existing_by_id = {
            _text(item.get("id")): item
            for item in self.data.get("network_external_networks", [])
            if isinstance(item, dict) and _text(item.get("id"))
        }
        records: List[dict] = []
        used_ids = set()
        for row in range(self.external_services_table.rowCount()):
            enabled_wrapper = self.external_services_table.cellWidget(row, 0)
            redundant_wrapper = self.external_services_table.cellWidget(row, 5)
            links = self.external_services_table.cellWidget(row, 6)
            medium = self.external_services_table.cellWidget(row, 7)
            location = self.external_services_table.cellWidget(row, 8)
            name_item = self.external_services_table.item(row, 1)
            provider_item = self.external_services_table.item(row, 2)
            service_item = self.external_services_table.item(row, 3)
            bandwidth_item = self.external_services_table.item(row, 4)
            record_id = _text(name_item.data(Qt.UserRole) if name_item else "")
            if not record_id or record_id in used_ids:
                sequence = 1
                while f"AUTO-EXT-{sequence}" in used_ids or f"AUTO-EXT-{sequence}" in existing_by_id:
                    sequence += 1
                record_id = f"AUTO-EXT-{sequence}"
            used_ids.add(record_id)
            try:
                bandwidth = max(0.0, float(bandwidth_item.text() if bandwidth_item else 0.0))
            except (TypeError, ValueError):
                bandwidth = 0.0
            source = deepcopy(existing_by_id.get(record_id, {}))
            source.update(
                {
                    "id": record_id,
                    "name": _text(name_item.text() if name_item else "")
                    or record_id,
                    "network_type": _text(source.get("network_type")) or "carrier",
                    "provider": _text(provider_item.text() if provider_item else ""),
                    "service_type": _text(service_item.text() if service_item else "")
                    or "External service",
                    "planner_enabled": bool(
                        enabled_wrapper
                        and enabled_wrapper._check.isChecked()
                    ),
                    "redundant": bool(
                        redundant_wrapper
                        and redundant_wrapper._check.isChecked()
                    ),
                    "required_links": int(links.value()) if isinstance(links, QSpinBox) else 1,
                    "medium": _text(medium.currentText() if isinstance(medium, QComboBox) else "fibre"),
                    "bandwidth_mbps": bandwidth,
                    "location_name": _text(location.currentData() if isinstance(location, QComboBox) else ""),
                    "router_asset_id": _text(self.external_router_combo.currentData()),
                }
            )
            records.append(source)
        return records

    def _update_review(self, *_args) -> None:
        technology = self.technology_combo.currentText().strip() or "Traditional"
        lines = [f"Technology: {technology}"]
        if technology.lower() == "traditional":
            lines.extend(
                [
                    f"Hierarchy: {self.topology_combo.currentText()}",
                    f"Resilience: {'Redundant' if self.redundant_check.isChecked() else 'Single path'}",
                    f"Independent uplinks: {self.link_count_spin.value()}",
                    f"Cabinet strategy: {self.rack_model_combo.currentText()}",
                ]
            )
            if self.aggregation_mode_combo.isEnabled():
                lines.append(
                    f"Aggregation placement: {self.aggregation_mode_combo.currentText()}"
                )
        enabled_external = [
            row for row in self._external_service_records()
            if bool(row.get("planner_enabled", True))
        ]
        lines.extend(
            [
                f"Rack size: {self.rack_size_spin.value()}U",
                f"Spare capacity: {self.spare_spin.value():.1f}%",
                "Endpoint demand: "
                + (
                    "Current Routing-tab connections only"
                    if self.connected_data_points_only_check.isChecked()
                    else "All placed data points"
                ),
                f"External services: {len(enabled_external)} enabled",
                *[
                    f"  {row.get('name')}: {float(row.get('bandwidth_mbps', 0.0) or 0.0):.3f} Mbps, "
                    f"{row.get('required_links', 1)} {row.get('medium', 'fibre')} link(s)"
                    for row in enabled_external
                ],
                "",
                "Preferred models:",
            ]
        )
        selected_count = 0
        for component, (combo, strict) in self.model_controls.items():
            if not combo.isEnabled() or not _text(combo.currentData()):
                continue
            selected_count += 1
            suffix = " (strict)" if strict.isChecked() else " (preferred)"
            lines.append(
                f"  {ASSET_MODEL_PREFERENCE_COMPONENTS.get(component, component)}: "
                f"{combo.currentText()}{suffix}"
            )
        if selected_count == 0:
            lines.append("  Automatic selection for every applicable component")
        lines.extend(
            [
                "",
                "Finish applies these settings and starts the capacity, port, rack and optical validation pass.",
            ]
        )
        self.review_text.setText("\n".join(lines))

    @staticmethod
    def _asset_label(asset: dict) -> str:
        parts = [
            _text(asset.get("manufacturer")),
            _text(asset.get("model")),
        ]
        product = " ".join(value for value in parts if value).strip()
        name = _text(asset.get("name"))
        asset_id = _text(asset.get("id"))
        if product and name and name.casefold() not in product.casefold():
            product = f"{product} — {name}"
        return f"{product or name or asset_id} [{asset_id}]"

    def _component_assets(self, component: str) -> List[dict]:
        rows = [
            row
            for row in self.data.get("network_assets", [])
            if isinstance(row, dict) and _text(row.get("id"))
        ]

        def asset_type(row: dict) -> str:
            return _text(row.get("asset_type")).lower()

        def layer(row: dict) -> str:
            return _text(
                row.get("network_layer") or row.get("design_layer")
            ).lower()

        def name(row: dict) -> str:
            return _text(row.get("name")).lower()

        if component == "access_switch":
            selected = [
                row
                for row in rows
                if asset_type(row) == "network_switch"
                and (
                    layer(row) == "access"
                    or (
                        _text(row.get("output_connection_type")).lower()
                        == "copper"
                        and not any(
                            word in name(row)
                            for word in ("core", "aggregation", "distribution")
                        )
                    )
                )
            ]
        elif component == "aggregation_switch":
            selected = [
                row
                for row in rows
                if asset_type(row) == "network_switch"
                and (
                    layer(row) in {"aggregation", "distribution"}
                    or any(word in name(row) for word in ("aggregation", "distribution"))
                )
            ]
        elif component == "core_switch":
            selected = [
                row
                for row in rows
                if asset_type(row) == "network_switch"
                and (layer(row) == "core" or "core" in name(row))
            ]
        elif component == "edge_router":
            selected = [
                row
                for row in rows
                if asset_type(row) in {"network_router", "firewall"}
            ]
        elif component == "wireless_access_point":
            selected = [
                row for row in rows
                if asset_type(row) in {"wireless_access_point", "wireless_device"}
            ]
        elif component == "optical_line_terminal":
            selected = [row for row in rows if asset_type(row) == "optical_line_terminal"]
        elif component == "optical_network_terminal":
            selected = [row for row in rows if asset_type(row) == "optical_network_terminal"]
        elif component == "fibre_splitter":
            selected = [row for row in rows if asset_type(row) == "fibre_splitter"]
        elif component == "copper_patch_panel":
            selected = [
                row
                for row in rows
                if asset_type(row) == "patch_panel"
                and _text(row.get("patch_panel_type")).lower() == "copper"
            ]
        elif component == "fibre_patch_panel":
            selected = [
                row
                for row in rows
                if asset_type(row) == "patch_panel"
                and _text(row.get("patch_panel_type")).lower() == "fibre"
            ]
        elif component == "rack_ups":
            selected = [row for row in rows if asset_type(row) == "ups"]
        elif component == "rack_pdu":
            selected = [row for row in rows if asset_type(row) == "pdu"]
        elif component == "cable_management":
            selected = [row for row in rows if asset_type(row) == "cable_management"]
        else:
            selected = []
        return sorted(
            selected,
            key=lambda row: (
                _text(row.get("manufacturer")).casefold(),
                _text(row.get("model")).casefold(),
                _text(row.get("name")).casefold(),
                _text(row.get("id")),
            ),
        )

    def _update_availability(self, *_args) -> None:
        traditional = self.technology_combo.currentText().strip().lower() == "traditional"
        three_tier = _text(self.topology_combo.currentData()) == "three_tier"
        self.topology_combo.setEnabled(traditional)
        self.rack_model_combo.setEnabled(traditional)
        self.redundant_check.setEnabled(traditional)
        self.link_count_spin.setEnabled(traditional)
        aggregation_available = (
            traditional
            and three_tier
            and _text(self.rack_model_combo.currentData()) == "end_of_row"
        )
        self.aggregation_mode_combo.setEnabled(aggregation_available)
        if not aggregation_available:
            dedicated_index = self.aggregation_mode_combo.findData("dedicated")
            if dedicated_index >= 0:
                self.aggregation_mode_combo.setCurrentIndex(dedicated_index)
        for component, (combo, strict) in self.model_controls.items():
            relevant = True
            if component in {
                "access_switch",
                "aggregation_switch",
                "copper_patch_panel",
            }:
                relevant = traditional
            elif component in {
                "optical_line_terminal",
                "optical_network_terminal",
                "fibre_splitter",
            }:
                relevant = not traditional
            if component == "aggregation_switch":
                relevant = traditional and three_tier
            combo.setEnabled(relevant)
            strict.setEnabled(relevant and bool(_text(combo.currentData())))

    def _refresh_layer_estimate(self) -> None:
        working = deepcopy(self.data)
        settings = working.setdefault("network_settings", {})
        settings["technology"] = self.technology_combo.currentText().strip()
        settings["topology_model"] = (
            _text(self.topology_combo.currentData()) or "collapsed_core"
        )
        settings["redundant_core"] = bool(self.redundant_check.isChecked())
        settings["independent_link_count"] = int(self.link_count_spin.value())
        settings["spare_capacity_percent"] = float(self.spare_spin.value())
        settings["auto_planner_connected_data_points_only"] = bool(
            self.connected_data_points_only_check.isChecked()
        )
        settings["auto_planner_same_floor_only"] = bool(
            self.same_floor_only_check.isChecked()
        )
        settings["prevent_additional_equipment_rooms"] = bool(
            self.prevent_additional_rooms_check.isChecked()
        )
        settings["access_stacking_enabled"] = bool(
            self.access_stacking_check.isChecked()
        )
        settings["access_stack_max_members"] = int(self.access_stack_max_spin.value())
        settings["access_stack_topology"] = (
            _text(self.access_stack_topology_combo.currentData()) or "ring"
        )
        settings["rack_deployment_model"] = (
            _text(self.rack_model_combo.currentData()) or "end_of_row"
        )
        settings["default_rack_size_u"] = int(self.rack_size_spin.value())
        model_preferences: Dict[str, dict] = {}
        for component, (combo, strict) in self.model_controls.items():
            asset_id = _text(combo.currentData())
            model_preferences[component] = {
                "preferred_asset_ids": [asset_id] if asset_id else [],
                "strict": bool(strict.isChecked() and asset_id),
            }
        settings["asset_model_preferences"] = normalise_asset_model_preferences(
            model_preferences
        )
        try:
            estimate = estimate_network_switch_counts(working)
            self.layer_estimate_label.setText(_layer_switch_estimate_text(estimate))
        except NetworkPlanningError as exc:
            self.layer_estimate_label.setText(f"Estimate unavailable: {exc}")

    def apply_settings(self) -> None:
        settings = self.data.setdefault("network_settings", {})
        technology = self.technology_combo.currentText().strip()
        settings["technology"] = technology
        settings["topology_model"] = (
            _text(self.topology_combo.currentData()) or "collapsed_core"
        )
        settings["redundant_core"] = bool(self.redundant_check.isChecked())
        settings["independent_link_count"] = int(self.link_count_spin.value())
        settings["access_stacking_enabled"] = bool(
            self.access_stacking_check.isChecked()
        )
        settings["access_stack_max_members"] = int(self.access_stack_max_spin.value())
        settings["access_stack_topology"] = (
            _text(self.access_stack_topology_combo.currentData()) or "ring"
        )
        settings["spare_capacity_percent"] = float(self.spare_spin.value())
        settings["auto_planner_connected_data_points_only"] = bool(
            self.connected_data_points_only_check.isChecked()
        )
        settings["auto_planner_same_floor_only"] = bool(
            self.same_floor_only_check.isChecked()
        )
        settings["prevent_additional_equipment_rooms"] = bool(
            self.prevent_additional_rooms_check.isChecked()
        )
        settings["rack_deployment_model"] = (
            _text(self.rack_model_combo.currentData()) or "end_of_row"
        )
        aggregation_mode = (
            _text(self.aggregation_mode_combo.currentData()) or "dedicated"
        )
        if (
            technology.lower() != "traditional"
            or settings["topology_model"] != "three_tier"
            or settings["rack_deployment_model"] != "end_of_row"
        ):
            aggregation_mode = "dedicated"
        settings["aggregation_rack_mode"] = aggregation_mode
        settings["default_rack_size_u"] = int(self.rack_size_spin.value())
        settings["tor_keep_final_connections_in_cabinet"] = True
        settings["tor_allow_adjacent_cabinet_uplinks"] = True

        preferences: Dict[str, dict] = {}
        for component, (combo, strict) in self.model_controls.items():
            asset_id = _text(combo.currentData())
            preferences[component] = {
                "preferred_asset_ids": [asset_id] if asset_id else [],
                "strict": bool(strict.isChecked() and asset_id),
            }
        settings["asset_model_preferences"] = normalise_asset_model_preferences(
            preferences
        )
        external_records = self._external_service_records()
        self.data["network_external_networks"] = external_records
        enabled_external = [
            row for row in external_records
            if bool(row.get("planner_enabled", True))
        ]
        settings["external_network_redundancy"] = any(
            bool(row.get("redundant", True)) for row in enabled_external
        )
        settings["external_network_link_count"] = max(
            [int(row.get("required_links", 1) or 1) for row in enabled_external]
            or [1]
        )


class NetworkPlannerDialog(QDialog):
    def __init__(self, parent, data: dict, on_save: Callable[[dict], None]):
        super().__init__(parent)
        self.setWindowTitle("Network Planning")
        screen = parent.screen() if parent is not None and hasattr(parent, "screen") else QApplication.primaryScreen()
        available = screen.availableGeometry() if screen is not None else None
        target_width = min(1120, max(720, available.width() - 120)) if available is not None else 1100
        target_height = min(720, max(520, available.height() - 140)) if available is not None else 700
        self.resize(target_width, target_height)
        self.setMinimumSize(min(720, target_width), min(500, target_height))
        self.data = _planner_working_copy(data)
        self.on_save = on_save

        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs, 1)

        settings_tab = QScrollArea()
        settings_tab.setWidgetResizable(True)
        settings_tab.setFrameShape(QScrollArea.NoFrame)
        settings_tab.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        settings_tab.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        settings_content = QWidget()
        settings_content.setObjectName("networkPlannerSettingsContent")
        settings_layout = QFormLayout(settings_content)
        settings_layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        settings_layout.setRowWrapPolicy(QFormLayout.WrapLongRows)
        settings_tab.setWidget(settings_content)
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

        self.rack_deployment_combo = QComboBox()
        self.rack_deployment_combo.addItem(
            "Top of Rack — local access stack per cabinet", "top_of_rack"
        )
        self.rack_deployment_combo.addItem(
            "End of Row — consolidated access switching", "end_of_row"
        )
        rack_index = self.rack_deployment_combo.findData(
            _text(settings.get("rack_deployment_model")) or "end_of_row"
        )
        if rack_index >= 0:
            self.rack_deployment_combo.setCurrentIndex(rack_index)
        self.rack_deployment_combo.setToolTip(
            "Top of Rack keeps final copper patching inside the access-switch cabinet. "
            "End of Row permits consolidated switching across adjacent cabinets."
        )

        self.aggregation_rack_combo = QComboBox()
        self.aggregation_rack_combo.addItem(
            "Dedicated aggregation cabinet", "dedicated"
        )
        self.aggregation_rack_combo.addItem(
            "Share End-of-Row cabinet", "shared_eor"
        )
        aggregation_index = self.aggregation_rack_combo.findData(
            _text(settings.get("aggregation_rack_mode")) or "dedicated"
        )
        if aggregation_index >= 0:
            self.aggregation_rack_combo.setCurrentIndex(aggregation_index)

        self.independent_link_count_spin = QSpinBox()
        self.independent_link_count_spin.setRange(1, 16)
        self.independent_link_count_spin.setValue(
            max(1, int(settings.get("independent_link_count", 2) or 2))
        )
        self.independent_link_count_spin.setToolTip(
            "Default number of separate uplinks generated for each downstream device. "
            "Redundant designs require at least two distinct source devices and two different core paths."
        )
        self.access_stacking_check = QCheckBox(
            "Stack compatible access-layer switches"
        )
        self.access_stacking_check.setChecked(
            bool(settings.get("access_stacking_enabled", True))
        )
        self.access_stack_max_spin = QSpinBox()
        self.access_stack_max_spin.setRange(1, 64)
        self.access_stack_max_spin.setValue(
            max(1, int(settings.get("access_stack_max_members", 8) or 8))
        )
        self.access_stack_max_spin.setToolTip(
            "The lower of this value and the switch model's stacking limit is used."
        )
        self.access_stack_topology_combo = QComboBox()
        self.access_stack_topology_combo.addItem("Ring (resilient)", "ring")
        self.access_stack_topology_combo.addItem("Chain", "chain")
        stack_topology_index = self.access_stack_topology_combo.findData(
            _text(settings.get("access_stack_topology")) or "ring"
        )
        if stack_topology_index >= 0:
            self.access_stack_topology_combo.setCurrentIndex(stack_topology_index)
        self.access_stacking_check.toggled.connect(self.access_stack_max_spin.setEnabled)
        self.access_stacking_check.toggled.connect(
            self.access_stack_topology_combo.setEnabled
        )
        self.access_stack_max_spin.setEnabled(self.access_stacking_check.isChecked())
        self.access_stack_topology_combo.setEnabled(self.access_stacking_check.isChecked())
        self.layer_estimate_button = QPushButton("Estimate layer switch quantities")
        self.layer_estimate_button.clicked.connect(self._refresh_layer_estimate)
        self.layer_estimate_label = QLabel(
            "Calculate an estimated minimum before generating the design."
        )
        self.layer_estimate_label.setWordWrap(True)
        self.spare_capacity_spin = QDoubleSpinBox()
        self.spare_capacity_spin.setRange(0.0, 100.0)
        self.spare_capacity_spin.setDecimals(1)
        self.spare_capacity_spin.setSuffix(" %")
        self.spare_capacity_spin.setValue(
            float(settings.get("spare_capacity_percent", 15.0) or 0.0)
        )

        self.default_north_south_bandwidth_spin = QDoubleSpinBox()
        self.default_north_south_bandwidth_spin.setRange(0.0, 1_000_000_000.0)
        self.default_north_south_bandwidth_spin.setDecimals(3)
        self.default_north_south_bandwidth_spin.setSuffix(" Mbps")
        self.default_north_south_bandwidth_spin.setValue(
            float(
                settings.get(
                    "default_north_south_bandwidth_mbps",
                    settings.get("default_expected_bandwidth_mbps", 0.0),
                )
                or 0.0
            )
        )
        self.default_north_south_bandwidth_spin.setToolTip(
            "Default WAN/internet traffic for a generic endpoint port."
        )

        self.default_east_west_bandwidth_spin = QDoubleSpinBox()
        self.default_east_west_bandwidth_spin.setRange(0.0, 1_000_000_000.0)
        self.default_east_west_bandwidth_spin.setDecimals(3)
        self.default_east_west_bandwidth_spin.setSuffix(" Mbps")
        self.default_east_west_bandwidth_spin.setValue(
            float(settings.get("default_east_west_bandwidth_mbps", 0.0) or 0.0)
        )
        self.default_east_west_bandwidth_spin.setToolTip(
            "Default internal traffic for a generic endpoint port."
        )

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

        self.connected_data_points_only_check = QCheckBox(
            "Only account for data points with current connections"
        )
        self.connected_data_points_only_check.setChecked(
            bool(settings.get("auto_planner_connected_data_points_only", False))
        )
        self.connected_data_points_only_check.setToolTip(
            "Size and generate the network using only data points that already have "
            "a connection on the Routing tab. Unconnected placed data points are ignored."
        )

        self.same_floor_only_check = QCheckBox(
            "Keep endpoint assignments and cable routes on the same floor"
        )
        self.same_floor_only_check.setChecked(
            bool(settings.get("auto_planner_same_floor_only", False))
        )
        self.same_floor_only_check.setToolTip(
            "Use the same hard floor restriction as zone-based design. The "
            "planner will not select a room or route through nodes on another floor."
        )

        self.prevent_additional_rooms_check = QCheckBox(
            "Prevent the automatic planner from creating additional DERs or comms rooms"
        )
        self.prevent_additional_rooms_check.setChecked(
            bool(settings.get("prevent_additional_equipment_rooms", False))
        )
        self.prevent_additional_rooms_check.setToolTip(
            "Reuse existing DERs and comms rooms only. If their capacity is "
            "insufficient, the planner stops and reports the affected location."
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

        self.auto_design_button = QPushButton("Launch Guided Automatic Planner")
        self.auto_design_button.setToolTip(
            "Open the guided architecture, rack strategy and model-selection wizard, then generate the network."
        )
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
        settings_layout.addRow("Rack deployment model", self.rack_deployment_combo)
        settings_layout.addRow("Aggregation rack placement", self.aggregation_rack_combo)
        settings_layout.addRow(
            "Independent links per target", self.independent_link_count_spin
        )
        settings_layout.addRow("", self.access_stacking_check)
        settings_layout.addRow(
            "Maximum access stack members", self.access_stack_max_spin
        )
        settings_layout.addRow(
            "Access stack interconnect", self.access_stack_topology_combo
        )
        settings_layout.addRow("", self.layer_estimate_button)
        settings_layout.addRow("Layer estimate", self.layer_estimate_label)
        settings_layout.addRow("Spare port, PoE and traffic capacity", self.spare_capacity_spin)
        settings_layout.addRow(
            "Default north-south endpoint traffic",
            self.default_north_south_bandwidth_spin,
        )
        settings_layout.addRow(
            "Default east-west endpoint traffic",
            self.default_east_west_bandwidth_spin,
        )
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
        settings_layout.addRow("", self.connected_data_points_only_check)
        settings_layout.addRow("", self.same_floor_only_check)
        settings_layout.addRow("", self.prevent_additional_rooms_check)
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
        self.topology_model_combo.currentIndexChanged.connect(
            self._update_layer_rule_availability
        )
        self.rack_deployment_combo.currentIndexChanged.connect(
            self._update_layer_rule_availability
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
                "Asset group",
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
                "North-south Mbps",
                "NS concurrency %",
                "East-west Mbps",
                "EW concurrency %",
                "Expected pps",
                "Rack U",
                "Switch U",
                "OLT units/U",
            ]
        )
        self.assets_tab.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.asset_group_by_combo = QComboBox()
        self.asset_group_by_combo.addItem("Network type / role", "type")
        self.asset_group_by_combo.addItem("Manufacturer", "manufacturer")
        self.asset_group_filter_combo = QComboBox()
        self.asset_group_filter_combo.setMinimumContentsLength(24)
        self.assets_tab.filter_layout.addWidget(QLabel("Group by"))
        self.assets_tab.filter_layout.addWidget(self.asset_group_by_combo)
        self.assets_tab.filter_layout.addWidget(QLabel("Show"))
        self.assets_tab.filter_layout.addWidget(self.asset_group_filter_combo, 1)
        self.asset_group_by_combo.currentIndexChanged.connect(
            self._network_asset_grouping_changed
        )
        self.asset_group_filter_combo.currentIndexChanged.connect(
            self._network_asset_filter_changed
        )
        self.instances_tab = _CrudTab(
            [
                "ID",
                "Name",
                "Asset",
                "Location",
                "Floor",
                "Department",
                "Route anchor",
                "North-south Mbps",
                "East-west Mbps",
                "Total Mbps",
                "Rack",
                "Start U",
                "Rack U",
                "Management IP",
            ]
        )
        self.instances_tab.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.instances_tab.delete_button.setText("Delete selected")
        self.instances_tab.delete_button.setToolTip(
            "Delete every selected installed asset and its dependent network records."
        )
        self.connections_tab = _CrudTab(
            [
                "ID", "From", "Port", "To", "Port", "Role", "Medium",
                "North-south Mbps", "East-west Mbps", "Total Mbps",
                "Cable", "VLANs",
            ]
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
            "North-south traffic reaches routers, WAN, cloud or internet links. East-west "
            "traffic remains within the site switching fabric. For assets with multiple "
            "data ports, the planner apportions both values across those ports. Directional "
            "concurrency controls the busy-hour percentage applied before demand is summed. "
            "Profiles can be applied in bulk to project endpoint assets and wireless "
            "network-asset models."
        )
        endpoint_traffic_info.setWordWrap(True)
        endpoint_traffic_layout.addWidget(endpoint_traffic_info)
        profile_row = QHBoxLayout()
        self.usage_profile_combo = QComboBox()
        for profile in sorted(
            (
                item
                for item in self.data.get("network_usage_profiles", [])
                if isinstance(item, dict) and _text(item.get("id"))
            ),
            key=lambda item: (_text(item.get("name")).casefold(), _text(item.get("id"))),
        ):
            self.usage_profile_combo.addItem(
                f"{_text(profile.get('name'))} — "
                f"NS {float(profile.get('north_south_bandwidth_mbps', 0.0) or 0.0):g} Mbps / "
                f"{100.0 * float(profile.get('north_south_concurrency_factor', 1.0) or 0.0):g}% concurrent; "
                f"EW {float(profile.get('east_west_bandwidth_mbps', 0.0) or 0.0):g} Mbps / "
                f"{100.0 * float(profile.get('east_west_concurrency_factor', 1.0) or 0.0):g}% concurrent",
                _text(profile.get("id")),
            )
        self.apply_usage_profile_button = QPushButton("Apply profile to selected assets")
        self.apply_usage_profile_button.clicked.connect(self._apply_usage_profile_to_selected)
        profile_row.addWidget(QLabel("Typical usage profile"))
        profile_row.addWidget(self.usage_profile_combo, 1)
        profile_row.addWidget(self.apply_usage_profile_button)
        endpoint_traffic_layout.addLayout(profile_row)
        self.endpoint_traffic_table = QTableWidget(0, 9)
        self.endpoint_traffic_table.setHorizontalHeaderLabels(
            [
                "Asset ID", "Asset name", "Data ports", "Usage profile",
                "North-south Mbps", "NS concurrency %", "East-west Mbps",
                "EW concurrency %", "Expected packet rate pps",
            ]
        )
        self.endpoint_traffic_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.endpoint_traffic_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(7, QHeaderView.ResizeToContents)
        self.endpoint_traffic_table.horizontalHeader().setSectionResizeMode(8, QHeaderView.ResizeToContents)
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
        self.import_assets_button = QPushButton("Import asset pack...")
        self.export_selected_assets_button = QPushButton("Export selected...")
        self.export_all_assets_button = QPushButton("Export library...")
        self.import_assets_button.clicked.connect(self.import_network_assets)
        self.export_selected_assets_button.clicked.connect(
            self.export_selected_network_assets
        )
        self.export_all_assets_button.clicked.connect(
            self.export_network_asset_library
        )
        self.assets_tab.button_layout.insertWidget(3, self.import_assets_button)
        self.assets_tab.button_layout.insertWidget(
            4, self.export_selected_assets_button
        )
        self.assets_tab.button_layout.insertWidget(5, self.export_all_assets_button)

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

    def _network_asset_group_details(self, asset: dict) -> Tuple[str, str, object]:
        mode = _text(self.asset_group_by_combo.currentData()) or "type"
        if mode == "manufacturer":
            label = _text(asset.get("manufacturer")) or "Unspecified manufacturer"
            return label.casefold(), label, label.casefold()
        group = network_asset_group(asset)
        label = NETWORK_ASSET_GROUP_LABELS.get(group, "Other")
        return group, label, NETWORK_ASSET_GROUP_ORDER.get(group, 999)

    def _refresh_network_asset_filter_options(self) -> None:
        combo = self.asset_group_filter_combo
        previous = combo.currentData()
        mode = _text(self.asset_group_by_combo.currentData()) or "type"
        groups = {}
        for asset in self._items("network_assets"):
            key, label, sort_key = self._network_asset_group_details(asset)
            groups.setdefault(key, (label, sort_key))
        ordered = sorted(
            groups.items(), key=lambda row: (row[1][1], row[1][0].casefold())
        )
        combo.blockSignals(True)
        combo.clear()
        combo.addItem(
            "All network types / roles" if mode == "type" else "All manufacturers",
            "",
        )
        for key, (label, _sort_key) in ordered:
            combo.addItem(label, key)
        index = combo.findData(previous)
        combo.setCurrentIndex(index if index >= 0 else 0)
        combo.blockSignals(False)
        self.assets_tab.table.horizontalHeaderItem(2).setText(
            "Asset group" if mode == "type" else "Manufacturer"
        )

    def _network_asset_grouping_changed(self, *_args) -> None:
        self._refresh_network_asset_filter_options()
        self._refresh_tab(self.assets_tab)

    def _network_asset_filter_changed(self, *_args) -> None:
        self._refresh_tab(self.assets_tab)

    @staticmethod
    def _planning_error_asset_focus(error: NetworkPlanningError) -> dict:
        """Return exact asset IDs and fallback layer/type hints from a fault."""
        details = dict(getattr(error, "details", {}) or {})
        asset_ids = set()
        role_hints = set()

        for key, value in details.items():
            key_text = _text(key).lower()
            if key_text.endswith("asset_id"):
                asset_id = _text(value)
                if asset_id:
                    asset_ids.add(asset_id)
            if key_text.endswith("alternatives") and isinstance(value, list):
                for row in value:
                    if not isinstance(row, dict):
                        continue
                    asset_id = _text(row.get("asset_id"))
                    if asset_id:
                        asset_ids.add(asset_id)

        for key in ("from_role", "to_role", "asset_role", "network_layer"):
            value = _text(details.get(key)).lower()
            if value:
                role_hints.add(value)

        scope = _text(details.get("capacity_scope")).lower()
        label = _text(details.get("label")).lower()
        error_code = _text(getattr(error, "code", "")).lower()
        combined = " ".join([scope, label, error_code, *sorted(role_hints)])
        if "access" in combined:
            role_hints.add("access")
        if "aggregation" in combined or "distribution" in combined:
            role_hints.add("aggregation")
        if "core" in combined or "upstream" in combined:
            role_hints.add("core")
        if "edge_router" in combined or "external_north_south" in combined:
            role_hints.add("edge_router")
        if "ont" in combined:
            role_hints.add("ont")
        if "olt" in combined:
            role_hints.add("olt")

        return {
            "asset_ids": asset_ids,
            "role_hints": role_hints,
            "error_code": error_code or "planning constraint",
        }

    @staticmethod
    def _asset_matches_planning_roles(asset: dict, role_hints: set) -> bool:
        asset_type = _text(asset.get("asset_type")).lower()
        layer = _text(
            asset.get("network_layer") or asset.get("design_layer")
        ).lower()
        name = _text(asset.get("name")).lower()
        output_type = _text(asset.get("output_connection_type")).lower()
        for role in role_hints:
            role = _text(role).lower()
            if role in {"access", "access_switch"} and (
                asset_type == "network_switch"
                and (
                    layer == "access"
                    or output_type == "copper"
                    or "access" in name
                )
            ):
                return True
            if role in {"aggregation", "aggregation_switch", "distribution"} and (
                asset_type == "network_switch"
                and (
                    layer in {"aggregation", "distribution"}
                    or "aggregation" in name
                    or "distribution" in name
                )
            ):
                return True
            if role in {"core", "core_switch", "upstream"} and (
                asset_type == "network_switch"
                and (layer == "core" or "core" in name)
            ):
                return True
            if role == "edge_router" and asset_type in {
                "network_router", "firewall"
            }:
                return True
            if role in {"ont", "optical_network_terminal"} and asset_type == "optical_network_terminal":
                return True
            if role in {"olt", "optical_line_terminal"} and asset_type == "optical_line_terminal":
                return True
        return False

    def _focus_assets_for_planning_error(self, error: NetworkPlanningError) -> None:
        """Highlight and select Asset Library rows implicated by a planner fault."""
        focus = self._planning_error_asset_focus(error)
        assets = self._items("network_assets")
        exact_rows = [
            index
            for index, asset in enumerate(assets)
            if _text(asset.get("id")) in focus["asset_ids"]
        ]
        matched_rows = list(exact_rows)
        if not matched_rows:
            matched_rows = [
                index
                for index, asset in enumerate(assets)
                if self._asset_matches_planning_roles(asset, focus["role_hints"])
            ]

        if self.asset_group_filter_combo.currentIndex() > 0:
            self.asset_group_filter_combo.blockSignals(True)
            self.asset_group_filter_combo.setCurrentIndex(0)
            self.asset_group_filter_combo.blockSignals(False)
            self._refresh_tab(self.assets_tab)

        table = self.assets_tab.table
        table.clearSelection()
        display_rows = [
            self.assets_tab.display_row_for_source_index(source_row)
            for source_row in matched_rows
        ]
        display_rows = [row for row in display_rows if row >= 0]
        for row in display_rows:
            for column in range(table.columnCount()):
                item = table.item(row, column)
                if item is None:
                    continue
                item.setBackground(Qt.GlobalColor.yellow)
                font = item.font()
                font.setBold(True)
                item.setFont(font)

        if display_rows:
            first_item = table.item(display_rows[0], 0)
            if first_item is not None:
                table.setCurrentItem(first_item)
                table.scrollToItem(first_item, QAbstractItemView.PositionAtCenter)
            for row in display_rows:
                for column in range(table.columnCount()):
                    item = table.item(row, column)
                    if item is not None:
                        item.setSelected(True)
            match_kind = "exact model" if exact_rows else "relevant model type"
            self.assets_tab.notice_label.setText(
                f"Planner review: highlighted {len(display_rows)} {match_kind} "
                f"row(s) involved in {focus['error_code']}. Double-click a row to edit it."
            )
        else:
            self.assets_tab.notice_label.setText(
                "Planner review: the fault did not identify a model currently present "
                "in the Asset Library. The full library is shown so the missing or "
                "incompatible model can be added or corrected."
            )
        self.assets_tab.notice_label.show()

    def _refresh_current_tab(self, *_args) -> None:
        widget = self.tabs.currentWidget()
        if widget in self._dirty_tabs:
            self._refresh_tab(widget)

    def _refresh_tab(self, widget) -> None:
        key = self._data_tabs.get(widget)
        if not key:
            return
        if key == "assets":
            self._refresh_network_asset_filter_options()
            selected_group = _text(self.asset_group_filter_combo.currentData())
            entries = []
            for source_index, item in enumerate(self._items("network_assets")):
                group_key, group_label, group_sort = self._network_asset_group_details(item)
                if selected_group and group_key != selected_group:
                    continue
                entries.append((group_sort, group_label.casefold(), _text(item.get("name")).casefold(), _text(item.get("id")), source_index, item, group_label))
            entries.sort(key=lambda row: row[:5])
            self.assets_tab.set_rows(
                [
                    [item.get("id", ""), item.get("name", ""), group_label,
                     item.get("asset_type", ""), item.get("number_of_ports", 0),
                     item.get("connections_in", 0), item.get("connections_out", 0),
                     item.get("uplink_ports", 0),
                     "Yes" if item.get("supports_stacking", False) else "No",
                     item.get("max_stack_members", 1), item.get("power_input_w", 0),
                     item.get("poe_budget_w", 0), item.get("bandwidth_capacity_gbps", 0),
                     item.get("packet_throughput_mpps", 0),
                     item.get("expected_north_south_bandwidth_mbps", item.get("expected_bandwidth_mbps", 0)),
                     round(100.0 * float(item.get("north_south_concurrency_factor", 1.0) or 0.0), 3),
                     item.get("expected_east_west_bandwidth_mbps", 0),
                     round(100.0 * float(item.get("east_west_concurrency_factor", 1.0) or 0.0), 3),
                     item.get("expected_packet_rate_pps", 0), item.get("rack_units", 0),
                     item.get("switch_rack_unit_allowance", 0), item.get("olt_units_per_rack_unit", 1)]
                    for _sort, _label_sort, _name, _id, _source, item, group_label in entries
                ],
                source_indices=[entry[4] for entry in entries],
            )
        elif key == "endpoint_traffic":
            self._refresh_endpoint_traffic_table()
        elif key == "instances":
            self.instances_tab.set_rows([
                [item.get("id", ""), item.get("name", ""), item.get("asset_id", ""),
                 item.get("location_name", ""), item.get("floor", 0),
                 item.get("department_name", "") or item.get("department_id", ""),
                 item.get("route_anchor", ""),
                 item.get("expected_north_south_bandwidth_mbps", item.get("expected_bandwidth_mbps", 0)),
                 item.get("expected_east_west_bandwidth_mbps", 0),
                 item.get("expected_bandwidth_mbps", 0),
                 item.get("rack_name", ""),
                 item.get("rack_start_u", 0), item.get("rack_size_u", 0) or "",
                 item.get("management_ip", "")]
                for item in self._items("network_asset_instances")
            ])
        elif key == "connections":
            self.connections_tab.set_rows([
                [item.get("id", ""), item.get("from_instance_id", ""), item.get("from_port", ""),
                 item.get("to_instance_id", ""), item.get("to_port", ""),
                 item.get("connection_role", ""), item.get("medium", ""),
                 item.get("expected_north_south_bandwidth_mbps", item.get("expected_bandwidth_mbps", 0)),
                 item.get("expected_east_west_bandwidth_mbps", 0),
                 item.get("expected_bandwidth_mbps", 0),
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

    def _traffic_profile_asset_entries(self) -> List[Tuple[str, dict]]:
        """Return endpoint and wireless asset models eligible for usage profiles."""

        entries: List[Tuple[str, dict]] = [
            ("project", item)
            for item in self.data.get("assets", [])
            if isinstance(item, dict) and _text(item.get("id"))
        ]
        entries.extend(
            ("network", item)
            for item in self.data.get("network_assets", [])
            if isinstance(item, dict)
            and _text(item.get("id"))
            and _text(item.get("asset_type")).lower()
            in {"wireless_access_point", "wireless_device"}
        )
        return sorted(
            entries,
            key=lambda row: (
                _text(row[1].get("name")).casefold(),
                _text(row[1].get("id")),
                row[0],
            ),
        )

    def _traffic_profile_asset(self, key) -> Optional[dict]:
        source = "project"
        asset_id = ""
        if isinstance(key, (tuple, list)) and len(key) >= 2:
            source = _text(key[0]) or "project"
            asset_id = _text(key[1])
        else:
            asset_id = _text(key)
        collection = (
            self.data.get("network_assets", [])
            if source == "network"
            else self.data.get("assets", [])
        )
        return next(
            (
                row
                for row in collection
                if isinstance(row, dict) and _text(row.get("id")) == asset_id
            ),
            None,
        )

    def _refresh_endpoint_traffic_table(self) -> None:
        table = self.endpoint_traffic_table
        profiles = {
            _text(item.get("id")): item
            for item in self.data.get("network_usage_profiles", [])
            if isinstance(item, dict) and _text(item.get("id"))
        }
        table.blockSignals(True)
        table.setRowCount(0)
        for source, asset in self._traffic_profile_asset_entries():
            row = table.rowCount()
            table.insertRow(row)
            profile_id = _text(asset.get("usage_profile_id"))
            profile_name = _text(profiles.get(profile_id, {}).get("name"))
            legacy = max(
                0.0, float(asset.get("expected_bandwidth_mbps", 0.0) or 0.0)
            )
            has_directional = (
                "expected_north_south_bandwidth_mbps" in asset
                or "expected_east_west_bandwidth_mbps" in asset
            )
            name = _text(asset.get("name")) or _text(asset.get("id"))
            if source == "network":
                name = f"{name} [wireless network asset]"
            values = [
                _text(asset.get("id")),
                name,
                (
                    max(1, int(asset.get("number_of_ports", 1) or 1))
                    if source == "network"
                    else max(0, int(asset.get("data_points", 0) or 0))
                ),
                profile_name or profile_id,
                max(
                    0.0,
                    float(
                        asset.get(
                            "expected_north_south_bandwidth_mbps",
                            legacy if not has_directional else 0.0,
                        )
                        or 0.0
                    ),
                ),
                100.0
                * min(
                    1.0,
                    max(
                        0.0,
                        float(
                            asset.get("north_south_concurrency_factor", 1.0)
                            or 0.0
                        ),
                    ),
                ),
                max(
                    0.0,
                    float(
                        asset.get("expected_east_west_bandwidth_mbps", 0.0)
                        or 0.0
                    ),
                ),
                100.0
                * min(
                    1.0,
                    max(
                        0.0,
                        float(
                            asset.get("east_west_concurrency_factor", 1.0)
                            or 0.0
                        ),
                    ),
                ),
                max(
                    0.0,
                    float(asset.get("expected_packet_rate_pps", 0.0) or 0.0),
                ),
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value))
                if column < 4:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                if column == 0:
                    item.setData(Qt.UserRole, (source, values[0]))
                table.setItem(row, column, item)
        table.blockSignals(False)

    def _endpoint_traffic_changed(self, item: QTableWidgetItem) -> None:
        if item.column() not in {4, 5, 6, 7, 8}:
            return
        id_item = self.endpoint_traffic_table.item(item.row(), 0)
        key = id_item.data(Qt.UserRole) if id_item else ""
        asset = self._traffic_profile_asset(key)
        if asset is None:
            return
        try:
            value = max(0.0, float(item.text()))
        except (TypeError, ValueError):
            value = 0.0
        if item.column() in {5, 7}:
            value = min(100.0, value)
        field = {
            4: "expected_north_south_bandwidth_mbps",
            5: "north_south_concurrency_factor",
            6: "expected_east_west_bandwidth_mbps",
            7: "east_west_concurrency_factor",
            8: "expected_packet_rate_pps",
        }[item.column()]
        asset[field] = value / 100.0 if item.column() in {5, 7} else value
        if item.column() in {4, 6}:
            asset["expected_bandwidth_mbps"] = round(
                max(
                    0.0,
                    float(
                        asset.get(
                            "expected_north_south_bandwidth_mbps", 0.0
                        )
                        or 0.0
                    ),
                )
                + max(
                    0.0,
                    float(
                        asset.get("expected_east_west_bandwidth_mbps", 0.0)
                        or 0.0
                    ),
                ),
                6,
            )
        if item.column() in {4, 5, 6, 7}:
            asset["usage_profile_id"] = ""
            profile_item = self.endpoint_traffic_table.item(item.row(), 3)
            if profile_item is not None:
                profile_item.setText("")
        item.setText(str(value))

    def _apply_usage_profile_to_selected(self) -> None:
        profile_id = _text(self.usage_profile_combo.currentData())
        profile = next(
            (
                row
                for row in self.data.get("network_usage_profiles", [])
                if isinstance(row, dict) and _text(row.get("id")) == profile_id
            ),
            None,
        )
        if profile is None:
            return
        selected_rows = sorted(
            {
                index.row()
                for index in self.endpoint_traffic_table.selectionModel().selectedRows()
            }
        )
        if not selected_rows:
            QMessageBox.information(
                self,
                "Apply usage profile",
                "Select one or more endpoint or wireless-asset rows first.",
            )
            return
        updated = 0
        updated_instances = 0
        for row in selected_rows:
            id_item = self.endpoint_traffic_table.item(row, 0)
            asset_key = id_item.data(Qt.UserRole) if id_item else ""
            asset = self._traffic_profile_asset(asset_key)
            if asset is None:
                continue
            north_south = max(
                0.0,
                float(profile.get("north_south_bandwidth_mbps", 0.0) or 0.0),
            )
            east_west = max(
                0.0,
                float(profile.get("east_west_bandwidth_mbps", 0.0) or 0.0),
            )
            north_south_factor = min(
                1.0,
                max(
                    0.0,
                    float(profile.get("north_south_concurrency_factor", 1.0) or 0.0),
                ),
            )
            east_west_factor = min(
                1.0,
                max(
                    0.0,
                    float(profile.get("east_west_concurrency_factor", 1.0) or 0.0),
                ),
            )
            asset["usage_profile_id"] = profile_id
            asset["expected_north_south_bandwidth_mbps"] = north_south
            asset["expected_east_west_bandwidth_mbps"] = east_west
            asset["north_south_concurrency_factor"] = north_south_factor
            asset["east_west_concurrency_factor"] = east_west_factor
            asset["expected_bandwidth_mbps"] = round(
                north_south + east_west, 6
            )
            asset["expected_packet_rate_pps"] = max(
                0.0,
                float(profile.get("expected_packet_rate_pps", 0.0) or 0.0),
            )
            asset["poe_power_w"] = max(
                0.0, float(profile.get("poe_power_w", 0.0) or 0.0)
            )
            source = (
                _text(asset_key[0])
                if isinstance(asset_key, (tuple, list)) and asset_key
                else "project"
            )
            if source == "network":
                asset_id = _text(asset.get("id"))
                for instance in self.data.get("network_asset_instances", []):
                    if (
                        not isinstance(instance, dict)
                        or _text(instance.get("asset_id")) != asset_id
                    ):
                        continue
                    instance["usage_profile_id"] = profile_id
                    instance["expected_north_south_bandwidth_mbps"] = north_south
                    instance["expected_east_west_bandwidth_mbps"] = east_west
                    instance["north_south_concurrency_factor"] = north_south_factor
                    instance["east_west_concurrency_factor"] = east_west_factor
                    instance["expected_bandwidth_mbps"] = round(
                        north_south + east_west, 6
                    )
                    instance["expected_packet_rate_pps"] = max(
                        0.0,
                        float(
                            profile.get("expected_packet_rate_pps", 0.0) or 0.0
                        ),
                    )
                    updated_instances += 1
            updated += 1
        self._refresh_endpoint_traffic_table()
        QMessageBox.information(
            self,
            "Apply usage profile",
            f"Applied {_text(profile.get('name')) or profile_id} to {updated} asset model(s)"
            + (
                f" and {updated_instances} installed wireless device(s)."
                if updated_instances
                else "."
            ),
        )

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
        traditional = (
            self.technology_combo.currentText().strip().lower() == "traditional"
        )
        for widget in (
            self.topology_model_combo,
            self.rack_deployment_combo,
            self.independent_link_count_spin,
            self.redundant_core_check,
            self.layer_rules_table,
            self.load_profile_rules_button,
            self.add_layer_rule_button,
            self.remove_layer_rule_button,
        ):
            widget.setEnabled(traditional)

        three_tier = (
            _text(self.topology_model_combo.currentData()) == "three_tier"
        )
        end_of_row = (
            _text(self.rack_deployment_combo.currentData()) == "end_of_row"
        )
        aggregation_available = traditional and three_tier and end_of_row
        self.aggregation_rack_combo.setEnabled(aggregation_available)
        if not aggregation_available:
            dedicated_index = self.aggregation_rack_combo.findData("dedicated")
            if dedicated_index >= 0:
                self.aggregation_rack_combo.setCurrentIndex(dedicated_index)

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
                "by planning issue resolutions, including adjacent-DER choices?",
            )
            != QMessageBox.Yes
        ):
            return
        settings["auto_planner_resolution_overrides"] = {}
        QMessageBox.information(
            self, "Planner overrides", "Automatic-planner overrides were cleared."
        )

    def _refresh_layer_estimate(self) -> None:
        self._sync_planner_settings()
        try:
            estimate = estimate_network_switch_counts(self.data)
            self.layer_estimate_label.setText(_layer_switch_estimate_text(estimate))
        except NetworkPlanningError as exc:
            self.layer_estimate_label.setText(f"Estimate unavailable: {exc}")

    def _sync_planner_settings(self) -> None:
        settings = self.data.setdefault("network_settings", {})
        settings["technology"] = self.technology_combo.currentText().strip()
        settings["expected_mer_count"] = int(self.expected_mer_spin.value())
        settings["redundant_core"] = self.redundant_core_check.isChecked()
        settings["topology_model"] = (
            _text(self.topology_model_combo.currentData()) or "collapsed_core"
        )
        settings["rack_deployment_model"] = (
            _text(self.rack_deployment_combo.currentData()) or "end_of_row"
        )
        aggregation_mode = (
            _text(self.aggregation_rack_combo.currentData()) or "dedicated"
        )
        if (
            settings["technology"].lower() != "traditional"
            or settings["topology_model"] != "three_tier"
            or settings["rack_deployment_model"] != "end_of_row"
        ):
            aggregation_mode = "dedicated"
        settings["aggregation_rack_mode"] = aggregation_mode
        settings["tor_keep_final_connections_in_cabinet"] = True
        settings["tor_allow_adjacent_cabinet_uplinks"] = True
        settings["independent_link_count"] = int(
            self.independent_link_count_spin.value()
        )
        settings["access_stacking_enabled"] = bool(
            self.access_stacking_check.isChecked()
        )
        settings["access_stack_max_members"] = int(self.access_stack_max_spin.value())
        settings["access_stack_topology"] = (
            _text(self.access_stack_topology_combo.currentData()) or "ring"
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
        settings["auto_planner_connected_data_points_only"] = bool(
            self.connected_data_points_only_check.isChecked()
        )
        settings["auto_planner_same_floor_only"] = bool(
            self.same_floor_only_check.isChecked()
        )
        settings["prevent_additional_equipment_rooms"] = bool(
            self.prevent_additional_rooms_check.isChecked()
        )
        settings["ignore_link_bandwidth_errors"] = bool(
            self.ignore_link_bandwidth_check.isChecked()
        )
        settings["spare_capacity_percent"] = float(self.spare_capacity_spin.value())
        settings["default_north_south_bandwidth_mbps"] = float(
            self.default_north_south_bandwidth_spin.value()
        )
        settings["default_east_west_bandwidth_mbps"] = float(
            self.default_east_west_bandwidth_spin.value()
        )
        settings["default_expected_bandwidth_mbps"] = float(
            self.default_north_south_bandwidth_spin.value()
            + self.default_east_west_bandwidth_spin.value()
        )
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
            f"Rack deployment: {summary.get('rack_deployment_model', 'end_of_row')}",
            f"Aggregation placement: {summary.get('aggregation_rack_mode', 'dedicated')}",
            f"Planned cabinets: {summary.get('rack_count', 0)}",
            f"Final cross-cabinet connections: {summary.get('final_cross_cabinet_connections', 0)}",
            f"Adjacent cabinet uplinks: {summary.get('adjacent_cabinet_uplinks', 0)}",
            f"Cabinet-row uplinks: {summary.get('cabinet_row_uplinks', 0)}",
            f"Independent links: {summary.get('independent_link_count', '')}",
            f"Objective: {summary.get('objective', '')}",
            "",
            f"Endpoint locations: {summary.get('endpoint_locations', 0)}",
            f"Required ports: {summary.get('required_ports', 0)}",
            f"Required PoE: {summary.get('required_poe_w', 0)} W",
            f"North-south bandwidth: {summary.get('required_north_south_bandwidth_mbps', summary.get('required_bandwidth_mbps', 0))} Mbps",
            f"East-west bandwidth: {summary.get('required_east_west_bandwidth_mbps', 0)} Mbps",
            f"Combined switching bandwidth: {summary.get('required_bandwidth_mbps', 0)} Mbps",
            f"Expected packet rate: {summary.get('required_packet_rate_pps', 0)} pps",
            f"Installed endpoint ports: {summary.get('installed_endpoint_ports', 0)}",
            f"Installed PoE budget: {summary.get('installed_poe_budget_w', 0)} W",
            f"Installed endpoint bandwidth capacity: {summary.get('installed_bandwidth_capacity_mbps', 0)} Mbps",
            f"Installed endpoint packet capacity: {summary.get('installed_packet_throughput_pps', 0)} pps",
            f"Spare capacity: {summary.get('spare_capacity_percent', 0)}%",
            "Endpoint demand source: "
            + (
                "Current Routing-tab connections only"
                if summary.get("connected_data_points_only", False)
                else "All placed data points"
            ),
            "Endpoint floor restriction: "
            + (
                "Same-floor rooms and routes only"
                if summary.get("same_floor_only", False)
                else "Cross-floor assignments permitted"
            ),
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
        modular_configurations = (
            summary.get("modular_chassis_configurations", []) or []
        )
        if modular_configurations:
            lines.extend(["", "Demand-configured modular chassis:"])
            for configuration in modular_configurations:
                line_cards = [
                    f"slot {module.get('slot')}: {_text(module.get('model'))}"
                    for module in configuration.get("chassis_modules", []) or []
                    if _text(module.get("module_type")) == "line_card"
                ]
                lines.append(
                    f"  {_text(configuration.get('name'))}: "
                    + (", ".join(line_cards) if line_cards else "no line cards")
                )
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
        model_preferences = normalise_asset_model_preferences(
            summary.get("asset_model_preferences", {})
        )
        selected_models = [
            (component, policy)
            for component, policy in model_preferences.items()
            if policy.get("preferred_asset_ids")
        ]
        if selected_models:
            lines.extend(["", "Preferred equipment models:"])
            asset_names = {
                _text(asset.get("id")): (
                    _text(asset.get("name")) or _text(asset.get("id"))
                )
                for asset in self.data.get("network_assets", [])
                if isinstance(asset, dict)
            }
            for component, policy in selected_models:
                ids = policy.get("preferred_asset_ids", [])
                names = [asset_names.get(_text(value), _text(value)) for value in ids]
                strict_text = " (strict)" if policy.get("strict") else ""
                lines.append(
                    f"  {ASSET_MODEL_PREFERENCE_COMPONENTS.get(component, component)}: "
                    + ", ".join(names)
                    + strict_text
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
            for location, count in sorted(
                (overrides.get("additional_der_by_location", {}) or {}).items()
            ):
                asset_id = _text(
                    (overrides.get("additional_der_asset_by_location", {}) or {}).get(
                        location, ""
                    )
                )
                asset_text = f" using {asset_id}" if asset_id else ""
                lines.append(
                    f"  Adjacent DERs at {location}: {count}{asset_text}"
                )
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
        # Capture edits made on the main settings tab before opening the guided
        # setup. The wizard then becomes the authoritative launch step.
        self._sync_planner_settings()
        wizard = AutoPlannerSetupWizard(self, self.data)
        if wizard.exec() != QDialog.Accepted:
            return
        previous_topology = _text(
            self.topology_model_combo.currentData()
        ) or "collapsed_core"
        wizard.apply_settings()
        settings = self.data.setdefault("network_settings", {})

        _set_combo_text(
            self.technology_combo,
            _text(settings.get("technology")) or "Traditional",
        )
        topology_index = self.topology_model_combo.findData(
            _text(settings.get("topology_model")) or "collapsed_core"
        )
        if topology_index >= 0:
            self.topology_model_combo.setCurrentIndex(topology_index)
        rack_index = self.rack_deployment_combo.findData(
            _text(settings.get("rack_deployment_model")) or "end_of_row"
        )
        if rack_index >= 0:
            self.rack_deployment_combo.setCurrentIndex(rack_index)
        aggregation_index = self.aggregation_rack_combo.findData(
            _text(settings.get("aggregation_rack_mode")) or "dedicated"
        )
        if aggregation_index >= 0:
            self.aggregation_rack_combo.setCurrentIndex(aggregation_index)
        self.redundant_core_check.setChecked(
            bool(settings.get("redundant_core", True))
        )
        self.independent_link_count_spin.setValue(
            max(1, int(settings.get("independent_link_count", 2) or 2))
        )
        self.spare_capacity_spin.setValue(
            float(settings.get("spare_capacity_percent", 15.0) or 0.0)
        )
        self.connected_data_points_only_check.setChecked(
            bool(settings.get("auto_planner_connected_data_points_only", False))
        )
        self.same_floor_only_check.setChecked(
            bool(settings.get("auto_planner_same_floor_only", False))
        )
        self.prevent_additional_rooms_check.setChecked(
            bool(settings.get("prevent_additional_equipment_rooms", False))
        )
        self.default_rack_size_spin.setValue(
            max(1, int(settings.get("default_rack_size_u", 42) or 42))
        )
        current_topology = _text(
            self.topology_model_combo.currentData()
        ) or "collapsed_core"
        if current_topology != previous_topology:
            self._load_layer_profile_defaults()
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
        applied_resolution_signatures = set()

        def planning_error_signature(error: NetworkPlanningError) -> tuple:
            code = _text(getattr(error, "code", ""))
            details = dict(getattr(error, "details", {}) or {})
            issues = details.get("issues", []) or []
            if isinstance(issues, list) and issues:
                return (
                    code,
                    tuple(
                        sorted(
                            (
                                _text(issue.get("location_name")),
                                int(issue.get("actual_port_count", 0) or 0),
                                int(
                                    issue.get(
                                        "required_port_count_with_spare", 0
                                    )
                                    or 0
                                ),
                                int(issue.get("switches_with_spare", 0) or 0),
                                int(issue.get("stacks_with_spare", 0) or 0),
                            )
                            for issue in issues
                            if isinstance(issue, dict)
                        )
                    ),
                )
            return (
                code,
                _text(details.get("location_name")),
                _text(details.get("from_name")),
                _text(details.get("to_name")),
                round(float(details.get("required_bandwidth_mbps", 0.0) or 0.0), 6),
                int(details.get("current_location_switch_count", 0) or 0),
                int(details.get("required_port_count_with_spare", 0) or 0),
            )

        max_resolution_rounds = max(
            32, min(256, len(self.data.get("locations", [])) * 4 + 16)
        )
        progress = QProgressDialog(
            "Preparing automatic network plan...", "", 0, 100, self
        )
        progress.setWindowTitle("Automatic network planning")
        progress.setWindowModality(Qt.WindowModal)
        progress.setCancelButton(None)
        progress.setMinimumDuration(0)
        progress.setAutoClose(False)
        progress.setAutoReset(False)

        for _attempt in range(max_resolution_rounds):
            snapshot = self._save_payload()
            progress_message = (
                "Preparing automatic network plan..."
                if _attempt == 0
                else (
                    "Continuing the same planning session with "
                    f"{len(applied_resolution_signatures)} saved decision set(s)..."
                )
            )
            progress.setLabelText(progress_message)
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
                progress.hide()

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
                progress.close()
                QMessageBox.critical(
                    self,
                    "Automatic network planning",
                    f"Unexpected planning error:\n{unexpected_error}",
                )
                return

            if planning_error is None:
                progress.close()
                QMessageBox.critical(
                    self,
                    "Automatic network planning",
                    "The planner stopped without returning a diagnostic.",
                )
                return

            signature = planning_error_signature(planning_error)
            if signature in applied_resolution_signatures:
                progress.close()
                QMessageBox.critical(
                    self,
                    "Automatic network planning",
                    "The same planning issue was returned after its saved "
                    "resolution was applied. The solver has stopped rather than "
                    "asking for the same decision again. Review the affected "
                    "asset model, stack limit and rack settings.\n\n"
                    f"{planning_error}",
                )
                return

            error_code = _text(getattr(planning_error, "code", ""))
            if error_code in {
                "access_stack_spare_capacity_batch",
                "der_patch_panel_capacity_batch",
            }:
                resolution = PlanningResolutionSequenceDialog(
                    self, planning_error, self.data
                )
                if resolution.exec() != QDialog.Accepted:
                    progress.close()
                    return
            else:
                resolution = PlanningResolutionDialog(
                    self, planning_error, self.data
                )
                if resolution.exec() != QDialog.Accepted:
                    progress.close()
                    return
                if resolution.selected_action == "review_assets":
                    progress.close()
                    self.tabs.setCurrentWidget(self.assets_tab)
                    self.refresh_tables()
                    self._focus_assets_for_planning_error(planning_error)
                    return
                if resolution.selected_action == "review_location":
                    progress.close()
                    QMessageBox.information(
                        self,
                        "Edit location cabinet constraint",
                        "Close Network Configuration, then edit the comms-room point "
                        "on the drawing to change its cabinet type or maximum cabinet count.",
                    )
                    return

            applied_resolution_signatures.add(signature)
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
            progress.close()
            QMessageBox.critical(
                self,
                "Automatic network planning",
                "The planner could not resolve the design after all permitted decision rounds. "
                "Review the asset library and the saved planner overrides.",
            )
            return

        progress.close()
        self.refresh_tables()
        self.on_save(self._save_payload())
        self.tabs.setCurrentWidget(self.summary_text)
        completion_message = (
            f"Generated {summary.get('technology', technology)} network configuration.\n\n"
            f"Endpoint ports: {summary.get('required_ports', 0)}\n"
            f"Generated components: {summary.get('auto_generated_instances', 0)}\n"
            f"Copper: {summary.get('estimated_copper_length_m', 0)} m\n"
            f"Fibre: {summary.get('estimated_fibre_length_m', 0)} m"
        )
        warnings = summary.get("warnings", []) or []
        if warnings:
            displayed_warnings = [str(warning) for warning in warnings[:8]]
            if len(warnings) > len(displayed_warnings):
                displayed_warnings.append(
                    f"... and {len(warnings) - len(displayed_warnings)} more warning(s)."
                )
            QMessageBox.warning(
                self,
                "Automatic network planning warnings",
                completion_message
                + "\n\nWarnings:\nâ€¢ "
                + "\nâ€¢ ".join(displayed_warnings),
            )
        else:
            QMessageBox.information(
                self,
                "Automatic network planning",
                completion_message,
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

    def _export_network_asset_rows(self, rows: Sequence[dict], title: str) -> None:
        if not rows:
            QMessageBox.information(
                self, "Export network assets", "Select at least one asset to export."
            )
            return
        default_name = (
            f"{_text(rows[0].get('id'))}.asset-pack.json"
            if len(rows) == 1
            else "network-asset-library.asset-pack.json"
        )
        path, _selected_filter = QFileDialog.getSaveFileName(
            self,
            title,
            default_name,
            "Asset packs (*.asset-pack.json *.json)",
        )
        if not path:
            return
        try:
            write_asset_pack(
                path,
                "network_assets",
                rows,
                name=(
                    _text(rows[0].get("name"))
                    if len(rows) == 1
                    else "Network Asset Library"
                ),
            )
        except (OSError, AssetPackError) as exc:
            QMessageBox.critical(self, "Export failed", str(exc))
            return
        QMessageBox.information(
            self,
            "Export complete",
            f"Exported {len(rows)} network asset(s) to:\n{path}",
        )

    def export_selected_network_assets(self) -> None:
        assets = self._items("network_assets")
        rows = [
            assets[index]
            for index in self.assets_tab.selected_rows()
            if 0 <= index < len(assets)
        ]
        self._export_network_asset_rows(rows, "Export selected network assets")

    def export_network_asset_library(self) -> None:
        self._export_network_asset_rows(
            self._items("network_assets"), "Export Network Asset Library"
        )

    def import_network_assets(self) -> None:
        path, _selected_filter = QFileDialog.getOpenFileName(
            self,
            "Import Network Asset Pack",
            str(Path(__file__).resolve().parent.parent / "asset_packs"),
            "Asset packs (*.asset-pack.json *.json)",
        )
        if not path:
            return
        try:
            payload = read_asset_pack(path, "network_assets")
        except AssetPackError as exc:
            QMessageBox.critical(self, "Import failed", str(exc))
            return

        existing_ids = {
            _text(row.get("id"))
            for row in self._items("network_assets")
            if isinstance(row, dict) and _text(row.get("id"))
        }
        duplicate_ids = sorted(
            existing_ids
            & {_text(row.get("id")) for row in payload.get("assets", [])}
        )
        replace_existing = False
        if duplicate_ids:
            answer = QMessageBox.question(
                self,
                "Existing network assets",
                f"{len(duplicate_ids)} asset ID(s) already exist.\n\n"
                "Yes: replace existing definitions\n"
                "No: keep existing definitions and import only new assets\n"
                "Cancel: do not import",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
                QMessageBox.No,
            )
            if answer == QMessageBox.Cancel:
                return
            replace_existing = answer == QMessageBox.Yes

        merged, result = merge_asset_rows(
            self._items("network_assets"),
            payload.get("assets", []),
            replace_existing=replace_existing,
        )
        self.data["network_assets"] = merged
        self.tabs.setCurrentWidget(self.assets_tab)
        self.asset_group_filter_combo.blockSignals(True)
        self.asset_group_filter_combo.setCurrentIndex(0)
        self.asset_group_filter_combo.blockSignals(False)
        self.refresh_tables()
        imported_ids = set(result.get("imported_ids", []))
        self.assets_tab.table.clearSelection()
        for source_row, asset in enumerate(merged):
            if _text(asset.get("id")) in imported_ids:
                display_row = self.assets_tab.display_row_for_source_index(source_row)
                if display_row >= 0:
                    self.assets_tab.table.selectRow(display_row)
        QMessageBox.information(
            self,
            "Import complete",
            f"Added: {result['added']}\n"
            f"Replaced: {result['replaced']}\n"
            f"Skipped: {result['skipped']}",
        )

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
            auto_connect_pending_imported_wireless_devices(self.data)
            self.refresh_tables()

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
            auto_connect_pending_imported_wireless_devices(self.data)
            self.refresh_tables()

    def delete_instance(self) -> None:
        rows = self.instances_tab.selected_rows()
        instances = self._items("network_asset_instances")
        selected = [
            instances[row]
            for row in rows
            if 0 <= row < len(instances)
        ]
        instance_ids = {
            _text(item.get("id"))
            for item in selected
            if _text(item.get("id"))
        }
        if not instance_ids:
            return

        ordered_ids = [
            _text(item.get("id"))
            for item in selected
            if _text(item.get("id")) in instance_ids
        ]
        if len(ordered_ids) == 1:
            prompt = (
                f"Delete installed asset {ordered_ids[0]} and all of its dependent "
                "network records?"
            )
        else:
            preview = ", ".join(ordered_ids[:8])
            if len(ordered_ids) > 8:
                preview += f", and {len(ordered_ids) - 8} more"
            prompt = (
                f"Delete {len(ordered_ids)} selected installed assets and all of "
                f"their dependent network records?\n\n{preview}"
            )

        if (
            QMessageBox.question(
                self,
                "Delete installed assets",
                prompt,
            )
            != QMessageBox.Yes
        ):
            return

        removed_connection_ids = {
            connection_id
            for connection in self._items("network_connections")
            if (
                _text(connection.get("from_instance_id")) in instance_ids
                or _text(connection.get("to_instance_id")) in instance_ids
            )
            for connection_id in [_text(connection.get("id"))]
            if connection_id
        }
        removed_assignment_ids = {
            assignment_id
            for assignment in self._items("network_endpoint_assignments")
            if any(
                _text(assignment.get(field)) in instance_ids
                for field in (
                    "network_instance_id",
                    "physical_patch_panel_instance_id",
                    "horizontal_cable_from_instance_id",
                )
            )
            for assignment_id in [_text(assignment.get("id"))]
            if assignment_id
        }
        removed_fibre_cable_ids = {
            cable_id
            for cable in self._items("network_fibre_cables")
            if (
                _text(cable.get("from_instance_id")) in instance_ids
                or _text(cable.get("to_instance_id")) in instance_ids
            )
            for cable_id in [_text(cable.get("id"))]
            if cable_id
        }

        self.data["network_asset_instances"] = [
            item
            for item in instances
            if _text(item.get("id")) not in instance_ids
        ]
        self.data["network_connections"] = [
            connection
            for connection in self._items("network_connections")
            if _text(connection.get("from_instance_id")) not in instance_ids
            and _text(connection.get("to_instance_id")) not in instance_ids
        ]
        self.data["network_power_connections"] = [
            link
            for link in self._items("network_power_connections")
            if _text(link.get("from_instance_id")) not in instance_ids
            and _text(link.get("to_instance_id")) not in instance_ids
        ]
        self.data["network_endpoint_assignments"] = [
            assignment
            for assignment in self._items("network_endpoint_assignments")
            if not any(
                _text(assignment.get(field)) in instance_ids
                for field in (
                    "network_instance_id",
                    "physical_patch_panel_instance_id",
                    "horizontal_cable_from_instance_id",
                )
            )
        ]
        self.data["network_patch_leads"] = [
            lead
            for lead in self._items("network_patch_leads")
            if _text(lead.get("instance_id")) not in instance_ids
            and _text(lead.get("peer_instance_id")) not in instance_ids
            and _text(lead.get("connection_id")) not in removed_connection_ids
            and _text(lead.get("assignment_id")) not in removed_assignment_ids
        ]
        self.data["network_optic_modules"] = [
            module
            for module in self._items("network_optic_modules")
            if _text(module.get("host_instance_id")) not in instance_ids
            and _text(module.get("connection_id")) not in removed_connection_ids
        ]
        self.data["network_optical_paths"] = [
            path
            for path in self._items("network_optical_paths")
            if _text(path.get("source_instance_id")) not in instance_ids
            and _text(path.get("destination_instance_id")) not in instance_ids
        ]
        self.data["network_ip_allocations"] = [
            allocation
            for allocation in self._items("network_ip_allocations")
            if _text(allocation.get("instance_id")) not in instance_ids
        ]
        self.data["network_redundancy_groups"] = [
            group
            for group in self._items("network_redundancy_groups")
            if _text(group.get("protected_instance_id")) not in instance_ids
            and _text(group.get("primary_olt_instance_id")) not in instance_ids
            and _text(group.get("secondary_olt_instance_id")) not in instance_ids
            and not instance_ids.intersection(
                {_text(value) for value in group.get("source_instance_ids", [])}
            )
            and not instance_ids.intersection(
                {_text(value) for value in group.get("source_core_instance_ids", [])}
            )
        ]
        self.data["network_fibre_cables"] = [
            cable
            for cable in self._items("network_fibre_cables")
            if _text(cable.get("from_instance_id")) not in instance_ids
            and _text(cable.get("to_instance_id")) not in instance_ids
        ]
        self.data["network_fibre_splices"] = [
            splice
            for splice in self._items("network_fibre_splices")
            if _text(splice.get("termination_instance_id")) not in instance_ids
            and _text(splice.get("circuit_id")) not in removed_connection_ids
            and _text(splice.get("incoming_cable_id")) not in removed_fibre_cable_ids
            and _text(splice.get("outgoing_cable_id")) not in removed_fibre_cable_ids
        ]

        for node in self._items("network_fibre_nodes"):
            if _text(node.get("linked_instance_id")) in instance_ids:
                node["linked_instance_id"] = ""

        for external in self._items("network_external_networks"):
            if _text(external.get("demarcation_instance_id")) in instance_ids:
                external["demarcation_instance_id"] = ""
            external["peer_instance_ids"] = [
                value
                for value in external.get("peer_instance_ids", [])
                if _text(value) not in instance_ids
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
