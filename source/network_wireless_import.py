"""Wireless-device CSV import wizard and import helpers.

The importer is intentionally isolated from the automatic network planner.  It
creates installed network-asset instances at the surveyed coordinates and, when
requested, assigns each imported instance a same-floor corridor ``route_anchor``.
Only these imported instances receive that special anchor, so existing manual
and automatically planned devices retain their established routing behaviour.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
import math
from pathlib import Path
import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDoubleSpinBox,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWizard,
    QWizardPage,
)

from network_auto_planner import RoutingGraph, auto_connect_manual_devices
from network_schema import ensure_network_schema, next_network_id


CREATE_ASSET_TOKEN = "__create_wireless_asset__"
SKIP_FLOOR_TOKEN = "__skip_floor__"


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalised_floor_number(value) -> Optional[int]:
    text = _text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        pass
    matches = re.findall(r"[-+]?\d+", text)
    if not matches:
        return None
    try:
        return int(matches[-1])
    except ValueError:
        return None


def project_floor_levels(data: dict) -> List[int]:
    """Return every floor currently represented in the cable-route project."""

    floors: set[int] = set()

    def add(value) -> None:
        try:
            floors.add(int(value))
        except (TypeError, ValueError):
            return

    for key in ("floor_dxf_files", "locations", "data_points", "departments"):
        for row in data.get(key, []):
            if isinstance(row, dict):
                add(row.get("floor"))
    for row in data.get("corridors", {}).get("nodes", []):
        if isinstance(row, dict):
            add(row.get("floor"))
    for row in data.get("network_asset_instances", []):
        if isinstance(row, dict):
            add(row.get("floor"))
    for transition in data.get("transitions", []):
        if not isinstance(transition, dict):
            continue
        for floor in (transition.get("floor_locations") or {}).keys():
            add(floor)

    return sorted(floors) or [0]


def _header_match(headers: Sequence[str], candidates: Sequence[str]) -> str:
    by_lower = {_text(header).lower(): _text(header) for header in headers}
    for candidate in candidates:
        if candidate.lower() in by_lower:
            return by_lower[candidate.lower()]
    for candidate in candidates:
        token = candidate.lower()
        for header in headers:
            if token in _text(header).lower():
                return _text(header)
    return _text(headers[0]) if headers else ""


def read_wireless_csv(path: str) -> Tuple[List[str], List[dict]]:
    """Read a wireless-planning CSV using common spreadsheet encodings."""

    last_error: Optional[Exception] = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            with open(path, "r", newline="", encoding=encoding) as handle:
                reader = csv.DictReader(handle)
                headers = [_text(value) for value in (reader.fieldnames or [])]
                if not headers:
                    raise ValueError("The CSV does not contain a header row.")
                rows = []
                for row_number, row in enumerate(reader, start=2):
                    if not isinstance(row, dict):
                        continue
                    payload = {_text(key): value for key, value in row.items() if key is not None}
                    if not any(_text(value) for value in payload.values()):
                        continue
                    payload["__csv_row_number__"] = row_number
                    rows.append(payload)
                if not rows:
                    raise ValueError("The CSV does not contain any device rows.")
                return headers, rows
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise ValueError(f"Unable to decode the CSV: {last_error}") from last_error
    raise ValueError("Unable to read the CSV.")


def _infer_wireless_category(profile: str) -> str:
    value = _text(profile).lower()
    if "gateway" in value and "iot" in value:
        return "iot_gateway"
    if "gateway" in value:
        return "radio_gateway"
    if "bridge" in value:
        return "wireless_bridge"
    if "sensor" in value:
        return "wireless_sensor"
    if any(token in value for token in ("access point", "wi-fi", "wifi", "wlan", " ap")):
        return "access_point"
    return "other"


def _infer_frequencies(profile: str) -> List[str]:
    value = _text(profile).lower()
    result: List[str] = []
    if "tri-band" in value or "triband" in value or "wi-fi 7" in value or "wifi 7" in value:
        return ["2.4 GHz", "5 GHz", "6 GHz"]
    if "dual-band" in value or "dual band" in value:
        return ["2.4 GHz", "5 GHz"]
    for token, label in (
        ("2.4", "2.4 GHz"),
        ("5 ghz", "5 GHz"),
        ("6 ghz", "6 GHz"),
        ("60 ghz", "60 GHz"),
        ("868", "868 MHz"),
        ("433", "433 MHz"),
    ):
        if token in value and label not in result:
            result.append(label)
    return result


def _new_wireless_asset(data: dict, profile: str) -> dict:
    assets = data.setdefault("network_assets", [])
    asset_id = next_network_id(assets, "NA")
    profile_name = _text(profile) or "Imported wireless device"
    category = _infer_wireless_category(profile_name)
    return {
        "id": asset_id,
        "name": profile_name,
        "manufacturer": "",
        "model": profile_name,
        "asset_type": "wireless_device",
        "wireless_device_category": category,
        "frequencies": _infer_frequencies(profile_name) or ["Unspecified radio"],
        "power_input_w": 0.0,
        "poe_budget_w": 0.0,
        "expected_bandwidth_mbps": 0.0,
        "expected_packet_rate_pps": 0.0,
        "number_of_ports": 1,
        "connections_in": 1,
        "connections_out": 0,
        "uplink_port_count": 1,
        "input_connection_type": "rj45",
        "output_connection_type": "",
        "uplink_connection_type": "rj45",
        "rack_units": 0,
        "port_definitions": [
            {
                "port_type": "rj45",
                "port_count": 1,
                "port_use": "uplink",
                "name_prefix": "Ethernet",
                "supported_speeds_mbps": [1000, 2500, 5000, 10000],
                "default_speed_mbps": 2500,
            }
        ],
        "wireless_import_profile": profile_name,
        "wireless_import_created": True,
        "notes": "Created automatically by the wireless-device CSV importer.",
    }


def _corridor_nodes_by_floor(data: dict) -> Dict[int, List[dict]]:
    result: Dict[int, List[dict]] = defaultdict(list)
    for row in data.get("corridors", {}).get("nodes", []):
        if not isinstance(row, dict) or not _text(row.get("name")):
            continue
        try:
            floor = int(row.get("floor", 0))
            x = float(row.get("x", 0.0))
            y = float(row.get("y", 0.0))
        except (TypeError, ValueError):
            continue
        result[floor].append({**row, "floor": floor, "x": x, "y": y})
    return result


def nearest_corridor_node(
    nodes_by_floor: Dict[int, List[dict]], floor: int, x: float, y: float
) -> Tuple[str, float]:
    candidates = nodes_by_floor.get(int(floor), [])
    if not candidates:
        return "", 0.0
    node = min(
        candidates,
        key=lambda row: (float(row.get("x", 0.0)) - x) ** 2
        + (float(row.get("y", 0.0)) - y) ** 2,
    )
    distance = math.hypot(float(node.get("x", 0.0)) - x, float(node.get("y", 0.0)) - y)
    return _text(node.get("name")), distance


def _source_key(source_id: str, source_floor: str) -> str:
    return f"{_text(source_floor)}::{_text(source_id)}".lower()


def import_wireless_devices(data: dict, config: dict) -> dict:
    """Apply a validated wizard configuration to the current project data."""

    ensure_network_schema(data)
    rows = list(config.get("rows") or [])
    columns = dict(config.get("columns") or {})
    floor_map = dict(config.get("floor_map") or {})
    asset_map = dict(config.get("asset_map") or {})
    duplicate_mode = _text(config.get("duplicate_mode") or "update").lower()
    attach_to_corridor = bool(config.get("attach_to_corridor", True))
    auto_connect = bool(config.get("auto_connect", False))

    result = {
        "created_instance_ids": [],
        "updated_instance_ids": [],
        "skipped_rows": 0,
        "unmapped_rows": 0,
        "invalid_rows": 0,
        "without_corridor": 0,
        "created_asset_ids": [],
        "auto_connect": {},
        "warnings": [],
    }

    assets = data.setdefault("network_assets", [])
    instances = data.setdefault("network_asset_instances", [])
    asset_ids = {_text(row.get("id")) for row in assets if isinstance(row, dict)}
    profile_asset_ids: Dict[str, str] = {}

    for profile, mapped_asset_id in asset_map.items():
        if mapped_asset_id == CREATE_ASSET_TOKEN:
            existing_asset = next(
                (
                    row
                    for row in assets
                    if isinstance(row, dict)
                    and _text(row.get("asset_type")).lower() == "wireless_device"
                    and (
                        _text(row.get("wireless_import_profile")) == _text(profile)
                        or (
                            bool(row.get("wireless_import_created"))
                            and _text(row.get("name")) == _text(profile)
                        )
                    )
                ),
                None,
            )
            if existing_asset is not None:
                profile_asset_ids[_text(profile)] = _text(existing_asset.get("id"))
            else:
                asset = _new_wireless_asset(data, profile)
                while _text(asset.get("id")) in asset_ids:
                    asset["id"] = next_network_id(assets, "NA")
                assets.append(asset)
                asset_ids.add(_text(asset.get("id")))
                profile_asset_ids[_text(profile)] = _text(asset.get("id"))
                result["created_asset_ids"].append(_text(asset.get("id")))
        else:
            profile_asset_ids[_text(profile)] = _text(mapped_asset_id)

    known_assets = {
        _text(row.get("id")): row for row in assets if isinstance(row, dict) and _text(row.get("id"))
    }
    existing_imports = {
        _source_key(row.get("wireless_import_source_id"), row.get("wireless_import_source_floor")): row
        for row in instances
        if isinstance(row, dict)
        and bool(row.get("imported_wireless_device"))
        and _text(row.get("wireless_import_source_id"))
    }
    used_instance_ids = {
        _text(row.get("id")) for row in instances if isinstance(row, dict) and _text(row.get("id"))
    }
    corridor_nodes = _corridor_nodes_by_floor(data)

    x_scale = float(config.get("coordinate_scale", 1.0) or 1.0)
    x_offset = float(config.get("x_offset", 0.0) or 0.0)
    y_offset = float(config.get("y_offset", 0.0) or 0.0)
    swap_axes = bool(config.get("swap_axes", False))
    invert_x = bool(config.get("invert_x", False))
    invert_y = bool(config.get("invert_y", False))
    absolute_mode = _text(config.get("coordinate_mode")) == "insertion_plus_delta"
    source_file = Path(_text(config.get("source_file"))).name

    imported_ids: List[str] = []
    for row in rows:
        source_id = _text(row.get(columns.get("name", "")))
        source_floor = _text(row.get(columns.get("floor", "")))
        profile = (
            _text(row.get(columns.get("profile", "")))
            if columns.get("profile")
            else "Imported wireless device"
        ) or "Imported wireless device"
        target_floor = floor_map.get(source_floor)
        if target_floor in (None, SKIP_FLOOR_TOKEN):
            result["unmapped_rows"] += 1
            continue
        asset_id = profile_asset_ids.get(profile, "")
        if not source_id or not asset_id or asset_id not in known_assets:
            result["invalid_rows"] += 1
            continue
        try:
            x = float(row.get(columns.get("x", ""), ""))
            y = float(row.get(columns.get("y", ""), ""))
            if absolute_mode:
                x += float(row.get(columns.get("insertion_x", ""), ""))
                y += float(row.get(columns.get("insertion_y", ""), ""))
        except (TypeError, ValueError):
            result["invalid_rows"] += 1
            continue
        if swap_axes:
            x, y = y, x
        if invert_x:
            x = -x
        if invert_y:
            y = -y
        x = x * x_scale + x_offset
        y = y * x_scale + y_offset
        target_floor = int(target_floor)

        key = _source_key(source_id, source_floor)
        existing = existing_imports.get(key)
        if existing is not None and duplicate_mode == "skip":
            result["skipped_rows"] += 1
            continue

        route_anchor = ""
        corridor_distance = 0.0
        if attach_to_corridor:
            route_anchor, corridor_distance = nearest_corridor_node(
                corridor_nodes, target_floor, x, y
            )
            if not route_anchor:
                result["without_corridor"] += 1

        payload = {
            "name": source_id,
            "asset_id": asset_id,
            "location_name": "",
            "floor": target_floor,
            "x": round(x, 6),
            "y": round(y, 6),
            "rack_name": "",
            "rack_start_u": 0,
            "rack_size_u": 0,
            "network_layer": "endpoint",
            "route_anchor": route_anchor,
            "wireless_device_layer": True,
            "imported_wireless_device": True,
            "wireless_import_source_file": source_file,
            "wireless_import_source_id": source_id,
            "wireless_import_source_floor": source_floor,
            "wireless_import_profile": profile,
            "wireless_import_row_number": int(row.get("__csv_row_number__", 0) or 0),
            "wireless_import_corridor_distance_m": round(corridor_distance, 3),
            "auto_generated": False,
        }
        if existing is not None and duplicate_mode == "update":
            existing.update(payload)
            instance_id = _text(existing.get("id"))
            result["updated_instance_ids"].append(instance_id)
        else:
            instance_id = next_network_id(instances, "NI")
            while instance_id in used_instance_ids:
                # ``next_network_id`` normally handles this, but retain a guard
                # for malformed projects containing repeated blank records.
                instances.append({"id": instance_id})
                instance_id = next_network_id(instances, "NI")
                instances.pop()
            payload["id"] = instance_id
            instances.append(payload)
            used_instance_ids.add(instance_id)
            existing_imports[key] = payload
            result["created_instance_ids"].append(instance_id)
        imported_ids.append(instance_id)

    ensure_network_schema(data)
    if imported_ids:
        graph = RoutingGraph(data)
        instances_by_id = {
            _text(row.get("id")): row
            for row in data.get("network_asset_instances", [])
            if isinstance(row, dict) and _text(row.get("id"))
        }
        imported_id_set = set(imported_ids)
        for connection in data.get("network_connections", []):
            if not isinstance(connection, dict) or not bool(connection.get("auto_connected")):
                continue
            source_id = _text(connection.get("from_instance_id"))
            target_id = _text(connection.get("to_instance_id"))
            if not ({source_id, target_id} & imported_id_set):
                continue
            source = instances_by_id.get(source_id, {})
            target = instances_by_id.get(target_id, {})
            source_anchor = _text(source.get("route_anchor")) or _text(source.get("location_name"))
            target_anchor = _text(target.get("route_anchor")) or _text(target.get("location_name"))
            length_m, route_path = graph.route(source_anchor, target_anchor)
            connection["route_path"] = route_path
            connection["length_m"] = round(max(0.0, length_m), 3)
    if auto_connect and imported_ids:
        result["auto_connect"] = auto_connect_manual_devices(data, imported_ids)
        result["warnings"].extend(result["auto_connect"].get("warnings", []))
    return result


class WirelessDeviceImportWizard(QWizard):
    """Walk the user through file, column, floor, asset and routing mapping."""

    def __init__(self, parent=None, data: Optional[dict] = None, initial_path: str = ""):
        super().__init__(parent)
        self.data = data or {}
        ensure_network_schema(self.data)
        self.headers: List[str] = []
        self.rows: List[dict] = []
        self._loaded_path = ""
        self.setWindowTitle("Import Wireless Devices")
        self.setWizardStyle(QWizard.ModernStyle)
        self.setOption(QWizard.NoBackButtonOnStartPage, True)
        self.resize(860, 650)

        self._build_file_page(initial_path)
        self._build_columns_page()
        self._build_floor_page()
        self._build_asset_page()
        self._build_options_page()
        self.currentIdChanged.connect(self._page_changed)

        if initial_path:
            self._load_file(show_errors=False)

    def _build_file_page(self, initial_path: str) -> None:
        page = QWizardPage()
        page.setTitle("Select wireless-planning CSV")
        page.setSubTitle(
            "Choose the export file. The wizard will detect its columns, floor levels and wireless profiles."
        )
        layout = QVBoxLayout(page)
        row = QHBoxLayout()
        self.file_edit = QLineEdit(initial_path)
        browse = QPushButton("Browse…")
        browse.clicked.connect(self._browse)
        row.addWidget(self.file_edit, 1)
        row.addWidget(browse)
        layout.addLayout(row)
        self.file_status = QLabel("No CSV loaded.")
        self.file_status.setWordWrap(True)
        layout.addWidget(self.file_status)
        self.file_preview = QTextEdit()
        self.file_preview.setReadOnly(True)
        layout.addWidget(self.file_preview, 1)
        self.file_page = page
        self.addPage(page)

    def _build_columns_page(self) -> None:
        page = QWizardPage()
        page.setTitle("Map CSV columns and coordinates")
        page.setSubTitle(
            "Confirm which export columns identify each device, its wireless profile, floor and plotted position."
        )
        layout = QVBoxLayout(page)
        form = QFormLayout()
        layout.addLayout(form)
        self.name_column_combo = QComboBox()
        self.profile_column_combo = QComboBox()
        self.floor_column_combo = QComboBox()
        self.x_column_combo = QComboBox()
        self.y_column_combo = QComboBox()
        self.insertion_x_column_combo = QComboBox()
        self.insertion_y_column_combo = QComboBox()
        form.addRow("Device name / ID", self.name_column_combo)
        form.addRow("Wireless profile / source type", self.profile_column_combo)
        form.addRow("Source floor level", self.floor_column_combo)
        form.addRow("X coordinate / delta", self.x_column_combo)
        form.addRow("Y coordinate / delta", self.y_column_combo)

        self.coordinate_mode_combo = QComboBox()
        self.coordinate_mode_combo.addItem("Use selected X and Y columns directly", "direct")
        self.coordinate_mode_combo.addItem(
            "Add insertion-point X/Y to the selected delta columns", "insertion_plus_delta"
        )
        self.coordinate_mode_combo.currentIndexChanged.connect(self._coordinate_mode_changed)
        form.addRow("Coordinate interpretation", self.coordinate_mode_combo)
        form.addRow("Insertion-point X", self.insertion_x_column_combo)
        form.addRow("Insertion-point Y", self.insertion_y_column_combo)

        self.scale_spin = QDoubleSpinBox()
        self.scale_spin.setDecimals(8)
        self.scale_spin.setRange(0.00000001, 1_000_000.0)
        self.scale_spin.setValue(1.0)
        self.x_offset_spin = QDoubleSpinBox()
        self.y_offset_spin = QDoubleSpinBox()
        for spin in (self.x_offset_spin, self.y_offset_spin):
            spin.setDecimals(6)
            spin.setRange(-1_000_000_000.0, 1_000_000_000.0)
        form.addRow("Coordinate scale", self.scale_spin)
        form.addRow("Project X offset", self.x_offset_spin)
        form.addRow("Project Y offset", self.y_offset_spin)
        transform_row = QHBoxLayout()
        self.swap_axes_check = QCheckBox("Swap X and Y")
        self.invert_x_check = QCheckBox("Invert X")
        self.invert_y_check = QCheckBox("Invert Y")
        transform_row.addWidget(self.swap_axes_check)
        transform_row.addWidget(self.invert_x_check)
        transform_row.addWidget(self.invert_y_check)
        transform_row.addStretch(1)
        layout.addLayout(transform_row)
        note = QLabel(
            "For exports matching the supplied sample, use the delta columns directly. "
            "Offsets can align the wireless coordinate origin with the cable-route drawing origin."
        )
        note.setWordWrap(True)
        layout.addWidget(note)
        layout.addStretch(1)
        self.columns_page = page
        self.addPage(page)
        self._coordinate_mode_changed()

    def _build_floor_page(self) -> None:
        page = QWizardPage()
        page.setTitle("Map source floors")
        page.setSubTitle(
            "Each CSV floor is compared with floors already present in the cable-route project."
        )
        layout = QVBoxLayout(page)
        self.floor_table = QTableWidget(0, 3)
        self.floor_table.setHorizontalHeaderLabels(
            ["CSV floor level", "Devices", "Cable-route planner floor"]
        )
        self.floor_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.floor_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.floor_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.floor_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.floor_table)
        self.project_floor_label = QLabel()
        self.project_floor_label.setWordWrap(True)
        layout.addWidget(self.project_floor_label)
        self.floor_page = page
        self.addPage(page)

    def _build_asset_page(self) -> None:
        page = QWizardPage()
        page.setTitle("Map wireless profiles to asset models")
        page.setSubTitle(
            "Map each source radio profile to an existing wireless asset or create a new generic wireless-device asset."
        )
        layout = QVBoxLayout(page)
        self.asset_table = QTableWidget(0, 3)
        self.asset_table.setHorizontalHeaderLabels(
            ["CSV wireless profile", "Devices", "Network asset model"]
        )
        self.asset_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.asset_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.asset_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.asset_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.asset_table)
        note = QLabel(
            "New definitions use the Wireless device asset type, one Ethernet uplink and frequencies inferred from the profile name where possible."
        )
        note.setWordWrap(True)
        layout.addWidget(note)
        self.asset_page = page
        self.addPage(page)

    def _build_options_page(self) -> None:
        page = QWizardPage()
        page.setTitle("Routing and import options")
        page.setSubTitle("Review how the imported devices will be added to the project.")
        layout = QVBoxLayout(page)
        self.attach_corridor_check = QCheckBox(
            "Assign each imported device to its nearest same-floor corridor node for routing"
        )
        self.attach_corridor_check.setChecked(True)
        self.attach_corridor_check.setToolTip(
            "Stores a route anchor on imported wireless devices only. Their actual symbols remain at the surveyed coordinates."
        )
        self.auto_connect_check = QCheckBox(
            "Also create network links to the nearest compatible upstream access device"
        )
        self.auto_connect_check.setChecked(False)
        self.auto_connect_check.setToolTip(
            "Uses the existing topology rules and free ports. Links start at the assigned corridor route anchor."
        )
        self.duplicate_combo = QComboBox()
        self.duplicate_combo.addItem(
            "Update previously imported devices with the same source floor and ID", "update"
        )
        self.duplicate_combo.addItem("Skip previously imported devices", "skip")
        self.duplicate_combo.addItem("Create additional instances", "add")
        form = QFormLayout()
        form.addRow("When a device already exists", self.duplicate_combo)
        layout.addWidget(self.attach_corridor_check)
        layout.addWidget(self.auto_connect_check)
        layout.addLayout(form)
        layer_note = QLabel(
            "All imported instances are placed on the dedicated Wireless devices drawing layer and use the wireless radio symbol."
        )
        layer_note.setWordWrap(True)
        layout.addWidget(layer_note)
        self.review_text = QTextEdit()
        self.review_text.setReadOnly(True)
        layout.addWidget(self.review_text, 1)
        self.options_page = page
        self.addPage(page)

    def _browse(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select wireless device CSV",
            self.file_edit.text().strip(),
            "CSV files (*.csv);;All files (*.*)",
        )
        if not path:
            return
        self.file_edit.setText(path)
        self._load_file(show_errors=True)

    def _load_file(self, show_errors: bool = True) -> bool:
        path = self.file_edit.text().strip()
        if not path:
            if show_errors:
                QMessageBox.critical(self, "Wireless import", "Select a CSV file.")
            return False
        try:
            headers, rows = read_wireless_csv(path)
        except Exception as exc:
            self.file_status.setText(f"Unable to load CSV: {exc}")
            if show_errors:
                QMessageBox.critical(self, "Wireless import", str(exc))
            return False
        self.headers = headers
        self.rows = rows
        self._loaded_path = str(Path(path))
        self.file_status.setText(
            f"Loaded {len(rows):,} devices with {len(headers)} columns from {Path(path).name}."
        )
        preview_lines = [", ".join(headers)]
        for row in rows[:5]:
            preview_lines.append(", ".join(_text(row.get(header)) for header in headers))
        self.file_preview.setPlainText("\n".join(preview_lines))
        self._populate_column_combos()
        return True

    def _populate_combo(self, combo: QComboBox, selected: str, allow_blank: bool = False) -> None:
        combo.blockSignals(True)
        combo.clear()
        if allow_blank:
            combo.addItem("Use one profile for all rows", "")
        for header in self.headers:
            combo.addItem(header, header)
        index = combo.findData(selected)
        combo.setCurrentIndex(index if index >= 0 else 0)
        combo.blockSignals(False)

    def _populate_column_combos(self) -> None:
        self._populate_combo(
            self.name_column_combo,
            _header_match(self.headers, ["access_point", "device_name", "name", "id"]),
        )
        self._populate_combo(
            self.profile_column_combo,
            _header_match(self.headers, ["ap_radio_profile", "radio_profile", "device_type", "asset_type"]),
            allow_blank=True,
        )
        self._populate_combo(
            self.floor_column_combo,
            _header_match(self.headers, ["floor_level", "floor", "level"]),
        )
        self._populate_combo(
            self.x_column_combo,
            _header_match(self.headers, ["x_delta_from_insertion_point_m", "delta_x", "x_coordinate", "x"]),
        )
        self._populate_combo(
            self.y_column_combo,
            _header_match(self.headers, ["y_delta_from_insertion_point_m", "delta_y", "y_coordinate", "y"]),
        )
        self._populate_combo(
            self.insertion_x_column_combo,
            _header_match(self.headers, ["insertion_point_x_m", "insertion_x", "origin_x"]),
        )
        self._populate_combo(
            self.insertion_y_column_combo,
            _header_match(self.headers, ["insertion_point_y_m", "insertion_y", "origin_y"]),
        )

    def _coordinate_mode_changed(self, *_args) -> None:
        enabled = _text(self.coordinate_mode_combo.currentData()) == "insertion_plus_delta"
        self.insertion_x_column_combo.setEnabled(enabled)
        self.insertion_y_column_combo.setEnabled(enabled)

    def _columns(self) -> dict:
        return {
            "name": _text(self.name_column_combo.currentData()),
            "profile": _text(self.profile_column_combo.currentData()),
            "floor": _text(self.floor_column_combo.currentData()),
            "x": _text(self.x_column_combo.currentData()),
            "y": _text(self.y_column_combo.currentData()),
            "insertion_x": _text(self.insertion_x_column_combo.currentData()),
            "insertion_y": _text(self.insertion_y_column_combo.currentData()),
        }

    def _rebuild_mapping_pages(self) -> None:
        columns = self._columns()
        floor_counts = Counter(_text(row.get(columns["floor"])) for row in self.rows)
        project_floors = project_floor_levels(self.data)
        self.project_floor_label.setText(
            "Floors detected in the cable-route project: " + ", ".join(str(value) for value in project_floors)
        )
        self.floor_table.setRowCount(len(floor_counts))
        for row_index, (source_floor, count) in enumerate(
            sorted(floor_counts.items(), key=lambda item: (_normalised_floor_number(item[0]) is None, _normalised_floor_number(item[0]) or 0, item[0]))
        ):
            source_item = QTableWidgetItem(source_floor or "(blank)")
            source_item.setData(Qt.UserRole, source_floor)
            count_item = QTableWidgetItem(f"{count:,}")
            count_item.setTextAlignment(Qt.AlignCenter)
            combo = QComboBox()
            combo.setEditable(True)
            combo.addItem("Do not import this source floor", SKIP_FLOOR_TOKEN)
            for floor in project_floors:
                combo.addItem(str(floor), floor)
            inferred = _normalised_floor_number(source_floor)
            if inferred is not None:
                index = combo.findData(inferred)
                if index >= 0:
                    combo.setCurrentIndex(index)
                else:
                    combo.addItem(str(inferred), inferred)
                    combo.setCurrentIndex(combo.count() - 1)
            self.floor_table.setItem(row_index, 0, source_item)
            self.floor_table.setItem(row_index, 1, count_item)
            self.floor_table.setCellWidget(row_index, 2, combo)

        profile_column = columns.get("profile")
        profile_counts = Counter(
            (_text(row.get(profile_column)) if profile_column else "Imported wireless device")
            or "Imported wireless device"
            for row in self.rows
        )
        eligible_assets = [
            row
            for row in self.data.get("network_assets", [])
            if isinstance(row, dict)
            and _text(row.get("asset_type")).lower() in {"wireless_device", "wireless_access_point"}
            and _text(row.get("id"))
        ]
        self.asset_table.setRowCount(len(profile_counts))
        for row_index, (profile, count) in enumerate(sorted(profile_counts.items())):
            profile_item = QTableWidgetItem(profile)
            profile_item.setData(Qt.UserRole, profile)
            count_item = QTableWidgetItem(f"{count:,}")
            count_item.setTextAlignment(Qt.AlignCenter)
            combo = QComboBox()
            combo.addItem("Create a new Wireless device asset", CREATE_ASSET_TOKEN)
            matching_asset_id = ""
            for asset in sorted(eligible_assets, key=lambda item: (_text(item.get("name")), _text(item.get("id")))):
                asset_id = _text(asset.get("id"))
                combo.addItem(
                    f"{asset_id} - {_text(asset.get('name'))} [{_text(asset.get('asset_type'))}]",
                    asset_id,
                )
                if (
                    _text(asset.get("wireless_import_profile")) == profile
                    or _text(asset.get("name")) == profile
                    or _text(asset.get("model")) == profile
                ):
                    matching_asset_id = matching_asset_id or asset_id
            if matching_asset_id:
                matching_index = combo.findData(matching_asset_id)
                if matching_index >= 0:
                    combo.setCurrentIndex(matching_index)
            self.asset_table.setItem(row_index, 0, profile_item)
            self.asset_table.setItem(row_index, 1, count_item)
            self.asset_table.setCellWidget(row_index, 2, combo)

    def _floor_map(self) -> Optional[dict]:
        result = {}
        for row_index in range(self.floor_table.rowCount()):
            source_item = self.floor_table.item(row_index, 0)
            combo = self.floor_table.cellWidget(row_index, 2)
            source_floor = _text(source_item.data(Qt.UserRole) if source_item else "")
            if not isinstance(combo, QComboBox):
                return None
            value = combo.currentData()
            if value == SKIP_FLOOR_TOKEN:
                result[source_floor] = SKIP_FLOOR_TOKEN
                continue
            if value is None:
                value = _normalised_floor_number(combo.currentText())
            try:
                result[source_floor] = int(value)
            except (TypeError, ValueError):
                return None
        return result

    def _asset_map(self) -> dict:
        result = {}
        for row_index in range(self.asset_table.rowCount()):
            profile_item = self.asset_table.item(row_index, 0)
            combo = self.asset_table.cellWidget(row_index, 2)
            profile = _text(profile_item.data(Qt.UserRole) if profile_item else "")
            if isinstance(combo, QComboBox):
                result[profile] = _text(combo.currentData())
        return result

    def _update_review(self) -> None:
        if not self.rows:
            self.review_text.setPlainText("No CSV has been loaded.")
            return
        floor_map = self._floor_map() or {}
        asset_map = self._asset_map()
        mapped_rows = 0
        for row in self.rows:
            source_floor = _text(row.get(self._columns().get("floor", "")))
            if floor_map.get(source_floor) != SKIP_FLOOR_TOKEN:
                mapped_rows += 1
        create_count = sum(1 for value in asset_map.values() if value == CREATE_ASSET_TOKEN)
        lines = [
            f"Source file: {Path(self._loaded_path).name if self._loaded_path else ''}",
            f"CSV device rows: {len(self.rows):,}",
            f"Rows mapped to project floors: {mapped_rows:,}",
            f"Wireless profiles: {len(asset_map):,}",
            f"New wireless asset definitions: {create_count:,}",
            f"Nearest-corridor route anchors: {'Yes' if self.attach_corridor_check.isChecked() else 'No'}",
            f"Create upstream network links: {'Yes' if self.auto_connect_check.isChecked() else 'No'}",
            "Drawing layer: Wireless devices",
        ]
        self.review_text.setPlainText("\n".join(lines))

    def _page_changed(self, page_id: int) -> None:
        if self.page(page_id) is self.options_page:
            self._update_review()

    def validateCurrentPage(self) -> bool:
        page = self.currentPage()
        if page is self.file_page:
            return self._load_file(show_errors=True)
        if page is self.columns_page:
            columns = self._columns()
            required = [columns.get(key) for key in ("name", "floor", "x", "y")]
            if not all(required):
                QMessageBox.critical(self, "Wireless import", "Map the device name, floor, X and Y columns.")
                return False
            if len(set(required)) != len(required):
                QMessageBox.critical(self, "Wireless import", "Device name, floor, X and Y must use distinct CSV columns.")
                return False
            if _text(self.coordinate_mode_combo.currentData()) == "insertion_plus_delta":
                if not columns.get("insertion_x") or not columns.get("insertion_y"):
                    QMessageBox.critical(self, "Wireless import", "Select both insertion-point coordinate columns.")
                    return False
            self._rebuild_mapping_pages()
            return True
        if page is self.floor_page:
            if self._floor_map() is None:
                QMessageBox.critical(
                    self,
                    "Wireless import",
                    "Every source floor must map to a numeric project floor or be marked Do not import.",
                )
                return False
        return super().validateCurrentPage()

    def import_config(self) -> dict:
        return {
            "source_file": self._loaded_path,
            "rows": self.rows,
            "columns": self._columns(),
            "floor_map": self._floor_map() or {},
            "asset_map": self._asset_map(),
            "coordinate_mode": _text(self.coordinate_mode_combo.currentData()),
            "coordinate_scale": float(self.scale_spin.value()),
            "x_offset": float(self.x_offset_spin.value()),
            "y_offset": float(self.y_offset_spin.value()),
            "swap_axes": self.swap_axes_check.isChecked(),
            "invert_x": self.invert_x_check.isChecked(),
            "invert_y": self.invert_y_check.isChecked(),
            "attach_to_corridor": self.attach_corridor_check.isChecked(),
            "auto_connect": self.auto_connect_check.isChecked(),
            "duplicate_mode": _text(self.duplicate_combo.currentData()),
        }
