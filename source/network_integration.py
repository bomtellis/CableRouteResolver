"""Runtime integration of network planning into CableRouteResolver.

The extension is installed once, after ``CableRouteEditor`` has been declared.
This keeps the network feature isolated from the room-asset model and allows the
extension to be applied to the current project baseline without replacing the
large application module.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import re
from typing import Optional

from PySide6.QtCore import Qt, QTimer, QSize
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QMessageBox,
    QPushButton,
    QTabWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
    QInputDialog,
    QSizePolicy,
    QStyle,
)

from network_dialogs import (
    NETWORK_TECHNOLOGIES,
    NetworkConnectionEditorDialog,
    NetworkInstanceEditorDialog,
    NetworkPlannerDialog,
    rack_selection_records,
)
from network_auto_planner import (
    auto_connect_manual_devices,
    auto_connect_pending_imported_wireless_devices,
)
from network_reports import write_network_schedules
from network_topology import NetworkTopologyDialog
from network_physical_fibre import PhysicalFibreTopologyDialog
from network_fibre_dialogs import FibreNodeEditorDialog
from network_wireless_import import WirelessDeviceImportWizard, import_wireless_devices
from network_services import circuit_trace, ensure_physical_fibre_for_design, generate_ip_address_plan
from network_schema import (
    NETWORK_SCHEMA_VERSION,
    ensure_network_schema,
    find_nearest_network_instance,
    install_json_store_extensions,
    network_instances_by_id,
    network_instances_for_floor,
    next_network_id,
    validate_network_data,
)
from ui_theme import bootstrap_icon_for


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _install_location_types() -> None:
    """Add MER and PoLAN to existing location dialogs without replacing them."""

    try:
        from dialogs import BulkLocationPlacementDialog, LocationEditorDialog
    except Exception:
        return

    for dialog_class in (LocationEditorDialog, BulkLocationPlacementDialog):
        if getattr(dialog_class, "_network_location_types_installed", False):
            continue
        original_init = dialog_class.__init__

        def init_wrapper(self, *args, __original=original_init, **kwargs):
            __original(self, *args, **kwargs)
            combo = getattr(self, "kind_combo", None)
            if combo is None:
                return
            current = combo.currentText()
            existing = {combo.itemText(index) for index in range(combo.count())}
            for value in ("mer", "polan", "telco_pop", "external_network", "fibre_joint"):
                if value not in existing:
                    combo.addItem(value)
            combo.setToolTip(
                "location = general room; comms_room = conventional telecoms room; "
                "mer = Main Equipment Room/network root; polan = passive optical LAN location; "
                "telco_pop = carrier point of presence; external_network = peering demarcation; "
                "fibre_joint = physical fibre joint location"
            )
            index = combo.findText(current)
            if index >= 0:
                combo.setCurrentIndex(index)

        dialog_class.__init__ = init_wrapper
        dialog_class._network_location_types_installed = True


def _safe_push_undo(editor, label: str) -> None:
    method = getattr(editor, "push_network_undo_state", None)
    if not callable(method):
        method = getattr(editor, "push_undo_state", None)
    if callable(method):
        method(label)


def _ensure_network_schema_current(data: dict) -> dict:
    if int(data.get("network_schema_version", 0) or 0) < NETWORK_SCHEMA_VERSION:
        return ensure_network_schema(data)
    return data


def _schedule_network_canvas_refresh(editor, delay_ms: int = 60) -> None:
    """Coalesce repeated main-canvas redraws from network dialogs."""

    if getattr(editor, "_network_canvas_refresh_pending", False):
        return
    editor._network_canvas_refresh_pending = True

    def refresh() -> None:
        editor._network_canvas_refresh_pending = False
        method = getattr(editor, "refresh_canvas", None)
        if callable(method):
            method()

    QTimer.singleShot(max(0, int(delay_ms)), refresh)


def _network_pick_radius(editor) -> float:
    method = getattr(editor, "_select_pick_radius", None)
    if callable(method):
        try:
            return max(0.2, float(method()))
        except Exception:
            pass
    canvas = getattr(editor, "canvas", None)
    scale = 1.0
    if canvas is not None:
        transform = getattr(canvas, "transform", None)
        if callable(transform):
            try:
                scale = max(0.001, float(transform().m11()))
            except Exception:
                pass
    return max(0.25, 12.0 / scale)


def _is_active_main_graph_asset(asset: dict, instance: Optional[dict] = None) -> bool:
    asset_type = _text(asset.get("asset_type")).lower()
    asset_name = _text(asset.get("name")).lower()
    role = _text((instance or {}).get("design_role")).lower()
    passive_or_power_tokens = (
        "patch", "splitter", "coupler", "adapter", "splice",
        "cable management", "cable-management", "cable manager",
        "ups", "pdu", "power distribution", "power supply",
        "battery", "rectifier", "shelf", "blanking panel",
    )
    if asset_type in {
        "patch_panel", "fibre_splitter", "cable_management",
        "cable_manager", "ups", "pdu", "power_device",
    }:
        return False
    if any(token in asset_name or token in role for token in passive_or_power_tokens):
        return False
    if asset_type in {
        "network_router", "firewall", "network_switch",
        "wireless_access_point", "wireless_device", "optical_line_terminal",
        "optical_network_terminal", "telco_pop", "external_network",
    }:
        return True
    return any(token in role for token in (
        "core", "distribution", "aggregation", "access",
        "gateway", "router", "firewall", "olt", "ont",
        "wireless", "client", "telco", "peering", "external",
    ))


def _find_network_instance(editor, x: float, y: float) -> Optional[str]:
    """Find a selectable main-canvas network instance, excluding patch panels."""
    _ensure_network_schema_current(editor.store.data)
    data = editor.store.data
    assets = {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    floor = int(editor.floor_spin.value())
    radius = _network_pick_radius(editor)
    best_id = None
    best_distance = float(radius)
    for instance_id, instance in network_instances_for_floor(data, floor).items():
        asset = assets.get(_text(instance.get("asset_id")), {})
        asset_type = _text(asset.get("asset_type")).lower()
        asset_name = _text(asset.get("name")).lower()
        if not _is_active_main_graph_asset(asset, instance):
            continue
        dx = float(instance.get("x", 0.0)) - float(x)
        dy = float(instance.get("y", 0.0)) - float(y)
        distance = (dx * dx + dy * dy) ** 0.5
        if distance <= best_distance:
            best_id = instance_id
            best_distance = distance
    return best_id


def _point_segment_distance(
    px: float, py: float, ax: float, ay: float, bx: float, by: float
) -> float:
    dx = bx - ax
    dy = by - ay
    if abs(dx) < 1e-12 and abs(dy) < 1e-12:
        return ((px - ax) ** 2 + (py - ay) ** 2) ** 0.5
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / ((dx * dx) + (dy * dy))))
    cx = ax + t * dx
    cy = ay + t * dy
    return ((px - cx) ** 2 + (py - cy) ** 2) ** 0.5


def _find_network_location(editor, x: float, y: float) -> Optional[str]:
    floor = int(editor.floor_spin.value())
    radius = _network_pick_radius(editor)
    best_name = None
    best_distance = radius
    for name, point in editor.store.all_points().items():
        if _text(point.get("kind")).lower() not in {"mer", "polan"}:
            continue
        if int(point.get("floor", floor)) != floor:
            continue
        distance = (
            (float(point.get("x", 0.0)) - float(x)) ** 2
            + (float(point.get("y", 0.0)) - float(y)) ** 2
        ) ** 0.5
        if distance <= best_distance:
            best_name = name
            best_distance = distance
    return best_name


def _network_connection_segments(
    editor, connection: dict, instances: dict, all_points: dict
):
    source = instances.get(_text(connection.get("from_instance_id")))
    target = instances.get(_text(connection.get("to_instance_id")))
    if not source or not target:
        return []
    route_points = [
        all_points[name]
        for name in connection.get("route_path", [])
        if _text(name) in all_points
    ]
    points = [source] + route_points + [target]
    segments = []
    floor = int(editor.floor_spin.value())
    for a, b in zip(points, points[1:]):
        try:
            af = int(a.get("floor", floor))
            bf = int(b.get("floor", floor))
        except Exception:
            continue
        if af != floor or bf != floor:
            continue
        segments.append(
            (
                float(a.get("x", 0.0)),
                float(a.get("y", 0.0)),
                float(b.get("x", 0.0)),
                float(b.get("y", 0.0)),
            )
        )
    return segments


def _find_network_connection(editor, x: float, y: float) -> Optional[str]:
    ensure_network_schema(editor.store.data)
    instances = network_instances_by_id(editor.store.data)
    all_points = editor.store.all_points()
    radius = max(_network_pick_radius(editor), 0.5)
    best_id = None
    best_distance = radius
    for connection in editor.store.data.get("network_connections", []):
        if not isinstance(connection, dict):
            continue
        connection_id = _text(connection.get("id"))
        if not connection_id:
            continue
        for ax, ay, bx, by in _network_connection_segments(
            editor, connection, instances, all_points
        ):
            distance = _point_segment_distance(float(x), float(y), ax, ay, bx, by)
            if distance <= best_distance:
                best_id = connection_id
                best_distance = distance
    return best_id


def _find_routing_edge(editor, x: float, y: float) -> Optional[str]:
    """Return the nearest ordinary routing-graph edge on the current floor.

    The network extension wraps the editor's right-click handler.  Ordinary
    graph edges must be detected before network overlays outside Network mode,
    otherwise a nearby network symbol/link can consume the click and prevent
    the editor's original Delete Edge menu from opening.
    """

    all_points = editor.store.all_points()
    floor = int(editor.floor_spin.value())
    # Keep this tolerance slightly tighter than the network overlay tolerance.
    # It only decides whether to delegate to the original handler; the original
    # editor still performs its own final hit-test before showing its menu.
    radius = max(0.25, _network_pick_radius(editor) * 0.65)
    best_id = None
    best_distance = radius

    for index, connection in enumerate(editor.store.data.get("connections", [])):
        if not isinstance(connection, dict):
            continue
        from_name = _text(connection.get("from"))
        to_name = _text(connection.get("to"))
        start = all_points.get(from_name)
        end = all_points.get(to_name)
        if not start or not end:
            continue
        try:
            start_floor = int(start.get("floor", floor))
            end_floor = int(end.get("floor", floor))
        except (TypeError, ValueError):
            continue
        if start_floor != floor or end_floor != floor:
            continue

        distance = _point_segment_distance(
            float(x),
            float(y),
            float(start.get("x", 0.0)),
            float(start.get("y", 0.0)),
            float(end.get("x", 0.0)),
            float(end.get("y", 0.0)),
        )
        if distance <= best_distance:
            best_distance = distance
            best_id = _text(connection.get("id")) or f"connection:{index}"

    return best_id


def _move_network_location(editor, name: str, x: float, y: float) -> bool:
    moved = False
    floor = int(editor.floor_spin.value())
    for location in editor.store.data.get("locations", []):
        if _text(location.get("name")) == name and _text(
            location.get("kind")
        ).lower() in {"mer", "polan"}:
            location["x"] = round(float(x), 3)
            location["y"] = round(float(y), 3)
            location["floor"] = floor
            moved = True
            break
    if not moved:
        return False
    for instance in editor.store.data.get("network_asset_instances", []):
        if _text(instance.get("location_name")) == name:
            instance["x"] = round(float(x), 3)
            instance["y"] = round(float(y), 3)
            instance["floor"] = floor
    return True


def _begin_network_selection(editor, x: float, y: float) -> bool:
    instance_id = _find_network_instance(editor, x, y)
    if instance_id:
        editor.selected_point_name = instance_id
        editor._network_drag_instance_id = instance_id
        editor._network_drag_location_name = None
        _safe_push_undo(editor, "Move network asset")
        editor.refresh_canvas()
        if hasattr(editor, "set_status"):
            editor.set_status(f"Selected network asset {instance_id}")
        return True

    location_name = _find_network_location(editor, x, y)
    if location_name:
        editor.selected_point_name = location_name
        editor._network_drag_instance_id = None
        editor._network_drag_location_name = location_name
        _safe_push_undo(editor, "Move network location")
        editor.refresh_canvas()
        if hasattr(editor, "set_status"):
            editor.set_status(f"Selected network location {location_name}")
        return True

    connection_id = _find_network_connection(editor, x, y)
    if connection_id:
        editor.selected_point_name = connection_id
        editor._network_drag_instance_id = None
        editor._network_drag_location_name = None
        editor.refresh_canvas()
        if hasattr(editor, "set_status"):
            editor.set_status(f"Selected network connection {connection_id}")
        return True

    editor.selected_point_name = None
    editor._network_drag_instance_id = None
    editor._network_drag_location_name = None
    editor.refresh_canvas()
    return True


def _replace_by_id(items: list, value: dict, old_id: str = "") -> bool:
    value_id = _text(value.get("id"))
    for index, item in enumerate(items):
        item_id = _text(item.get("id"))
        if old_id and item_id == old_id:
            items[index] = value
            return True
        if not old_id and item_id == value_id:
            items[index] = value
            return True
    items.append(value)
    return False


def _open_network_planner(editor) -> None:
    _ensure_network_schema_current(editor.store.data)

    def save(payload: dict) -> None:
        _safe_push_undo(editor, "Update network planning data")
        for key in (
            "network_settings", "assets", "locations", "network_assets",
            "network_asset_instances", "network_racks", "network_connections",
            "network_power_connections", "network_endpoint_assignments",
            "network_redundancy_groups", "network_vlans", "network_routes",
            "network_ip_allocations", "network_external_networks",
            "network_optic_modules", "network_optical_paths",
            "network_fibre_cable_types", "network_fibre_cables",
            "network_fibre_nodes", "network_fibre_splices",
            "network_patch_leads", "network_design_summary",
        ):
            if key in payload:
                editor.store.data[key] = payload[key]
        ensure_network_schema(editor.store.data)
        _sync_network_technology_controls(editor)
        _schedule_network_canvas_refresh(editor)
        refresh = getattr(editor, "refresh_rhs_search_sidebar", None)
        if callable(refresh):
            refresh()


    dialog = NetworkPlannerDialog(editor, editor.store.data, save)
    editor._network_planner_dialog = dialog
    dialog.exec()
    editor._network_planner_dialog = None


def _open_network_topology(editor) -> None:
    """Open the editable logical network hierarchy and rack/device views."""
    _ensure_network_schema_current(editor.store.data)
    windows = getattr(editor, "_network_topology_windows", None)
    if windows is None:
        windows = []
        editor._network_topology_windows = windows

    dialog = NetworkTopologyDialog(editor, editor.store.data, on_change=lambda payload: _apply_network_payload(editor, payload))
    windows.append(dialog)
    editor._network_topology_dialog = dialog

    def forget_window() -> None:
        if dialog in windows:
            windows.remove(dialog)
        if getattr(editor, "_network_topology_dialog", None) is dialog:
            editor._network_topology_dialog = windows[-1] if windows else None

    dialog.destroyed.connect(forget_window)
    dialog.showMaximized()


def _apply_network_payload(editor, payload: dict) -> None:
    """Apply network-only edits without copying or redrawing the whole project."""

    _safe_push_undo(editor, "Edit network topology")
    changed = False
    for key in (
        "network_settings", "network_assets", "network_asset_instances",
        "network_racks", "network_connections", "network_power_connections",
        "network_endpoint_assignments", "network_patch_leads",
        "network_redundancy_groups", "network_vlans", "network_routes",
        "network_ip_allocations", "network_external_networks",
        "network_optic_modules", "network_optical_paths",
        "network_fibre_cable_types", "network_fibre_cables",
        "network_fibre_nodes", "network_fibre_splices",
        "network_design_summary", "locations", "assets",
    ):
        if key not in payload:
            continue
        value = payload[key]
        if value is editor.store.data.get(key):
            continue
        editor.store.data[key] = deepcopy(value)
        changed = True
    if changed:
        ensure_network_schema(editor.store.data)
    _schedule_network_canvas_refresh(editor)


def _open_physical_fibre_topology(editor, connection_id: str = "") -> None:
    """Open the floor-aware physical fibre overlay and splice editor."""
    _ensure_network_schema_current(editor.store.data)
    windows = getattr(editor, "_physical_fibre_windows", None)
    if windows is None:
        windows = []
        editor._physical_fibre_windows = windows
    dialog = PhysicalFibreTopologyDialog(
        editor,
        editor.store.data,
        on_change=lambda payload: _apply_network_payload(editor, payload),
        initial_trace_connection_id=connection_id,
    )
    windows.append(dialog)
    editor._physical_fibre_dialog = dialog

    def forget_window() -> None:
        if dialog in windows:
            windows.remove(dialog)
        if getattr(editor, "_physical_fibre_dialog", None) is dialog:
            editor._physical_fibre_dialog = windows[-1] if windows else None

    dialog.destroyed.connect(forget_window)
    dialog.showMaximized()


def _generate_network_ip_plan(editor) -> None:
    ensure_network_schema(editor.store.data)
    try:
        result = generate_ip_address_plan(editor.store.data)
    except Exception as exc:
        QMessageBox.critical(editor, "IP address plan", str(exc))
        return
    editor.refresh_canvas()
    QMessageBox.information(
        editor,
        "IP address plan",
        f"Generated {result.get('vlan_count', 0)} VLAN subnet(s), "
        f"{result.get('management_allocations', 0)} management addresses and router interfaces.",
    )


def _synchronise_physical_fibre(editor) -> None:
    ensure_network_schema(editor.store.data)
    summary = ensure_physical_fibre_for_design(editor.store.data, replace_auto=False)
    editor.refresh_canvas()
    QMessageBox.information(
        editor,
        "Physical fibre layer",
        f"Synchronised {summary.get('cable_count', 0)} routed fibre cable(s) and "
        f"{summary.get('node_count', 0)} passive fibre node(s).",
    )


def _import_wireless_devices(editor) -> None:
    ensure_network_schema(editor.store.data)
    wizard = WirelessDeviceImportWizard(editor, editor.store.data)
    if wizard.exec() != QDialog.Accepted:
        return
    _safe_push_undo(editor, "Import wireless devices")
    try:
        result = import_wireless_devices(editor.store.data, wizard.import_config())
    except Exception as exc:
        QMessageBox.critical(editor, "Wireless device import", str(exc))
        return

    imported_ids = list(result.get("created_instance_ids", [])) + list(
        result.get("updated_instance_ids", [])
    )
    if imported_ids:
        editor.selected_point_name = imported_ids[0]
        first = network_instances_by_id(editor.store.data).get(imported_ids[0], {})
        try:
            editor.floor_spin.setValue(int(first.get("floor", editor.floor_spin.value())))
        except (TypeError, ValueError):
            pass
    editor.refresh_canvas()

    auto_result = result.get("auto_connect", {}) or {}
    lines = [
        f"Created devices: {len(result.get('created_instance_ids', [])):,}",
        f"Updated devices: {len(result.get('updated_instance_ids', [])):,}",
        f"Created wireless asset models: {len(result.get('created_asset_ids', [])):,}",
        f"Skipped existing rows: {int(result.get('skipped_rows', 0)):,}",
        f"Rows not mapped to a floor: {int(result.get('unmapped_rows', 0)):,}",
        f"Invalid rows: {int(result.get('invalid_rows', 0)):,}",
        f"Devices assigned an inferred department: {int(result.get('department_inferred', 0)):,}",
        f"Department conflicts resolved in the wizard: {int(result.get('department_resolved', 0)):,}",
        f"Devices left without a department: {int(result.get('department_unassigned', 0)):,}",
        f"Devices anchored to the corridor graph: {int(result.get('graph_anchored', 0)):,}",
        f"Devices without a same-floor corridor node: {int(result.get('without_corridor', 0)):,}",
    ]
    if auto_result:
        lines.append(
            f"Created upstream network links: {len(auto_result.get('created_connection_ids', [])):,}"
        )
    warnings = list(result.get("warnings", []))
    if warnings:
        lines.extend(["", "Warnings:"] + [f"• {value}" for value in warnings[:12]])
        if len(warnings) > 12:
            lines.append(f"• …and {len(warnings) - 12:,} more")
    QMessageBox.information(editor, "Wireless device import", "\n".join(lines))
    if hasattr(editor, "set_status"):
        editor.set_status(
            f"Imported {len(imported_ids):,} wireless devices onto the Wireless devices layer"
        )


def _export_network_schedules(editor) -> None:
    ensure_network_schema(editor.store.data)
    output_directory = QFileDialog.getExistingDirectory(
        editor,
        "Select network schedule output folder",
        (
            str(Path(editor.current_json_path).parent)
            if getattr(editor, "current_json_path", None)
            else ""
        ),
    )
    if not output_directory:
        return

    if getattr(editor, "current_json_path", None):
        prefix = Path(editor.current_json_path).stem
    else:
        prefix = _text(editor.store.data.get("project", {}).get("name")) or "network"
    prefix = re.sub(r"[^A-Za-z0-9_.-]+", "_", prefix).strip("_") or "network"

    try:
        paths = write_network_schedules(
            editor.store.data, Path(output_directory), prefix
        )
    except Exception as exc:
        QMessageBox.critical(editor, "Network schedules", str(exc))
        return

    QMessageBox.information(
        editor,
        "Network schedules",
        "Created the following schedules:\n\n" + "\n".join(path.name for path in paths),
    )
    if hasattr(editor, "set_status"):
        editor.set_status(f"Created {len(paths)} network schedule CSV files")


def _validate_network(editor) -> None:
    messages = validate_network_data(editor.store.data, include_advisories=True)
    if not messages:
        QMessageBox.information(
            editor, "Network validation", "No network planning issues were found."
        )
        return
    QMessageBox.warning(
        editor, "Network validation", "\n".join(f"• {item}" for item in messages)
    )


def _set_network_mode(editor, mode: str) -> None:
    setter = getattr(editor, "_set_editor_mode", None)
    if callable(setter):
        setter(mode)
    elif hasattr(editor, "mode_combo"):
        editor.mode_combo.setCurrentText(mode)


def _technology_changed(editor, value: str) -> None:
    ensure_network_schema(editor.store.data)
    editor.store.data["network_settings"]["technology"] = (
        "PoLAN" if value == "PoLAN" else "Traditional"
    )
    if hasattr(editor, "set_status"):
        editor.set_status(
            f"Network technology: {editor.store.data['network_settings']['technology']}"
        )


def _sync_network_technology_controls(editor) -> None:
    ensure_network_schema(editor.store.data)
    combo = getattr(editor, "network_technology_combo", None)
    if combo is None:
        return
    value = editor.store.data["network_settings"].get("technology", "Traditional")
    combo.blockSignals(True)
    combo.setCurrentText(value)
    combo.blockSignals(False)


def _add_network_layer_action(editor) -> None:
    for button in editor.findChildren(QToolButton):
        if button.text().strip().lower() != "layers" or button.menu() is None:
            continue
        menu = button.menu()
        if any(action.text() == "Network Planning" for action in menu.actions()):
            return
        menu.addSeparator()
        for label, target in (
            ("Network Planning", editor.show_network_check),
            (
                "Network Assets",
                getattr(editor, "show_network_assets_check", editor.show_network_check),
            ),
            (
                "Network Links",
                getattr(
                    editor, "show_network_connections_check", editor.show_network_check
                ),
            ),
            (
                "Wireless Devices",
                getattr(
                    editor, "show_wireless_devices_check", editor.show_network_check
                ),
            ),
            (
                "Physical Fibre",
                getattr(editor, "show_physical_fibre_check", editor.show_network_check),
            ),
        ):
            action = QAction(label, menu)
            action.setCheckable(True)
            action.setChecked(target.isChecked())
            action.toggled.connect(target.setChecked)
            target.toggled.connect(action.setChecked)
            menu.addAction(action)
        return


def _add_network_search_tab(editor) -> None:
    tabs = getattr(editor, "search_tabs", None)
    search_lists = getattr(editor, "search_lists", None)
    if (
        tabs is None
        or not isinstance(search_lists, dict)
        or "Network Assets" in search_lists
    ):
        return
    widget = QListWidget()
    widget.itemDoubleClicked.connect(editor._rhs_search_item_activated)
    tabs.addTab(widget, "Network Assets")
    search_lists["Network Assets"] = widget



def _network_ribbon_icon(editor, icon_enum):
    try:
        return bootstrap_icon_for(icon_enum)
    except Exception:
        return editor.style().standardIcon(QStyle.SP_FileIcon)


def _configure_network_ribbon_control(editor, button: QToolButton) -> QToolButton:
    button.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
    button.setIconSize(QSize(18, 18))
    button.setMinimumSize(96, 30)
    button.setMaximumSize(142, 34)
    button.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
    button.setAutoRaise(False)
    configure = getattr(editor, "_configure_ribbon_button", None)
    if callable(configure):
        configure(button)
    return button


def _network_ribbon_button(editor, text: str, handler, icon_enum, tooltip: str = "") -> QToolButton:
    button = QToolButton()
    button.setText(text)
    button.setToolTip(tooltip or text)
    button.setIcon(_network_ribbon_icon(editor, icon_enum))
    _configure_network_ribbon_control(editor, button)
    button.clicked.connect(handler)
    return button


def _network_ribbon_toggle(editor, text: str, icon_enum, checked: bool = True, tooltip: str = "") -> QToolButton:
    button = QToolButton()
    button.setText(text)
    button.setToolTip(tooltip or text)
    button.setCheckable(True)
    button.setChecked(bool(checked))
    button.setIcon(_network_ribbon_icon(editor, icon_enum))
    _configure_network_ribbon_control(editor, button)
    return button


def _add_network_ribbon_group(editor, parent_layout, title: str, widgets, columns: int = 3) -> None:
    add_group = getattr(editor, "_add_ribbon_group", None)
    if callable(add_group):
        add_group(parent_layout, title, widgets, columns=columns)
        return

    panel = QWidget()
    panel_layout = QVBoxLayout(panel)
    panel_layout.setContentsMargins(4, 4, 4, 4)
    if widgets and all(isinstance(row, list) for row in widgets):
        for row in widgets:
            row_widget = QWidget()
            row_layout = QHBoxLayout(row_widget)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(4)
            for widget in row:
                row_layout.addWidget(widget)
            panel_layout.addWidget(row_widget)
    else:
        row_widget = QWidget()
        row_layout = QHBoxLayout(row_widget)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(4)
        for widget in widgets:
            row_layout.addWidget(widget)
        panel_layout.addWidget(row_widget)
    label = QLabel(title)
    label.setAlignment(Qt.AlignCenter)
    panel_layout.addWidget(label)
    parent_layout.addWidget(panel)

def _augment_network_ui(editor) -> None:
    ensure_network_schema(editor.store.data)

    editor.show_network_check = _network_ribbon_toggle(
        editor,
        "Planning",
        QStyle.SP_ComputerIcon,
        True,
        "Show network planning layer",
    )
    editor.show_network_check.toggled.connect(editor.refresh_canvas)

    editor.show_network_assets_check = _network_ribbon_toggle(
        editor,
        "Assets",
        QStyle.SP_DriveNetIcon,
        True,
        "Show installed network assets",
    )
    editor.show_network_assets_check.toggled.connect(editor.refresh_canvas)

    editor.show_network_connections_check = _network_ribbon_toggle(
        editor,
        "Links",
        QStyle.SP_ArrowRight,
        True,
        "Show network links and connections",
    )
    editor.show_network_connections_check.toggled.connect(editor.refresh_canvas)

    editor.show_wireless_devices_check = _network_ribbon_toggle(
        editor,
        "Wireless",
        QStyle.SP_DriveNetIcon,
        True,
        "Show imported wireless devices",
    )
    editor.show_wireless_devices_check.toggled.connect(editor.refresh_canvas)

    editor.show_physical_fibre_check = _network_ribbon_toggle(
        editor,
        "Fibre",
        QStyle.SP_DirLinkIcon,
        bool(
            editor.store.data.get("network_settings", {})
            .get("physical_fibre_layer", {})
            .get("visible", True)
        ),
        "Show physical fibre layer",
    )
    editor.show_physical_fibre_check.toggled.connect(
        lambda checked: editor.store.data.setdefault("network_settings", {})
        .setdefault("physical_fibre_layer", {})
        .__setitem__("visible", bool(checked))
    )
    editor.show_physical_fibre_check.toggled.connect(editor.refresh_canvas)

    ribbon = editor.findChild(QTabWidget, "AeroRibbon")
    if ribbon is not None:
        tab = QWidget()
        layout = QHBoxLayout(tab)
        layout.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        layout.setContentsMargins(10, 1, 10, 1)
        layout.setSpacing(10)

        editor.network_technology_combo = QComboBox()
        editor.network_technology_combo.addItems(NETWORK_TECHNOLOGIES)
        editor.network_technology_combo.setCurrentText(
            editor.store.data["network_settings"].get("technology", "Traditional")
        )
        editor.network_technology_combo.currentTextChanged.connect(
            lambda value: _technology_changed(editor, value)
        )
        editor.network_technology_combo.setMinimumWidth(126)
        editor.network_technology_combo.setMaximumWidth(150)

        _add_network_ribbon_group(
            editor,
            layout,
            "Layers",
            [
                [QLabel("Technology"), editor.network_technology_combo, editor.show_network_check, editor.show_network_assets_check, editor.show_network_connections_check],
                [editor.show_wireless_devices_check, editor.show_physical_fibre_check],
            ],
            columns=3,
        )

        _add_network_ribbon_group(
            editor,
            layout,
            "Design",
            [
                _network_ribbon_button(
                    editor,
                    "Topology",
                    lambda: _open_network_topology(editor),
                    QStyle.SP_FileDialogDetailedView,
                    "Edit the logical network hierarchy, racks, devices and patching",
                ),
                _network_ribbon_button(
                    editor,
                    "Fibre View",
                    lambda: _open_physical_fibre_topology(editor),
                    QStyle.SP_DirLinkIcon,
                    "Edit routed fibre cables, enclosures, cassettes, joints and core splices",
                ),
                _network_ribbon_button(
                    editor,
                    "Planner",
                    lambda: _open_network_planner(editor),
                    QStyle.SP_ComputerIcon,
                    "Open the automatic network planner",
                ),
                _network_ribbon_button(
                    editor,
                    "Wireless CSV",
                    lambda: _import_wireless_devices(editor),
                    QStyle.SP_DialogOpenButton,
                    "Import wireless access points, IoT radio gateways and similar devices",
                ),
                _network_ribbon_button(
                    editor,
                    "Validate",
                    lambda: _validate_network(editor),
                    QStyle.SP_DialogApplyButton,
                    "Validate network planning data",
                ),
                _network_ribbon_button(
                    editor,
                    "Sync Fibre",
                    lambda: _synchronise_physical_fibre(editor),
                    QStyle.SP_BrowserReload,
                    "Synchronise logical network links to physical fibre routes",
                ),
                _network_ribbon_button(
                    editor,
                    "IP Plan",
                    lambda: _generate_network_ip_plan(editor),
                    QStyle.SP_FileDialogContentsView,
                    "Generate an IP address plan",
                ),
                _network_ribbon_button(
                    editor,
                    "Schedules",
                    lambda: _export_network_schedules(editor),
                    QStyle.SP_DriveHDIcon,
                    "Export network schedules",
                ),
            ],
            columns=4,
        )

        _add_network_ribbon_group(
            editor,
            layout,
            "Placement",
            [
                _network_ribbon_button(
                    editor,
                    "Select",
                    lambda: _set_network_mode(editor, "network_select"),
                    QStyle.SP_ArrowUp,
                    "Select, move, edit or delete network items",
                ),
                _network_ribbon_button(
                    editor,
                    "Asset",
                    lambda: _set_network_mode(editor, "network_asset"),
                    QStyle.SP_DriveNetIcon,
                    "Place a network asset instance",
                ),
                _network_ribbon_button(
                    editor,
                    "Connect",
                    lambda: _set_network_mode(editor, "network_connection"),
                    QStyle.SP_ArrowRight,
                    "Connect network assets",
                ),
                _network_ribbon_button(
                    editor,
                    "Fibre Node",
                    lambda: _set_network_mode(editor, "fibre_node"),
                    QStyle.SP_DirLinkIcon,
                    "Place a physical fibre node",
                ),
                _network_ribbon_button(
                    editor,
                    "MER",
                    lambda: _set_network_mode(editor, "mer_location"),
                    QStyle.SP_DirHomeIcon,
                    "Place a main equipment room",
                ),
                _network_ribbon_button(
                    editor,
                    "PoLAN",
                    lambda: _set_network_mode(editor, "polan_location"),
                    QStyle.SP_DirIcon,
                    "Place a PoLAN location",
                ),
            ],
            columns=3,
        )

        layout.addStretch(1)
        ribbon.addTab(tab, _network_ribbon_icon(editor, QStyle.SP_DriveNetIcon), "Network")
    else:
        # Compatibility fallback for older sidebar-based layouts.
        dock_parent = editor.centralWidget().layout() if editor.centralWidget() else None
        if dock_parent is not None:
            panel = QWidget()
            panel_layout = QHBoxLayout(panel)
            for text, handler in (
                ("Topology", lambda: _open_network_topology(editor)),
                ("Network Planner", lambda: _open_network_planner(editor)),
                ("Import Wireless CSV", lambda: _import_wireless_devices(editor)),
                ("Network Mode", lambda: _set_network_mode(editor, "network_select")),
                ("Place Network Asset", lambda: _set_network_mode(editor, "network_asset")),
                ("Connect Network Assets", lambda: _set_network_mode(editor, "network_connection")),
                ("Export Network Schedules", lambda: _export_network_schedules(editor)),
            ):
                control = QPushButton(text)
                control.clicked.connect(handler)
                panel_layout.addWidget(control)
            dock_parent.insertWidget(0, panel)

    _add_network_layer_action(editor)
    _add_network_search_tab(editor)

def _add_network_location(editor, kind: str, x: float, y: float) -> None:
    floor = int(editor.floor_spin.value())
    existing = [
        item
        for item in editor.store.data.get("locations", [])
        if _text(item.get("kind")).lower() == kind
    ]
    prefix = {"mer": "MER", "polan": "POLAN", "telco_pop": "TELCO-POP", "external_network": "EXT-NET", "fibre_joint": "FIBRE-JOINT"}.get(kind, kind.upper())
    default_name = f"{prefix}-{len(existing) + 1}"
    name, ok = QInputDialog.getText(
        editor,
        {"mer": "Main Equipment Room", "polan": "PoLAN location", "telco_pop": "Telecommunications Point of Presence", "external_network": "External Network Demarcation", "fibre_joint": "Fibre Joint Location"}.get(kind, "Network location"),
        "Location name:",
        text=default_name,
    )
    if not ok or not _text(name):
        return
    name = _text(name)
    if name in editor.store.names_in_use():
        QMessageBox.critical(
            editor, "Duplicate name", "A point with this name already exists."
        )
        return
    _safe_push_undo(editor, f"Add {prefix} location")
    editor.store.add_location(
        name, floor, float(x), float(y), kind=kind, department_ids=[]
    )
    editor.selected_point_name = name
    editor.refresh_canvas()
    if hasattr(editor, "set_status"):
        editor.set_status(f"Added {prefix} location {name}")


def _place_fibre_node(editor, x: float, y: float) -> None:
    """Place a passive physical-fibre object on the current DXF floor."""
    ensure_network_schema(editor.store.data)
    dialog = FibreNodeEditorDialog(
        editor,
        nodes=editor.store.data.get("network_fibre_nodes", []),
        instances=editor.store.data.get("network_asset_instances", []),
        locations=editor.store.data.get("locations", []),
        racks=rack_selection_records(editor.store.data),
        suggested_id=next_network_id(editor.store.data.get("network_fibre_nodes", []), "FN"),
        default_floor=int(editor.floor_spin.value()),
        default_x=float(x),
        default_y=float(y),
        default_auto_connect=bool(
            editor.store.data.get("network_settings", {}).get(
                "auto_connect_new_manual_devices", True
            )
        ),
    )
    if dialog.exec() != QDialog.Accepted or not dialog.result:
        return
    _safe_push_undo(editor, "Place physical fibre node")
    editor.store.data.setdefault("network_fibre_nodes", []).append(dialog.result)
    editor.selected_point_name = dialog.result["id"]
    editor.refresh_canvas()
    if hasattr(editor, "set_status"):
        editor.set_status(f"Added physical fibre node {dialog.result['id']} on floor {dialog.result['floor']}")


def _place_or_select_network_asset(editor, x: float, y: float) -> None:
    instance_id = _find_network_instance(editor, x, y)
    if instance_id:
        editor.selected_point_name = instance_id
        editor._network_drag_instance_id = instance_id
        editor.refresh_canvas()
        if hasattr(editor, "set_status"):
            editor.set_status(
                f"Selected network asset {instance_id}; double-click to edit"
            )
        return

    assets = editor.store.data.get("network_assets", [])
    if not assets:
        QMessageBox.information(
            editor,
            "Network asset library",
            "Create at least one network asset definition in Network Planner before placing an instance.",
        )
        _open_network_planner(editor)
        return

    dialog = NetworkInstanceEditorDialog(
        editor,
        assets=assets,
        locations=editor.store.data.get("locations", []),
        racks=rack_selection_records(editor.store.data),
        suggested_id=next_network_id(
            editor.store.data.get("network_asset_instances", []), "NI"
        ),
        default_floor=int(editor.floor_spin.value()),
        default_x=float(x),
        default_y=float(y),
    )
    if dialog.exec() != QDialog.Accepted or not dialog.result:
        return
    _safe_push_undo(editor, "Place network asset")
    editor.store.data["network_asset_instances"].append(dialog.result)
    auto_result = None
    if dialog.auto_connect_requested:
        auto_result = auto_connect_manual_devices(
            editor.store.data, [_text(dialog.result.get("id"))]
        )
    wireless_result = auto_connect_pending_imported_wireless_devices(
        editor.store.data
    )
    editor.selected_point_name = dialog.result["id"]
    editor.refresh_canvas()
    if hasattr(editor, "set_status"):
        created = len((auto_result or {}).get("created_connection_ids", []))
        wireless_created = len(
            (wireless_result or {}).get("created_connection_ids", [])
        )
        suffix_parts = []
        if created:
            suffix_parts.append(f"auto-connected {created} device link(s)")
        if wireless_created:
            suffix_parts.append(
                f"connected {wireless_created} pending wireless device(s)"
            )
        suffix = f" and {', '.join(suffix_parts)}" if suffix_parts else ""
        editor.set_status(f"Placed network asset {dialog.result['id']}{suffix}")


def _connect_network_asset(editor, x: float, y: float) -> None:
    picked = _find_network_instance(editor, x, y)
    if not picked:
        if hasattr(editor, "set_status"):
            editor.set_status("No nearby network asset instance")
        return

    if not getattr(editor, "_network_connection_start", None):
        editor._network_connection_start = picked
        editor.selected_point_name = picked
        editor.refresh_canvas()
        if hasattr(editor, "set_status"):
            editor.set_status(
                f"Network connection start: {picked}; select the destination"
            )
        return

    start = editor._network_connection_start
    editor._network_connection_start = None
    if start == picked:
        if hasattr(editor, "set_status"):
            editor.set_status("Network connection cancelled: endpoints are the same")
        return

    dialog = NetworkConnectionEditorDialog(
        editor,
        instances=editor.store.data.get("network_asset_instances", []),
        vlans=editor.store.data.get("network_vlans", []),
        route_profiles=list(editor.store.data.get("route_profiles", {}).keys()),
        suggested_id=next_network_id(
            editor.store.data.get("network_connections", []), "NC"
        ),
        default_from=start,
        default_to=picked,
        assets=editor.store.data.get("network_assets", []),
    )
    if dialog.exec() == QDialog.Accepted and dialog.result:
        _safe_push_undo(editor, "Add network connection")
        editor.store.data["network_connections"].append(dialog.result)
        editor.selected_point_name = picked
        editor.refresh_canvas()
        if hasattr(editor, "set_status"):
            editor.set_status(f"Added network connection {dialog.result['id']}")


def _edit_network_instance(editor, instance_id: str) -> None:
    instances = editor.store.data.get("network_asset_instances", [])
    current = next(
        (item for item in instances if _text(item.get("id")) == instance_id), None
    )
    if current is None:
        return
    dialog = NetworkInstanceEditorDialog(
        editor,
        instance=current,
        assets=editor.store.data.get("network_assets", []),
        locations=editor.store.data.get("locations", []),
        racks=rack_selection_records(editor.store.data),
        suggested_id=instance_id,
    )
    if dialog.exec() != QDialog.Accepted or not dialog.result:
        return
    new_id = _text(dialog.result.get("id"))
    if new_id != instance_id and any(
        _text(item.get("id")) == new_id for item in instances
    ):
        QMessageBox.critical(
            editor, "Duplicate ID", f"Network instance {new_id} already exists."
        )
        return
    _safe_push_undo(editor, "Edit network asset instance")
    _replace_by_id(instances, dialog.result, old_id=instance_id)
    if new_id != instance_id:
        for connection in editor.store.data.get("network_connections", []):
            if _text(connection.get("from_instance_id")) == instance_id:
                connection["from_instance_id"] = new_id
            if _text(connection.get("to_instance_id")) == instance_id:
                connection["to_instance_id"] = new_id
        for power_link in editor.store.data.get("network_power_connections", []):
            if _text(power_link.get("from_instance_id")) == instance_id:
                power_link["from_instance_id"] = new_id
            if _text(power_link.get("to_instance_id")) == instance_id:
                power_link["to_instance_id"] = new_id
    if dialog.auto_connect_requested:
        auto_connect_manual_devices(editor.store.data, [new_id])
    auto_connect_pending_imported_wireless_devices(editor.store.data)
    editor.selected_point_name = new_id
    editor.refresh_canvas()


def _delete_network_instance(editor, instance_id: str) -> None:
    if (
        QMessageBox.question(
            editor,
            "Delete network asset",
            f"Delete installed network asset {instance_id} and all of its network connections?",
        )
        != QMessageBox.Yes
    ):
        return
    _safe_push_undo(editor, "Delete network asset instance")
    editor.store.data["network_asset_instances"] = [
        item
        for item in editor.store.data.get("network_asset_instances", [])
        if _text(item.get("id")) != instance_id
    ]
    editor.store.data["network_connections"] = [
        item
        for item in editor.store.data.get("network_connections", [])
        if _text(item.get("from_instance_id")) != instance_id
        and _text(item.get("to_instance_id")) != instance_id
    ]
    editor.store.data["network_power_connections"] = [
        item
        for item in editor.store.data.get("network_power_connections", [])
        if _text(item.get("from_instance_id")) != instance_id
        and _text(item.get("to_instance_id")) != instance_id
    ]
    editor.selected_point_name = None
    editor.refresh_canvas()


def _edit_network_location(editor, name: str) -> bool:
    point = editor.store.all_points().get(name)
    if not point or _text(point.get("kind")).lower() not in {"mer", "polan"}:
        return False
    try:
        from dialogs import LocationEditorDialog
    except Exception:
        return False
    item = next(
        (
            row
            for row in editor.store.data.get("locations", [])
            if _text(row.get("name")) == name
        ),
        None,
    )
    if item is None:
        return False
    options = (
        editor.department_options() if hasattr(editor, "department_options") else []
    )
    dialog = LocationEditorDialog(editor, name, dict(item), options)
    if dialog.exec() != QDialog.Accepted or not dialog.result:
        return True
    new_name = _text(dialog.result.get("name"))
    if new_name != name and new_name in editor.store.names_in_use():
        QMessageBox.critical(editor, "Duplicate name", "Name already exists")
        return True
    _safe_push_undo(editor, "Edit network location")
    editor.store.set_point_position(name, dialog.result["x"], dialog.result["y"])
    editor.store.rename_point(name, new_name)
    for location in editor.store.data.get("locations", []):
        if _text(location.get("name")) == new_name:
            location["kind"] = dialog.result["kind"]
            location["department_ids"] = list(dialog.result.get("department_ids", []))
            break
    for instance in editor.store.data.get("network_asset_instances", []):
        if _text(instance.get("location_name")) == name:
            instance["location_name"] = new_name
    editor.selected_point_name = new_name
    editor.refresh_canvas()
    return True


def _delete_network_location(editor, name: str) -> None:
    linked_instances = [
        item
        for item in editor.store.data.get("network_asset_instances", [])
        if _text(item.get("location_name")) == name
    ]
    extra = ""
    if linked_instances:
        extra = f"\n\nThis will also delete {len(linked_instances)} installed network asset(s) and their network connections."
    if (
        QMessageBox.question(
            editor,
            "Delete network location",
            f"Delete network location {name}?{extra}",
        )
        != QMessageBox.Yes
    ):
        return
    linked_ids = {
        _text(item.get("id")) for item in linked_instances if _text(item.get("id"))
    }
    _safe_push_undo(editor, "Delete network location")
    editor.store.data["locations"] = [
        item
        for item in editor.store.data.get("locations", [])
        if _text(item.get("name")) != name
    ]
    if linked_ids:
        editor.store.data["network_asset_instances"] = [
            item
            for item in editor.store.data.get("network_asset_instances", [])
            if _text(item.get("id")) not in linked_ids
        ]
        editor.store.data["network_connections"] = [
            item
            for item in editor.store.data.get("network_connections", [])
            if _text(item.get("from_instance_id")) not in linked_ids
            and _text(item.get("to_instance_id")) not in linked_ids
        ]
        editor.store.data["network_endpoint_assignments"] = [
            item
            for item in editor.store.data.get("network_endpoint_assignments", [])
            if _text(item.get("network_instance_id")) not in linked_ids
        ]
    editor.selected_point_name = None
    editor.refresh_canvas()


def _edit_network_connection(editor, connection_id: str) -> None:
    connections = editor.store.data.get("network_connections", [])
    current = next(
        (item for item in connections if _text(item.get("id")) == connection_id), None
    )
    if current is None:
        return
    dialog = NetworkConnectionEditorDialog(
        editor,
        connection=current,
        instances=editor.store.data.get("network_asset_instances", []),
        vlans=editor.store.data.get("network_vlans", []),
        route_profiles=list(editor.store.data.get("route_profiles", {}).keys()),
        suggested_id=connection_id,
        assets=editor.store.data.get("network_assets", []),
    )
    if dialog.exec() != QDialog.Accepted or not dialog.result:
        return
    new_id = _text(dialog.result.get("id"))
    if new_id != connection_id and any(
        _text(item.get("id")) == new_id for item in connections
    ):
        QMessageBox.critical(
            editor, "Duplicate ID", f"Network connection {new_id} already exists."
        )
        return
    _safe_push_undo(editor, "Edit network connection")
    _replace_by_id(connections, dialog.result, old_id=connection_id)
    editor.selected_point_name = new_id
    editor.refresh_canvas()


def _delete_network_connection(editor, connection_id: str) -> None:
    if (
        QMessageBox.question(
            editor,
            "Delete network connection",
            f"Delete network connection {connection_id}?",
        )
        != QMessageBox.Yes
    ):
        return
    _safe_push_undo(editor, "Delete network connection")
    editor.store.data["network_connections"] = [
        item
        for item in editor.store.data.get("network_connections", [])
        if _text(item.get("id")) != connection_id
    ]
    editor.store.data["network_patch_leads"] = [
        item for item in editor.store.data.get("network_patch_leads", [])
        if _text(item.get("connection_id")) != connection_id
    ]
    for cable in editor.store.data.get("network_fibre_cables", []):
        cable["logical_connection_ids"] = [
            value for value in cable.get("logical_connection_ids", [])
            if _text(value) != connection_id
        ]
    editor.selected_point_name = None
    editor.refresh_canvas()


def _show_network_location_context_menu(editor, event, name: str) -> None:
    # Opening a menu must not invalidate the main viewer. The selected object
    # is passed directly to every action and is redrawn only after an edit.
    menu = QMenu(editor)
    edit_action = menu.addAction("Edit network location")
    delete_action = menu.addAction("Delete network location")
    action = menu.exec(event.globalPosition().toPoint())
    if action == edit_action:
        _edit_network_location(editor, name)
    elif action == delete_action:
        _delete_network_location(editor, name)


def _show_network_connection_context_menu(editor, event, connection_id: str) -> None:
    menu = QMenu(editor)
    trace_action = menu.addAction("Trace circuit")
    physical_action = menu.addAction("Open in physical fibre map")
    menu.addSeparator()
    edit_action = menu.addAction("Edit network connection")
    delete_action = menu.addAction("Delete network connection")
    action = menu.exec(event.globalPosition().toPoint())
    if action == trace_action:
        trace = circuit_trace(editor.store.data, connection_id)
        editor._network_trace = trace
        if hasattr(editor, "set_status"):
            editor.set_status(
                f"Circuit {connection_id}: {len(trace.get('instance_ids', []))} devices, "
                f"{len(trace.get('patch_lead_ids', []))} patch leads, "
                f"{len(trace.get('fibre_cable_ids', []))} physical fibre cables"
            )
        _open_network_topology(editor)
    elif action == physical_action:
        _open_physical_fibre_topology(editor, connection_id)
    elif action == edit_action:
        _edit_network_connection(editor, connection_id)
    elif action == delete_action:
        _delete_network_connection(editor, connection_id)


def _show_network_context_menu(editor, event, instance_id: str) -> None:
    menu = QMenu(editor)
    edit_action = menu.addAction("Edit installed network asset")
    connect_action = menu.addAction("Start network connection")
    delete_action = menu.addAction("Delete installed network asset")
    action = menu.exec(event.globalPosition().toPoint())
    if action == edit_action:
        _edit_network_instance(editor, instance_id)
    elif action == connect_action:
        editor._network_connection_start = instance_id
        _set_network_mode(editor, "network_connection")
        if hasattr(editor, "set_status"):
            editor.set_status(f"Network connection start: {instance_id}")
    elif action == delete_action:
        _delete_network_instance(editor, instance_id)


def _refresh_network_search(editor) -> None:
    widget = getattr(editor, "search_lists", {}).get("Network Assets")
    if widget is None:
        return
    widget.clear()
    search_text = ""
    if hasattr(editor, "sidebar_search_edit"):
        search_text = editor.sidebar_search_edit.text().strip().lower()
    assets = {
        _text(item.get("id")): item
        for item in editor.store.data.get("network_assets", [])
        if _text(item.get("id"))
    }
    rows = []
    for instance in editor.store.data.get("network_asset_instances", []):
        instance_id = _text(instance.get("id"))
        asset = assets.get(_text(instance.get("asset_id")), {})
        asset_type = _text(asset.get("asset_type")).lower()
        asset_name = _text(asset.get("name")).lower()
        if not _is_active_main_graph_asset(asset, instance):
            continue
        label = (
            f"{instance_id} - {_text(instance.get('name')) or instance_id} | "
            f"{_text(asset.get('asset_type'))} | {_text(instance.get('location_name'))}"
        )
        if search_text and search_text not in label.lower():
            continue
        rows.append((label, instance_id))
    for label, instance_id in sorted(rows, key=lambda row: row[0].lower()):
        item = QListWidgetItem(label)
        item.setData(Qt.UserRole, "Network Assets")
        item.setData(Qt.UserRole + 1, instance_id)
        widget.addItem(item)


def _centre_on_network_instance(editor, instance_id: str) -> None:
    instance = network_instances_by_id(editor.store.data).get(instance_id)
    if not instance:
        return
    floor = int(instance.get("floor", 0))
    if editor.floor_spin.value() != floor:
        editor.floor_spin.setValue(floor)
    editor.selected_point_name = instance_id
    editor.refresh_canvas()
    canvas = getattr(editor, "canvas", None)
    if canvas is not None:
        centre = getattr(canvas, "centerOn", None)
        if callable(centre):
            try:
                centre(
                    editor.world_to_scene(
                        instance.get("x", 0.0), instance.get("y", 0.0)
                    )
                )
            except Exception:
                pass
        elif hasattr(canvas, "fit_to_rect"):
            from PySide6.QtCore import QRectF

            canvas.fit_to_rect(
                QRectF(
                    float(instance.get("x", 0.0)) - 10.0,
                    -float(instance.get("y", 0.0)) - 10.0,
                    20.0,
                    20.0,
                ),
                Qt.KeepAspectRatio,
            )
    if hasattr(editor, "set_status"):
        editor.set_status(f"Centred on network asset {instance_id}")


def install_network_planning(editor_class) -> None:
    """Install the network planning extension onto ``CableRouteEditor``."""

    if getattr(editor_class, "_network_planning_installed", False):
        return

    from models import JsonStore

    install_json_store_extensions(JsonStore)
    _install_location_types()

    original_init = editor_class.__init__
    original_mode_definitions = getattr(editor_class, "_mode_definitions", None)
    original_refresh_canvas = editor_class.refresh_canvas
    original_on_left_click = editor_class.on_left_click
    original_on_double_click = getattr(editor_class, "on_double_click", None)
    original_on_right_click = editor_class.on_right_click
    original_on_drag = getattr(editor_class, "on_drag", None)
    original_on_left_release = getattr(editor_class, "on_left_release", None)
    original_open_json = getattr(editor_class, "open_json", None)
    original_refresh_search = getattr(editor_class, "refresh_rhs_search_sidebar", None)
    original_search_activated = getattr(
        editor_class, "_rhs_search_item_activated", None
    )
    original_visible = getattr(editor_class, "_is_point_kind_visible", None)

    if original_mode_definitions is not None:

        def mode_definitions(self):
            rows = list(original_mode_definitions(self))
            existing = {row[0] for row in rows}
            additions = [
                ("network_select", "Network", "select"),
                ("network_asset", "Net Asset", "data_point"),
                ("network_connection", "Net Link", "edge"),
                ("mer_location", "MER", "location"),
                ("polan_location", "PoLAN", "location"),
                ("telco_pop_location", "Telco PoP", "location"),
                ("fibre_node", "Fibre Node", "location"),
            ]
            rows.extend(row for row in additions if row[0] not in existing)
            return rows

        editor_class._mode_definitions = mode_definitions

    def init_wrapper(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        ensure_network_schema(self.store.data)
        self._network_connection_start = None
        self._network_drag_instance_id = None
        self._network_drag_location_name = None
        self._network_planner_dialog = None
        self._network_topology_dialog = None
        self._physical_fibre_dialog = None
        self._physical_fibre_windows = []
        _augment_network_ui(self)
        self.refresh_canvas()

    editor_class.__init__ = init_wrapper

    def refresh_canvas_wrapper(self, *_signal_args, **_signal_kwargs):
        ensure_network_schema(self.store.data)
        # Qt signals may pass bool/int values, but the original method accepts only self.
        result = original_refresh_canvas(self)

        # ``original_init`` calls ``self.refresh_canvas()`` before the network
        # controls are appended by ``_augment_network_ui``.  Treat the network
        # layer as enabled during that first construction-time refresh.
        layer_control = getattr(self, "show_network_check", None)
        show_network = True
        if layer_control is not None:
            try:
                show_network = bool(layer_control.isChecked())
            except RuntimeError:
                # The Qt object may already be queued for deletion while the
                # application is closing.  Avoid turning shutdown into a crash.
                show_network = True

        canvas = getattr(self, "canvas", None)
        if canvas is not None and hasattr(canvas, "set_visible_layers"):
            try:
                assets_control = getattr(self, "show_network_assets_check", None)
                links_control = getattr(self, "show_network_connections_check", None)
                wireless_control = getattr(self, "show_wireless_devices_check", None)
                show_network_assets = show_network
                show_network_connections = show_network
                show_wireless_devices = show_network
                if assets_control is not None:
                    try:
                        show_network_assets = show_network and bool(
                            assets_control.isChecked()
                        )
                    except RuntimeError:
                        show_network_assets = show_network
                if links_control is not None:
                    try:
                        show_network_connections = show_network and bool(
                            links_control.isChecked()
                        )
                    except RuntimeError:
                        show_network_connections = show_network
                if wireless_control is not None:
                    try:
                        show_wireless_devices = show_network and bool(
                            wireless_control.isChecked()
                        )
                    except RuntimeError:
                        show_wireless_devices = show_network
                fibre_control = getattr(self, "show_physical_fibre_check", None)
                show_physical_fibre = bool(
                    fibre_control.isChecked() if fibre_control is not None else
                    self.store.data.get("network_settings", {}).get("physical_fibre_layer", {}).get("visible", True)
                )
                canvas.set_visible_layers(
                    show_network=show_network,
                    show_network_assets=show_network_assets,
                    show_network_connections=show_network_connections,
                    show_wireless_devices=show_wireless_devices,
                    show_physical_fibre=show_physical_fibre,
                )
            except TypeError:
                # The extension package includes a renderer with this argument;
                # this fallback keeps older custom renderers usable.
                setattr(canvas, "show_network", show_network)
                setattr(canvas, "show_physical_fibre", bool(
                    getattr(self, "show_physical_fibre_check", None).isChecked()
                    if getattr(self, "show_physical_fibre_check", None) is not None else True
                ))
                canvas.update()
        _refresh_network_search(self)
        return result

    editor_class.refresh_canvas = refresh_canvas_wrapper

    def on_left_click_wrapper(self, event, sx, sy):
        mode = self.mode_combo.currentText()
        x, y = self.snap(float(sx), float(sy))
        if mode == "mer_location":
            _add_network_location(self, "mer", x, y)
            return
        if mode == "polan_location":
            _add_network_location(self, "polan", x, y)
            return
        if mode == "telco_pop_location":
            _add_network_location(self, "telco_pop", x, y)
            return
        if mode == "fibre_node":
            _place_fibre_node(self, x, y)
            return
        if mode == "network_select":
            _begin_network_selection(self, x, y)
            return
        if mode == "delete":
            instance_id = _find_network_instance(self, x, y)
            if instance_id:
                _delete_network_instance(self, instance_id)
                return
            location_name = _find_network_location(self, x, y)
            if location_name:
                _delete_network_location(self, location_name)
                return
            connection_id = _find_network_connection(self, x, y)
            if connection_id:
                _delete_network_connection(self, connection_id)
                return
        if mode == "network_asset":
            _place_or_select_network_asset(self, x, y)
            return
        if mode == "network_connection":
            _connect_network_asset(self, x, y)
            return
        return original_on_left_click(self, event, sx, sy)

    editor_class.on_left_click = on_left_click_wrapper

    if original_on_double_click is not None:

        def on_double_click_wrapper(self, event, sx, sy):
            x, y = float(sx), float(sy)
            instance_id = _find_network_instance(self, x, y)
            if instance_id:
                _edit_network_instance(self, instance_id)
                return
            location_name = _find_network_location(self, x, y)
            if location_name and _edit_network_location(self, location_name):
                return
            connection_id = _find_network_connection(self, x, y)
            if connection_id:
                _edit_network_connection(self, connection_id)
                return
            floor = int(self.floor_spin.value())
            nearest = self.find_nearest_selectable_name(x, y, floor)
            if nearest and _edit_network_location(self, nearest):
                return
            return original_on_double_click(self, event, sx, sy)

        editor_class.on_double_click = on_double_click_wrapper

    def on_right_click_wrapper(self, event, sx, sy):
        x = float(sx)
        y = float(sy)
        mode = _text(self.mode_combo.currentText()).lower()
        network_modes = {
            "network_select",
            "network_asset",
            "network_connection",
            "fibre_node",
        }

        # Preserve the host editor's ordinary routing-edge context menu.  The
        # network overlay uses a deliberately generous pick radius, so without
        # this priority check a nearby network item can swallow a right-click
        # intended for Delete Edge.  Network mode remains explicit and keeps
        # network links/assets first.
        if mode not in network_modes and _find_routing_edge(self, x, y):
            return original_on_right_click(self, event, sx, sy)

        instance_id = _find_network_instance(self, x, y)
        if instance_id:
            _show_network_context_menu(self, event, instance_id)
            return
        location_name = _find_network_location(self, x, y)
        if location_name:
            _show_network_location_context_menu(self, event, location_name)
            return
        connection_id = _find_network_connection(self, x, y)
        if connection_id:
            _show_network_connection_context_menu(self, event, connection_id)
            return
        return original_on_right_click(self, event, sx, sy)

    editor_class.on_right_click = on_right_click_wrapper

    if original_on_drag is not None:

        def on_drag_wrapper(self, event, sx, sy):
            mode = self.mode_combo.currentText()
            if mode in {"network_select", "network_asset"} and getattr(
                self, "_network_drag_instance_id", None
            ):
                instance = network_instances_by_id(self.store.data).get(
                    self._network_drag_instance_id
                )
                if instance is not None:
                    x, y = self.snap(float(sx), float(sy))
                    instance["x"] = round(float(x), 3)
                    instance["y"] = round(float(y), 3)
                    instance["floor"] = int(self.floor_spin.value())
                    instance["location_name"] = ""
                    if hasattr(self, "refresh_canvas_geometry_only"):
                        self.refresh_canvas_geometry_only()
                    else:
                        self.refresh_canvas()
                    return
            if mode == "network_select" and getattr(
                self, "_network_drag_location_name", None
            ):
                x, y = self.snap(float(sx), float(sy))
                if _move_network_location(self, self._network_drag_location_name, x, y):
                    if hasattr(self, "refresh_canvas_geometry_only"):
                        self.refresh_canvas_geometry_only()
                    else:
                        self.refresh_canvas()
                    return
            return original_on_drag(self, event, sx, sy)

        editor_class.on_drag = on_drag_wrapper

    if original_on_left_release is not None:

        def on_left_release_wrapper(self, event):
            self._network_drag_instance_id = None
            self._network_drag_location_name = None
            return original_on_left_release(self, event)

        editor_class.on_left_release = on_left_release_wrapper

    if original_open_json is not None:

        def open_json_wrapper(self, *_signal_args, **_signal_kwargs):
            # QAction.triggered(bool) must not be forwarded to the original zero-argument method.
            result = original_open_json(self)
            ensure_network_schema(self.store.data)
            _sync_network_technology_controls(self)
            self.refresh_canvas()
            return result

        editor_class.open_json = open_json_wrapper

    if original_refresh_search is not None:

        def refresh_search_wrapper(self, *_signal_args, **_signal_kwargs):
            # Qt signals such as ``textChanged`` and ``currentChanged`` pass a
            # value/index to their slots.  The application's original refresh
            # method deliberately accepts only ``self``; do not forward those
            # signal arguments into it.
            result = original_refresh_search(self)
            _refresh_network_search(self)
            return result

        editor_class.refresh_rhs_search_sidebar = refresh_search_wrapper

    if original_search_activated is not None:

        def search_activated_wrapper(self, item):
            if item.data(Qt.UserRole) == "Network Assets":
                instance_id = _text(item.data(Qt.UserRole + 1))
                if not instance_id:
                    instance_id = item.text().split(" - ", 1)[0].strip()
                _centre_on_network_instance(self, instance_id)
                return
            return original_search_activated(self, item)

        editor_class._rhs_search_item_activated = search_activated_wrapper

    if original_visible is not None:

        def visible_wrapper(self, point):
            kind = _text((point or {}).get("kind")).lower()
            if kind in {"mer", "polan", "telco_pop", "external_network", "fibre_joint"}:
                return self.show_locations_check.isChecked()
            return original_visible(self, point)

        editor_class._is_point_kind_visible = visible_wrapper

    editor_class.open_network_topology = _open_network_topology
    editor_class.open_physical_fibre_topology = _open_physical_fibre_topology
    editor_class.open_network_planner = _open_network_planner
    editor_class.import_wireless_devices = _import_wireless_devices
    editor_class.export_network_schedules = _export_network_schedules
    editor_class.validate_network_plan = _validate_network
    editor_class._network_planning_installed = True
