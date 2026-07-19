"""Reusable topology and cabinet snapshots for Report Studio PDFs."""

from __future__ import annotations

from copy import deepcopy
import math
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from PySide6.QtCore import QBuffer, QIODevice, QPointF, QRectF, Qt
from PySide6.QtGui import (
    QBrush,
    QColor,
    QFont,
    QFontDatabase,
    QImage,
    QPainter,
    QPainterPath,
    QPen,
    QPolygonF,
)


_ROOM_DRAWING_FONT = "Arial"


def _ensure_room_drawing_font() -> str:
    """Load a normal Windows font when the isolated Qt runtime has no font database."""
    global _ROOM_DRAWING_FONT
    if QFontDatabase.families():
        return _ROOM_DRAWING_FONT
    for candidate in (
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/segoeui.ttf"),
        Path("C:/Windows/Fonts/calibri.ttf"),
    ):
        if not candidate.exists():
            continue
        font_id = QFontDatabase.addApplicationFont(str(candidate))
        families = QFontDatabase.applicationFontFamilies(font_id) if font_id >= 0 else []
        if families:
            _ROOM_DRAWING_FONT = str(families[0])
            break
    return _ROOM_DRAWING_FONT


NETWORK_SNIPPET_VARIABLE_NAMES = (
    "project_name",
    "room_type",
    "location_name",
    "floor",
    "rack_name",
    "cabinet_count",
    "device_count",
    "switch_count",
    "data_port_count",
    "capacity_u",
    "used_u",
    "free_u",
    "utilisation_percent",
    "manufacturers",
    "models",
    "cabinet_width_mm",
    "cabinet_depth_mm",
    "access_clearance_mm",
    "room_width_mm",
    "room_depth_mm",
    "room_area_m2",
    "door_width_mm",
    "door_wall",
    "layout_status",
    "equipment_power_w",
    "poe_load_w",
    "poe_budget_w",
    "total_power_requirement_w",
    "spare_capacity_percent",
    "recommended_power_capacity_w",
    "ups_capacity_w",
    "ups_headroom_w",
    "power_feed_count",
    "power_status",
)

DEFAULT_NETWORK_SNIPPET_TEMPLATES = [
    {
        "id": "builtin-room-floor-layout",
        "name": "Scaled cabinet room floor layout",
        "view_type": "room_layout",
        "title_template": "{room_type} layout - {location_name} - Floor {floor}",
        "callout_templates": [
            "Room: {room_width_mm} x {room_depth_mm} mm | {room_area_m2} m2",
            "{cabinet_count} cabinets | nominal {cabinet_width_mm} x {cabinet_depth_mm} mm",
            "Project access rule: {access_clearance_mm} mm on accessible faces",
            "Door: {door_width_mm} mm clear opening | {layout_status}",
        ],
        "width_pt": 280.0,
        "height_pt": 235.0,
        "scale_denominator": 50,
        "builtin": True,
    },
    {
        "id": "builtin-room-cutaway",
        "name": "Rotatable 3D room cutaway",
        "view_type": "room_cutaway",
        "title_template": "{room_type} 3D proposal - {location_name} - Floor {floor}",
        "callout_templates": [
            "Room: {room_width_mm} x {room_depth_mm} mm | {room_area_m2} m2",
            "{cabinet_count} cabinets | {layout_status}",
        ],
        "width_pt": 300.0,
        "height_pt": 225.0,
        "scale_denominator": 0,
        "builtin": True,
    },
    {
        "id": "builtin-room-topology",
        "name": "Room topology summary",
        "view_type": "topology",
        "title_template": "{room_type} topology - {location_name} - Floor {floor}",
        "callout_templates": [
            "{device_count} installed devices | {switch_count} switches",
            "{data_port_count} modelled data ports | {cabinet_count} cabinets",
        ],
        "width_pt": 250.0,
        "height_pt": 165.0,
        "scale_denominator": 0,
        "builtin": True,
    },
    {
        "id": "builtin-room-power-summary",
        "name": "Comms room power requirements",
        "view_type": "power_summary",
        "title_template": "{room_type} power requirements - {location_name} - Floor {floor}",
        "callout_templates": [
            "Estimated demand: {total_power_requirement_w} W | {recommended_power_capacity_w} W including {spare_capacity_percent}% spare",
            "Equipment: {equipment_power_w} W | PoE endpoints: {poe_load_w} W / {poe_budget_w} W budget",
            "UPS-backed capacity: {ups_capacity_w} W | {power_status}",
        ],
        "width_pt": 300.0,
        "height_pt": 210.0,
        "scale_denominator": 0,
        "builtin": True,
    },
    {
        "id": "builtin-cabinet-elevation",
        "name": "Cabinet elevation and utilisation",
        "view_type": "cabinet",
        "title_template": "{room_type} cabinet - {location_name} / {rack_name}",
        "callout_templates": [
            "Rack utilisation: {used_u}U used / {capacity_u}U ({utilisation_percent}%)",
            "Free space: {free_u}U | {device_count} devices | {switch_count} switches",
            "Data ports: {data_port_count}",
        ],
        "width_pt": 180.0,
        "height_pt": 260.0,
        "scale_denominator": 20,
        "builtin": True,
    },
    {
        "id": "builtin-all-cabinets-elevation",
        "name": "All cabinets elevation and utilisation",
        "view_type": "cabinet_all",
        "title_template": "{room_type} all cabinets - {location_name}",
        "callout_templates": [
            "Rack utilisation: {used_u}U used / {capacity_u}U ({utilisation_percent}%)",
            "Free space: {free_u}U | {device_count} devices | {switch_count} switches",
            "Data ports: {data_port_count}",
        ],
        "width_pt": 300.0,
        "height_pt": 260.0,
        "scale_denominator": 20,
        "builtin": True,
    },
    {
        "id": "builtin-cabinet-compact",
        "name": "Compact cabinet detail",
        "view_type": "cabinet",
        "title_template": "{location_name} - {rack_name}",
        "callout_templates": [
            "{used_u}/{capacity_u}U | {switch_count} switches | {data_port_count} ports",
        ],
        "width_pt": 145.0,
        "height_pt": 210.0,
        "scale_denominator": 25,
        "builtin": True,
    },
]


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _integer(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _number(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _room_type_label(kind: str) -> str:
    value = _text(kind).lower()
    if value == "mer":
        return "MER"
    if value == "distributed_equipment_room":
        return "DER"
    return "Comms room"


def _instance_rack_half_ranges(instance: dict, asset: dict) -> List[Tuple[int, int]]:
    """Return occupied half-U ranges without confusing rack capacity with device size."""
    asset_type = _text(asset.get("asset_type")).lower()
    member_starts = [
        max(1, _integer(value, 1))
        for value in instance.get("stack_member_start_half_us", []) or []
        if _integer(value) > 0
    ]
    if member_starts:
        member_units = max(
            1,
            _integer(
                asset.get("switch_rack_unit_allowance"),
                _integer(asset.get("rack_units"), 1),
            ),
        )
        height = member_units * 2
        return [(start, start + height - 1) for start in member_starts]
    start = max(0, _integer(instance.get("rack_start_half_u")))
    if start <= 0:
        start_u = max(0, _integer(instance.get("rack_start_u")))
        start = (start_u - 1) * 2 + 1 if start_u > 0 else 0
    explicit_half_height = max(
        0,
        _integer(instance.get("rack_height_half_u")),
        _integer(asset.get("rack_height_half_u")),
    )
    if explicit_half_height > 0:
        height = explicit_half_height
    elif asset_type == "network_switch":
        members = (
            max(1, _integer(instance.get("stack_member_count"), 1))
            if bool(instance.get("logical_stack"))
            else 1
        )
        height = max(
            1,
            _integer(
                asset.get("switch_rack_unit_allowance"),
                _integer(asset.get("rack_units"), 1),
            ),
        ) * members * 2
    else:
        height = max(0, _integer(asset.get("rack_units"), 0)) * 2
    return [(start, start + height - 1)] if start > 0 and height > 0 else []


def _occupied_rack_units(instances: Sequence[dict], asset_by_id: Dict[str, dict], capacity_u: int) -> float:
    occupied = set()
    maximum_half_slot = max(0, int(capacity_u or 0) * 2)
    for instance in instances:
        asset = asset_by_id.get(_text(instance.get("asset_id")), {})
        for start, end in _instance_rack_half_ranges(instance, asset):
            for slot in range(max(1, start), end + 1):
                if maximum_half_slot <= 0 or slot <= maximum_half_slot:
                    occupied.add(slot)
    return len(occupied) / 2.0


def _display_rack_units(value: float):
    value = max(0.0, float(value or 0.0))
    return int(round(value)) if abs(value - round(value)) < 0.001 else round(value, 1)


def network_report_snippet_catalog(
    data: dict,
    allowed_locations: Optional[Sequence[Tuple[int, str]]] = None,
) -> List[dict]:
    """Describe topology and per-cabinet snippets available for room locations."""

    allowed = (
        {(int(floor), _text(location)) for floor, location in allowed_locations}
        if allowed_locations is not None
        else None
    )
    rooms: Dict[Tuple[int, str], dict] = {}
    for row in data.get("locations", []) or []:
        if not isinstance(row, dict):
            continue
        kind = _text(row.get("kind")).lower()
        if kind not in {"comms_room", "mer", "distributed_equipment_room"}:
            continue
        name = _text(row.get("name"))
        key = (_integer(row.get("floor")), name)
        if not name or (allowed is not None and key not in allowed):
            continue
        rooms[key] = row

    racks: Dict[Tuple[int, str], set] = {key: set() for key in rooms}
    for row in data.get("network_racks", []) or []:
        if not isinstance(row, dict):
            continue
        location = _text(row.get("location_name"))
        floor = _integer(row.get("floor"))
        key = (floor, location)
        rack = _text(row.get("name") or row.get("id"))
        if key in racks and rack:
            racks[key].add(rack)
    for row in data.get("network_asset_instances", []) or []:
        if not isinstance(row, dict):
            continue
        location = _text(row.get("location_name"))
        floor = _integer(row.get("floor"))
        key = (floor, location)
        rack = _text(row.get("rack_name"))
        if key in racks and rack:
            racks[key].add(rack)

    result = []
    for (floor, location), room in sorted(
        rooms.items(), key=lambda item: (item[0][0], item[0][1].casefold())
    ):
        room_type = _room_type_label(room.get("kind"))
        rack_names = sorted(racks.get((floor, location), set()), key=str.casefold)
        result.append(
            {
                "id": f"room-layout:{floor}:{location}",
                "view_type": "room_layout",
                "floor": floor,
                "location_name": location,
                "rack_name": "",
                "rack_names": rack_names,
                "room_type": room_type,
                "title": f"{room_type} scaled cabinet layout - {location} - Floor {floor}",
            }
        )
        result.append(
            {
                "id": f"room-cutaway:{floor}:{location}",
                "view_type": "room_cutaway",
                "floor": floor,
                "location_name": location,
                "rack_name": "",
                "rack_names": rack_names,
                "room_type": room_type,
                "title": f"{room_type} 3D room cutaway - {location} - Floor {floor}",
            }
        )
        result.append(
            {
                "id": f"topology:{floor}:{location}",
                "view_type": "topology",
                "floor": floor,
                "location_name": location,
                "rack_name": "",
                "room_type": room_type,
                "title": f"{room_type} topology - {location} - Floor {floor}",
            }
        )
        result.append(
            {
                "id": f"power-summary:{floor}:{location}",
                "view_type": "power_summary",
                "floor": floor,
                "location_name": location,
                "rack_name": "",
                "rack_names": rack_names,
                "room_type": room_type,
                "title": f"{room_type} power requirements - {location} - Floor {floor}",
            }
        )
        if rack_names:
            result.append(
                {
                    "id": f"cabinet-all:{floor}:{location}",
                    "view_type": "cabinet_all",
                    "floor": floor,
                    "location_name": location,
                    "rack_name": "",
                    "rack_names": rack_names,
                    "room_type": room_type,
                    "title": f"{room_type} all cabinets - {location} - Floor {floor}",
                }
            )
        for rack_name in rack_names:
            result.append(
                {
                    "id": f"cabinet:{floor}:{location}:{rack_name}",
                    "view_type": "cabinet",
                    "floor": floor,
                    "location_name": location,
                    "rack_name": rack_name,
                    "room_type": room_type,
                    "title": f"{room_type} cabinet - {location} / {rack_name} - Floor {floor}",
                }
            )
    return result


def network_room_power_summary(data: dict, snippet: dict) -> dict:
    """Return rated equipment and assigned PoE demand for one comms room."""

    floor = _integer(snippet.get("floor"))
    location = _text(snippet.get("location_name"))
    assets = {
        _text(row.get("id")): row
        for row in data.get("network_assets", []) or []
        if isinstance(row, dict) and _text(row.get("id"))
    }
    instances = [
        row
        for row in data.get("network_asset_instances", []) or []
        if isinstance(row, dict)
        and _integer(row.get("floor")) == floor
        and _text(row.get("location_name")) == location
    ]
    instance_ids = {_text(row.get("id")) for row in instances}
    poe_by_instance: Dict[str, float] = {}
    for assignment in data.get("network_endpoint_assignments", []) or []:
        if not isinstance(assignment, dict):
            continue
        instance_id = _text(assignment.get("network_instance_id"))
        if instance_id in instance_ids:
            poe_by_instance[instance_id] = poe_by_instance.get(instance_id, 0.0) + max(
                0.0, _number(assignment.get("poe_power_w"))
            )

    rack_rows: Dict[str, dict] = {}
    equipment_power = 0.0
    poe_budget = 0.0
    poe_load = 0.0
    ups_capacity = 0.0
    feed_labels = set()
    for instance in instances:
        instance_id = _text(instance.get("id"))
        asset = assets.get(_text(instance.get("asset_id")), {})
        asset_type = _text(asset.get("asset_type")).lower()
        members = (
            max(1, _integer(instance.get("stack_member_count"), 1))
            if bool(instance.get("logical_stack"))
            else 1
        )
        base_power = max(0.0, _number(asset.get("power_input_w"))) * members
        instance_poe_budget = max(0.0, _number(asset.get("poe_budget_w"))) * members
        instance_poe_load = max(0.0, poe_by_instance.get(instance_id, 0.0))
        equipment_power += base_power
        poe_budget += instance_poe_budget
        poe_load += instance_poe_load
        if asset_type == "ups" or (
            asset_type == "power_device" and bool(asset.get("ups_backed_source", False))
        ):
            ups_capacity += max(
                0.0,
                _number(
                    asset.get(
                        "power_capacity_w",
                        asset.get("rated_power_w", asset.get("output_capacity_w", 0.0)),
                    )
                ),
            ) * members
        for field in ("power_feed", "ups_source"):
            value = _text(instance.get(field))
            if value:
                feed_labels.add(value)
        rack_name = _text(instance.get("rack_name")) or "Unracked"
        rack_row = rack_rows.setdefault(
            rack_name,
            {
                "rack_name": rack_name,
                "device_count": 0,
                "equipment_power_w": 0.0,
                "poe_load_w": 0.0,
                "poe_budget_w": 0.0,
            },
        )
        rack_row["device_count"] += members
        rack_row["equipment_power_w"] += base_power
        rack_row["poe_load_w"] += instance_poe_load
        rack_row["poe_budget_w"] += instance_poe_budget

    instance_by_id = {_text(row.get("id")): row for row in instances}
    for connection in data.get("network_power_connections", []) or []:
        if not isinstance(connection, dict):
            continue
        target_id = _text(connection.get("to_instance_id"))
        if target_id not in instance_by_id:
            continue
        label = _text(connection.get("feed_label")) or _text(connection.get("from_instance_id"))
        if label:
            feed_labels.add(label)

    spare_percent = max(
        0.0,
        _number((data.get("network_settings", {}) or {}).get("spare_capacity_percent"), 15.0),
    )
    total_requirement = equipment_power + poe_load
    recommended_capacity = total_requirement * (1.0 + spare_percent / 100.0)
    ups_headroom = ups_capacity - total_requirement
    if total_requirement <= 0.001:
        status = "No rated power demand is modelled"
    elif ups_capacity <= 0.001:
        status = "UPS-backed capacity is not modelled"
    elif ups_capacity + 0.001 >= recommended_capacity:
        status = "UPS capacity meets the spare-capacity target"
    elif ups_capacity + 0.001 >= total_requirement:
        status = "UPS covers current demand but not the spare target"
    else:
        status = f"UPS capacity shortfall: {total_requirement - ups_capacity:.1f} W"

    rows = []
    for row in sorted(rack_rows.values(), key=lambda value: value["rack_name"].casefold()):
        rows.append(
            {
                **row,
                "total_power_w": row["equipment_power_w"] + row["poe_load_w"],
            }
        )
    return {
        "equipment_power_w": round(equipment_power, 1),
        "poe_load_w": round(poe_load, 1),
        "poe_budget_w": round(poe_budget, 1),
        "total_power_requirement_w": round(total_requirement, 1),
        "spare_capacity_percent": round(spare_percent, 1),
        "recommended_power_capacity_w": round(recommended_capacity, 1),
        "ups_capacity_w": round(ups_capacity, 1),
        "ups_headroom_w": round(ups_headroom, 1),
        "power_feed_count": len(feed_labels),
        "power_status": status,
        "rack_rows": rows,
    }


def network_report_snippet_variables(data: dict, snippet: dict) -> dict:
    """Resolve configurable template variables from the current network model."""

    floor = _integer(snippet.get("floor"))
    location = _text(snippet.get("location_name"))
    rack_name = _text(snippet.get("rack_name"))
    asset_by_id = {
        _text(row.get("id")): row
        for row in data.get("network_assets", []) or []
        if isinstance(row, dict) and _text(row.get("id"))
    }
    location_instances = [
        row
        for row in data.get("network_asset_instances", []) or []
        if isinstance(row, dict)
        and _integer(row.get("floor")) == floor
        and _text(row.get("location_name")) == location
    ]
    instances = (
        [row for row in location_instances if _text(row.get("rack_name")) == rack_name]
        if rack_name
        else location_instances
    )
    racks = {
        _text(row.get("name") or row.get("id")): row
        for row in data.get("network_racks", []) or []
        if isinstance(row, dict)
        and _integer(row.get("floor")) == floor
        and _text(row.get("location_name")) == location
        and _text(row.get("name") or row.get("id"))
    }
    if not racks:
        racks = {
            _text(row.get("rack_name")): {}
            for row in location_instances
            if _text(row.get("rack_name"))
        }
    rack = racks.get(rack_name, {}) if rack_name else {}
    capacity_u = max(0, _integer(rack.get("capacity_u"), 0))
    if rack_name and capacity_u <= 0:
        capacity_u = 42
    used_u = 0.0
    switch_count = 0
    data_port_count = 0
    manufacturers = set()
    models = set()
    for instance in instances:
        asset = asset_by_id.get(_text(instance.get("asset_id")), {})
        if _text(asset.get("asset_type")).lower() == "network_switch":
            switch_count += max(1, _integer(instance.get("stack_member_count"), 1))
        data_port_count += max(
            0,
            _integer(asset.get("connections_out"), _integer(asset.get("number_of_ports"), 0)),
        )
        manufacturer = _text(asset.get("manufacturer"))
        model = _text(asset.get("model"))
        if manufacturer:
            manufacturers.add(manufacturer)
        if model:
            models.add(model)
    if not rack_name:
        capacity_u = sum(max(0, _integer(row.get("capacity_u"), 42)) for row in racks.values())
        for name, rack_row in racks.items():
            rack_capacity = max(1, _integer(rack_row.get("capacity_u"), 42))
            used_u += _occupied_rack_units(
                [row for row in location_instances if _text(row.get("rack_name")) == name],
                asset_by_id,
                rack_capacity,
            )
    else:
        used_u = _occupied_rack_units(instances, asset_by_id, capacity_u)
    free_u = max(0.0, capacity_u - used_u)
    utilisation = int(round(100.0 * used_u / capacity_u)) if capacity_u else 0
    layout = snippet.get("room_layout") if isinstance(snippet.get("room_layout"), dict) else {}
    room_width_mm = float(layout.get("room_width_mm", 0.0) or 0.0)
    room_depth_mm = float(layout.get("room_depth_mm", 0.0) or 0.0)
    warnings = room_layout_compliance(layout) if layout else []
    power = network_room_power_summary(data, snippet)
    return {
        "project_name": _text(data.get("project_name") or data.get("name")),
        "room_type": _text(snippet.get("room_type")) or "Comms room",
        "location_name": location,
        "floor": floor,
        "rack_name": rack_name or "All cabinets",
        "cabinet_count": 1 if rack_name and rack_name in racks else len(racks),
        "device_count": len(instances),
        "switch_count": switch_count,
        "data_port_count": data_port_count,
        "capacity_u": capacity_u,
        "used_u": _display_rack_units(used_u),
        "free_u": _display_rack_units(free_u),
        "utilisation_percent": utilisation,
        "manufacturers": ", ".join(sorted(manufacturers, key=str.casefold)) or "Not specified",
        "models": ", ".join(sorted(models, key=str.casefold)) or "Not specified",
        "cabinet_width_mm": int(round(float(layout.get("cabinet_width_mm", 600.0) or 600.0))),
        "cabinet_depth_mm": int(round(float(layout.get("cabinet_depth_mm", 1070.0) or 1070.0))),
        "access_clearance_mm": int(round(float(layout.get("access_clearance_mm", 1200.0) or 1200.0))),
        "room_width_mm": int(round(room_width_mm)),
        "room_depth_mm": int(round(room_depth_mm)),
        "room_area_m2": f"{room_width_mm * room_depth_mm / 1_000_000.0:.2f}",
        "door_width_mm": int(round(float(layout.get("door_width_mm", 900.0) or 900.0))),
        "door_wall": _text(layout.get("door_wall") or "south").title(),
        "layout_status": "NON-CONFORMANT - " + "; ".join(warnings[:3]) if warnings else "Conforms to configured clearances",
        **{
            key: power[key]
            for key in (
                "equipment_power_w",
                "poe_load_w",
                "poe_budget_w",
                "total_power_requirement_w",
                "spare_capacity_percent",
                "recommended_power_capacity_w",
                "ups_capacity_w",
                "ups_headroom_w",
                "power_feed_count",
                "power_status",
            )
        },
    }


def suggested_network_room_layout(data: dict, snippet: dict) -> dict:
    """Create a conservative single-row cabinet room proposal in millimetres."""

    names = [_text(value) for value in snippet.get("rack_names", []) or [] if _text(value)]
    if not names:
        floor = _integer(snippet.get("floor"))
        location = _text(snippet.get("location_name"))
        names = sorted(
            {
                _text(row.get("name") or row.get("id"))
                for row in data.get("network_racks", []) or []
                if isinstance(row, dict)
                and _integer(row.get("floor")) == floor
                and _text(row.get("location_name")) == location
                and _text(row.get("name") or row.get("id"))
            },
            key=str.casefold,
        )
    if not names:
        names = ["Cabinet 1"]
    cabinet_width = 600.0
    cabinet_depth = 1070.0
    clearance = 1200.0
    room_width = clearance * 2.0 + cabinet_width * len(names)
    room_depth = clearance * 2.0 + cabinet_depth
    asset_by_id = {
        _text(row.get("id")): row
        for row in data.get("network_assets", []) or []
        if isinstance(row, dict) and _text(row.get("id"))
    }
    rack_instances = {
        name: [
            row for row in data.get("network_asset_instances", []) or []
            if isinstance(row, dict)
            and _integer(row.get("floor")) == _integer(snippet.get("floor"))
            and _text(row.get("location_name")) == _text(snippet.get("location_name"))
            and _text(row.get("rack_name")) == name
        ]
        for name in names
    }
    cabinet_rows = []
    for index, name in enumerate(names):
        installed_assets = [
            asset_by_id.get(_text(instance.get("asset_id")), {})
            for instance in rack_instances.get(name, [])
        ]
        measured_widths = [float(asset.get("physical_width_mm", 0.0) or 0.0) for asset in installed_assets if float(asset.get("physical_width_mm", 0.0) or 0.0) > 0.0]
        measured_depths = [float(asset.get("physical_depth_mm", 0.0) or 0.0) for asset in installed_assets if float(asset.get("physical_depth_mm", 0.0) or 0.0) > 0.0]
        measured_height = sum(
            float(asset.get("physical_height_mm", 0.0) or 0.0)
            * max(1, _integer(instance.get("stack_member_count"), 1))
            for instance, asset in zip(rack_instances.get(name, []), installed_assets)
            if float(asset.get("physical_height_mm", 0.0) or 0.0) > 0.0
        )
        derived_width = max(cabinet_width, (max(measured_widths) + 100.0) if measured_widths else cabinet_width)
        derived_depth = max(cabinet_depth, (max(measured_depths) + 200.0) if measured_depths else cabinet_depth)
        cabinet_rows.append({
            "id": f"cabinet-{index + 1}",
            "name": name,
            "width_mm": derived_width,
            "depth_mm": derived_depth,
            "height_mm": max(2000.0, measured_height + 180.0),
            "dimensioned_asset_count": len({id(asset) for asset in installed_assets if asset and (float(asset.get("physical_width_mm", 0.0) or 0.0) > 0.0 or float(asset.get("physical_depth_mm", 0.0) or 0.0) > 0.0)}),
        })
    cabinet_width = max(row["width_mm"] for row in cabinet_rows)
    cabinet_depth = max(row["depth_mm"] for row in cabinet_rows)
    layout = {
        "version": 1,
        "design_variant": "single_row",
        "room_width_mm": room_width,
        "room_depth_mm": room_depth,
        "cabinet_width_mm": cabinet_width,
        "cabinet_depth_mm": cabinet_depth,
        "cabinet_door_swing_mm": cabinet_width,
        "person_width_mm": 600.0,
        "person_depth_mm": 400.0,
        "access_clearance_mm": clearance,
        "door_width_mm": 900.0,
        "door_wall": "south",
        "door_offset_mm": 150.0,
        "view_angle_deg": 35.0,
        "cabinets": cabinet_rows,
    }
    return apply_network_room_layout_variant(layout, "single_row")


NETWORK_ROOM_LAYOUT_VARIANTS = (
    ("single_row", "Joined row - front and rear access"),
    ("all_faces", "Separated cabinets - access on every face"),
    ("two_rows", "Two rows - shared central aisle"),
    ("front_only", "Joined row - front service access only"),
)


def apply_network_room_layout_variant(layout: dict, variant: str) -> dict:
    """Apply a repeatable cabinet arrangement and explicit service faces."""
    result = deepcopy(layout if isinstance(layout, dict) else {})
    cabinets = [row for row in result.get("cabinets", []) or [] if isinstance(row, dict)]
    if not cabinets:
        return result
    variant = _text(variant).lower()
    valid = {value for value, _label in NETWORK_ROOM_LAYOUT_VARIANTS}
    if variant not in valid:
        variant = "single_row"
    width = max(1.0, float(result.get("cabinet_width_mm", 600.0) or 600.0))
    depth = max(1.0, float(result.get("cabinet_depth_mm", 1070.0) or 1070.0))
    clearance = max(0.0, float(result.get("access_clearance_mm", 1200.0) or 1200.0))
    count = len(cabinets)
    sizes = [
        (
            max(1.0, float(row.get("width_mm", width) or width)),
            max(1.0, float(row.get("depth_mm", depth) or depth)),
        )
        for row in cabinets
    ]
    if variant == "all_faces":
        result["room_width_mm"] = clearance * (count + 1) + sum(item[0] for item in sizes)
        result["room_depth_mm"] = clearance * 2.0 + max(item[1] for item in sizes)
        cursor_x = clearance
        for index, row in enumerate(cabinets):
            row.update(x_mm=cursor_x, y_mm=clearance)
            cursor_x += sizes[index][0] + clearance
            row["rotation_deg"] = 0
            row["accessible_faces"] = ["front", "rear", "left", "right"]
    elif variant == "two_rows":
        columns = max(1, int(math.ceil(count / 2.0)))
        column_widths = [max((sizes[index][0] for index in range(column, count, columns)), default=width) for column in range(columns)]
        row_depths = [max((sizes[index][1] for index in range(row_index * columns, min(count, (row_index + 1) * columns))), default=depth) for row_index in range(2)]
        result["room_width_mm"] = clearance * 2.0 + sum(column_widths)
        result["room_depth_mm"] = clearance * 3.0 + sum(row_depths)
        column_x = [clearance]
        for column_width in column_widths[:-1]: column_x.append(column_x[-1] + column_width)
        for index, row in enumerate(cabinets):
            row_index, column = divmod(index, columns)
            row.update(
                x_mm=column_x[column],
                y_mm=clearance + (row_depths[0] + clearance if row_index else 0.0),
            )
            row["rotation_deg"] = 0
            row["accessible_faces"] = ["front", "rear"]
    elif variant == "front_only":
        result["room_width_mm"] = clearance * 2.0 + sum(item[0] for item in sizes)
        result["room_depth_mm"] = clearance + max(item[1] for item in sizes)
        cursor_x = clearance
        for index, row in enumerate(cabinets):
            row.update(x_mm=cursor_x, y_mm=clearance)
            cursor_x += sizes[index][0]
            row["rotation_deg"] = 0
            row["accessible_faces"] = ["front"]
    else:
        result["room_width_mm"] = clearance * 2.0 + sum(item[0] for item in sizes)
        result["room_depth_mm"] = clearance * 2.0 + max(item[1] for item in sizes)
        cursor_x = clearance
        for index, row in enumerate(cabinets):
            faces = ["front", "rear"]
            if index == 0:
                faces.append("left")
            if index == count - 1:
                faces.append("right")
            row.update(x_mm=cursor_x, y_mm=clearance)
            cursor_x += sizes[index][0]
            row["rotation_deg"] = 0
            row["accessible_faces"] = faces
    result["cabinets"] = cabinets
    result["design_variant"] = variant
    return result


def _layout_rects(layout: dict) -> List[dict]:
    width = max(1.0, float(layout.get("cabinet_width_mm", 600.0) or 600.0))
    depth = max(1.0, float(layout.get("cabinet_depth_mm", 1070.0) or 1070.0))
    rects = []
    face_maps = {
        0: {"front": "front", "rear": "rear", "left": "left", "right": "right"},
        90: {"front": "left", "rear": "right", "left": "rear", "right": "front"},
        180: {"front": "rear", "rear": "front", "left": "right", "right": "left"},
        270: {"front": "right", "rear": "left", "left": "front", "right": "rear"},
    }
    for row in layout.get("cabinets", []) or []:
        if not isinstance(row, dict):
            continue
        rotation = int(round(float(row.get("rotation_deg", 0) or 0) / 90.0) * 90) % 360
        rects.append({
            **row,
            "x": float(row.get("x_mm", 0.0) or 0.0),
            "y": float(row.get("y_mm", 0.0) or 0.0),
            "w": max(1.0, float(row.get("depth_mm", depth) or depth)) if rotation in {90, 270} else max(1.0, float(row.get("width_mm", width) or width)),
            "h": max(1.0, float(row.get("width_mm", width) or width)) if rotation in {90, 270} else max(1.0, float(row.get("depth_mm", depth) or depth)),
            "rotation_deg": rotation,
            "face_edges": face_maps[rotation],
        })
    return rects


def auto_arrange_network_room_cabinets(layout: dict) -> dict:
    """Repack rotated, variable-sized cabinets inside the current room envelope."""
    result = deepcopy(layout if isinstance(layout, dict) else {})
    cabinets = [row for row in result.get("cabinets", []) or [] if isinstance(row, dict)]
    if not cabinets:
        return result
    room_w = max(0.0, float(result.get("room_width_mm", 0.0) or 0.0))
    clearance = max(0.0, float(result.get("access_clearance_mm", 1200.0) or 1200.0))
    default_w = max(1.0, float(result.get("cabinet_width_mm", 600.0) or 600.0))
    default_d = max(1.0, float(result.get("cabinet_depth_mm", 1070.0) or 1070.0))
    face_maps = {
        0: {"front": "front", "rear": "rear", "left": "left", "right": "right"},
        90: {"front": "left", "rear": "right", "left": "rear", "right": "front"},
        180: {"front": "rear", "rear": "front", "left": "right", "right": "left"},
        270: {"front": "right", "rear": "left", "left": "front", "right": "rear"},
    }

    def geometry(row):
        rotation = int(row.get("rotation_deg", 0) or 0) % 360
        width = max(1.0, float(row.get("width_mm", default_w) or default_w))
        depth = max(1.0, float(row.get("depth_mm", default_d) or default_d))
        if rotation in {90, 270}:
            width, depth = depth, width
        local_faces = {_text(face).lower() for face in row.get("accessible_faces", []) or []}
        global_faces = {face_maps.get(rotation, face_maps[0]).get(face, face) for face in local_faces}
        return width, depth, global_faces

    x = clearance
    y = clearance
    row_depth = 0.0
    previous_faces = set()
    for index, row in enumerate(cabinets):
        width, depth, faces = geometry(row)
        gap = clearance if index and ("right" in previous_faces or "left" in faces) else 0.0
        proposed_x = x + gap
        if index and proposed_x + width + clearance > room_w and x > clearance:
            x = clearance
            y += row_depth + clearance
            row_depth = 0.0
            previous_faces = set()
            proposed_x = x
        row["x_mm"] = proposed_x
        row["y_mm"] = y
        x = proposed_x + width
        row_depth = max(row_depth, depth)
        previous_faces = faces
    result["cabinets"] = cabinets
    result["design_variant"] = "custom"
    return result


def room_layout_compliance(layout: dict) -> List[str]:
    """Return concise warning messages without preventing a custom arrangement."""

    if not isinstance(layout, dict):
        return ["Layout data is unavailable"]
    room_w = max(0.0, float(layout.get("room_width_mm", 0.0) or 0.0))
    room_d = max(0.0, float(layout.get("room_depth_mm", 0.0) or 0.0))
    clearance = max(0.0, float(layout.get("access_clearance_mm", 1200.0) or 1200.0))
    rects = _layout_rects(layout)
    cabinet_door_swing = max(
        0.0,
        float(layout.get("cabinet_door_swing_mm", layout.get("cabinet_width_mm", 600.0)) or 0.0),
    )
    warnings: List[str] = []
    tolerance = 5.0

    def overlaps(a0, a1, b0, b1):
        return min(a1, b1) - max(a0, b0) > tolerance

    def cabinet_door_rect(rect):
        edge = rect.get("face_edges", {}).get("front", "front")
        if edge == "front":
            return (rect["x"], rect["y"] - cabinet_door_swing, rect["w"], cabinet_door_swing)
        if edge == "rear":
            return (rect["x"], rect["y"] + rect["h"], rect["w"], cabinet_door_swing)
        if edge == "left":
            return (rect["x"] - cabinet_door_swing, rect["y"], cabinet_door_swing, rect["h"])
        return (rect["x"] + rect["w"], rect["y"], cabinet_door_swing, rect["h"])

    for index, rect in enumerate(rects):
        name = _text(rect.get("name")) or f"Cabinet {index + 1}"
        if rect["x"] < 0 or rect["y"] < 0 or rect["x"] + rect["w"] > room_w or rect["y"] + rect["h"] > room_d:
            warnings.append(f"{name} is outside the room")
        for other in rects[index + 1 :]:
            if overlaps(rect["x"], rect["x"] + rect["w"], other["x"], other["x"] + other["w"]) and overlaps(rect["y"], rect["y"] + rect["h"], other["y"], other["y"] + other["h"]):
                warnings.append(f"{name} overlaps {_text(other.get('name')) or 'another cabinet'}")

        joined_west = any(
            abs((other["x"] + other["w"]) - rect["x"]) <= tolerance
            and abs(other["y"] - rect["y"]) <= tolerance
            and abs(other["h"] - rect["h"]) <= tolerance
            for other in rects if other is not rect
        )
        joined_east = any(
            abs((rect["x"] + rect["w"]) - other["x"]) <= tolerance
            and abs(other["y"] - rect["y"]) <= tolerance
            and abs(other["h"] - rect["h"]) <= tolerance
            for other in rects if other is not rect
        )
        distances = {
            "front": rect["y"],
            "rear": room_d - (rect["y"] + rect["h"]),
            "left": rect["x"],
            "right": room_w - (rect["x"] + rect["w"]),
        }
        for other in rects:
            if other is rect:
                continue
            if overlaps(rect["x"], rect["x"] + rect["w"], other["x"], other["x"] + other["w"]):
                if other["y"] + other["h"] <= rect["y"] + tolerance:
                    distances["front"] = min(distances["front"], rect["y"] - (other["y"] + other["h"]))
                if other["y"] >= rect["y"] + rect["h"] - tolerance:
                    distances["rear"] = min(distances["rear"], other["y"] - (rect["y"] + rect["h"]))
            if overlaps(rect["y"], rect["y"] + rect["h"], other["y"], other["y"] + other["h"]):
                if other["x"] + other["w"] <= rect["x"] + tolerance:
                    distances["left"] = min(distances["left"], rect["x"] - (other["x"] + other["w"]))
                if other["x"] >= rect["x"] + rect["w"] - tolerance:
                    distances["right"] = min(distances["right"], other["x"] - (rect["x"] + rect["w"]))
        configured_faces = rect.get("accessible_faces")
        if isinstance(configured_faces, (list, tuple, set)):
            selected_faces = {_text(face).lower() for face in configured_faces}
            required_edges = {
                local_face: rect.get("face_edges", {}).get(local_face, local_face)
                for local_face in selected_faces
                if local_face in {"front", "rear", "left", "right"}
            }
            if not selected_faces:
                warnings.append(f"{name} has no configured accessible face")
            short = [
                local_face
                for local_face, edge in required_edges.items()
                if distances[edge] + tolerance < clearance
            ]
        else:
            accessible = {"front": True, "rear": True, "left": not joined_west, "right": not joined_east}
            required_edges = {face: face for face, required in accessible.items() if required}
            short = [face for face, required in accessible.items() if required and distances[face] + tolerance < clearance]
        if short:
            minimum = min(distances[required_edges[face]] for face in short)
            warnings.append(f"{name}: {', '.join(short)} clearance is {max(0, int(round(minimum)))} mm")

        if bool(rect.get("front_door_enabled", True)) and cabinet_door_swing > 0.0:
            door_rect = cabinet_door_rect(rect)
            if (
                door_rect[0] < -tolerance
                or door_rect[1] < -tolerance
                or door_rect[0] + door_rect[2] > room_w + tolerance
                or door_rect[1] + door_rect[3] > room_d + tolerance
            ):
                warnings.append(f"{name}: front door swing extends outside the room")
            for other in rects:
                if other is rect:
                    continue
                if overlaps(door_rect[0], door_rect[0] + door_rect[2], other["x"], other["x"] + other["w"]) and overlaps(door_rect[1], door_rect[1] + door_rect[3], other["y"], other["y"] + other["h"]):
                    warnings.append(f"{name}: front door swing conflicts with {_text(other.get('name')) or 'another cabinet'}")
                    break

    door_width = max(0.0, float(layout.get("door_width_mm", 900.0) or 900.0))
    door_offset = max(0.0, float(layout.get("door_offset_mm", 0.0) or 0.0))
    wall = _text(layout.get("door_wall") or "south").lower()
    wall_length = room_w if wall in {"north", "south"} else room_d
    if door_offset + door_width > wall_length + tolerance:
        warnings.append("Door opening extends beyond the selected wall")
    if wall == "south": swing = (door_offset, 0.0, door_width, door_width)
    elif wall == "north": swing = (door_offset, room_d - door_width, door_width, door_width)
    elif wall == "west": swing = (0.0, door_offset, door_width, door_width)
    else:
        swing = (room_w - door_width, door_offset, door_width, door_width)
    for rect in rects:
        if overlaps(swing[0], swing[0] + swing[2], rect["x"], rect["x"] + rect["w"]) and overlaps(swing[1], swing[1] + swing[3], rect["y"], rect["y"] + rect["h"]):
            warnings.append(f"Door swing conflicts with {_text(rect.get('name')) or 'a cabinet'}")
            break
    for rect in rects:
        if not bool(rect.get("front_door_enabled", True)) or cabinet_door_swing <= 0.0:
            continue
        cabinet_swing = cabinet_door_rect(rect)
        if overlaps(swing[0], swing[0] + swing[2], cabinet_swing[0], cabinet_swing[0] + cabinet_swing[2]) and overlaps(swing[1], swing[1] + swing[3], cabinet_swing[1], cabinet_swing[1] + cabinet_swing[3]):
            warnings.append(f"Room door swing conflicts with {_text(rect.get('name')) or 'a cabinet'} front door swing")
    return list(dict.fromkeys(warnings))


def _encode_png(image: QImage) -> bytes:
    buffer = QBuffer()
    if not buffer.open(QIODevice.WriteOnly) or not image.save(buffer, "PNG"):
        raise RuntimeError("Could not encode the room proposal image.")
    return bytes(buffer.data())


def _draw_top_down_person(
    painter: QPainter,
    x: float,
    y: float,
    footprint_width: float,
    footprint_depth: float,
) -> None:
    """Draw a line figure inside an exact plan-view physical envelope."""
    scale_x = max(1.0, float(footprint_width)) / 0.69
    scale_y = max(1.0, float(footprint_depth)) / 0.80
    line_width = max(
        1.0,
        min(2.2, min(float(footprint_width), float(footprint_depth)) * 0.012),
    )
    painter.setPen(QPen(QColor("#2563eb"), line_width, Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
    painter.setBrush(Qt.NoBrush)
    head_y = y - scale_y * 0.16
    painter.drawEllipse(QPointF(x, head_y), scale_x * 0.12, scale_y * 0.12)
    body = QPainterPath()
    body.moveTo(x - scale_x * 0.07, y - scale_y * 0.03)
    body.cubicTo(
        x - scale_x * 0.24,
        y - scale_y * 0.03,
        x - scale_x * 0.27,
        y + scale_y * 0.10,
        x - scale_x * 0.17,
        y + scale_y * 0.20,
    )
    body.lineTo(x - scale_x * 0.09, y + scale_y * 0.36)
    body.lineTo(x + scale_x * 0.09, y + scale_y * 0.36)
    body.lineTo(x + scale_x * 0.17, y + scale_y * 0.20)
    body.cubicTo(
        x + scale_x * 0.27,
        y + scale_y * 0.10,
        x + scale_x * 0.24,
        y - scale_y * 0.03,
        x + scale_x * 0.07,
        y - scale_y * 0.03,
    )
    painter.drawPath(body)
    painter.drawEllipse(QPointF(x - scale_x * 0.30, y + scale_y * 0.17), scale_x * 0.045, scale_y * 0.06)
    painter.drawEllipse(QPointF(x + scale_x * 0.30, y + scale_y * 0.17), scale_x * 0.045, scale_y * 0.06)
    painter.drawEllipse(QPointF(x - scale_x * 0.08, y + scale_y * 0.43), scale_x * 0.055, scale_y * 0.09)
    painter.drawEllipse(QPointF(x + scale_x * 0.08, y + scale_y * 0.43), scale_x * 0.055, scale_y * 0.09)


def _draw_plan_door(
    painter: QPainter,
    point,
    wall: str,
    offset: float,
    door_width: float,
    room_w: float,
    room_d: float,
    sx: float,
    sy: float,
) -> None:
    """Draw a conventional inward-opening leaf and quarter-swing in plan view."""
    wall = _text(wall).lower()
    if wall == "south":
        hinge = point(offset, 0.0)
        opened = point(offset, door_width)
        jamb = point(offset + door_width, 0.0)
        arc_rect = QRectF(hinge.x() - door_width * sx, hinge.y() - door_width * sy, door_width * sx * 2.0, door_width * sy * 2.0)
        start_angle, span_angle = 90.0, -90.0
    elif wall == "north":
        hinge = point(offset, room_d)
        opened = point(offset, room_d - door_width)
        jamb = point(offset + door_width, room_d)
        arc_rect = QRectF(hinge.x() - door_width * sx, hinge.y() - door_width * sy, door_width * sx * 2.0, door_width * sy * 2.0)
        start_angle, span_angle = -90.0, 90.0
    elif wall == "west":
        hinge = point(0.0, offset)
        opened = point(door_width, offset)
        jamb = point(0.0, offset + door_width)
        arc_rect = QRectF(hinge.x() - door_width * sx, hinge.y() - door_width * sy, door_width * sx * 2.0, door_width * sy * 2.0)
        start_angle, span_angle = 0.0, 90.0
    else:
        hinge = point(room_w, offset)
        opened = point(room_w - door_width, offset)
        jamb = point(room_w, offset + door_width)
        arc_rect = QRectF(hinge.x() - door_width * sx, hinge.y() - door_width * sy, door_width * sx * 2.0, door_width * sy * 2.0)
        start_angle, span_angle = 180.0, -90.0
    painter.drawLine(hinge, opened)
    arc = QPainterPath()
    arc.arcMoveTo(arc_rect, start_angle)
    arc.arcTo(arc_rect, start_angle, span_angle)
    painter.drawPath(arc)
    painter.drawLine(jamb, jamb)


def render_network_room_layout_png(layout: dict, view_type: str = "room_layout", pixel_width: int = 1400) -> bytes:
    """Render a dimensioned plan or rotatable cutaway from shared millimetre coordinates."""

    font_family = _ensure_room_drawing_font()
    if _text(view_type).lower() == "room_cutaway":
        return _render_network_room_cutaway_png(layout, pixel_width=pixel_width)
    room_w = max(1000.0, float(layout.get("room_width_mm", 1000.0) or 1000.0))
    room_d = max(1000.0, float(layout.get("room_depth_mm", 1000.0) or 1000.0))
    width = max(720, int(pixel_width))
    height = max(520, int(round(width * room_d / room_w)))
    image = QImage(width, height, QImage.Format_ARGB32_Premultiplied)
    image.fill(QColor("#ffffff"))
    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing, True)
    sx, sy = width / room_w, height / room_d

    def point(x, y):
        return QPointF(x * sx, height - y * sy)

    clearance = max(0.0, float(layout.get("access_clearance_mm", 1200.0) or 1200.0))
    rects = _layout_rects(layout)
    painter.setPen(Qt.NoPen)
    painter.setBrush(QBrush(QColor("#dcfce7")))
    for rect in rects:
        local_faces = {_text(face).lower() for face in rect.get("accessible_faces", ["front", "rear", "left", "right"])}
        faces = {rect.get("face_edges", {}).get(face, face) for face in local_faces}
        if "front" in faces:
            painter.drawRect(QRectF(rect["x"] * sx, height - rect["y"] * sy, rect["w"] * sx, clearance * sy))
        if "rear" in faces:
            painter.drawRect(QRectF(rect["x"] * sx, height - (rect["y"] + rect["h"] + clearance) * sy, rect["w"] * sx, clearance * sy))
        if "left" in faces:
            painter.drawRect(QRectF((rect["x"] - clearance) * sx, height - (rect["y"] + rect["h"]) * sy, clearance * sx, rect["h"] * sy))
        if "right" in faces:
            painter.drawRect(QRectF((rect["x"] + rect["w"]) * sx, height - (rect["y"] + rect["h"]) * sy, clearance * sx, rect["h"] * sy))
    painter.setPen(QPen(QColor("#111827"), max(5.0, width * 0.006)))
    painter.setBrush(Qt.NoBrush)
    painter.drawRect(QRectF(1.0, 1.0, width - 2.0, height - 2.0))

    painter.setFont(QFont(font_family, max(10, int(width * 0.013)), QFont.Bold))
    for index, rect in enumerate(rects):
        painter.setFont(QFont(font_family, max(10, int(width * 0.013)), QFont.Bold))
        box = QRectF(rect["x"] * sx, height - (rect["y"] + rect["h"]) * sy, rect["w"] * sx, rect["h"] * sy)
        painter.setPen(QPen(QColor("#0f172a"), max(2.0, width * 0.002)))
        painter.setBrush(QBrush(QColor("#64748b")))
        painter.drawRect(box)
        painter.setPen(QPen(QColor("#ffffff"), 1.0))
        painter.drawText(box, Qt.AlignCenter | Qt.TextWordWrap, _text(rect.get("name")) or f"Cabinet {index + 1}")
        painter.setFont(QFont(font_family, max(8, int(width * 0.009))))
        painter.drawText(box.adjusted(3.0, 3.0, -3.0, -3.0), Qt.AlignBottom | Qt.AlignHCenter, f"{int(round(rect['w']))} x {int(round(rect['h']))} mm")
        painter.setFont(QFont(font_family, max(10, int(width * 0.013)), QFont.Bold))
        face_lines = {
            "front": (box.bottomLeft(), box.bottomRight()),
            "rear": (box.topLeft(), box.topRight()),
            "left": (box.topLeft(), box.bottomLeft()),
            "right": (box.topRight(), box.bottomRight()),
        }
        painter.setPen(QPen(QColor("#fbbf24"), max(3.0, width * 0.003)))
        for local_face in rect.get("accessible_faces", ["front"]):
            face = rect.get("face_edges", {}).get(_text(local_face).lower(), _text(local_face).lower())
            if face in face_lines:
                painter.drawLine(*face_lines[face])

        if bool(rect.get("front_door_enabled", True)):
            front_edge = rect.get("face_edges", {}).get("front", "front")
            swing_mm = max(0.0, float(layout.get("cabinet_door_swing_mm", layout.get("cabinet_width_mm", 600.0)) or 0.0))
            if front_edge in {"front", "rear"}:
                radius = swing_mm * sx
            else:
                radius = swing_mm * sy
            if front_edge == "front":
                hinge = box.bottomLeft(); closed = QPointF(hinge.x() + radius, hinge.y()); opened = QPointF(hinge.x(), hinge.y() + radius)
            elif front_edge == "rear":
                hinge = box.topRight(); closed = QPointF(hinge.x() - radius, hinge.y()); opened = QPointF(hinge.x(), hinge.y() - radius)
            elif front_edge == "left":
                hinge = box.bottomLeft(); closed = QPointF(hinge.x(), hinge.y() - radius); opened = QPointF(hinge.x() - radius, hinge.y())
            else:
                hinge = box.topRight(); closed = QPointF(hinge.x(), hinge.y() + radius); opened = QPointF(hinge.x() + radius, hinge.y())
            control = QPointF(closed.x() + opened.x() - hinge.x(), closed.y() + opened.y() - hinge.y())
            door_path = QPainterPath(closed); door_path.quadTo(control, opened)
            painter.setPen(QPen(QColor("#2563eb"), max(1.5, width * 0.0018)))
            painter.drawPath(door_path)
            painter.drawLine(hinge, opened)

        # Placement dimensions are referenced from the west and south walls.
        dimension_pen = QPen(QColor("#7c3aed"), max(1.5, width * 0.0014), Qt.DashLine)
        painter.setPen(dimension_pen)
        centre_y = height - (rect["y"] + rect["h"] / 2.0) * sy
        centre_x = (rect["x"] + rect["w"] / 2.0) * sx
        painter.drawLine(QPointF(0.0, centre_y), QPointF(rect["x"] * sx, centre_y))
        painter.drawLine(QPointF(centre_x, height), QPointF(centre_x, height - rect["y"] * sy))
        painter.setFont(QFont(font_family, max(7, int(width * 0.008))))
        painter.drawText(QRectF(2.0, centre_y - 17.0, max(45.0, rect["x"] * sx - 4.0), 15.0), Qt.AlignCenter, f"W {int(round(rect['x']))} mm")
        painter.drawText(QRectF(centre_x - 50.0, height - rect["y"] * sy, 100.0, 16.0), Qt.AlignCenter, f"S {int(round(rect['y']))} mm")

    person_width_mm = max(100.0, float(layout.get("person_width_mm", 600.0) or 600.0))
    person_depth_mm = max(100.0, float(layout.get("person_depth_mm", 400.0) or 400.0))
    if rects:
        row_left = min(row["x"] for row in rects)
        row_right = max(row["x"] + row["w"] for row in rects)
        row_front = min(row["y"] for row in rects)
        row_rear = max(row["y"] + row["h"] for row in rects)
        people = [
            ((row_left + row_right) / 2.0, max(180.0, row_front * 0.45)),
            ((row_left + row_right) / 2.0, min(room_d - 180.0, row_rear + (room_d - row_rear) * 0.5)),
            (max(180.0, row_left * 0.45), (row_front + row_rear) / 2.0),
        ]
        if len(rects) == 1:
            people.append((min(room_w - 180.0, row_right + (room_w - row_right) * 0.5), (row_front + row_rear) / 2.0))
        for px, py in people:
            screen = point(px, py)
            _draw_top_down_person(
                painter,
                screen.x(),
                screen.y(),
                person_width_mm * sx,
                person_depth_mm * sy,
            )

    door_width = max(100.0, float(layout.get("door_width_mm", 900.0) or 900.0))
    offset = max(0.0, float(layout.get("door_offset_mm", 150.0) or 150.0))
    wall = _text(layout.get("door_wall") or "south").lower()
    painter.setPen(QPen(QColor("#ffffff"), max(8.0, width * 0.009)))
    if wall in {"south", "north"}:
        y = 0.0 if wall == "south" else room_d
        painter.drawLine(point(offset, y), point(offset + door_width, y))
    else:
        x = 0.0 if wall == "west" else room_w
        painter.drawLine(point(x, offset), point(x, offset + door_width))
    painter.setPen(QPen(QColor("#2563eb"), max(2.0, width * 0.002)))
    painter.setBrush(Qt.NoBrush)
    _draw_plan_door(
        painter, point, wall, offset, door_width, room_w, room_d, sx, sy
    )

    label_font = QFont(font_family, max(9, int(width * 0.012)), QFont.Bold)
    painter.setFont(label_font)
    painter.setPen(QPen(QColor("#0f172a"), 1.0))
    painter.setBrush(QBrush(QColor(255, 255, 255, 215)))
    painter.drawRect(QRectF(width * 0.2, 4.0, width * 0.6, max(24.0, height * 0.055)))
    painter.drawText(QRectF(width * 0.2, 4.0, width * 0.6, max(24.0, height * 0.055)), Qt.AlignCenter, f"{int(round(room_w))} x {int(round(room_d))} mm | {room_w * room_d / 1_000_000.0:.2f} m2")
    painter.drawText(QRectF(width * 0.01, height * 0.89, width * 0.98, height * 0.09), Qt.AlignCenter, f"{int(round(clearance))} mm clear access | Person envelope {int(round(person_width_mm))} x {int(round(person_depth_mm))} mm | FRONT marked in amber")
    painter.end()
    return _encode_png(image)


def network_room_cutaway_visible_walls(view_angle_deg: float) -> Tuple[str, str]:
    """Return the two far walls; camera-facing walls are intentionally omitted."""
    angle = math.radians(float(view_angle_deg or 0.0) % 360.0)
    x_wall = "west" if math.sin(angle) >= 0.0 else "east"
    y_wall = "south" if math.cos(angle) >= 0.0 else "north"
    return x_wall, y_wall


def _render_network_room_cutaway_png(layout: dict, pixel_width: int = 1400) -> bytes:
    font_family = _ensure_room_drawing_font()
    width = max(760, int(pixel_width)); height = max(600, int(width * 0.68))
    image = QImage(width, height, QImage.Format_ARGB32_Premultiplied); image.fill(QColor("#ffffff"))
    painter = QPainter(image); painter.setRenderHint(QPainter.Antialiasing, True)
    room_w = max(1000.0, float(layout.get("room_width_mm", 1000.0) or 1000.0))
    room_d = max(1000.0, float(layout.get("room_depth_mm", 1000.0) or 1000.0))
    view_angle_deg = float(layout.get("view_angle_deg", 35.0) or 35.0) % 360.0
    angle = math.radians(view_angle_deg)
    maximum_cabinet_height = max(
        [float(row.get("height_mm", 2000.0) or 2000.0) for row in layout.get("cabinets", []) or []]
        or [2000.0]
    )

    def raw(x, y, z=0.0):
        cx, cy = room_w / 2.0, room_d / 2.0
        dx, dy = x - cx, y - cy
        u = dx * math.cos(angle) - dy * math.sin(angle)
        v = (dx * math.sin(angle) + dy * math.cos(angle)) * 0.46 - z * 0.78
        return u, v

    room_height = max(2400.0, maximum_cabinet_height + 400.0)
    corners = [raw(x, y, z) for x in (0.0, room_w) for y in (0.0, room_d) for z in (0.0, room_height)]
    min_u, max_u = min(p[0] for p in corners), max(p[0] for p in corners)
    min_v, max_v = min(p[1] for p in corners), max(p[1] for p in corners)
    factor = min(width * 0.82 / max(1.0, max_u - min_u), height * 0.78 / max(1.0, max_v - min_v))

    def project(x, y, z=0.0):
        u, v = raw(x, y, z)
        return QPointF(width * 0.5 + (u - (min_u + max_u) / 2.0) * factor, height * 0.54 + (v - (min_v + max_v) / 2.0) * factor)

    def polygon(points):
        return QPolygonF([project(*point) for point in points])

    painter.setPen(QPen(QColor("#475569"), 2.0)); painter.setBrush(QBrush(QColor("#e2e8f0")))
    painter.drawPolygon(polygon([(0, 0, 0), (room_w, 0, 0), (room_w, room_d, 0), (0, room_d, 0)]))
    painter.setBrush(QBrush(QColor(219, 234, 254, 150)))
    visible_walls = network_room_cutaway_visible_walls(view_angle_deg)
    if "south" in visible_walls:
        painter.drawPolygon(polygon([(0, 0, 0), (room_w, 0, 0), (room_w, 0, room_height), (0, 0, room_height)]))
    else:
        painter.drawPolygon(polygon([(0, room_d, 0), (room_w, room_d, 0), (room_w, room_d, room_height), (0, room_d, room_height)]))
    if "west" in visible_walls:
        painter.drawPolygon(polygon([(0, 0, 0), (0, room_d, 0), (0, room_d, room_height), (0, 0, room_height)]))
    else:
        painter.drawPolygon(polygon([(room_w, 0, 0), (room_w, room_d, 0), (room_w, room_d, room_height), (room_w, 0, room_height)]))

    door_width = max(100.0, float(layout.get("door_width_mm", 900.0) or 900.0))
    door_offset = max(0.0, float(layout.get("door_offset_mm", 150.0) or 150.0))
    door_wall = _text(layout.get("door_wall") or "south").lower()
    if door_wall in visible_walls:
        if door_wall == "south":
            door_points = [(door_offset, 0, 0), (door_offset, door_width, 0), (door_offset, door_width, 2100), (door_offset, 0, 2100)]
        elif door_wall == "north":
            door_points = [(door_offset, room_d, 0), (door_offset, room_d-door_width, 0), (door_offset, room_d-door_width, 2100), (door_offset, room_d, 2100)]
        elif door_wall == "west":
            door_points = [(0, door_offset, 0), (door_width, door_offset, 0), (door_width, door_offset, 2100), (0, door_offset, 2100)]
        else:
            door_points = [(room_w, door_offset, 0), (room_w-door_width, door_offset, 0), (room_w-door_width, door_offset, 2100), (room_w, door_offset, 2100)]
        painter.setPen(QPen(QColor("#2563eb"), 3.0)); painter.setBrush(QBrush(QColor(96, 165, 250, 105)))
        painter.drawPolygon(polygon(door_points))

    rects = sorted(_layout_rects(layout), key=lambda row: raw(row["x"] + row["w"] / 2, row["y"] + row["h"] / 2)[1])
    colours = [QColor("#475569"), QColor("#64748b"), QColor("#334155")]
    for index, rect in enumerate(rects):
        x, y, w, d = rect["x"], rect["y"], rect["w"], rect["h"]
        cabinet_h = max(500.0, float(rect.get("height_mm", 2000.0) or 2000.0))
        painter.setPen(QPen(QColor("#0f172a"), 2.0))
        painter.setBrush(QBrush(colours[index % len(colours)]))
        painter.drawPolygon(polygon([(x, y, 0), (x+w, y, 0), (x+w, y, cabinet_h), (x, y, cabinet_h)]))
        painter.setBrush(QBrush(QColor("#94a3b8")))
        painter.drawPolygon(polygon([(x+w, y, 0), (x+w, y+d, 0), (x+w, y+d, cabinet_h), (x+w, y, cabinet_h)]))
        painter.setBrush(QBrush(QColor("#cbd5e1")))
        painter.drawPolygon(polygon([(x, y, cabinet_h), (x+w, y, cabinet_h), (x+w, y+d, cabinet_h), (x, y+d, cabinet_h)]))
        label = project(x + w / 2, y, cabinet_h * 0.55)
        painter.setPen(QPen(QColor("#ffffff"), 1.0)); painter.setFont(QFont(font_family, max(9, int(width * 0.012)), QFont.Bold))
        painter.drawText(QRectF(label.x() - 70, label.y() - 12, 140, 24), Qt.AlignCenter, _text(rect.get("name")) or f"Cabinet {index + 1}")
    for rect in rects[: min(3, len(rects))]:
        px, py = rect["x"] + rect["w"] / 2.0, max(120.0, rect["y"] * 0.45)
        feet = project(px, py, 0); head = project(px, py, 1700)
        painter.setPen(QPen(QColor("#2563eb"), max(3.0, width * 0.004))); painter.drawLine(feet, head)
        painter.drawLine(QPointF(head.x()-14, head.y()+24), QPointF(head.x()+14, head.y()+24))
        painter.drawLine(feet, QPointF(feet.x()-10, feet.y()+16)); painter.drawLine(feet, QPointF(feet.x()+10, feet.y()+16))
        painter.setBrush(QBrush(QColor("#60a5fa"))); painter.drawEllipse(head, max(5.0, width * 0.007), max(5.0, width * 0.007))
    warnings = room_layout_compliance(layout)
    painter.setPen(QPen(QColor("#991b1b" if warnings else "#166534"), 1.0)); painter.setFont(QFont(font_family, max(10, int(width * 0.014)), QFont.Bold))
    status = "NON-CONFORMANT: " + "; ".join(warnings[:2]) if warnings else "CONFORMS TO CONFIGURED CLEARANCES"
    painter.drawText(QRectF(12, height - 42, width - 24, 32), Qt.AlignCenter | Qt.TextWordWrap, status)
    painter.end(); return _encode_png(image)


class _TemplateVariables(dict):
    def __missing__(self, key):
        return "{" + str(key) + "}"


def format_network_snippet_template(template: str, variables: dict) -> str:
    try:
        return str(template or "").format_map(_TemplateVariables(variables))
    except (ValueError, KeyError):
        return str(template or "")


def cabinet_snippet_physical_size_mm(data: dict, snippet: dict) -> Tuple[float, float]:
    """Return the nominal rack elevation footprint used for scaled PDF placement."""

    values = network_report_snippet_variables(data, snippet)
    all_cabinets = _text(snippet.get("view_type")).lower() == "cabinet_all"
    rack_count = max(1, _integer(values.get("cabinet_count"), 1)) if all_cabinets else 1
    capacity_u = max(1, _integer(values.get("capacity_u"), 42))
    if all_cabinets:
        floor = _integer(snippet.get("floor"))
        location = _text(snippet.get("location_name"))
        room_capacities = [
            max(1, _integer(row.get("capacity_u"), 42))
            for row in data.get("network_racks", []) or []
            if isinstance(row, dict)
            and _integer(row.get("floor")) == floor
            and _text(row.get("location_name")) == location
        ]
        capacity_u = max(room_capacities) if room_capacities else max(
            1, int(math.ceil(capacity_u / rack_count))
        )
    # A conventional 19-inch cabinet is represented using a nominal 600 mm
    # external width and the exact 44.45 mm rack-unit pitch plus header/base.
    return 600.0 * rack_count, capacity_u * 44.45 + 180.0


def render_network_power_summary_png(
    data: dict,
    snippet: dict,
    pixel_width: int = 1600,
) -> bytes:
    """Render a room power-demand card with a per-cabinet breakdown."""

    summary = network_room_power_summary(data, snippet)
    rows = summary["rack_rows"] or [
        {
            "rack_name": "No installed equipment",
            "device_count": 0,
            "equipment_power_w": 0.0,
            "poe_load_w": 0.0,
            "total_power_w": 0.0,
        }
    ]
    width = max(900, int(pixel_width))
    row_height = 70
    height = max(700, 500 + row_height * len(rows))
    image = QImage(width, height, QImage.Format_ARGB32_Premultiplied)
    image.fill(QColor("#f8fafc"))
    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing, True)
    painter.setRenderHint(QPainter.TextAntialiasing, True)
    family = _ensure_room_drawing_font()

    def font(pixel_size, bold=False):
        value = QFont(family)
        value.setPixelSize(int(pixel_size))
        value.setBold(bool(bold))
        return value

    margin = 46.0
    painter.setPen(QPen(QColor("#0f172a")))
    painter.setFont(font(34, True))
    title = (
        f"{_text(snippet.get('room_type')) or 'Comms room'} power requirements - "
        f"{_text(snippet.get('location_name'))} - Floor {_integer(snippet.get('floor'))}"
    )
    painter.drawText(QRectF(margin, 28, width - margin * 2, 48), Qt.AlignLeft | Qt.AlignVCenter, title)
    painter.setFont(font(20))
    painter.setPen(QPen(QColor("#475569")))
    painter.drawText(
        QRectF(margin, 78, width - margin * 2, 34),
        Qt.AlignLeft | Qt.AlignVCenter,
        "Rated installed equipment plus assigned endpoint PoE demand",
    )

    cards = [
        ("EQUIPMENT", summary["equipment_power_w"], "#176b87"),
        ("POE ENDPOINTS", summary["poe_load_w"], "#7c3aed"),
        ("TOTAL DEMAND", summary["total_power_requirement_w"], "#0b6b50"),
        ("WITH SPARE", summary["recommended_power_capacity_w"], "#b45309"),
    ]
    gap = 18.0
    card_y = 132.0
    card_h = 128.0
    card_w = (width - margin * 2 - gap * 3) / 4.0
    for index, (label, watts, colour) in enumerate(cards):
        x = margin + index * (card_w + gap)
        painter.setPen(QPen(QColor("#cbd5e1"), 2))
        painter.setBrush(QBrush(QColor("#ffffff")))
        painter.drawRoundedRect(QRectF(x, card_y, card_w, card_h), 12, 12)
        painter.setPen(QPen(QColor(colour)))
        painter.setFont(font(18, True))
        painter.drawText(QRectF(x + 18, card_y + 14, card_w - 36, 28), Qt.AlignLeft | Qt.AlignVCenter, label)
        painter.setFont(font(36, True))
        painter.drawText(
            QRectF(x + 18, card_y + 48, card_w - 36, 52),
            Qt.AlignLeft | Qt.AlignVCenter,
            f"{float(watts):,.1f} W",
        )

    status_ok = summary["ups_capacity_w"] >= summary["recommended_power_capacity_w"] > 0
    status_colour = QColor("#dcfce7" if status_ok else "#fff7ed")
    status_pen = QColor("#166534" if status_ok else "#9a3412")
    painter.setPen(QPen(status_pen, 2))
    painter.setBrush(QBrush(status_colour))
    painter.drawRoundedRect(QRectF(margin, 280, width - margin * 2, 82), 10, 10)
    painter.setPen(QPen(status_pen))
    painter.setFont(font(21, True))
    painter.drawText(
        QRectF(margin + 18, 292, width - margin * 2 - 36, 28),
        Qt.AlignLeft | Qt.AlignVCenter,
        summary["power_status"],
    )
    painter.setFont(font(18))
    painter.drawText(
        QRectF(margin + 18, 324, width - margin * 2 - 36, 25),
        Qt.AlignLeft | Qt.AlignVCenter,
        f"UPS-backed capacity: {summary['ups_capacity_w']:,.1f} W | "
        f"PoE budget: {summary['poe_budget_w']:,.1f} W | "
        f"Modelled feeds: {summary['power_feed_count']}",
    )

    table_y = 394.0
    columns = [
        ("CABINET", 0.00, 0.34, Qt.AlignLeft),
        ("DEVICES", 0.34, 0.13, Qt.AlignRight),
        ("EQUIPMENT", 0.47, 0.18, Qt.AlignRight),
        ("POE LOAD", 0.65, 0.17, Qt.AlignRight),
        ("TOTAL", 0.82, 0.18, Qt.AlignRight),
    ]
    table_width = width - margin * 2
    painter.setPen(Qt.NoPen)
    painter.setBrush(QBrush(QColor("#0f3b4c")))
    painter.drawRoundedRect(QRectF(margin, table_y, table_width, 54), 8, 8)
    painter.setFont(font(17, True))
    painter.setPen(QPen(QColor("#ffffff")))
    for label, start, span, alignment in columns:
        painter.drawText(
            QRectF(margin + start * table_width + 14, table_y, span * table_width - 28, 54),
            alignment | Qt.AlignVCenter,
            label,
        )
    painter.setFont(font(18))
    for index, row in enumerate(rows):
        y = table_y + 54 + index * row_height
        painter.setPen(Qt.NoPen)
        painter.setBrush(QBrush(QColor("#ffffff" if index % 2 == 0 else "#f1f5f9")))
        painter.drawRect(QRectF(margin, y, table_width, row_height))
        values = [
            str(row["rack_name"]),
            str(row["device_count"]),
            f"{row['equipment_power_w']:,.1f} W",
            f"{row['poe_load_w']:,.1f} W",
            f"{row['total_power_w']:,.1f} W",
        ]
        painter.setPen(QPen(QColor("#1e293b")))
        for value, (_label, start, span, alignment) in zip(values, columns):
            painter.drawText(
                QRectF(margin + start * table_width + 14, y, span * table_width - 28, row_height),
                alignment | Qt.AlignVCenter,
                value,
            )
    painter.setPen(QPen(QColor("#64748b")))
    painter.setFont(font(15))
    painter.drawText(
        QRectF(margin, height - 48, width - margin * 2, 28),
        Qt.AlignLeft | Qt.AlignVCenter,
        f"Recommended capacity includes the configured {summary['spare_capacity_percent']:g}% spare allowance. "
        "Values depend on asset power ratings and assigned PoE loads.",
    )
    painter.end()
    return _encode_png(image)


def render_network_report_snippet_png(
    data: dict,
    snippet: dict,
    pixel_width: int = 2400,
    pixel_height: int = 1600,
) -> bytes:
    """Render the same topology/rack scene used by Network Topology as PNG bytes."""

    if _text(snippet.get("view_type")).lower() == "power_summary":
        return render_network_power_summary_png(data, snippet, pixel_width=pixel_width)
    return render_network_report_snippets_png(
        data,
        [snippet],
        pixel_width=pixel_width,
        pixel_height=pixel_height,
    )[0]


def render_network_report_snippets_png(
    data: dict,
    snippets: Sequence[dict],
    pixel_width: int = 2400,
    pixel_height: int = 1600,
    progress_callback=None,
) -> List[bytes]:
    """Render several topology snippets while reusing one prepared topology scene."""

    if all(
        _text(snippet.get("view_type")).lower() == "power_summary"
        for snippet in snippets
    ):
        rendered = []
        for index, snippet in enumerate(snippets):
            if progress_callback is not None and progress_callback(index, snippet) is False:
                break
            rendered.append(
                render_network_power_summary_png(data, snippet, pixel_width=pixel_width)
            )
        return rendered

    from network_topology import NetworkTopologyDialog

    dialog = NetworkTopologyDialog(None, deepcopy(data), on_change=None)
    try:
        dialog._initial_rebuild_scene()
        rendered = []
        for index, snippet in enumerate(snippets):
            if progress_callback is not None and progress_callback(index, snippet) is False:
                break
            rendered.append(
                _render_network_report_snippet_from_dialog(
                    dialog,
                    snippet,
                    pixel_width=pixel_width,
                    pixel_height=pixel_height,
                )
            )
        return rendered
    finally:
        dialog.close()
        dialog.deleteLater()


def _render_network_report_snippet_from_dialog(
    dialog,
    snippet: dict,
    pixel_width: int,
    pixel_height: int,
) -> bytes:
    if _text(snippet.get("view_type")).lower() == "power_summary":
        return render_network_power_summary_png(data=dialog.data, snippet=snippet, pixel_width=pixel_width)
    dialog.search_edit.blockSignals(True)
    dialog.search_edit.clear()
    dialog.search_edit.blockSignals(False)
    floor = _integer(snippet.get("floor"))
    location = _text(snippet.get("location_name"))
    view_type = _text(snippet.get("view_type")).lower()
    if view_type in {"cabinet", "cabinet_all"}:
        selected_rack = _text(snippet.get("rack_name")) if view_type == "cabinet" else "__all__"
        dialog.rack_focus = (floor, location, selected_rack)
        dialog.switch_port_focus = None
        dialog.rebuild_scene(fit=False)
    else:
        dialog.rack_focus = None
        dialog.switch_port_focus = None
        dialog.show_clients_check.blockSignals(True)
        dialog.show_clients_check.setChecked(False)
        dialog.show_clients_check.blockSignals(False)
        if view_type == "topology" and location:
            dialog.focus_location(location)
        else:
            floor_index = dialog.floor_combo.findData(floor)
            if floor_index >= 0:
                dialog.floor_combo.blockSignals(True)
                dialog.floor_combo.setCurrentIndex(floor_index)
                dialog.floor_combo.blockSignals(False)
            dialog.rebuild_scene(fit=False)
            if location:
                dialog.search_edit.setText(location)

    source = (
        _topology_location_source_rect(dialog, floor, location)
        if view_type == "topology" and location
        else dialog.scene.itemsBoundingRect()
    )
    if source.isEmpty():
        raise ValueError(f"No network view is available for {snippet.get('title', 'snippet')}.")
    margin = max(24.0, min(source.width(), source.height()) * 0.035)
    source = source.adjusted(-margin, -margin, margin, margin)
    maximum_dimension = max(480, int(pixel_width), int(pixel_height))
    aspect = max(0.05, float(source.width()) / max(1.0, float(source.height())))
    if aspect >= 1.0:
        width = maximum_dimension
        height = max(220, int(round(width / aspect)))
    else:
        height = maximum_dimension
        width = max(220, int(round(height * aspect)))
    image = QImage(width, height, QImage.Format_ARGB32_Premultiplied)
    image.fill(QColor("#ffffff"))
    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing, True)
    painter.setRenderHint(QPainter.TextAntialiasing, True)
    painter.setRenderHint(QPainter.SmoothPixmapTransform, True)
    dialog.scene.render(
        painter,
        QRectF(0.0, 0.0, float(width), float(height)),
        source,
        Qt.KeepAspectRatio,
    )
    painter.end()

    buffer = QBuffer()
    if not buffer.open(QIODevice.WriteOnly) or not image.save(buffer, "PNG"):
        raise RuntimeError("Could not encode the network view snippet.")
    return bytes(buffer.data())


def _topology_location_source_rect(dialog, floor: int, location: str) -> QRectF:
    """Return a tight frame around devices installed in one topology location."""
    source = QRectF()
    for node_id, item in dialog.node_items.items():
        node = dialog.model.nodes.get(node_id)
        if (
            node is None
            or _integer(node.floor) != int(floor)
            or _text(node.location_name) != _text(location)
        ):
            continue
        bounds = item.sceneBoundingRect()
        source = bounds if source.isNull() else source.united(bounds)
    return source if not source.isNull() else dialog.scene.itemsBoundingRect()
