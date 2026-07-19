"""Shared services for physical fibre, circuit tracing and IP address planning."""
from __future__ import annotations

from collections import defaultdict, deque
from copy import deepcopy
import ipaddress
import math
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

FIBRE_COLOURS = [
    "Blue", "Orange", "Green", "Brown", "Slate", "White",
    "Red", "Black", "Yellow", "Violet", "Rose", "Aqua",
]


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def next_record_id(items: Iterable[dict], prefix: str) -> str:
    used = {_text(item.get("id")) for item in items if isinstance(item, dict)}
    number = 1
    while f"{prefix}{number}" in used:
        number += 1
    return f"{prefix}{number}"


def fibre_colour(core_number: int) -> Tuple[str, int, str]:
    number = max(1, int(core_number))
    tube_number = (number - 1) // 12 + 1
    core_colour = FIBRE_COLOURS[(number - 1) % 12]
    tube_colour = FIBRE_COLOURS[(tube_number - 1) % 12]
    return core_colour, tube_number, tube_colour


def build_fibre_cores(
    core_count: int,
    allocated_count: int = 0,
    circuit_id: str = "",
    existing: Optional[Sequence[dict]] = None,
) -> List[dict]:
    existing_by_number = {
        _int(item.get("number")): item
        for item in (existing or [])
        if isinstance(item, dict) and _int(item.get("number")) > 0
    }
    result: List[dict] = []
    for number in range(1, max(1, int(core_count)) + 1):
        colour, tube_number, tube_colour = fibre_colour(number)
        old = deepcopy(existing_by_number.get(number, {}))
        allocated = number <= max(0, int(allocated_count))
        old.update(
            {
                "number": number,
                "colour": _text(old.get("colour")) or colour,
                "tube_number": max(1, _int(old.get("tube_number"), tube_number)),
                "tube_colour": _text(old.get("tube_colour")) or tube_colour,
                "status": _text(old.get("status")) or ("allocated" if allocated else "dark"),
                "circuit_id": _text(old.get("circuit_id")) or (circuit_id if allocated else ""),
                "from_termination": _text(old.get("from_termination")),
                "to_termination": _text(old.get("to_termination")),
                "notes": _text(old.get("notes")),
            }
        )
        result.append(old)
    return result


def _optional_float(value):
    return None if _text(value) == "" else _float(value)


def _fibre_cable_type_map(data: dict) -> Dict[str, dict]:
    return {
        _text(row.get("id")): row
        for row in data.get("network_fibre_cable_types", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }


def _select_fibre_cable_type(
    data: dict, required_cores: int, preferred_id: str = ""
) -> dict:
    cable_types = list(_fibre_cable_type_map(data).values())
    preferred = next(
        (row for row in cable_types if _text(row.get("id")) == _text(preferred_id)),
        None,
    )
    if preferred is not None and _int(preferred.get("core_count")) >= required_cores:
        return preferred
    eligible = [row for row in cable_types if _int(row.get("core_count")) >= required_cores]
    if eligible:
        return min(eligible, key=lambda row: (_int(row.get("core_count")), _text(row.get("id"))))
    if cable_types:
        # Retain the declared construction/loss values but allow a project-specific
        # core count when the library has not yet defined a large enough cable.
        return max(cable_types, key=lambda row: (_int(row.get("core_count")), _text(row.get("id"))))
    return {
        "id": "",
        "name": "OS2 fixed installation fibre",
        "fibre_standard": "OS2",
        "core_count": max(1, required_cores),
        "attenuation_db_per_m": 0.00035,
        "connector_loss_db": 0.5,
        "reflection_loss_db": 55.0,
        "splice_loss_db": 0.1,
        "wavelength_nm": 1310,
    }


def _route_point_map(data: dict) -> Dict[str, dict]:
    points: Dict[str, dict] = {}
    for collection in (
        data.get("locations", []),
        data.get("data_points", []),
        data.get("corridors", {}).get("nodes", []),
    ):
        for row in collection:
            if isinstance(row, dict) and _text(row.get("name")):
                points[_text(row.get("name"))] = row
    for transition in data.get("transitions", []):
        if not isinstance(transition, dict):
            continue
        transition_id = _text(transition.get("id"))
        for floor_text, coordinates in (transition.get("floor_locations") or {}).items():
            if not isinstance(coordinates, dict):
                continue
            row = dict(coordinates)
            row["floor"] = _int(floor_text)
            row["name"] = f"{transition_id}-F{floor_text}"
            points[row["name"]] = row
    return points


def _route_path_length(data: dict, route_path: Sequence[str], fallback: float = 0.0) -> float:
    points = _route_point_map(data)
    rows = [points.get(_text(name)) for name in route_path]
    if len(rows) < 2 or any(row is None for row in rows):
        return max(0.0, fallback)
    floor_height = max(0.0, _float(data.get("building", {}).get("floor_height_m"), 4.0))
    total = 0.0
    for a, b in zip(rows, rows[1:]):
        dx = _float(a.get("x")) - _float(b.get("x"))
        dy = _float(a.get("y")) - _float(b.get("y"))
        dz = (_int(a.get("floor")) - _int(b.get("floor"))) * floor_height
        total += math.sqrt(dx * dx + dy * dy + dz * dz)
    return total


def _fibre_connection_demands(data: dict) -> List[dict]:
    routed_physical_parents = {
        _text(connection.get("parent_logical_connection_id"))
        for connection in data.get("network_connections", [])
        if isinstance(connection, dict)
        and _text(connection.get("medium")).lower() == "fibre"
        and _text(connection.get("physical_segment")).lower() == "fibre_backbone"
        and connection.get("route_path")
        and _text(connection.get("parent_logical_connection_id"))
    }
    demands: List[dict] = []
    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict) or _text(connection.get("medium")).lower() != "fibre":
            continue
        cable_spec = _text(connection.get("cable_specification")).lower()
        route_path = [_text(value) for value in connection.get("route_path", []) if _text(value)]
        if "patch lead" in cable_spec or "patch cord" in cable_spec:
            continue
        if not route_path and max(0.0, _float(connection.get("length_m"))) <= 0.0:
            continue
        connection_id = _text(connection.get("id"))
        if not connection_id:
            continue
        if connection_id in routed_physical_parents and not bool(connection.get("physical_connection")):
            continue
        logical_id = _text(connection.get("parent_logical_connection_id")) or connection_id
        demands.append(
            {
                "id": logical_id,
                "connection_id": connection_id,
                "from_instance_id": _text(connection.get("from_instance_id")),
                "from_port": _text(connection.get("from_port")),
                "to_instance_id": _text(connection.get("to_instance_id")),
                "to_port": _text(connection.get("to_port")),
                "route_path": route_path,
                "length_m": max(0.0, _float(connection.get("length_m"))),
                "used_cores": max(1, _int(connection.get("fibre_count"), 2)),
                "cable_core_count": max(
                    max(1, _int(connection.get("fibre_count"), 2)),
                    _int(connection.get("cable_core_count"), 0),
                ),
                "from_termination_method": _text(connection.get("from_termination_method")),
                "to_termination_method": _text(connection.get("to_termination_method")),
                "from_connector_type": _text(connection.get("from_connector_type")),
                "to_connector_type": _text(connection.get("to_connector_type")),
                "connectorised_breakout": bool(connection.get("connectorised_breakout", False)),
                "from_panel_termination_allocations": list(connection.get("from_panel_termination_allocations", [])) if isinstance(connection.get("from_panel_termination_allocations", []), list) else [],
                "to_panel_termination_allocations": list(connection.get("to_panel_termination_allocations", [])) if isinstance(connection.get("to_panel_termination_allocations", []), list) else [],
                "auto_generated": bool(connection.get("auto_generated", False)),
                "redundancy_role": _text(connection.get("redundancy_role")),
                "protection_group": _text(connection.get("protection_group")),
            }
        )
    # Parent logical IDs can occur on hidden panel-to-panel records only once;
    # retain the routed/longest representation when malformed data duplicates it.
    result: Dict[str, dict] = {}
    for row in demands:
        existing = result.get(row["id"])
        score = (len(row["route_path"]), row["length_m"], int(bool(row["connection_id"])))
        old_score = (len(existing["route_path"]), existing["length_m"], int(bool(existing["connection_id"]))) if existing else (-1, -1.0, -1)
        if existing is None or score > old_score:
            result[row["id"]] = row
    return list(result.values())


def _apply_cable_type(cable: dict, cable_type: dict, core_count: int) -> None:
    cable["cable_type_id"] = _text(cable_type.get("id"))
    cable["cable_type"] = _text(cable_type.get("name")) or _text(cable_type.get("fibre_standard")) or "OS2 fixed installation fibre"
    cable["core_count"] = max(1, int(core_count))
    cable["attenuation_db_per_m"] = max(0.0, _float(cable_type.get("attenuation_db_per_m"), 0.00035))
    cable["connector_loss_db"] = max(0.0, _float(cable_type.get("connector_loss_db"), 0.5))
    cable["reflection_loss_db"] = max(0.0, _float(cable_type.get("reflection_loss_db"), 55.0))
    cable["minimum_return_loss_db"] = cable["reflection_loss_db"]
    cable["splice_loss_db"] = max(0.0, _float(cable_type.get("splice_loss_db"), 0.1))
    cable["wavelength_nm"] = max(0, _int(cable_type.get("wavelength_nm"), 1310))


def update_fibre_cable_loss(cable: dict) -> dict:
    cable["connector_count"] = max(0, _int(cable.get("connector_count")))
    cable["splice_count"] = max(0, _int(cable.get("splice_count")))
    cable["estimated_attenuation_db"] = round(
        max(0.0, _float(cable.get("length_m")))
        * max(0.0, _float(cable.get("attenuation_db_per_m"))),
        6,
    )
    cable["estimated_connector_loss_db"] = round(
        cable["connector_count"] * max(0.0, _float(cable.get("connector_loss_db"))), 6
    )
    cable["estimated_splice_loss_db"] = round(
        cable["splice_count"] * max(0.0, _float(cable.get("splice_loss_db"))), 6
    )
    cable["estimated_total_loss_db"] = round(
        cable["estimated_attenuation_db"]
        + cable["estimated_connector_loss_db"]
        + cable["estimated_splice_loss_db"],
        6,
    )
    return cable


def _designate_cable_cores(cable: dict, demands: Sequence[dict]) -> Dict[str, List[int]]:
    cores = build_fibre_cores(max(1, _int(cable.get("core_count"))))
    allocation: Dict[str, List[int]] = {}
    cursor = 1
    for demand in sorted(demands, key=lambda row: row["id"]):
        count = max(1, _int(demand.get("used_cores"), 1))
        numbers = list(range(cursor, min(cursor + count, len(cores) + 1)))
        if len(numbers) != count:
            raise ValueError(f"Cable {cable.get('id')} has insufficient cores for circuit {demand['id']}.")
        allocation[demand["id"]] = numbers
        for number in numbers:
            core = cores[number - 1]
            core["status"] = "allocated"
            core["circuit_id"] = demand["id"]
            core["from_termination"] = _text(cable.get("from_instance_id")) or _text(cable.get("from_location"))
            core["to_termination"] = _text(cable.get("to_instance_id")) or _text(cable.get("to_location"))
        cursor += count
    cable["cores"] = cores
    cable["core_designations"] = [
        {"circuit_id": circuit_id, "core_numbers": numbers}
        for circuit_id, numbers in sorted(allocation.items())
    ]
    return allocation


def _new_fibre_cable(
    data: dict,
    cables: List[dict],
    demands: Sequence[dict],
    route_path: Sequence[str],
    routing_role: str,
    preferred_type_id: str,
    required_cores: int,
    *,
    from_instance_id: str = "",
    from_port: str = "",
    to_instance_id: str = "",
    to_port: str = "",
    parent_cable_id: str = "",
    branch_node_id: str = "",
    from_method: str = "connectorised",
    to_method: str = "connectorised",
) -> Tuple[dict, Dict[str, List[int]]]:
    cable_type = _select_fibre_cable_type(data, required_cores, preferred_type_id)
    declared = max(required_cores, _int(cable_type.get("core_count"), required_cores))
    logical_ids = sorted({_text(row.get("id")) for row in demands if _text(row.get("id"))})
    fallback_length = max((_float(row.get("length_m")) for row in demands), default=0.0)
    cable_id = next_record_id(cables, "FOC")
    from_location = _record_location(data, from_instance_id)[0] if from_instance_id else (_text(route_path[0]) if route_path else "")
    to_location = _record_location(data, to_instance_id)[0] if to_instance_id else (_text(route_path[-1]) if route_path else "")
    cable = {
        "id": cable_id,
        "name": f"{routing_role.replace('_', ' ').title()} fibre {cable_id}",
        "from_instance_id": from_instance_id,
        "from_port": from_port,
        "to_instance_id": to_instance_id,
        "to_port": to_port,
        "from_location": from_location,
        "to_location": to_location,
        "route_path": list(route_path),
        "length_m": _route_path_length(data, route_path, fallback_length),
        "slack_length_m": 0.0,
        "logical_connection_ids": logical_ids,
        "splice_ids": [],
        "routing_role": routing_role,
        "parent_cable_id": parent_cable_id,
        "branch_node_id": branch_node_id,
        "from_termination_method": from_method,
        "to_termination_method": to_method,
        "connector_count": int(from_method == "connectorised") + int(to_method == "connectorised"),
        "splice_count": int(from_method == "spliced") + int(to_method == "spliced"),
        "installation_status": "planned" if any(row.get("auto_generated") for row in demands) else "installed",
        "owner": "",
        "drawing_layer": "NET-FIBRE-CABLE",
        "sheath_colour": _text(cable_type.get("sheath_colour")) or "Black",
        "label": cable_id,
        "notes": "Generated from designated logical fibre circuits.",
        "auto_generated": any(bool(row.get("auto_generated")) for row in demands),
    }
    _apply_cable_type(cable, cable_type, declared)
    allocation = _designate_cable_cores(cable, demands)
    update_fibre_cable_loss(cable)
    cables.append(cable)
    return cable, allocation


def _ensure_branch_enclosure(
    data: dict,
    node_name: str,
    incoming_cable: dict,
    max_splices_per_cassette: int,
) -> Tuple[dict, List[dict]]:
    nodes = data.setdefault("network_fibre_nodes", [])
    point = _route_point_map(data).get(node_name, {})
    enclosure = next(
        (
            row for row in nodes
            if isinstance(row, dict)
            and _text(row.get("node_type")) == "splice_enclosure"
            and _text(row.get("route_anchor")) == node_name
            and bool(row.get("auto_generated"))
        ),
        None,
    )
    if enclosure is None:
        enclosure = {
            "id": next_record_id(nodes, "FSE"),
            "name": f"Splice enclosure at {node_name}",
            "node_type": "splice_enclosure",
            "location_name": node_name if _text(point.get("kind")) == "location" else "",
            "floor": _int(point.get("floor")),
            "x": _float(point.get("x")),
            "y": _float(point.get("y")),
            "rack_name": "",
            "rack_start_u": 0,
            "rack_units": 0,
            "parent_node_id": "",
            "linked_instance_id": "",
            "route_anchor": node_name,
            "incoming_cable_id": _text(incoming_cable.get("id")),
            "splice_capacity": max(1, _int(incoming_cable.get("core_count"))),
            "cassette_capacity": max(1, _int(incoming_cable.get("core_count"))),
            "drawing_layer": "NET-FIBRE-NODE",
            "symbol": "splice_enclosure",
            "label": f"SE {node_name}",
            "notes": "Auto-generated spine branch enclosure.",
            "auto_generated": True,
        }
        nodes.append(enclosure)
    required_trays = max(1, int(math.ceil(max(1, _int(incoming_cable.get("core_count"))) / max_splices_per_cassette)))
    cassettes = [
        row for row in nodes
        if isinstance(row, dict)
        and _text(row.get("node_type")) == "splice_cassette"
        and _text(row.get("parent_node_id")) == _text(enclosure.get("id"))
    ]
    while len(cassettes) < required_trays:
        tray_number = len(cassettes) + 1
        cassette = {
            "id": next_record_id(nodes, "FSC"),
            "name": f"{enclosure['name']} tray {tray_number}",
            "node_type": "splice_cassette",
            "location_name": _text(enclosure.get("location_name")),
            "floor": _int(enclosure.get("floor")),
            "x": _float(enclosure.get("x")),
            "y": _float(enclosure.get("y")),
            "rack_name": "",
            "rack_start_u": 0,
            "rack_units": 0,
            "parent_node_id": _text(enclosure.get("id")),
            "linked_instance_id": "",
            "route_anchor": node_name,
            "incoming_cable_id": _text(incoming_cable.get("id")),
            "tray_number": tray_number,
            "max_splices_per_tray": max_splices_per_cassette,
            "cassette_capacity": max_splices_per_cassette,
            "splice_capacity": max_splices_per_cassette,
            "drawing_layer": "NET-FIBRE-SPLICE",
            "symbol": "splice_cassette",
            "label": f"Tray {tray_number}",
            "notes": "Maximum 24 splice positions per cassette tray.",
            "auto_generated": True,
        }
        nodes.append(cassette)
        cassettes.append(cassette)
    cassettes.sort(key=lambda row: (_int(row.get("tray_number")), _text(row.get("id"))))
    return enclosure, cassettes


def _add_splice_record(
    data: dict,
    enclosure: dict,
    cassette: dict,
    incoming_cable: dict,
    incoming_core: int,
    outgoing_cable: Optional[dict],
    outgoing_core: int,
    circuit_id: str,
    loss_db: float,
    *,
    pigtail: bool = False,
    connectorised: bool = False,
    termination_instance_id: str = "",
    termination_port: str = "",
) -> dict:
    splices = data.setdefault("network_fibre_splices", [])
    splice = {
        "id": next_record_id(splices, "FS"),
        "node_id": _text(enclosure.get("id")),
        "cassette_id": _text(cassette.get("id")),
        "incoming_cable_id": _text(incoming_cable.get("id")),
        "incoming_core": incoming_core,
        "outgoing_cable_id": _text((outgoing_cable or {}).get("id")),
        "outgoing_core": max(1, outgoing_core),
        "splice_type": "pigtail" if pigtail else ("connectorised" if connectorised else "fusion"),
        "circuit_id": circuit_id,
        "loss_db": max(0.0, loss_db),
        "pigtail": bool(pigtail),
        "connectorised": bool(connectorised),
        "termination_instance_id": termination_instance_id,
        "termination_port": termination_port,
        "drawing_layer": "NET-FIBRE-SPLICE",
        "label": "",
        "notes": "Auto-generated designated fibre splice.",
        "auto_generated": True,
    }
    splice["label"] = splice["id"]
    splices.append(splice)
    for cable in (incoming_cable, outgoing_cable):
        if not cable:
            continue
        ids = [_text(value) for value in cable.get("splice_ids", []) if _text(value)]
        if splice["id"] not in ids:
            ids.append(splice["id"])
        cable["splice_ids"] = ids
        cable["splice_count"] = max(_int(cable.get("splice_count")), len(ids))
        update_fibre_cable_loss(cable)
    return splice


def _merge_panel_termination_allocations(
    demands: Sequence[dict], side: str
) -> List[dict]:
    """Combine per-circuit panel allocations into cassette-level records."""
    key = f"{side}_panel_termination_allocations"
    merged: Dict[Tuple[str, int], dict] = {}
    for demand in demands:
        rows = demand.get(key, [])
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            panel_id = _text(row.get("panel_instance_id") or row.get("panel_id"))
            position = max(1, _int(row.get("cassette_position"), 1))
            if not panel_id:
                continue
            item = merged.setdefault(
                (panel_id, position),
                {
                    "panel_instance_id": panel_id,
                    "cassette_position": position,
                    "ports": [],
                    "termination_mode": _text(row.get("termination_mode")),
                    "rear_connector": _text(row.get("rear_connector")),
                    "rear_connector_count": 0,
                    "fibre_count": 0,
                    "logical_connection_ids": [],
                },
            )
            for port in row.get("ports", []):
                port = _text(port)
                if port and port not in item["ports"]:
                    item["ports"].append(port)
            item["fibre_count"] += max(0, _int(row.get("fibre_count")))
            logical_id = _text(demand.get("id"))
            if logical_id and logical_id not in item["logical_connection_ids"]:
                item["logical_connection_ids"].append(logical_id)
            item["rear_connector_count"] = max(
                item["rear_connector_count"],
                max(0, _int(row.get("rear_connector_count"))),
            )
    return [merged[key] for key in sorted(merged)]


def _configure_panel_allocations_for_mpo(
    data: dict,
    allocations: Sequence[dict],
    rear_connector: str,
) -> None:
    """Convert allocated modular cassettes to connectorised MPO/MTP breakout."""
    rear_connector = _text(rear_connector).lower() or "mpo-24"
    rear_fibres = 24 if rear_connector.endswith("24") else 12
    instances = {
        _text(row.get("id")): row
        for row in data.get("network_asset_instances", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    for allocation in allocations:
        panel = instances.get(_text(allocation.get("panel_instance_id")))
        if panel is None:
            continue
        position = max(1, _int(allocation.get("cassette_position"), 1))
        cassettes = panel.get("fibre_cassettes", [])
        if not isinstance(cassettes, list):
            continue
        cassette = next(
            (
                row for index, row in enumerate(cassettes, start=1)
                if isinstance(row, dict)
                and max(1, _int(row.get("position"), index)) == position
            ),
            None,
        )
        if cassette is None:
            continue
        ports = [_text(value) for value in allocation.get("ports", []) if _text(value)]
        used_names = [
            _text(value)
            for value in cassette.get("used_front_connector_names", [])
            if _text(value)
        ]
        for port in ports:
            if port not in used_names:
                used_names.append(port)
        fibre_count = max(
            max(0, _int(cassette.get("used_fibres"))),
            max(0, _int(allocation.get("fibre_count"))),
            len(used_names) * 2,
        )
        cassette["termination_mode"] = "connectorised"
        cassette["rear_connector"] = rear_connector
        cassette["used_front_connector_names"] = used_names
        cassette["used_front_connectors"] = max(
            max(0, _int(cassette.get("used_front_connectors"))), len(used_names)
        )
        cassette["used_fibres"] = fibre_count
        cassette["rear_connector_count"] = max(
            1,
            min(4, int(math.ceil(fibre_count / float(rear_fibres)))),
        )
        allocation["termination_mode"] = "connectorised"
        allocation["rear_connector"] = rear_connector
        allocation["rear_connector_count"] = cassette["rear_connector_count"]
        panel["connectorised_breakout"] = True
        panel["termination_count"] = sum(
            max(0, _int(row.get("used_front_connectors")))
            for row in cassettes
            if isinstance(row, dict)
        )
        panel["available_connector_count"] = max(
            0,
            sum(
                max(0, _int(row.get("front_connector_capacity"), 12))
                for row in cassettes
                if isinstance(row, dict)
            )
            - _int(panel.get("termination_count")),
        )


def _apply_cable_panel_metadata(
    data: dict,
    cable: dict,
    demands: Sequence[dict],
    *,
    include_from: bool,
    include_to: bool,
    mpo_threshold: int,
    mpo_connector: str,
) -> None:
    """Attach panel/cassette allocations and apply large-trunk MPO breakout."""
    from_allocations = (
        _merge_panel_termination_allocations(demands, "from")
        if include_from else []
    )
    to_allocations = (
        _merge_panel_termination_allocations(demands, "to")
        if include_to else []
    )
    cable["from_panel_termination_allocations"] = from_allocations
    cable["to_panel_termination_allocations"] = to_allocations

    large_trunk = max(1, _int(cable.get("core_count"))) >= max(12, mpo_threshold)
    if not large_trunk or not (from_allocations or to_allocations):
        return

    cable["connectorised_breakout"] = True
    if from_allocations:
        cable["from_termination_method"] = "connectorised"
        cable["from_connector_type"] = mpo_connector
        _configure_panel_allocations_for_mpo(data, from_allocations, mpo_connector)
    if to_allocations:
        cable["to_termination_method"] = "connectorised"
        cable["to_connector_type"] = mpo_connector
        _configure_panel_allocations_for_mpo(data, to_allocations, mpo_connector)
    cable["connector_count"] = int(
        _text(cable.get("from_termination_method")) == "connectorised"
    ) + int(_text(cable.get("to_termination_method")) == "connectorised")
    cable["notes"] = (
        "Generated high-core-count spine with MPO/MTP rear interfaces into "
        "shared modular LC/SC breakout cassettes."
    )
    update_fibre_cable_loss(cable)


def _sync_direct_fibre_cables(data: dict, cables: List[dict], demands: Sequence[dict]) -> List[dict]:
    planning = data.get("network_settings", {}).get("physical_fibre_planning", {})
    preferred = _text(planning.get("default_cable_type_id"))
    spare = max(0.0, _float(planning.get("spare_core_percent"), 15.0)) / 100.0
    mpo_threshold = max(12, _int(planning.get("mpo_breakout_minimum_cores"), 48))
    mpo_connector = _text(planning.get("mpo_breakout_connector")).lower() or "mpo-24"
    if mpo_connector not in {"mpo-12", "mtp-12", "mpo-24", "mtp-24"}:
        mpo_connector = "mpo-24"
    created: List[dict] = []
    assets = {_text(row.get("id")): row for row in data.get("network_assets", []) if isinstance(row, dict)}
    instances = {_text(row.get("id")): row for row in data.get("network_asset_instances", []) if isinstance(row, dict)}
    grouped: Dict[Tuple[object, ...], List[dict]] = defaultdict(list)
    for demand in demands:
        route_path = tuple(_text(value) for value in demand.get("route_path", []) if _text(value))
        route_key: Tuple[object, ...]
        if len(route_path) >= 2:
            route_key = ("route", route_path)
        else:
            # Without a routed path, only consolidate circuits between the same
            # physical endpoints so unrelated fallback lengths cannot be merged.
            route_key = (
                "endpoints",
                _text(demand.get("from_instance_id")),
                _text(demand.get("to_instance_id")),
                round(max(0.0, _float(demand.get("length_m"))), 3),
            )
        grouped[
            route_key
            + (
                _text(demand.get("redundancy_role")).lower(),
                _text(demand.get("from_termination_method")).lower(),
                _text(demand.get("to_termination_method")).lower(),
                _text(demand.get("from_connector_type")).lower(),
                _text(demand.get("to_connector_type")).lower(),
                bool(demand.get("connectorised_breakout", False)),
            )
        ].append(demand)

    for group in sorted(grouped.values(), key=lambda rows: min(row["id"] for row in rows)):
        group = sorted(group, key=lambda row: row["id"])
        first_demand = group[0]
        used_cores = sum(max(1, _int(row.get("used_cores"), 1)) for row in group)
        # A declared larger cable is a minimum for the shared run, not a reason
        # to install that same spare allowance once per logical circuit.
        declared_cores = max(
            [used_cores]
            + [
                max(1, _int(row.get("cable_core_count"), row.get("used_cores", 1)))
                for row in group
            ]
        )
        required = max(declared_cores, int(math.ceil(used_cores * (1.0 + spare))))
        target_asset = assets.get(_text(instances.get(first_demand["to_instance_id"], {}).get("asset_id")), {})
        target_is_splitter = _text(target_asset.get("asset_type")) == "fibre_splitter"
        from_method = _text(first_demand.get("from_termination_method")) or "connectorised"
        to_method = _text(first_demand.get("to_termination_method"))
        if not to_method:
            to_method = _text(planning.get("splitter_termination_method")) if target_is_splitter else "connectorised"
        cable, _allocation = _new_fibre_cable(
            data, cables, group, first_demand["route_path"], "direct", preferred, required,
            from_instance_id=first_demand["from_instance_id"], from_port=first_demand["from_port"],
            to_instance_id=first_demand["to_instance_id"], to_port=first_demand["to_port"],
            from_method=from_method, to_method=to_method or "connectorised",
        )
        cable["connectorised_breakout"] = bool(first_demand.get("connectorised_breakout", False))
        cable["from_connector_type"] = _text(first_demand.get("from_connector_type"))
        cable["to_connector_type"] = _text(first_demand.get("to_connector_type"))
        _apply_cable_panel_metadata(
            data,
            cable,
            group,
            include_from=any(row.get("from_panel_termination_allocations") for row in group),
            include_to=any(row.get("to_panel_termination_allocations") for row in group),
            mpo_threshold=mpo_threshold,
            mpo_connector=mpo_connector,
        )
        if cable.get("connectorised_breakout") and "MPO/MTP" not in _text(cable.get("notes")):
            cable["notes"] = "Generated high-core-count MPO/MTP trunk with modular cassette breakout."
        created.append(cable)
    return created


def _sync_spine_and_spur_cables(data: dict, cables: List[dict], demands: Sequence[dict]) -> List[dict]:
    planning = data.get("network_settings", {}).get("physical_fibre_planning", {})
    spare = max(0.0, _float(planning.get("spare_core_percent"), 15.0)) / 100.0
    max_per_tray = max(1, min(24, _int(planning.get("max_splices_per_cassette"), 24)))
    branch_method = _text(planning.get("branch_termination_method")) or "spliced"
    splitter_method = _text(planning.get("splitter_termination_method")) or "connectorised"
    extend_reduced_spine = bool(planning.get("extend_reduced_count_spine", True))
    mpo_threshold = max(12, _int(planning.get("mpo_breakout_minimum_cores"), 48))
    mpo_connector = _text(planning.get("mpo_breakout_connector")).lower() or "mpo-24"
    if mpo_connector not in {"mpo-12", "mtp-12", "mpo-24", "mtp-24"}:
        mpo_connector = "mpo-24"
    created: List[dict] = []
    demand_by_id = {row["id"]: row for row in demands}
    assets = {_text(row.get("id")): row for row in data.get("network_assets", []) if isinstance(row, dict)}
    instances = {_text(row.get("id")): row for row in data.get("network_asset_instances", []) if isinstance(row, dict)}

    grouped: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    direct_fallback: List[dict] = []
    for demand in demands:
        if len(demand["route_path"]) < 2:
            direct_fallback.append(demand)
            continue
        grouped[(demand["from_instance_id"], demand["route_path"][0])].append(demand)

    created.extend(_sync_direct_fibre_cables(data, cables, direct_fallback))
    for (_source_instance_id, root), group in sorted(grouped.items()):
        if len(group) < 2:
            created.extend(_sync_direct_fibre_cables(data, cables, group))
            continue
        edge_demands: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
        outgoing: Dict[str, Set[str]] = defaultdict(set)
        incoming: Dict[str, Set[str]] = defaultdict(set)
        for demand in group:
            path = demand["route_path"]
            for a, b in zip(path, path[1:]):
                edge_demands[(a, b)].add(demand["id"])
                outgoing[a].add(b)
                incoming[b].add(a)

        segments: List[dict] = []
        visited: Set[Tuple[str, str]] = set()

        def walk(a: str, b: str) -> None:
            if (a, b) in visited:
                return
            visited.add((a, b))
            demand_ids = set(edge_demands[(a, b)])
            path = [a, b]
            current = b
            while True:
                candidates = sorted(outgoing.get(current, set()))
                if len(candidates) != 1:
                    break
                nxt = candidates[0]
                if (current, nxt) in visited or set(edge_demands[(current, nxt)]) != demand_ids:
                    break
                visited.add((current, nxt))
                path.append(nxt)
                current = nxt
            segments.append({"path": path, "demand_ids": demand_ids})
            for nxt in sorted(outgoing.get(current, set())):
                walk(current, nxt)

        for child in sorted(outgoing.get(root, set())):
            walk(root, child)
        for edge_key in sorted(edge_demands):
            if edge_key not in visited:
                walk(*edge_key)

        segment_by_start: Dict[str, List[int]] = defaultdict(list)
        segment_by_end: Dict[str, List[int]] = defaultdict(list)
        for index, segment in enumerate(segments):
            segment_by_start[segment["path"][0]].append(index)
            segment_by_end[segment["path"][-1]].append(index)

        role_by_index: Dict[int, str] = {
            index: ("spine" if len(segment["demand_ids"]) > 1 else "spur")
            for index, segment in enumerate(segments)
        }

        def continuation_score(index: int) -> Tuple[int, int, float, str]:
            segment = segments[index]
            segment_demands = [demand_by_id[value] for value in segment["demand_ids"]]
            used = sum(max(1, _int(row.get("used_cores"), 1)) for row in segment_demands)
            remaining_distance = max(
                (_route_path_length(data, row.get("route_path", []), row.get("length_m", 0.0)) for row in segment_demands),
                default=0.0,
            )
            return (len(segment["demand_ids"]), used, remaining_distance, "|".join(segment["path"]))

        # Walk outward from every root segment. At each branch, keep the dominant
        # route as the spine. Once only one circuit remains it becomes a
        # reduced-count spine extension; side branches remain true spurs.
        root_indices = sorted(segment_by_start.get(root, []), key=continuation_score, reverse=True)
        queue = deque(root_indices)
        queued = set(root_indices)
        while queue:
            parent_index = queue.popleft()
            parent_segment = segments[parent_index]
            child_indices = sorted(
                segment_by_start.get(parent_segment["path"][-1], []),
                key=continuation_score,
                reverse=True,
            )
            if child_indices and extend_reduced_spine and role_by_index.get(parent_index) in {"spine", "spine_extension"}:
                dominant = child_indices[0]
                if len(segments[dominant]["demand_ids"]) == 1:
                    role_by_index[dominant] = "spine_extension"
            for child_index in child_indices:
                if child_index not in queued:
                    queued.add(child_index)
                    queue.append(child_index)

        # Parent-before-child order guarantees parent cable identifiers are
        # available when each reduced-count continuation is created.
        creation_order: List[int] = []
        queue = deque(root_indices)
        seen_indices: Set[int] = set()
        while queue:
            index = queue.popleft()
            if index in seen_indices:
                continue
            seen_indices.add(index)
            creation_order.append(index)
            for child_index in segment_by_start.get(segments[index]["path"][-1], []):
                queue.append(child_index)
        creation_order.extend(index for index in range(len(segments)) if index not in seen_indices)

        cable_for_segment: Dict[int, dict] = {}
        allocation_for_segment: Dict[int, Dict[str, List[int]]] = {}
        for index in creation_order:
            segment = segments[index]
            segment_demands = [demand_by_id[value] for value in sorted(segment["demand_ids"])]
            used = sum(row["used_cores"] for row in segment_demands)
            required = max(used, int(math.ceil(used * (1.0 + spare))))
            role = role_by_index[index]
            first = segment["path"][0] == root and not segment_by_end.get(segment["path"][0])
            if role == "spur":
                preferred = _text(planning.get("spur_cable_type_id"))
            elif first:
                preferred = _text(planning.get("spine_cable_type_id"))
            else:
                # A downstream continuation is deliberately allowed to select
                # the smallest library cable that satisfies its remaining load.
                preferred = "" if extend_reduced_spine else _text(planning.get("spine_cable_type_id"))
            final_demands = [row for row in segment_demands if row["route_path"][-1] == segment["path"][-1]]
            sole_final = final_demands[0] if len(final_demands) == 1 and len(segment["demand_ids"]) == 1 else None
            target_asset = assets.get(_text(instances.get(_text((sole_final or {}).get("to_instance_id")), {}).get("asset_id")), {})
            target_is_splitter = _text(target_asset.get("asset_type")) == "fibre_splitter"
            to_method = splitter_method if target_is_splitter else ("connectorised" if sole_final else branch_method)
            parent_indices = segment_by_end.get(segment["path"][0], [])
            parent_cable_id = next(
                (
                    _text(cable_for_segment[parent_index].get("id"))
                    for parent_index in parent_indices
                    if parent_index in cable_for_segment
                ),
                "",
            )
            cable, allocation = _new_fibre_cable(
                data, cables, segment_demands, segment["path"], role, preferred, required,
                from_instance_id=segment_demands[0]["from_instance_id"] if first else "",
                from_port=segment_demands[0]["from_port"] if first else "",
                to_instance_id=_text((sole_final or {}).get("to_instance_id")),
                to_port=_text((sole_final or {}).get("to_port")),
                parent_cable_id=parent_cable_id,
                branch_node_id=segment["path"][0] if not first else "",
                from_method="connectorised" if first else branch_method,
                to_method=to_method,
            )
            if role == "spine_extension":
                cable["notes"] = (
                    "Reduced-count continuation of the dominant spine route; "
                    "side branches are modelled as separate spur cables."
                )
                cable["spine_continuation"] = True
            _apply_cable_panel_metadata(
                data,
                cable,
                segment_demands,
                include_from=first,
                include_to=bool(sole_final),
                mpo_threshold=mpo_threshold,
                mpo_connector=mpo_connector,
            )
            cable_for_segment[index] = cable
            allocation_for_segment[index] = allocation
            created.append(cable)

        # Map core designations through each branch. Every circuit is explicitly
        # spliced from its incoming spine core to the designated outgoing core.
        for node_name, parent_indices in sorted(segment_by_end.items()):
            child_indices = segment_by_start.get(node_name, [])
            if not child_indices:
                continue
            for parent_index in parent_indices:
                parent_segment = segments[parent_index]
                parent_cable = cable_for_segment[parent_index]
                enclosure, cassettes = _ensure_branch_enclosure(data, node_name, parent_cable, max_per_tray)
                splice_position = 0
                for child_index in child_indices:
                    child_segment = segments[child_index]
                    child_cable = cable_for_segment[child_index]
                    common_ids = sorted(parent_segment["demand_ids"] & child_segment["demand_ids"])
                    for circuit_id in common_ids:
                        in_numbers = allocation_for_segment[parent_index][circuit_id]
                        out_numbers = allocation_for_segment[child_index][circuit_id]
                        for in_core, out_core in zip(in_numbers, out_numbers):
                            cassette = cassettes[min(len(cassettes) - 1, splice_position // max_per_tray)]
                            _add_splice_record(
                                data, enclosure, cassette, parent_cable, in_core,
                                child_cable, out_core, circuit_id,
                                _float(parent_cable.get("splice_loss_db"), 0.1),
                            )
                            splice_position += 1

        # A spliced splitter termination uses a designated pigtail in the final
        # cassette. Connectorised splitter spurs terminate directly and create no
        # pigtail splice record.
        if splitter_method == "spliced" and bool(planning.get("splitter_pigtail", True)):
            for index, segment in enumerate(segments):
                cable = cable_for_segment[index]
                for circuit_id in sorted(segment["demand_ids"]):
                    demand = demand_by_id[circuit_id]
                    if demand["route_path"][-1] != segment["path"][-1]:
                        continue
                    target_instance = instances.get(demand["to_instance_id"], {})
                    target_asset = assets.get(_text(target_instance.get("asset_id")), {})
                    if _text(target_asset.get("asset_type")) != "fibre_splitter":
                        continue
                    enclosure, cassettes = _ensure_branch_enclosure(data, segment["path"][-1], cable, max_per_tray)
                    for ordinal, in_core in enumerate(allocation_for_segment[index][circuit_id]):
                        cassette = cassettes[min(len(cassettes) - 1, ordinal // max_per_tray)]
                        _add_splice_record(
                            data, enclosure, cassette, cable, in_core, None, 1,
                            circuit_id, _float(cable.get("splice_loss_db"), 0.1),
                            pigtail=True,
                            termination_instance_id=demand["to_instance_id"],
                            termination_port=demand["to_port"],
                        )
    return created

def sync_fibre_cables_from_connections(data: dict, replace_auto: bool = False) -> List[dict]:
    """Build designated fixed-installation fibre from logical connections.

    ``direct`` creates one home-run cable per logical circuit. ``spine_and_spur``
    constructs a routed trunk, reduces cable core count after each branch, creates
    24-position cassette trays, and records every through-splice/pigtail so field
    schedules can identify the exact cable, tube and core.
    """
    cables = data.setdefault("network_fibre_cables", [])
    if replace_auto:
        cables[:] = [item for item in cables if not bool(item.get("auto_generated"))]
        data["network_fibre_nodes"] = [item for item in data.get("network_fibre_nodes", []) if not bool(item.get("auto_generated"))]
        data["network_fibre_splices"] = [item for item in data.get("network_fibre_splices", []) if not bool(item.get("auto_generated"))]
    demands = _fibre_connection_demands(data)
    mode = _text(
        data.get("network_settings", {})
        .get("physical_fibre_planning", {})
        .get("routing_mode")
    ).lower()
    if mode == "spine_and_spur":
        created = _sync_spine_and_spur_cables(data, cables, demands)
    else:
        created = _sync_direct_fibre_cables(data, cables, demands)
    set_core_status_from_splices(data)
    calculate_optical_budgets(data)
    return created


def calculate_optical_budgets(data: dict) -> List[dict]:
    """Calculate optical budgets from installed transceivers and passive plant.

    SFP/QSFP-family cages obtain transmit, receive, wavelength, insertion and
    return-loss data from ``network_optic_modules`` rather than from the host
    switch. Integrated optics such as PON ports retain port/asset fallback for
    backwards compatibility.
    """
    assets = {_text(row.get("id")): row for row in data.get("network_assets", []) if isinstance(row, dict)}
    instances = {_text(row.get("id")): row for row in data.get("network_asset_instances", []) if isinstance(row, dict)}
    modules = {_text(row.get("id")): row for row in data.get("network_optic_modules", []) if isinstance(row, dict) and _text(row.get("id"))}
    modules_by_connection_host: Dict[Tuple[str, str], dict] = {}
    for module in modules.values():
        key = (_text(module.get("connection_id")), _text(module.get("host_instance_id")))
        if key[0] and key[1]:
            modules_by_connection_host[key] = module

    cable_loss_by_connection: Dict[str, float] = defaultdict(float)
    cable_ids_by_connection: Dict[str, List[str]] = defaultdict(list)
    return_loss_by_connection: Dict[str, List[float]] = defaultdict(list)
    for cable in data.get("network_fibre_cables", []):
        if not isinstance(cable, dict):
            continue
        update_fibre_cable_loss(cable)
        for connection_id in cable.get("logical_connection_ids", []):
            cid = _text(connection_id)
            if not cid:
                continue
            cable_loss_by_connection[cid] += max(0.0, _float(cable.get("estimated_total_loss_db")))
            cable_ids_by_connection[cid].append(_text(cable.get("id")))
            value = max(0.0, _float(cable.get("minimum_return_loss_db", cable.get("reflection_loss_db"))))
            if value:
                return_loss_by_connection[cid].append(value)

    adjacency: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    logical_connections: Dict[str, dict] = {}
    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict) or _text(connection.get("medium")).lower() != "fibre":
            continue
        if bool(connection.get("physical_connection")) or bool(connection.get("topology_hidden")):
            continue
        cid = _text(connection.get("id")); a = _text(connection.get("from_instance_id")); b = _text(connection.get("to_instance_id"))
        if not cid or a not in instances or b not in instances or a == b:
            continue
        logical_connections[cid] = connection
        adjacency[a].append((b, cid)); adjacency[b].append((a, cid))

    cage_types = {"sfp", "sfp+", "sfp28", "qsfp", "qsfp+", "qsfp28", "qsfp56", "qsfpdd", "osfp"}
    passive_types = {"fibre_splitter", "patch_panel"}

    def observed_port(connection: dict, instance_id: str) -> str:
        return _text(connection.get("from_port")) if _text(connection.get("from_instance_id")) == instance_id else _text(connection.get("to_port"))

    def port_definition(asset: dict, port_name: str) -> dict:
        target = _text(port_name).lower()
        rows = [row for row in asset.get("port_definitions", []) if isinstance(row, dict)]
        for row in rows:
            explicit = [_text(value).lower() for value in row.get("explicit_names", []) if _text(value)] if isinstance(row.get("explicit_names", []), list) else []
            prefix = _text(row.get("name_prefix")).lower()
            if target in explicit or (prefix and target.startswith(prefix)):
                return row
        if len(rows) == 1:
            return rows[0]
        return {}

    def optic_values(instance_id: str, connection_id: str) -> dict:
        instance = instances.get(instance_id, {})
        host_asset = assets.get(_text(instance.get("asset_id")), {})
        connection = logical_connections.get(connection_id, {})
        port = port_definition(host_asset, observed_port(connection, instance_id))
        port_type = _text(port.get("port_type")).lower()
        module = modules_by_connection_host.get((connection_id, instance_id))
        if module is not None:
            optic_asset = assets.get(_text(module.get("asset_id")), {})
            return {
                "tx": _optional_float(optic_asset.get("optical_tx_power_dbm")),
                "rx": _optional_float(optic_asset.get("optical_receiver_sensitivity_dbm")),
                "loss": max(0.0, _float(optic_asset.get("optical_insertion_loss_db"))),
                "return_loss": max(0.0, _float(optic_asset.get("optical_return_loss_db"))),
                "wavelength_nm": _int(optic_asset.get("optical_wavelength_nm")),
                "module_id": _text(module.get("id")),
                "module_name": _text(optic_asset.get("name")) or _text(module.get("id")),
                "missing_module": False,
                "passive": False,
            }
        asset_type = _text(host_asset.get("asset_type")).lower()
        passive = asset_type in passive_types
        if port_type in cage_types and not passive:
            return {
                "tx": None, "rx": None, "loss": 0.0, "return_loss": 0.0,
                "wavelength_nm": 0, "module_id": "", "module_name": "",
                "missing_module": True, "passive": False,
            }
        # Integrated optical interfaces (for example PON) may define values on
        # the port row, falling back to the host asset for older projects.
        return {
            "tx": _optional_float(port.get("transmit_power_dbm", host_asset.get("optical_tx_power_dbm"))),
            "rx": _optional_float(port.get("receiver_sensitivity_dbm", host_asset.get("optical_receiver_sensitivity_dbm"))),
            "loss": max(0.0, _float(port.get("insertion_loss_db", host_asset.get("optical_insertion_loss_db")))),
            "return_loss": max(0.0, _float(port.get("return_loss_db", host_asset.get("optical_return_loss_db")))),
            "wavelength_nm": _int(port.get("wavelength_nm"), _int(host_asset.get("optical_wavelength_nm"))),
            "module_id": "", "module_name": _text(host_asset.get("name")),
            "missing_module": False, "passive": passive,
        }

    optical_paths: List[dict] = []
    seen: Set[Tuple[str, str, Tuple[str, ...], str]] = set()
    for source_id in sorted(instances):
        queue = deque([(source_id, [], [source_id], 0.0, [], [], None, "", 0.0, 0)])
        while queue and len(optical_paths) < 10000:
            current, connection_ids, visited_instances, passive_loss, passive_returns, missing_properties, source_tx, source_module_id, source_optic_loss, source_wavelength = queue.popleft()
            if len(connection_ids) > 32:
                continue
            for neighbour, cid in adjacency.get(current, []):
                if neighbour in visited_instances:
                    continue
                current_source_tx = source_tx
                current_source_module = source_module_id
                current_source_loss = source_optic_loss
                current_wavelength = source_wavelength
                next_missing = list(missing_properties)
                if current_source_tx is None:
                    source_values = optic_values(current, cid)
                    if source_values["missing_module"]:
                        continue
                    if source_values["tx"] is None:
                        continue
                    current_source_tx = source_values["tx"]
                    current_source_module = source_values["module_id"]
                    current_source_loss = source_values["loss"]
                    current_wavelength = source_values["wavelength_nm"]
                    if _text(current_source_tx) == "":
                        next_missing.append(f"{_text(instances.get(current, {}).get('name')) or current} transmit power")
                next_connection_ids = connection_ids + [cid]
                next_visited = visited_instances + [neighbour]
                neighbour_values = optic_values(neighbour, cid)
                neighbour_asset = assets.get(_text(instances.get(neighbour, {}).get("asset_id")), {})
                passive = bool(neighbour_values["passive"])
                if passive:
                    if _text(neighbour_asset.get("optical_insertion_loss_db")) == "" and neighbour_values["loss"] <= 0.0:
                        next_missing.append(f"{_text(instances.get(neighbour, {}).get('name')) or neighbour} insertion loss")
                    if _text(neighbour_asset.get("optical_return_loss_db")) == "" and neighbour_values["return_loss"] <= 0.0:
                        next_missing.append(f"{_text(instances.get(neighbour, {}).get('name')) or neighbour} return loss")
                next_passive_loss = passive_loss + (neighbour_values["loss"] if passive else 0.0)
                next_returns = passive_returns + ([neighbour_values["return_loss"]] if neighbour_values["return_loss"] else [])
                if neighbour != source_id and neighbour_values["rx"] is not None and not passive:
                    key = (source_id, neighbour, tuple(next_connection_ids), current_source_module)
                    if key in seen:
                        continue
                    seen.add(key)
                    cable_loss = sum(cable_loss_by_connection.get(value, 0.0) for value in next_connection_ids)
                    active_optic_loss = current_source_loss + neighbour_values["loss"]
                    total_loss = cable_loss + next_passive_loss + active_optic_loss
                    available = float(current_source_tx) - float(neighbour_values["rx"])
                    margin = available - total_loss
                    return_values = list(next_returns)
                    if neighbour_values["return_loss"]:
                        return_values.append(neighbour_values["return_loss"])
                    for value in next_connection_ids:
                        return_values.extend(return_loss_by_connection.get(value, []))
                    link_speeds = [max(0, _int(logical_connections.get(value, {}).get("link_speed_mbps"))) for value in next_connection_ids]
                    link_speeds = [value for value in link_speeds if value > 0]
                    optical_paths.append({
                        "id": f"OP{len(optical_paths) + 1}", "source_instance_id": source_id, "destination_instance_id": neighbour,
                        "source_optic_module_id": current_source_module, "destination_optic_module_id": neighbour_values["module_id"],
                        "connection_ids": next_connection_ids,
                        "fibre_cable_ids": sorted({cable_id for value in next_connection_ids for cable_id in cable_ids_by_connection.get(value, []) if cable_id}),
                        "link_speed_mbps": min(link_speeds) if link_speeds else 0,
                        "wavelength_nm": current_wavelength or neighbour_values["wavelength_nm"],
                        "transmit_power_dbm": round(float(current_source_tx), 3), "receiver_sensitivity_dbm": round(float(neighbour_values["rx"]), 3),
                        "cable_loss_db": round(cable_loss, 6), "passive_loss_db": round(next_passive_loss, 6),
                        "active_optic_loss_db": round(active_optic_loss, 6), "path_loss_db": round(total_loss, 6),
                        "available_budget_db": round(available, 6), "margin_db": "" if next_missing else round(margin, 6),
                        "minimum_return_loss_db": round(min(return_values), 3) if return_values else 0.0,
                        "status": "unconfigured" if next_missing else ("pass" if margin >= 0.0 else "fail"),
                        "missing_properties": ", ".join(dict.fromkeys(next_missing)),
                        "notes": "Optical budget cannot be verified until all optic/passive values are configured." if next_missing else "Calculated from installed transceiver transmit/receive values, active optic insertion loss and designated passive fibre losses.",
                    })
                    continue
                queue.append((neighbour, next_connection_ids, next_visited, next_passive_loss, next_returns, next_missing, current_source_tx, current_source_module, current_source_loss, current_wavelength))

    covered_connection_ids = {_text(cid) for path in optical_paths for cid in path.get("connection_ids", []) if _text(cid)}
    for cid, connection in sorted(logical_connections.items()):
        if cid in covered_connection_ids:
            continue
        source_id = _text(connection.get("from_instance_id")); destination_id = _text(connection.get("to_instance_id"))
        source = optic_values(source_id, cid); destination = optic_values(destination_id, cid)
        missing: List[str] = []
        for values, instance_id, side in ((source, source_id, "source"), (destination, destination_id, "destination")):
            if values["passive"]:
                if values["loss"] <= 0.0: missing.append(f"{side} passive insertion loss")
                if values["return_loss"] <= 0.0: missing.append(f"{side} passive return loss")
            else:
                if values["missing_module"]: missing.append(f"{side} pluggable optic module")
                if values["tx"] is None: missing.append(f"{side} transmit power")
                if values["rx"] is None: missing.append(f"{side} receiver sensitivity")
        cable_loss = cable_loss_by_connection.get(cid, 0.0); return_values = return_loss_by_connection.get(cid, [])
        optical_paths.append({
            "id": f"OP{len(optical_paths) + 1}", "source_instance_id": source_id, "destination_instance_id": destination_id,
            "source_optic_module_id": source["module_id"], "destination_optic_module_id": destination["module_id"],
            "connection_ids": [cid], "fibre_cable_ids": sorted({value for value in cable_ids_by_connection.get(cid, []) if value}),
            "link_speed_mbps": max(0, _int(connection.get("link_speed_mbps"))),
            "transmit_power_dbm": "", "receiver_sensitivity_dbm": "", "cable_loss_db": round(cable_loss, 6),
            "passive_loss_db": 0.0, "active_optic_loss_db": 0.0, "path_loss_db": round(cable_loss, 6),
            "available_budget_db": "", "margin_db": "", "minimum_return_loss_db": round(min(return_values), 3) if return_values else 0.0,
            "status": "unconfigured", "missing_properties": ", ".join(dict.fromkeys(missing)) or "complete active optical path",
            "notes": "Optical budget cannot be verified until the listed optic/passive properties are configured.",
        })

    data["network_optical_paths"] = optical_paths
    margins_by_connection: Dict[str, List[float]] = defaultdict(list)
    for path in optical_paths:
        if _text(path.get("status")).lower() not in {"pass", "fail"} or _text(path.get("margin_db")) == "":
            continue
        for cid in path.get("connection_ids", []):
            margins_by_connection[_text(cid)].append(_float(path.get("margin_db")))
    for cid, connection in logical_connections.items():
        margins = margins_by_connection.get(cid, [])
        connection["optical_budget_status"] = "unconfigured" if not margins else ("pass" if min(margins) >= 0.0 else "fail")
        connection["optical_minimum_margin_db"] = round(min(margins), 6) if margins else ""
    return optical_paths


def circuit_trace(data: dict, connection_id: str) -> dict:
    """Return all logical and physical records participating in a circuit."""
    requested = _text(connection_id)
    logical_ids: Set[str] = set()
    instance_ids: Set[str] = set()
    patch_lead_ids: Set[str] = set()
    fibre_cable_ids: Set[str] = set()
    splice_ids: Set[str] = set()
    protection_groups: Set[str] = set()

    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict):
            continue
        cid = _text(connection.get("id"))
        if cid == requested or _text(connection.get("parent_logical_connection_id")) == requested:
            logical_ids.add(cid)
            pg = _text(connection.get("protection_group"))
            if pg:
                protection_groups.add(pg)
    if protection_groups:
        for connection in data.get("network_connections", []):
            if _text(connection.get("protection_group")) in protection_groups:
                logical_ids.add(_text(connection.get("id")))

    changed = True
    while changed:
        changed = False
        for connection in data.get("network_connections", []):
            cid = _text(connection.get("id"))
            parent = _text(connection.get("parent_logical_connection_id"))
            if cid in logical_ids or parent in logical_ids:
                before = len(logical_ids)
                logical_ids.add(cid)
                if parent:
                    logical_ids.add(parent)
                changed = changed or len(logical_ids) != before

    for connection in data.get("network_connections", []):
        if _text(connection.get("id")) in logical_ids:
            instance_ids.add(_text(connection.get("from_instance_id")))
            instance_ids.add(_text(connection.get("to_instance_id")))
    for lead in data.get("network_patch_leads", []):
        if _text(lead.get("connection_id")) in logical_ids:
            patch_lead_ids.add(_text(lead.get("id")))
            instance_ids.add(_text(lead.get("instance_id")))
            instance_ids.add(_text(lead.get("peer_instance_id")))
    for cable in data.get("network_fibre_cables", []):
        cable_logical = {_text(value) for value in cable.get("logical_connection_ids", [])}
        if cable_logical & logical_ids:
            fibre_cable_ids.add(_text(cable.get("id")))
            splice_ids.update(_text(value) for value in cable.get("splice_ids", []) if _text(value))
    for splice in data.get("network_fibre_splices", []):
        sid = _text(splice.get("id"))
        if sid in splice_ids or _text(splice.get("circuit_id")) in logical_ids:
            splice_ids.add(sid)
            fibre_cable_ids.add(_text(splice.get("incoming_cable_id")))
            fibre_cable_ids.add(_text(splice.get("outgoing_cable_id")))
    return {
        "connection_ids": sorted(value for value in logical_ids if value),
        "instance_ids": sorted(value for value in instance_ids if value),
        "patch_lead_ids": sorted(value for value in patch_lead_ids if value),
        "fibre_cable_ids": sorted(value for value in fibre_cable_ids if value),
        "splice_ids": sorted(value for value in splice_ids if value),
    }


def _prefix_for_hosts(host_count: int) -> int:
    needed = max(4, int(host_count) + 2)
    bits = int(math.ceil(math.log2(needed)))
    return max(1, min(30, 32 - bits))


def _next_aligned_network(start: int, prefix: int) -> ipaddress.IPv4Network:
    size = 1 << (32 - prefix)
    aligned = ((int(start) + size - 1) // size) * size
    return ipaddress.ip_network((aligned, prefix))


def generate_ip_address_plan(data: dict, base_cidr: Optional[str] = None) -> dict:
    """Generate deterministic VLAN subnets, gateways, router interfaces and management IPs."""
    settings = data.setdefault("network_settings", {})
    cidr = _text(base_cidr) or _text(settings.get("ip_plan_base_cidr")) or "10.0.0.0/8"
    base = ipaddress.ip_network(cidr, strict=False)
    if not isinstance(base, ipaddress.IPv4Network):
        raise ValueError("Only IPv4 automatic address generation is currently supported.")
    settings["ip_plan_base_cidr"] = str(base)

    vlan_store = data.setdefault("network_vlans", [])
    vlans = [item for item in vlan_store if isinstance(item, dict)]
    if not vlans:
        endpoint_count = max(2, len(data.get("network_endpoint_assignments", [])))
        instance_count = max(2, len(data.get("network_asset_instances", [])))
        templates = [
            {"id": "VLAN10", "vlan_id": 10, "name": "Management", "purpose": "Network management", "requested_hosts": max(64, instance_count + 24), "security_zone": "Management", "notes": "Automatically generated management VLAN."},
            {"id": "VLAN20", "vlan_id": 20, "name": "Data", "purpose": "User and endpoint data", "requested_hosts": max(254, endpoint_count + 64), "security_zone": "Internal", "notes": "Automatically generated data VLAN."},
        ]
        asset_names = " ".join(
            _text(item.get("endpoint_asset_name")).lower()
            for item in data.get("network_endpoint_assignments", [])
            if isinstance(item, dict)
        )
        if any(token in asset_names for token in ("phone", "telephone", "voice")):
            templates.append({"id": "VLAN30", "vlan_id": 30, "name": "Voice", "purpose": "IP telephony", "requested_hosts": max(64, endpoint_count // 4 + 32), "security_zone": "Voice", "notes": "Automatically generated voice VLAN."})
        assets = {_text(a.get("id")): a for a in data.get("network_assets", []) if isinstance(a, dict)}
        if any(_text(assets.get(_text(i.get("asset_id")), {}).get("asset_type")) in {"wireless_access_point", "wireless_device"} for i in data.get("network_asset_instances", []) if isinstance(i, dict)):
            templates.append({"id": "VLAN40", "vlan_id": 40, "name": "Wireless", "purpose": "Managed wireless clients", "requested_hosts": max(254, endpoint_count), "security_zone": "Wireless", "notes": "Automatically generated wireless VLAN."})
        if any(token in asset_names for token in ("sensor", "iot", "bms", "meter", "cctv", "access control")):
            templates.append({"id": "VLAN50", "vlan_id": 50, "name": "IoT", "purpose": "IoT and building systems", "requested_hosts": max(126, endpoint_count // 3 + 32), "security_zone": "IoT", "notes": "Automatically generated IoT VLAN."})
        vlan_store.extend(templates)
        vlans = templates
    vlans.sort(key=lambda item: (_int(item.get("vlan_id")), _text(item.get("id"))))
    cursor = int(base.network_address)
    allocated: List[ipaddress.IPv4Network] = []
    for vlan in vlans:
        requested = max(2, _int(vlan.get("requested_hosts"), 254))
        existing = _text(vlan.get("subnet"))
        network = None
        if existing:
            try:
                candidate = ipaddress.ip_network(existing, strict=False)
                if isinstance(candidate, ipaddress.IPv4Network) and candidate.subnet_of(base):
                    network = candidate
            except ValueError:
                network = None
        if network is None:
            prefix = _prefix_for_hosts(requested)
            network = _next_aligned_network(cursor, prefix)
            while not network.subnet_of(base) or any(network.overlaps(other) for other in allocated):
                cursor = int(network.broadcast_address) + 1
                network = _next_aligned_network(cursor, prefix)
            vlan["subnet"] = str(network)
        allocated.append(network)
        cursor = max(cursor, int(network.broadcast_address) + 1)
        hosts = list(network.hosts())
        vlan["subnet_mask"] = str(network.netmask)
        vlan["prefix_length"] = network.prefixlen
        vlan["gateway"] = str(hosts[0]) if hosts else ""
        if len(hosts) > 3:
            dhcp_start_index = min(9, len(hosts) - 2)
            vlan["dhcp_start"] = str(hosts[dhcp_start_index])
            vlan["dhcp_end"] = str(hosts[-2])
            vlan["dhcp_scope"] = f"{vlan['dhcp_start']} - {vlan['dhcp_end']}"
        else:
            vlan["dhcp_start"] = ""
            vlan["dhcp_end"] = ""
            vlan["dhcp_scope"] = ""

    assets = {_text(a.get("id")): a for a in data.get("network_assets", []) if isinstance(a, dict)}
    instances = [item for item in data.get("network_asset_instances", []) if isinstance(item, dict)]
    routers = [
        item for item in instances
        if _text(assets.get(_text(item.get("asset_id")), {}).get("asset_type")) in {"network_router", "firewall", "telco_pop", "external_network"}
        or any(word in _text(item.get("design_role")).lower() for word in ("router", "gateway", "firewall", "core"))
    ]
    router = routers[0] if routers else (instances[0] if instances else None)
    if router is not None:
        router_addresses = []
        for vlan in vlans:
            if _text(vlan.get("gateway")):
                router_addresses.append(
                    {
                        "vlan_id": _text(vlan.get("id")),
                        "vlan_number": _int(vlan.get("vlan_id")),
                        "interface": f"VLAN{_int(vlan.get('vlan_id'))}",
                        "ip_address": f"{vlan['gateway']}/{_int(vlan.get('prefix_length'))}",
                    }
                )
        router["router_ip_addresses"] = router_addresses

    routes = [row for row in data.setdefault("network_routes", []) if not bool(row.get("auto_generated", False))]
    gateway_allocations: List[dict] = []
    if router is not None:
        for vlan in vlans:
            route_id = next_record_id(routes, "NR")
            routes.append({
                "id": route_id,
                "source": _text(router.get("id")),
                "destination": _text(vlan.get("subnet")),
                "vlan_id": _text(vlan.get("id")),
                "protocol": "connected",
                "next_hop": "",
                "metric": 0,
                "firewall_policy": "",
                "notes": "Automatically generated connected VLAN route.",
                "auto_generated": True,
            })
            if _text(vlan.get("gateway")):
                gateway_allocations.append({
                    "id": next_record_id(gateway_allocations, "IPG"),
                    "instance_id": _text(router.get("id")),
                    "vlan_id": _text(vlan.get("id")),
                    "address": _text(vlan.get("gateway")),
                    "prefix_length": _int(vlan.get("prefix_length")),
                    "gateway": "",
                    "purpose": "gateway",
                    "auto_generated": True,
                })
        external = next((row for row in data.get("network_external_networks", []) if isinstance(row, dict)), None)
        if external is not None:
            routes.append({
                "id": next_record_id(routes, "NR"),
                "source": _text(router.get("id")),
                "destination": "0.0.0.0/0",
                "vlan_id": "",
                "protocol": "static",
                "next_hop": _text(external.get("demarcation_instance_id")) or _text(external.get("name")),
                "metric": 1,
                "firewall_policy": "External peering",
                "notes": f"Automatically generated default route to {_text(external.get('name'))}.",
                "auto_generated": True,
            })
    data["network_routes"] = routes

    management_vlan = next(
        (v for v in vlans if "management" in (_text(v.get("purpose")) + " " + _text(v.get("name"))).lower()),
        vlans[0] if vlans else None,
    )
    allocations: List[dict] = list(gateway_allocations)
    if management_vlan and _text(management_vlan.get("subnet")):
        network = ipaddress.ip_network(management_vlan["subnet"], strict=False)
        hosts = list(network.hosts())
        address_index = 19 if len(hosts) > 25 else 1
        ordered = sorted(instances, key=lambda item: (_text(item.get("design_role")), _text(item.get("id"))))
        for instance in ordered:
            while address_index < len(hosts) and str(hosts[address_index]) == _text(management_vlan.get("gateway")):
                address_index += 1
            if address_index >= len(hosts):
                break
            ip_text = str(hosts[address_index])
            instance["management_ip"] = ip_text
            instance["management_vlan"] = _text(management_vlan.get("id"))
            allocations.append(
                {
                    "id": next_record_id(allocations, "IPA"),
                    "instance_id": _text(instance.get("id")),
                    "vlan_id": _text(management_vlan.get("id")),
                    "address": ip_text,
                    "prefix_length": network.prefixlen,
                    "gateway": _text(management_vlan.get("gateway")),
                    "purpose": "management",
                    "auto_generated": True,
                }
            )
            address_index += 1
    data["network_ip_allocations"] = allocations
    return {
        "base_cidr": str(base),
        "vlan_count": len(vlans),
        "management_allocations": len([row for row in allocations if _text(row.get("purpose")) == "management"]),
        "gateway_allocations": len(gateway_allocations),
        "route_count": len(routes),
        "router_instance_id": _text(router.get("id")) if router else "",
    }


def fibre_layer_defaults() -> dict:
    """Return drawing defaults for the independent physical-fibre overlay."""
    return {
        "name": "Physical Fibre",
        "visible": True,
        "show_base_graph": True,
        "show_route_nodes": False,
        "show_core_counts": True,
        "show_dark_fibre": True,
        "show_splice_labels": True,
        "dxf_layer_prefix": "NET-FIBRE",
        "cable_layer": "NET-FIBRE-CABLE",
        "node_layer": "NET-FIBRE-NODE",
        "splice_layer": "NET-FIBRE-SPLICE",
        "label_layer": "NET-FIBRE-TEXT",
        # Physical coordinates are in the project/DXF world coordinate system.
        # Keep symbols and labels deliberately small so they overlay a floor
        # plan without obscuring rooms, routes or nearby fibre records.
        "symbol_scale": 0.075,
        "label_scale": 0.09,
        "cable_width_scale": 0.18,
    }


def cable_core_statistics(cable: dict) -> dict:
    """Summarise allocated, spliced, reserved and dark fibres for one cable."""
    cores = [row for row in cable.get("cores", []) if isinstance(row, dict)]
    counts = {"allocated": 0, "spliced": 0, "reserved": 0, "fault": 0, "dark": 0}
    for core in cores:
        status = _text(core.get("status")).lower() or "dark"
        if status not in counts:
            status = "allocated" if _text(core.get("circuit_id")) else "dark"
        counts[status] += 1
    declared = max(0, _int(cable.get("core_count")))
    represented = sum(counts.values())
    if declared > represented:
        counts["dark"] += declared - represented
    counts["used"] = counts["allocated"] + counts["spliced"] + counts["reserved"]
    counts["available"] = counts["dark"]
    counts["total"] = declared
    return counts


def _record_location(data: dict, instance_id: str) -> tuple[str, int, float, float]:
    instances = {
        _text(item.get("id")): item
        for item in data.get("network_asset_instances", [])
        if isinstance(item, dict)
    }
    locations = {
        _text(item.get("name")): item
        for item in data.get("locations", [])
        if isinstance(item, dict)
    }
    instance = instances.get(_text(instance_id), {})
    location_name = _text(instance.get("location_name"))
    location = locations.get(location_name, {})
    return (
        location_name,
        _int(instance.get("floor", location.get("floor", 0))),
        _float(instance.get("x", location.get("x", 0.0))),
        _float(instance.get("y", location.get("y", 0.0))),
    )


def sync_fibre_nodes_from_design(data: dict, replace_auto: bool = False) -> List[dict]:
    """Create physical terminations and cassette records for generated fibre ends.

    Modular patch-panel instances create one physical cassette node per fitted
    cassette position.  Cassette records retain front connector usage and
    splice/MPO/MTP rear-interface configuration so physical schedules do not
    collapse a four-cassette chassis into one oversized generic tray.
    """
    nodes = data.setdefault("network_fibre_nodes", [])
    if replace_auto:
        nodes[:] = [item for item in nodes if not bool(item.get("auto_generated"))]
    assets = {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if isinstance(item, dict)
    }
    instances = {
        _text(item.get("id")): item
        for item in data.get("network_asset_instances", [])
        if isinstance(item, dict)
    }
    existing_by_instance_type = {
        (_text(item.get("linked_instance_id")), _text(item.get("node_type"))): item
        for item in nodes
        if isinstance(item, dict) and _text(item.get("linked_instance_id"))
    }
    existing_cassettes = {
        (
            _text(item.get("linked_instance_id")),
            max(1, _int(item.get("cassette_position", item.get("tray_number")), 1)),
        ): item
        for item in nodes
        if isinstance(item, dict)
        and _text(item.get("linked_instance_id"))
        and _text(item.get("node_type")) == "splice_cassette"
    }
    created: List[dict] = []
    endpoint_ids: Set[str] = set()
    physical_cable_ids_by_logical: Dict[str, List[str]] = defaultdict(list)
    for cable in data.get("network_fibre_cables", []):
        if not isinstance(cable, dict):
            continue
        endpoint_ids.add(_text(cable.get("from_instance_id")))
        endpoint_ids.add(_text(cable.get("to_instance_id")))
        cable_id = _text(cable.get("id"))
        if cable_id:
            for logical_id in cable.get("logical_connection_ids", []):
                logical_id = _text(logical_id)
                if logical_id and cable_id not in physical_cable_ids_by_logical[logical_id]:
                    physical_cable_ids_by_logical[logical_id].append(cable_id)

    # A single high-core-count trunk can fan out across several modular
    # panel chassis.  Only the first panel is the cable endpoint record,
    # while the detailed allocation metadata names every additional panel.
    # Include all fitted modular panels so each chassis and cassette remains
    # visible in the physical-fibre schedule and editor.
    for instance_id, instance in instances.items():
        asset = assets.get(_text(instance.get("asset_id")), {})
        if (
            _text(asset.get("asset_type")).lower() == "patch_panel"
            and _text(asset.get("patch_panel_type")).lower() == "fibre"
            and (
                bool(asset.get("modular_patch_panel"))
                or isinstance(instance.get("fibre_cassettes"), list)
            )
        ):
            endpoint_ids.add(instance_id)
    for instance_id in sorted(value for value in endpoint_ids if value):
        instance = instances.get(instance_id)
        if not instance:
            continue
        asset = assets.get(_text(instance.get("asset_id")), {})
        asset_type = _text(asset.get("asset_type")).lower()
        is_panel = asset_type == "patch_panel" and _text(asset.get("patch_panel_type")).lower() == "fibre"
        is_modular_panel = is_panel and bool(asset.get("modular_patch_panel"))
        location_name, floor, x, y = _record_location(data, instance_id)
        termination = existing_by_instance_type.get((instance_id, "termination"))
        if termination is None:
            termination = {
                "id": next_record_id(nodes, "FTN"),
                "name": f"{_text(instance.get('name')) or instance_id} fibre termination",
                "node_type": "termination",
                "linked_instance_id": instance_id,
                "location_name": location_name,
                "floor": floor,
                "x": x,
                "y": y,
                "rack_name": _text(instance.get("rack_name")),
                "rack_start_u": _int(instance.get("rack_start_u")),
                "rack_units": 1,
                "parent_node_id": "",
                "cassette_capacity": 0,
                "drawing_layer": "NET-FIBRE-NODE",
                "notes": "Generated physical fibre termination linked to the logical design.",
                "auto_generated": True,
            }
            nodes.append(termination)
            existing_by_instance_type[(instance_id, "termination")] = termination
            created.append(termination)

        if not is_panel:
            continue

        cassette_rows = instance.get("fibre_cassettes", [])
        if not isinstance(cassette_rows, list) or not cassette_rows:
            cassette_total = (
                max(1, min(4, _int(asset.get("patch_panel_cassette_count"), 4)))
                if is_modular_panel
                else 1
            )
            cassette_rows = [
                {
                    "position": position,
                    "front_connector": _text(asset.get("patch_panel_cassette_front_connector")) or "lc_duplex",
                    "front_connector_capacity": 12 if is_modular_panel else max(12, _int(asset.get("number_of_ports"), 12)),
                    "termination_mode": _text(asset.get("patch_panel_cassette_termination_mode")) or "spliced",
                    "rear_connector": _text(asset.get("patch_panel_cassette_rear_connector")) or "splice",
                    "rear_connector_count": _int(asset.get("patch_panel_cassette_rear_connector_count"), 0),
                    "used_front_connectors": 0,
                    "used_fibres": 0,
                    "cable_ids": [],
                }
                for position in range(1, cassette_total + 1)
            ]

        for row_index, cassette_config in enumerate(cassette_rows, start=1):
            if not isinstance(cassette_config, dict):
                continue
            position = max(1, _int(cassette_config.get("position"), row_index))
            cassette = existing_cassettes.get((instance_id, position))
            if cassette is None:
                cassette = {
                    "id": next_record_id(nodes, "FSC"),
                    "node_type": "splice_cassette",
                    "linked_instance_id": instance_id,
                    "location_name": location_name,
                    "floor": floor,
                    "x": x,
                    "y": y,
                    "rack_name": _text(instance.get("rack_name")),
                    "rack_start_u": _int(instance.get("rack_start_u")),
                    "rack_units": 1,
                    "parent_node_id": _text(termination.get("id")),
                    "drawing_layer": "NET-FIBRE-SPLICE",
                    "auto_generated": True,
                }
                nodes.append(cassette)
                existing_cassettes[(instance_id, position)] = cassette
                created.append(cassette)
            front_capacity = max(1, _int(cassette_config.get("front_connector_capacity"), 12))
            front_connector = _text(cassette_config.get("front_connector")) or "lc_duplex"
            fibres_per_position = 1 if front_connector == "sc_simplex" else 2
            mode = _text(cassette_config.get("termination_mode")) or "spliced"
            rear = _text(cassette_config.get("rear_connector")) or ("splice" if mode == "spliced" else "mpo-24")
            logical_circuit_ids = [
                _text(value)
                for value in cassette_config.get("cable_ids", [])
                if _text(value)
            ] if isinstance(cassette_config.get("cable_ids", []), list) else []
            associated_physical_cable_ids: List[str] = []
            for logical_id in logical_circuit_ids:
                for cable_id in physical_cable_ids_by_logical.get(logical_id, []):
                    if cable_id not in associated_physical_cable_ids:
                        associated_physical_cable_ids.append(cable_id)
            cassette.update({
                "name": f"{_text(instance.get('name')) or instance_id} cassette {position}",
                "cassette_position": position,
                "tray_number": position,
                "front_connector": front_connector,
                "front_connector_capacity": front_capacity,
                "cassette_capacity": front_capacity,
                "splice_capacity": front_capacity * fibres_per_position,
                "termination_mode": mode,
                "rear_connector": rear,
                "rear_connector_count": max(0, _int(cassette_config.get("rear_connector_count"))),
                "used_front_connectors": max(0, _int(cassette_config.get("used_front_connectors"))),
                "used_front_connector_names": list(cassette_config.get("used_front_connector_names", [])) if isinstance(cassette_config.get("used_front_connector_names", []), list) else [],
                "used_fibres": max(0, _int(cassette_config.get("used_fibres"))),
                "incoming_cable_id": (
                    associated_physical_cable_ids[0]
                    if associated_physical_cable_ids else ""
                ),
                "cable_ids": associated_physical_cable_ids,
                "circuit_ids": logical_circuit_ids,
                "notes": (
                    f"Modular cassette {position}: {front_connector}, {mode}; "
                    f"rear interface {rear} x{max(0, _int(cassette_config.get('rear_connector_count')))}."
                ),
            })
    return created


def ensure_physical_fibre_for_design(data: dict, replace_auto: bool = False) -> dict:
    """Synchronise logical fibre links into the separate physical-fibre layer."""
    created_cables = sync_fibre_cables_from_connections(data, replace_auto=replace_auto)
    # The cable planner creates branch enclosures and cassette trays.  It has
    # already cleared stale auto-generated nodes when requested, so the endpoint
    # termination pass must preserve the newly created branch plant.
    created_nodes = sync_fibre_nodes_from_design(data, replace_auto=False)
    optical_paths = calculate_optical_budgets(data)
    return {
        "created_cables": len(created_cables),
        "created_nodes": len(created_nodes),
        "cable_count": len(data.get("network_fibre_cables", [])),
        "node_count": len(data.get("network_fibre_nodes", [])),
        "optical_path_count": len(optical_paths),
        "optical_path_failures": len([row for row in optical_paths if _text(row.get("status")) == "fail"]),
    }


def cable_route_points(data: dict, cable: dict) -> List[dict]:
    """Resolve a physical cable's graph path into floor-aware coordinates."""
    points: Dict[str, dict] = {}
    for item in data.get("locations", []):
        if isinstance(item, dict) and _text(item.get("name")):
            points[_text(item.get("name"))] = item
    for item in data.get("data_points", []):
        if isinstance(item, dict) and _text(item.get("name")):
            points[_text(item.get("name"))] = item
    for item in data.get("corridors", {}).get("nodes", []):
        if isinstance(item, dict) and _text(item.get("name")):
            points[_text(item.get("name"))] = item
    for transition in data.get("transitions", []):
        if not isinstance(transition, dict):
            continue
        transition_id = _text(transition.get("id"))
        for floor_text, coordinates in (transition.get("floor_locations") or {}).items():
            if not isinstance(coordinates, dict):
                continue
            row = dict(coordinates)
            row["floor"] = _int(floor_text)
            row["name"] = f"{transition_id}-F{floor_text}"
            points[row["name"]] = row
    route = []
    for name in cable.get("route_path", []):
        point = points.get(_text(name))
        if point:
            route.append(
                {
                    "name": _text(name),
                    "floor": _int(point.get("floor")),
                    "x": _float(point.get("x")),
                    "y": _float(point.get("y")),
                }
            )
    if route:
        return route
    # Fallback to active-device positions for manually created cables.
    for instance_key in ("from_instance_id", "to_instance_id"):
        location_name, floor, x, y = _record_location(data, _text(cable.get(instance_key)))
        route.append({"name": location_name or _text(cable.get(instance_key)), "floor": floor, "x": x, "y": y})
    return route


def splice_arrangement_rows(data: dict, node_id: str = "") -> List[dict]:
    """Return a denormalised splice matrix for drawing and reporting."""
    cables = {
        _text(item.get("id")): item
        for item in data.get("network_fibre_cables", [])
        if isinstance(item, dict)
    }
    nodes = {
        _text(item.get("id")): item
        for item in data.get("network_fibre_nodes", [])
        if isinstance(item, dict)
    }
    rows: List[dict] = []
    for splice in data.get("network_fibre_splices", []):
        if not isinstance(splice, dict):
            continue
        if node_id and _text(splice.get("node_id")) != _text(node_id):
            continue
        incoming = cables.get(_text(splice.get("incoming_cable_id")), {})
        outgoing = cables.get(_text(splice.get("outgoing_cable_id")), {})
        in_core_no = _int(splice.get("incoming_core"), 1)
        out_core_no = _int(splice.get("outgoing_core"), 1)
        in_core = next((c for c in incoming.get("cores", []) if _int(c.get("number")) == in_core_no), {})
        out_core = next((c for c in outgoing.get("cores", []) if _int(c.get("number")) == out_core_no), {})
        node = nodes.get(_text(splice.get("node_id")), {})
        rows.append(
            {
                "splice_id": _text(splice.get("id")),
                "node_id": _text(splice.get("node_id")),
                "node_name": _text(node.get("name")),
                "node_type": _text(node.get("node_type")),
                "cassette_id": _text(splice.get("cassette_id")),
                "incoming_cable_id": _text(splice.get("incoming_cable_id")),
                "incoming_core": in_core_no,
                "incoming_colour": _text(in_core.get("colour")),
                "incoming_tube": _int(in_core.get("tube_number")),
                "incoming_tube_colour": _text(in_core.get("tube_colour")),
                "outgoing_cable_id": _text(splice.get("outgoing_cable_id")),
                "outgoing_core": out_core_no,
                "outgoing_colour": _text(out_core.get("colour")),
                "outgoing_tube": _int(out_core.get("tube_number")),
                "outgoing_tube_colour": _text(out_core.get("tube_colour")),
                "splice_type": _text(splice.get("splice_type")) or "fusion",
                "circuit_id": _text(splice.get("circuit_id")),
                "loss_db": _float(splice.get("loss_db"), 0.1),
                "notes": _text(splice.get("notes")),
            }
        )
    return rows


def set_core_status_from_splices(data: dict) -> None:
    """Mark cable cores participating in splice records as spliced/allocated."""
    cables = {
        _text(item.get("id")): item
        for item in data.get("network_fibre_cables", [])
        if isinstance(item, dict)
    }
    for splice in data.get("network_fibre_splices", []):
        if not isinstance(splice, dict):
            continue
        circuit_id = _text(splice.get("circuit_id"))
        for cable_key, core_key in (("incoming_cable_id", "incoming_core"), ("outgoing_cable_id", "outgoing_core")):
            cable = cables.get(_text(splice.get(cable_key)))
            if not cable:
                continue
            number = _int(splice.get(core_key), 1)
            core = next((item for item in cable.get("cores", []) if _int(item.get("number")) == number), None)
            if core is not None:
                core["status"] = "spliced"
                if circuit_id:
                    core["circuit_id"] = circuit_id


def network_traffic_loads(data: dict) -> dict:
    """Calculate protected traffic carried by every logical network tier.

    Traffic is propagated from each endpoint-serving device towards every
    valid upstream device. A demand is counted once at a reconvergent device,
    but it is applied to every independent protected uplink that can carry it.
    Peer links at the same logical tier are excluded from demand propagation.

    Network-asset records describe device capability, not traffic generated by
    every installed copy. Demand therefore originates from endpoint
    assignments, with an instance-level fallback only for genuine leaf devices
    that have no assignment records.
    """

    assets = {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    instances = {
        _text(item.get("id")): item
        for item in data.get("network_asset_instances", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }

    def tier(instance_id: str) -> int:
        instance = instances.get(instance_id, {})
        asset = assets.get(_text(instance.get("asset_id")), {})
        asset_type = _text(asset.get("asset_type")).lower()
        role = _text(instance.get("design_role") or instance.get("network_layer")).lower()
        if asset_type in {"telco_pop", "external_network"} or any(
            token in role for token in ("external", "carrier", "internet", "peering", "telco")
        ):
            return 0
        if asset_type in {"network_router", "firewall"} or any(
            token in role for token in ("gateway", "router", "firewall", "edge")
        ):
            return 1
        if "core" in role:
            return 2
        if "aggregation" in role or "distribution" in role:
            return 3
        if asset_type == "optical_line_terminal" or role.startswith("olt_"):
            return 4
        if asset_type in {"network_switch", "fibre_splitter", "patch_panel"} or any(
            token in role for token in ("access", "splitter")
        ):
            return 5
        if asset_type in {"optical_network_terminal", "wireless_access_point", "wireless_device"} or role == "ont":
            return 6
        return 7

    direct: Dict[str, Dict[str, float]] = {
        instance_id: {"bandwidth_mbps": 0.0, "packet_rate_pps": 0.0}
        for instance_id in instances
    }
    assignment_counts: Dict[str, int] = defaultdict(int)
    total_endpoint_bandwidth = 0.0
    total_endpoint_packets = 0.0

    for assignment in data.get("network_endpoint_assignments", []):
        if not isinstance(assignment, dict):
            continue
        instance_id = _text(assignment.get("network_instance_id"))
        bandwidth = max(0.0, _float(assignment.get("expected_bandwidth_mbps")))
        packets = max(0.0, _float(assignment.get("expected_packet_rate_pps")))
        total_endpoint_bandwidth += bandwidth
        total_endpoint_packets += packets
        if instance_id not in direct:
            continue
        direct[instance_id]["bandwidth_mbps"] += bandwidth
        direct[instance_id]["packet_rate_pps"] += packets
        assignment_counts[instance_id] += 1

    # downstream -> logical upstream routes.  A same-speed LAG is one route
    # with several physical/logical member connections, not several protected
    # paths carrying the full demand independently.
    upstream_by_instance: Dict[str, List[dict]] = defaultdict(list)
    downstream_count: Dict[str, int] = defaultdict(int)
    route_groups: Dict[str, dict] = {}

    def orient_connection(source: str, target: str, row: dict) -> Optional[Tuple[str, str]]:
        source_tier = tier(source)
        target_tier = tier(target)
        if source_tier < target_tier:
            return source, target
        if target_tier < source_tier:
            return target, source

        # Equal-tier links are normally peer/stack/inter-core links and do not
        # represent another downstream demand path. Honour explicit layer
        # metadata only when it identifies a genuine upstream/downstream pair.
        source_layer = _text(row.get("source_layer")).lower()
        target_layer = _text(row.get("target_layer")).lower()
        layer_rank = {
            "external": 0,
            "edge": 1,
            "core": 2,
            "aggregation": 3,
            "distribution": 3,
            "olt": 4,
            "access": 5,
            "splitter": 5,
            "ont": 6,
            "endpoint": 7,
        }
        if source_layer in layer_rank and target_layer in layer_rank:
            if layer_rank[source_layer] < layer_rank[target_layer]:
                # Planner layer records store the downstream target at
                # from_instance and the upstream source at to_instance.
                return target, source
            if layer_rank[target_layer] < layer_rank[source_layer]:
                return source, target
        return None

    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict):
            continue
        if (
            bool(connection.get("topology_hidden"))
            or bool(connection.get("physical_connection"))
            or bool(connection.get("endpoint_assignment_connection"))
        ):
            # Planner-created wireless endpoint links are a topology/rendering
            # representation of an endpoint assignment.  The assignment already
            # originates the demand at the serving switch/ONT, so propagating the
            # same demand through this visible edge would count it twice.
            continue
        source = _text(connection.get("from_instance_id"))
        target = _text(connection.get("to_instance_id"))
        connection_id = _text(connection.get("id"))
        if source not in instances or target not in instances or source == target:
            continue
        oriented = orient_connection(source, target, connection)
        if oriented is None:
            continue
        upstream, downstream = oriented
        lag_id = _text(connection.get("link_aggregation_group_id"))
        route_key = (
            f"lag::{lag_id}::{upstream}::{downstream}"
            if lag_id
            else f"connection::{connection_id}"
        )
        route = route_groups.setdefault(
            route_key,
            {
                "route_id": route_key,
                "upstream": upstream,
                "downstream": downstream,
                "lag_id": lag_id,
                "members": [],
            },
        )
        if connection_id and all(
            _text(member.get("id")) != connection_id
            for member in route["members"]
        ):
            route["members"].append(connection)

    for route in route_groups.values():
        upstream_by_instance[route["downstream"]].append(route)
        downstream_count[route["upstream"]] += 1

    # A manually installed leaf device may have an expected load stored on the
    # instance rather than endpoint assignments. Use that value only when the
    # device has no logical children, avoiding double-counting planner-derived
    # aggregate values on OLT/core instances.
    for instance_id, instance in instances.items():
        asset = assets.get(_text(instance.get("asset_id")), {})
        asset_type = _text(asset.get("asset_type")).lower()
        if (
            assignment_counts.get(instance_id)
            or downstream_count.get(instance_id)
            or tier(instance_id) < 5
            or asset_type in {"fibre_splitter", "patch_panel", "cable_management", "cable_manager", "pdu", "ups", "power_device"}
        ):
            continue
        if direct[instance_id]["bandwidth_mbps"] <= 0.0:
            direct[instance_id]["bandwidth_mbps"] = max(
                0.0, _float(instance.get("expected_bandwidth_mbps"))
            )
        if direct[instance_id]["packet_rate_pps"] <= 0.0:
            direct[instance_id]["packet_rate_pps"] = max(
                0.0, _float(instance.get("expected_packet_rate_pps"))
            )

    carried: Dict[str, Dict[str, float]] = {
        instance_id: {"bandwidth_mbps": 0.0, "packet_rate_pps": 0.0}
        for instance_id in instances
    }
    connection_loads: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"bandwidth_mbps": 0.0, "packet_rate_pps": 0.0}
    )
    lag_loads: Dict[str, Dict[str, object]] = {}
    for route in route_groups.values():
        lag_id = _text(route.get("lag_id"))
        if not lag_id:
            continue
        members = list(route.get("members", []))
        lag_loads[lag_id] = {
            "bandwidth_mbps": 0.0,
            "packet_rate_pps": 0.0,
            "member_connection_ids": [
                _text(member.get("id")) for member in members if _text(member.get("id"))
            ],
            "member_count": len(members),
            "member_speed_mbps": max(
                [max(0, _int(member.get("link_speed_mbps"))) for member in members]
                or [0]
            ),
            "aggregate_capacity_mbps": sum(
                max(0, _int(member.get("link_speed_mbps"))) for member in members
            ),
            "upstream_instance_id": _text(route.get("upstream")),
            "downstream_instance_id": _text(route.get("downstream")),
        }
    unique_by_tier: Dict[int, Dict[str, float]] = defaultdict(
        lambda: {"bandwidth_mbps": 0.0, "packet_rate_pps": 0.0}
    )

    # Propagate each device's direct demand independently. The per-origin
    # visited set removes reconvergent double counting at a shared upstream
    # switch, while every protected edge still receives the full demand.
    for origin_id, demand in direct.items():
        bandwidth = max(0.0, _float(demand.get("bandwidth_mbps")))
        packets = max(0.0, _float(demand.get("packet_rate_pps")))
        if bandwidth <= 0.0 and packets <= 0.0:
            continue
        queue = deque([origin_id])
        visited_nodes: Set[str] = set()
        visited_routes: Set[str] = set()
        while queue:
            current = queue.popleft()
            if current in visited_nodes:
                continue
            visited_nodes.add(current)
            carried[current]["bandwidth_mbps"] += bandwidth
            carried[current]["packet_rate_pps"] += packets
            for route in upstream_by_instance.get(current, []):
                route_id = _text(route.get("route_id"))
                upstream = _text(route.get("upstream"))
                if route_id and route_id not in visited_routes:
                    visited_routes.add(route_id)
                    members = list(route.get("members", []))
                    weights = [
                        max(0.0, _float(member.get("link_speed_mbps")))
                        for member in members
                    ]
                    total_weight = sum(weights)
                    if total_weight <= 0.0:
                        weights = [1.0 for _member in members]
                        total_weight = float(len(weights) or 1)
                    for member, weight in zip(members, weights):
                        connection_id = _text(member.get("id"))
                        if not connection_id:
                            continue
                        fraction = weight / total_weight
                        connection_loads[connection_id]["bandwidth_mbps"] += (
                            bandwidth * fraction
                        )
                        connection_loads[connection_id]["packet_rate_pps"] += (
                            packets * fraction
                        )
                    lag_id = _text(route.get("lag_id"))
                    if lag_id and lag_id in lag_loads:
                        lag_loads[lag_id]["bandwidth_mbps"] = (
                            _float(lag_loads[lag_id].get("bandwidth_mbps"))
                            + bandwidth
                        )
                        lag_loads[lag_id]["packet_rate_pps"] = (
                            _float(lag_loads[lag_id].get("packet_rate_pps"))
                            + packets
                        )
                if upstream and upstream not in visited_nodes:
                    queue.append(upstream)

        # One endpoint demand may traverse two protected devices at the same
        # tier. Count it once for the displayed layer total while preserving the
        # full load on every independent device and connection above.
        for reached_tier in {tier(instance_id) for instance_id in visited_nodes}:
            unique_by_tier[reached_tier]["bandwidth_mbps"] += bandwidth
            unique_by_tier[reached_tier]["packet_rate_pps"] += packets

    return {
        "direct_by_instance": direct,
        "carried_by_instance": carried,
        "by_connection": dict(connection_loads),
        "by_link_aggregation_group": lag_loads,
        "unique_by_tier": dict(unique_by_tier),
        "tier_by_instance": {
            instance_id: tier(instance_id) for instance_id in instances
        },
        "total_endpoint_bandwidth_mbps": total_endpoint_bandwidth,
        "total_endpoint_packet_rate_pps": total_endpoint_packets,
    }
