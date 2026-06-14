"""Automatic Traditional LAN and PoLAN topology planning.

The planner consumes the CableRouteResolver routing graph and room/data-point
model.  It writes only records marked ``auto_generated`` and therefore leaves
manually installed network assets and connections untouched.
"""

from __future__ import annotations

from collections import defaultdict, deque
from copy import deepcopy
from dataclasses import dataclass, field
from heapq import heappop, heappush
from itertools import count
import math
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from network_schema import ensure_network_schema


class NetworkPlanningError(RuntimeError):
    """Raised when the available network-asset library cannot meet demand."""


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


def _distance(a: dict, b: dict, floor_height_m: float = 4.0) -> float:
    dx = _float(a.get("x")) - _float(b.get("x"))
    dy = _float(a.get("y")) - _float(b.get("y"))
    dz = abs(_int(a.get("floor")) - _int(b.get("floor"))) * floor_height_m
    return math.sqrt(dx * dx + dy * dy + dz * dz)


def _split_ratio_outputs(asset: dict) -> int:
    ratio = _text(asset.get("split_ratio"))
    if ":" in ratio:
        try:
            return max(0, int(ratio.split(":", 1)[1]))
        except ValueError:
            pass
    return max(0, _int(asset.get("connections_out")), _int(asset.get("number_of_ports")) - 1)


def _asset_type(asset: dict) -> str:
    value = _text(asset.get("asset_type")).lower()
    notes = _text(asset.get("notes")).lower()
    if value == "other" and "network_router" in notes:
        return "network_router"
    return value


@dataclass
class PortDemand:
    endpoint_name: str
    endpoint_port: int
    endpoint_asset_id: str
    endpoint_asset_name: str
    department_id: str
    department_name: str
    floor: int
    x: float
    y: float
    extension_distance_m: float
    poe_power_w: float
    room_type_id: str


@dataclass
class EndpointDemand:
    name: str
    floor: int
    x: float
    y: float
    department_id: str
    department_name: str
    room_type_id: str
    extension_distance_m: float
    ports: List[PortDemand] = field(default_factory=list)
    existing_comms_room: str = ""

    @property
    def port_count(self) -> int:
        return len(self.ports)

    @property
    def poe_power_w(self) -> float:
        return sum(item.poe_power_w for item in self.ports)


class RoutingGraph:
    """Small cached Dijkstra wrapper around the established cable graph."""

    def __init__(self, data: dict):
        self.data = data
        self.points: Dict[str, dict] = {}
        self.graph: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        self.floor_height_m = _float(data.get("building", {}).get("floor_height_m"), 4.0)
        self._trees: Dict[str, Tuple[Dict[str, float], Dict[str, str]]] = {}
        self._build()

    def _add_point(self, name: str, payload: dict, kind: str) -> None:
        name = _text(name)
        if not name:
            return
        self.points[name] = {
            "name": name,
            "x": _float(payload.get("x")),
            "y": _float(payload.get("y")),
            "floor": _int(payload.get("floor")),
            "kind": kind,
        }
        self.graph.setdefault(name, [])

    def _build(self) -> None:
        for item in self.data.get("locations", []):
            if isinstance(item, dict):
                self._add_point(item.get("name"), item, _text(item.get("kind")) or "location")
        for item in self.data.get("data_points", []):
            if isinstance(item, dict):
                self._add_point(item.get("name"), item, "data_point")
        for item in self.data.get("corridors", {}).get("nodes", []):
            if isinstance(item, dict):
                self._add_point(item.get("name"), item, "corridor_node")

        for transition in self.data.get("transitions", []):
            if not isinstance(transition, dict):
                continue
            transition_id = _text(transition.get("id"))
            for floor_text, coordinates in (transition.get("floor_locations") or {}).items():
                if not isinstance(coordinates, dict):
                    continue
                point = dict(coordinates)
                point["floor"] = _int(floor_text)
                self._add_point(f"{transition_id}-F{floor_text}", point, "transition_node")

        for edge in self.data.get("corridors", {}).get("edges", []):
            if not isinstance(edge, dict):
                continue
            a_name = _text(edge.get("from"))
            b_name = _text(edge.get("to"))
            a = self.points.get(a_name)
            b = self.points.get(b_name)
            if not a or not b:
                continue
            weight = _distance(a, b, self.floor_height_m)
            self.graph[a_name].append((b_name, weight))
            self.graph[b_name].append((a_name, weight))

        for transition in self.data.get("transitions", []):
            if not isinstance(transition, dict):
                continue
            transition_id = _text(transition.get("id"))
            names = [
                f"{transition_id}-F{floor_text}"
                for floor_text in (transition.get("floor_locations") or {}).keys()
                if f"{transition_id}-F{floor_text}" in self.points
            ]
            for index, a_name in enumerate(names):
                for b_name in names[index + 1 :]:
                    a = self.points[a_name]
                    b = self.points[b_name]
                    weight = abs(_int(a.get("floor")) - _int(b.get("floor"))) * self.floor_height_m
                    self.graph[a_name].append((b_name, weight))
                    self.graph[b_name].append((a_name, weight))

    def _tree(self, source: str) -> Tuple[Dict[str, float], Dict[str, str]]:
        source = _text(source)
        if source in self._trees:
            return self._trees[source]
        distances: Dict[str, float] = {}
        parents: Dict[str, str] = {}
        if source not in self.graph:
            self._trees[source] = (distances, parents)
            return distances, parents
        queue: List[Tuple[float, str]] = [(0.0, source)]
        tentative: Dict[str, float] = {source: 0.0}
        while queue:
            cost, node = heappop(queue)
            if node in distances:
                continue
            if cost > tentative.get(node, float("inf")):
                continue
            distances[node] = cost
            for next_node, weight in self.graph.get(node, []):
                if next_node in distances:
                    continue
                new_cost = cost + float(weight)
                if new_cost + 1e-12 < tentative.get(next_node, float("inf")):
                    tentative[next_node] = new_cost
                    parents[next_node] = node
                    heappush(queue, (new_cost, next_node))
        self._trees[source] = (distances, parents)
        return distances, parents

    def route(self, source: str, destination: str) -> Tuple[float, List[str]]:
        source = _text(source)
        destination = _text(destination)
        if not source or not destination:
            return 0.0, []
        if source == destination:
            return 0.0, [source]
        distances, parents = self._tree(source)
        if destination not in distances:
            a = self.points.get(source)
            b = self.points.get(destination)
            if a and b:
                return _distance(a, b, self.floor_height_m), [source, destination]
            return 0.0, [source, destination]
        path = [destination]
        node = destination
        guard = 0
        while node != source and guard < len(self.points) + 1:
            node = parents.get(node, source)
            path.append(node)
            guard += 1
        path.reverse()
        return float(distances[destination]), path


DEFAULT_POE_BY_KEYWORD = {
    "wireless access point": 30.0,
    "access point": 30.0,
    "desk phone": 7.0,
    "bedside telephone": 7.0,
    "cctv": 15.0,
    "door access": 15.0,
    "rfid reader": 12.0,
    "room display": 15.0,
    "patient information screen": 25.0,
    "clock": 5.0,
}


def _poe_power_for_asset(asset: dict, settings: dict) -> float:
    for field_name in ("poe_power_w", "poe_w", "poe_load_w"):
        if field_name in asset:
            return max(0.0, _float(asset.get(field_name)))
    if not bool(asset.get("requires_poe", False)) and _text(asset.get("connection_type")).lower() != "wired":
        return 0.0
    name = _text(asset.get("name")).lower()
    configured = settings.get("poe_power_defaults", {})
    if isinstance(configured, dict):
        for keyword, watts in configured.items():
            if _text(keyword).lower() in name:
                return max(0.0, _float(watts))
    for keyword, watts in DEFAULT_POE_BY_KEYWORD.items():
        if keyword in name:
            return watts
    return max(0.0, _float(settings.get("default_poe_power_w"), 0.0)) if asset.get("requires_poe") else 0.0


def build_endpoint_demands(data: dict) -> Tuple[List[EndpointDemand], List[str]]:
    """Expand room-type data into individual physical network-port demand."""

    settings = data.setdefault("network_settings", {})
    room_types = {
        _text(item.get("id")): item
        for item in data.get("room_types", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    assets = {
        _text(item.get("id")): item
        for item in data.get("assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    departments = {
        _text(item.get("id")): _text(item.get("name")) or _text(item.get("id"))
        for item in data.get("departments", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    existing_sources = {
        _text(item.get("to")): _text(item.get("from"))
        for item in data.get("connections", [])
        if isinstance(item, dict) and _text(item.get("to")) and _text(item.get("from"))
    }

    warnings: List[str] = []
    results: List[EndpointDemand] = []
    missing_room_types: set[str] = set()

    for point in data.get("data_points", []):
        if not isinstance(point, dict):
            continue
        name = _text(point.get("name"))
        if not name:
            continue
        department_ids = [_text(value) for value in point.get("department_ids", []) if _text(value)]
        department_id = department_ids[0] if department_ids else "UNASSIGNED"
        room_type_id = _text(point.get("room_type_id"))
        room_type = room_types.get(room_type_id, {})
        requested_ports = max(0, _int(point.get("qty"), 1))
        templates: List[Tuple[str, str, float]] = []

        if room_type:
            for room_asset in room_type.get("assets", []):
                if not isinstance(room_asset, dict):
                    continue
                asset_id = _text(room_asset.get("asset_id"))
                asset = assets.get(asset_id, {})
                asset_name = _text(asset.get("name")) or asset_id or "Generic endpoint"
                quantity = max(0, _int(room_asset.get("qty"), 1))
                data_points_per_asset = max(0, _int(asset.get("data_points"), 1))
                power = _poe_power_for_asset(asset, settings)
                for _ in range(quantity * data_points_per_asset):
                    templates.append((asset_id, asset_name, power))
        elif room_type_id:
            missing_room_types.add(room_type_id)

        target_ports = max(requested_ports, len(templates))
        if target_ports <= 0:
            continue
        while len(templates) < target_ports:
            templates.append(("", "Generic network port", 0.0))

        endpoint = EndpointDemand(
            name=name,
            floor=_int(point.get("floor")),
            x=_float(point.get("x")),
            y=_float(point.get("y")),
            department_id=department_id,
            department_name=departments.get(department_id, department_id),
            room_type_id=room_type_id,
            extension_distance_m=max(0.0, _float(point.get("extension_distance_m"))),
            existing_comms_room=existing_sources.get(name, ""),
        )
        for index, (asset_id, asset_name, poe_w) in enumerate(templates[:target_ports], start=1):
            endpoint.ports.append(
                PortDemand(
                    endpoint_name=name,
                    endpoint_port=index,
                    endpoint_asset_id=asset_id,
                    endpoint_asset_name=asset_name,
                    department_id=department_id,
                    department_name=endpoint.department_name,
                    floor=endpoint.floor,
                    x=endpoint.x,
                    y=endpoint.y,
                    extension_distance_m=endpoint.extension_distance_m,
                    poe_power_w=poe_w,
                    room_type_id=room_type_id,
                )
            )
        results.append(endpoint)

    if missing_room_types:
        warnings.append(
            "Missing room-type definitions for: " + ", ".join(sorted(missing_room_types))
        )
    return results, warnings


def _candidate_assets(data: dict, asset_type: str, predicate=None) -> List[dict]:
    rows = []
    for asset in data.get("network_assets", []):
        if not isinstance(asset, dict) or _asset_type(asset) != asset_type:
            continue
        if predicate is not None and not predicate(asset):
            continue
        rows.append(asset)
    return rows


def _usable_ports(asset: dict, spare_fraction: float) -> int:
    ports = max(0, _int(asset.get("number_of_ports")))
    return max(0, int(math.floor(ports / max(1.0, 1.0 + spare_fraction))))


def _usable_poe(asset: dict, spare_fraction: float) -> float:
    return max(0.0, _float(asset.get("poe_budget_w"))) / max(1.0, 1.0 + spare_fraction)


def _minimum_asset_mix(
    candidates: Sequence[dict],
    actual_ports: int,
    actual_poe_w: float,
    spare_fraction: float,
    label: str,
) -> List[dict]:
    """Return a minimum-device-count capacity mix using a Pareto frontier."""

    candidates = [item for item in candidates if _int(item.get("number_of_ports")) > 0]
    if not candidates:
        raise NetworkPlanningError(f"No usable {label} assets exist in the network asset library.")

    required_ports = max(0, int(math.ceil(actual_ports * (1.0 + spare_fraction))))
    required_poe = max(0, int(math.ceil(actual_poe_w * (1.0 + spare_fraction))))
    max_ports = max(_int(item.get("number_of_ports")) for item in candidates)
    max_poe = max(int(math.floor(_float(item.get("poe_budget_w")))) for item in candidates)
    if required_poe > 0 and max_poe <= 0:
        raise NetworkPlanningError(f"The available {label} assets have no PoE budget.")

    lower = max(
        1,
        int(math.ceil(required_ports / max_ports)) if max_ports else 1,
        int(math.ceil(required_poe / max_poe)) if required_poe and max_poe else 1,
    )
    single_counts = []
    for asset in candidates:
        ports = _int(asset.get("number_of_ports"))
        poe = int(math.floor(_float(asset.get("poe_budget_w"))))
        if ports <= 0 or (required_poe and poe <= 0):
            continue
        single_counts.append(
            max(
                int(math.ceil(required_ports / ports)) if required_ports else 0,
                int(math.ceil(required_poe / poe)) if required_poe else 0,
                1,
            )
        )
    upper = min(single_counts) if single_counts else lower + 100

    port_cap = required_ports + max_ports
    poe_cap = required_poe + max_poe
    frontier: Dict[Tuple[int, int], Tuple[List[int], float, int]] = {(0, 0): ([], 0.0, 0)}

    for device_count in range(1, upper + 1):
        generated: Dict[Tuple[int, int], Tuple[List[int], float, int]] = {}
        for (ports, poe), (indices, power, rack) in frontier.items():
            for candidate_index, asset in enumerate(candidates):
                next_ports = min(port_cap, ports + _int(asset.get("number_of_ports")))
                next_poe = min(poe_cap, poe + int(math.floor(_float(asset.get("poe_budget_w")))))
                next_power = power + _float(asset.get("power_input_w"))
                next_rack = rack + _int(asset.get("rack_units"))
                key = (next_ports, next_poe)
                previous = generated.get(key)
                score = (next_rack, next_power, indices + [candidate_index])
                if previous is None or (next_rack, next_power) < (previous[2], previous[1]):
                    generated[key] = (indices + [candidate_index], next_power, next_rack)

        feasible = [
            (key, value)
            for key, value in generated.items()
            if key[0] >= required_ports and key[1] >= required_poe
        ]
        if feasible and device_count >= lower:
            key, value = min(
                feasible,
                key=lambda row: (
                    row[1][2],
                    row[1][1],
                    row[0][0] - required_ports,
                    row[0][1] - required_poe,
                ),
            )
            return [candidates[index] for index in value[0]]

        # Retain only capacity states that are not dominated at this device count.
        sorted_states = sorted(generated.items(), key=lambda row: (-row[0][0], -row[0][1]))
        pruned: Dict[Tuple[int, int], Tuple[List[int], float, int]] = {}
        best_poe = -1
        for key, value in sorted_states:
            if key[1] > best_poe:
                pruned[key] = value
                best_poe = key[1]
        if len(pruned) > 8000:
            ordered = sorted(
                pruned.items(),
                key=lambda row: -(
                    row[0][0] / max(1, required_ports)
                    + row[0][1] / max(1, required_poe or 1)
                ),
            )[:8000]
            pruned = dict(ordered)
        frontier = pruned

    raise NetworkPlanningError(
        f"Unable to find a {label} combination for {actual_ports} ports and {actual_poe_w:.1f} W PoE."
    )


def _pack_ports_to_devices(
    port_items: Sequence[PortDemand],
    device_assets: Sequence[dict],
    spare_fraction: float,
) -> List[List[PortDemand]]:
    # Spare capacity is enforced when selecting the aggregate device mix.
    # Use each device's physical capacity for the actual port allocation so
    # rounding on small switches cannot consume the reserved aggregate margin.
    devices = [
        {
            "ports": max(0, _int(asset.get("number_of_ports"))),
            "poe": max(0.0, _float(asset.get("poe_budget_w"))),
            "items": [],
        }
        for asset in device_assets
    ]
    for item in sorted(port_items, key=lambda row: (-row.poe_power_w, row.endpoint_name, row.endpoint_port)):
        choices = []
        for index, device in enumerate(devices):
            if device["ports"] < 1 or device["poe"] + 1e-9 < item.poe_power_w:
                continue
            choices.append(
                (
                    device["poe"] - item.poe_power_w,
                    device["ports"] - 1,
                    index,
                )
            )
        if not choices:
            raise NetworkPlanningError(
                f"Unable to allocate {item.endpoint_name} port {item.endpoint_port}; "
                f"it requires {item.poe_power_w:.1f} W PoE."
            )
        _, _, selected = min(choices)
        devices[selected]["ports"] -= 1
        devices[selected]["poe"] -= item.poe_power_w
        devices[selected]["items"].append(item)
    return [device["items"] for device in devices]


def _next_identifier(existing: set[str], prefix: str) -> str:
    # Start beyond the current collection size.  Restarting at 1 for every
    # generated endpoint assignment makes large projects O(n²).
    number = max(1, len(existing) + 1)
    while True:
        value = f"{prefix}{number}"
        if value not in existing:
            existing.add(value)
            return value
        number += 1


def _clear_previous_auto_design(data: dict) -> None:
    data["network_asset_instances"] = [
        item for item in data.get("network_asset_instances", []) if not bool(item.get("auto_generated"))
    ]
    data["network_connections"] = [
        item for item in data.get("network_connections", []) if not bool(item.get("auto_generated"))
    ]
    data["network_endpoint_assignments"] = [
        item for item in data.get("network_endpoint_assignments", []) if not bool(item.get("auto_generated"))
    ]
    data["network_assets"] = [
        item for item in data.get("network_assets", []) if not bool(item.get("auto_network_asset"))
    ]
    data["locations"] = [
        item for item in data.get("locations", []) if not bool(item.get("auto_network_location"))
    ]
    data["network_redundancy_groups"] = [
        item for item in data.get("network_redundancy_groups", []) if not bool(item.get("auto_generated"))
    ]


class DesignBuilder:
    def __init__(self, data: dict, technology: str):
        self.data = data
        self.technology = technology
        self.settings = data.setdefault("network_settings", {})
        self.graph = RoutingGraph(data)
        self.asset_ids = {_text(item.get("id")) for item in data.get("network_assets", []) if _text(item.get("id"))}
        self.instance_ids = {_text(item.get("id")) for item in data.get("network_asset_instances", []) if _text(item.get("id"))}
        self.connection_ids = {_text(item.get("id")) for item in data.get("network_connections", []) if _text(item.get("id"))}
        self.assignment_ids = {_text(item.get("id")) for item in data.get("network_endpoint_assignments", []) if _text(item.get("id"))}
        self.location_names = {_text(item.get("name")) for item in data.get("locations", []) if _text(item.get("name"))}
        self.warnings: List[str] = []
        self.instances: List[dict] = []
        self.connections: List[dict] = []
        self.assignments: List[dict] = []
        self.locations: List[dict] = []
        self.redundancy_groups: List[dict] = []
        self.total_copper_m = 0.0
        self.total_fibre_m = 0.0

    def add_instance(self, asset: dict, name: str, location: dict, role: str, **extra) -> dict:
        instance_id = _next_identifier(self.instance_ids, "AUTO-NI-")
        instance = {
            "id": instance_id,
            "name": name,
            "asset_id": _text(asset.get("id")),
            "location_name": _text(location.get("name")),
            "floor": _int(location.get("floor")),
            "x": round(_float(location.get("x")), 3),
            "y": round(_float(location.get("y")), 3),
            "rack_name": _text(extra.pop("rack_name", "")),
            "rack_start_u": _int(extra.pop("rack_start_u", 0)),
            "management_ip": "",
            "management_vlan": "",
            "power_feed": "",
            "ups_source": "",
            "notes": "Automatically generated minimum-component design.",
            "auto_generated": True,
            "design_role": role,
            **extra,
        }
        self.instances.append(instance)
        return instance

    def add_connection(
        self,
        from_instance: dict,
        from_port: str,
        to_instance: dict,
        to_port: str,
        medium: str,
        route_source: str = "",
        route_destination: str = "",
        **extra,
    ) -> dict:
        connection_id = _next_identifier(self.connection_ids, "AUTO-NC-")
        length_m, route_path = self.graph.route(route_source, route_destination) if route_source and route_destination else (0.0, [])
        connection = {
            "id": connection_id,
            "from_instance_id": _text(from_instance.get("id")),
            "from_port": str(from_port),
            "to_instance_id": _text(to_instance.get("id")),
            "to_port": str(to_port),
            "connection_role": "uplink",
            "medium": medium,
            "cable_specification": "OS2 single-mode fibre" if medium == "fibre" else "Category 6A",
            "fibre_count": 2 if medium == "fibre" else 0,
            "vlan_ids": [],
            "route_profile": "",
            "route_path": route_path,
            "length_m": round(length_m, 3),
            "notes": "Automatically generated network topology connection.",
            "auto_generated": True,
            **extra,
        }
        self.connections.append(connection)
        if medium == "fibre":
            self.total_fibre_m += length_m
        else:
            self.total_copper_m += length_m
        return connection

    def add_assignment(
        self,
        item: PortDemand,
        instance: dict,
        port_name: str,
        copper_length_m: float,
        route_path: Sequence[str],
        **extra,
    ) -> dict:
        assignment = {
            "id": _next_identifier(self.assignment_ids, "AUTO-EA-"),
            "endpoint_name": item.endpoint_name,
            "endpoint_port": item.endpoint_port,
            "endpoint_asset_id": item.endpoint_asset_id,
            "endpoint_asset_name": item.endpoint_asset_name,
            "department_id": item.department_id,
            "department_name": item.department_name,
            "room_type_id": item.room_type_id,
            "floor": item.floor,
            "x": round(item.x, 3),
            "y": round(item.y, 3),
            "network_instance_id": _text(instance.get("id")),
            "network_port": str(port_name),
            "poe_power_w": round(item.poe_power_w, 3),
            "copper_length_m": round(max(0.0, copper_length_m), 3),
            "route_path": list(route_path),
            "vlan_ids": [],
            "technology": self.technology,
            "auto_generated": True,
            **extra,
        }
        self.assignments.append(assignment)
        self.total_copper_m += max(0.0, copper_length_m)
        return assignment

    def add_polan_location(self, floor: int, x: float, y: float, department_id: str, anchor: str) -> dict:
        name = _next_identifier(self.location_names, f"AUTO-POLAN-F{floor}-")
        location = {
            "name": name,
            "floor": int(floor),
            "x": round(float(x), 3),
            "y": round(float(y), 3),
            "kind": "polan",
            "department_id": department_id if department_id != "UNASSIGNED" else "",
            "department_ids": [department_id] if department_id != "UNASSIGNED" else [],
            "anchor_point_name": anchor,
            "auto_network_location": True,
        }
        self.locations.append(location)
        self.graph._add_point(name, location, "polan")
        return location

    def commit(self) -> None:
        self.data.setdefault("locations", []).extend(self.locations)
        self.data.setdefault("network_asset_instances", []).extend(self.instances)
        self.data.setdefault("network_connections", []).extend(self.connections)
        self.data.setdefault("network_endpoint_assignments", []).extend(self.assignments)
        self.data.setdefault("network_redundancy_groups", []).extend(self.redundancy_groups)


def _locations_by_kind(data: dict, kinds: Iterable[str]) -> List[dict]:
    wanted = {value.lower() for value in kinds}
    return [
        item
        for item in data.get("locations", [])
        if isinstance(item, dict) and _text(item.get("kind")).lower() in wanted and _text(item.get("name"))
    ]


def _nearest_location(point: EndpointDemand | dict, locations: Sequence[dict], same_floor_first: bool = True) -> Optional[dict]:
    if not locations:
        return None
    floor = _int(point.get("floor")) if isinstance(point, dict) else point.floor
    x = _float(point.get("x")) if isinstance(point, dict) else point.x
    y = _float(point.get("y")) if isinstance(point, dict) else point.y
    ordered = sorted(
        locations,
        key=lambda location: (
            0 if not same_floor_first or _int(location.get("floor")) == floor else 1,
            math.hypot(_float(location.get("x")) - x, _float(location.get("y")) - y),
        ),
    )
    return ordered[0]


def _choose_core_candidates(data: dict) -> List[dict]:
    candidates = _candidate_assets(
        data,
        "network_switch",
        lambda asset: _text(asset.get("output_connection_type")).lower() == "fibre"
        or "core" in _text(asset.get("name")).lower()
        or "distribution" in _text(asset.get("name")).lower(),
    )
    if not candidates:
        candidates = _candidate_assets(data, "network_switch")
    return candidates


def _build_core_layer(
    builder: DesignBuilder,
    leaves_by_root: Dict[str, List[dict]],
    roots: Dict[str, dict],
    spare_fraction: float,
) -> List[dict]:
    cores: List[dict] = []
    candidates = _choose_core_candidates(builder.data)
    if not candidates:
        builder.warnings.append("No core/distribution switch asset was available; uplinks were not terminated at a core.")
        return cores

    for root_name, leaves in leaves_by_root.items():
        if not leaves:
            continue
        root = roots[root_name]
        mix = _minimum_asset_mix(candidates, len(leaves), 0.0, spare_fraction, "core switch")
        core_instances = [
            builder.add_instance(
                asset,
                f"AUTO Core {root_name} {index + 1}",
                root,
                "core_switch",
                rack_name=f"AUTO-RACK-{root_name}",
                rack_start_u=index + 1,
            )
            for index, asset in enumerate(mix)
        ]
        # Spare capacity is already included in the selected aggregate mix.
        capacities = [max(0, _int(asset.get("number_of_ports"))) for asset in mix]
        core_index = 0
        core_port = 1
        for leaf in leaves:
            while core_index < len(capacities) and core_port > capacities[core_index]:
                core_index += 1
                core_port = 1
            if core_index >= len(core_instances):
                raise NetworkPlanningError("Core switch port allocation exceeded the selected capacity.")
            leaf_location = _text(leaf.get("route_anchor")) or _text(leaf.get("location_name"))
            builder.add_connection(
                leaf,
                f"Uplink-{_int(leaf.get('_uplinks_used'), 0) + 1}",
                core_instances[core_index],
                str(core_port),
                "fibre",
                leaf_location,
                root_name,
                redundancy_role=_text(leaf.get("core_redundancy_role")),
            )
            leaf["_uplinks_used"] = _int(leaf.get("_uplinks_used"), 0) + 1
            core_port += 1
        cores.extend(core_instances)
    return cores


def _traditional_design(builder: DesignBuilder, endpoints: Sequence[EndpointDemand], spare_fraction: float) -> None:
    comms_rooms = _locations_by_kind(builder.data, {"comms_room"})
    if not comms_rooms:
        raise NetworkPlanningError("Traditional network planning requires at least one comms_room location.")
    room_map = {_text(item.get("name")): item for item in comms_rooms}
    max_copper_m = max(1.0, _float(builder.settings.get("traditional_max_copper_m"), 90.0))

    # Keep each department/floor together by choosing its dominant established comms-room route.
    grouped: Dict[Tuple[str, int], List[EndpointDemand]] = defaultdict(list)
    for endpoint in endpoints:
        grouped[(endpoint.department_id, endpoint.floor)].append(endpoint)

    endpoint_room: Dict[str, dict] = {}
    for _, group in grouped.items():
        weighted_sources: Dict[str, int] = defaultdict(int)
        for endpoint in group:
            if endpoint.existing_comms_room in room_map:
                weighted_sources[endpoint.existing_comms_room] += endpoint.port_count
        if weighted_sources:
            dominant_name = max(weighted_sources, key=weighted_sources.get)
            dominant = room_map[dominant_name]
        else:
            centre = {
                "floor": group[0].floor,
                "x": sum(item.x * item.port_count for item in group) / max(1, sum(item.port_count for item in group)),
                "y": sum(item.y * item.port_count for item in group) / max(1, sum(item.port_count for item in group)),
            }
            dominant = _nearest_location(centre, comms_rooms)
        for endpoint in group:
            selected = dominant
            if selected is not None:
                route_length, _ = builder.graph.route(_text(selected.get("name")), endpoint.name)
                route_length += endpoint.extension_distance_m
                if route_length > max_copper_m:
                    own = room_map.get(endpoint.existing_comms_room)
                    if own is None:
                        own = _nearest_location(endpoint, comms_rooms)
                    selected = own
            if selected is None:
                raise NetworkPlanningError(f"No comms room could be selected for {endpoint.name}.")
            endpoint_room[endpoint.name] = selected

    room_ports: Dict[str, List[PortDemand]] = defaultdict(list)
    for endpoint in endpoints:
        room_ports[_text(endpoint_room[endpoint.name].get("name"))].extend(endpoint.ports)

    switch_candidates = _candidate_assets(
        builder.data,
        "network_switch",
        lambda asset: _text(asset.get("output_connection_type")).lower() == "copper"
        and _int(asset.get("number_of_ports")) > 0,
    )
    if not switch_candidates:
        raise NetworkPlanningError("No copper-output network switch assets are available for Traditional design.")

    access_switches: List[dict] = []
    for room_name in sorted(room_ports):
        room = room_map[room_name]
        port_items = room_ports[room_name]
        mix = _minimum_asset_mix(
            switch_candidates,
            len(port_items),
            sum(item.poe_power_w for item in port_items),
            spare_fraction,
            f"access switch at {room_name}",
        )
        packed = _pack_ports_to_devices(port_items, mix, spare_fraction)
        for switch_number, (asset, assigned) in enumerate(zip(mix, packed), start=1):
            instance = builder.add_instance(
                asset,
                f"AUTO {room_name} Access Switch {switch_number}",
                room,
                "access_switch",
                rack_name=f"AUTO-RACK-{room_name}",
                rack_start_u=switch_number,
                route_anchor=room_name,
            )
            for port_number, item in enumerate(assigned, start=1):
                length_m, path = builder.graph.route(room_name, item.endpoint_name)
                length_m += item.extension_distance_m
                builder.add_assignment(
                    item,
                    instance,
                    str(port_number),
                    length_m,
                    path,
                    source_location=room_name,
                )
            access_switches.append(instance)

    mer_locations = _locations_by_kind(builder.data, {"mer"})
    if not mer_locations:
        mer_locations = [comms_rooms[0]]
        builder.warnings.append("No MER exists; the first comms room was used as the network root.")
    redundant = bool(builder.settings.get("redundant_core", True))
    roots_list = mer_locations[:2] if redundant and len(mer_locations) >= 2 else mer_locations[:1]
    roots = {_text(item.get("name")): item for item in roots_list}
    leaves_by_root = {name: list(access_switches) for name in roots}
    if redundant and len(roots) < 2:
        builder.warnings.append("Only one MER is available, so access-switch core uplinks are not room-diverse.")
    _build_core_layer(builder, leaves_by_root, roots, spare_fraction)


def _fits_any_ont(candidates: Sequence[dict], ports: int, poe_w: float, spare_fraction: float) -> bool:
    required_ports = int(math.ceil(ports * (1.0 + spare_fraction)))
    required_poe = poe_w * (1.0 + spare_fraction)
    return any(
        _int(asset.get("number_of_ports")) >= required_ports
        and _float(asset.get("poe_budget_w")) + 1e-9 >= required_poe
        for asset in candidates
    )


def _choose_single_ont(candidates: Sequence[dict], items: Sequence[PortDemand], spare_fraction: float) -> dict:
    ports = len(items)
    poe = sum(item.poe_power_w for item in items)
    feasible = [
        asset
        for asset in candidates
        if _int(asset.get("number_of_ports")) >= math.ceil(ports * (1.0 + spare_fraction))
        and _float(asset.get("poe_budget_w")) + 1e-9 >= poe * (1.0 + spare_fraction)
    ]
    if not feasible:
        raise NetworkPlanningError(f"No ONT can support a cluster of {ports} ports and {poe:.1f} W PoE.")
    return min(
        feasible,
        key=lambda asset: (
            _int(asset.get("number_of_ports")) - ports,
            _float(asset.get("poe_budget_w")) - poe,
            _float(asset.get("power_input_w")),
        ),
    )


def _cluster_polan_ports(
    endpoints: Sequence[EndpointDemand],
    ont_candidates: Sequence[dict],
    spare_fraction: float,
    max_copper_m: float,
) -> List[Tuple[dict, List[PortDemand], EndpointDemand]]:
    """Department-first capacitated clustering with an ONT at a local medoid."""

    groups: Dict[Tuple[str, int, str], List[EndpointDemand]] = defaultdict(list)
    for endpoint in endpoints:
        # The existing comms-room route is used as a graph branch.  This avoids
        # clustering ports that are physically close in XY but separated by
        # walls or by different established cable-routing branches.
        groups[(endpoint.department_id, endpoint.floor, endpoint.existing_comms_room)].append(endpoint)

    clusters: List[Tuple[dict, List[PortDemand], EndpointDemand]] = []
    for group_key in sorted(groups, key=lambda value: (value[1], value[0], value[2])):
        points = groups[group_key]
        remaining: Dict[str, deque[PortDemand]] = {
            point.name: deque(sorted(point.ports, key=lambda item: -item.poe_power_w)) for point in points
        }
        point_map = {point.name: point for point in points}

        while any(remaining.values()):
            seed_name = max(
                (name for name, queue in remaining.items() if queue),
                key=lambda name: (len(remaining[name]), sum(item.poe_power_w for item in remaining[name])),
            )
            seed = point_map[seed_name]
            candidates = sorted(
                (point for point in points if remaining[point.name]),
                key=lambda point: (
                    math.hypot(point.x - seed.x, point.y - seed.y),
                    -len(remaining[point.name]),
                ),
            )
            cluster_items: List[PortDemand] = []
            represented: List[EndpointDemand] = []
            for point in candidates:
                if math.hypot(point.x - seed.x, point.y - seed.y) > max_copper_m + 1e-9:
                    continue
                took_any = False
                queue = remaining[point.name]
                while queue:
                    item = queue[0]
                    next_ports = len(cluster_items) + 1
                    next_poe = sum(row.poe_power_w for row in cluster_items) + item.poe_power_w
                    if not _fits_any_ont(ont_candidates, next_ports, next_poe, spare_fraction):
                        break
                    cluster_items.append(queue.popleft())
                    took_any = True
                if took_any:
                    represented.append(point)

            if not cluster_items:
                item = remaining[seed_name][0]
                raise NetworkPlanningError(
                    f"No ONT can serve {item.endpoint_name} port {item.endpoint_port} "
                    f"with {item.poe_power_w:.1f} W PoE and the configured spare capacity."
                )

            # Select the point minimising weighted copper length.  The seed is a
            # guaranteed fallback because every cluster point lies within the
            # configured maximum copper distance from it.
            counts = defaultdict(int)
            for item in cluster_items:
                counts[item.endpoint_name] += 1
            medoid = min(
                represented,
                key=lambda candidate: sum(
                    counts[other.name] * math.hypot(candidate.x - other.x, candidate.y - other.y)
                    for other in represented
                ),
            )
            if any(math.hypot(medoid.x - other.x, medoid.y - other.y) > max_copper_m for other in represented):
                medoid = seed
            asset = _choose_single_ont(ont_candidates, cluster_items, spare_fraction)
            clusters.append((asset, cluster_items, medoid))
    return clusters


def _protected_splitter_asset(builder: DesignBuilder, base_asset: dict) -> dict:
    existing_id = f"AUTO-PROTECTED-{_text(base_asset.get('id'))}"
    for asset in builder.data.get("network_assets", []):
        if _text(asset.get("id")) == existing_id:
            return asset
    asset = deepcopy(base_asset)
    asset["id"] = existing_id
    asset["name"] = f"Protected {base_asset.get('name', 'PoLAN splitter')}"
    asset["connections_in"] = 2
    asset["number_of_ports"] = _split_ratio_outputs(base_asset) + 2
    asset["notes"] = (
        _text(base_asset.get("notes"))
        + " Auto-generated 2:N protected splitter/coupler for primary and standby OLT feeder inputs."
    ).strip()
    asset["auto_network_asset"] = True
    builder.data.setdefault("network_assets", []).append(asset)
    builder.asset_ids.add(existing_id)
    return asset


def _polan_design(builder: DesignBuilder, endpoints: Sequence[EndpointDemand], spare_fraction: float) -> None:
    max_copper_m = max(1.0, _float(builder.settings.get("polan_max_ont_copper_m"), 30.0))
    ont_candidates = _candidate_assets(
        builder.data,
        "optical_network_terminal",
        lambda asset: _int(asset.get("number_of_ports")) > 0
        and _text(asset.get("output_connection_type")).lower() == "copper",
    )
    if not ont_candidates:
        raise NetworkPlanningError("No copper-output ONT assets exist in the network asset library.")

    clusters = _cluster_polan_ports(endpoints, ont_candidates, spare_fraction, max_copper_m)
    ont_records: List[dict] = []
    endpoint_lookup = {item.name: item for item in endpoints}

    for cluster_number, (asset, port_items, medoid) in enumerate(clusters, start=1):
        location = builder.add_polan_location(
            medoid.floor,
            medoid.x,
            medoid.y,
            medoid.department_id,
            medoid.name,
        )
        ont = builder.add_instance(
            asset,
            f"AUTO ONT F{medoid.floor} {cluster_number}",
            location,
            "ont",
            route_anchor=medoid.name,
            department_id=medoid.department_id,
            department_name=medoid.department_name,
        )
        source_counts: Dict[str, int] = defaultdict(int)
        for port_number, item in enumerate(port_items, start=1):
            endpoint = endpoint_lookup[item.endpoint_name]
            copper = math.hypot(endpoint.x - medoid.x, endpoint.y - medoid.y) + item.extension_distance_m
            if copper > max_copper_m + item.extension_distance_m + 1e-9:
                raise NetworkPlanningError(
                    f"ONT placement for {item.endpoint_name} exceeds {max_copper_m:.1f} m local copper limit."
                )
            builder.add_assignment(
                item,
                ont,
                str(port_number),
                copper,
                [item.endpoint_name, _text(location.get("name"))],
                ont_location=_text(location.get("name")),
            )
            if endpoint.existing_comms_room:
                source_counts[endpoint.existing_comms_room] += 1
        ont_records.append(
            {
                "instance": ont,
                "location": location,
                "anchor": medoid.name,
                "department_id": medoid.department_id,
                "floor": medoid.floor,
                "source_room": max(source_counts, key=source_counts.get) if source_counts else "",
            }
        )

    olt_candidates = _candidate_assets(
        builder.data,
        "optical_line_terminal",
        lambda asset: max(_int(asset.get("connections_out")), _int(asset.get("number_of_ports"))) > 0,
    )
    if not olt_candidates:
        raise NetworkPlanningError("No OLT with a defined PON-port count exists in the network asset library.")
    olt_max_split = max(_split_ratio_outputs(asset) for asset in olt_candidates)
    splitter_candidates = _candidate_assets(
        builder.data,
        "fibre_splitter",
        lambda asset: 0 < _split_ratio_outputs(asset) <= max(1, olt_max_split),
    )
    if not splitter_candidates:
        raise NetworkPlanningError("No fibre splitter compatible with the available OLT split ratio exists.")

    largest_splitter = max(splitter_candidates, key=_split_ratio_outputs)
    splitter_capacity = max(1, int(math.floor(_split_ratio_outputs(largest_splitter) / (1.0 + spare_fraction))))
    failover = bool(builder.settings.get("polan_olt_failover", True))
    comms_and_polan = _locations_by_kind(builder.data, {"comms_room", "polan"})

    splitter_records: List[dict] = []
    onts_by_floor: Dict[int, List[dict]] = defaultdict(list)
    for record in ont_records:
        onts_by_floor[record["floor"]].append(record)

    for floor in sorted(onts_by_floor):
        records = sorted(
            onts_by_floor[floor],
            key=lambda row: (row["department_id"], _float(row["location"].get("x")), _float(row["location"].get("y"))),
        )
        for start in range(0, len(records), splitter_capacity):
            group = records[start : start + splitter_capacity]
            required_outputs = int(math.ceil(len(group) * (1.0 + spare_fraction)))
            base_asset = min(
                (asset for asset in splitter_candidates if _split_ratio_outputs(asset) >= required_outputs),
                key=_split_ratio_outputs,
                default=largest_splitter,
            )
            asset = _protected_splitter_asset(builder, base_asset) if failover else base_asset
            same_floor_locations = [item for item in comms_and_polan if _int(item.get("floor")) == floor]
            if same_floor_locations:
                location = min(
                    same_floor_locations,
                    key=lambda candidate: sum(
                        math.hypot(
                            _float(candidate.get("x")) - _float(row["location"].get("x")),
                            _float(candidate.get("y")) - _float(row["location"].get("y")),
                        )
                        for row in group
                    ),
                )
                anchor = _text(location.get("name"))
            else:
                centre_x = sum(_float(row["location"].get("x")) for row in group) / len(group)
                centre_y = sum(_float(row["location"].get("y")) for row in group) / len(group)
                location = builder.add_polan_location(floor, centre_x, centre_y, "", group[0]["anchor"])
                anchor = group[0]["anchor"]
            splitter = builder.add_instance(
                asset,
                f"AUTO Splitter F{floor} {len(splitter_records) + 1}",
                location,
                "protected_splitter" if failover else "splitter",
                route_anchor=anchor,
                protected=failover,
            )
            for output_number, ont_record in enumerate(group, start=1):
                builder.add_connection(
                    splitter,
                    f"Output-{output_number}",
                    ont_record["instance"],
                    "PON-1",
                    "fibre",
                    anchor,
                    ont_record["anchor"],
                    connection_role="output",
                    fibre_count=1,
                )
            splitter_records.append(
                {
                    "instance": splitter,
                    "location": location,
                    "anchor": anchor,
                    "output_count": len(group),
                    "split_capacity": _split_ratio_outputs(base_asset),
                }
            )

    mer_locations = _locations_by_kind(builder.data, {"mer"})
    if not mer_locations:
        fallback = _locations_by_kind(builder.data, {"comms_room"})
        if not fallback:
            raise NetworkPlanningError("PoLAN planning requires a MER or comms room for OLT placement.")
        mer_locations = [fallback[0]]
        builder.warnings.append("No MER exists; the first comms room was used for OLT placement.")
    primary_root = mer_locations[0]
    secondary_root = mer_locations[1] if len(mer_locations) > 1 else mer_locations[0]
    if failover and len(mer_locations) < 2:
        builder.warnings.append("Only one MER exists; primary and standby OLTs are equipment-redundant but not room-diverse.")

    required_pon_ports = len(splitter_records)
    eligible_olts = [
        asset
        for asset in olt_candidates
        if _split_ratio_outputs(asset) >= max(record["split_capacity"] for record in splitter_records)
    ]
    if not eligible_olts:
        raise NetworkPlanningError("The selected splitter ratio exceeds every available OLT capability.")

    def build_olt_side(side: str, root: dict) -> List[dict]:
        mix = _minimum_asset_mix(eligible_olts, required_pon_ports, 0.0, spare_fraction, f"{side} OLT")
        instances = [
            builder.add_instance(
                asset,
                f"AUTO {side} OLT {index + 1}",
                root,
                "olt_primary" if side == "Primary" else "olt_secondary",
                rack_name=f"AUTO-RACK-{_text(root.get('name'))}",
                rack_start_u=index + 1,
                route_anchor=_text(root.get("name")),
                olt_side=side.lower(),
                core_redundancy_role=side.lower(),
            )
            for index, asset in enumerate(mix)
        ]
        capacities = [
            max(_int(asset.get("connections_out")), _int(asset.get("number_of_ports")))
            for asset in mix
        ]
        instance_index = 0
        port_number = 1
        for splitter_index, splitter_record in enumerate(splitter_records, start=1):
            while instance_index < len(capacities) and port_number > capacities[instance_index]:
                instance_index += 1
                port_number = 1
            if instance_index >= len(instances):
                raise NetworkPlanningError("OLT PON-port allocation exceeded selected capacity.")
            input_name = "Input-A" if side == "Primary" else "Input-B"
            protection_group = f"AUTO-PG-{splitter_index}"
            builder.add_connection(
                instances[instance_index],
                f"PON-{port_number}",
                splitter_record["instance"],
                input_name,
                "fibre",
                _text(root.get("name")),
                splitter_record["anchor"],
                connection_role="uplink",
                redundancy_role=side.lower(),
                protection_group=protection_group,
                standby=side != "Primary",
                fibre_count=1,
            )
            if side == "Primary":
                builder.redundancy_groups.append(
                    {
                        "id": protection_group,
                        "technology": "PoLAN",
                        "protected_instance_id": _text(splitter_record["instance"].get("id")),
                        "primary_olt_instance_id": _text(instances[instance_index].get("id")),
                        "secondary_olt_instance_id": "",
                        "protection_type": "Type B / dual-fed protected splitter",
                        "auto_generated": True,
                    }
                )
            else:
                for group in builder.redundancy_groups:
                    if _text(group.get("id")) == protection_group:
                        group["secondary_olt_instance_id"] = _text(instances[instance_index].get("id"))
                        break
            port_number += 1
        return instances

    primary_olts = build_olt_side("Primary", primary_root)
    secondary_olts = build_olt_side("Secondary", secondary_root) if failover else []

    roots = {_text(primary_root.get("name")): primary_root}
    leaves_by_root = {_text(primary_root.get("name")): primary_olts}
    if secondary_olts:
        roots[_text(secondary_root.get("name"))] = secondary_root
        leaves_by_root.setdefault(_text(secondary_root.get("name")), []).extend(secondary_olts)
    _build_core_layer(builder, leaves_by_root, roots, spare_fraction)


def generate_network_design(data: dict, technology: Optional[str] = None) -> dict:
    """Generate and install an optimised network design into ``data``.

    Objective order:
      1. Satisfy every physical endpoint port and its PoE load.
      2. Respect configured spare capacity and PoLAN ONT copper distance.
      3. Keep departments together where topology and distance permit.
      4. Minimise active/passive component count.
      5. Minimise excess capacity and local copper length.
    """

    ensure_network_schema(data)
    _clear_previous_auto_design(data)
    settings = data.setdefault("network_settings", {})
    technology_value = _text(technology or settings.get("technology") or "Traditional")
    technology_value = "PoLAN" if technology_value.lower() == "polan" else "Traditional"
    settings["technology"] = technology_value
    settings.setdefault("spare_capacity_percent", 15.0)
    settings.setdefault("traditional_max_copper_m", 90.0)
    settings.setdefault("polan_max_ont_copper_m", 30.0)
    settings.setdefault("polan_olt_failover", True)
    spare_fraction = max(0.0, _float(settings.get("spare_capacity_percent"), 15.0)) / 100.0

    endpoints, warnings = build_endpoint_demands(data)
    if not endpoints:
        raise NetworkPlanningError("No data-point port demand was found in the project.")

    builder = DesignBuilder(data, technology_value)
    builder.warnings.extend(warnings)
    if technology_value == "Traditional":
        _traditional_design(builder, endpoints, spare_fraction)
    else:
        _polan_design(builder, endpoints, spare_fraction)
    builder.commit()

    assets_by_id = {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    installed_ports = sum(
        _int(assets_by_id.get(_text(item.get("asset_id")), {}).get("number_of_ports"))
        for item in builder.instances
        if _text(item.get("design_role")) in {"access_switch", "ont"}
    )
    installed_poe = sum(
        _float(assets_by_id.get(_text(item.get("asset_id")), {}).get("poe_budget_w"))
        for item in builder.instances
        if _text(item.get("design_role")) in {"access_switch", "ont"}
    )
    demand_ports = sum(endpoint.port_count for endpoint in endpoints)
    demand_poe = sum(endpoint.poe_power_w for endpoint in endpoints)
    role_counts: Dict[str, int] = defaultdict(int)
    for instance in builder.instances:
        role_counts[_text(instance.get("design_role")) or "other"] += 1

    summary = {
        "technology": technology_value,
        "objective": "Minimum feasible component count after department-first, graph-branch-aware proximity clustering, subject to port, PoE, spare-capacity and distance constraints",
        "endpoint_locations": len(endpoints),
        "required_ports": demand_ports,
        "required_poe_w": round(demand_poe, 3),
        "installed_endpoint_ports": installed_ports,
        "installed_poe_budget_w": round(installed_poe, 3),
        "spare_capacity_percent": round(spare_fraction * 100.0, 3),
        "auto_generated_instances": len(builder.instances),
        "auto_generated_connections": len(builder.connections),
        "endpoint_assignments": len(builder.assignments),
        "component_counts": dict(sorted(role_counts.items())),
        "estimated_copper_length_m": round(builder.total_copper_m, 3),
        "estimated_fibre_length_m": round(builder.total_fibre_m, 3),
        "polan_max_ont_copper_m": _float(settings.get("polan_max_ont_copper_m"), 30.0),
        "olt_failover_enabled": bool(settings.get("polan_olt_failover", True)) if technology_value == "PoLAN" else False,
        "warnings": builder.warnings,
    }
    data["network_design_summary"] = summary
    ensure_network_schema(data)
    return summary
