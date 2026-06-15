"""Automatic Traditional LAN and PoLAN topology planning.

The planner consumes the CableRouteResolver routing graph and room/data-point
model.  It writes only records marked ``auto_generated`` and therefore leaves
manually installed network assets and connections untouched.
"""

from __future__ import annotations

from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from dataclasses import dataclass, field
from heapq import heappop, heappush
from itertools import count
import math
import os
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from network_schema import (
    ensure_network_schema,
    normalise_layer_connection_rules,
)
from network_services import ensure_physical_fibre_for_design


class NetworkPlanningError(RuntimeError):
    """Raised when the available network-asset library cannot meet demand."""


_ROUTE_WORKER_GRAPH = None


def _init_route_worker(graph):
    """Initialise one route worker with a read-only copy of the cable graph."""

    global _ROUTE_WORKER_GRAPH
    _ROUTE_WORKER_GRAPH = graph


def _dijkstra_tree(graph, source: str) -> Tuple[Dict[str, float], Dict[str, str]]:
    """Return a shortest-path tree using only plain, picklable structures."""

    distances: Dict[str, float] = {}
    parents: Dict[str, str] = {}
    if source not in graph:
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
        for next_node, weight in graph.get(node, []):
            if next_node in distances:
                continue
            new_cost = cost + float(weight)
            if new_cost + 1e-12 < tentative.get(next_node, float("inf")):
                tentative[next_node] = new_cost
                parents[next_node] = node
                heappush(queue, (new_cost, next_node))
    return distances, parents


def _route_tree_worker(source: str):
    """Process-pool worker returning one complete shortest-path tree."""

    graph = _ROUTE_WORKER_GRAPH or {}
    distances, parents = _dijkstra_tree(graph, source)
    return source, distances, parents


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


def _parse_split_outputs(value) -> int:
    """Return the output side of a split-ratio value such as ``1:32``."""
    text = _text(value)
    if not text:
        return 0
    if ":" in text:
        text = text.rsplit(":", 1)[1]
    try:
        return max(0, int(float(text)))
    except (TypeError, ValueError):
        return 0


def _split_ratio_outputs(asset: dict) -> int:
    """Return the physical output capacity of a passive fibre splitter."""
    parsed = _parse_split_outputs(asset.get("split_ratio"))
    if parsed > 0:
        return parsed
    return max(
        0, _int(asset.get("connections_out")), _int(asset.get("number_of_ports")) - 1
    )


def _olt_supported_split_outputs(asset: dict) -> int:
    """Return the maximum ONTs supported per OLT PON port.

    OLT ``number_of_ports``/``connections_out`` describe the number of PON
    interfaces and must not be interpreted as the optical split ratio.  A
    value of zero means the library does not declare a per-PON split limit.
    """
    for field_name in (
        "max_split_ratio",
        "supported_split_ratio",
        "pon_split_ratio",
        "split_ratio",
        "max_onts_per_pon",
        "max_ont_per_pon",
        "onts_per_pon",
    ):
        parsed = _parse_split_outputs(asset.get(field_name))
        if parsed > 0:
            return parsed
    return 0


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
        self.floor_height_m = _float(
            data.get("building", {}).get("floor_height_m"), 4.0
        )
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
                self._add_point(
                    item.get("name"), item, _text(item.get("kind")) or "location"
                )
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
            for floor_text, coordinates in (
                transition.get("floor_locations") or {}
            ).items():
                if not isinstance(coordinates, dict):
                    continue
                point = dict(coordinates)
                point["floor"] = _int(floor_text)
                self._add_point(
                    f"{transition_id}-F{floor_text}", point, "transition_node"
                )

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
                    weight = (
                        abs(_int(a.get("floor")) - _int(b.get("floor")))
                        * self.floor_height_m
                    )
                    self.graph[a_name].append((b_name, weight))
                    self.graph[b_name].append((a_name, weight))

        # Existing project connections are part of the routing graph where both
        # endpoints are known points.  This preserves user-drawn spurs between
        # rooms/data points and the corridor network.
        for connection in self.data.get("connections", []):
            if not isinstance(connection, dict):
                continue
            a_name = _text(connection.get("from"))
            b_name = _text(connection.get("to"))
            a = self.points.get(a_name)
            b = self.points.get(b_name)
            if not a or not b or a_name == b_name:
                continue
            explicit = _float(
                connection.get("length_m", connection.get("length", 0.0)), 0.0
            )
            weight = explicit if explicit > 0.0 else _distance(a, b, self.floor_height_m)
            if not any(neighbour == b_name for neighbour, _weight in self.graph[a_name]):
                self.graph[a_name].append((b_name, weight))
                self.graph[b_name].append((a_name, weight))

        # Locations and data points commonly sit beside, rather than directly
        # on, a corridor node.  Attach every unconnected endpoint to its nearest
        # same-floor corridor/transition point so fibre and copper routes follow
        # the established graph instead of falling back to a straight A-B line.
        route_nodes_by_floor: Dict[int, List[str]] = defaultdict(list)
        for point_name, point in self.points.items():
            if _text(point.get("kind")).lower() in {"corridor_node", "transition_node"}:
                route_nodes_by_floor[_int(point.get("floor"))].append(point_name)
        for point_name, point in list(self.points.items()):
            if _text(point.get("kind")).lower() in {"corridor_node", "transition_node"}:
                continue
            if self.graph.get(point_name):
                continue
            candidates = route_nodes_by_floor.get(_int(point.get("floor")), [])
            if not candidates:
                continue
            nearest_name = min(
                candidates,
                key=lambda candidate: _distance(point, self.points[candidate], self.floor_height_m),
            )
            weight = _distance(point, self.points[nearest_name], self.floor_height_m)
            self.graph[point_name].append((nearest_name, weight))
            self.graph[nearest_name].append((point_name, weight))

    def _tree(self, source: str) -> Tuple[Dict[str, float], Dict[str, str]]:
        source = _text(source)
        if source in self._trees:
            return self._trees[source]
        distances, parents = _dijkstra_tree(self.graph, source)
        self._trees[source] = (distances, parents)
        return distances, parents

    def precompute_sources(
        self,
        sources: Iterable[str],
        max_workers: int = 0,
        parallel_threshold: int = 4,
    ) -> Tuple[int, int]:
        """Precompute independent route trees in a multiprocessing pool.

        The graph is transferred once to each worker by the pool initializer.
        Results are cached in this object, so all later route requests reuse the
        precomputed shortest-path trees.  Restricted/frozen Python runtimes fall
        back to the same deterministic in-process calculation.
        """

        pending = sorted(
            {
                _text(source)
                for source in sources
                if _text(source) in self.graph and _text(source) not in self._trees
            }
        )
        if not pending:
            return 0, 0

        cpu_count = max(1, os.cpu_count() or 1)
        requested = max(0, int(max_workers or 0))
        workers = min(
            len(pending),
            requested if requested > 0 else max(1, cpu_count - 1),
        )
        threshold = max(1, int(parallel_threshold or 1))

        if workers <= 1 or len(pending) < threshold:
            for source in pending:
                self._trees[source] = _dijkstra_tree(self.graph, source)
            return len(pending), 1

        plain_graph = {name: list(neighbours) for name, neighbours in self.graph.items()}
        try:
            with ProcessPoolExecutor(
                max_workers=workers,
                initializer=_init_route_worker,
                initargs=(plain_graph,),
            ) as pool:
                for source, distances, parents in pool.map(
                    _route_tree_worker, pending, chunksize=1
                ):
                    self._trees[source] = (distances, parents)
            return len(pending), workers
        except Exception:
            for source in pending:
                self._trees[source] = _dijkstra_tree(self.graph, source)
            return len(pending), 1

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
    if (
        not bool(asset.get("requires_poe", False))
        and _text(asset.get("connection_type")).lower() != "wired"
    ):
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
    return (
        max(0.0, _float(settings.get("default_poe_power_w"), 0.0))
        if asset.get("requires_poe")
        else 0.0
    )


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
        department_ids = [
            _text(value) for value in point.get("department_ids", []) if _text(value)
        ]
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
        for index, (asset_id, asset_name, poe_w) in enumerate(
            templates[:target_ports], start=1
        ):
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
            "Missing room-type definitions for: "
            + ", ".join(sorted(missing_room_types))
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
        raise NetworkPlanningError(
            f"No usable {label} assets exist in the network asset library."
        )

    required_ports = max(0, int(math.ceil(actual_ports * (1.0 + spare_fraction))))
    required_poe = max(0, int(math.ceil(actual_poe_w * (1.0 + spare_fraction))))
    max_ports = max(_int(item.get("number_of_ports")) for item in candidates)
    max_poe = max(
        int(math.floor(_float(item.get("poe_budget_w")))) for item in candidates
    )
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
    frontier: Dict[Tuple[int, int], Tuple[List[int], float, int]] = {
        (0, 0): ([], 0.0, 0)
    }

    for device_count in range(1, upper + 1):
        generated: Dict[Tuple[int, int], Tuple[List[int], float, int]] = {}
        for (ports, poe), (indices, power, rack) in frontier.items():
            for candidate_index, asset in enumerate(candidates):
                next_ports = min(port_cap, ports + _int(asset.get("number_of_ports")))
                next_poe = min(
                    poe_cap, poe + int(math.floor(_float(asset.get("poe_budget_w"))))
                )
                next_power = power + _float(asset.get("power_input_w"))
                next_rack = rack + _int(asset.get("rack_units"))
                key = (next_ports, next_poe)
                previous = generated.get(key)
                score = (next_rack, next_power, indices + [candidate_index])
                if previous is None or (next_rack, next_power) < (
                    previous[2],
                    previous[1],
                ):
                    generated[key] = (
                        indices + [candidate_index],
                        next_power,
                        next_rack,
                    )

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
        sorted_states = sorted(
            generated.items(), key=lambda row: (-row[0][0], -row[0][1])
        )
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
    for item in sorted(
        port_items,
        key=lambda row: (-row.poe_power_w, row.endpoint_name, row.endpoint_port),
    ):
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
        item
        for item in data.get("network_asset_instances", [])
        if not bool(item.get("auto_generated"))
    ]
    data["network_connections"] = [
        item
        for item in data.get("network_connections", [])
        if not bool(item.get("auto_generated"))
    ]
    data["network_patch_leads"] = [item for item in data.get("network_patch_leads", []) if not bool(item.get("auto_generated"))]
    data["network_endpoint_assignments"] = [
        item
        for item in data.get("network_endpoint_assignments", [])
        if not bool(item.get("auto_generated"))
    ]
    data["network_assets"] = [
        item
        for item in data.get("network_assets", [])
        if not bool(item.get("auto_network_asset"))
    ]
    data["locations"] = [
        item
        for item in data.get("locations", [])
        if not bool(item.get("auto_network_location"))
    ]
    data["network_redundancy_groups"] = [
        item
        for item in data.get("network_redundancy_groups", [])
        if not bool(item.get("auto_generated"))
    ]
    data["network_fibre_cables"] = [
        item for item in data.get("network_fibre_cables", [])
        if not bool(item.get("auto_generated"))
    ]
    data["network_fibre_nodes"] = [
        item for item in data.get("network_fibre_nodes", [])
        if not bool(item.get("auto_generated"))
    ]
    data["network_fibre_splices"] = [
        item for item in data.get("network_fibre_splices", [])
        if not bool(item.get("auto_generated"))
    ]


class DesignBuilder:
    def __init__(self, data: dict, technology: str):
        self.data = data
        self.technology = technology
        self.settings = data.setdefault("network_settings", {})
        self.graph = RoutingGraph(data)
        self.asset_ids = {
            _text(item.get("id"))
            for item in data.get("network_assets", [])
            if _text(item.get("id"))
        }
        self.instance_ids = {
            _text(item.get("id"))
            for item in data.get("network_asset_instances", [])
            if _text(item.get("id"))
        }
        self.connection_ids = {
            _text(item.get("id"))
            for item in data.get("network_connections", [])
            if _text(item.get("id"))
        }
        self.patch_lead_ids = {_text(item.get("id")) for item in data.get("network_patch_leads", []) if _text(item.get("id"))}
        self.assignment_ids = {
            _text(item.get("id"))
            for item in data.get("network_endpoint_assignments", [])
            if _text(item.get("id"))
        }
        self.location_names = {
            _text(item.get("name"))
            for item in data.get("locations", [])
            if _text(item.get("name"))
        }
        self.warnings: List[str] = []
        self.instances: List[dict] = []
        self.connections: List[dict] = []
        self.assignments: List[dict] = []
        self.patch_leads: List[dict] = []
        self.locations: List[dict] = []
        self.redundancy_groups: List[dict] = []
        self.total_copper_m = 0.0
        self.total_fibre_m = 0.0

    def add_instance(
        self, asset: dict, name: str, location: dict, role: str, **extra
    ) -> dict:
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

    def _asset_for_instance(self, instance: dict) -> dict:
        asset_id = _text(instance.get("asset_id"))
        return next((row for row in self.data.get("network_assets", []) if _text(row.get("id")) == asset_id), {})

    def _port_definition(self, instance: dict, port_name: str, medium: str, preferred_use: str = "") -> dict:
        asset = self._asset_for_instance(instance)
        rows = [row for row in asset.get("port_definitions", []) if isinstance(row, dict)]
        if preferred_use:
            matching = [row for row in rows if _text(row.get("port_use")).lower() == preferred_use]
            if matching:
                rows = matching
        if rows:
            return rows[0]
        return {"port_type": "lc" if medium == "fibre" else "rj45", "port_use": preferred_use or "other"}

    def add_patch_lead(self, *, connection_id: str = "", assignment_id: str = "", instance: dict, port: str, medium: str, peer_instance_id: str = "", peer_port: str = "", endpoint_name: str = "", preferred_use: str = "") -> dict:
        definition = self._port_definition(instance, port, medium, preferred_use)
        default_length = 2.0 if medium in {"copper", "fibre"} else 0.0
        lead = {
            "id": _next_identifier(self.patch_lead_ids, "AUTO-PL-"),
            "connection_id": connection_id,
            "assignment_id": assignment_id,
            "instance_id": _text(instance.get("id")),
            "port": str(port),
            "peer_instance_id": peer_instance_id,
            "peer_port": str(peer_port),
            "endpoint_name": endpoint_name,
            "port_type": _text(definition.get("port_type")) or ("lc" if medium == "fibre" else "rj45"),
            "port_use": _text(definition.get("port_use")) or preferred_use or "patch",
            "medium": medium,
            "cable_specification": "OS2 fibre patch lead" if medium == "fibre" else "Category 6A copper patch lead",
            "length_m": default_length,
            "auto_generated": True,
        }
        self.patch_leads.append(lead)
        return lead

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
        generate_patch_leads = bool(extra.pop("generate_patch_leads", True))
        connection_id = _next_identifier(self.connection_ids, "AUTO-NC-")
        length_m, route_path = (
            self.graph.route(route_source, route_destination)
            if route_source and route_destination
            else (0.0, [])
        )
        cable_specification = {
            "fibre": "OS2 single-mode fibre",
            "copper": "Category 6A",
            "stacking": "Switch stack interconnect",
        }.get(medium, "")
        connection = {
            "id": connection_id,
            "from_instance_id": _text(from_instance.get("id")),
            "from_port": str(from_port),
            "to_instance_id": _text(to_instance.get("id")),
            "to_port": str(to_port),
            "connection_role": "uplink",
            "medium": medium,
            "cable_specification": cable_specification,
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
        if generate_patch_leads and medium in {"copper", "fibre"}:
            self.add_patch_lead(connection_id=connection_id, instance=from_instance, port=str(from_port), medium=medium, peer_instance_id=_text(to_instance.get("id")), peer_port=str(to_port), preferred_use="uplink")
            self.add_patch_lead(connection_id=connection_id, instance=to_instance, port=str(to_port), medium=medium, peer_instance_id=_text(from_instance.get("id")), peer_port=str(from_port), preferred_use="input")
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
        self.add_patch_lead(assignment_id=assignment["id"], instance=instance, port=str(port_name), medium="copper", endpoint_name=item.endpoint_name, preferred_use="client")
        self.total_copper_m += max(0.0, copper_length_m)
        return assignment

    def add_polan_location(
        self, floor: int, x: float, y: float, department_id: str, anchor: str
    ) -> dict:
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
        # Commit generated locations before instances.  Older save paths and
        # interrupted planner runs could retain instances while losing their
        # AUTO-POLAN location rows, which then caused validation failures.
        locations = self.data.setdefault("locations", [])
        existing_location_names = {
            _text(item.get("name"))
            for item in locations
            if isinstance(item, dict) and _text(item.get("name"))
        }
        for location in self.locations:
            name = _text(location.get("name"))
            if name and name not in existing_location_names:
                locations.append(location)
                existing_location_names.add(name)

        # Defensive recovery: every generated instance must have a persisted
        # location row.  Reconstruct only planner-owned AUTO-POLAN locations;
        # manual location references remain subject to normal validation.
        for instance in self.instances:
            location_name = _text(instance.get("location_name"))
            if (
                location_name
                and location_name.startswith("AUTO-POLAN-")
                and location_name not in existing_location_names
            ):
                locations.append(
                    {
                        "name": location_name,
                        "floor": _int(instance.get("floor")),
                        "x": round(_float(instance.get("x")), 3),
                        "y": round(_float(instance.get("y")), 3),
                        "kind": "polan",
                        "department_id": "",
                        "department_ids": [],
                        "anchor_point_name": _text(instance.get("route_anchor")),
                        "auto_network_location": True,
                        "recovered_from_instance": True,
                    }
                )
                existing_location_names.add(location_name)

        self.data.setdefault("network_asset_instances", []).extend(self.instances)
        self.data.setdefault("network_connections", []).extend(self.connections)
        self.data.setdefault("network_endpoint_assignments", []).extend(
            self.assignments
        )
        self.data.setdefault("network_patch_leads", []).extend(self.patch_leads)
        self.data.setdefault("network_redundancy_groups", []).extend(
            self.redundancy_groups
        )


def _locations_by_kind(data: dict, kinds: Iterable[str]) -> List[dict]:
    wanted = {value.lower() for value in kinds}
    return [
        item
        for item in data.get("locations", [])
        if isinstance(item, dict)
        and _text(item.get("kind")).lower() in wanted
        and _text(item.get("name"))
    ]


def _nearest_location(
    point: EndpointDemand | dict,
    locations: Sequence[dict],
    same_floor_first: bool = True,
) -> Optional[dict]:
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
    def declared_layer(asset: dict) -> str:
        return _text(
            asset.get("network_layer") or asset.get("design_layer")
        ).lower()

    def is_access_asset(asset: dict) -> bool:
        name = _text(asset.get("name")).lower()
        return declared_layer(asset) == "access" or (
            "access" in name
            and not any(
                word in name for word in ("core", "distribution", "aggregation")
            )
        )

    explicit = _candidate_assets(
        data,
        "network_switch",
        lambda asset: (
            declared_layer(asset) == "core"
            or "core" in _text(asset.get("name")).lower()
        )
        and not is_access_asset(asset),
    )
    if explicit:
        return explicit

    candidates = _candidate_assets(
        data,
        "network_switch",
        lambda asset: (
            _text(asset.get("output_connection_type")).lower() == "fibre"
            or declared_layer(asset) in {"distribution", "aggregation"}
            or any(
                word in _text(asset.get("name")).lower()
                for word in ("distribution", "aggregation")
            )
        )
        and not is_access_asset(asset),
    )
    if not candidates:
        candidates = _candidate_assets(
            data, "network_switch", lambda asset: not is_access_asset(asset)
        )
    return candidates




def _choose_aggregation_candidates(data: dict) -> List[dict]:
    """Return switches suitable for the aggregation/distribution layer."""

    candidates = _candidate_assets(
        data,
        "network_switch",
        lambda asset: (
            _text(
                asset.get("network_layer") or asset.get("design_layer")
            ).lower()
            in {"aggregation", "distribution"}
            or any(
                word in _text(asset.get("name")).lower()
                for word in ("aggregation", "distribution")
            )
        ),
    )
    return candidates or _choose_core_candidates(data)


def _layer_rule(settings: dict, source: str, target: str) -> Optional[dict]:
    rules = normalise_layer_connection_rules(
        settings.get("layer_connection_rules"),
        _text(settings.get("topology_model")) or "collapsed_core",
        bool(settings.get("redundant_core", True)),
        _int(settings.get("independent_link_count"), 2),
    )
    settings["layer_connection_rules"] = rules
    for rule in rules:
        if (
            bool(rule.get("enabled", True))
            and _text(rule.get("source_layer")).lower() == source
            and _text(rule.get("target_layer")).lower() == target
        ):
            return rule
    return None


def _peer_port_demand(device_count: int, links_per_pair: int) -> int:
    count = max(0, int(device_count))
    links = max(0, int(links_per_pair))
    if count < 2 or links <= 0:
        return 0
    edge_count = 1 if count == 2 else count
    return edge_count * links * 2


def _minimum_layer_mix(
    candidates: Sequence[dict],
    fixed_ports: int,
    per_device_ports: int,
    peer_links_per_pair: int,
    minimum_devices: int,
    spare_fraction: float,
    label: str,
) -> List[dict]:
    """Select a layer mix while accounting for uplinks and peer interconnects."""

    if not candidates:
        raise NetworkPlanningError(
            f"No usable {label} assets exist in the network asset library."
        )
    minimum_devices = max(1, int(minimum_devices or 1))
    device_count = minimum_devices
    selected: List[dict] = []
    for _ in range(32):
        required_ports = (
            max(0, int(fixed_ports))
            + device_count * max(0, int(per_device_ports))
            + _peer_port_demand(device_count, peer_links_per_pair)
        )
        selected = _minimum_asset_mix(
            candidates,
            required_ports,
            0.0,
            spare_fraction,
            label,
        )
        if len(selected) < minimum_devices:
            padding_asset = min(
                candidates,
                key=lambda asset: (
                    max(1, _rack_units_for_asset(asset, 1)),
                    _float(asset.get("power_input_w")),
                    -_int(asset.get("number_of_ports")),
                    _text(asset.get("id")),
                ),
            )
            selected.extend(
                [padding_asset] * (minimum_devices - len(selected))
            )
        next_count = len(selected)
        if next_count == device_count:
            return selected
        device_count = next_count
    raise NetworkPlanningError(
        f"Unable to stabilise the generated {label} device count."
    )


def _build_layer_instances(
    builder: DesignBuilder,
    assets: Sequence[dict],
    roots: Sequence[dict],
    layer: str,
) -> List[dict]:
    if not roots:
        raise NetworkPlanningError(
            f"No root location is available for the {layer} layer."
        )
    role = f"{layer}_switch"
    label = "Aggregation" if layer == "aggregation" else "Core"
    rack_size_u = max(1, _int(builder.settings.get("default_rack_size_u"), 42))
    instances: List[dict] = []
    rack_next_u: Dict[str, int] = defaultdict(lambda: 1)
    for index, asset in enumerate(assets):
        root = roots[index % len(roots)]
        root_name = _text(root.get("name"))
        rack_name = f"AUTO-RACK-{root_name}"
        rack_u = _rack_units_for_asset(asset, 1)
        if rack_next_u[rack_name] + rack_u - 1 > rack_size_u:
            rack_number = 2
            while f"{rack_name}-{rack_number}" in rack_next_u:
                rack_number += 1
            rack_name = f"{rack_name}-{rack_number}"
        instance = builder.add_instance(
            asset,
            f"AUTO {label} {index + 1}",
            root,
            role,
            rack_name=rack_name,
            rack_start_u=rack_next_u[rack_name],
            rack_size_u=rack_size_u,
            route_anchor=root_name,
            network_layer=layer,
        )
        rack_next_u[rack_name] += rack_u
        instances.append(instance)
    return instances


def _instance_total_ports(builder: DesignBuilder, instance: dict) -> int:
    asset = builder._asset_for_instance(instance)
    members = (
        max(1, _int(instance.get("stack_member_count"), 1))
        if bool(instance.get("logical_stack"))
        else 1
    )
    return max(0, _int(asset.get("number_of_ports"))) * members


def _instance_remaining_layer_ports(builder: DesignBuilder, instance: dict) -> int:
    return max(
        0,
        _instance_total_ports(builder, instance)
        - _int(instance.get("_layer_ports_used"), 0),
    )


def _reserve_layer_port(
    builder: DesignBuilder, instance: dict, prefix: str
) -> str:
    used = _int(instance.get("_layer_ports_used"), 0)
    capacity = _instance_total_ports(builder, instance)
    if capacity > 0 and used >= capacity:
        raise NetworkPlanningError(
            f"{instance.get('name') or instance.get('id')} has no free physical port "
            f"for the configured layer connection rules."
        )
    instance["_layer_ports_used"] = used + 1
    key = f"_layer_{prefix.lower()}_ports_used"
    ordinal = _int(instance.get(key), 0) + 1
    instance[key] = ordinal
    return f"{prefix}-{ordinal}"


def _declared_uplink_capacity(builder: DesignBuilder, instance: dict) -> int:
    asset = builder._asset_for_instance(instance)
    members = (
        max(1, _int(instance.get("stack_member_count"), 1))
        if bool(instance.get("logical_stack"))
        else 1
    )
    structured = sum(
        max(0, _int(row.get("port_count")))
        for row in asset.get("port_definitions", [])
        if isinstance(row, dict)
        and _text(row.get("port_use")).lower() == "uplink"
    )
    declared = max(structured, max(0, _int(asset.get("uplink_ports"))))
    return declared * members


def _reserve_target_uplink(builder: DesignBuilder, target: dict) -> str:
    layer = _text(target.get("network_layer") or target.get("design_role")).lower()
    if layer in {"access", "access_switch"}:
        used = _int(target.get("_uplinks_used"), 0)
        declared = _declared_uplink_capacity(builder, target)
        if declared > 0 and used >= declared:
            raise NetworkPlanningError(
                f"{target.get('name') or target.get('id')} exposes only {declared} "
                "uplink port(s), which is insufficient for the configured independent links."
            )
        target["_uplinks_used"] = used + 1
        return f"Uplink-{used + 1}"
    return _reserve_layer_port(builder, target, "Uplink")


def _ordered_sources_with_capacity(
    builder: DesignBuilder, sources: Sequence[dict], offset: int = 0
) -> List[dict]:
    if not sources:
        return []
    rotated = list(sources[offset % len(sources) :]) + list(
        sources[: offset % len(sources)]
    )
    return sorted(
        rotated,
        key=lambda instance: (
            -_instance_remaining_layer_ports(builder, instance),
            _int(instance.get("_layer_ports_used"), 0),
            _text(instance.get("id")),
        ),
    )


def _select_direct_sources(
    builder: DesignBuilder,
    sources: Sequence[dict],
    links: int,
    distinct: int,
    offset: int,
) -> List[Tuple[dict, str]]:
    if len(sources) < distinct:
        raise NetworkPlanningError(
            f"The layer rule requires {distinct} distinct source devices but only "
            f"{len(sources)} were generated."
        )
    selected: List[Tuple[dict, str]] = []
    used_ids: set[str] = set()
    for link_index in range(links):
        candidates = _ordered_sources_with_capacity(
            builder, sources, offset + link_index
        )
        if link_index < distinct:
            candidates = [
                source
                for source in candidates
                if _text(source.get("id")) not in used_ids
            ]
        candidates = [
            source
            for source in candidates
            if _instance_remaining_layer_ports(builder, source) > 0
        ]
        if not candidates:
            raise NetworkPlanningError(
                "The selected upstream switches do not have enough free ports for "
                "the configured independent links."
            )
        source = candidates[0]
        source_id = _text(source.get("id"))
        used_ids.add(source_id)
        selected.append((source, source_id))
    return selected


def _select_aggregation_sources(
    builder: DesignBuilder,
    aggregations: Sequence[dict],
    links: int,
    distinct: int,
    offset: int,
) -> List[Tuple[dict, str]]:
    """Select aggregation links with at least two different upstream core paths."""

    if len(aggregations) < distinct:
        raise NetworkPlanningError(
            f"The access rule requires {distinct} distinct aggregation switches but "
            f"only {len(aggregations)} were generated."
        )
    required_core_diversity = min(2, distinct)
    all_core_ids = {
        core_id
        for aggregation in aggregations
        for core_id in aggregation.get("upstream_core_ids", [])
        if _text(core_id)
    }
    if len(all_core_ids) < required_core_diversity:
        raise NetworkPlanningError(
            "Independent access links cannot be sourced through two different cores. "
            "Increase the core count or change the Core → Aggregation rule."
        )

    rotated = list(aggregations[offset % len(aggregations) :]) + list(
        aggregations[: offset % len(aggregations)]
    )
    selected: List[Tuple[dict, str]] = []
    used_sources: set[str] = set()
    used_cores: set[str] = set()

    for link_index in range(links):
        candidates: List[Tuple[int, int, int, str, dict, str]] = []
        for aggregation in rotated:
            aggregation_id = _text(aggregation.get("id"))
            if link_index < distinct and aggregation_id in used_sources:
                continue
            if _instance_remaining_layer_ports(builder, aggregation) <= 0:
                continue
            core_ids = [
                _text(value)
                for value in aggregation.get("upstream_core_ids", [])
                if _text(value)
            ]
            for core_id in core_ids:
                candidates.append(
                    (
                        0 if core_id not in used_cores else 1,
                        0 if aggregation_id not in used_sources else 1,
                        -_instance_remaining_layer_ports(builder, aggregation),
                        aggregation_id,
                        aggregation,
                        core_id,
                    )
                )
        if not candidates:
            raise NetworkPlanningError(
                "The aggregation layer cannot satisfy the configured independent "
                "access links with the available ports and source diversity."
            )
        candidates.sort(key=lambda row: row[:4])
        _, _, _, _, aggregation, core_id = candidates[0]
        selected.append((aggregation, core_id))
        used_sources.add(_text(aggregation.get("id")))
        used_cores.add(core_id)

    if len(used_cores) < required_core_diversity:
        raise NetworkPlanningError(
            "The generated access links do not resolve through two different core switches."
        )
    return selected


def _connect_layer_targets(
    builder: DesignBuilder,
    sources: Sequence[dict],
    targets: Sequence[dict],
    rule: dict,
    source_layer: str,
    target_layer: str,
    require_core_path_diversity: bool = False,
) -> None:
    links = max(1, _int(rule.get("links_per_target"), 1))
    distinct = max(
        1,
        min(links, _int(rule.get("minimum_distinct_sources"), 1)),
    )
    rule_id = _text(rule.get("id")) or f"{source_layer}_to_{target_layer}"
    for target_index, target in enumerate(targets):
        if require_core_path_diversity:
            selected = _select_aggregation_sources(
                builder, sources, links, distinct, target_index
            )
        else:
            selected = _select_direct_sources(
                builder, sources, links, distinct, target_index
            )
        protection_group = f"AUTO-LAYER-{target.get('id')}"
        source_ids: List[str] = []
        core_ids: List[str] = []
        for link_index, (source, core_id) in enumerate(selected, start=1):
            target_port = _reserve_target_uplink(builder, target)
            source_port = _reserve_layer_port(builder, source, "Downlink")
            target_anchor = _text(target.get("route_anchor")) or _text(
                target.get("location_name")
            )
            source_anchor = _text(source.get("route_anchor")) or _text(
                source.get("location_name")
            )
            builder.add_connection(
                target,
                target_port,
                source,
                source_port,
                "fibre",
                target_anchor,
                source_anchor,
                connection_role="uplink",
                redundancy_role=(
                    "primary" if link_index == 1 else f"independent-{link_index}"
                ),
                protection_group=protection_group,
                layer_rule_id=rule_id,
                source_layer=source_layer,
                target_layer=target_layer,
                independent_link_index=link_index,
                source_core_instance_id=core_id,
                fibre_count=2,
            )
            source_ids.append(_text(source.get("id")))
            core_ids.append(core_id)
            if target_layer == "aggregation":
                upstream = target.setdefault("upstream_core_ids", [])
                if core_id and core_id not in upstream:
                    upstream.append(core_id)

        if links > 1:
            builder.redundancy_groups.append(
                {
                    "id": protection_group,
                    "technology": builder.technology,
                    "protected_instance_id": _text(target.get("id")),
                    "primary_olt_instance_id": "",
                    "secondary_olt_instance_id": "",
                    "protection_type": "independent_layer_uplinks",
                    "source_layer": source_layer,
                    "target_layer": target_layer,
                    "source_instance_ids": source_ids,
                    "source_core_instance_ids": core_ids,
                    "required_distinct_sources": distinct,
                    "auto_generated": True,
                }
            )


def _connect_peer_ring(
    builder: DesignBuilder,
    instances: Sequence[dict],
    rule: Optional[dict],
    layer: str,
) -> None:
    if rule is None or len(instances) < 2:
        return
    links = max(1, _int(rule.get("links_per_target"), 1))
    if len(instances) == 2:
        pairs = [(instances[0], instances[1])]
    else:
        pairs = [
            (instances[index], instances[(index + 1) % len(instances)])
            for index in range(len(instances))
        ]
    for pair_index, (left, right) in enumerate(pairs, start=1):
        for link_index in range(1, links + 1):
            left_port = _reserve_layer_port(builder, left, "Peer")
            right_port = _reserve_layer_port(builder, right, "Peer")
            left_anchor = _text(left.get("route_anchor")) or _text(
                left.get("location_name")
            )
            right_anchor = _text(right.get("route_anchor")) or _text(
                right.get("location_name")
            )
            builder.add_connection(
                left,
                left_port,
                right,
                right_port,
                "fibre",
                left_anchor,
                right_anchor,
                connection_role="uplink",
                redundancy_role="cross_link",
                protection_group=f"AUTO-{layer.upper()}-PEER-{pair_index}",
                layer_rule_id=_text(rule.get("id")) or f"{layer}_peer",
                source_layer=layer,
                target_layer=layer,
                independent_link_index=link_index,
                fibre_count=2,
            )


def _build_traditional_layer_topology(
    builder: DesignBuilder,
    access_switches: Sequence[dict],
    comms_rooms: Sequence[dict],
    spare_fraction: float,
) -> List[dict]:
    """Build collapsed-core or three-tier active topology from layer rules."""

    mer_locations = _locations_by_kind(builder.data, {"mer"})
    if not mer_locations:
        mer_locations = [comms_rooms[0]]
        builder.warnings.append(
            "No MER exists; the first comms room was used as the network root."
        )
    roots = list(mer_locations)
    settings = builder.settings
    topology_model = _text(settings.get("topology_model")).lower()
    if topology_model not in {"collapsed_core", "three_tier"}:
        topology_model = "collapsed_core"
    settings["topology_model"] = topology_model
    settings["layer_connection_rules"] = normalise_layer_connection_rules(
        settings.get("layer_connection_rules"),
        topology_model,
        bool(settings.get("redundant_core", True)),
        _int(settings.get("independent_link_count"), 2),
    )

    core_peer = _layer_rule(settings, "core", "core")
    core_candidates = _choose_core_candidates(builder.data)
    if not core_candidates:
        raise NetworkPlanningError(
            "No core switch asset is available for the configured topology."
        )

    if topology_model == "collapsed_core":
        core_access = _layer_rule(settings, "core", "access")
        if core_access is None:
            raise NetworkPlanningError(
                "Collapsed-core planning requires an enabled Core → Access layer rule."
            )
        core_minimum = max(
            1,
            _int(core_access.get("minimum_distinct_sources"), 1),
            2 if core_peer is not None else 1,
        )
        core_mix = _minimum_layer_mix(
            core_candidates,
            len(access_switches) * _int(core_access.get("links_per_target"), 1),
            0,
            _int(core_peer.get("links_per_target"), 0) if core_peer else 0,
            core_minimum,
            spare_fraction,
            "core switch",
        )
        cores = _build_layer_instances(builder, core_mix, roots, "core")
        _connect_peer_ring(builder, cores, core_peer, "core")
        _connect_layer_targets(
            builder,
            cores,
            access_switches,
            core_access,
            "core",
            "access",
        )
        return cores

    core_aggregation = _layer_rule(settings, "core", "aggregation")
    aggregation_access = _layer_rule(settings, "aggregation", "access")
    aggregation_peer = _layer_rule(settings, "aggregation", "aggregation")
    if core_aggregation is None or aggregation_access is None:
        raise NetworkPlanningError(
            "Three-tier planning requires enabled Core → Aggregation and "
            "Aggregation → Access rules."
        )

    aggregation_candidates = _choose_aggregation_candidates(builder.data)
    if not aggregation_candidates:
        raise NetworkPlanningError(
            "No aggregation/distribution switch asset is available for the three-tier topology."
        )
    aggregation_minimum = max(
        1,
        _int(aggregation_access.get("minimum_distinct_sources"), 1),
        2 if aggregation_peer is not None else 1,
    )
    aggregation_mix = _minimum_layer_mix(
        aggregation_candidates,
        len(access_switches)
        * _int(aggregation_access.get("links_per_target"), 1),
        _int(core_aggregation.get("links_per_target"), 1),
        (
            _int(aggregation_peer.get("links_per_target"), 0)
            if aggregation_peer
            else 0
        ),
        aggregation_minimum,
        spare_fraction,
        "aggregation switch",
    )
    aggregations = _build_layer_instances(
        builder, aggregation_mix, roots, "aggregation"
    )

    core_minimum = max(
        1,
        _int(core_aggregation.get("minimum_distinct_sources"), 1),
        2 if core_peer is not None else 1,
        2
        if _int(aggregation_access.get("minimum_distinct_sources"), 1) >= 2
        else 1,
    )
    core_mix = _minimum_layer_mix(
        core_candidates,
        len(aggregations) * _int(core_aggregation.get("links_per_target"), 1),
        0,
        _int(core_peer.get("links_per_target"), 0) if core_peer else 0,
        core_minimum,
        spare_fraction,
        "core switch",
    )
    cores = _build_layer_instances(builder, core_mix, roots, "core")

    _connect_peer_ring(builder, cores, core_peer, "core")
    _connect_layer_targets(
        builder,
        cores,
        aggregations,
        core_aggregation,
        "core",
        "aggregation",
    )
    _connect_peer_ring(
        builder, aggregations, aggregation_peer, "aggregation"
    )
    _connect_layer_targets(
        builder,
        aggregations,
        access_switches,
        aggregation_access,
        "aggregation",
        "access",
        require_core_path_diversity=(
            _int(aggregation_access.get("minimum_distinct_sources"), 1) >= 2
        ),
    )
    return cores + aggregations


def _rack_units_for_asset(asset: dict, member_count: int = 1) -> int:
    member_count = max(1, int(member_count or 1))
    if _text(asset.get("asset_type")) == "network_switch":
        allowance = max(
            0,
            _int(
                asset.get("switch_rack_unit_allowance"),
                _int(asset.get("rack_units"), 1),
            ),
        )
        return max(1, allowance) * member_count
    return max(0, _int(asset.get("rack_units"), 1))


def _assert_generated_capacity(builder: DesignBuilder, spare_fraction: float) -> None:
    assets_by_id = {
        _text(item.get("id")): item
        for item in builder.data.get("network_assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    loads: Dict[str, Tuple[int, float]] = defaultdict(lambda: (0, 0.0))
    for assignment in builder.assignments:
        instance_id = _text(assignment.get("network_instance_id"))
        if not instance_id:
            continue
        ports, poe = loads[instance_id]
        loads[instance_id] = (
            ports + 1,
            poe + max(0.0, _float(assignment.get("poe_power_w"))),
        )

    for instance in builder.instances:
        instance_id = _text(instance.get("id"))
        role = _text(instance.get("design_role"))
        if role not in {"access_switch", "ont"}:
            continue
        asset = assets_by_id.get(_text(instance.get("asset_id")), {})
        stack_members = (
            max(1, _int(instance.get("stack_member_count"), 1))
            if bool(instance.get("logical_stack"))
            else 1
        )
        port_capacity = max(0, _int(asset.get("number_of_ports"))) * stack_members
        poe_capacity = max(0.0, _float(asset.get("poe_budget_w"))) * stack_members
        used_ports, used_poe = loads.get(instance_id, (0, 0.0))
        required_ports = int(math.ceil(used_ports * (1.0 + spare_fraction)))
        required_poe = used_poe * (1.0 + spare_fraction)
        if port_capacity and required_ports > port_capacity:
            raise NetworkPlanningError(
                f"Generated {instance.get('name') or instance_id} would use {used_ports} ports "
                f"plus spare capacity ({required_ports} required) but only has {port_capacity}."
            )
        if required_poe > poe_capacity + 1e-9:
            raise NetworkPlanningError(
                f"Generated {instance.get('name') or instance_id} would use {used_poe:.1f} W PoE "
                f"plus spare capacity ({required_poe:.1f} W required) but only has {poe_capacity:.1f} W."
            )


def _build_core_layer(
    builder: DesignBuilder,
    leaves_by_root: Dict[str, List[dict]],
    roots: Dict[str, dict],
    spare_fraction: float,
) -> List[dict]:
    cores: List[dict] = []
    candidates = _choose_core_candidates(builder.data)
    if not candidates:
        builder.warnings.append(
            "No core/distribution switch asset was available; uplinks were not terminated at a core."
        )
        return cores

    for root_name, leaves in leaves_by_root.items():
        if not leaves:
            continue
        root = roots[root_name]
        mix = _minimum_asset_mix(
            candidates, len(leaves), 0.0, spare_fraction, "core switch"
        )
        rack_size_u = max(1, _int(builder.settings.get("default_rack_size_u"), 42))
        rack_index = 1
        next_rack_u = 1
        core_instances = []
        for index, asset in enumerate(mix):
            rack_u = _rack_units_for_asset(asset, 1)
            if next_rack_u + rack_u - 1 > rack_size_u and next_rack_u > 1:
                rack_index += 1
                next_rack_u = 1
            core_instances.append(
                builder.add_instance(
                    asset,
                    f"AUTO Core {root_name} {index + 1}",
                    root,
                    "core_switch",
                    rack_name=(
                        f"AUTO-RACK-{root_name}"
                        if rack_index == 1
                        else f"AUTO-RACK-{root_name}-{rack_index}"
                    ),
                    rack_start_u=next_rack_u,
                    rack_size_u=rack_size_u,
                )
            )
            next_rack_u += rack_u
        # Spare capacity is already included in the selected aggregate mix.
        capacities = [max(0, _int(asset.get("number_of_ports"))) for asset in mix]
        core_index = 0
        core_port = 1
        for leaf in leaves:
            while core_index < len(capacities) and core_port > capacities[core_index]:
                core_index += 1
                core_port = 1
            if core_index >= len(core_instances):
                raise NetworkPlanningError(
                    "Core switch port allocation exceeded the selected capacity."
                )
            leaf_location = _text(leaf.get("route_anchor")) or _text(
                leaf.get("location_name")
            )
            builder.add_connection(
                leaf,
                f"Uplink-{_int(leaf.get('_uplinks_used'), 0) + 1}",
                core_instances[core_index],
                str(core_port),
                "fibre",
                leaf_location,
                root_name,
                redundancy_role=_text(leaf.get("core_redundancy_role")),
                fibre_count=1,
            )
            leaf["_uplinks_used"] = _int(leaf.get("_uplinks_used"), 0) + 1
            core_port += 1
        cores.extend(core_instances)
    return cores


def _traditional_design(
    builder: DesignBuilder, endpoints: Sequence[EndpointDemand], spare_fraction: float
) -> None:
    comms_rooms = _locations_by_kind(builder.data, {"comms_room"})
    if not comms_rooms:
        raise NetworkPlanningError(
            "Traditional network planning requires at least one comms_room location."
        )
    room_map = {_text(item.get("name")): item for item in comms_rooms}
    max_copper_m = max(
        1.0, _float(builder.settings.get("traditional_max_copper_m"), 90.0)
    )

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
                "x": sum(item.x * item.port_count for item in group)
                / max(1, sum(item.port_count for item in group)),
                "y": sum(item.y * item.port_count for item in group)
                / max(1, sum(item.port_count for item in group)),
            }
            dominant = _nearest_location(centre, comms_rooms)
        for endpoint in group:
            selected = dominant
            if selected is not None:
                route_length, _ = builder.graph.route(
                    _text(selected.get("name")), endpoint.name
                )
                route_length += endpoint.extension_distance_m
                if route_length > max_copper_m:
                    own = room_map.get(endpoint.existing_comms_room)
                    if own is None:
                        own = _nearest_location(endpoint, comms_rooms)
                    selected = own
            if selected is None:
                raise NetworkPlanningError(
                    f"No comms room could be selected for {endpoint.name}."
                )
            endpoint_room[endpoint.name] = selected

    room_ports: Dict[str, List[PortDemand]] = defaultdict(list)
    for endpoint in endpoints:
        room_ports[_text(endpoint_room[endpoint.name].get("name"))].extend(
            endpoint.ports
        )

    switch_candidates = _candidate_assets(
        builder.data,
        "network_switch",
        lambda asset: _text(asset.get("output_connection_type")).lower() == "copper"
        and _int(asset.get("number_of_ports")) > 0
        and not any(
            word in _text(asset.get("name")).lower()
            for word in ("core", "distribution", "aggregation")
        ),
    )
    if not switch_candidates:
        raise NetworkPlanningError(
            "No copper-output network switch assets are available for Traditional design."
        )

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
        switch_number = 1
        stack_number = 1
        rack_size_u = max(1, _int(builder.settings.get("default_rack_size_u"), 42))
        rack_index = 1
        next_rack_u = 1
        index = 0
        while index < len(mix):
            asset = mix[index]
            per_member_rack_u = max(1, _rack_units_for_asset(asset, 1))
            max_stack_members = (
                max(1, _int(asset.get("max_stack_members"), 1))
                if bool(asset.get("supports_stacking"))
                else 1
            )
            # A logical stack must fit completely inside one physical rack.
            # Limit stack size by the available rack height so a generated stack
            # can never overflow into a non-existent U position.
            max_stack_members = min(
                max_stack_members,
                max(1, rack_size_u // per_member_rack_u),
            )
            group_end = index + 1
            while (
                group_end < len(mix)
                and group_end - index < max_stack_members
                and _text(mix[group_end].get("id")) == _text(asset.get("id"))
            ):
                group_end += 1

            group_assignments = packed[index:group_end]
            member_count = group_end - index
            is_stack = member_count > 1
            instance_name = (
                f"AUTO {room_name} Access Stack {stack_number}"
                if is_stack
                else f"AUTO {room_name} Access Switch {switch_number}"
            )
            rack_u = _rack_units_for_asset(asset, member_count)
            if rack_u > rack_size_u:
                raise NetworkPlanningError(
                    f"{instance_name} requires {rack_u}U but the configured rack size is "
                    f"only {rack_size_u}U. Reduce the stack size or increase default_rack_size_u."
                )
            if next_rack_u + rack_u - 1 > rack_size_u:
                rack_index += 1
                next_rack_u = 1

            instance = builder.add_instance(
                asset,
                instance_name,
                room,
                "access_switch",
                rack_name=(
                    f"AUTO-RACK-{room_name}"
                    if rack_index == 1
                    else f"AUTO-RACK-{room_name}-{rack_index}"
                ),
                rack_start_u=next_rack_u,
                rack_size_u=rack_size_u,
                route_anchor=room_name,
                logical_stack=is_stack,
                stack_member_count=member_count,
                stack_member_asset_id=_text(asset.get("id")) if is_stack else "",
                stack_max_members=max_stack_members if is_stack else 1,
                stack_interconnect_count=max(0, member_count - 1) if is_stack else 0,
                stack_interconnect_medium="stacking",
                stack_interconnect_specification=(
                    "Cisco StackWise stacking cables" if is_stack else ""
                ),
            )
            instance["rack_size_u"] = rack_size_u
            for member_offset, assigned in enumerate(group_assignments, start=1):
                for port_number, item in enumerate(assigned, start=1):
                    length_m, path = builder.graph.route(room_name, item.endpoint_name)
                    length_m += item.extension_distance_m
                    builder.add_assignment(
                        item,
                        instance,
                        (
                            f"{member_offset}/{port_number}"
                            if is_stack
                            else str(port_number)
                        ),
                        length_m,
                        path,
                        source_location=room_name,
                        stack_member=member_offset if is_stack else 0,
                    )
            access_switches.append(instance)
            switch_number += member_count
            next_rack_u += rack_u
            if is_stack:
                stack_number += 1
            index = group_end

    _build_traditional_layer_topology(
        builder, access_switches, comms_rooms, spare_fraction
    )


def _fits_any_ont(
    candidates: Sequence[dict], ports: int, poe_w: float, spare_fraction: float
) -> bool:
    required_ports = int(math.ceil(ports * (1.0 + spare_fraction)))
    required_poe = poe_w * (1.0 + spare_fraction)
    return any(
        _int(asset.get("number_of_ports")) >= required_ports
        and _float(asset.get("poe_budget_w")) + 1e-9 >= required_poe
        for asset in candidates
    )


def _choose_single_ont(
    candidates: Sequence[dict], items: Sequence[PortDemand], spare_fraction: float
) -> dict:
    ports = len(items)
    poe = sum(item.poe_power_w for item in items)
    feasible = [
        asset
        for asset in candidates
        if _int(asset.get("number_of_ports"))
        >= math.ceil(ports * (1.0 + spare_fraction))
        and _float(asset.get("poe_budget_w")) + 1e-9 >= poe * (1.0 + spare_fraction)
    ]
    if not feasible:
        raise NetworkPlanningError(
            f"No ONT can support a cluster of {ports} ports and {poe:.1f} W PoE."
        )
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
    graph: RoutingGraph,
) -> List[Tuple[dict, List[PortDemand], EndpointDemand]]:
    """Cluster PoLAN ports using cable-graph distance only."""

    groups: Dict[Tuple[str, int, str], List[EndpointDemand]] = defaultdict(list)
    for endpoint in endpoints:
        groups[(endpoint.department_id, endpoint.floor, endpoint.existing_comms_room)].append(endpoint)

    route_cache: Dict[Tuple[str, str], Tuple[float, List[str]]] = {}

    def routed(a_name: str, b_name: str) -> Tuple[float, List[str]]:
        key = (a_name, b_name)
        if key not in route_cache:
            route_cache[key] = graph.route(a_name, b_name)
        return route_cache[key]

    clusters: List[Tuple[dict, List[PortDemand], EndpointDemand]] = []
    for group_key in sorted(groups, key=lambda value: (value[1], value[0], value[2])):
        points = groups[group_key]
        remaining: Dict[str, deque[PortDemand]] = {
            point.name: deque(sorted(point.ports, key=lambda item: -item.poe_power_w))
            for point in points
        }
        point_map = {point.name: point for point in points}

        while any(remaining.values()):
            seed_name = max(
                (name for name, queue in remaining.items() if queue),
                key=lambda name: (
                    len(remaining[name]),
                    sum(item.poe_power_w for item in remaining[name]),
                ),
            )
            seed = point_map[seed_name]
            reachable: List[Tuple[float, EndpointDemand]] = []
            for point in points:
                if not remaining[point.name]:
                    continue
                try:
                    distance, _ = routed(seed.name, point.name)
                except NetworkPlanningError:
                    continue
                if distance + point.extension_distance_m <= max_copper_m + 1e-9:
                    reachable.append((distance, point))

            candidates = [
                point
                for _distance_m, point in sorted(
                    reachable,
                    key=lambda row: (row[0], -len(remaining[row[1].name]), row[1].name),
                )
            ]

            cluster_items: List[PortDemand] = []
            represented: List[EndpointDemand] = []
            cluster_poe = 0.0
            for point in candidates:
                took_any = False
                queue = remaining[point.name]
                while queue:
                    item = queue[0]
                    next_ports = len(cluster_items) + 1
                    next_poe = cluster_poe + item.poe_power_w
                    if not _fits_any_ont(ont_candidates, next_ports, next_poe, spare_fraction):
                        break
                    cluster_items.append(queue.popleft())
                    cluster_poe = next_poe
                    took_any = True
                if took_any:
                    represented.append(point)

            if not cluster_items:
                item = remaining[seed_name][0]
                raise NetworkPlanningError(
                    f"No ONT can serve {item.endpoint_name} port {item.endpoint_port} within "
                    f"the configured {max_copper_m:.1f} m routed copper limit."
                )

            counts: Dict[str, int] = defaultdict(int)
            for item in cluster_items:
                counts[item.endpoint_name] += 1

            feasible_medoids: List[Tuple[float, float, EndpointDemand]] = []
            for candidate in represented:
                weighted_total = 0.0
                worst = 0.0
                feasible = True
                for other in represented:
                    try:
                        distance, _ = routed(candidate.name, other.name)
                    except NetworkPlanningError:
                        feasible = False
                        break
                    total = distance + other.extension_distance_m
                    if total > max_copper_m + 1e-9:
                        feasible = False
                        break
                    weighted_total += counts[other.name] * total
                    worst = max(worst, total)
                if feasible:
                    feasible_medoids.append((weighted_total, worst, candidate))

            if not feasible_medoids:
                names = ", ".join(sorted(counts))
                raise NetworkPlanningError(
                    f"No graph-connected ONT position can serve PoLAN endpoints {names} within "
                    f"{max_copper_m:.1f} m."
                )

            medoid = min(feasible_medoids, key=lambda row: (row[0], row[1], row[2].name))[2]
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
    output_count = _split_ratio_outputs(base_asset)
    asset["split_ratio"] = f"2:{output_count}"
    asset["split_input_count"] = 2
    asset["split_output_count"] = output_count
    asset["connections_in"] = 2
    asset["connections_out"] = output_count
    asset["number_of_ports"] = output_count + 2
    asset["input_connection_type"] = "fibre"
    asset["output_connection_type"] = "fibre"
    asset["uplink_connection_type"] = "fibre"
    asset["port_definitions"] = [
        {
            "port_type": "lc",
            "port_count": 2,
            "port_use": "input",
            "name_prefix": "Input",
            "explicit_names": ["Input-A", "Input-B"],
        },
        {
            "port_type": "lc",
            "port_count": output_count,
            "port_use": "output",
            "name_prefix": "Output",
            "explicit_names": [],
        },
    ]
    asset["notes"] = (
        _text(base_asset.get("notes"))
        + " Auto-generated 2:N protected splitter/coupler for primary and standby OLT feeder inputs."
    ).strip()
    asset["auto_network_asset"] = True
    builder.data.setdefault("network_assets", []).append(asset)
    builder.asset_ids.add(existing_id)
    return asset


def _polan_design(
    builder: DesignBuilder, endpoints: Sequence[EndpointDemand], spare_fraction: float
) -> None:
    max_copper_m = max(
        1.0, _float(builder.settings.get("polan_max_ont_copper_m"), 30.0)
    )
    ont_candidates = _candidate_assets(
        builder.data,
        "optical_network_terminal",
        lambda asset: _int(asset.get("number_of_ports")) > 0
        and _text(asset.get("output_connection_type")).lower() == "copper",
    )
    if not ont_candidates:
        raise NetworkPlanningError(
            "No copper-output ONT assets exist in the network asset library."
        )

    clusters = _cluster_polan_ports(
        endpoints, ont_candidates, spare_fraction, max_copper_m, builder.graph
    )
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
            route_length, route_path = builder.graph.route(medoid.name, endpoint.name)
            copper = route_length + item.extension_distance_m
            if copper > max_copper_m + 1e-9:
                raise NetworkPlanningError(
                    f"ONT placement for {item.endpoint_name} requires {copper:.1f} m of routed copper; "
                    f"the configured limit is {max_copper_m:.1f} m."
                )
            builder.add_assignment(
                item,
                ont,
                str(port_number),
                copper,
                route_path,
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
                "source_room": (
                    max(source_counts, key=source_counts.get) if source_counts else ""
                ),
                "endpoint_names": sorted({item.endpoint_name for item in port_items}),
            }
        )

    olt_candidates = _candidate_assets(
        builder.data,
        "optical_line_terminal",
        lambda asset: max(
            _int(asset.get("connections_out")), _int(asset.get("number_of_ports"))
        )
        > 0,
    )
    if not olt_candidates:
        raise NetworkPlanningError(
            "No OLT with a defined PON-port count exists in the network asset library."
        )
    declared_olt_split_limits = [
        _olt_supported_split_outputs(asset)
        for asset in olt_candidates
        if _olt_supported_split_outputs(asset) > 0
    ]
    olt_max_split = max(declared_olt_split_limits, default=0)
    splitter_candidates = _candidate_assets(
        builder.data,
        "fibre_splitter",
        lambda asset: (
            _split_ratio_outputs(asset) > 0
            and (olt_max_split <= 0 or _split_ratio_outputs(asset) <= olt_max_split)
        ),
    )
    if not splitter_candidates:
        available_ratios = sorted(
            {
                _split_ratio_outputs(asset)
                for asset in _candidate_assets(builder.data, "fibre_splitter")
                if _split_ratio_outputs(asset) > 0
            }
        )
        if not available_ratios:
            raise NetworkPlanningError(
                "No fibre splitter with a valid output split ratio exists in the network asset library."
            )
        raise NetworkPlanningError(
            "No fibre splitter is compatible with the declared OLT per-PON split limit "
            f"of 1:{olt_max_split}. Available splitter ratios: "
            + ", ".join(f"1:{value}" for value in available_ratios)
            + ". Update the OLT supported split-ratio field or add a compatible splitter."
        )
    if olt_max_split <= 0:
        builder.warnings.append(
            "The OLT asset library does not declare a maximum ONT split ratio per PON port; "
            "splitter compatibility was therefore based on available splitter definitions."
        )

    largest_splitter = max(splitter_candidates, key=_split_ratio_outputs)
    configured_splitter_limit = max(
        1, _int(builder.settings.get("polan_max_onts_per_splitter"), 16)
    )
    splitter_capacity = min(
        configured_splitter_limit,
        max(
            1,
            int(
                math.floor(
                    _split_ratio_outputs(largest_splitter) / (1.0 + spare_fraction)
                )
            ),
        ),
    )
    failover = bool(builder.settings.get("polan_olt_failover", True))

    # PoLAN splitters are passive distribution devices and must not be placed in
    # conventional comms rooms.  MER locations remain the OLT/core roots only.
    # Existing PoLAN locations can host splitters; where no suitable PoLAN
    # location exists on the floor, create an auto-generated PoLAN distribution
    # location near the ONT group.
    polan_distribution_locations = _locations_by_kind(builder.data, {"polan"})

    splitter_records: List[dict] = []
    onts_by_floor: Dict[int, List[dict]] = defaultdict(list)
    for record in ont_records:
        onts_by_floor[record["floor"]].append(record)

    max_splitter_route_m = max(
        0.0,
        _float(builder.settings.get("polan_max_splitter_ont_route_m"), 120.0),
    )

    route_cache: Dict[Tuple[str, str], Tuple[float, List[str]]] = {}

    def routed(source: str, destination: str) -> Tuple[float, List[str]]:
        key = (source, destination)
        if key not in route_cache:
            route_cache[key] = builder.graph.route(source, destination)
        return route_cache[key]

    def endpoint_route_from_anchor(anchor: str, endpoint_name: str) -> float:
        distance, _ = routed(anchor, endpoint_name)
        endpoint = endpoint_lookup[endpoint_name]
        return distance + endpoint.extension_distance_m

    corridor_candidates_by_floor: Dict[int, List[str]] = defaultdict(list)
    for name, point in builder.graph.points.items():
        if _text(point.get("kind")).lower() in {"corridor_node", "transition_node"}:
            corridor_candidates_by_floor[_int(point.get("floor"))].append(name)
    for floor in corridor_candidates_by_floor:
        corridor_candidates_by_floor[floor].sort()

    used_splitter_anchors: set[str] = set()

    def group_endpoint_names(group: Sequence[dict]) -> List[str]:
        return sorted({
            endpoint_name
            for row in group
            for endpoint_name in row.get("endpoint_names", [])
        })

    def feasible_splitter_anchors(
        group: Sequence[dict], floor: int
    ) -> List[Tuple[float, float, str]]:
        """Return corridor/transition nodes that serve the complete group.

        Every distance is measured along the cable-routing graph from the
        candidate spatial graph node to each endpoint device.  Endpoint
        extension distance is then added to the graph route.
        """
        endpoint_names = group_endpoint_names(group)
        feasible: List[Tuple[float, float, str]] = []
        for anchor in corridor_candidates_by_floor.get(floor, []):
            endpoint_distances: List[float] = []
            try:
                for endpoint_name in endpoint_names:
                    endpoint_distances.append(
                        endpoint_route_from_anchor(anchor, endpoint_name)
                    )
            except NetworkPlanningError:
                continue
            worst = max(endpoint_distances, default=0.0)
            if max_splitter_route_m > 0.0 and worst > max_splitter_route_m + 1e-9:
                continue
            feasible.append((sum(endpoint_distances), worst, anchor))
        return feasible

    def graph_groups(records: Sequence[dict], floor: int) -> List[List[dict]]:
        """Build only groups having a common routed splitter anchor.

        The previous capacity-only grouping could combine ONTs from separate
        spatial graph branches and fail later even though smaller valid groups
        existed.  This incremental grouping retains a candidate only when the
        enlarged group still has at least one corridor/transition node that can
        reach every endpoint within the configured routed limit.
        """
        remaining = sorted(records, key=lambda row: _text(row["instance"].get("id")))
        groups: List[List[dict]] = []
        while remaining:
            seed = remaining.pop(0)
            group = [seed]
            if not feasible_splitter_anchors(group, floor):
                names = ", ".join(group_endpoint_names(group))
                raise NetworkPlanningError(
                    f"No corridor/transition graph node on floor {floor} can reach endpoint devices "
                    f"{names} within the configured {max_splitter_route_m:.1f} m routed limit. "
                    "Check that each endpoint is connected to the corridor graph or increase the limit."
                )

            ranked = []
            for row in remaining:
                try:
                    distance, _ = routed(seed["anchor"], row["anchor"])
                except NetworkPlanningError:
                    continue
                ranked.append((distance, _text(row["instance"].get("id")), row))
            ranked.sort(key=lambda item: (item[0], item[1]))

            selected_ids = set()
            for _distance_m, _row_id, row in ranked:
                if len(group) >= splitter_capacity:
                    break
                trial_group = group + [row]
                if not feasible_splitter_anchors(trial_group, floor):
                    continue
                group.append(row)
                selected_ids.add(id(row))

            remaining = [row for row in remaining if id(row) not in selected_ids]
            groups.append(group)
        return groups

    def best_splitter_anchor(group: Sequence[dict], floor: int) -> str:
        candidates = corridor_candidates_by_floor.get(floor, [])
        if not candidates:
            raise NetworkPlanningError(
                f"No corridor or transition graph nodes exist on floor {floor} for splitter placement."
            )

        scored = []
        for total, worst, anchor in feasible_splitter_anchors(group, floor):
            reuse_penalty = 1 if anchor in used_splitter_anchors else 0
            scored.append((reuse_penalty, total, worst, anchor))

        if not scored:
            names = ", ".join(group_endpoint_names(group))
            raise NetworkPlanningError(
                f"No corridor/transition graph node on floor {floor} can reach endpoint devices "
                f"{names} within the configured {max_splitter_route_m:.1f} m routed limit."
            )

        _reuse, _total, _worst, anchor = min(
            scored, key=lambda item: (item[0], item[1], item[2], item[3])
        )
        used_splitter_anchors.add(anchor)
        return anchor

    location_by_anchor: Dict[str, dict] = {}
    for location in polan_distribution_locations:
        anchor = _text(location.get("anchor_point_name"))
        if anchor:
            location_by_anchor.setdefault(anchor, location)

    for floor in sorted(onts_by_floor):
        records = onts_by_floor[floor]
        for group in graph_groups(records, floor):
            required_outputs = int(math.ceil(len(group) * (1.0 + spare_fraction)))
            base_asset = min(
                (
                    asset
                    for asset in splitter_candidates
                    if _split_ratio_outputs(asset) >= required_outputs
                ),
                key=_split_ratio_outputs,
                default=largest_splitter,
            )
            asset = (
                _protected_splitter_asset(builder, base_asset)
                if failover
                else base_asset
            )

            anchor = best_splitter_anchor(group, floor)
            location = location_by_anchor.get(anchor)
            if location is None:
                anchor_point = builder.graph.points[anchor]
                location = builder.add_polan_location(
                    floor,
                    _float(anchor_point.get("x")),
                    _float(anchor_point.get("y")),
                    group[0]["department_id"],
                    anchor,
                )
                polan_distribution_locations.append(location)
                location_by_anchor[anchor] = location

            splitter = builder.add_instance(
                asset,
                f"AUTO Splitter F{floor} {len(splitter_records) + 1}",
                location,
                "protected_splitter" if failover else "splitter",
                route_anchor=anchor,
                protected=failover,
            )
            for output_number, ont_record in enumerate(group, start=1):
                route_length, _route_path = routed(anchor, ont_record["anchor"])
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
        raise NetworkPlanningError(
            "PoLAN planning requires at least one MER for OLT placement."
        )
    primary_root = mer_locations[0]
    secondary_root = mer_locations[1] if len(mer_locations) > 1 else mer_locations[0]
    if failover and len(mer_locations) < 2:
        builder.warnings.append(
            "Only one MER exists; primary and standby OLTs are equipment-redundant but not room-diverse."
        )

    required_pon_ports = len(splitter_records)
    required_split_capacity = max(
        record["split_capacity"] for record in splitter_records
    )
    eligible_olts = [
        asset
        for asset in olt_candidates
        if (
            _olt_supported_split_outputs(asset) <= 0
            or _olt_supported_split_outputs(asset) >= required_split_capacity
        )
    ]
    if not eligible_olts:
        raise NetworkPlanningError(
            "The selected splitter ratio exceeds every available OLT capability."
        )

    def build_olt_side(side: str, root: dict) -> List[dict]:
        mix = _minimum_asset_mix(
            eligible_olts, required_pon_ports, 0.0, spare_fraction, f"{side} OLT"
        )
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
            while (
                instance_index < len(capacities)
                and port_number > capacities[instance_index]
            ):
                instance_index += 1
                port_number = 1
            if instance_index >= len(instances):
                raise NetworkPlanningError(
                    "OLT PON-port allocation exceeded selected capacity."
                )
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
                        "protected_instance_id": _text(
                            splitter_record["instance"].get("id")
                        ),
                        "primary_olt_instance_id": _text(
                            instances[instance_index].get("id")
                        ),
                        "secondary_olt_instance_id": "",
                        "protection_type": "Type B / dual-fed protected splitter",
                        "auto_generated": True,
                    }
                )
            else:
                for group in builder.redundancy_groups:
                    if _text(group.get("id")) == protection_group:
                        group["secondary_olt_instance_id"] = _text(
                            instances[instance_index].get("id")
                        )
                        break
            port_number += 1
        return instances

    primary_olts = build_olt_side("Primary", primary_root)
    secondary_olts = build_olt_side("Secondary", secondary_root) if failover else []

    roots = {_text(primary_root.get("name")): primary_root}
    leaves_by_root = {_text(primary_root.get("name")): primary_olts}
    if secondary_olts:
        roots[_text(secondary_root.get("name"))] = secondary_root
        leaves_by_root.setdefault(_text(secondary_root.get("name")), []).extend(
            secondary_olts
        )
    _build_core_layer(builder, leaves_by_root, roots, spare_fraction)



def _fibre_patch_panel_asset(builder: DesignBuilder) -> dict:
    """Return a usable fibre patch panel, creating a conservative 24-port 1U model when absent."""
    candidates = _candidate_assets(
        builder.data,
        "patch_panel",
        lambda asset: _text(asset.get("patch_panel_type")).lower() == "fibre"
        and max(_int(asset.get("number_of_ports")), _int(asset.get("connections_in")), _int(asset.get("connections_out"))) > 0,
    )
    if candidates:
        return min(
            candidates,
            key=lambda asset: (
                max(1, _int(asset.get("rack_units"), 1)),
                -max(_int(asset.get("number_of_ports")), _int(asset.get("connections_in")), _int(asset.get("connections_out"))),
                _text(asset.get("id")),
            ),
        )

    asset_id = "AUTO-FIBRE-PATCH-PANEL-24"
    for asset in builder.data.get("network_assets", []):
        if _text(asset.get("id")) == asset_id:
            return asset
    asset = {
        "id": asset_id,
        "name": "24-port OS2 fibre patch panel",
        "asset_type": "patch_panel",
        "patch_panel_type": "fibre",
        "number_of_ports": 24,
        "connections_in": 24,
        "connections_out": 24,
        "uplink_ports": 0,
        "port_definitions": [
            {"port_type": "lc", "port_count": 24, "port_use": "patch", "name_prefix": "LC"}
        ],
        "input_connection_type": "fibre",
        "output_connection_type": "fibre",
        "rack_units": 1,
        "power_input_w": 0.0,
        "notes": "Automatically generated fibre patch-panel allowance.",
        "auto_network_asset": True,
    }
    builder.data.setdefault("network_assets", []).append(asset)
    builder.asset_ids.add(asset_id)
    builder.warnings.append(
        "No fibre patch-panel asset was available; a 24-port 1U OS2 patch panel was generated automatically."
    )
    return asset


def _install_fibre_patch_panels(builder: DesignBuilder, spare_fraction: float) -> None:
    """Install rack fibre panels without changing the logical topology.

    The original active-device connection remains the authoritative logical
    topology link.  Separate hidden physical connections model the equipment
    patch leads and the panel-to-panel backbone.  Fibre links within one rack
    remain direct and do not require patch panels.
    """
    instance_by_id = {_text(row.get("id")): row for row in builder.instances}

    def endpoint_asset_type(connection: dict, field: str) -> str:
        instance = instance_by_id.get(_text(connection.get(field)))
        return _asset_type(builder._asset_for_instance(instance)) if instance else ""

    def rack_key(instance: dict) -> Tuple[int, str, str]:
        return (
            _int(instance.get("floor")),
            _text(instance.get("location_name")),
            _text(instance.get("rack_name")),
        )

    eligible: List[dict] = []
    for row in list(builder.connections):
        if _text(row.get("medium")).lower() != "fibre":
            continue
        if bool(row.get("physical_connection")):
            continue
        if endpoint_asset_type(row, "from_instance_id") == "fibre_splitter":
            continue
        if endpoint_asset_type(row, "to_instance_id") == "fibre_splitter":
            continue
        a = instance_by_id.get(_text(row.get("from_instance_id")))
        b = instance_by_id.get(_text(row.get("to_instance_id")))
        if not a or not b:
            continue
        if rack_key(a) == rack_key(b):
            row["physical_segment"] = "intra_rack"
            continue
        row["logical_topology"] = True
        eligible.append(row)

    if not eligible:
        return

    terminations: Dict[Tuple[int, str, str], int] = defaultdict(int)
    for connection in eligible:
        fibres = max(1, _int(connection.get("fibre_count"), 1))
        for field in ("from_instance_id", "to_instance_id"):
            instance = instance_by_id.get(_text(connection.get(field)))
            if instance:
                terminations[rack_key(instance)] += fibres

    panel_asset = _fibre_patch_panel_asset(builder)
    capacity = max(
        1,
        _int(panel_asset.get("number_of_ports")),
        _int(panel_asset.get("connections_in")),
        _int(panel_asset.get("connections_out")),
    )
    locations = {
        _text(row.get("name")): row
        for row in builder.data.get("locations", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }
    panels_by_rack: Dict[Tuple[int, str, str], List[dict]] = defaultdict(list)
    for key, used in sorted(terminations.items()):
        floor, location_name, rack_name = key
        required = max(1, int(math.ceil(used * (1.0 + spare_fraction))))
        count_needed = max(1, int(math.ceil(required / capacity)))
        location = locations.get(location_name) or {
            "name": location_name, "floor": floor, "x": 0.0, "y": 0.0
        }
        for index in range(count_needed):
            panel = builder.add_instance(
                panel_asset,
                f"AUTO {location_name} {rack_name or 'Rack'} Fibre Patch Panel {index + 1}",
                location,
                "fibre_patch_panel",
                rack_name=rack_name,
                target_rack_name=rack_name,
                rack_start_u=0,
                rack_size_u=max(1, _int(builder.settings.get("default_rack_size_u"), 42)),
                route_anchor=location_name,
                termination_count=max(0, min(capacity, used - index * capacity)),
                cabinet_patch_panel=True,
            )
            panels_by_rack[key].append(panel)
            instance_by_id[_text(panel.get("id"))] = panel

    next_port: Dict[Tuple[int, str, str], int] = defaultdict(int)

    def allocate(instance: dict) -> Tuple[dict, str]:
        key = rack_key(instance)
        index = next_port[key]
        panel_index, port_index = divmod(index, capacity)
        panels = panels_by_rack.get(key, [])
        if panel_index >= len(panels):
            raise NetworkPlanningError(
                f"Fibre patch-panel capacity was exceeded for {key[1]} / {key[2] or 'unassigned rack'}."
            )
        next_port[key] += 1
        return panels[panel_index], f"LC-{port_index + 1}"

    for logical in eligible:
        a = instance_by_id.get(_text(logical.get("from_instance_id")))
        b = instance_by_id.get(_text(logical.get("to_instance_id")))
        if not a or not b:
            continue
        a_panel, a_panel_port = allocate(a)
        b_panel, b_panel_port = allocate(b)
        parent_id = _text(logical.get("id"))
        common = {
            "generate_patch_leads": False,
            "topology_hidden": True,
            "physical_connection": True,
            "parent_logical_connection_id": parent_id,
            "fibre_count": max(1, _int(logical.get("fibre_count"), 1)),
        }
        builder.add_connection(
            a, _text(logical.get("from_port")), a_panel, a_panel_port, "fibre",
            physical_segment="fibre_patch_cable", cable_specification="OS2 fibre patch lead",
            **common,
        )
        backbone = builder.add_connection(
            a_panel, a_panel_port, b_panel, b_panel_port, "fibre",
            _text(a.get("route_anchor")) or _text(a.get("location_name")),
            _text(b.get("route_anchor")) or _text(b.get("location_name")),
            physical_segment="fibre_backbone", **common,
        )
        # Preserve the logical link's route metrics on the permanent backbone.
        backbone["route_path"] = list(logical.get("route_path", []))
        backbone["length_m"] = _float(logical.get("length_m"))
        backbone["redundancy_role"] = _text(logical.get("redundancy_role"))
        backbone["protection_group"] = _text(logical.get("protection_group"))
        builder.add_connection(
            b_panel, b_panel_port, b, _text(logical.get("to_port")), "fibre",
            physical_segment="fibre_patch_cable", cable_specification="OS2 fibre patch lead",
            **common,
        )


def _copper_patch_panel_asset(builder: DesignBuilder) -> dict:
    candidates = _candidate_assets(
        builder.data,
        "patch_panel",
        lambda asset: _text(asset.get("patch_panel_type")).lower() == "copper"
        and _int(asset.get("number_of_ports")) > 0,
    )
    if candidates:
        return min(candidates, key=lambda a: (max(1, _int(a.get("rack_units"), 1)), -_int(a.get("number_of_ports")), _text(a.get("id"))))
    asset = {
        "id": "AUTO-COPPER-PATCH-PANEL-48",
        "name": "48-port Category 6A patch panel",
        "asset_type": "patch_panel",
        "patch_panel_type": "copper",
        "number_of_ports": 48,
        "connections_in": 48,
        "connections_out": 48,
        "port_definitions": [{"port_type": "rj45", "port_count": 48, "port_use": "patch", "name_prefix": "Port"}],
        "input_connection_type": "copper",
        "output_connection_type": "copper",
        "rack_units": 1,
        "power_input_w": 0.0,
        "notes": "Automatically generated copper patch-panel allowance.",
        "auto_network_asset": True,
    }
    builder.data.setdefault("network_assets", []).append(asset)
    builder.asset_ids.add(asset["id"])
    return asset


def _install_copper_patch_panels(builder: DesignBuilder, spare_fraction: float) -> None:
    """Provide copper panels and hidden patch-cable connections for access switches."""
    assignments_by_instance: Dict[str, List[dict]] = defaultdict(list)
    for assignment in builder.assignments:
        instance_id = _text(assignment.get("network_instance_id"))
        if instance_id:
            assignments_by_instance[instance_id].append(assignment)
    if not assignments_by_instance:
        return
    instance_by_id = {_text(row.get("id")): row for row in builder.instances}
    locations = {_text(row.get("name")): row for row in builder.data.get("locations", []) if isinstance(row, dict)}
    panel_asset = _copper_patch_panel_asset(builder)
    capacity = max(1, _int(panel_asset.get("number_of_ports"), 48))
    for instance_id, assignments in sorted(assignments_by_instance.items()):
        switch = instance_by_id.get(instance_id)
        if not switch:
            continue
        asset = builder._asset_for_instance(switch)
        if _asset_type(asset) != "network_switch":
            continue
        required = max(1, int(math.ceil(len(assignments) * (1.0 + spare_fraction))))
        panel_count = max(1, int(math.ceil(required / capacity)))
        location_name = _text(switch.get("location_name"))
        location = locations.get(location_name) or {
            "name": location_name, "floor": _int(switch.get("floor")), "x": _float(switch.get("x")), "y": _float(switch.get("y"))
        }
        panels = []
        for index in range(panel_count):
            panel = builder.add_instance(
                panel_asset,
                f"AUTO {location_name} Copper Patch Panel {instance_id} {index + 1}",
                location,
                "copper_patch_panel",
                rack_name=_text(switch.get("rack_name")),
                target_rack_name=_text(switch.get("rack_name")),
                rack_start_u=0,
                rack_size_u=max(1, _int(builder.settings.get("default_rack_size_u"), 42)),
                route_anchor=location_name,
                associated_switch_id=instance_id,
            )
            panels.append(panel)
        for index, assignment in enumerate(assignments):
            panel, port_index = panels[index // capacity], (index % capacity) + 1
            builder.add_connection(
                switch,
                _text(assignment.get("network_port")),
                panel,
                f"Port-{port_index}",
                "copper",
                generate_patch_leads=False,
                topology_hidden=True,
                physical_connection=True,
                physical_segment="copper_patch_cable",
                assignment_id=_text(assignment.get("id")),
                cable_specification="Category 6A copper patch lead",
                length_m=2.0,
            )


def _support_rack_asset(
    builder: DesignBuilder,
    *,
    asset_id: str,
    name: str,
    asset_type: str,
    rack_units: int,
    notes: str,
) -> dict:
    """Return an existing support asset or create a conservative generated one."""
    for asset in builder.data.get("network_assets", []):
        if _text(asset.get("id")) == asset_id:
            return asset
    asset = {
        "id": asset_id,
        "name": name,
        "asset_type": asset_type,
        "number_of_ports": 0,
        "connections_in": 0,
        "connections_out": 0,
        "rack_units": max(1, int(rack_units)),
        "power_input_w": 0.0,
        "notes": notes,
        "auto_network_asset": True,
    }
    builder.data.setdefault("network_assets", []).append(asset)
    builder.asset_ids.add(asset_id)
    return asset


def _ups_asset(builder: DesignBuilder) -> dict:
    candidates = [
        asset
        for asset in builder.data.get("network_assets", [])
        if isinstance(asset, dict)
        and _asset_type(asset) in {"ups", "rack_ups", "uninterruptible_power_supply"}
        and _int(asset.get("rack_units"), 0) > 0
    ]
    if candidates:
        return min(
            candidates,
            key=lambda asset: (
                max(1, _int(asset.get("rack_units"), 1)),
                _text(asset.get("id")),
            ),
        )
    builder.warnings.append(
        "No rack UPS asset was available; a conservative 2U rack UPS was generated automatically."
    )
    return _support_rack_asset(
        builder,
        asset_id="AUTO-RACK-UPS-2U",
        name="2U rack UPS",
        asset_type="ups",
        rack_units=2,
        notes="Automatically generated dual-UPS rack allowance.",
    )


def _cable_manager_asset(builder: DesignBuilder) -> dict:
    candidates = [
        asset
        for asset in builder.data.get("network_assets", [])
        if isinstance(asset, dict)
        and (
            _asset_type(asset) in {"cable_management", "cable_manager"}
            or "cable management" in _text(asset.get("name")).lower()
        )
        and _int(asset.get("rack_units"), 0) > 0
    ]
    if candidates:
        return min(
            candidates,
            key=lambda asset: (
                max(1, _int(asset.get("rack_units"), 1)),
                _text(asset.get("id")),
            ),
        )
    return _support_rack_asset(
        builder,
        asset_id="AUTO-CABLE-MANAGER-1U",
        name="1U horizontal cable management loop",
        asset_type="cable_management",
        rack_units=1,
        notes="Automatically generated 1U horizontal cable-management allowance.",
    )


def _repack_generated_racks(builder: DesignBuilder) -> None:
    """Arrange generated rack equipment using physical cabling conventions.

    Fibre patch panels are placed at the top of their serving rack. Copper
    patch panels are placed immediately above a cable manager and their
    associated switch. Powered racks receive a dual-UPS allowance at the
    bottom. Rack support assets are real installed instances and therefore
    appear in the rack elevation, but remain absent from logical topology.
    """
    assets = {
        _text(row.get("id")): row
        for row in builder.data.get("network_assets", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    rack_size_u = max(1, _int(builder.settings.get("default_rack_size_u"), 42))
    ups_asset = _ups_asset(builder)
    manager_asset = _cable_manager_asset(builder)
    assets[_text(ups_asset.get("id"))] = ups_asset
    assets[_text(manager_asset.get("id"))] = manager_asset
    ups_u = max(1, _int(ups_asset.get("rack_units"), 2))
    manager_u = max(1, _int(manager_asset.get("rack_units"), 1))
    ups_reserved_u = ups_u * 2 + 1

    groups: Dict[Tuple[int, str], List[dict]] = defaultdict(list)
    for instance in list(builder.instances):
        role = _text(instance.get("design_role"))
        if role in {"rack_ups", "cable_management"}:
            continue
        asset = assets.get(_text(instance.get("asset_id")), {})
        units = _rack_units_for_asset(
            asset,
            max(1, _int(instance.get("stack_member_count"), 1))
            if bool(instance.get("logical_stack")) else 1,
        )
        if units <= 0:
            continue
        instance["_calculated_rack_units"] = units
        instance["_powered_rack_item"] = _float(asset.get("power_input_w"), 0.0) > 0.0
        groups[(_int(instance.get("floor")), _text(instance.get("location_name")))].append(instance)

    locations = {
        _text(row.get("name")): row
        for row in builder.data.get("locations", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }

    for (floor, location_name), items in sorted(groups.items()):
        location = locations.get(location_name) or {
            "name": location_name, "floor": floor, "x": 0.0, "y": 0.0
        }
        fibre_panels = [i for i in items if _text(i.get("design_role")) == "fibre_patch_panel"]
        copper_panels = [i for i in items if _text(i.get("design_role")) == "copper_patch_panel"]
        equipment = [i for i in items if i not in fibre_panels and i not in copper_panels]
        copper_by_switch: Dict[str, List[dict]] = defaultdict(list)
        for panel in copper_panels:
            copper_by_switch[_text(panel.get("associated_switch_id"))].append(panel)

        racks: List[dict] = []
        rack_by_name: Dict[str, dict] = {}

        def create_rack(preferred_name: str = "") -> dict:
            index = len(racks) + 1
            name = preferred_name or (
                f"AUTO-RACK-{location_name}" if index == 1 else f"AUTO-RACK-{location_name}-{index}"
            )
            if name in rack_by_name:
                return rack_by_name[name]
            rack = {"index": index, "name": name, "bottom_next": 1, "top_next": rack_size_u, "ups_added": False}
            racks.append(rack)
            rack_by_name[name] = rack
            return rack

        def ensure_ups(rack: dict) -> None:
            if rack["ups_added"]:
                return
            if rack["bottom_next"] + ups_reserved_u - 1 > rack["top_next"]:
                raise NetworkPlanningError(f"A {rack_size_u}U rack at {location_name} cannot accommodate the dual UPS allowance.")
            first = rack["bottom_next"]
            second = first + ups_u + 1
            for number, start_u in ((1, first), (2, second)):
                builder.add_instance(
                    ups_asset,
                    f"AUTO {location_name} {rack['name']} UPS {number}",
                    location,
                    "rack_ups",
                    rack_name=rack["name"], rack_start_u=start_u,
                    rack_size_u=rack_size_u, route_anchor=location_name,
                    ups_pair_number=number,
                )
            rack["bottom_next"] = second + ups_u
            rack["ups_added"] = True

        def choose_bottom(units: int, powered: bool, preferred: str = "") -> dict:
            candidates = []
            if preferred:
                candidates.append(create_rack(preferred))
            candidates.extend(r for r in racks if r not in candidates)
            for rack in candidates:
                prospective = rack["bottom_next"] + (ups_reserved_u if powered and not rack["ups_added"] else 0)
                if prospective + units - 1 <= rack["top_next"]:
                    return rack
            return create_rack()

        def place_bottom(instance: dict, rack: dict, units: int, powered: bool = False) -> None:
            if powered:
                ensure_ups(rack)
            if rack["bottom_next"] + units - 1 > rack["top_next"]:
                raise NetworkPlanningError(f"Rack {rack['name']} at {location_name} has insufficient free rack units.")
            instance["rack_name"] = rack["name"]
            instance["rack_start_u"] = rack["bottom_next"]
            instance["rack_size_u"] = rack_size_u
            rack["bottom_next"] += units

        equipment.sort(key=lambda i: (_text(i.get("target_rack_name") or i.get("rack_name")), _text(i.get("design_role")), _text(i.get("name"))))
        for instance in equipment:
            asset = assets.get(_text(instance.get("asset_id")), {})
            units = max(1, _int(instance.get("_calculated_rack_units"), 1))
            powered = bool(instance.get("_powered_rack_item"))
            preferred = _text(instance.get("target_rack_name") or instance.get("rack_name"))
            assembly_extra = 0
            panels = copper_by_switch.get(_text(instance.get("id")), [])
            if panels:
                assembly_extra = sum(max(1, _int(p.get("_calculated_rack_units"), 1)) + manager_u for p in panels)
            rack = choose_bottom(units + assembly_extra, powered, preferred)
            place_bottom(instance, rack, units, powered)
            # Bottom-to-top order is switch, cable manager, copper patch panel;
            # therefore the rack elevation reads panel / manager / switch.
            for panel_index, panel in enumerate(panels, start=1):
                manager = builder.add_instance(
                    manager_asset,
                    f"AUTO {location_name} Copper Cable Manager {instance.get('id')} {panel_index}",
                    location,
                    "cable_management",
                    rack_name=rack["name"], rack_start_u=rack["bottom_next"],
                    rack_size_u=rack_size_u, route_anchor=location_name,
                    associated_patch_panel_id=_text(panel.get("id")),
                    associated_switch_id=_text(instance.get("id")),
                )
                rack["bottom_next"] += manager_u
                panel_u = max(1, _int(panel.get("_calculated_rack_units"), 1))
                place_bottom(panel, rack, panel_u, False)

        # Any unassociated copper panels are still installed with a manager.
        associated_ids = {id(p) for rows in copper_by_switch.values() for p in rows}
        for panel in copper_panels:
            if id(panel) in associated_ids:
                continue
            panel_u = max(1, _int(panel.get("_calculated_rack_units"), 1))
            rack = choose_bottom(panel_u + manager_u, False, _text(panel.get("target_rack_name")))
            builder.add_instance(
                manager_asset, f"AUTO {location_name} Copper Cable Manager {_text(panel.get('id'))}",
                location, "cable_management", rack_name=rack["name"],
                rack_start_u=rack["bottom_next"], rack_size_u=rack_size_u,
                route_anchor=location_name, associated_patch_panel_id=_text(panel.get("id")),
            )
            rack["bottom_next"] += manager_u
            place_bottom(panel, rack, panel_u, False)

        # Fibre panels are always mounted at the top of their intended rack,
        # with a horizontal manager directly beneath each panel.
        fibre_panels.sort(key=lambda p: (_text(p.get("target_rack_name")), _text(p.get("name"))))
        for index, panel in enumerate(fibre_panels, start=1):
            preferred = _text(panel.get("target_rack_name") or panel.get("rack_name"))
            rack = create_rack(preferred) if preferred else (racks[0] if racks else create_rack())
            panel_u = max(1, _int(panel.get("_calculated_rack_units"), 1))
            if rack["top_next"] - panel_u - manager_u + 1 < rack["bottom_next"]:
                rack = create_rack()
            panel_start = rack["top_next"] - panel_u + 1
            manager_start = panel_start - manager_u
            panel["rack_name"] = rack["name"]
            panel["rack_start_u"] = panel_start
            panel["rack_size_u"] = rack_size_u
            builder.add_instance(
                manager_asset,
                f"AUTO {location_name} Fibre Cable Manager {index}",
                location,
                "cable_management",
                rack_name=rack["name"], rack_start_u=manager_start,
                rack_size_u=rack_size_u, route_anchor=location_name,
                associated_patch_panel_id=_text(panel.get("id")),
            )
            rack["top_next"] = manager_start - 1

        for instance in items:
            instance.pop("_calculated_rack_units", None)
            instance.pop("_powered_rack_item", None)


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
    settings.setdefault("topology_model", "collapsed_core")
    settings.setdefault("independent_link_count", 2)
    settings.setdefault("layer_connection_rules", [])
    settings.setdefault("polan_max_onts_per_splitter", 16)
    settings.setdefault("polan_max_splitter_to_ont_m", 150.0)
    settings.setdefault("auto_planner_max_workers", 0)
    settings.setdefault("auto_planner_parallel_threshold", 4)
    spare_fraction = (
        max(0.0, _float(settings.get("spare_capacity_percent"), 15.0)) / 100.0
    )

    endpoints, warnings = build_endpoint_demands(data)
    if not endpoints:
        raise NetworkPlanningError(
            "No data-point port demand was found in the project."
        )

    builder = DesignBuilder(data, technology_value)
    builder.warnings.extend(warnings)

    # Shortest-path tree generation dominates large projects.  Precompute each
    # independent source tree in parallel before allocation and grouping begin.
    if technology_value == "Traditional":
        route_sources = {
            _text(item.get("name"))
            for item in data.get("locations", [])
            if isinstance(item, dict)
            and _text(item.get("kind")).lower() in {"comms_room", "mer"}
        }
    else:
        route_sources = {endpoint.name for endpoint in endpoints}
        route_sources.update(
            name
            for name, point in builder.graph.points.items()
            if _text(point.get("kind")).lower()
            in {"corridor_node", "transition_node"}
        )
        route_sources.update(
            _text(item.get("name"))
            for item in data.get("locations", [])
            if isinstance(item, dict) and _text(item.get("kind")).lower() == "mer"
        )

    precomputed_sources, route_workers_used = builder.graph.precompute_sources(
        route_sources,
        max_workers=max(0, _int(settings.get("auto_planner_max_workers"), 0)),
        parallel_threshold=max(
            1, _int(settings.get("auto_planner_parallel_threshold"), 4)
        ),
    )

    if technology_value == "Traditional":
        _traditional_design(builder, endpoints, spare_fraction)
    else:
        _polan_design(builder, endpoints, spare_fraction)

    # Fibre terminations require physical patch-panel capacity and rack space.
    # Repack the complete generated equipment set afterwards so OLTs, cores and
    # panels share the location racks without ever exceeding the configured U.
    _install_copper_patch_panels(builder, spare_fraction)
    _install_fibre_patch_panels(builder, spare_fraction)
    _repack_generated_racks(builder)

    _assert_generated_capacity(builder, spare_fraction)
    builder.commit()
    physical_fibre_summary = ensure_physical_fibre_for_design(data, replace_auto=False)

    assets_by_id = {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    installed_ports = sum(
        _int(assets_by_id.get(_text(item.get("asset_id")), {}).get("number_of_ports"))
        * (
            max(1, _int(item.get("stack_member_count"), 1))
            if bool(item.get("logical_stack"))
            else 1
        )
        for item in builder.instances
        if _text(item.get("design_role")) in {"access_switch", "ont"}
    )
    installed_poe = sum(
        _float(assets_by_id.get(_text(item.get("asset_id")), {}).get("poe_budget_w"))
        * (
            max(1, _int(item.get("stack_member_count"), 1))
            if bool(item.get("logical_stack"))
            else 1
        )
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
        "topology_model": (
            _text(settings.get("topology_model"))
            if technology_value == "Traditional"
            else "polan"
        ),
        "independent_link_count": (
            _int(settings.get("independent_link_count"), 2)
            if technology_value == "Traditional"
            else 1
        ),
        "layer_connection_rules": (
            deepcopy(settings.get("layer_connection_rules", []))
            if technology_value == "Traditional"
            else []
        ),
        "objective": "Minimum feasible component count after department-first, graph-branch-aware proximity clustering, subject to port, PoE, spare-capacity, distance and configured layer-diversity constraints",
        "endpoint_locations": len(endpoints),
        "required_ports": demand_ports,
        "required_poe_w": round(demand_poe, 3),
        "installed_endpoint_ports": installed_ports,
        "installed_poe_budget_w": round(installed_poe, 3),
        "spare_capacity_percent": round(spare_fraction * 100.0, 3),
        "auto_generated_instances": len(builder.instances),
        "auto_generated_connections": len(builder.connections),
        "route_sources_precomputed": precomputed_sources,
        "route_workers_used": route_workers_used,
        "endpoint_assignments": len(builder.assignments),
        "component_counts": dict(sorted(role_counts.items())),
        "estimated_copper_length_m": round(builder.total_copper_m, 3),
        "estimated_fibre_length_m": round(builder.total_fibre_m, 3),
        "physical_fibre_cables": physical_fibre_summary.get("cable_count", 0),
        "physical_fibre_nodes": physical_fibre_summary.get("node_count", 0),
        "physical_fibre_layer": deepcopy(settings.get("physical_fibre_layer", {})),
        "polan_max_ont_copper_m": _float(settings.get("polan_max_ont_copper_m"), 30.0),
        "polan_max_onts_per_splitter": _int(
            settings.get("polan_max_onts_per_splitter"), 16
        ),
        "polan_max_splitter_to_ont_m": _float(
            settings.get("polan_max_splitter_to_ont_m"), 150.0
        ),
        "olt_failover_enabled": (
            bool(settings.get("polan_olt_failover", True))
            if technology_value == "PoLAN"
            else False
        ),
        "warnings": builder.warnings,
    }
    data["network_design_summary"] = summary
    ensure_network_schema(data)
    return summary
