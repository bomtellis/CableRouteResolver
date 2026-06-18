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
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from network_schema import (
    PLUGGABLE_OPTIC_PORT_TYPES,
    compatible_port_speeds,
    default_port_speeds,
    ensure_network_schema,
    normalise_layer_connection_rules,
    normalise_manufacturer_preferences,
    normalise_port_speeds,
    optic_form_factors_for_cage,
    port_speed_label,
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
    expected_bandwidth_mbps: float
    expected_packet_rate_pps: float
    room_type_id: str
    selected_network_port: str = ""
    selected_link_speed_mbps: int = 0


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

    @property
    def expected_bandwidth_mbps(self) -> float:
        return sum(item.expected_bandwidth_mbps for item in self.ports)

    @property
    def expected_packet_rate_pps(self) -> float:
        return sum(item.expected_packet_rate_pps for item in self.ports)


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
        templates: List[Tuple[str, str, float, float, float]] = []

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
                divisor = max(1, data_points_per_asset)
                bandwidth_per_port = max(
                    0.0,
                    _float(
                        asset.get(
                            "expected_bandwidth_mbps",
                            settings.get("default_expected_bandwidth_mbps", 0.0),
                        )
                    ),
                ) / divisor
                packets_per_port = max(
                    0.0,
                    _float(
                        asset.get(
                            "expected_packet_rate_pps",
                            settings.get("default_expected_packet_rate_pps", 0.0),
                        )
                    ),
                ) / divisor
                for _ in range(quantity * data_points_per_asset):
                    templates.append(
                        (
                            asset_id,
                            asset_name,
                            power,
                            bandwidth_per_port,
                            packets_per_port,
                        )
                    )
        elif room_type_id:
            missing_room_types.add(room_type_id)

        target_ports = max(requested_ports, len(templates))
        if target_ports <= 0:
            continue
        while len(templates) < target_ports:
            templates.append(
                (
                    "",
                    "Generic network port",
                    0.0,
                    max(
                        0.0,
                        _float(settings.get("default_expected_bandwidth_mbps")),
                    ),
                    max(
                        0.0,
                        _float(settings.get("default_expected_packet_rate_pps")),
                    ),
                )
            )

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
        for index, (
            asset_id,
            asset_name,
            poe_w,
            bandwidth_mbps,
            packet_rate_pps,
        ) in enumerate(
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
                    expected_bandwidth_mbps=bandwidth_mbps,
                    expected_packet_rate_pps=packet_rate_pps,
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


def _apply_manufacturer_preference(data: dict, candidates: Sequence[dict], component: str, label: str) -> List[dict]:
    """Order or constrain candidates using the configured manufacturer policy."""
    rows = list(candidates)
    preferences = normalise_manufacturer_preferences(
        data.get("network_settings", {}).get("manufacturer_preferences")
    )
    policy = preferences.get(component, {"preferred_manufacturers": [], "strict": False})
    preferred = [_text(value) for value in policy.get("preferred_manufacturers", []) if _text(value)]
    if not preferred:
        return rows
    order = {name.casefold(): index for index, name in enumerate(preferred)}
    matching = [row for row in rows if _text(row.get("manufacturer")).casefold() in order]
    if bool(policy.get("strict")):
        if not matching:
            raise NetworkPlanningError(
                f"No {label} asset matches the strict manufacturer preference: "
                + ", ".join(preferred)
            )
        rows = matching
    return sorted(
        rows,
        key=lambda row: (
            order.get(_text(row.get("manufacturer")).casefold(), len(order) + 1),
            _text(row.get("manufacturer")).casefold(),
            _text(row.get("model")).casefold(),
            _text(row.get("id")),
        ),
    )


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


def _ensure_aggregate_traffic_capacity(
    candidates: Sequence[dict],
    selected: Sequence[dict],
    actual_bandwidth_mbps: float,
    actual_packet_rate_pps: float,
    spare_fraction: float,
    label: str,
) -> List[dict]:
    """Extend a selected asset mix until declared traffic limits are satisfied.

    A zero manufacturer capacity means the metric is not declared and is not
    treated as a hard constraint. Port and PoE capacity selected by
    ``_minimum_asset_mix`` is always retained.
    """

    result = list(selected)
    required_bandwidth = max(0.0, actual_bandwidth_mbps) * (1.0 + spare_fraction)
    required_packets = max(0.0, actual_packet_rate_pps) * (1.0 + spare_fraction)
    bandwidth_declared = any(
        _float(item.get("bandwidth_capacity_gbps")) > 0 for item in candidates
    )
    packets_declared = any(
        _float(item.get("packet_throughput_mpps")) > 0 for item in candidates
    )
    if not bandwidth_declared and not packets_declared:
        return result

    def totals(items: Sequence[dict]) -> Tuple[float, float]:
        return (
            sum(
                max(0.0, _float(item.get("bandwidth_capacity_gbps"))) * 1000.0
                for item in items
            ),
            sum(
                max(0.0, _float(item.get("packet_throughput_mpps")))
                * 1_000_000.0
                for item in items
            ),
        )

    for _ in range(256):
        bandwidth, packets = totals(result)
        bandwidth_ok = not bandwidth_declared or bandwidth + 1e-9 >= required_bandwidth
        packets_ok = not packets_declared or packets + 1e-9 >= required_packets
        if bandwidth_ok and packets_ok:
            return result

        choices = []
        for candidate in candidates:
            bw = max(0.0, _float(candidate.get("bandwidth_capacity_gbps"))) * 1000.0
            pps = (
                max(0.0, _float(candidate.get("packet_throughput_mpps")))
                * 1_000_000.0
            )
            if bandwidth_declared and not bandwidth_ok and bw <= 0:
                continue
            if packets_declared and not packets_ok and pps <= 0:
                continue
            contribution = (
                bw / max(1.0, required_bandwidth)
                + pps / max(1.0, required_packets)
            )
            choices.append(
                (
                    -contribution,
                    max(1, _rack_units_for_asset(candidate, 1)),
                    _float(candidate.get("power_input_w")),
                    _text(candidate.get("id")),
                    candidate,
                )
            )
        if not choices:
            missing = []
            if bandwidth_declared and not bandwidth_ok:
                missing.append(f"{required_bandwidth:.3f} Mbps")
            if packets_declared and not packets_ok:
                missing.append(f"{required_packets:.0f} packets/s")
            raise NetworkPlanningError(
                f"The available {label} assets cannot satisfy " + " and ".join(missing) + "."
            )
        result.append(min(choices)[-1])

    raise NetworkPlanningError(f"Unable to stabilise traffic capacity for {label}.")


def _asset_endpoint_port_slots(asset: dict) -> List[dict]:
    """Expand client-facing physical ports with their selectable line rates."""

    rows = [
        row
        for row in asset.get("port_definitions", [])
        if isinstance(row, dict) and _int(row.get("port_count")) > 0
    ]
    excluded_uses = {"uplink", "input", "pon", "stacking", "power", "console", "management"}
    endpoint_rows = [
        row
        for row in rows
        if _text(row.get("port_use")).lower() not in excluded_uses
    ]
    if not endpoint_rows:
        endpoint_rows = [
            row
            for row in rows
            if _text(row.get("port_use")).lower() in {"client", "output", "downlink", "other"}
        ]

    slots: List[dict] = []
    counters: Dict[str, int] = defaultdict(int)
    for row in endpoint_rows:
        port_type = _text(row.get("port_type")).lower() or (
            "rj45" if _text(asset.get("output_connection_type")).lower() == "copper" else "other"
        )
        speeds = normalise_port_speeds(row.get("supported_speeds_mbps"))
        if not speeds:
            speeds = default_port_speeds(port_type)
        explicit = (
            [_text(value) for value in row.get("explicit_names", []) if _text(value)]
            if isinstance(row.get("explicit_names", []), list)
            else []
        )
        count = max(0, _int(row.get("port_count")))
        for name in explicit[:count]:
            slots.append({"name": name, "port_type": port_type, "speeds": list(speeds)})
        remaining = count - min(count, len(explicit))
        prefix = _text(row.get("name_prefix")) or {
            "rj45": "", "lc": "LC", "sc": "SC", "mpo": "MPO",
            "sfp": "SFP", "sfp+": "SFP+", "sfp28": "SFP28",
            "qsfp": "QSFP", "qsfp+": "QSFP+", "qsfp28": "QSFP28",
            "qsfp56": "QSFP56", "qsfpdd": "QSFP-DD", "osfp": "OSFP",
        }.get(port_type, port_type.upper())
        for _ in range(remaining):
            counters[prefix] += 1
            name = f"{prefix}-{counters[prefix]}" if prefix else str(counters[prefix])
            slots.append({"name": name, "port_type": port_type, "speeds": list(speeds)})

    if not slots:
        port_type = "rj45" if _text(asset.get("output_connection_type")).lower() == "copper" else "other"
        speeds = default_port_speeds(port_type)
        slots = [
            {"name": str(number), "port_type": port_type, "speeds": list(speeds)}
            for number in range(1, max(0, _int(asset.get("number_of_ports"))) + 1)
        ]
    return slots


def _endpoint_capacity_asset(asset: dict) -> dict:
    """Return a planner-only asset copy whose port count excludes uplinks."""

    result = deepcopy(asset)
    result["number_of_ports"] = len(_asset_endpoint_port_slots(asset))
    return result


def _slot_selected_speed(slot: dict, required_mbps: float) -> int:
    speeds = normalise_port_speeds(slot.get("speeds"))
    eligible = [speed for speed in speeds if speed + 1e-9 >= max(0.0, required_mbps)]
    return min(eligible) if eligible else 0


def _can_allocate_endpoint_speeds(asset: dict, items: Sequence[PortDemand]) -> bool:
    slots = list(_asset_endpoint_port_slots(asset))
    for item in sorted(items, key=lambda row: -row.expected_bandwidth_mbps):
        choices = [
            (
                _slot_selected_speed(slot, item.expected_bandwidth_mbps)
                - item.expected_bandwidth_mbps,
                index,
            )
            for index, slot in enumerate(slots)
            if _slot_selected_speed(slot, item.expected_bandwidth_mbps) > 0
            or item.expected_bandwidth_mbps <= 0.0
        ]
        if not choices:
            return False
        _headroom, index = min(choices)
        slots.pop(index)
    return True


def _pack_ports_to_devices(
    port_items: Sequence[PortDemand],
    device_assets: Sequence[dict],
    spare_fraction: float,
) -> List[List[PortDemand]]:
    # Spare capacity is enforced when selecting the aggregate device mix.  The
    # physical allocation below additionally reserves a real client-facing port
    # whose declared line rate can carry each endpoint's expected bandwidth.
    devices = [
        {
            "slots": list(_asset_endpoint_port_slots(asset)),
            "poe": max(0.0, _float(asset.get("poe_budget_w"))),
            "bandwidth": (
                max(0.0, _float(asset.get("bandwidth_capacity_gbps"))) * 1000.0
                if _float(asset.get("bandwidth_capacity_gbps")) > 0
                else None
            ),
            "packets": (
                max(0.0, _float(asset.get("packet_throughput_mpps"))) * 1_000_000.0
                if _float(asset.get("packet_throughput_mpps")) > 0
                else None
            ),
            "items": [],
        }
        for asset in device_assets
    ]
    for item in sorted(
        port_items,
        key=lambda row: (
            -row.expected_bandwidth_mbps,
            -row.expected_packet_rate_pps,
            -row.poe_power_w,
            row.endpoint_name,
            row.endpoint_port,
        ),
    ):
        choices = []
        for device_index, device in enumerate(devices):
            if device["poe"] + 1e-9 < item.poe_power_w:
                continue
            if (
                device["bandwidth"] is not None
                and device["bandwidth"] + 1e-9 < item.expected_bandwidth_mbps
            ):
                continue
            if (
                device["packets"] is not None
                and device["packets"] + 1e-9 < item.expected_packet_rate_pps
            ):
                continue
            for slot_index, slot in enumerate(device["slots"]):
                selected_speed = _slot_selected_speed(slot, item.expected_bandwidth_mbps)
                if selected_speed <= 0 and item.expected_bandwidth_mbps > 0.0:
                    continue
                choices.append(
                    (
                        selected_speed - item.expected_bandwidth_mbps,
                        (
                            device["bandwidth"] - item.expected_bandwidth_mbps
                            if device["bandwidth"] is not None
                            else float("inf")
                        ),
                        (
                            device["packets"] - item.expected_packet_rate_pps
                            if device["packets"] is not None
                            else float("inf")
                        ),
                        device["poe"] - item.poe_power_w,
                        len(device["slots"]) - 1,
                        device_index,
                        slot_index,
                        selected_speed,
                    )
                )
        if not choices:
            required = port_speed_label(int(math.ceil(item.expected_bandwidth_mbps)))
            raise NetworkPlanningError(
                f"Unable to allocate {item.endpoint_name} port {item.endpoint_port}; "
                f"it requires {item.poe_power_w:.1f} W PoE and a client port capable "
                f"of at least {required}."
            )
        *_, selected_device, selected_slot, selected_speed = min(choices)
        device = devices[selected_device]
        slot = device["slots"].pop(selected_slot)
        device["poe"] -= item.poe_power_w
        if device["bandwidth"] is not None:
            device["bandwidth"] -= item.expected_bandwidth_mbps
        if device["packets"] is not None:
            device["packets"] -= item.expected_packet_rate_pps
        item.selected_network_port = _text(slot.get("name"))
        item.selected_link_speed_mbps = max(0, int(selected_speed))
        device["items"].append(item)
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
    data["network_power_connections"] = [
        item
        for item in data.get("network_power_connections", [])
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
    data["network_optic_modules"] = [
        item
        for item in data.get("network_optic_modules", [])
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
    # Optical paths are derived from the current logical and physical design.
    # Never carry calculated paths across a fresh automatic design pass.
    data["network_optical_paths"] = []


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
        self.link_aggregation_ids = {
            _text(item.get("link_aggregation_group_id"))
            for item in data.get("network_connections", [])
            if isinstance(item, dict) and _text(item.get("link_aggregation_group_id"))
        }
        self.power_connection_ids = {
            _text(item.get("id"))
            for item in data.get("network_power_connections", [])
            if _text(item.get("id"))
        }
        self.patch_lead_ids = {_text(item.get("id")) for item in data.get("network_patch_leads", []) if _text(item.get("id"))}
        self.optic_module_ids = {_text(item.get("id")) for item in data.get("network_optic_modules", []) if _text(item.get("id"))}
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
        self.power_connections: List[dict] = []
        self.assignments: List[dict] = []
        self.patch_leads: List[dict] = []
        self.optic_modules: List[dict] = []
        self.locations: List[dict] = []
        self.redundancy_groups: List[dict] = []
        self.total_copper_m = 0.0
        self.total_fibre_m = 0.0
        self._reserved_ports: Dict[str, set[str]] = defaultdict(set)
        for connection in data.get("network_connections", []):
            if not isinstance(connection, dict):
                continue
            for side in ("from", "to"):
                instance_id = _text(connection.get(f"{side}_instance_id"))
                port = _text(connection.get(f"{side}_port"))
                if instance_id and port:
                    self._reserved_ports[instance_id].add(port)
        for assignment in data.get("network_endpoint_assignments", []):
            if isinstance(assignment, dict):
                instance_id = _text(assignment.get("network_instance_id")); port = _text(assignment.get("network_port"))
                if instance_id and port:
                    self._reserved_ports[instance_id].add(port)

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

    @staticmethod
    def _port_medium(port_type: str) -> str:
        return "fibre" if _text(port_type).lower() in PLUGGABLE_OPTIC_PORT_TYPES | {"pon", "lc", "sc", "mpo"} else "copper"

    def _expanded_ports(self, instance: dict) -> List[dict]:
        asset = self._asset_for_instance(instance)
        rows = [row for row in asset.get("port_definitions", []) if isinstance(row, dict) and _int(row.get("port_count")) > 0]
        result: List[dict] = []
        counters: Dict[str, int] = defaultdict(int)
        for row in rows:
            port_type = _text(row.get("port_type")).lower() or "other"
            port_use = _text(row.get("port_use")).lower() or "other"
            speeds = normalise_port_speeds(row.get("supported_speeds_mbps")) or default_port_speeds(port_type)
            explicit = [_text(value) for value in row.get("explicit_names", []) if _text(value)] if isinstance(row.get("explicit_names", []), list) else []
            count = max(0, _int(row.get("port_count")))
            for name in explicit[:count]:
                result.append({**row, "name": name, "port_type": port_type, "port_use": port_use, "medium": self._port_medium(port_type), "supported_speeds_mbps": speeds})
            remaining = count - min(count, len(explicit))
            prefix = _text(row.get("name_prefix")) or {
                "pon": "PON", "sfp": "SFP", "sfp+": "SFP+", "sfp28": "SFP28",
                "qsfp": "QSFP", "qsfp+": "QSFP+", "qsfp28": "QSFP28",
                "qsfp56": "QSFP56", "qsfpdd": "QSFP-DD", "osfp": "OSFP",
                "lc": "LC", "sc": "SC", "mpo": "MPO", "rj45": "",
            }.get(port_type, port_type.upper())
            for _ in range(remaining):
                counters[prefix] += 1
                name = f"{prefix}-{counters[prefix]}" if prefix else str(counters[prefix])
                result.append({**row, "name": name, "port_type": port_type, "port_use": port_use, "medium": self._port_medium(port_type), "supported_speeds_mbps": speeds})
        members = max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1
        if members > 1:
            result = [{**row, "name": f"{member}/{row['name']}"} for member in range(1, members + 1) for row in result]
        return result

    def _port_definition(self, instance: dict, port_name: str, medium: str, preferred_use: str = "") -> dict:
        ports = self._expanded_ports(instance)
        target = _text(port_name).lower()
        exact = next((row for row in ports if _text(row.get("name")).lower() == target), None)
        if exact is not None:
            return exact
        candidates = [row for row in ports if _text(row.get("medium")) == medium]
        if preferred_use:
            preferred = [row for row in candidates if _text(row.get("port_use")) == preferred_use]
            if preferred:
                candidates = preferred
        return candidates[0] if candidates else {"name": port_name, "port_type": "lc" if medium == "fibre" else "rj45", "port_use": preferred_use or "other", "medium": medium, "supported_speeds_mbps": default_port_speeds("lc" if medium == "fibre" else "rj45")}

    @staticmethod
    def _requested_use(port_name: str, fallback: str) -> str:
        value = _text(port_name).lower()
        for token, use in (("uplink", "uplink"), ("downlink", "downlink"), ("pon", "pon"), ("input", "input"), ("output", "output"), ("client", "client"), ("peer", "uplink")):
            if token in value:
                return use
        return fallback

    def _connection_port_candidates(
        self,
        instance: dict,
        requested: str,
        medium: str,
        fallback_use: str,
    ) -> List[dict]:
        """Return free ports ordered by the requested name and intended use."""

        instance_id = _text(instance.get("id"))
        expanded = self._expanded_ports(instance)
        rows = [
            row
            for row in expanded
            if _text(row.get("medium")) == medium
            and _text(row.get("name"))
            not in self._reserved_ports.get(instance_id, set())
        ]
        if not rows and not expanded:
            rows = [
                {
                    "name": requested,
                    "port_type": "lc" if medium == "fibre" else "rj45",
                    "port_use": fallback_use,
                    "medium": medium,
                    "supported_speeds_mbps": [],
                }
            ]
        wanted = self._requested_use(requested, fallback_use)
        requested_lower = _text(requested).lower()
        exact_rows = [
            row
            for row in rows
            if _text(row.get("name")).lower() == requested_lower
        ]
        preferred_rows = [
            row
            for row in rows
            if _text(row.get("port_use")).lower() == wanted
        ]
        if preferred_rows:
            exact_ids = {id(row) for row in exact_rows}
            rows = exact_rows + [
                row for row in preferred_rows if id(row) not in exact_ids
            ]
        indexed_rows = list(enumerate(rows))
        indexed_rows.sort(
            key=lambda indexed: (
                0
                if _text(indexed[1].get("name")).lower() == requested_lower
                else 1,
                0
                if _text(indexed[1].get("port_use")).lower() == wanted
                else 1,
                indexed[0],
            )
        )
        return [row for _index, row in indexed_rows]

    def _select_connection_port_bundle(
        self,
        from_instance: dict,
        requested_from: str,
        to_instance: dict,
        requested_to: str,
        medium: str,
        requested_speed: int,
        required_bandwidth_mbps: float,
        *,
        reserve: bool = True,
    ) -> Tuple[List[Tuple[dict, dict]], int]:
        """Select one link or a same-speed bundle able to carry the demand."""

        left_rows = self._connection_port_candidates(
            from_instance, requested_from, medium, "uplink"
        )
        right_rows = self._connection_port_candidates(
            to_instance, requested_to, medium, "input"
        )

        speed_candidates: Set[int] = set()
        for left in left_rows:
            left_speeds = normalise_port_speeds(
                left.get("supported_speeds_mbps")
            )
            for right in right_rows:
                right_speeds = normalise_port_speeds(
                    right.get("supported_speeds_mbps")
                )
                common = compatible_port_speeds(left_speeds, right_speeds)
                if requested_speed > 0:
                    if not common or requested_speed in common:
                        speed_candidates.add(requested_speed)
                else:
                    speed_candidates.update(common)

        choices: List[tuple] = []
        required = max(0.0, float(required_bandwidth_mbps or 0.0))
        for speed in sorted(speed_candidates):
            if speed <= 0:
                continue
            left_eligible = [
                row
                for row in left_rows
                if not normalise_port_speeds(row.get("supported_speeds_mbps"))
                or speed
                in normalise_port_speeds(row.get("supported_speeds_mbps"))
            ]
            right_eligible = [
                row
                for row in right_rows
                if not normalise_port_speeds(row.get("supported_speeds_mbps"))
                or speed
                in normalise_port_speeds(row.get("supported_speeds_mbps"))
            ]
            member_count = max(
                1,
                int(math.ceil(required / float(speed) - 1e-12))
                if required > 0.0
                else 1,
            )
            if member_count > min(len(left_eligible), len(right_eligible)):
                continue
            pairs = list(
                zip(
                    left_eligible[:member_count],
                    right_eligible[:member_count],
                )
            )
            aggregate_capacity = speed * member_count
            choices.append(
                (
                    member_count,
                    max(0.0, aggregate_capacity - required),
                    aggregate_capacity,
                    speed,
                    tuple(_text(row.get("name")) for row in left_eligible[:member_count]),
                    tuple(_text(row.get("name")) for row in right_eligible[:member_count]),
                    pairs,
                )
            )

        # Preserve legacy support for unstructured or speed-transparent devices.
        # Their capacity is unknown, so only one logical connection can be made.
        if not choices:
            for left in left_rows:
                for right in right_rows:
                    left_speeds = normalise_port_speeds(
                        left.get("supported_speeds_mbps")
                    )
                    right_speeds = normalise_port_speeds(
                        right.get("supported_speeds_mbps")
                    )
                    common = compatible_port_speeds(left_speeds, right_speeds)
                    if left_speeds and right_speeds and not common:
                        continue
                    if requested_speed > 0 and common and requested_speed not in common:
                        continue
                    if not common and not left_speeds and not right_speeds:
                        choices.append(
                            (
                                1,
                                float("inf"),
                                0,
                                requested_speed,
                                (_text(left.get("name")),),
                                (_text(right.get("name")),),
                                [(left, right)],
                            )
                        )
                        break
                if choices:
                    break

        if not choices:
            from_name = from_instance.get("name") or from_instance.get("id")
            to_name = to_instance.get("name") or to_instance.get("id")
            requirement = (
                port_speed_label(requested_speed)
                if requested_speed
                else f"{required_bandwidth_mbps:g} Mbps required traffic"
            )
            raise NetworkPlanningError(
                f"No compatible free {medium} port or same-speed link aggregation "
                f"group exists between {from_name} and {to_name} for {requirement}."
            )

        selected_choice = min(choices, key=lambda row: row[:-1])
        selected_speed = selected_choice[3]
        pairs = selected_choice[-1]
        if reserve:
            from_id = _text(from_instance.get("id"))
            to_id = _text(to_instance.get("id"))
            for left, right in pairs:
                self._reserved_ports[from_id].add(_text(left.get("name")))
                self._reserved_ports[to_id].add(_text(right.get("name")))
        return pairs, selected_speed

    def preview_connection_bundle(
        self,
        from_instance: dict,
        requested_from: str,
        to_instance: dict,
        requested_to: str,
        medium: str,
        requested_speed: int,
        required_bandwidth_mbps: float,
    ) -> Tuple[int, int]:
        """Return the required member count and speed without reserving ports."""

        pairs, selected_speed = self._select_connection_port_bundle(
            from_instance,
            requested_from,
            to_instance,
            requested_to,
            medium,
            requested_speed,
            required_bandwidth_mbps,
            reserve=False,
        )
        return len(pairs), selected_speed

    def _matching_optic_assets(
        self, host_asset: dict, port: dict, speed_mbps: int, required_reach_m: float
    ) -> List[dict]:
        cage = _text(port.get("port_type")).lower()
        allowed_forms = optic_form_factors_for_cage(cage)
        form_speed_matches: List[dict] = []
        reachable: List[dict] = []
        for asset in self.data.get("network_assets", []):
            if not isinstance(asset, dict) or _asset_type(asset) != "optical_transceiver":
                continue
            form = _text(asset.get("optic_form_factor")).lower()
            speeds = normalise_port_speeds(asset.get("supported_speeds_mbps"))
            if form not in allowed_forms or (speed_mbps > 0 and speeds and speed_mbps not in speeds):
                continue
            form_speed_matches.append(asset)
            reach = max(0.0, _float(asset.get("optic_reach_m")))
            if reach <= 0.0 or required_reach_m <= reach + 1e-9:
                reachable.append(asset)
        if form_speed_matches and not reachable:
            longest = max(max(0.0, _float(row.get("optic_reach_m"))) for row in form_speed_matches)
            raise NetworkPlanningError(
                f"No {cage.upper()} optic supporting {port_speed_label(speed_mbps)} reaches "
                f"the required {required_reach_m:.1f} m route; the longest declared reach is {longest:.1f} m."
            )
        return reachable

    def _create_fallback_optic_asset(
        self, host_asset: dict, port: dict, speed_mbps: int
    ) -> dict:
        cage = _text(port.get("port_type")).lower()
        asset_id = f"AUTO-OPTIC-{cage.upper().replace('+','P').replace('-','')}-{speed_mbps or 'UNSPEC'}"
        existing = next(
            (row for row in self.data.get("network_assets", []) if _text(row.get("id")) == asset_id),
            None,
        )
        if existing is not None:
            return existing
        tx = port.get("transmit_power_dbm", host_asset.get("optical_tx_power_dbm", ""))
        rx = port.get(
            "receiver_sensitivity_dbm",
            host_asset.get("optical_receiver_sensitivity_dbm", ""),
        )
        optic = {
            "id": asset_id,
            "name": (
                f"Auto {port_speed_label(speed_mbps) if speed_mbps else ''} "
                f"{cage.upper()} optical transceiver"
            ).replace("  ", " ").strip(),
            "asset_type": "optical_transceiver",
            "manufacturer": "",
            "model": "",
            "optic_form_factor": cage,
            "supported_speeds_mbps": (
                [speed_mbps]
                if speed_mbps
                else normalise_port_speeds(port.get("supported_speeds_mbps"))
            ),
            "optic_connector_type": (
                "mpo"
                if cage in {"qsfpdd", "osfp"} and speed_mbps >= 400000
                else "lc"
            ),
            "optic_fibre_standard": _text(host_asset.get("optical_standard")) or "OS2",
            "optic_reach_m": 0.0,
            "optical_tx_power_dbm": tx,
            "optical_receiver_sensitivity_dbm": rx,
            "optical_insertion_loss_db": port.get("insertion_loss_db", ""),
            "optical_return_loss_db": port.get(
                "return_loss_db", host_asset.get("optical_return_loss_db", "")
            ),
            "optical_wavelength_nm": _int(
                port.get("wavelength_nm"),
                _int(host_asset.get("optical_wavelength_nm")),
            ),
            "rack_units": 0,
            "number_of_ports": 0,
            "port_definitions": [],
            "auto_network_asset": True,
            "auto_optic_definition": True,
            "notes": (
                "Planner-created optic definition. Configure transmit power, receiver "
                "sensitivity and reach in the network asset or optical properties dialog."
            ),
        }
        self.data.setdefault("network_assets", []).append(optic)
        self.asset_ids.add(asset_id)
        if _text(tx) == "" or _text(rx) == "":
            self.warnings.append(
                f"{optic['name']} was created without complete transmit/receive values; "
                "its optical budget remains unconfigured."
            )
        return optic

    def _estimated_fixed_fibre_loss_db(self, length_m: float) -> float:
        planning = self.settings.get("physical_fibre_planning", {})
        mode = _text(planning.get("routing_mode")).lower() or "direct"
        type_id = _text(
            planning.get(
                "spine_cable_type_id" if mode == "spine_and_spur" else "default_cable_type_id"
            )
        )
        cable_type = next(
            (
                row
                for row in self.data.get("network_fibre_cable_types", [])
                if _text(row.get("id")) == type_id
            ),
            {},
        )
        attenuation = max(0.0, _float(cable_type.get("attenuation_db_per_m"), 0.00035))
        connector_loss = max(0.0, _float(cable_type.get("connector_loss_db"), 0.5))
        splice_loss = max(0.0, _float(cable_type.get("splice_loss_db"), 0.1))
        splice_count = 1 if mode == "spine_and_spur" else 0
        return max(0.0, float(length_m)) * attenuation + 2.0 * connector_loss + splice_count * splice_loss

    @staticmethod
    def _optic_endpoint_values(host_asset: dict, port: dict, optic_asset: Optional[dict]) -> dict:
        source = optic_asset or {}
        return {
            "tx": source.get(
                "optical_tx_power_dbm",
                port.get("transmit_power_dbm", host_asset.get("optical_tx_power_dbm", "")),
            ),
            "rx": source.get(
                "optical_receiver_sensitivity_dbm",
                port.get(
                    "receiver_sensitivity_dbm",
                    host_asset.get("optical_receiver_sensitivity_dbm", ""),
                ),
            ),
            "insertion": source.get(
                "optical_insertion_loss_db",
                port.get("insertion_loss_db", host_asset.get("optical_insertion_loss_db", "")),
            ),
            "return_loss": source.get(
                "optical_return_loss_db",
                port.get("return_loss_db", host_asset.get("optical_return_loss_db", "")),
            ),
            "wavelength": max(
                0,
                _int(
                    source.get(
                        "optical_wavelength_nm",
                        port.get("wavelength_nm", host_asset.get("optical_wavelength_nm", 0)),
                    )
                ),
            ),
            "standard": _text(
                source.get("optic_fibre_standard")
                or host_asset.get("optic_fibre_standard")
                or host_asset.get("optical_standard")
            ).upper(),
            "connector": _text(source.get("optic_connector_type")).lower(),
        }

    def _select_optic_pair(
        self,
        from_instance: dict,
        from_port: dict,
        to_instance: dict,
        to_port: dict,
        speed_mbps: int,
        route_length_m: float,
    ) -> Tuple[Optional[dict], Optional[dict], float, Optional[float]]:
        from_asset = self._asset_for_instance(from_instance)
        to_asset = self._asset_for_instance(to_instance)
        from_pluggable = _text(from_port.get("port_type")).lower() in PLUGGABLE_OPTIC_PORT_TYPES
        to_pluggable = _text(to_port.get("port_type")).lower() in PLUGGABLE_OPTIC_PORT_TYPES

        from_candidates: List[Optional[dict]]
        to_candidates: List[Optional[dict]]
        if from_pluggable:
            rows = self._matching_optic_assets(from_asset, from_port, speed_mbps, route_length_m)
            from_candidates = rows or [self._create_fallback_optic_asset(from_asset, from_port, speed_mbps)]
        else:
            from_candidates = [None]
        if to_pluggable:
            rows = self._matching_optic_assets(to_asset, to_port, speed_mbps, route_length_m)
            to_candidates = rows or [self._create_fallback_optic_asset(to_asset, to_port, speed_mbps)]
        else:
            to_candidates = [None]

        passive_types = {"fibre_splitter", "patch_panel"}
        both_active = (
            _asset_type(from_asset) not in passive_types
            and _asset_type(to_asset) not in passive_types
        )
        estimated_passive_loss = self._estimated_fixed_fibre_loss_db(route_length_m)
        required_margin = max(
            0.0,
            _float(
                self.settings.get("physical_fibre_planning", {}).get(
                    "minimum_optical_margin_db", 3.0
                )
            ),
        )
        passing: List[tuple] = []
        incomplete: List[tuple] = []
        failing: List[tuple] = []

        for left in from_candidates:
            left_values = self._optic_endpoint_values(from_asset, from_port, left)
            for right in to_candidates:
                right_values = self._optic_endpoint_values(to_asset, to_port, right)
                if (
                    left_values["connector"]
                    and right_values["connector"]
                    and left_values["connector"] != right_values["connector"]
                ):
                    continue
                if (
                    left_values["standard"]
                    and right_values["standard"]
                    and left_values["standard"] != right_values["standard"]
                ):
                    continue
                # Passive optical devices do not transmit at their stored reference
                # wavelength.  In particular, a PON splitter sits between asymmetric
                # downstream/upstream wavelengths, so comparing its wavelength with
                # an integrated OLT/ONT optic incorrectly rejects valid 2.5G PON links.
                # Wavelength equality is therefore only required for a direct path
                # between two active optical endpoints.
                if (
                    both_active
                    and left_values["wavelength"]
                    and right_values["wavelength"]
                    and left_values["wavelength"] != right_values["wavelength"]
                ):
                    continue

                left_id = _text((left or {}).get("id"))
                right_id = _text((right or {}).get("id"))
                reach_headroom = sum(
                    max(0.0, _float((optic or {}).get("optic_reach_m")) - route_length_m)
                    for optic in (left, right)
                    if optic is not None and _float(optic.get("optic_reach_m")) > 0
                )
                if not both_active:
                    passing.append((0.0, reach_headroom, left_id, right_id, left, right, None))
                    continue

                configured = all(
                    _text(value) != ""
                    for value in (
                        left_values["tx"],
                        left_values["rx"],
                        right_values["tx"],
                        right_values["rx"],
                    )
                )
                if not configured:
                    incomplete.append((reach_headroom, left_id, right_id, left, right, None))
                    continue
                active_loss = max(0.0, _float(left_values["insertion"])) + max(
                    0.0, _float(right_values["insertion"])
                )
                forward_margin = (
                    _float(left_values["tx"])
                    - _float(right_values["rx"])
                    - estimated_passive_loss
                    - active_loss
                )
                reverse_margin = (
                    _float(right_values["tx"])
                    - _float(left_values["rx"])
                    - estimated_passive_loss
                    - active_loss
                )
                margin = min(forward_margin, reverse_margin)
                row = (
                    max(0.0, margin - required_margin),
                    reach_headroom,
                    left_id,
                    right_id,
                    left,
                    right,
                    margin,
                )
                if margin + 1e-9 >= required_margin:
                    passing.append(row)
                else:
                    failing.append(row)

        if passing:
            row = min(passing, key=lambda value: value[:4])
            return row[4], row[5], estimated_passive_loss, row[6]
        if incomplete:
            row = min(incomplete, key=lambda value: value[:3])
            self.warnings.append(
                f"The optic pair for {from_instance.get('name') or from_instance.get('id')} "
                f"to {to_instance.get('name') or to_instance.get('id')} cannot be fully "
                "budget-checked until transmit and receive values are configured."
            )
            return row[3], row[4], estimated_passive_loss, None
        if failing:
            best = max(failing, key=lambda value: value[6])
            raise NetworkPlanningError(
                f"No compatible optic pair meets the required {required_margin:.2f} dB design "
                f"margin over the estimated {estimated_passive_loss:.3f} dB passive path. "
                f"Best calculated margin is {best[6]:.3f} dB."
            )
        raise NetworkPlanningError(
            f"No mutually compatible optical transceivers are available for the selected "
            f"{port_speed_label(speed_mbps)} ports, connector, wavelength and fibre standard."
        )

    def _add_optic_module(
        self,
        connection_id: str,
        side: str,
        instance: dict,
        port: dict,
        speed_mbps: int,
        optic_asset: Optional[dict],
    ) -> str:
        if (
            _text(port.get("port_type")).lower() not in PLUGGABLE_OPTIC_PORT_TYPES
            or optic_asset is None
        ):
            return ""
        module_id = _next_identifier(self.optic_module_ids, "AUTO-OM-")
        self.optic_modules.append(
            {
                "id": module_id,
                "asset_id": _text(optic_asset.get("id")),
                "host_instance_id": _text(instance.get("id")),
                "host_port": _text(port.get("name")),
                "connection_id": connection_id,
                "side": side,
                "link_speed_mbps": speed_mbps,
                "auto_generated": True,
                "notes": (
                    "Automatically inserted into the selected optical cage by the "
                    "network planner after speed, reach and light-budget checks."
                ),
            }
        )
        return module_id

    def add_patch_lead(self, *, connection_id: str = "", assignment_id: str = "", instance: dict, port: str, medium: str, peer_instance_id: str = "", peer_port: str = "", endpoint_name: str = "", preferred_use: str = "") -> dict:
        definition = self._port_definition(instance, port, medium, preferred_use)
        default_length = 2.0 if medium in {"copper", "fibre"} else 0.0
        lead = {
            "id": _next_identifier(self.patch_lead_ids, "AUTO-PL-"), "connection_id": connection_id, "assignment_id": assignment_id,
            "instance_id": _text(instance.get("id")), "port": str(port), "peer_instance_id": peer_instance_id, "peer_port": str(peer_port),
            "endpoint_name": endpoint_name, "port_type": _text(definition.get("port_type")) or ("lc" if medium == "fibre" else "rj45"),
            "port_use": _text(definition.get("port_use")) or preferred_use or "patch", "medium": medium,
            "cable_specification": "OS2 fibre patch lead" if medium == "fibre" else "Category 6A copper patch lead",
            "length_m": default_length, "auto_generated": True,
        }
        self.patch_leads.append(lead)
        return lead

    def add_connection(self, from_instance: dict, from_port: str, to_instance: dict, to_port: str, medium: str, route_source: str = "", route_destination: str = "", **extra) -> dict:
        generate_patch_leads = bool(extra.pop("generate_patch_leads", True))
        requested_speed = max(0, _int(extra.pop("link_speed_mbps", 0)))
        physical_connection = bool(extra.get("physical_connection"))
        parent_logical_id = _text(extra.get("parent_logical_connection_id"))
        explicit_bandwidth = extra.get("expected_bandwidth_mbps", None)
        explicit_packets = extra.get("expected_packet_rate_pps", None)
        required_bandwidth = (
            max(0.0, _float(explicit_bandwidth))
            if explicit_bandwidth is not None and _text(explicit_bandwidth) != ""
            else max(
                0.0,
                _float(from_instance.get("expected_bandwidth_mbps")),
                _float(to_instance.get("expected_bandwidth_mbps")),
            )
        )
        required_packets = (
            max(0.0, _float(explicit_packets))
            if explicit_packets is not None and _text(explicit_packets) != ""
            else max(
                0.0,
                _float(from_instance.get("expected_packet_rate_pps")),
                _float(to_instance.get("expected_packet_rate_pps")),
            )
        )

        if physical_connection:
            selected_from = self._port_definition(from_instance, from_port, medium, self._requested_use(from_port, "uplink"))
            selected_to = self._port_definition(to_instance, to_port, medium, self._requested_use(to_port, "input"))
            selected_from = {**selected_from, "name": _text(from_port)}
            selected_to = {**selected_to, "name": _text(to_port)}
            parent = next((row for row in self.connections if _text(row.get("id")) == parent_logical_id), None) or next((row for row in self.data.get("network_connections", []) if _text(row.get("id")) == parent_logical_id), None)
            link_speed = requested_speed or max(0, _int((parent or {}).get("link_speed_mbps")))
            selected_pairs = [(selected_from, selected_to)]
        else:
            selected_pairs, link_speed = self._select_connection_port_bundle(
                from_instance,
                from_port,
                to_instance,
                to_port,
                medium,
                requested_speed,
                required_bandwidth,
            )

        member_count = len(selected_pairs)
        lag_id = (
            _next_identifier(self.link_aggregation_ids, "AUTO-LAG-")
            if not physical_connection and member_count > 1
            else ""
        )
        aggregate_capacity = max(0, link_speed) * member_count
        length_m, route_path = self.graph.route(route_source, route_destination) if route_source and route_destination else (0.0, [])
        cable_specification = {"fibre": "OS2 single-mode fibre", "copper": "Category 6A", "stacking": "Switch stack interconnect"}.get(medium, "")
        created: List[dict] = []

        for member_index, (selected_from, selected_to) in enumerate(selected_pairs, start=1):
            selected_from_port = _text(selected_from.get("name"))
            selected_to_port = _text(selected_to.get("name"))
            connection_id = _next_identifier(self.connection_ids, "AUTO-NC-")
            connection = {
                "id": connection_id, "from_instance_id": _text(from_instance.get("id")), "from_port": selected_from_port,
                "to_instance_id": _text(to_instance.get("id")), "to_port": selected_to_port, "connection_role": "uplink",
                "medium": medium, "link_speed_mbps": link_speed, "cable_specification": cable_specification,
                "fibre_count": 2 if medium == "fibre" else 0, "vlan_ids": [], "route_profile": "",
                "route_path": route_path, "length_m": round(length_m, 3),
                "notes": "Automatically generated network topology connection.", "auto_generated": True, **extra,
            }
            if lag_id:
                connection.update(
                    {
                        "link_aggregation_group_id": lag_id,
                        "link_aggregation_mode": "lacp",
                        "link_aggregation_member_index": member_index,
                        "link_aggregation_member_count": member_count,
                        "aggregate_link_speed_mbps": aggregate_capacity,
                        "aggregate_expected_bandwidth_mbps": round(required_bandwidth, 6),
                        "aggregate_expected_packet_rate_pps": round(required_packets, 3),
                        "expected_bandwidth_mbps": round(
                            required_bandwidth / member_count, 6
                        ),
                        "expected_packet_rate_pps": round(
                            required_packets / member_count, 3
                        ),
                    }
                )
                connection["notes"] = (
                    _text(connection.get("notes"))
                    + f" Member {member_index} of {member_count} in {lag_id}; "
                    f"aggregate capacity {port_speed_label(aggregate_capacity)}."
                ).strip()

            if medium == "fibre" and not physical_connection:
                from_optic, to_optic, estimated_loss, estimated_margin = self._select_optic_pair(
                    from_instance, selected_from, to_instance, selected_to, link_speed, length_m
                )
                connection["from_optic_module_id"] = self._add_optic_module(
                    connection_id, "from", from_instance, selected_from, link_speed, from_optic
                )
                connection["to_optic_module_id"] = self._add_optic_module(
                    connection_id, "to", to_instance, selected_to, link_speed, to_optic
                )
                connection["estimated_optical_path_loss_db"] = round(estimated_loss, 6)
                connection["required_optical_margin_db"] = max(
                    0.0,
                    _float(
                        self.settings.get("physical_fibre_planning", {}).get(
                            "minimum_optical_margin_db", 3.0
                        )
                    ),
                )
                connection["estimated_optical_margin_db"] = (
                    "" if estimated_margin is None else round(estimated_margin, 6)
                )

            self.connections.append(connection)
            created.append(connection)
            if generate_patch_leads and medium in {"copper", "fibre"}:
                self.add_patch_lead(connection_id=connection_id, instance=from_instance, port=selected_from_port, medium=medium, peer_instance_id=_text(to_instance.get("id")), peer_port=selected_to_port, preferred_use="uplink")
                self.add_patch_lead(connection_id=connection_id, instance=to_instance, port=selected_to_port, medium=medium, peer_instance_id=_text(from_instance.get("id")), peer_port=selected_from_port, preferred_use="input")
            if medium == "fibre":
                self.total_fibre_m += length_m
            else:
                self.total_copper_m += length_m

        if lag_id:
            from_name = from_instance.get("name") or from_instance.get("id")
            to_name = to_instance.get("name") or to_instance.get("id")
            self.warnings.append(
                f"Created {lag_id} between {from_name} and {to_name} using "
                f"{member_count} × {port_speed_label(link_speed)} ports "
                f"({port_speed_label(aggregate_capacity)} aggregate) for "
                f"{required_bandwidth:.3f} Mbps required traffic."
            )
        return created[0]

    def add_power_connection(
        self,
        from_instance: dict,
        from_port: str,
        to_instance: dict,
        to_port: str,
        *,
        feed_label: str = "",
        load_w: float = 0.0,
        capacity_w: float = 0.0,
        notes: str = "Automatically generated rack power connection.",
    ) -> dict:
        record = {
            "id": _next_identifier(self.power_connection_ids, "AUTO-PC-"),
            "from_instance_id": _text(from_instance.get("id")),
            "from_port": _text(from_port),
            "to_instance_id": _text(to_instance.get("id")),
            "to_port": _text(to_port),
            "feed_label": _text(feed_label),
            "phase": "",
            "voltage_v": 230.0,
            "capacity_w": max(0.0, float(capacity_w or 0.0)),
            "load_w": max(0.0, float(load_w or 0.0)),
            "notes": notes,
            "auto_generated": True,
        }
        self.power_connections.append(record)
        return record

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
            "expected_bandwidth_mbps": round(item.expected_bandwidth_mbps, 6),
            "expected_packet_rate_pps": round(item.expected_packet_rate_pps, 3),
            "link_speed_mbps": max(0, int(item.selected_link_speed_mbps)),
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
        self.data.setdefault("network_power_connections", []).extend(self.power_connections)
        self.data.setdefault("network_endpoint_assignments", []).extend(
            self.assignments
        )
        self.data.setdefault("network_patch_leads", []).extend(self.patch_leads)
        self.data.setdefault("network_optic_modules", []).extend(self.optic_modules)
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
        return _apply_manufacturer_preference(data, explicit, "core_switch", "core switch")

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
    return _apply_manufacturer_preference(data, candidates, "core_switch", "core switch")




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
    candidates = candidates or _choose_core_candidates(data)
    return _apply_manufacturer_preference(data, candidates, "aggregation_switch", "aggregation switch")


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
    expected_bandwidth_mbps: float = 0.0,
    expected_packet_rate_pps: float = 0.0,
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
        selected = _ensure_aggregate_traffic_capacity(
            candidates,
            selected,
            expected_bandwidth_mbps,
            expected_packet_rate_pps,
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
    total_bandwidth_mbps = sum(
        max(0.0, _float(item.get("expected_bandwidth_mbps")))
        for item in access_switches
    )
    total_packet_rate_pps = sum(
        max(0.0, _float(item.get("expected_packet_rate_pps")))
        for item in access_switches
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
            total_bandwidth_mbps,
            total_packet_rate_pps,
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
        total_bandwidth_mbps,
        total_packet_rate_pps,
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
        total_bandwidth_mbps,
        total_packet_rate_pps,
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
    loads: Dict[str, Tuple[int, float, float, float]] = defaultdict(
        lambda: (0, 0.0, 0.0, 0.0)
    )
    for assignment in builder.assignments:
        instance_id = _text(assignment.get("network_instance_id"))
        if not instance_id:
            continue
        ports, poe, bandwidth, packets = loads[instance_id]
        loads[instance_id] = (
            ports + 1,
            poe + max(0.0, _float(assignment.get("poe_power_w"))),
            bandwidth
            + max(0.0, _float(assignment.get("expected_bandwidth_mbps"))),
            packets
            + max(0.0, _float(assignment.get("expected_packet_rate_pps"))),
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
        port_capacity = len(_asset_endpoint_port_slots(asset)) * stack_members
        poe_capacity = max(0.0, _float(asset.get("poe_budget_w"))) * stack_members
        used_ports, used_poe, used_bandwidth, used_packets = loads.get(
            instance_id, (0, 0.0, 0.0, 0.0)
        )
        required_ports = int(math.ceil(used_ports * (1.0 + spare_fraction)))
        required_poe = used_poe * (1.0 + spare_fraction)
        required_bandwidth = used_bandwidth * (1.0 + spare_fraction)
        required_packets = used_packets * (1.0 + spare_fraction)
        bandwidth_capacity = (
            max(0.0, _float(asset.get("bandwidth_capacity_gbps")))
            * 1000.0
            * stack_members
        )
        packet_capacity = (
            max(0.0, _float(asset.get("packet_throughput_mpps")))
            * 1_000_000.0
            * stack_members
        )
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
        if bandwidth_capacity and required_bandwidth > bandwidth_capacity + 1e-9:
            raise NetworkPlanningError(
                f"Generated {instance.get('name') or instance_id} would carry "
                f"{used_bandwidth:.3f} Mbps plus spare capacity "
                f"({required_bandwidth:.3f} Mbps required) but only has "
                f"{bandwidth_capacity:.3f} Mbps."
            )
        if packet_capacity and required_packets > packet_capacity + 1e-9:
            raise NetworkPlanningError(
                f"Generated {instance.get('name') or instance_id} would process "
                f"{used_packets:.0f} packets/s plus spare capacity "
                f"({required_packets:.0f} packets/s required) but only has "
                f"{packet_capacity:.0f} packets/s."
            )


def _build_core_layer(
    builder: DesignBuilder,
    leaves_by_root: Dict[str, List[dict]],
    roots: Dict[str, dict],
    spare_fraction: float,
    *,
    links_per_leaf: int = 1,
    minimum_distinct_cores: int = 1,
    protection_prefix: str = "AUTO-CORE-UPLINK",
) -> List[dict]:
    """Build the smallest capable core layer across the available MERs.

    A single core switch is preferred at each MER when that device has enough
    physical ports, switching bandwidth and packet-forwarding capacity. When
    protected uplinks are required, the second path is taken to a core in a
    different MER before another core is added to the local MER. Additional
    local cores are generated only when a single asset cannot satisfy the
    calculated protected demand.
    """

    candidates = _choose_core_candidates(builder.data)
    if not candidates:
        builder.warnings.append(
            "No core/distribution switch asset was available; uplinks were not terminated at a core."
        )
        return []

    desired_links = max(1, int(links_per_leaf or 1))
    requested_distinct = max(1, min(desired_links, int(minimum_distinct_cores or 1)))
    active_root_names = [
        root_name
        for root_name, leaves in leaves_by_root.items()
        if leaves and root_name in roots
    ]
    if not active_root_names:
        return []

    root_count = len(active_root_names)
    total_leaves = sum(len(leaves_by_root.get(name, [])) for name in active_root_names)
    total_bandwidth_mbps = sum(
        max(0.0, _float(leaf.get("expected_bandwidth_mbps")))
        for name in active_root_names
        for leaf in leaves_by_root.get(name, [])
    )
    total_packet_rate_pps = sum(
        max(0.0, _float(leaf.get("expected_packet_rate_pps")))
        for name in active_root_names
        for leaf in leaves_by_root.get(name, [])
    )

    def asset_port_capacity(asset: dict) -> int:
        structured = sum(
            max(0, _int(row.get("port_count")))
            for row in asset.get("port_definitions", [])
            if isinstance(row, dict)
            and _text(row.get("port_use")).lower()
            not in {"power", "console", "management", "stacking"}
        )
        return max(structured, max(0, _int(asset.get("number_of_ports"))))

    candidate_by_id = {_text(asset.get("id")): asset for asset in candidates}
    core_groups: Dict[str, List[dict]] = {}
    core_assets: Dict[str, dict] = {}
    all_cores: List[dict] = []
    rack_size_u = max(1, _int(builder.settings.get("default_rack_size_u"), 42))

    for root_name in active_root_names:
        leaves = leaves_by_root.get(root_name, [])
        local_count = len(leaves)

        if root_count > 1 and desired_links > 1:
            # One primary link from local leaves plus a fair share of secondary
            # links from the other MERs. This is the actual protected port load
            # on one core at this MER, rather than multiplying every leaf by the
            # number of redundant paths at every MER.
            remote_copies = min(desired_links - 1, root_count - 1)
            remote_share = int(
                math.ceil(
                    max(0, total_leaves - local_count)
                    * remote_copies
                    / max(1, root_count - 1)
                )
            )
            required_ports = local_count + remote_share
            # Either MER must be capable of carrying the complete service load
            # after failure of the other protected path.
            protected_bandwidth = total_bandwidth_mbps
            protected_packets = total_packet_rate_pps
        else:
            required_ports = local_count * desired_links
            protected_bandwidth = sum(
                max(0.0, _float(item.get("expected_bandwidth_mbps")))
                for item in leaves
            )
            protected_packets = sum(
                max(0.0, _float(item.get("expected_packet_rate_pps")))
                for item in leaves
            )

        def required_ports_for_candidate(asset: dict) -> int:
            fake_core = {
                "id": f"__AUTO-CORE-CANDIDATE-{_text(asset.get('id'))}",
                "asset_id": _text(asset.get("id")),
            }

            def members_for(leaf: dict) -> int:
                count_needed, _speed = builder.preview_connection_bundle(
                    leaf,
                    "Uplink-1",
                    fake_core,
                    "1",
                    "fibre",
                    0,
                    max(0.0, _float(leaf.get("expected_bandwidth_mbps"))),
                )
                return max(1, count_needed)

            try:
                local_ports = sum(members_for(leaf) for leaf in leaves)
                if root_count > 1 and desired_links > 1:
                    remote_copies = min(desired_links - 1, root_count - 1)
                    remote_ports = sum(
                        members_for(leaf)
                        for other_root in active_root_names
                        if other_root != root_name
                        for leaf in leaves_by_root.get(other_root, [])
                    )
                    return local_ports + int(
                        math.ceil(
                            remote_ports
                            * remote_copies
                            / max(1, root_count - 1)
                        )
                    )
                return local_ports * desired_links
            except NetworkPlanningError:
                return 10**9

        candidate_port_requirements = {
            _text(asset.get("id")): required_ports_for_candidate(asset)
            for asset in candidates
        }
        finite_requirements = [
            value for value in candidate_port_requirements.values() if value < 10**9
        ]
        if finite_requirements:
            required_ports = max(required_ports, min(finite_requirements))

        capacity_candidates = [
            {**asset, "number_of_ports": asset_port_capacity(asset)}
            for asset in candidates
        ]

        # Prefer one switch that satisfies every declared requirement. Selecting
        # by ports first and then appending devices for traffic capacity can
        # otherwise retain a small switch beside a larger switch even though the
        # larger model alone satisfies ports, bandwidth and packet throughput.
        required_port_capacity = int(
            math.ceil(max(0, required_ports) * (1.0 + spare_fraction))
        )
        required_bandwidth_capacity = max(0.0, protected_bandwidth) * (
            1.0 + spare_fraction
        )
        required_packet_capacity = max(0.0, protected_packets) * (
            1.0 + spare_fraction
        )
        bandwidth_declared = any(
            _float(asset.get("bandwidth_capacity_gbps")) > 0
            for asset in candidates
        )
        packets_declared = any(
            _float(asset.get("packet_throughput_mpps")) > 0
            for asset in candidates
        )
        single_capable = []
        for asset in candidates:
            ports = asset_port_capacity(asset)
            candidate_required_port_capacity = int(
                math.ceil(
                    max(
                        0,
                        candidate_port_requirements.get(
                            _text(asset.get("id")), required_ports
                        ),
                    )
                    * (1.0 + spare_fraction)
                )
            )
            bandwidth = max(
                0.0, _float(asset.get("bandwidth_capacity_gbps")) * 1000.0
            )
            packets = max(
                0.0,
                _float(asset.get("packet_throughput_mpps")) * 1_000_000.0,
            )
            if ports < candidate_required_port_capacity:
                continue
            if bandwidth_declared and bandwidth + 1e-9 < required_bandwidth_capacity:
                continue
            if packets_declared and packets + 1e-9 < required_packet_capacity:
                continue
            single_capable.append(
                (
                    max(1, _rack_units_for_asset(asset, 1)),
                    _float(asset.get("power_input_w")),
                    ports - candidate_required_port_capacity,
                    (
                        bandwidth - required_bandwidth_capacity
                        if bandwidth_declared
                        else 0.0
                    ),
                    (
                        packets - required_packet_capacity
                        if packets_declared
                        else 0.0
                    ),
                    _text(asset.get("id")),
                    asset,
                )
            )

        if single_capable:
            mix = [min(single_capable, key=lambda row: row[:-1])[-1]]
        else:
            mix = _minimum_asset_mix(
                capacity_candidates,
                required_ports,
                0.0,
                spare_fraction,
                f"core switch at {root_name}",
            )
            mix = [
                candidate_by_id.get(_text(asset.get("id")), asset)
                for asset in mix
            ]
            mix = _ensure_aggregate_traffic_capacity(
                candidates,
                mix,
                protected_bandwidth,
                protected_packets,
                spare_fraction,
                f"core switch at {root_name}",
            )

        # With only one MER, device diversity must be local. With multiple MERs,
        # the second distinct core is supplied by the other MER and does not
        # justify a duplicate core at each location.
        if root_count == 1:
            while len(mix) < requested_distinct:
                mix.append(
                    min(
                        candidates,
                        key=lambda asset: (
                            max(1, _rack_units_for_asset(asset, 1)),
                            -asset_port_capacity(asset),
                            -_float(asset.get("bandwidth_capacity_gbps")),
                            -_float(asset.get("packet_throughput_mpps")),
                            _text(asset.get("id")),
                        ),
                    )
                )

        next_rack_u = 1
        rack_index = 1
        group: List[dict] = []
        for index, asset in enumerate(mix, start=1):
            rack_u = _rack_units_for_asset(asset, 1)
            if next_rack_u + rack_u - 1 > rack_size_u and next_rack_u > 1:
                rack_index += 1
                next_rack_u = 1
            instance = builder.add_instance(
                asset,
                f"AUTO Core {root_name} {index}",
                roots[root_name],
                "core_switch",
                rack_name=(
                    f"AUTO-RACK-{root_name}"
                    if rack_index == 1
                    else f"AUTO-RACK-{root_name}-{rack_index}"
                ),
                rack_start_u=next_rack_u,
                rack_size_u=rack_size_u,
                route_anchor=root_name,
                network_layer="core",
                expected_bandwidth_mbps=round(protected_bandwidth, 6),
                expected_packet_rate_pps=round(protected_packets, 3),
            )
            group.append(instance)
            core_assets[_text(instance.get("id"))] = asset
            all_cores.append(instance)
            next_rack_u += rack_u
        core_groups[root_name] = group

    port_capacity = {
        _text(core.get("id")): asset_port_capacity(
            core_assets.get(_text(core.get("id")), {})
        )
        for core in all_cores
    }
    used_ports: Dict[str, int] = defaultdict(int)

    def ordered_available(instances: Sequence[dict], offset: int = 0) -> List[dict]:
        rows = list(instances)
        if rows:
            offset %= len(rows)
            rows = rows[offset:] + rows[:offset]
        return sorted(
            rows,
            key=lambda core: (
                used_ports[_text(core.get("id"))]
                / max(1, port_capacity.get(_text(core.get("id")), 0)),
                used_ports[_text(core.get("id"))],
                _text(core.get("id")),
            ),
        )

    for root_index, root_name in enumerate(active_root_names):
        local_group = core_groups.get(root_name, [])
        remote_root_names = [
            active_root_names[(root_index + step) % root_count]
            for step in range(1, root_count)
        ]
        for leaf_index, leaf in enumerate(
            leaves_by_root.get(root_name, []), start=1
        ):
            candidate_buckets: List[List[dict]] = [
                ordered_available(local_group, leaf_index - 1)
            ]
            candidate_buckets.extend(
                ordered_available(core_groups.get(remote_root, []), leaf_index - 1)
                for remote_root in remote_root_names
            )
            flat_candidates: List[dict] = []
            for bucket in candidate_buckets:
                for core in bucket:
                    core_id = _text(core.get("id"))
                    remaining = (
                        port_capacity.get(core_id, 0) - used_ports[core_id]
                    )
                    if remaining <= 0:
                        continue
                    try:
                        member_count, _speed = builder.preview_connection_bundle(
                            leaf,
                            f"Uplink-{_int(leaf.get('_uplinks_used'), 0) + 1}",
                            core,
                            str(used_ports[core_id] + 1),
                            "fibre",
                            0,
                            max(
                                0.0,
                                _float(leaf.get("expected_bandwidth_mbps")),
                            ),
                        )
                    except NetworkPlanningError:
                        continue
                    if member_count <= remaining:
                        flat_candidates.append(core)

            selected: List[dict] = []
            selected_ids: Set[str] = set()
            for core in flat_candidates:
                core_id = _text(core.get("id"))
                if core_id in selected_ids:
                    continue
                selected.append(core)
                selected_ids.add(core_id)
                if len(selected) >= desired_links:
                    break

            if len(selected) < desired_links:
                raise NetworkPlanningError(
                    f"Core capacity cannot provide {desired_links} uplink(s) for "
                    f"{leaf.get('name') or leaf.get('id')}."
                )
            if len(selected_ids) < requested_distinct:
                raise NetworkPlanningError(
                    f"The design requires {requested_distinct} distinct core paths for "
                    f"{leaf.get('name') or leaf.get('id')}, but only "
                    f"{len(selected_ids)} are available."
                )

            protection_group = (
                f"{protection_prefix}-{_text(leaf.get('id')) or leaf_index}"
            )
            source_ids: List[str] = []
            connection_ids: List[str] = []
            for link_index, core in enumerate(selected, start=1):
                core_id = _text(core.get("id"))
                core_root = _text(core.get("route_anchor")) or _text(
                    core.get("location_name")
                )
                leaf_location = _text(leaf.get("route_anchor")) or _text(
                    leaf.get("location_name")
                )
                connection = builder.add_connection(
                    leaf,
                    f"Uplink-{_int(leaf.get('_uplinks_used'), 0) + 1}",
                    core,
                    str(used_ports[core_id] + 1),
                    "fibre",
                    leaf_location,
                    core_root,
                    redundancy_role=(
                        "primary" if link_index == 1 else "secondary"
                    ),
                    protection_group=(
                        protection_group if len(selected) > 1 else ""
                    ),
                    standby=link_index > 1,
                    expected_bandwidth_mbps=max(
                        0.0, _float(leaf.get("expected_bandwidth_mbps"))
                    ),
                    expected_packet_rate_pps=max(
                        0.0, _float(leaf.get("expected_packet_rate_pps"))
                    ),
                    fibre_count=2,
                )
                member_count = max(
                    1, _int(connection.get("link_aggregation_member_count"), 1)
                )
                used_ports[core_id] += member_count
                leaf["_uplinks_used"] = (
                    _int(leaf.get("_uplinks_used"), 0) + member_count
                )
                source_ids.append(core_id)
                lag_id = _text(connection.get("link_aggregation_group_id"))
                if lag_id:
                    connection_ids.extend(
                        _text(row.get("id"))
                        for row in builder.connections
                        if _text(row.get("link_aggregation_group_id")) == lag_id
                    )
                else:
                    connection_ids.append(_text(connection.get("id")))

            if len(selected) > 1:
                builder.redundancy_groups.append(
                    {
                        "id": protection_group,
                        "technology": builder.technology,
                        "protected_instance_id": _text(leaf.get("id")),
                        "source_instance_ids": source_ids,
                        "source_core_instance_ids": source_ids,
                        "connection_ids": connection_ids,
                        "required_distinct_sources": requested_distinct,
                        "protection_type": "independent_core_uplinks",
                        "auto_generated": True,
                    }
                )

    return all_cores

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
    switch_candidates = _apply_manufacturer_preference(
        builder.data, switch_candidates, "access_switch", "access switch"
    )
    switch_candidates = [_endpoint_capacity_asset(asset) for asset in switch_candidates]

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
        mix = _ensure_aggregate_traffic_capacity(
            switch_candidates,
            mix,
            sum(item.expected_bandwidth_mbps for item in port_items),
            sum(item.expected_packet_rate_pps for item in port_items),
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
                expected_bandwidth_mbps=round(
                    sum(
                        item.expected_bandwidth_mbps
                        for assigned in group_assignments
                        for item in assigned
                    ),
                    6,
                ),
                expected_packet_rate_pps=round(
                    sum(
                        item.expected_packet_rate_pps
                        for assigned in group_assignments
                        for item in assigned
                    ),
                    3,
                ),
            )
            instance["rack_size_u"] = rack_size_u
            for member_offset, assigned in enumerate(group_assignments, start=1):
                for port_number, item in enumerate(assigned, start=1):
                    length_m, path = builder.graph.route(room_name, item.endpoint_name)
                    length_m += item.extension_distance_m
                    physical_port = item.selected_network_port or str(port_number)
                    builder.add_assignment(
                        item,
                        instance,
                        (f"{member_offset}/{physical_port}" if is_stack else physical_port),
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
    candidates: Sequence[dict],
    items: Sequence[PortDemand],
    spare_fraction: float,
) -> bool:
    ports = len(items)
    poe_w = sum(item.poe_power_w for item in items)
    bandwidth_mbps = sum(item.expected_bandwidth_mbps for item in items)
    packet_rate_pps = sum(item.expected_packet_rate_pps for item in items)
    required_ports = int(math.ceil(ports * (1.0 + spare_fraction)))
    required_poe = poe_w * (1.0 + spare_fraction)
    required_bandwidth = bandwidth_mbps * (1.0 + spare_fraction)
    required_packets = packet_rate_pps * (1.0 + spare_fraction)
    return any(
        _int(asset.get("number_of_ports")) >= required_ports
        and _float(asset.get("poe_budget_w")) + 1e-9 >= required_poe
        and _can_allocate_endpoint_speeds(asset, items)
        and (
            _float(asset.get("bandwidth_capacity_gbps")) <= 0
            or _float(asset.get("bandwidth_capacity_gbps")) * 1000.0 + 1e-9
            >= required_bandwidth
        )
        and (
            _float(asset.get("packet_throughput_mpps")) <= 0
            or _float(asset.get("packet_throughput_mpps")) * 1_000_000.0 + 1e-9
            >= required_packets
        )
        for asset in candidates
    )


def _choose_single_ont(
    candidates: Sequence[dict], items: Sequence[PortDemand], spare_fraction: float
) -> dict:
    ports = len(items)
    poe = sum(item.poe_power_w for item in items)
    bandwidth = sum(item.expected_bandwidth_mbps for item in items)
    packets = sum(item.expected_packet_rate_pps for item in items)
    feasible = [
        asset
        for asset in candidates
        if _int(asset.get("number_of_ports"))
        >= math.ceil(ports * (1.0 + spare_fraction))
        and _can_allocate_endpoint_speeds(asset, items)
        and _float(asset.get("poe_budget_w")) + 1e-9 >= poe * (1.0 + spare_fraction)
        and (
            _float(asset.get("bandwidth_capacity_gbps")) <= 0
            or _float(asset.get("bandwidth_capacity_gbps")) * 1000.0 + 1e-9
            >= bandwidth * (1.0 + spare_fraction)
        )
        and (
            _float(asset.get("packet_throughput_mpps")) <= 0
            or _float(asset.get("packet_throughput_mpps")) * 1_000_000.0 + 1e-9
            >= packets * (1.0 + spare_fraction)
        )
    ]
    if not feasible:
        raise NetworkPlanningError(
            f"No ONT can support a cluster of {ports} ports, {poe:.1f} W PoE, "
            f"{bandwidth:.3f} Mbps and {packets:.0f} packets/s."
        )
    return min(
        feasible,
        key=lambda asset: (
            _int(asset.get("number_of_ports")) - ports,
            _float(asset.get("poe_budget_w")) - poe,
            (
                _float(asset.get("bandwidth_capacity_gbps")) * 1000.0 - bandwidth
                if _float(asset.get("bandwidth_capacity_gbps")) > 0
                else float("inf")
            ),
            (
                _float(asset.get("packet_throughput_mpps")) * 1_000_000.0 - packets
                if _float(asset.get("packet_throughput_mpps")) > 0
                else float("inf")
            ),
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
            cluster_bandwidth = 0.0
            cluster_packets = 0.0
            for point in candidates:
                took_any = False
                queue = remaining[point.name]
                while queue:
                    item = queue[0]
                    next_ports = len(cluster_items) + 1
                    next_poe = cluster_poe + item.poe_power_w
                    next_bandwidth = cluster_bandwidth + item.expected_bandwidth_mbps
                    next_packets = cluster_packets + item.expected_packet_rate_pps
                    if not _fits_any_ont(
                        ont_candidates, cluster_items + [item], spare_fraction
                    ):
                        break
                    cluster_items.append(queue.popleft())
                    cluster_poe = next_poe
                    cluster_bandwidth = next_bandwidth
                    cluster_packets = next_packets
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
            _pack_ports_to_devices(cluster_items, [asset], spare_fraction)
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
    total_bandwidth_mbps = sum(
        endpoint.expected_bandwidth_mbps for endpoint in endpoints
    )
    total_packet_rate_pps = sum(
        endpoint.expected_packet_rate_pps for endpoint in endpoints
    )
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
    ont_candidates = _apply_manufacturer_preference(
        builder.data, ont_candidates, "optical_network_terminal", "ONT"
    )
    ont_candidates = [_endpoint_capacity_asset(asset) for asset in ont_candidates]

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
            expected_bandwidth_mbps=round(
                sum(item.expected_bandwidth_mbps for item in port_items), 6
            ),
            expected_packet_rate_pps=round(
                sum(item.expected_packet_rate_pps for item in port_items), 3
            ),
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
                item.selected_network_port or str(port_number),
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
    olt_candidates = _apply_manufacturer_preference(
        builder.data, olt_candidates, "optical_line_terminal", "OLT"
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
    if splitter_candidates:
        splitter_candidates = _apply_manufacturer_preference(
            builder.data, splitter_candidates, "fibre_splitter", "fibre splitter"
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

            splitter_bandwidth_mbps = sum(
                max(0.0, _float(row["instance"].get("expected_bandwidth_mbps")))
                for row in group
            )
            splitter_packet_rate_pps = sum(
                max(0.0, _float(row["instance"].get("expected_packet_rate_pps")))
                for row in group
            )
            splitter = builder.add_instance(
                asset,
                f"AUTO Splitter F{floor} {len(splitter_records) + 1}",
                location,
                "protected_splitter" if failover else "splitter",
                route_anchor=anchor,
                protected=failover,
                expected_bandwidth_mbps=round(splitter_bandwidth_mbps, 6),
                expected_packet_rate_pps=round(splitter_packet_rate_pps, 3),
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
                    expected_bandwidth_mbps=max(
                        0.0,
                        _float(ont_record["instance"].get("expected_bandwidth_mbps")),
                    ),
                    expected_packet_rate_pps=max(
                        0.0,
                        _float(ont_record["instance"].get("expected_packet_rate_pps")),
                    ),
                    fibre_count=1,
                )
            splitter_records.append(
                {
                    "instance": splitter,
                    "location": location,
                    "anchor": anchor,
                    "output_count": len(group),
                    "split_capacity": _split_ratio_outputs(base_asset),
                    "expected_bandwidth_mbps": round(splitter_bandwidth_mbps, 6),
                    "expected_packet_rate_pps": round(splitter_packet_rate_pps, 3),
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

    # OLT chassis expose both PON interfaces and Ethernet uplinks.  The generic
    # asset port count must not be used as the PON count.  A protected core path
    # may use a same-speed LACP bundle, so reserve enough OLT uplink ports for
    # every independent core path and expose the resulting per-path aggregate
    # capacity to the OLT packing calculation.
    core_candidates_for_olt = _choose_core_candidates(builder.data)
    core_fibre_speeds = {
        speed
        for core_asset in core_candidates_for_olt
        for port_row in core_asset.get("port_definitions", [])
        if isinstance(port_row, dict)
        and _text(port_row.get("port_type")).lower()
        in PLUGGABLE_OPTIC_PORT_TYPES | {"pon", "lc", "sc", "mpo"}
        for speed in normalise_port_speeds(port_row.get("supported_speeds_mbps"))
    }

    olt_core_links = max(
        2 if bool(builder.settings.get("redundant_core", True)) else 1,
        _int(builder.settings.get("independent_link_count"), 2)
        if bool(builder.settings.get("redundant_core", True))
        else 1,
    )

    def olt_pon_port_count(asset: dict) -> int:
        structured = sum(
            max(0, _int(row.get("port_count")))
            for row in asset.get("port_definitions", [])
            if isinstance(row, dict)
            and (
                _text(row.get("port_type")).lower() == "pon"
                or _text(row.get("port_use")).lower() == "pon"
            )
        )
        return max(1, structured or _int(asset.get("connections_out"), 0))

    def protected_olt_uplink_capacity_mbps(asset: dict) -> float:
        speed_port_counts: Dict[int, int] = defaultdict(int)
        for row in asset.get("port_definitions", []):
            if not isinstance(row, dict):
                continue
            if _text(row.get("port_use")).lower() != "uplink":
                continue
            if _text(row.get("port_type")).lower() not in PLUGGABLE_OPTIC_PORT_TYPES:
                continue
            port_count = max(0, _int(row.get("port_count")))
            for speed in normalise_port_speeds(row.get("supported_speeds_mbps")):
                if core_fibre_speeds and speed not in core_fibre_speeds:
                    continue
                speed_port_counts[speed] += port_count

        aggregate_path_capacity = 0.0
        for speed, available_ports in speed_port_counts.items():
            members_per_path = available_ports // max(1, olt_core_links)
            if members_per_path <= 0:
                continue
            aggregate_path_capacity = max(
                aggregate_path_capacity,
                float(speed * members_per_path),
            )

        declared = max(0.0, _float(asset.get("bandwidth_capacity_gbps"))) * 1000.0
        if declared > 0.0 and aggregate_path_capacity > 0.0:
            return min(aggregate_path_capacity, declared)
        return max(aggregate_path_capacity, declared)

    planning_olts: List[dict] = []
    for asset in eligible_olts:
        protected_capacity = protected_olt_uplink_capacity_mbps(asset)
        if protected_capacity <= 0.0:
            continue
        planning_olts.append(
            {
                **asset,
                "number_of_ports": olt_pon_port_count(asset),
                "connections_out": olt_pon_port_count(asset),
                "bandwidth_capacity_gbps": protected_capacity / 1000.0,
                "_protected_uplink_capacity_mbps": protected_capacity,
            }
        )
    if not planning_olts:
        raise NetworkPlanningError(
            "No OLT has an uplink speed compatible with the available core switches."
        )

    def build_olt_side(side: str, root: dict) -> List[dict]:
        mix = _minimum_asset_mix(
            planning_olts, required_pon_ports, 0.0, spare_fraction, f"{side} OLT"
        )
        mix = _ensure_aggregate_traffic_capacity(
            planning_olts,
            mix,
            total_bandwidth_mbps,
            total_packet_rate_pps,
            spare_fraction,
            f"{side} OLT",
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
                expected_bandwidth_mbps=0.0,
                expected_packet_rate_pps=0.0,
            )
            for index, asset in enumerate(mix)
        ]
        remaining_ports = [max(1, _int(asset.get("number_of_ports"), 1)) for asset in mix]
        remaining_bandwidth = [
            max(0.0, _float(asset.get("_protected_uplink_capacity_mbps")))
            for asset in mix
        ]
        used_pon_ports = [0 for _asset in mix]
        allocation_rows = sorted(
            enumerate(splitter_records, start=1),
            key=lambda row: (
                -max(0.0, _float(row[1].get("expected_bandwidth_mbps"))),
                -max(0.0, _float(row[1].get("expected_packet_rate_pps"))),
                row[0],
            ),
        )
        for splitter_index, splitter_record in allocation_rows:
            branch_bandwidth = max(
                0.0, _float(splitter_record.get("expected_bandwidth_mbps"))
            )
            candidates = [
                index
                for index in range(len(instances))
                if remaining_ports[index] > 0
                and remaining_bandwidth[index] + 1e-9 >= branch_bandwidth
            ]
            if not candidates:
                expandable = [
                    asset
                    for asset in planning_olts
                    if max(0.0, _float(asset.get("_protected_uplink_capacity_mbps")))
                    + 1e-9
                    >= branch_bandwidth
                ]
                if not expandable:
                    raise NetworkPlanningError(
                        f"No available {side.lower()} OLT can carry splitter "
                        f"{splitter_record['instance'].get('name') or splitter_index} "
                        f"({branch_bandwidth:.3f} Mbps) over one protected core path."
                    )
                extra_asset = min(
                    expandable,
                    key=lambda asset: (
                        max(0.0, _float(asset.get("_protected_uplink_capacity_mbps")))
                        - branch_bandwidth,
                        max(1, _rack_units_for_asset(asset, 1)),
                        _text(asset.get("id")),
                    ),
                )
                mix.append(extra_asset)
                extra_index = len(instances)
                instances.append(
                    builder.add_instance(
                        extra_asset,
                        f"AUTO {side} OLT {extra_index + 1}",
                        root,
                        "olt_primary" if side == "Primary" else "olt_secondary",
                        rack_name=f"AUTO-RACK-{_text(root.get('name'))}",
                        rack_start_u=extra_index + 1,
                        route_anchor=_text(root.get("name")),
                        olt_side=side.lower(),
                        core_redundancy_role=side.lower(),
                        expected_bandwidth_mbps=0.0,
                        expected_packet_rate_pps=0.0,
                    )
                )
                remaining_ports.append(
                    max(1, _int(extra_asset.get("number_of_ports"), 1))
                )
                remaining_bandwidth.append(
                    max(
                        0.0,
                        _float(extra_asset.get("_protected_uplink_capacity_mbps")),
                    )
                )
                used_pon_ports.append(0)
                candidates = [extra_index]
                builder.warnings.append(
                    f"An additional {side.lower()} OLT was added to avoid "
                    "fragmenting protected uplink capacity across PON branches."
                )
            instance_index = min(
                candidates,
                key=lambda index: (
                    -remaining_bandwidth[index],
                    -remaining_ports[index],
                    index,
                ),
            )
            used_pon_ports[instance_index] += 1
            port_number = used_pon_ports[instance_index]
            remaining_ports[instance_index] -= 1
            remaining_bandwidth[instance_index] = max(
                0.0, remaining_bandwidth[instance_index] - branch_bandwidth
            )
            input_name = "Input-A" if side == "Primary" else "Input-B"
            protection_group = f"AUTO-PG-{splitter_index}" if failover else ""
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
                expected_bandwidth_mbps=max(
                    0.0, _float(splitter_record.get("expected_bandwidth_mbps"))
                ),
                expected_packet_rate_pps=max(
                    0.0, _float(splitter_record.get("expected_packet_rate_pps"))
                ),
                fibre_count=1,
            )
            instances[instance_index]["expected_bandwidth_mbps"] = round(
                max(0.0, _float(instances[instance_index].get("expected_bandwidth_mbps")))
                + max(0.0, _float(splitter_record.get("expected_bandwidth_mbps"))),
                6,
            )
            instances[instance_index]["expected_packet_rate_pps"] = round(
                max(0.0, _float(instances[instance_index].get("expected_packet_rate_pps")))
                + max(0.0, _float(splitter_record.get("expected_packet_rate_pps"))),
                3,
            )
            if side == "Primary" and failover:
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
    _build_core_layer(
        builder,
        leaves_by_root,
        roots,
        spare_fraction,
        links_per_leaf=olt_core_links,
        minimum_distinct_cores=2 if olt_core_links > 1 else 1,
        protection_prefix="AUTO-OLT-CORE",
    )



def _external_network_asset(builder: DesignBuilder, record: dict, port_count: int) -> dict:
    candidates = _candidate_assets(
        builder.data,
        "external_network",
        lambda asset: max(_int(asset.get("number_of_ports")), _int(asset.get("connections_out"))) >= port_count,
    )
    if candidates:
        return min(candidates, key=lambda asset: (_int(asset.get("rack_units")), _text(asset.get("id"))))
    asset_id = f"AUTO-EXTERNAL-{_text(record.get('id')) or len(builder.asset_ids) + 1}"
    asset = {
        "id": asset_id,
        "name": _text(record.get("name")) or "External network",
        "asset_type": "external_network",
        "number_of_ports": max(1, port_count),
        "connections_in": max(1, port_count),
        "connections_out": max(1, port_count),
        "uplink_ports": max(1, port_count),
        "input_connection_type": _text(record.get("medium")) or "fibre",
        "output_connection_type": _text(record.get("medium")) or "fibre",
        "uplink_connection_type": _text(record.get("medium")) or "fibre",
        "rack_units": 0,
        "power_input_w": 0.0,
        "notes": "Auto-generated external carrier / internet demarcation object.",
        "auto_network_asset": True,
    }
    builder.data.setdefault("network_assets", []).append(asset)
    builder.asset_ids.add(asset_id)
    return asset


def _install_external_network_connections(builder: DesignBuilder, spare_fraction: float) -> None:
    """Install edge routers and independent carrier/internet connections."""
    external_records = [
        row for row in builder.data.get("network_external_networks", [])
        if isinstance(row, dict) and _text(row.get("id"))
    ]
    if not external_records:
        return
    router_candidates = _candidate_assets(
        builder.data,
        "network_router",
        lambda asset: max(_int(asset.get("number_of_ports")), _int(asset.get("uplink_ports")), _int(asset.get("connections_in")) + _int(asset.get("connections_out"))) >= 2,
    )
    if not router_candidates:
        router_candidates = _candidate_assets(
            builder.data,
            "firewall",
            lambda asset: max(_int(asset.get("number_of_ports")), _int(asset.get("uplink_ports")), _int(asset.get("connections_in")) + _int(asset.get("connections_out"))) >= 2,
        )
    if not router_candidates:
        builder.warnings.append(
            "External networks are configured but no router/firewall asset with at least two ports is available."
        )
        return
    router_candidates = _apply_manufacturer_preference(
        builder.data, router_candidates, "edge_router", "edge router"
    )
    default_links = max(1, _int(builder.settings.get("external_network_link_count"), 2))
    required_router_count = max(
        max(
            1,
            _int(record.get("required_links"), default_links if bool(record.get("redundant", True)) else 1),
        )
        for record in external_records
    )
    if any(bool(record.get("redundant", True)) for record in external_records):
        required_router_count = max(2, required_router_count)

    roots = _locations_by_kind(builder.data, {"mer", "comms_room", "telco_pop"})
    if not roots:
        builder.warnings.append("External networks were not connected because no MER/comms-room location exists.")
        return
    best_router = min(
        router_candidates,
        key=lambda asset: (
            max(1, _rack_units_for_asset(asset, 1)),
            -_int(asset.get("number_of_ports")),
            _text(asset.get("id")),
        ),
    )
    routers: List[dict] = []
    for index in range(required_router_count):
        root = roots[index % len(roots)]
        router = builder.add_instance(
            best_router,
            f"AUTO Edge Router {index + 1}",
            root,
            "edge_router",
            rack_name=f"AUTO-RACK-{_text(root.get('name'))}",
            rack_start_u=index + 1,
            rack_size_u=max(1, _int(builder.settings.get("default_rack_size_u"), 42)),
            route_anchor=_text(root.get("name")),
            network_layer="edge",
        )
        routers.append(router)

    cores = [
        row for row in builder.instances
        if _text(row.get("design_role")) == "core_switch"
    ]
    if cores:
        for index, router in enumerate(routers):
            core = cores[index % len(cores)]
            group_id = f"AUTO-EDGE-CORE-{index + 1}"
            builder.add_connection(
                router,
                "LAN-1",
                core,
                f"Edge-{index + 1}",
                "fibre",
                _text(router.get("route_anchor")),
                _text(core.get("route_anchor")) or _text(core.get("location_name")),
                connection_role="output",
                redundancy_role="primary" if index == 0 else "secondary",
                protection_group=group_id if len(routers) > 1 else "",
                standby=index > 0,
                fibre_count=2,
            )
    else:
        builder.warnings.append("Edge routers were generated without a core connection because no generated core switch exists.")

    existing_instances = {
        _text(row.get("id")): row
        for row in builder.data.get("network_asset_instances", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    existing_instances.update({_text(row.get("id")): row for row in builder.instances})
    locations = {
        _text(row.get("name")): row
        for row in builder.data.get("locations", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }
    for record_index, record in enumerate(external_records, start=1):
        desired_links = max(
            1,
            _int(record.get("required_links"), default_links if bool(record.get("redundant", True)) else 1),
        )
        desired_links = min(desired_links, len(routers))
        location = locations.get(_text(record.get("location_name"))) or roots[(record_index - 1) % len(roots)]
        demarcation = existing_instances.get(_text(record.get("demarcation_instance_id")))
        if demarcation is None:
            external_asset = _external_network_asset(builder, record, desired_links)
            demarcation = builder.add_instance(
                external_asset,
                f"AUTO {_text(record.get('name')) or 'External Network'} Demarcation",
                location,
                "external_network",
                route_anchor=_text(location.get("name")),
                network_layer="external",
                external_network_id=_text(record.get("id")),
            )
            record["demarcation_instance_id"] = _text(demarcation.get("id"))
            existing_instances[_text(demarcation.get("id"))] = demarcation
        selected_routers = routers[:desired_links]
        peer_ids: List[str] = []
        connection_ids: List[str] = []
        protection_group = f"AUTO-EXT-PG-{_text(record.get('id'))}"
        medium = _text(record.get("medium")).lower() or "fibre"
        for link_index, router in enumerate(selected_routers, start=1):
            connection = builder.add_connection(
                demarcation,
                f"Service-{link_index}",
                router,
                f"WAN-{record_index}-{link_index}",
                medium,
                _text(demarcation.get("route_anchor")) or _text(demarcation.get("location_name")),
                _text(router.get("route_anchor")) or _text(router.get("location_name")),
                connection_role="uplink",
                redundancy_role="primary" if link_index == 1 else "secondary",
                protection_group=protection_group if desired_links > 1 else "",
                standby=link_index > 1,
                fibre_count=2 if medium == "fibre" else 0,
                external_network_id=_text(record.get("id")),
            )
            peer_ids.append(_text(router.get("id")))
            connection_ids.append(_text(connection.get("id")))
        record["peer_instance_ids"] = peer_ids
        record["redundancy_group_id"] = protection_group if desired_links > 1 else ""
        if desired_links > 1:
            builder.redundancy_groups.append(
                {
                    "id": protection_group,
                    "technology": builder.technology,
                    "protected_instance_id": _text(demarcation.get("id")),
                    "source_instance_ids": peer_ids,
                    "connection_ids": connection_ids,
                    "required_distinct_sources": desired_links,
                    "protection_type": "redundant_external_network_links",
                    "auto_generated": True,
                }
            )


def _fibre_patch_panel_asset(builder: DesignBuilder) -> dict:
    """Return a modular fibre panel, creating a four-cassette model when absent.

    Fixed fibre panels remain valid user assets, but the automatic design uses a
    modular chassis so separate incoming cables can share unused connector and
    cassette capacity and high-core-count trunks can use MPO/MTP breakout
    cassettes.
    """
    candidates = _candidate_assets(
        builder.data,
        "patch_panel",
        lambda asset: _text(asset.get("patch_panel_type")).lower() == "fibre"
        and bool(asset.get("modular_patch_panel") or _text(asset.get("patch_panel_format")).lower() == "modular_cassette")
        and max(_int(asset.get("number_of_ports")), _int(asset.get("connections_in")), _int(asset.get("connections_out"))) > 0,
    )
    candidates = _apply_manufacturer_preference(
        builder.data, candidates, "fibre_patch_panel", "modular fibre patch panel"
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

    asset_id = "AUTO-FIBRE-PATCH-PANEL-MODULAR-4X12"
    for asset in builder.data.get("network_assets", []):
        if _text(asset.get("id")) == asset_id:
            return asset

    cassette_count = 4
    cassette_capacity = 12
    explicit_names = [
        f"C{cassette}-LC-{position:02d}"
        for cassette in range(1, cassette_count + 1)
        for position in range(1, cassette_capacity + 1)
    ]
    planning = builder.settings.get("physical_fibre_planning", {})
    mpo_connector = _text(planning.get("mpo_breakout_connector")).lower() or "mpo-24"
    if mpo_connector not in {"mpo-12", "mtp-12", "mpo-24", "mtp-24"}:
        mpo_connector = "mpo-24"
    asset = {
        "id": asset_id,
        "name": "4-cassette OS2 modular fibre patch panel",
        "asset_type": "patch_panel",
        "patch_panel_type": "fibre",
        "patch_panel_format": "modular_cassette",
        "modular_patch_panel": True,
        "patch_panel_cassette_count": cassette_count,
        "patch_panel_cassette_capacity": cassette_capacity,
        "patch_panel_cassette_front_connector": "lc_duplex",
        "patch_panel_cassette_termination_mode": "spliced",
        "patch_panel_cassette_rear_connector": mpo_connector,
        "patch_panel_cassette_rear_connector_count": 1,
        "patch_panel_mpo_breakout_minimum_cores": max(
            12, _int(planning.get("mpo_breakout_minimum_cores"), 48)
        ),
        "number_of_ports": cassette_count * cassette_capacity,
        "connections_in": cassette_count * cassette_capacity,
        "connections_out": cassette_count * cassette_capacity,
        "uplink_ports": 0,
        "port_definitions": [
            {
                "port_type": "lc",
                "port_count": cassette_count * cassette_capacity,
                "port_use": "patch",
                "name_prefix": "LC",
                "explicit_names": explicit_names,
                "supported_speeds_mbps": [],
            }
        ],
        "input_connection_type": "fibre",
        "output_connection_type": "fibre",
        "rack_units": 1,
        "power_input_w": 0.0,
        "optical_insertion_loss_db": 0.5,
        "optical_return_loss_db": 55.0,
        "notes": (
            "Automatically generated modular fibre panel with four 12-position "
            "LC duplex cassettes. Cassettes may be spliced or connectorised "
            "with multiple MPO/MTP rear interfaces."
        ),
        "auto_network_asset": True,
    }
    builder.data.setdefault("network_assets", []).append(asset)
    builder.asset_ids.add(asset_id)
    builder.warnings.append(
        "No modular fibre patch-panel asset was available; a 48-position 1U "
        "panel with four configurable 12-position cassettes was generated automatically."
    )
    return asset


def _install_fibre_patch_panels(builder: DesignBuilder, spare_fraction: float) -> None:
    """Terminate cabinet-to-field fibre on shared modular patch panels.

    The allocator fills unused compatible cassette positions before adding a new
    cassette or panel.  Cables at or above the configured core threshold use
    connectorised MPO/MTP rear interfaces; smaller cables use the panel's
    configured cassette termination method.  Logical topology links remain
    direct active-device links and all panel records stay topology-hidden.
    """

    instance_by_id = {_text(row.get("id")): row for row in builder.instances}

    def rack_key(instance: dict) -> Optional[Tuple[int, str, str]]:
        rack_name = _text(instance.get("rack_name"))
        if not rack_name:
            return None
        return (
            _int(instance.get("floor")),
            _text(instance.get("location_name")),
            rack_name,
        )

    eligible: List[Tuple[dict, dict, dict]] = []
    for row in list(builder.connections):
        if _text(row.get("medium")).lower() != "fibre":
            continue
        if bool(row.get("physical_connection")):
            continue
        a = instance_by_id.get(_text(row.get("from_instance_id")))
        b = instance_by_id.get(_text(row.get("to_instance_id")))
        if not a or not b:
            continue
        a_rack = rack_key(a)
        b_rack = rack_key(b)
        if a_rack is not None and a_rack == b_rack:
            row["physical_segment"] = "intra_rack"
            continue
        if a_rack is None and b_rack is None:
            continue
        row["logical_topology"] = True
        eligible.append((row, a, b))

    if not eligible:
        return

    panel_asset = _fibre_patch_panel_asset(builder)
    cassette_count = max(1, min(4, _int(panel_asset.get("patch_panel_cassette_count"), 4)))
    cassette_capacity = 12
    capacity = cassette_count * cassette_capacity
    front_key = _text(panel_asset.get("patch_panel_cassette_front_connector")).lower() or "lc_duplex"
    front_port_type = "sc" if front_key.startswith("sc_") else "lc"
    fibres_per_position = 1 if front_key == "sc_simplex" else 2
    default_mode = _text(panel_asset.get("patch_panel_cassette_termination_mode")).lower() or "spliced"
    if default_mode not in {"spliced", "connectorised"}:
        default_mode = "spliced"
    default_rear = _text(panel_asset.get("patch_panel_cassette_rear_connector")).lower() or "mpo-24"
    if default_rear not in {"mpo-12", "mtp-12", "mpo-24", "mtp-24"}:
        default_rear = "mpo-24"
    planning = builder.settings.get("physical_fibre_planning", {})
    mpo_threshold = max(
        12,
        _int(
            panel_asset.get("patch_panel_mpo_breakout_minimum_cores"),
            _int(planning.get("mpo_breakout_minimum_cores"), 48),
        ),
    )
    planned_mpo_connector = _text(planning.get("mpo_breakout_connector")).lower() or default_rear
    if planned_mpo_connector not in {"mpo-12", "mtp-12", "mpo-24", "mtp-24"}:
        planned_mpo_connector = default_rear
    rear_fibres = 24 if planned_mpo_connector.endswith("24") else 12

    default_cable_cores = max(2, _int(builder.settings.get("default_fibre_core_count"), 12))
    connection_plan: Dict[str, dict] = {}
    required_positions_by_rack: Dict[Tuple[int, str, str], int] = defaultdict(int)
    for logical, a, b in eligible:
        logical_id = _text(logical.get("id"))
        used_cores = max(1, _int(logical.get("fibre_count"), 2))
        declared_core_count = _int(logical.get("cable_core_count"), 0)
        cable_core_count = max(
            used_cores,
            declared_core_count if declared_core_count > 0 else default_cable_cores,
        )
        connector_positions = max(1, int(math.ceil(cable_core_count / float(fibres_per_position))))
        active_positions = max(1, int(math.ceil(used_cores / float(fibres_per_position))))
        connectorised_breakout = cable_core_count >= mpo_threshold
        termination_mode = "connectorised" if connectorised_breakout else default_mode
        rear_connector = planned_mpo_connector if termination_mode == "connectorised" else "splice"
        connection_plan[logical_id] = {
            "used_cores": used_cores,
            "cable_core_count": cable_core_count,
            "connector_positions": connector_positions,
            "active_positions": active_positions,
            "termination_mode": termination_mode,
            "rear_connector": rear_connector,
            "connectorised_breakout": connectorised_breakout,
        }
        for instance in (a, b):
            key = rack_key(instance)
            if key is not None:
                required_positions_by_rack[key] += connector_positions

    locations = {
        _text(row.get("name")): row
        for row in builder.data.get("locations", [])
        if isinstance(row, dict) and _text(row.get("name"))
    }
    panels_by_rack: Dict[Tuple[int, str, str], List[dict]] = defaultdict(list)

    def empty_cassettes() -> List[dict]:
        return [
            {
                "position": index,
                "front_connector": front_key,
                "front_connector_capacity": cassette_capacity,
                "termination_mode": default_mode,
                "rear_connector": default_rear if default_mode == "connectorised" else "splice",
                "rear_connector_count": 0,
                "used_front_connectors": 0,
                "used_front_connector_names": [],
                "used_fibres": 0,
                "cable_ids": [],
                "associated_instance_ids": [],
            }
            for index in range(1, cassette_count + 1)
        ]

    def add_panel(key: Tuple[int, str, str]) -> dict:
        floor, location_name, rack_name = key
        location = locations.get(location_name) or {
            "name": location_name,
            "floor": floor,
            "x": 0.0,
            "y": 0.0,
        }
        panel_index = len(panels_by_rack[key]) + 1
        panel = builder.add_instance(
            panel_asset,
            f"AUTO {location_name} {rack_name} Modular Fibre Patch Panel {panel_index}",
            location,
            "fibre_patch_panel",
            rack_name=rack_name,
            target_rack_name=rack_name,
            rack_start_u=0,
            rack_size_u=max(1, _int(builder.settings.get("default_rack_size_u"), 42)),
            route_anchor=location_name,
            termination_count=0,
            available_connector_count=capacity,
            cabinet_patch_panel=True,
            external_field_termination=True,
            fibre_cassettes=empty_cassettes(),
        )
        panels_by_rack[key].append(panel)
        instance_by_id[_text(panel.get("id"))] = panel
        return panel

    for key, used in sorted(required_positions_by_rack.items()):
        required = max(1, int(math.ceil(used * (1.0 + spare_fraction))))
        for _ in range(max(1, int(math.ceil(required / float(capacity))))):
            add_panel(key)

    def panel_port_names(panel: dict) -> List[str]:
        names = [
            _text(row.get("name"))
            for row in builder._expanded_ports(panel)
            if _text(row.get("name"))
        ]
        if len(names) < capacity:
            connector_label = "SC" if front_port_type == "sc" else "LC"
            names = [
                f"C{cassette}-{connector_label}-{position:02d}"
                for cassette in range(1, cassette_count + 1)
                for position in range(1, cassette_capacity + 1)
            ]
        return names[:capacity]

    def allocate(
        instance: dict,
        count: int,
        *,
        logical_id: str,
        cable_core_count: int,
        termination_mode: str,
        rear_connector: str,
    ) -> List[dict]:
        key = rack_key(instance)
        if key is None:
            raise NetworkPlanningError("Cannot allocate a fibre panel outside a rack.")
        remaining = max(1, int(count))
        remaining_fibres = max(1, int(cable_core_count))
        allocations: List[dict] = []
        panels = panels_by_rack[key]
        panel_cursor = 0
        while remaining > 0:
            if panel_cursor >= len(panels):
                add_panel(key)
            panel = panels[panel_cursor]
            names = panel_port_names(panel)
            cassette_rows = panel.setdefault("fibre_cassettes", empty_cassettes())
            progress = False
            for cassette_index in range(cassette_count):
                cassette = cassette_rows[cassette_index]
                used_names = {
                    _text(value)
                    for value in cassette.get("used_front_connector_names", [])
                    if _text(value)
                }
                existing_mode = _text(cassette.get("termination_mode")).lower()
                existing_rear = _text(cassette.get("rear_connector")).lower()
                cassette_used = bool(used_names)
                compatible = (
                    not cassette_used
                    or (
                        existing_mode == termination_mode
                        and (
                            termination_mode != "connectorised"
                            or existing_rear == rear_connector
                        )
                    )
                )
                if not compatible:
                    continue
                cassette_names = names[
                    cassette_index * cassette_capacity:
                    (cassette_index + 1) * cassette_capacity
                ]
                available = [name for name in cassette_names if name not in used_names]
                if not available:
                    continue
                take = min(remaining, len(available))
                selected = available[:take]
                used_names.update(selected)
                chunk_fibres = min(remaining_fibres, take * fibres_per_position)
                cassette["position"] = cassette_index + 1
                cassette["front_connector"] = front_key
                cassette["front_connector_capacity"] = cassette_capacity
                cassette["termination_mode"] = termination_mode
                cassette["rear_connector"] = rear_connector if termination_mode == "connectorised" else "splice"
                cassette["used_front_connector_names"] = sorted(used_names, key=lambda value: names.index(value) if value in names else capacity)
                cassette["used_front_connectors"] = len(used_names)
                cassette["used_fibres"] = max(0, _int(cassette.get("used_fibres"))) + chunk_fibres
                cassette["rear_connector_count"] = (
                    max(1, min(4, int(math.ceil(cassette["used_fibres"] / float(rear_fibres)))))
                    if termination_mode == "connectorised"
                    else 0
                )
                cable_ids = [_text(value) for value in cassette.get("cable_ids", []) if _text(value)]
                if logical_id not in cable_ids:
                    cable_ids.append(logical_id)
                cassette["cable_ids"] = cable_ids
                associated_ids = [_text(value) for value in cassette.get("associated_instance_ids", []) if _text(value)]
                instance_id = _text(instance.get("id"))
                if instance_id and instance_id not in associated_ids:
                    associated_ids.append(instance_id)
                cassette["associated_instance_ids"] = associated_ids

                allocations.append(
                    {
                        "panel": panel,
                        "panel_id": _text(panel.get("id")),
                        "cassette_position": cassette_index + 1,
                        "ports": selected,
                        "termination_mode": termination_mode,
                        "rear_connector": cassette["rear_connector"],
                        "rear_connector_count": cassette["rear_connector_count"],
                        "fibre_count": chunk_fibres,
                    }
                )
                remaining -= take
                remaining_fibres = max(0, remaining_fibres - chunk_fibres)
                progress = True
                if remaining <= 0:
                    break
            panel["termination_count"] = sum(
                max(0, _int(row.get("used_front_connectors")))
                for row in panel.get("fibre_cassettes", [])
                if isinstance(row, dict)
            )
            panel["available_connector_count"] = max(0, capacity - _int(panel.get("termination_count")))
            associated = [_text(value) for value in panel.get("associated_instance_ids", []) if _text(value)]
            instance_id = _text(instance.get("id"))
            if instance_id and instance_id not in associated:
                associated.append(instance_id)
            panel["associated_instance_ids"] = associated
            if remaining <= 0:
                break
            panel_cursor += 1
            if not progress and panel_cursor >= len(panels):
                add_panel(key)
        return allocations

    def flatten_ports(allocations: Sequence[dict]) -> List[str]:
        return [
            _text(port)
            for allocation in allocations
            for port in allocation.get("ports", [])
            if _text(port)
        ]

    def allocation_summary(allocations: Sequence[dict]) -> List[dict]:
        return [
            {
                "panel_instance_id": _text(row.get("panel_id")),
                "cassette_position": _int(row.get("cassette_position")),
                "ports": list(row.get("ports", [])),
                "termination_mode": _text(row.get("termination_mode")),
                "rear_connector": _text(row.get("rear_connector")),
                "rear_connector_count": _int(row.get("rear_connector_count")),
                "fibre_count": _int(row.get("fibre_count")),
            }
            for row in allocations
        ]

    for logical, a, b in eligible:
        parent_id = _text(logical.get("id"))
        plan = connection_plan[parent_id]
        used_cores = plan["used_cores"]
        cable_core_count = plan["cable_core_count"]
        adapter_count = plan["connector_positions"]
        active_positions = plan["active_positions"]
        termination_mode = plan["termination_mode"]
        rear_connector = plan["rear_connector"]

        a_termination = a
        a_port = _text(logical.get("from_port"))
        a_allocations: List[dict] = []
        if rack_key(a) is not None:
            a_allocations = allocate(
                a,
                adapter_count,
                logical_id=parent_id,
                cable_core_count=cable_core_count,
                termination_mode=termination_mode,
                rear_connector=rear_connector,
            )
            a_panel_ports = flatten_ports(a_allocations)
            a_termination = a_allocations[0]["panel"]
            a_port = a_panel_ports[0]
            builder.add_connection(
                a,
                _text(logical.get("from_port")),
                a_termination,
                a_port,
                "fibre",
                generate_patch_leads=False,
                topology_hidden=True,
                physical_connection=True,
                parent_logical_connection_id=parent_id,
                physical_segment="fibre_patch_cable",
                cable_specification=(
                    "OS2 MPO/MTP equipment breakout lead"
                    if plan["connectorised_breakout"]
                    else "OS2 fibre patch lead"
                ),
                fibre_count=used_cores,
                terminated_panel_ports=list(a_panel_ports[:active_positions]),
                field_terminated_panel_ports=list(a_panel_ports),
                panel_termination_allocations=allocation_summary(a_allocations),
            )

        b_termination = b
        b_port = _text(logical.get("to_port"))
        b_allocations: List[dict] = []
        if rack_key(b) is not None:
            b_allocations = allocate(
                b,
                adapter_count,
                logical_id=parent_id,
                cable_core_count=cable_core_count,
                termination_mode=termination_mode,
                rear_connector=rear_connector,
            )
            b_panel_ports = flatten_ports(b_allocations)
            b_termination = b_allocations[0]["panel"]
            b_port = b_panel_ports[0]
            builder.add_connection(
                b_termination,
                b_port,
                b,
                _text(logical.get("to_port")),
                "fibre",
                generate_patch_leads=False,
                topology_hidden=True,
                physical_connection=True,
                parent_logical_connection_id=parent_id,
                physical_segment="fibre_patch_cable",
                cable_specification=(
                    "OS2 MPO/MTP equipment breakout lead"
                    if plan["connectorised_breakout"]
                    else "OS2 fibre patch lead"
                ),
                fibre_count=used_cores,
                terminated_panel_ports=list(b_panel_ports[:active_positions]),
                field_terminated_panel_ports=list(b_panel_ports),
                panel_termination_allocations=allocation_summary(b_allocations),
            )

        backbone = builder.add_connection(
            a_termination,
            a_port,
            b_termination,
            b_port,
            "fibre",
            _text(a.get("route_anchor")) or _text(a.get("location_name")),
            _text(b.get("route_anchor")) or _text(b.get("location_name")),
            generate_patch_leads=False,
            topology_hidden=True,
            physical_connection=True,
            parent_logical_connection_id=parent_id,
            physical_segment="fibre_backbone",
            cable_specification=(
                f"OS2 {rear_connector.upper()} trunk with modular cassette breakout"
                if plan["connectorised_breakout"]
                else "OS2 fixed installation fibre"
            ),
            fibre_count=used_cores,
            cable_core_count=cable_core_count,
            from_termination_method=termination_mode,
            to_termination_method=termination_mode,
            from_connector_type=rear_connector if termination_mode == "connectorised" else front_port_type,
            to_connector_type=rear_connector if termination_mode == "connectorised" else front_port_type,
            connectorised_breakout=bool(plan["connectorised_breakout"]),
            from_panel_termination_allocations=allocation_summary(a_allocations),
            to_panel_termination_allocations=allocation_summary(b_allocations),
            from_terminated_panel_ports=flatten_ports(a_allocations),
            to_terminated_panel_ports=flatten_ports(b_allocations),
            cabinet_to_field=(rack_key(a) is None or rack_key(b) is None),
        )
        backbone["route_path"] = list(logical.get("route_path", []))
        backbone["length_m"] = _float(logical.get("length_m"))
        backbone["redundancy_role"] = _text(logical.get("redundancy_role"))
        backbone["protection_group"] = _text(logical.get("protection_group"))


def _copper_patch_panel_asset(builder: DesignBuilder) -> dict:
    candidates = _candidate_assets(
        builder.data,
        "patch_panel",
        lambda asset: _text(asset.get("patch_panel_type")).lower() == "copper"
        and _int(asset.get("number_of_ports")) > 0,
    )
    candidates = _apply_manufacturer_preference(builder.data, candidates, "copper_patch_panel", "copper patch panel")
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
    """Terminate traditional horizontal copper cabling on rack patch panels.

    Endpoint assignments remain logically associated with their serving access
    switch so capacity, PoE and traffic calculations are unchanged.  Physical
    fields on each assignment identify the permanent horizontal-cable
    termination, while a hidden switch-to-panel connection and one patch-lead
    record model the short rack patch cord.
    """
    if _text(builder.technology).lower() != "traditional":
        return

    assignments_by_instance: Dict[str, List[dict]] = defaultdict(list)
    for assignment in builder.assignments:
        instance_id = _text(assignment.get("network_instance_id"))
        if instance_id:
            assignments_by_instance[instance_id].append(assignment)
    if not assignments_by_instance:
        return

    # add_assignment() creates a direct endpoint patch-lead for backwards
    # compatibility.  Traditional designs replace those records with the
    # correct switch-to-patch-panel cord once panel positions are known.
    panelised_assignment_ids = {
        _text(assignment.get("id"))
        for rows in assignments_by_instance.values()
        for assignment in rows
        if _text(assignment.get("id"))
    }
    builder.patch_leads = [
        lead for lead in builder.patch_leads
        if not (
            bool(lead.get("auto_generated"))
            and _text(lead.get("assignment_id")) in panelised_assignment_ids
        )
    ]

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
            panel_port = f"Port-{port_index}"
            switch_port = _text(assignment.get("network_port"))
            assignment_id = _text(assignment.get("id"))

            # Keep the logical endpoint-to-switch association intact, but
            # explicitly record where the field cable terminates physically.
            assignment["physical_patch_panel_instance_id"] = _text(panel.get("id"))
            assignment["physical_patch_panel_port"] = panel_port
            assignment["horizontal_cable_from_instance_id"] = _text(panel.get("id"))
            assignment["horizontal_cable_from_port"] = panel_port
            assignment["horizontal_cable_to_endpoint"] = _text(assignment.get("endpoint_name"))
            assignment["horizontal_cable_to_port"] = _text(assignment.get("endpoint_port"))
            assignment["physical_termination_type"] = "copper_patch_panel"

            patch_connection = builder.add_connection(
                switch,
                switch_port,
                panel,
                panel_port,
                "copper",
                generate_patch_leads=False,
                topology_hidden=True,
                physical_connection=True,
                physical_segment="copper_patch_cable",
                assignment_id=assignment_id,
                cable_specification="Category 6A copper patch lead",
                length_m=2.0,
            )
            builder.add_patch_lead(
                connection_id=_text(patch_connection.get("id")),
                assignment_id=assignment_id,
                instance=switch,
                port=switch_port,
                medium="copper",
                peer_instance_id=_text(panel.get("id")),
                peer_port=panel_port,
                endpoint_name=_text(assignment.get("endpoint_name")),
                preferred_use="client",
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
        "rack_units": max(0, int(rack_units)),
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
    candidates = _apply_manufacturer_preference(builder.data, candidates, "rack_ups", "rack UPS")
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


def _pdu_asset(builder: DesignBuilder) -> dict:
    candidates = [
        asset
        for asset in builder.data.get("network_assets", [])
        if isinstance(asset, dict)
        and _asset_type(asset) == "pdu"
        and (
            _text(asset.get("rack_mount_style")).lower() == "vertical_side"
            or _int(asset.get("rack_units"), 0) == 0
        )
    ]
    candidates = _apply_manufacturer_preference(
        builder.data, candidates, "rack_pdu", "rack PDU"
    )
    if candidates:
        return max(
            candidates,
            key=lambda asset: (
                max(0, _int(asset.get("power_outlet_count"), 0)),
                max(0.0, _float(asset.get("power_capacity_w"), 0.0)),
                _text(asset.get("id")),
            ),
        )
    builder.warnings.append(
        "No vertical rack PDU asset was available; a 42-outlet side-mounted PDU was generated automatically."
    )
    asset = _support_rack_asset(
        builder,
        asset_id="AUTO-RACK-PDU-42",
        name="42-outlet vertical rack PDU",
        asset_type="pdu",
        rack_units=0,
        notes="Automatically generated vertical side-mounted rack power distribution unit.",
    )
    asset.update(
        {
            "rack_mount_style": "vertical_side",
            "power_outlet_count": max(
                1, _int(builder.settings.get("default_pdu_outlet_count"), 42)
            ),
            "power_capacity_w": max(
                0.0, _float(builder.settings.get("default_pdu_capacity_w"), 7360.0)
            ),
            "power_feed_count": 1,
        }
    )
    return asset


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
    candidates = _apply_manufacturer_preference(builder.data, candidates, "cable_management", "cable-management panel")
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
    pdu_asset = _pdu_asset(builder)
    manager_asset = _cable_manager_asset(builder)
    assets[_text(ups_asset.get("id"))] = ups_asset
    assets[_text(pdu_asset.get("id"))] = pdu_asset
    assets[_text(manager_asset.get("id"))] = manager_asset
    ups_u = max(1, _int(ups_asset.get("rack_units"), 2))
    manager_u = max(1, _int(manager_asset.get("rack_units"), 1))
    ups_reserved_u = ups_u * 2 + 1

    groups: Dict[Tuple[int, str], List[dict]] = defaultdict(list)
    for instance in list(builder.instances):
        role = _text(instance.get("design_role"))
        if role in {"rack_ups", "rack_pdu", "cable_management"}:
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
            if preferred_name:
                name = preferred_name
                if name in rack_by_name:
                    return rack_by_name[name]
            else:
                # Previously this used ``len(racks) + 1`` directly in the
                # generated name.  A retained/preferred rack could already
                # have that name (for example AUTO-RACK-MER-2-2 while it was
                # the first rack reconstructed).  The fallback then returned
                # the full existing rack instead of creating another rack,
                # leaving room for the device but not its top-mounted fibre
                # panel.  Always choose an unused automatic rack name.
                suffix = 1
                while True:
                    name = (
                        f"AUTO-RACK-{location_name}"
                        if suffix == 1
                        else f"AUTO-RACK-{location_name}-{suffix}"
                    )
                    if name not in rack_by_name:
                        break
                    suffix += 1
            index = len(racks) + 1
            rack = {
                "index": index,
                "name": name,
                "bottom_next": 1,
                "top_next": rack_size_u,
                "ups_added": False,
                "ups_instances": [],
                "pdu_instances": {"left": [], "right": []},
                "powered_items": [],
            }
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
                ups_instance = builder.add_instance(
                    ups_asset,
                    f"AUTO {location_name} {rack['name']} UPS {number}",
                    location,
                    "rack_ups",
                    rack_name=rack["name"], rack_start_u=start_u,
                    rack_size_u=rack_size_u, route_anchor=location_name,
                    ups_pair_number=number,
                    ups_backed_source=True,
                )
                rack["ups_instances"].append(ups_instance)
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
            rack = create_rack()
            prospective = rack["bottom_next"] + (ups_reserved_u if powered else 0)
            if prospective + units - 1 > rack["top_next"]:
                raise NetworkPlanningError(
                    f"A {rack_size_u}U rack at {location_name} cannot accommodate "
                    f"the requested {units}U equipment and termination allowance."
                )
            return rack

        def place_bottom(instance: dict, rack: dict, units: int, powered: bool = False) -> None:
            if powered:
                ensure_ups(rack)
            if rack["bottom_next"] + units - 1 > rack["top_next"]:
                raise NetworkPlanningError(f"Rack {rack['name']} at {location_name} has insufficient free rack units.")
            instance["rack_name"] = rack["name"]
            instance["rack_start_u"] = rack["bottom_next"]
            instance["rack_size_u"] = rack_size_u
            rack["bottom_next"] += units
            if powered:
                rack["powered_items"].append(instance)

        equipment_by_id = {_text(row.get("id")): row for row in equipment}
        fibre_by_owner: Dict[str, List[dict]] = defaultdict(list)
        unowned_fibre: List[dict] = []
        for panel in fibre_panels:
            associated = [
                _text(value) for value in panel.get("associated_instance_ids", [])
                if _text(value) in equipment_by_id
            ]
            if associated:
                # A panel was created for one pre-repack cabinet. Keep it with a
                # real terminating device after repack instead of recreating the
                # stale cabinet name as a new mostly-empty rack.
                owner_id = sorted(
                    associated,
                    key=lambda value: (
                        0 if _text(equipment_by_id[value].get("design_role")) in {"core_switch", "edge_router", "olt_primary", "olt_secondary"} else 1,
                        _text(equipment_by_id[value].get("name")),
                    ),
                )[0]
                fibre_by_owner[owner_id].append(panel)
                panel["rack_owner_instance_id"] = owner_id
            else:
                unowned_fibre.append(panel)

        fibre_manager_index = 0

        def place_fibre_panel(panel: dict, rack: dict) -> None:
            nonlocal fibre_manager_index
            panel_u = max(1, _int(panel.get("_calculated_rack_units"), 1))
            if rack["top_next"] - panel_u - manager_u + 1 < rack["bottom_next"]:
                raise NetworkPlanningError(
                    f"Rack {rack['name']} at {location_name} has insufficient top space for its fibre termination panel."
                )
            panel_start = rack["top_next"] - panel_u + 1
            manager_start = panel_start - manager_u
            panel["rack_name"] = rack["name"]
            panel["target_rack_name"] = rack["name"]
            panel["rack_start_u"] = panel_start
            panel["rack_size_u"] = rack_size_u
            fibre_manager_index += 1
            builder.add_instance(
                manager_asset,
                f"AUTO {location_name} Fibre Cable Manager {fibre_manager_index}",
                location,
                "cable_management",
                rack_name=rack["name"],
                rack_start_u=manager_start,
                rack_size_u=rack_size_u,
                route_anchor=location_name,
                associated_patch_panel_id=_text(panel.get("id")),
            )
            rack["top_next"] = manager_start - 1

        equipment.sort(
            key=lambda i: (
                _text(i.get("target_rack_name") or i.get("rack_name")),
                _text(i.get("design_role")),
                _text(i.get("name")),
            )
        )
        for instance in equipment:
            units = max(1, _int(instance.get("_calculated_rack_units"), 1))
            powered = bool(instance.get("_powered_rack_item"))
            preferred = _text(instance.get("target_rack_name") or instance.get("rack_name"))
            copper_rows = copper_by_switch.get(_text(instance.get("id")), [])
            fibre_rows = fibre_by_owner.get(_text(instance.get("id")), [])
            copper_extra = sum(
                max(1, _int(panel.get("_calculated_rack_units"), 1)) + manager_u
                for panel in copper_rows
            )
            fibre_extra = sum(
                max(1, _int(panel.get("_calculated_rack_units"), 1)) + manager_u
                for panel in fibre_rows
            )
            rack = choose_bottom(units + copper_extra + fibre_extra, powered, preferred)
            place_bottom(instance, rack, units, powered)
            # Bottom-to-top order is switch, cable manager, copper patch panel;
            # therefore the rack elevation reads panel / manager / switch.
            for panel_index, panel in enumerate(copper_rows, start=1):
                builder.add_instance(
                    manager_asset,
                    f"AUTO {location_name} Copper Cable Manager {instance.get('id')} {panel_index}",
                    location,
                    "cable_management",
                    rack_name=rack["name"],
                    rack_start_u=rack["bottom_next"],
                    rack_size_u=rack_size_u,
                    route_anchor=location_name,
                    associated_patch_panel_id=_text(panel.get("id")),
                    associated_switch_id=_text(instance.get("id")),
                )
                rack["bottom_next"] += manager_u
                panel_u = max(1, _int(panel.get("_calculated_rack_units"), 1))
                place_bottom(panel, rack, panel_u, False)
            for panel in fibre_rows:
                place_fibre_panel(panel, rack)

        # Any unassociated copper panels are still installed with a manager.
        associated_ids = {id(p) for rows in copper_by_switch.values() for p in rows}
        for panel in copper_panels:
            if id(panel) in associated_ids:
                continue
            panel_u = max(1, _int(panel.get("_calculated_rack_units"), 1))
            rack = choose_bottom(panel_u + manager_u, False)
            builder.add_instance(
                manager_asset,
                f"AUTO {location_name} Copper Cable Manager {_text(panel.get('id'))}",
                location,
                "cable_management",
                rack_name=rack["name"],
                rack_start_u=rack["bottom_next"],
                rack_size_u=rack_size_u,
                route_anchor=location_name,
                associated_patch_panel_id=_text(panel.get("id")),
            )
            rack["bottom_next"] += manager_u
            place_bottom(panel, rack, panel_u, False)

        # Unowned fibre panels use an existing rack with genuine spare space.
        # A stale pre-repack target name is never allowed to create a new rack.
        for panel in sorted(unowned_fibre, key=lambda row: _text(row.get("name"))):
            panel_u = max(1, _int(panel.get("_calculated_rack_units"), 1))
            rack = next(
                (
                    row for row in racks
                    if row["top_next"] - panel_u - manager_u + 1 >= row["bottom_next"]
                ),
                None,
            )
            if rack is None:
                rack = create_rack()
            place_fibre_panel(panel, rack)

        def pdu_load(pdu: dict) -> float:
            return sum(
                max(0.0, _float(row.get("load_w")))
                for row in builder.power_connections
                if _text(row.get("from_instance_id")) == _text(pdu.get("id"))
            )

        def pdu_outlets_used(pdu: dict) -> int:
            return sum(
                1
                for row in builder.power_connections
                if _text(row.get("from_instance_id")) == _text(pdu.get("id"))
            )

        def ensure_pdu(rack: dict, side: str, required_load_w: float) -> dict:
            side = "right" if side == "right" else "left"
            outlet_capacity = max(1, _int(pdu_asset.get("power_outlet_count"), 42))
            watt_capacity = max(0.0, _float(pdu_asset.get("power_capacity_w"), 7360.0))
            for pdu in rack["pdu_instances"][side]:
                outlets_ok = pdu_outlets_used(pdu) < outlet_capacity
                watts_ok = watt_capacity <= 0.0 or pdu_load(pdu) + required_load_w <= watt_capacity + 1e-9
                if outlets_ok and watts_ok:
                    return pdu
            ensure_ups(rack)
            position = len(rack["pdu_instances"][side]) + 1
            pdu = builder.add_instance(
                pdu_asset,
                f"AUTO {location_name} {rack['name']} {side.title()} PDU {position}",
                location,
                "rack_pdu",
                rack_name=rack["name"],
                rack_start_u=0,
                rack_size_u=rack_size_u,
                route_anchor=location_name,
                rack_mount_style="vertical_side",
                rack_side=side,
                side_mount_position=position,
                power_feed_count=1,
            )
            rack["pdu_instances"][side].append(pdu)
            ups = rack["ups_instances"][(position - 1) % len(rack["ups_instances"])]
            pdu["upstream_power_source_id"] = _text(ups.get("id"))
            builder.add_power_connection(
                ups,
                f"Output-{position}",
                pdu,
                "Input-1",
                feed_label=f"{side.title()} PDU supply",
                capacity_w=watt_capacity,
                notes="Automatically generated UPS-backed rack PDU supply.",
            )
            return pdu

        critical_types = {"network_switch", "network_router", "firewall", "optical_line_terminal"}
        critical_roles = {"core_switch", "aggregation_switch", "access_switch", "olt_primary", "olt_secondary", "edge_router"}
        dual_critical = bool(builder.settings.get("auto_dual_power_critical_devices", True))
        for rack in racks:
            for item_index, instance in enumerate(rack["powered_items"]):
                asset = assets.get(_text(instance.get("asset_id")), {})
                load_w = max(0.0, _float(asset.get("power_input_w")))
                feeds = max(1, _int(asset.get("power_feed_count"), 1))
                if bool(asset.get("redundant_power_supplies")):
                    feeds = max(feeds, 2)
                if dual_critical and (
                    _asset_type(asset) in critical_types
                    or _text(instance.get("design_role")) in critical_roles
                ):
                    feeds = max(feeds, 2)
                feeds = min(2, feeds)
                sides = ["left", "right"] if feeds > 1 else (["left"] if item_index % 2 == 0 else ["right"])
                assigned_pdus: List[str] = []
                for feed_index, side in enumerate(sides, start=1):
                    pdu = ensure_pdu(rack, side, load_w)
                    outlet_number = pdu_outlets_used(pdu) + 1
                    feed_name = chr(ord("A") + feed_index - 1)
                    builder.add_power_connection(
                        pdu,
                        f"Outlet-{outlet_number}",
                        instance,
                        f"PSU-{feed_name}",
                        feed_label=f"Feed {feed_name}",
                        load_w=load_w,
                        capacity_w=max(0.0, _float(pdu_asset.get("power_capacity_w"))),
                    )
                    assigned_pdus.append(_text(pdu.get("id")))
                instance["power_feed_count"] = len(assigned_pdus)
                instance["power_pdu_instance_ids"] = assigned_pdus
                instance["power_feed"] = "/".join(
                    _text(next((p.get("rack_side") for side in rack["pdu_instances"].values() for p in side if _text(p.get("id")) == pdu_id), "")).upper()
                    for pdu_id in assigned_pdus
                )
                instance["ups_source"] = "/".join(
                    _text(pdu.get("upstream_power_source_id"))
                    for side in rack["pdu_instances"].values()
                    for pdu in side
                    if _text(pdu.get("id")) in assigned_pdus
                )

        for instance in items:
            instance.pop("_calculated_rack_units", None)
            instance.pop("_powered_rack_item", None)



_AUTO_FIBRE_PORT_TYPES = set(PLUGGABLE_OPTIC_PORT_TYPES) | {"lc", "sc", "mpo", "pon"}


def _auto_connect_layer(instance: dict, asset: dict) -> str:
    """Infer the logical layer of an installed element for guided auto-connect."""

    declared = _text(
        instance.get("network_layer")
        or instance.get("design_layer")
        or asset.get("network_layer")
        or asset.get("design_layer")
    ).lower()
    aliases = {
        "distribution": "aggregation",
        "distribution_switch": "aggregation",
        "aggregation_switch": "aggregation",
        "access_switch": "access",
        "core_switch": "core",
        "edge_router": "edge",
        "router": "edge",
        "firewall": "edge",
        "wireless": "endpoint",
        "client": "endpoint",
    }
    if declared:
        return aliases.get(declared, declared)

    role = _text(instance.get("design_role")).lower()
    for token, layer in (
        ("core", "core"),
        ("aggregation", "aggregation"),
        ("distribution", "aggregation"),
        ("access", "access"),
        ("router", "edge"),
        ("firewall", "edge"),
        ("olt", "olt"),
        ("splitter", "splitter"),
        ("ont", "ont"),
    ):
        if token in role:
            return layer

    asset_type = _asset_type(asset)
    if asset_type in {"network_router", "firewall", "telco_pop", "external_network"}:
        return "edge"
    if asset_type == "optical_line_terminal":
        return "olt"
    if asset_type == "fibre_splitter":
        return "splitter"
    if asset_type == "optical_network_terminal":
        return "ont"
    if asset_type == "network_switch":
        name = (_text(asset.get("name")) + " " + _text(asset.get("model"))).lower()
        if "core" in name:
            return "core"
        if "aggregation" in name or "distribution" in name:
            return "aggregation"
        return "access"
    if asset_type in {"wireless_access_point"}:
        return "endpoint"
    return ""


def _auto_connect_expanded_ports(instance: dict, asset: dict) -> List[dict]:
    """Expand structured port rows into the physical names used by rack/device views."""

    rows = [
        row
        for row in asset.get("port_definitions", [])
        if isinstance(row, dict) and _int(row.get("port_count")) > 0
    ]
    if not rows and _int(asset.get("number_of_ports")) > 0:
        port_type = (
            "pon"
            if _asset_type(asset) in {"optical_line_terminal", "optical_network_terminal"}
            else "lc"
            if _asset_type(asset) == "fibre_splitter"
            or _text(asset.get("patch_panel_type")).lower() == "fibre"
            else "rj45"
        )
        rows = [
            {
                "port_type": port_type,
                "port_count": _int(asset.get("number_of_ports")),
                "port_use": "client",
                "name_prefix": "",
                "explicit_names": [],
            }
        ]

    base_ports: List[dict] = []
    counters: Dict[str, int] = defaultdict(int)
    for row in rows:
        port_type = _text(row.get("port_type")).lower() or "other"
        port_use = _text(row.get("port_use")).lower() or "other"
        count = max(0, _int(row.get("port_count")))
        explicit = (
            [_text(value) for value in row.get("explicit_names", []) if _text(value)]
            if isinstance(row.get("explicit_names", []), list)
            else []
        )
        for name in explicit[:count]:
            base_ports.append(
                {
                    "name": name,
                    "port_type": port_type,
                    "port_use": port_use,
                    "medium": "fibre" if port_type in _AUTO_FIBRE_PORT_TYPES else "copper",
                    "supported_speeds_mbps": normalise_port_speeds(row.get("supported_speeds_mbps")) or default_port_speeds(port_type),
                }
            )
        remaining = count - min(count, len(explicit))
        prefix = _text(row.get("name_prefix")) or {
            "pon": "PON",
            "sfp": "SFP",
            "sfp+": "SFP+",
            "qsfp": "QSFP",
            "qsfp28": "QSFP28",
            "lc": "LC",
            "sc": "SC",
            "mpo": "MPO",
            "rj45": "",
        }.get(port_type, port_type.upper())
        for _ in range(remaining):
            counters[prefix] += 1
            name = (
                prefix
                if remaining == 1
                and counters[prefix] == 1
                and prefix.lower().startswith("input-")
                else f"{prefix}-{counters[prefix]}"
                if prefix
                else str(counters[prefix])
            )
            base_ports.append(
                {
                    "name": name,
                    "port_type": port_type,
                    "port_use": port_use,
                    "medium": "fibre" if port_type in _AUTO_FIBRE_PORT_TYPES else "copper",
                    "supported_speeds_mbps": normalise_port_speeds(row.get("supported_speeds_mbps")) or default_port_speeds(port_type),
                }
            )

    members = (
        max(1, _int(instance.get("stack_member_count"), 1))
        if bool(instance.get("logical_stack"))
        else 1
    )
    if members <= 1:
        return base_ports
    return [
        {**port, "name": f"{member}/{port['name']}"}
        for member in range(1, members + 1)
        for port in base_ports
    ]


def auto_connect_manual_devices(
    data: dict,
    instance_ids: Optional[Sequence[str]] = None,
) -> dict:
    """Connect manually installed devices to the nearest valid upstream layer.

    The configured collapsed-core/three-tier rules determine link count and
    source diversity for access and aggregation devices. Other supported
    element types use deterministic parent relationships (router/core,
    core/OLT, OLT/splitter, splitter/ONT and access/endpoint). Existing logical
    links are retained and count towards the required links. Only free,
    compatible physical ports are allocated.
    """

    ensure_network_schema(data)
    assets = {
        _text(row.get("id")): row
        for row in data.get("network_assets", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    instances = {
        _text(row.get("id")): row
        for row in data.get("network_asset_instances", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    requested_ids = {
        _text(value) for value in (instance_ids or []) if _text(value)
    }
    if requested_ids:
        selected = [
            instances[value]
            for value in requested_ids
            if value in instances and not bool(instances[value].get("auto_generated"))
        ]
    else:
        selected = [
            row for row in instances.values() if not bool(row.get("auto_generated"))
        ]

    result = {
        "created_connection_ids": [],
        "connected_instance_ids": [],
        "skipped_instance_ids": [],
        "warnings": [],
    }
    if not selected:
        return result

    settings = data.setdefault("network_settings", {})
    rules = normalise_layer_connection_rules(
        settings.get("layer_connection_rules"),
        _text(settings.get("topology_model")) or "collapsed_core",
        bool(settings.get("redundant_core", True)),
        _int(settings.get("independent_link_count"), 2),
    )
    graph = RoutingGraph(data)
    layers = {
        instance_id: _auto_connect_layer(instance, assets.get(_text(instance.get("asset_id")), {}))
        for instance_id, instance in instances.items()
    }

    occupied: Dict[str, set[str]] = defaultdict(set)
    logical_connections: List[dict] = []
    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict):
            continue
        source_id = _text(connection.get("from_instance_id"))
        target_id = _text(connection.get("to_instance_id"))
        source_port = _text(connection.get("from_port"))
        target_port = _text(connection.get("to_port"))
        if source_id and source_port:
            occupied[source_id].add(source_port)
        if target_id and target_port:
            occupied[target_id].add(target_port)
        if not bool(connection.get("topology_hidden")) and not bool(connection.get("physical_connection")):
            logical_connections.append(connection)
    for assignment in data.get("network_endpoint_assignments", []):
        if isinstance(assignment, dict):
            instance_id = _text(assignment.get("network_instance_id"))
            port = _text(assignment.get("network_port"))
            if instance_id and port:
                occupied[instance_id].add(port)
    for lead in data.get("network_patch_leads", []):
        if isinstance(lead, dict):
            instance_id = _text(lead.get("instance_id"))
            port = _text(lead.get("port"))
            if instance_id and port:
                occupied[instance_id].add(port)

    expanded_ports = {
        instance_id: _auto_connect_expanded_ports(
            instance, assets.get(_text(instance.get("asset_id")), {})
        )
        for instance_id, instance in instances.items()
    }

    def anchor(instance: dict) -> str:
        return _text(instance.get("route_anchor")) or _text(instance.get("location_name"))

    def rule_for_target(target_layer: str) -> Optional[dict]:
        matches = [
            row
            for row in rules
            if bool(row.get("enabled", True))
            and _text(row.get("target_layer")).lower() == target_layer
            and _text(row.get("source_layer")).lower() != target_layer
        ]
        return matches[0] if matches else None

    def connection_plan(device_id: str) -> Optional[dict]:
        layer = layers.get(device_id, "")
        if layer in {"access", "aggregation"}:
            rule = rule_for_target(layer)
            if rule:
                return {
                    "source_layer": _text(rule.get("source_layer")).lower(),
                    "target_layer": layer,
                    "links": max(1, _int(rule.get("links_per_target"), 1)),
                    "distinct": max(1, _int(rule.get("minimum_distinct_sources"), 1)),
                    "direction": "candidate_to_device",
                }
        if layer == "core":
            if any(value == "edge" for value in layers.values()):
                return {"source_layer": "edge", "target_layer": "core", "links": 1, "distinct": 1, "direction": "candidate_to_device"}
            peer = next(
                (
                    row
                    for row in rules
                    if bool(row.get("enabled", True))
                    and _text(row.get("source_layer")).lower() == "core"
                    and _text(row.get("target_layer")).lower() == "core"
                ),
                None,
            )
            if peer:
                return {"source_layer": "core", "target_layer": "core", "links": max(1, _int(peer.get("links_per_target"), 1)), "distinct": 1, "direction": "candidate_to_device"}
        if layer == "edge":
            links = max(1, _int(settings.get("independent_link_count"), 2 if settings.get("redundant_core") else 1))
            if not bool(settings.get("redundant_core", True)):
                links = 1
            return {"source_layer": "core", "target_layer": "edge", "links": links, "distinct": min(links, 2), "direction": "device_to_candidate"}
        if layer == "olt":
            source_layer = "edge" if any(value == "edge" for value in layers.values()) else "core"
            return {"source_layer": source_layer, "target_layer": "olt", "links": 1, "distinct": 1, "direction": "candidate_to_device"}
        if layer == "splitter":
            asset = assets.get(_text(instances[device_id].get("asset_id")), {})
            links = max(1, _int(asset.get("connections_in"), 1))
            return {"source_layer": "olt", "target_layer": "splitter", "links": links, "distinct": min(links, 2), "direction": "candidate_to_device"}
        if layer == "ont":
            return {"source_layer": "splitter", "target_layer": "ont", "links": 1, "distinct": 1, "direction": "candidate_to_device"}
        if layer == "endpoint":
            return {"source_layer": "access", "target_layer": "endpoint", "links": 1, "distinct": 1, "direction": "candidate_to_device"}
        return None

    def port_preferences(source_layer: str, target_layer: str) -> Tuple[List[str], List[str], List[str]]:
        if source_layer == "olt" and target_layer == "splitter":
            return ["pon", "output", "downlink"], ["input", "uplink"], ["fibre"]
        if source_layer == "splitter" and target_layer == "ont":
            return ["output", "downlink"], ["uplink", "input", "pon"], ["fibre"]
        if target_layer == "endpoint":
            return ["client", "downlink", "output"], ["uplink", "client", "input"], ["copper", "fibre"]
        return ["downlink", "output", "uplink"], ["uplink", "input", "downlink"], ["fibre", "copper"]

    def free_ports(instance_id: str, preferred_uses: Sequence[str], preferred_media: Sequence[str]) -> List[dict]:
        use_rank = {value: index for index, value in enumerate(preferred_uses)}
        medium_rank = {value: index for index, value in enumerate(preferred_media)}
        rows = [row for row in expanded_ports.get(instance_id, []) if _text(row.get("name")) not in occupied.get(instance_id, set())]
        return sorted(
            rows,
            key=lambda row: (
                use_rank.get(_text(row.get("port_use")).lower(), len(use_rank) + 1),
                medium_rank.get(_text(row.get("medium")).lower(), len(medium_rank) + 1),
                _text(row.get("name")).lower(),
            ),
        )

    def compatible_pair(source_id: str, target_id: str, source_layer: str, target_layer: str) -> Optional[Tuple[dict, dict, str, int]]:
        source_uses, target_uses, media = port_preferences(source_layer, target_layer)
        source_ports = free_ports(source_id, source_uses, media)
        target_ports = free_ports(target_id, target_uses, media)
        required = max(0.0, _float(instances.get(source_id, {}).get("expected_bandwidth_mbps")), _float(instances.get(target_id, {}).get("expected_bandwidth_mbps")))
        choices = []
        for medium in media:
            source_options = [row for row in source_ports if _text(row.get("medium")) == medium]
            target_options = [row for row in target_ports if _text(row.get("medium")) == medium]
            for source_port in source_options:
                for target_port in target_options:
                    source_speeds = normalise_port_speeds(source_port.get("supported_speeds_mbps"))
                    target_speeds = normalise_port_speeds(target_port.get("supported_speeds_mbps"))
                    common = compatible_port_speeds(source_speeds, target_speeds)
                    if source_speeds and target_speeds and not common:
                        continue
                    if common:
                        eligible = [value for value in common if value + 1e-9 >= required]
                        if not eligible:
                            continue
                        speed = min(eligible)
                    else:
                        speed = 0
                    choices.append((speed if speed else 10**12, _text(source_port.get("name")), _text(target_port.get("name")), source_port, target_port, medium, speed))
        if not choices:
            return None
        *_, source_port, target_port, medium, speed = min(choices, key=lambda row: row[:3])
        return source_port, target_port, medium, speed

    module_ids = {_text(row.get("id")) for row in data.get("network_optic_modules", []) if isinstance(row, dict) and _text(row.get("id"))}

    def add_manual_optic(connection: dict, side: str, instance_id: str, port: dict, speed: int) -> str:
        if _text(port.get("port_type")) not in PLUGGABLE_OPTIC_PORT_TYPES:
            return ""
        host_asset = assets.get(_text(instances.get(instance_id, {}).get("asset_id")), {})
        allowed = optic_form_factors_for_cage(_text(port.get("port_type")))
        candidates = [asset for asset in data.get("network_assets", []) if isinstance(asset, dict) and _asset_type(asset) == "optical_transceiver" and _text(asset.get("optic_form_factor")) in allowed and (not normalise_port_speeds(asset.get("supported_speeds_mbps")) or speed in normalise_port_speeds(asset.get("supported_speeds_mbps")))]
        if candidates:
            optic_asset = min(candidates, key=lambda asset: (_float(asset.get("optic_reach_m")) or 10**12, _text(asset.get("id"))))
        else:
            asset_id = f"AUTO-OPTIC-{_text(port.get('port_type')).upper().replace('+','P')}-{speed or 'UNSPEC'}"
            optic_asset = next((asset for asset in data.get("network_assets", []) if _text(asset.get("id")) == asset_id), None)
            if optic_asset is None:
                optic_asset = {"id": asset_id, "name": f"Auto {port_speed_label(speed) if speed else ''} {_text(port.get('port_type')).upper()} optical transceiver".replace("  ", " ").strip(), "asset_type": "optical_transceiver", "optic_form_factor": _text(port.get("port_type")), "supported_speeds_mbps": [speed] if speed else normalise_port_speeds(port.get("supported_speeds_mbps")), "optic_connector_type": "lc", "optic_fibre_standard": _text(host_asset.get("optical_standard")) or "OS2", "optic_reach_m": 0.0, "optical_tx_power_dbm": port.get("transmit_power_dbm", host_asset.get("optical_tx_power_dbm", "")), "optical_receiver_sensitivity_dbm": port.get("receiver_sensitivity_dbm", host_asset.get("optical_receiver_sensitivity_dbm", "")), "optical_insertion_loss_db": port.get("insertion_loss_db", ""), "optical_return_loss_db": port.get("return_loss_db", host_asset.get("optical_return_loss_db", "")), "optical_wavelength_nm": _int(port.get("wavelength_nm"), _int(host_asset.get("optical_wavelength_nm"))), "rack_units": 0, "number_of_ports": 0, "port_definitions": [], "auto_network_asset": True, "auto_optic_definition": True}
                data.setdefault("network_assets", []).append(optic_asset); assets[asset_id] = optic_asset
        module_id = _next_identifier(module_ids, "OM")
        data.setdefault("network_optic_modules", []).append({"id": module_id, "asset_id": _text(optic_asset.get("id")), "host_instance_id": instance_id, "host_port": _text(port.get("name")), "connection_id": _text(connection.get("id")), "side": side, "link_speed_mbps": speed, "auto_generated": False, "auto_connected": True, "notes": "Inserted by guided auto-connect."})
        return module_id

    connection_ids = {
        _text(row.get("id"))
        for row in data.get("network_connections", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }

    order = {"edge": 0, "core": 1, "aggregation": 2, "access": 3, "olt": 4, "splitter": 5, "ont": 6, "endpoint": 7}
    selected.sort(key=lambda row: (order.get(layers.get(_text(row.get("id")), ""), 99), _text(row.get("id"))))

    for device in selected:
        device_id = _text(device.get("id"))
        plan = connection_plan(device_id)
        if plan is None:
            result["skipped_instance_ids"].append(device_id)
            result["warnings"].append(f"{device_id}: unable to infer a supported network layer for auto-connect.")
            continue

        candidate_ids = [
            instance_id
            for instance_id, layer in layers.items()
            if instance_id != device_id and layer == plan["source_layer"]
        ]
        if not candidate_ids:
            result["skipped_instance_ids"].append(device_id)
            result["warnings"].append(
                f"{device_id}: no {plan['source_layer']} device is available for auto-connect."
            )
            continue

        existing_candidates: List[str] = []
        for connection in logical_connections:
            source_id = _text(connection.get("from_instance_id"))
            target_id = _text(connection.get("to_instance_id"))
            if device_id not in {source_id, target_id}:
                continue
            peer_id = target_id if source_id == device_id else source_id
            if peer_id in candidate_ids:
                existing_candidates.append(peer_id)
        links_needed = max(0, int(plan["links"]) - len(existing_candidates))
        if links_needed <= 0:
            result["skipped_instance_ids"].append(device_id)
            continue

        used_candidate_ids = set(existing_candidates)
        scored_candidates: List[Tuple[float, str, List[str]]] = []
        for candidate_id in candidate_ids:
            candidate = instances[candidate_id]
            if plan["direction"] == "candidate_to_device":
                source_id, target_id = candidate_id, device_id
            else:
                source_id, target_id = device_id, candidate_id
            pair = compatible_pair(source_id, target_id, layers.get(source_id, ""), layers.get(target_id, ""))
            if pair is None:
                continue
            distance, route_path = graph.route(anchor(instances[source_id]), anchor(instances[target_id]))
            scored_candidates.append((distance, candidate_id, route_path))
        scored_candidates.sort(key=lambda row: (row[0], row[1]))

        created_for_device = 0
        while links_needed > 0:
            candidates = [row for row in scored_candidates if row[1] not in used_candidate_ids]
            if not candidates and len(used_candidate_ids) >= int(plan["distinct"]):
                candidates = list(scored_candidates)
            if not candidates:
                break
            _distance_m, candidate_id, _route = candidates[0]
            if plan["direction"] == "candidate_to_device":
                source_id, target_id = candidate_id, device_id
            else:
                source_id, target_id = device_id, candidate_id
            pair = compatible_pair(source_id, target_id, layers.get(source_id, ""), layers.get(target_id, ""))
            if pair is None:
                scored_candidates = [row for row in scored_candidates if row[1] != candidate_id]
                continue
            source_port, target_port, medium, link_speed = pair
            length_m, route_path = graph.route(anchor(instances[source_id]), anchor(instances[target_id]))
            connection_id = _next_identifier(connection_ids, "NC")
            link_number = len(existing_candidates) + created_for_device + 1
            connection = {
                "id": connection_id,
                "from_instance_id": source_id,
                "from_port": _text(source_port.get("name")),
                "to_instance_id": target_id,
                "to_port": _text(target_port.get("name")),
                "connection_role": "uplink",
                "medium": medium,
                "link_speed_mbps": link_speed,
                "cable_specification": "OS2 single-mode fibre" if medium == "fibre" else "Category 6A",
                "fibre_count": 1 if plan["source_layer"] in {"olt", "splitter"} else 2 if medium == "fibre" else 0,
                "vlan_ids": [],
                "route_profile": "fibre_optic" if medium == "fibre" and "fibre_optic" in data.get("route_profiles", {}) else "",
                "route_path": route_path,
                "length_m": round(max(0.0, length_m), 3),
                "expected_bandwidth_mbps": round(max(0.0, _float(device.get("expected_bandwidth_mbps"), _float(assets.get(_text(device.get("asset_id")), {}).get("expected_bandwidth_mbps")))), 6),
                "expected_packet_rate_pps": round(max(0.0, _float(device.get("expected_packet_rate_pps"), _float(assets.get(_text(device.get("asset_id")), {}).get("expected_packet_rate_pps")))), 3),
                "source_layer": layers.get(source_id, ""),
                "target_layer": layers.get(target_id, ""),
                "independent_link_index": link_number,
                "protection_group": f"MANUAL-AUTO-{device_id}" if int(plan["links"]) > 1 else "",
                "redundancy_role": "primary" if link_number == 1 else "secondary" if link_number == 2 else f"path-{link_number}",
                "standby": bool(link_number > 1),
                "notes": "Automatically connected after manual device placement using the configured topology rules and routing graph.",
                "auto_generated": False,
                "auto_connected": True,
            }
            if medium == "fibre":
                connection["from_optic_module_id"] = add_manual_optic(connection, "from", source_id, source_port, link_speed)
                connection["to_optic_module_id"] = add_manual_optic(connection, "to", target_id, target_port, link_speed)
            data.setdefault("network_connections", []).append(connection)
            logical_connections.append(connection)
            occupied[source_id].add(connection["from_port"])
            occupied[target_id].add(connection["to_port"])
            result["created_connection_ids"].append(connection_id)
            created_for_device += 1
            links_needed -= 1
            used_candidate_ids.add(candidate_id)

        if created_for_device:
            result["connected_instance_ids"].append(device_id)
        else:
            result["skipped_instance_ids"].append(device_id)
            result["warnings"].append(
                f"{device_id}: compatible free ports were not available on the device and its candidate upstream equipment."
            )

    if result["created_connection_ids"]:
        ensure_physical_fibre_for_design(data, replace_auto=False)
    ensure_network_schema(data)
    return result

def generate_network_design(
    data: dict,
    technology: Optional[str] = None,
    progress_callback=None,
) -> dict:
    """Generate and install an optimised network design into ``data``.

    Objective order:
      1. Satisfy every physical endpoint port and its PoE load.
      2. Respect configured spare capacity and PoLAN ONT copper distance.
      3. Keep departments together where topology and distance permit.
      4. Minimise active/passive component count.
      5. Minimise excess capacity and local copper length.
    """

    def progress(value: int, message: str) -> None:
        if callable(progress_callback):
            progress_callback(max(0, min(100, int(value))), message)

    progress(2, "Validating network project data...")
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
    settings.setdefault("default_expected_bandwidth_mbps", 0.0)
    settings.setdefault("default_expected_packet_rate_pps", 0.0)
    settings.setdefault("topology_model", "collapsed_core")
    settings.setdefault("independent_link_count", 2)
    settings.setdefault("layer_connection_rules", [])
    settings.setdefault("polan_max_onts_per_splitter", 16)
    settings.setdefault("polan_max_splitter_ont_route_m", 120.0)
    settings.setdefault("auto_planner_max_workers", 0)
    settings.setdefault("auto_planner_parallel_threshold", 4)
    spare_fraction = (
        max(0.0, _float(settings.get("spare_capacity_percent"), 15.0)) / 100.0
    )

    progress(10, "Calculating endpoint, bandwidth and PoE demand...")
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

    progress(22, "Precomputing cable-routing paths...")
    precomputed_sources, route_workers_used = builder.graph.precompute_sources(
        route_sources,
        max_workers=max(0, _int(settings.get("auto_planner_max_workers"), 0)),
        parallel_threshold=max(
            1, _int(settings.get("auto_planner_parallel_threshold"), 4)
        ),
    )

    progress(42, f"Creating {technology_value} active network layers...")
    if technology_value == "Traditional":
        _traditional_design(builder, endpoints, spare_fraction)
    else:
        _polan_design(builder, endpoints, spare_fraction)

    progress(58, "Creating upstream, external and redundant links...")
    _install_external_network_connections(builder, spare_fraction)

    # Fibre terminations require physical patch-panel capacity and rack space.
    # Repack the complete generated equipment set afterwards so OLTs, cores and
    # panels share the location racks without ever exceeding the configured U.
    progress(68, "Installing patch panels, UPS equipment and rack PDUs...")
    _install_copper_patch_panels(builder, spare_fraction)
    _install_fibre_patch_panels(builder, spare_fraction)
    _repack_generated_racks(builder)

    progress(82, "Checking switch, rack, power and traffic capacity...")
    _assert_generated_capacity(builder, spare_fraction)
    builder.commit()
    progress(90, "Planning physical fibre and optical budgets...")
    physical_fibre_summary = ensure_physical_fibre_for_design(data, replace_auto=False)

    assets_by_id = {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    installed_ports = sum(
        len(_asset_endpoint_port_slots(assets_by_id.get(_text(item.get("asset_id")), {})))
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
    demand_bandwidth = sum(endpoint.expected_bandwidth_mbps for endpoint in endpoints)
    demand_packets = sum(endpoint.expected_packet_rate_pps for endpoint in endpoints)
    installed_bandwidth = sum(
        max(
            0.0,
            _float(
                assets_by_id.get(_text(item.get("asset_id")), {}).get(
                    "bandwidth_capacity_gbps"
                )
            ),
        )
        * 1000.0
        * (
            max(1, _int(item.get("stack_member_count"), 1))
            if bool(item.get("logical_stack"))
            else 1
        )
        for item in builder.instances
        if _text(item.get("design_role")) in {"access_switch", "ont"}
    )
    installed_packets = sum(
        max(
            0.0,
            _float(
                assets_by_id.get(_text(item.get("asset_id")), {}).get(
                    "packet_throughput_mpps"
                )
            ),
        )
        * 1_000_000.0
        * (
            max(1, _int(item.get("stack_member_count"), 1))
            if bool(item.get("logical_stack"))
            else 1
        )
        for item in builder.instances
        if _text(item.get("design_role")) in {"access_switch", "ont"}
    )
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
            else max(
                2 if bool(settings.get("redundant_core", True)) else 1,
                _int(settings.get("independent_link_count"), 2)
                if bool(settings.get("redundant_core", True)) else 1,
            )
        ),
        "layer_connection_rules": (
            deepcopy(settings.get("layer_connection_rules", []))
            if technology_value == "Traditional"
            else []
        ),
        "objective": "Minimum feasible component count after department-first, graph-branch-aware proximity clustering, subject to port, PoE, bandwidth, packet-throughput, spare-capacity, distance and configured layer-diversity constraints",
        "endpoint_locations": len(endpoints),
        "required_ports": demand_ports,
        "required_poe_w": round(demand_poe, 3),
        "required_bandwidth_mbps": round(demand_bandwidth, 6),
        "required_packet_rate_pps": round(demand_packets, 3),
        "installed_endpoint_ports": installed_ports,
        "installed_poe_budget_w": round(installed_poe, 3),
        "installed_bandwidth_capacity_mbps": round(installed_bandwidth, 6),
        "installed_packet_throughput_pps": round(installed_packets, 3),
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
        "physical_fibre_planning": deepcopy(settings.get("physical_fibre_planning", {})),
        "optical_path_count": physical_fibre_summary.get("optical_path_count", 0),
        "optical_path_failures": physical_fibre_summary.get("optical_path_failures", 0),
        "polan_max_ont_copper_m": _float(settings.get("polan_max_ont_copper_m"), 30.0),
        "polan_max_onts_per_splitter": _int(
            settings.get("polan_max_onts_per_splitter"), 16
        ),
        "polan_max_splitter_ont_route_m": _float(
            settings.get("polan_max_splitter_ont_route_m"), 120.0
        ),
        "olt_failover_enabled": (
            bool(settings.get("polan_olt_failover", True))
            if technology_value == "PoLAN"
            else False
        ),
        "warnings": builder.warnings,
    }
    summary["auto_generated_power_connections"] = len(builder.power_connections)
    data["network_design_summary"] = summary
    ensure_network_schema(data)
    progress(100, "Automatic network plan complete.")
    return summary
