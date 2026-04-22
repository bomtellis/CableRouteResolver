from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from heapq import heappop, heappush
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Point:
    name: str
    x: float
    y: float
    floor: int
    kind: str
    extension_distance_m: float = 0.0


def load_project(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_points(data: dict) -> Dict[str, Point]:
    points: Dict[str, Point] = {}

    for item in data.get("locations", []):
        name = str(item["name"])
        points[name] = Point(
            name=name,
            x=float(item["x"]),
            y=float(item["y"]),
            floor=int(item["floor"]),
            kind=str(item.get("kind", "location") or "location"),
            extension_distance_m=0.0,
        )

    for item in data.get("corridors", {}).get("nodes", []):
        name = str(item["name"])
        points[name] = Point(
            name=name,
            x=float(item["x"]),
            y=float(item["y"]),
            floor=int(item["floor"]),
            kind="corridor_node",
            extension_distance_m=0.0,
        )

    for item in data.get("data_points", []):
        name = str(item["name"])
        points[name] = Point(
            name=name,
            x=float(item["x"]),
            y=float(item["y"]),
            floor=int(item["floor"]),
            kind="data_point",
            extension_distance_m=float(item.get("extension_distance_m", 0.0) or 0.0),
        )

    for transition in data.get("transitions", []):
        transition_id = str(transition["id"])
        for floor_key, pos in (transition.get("floor_locations") or {}).items():
            floor = int(floor_key)
            name = f"{transition_id}-F{floor}"
            points[name] = Point(
                name=name,
                x=float(pos["x"]),
                y=float(pos["y"]),
                floor=floor,
                kind="transition_node",
                extension_distance_m=0.0,
            )

    return points


def point_distance(a: Point, b: Point, floor_height_m: float) -> float:
    dx = b.x - a.x
    dy = b.y - a.y
    dz = (b.floor - a.floor) * floor_height_m
    return math.sqrt((dx * dx) + (dy * dy) + (dz * dz))


def build_graph(data: dict, points: Dict[str, Point]) -> Dict[str, List[Tuple[str, float]]]:
    floor_height_m = float(data.get("building", {}).get("floor_height_m", 4.0) or 4.0)
    graph: Dict[str, List[Tuple[str, float]]] = {name: [] for name in points}

    for edge in data.get("corridors", {}).get("edges", []):
        start = str(edge["from"])
        end = str(edge["to"])
        if start not in points or end not in points:
            continue
        length = point_distance(points[start], points[end], floor_height_m)
        graph[start].append((end, length))

    for transition in data.get("transitions", []):
        transition_id = str(transition["id"])
        floors = sorted(int(f) for f in (transition.get("floors") or []))
        if not floors:
            floors = sorted(int(f) for f in (transition.get("floor_locations") or {}).keys())
        # connect adjacent floors in both directions
        for low, high in zip(floors, floors[1:]):
            a = f"{transition_id}-F{low}"
            b = f"{transition_id}-F{high}"
            if a in points and b in points:
                length = point_distance(points[a], points[b], floor_height_m)
                graph[a].append((b, length))
                graph[b].append((a, length))

    return graph


def allowed_graph_for_profile(
    data: dict,
    graph: Dict[str, List[Tuple[str, float]]],
    points: Dict[str, Point],
    profile_name: str,
) -> Dict[str, List[Tuple[str, float]]]:
    if not profile_name:
        return graph

    profiles = data.get("route_profiles", {}) or {}
    profile = profiles.get(profile_name)
    if not profile:
        raise ValueError(f"Route profile not found: {profile_name}")

    allowed_nodes = set(profile.get("allowed_nodes") or [])
    allowed_edges = {tuple(edge) for edge in (profile.get("allowed_edges") or []) if isinstance(edge, list) and len(edge) == 2}
    allowed_transitions = set(profile.get("allowed_transitions") or [])

    # Empty rules means unrestricted for that dimension.
    restrict_nodes = bool(allowed_nodes)
    restrict_edges = bool(allowed_edges)
    restrict_transitions = bool(allowed_transitions)

    filtered: Dict[str, List[Tuple[str, float]]] = {name: [] for name in graph}
    for start, neighbours in graph.items():
        start_point = points[start]
        start_transition_id = start.split("-F", 1)[0] if start_point.kind == "transition_node" and "-F" in start else None
        if restrict_nodes and start not in allowed_nodes and start_point.kind != "transition_node":
            continue
        if restrict_transitions and start_transition_id and start_transition_id not in allowed_transitions:
            continue

        for end, weight in neighbours:
            end_point = points[end]
            end_transition_id = end.split("-F", 1)[0] if end_point.kind == "transition_node" and "-F" in end else None
            if restrict_nodes and end not in allowed_nodes and end_point.kind != "transition_node":
                continue
            if restrict_transitions and end_transition_id and end_transition_id not in allowed_transitions:
                continue
            if restrict_edges and (start, end) not in allowed_edges:
                # allow internal transition travel even when explicit allowed_edges are used
                same_transition = (
                    start_point.kind == "transition_node"
                    and end_point.kind == "transition_node"
                    and start_transition_id == end_transition_id
                )
                if not same_transition:
                    continue
            filtered[start].append((end, weight))
    return filtered


def shortest_path_length(
    graph: Dict[str, List[Tuple[str, float]]],
    start: str,
    end: str,
) -> Tuple[float, List[str]]:
    if start not in graph:
        raise ValueError(f"Unknown start node: {start}")
    if end not in graph:
        raise ValueError(f"Unknown end node: {end}")
    if start == end:
        return 0.0, [start]

    dist: Dict[str, float] = {start: 0.0}
    prev: Dict[str, Optional[str]] = {start: None}
    heap: List[Tuple[float, str]] = [(0.0, start)]

    while heap:
        current_dist, node = heappop(heap)
        if current_dist > dist.get(node, math.inf):
            continue
        if node == end:
            break
        for neighbour, weight in graph.get(node, []):
            new_dist = current_dist + weight
            if new_dist < dist.get(neighbour, math.inf):
                dist[neighbour] = new_dist
                prev[neighbour] = node
                heappush(heap, (new_dist, neighbour))

    if end not in dist:
        raise ValueError(f"No route found from {start} to {end}")

    path: List[str] = []
    node: Optional[str] = end
    while node is not None:
        path.append(node)
        node = prev.get(node)
    path.reverse()
    return dist[end], path


def connection_rows(data: dict) -> List[dict]:
    points = build_points(data)
    graph = build_graph(data, points)
    rows: List[dict] = []

    for connection in data.get("connections", []):
        start = str(connection["from"])
        end = str(connection["to"])
        qty = int(connection.get("qty", 1) or 1)
        profile_name = str(connection.get("route_profile", "") or "")

        working_graph = allowed_graph_for_profile(data, graph, points, profile_name)
        route_length_m, path = shortest_path_length(working_graph, start, end)

        endpoint_extension_m = points[start].extension_distance_m + points[end].extension_distance_m
        total_length_m = route_length_m + endpoint_extension_m

        rows.append(
            {
                "start_location": start,
                "end_location": end,
                "cable_length_m": round(total_length_m, 3),
                "base_route_length_m": round(route_length_m, 3),
                "endpoint_extension_m": round(endpoint_extension_m, 3),
                "qty": qty,
                "path": " -> ".join(path),
            }
        )
    return rows


def write_csv(rows: Iterable[dict], output_path: Path) -> None:
    fieldnames = ["start_location", "end_location", "cable_length_m"]
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row[key] for key in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find shortest cable routes from a cable routing JSON file and write a CSV report."
    )
    parser.add_argument("json_file", help="Path to the cable routing JSON file")
    parser.add_argument(
        "-o",
        "--output",
        help="Output CSV path. Defaults to the JSON filename with _cable_lengths.csv appended.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print resolved paths and length breakdowns to the console.",
    )
    args = parser.parse_args()

    json_path = Path(args.json_file)
    output_path = Path(args.output) if args.output else json_path.with_name(f"{json_path.stem}_cable_lengths.csv")

    data = load_project(json_path)
    rows = connection_rows(data)
    write_csv(rows, output_path)

    print(f"Wrote {len(rows)} row(s) to {output_path}")
    if args.verbose:
        for row in rows:
            print(
                f"{row['start_location']} -> {row['end_location']}: "
                f"{row['cable_length_m']} m "
                f"(route {row['base_route_length_m']} m, "
                f"extension {row['endpoint_extension_m']} m, qty {row['qty']})"
            )
            print(f"  Path: {row['path']}")


if __name__ == "__main__":
    main()
