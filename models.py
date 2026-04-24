import json
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_JSON = {
    "project": {
        "name": "Cable Routing Project",
    },
    "building": {
        "floor_height_m": 4.0,
    },
    "departments": [],
    "locations": [],
    "data_points": [],
    "corridors": {
        "nodes": [],
        "edges": [],
        "auto_connect": False,
    },
    "transitions": [],
    "floor_dxf_files": [],
    "connections": [],
    "route_profiles": {
        "default": {
            "allowed_transitions": [],
            "allowed_nodes": [],
            "allowed_edges": [],
        }
    },
}


class JsonStore:
    def __init__(self, data: Optional[dict] = None):
        self.data = deepcopy(DEFAULT_JSON)
        if data:
            self._load_from_payload(data)

    def _load_from_payload(self, payload: dict) -> None:
        payload = deepcopy(payload)

        # Backwards compatibility for the AMR editor base project.
        if "transitions" not in payload and "lifts" in payload:
            payload["transitions"] = []
            for lift in payload.get("lifts", []):
                payload["transitions"].append(
                    {
                        "id": lift.get("id", "Transition-1"),
                        "floors": lift.get("served_floors", []),
                        "cable_limit": lift.get("capacity_size_units", 0),
                        "floor_locations": lift.get("floor_locations", {}),
                    }
                )
        if "connections" not in payload and "tasks" in payload:
            payload["connections"] = []
            for task in payload.get("tasks", []):
                payload["connections"].append(
                    {
                        "id": task.get("id", "C1"),
                        "from": task.get("pickup", ""),
                        "to": task.get("dropoff", ""),
                        "qty": task.get("qty", 1),
                        "route_profile": task.get("route_profile", ""),
                    }
                )
        for profile in payload.get("route_profiles", {}).values():
            if "allowed_transitions" not in profile and "allowed_lifts" in profile:
                profile["allowed_transitions"] = list(profile.get("allowed_lifts", []))
            profile.setdefault("allowed_transitions", [])
            profile.setdefault("allowed_nodes", [])
            profile.setdefault("allowed_edges", [])

        self.data.update(deepcopy(DEFAULT_JSON))
        for key, value in payload.items():
            self.data[key] = value

        self.data.setdefault("departments", [])
        self.data.setdefault("locations", [])
        self.data.setdefault("data_points", [])
        self.data.setdefault("corridors", {}).setdefault("nodes", [])
        self.data.setdefault("corridors", {}).setdefault("edges", [])
        self.data.setdefault("transitions", [])
        self.data.setdefault("floor_dxf_files", [])
        self.data.setdefault("connections", [])
        self.data.setdefault("route_profiles", deepcopy(DEFAULT_JSON["route_profiles"]))

        for dept in self.data.get("departments", []):
            dept.setdefault("id", "")
            dept.setdefault("name", dept.get("id", ""))
            dept.setdefault("floor", 0)
            dept.setdefault("x", 0.0)
            dept.setdefault("y", 0.0)

        for location in self.data.get("locations", []):
            if "department_ids" not in location:
                legacy = str(location.get("department_id", "")).strip()
                location["department_ids"] = [legacy] if legacy else []
            elif not isinstance(location.get("department_ids"), list):
                legacy_value = location.get("department_ids")
                if legacy_value in (None, ""):
                    location["department_ids"] = []
                else:
                    location["department_ids"] = [str(legacy_value).strip()]
            location["department_ids"] = [
                str(x).strip()
                for x in location.get("department_ids", [])
                if str(x).strip()
            ]

        for point in self.data.get("data_points", []):
            if "department_ids" not in point:
                legacy = str(point.get("department_id", "")).strip()
                point["department_ids"] = [legacy] if legacy else []
            elif not isinstance(point.get("department_ids"), list):
                legacy_value = point.get("department_ids")
                if legacy_value in (None, ""):
                    point["department_ids"] = []
                else:
                    point["department_ids"] = [str(legacy_value).strip()]
            point["department_ids"] = [
                str(x).strip()
                for x in point.get("department_ids", [])
                if str(x).strip()
            ]

    @classmethod
    def from_file(cls, path: str) -> "JsonStore":
        with open(path, "r", encoding="utf-8") as f:
            return cls(json.load(f))

    def save(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2)

    def floor_dxf_path(self, floor: int) -> Optional[str]:
        for entry in self.data.get("floor_dxf_files", []):
            try:
                if int(entry.get("floor")) == int(floor):
                    path = (entry.get("filepath") or "").strip()
                    return path or None
            except Exception:
                continue
        return None

    def set_floor_dxf_path(self, floor: int, filepath: str) -> None:
        entries = self.data.setdefault("floor_dxf_files", [])
        payload = {"floor": int(floor), "filepath": str(filepath)}
        for entry in entries:
            try:
                if int(entry.get("floor")) == int(floor):
                    entry.clear()
                    entry.update(payload)
                    return
            except Exception:
                continue
        entries.append(payload)
        entries.sort(key=lambda item: int(item.get("floor", 0)))

    def clear_floor_dxf_path(self, floor: int) -> None:
        self.data["floor_dxf_files"] = [
            entry
            for entry in self.data.get("floor_dxf_files", [])
            if int(entry.get("floor", -(10**9))) != int(floor)
        ]

    def names_in_use(self) -> set:
        names = set()
        for point in self.all_points().values():
            names.add(point["name"])
        return names

    def department_ids(self) -> set:
        return {
            str(item.get("id", "")).strip()
            for item in self.data.get("departments", [])
            if str(item.get("id", "")).strip()
        }

    def departments_for_floor(self, floor: int) -> Dict[str, dict]:
        result = {}
        for item in self.data.get("departments", []):
            if int(item.get("floor", 0)) != int(floor):
                continue
            dept_id = str(item.get("id", "")).strip()
            if not dept_id:
                continue
            result[dept_id] = {
                **item,
                "id": dept_id,
                "name": str(item.get("name", dept_id)),
                "floor": int(item.get("floor", 0)),
                "x": float(item.get("x", 0.0)),
                "y": float(item.get("y", 0.0)),
                "kind": "department",
            }
        return result

    def add_department(
        self,
        name: str,
        floor: int,
        x: float,
        y: float,
        department_id: Optional[str] = None,
    ) -> str:
        if department_id is None or not str(department_id).strip():
            department_id = self.suggest_next_department_id()
        department_id = str(department_id).strip()

        self.data["departments"].append(
            {
                "id": department_id,
                "name": str(name).strip(),
                "floor": int(floor),
                "x": round(x, 3),
                "y": round(y, 3),
            }
        )
        return department_id

    def set_department_position(self, department_id: str, x: float, y: float) -> None:
        x = round(x, 3)
        y = round(y, 3)
        for item in self.data.get("departments", []):
            if str(item.get("id", "")).strip() == str(department_id).strip():
                item["x"] = x
                item["y"] = y
                return

    def all_points(self) -> Dict[str, dict]:
        result: Dict[str, dict] = {}

        for item in self.data.get("locations", []):
            kind = str(item.get("kind", "location") or "location")
            result[item["name"]] = {**item, "kind": kind}

        for item in self.data.get("data_points", []):
            result[item["name"]] = {**item, "kind": "data_point"}

        for item in self.data.get("corridors", {}).get("nodes", []):
            result[item["name"]] = {**item, "kind": "corridor_node"}

        for transition in self.data.get("transitions", []):
            transition_id = transition["id"]
            for floor_str, pos in transition.get("floor_locations", {}).items():
                result[f"{transition_id}-F{floor_str}"] = {
                    "name": f"{transition_id}-F{floor_str}",
                    "floor": int(floor_str),
                    "x": float(pos["x"]),
                    "y": float(pos["y"]),
                    "kind": "transition_node",
                    "transition_id": transition_id,
                }

        return result

    def points_for_floor(self, floor: int) -> Dict[str, dict]:
        return {
            name: point
            for name, point in self.all_points().items()
            if int(point["floor"]) == int(floor)
        }

    def edges_for_floor(self, floor: int) -> List[dict]:
        points = self.all_points()
        edges = []
        for edge in self.data.get("corridors", {}).get("edges", []):
            a = points.get(edge["from"])
            b = points.get(edge["to"])
            if (
                a
                and b
                and int(a["floor"]) == int(floor)
                and int(b["floor"]) == int(floor)
            ):
                edges.append(edge)
        return edges

    def add_corridor_node(
        self,
        name: str,
        floor: int,
        x: float,
        y: float,
        height_affl_m: float = 0.0,
        cable_limit: int = 0,
    ) -> None:
        self.data["corridors"]["nodes"].append(
            {
                "name": name,
                "floor": floor,
                "x": round(x, 3),
                "y": round(y, 3),
                "height_affl_m": float(height_affl_m),
                "cable_limit": int(cable_limit),
            }
        )

    def add_location(
        self,
        name: str,
        floor: int,
        x: float,
        y: float,
        kind: str = "location",
        department_ids: Optional[List[str]] = None,
    ) -> None:
        self.data["locations"].append(
            {
                "name": name,
                "floor": floor,
                "x": round(x, 3),
                "y": round(y, 3),
                "kind": kind,
                "department_ids": [
                    str(x).strip() for x in (department_ids or []) if str(x).strip()
                ],
            }
        )

    def add_data_point(
        self,
        name: str,
        floor: int,
        x: float,
        y: float,
        qty: int = 1,
        extension_distance_m: float = 0.0,
        department_ids: Optional[List[str]] = None,
    ) -> None:
        self.data["data_points"].append(
            {
                "name": name,
                "floor": floor,
                "x": round(x, 3),
                "y": round(y, 3),
                "qty": int(qty),
                "extension_distance_m": float(extension_distance_m),
                "department_ids": [
                    str(x).strip() for x in (department_ids or []) if str(x).strip()
                ],
            }
        )

    def add_edge(self, from_name: str, to_name: str) -> None:
        edges = self.data["corridors"]["edges"]
        if not any(e["from"] == from_name and e["to"] == to_name for e in edges):
            edges.append({"from": from_name, "to": to_name})

    def remove_edge(self, from_name: str, to_name: str) -> None:
        self.data["corridors"]["edges"] = [
            e
            for e in self.data["corridors"]["edges"]
            if not (e["from"] == from_name and e["to"] == to_name)
        ]

    def set_point_position(self, name: str, x: float, y: float) -> None:
        x = round(x, 3)
        y = round(y, 3)

        for item in self.data.get("departments", []):
            if str(item.get("id", "")).strip() == str(name).strip():
                item["x"] = x
                item["y"] = y
                return

        for item in self.data.get("locations", []):
            if item["name"] == name:
                item["x"] = x
                item["y"] = y
                return
        for item in self.data.get("data_points", []):
            if item["name"] == name:
                item["x"] = x
                item["y"] = y
                return
        for item in self.data.get("corridors", {}).get("nodes", []):
            if item["name"] == name:
                item["x"] = x
                item["y"] = y
                return
        if "-F" in name:
            transition_id, floor_text = name.rsplit("-F", 1)
            for transition in self.data.get("transitions", []):
                if transition["id"] == transition_id and floor_text in transition.get(
                    "floor_locations", {}
                ):
                    transition["floor_locations"][floor_text]["x"] = x
                    transition["floor_locations"][floor_text]["y"] = y
                    return

    def move_transition_from_floor_up(
        self,
        transition_id: str,
        from_floor: int,
        new_x: float,
        new_y: float,
    ) -> int:
        transition_id = str(transition_id).strip()
        from_floor = int(from_floor)
        new_x = round(float(new_x), 3)
        new_y = round(float(new_y), 3)

        for transition in self.data.get("transitions", []):
            if str(transition.get("id", "")).strip() != transition_id:
                continue

            floor_locations = transition.setdefault("floor_locations", {})
            current = floor_locations.get(str(from_floor))
            if current is None:
                current = floor_locations.get(from_floor)

            if not isinstance(current, dict):
                return 0

            old_x = float(current.get("x", 0.0))
            old_y = float(current.get("y", 0.0))

            dx = new_x - old_x
            dy = new_y - old_y

            moved_count = 0

            for floor_key, pos in list(floor_locations.items()):
                try:
                    floor = int(floor_key)
                except Exception:
                    continue

                if floor < from_floor:
                    continue

                if not isinstance(pos, dict):
                    continue

                pos["x"] = round(float(pos.get("x", 0.0)) + dx, 3)
                pos["y"] = round(float(pos.get("y", 0.0)) + dy, 3)
                moved_count += 1

            return moved_count

        return 0

    def rename_point(self, old_name: str, new_name: str) -> None:
        if old_name == new_name:
            return
        for collection in (
            self.data.get("locations", []),
            self.data.get("data_points", []),
            self.data.get("corridors", {}).get("nodes", []),
        ):
            for item in collection:
                if item["name"] == old_name:
                    item["name"] = new_name

        for edge in self.data.get("corridors", {}).get("edges", []):
            if edge["from"] == old_name:
                edge["from"] = new_name
            if edge["to"] == old_name:
                edge["to"] = new_name

        for connection in self.data.get("connections", []):
            if connection.get("from") == old_name:
                connection["from"] = new_name
            if connection.get("to") == old_name:
                connection["to"] = new_name

        for profile in self.data.get("route_profiles", {}).values():
            profile["allowed_nodes"] = [
                new_name if x == old_name else x
                for x in profile.get("allowed_nodes", [])
            ]
            profile["allowed_edges"] = [
                [new_name if part == old_name else part for part in edge_pair]
                for edge_pair in profile.get("allowed_edges", [])
            ]

    def delete_point(self, name: str) -> None:
        self.data["locations"] = [
            x for x in self.data.get("locations", []) if x["name"] != name
        ]
        self.data["data_points"] = [
            x for x in self.data.get("data_points", []) if x["name"] != name
        ]
        self.data["corridors"]["nodes"] = [
            x
            for x in self.data.get("corridors", {}).get("nodes", [])
            if x["name"] != name
        ]
        self.data["corridors"]["edges"] = [
            e
            for e in self.data.get("corridors", {}).get("edges", [])
            if e["from"] != name and e["to"] != name
        ]
        self.data["connections"] = [
            c
            for c in self.data.get("connections", [])
            if c.get("from") != name and c.get("to") != name
        ]
        for profile in self.data.get("route_profiles", {}).values():
            profile["allowed_nodes"] = [
                x for x in profile.get("allowed_nodes", []) if x != name
            ]
            profile["allowed_edges"] = [
                pair for pair in profile.get("allowed_edges", []) if name not in pair
            ]

    def rename_department(
        self, old_id: str, new_id: str, new_name: Optional[str] = None
    ) -> None:
        old_id = str(old_id).strip()
        new_id = str(new_id).strip()
        for item in self.data.get("departments", []):
            if str(item.get("id", "")).strip() == old_id:
                item["id"] = new_id
                if new_name is not None:
                    item["name"] = str(new_name).strip()
                break

        for location in self.data.get("locations", []):
            updated = []
            for department_id in location.get("department_ids", []):
                updated.append(
                    new_id
                    if str(department_id).strip() == old_id
                    else str(department_id).strip()
                )
            location["department_ids"] = updated

    def delete_department(self, department_id: str) -> None:
        department_id = str(department_id).strip()
        self.data["departments"] = [
            item
            for item in self.data.get("departments", [])
            if str(item.get("id", "")).strip() != department_id
        ]

        for location in self.data.get("locations", []):
            location["department_ids"] = [
                str(x).strip()
                for x in location.get("department_ids", [])
                if str(x).strip() != department_id
            ]

    def upsert_transition(
        self,
        transition_id: str,
        floors: List[int],
        floor_locations: Dict[int, Tuple[float, float]],
        cable_limit: int = 0,
    ) -> None:
        transition = None
        for existing in self.data["transitions"]:
            if existing["id"] == transition_id:
                transition = existing
                break
        payload = {
            "id": transition_id,
            "floors": sorted(int(f) for f in floors),
            "cable_limit": int(cable_limit),
            "floor_locations": {
                str(f): {"x": round(pos[0], 3), "y": round(pos[1], 3)}
                for f, pos in floor_locations.items()
            },
        }
        if transition is None:
            self.data["transitions"].append(payload)
        else:
            transition.clear()
            transition.update(payload)

    def delete_transition(self, transition_id: str) -> None:
        names_to_delete = {
            f"{transition_id}-F{floor}"
            for transition in self.data.get("transitions", [])
            if transition["id"] == transition_id
            for floor in transition.get("floor_locations", {}).keys()
        }
        self.data["transitions"] = [
            x for x in self.data.get("transitions", []) if x["id"] != transition_id
        ]
        self.data["corridors"]["edges"] = [
            e
            for e in self.data.get("corridors", {}).get("edges", [])
            if e["from"] not in names_to_delete and e["to"] not in names_to_delete
        ]
        for profile in self.data.get("route_profiles", {}).values():
            profile["allowed_transitions"] = [
                x for x in profile.get("allowed_transitions", []) if x != transition_id
            ]
            profile["allowed_nodes"] = [
                x for x in profile.get("allowed_nodes", []) if x not in names_to_delete
            ]
            profile["allowed_edges"] = [
                pair
                for pair in profile.get("allowed_edges", [])
                if not any(name in pair for name in names_to_delete)
            ]

    def validate(self) -> List[str]:
        errors = []
        names = self.names_in_use()
        department_ids = self.department_ids()
        route_profile_names = set(self.data.get("route_profiles", {}).keys())
        transition_ids = {x.get("id") for x in self.data.get("transitions", [])}

        for edge in self.data.get("corridors", {}).get("edges", []):
            if edge.get("from") not in names:
                errors.append(f"Unknown edge start: {edge.get('from')}")
            if edge.get("to") not in names:
                errors.append(f"Unknown edge end: {edge.get('to')}")

        for connection in self.data.get("connections", []):
            connection_id = connection.get("id", "-")
            if connection.get("from") not in names:
                errors.append(
                    f"Connection {connection_id} start not found: {connection.get('from')}"
                )
            if connection.get("to") not in names:
                errors.append(
                    f"Connection {connection_id} end not found: {connection.get('to')}"
                )
            rp = connection.get("route_profile", "")
            if rp and rp not in route_profile_names:
                errors.append(
                    f"Connection {connection_id} route profile not found: {rp}"
                )
            try:
                qty = int(connection.get("qty", 1))
                if qty <= 0:
                    errors.append(
                        f"Connection {connection_id} qty must be greater than 0"
                    )
            except Exception:
                errors.append(f"Connection {connection_id} has invalid qty")

        for profile_name, profile in self.data.get("route_profiles", {}).items():
            for transition_id in profile.get("allowed_transitions", []):
                if transition_id not in transition_ids:
                    errors.append(
                        f"Route profile {profile_name} has unknown transition: {transition_id}"
                    )
            for node_name in profile.get("allowed_nodes", []):
                if node_name not in names:
                    errors.append(
                        f"Route profile {profile_name} has unknown node: {node_name}"
                    )
            for edge_pair in profile.get("allowed_edges", []):
                if len(edge_pair) != 2:
                    errors.append(
                        f"Route profile {profile_name} has invalid edge pair: {edge_pair}"
                    )
                    continue
                if edge_pair[0] not in names or edge_pair[1] not in names:
                    errors.append(
                        f"Route profile {profile_name} has unknown edge endpoint: {edge_pair}"
                    )

        for node in self.data.get("corridors", {}).get("nodes", []):
            if not str(node.get("name", "")).strip():
                errors.append("Corridor node has no name")

        for point in self.data.get("data_points", []):
            try:
                qty = int(point.get("qty", 1))
                if qty <= 0:
                    errors.append(
                        f"Data point {point.get('name', '-')} qty must be greater than 0"
                    )
            except Exception:
                errors.append(f"Data point {point.get('name', '-')} has invalid qty")

            raw_ids = point.get("department_ids", [])
            if not isinstance(raw_ids, list):
                errors.append(
                    f"Data point {point.get('name', '-')} has invalid department_ids"
                )
                continue

            for department_id in raw_ids:
                department_id = str(department_id).strip()
                if department_id and department_id not in self.department_ids():
                    errors.append(
                        f"Data point {point.get('name', '-')} references unknown department: {department_id}"
                    )

        seen_floors = set()
        for entry in self.data.get("floor_dxf_files", []):
            if not isinstance(entry, dict):
                errors.append(f"Invalid floor_dxf_files entry: {entry}")
                continue
            try:
                floor = int(entry.get("floor"))
            except Exception:
                errors.append(f"DXF mapping has invalid floor: {entry.get('floor')}")
                continue
            filepath = str(entry.get("filepath") or "").strip()
            if not filepath:
                errors.append(f"DXF mapping for floor {floor} has empty filepath")
            if floor in seen_floors:
                errors.append(f"Duplicate DXF mapping for floor {floor}")
            seen_floors.add(floor)

        seen_department_ids = set()
        for dept in self.data.get("departments", []):
            dept_id = str(dept.get("id", "")).strip()
            if not dept_id:
                errors.append("Department has no ID")
                continue
            if dept_id in seen_department_ids:
                errors.append(f"Duplicate department ID: {dept_id}")
            seen_department_ids.add(dept_id)

        for location in self.data.get("locations", []):
            raw_ids = location.get("department_ids", [])
            if not isinstance(raw_ids, list):
                errors.append(
                    f"Location {location.get('name', '-')} has invalid department_ids"
                )
                continue

            for department_id in raw_ids:
                department_id = str(department_id).strip()
                if department_id and department_id not in department_ids:
                    errors.append(
                        f"Location {location.get('name', '-')} references unknown department: {department_id}"
                    )
        return errors

    def suggest_next_department_id(self) -> str:
        nums = []
        for item in self.data.get("departments", []):
            dept_id = str(item.get("id", "")).strip()
            if dept_id.isdigit():
                nums.append(int(dept_id))
        return str(max(nums, default=0) + 1)

    def suggest_next_corridor_name(self, floor: int) -> str:
        prefix = f"C{floor}-"
        nums = []
        for item in self.data.get("corridors", {}).get("nodes", []):
            name = item["name"]
            if name.startswith(prefix):
                tail = name[len(prefix) :]
                if tail.isdigit():
                    nums.append(int(tail))
        return f"C{floor}-{max(nums, default=0) + 1}"

    def suggest_next_data_point_name(self, floor: int) -> str:
        prefix = f"DP{floor}-"
        nums = []
        for item in self.data.get("data_points", []):
            name = str(item.get("name", ""))
            if name.startswith(prefix):
                tail = name[len(prefix) :]
                if tail.isdigit():
                    nums.append(int(tail))
        return f"DP{floor}-{max(nums, default=0) + 1}"

    def suggest_next_transition_id(self) -> str:
        nums = []
        for item in self.data.get("transitions", []):
            transition_id = str(item.get("id", ""))
            if transition_id.startswith("TR-") and transition_id[3:].isdigit():
                nums.append(int(transition_id[3:]))
        return f"TR-{max(nums, default=0) + 1}"

    def suggest_next_connection_id(self) -> str:
        nums = []
        for connection in self.data.get("connections", []):
            connection_id = str(connection.get("id", ""))
            if connection_id.startswith("C") and connection_id[1:].isdigit():
                nums.append(int(connection_id[1:]))
        return f"C{max(nums, default=0) + 1}"

    def _point_record_by_name(self, name: str):
        name = str(name).strip()

        for item in self.data.get("corridors", {}).get("nodes", []):
            if str(item.get("name", "")).strip() == name:
                return "corridor_node", item

        for item in self.data.get("data_points", []):
            if str(item.get("name", "")).strip() == name:
                return "data_point", item

        return None, None

    def _suggest_next_corridor_name_for_floor(self, floor: int, used_names: set) -> str:
        prefix = f"C{int(floor)}-"
        nums = []
        for name in used_names:
            if str(name).startswith(prefix):
                tail = str(name)[len(prefix) :]
                if tail.isdigit():
                    nums.append(int(tail))
        return f"C{int(floor)}-{max(nums, default=0) + 1}"

    def _suggest_next_data_point_name_for_floor(
        self, floor: int, used_names: set
    ) -> str:
        prefix = f"DP{int(floor)}-"
        nums = []
        for name in used_names:
            if str(name).startswith(prefix):
                tail = str(name)[len(prefix) :]
                if tail.isdigit():
                    nums.append(int(tail))
        return f"DP{int(floor)}-{max(nums, default=0) + 1}"

    def clone_template_between_floors(
        self,
        source_names: List[str],
        target_floor: int,
        include_internal_edges: bool = True,
        offset_x: float = 0.0,
        offset_y: float = 0.0,
    ) -> dict:
        selected_names = [
            str(x).strip() for x in (source_names or []) if str(x).strip()
        ]
        if not selected_names:
            raise ValueError("Select one or more corridor nodes or data points")

        used_names = set(self.names_in_use())
        selected_set = set(selected_names)

        id_map = {}
        created_corridors = []
        created_data_points = []
        skipped = []

        for old_name in selected_names:
            kind, record = self._point_record_by_name(old_name)
            if kind is None or record is None:
                skipped.append(old_name)
                continue

            if kind == "corridor_node":
                new_name = self._suggest_next_corridor_name_for_floor(
                    target_floor, used_names
                )
                used_names.add(new_name)

                new_record = {
                    "name": new_name,
                    "floor": int(target_floor),
                    "x": round(float(record.get("x", 0.0)) + float(offset_x), 3),
                    "y": round(float(record.get("y", 0.0)) + float(offset_y), 3),
                    "height_affl_m": float(record.get("height_affl_m", 0.0) or 0.0),
                    "cable_limit": int(record.get("cable_limit", 0) or 0),
                }
                self.data.setdefault("corridors", {}).setdefault("nodes", []).append(
                    new_record
                )
                id_map[old_name] = new_name
                created_corridors.append(new_name)

            elif kind == "data_point":
                new_name = self._suggest_next_data_point_name_for_floor(
                    target_floor, used_names
                )
                used_names.add(new_name)

                new_record = {
                    "name": new_name,
                    "floor": int(target_floor),
                    "x": round(float(record.get("x", 0.0)) + float(offset_x), 3),
                    "y": round(float(record.get("y", 0.0)) + float(offset_y), 3),
                    "qty": int(record.get("qty", 1) or 1),
                    "extension_distance_m": float(
                        record.get("extension_distance_m", 0.0) or 0.0
                    ),
                    "department_ids": [],
                }
                self.data.setdefault("data_points", []).append(new_record)
                id_map[old_name] = new_name
                created_data_points.append(new_name)

        created_edges = []
        if include_internal_edges and id_map:
            existing_edges = self.data.setdefault("corridors", {}).setdefault(
                "edges", []
            )
            existing_pairs = {
                (str(edge.get("from", "")).strip(), str(edge.get("to", "")).strip())
                for edge in existing_edges
            }

            for edge in list(existing_edges):
                start = str(edge.get("from", "")).strip()
                end = str(edge.get("to", "")).strip()

                if start not in selected_set or end not in selected_set:
                    continue
                if start not in id_map or end not in id_map:
                    continue

                new_start = id_map[start]
                new_end = id_map[end]
                pair = (new_start, new_end)

                if pair in existing_pairs:
                    continue

                existing_edges.append({"from": new_start, "to": new_end})
                existing_pairs.add(pair)
                created_edges.append({"from": new_start, "to": new_end})

        return {
            "id_map": id_map,
            "created_corridors": created_corridors,
            "created_data_points": created_data_points,
            "created_edges": created_edges,
            "skipped": skipped,
        }

    @staticmethod
    def basename(path: Optional[str]) -> str:
        if not path:
            return "New file"
        return Path(path).name
