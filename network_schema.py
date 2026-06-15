"""Data model helpers for the Cable Routing Solver network-planning extension."""

from __future__ import annotations

from copy import deepcopy
from typing import Dict, Iterable, List, Optional

NETWORK_DEFAULTS = {
    "network_settings": {
        "technology": "Traditional",
        "expected_mer_count": 2,
        "redundant_core": True,
        "spare_capacity_percent": 15.0,
        "traditional_max_copper_m": 90.0,
        "polan_max_ont_copper_m": 30.0,
        "polan_max_onts_per_splitter": 8,
        "polan_max_splitter_ont_route_m": 120.0,
        "polan_olt_failover": True,
        "default_poe_power_w": 0.0,
        "poe_power_defaults": {},
        "default_rack_size_u": 42,
    },
    "network_assets": [],
    "network_asset_instances": [],
    "network_connections": [],
    "network_endpoint_assignments": [],
    "network_patch_leads": [],
    "network_redundancy_groups": [],
    "network_vlans": [],
    "network_routes": [],
    "network_design_summary": {},
}

NETWORK_ASSET_TYPES = {
    "patch_panel",
    "fibre_splitter",
    "network_switch",
    "network_router",
    "firewall",
    "wireless_access_point",
    "optical_line_terminal",
    "optical_network_terminal",
    "other",
}

NETWORK_MEDIA = {"copper", "fibre", "wireless", "virtual", "stacking", "none"}
NETWORK_CONNECTION_ROLES = {"input", "output", "uplink"}
NETWORK_LOCATION_TYPES = {"mer", "polan"}

NETWORK_PORT_TYPES = {"rj45", "sfp", "sfp+", "qsfp", "qsfp28", "pon", "lc", "sc", "mpo", "usb", "console", "power", "other"}
NETWORK_PORT_USES = {"input", "output", "uplink", "downlink", "management", "console", "pon", "client", "patch", "stacking", "power", "spare", "other"}


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _as_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalise_string_list(values) -> List[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = values.replace(";", ",").split(",")
    result: List[str] = []
    for value in values:
        text = _text(value)
        if text and text not in result:
            result.append(text)
    return result


def ensure_network_schema(data: dict) -> dict:
    """Ensure that *data* contains a complete, backwards-compatible network schema."""

    if not isinstance(data, dict):
        raise TypeError("Project data must be a dictionary")

    settings = data.setdefault("network_settings", {})
    if not isinstance(settings, dict):
        settings = {}
        data["network_settings"] = settings
    settings.setdefault("technology", "Traditional")
    settings.setdefault("expected_mer_count", 2)
    settings.setdefault("redundant_core", True)
    settings.setdefault("spare_capacity_percent", 15.0)
    settings.setdefault("traditional_max_copper_m", 90.0)
    settings.setdefault("polan_max_ont_copper_m", 30.0)
    settings.setdefault("polan_max_onts_per_splitter", 8)
    settings.setdefault("polan_max_splitter_ont_route_m", 120.0)
    settings.setdefault("polan_olt_failover", True)
    settings.setdefault("default_poe_power_w", 0.0)
    settings.setdefault("poe_power_defaults", {})
    settings.setdefault("default_rack_size_u", 42)
    technology = _text(settings.get("technology")) or "Traditional"
    settings["technology"] = "PoLAN" if technology.lower() == "polan" else "Traditional"
    settings["expected_mer_count"] = max(
        1, _as_int(settings.get("expected_mer_count"), 2)
    )
    settings["redundant_core"] = bool(settings.get("redundant_core", True))
    settings["spare_capacity_percent"] = max(
        0.0, _as_float(settings.get("spare_capacity_percent"), 15.0)
    )
    settings["traditional_max_copper_m"] = max(
        1.0, _as_float(settings.get("traditional_max_copper_m"), 90.0)
    )
    settings["polan_max_ont_copper_m"] = max(
        1.0, _as_float(settings.get("polan_max_ont_copper_m"), 30.0)
    )
    settings["polan_max_onts_per_splitter"] = max(
        1, _as_int(settings.get("polan_max_onts_per_splitter"), 8)
    )
    settings["polan_max_splitter_ont_route_m"] = max(
        0.0, _as_float(settings.get("polan_max_splitter_ont_route_m"), 120.0)
    )
    settings["polan_olt_failover"] = bool(settings.get("polan_olt_failover", True))
    settings["default_poe_power_w"] = max(
        0.0, _as_float(settings.get("default_poe_power_w"), 0.0)
    )
    settings["default_rack_size_u"] = max(
        1, _as_int(settings.get("default_rack_size_u"), 42)
    )
    if not isinstance(settings.get("poe_power_defaults"), dict):
        settings["poe_power_defaults"] = {}

    for key in (
        "network_assets",
        "network_asset_instances",
        "network_connections",
        "network_endpoint_assignments",
        "network_patch_leads",
        "network_redundancy_groups",
        "network_vlans",
        "network_routes",
    ):
        if not isinstance(data.get(key), list):
            data[key] = []

    if not isinstance(data.get("network_design_summary"), dict):
        data["network_design_summary"] = {}

    for asset in data["network_assets"]:
        if not isinstance(asset, dict):
            continue
        asset.setdefault("id", "")
        asset.setdefault("name", asset.get("id", ""))
        asset_type = _text(asset.get("asset_type")) or "other"
        if (
            asset_type == "other"
            and "network_router" in _text(asset.get("notes")).lower()
        ):
            asset_type = "network_router"
        asset["asset_type"] = (
            asset_type if asset_type in NETWORK_ASSET_TYPES else "other"
        )
        asset.setdefault("manufacturer", "")
        asset.setdefault("model", "")
        asset.setdefault("patch_panel_type", "")
        asset.setdefault("split_ratio", "")
        asset.setdefault("max_split_ratio", "")
        asset["frequencies"] = _normalise_string_list(asset.get("frequencies", []))
        asset["power_input_w"] = max(0.0, _as_float(asset.get("power_input_w")))
        asset["poe_budget_w"] = max(0.0, _as_float(asset.get("poe_budget_w")))
        asset["number_of_ports"] = max(0, _as_int(asset.get("number_of_ports")))
        asset["connections_in"] = max(0, _as_int(asset.get("connections_in")))
        asset["connections_out"] = max(0, _as_int(asset.get("connections_out")))
        asset["uplink_ports"] = max(0, _as_int(asset.get("uplink_ports")))
        asset["supports_stacking"] = bool(asset.get("supports_stacking", False))
        asset["max_stack_members"] = max(1, _as_int(asset.get("max_stack_members"), 1))
        if not asset["supports_stacking"]:
            asset["max_stack_members"] = 1
        asset["rack_units"] = max(0, _as_int(asset.get("rack_units"), 1))
        asset["olt_units_per_rack_unit"] = max(
            1, _as_int(asset.get("olt_units_per_rack_unit"), 1)
        )
        if asset["asset_type"] != "optical_line_terminal":
            asset["olt_units_per_rack_unit"] = 1
        asset["switch_rack_unit_allowance"] = max(
            0,
            _as_int(asset.get("switch_rack_unit_allowance"), asset["rack_units"] or 1),
        )
        if asset["asset_type"] != "network_switch":
            asset["switch_rack_unit_allowance"] = 0
        for field, default in (
            ("input_connection_type", "copper"),
            ("output_connection_type", "copper"),
            ("uplink_connection_type", "fibre"),
        ):
            value = _text(asset.get(field)).lower() or default
            asset[field] = value if value in NETWORK_MEDIA else default
        asset.setdefault("notes", "")
        # Structured physical port definitions supersede the legacy aggregate
        # counters while retaining those counters for backwards compatibility.
        raw_port_definitions = asset.get("port_definitions", [])
        if not isinstance(raw_port_definitions, list):
            raw_port_definitions = []
        port_definitions = []
        for row in raw_port_definitions:
            if not isinstance(row, dict):
                continue
            port_type = _text(row.get("port_type")).lower() or "other"
            port_use = _text(row.get("port_use")).lower() or "other"
            port_definitions.append({
                "port_type": port_type if port_type in NETWORK_PORT_TYPES else "other",
                "port_count": max(0, _as_int(row.get("port_count"))),
                "port_use": port_use if port_use in NETWORK_PORT_USES else "other",
                "name_prefix": _text(row.get("name_prefix")),
            })
        port_definitions = [row for row in port_definitions if row["port_count"] > 0]
        if not port_definitions and asset["number_of_ports"] > 0:
            default_type = "pon" if asset["asset_type"] in {"optical_line_terminal", "optical_network_terminal"} else ("lc" if asset["asset_type"] == "fibre_splitter" or asset.get("patch_panel_type") == "fibre" else "rj45")
            default_use = "patch" if asset["asset_type"] == "patch_panel" else ("pon" if default_type == "pon" else "client")
            port_definitions = [{"port_type": default_type, "port_count": asset["number_of_ports"], "port_use": default_use, "name_prefix": ""}]
        asset["port_definitions"] = port_definitions
        if port_definitions:
            asset["number_of_ports"] = sum(row["port_count"] for row in port_definitions)

    locations = {
        _text(item.get("name")): item
        for item in data.get("locations", [])
        if isinstance(item, dict) and _text(item.get("name"))
    }
    assets_by_id = {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }
    instances_by_id = {
        _text(item.get("id")): item
        for item in data.get("network_asset_instances", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }

    for instance in data["network_asset_instances"]:
        if not isinstance(instance, dict):
            continue
        instance.setdefault("id", "")
        instance.setdefault("name", instance.get("id", ""))
        instance.setdefault("asset_id", "")
        instance.setdefault("location_name", "")
        linked_location = locations.get(_text(instance.get("location_name")), {})
        instance["floor"] = _as_int(
            instance.get("floor", linked_location.get("floor", 0))
        )
        instance["x"] = _as_float(instance.get("x", linked_location.get("x", 0.0)))
        instance["y"] = _as_float(instance.get("y", linked_location.get("y", 0.0)))
        instance.setdefault("rack_name", "")
        instance["rack_start_u"] = max(0, _as_int(instance.get("rack_start_u")))
        instance["rack_size_u"] = max(0, _as_int(instance.get("rack_size_u")))
        instance.setdefault("management_ip", "")
        instance.setdefault("management_vlan", "")
        instance.setdefault("power_feed", "")
        instance.setdefault("ups_source", "")
        instance["logical_stack"] = bool(instance.get("logical_stack", False))
        instance["stack_member_count"] = max(
            1, _as_int(instance.get("stack_member_count"), 1)
        )
        instance.setdefault("stack_member_asset_id", "")
        instance["stack_interconnect_count"] = max(
            0,
            _as_int(
                instance.get("stack_interconnect_count"),
                max(0, instance["stack_member_count"] - 1),
            ),
        )
        instance["stack_interconnect_medium"] = (
            _text(instance.get("stack_interconnect_medium")).lower() or "stacking"
        )
        if instance["stack_interconnect_medium"] not in NETWORK_MEDIA:
            instance["stack_interconnect_medium"] = "stacking"
        instance.setdefault("stack_interconnect_specification", "")
        instance.setdefault("notes", "")

    for connection in data["network_connections"]:
        if not isinstance(connection, dict):
            continue
        connection.setdefault("id", "")
        connection.setdefault("from_instance_id", "")
        connection.setdefault("from_port", "")
        connection.setdefault("to_instance_id", "")
        connection.setdefault("to_port", "")
        role = _text(connection.get("connection_role")).lower() or "output"
        connection["connection_role"] = (
            role if role in NETWORK_CONNECTION_ROLES else "output"
        )
        medium = _text(connection.get("medium")).lower() or "copper"
        connection["medium"] = medium if medium in NETWORK_MEDIA else "copper"
        connection.setdefault("cable_specification", "")
        connection["fibre_count"] = max(0, _as_int(connection.get("fibre_count")))
        connection["vlan_ids"] = _normalise_string_list(connection.get("vlan_ids", []))
        connection.setdefault("route_profile", "")
        connection["route_path"] = _normalise_string_list(
            connection.get("route_path", [])
        )
        connection.setdefault("notes", "")

    for assignment in data["network_endpoint_assignments"]:
        if not isinstance(assignment, dict):
            continue
        assignment.setdefault("id", "")
        assignment.setdefault("endpoint_name", "")
        assignment["endpoint_port"] = max(
            1, _as_int(assignment.get("endpoint_port"), 1)
        )
        assignment.setdefault("endpoint_asset_id", "")
        assignment.setdefault("endpoint_asset_name", "")
        assignment.setdefault("department_id", "")
        assignment.setdefault("department_name", "")
        assignment.setdefault("room_type_id", "")
        assignment["floor"] = _as_int(assignment.get("floor"))
        assignment["x"] = _as_float(assignment.get("x"))
        assignment["y"] = _as_float(assignment.get("y"))
        assignment.setdefault("network_instance_id", "")
        assignment.setdefault("network_port", "")
        assignment["poe_power_w"] = max(0.0, _as_float(assignment.get("poe_power_w")))
        assignment["copper_length_m"] = max(
            0.0, _as_float(assignment.get("copper_length_m"))
        )
        assignment["route_path"] = _normalise_string_list(
            assignment.get("route_path", [])
        )
        assignment["vlan_ids"] = _normalise_string_list(assignment.get("vlan_ids", []))
        assignment.setdefault("technology", settings["technology"])
        assignment["auto_generated"] = bool(assignment.get("auto_generated", False))

    for lead in data["network_patch_leads"]:
        if not isinstance(lead, dict):
            continue
        lead.setdefault("id", "")
        lead.setdefault("connection_id", "")
        lead.setdefault("assignment_id", "")
        lead.setdefault("instance_id", "")
        lead.setdefault("port", "")
        lead.setdefault("peer_instance_id", "")
        lead.setdefault("peer_port", "")
        lead.setdefault("endpoint_name", "")
        lead.setdefault("port_type", "other")
        lead.setdefault("port_use", "patch")
        lead.setdefault("medium", "copper")
        lead.setdefault("cable_specification", "")
        lead["length_m"] = max(0.0, _as_float(lead.get("length_m"), 2.0))
        lead["auto_generated"] = bool(lead.get("auto_generated", False))

    for group in data["network_redundancy_groups"]:
        if not isinstance(group, dict):
            continue
        for field in (
            "id",
            "technology",
            "protected_instance_id",
            "primary_olt_instance_id",
            "secondary_olt_instance_id",
            "protection_type",
        ):
            group.setdefault(field, "")
        group["auto_generated"] = bool(group.get("auto_generated", False))

    for vlan in data["network_vlans"]:
        if not isinstance(vlan, dict):
            continue
        vlan.setdefault("id", "")
        vlan["vlan_id"] = max(0, _as_int(vlan.get("vlan_id")))
        for field in (
            "name",
            "purpose",
            "subnet",
            "gateway",
            "dhcp_scope",
            "security_zone",
            "notes",
        ):
            vlan.setdefault(field, "")

    for route in data["network_routes"]:
        if not isinstance(route, dict):
            continue
        route.setdefault("id", "")
        for field in (
            "source",
            "destination",
            "vlan_id",
            "protocol",
            "next_hop",
            "firewall_policy",
            "notes",
        ):
            route.setdefault(field, "")
        route["metric"] = max(0, _as_int(route.get("metric")))

    return data


def next_network_id(items: Iterable[dict], prefix: str) -> str:
    used = {_text(item.get("id")) for item in items if isinstance(item, dict)}
    number = 1
    while f"{prefix}{number}" in used:
        number += 1
    return f"{prefix}{number}"


def network_assets_by_id(data: dict) -> Dict[str, dict]:
    ensure_network_schema(data)
    return {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }


def network_instances_by_id(data: dict) -> Dict[str, dict]:
    ensure_network_schema(data)
    return {
        _text(item.get("id")): item
        for item in data.get("network_asset_instances", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }


def network_instances_for_floor(data: dict, floor: int) -> Dict[str, dict]:
    ensure_network_schema(data)
    result: Dict[str, dict] = {}
    locations = {
        _text(item.get("name")): item
        for item in data.get("locations", [])
        if isinstance(item, dict) and _text(item.get("name"))
    }
    for instance_id, instance in network_instances_by_id(data).items():
        linked = locations.get(_text(instance.get("location_name")), {})
        resolved_floor = _as_int(instance.get("floor", linked.get("floor", 0)))
        if resolved_floor != int(floor):
            continue
        result[instance_id] = {
            **instance,
            "id": instance_id,
            "name": _text(instance.get("name")) or instance_id,
            "floor": resolved_floor,
            "x": _as_float(instance.get("x", linked.get("x", 0.0))),
            "y": _as_float(instance.get("y", linked.get("y", 0.0))),
            "kind": "network_asset_instance",
        }
    return result


def find_nearest_network_instance(
    data: dict,
    floor: int,
    x: float,
    y: float,
    radius_world: float,
) -> Optional[str]:
    best_id: Optional[str] = None
    best_distance = float(radius_world)
    for instance_id, instance in network_instances_for_floor(data, floor).items():
        dx = _as_float(instance.get("x")) - float(x)
        dy = _as_float(instance.get("y")) - float(y)
        distance = (dx * dx + dy * dy) ** 0.5
        if distance <= best_distance:
            best_id = instance_id
            best_distance = distance
    return best_id


def validate_network_data(data: dict, include_advisories: bool = True) -> List[str]:
    ensure_network_schema(data)
    messages: List[str] = []

    def validate_unique_id(key: str, label: str) -> set[str]:
        seen: set[str] = set()
        for index, item in enumerate(data.get(key, []), start=1):
            if not isinstance(item, dict):
                messages.append(f"{label} row {index} is not an object.")
                continue
            item_id = _text(item.get("id"))
            if not item_id:
                messages.append(f"{label} row {index} has no ID.")
            elif item_id in seen:
                messages.append(f"Duplicate {label.lower()} ID: {item_id}.")
            seen.add(item_id)
        return seen

    asset_ids = validate_unique_id("network_assets", "Network asset")
    instance_ids = validate_unique_id("network_asset_instances", "Network instance")
    connection_ids = validate_unique_id("network_connections", "Network connection")
    validate_unique_id("network_endpoint_assignments", "Network endpoint assignment")
    validate_unique_id("network_patch_leads", "Network patch lead")
    validate_unique_id("network_redundancy_groups", "Network redundancy group")
    vlan_record_ids = validate_unique_id("network_vlans", "VLAN record")
    validate_unique_id("network_routes", "Network route")

    locations = {
        _text(item.get("name")): item
        for item in data.get("locations", [])
        if isinstance(item, dict) and _text(item.get("name"))
    }

    for asset in data.get("network_assets", []):
        if not isinstance(asset, dict):
            continue
        asset_id = _text(asset.get("id")) or "(unnamed)"
        asset_type = _text(asset.get("asset_type"))
        if asset_type not in NETWORK_ASSET_TYPES:
            messages.append(
                f"Network asset {asset_id} has unsupported type {asset_type!r}."
            )
        if asset_type == "patch_panel" and _text(asset.get("patch_panel_type")) not in {
            "copper",
            "fibre",
        }:
            messages.append(f"Patch panel {asset_id} must specify copper or fibre.")
        if asset_type == "fibre_splitter" and not _text(asset.get("split_ratio")):
            messages.append(f"Fibre splitter {asset_id} requires a split ratio.")
        for row_index, row in enumerate(asset.get("port_definitions", []), start=1):
            if _text(row.get("port_type")) not in NETWORK_PORT_TYPES:
                messages.append(f"Network asset {asset_id} port row {row_index} has an unsupported port type.")
            if _text(row.get("port_use")) not in NETWORK_PORT_USES:
                messages.append(f"Network asset {asset_id} port row {row_index} has an unsupported port use.")
            if _as_int(row.get("port_count")) <= 0:
                messages.append(f"Network asset {asset_id} port row {row_index} must have a positive port count.")
        if asset_type == "wireless_access_point" and not asset.get("frequencies"):
            messages.append(
                f"Wireless access point {asset_id} requires at least one frequency."
            )

    for instance in data.get("network_asset_instances", []):
        if not isinstance(instance, dict):
            continue
        instance_id = _text(instance.get("id")) or "(unnamed)"
        asset_id = _text(instance.get("asset_id"))
        if asset_id not in asset_ids:
            messages.append(
                f"Network instance {instance_id} references missing asset {asset_id!r}."
            )
        location_name = _text(instance.get("location_name"))
        if location_name and location_name not in locations:
            messages.append(
                f"Network instance {instance_id} references missing location {location_name!r}."
            )

    def rack_units_for(instance: dict, asset: dict) -> int:
        asset_type = _text(asset.get("asset_type"))
        stack_members = (
            max(1, _as_int(instance.get("stack_member_count"), 1))
            if bool(instance.get("logical_stack"))
            else 1
        )
        if asset_type == "network_switch":
            allowance = max(
                0,
                _as_int(
                    asset.get("switch_rack_unit_allowance"),
                    _as_int(asset.get("rack_units"), 1),
                ),
            )
            return max(1, allowance) * stack_members
        return max(0, _as_int(asset.get("rack_units"), 1))

    rack_groups: Dict[tuple[int, str, str], List[tuple[str, int, int, str]]] = {}
    rack_sizes: Dict[tuple[int, str, str], int] = {}
    default_rack_size = max(
        1, _as_int(data.get("network_settings", {}).get("default_rack_size_u"), 42)
    )
    for instance in data.get("network_asset_instances", []):
        if not isinstance(instance, dict):
            continue
        rack = _text(instance.get("rack_name"))
        if not rack:
            continue
        key = (
            _as_int(instance.get("floor")),
            _text(instance.get("location_name")),
            rack,
        )
        instance_id = _text(instance.get("id")) or "(unnamed)"
        asset = assets_by_id.get(_text(instance.get("asset_id")), {})
        start_u = max(1, _as_int(instance.get("rack_start_u"), 1))
        used_u = rack_units_for(instance, asset)
        if used_u <= 0:
            continue
        end_u = start_u + used_u - 1
        rack_groups.setdefault(key, []).append(
            (instance_id, start_u, end_u, _text(instance.get("shared_rack_unit_group")))
        )
        explicit_size = _as_int(instance.get("rack_size_u"))
        if explicit_size > 0:
            rack_sizes[key] = max(rack_sizes.get(key, 0), explicit_size)

    for key, rows in rack_groups.items():
        capacity = rack_sizes.get(key, default_rack_size)
        floor, location, rack = key
        rack_label = f"{location or 'Floor ' + str(floor)} / {rack}"
        for instance_id, start_u, end_u, _shared_group in rows:
            if end_u > capacity:
                messages.append(
                    f"Rack {rack_label} capacity is {capacity}U but {instance_id} occupies U{start_u}-U{end_u}."
                )
        ordered = sorted(rows, key=lambda row: (row[1], row[2], row[0]))
        for index, (instance_id, start_u, end_u, shared_group) in enumerate(ordered):
            for other_id, other_start, other_end, other_shared_group in ordered[index + 1 :]:
                if other_start > end_u:
                    break
                if other_end >= start_u:
                    if shared_group and shared_group == other_shared_group:
                        continue
                    messages.append(
                        f"Rack {rack_label} has overlapping rack units: {instance_id} U{start_u}-U{end_u} and {other_id} U{other_start}-U{other_end}."
                    )

    used_endpoints: set[tuple[str, str]] = set()
    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict):
            continue
        connection_id = _text(connection.get("id")) or "(unnamed)"
        from_id = _text(connection.get("from_instance_id"))
        to_id = _text(connection.get("to_instance_id"))
        if from_id not in instance_ids:
            messages.append(
                f"Network connection {connection_id} references missing source instance {from_id!r}."
            )
        if to_id not in instance_ids:
            messages.append(
                f"Network connection {connection_id} references missing destination instance {to_id!r}."
            )
        if from_id and from_id == to_id:
            messages.append(
                f"Network connection {connection_id} connects an instance to itself."
            )
        for side, instance_id, port in (
            ("source", from_id, _text(connection.get("from_port"))),
            ("destination", to_id, _text(connection.get("to_port"))),
        ):
            if not port:
                messages.append(
                    f"Network connection {connection_id} has no {side} port."
                )
                continue
            endpoint = (instance_id, port)
            if instance_id and endpoint in used_endpoints:
                messages.append(
                    f"Network endpoint {instance_id}:{port} is used by more than one connection."
                )
            used_endpoints.add(endpoint)
        for vlan_id in connection.get("vlan_ids", []):
            if _text(vlan_id) not in vlan_record_ids:
                messages.append(
                    f"Network connection {connection_id} references missing VLAN record {_text(vlan_id)!r}."
                )

    endpoint_names = {
        _text(item.get("name"))
        for item in data.get("data_points", [])
        if isinstance(item, dict) and _text(item.get("name"))
    }
    assigned_endpoint_ports: set[tuple[str, int]] = set()
    port_loads: Dict[str, int] = {}
    poe_loads: Dict[str, float] = {}
    max_ont_copper = _as_float(
        data.get("network_settings", {}).get("polan_max_ont_copper_m"), 30.0
    )

    for assignment in data.get("network_endpoint_assignments", []):
        if not isinstance(assignment, dict):
            continue
        assignment_id = _text(assignment.get("id")) or "(unnamed)"
        endpoint_name = _text(assignment.get("endpoint_name"))
        endpoint_port = max(1, _as_int(assignment.get("endpoint_port"), 1))
        instance_id = _text(assignment.get("network_instance_id"))
        network_port = _text(assignment.get("network_port"))
        if endpoint_name not in endpoint_names:
            messages.append(
                f"Endpoint assignment {assignment_id} references missing data point {endpoint_name!r}."
            )
        physical_endpoint = (endpoint_name, endpoint_port)
        if physical_endpoint in assigned_endpoint_ports:
            messages.append(
                f"Data-point port {endpoint_name}:{endpoint_port} is assigned more than once."
            )
        assigned_endpoint_ports.add(physical_endpoint)
        if instance_id not in instance_ids:
            messages.append(
                f"Endpoint assignment {assignment_id} references missing network instance {instance_id!r}."
            )
        if not network_port:
            messages.append(f"Endpoint assignment {assignment_id} has no network port.")
        elif instance_id:
            network_endpoint = (instance_id, network_port)
            if network_endpoint in used_endpoints:
                messages.append(
                    f"Network endpoint {instance_id}:{network_port} is used more than once."
                )
            used_endpoints.add(network_endpoint)
        port_loads[instance_id] = port_loads.get(instance_id, 0) + 1
        poe_loads[instance_id] = poe_loads.get(instance_id, 0.0) + max(
            0.0, _as_float(assignment.get("poe_power_w"))
        )
        if _text(assignment.get("technology")) == "PoLAN":
            copper = max(0.0, _as_float(assignment.get("copper_length_m")))
            extension = 0.0
            if copper > max_ont_copper + extension + 0.001:
                messages.append(
                    f"PoLAN endpoint assignment {assignment_id} has {copper:.1f} m of copper; "
                    f"the configured ONT limit is {max_ont_copper:.1f} m."
                )

    for instance_id, used_ports in port_loads.items():
        instance = instances_by_id.get(instance_id, {})
        asset = assets_by_id.get(_text(instance.get("asset_id")), {})
        stack_members = (
            max(1, _as_int(instance.get("stack_member_count"), 1))
            if bool(instance.get("logical_stack"))
            else 1
        )
        capacity = max(0, _as_int(asset.get("number_of_ports"))) * stack_members
        if capacity and used_ports > capacity:
            messages.append(
                f"Network instance {instance_id} uses {used_ports} endpoint ports but provides only {capacity}."
            )
        poe_budget = max(0.0, _as_float(asset.get("poe_budget_w"))) * stack_members
        poe_load = poe_loads.get(instance_id, 0.0)
        if poe_load > poe_budget + 0.001:
            messages.append(
                f"Network instance {instance_id} has {poe_load:.1f} W PoE load but only {poe_budget:.1f} W budget."
            )

    for group in data.get("network_redundancy_groups", []):
        if not isinstance(group, dict):
            continue
        group_id = _text(group.get("id")) or "(unnamed)"
        primary = _text(group.get("primary_olt_instance_id"))
        secondary = _text(group.get("secondary_olt_instance_id"))
        protected = _text(group.get("protected_instance_id"))
        if protected not in instance_ids:
            messages.append(
                f"Redundancy group {group_id} references missing protected instance {protected!r}."
            )
        if primary not in instance_ids or secondary not in instance_ids:
            messages.append(
                f"Redundancy group {group_id} must reference both primary and secondary OLT instances."
            )
        elif primary == secondary:
            messages.append(
                f"Redundancy group {group_id} uses the same OLT for primary and secondary service."
            )

    vlan_numbers: set[int] = set()
    for vlan in data.get("network_vlans", []):
        if not isinstance(vlan, dict):
            continue
        number = _as_int(vlan.get("vlan_id"))
        if number < 1 or number > 4094:
            messages.append(
                f"VLAN {_text(vlan.get('id')) or '(unnamed)'} must use an ID between 1 and 4094."
            )
        elif number in vlan_numbers:
            messages.append(f"VLAN number {number} is duplicated.")
        vlan_numbers.add(number)

    if include_advisories:
        mer_locations = [
            item
            for item in data.get("locations", [])
            if isinstance(item, dict) and _text(item.get("kind")).lower() == "mer"
        ]
        expected = _as_int(
            data.get("network_settings", {}).get("expected_mer_count"), 2
        )
        if not mer_locations:
            messages.append(
                "Advisory: no MER location has been created; the network tree has no root."
            )
        elif len(mer_locations) < expected:
            messages.append(
                f"Advisory: {len(mer_locations)} MER location(s) exist; the design expects {expected}."
            )

        technology = _text(data.get("network_settings", {}).get("technology"))
        assets = network_assets_by_id(data)
        if technology == "PoLAN":
            has_olt = any(
                _text(assets.get(_text(instance.get("asset_id")), {}).get("asset_type"))
                == "optical_line_terminal"
                for instance in data.get("network_asset_instances", [])
                if isinstance(instance, dict)
            )
            if not has_olt:
                messages.append(
                    "Advisory: a PoLAN design normally requires at least one OLT."
                )

    return messages


def install_json_store_extensions(json_store_class) -> None:
    """Install schema-aware helpers onto the application's JsonStore class."""

    if getattr(json_store_class, "_network_schema_installed", False):
        return

    original_init = json_store_class.__init__
    original_load = getattr(json_store_class, "_load_from_payload", None)
    original_save = getattr(json_store_class, "save", None)
    original_validate = getattr(json_store_class, "validate", None)

    def init_wrapper(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        ensure_network_schema(self.data)

    json_store_class.__init__ = init_wrapper

    if original_load is not None:

        def load_wrapper(self, payload):
            result = original_load(self, payload)
            ensure_network_schema(self.data)
            return result

        json_store_class._load_from_payload = load_wrapper

    if original_save is not None:

        def save_wrapper(self, path):
            ensure_network_schema(self.data)
            return original_save(self, path)

        json_store_class.save = save_wrapper

    if original_validate is not None:

        def validate_wrapper(self, *args, **kwargs):
            result = list(original_validate(self, *args, **kwargs) or [])
            result.extend(validate_network_data(self.data, include_advisories=False))
            return result

        json_store_class.validate = validate_wrapper

    def assets_by_id(self):
        return network_assets_by_id(self.data)

    def instances_by_id(self):
        return network_instances_by_id(self.data)

    def instances_for_floor(self, floor):
        return network_instances_for_floor(self.data, floor)

    def suggest_network_asset_id(self):
        return next_network_id(self.data.get("network_assets", []), "NA")

    def suggest_network_instance_id(self):
        return next_network_id(self.data.get("network_asset_instances", []), "NI")

    def suggest_network_connection_id(self):
        return next_network_id(self.data.get("network_connections", []), "NC")

    json_store_class.network_assets_by_id = assets_by_id
    json_store_class.network_instances_by_id = instances_by_id
    json_store_class.network_instances_for_floor = instances_for_floor
    json_store_class.suggest_network_asset_id = suggest_network_asset_id
    json_store_class.suggest_network_instance_id = suggest_network_instance_id
    json_store_class.suggest_network_connection_id = suggest_network_connection_id
    json_store_class._network_schema_installed = True
