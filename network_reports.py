from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from network_services import cable_core_statistics, splice_arrangement_rows


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


def _write_csv(path: Path, fieldnames: List[str], rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def _maps(data: dict) -> Tuple[Dict[str, dict], Dict[str, dict], Dict[str, dict]]:
    assets = {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if _text(item.get("id"))
    }
    instances = {
        _text(item.get("id")): item
        for item in data.get("network_asset_instances", [])
        if _text(item.get("id"))
    }
    locations = {
        _text(item.get("name")): item
        for item in data.get("locations", [])
        if _text(item.get("name"))
    }
    return assets, instances, locations


def _instance_description(instance: dict, assets: Dict[str, dict], locations: Dict[str, dict]) -> dict:
    asset = assets.get(_text(instance.get("asset_id")), {})
    location_name = _text(instance.get("location_name"))
    location = locations.get(location_name, {})
    stack_members = max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1
    rack_units = _int(asset.get("rack_units"))
    if _text(asset.get("asset_type")) == "network_switch":
        rack_units = max(1, _int(asset.get("switch_rack_unit_allowance"), rack_units or 1)) * stack_members
    return {
        "instance_id": _text(instance.get("id")),
        "instance_name": _text(instance.get("name")) or _text(instance.get("id")),
        "asset_id": _text(instance.get("asset_id")),
        "asset_name": _text(asset.get("name")),
        "asset_type": _text(asset.get("asset_type")),
        "manufacturer": _text(asset.get("manufacturer")),
        "model": _text(asset.get("model")),
        "location_name": location_name,
        "location_type": _text(location.get("kind")),
        "floor": instance.get("floor", location.get("floor", "")),
        "rack_name": _text(instance.get("rack_name")),
        "rack_start_u": instance.get("rack_start_u", ""),
        "rack_units": rack_units,
        "shared_rack_unit_group": _text(instance.get("shared_rack_unit_group")),
        "shared_rack_unit_position": instance.get("shared_rack_unit_position", ""),
        "shared_rack_unit_capacity": instance.get("shared_rack_unit_capacity", ""),
        "logical_stack": "Yes" if bool(instance.get("logical_stack")) else "No",
        "stack_member_count": stack_members,
        "management_ip": _text(instance.get("management_ip")),
        "management_vlan": _text(instance.get("management_vlan")),
    }


def _effective_ports(instance: dict, asset: dict) -> int:
    stack_members = max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1
    return max(0, _int(asset.get("number_of_ports"))) * stack_members


def _effective_poe_budget(instance: dict, asset: dict) -> float:
    stack_members = max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1
    return max(0.0, _float(asset.get("poe_budget_w"))) * stack_members


def _switch_schedule(data: dict) -> List[dict]:
    assets, instances, locations = _maps(data)
    endpoint_assignments: Dict[str, List[dict]] = {}
    for assignment in data.get("network_endpoint_assignments", []):
        instance_id = _text(assignment.get("network_instance_id"))
        if instance_id:
            endpoint_assignments.setdefault(instance_id, []).append(assignment)

    rows = []
    switch_types = {"network_switch", "optical_line_terminal"}
    technology = _text(data.get("network_settings", {}).get("technology"))
    for instance in instances.values():
        asset = assets.get(_text(instance.get("asset_id")), {})
        if _text(asset.get("asset_type")) not in switch_types:
            continue
        instance_id = _text(instance.get("id"))
        assignments = endpoint_assignments.get(instance_id, [])
        assigned_ports = len(assignments)
        poe_load = sum(_float(item.get("poe_power_w")) for item in assignments)
        number_of_ports = _effective_ports(instance, asset)
        poe_budget = _effective_poe_budget(instance, asset)
        row = _instance_description(instance, assets, locations)
        row.update(
            {
                "network_technology": technology,
                "design_role": _text(instance.get("design_role")),
                "number_of_ports": number_of_ports,
                "assigned_endpoint_ports": assigned_ports,
                "available_endpoint_ports": max(0, number_of_ports - assigned_ports),
                "port_utilisation_percent": round(100.0 * assigned_ports / number_of_ports, 3) if number_of_ports else 0.0,
                "connections_in": _int(asset.get("connections_in")),
                "connections_out": _int(asset.get("connections_out")),
                "uplink_ports": _int(asset.get("uplink_ports")),
                "input_connection_type": _text(asset.get("input_connection_type")),
                "output_connection_type": _text(asset.get("output_connection_type")),
                "uplink_connection_type": _text(asset.get("uplink_connection_type")),
                "poe_budget_w": poe_budget,
                "poe_load_w": round(poe_load, 3),
                "poe_headroom_w": round(max(0.0, poe_budget - poe_load), 3),
                "poe_utilisation_percent": round(100.0 * poe_load / poe_budget, 3) if poe_budget else 0.0,
                "power_input_w": _float(asset.get("power_input_w")),
            }
        )
        rows.append(row)
    return sorted(rows, key=lambda row: (str(row["floor"]), row["location_name"], row["instance_id"]))


def _natural_port_key(value: str):
    text = _text(value)
    if text.isdigit():
        return (0, int(text), "")
    prefix = "".join(ch for ch in text if not ch.isdigit())
    digits = "".join(ch for ch in text if ch.isdigit())
    return (1, int(digits) if digits else 0, prefix.lower())


def _port_schedule(data: dict) -> List[dict]:
    assets, instances, locations = _maps(data)
    connections = data.get("network_connections", [])
    endpoint_map: Dict[Tuple[str, str], List[dict]] = {}
    for connection in connections:
        for side in ("from", "to"):
            instance_id = _text(connection.get(f"{side}_instance_id"))
            port = _text(connection.get(f"{side}_port"))
            if instance_id and port:
                endpoint_map.setdefault((instance_id, port), []).append(connection)

    assignment_map: Dict[Tuple[str, str], List[dict]] = {}
    for assignment in data.get("network_endpoint_assignments", []):
        instance_id = _text(assignment.get("network_instance_id"))
        port = _text(assignment.get("network_port"))
        if instance_id and port:
            assignment_map.setdefault((instance_id, port), []).append(assignment)

    rows = []
    for instance in instances.values():
        instance_id = _text(instance.get("id"))
        asset = assets.get(_text(instance.get("asset_id")), {})
        per_member_ports = max(0, _int(asset.get("number_of_ports")))
        stack_members = max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1
        base = _instance_description(instance, assets, locations)
        if bool(instance.get("logical_stack")) and stack_members > 1:
            port_names = {
                f"{member}/{port}"
                for member in range(1, stack_members + 1)
                for port in range(1, per_member_ports + 1)
            }
        else:
            port_names = {str(number) for number in range(1, per_member_ports + 1)}
        port_names.update(port for (owner, port) in endpoint_map if owner == instance_id)
        port_names.update(port for (owner, port) in assignment_map if owner == instance_id)

        for port_name in sorted(port_names, key=_natural_port_key):
            linked = endpoint_map.get((instance_id, port_name), [])
            assignments = assignment_map.get((instance_id, port_name), [])
            if not linked and not assignments:
                rows.append(
                    {
                        **base,
                        "port": port_name,
                        "status": "Available",
                        "connection_id": "",
                        "connection_role": "",
                        "medium": "",
                        "connected_to_instance": "",
                        "connected_to_port": "",
                        "endpoint_name": "",
                        "endpoint_port": "",
                        "endpoint_asset_name": "",
                        "department_id": "",
                        "department_name": "",
                        "poe_power_w": 0.0,
                        "copper_length_m": 0.0,
                        "vlan_ids": "",
                        "cable_specification": "",
                    }
                )
                continue

            for assignment in assignments:
                rows.append(
                    {
                        **base,
                        "port": port_name,
                        "status": "Endpoint",
                        "connection_id": _text(assignment.get("id")),
                        "connection_role": "output",
                        "medium": "copper",
                        "connected_to_instance": _text(assignment.get("endpoint_name")),
                        "connected_to_port": _text(assignment.get("endpoint_port")),
                        "endpoint_name": _text(assignment.get("endpoint_name")),
                        "endpoint_port": _text(assignment.get("endpoint_port")),
                        "endpoint_asset_name": _text(assignment.get("endpoint_asset_name")),
                        "department_id": _text(assignment.get("department_id")),
                        "department_name": _text(assignment.get("department_name")),
                        "poe_power_w": _float(assignment.get("poe_power_w")),
                        "copper_length_m": _float(assignment.get("copper_length_m")),
                        "vlan_ids": ", ".join(str(x) for x in assignment.get("vlan_ids", []) if _text(x)),
                        "cable_specification": "Category 6A",
                    }
                )

            for connection in linked:
                is_from = _text(connection.get("from_instance_id")) == instance_id
                other_side = "to" if is_from else "from"
                rows.append(
                    {
                        **base,
                        "port": port_name,
                        "status": "Infrastructure",
                        "connection_id": _text(connection.get("id")),
                        "connection_role": _text(connection.get("connection_role")),
                        "medium": _text(connection.get("medium")),
                        "connected_to_instance": _text(connection.get(f"{other_side}_instance_id")),
                        "connected_to_port": _text(connection.get(f"{other_side}_port")),
                        "endpoint_name": "",
                        "endpoint_port": "",
                        "endpoint_asset_name": "",
                        "department_id": "",
                        "department_name": "",
                        "poe_power_w": 0.0,
                        "copper_length_m": _float(connection.get("length_m")),
                        "vlan_ids": ", ".join(str(x) for x in connection.get("vlan_ids", []) if _text(x)),
                        "cable_specification": _text(connection.get("cable_specification")),
                    }
                )
    return sorted(rows, key=lambda row: (str(row["floor"]), row["instance_id"], _natural_port_key(row["port"]), row["status"]))


def _patching_schedule(data: dict, medium: str) -> List[dict]:
    assets, instances, locations = _maps(data)
    rows = []
    for connection in data.get("network_connections", []):
        if _text(connection.get("medium")).lower() != medium.lower():
            continue
        from_instance = instances.get(_text(connection.get("from_instance_id")), {})
        to_instance = instances.get(_text(connection.get("to_instance_id")), {})
        from_desc = _instance_description(from_instance, assets, locations) if from_instance else {}
        to_desc = _instance_description(to_instance, assets, locations) if to_instance else {}
        rows.append(
            {
                "connection_id": _text(connection.get("id")),
                "connection_role": _text(connection.get("connection_role")),
                "medium": _text(connection.get("medium")),
                "from_instance_id": _text(connection.get("from_instance_id")),
                "from_instance_name": from_desc.get("instance_name", ""),
                "from_location": from_desc.get("location_name", ""),
                "from_port": _text(connection.get("from_port")),
                "to_instance_id": _text(connection.get("to_instance_id")),
                "to_instance_name": to_desc.get("instance_name", ""),
                "to_location": to_desc.get("location_name", ""),
                "to_port": _text(connection.get("to_port")),
                "endpoint_name": "",
                "endpoint_port": "",
                "endpoint_asset_name": "",
                "department_id": "",
                "department_name": "",
                "poe_power_w": 0.0,
                "length_m": _float(connection.get("length_m")),
                "cable_specification": _text(connection.get("cable_specification")),
                "fibre_count": _int(connection.get("fibre_count")),
                "vlan_ids": ", ".join(str(x) for x in connection.get("vlan_ids", []) if _text(x)),
                "route_profile": _text(connection.get("route_profile")),
                "route_path": " -> ".join(str(x) for x in connection.get("route_path", []) if _text(x)),
                "redundancy_role": _text(connection.get("redundancy_role")),
                "protection_group": _text(connection.get("protection_group")),
                "notes": _text(connection.get("notes")),
            }
        )

    if medium.lower() == "copper":
        for assignment in data.get("network_endpoint_assignments", []):
            instance = instances.get(_text(assignment.get("network_instance_id")), {})
            desc = _instance_description(instance, assets, locations) if instance else {}
            rows.append(
                {
                    "connection_id": _text(assignment.get("id")),
                    "connection_role": "output",
                    "medium": "copper",
                    "from_instance_id": _text(assignment.get("network_instance_id")),
                    "from_instance_name": desc.get("instance_name", ""),
                    "from_location": desc.get("location_name", ""),
                    "from_port": _text(assignment.get("network_port")),
                    "to_instance_id": "",
                    "to_instance_name": "",
                    "to_location": _text(assignment.get("endpoint_name")),
                    "to_port": _text(assignment.get("endpoint_port")),
                    "endpoint_name": _text(assignment.get("endpoint_name")),
                    "endpoint_port": _text(assignment.get("endpoint_port")),
                    "endpoint_asset_name": _text(assignment.get("endpoint_asset_name")),
                    "department_id": _text(assignment.get("department_id")),
                    "department_name": _text(assignment.get("department_name")),
                    "poe_power_w": _float(assignment.get("poe_power_w")),
                    "length_m": _float(assignment.get("copper_length_m")),
                    "cable_specification": "Category 6A",
                    "fibre_count": 0,
                    "vlan_ids": ", ".join(str(x) for x in assignment.get("vlan_ids", []) if _text(x)),
                    "route_profile": "",
                    "route_path": " -> ".join(str(x) for x in assignment.get("route_path", []) if _text(x)),
                    "redundancy_role": "",
                    "protection_group": "",
                    "notes": "Automatically assigned endpoint port" if assignment.get("auto_generated") else "",
                }
            )
    return sorted(rows, key=lambda row: row["connection_id"])


def _vlan_schedule(data: dict) -> List[dict]:
    rows = []
    for vlan in data.get("network_vlans", []):
        rows.append(
            {
                "id": _text(vlan.get("id")),
                "vlan_id": _int(vlan.get("vlan_id")),
                "name": _text(vlan.get("name")),
                "purpose": _text(vlan.get("purpose")),
                "requested_hosts": _int(vlan.get("requested_hosts"), 254),
                "subnet": _text(vlan.get("subnet")),
                "subnet_mask": _text(vlan.get("subnet_mask")),
                "prefix_length": _int(vlan.get("prefix_length")),
                "gateway": _text(vlan.get("gateway")),
                "dhcp_scope": _text(vlan.get("dhcp_scope")),
                "security_zone": _text(vlan.get("security_zone")),
                "notes": _text(vlan.get("notes")),
            }
        )
    return sorted(rows, key=lambda row: (row["vlan_id"], row["name"]))


def _routing_schedule(data: dict) -> List[dict]:
    rows = []
    for route in data.get("network_routes", []):
        rows.append(
            {
                "id": _text(route.get("id")),
                "source": _text(route.get("source")),
                "destination": _text(route.get("destination")),
                "vlan_id": _text(route.get("vlan_id")),
                "protocol": _text(route.get("protocol")),
                "next_hop": _text(route.get("next_hop")),
                "metric": _int(route.get("metric")),
                "firewall_policy": _text(route.get("firewall_policy")),
                "notes": _text(route.get("notes")),
            }
        )
    return sorted(rows, key=lambda row: row["id"])


def _splitter_schedule(data: dict) -> List[dict]:
    assets, instances, locations = _maps(data)
    rows = []
    for instance in instances.values():
        asset = assets.get(_text(instance.get("asset_id")), {})
        if _text(asset.get("asset_type")) != "fibre_splitter":
            continue
        instance_id = _text(instance.get("id"))
        incoming = [
            connection
            for connection in data.get("network_connections", [])
            if _text(connection.get("to_instance_id")) == instance_id
        ]
        outbound = sum(1 for c in data.get("network_connections", []) if _text(c.get("from_instance_id")) == instance_id)
        primary = next((item for item in incoming if _text(item.get("redundancy_role")) == "primary"), {})
        secondary = next((item for item in incoming if _text(item.get("redundancy_role")) == "secondary"), {})
        row = _instance_description(instance, assets, locations)
        row.update(
            {
                "design_role": _text(instance.get("design_role")),
                "split_ratio": _text(asset.get("split_ratio")),
                "input_connection_type": _text(asset.get("input_connection_type")),
                "output_connection_type": _text(asset.get("output_connection_type")),
                "configured_inputs": _int(asset.get("connections_in")),
                "configured_outputs": _int(asset.get("connections_out")),
                "connected_inputs": len(incoming),
                "connected_outputs": outbound,
                "available_outputs": max(0, _int(asset.get("connections_out")) - outbound),
                "primary_olt_instance": _text(primary.get("from_instance_id")),
                "primary_olt_port": _text(primary.get("from_port")),
                "secondary_olt_instance": _text(secondary.get("from_instance_id")),
                "secondary_olt_port": _text(secondary.get("from_port")),
                "protection_group": _text(primary.get("protection_group") or secondary.get("protection_group")),
                "failover_configured": "Yes" if primary and secondary else "No",
            }
        )
        rows.append(row)
    return sorted(rows, key=lambda row: (str(row["floor"]), row["location_name"], row["instance_id"]))


def _power_schedule(data: dict) -> List[dict]:
    assets, instances, locations = _maps(data)
    endpoint_loads: Dict[str, float] = {}
    endpoint_ports: Dict[str, int] = {}
    for assignment in data.get("network_endpoint_assignments", []):
        instance_id = _text(assignment.get("network_instance_id"))
        endpoint_loads[instance_id] = endpoint_loads.get(instance_id, 0.0) + _float(assignment.get("poe_power_w"))
        endpoint_ports[instance_id] = endpoint_ports.get(instance_id, 0) + 1

    rows = []
    total_input = 0.0
    total_poe_budget = 0.0
    total_poe_load = 0.0
    for instance in instances.values():
        instance_id = _text(instance.get("id"))
        asset = assets.get(_text(instance.get("asset_id")), {})
        power_input = _float(asset.get("power_input_w"))
        stack_members = max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1
        effective_power_input = power_input * stack_members
        poe_budget = _effective_poe_budget(instance, asset)
        poe_load = endpoint_loads.get(instance_id, 0.0)
        total_input += effective_power_input
        total_poe_budget += poe_budget
        total_poe_load += poe_load
        row = _instance_description(instance, assets, locations)
        row.update(
            {
                "design_role": _text(instance.get("design_role")),
                "power_input_w": effective_power_input,
                "poe_budget_w": poe_budget,
                "poe_load_w": round(poe_load, 3),
                "poe_headroom_w": round(max(0.0, poe_budget - poe_load), 3),
                "poe_utilisation_percent": round(100.0 * poe_load / poe_budget, 3) if poe_budget else 0.0,
                "endpoint_ports_served": endpoint_ports.get(instance_id, 0),
                "power_feed": _text(instance.get("power_feed")),
                "ups_source": _text(instance.get("ups_source")),
                "notes": _text(instance.get("notes")),
            }
        )
        rows.append(row)
    rows.sort(key=lambda row: (str(row["floor"]), row["location_name"], row["instance_id"]))
    rows.append(
        {
            "instance_id": "TOTAL",
            "instance_name": "Network design totals",
            "power_input_w": round(total_input, 3),
            "poe_budget_w": round(total_poe_budget, 3),
            "poe_load_w": round(total_poe_load, 3),
            "poe_headroom_w": round(max(0.0, total_poe_budget - total_poe_load), 3),
            "poe_utilisation_percent": round(100.0 * total_poe_load / total_poe_budget, 3) if total_poe_budget else 0.0,
        }
    )
    return rows



def _fibre_cable_schedule(data: dict) -> List[dict]:
    rows = []
    for cable in data.get("network_fibre_cables", []):
        if not isinstance(cable, dict):
            continue
        stats = cable_core_statistics(cable)
        rows.append({
            "cable_id": _text(cable.get("id")),
            "name": _text(cable.get("name")),
            "cable_type": _text(cable.get("cable_type")),
            "from_instance_id": _text(cable.get("from_instance_id")),
            "from_port": _text(cable.get("from_port")),
            "to_instance_id": _text(cable.get("to_instance_id")),
            "to_port": _text(cable.get("to_port")),
            "from_location": _text(cable.get("from_location")),
            "to_location": _text(cable.get("to_location")),
            "length_m": _float(cable.get("length_m")),
            "slack_length_m": _float(cable.get("slack_length_m")),
            "core_count": stats["total"],
            "used_cores": stats["used"],
            "allocated_cores": stats["allocated"],
            "spliced_cores": stats["spliced"],
            "reserved_cores": stats["reserved"],
            "dark_fibre_count": stats["dark"],
            "fault_cores": stats["fault"],
            "route_path": " -> ".join(_text(v) for v in cable.get("route_path", []) if _text(v)),
            "logical_connection_ids": ", ".join(_text(v) for v in cable.get("logical_connection_ids", []) if _text(v)),
            "splice_ids": ", ".join(_text(v) for v in cable.get("splice_ids", []) if _text(v)),
            "installation_status": _text(cable.get("installation_status")),
            "drawing_layer": _text(cable.get("drawing_layer")),
            "sheath_colour": _text(cable.get("sheath_colour")),
            "owner": _text(cable.get("owner")),
            "notes": _text(cable.get("notes")),
        })
    return sorted(rows, key=lambda row: row["cable_id"])


def _fibre_core_schedule(data: dict) -> List[dict]:
    rows = []
    for cable in data.get("network_fibre_cables", []):
        if not isinstance(cable, dict):
            continue
        for core in cable.get("cores", []):
            if not isinstance(core, dict):
                continue
            rows.append({
                "cable_id": _text(cable.get("id")),
                "cable_name": _text(cable.get("name")),
                "core_number": _int(core.get("number")),
                "core_colour": _text(core.get("colour")),
                "tube_number": _int(core.get("tube_number")),
                "tube_colour": _text(core.get("tube_colour")),
                "status": _text(core.get("status")) or "dark",
                "circuit_id": _text(core.get("circuit_id")),
                "from_termination": _text(core.get("from_termination")),
                "to_termination": _text(core.get("to_termination")),
                "notes": _text(core.get("notes")),
            })
    return sorted(rows, key=lambda row: (row["cable_id"], row["core_number"]))


def _fibre_node_schedule(data: dict) -> List[dict]:
    splice_counts: Dict[str, int] = {}
    for splice in data.get("network_fibre_splices", []):
        node_id = _text(splice.get("node_id"))
        if node_id:
            splice_counts[node_id] = splice_counts.get(node_id, 0) + 1
    rows = []
    for node in data.get("network_fibre_nodes", []):
        if not isinstance(node, dict):
            continue
        rows.append({
            "node_id": _text(node.get("id")),
            "name": _text(node.get("name")),
            "node_type": _text(node.get("node_type")),
            "location_name": _text(node.get("location_name")),
            "floor": _int(node.get("floor")),
            "x": _float(node.get("x")),
            "y": _float(node.get("y")),
            "rack_name": _text(node.get("rack_name")),
            "rack_start_u": _int(node.get("rack_start_u")),
            "rack_units": _int(node.get("rack_units")),
            "parent_node_id": _text(node.get("parent_node_id")),
            "linked_instance_id": _text(node.get("linked_instance_id")),
            "splice_capacity": _int(node.get("splice_capacity", node.get("cassette_capacity"))),
            "configured_splices": splice_counts.get(_text(node.get("id")), 0),
            "available_splice_capacity": max(0, _int(node.get("splice_capacity", node.get("cassette_capacity"))) - splice_counts.get(_text(node.get("id")), 0)),
            "drawing_layer": _text(node.get("drawing_layer")),
            "symbol": _text(node.get("symbol")),
            "notes": _text(node.get("notes")),
        })
    return sorted(rows, key=lambda row: (row["floor"], row["location_name"], row["node_id"]))


def _fibre_splice_schedule(data: dict) -> List[dict]:
    return splice_arrangement_rows(data)


def _dark_fibre_summary(data: dict) -> List[dict]:
    rows = []
    total_cores = total_dark = total_used = 0
    for cable in data.get("network_fibre_cables", []):
        if not isinstance(cable, dict):
            continue
        stats = cable_core_statistics(cable)
        total_cores += stats["total"]
        total_dark += stats["dark"]
        total_used += stats["used"]
        rows.append({
            "cable_id": _text(cable.get("id")),
            "name": _text(cable.get("name")),
            "core_count": stats["total"],
            "used_cores": stats["used"],
            "dark_fibre_count": stats["dark"],
            "dark_fibre_percent": round(100.0 * stats["dark"] / stats["total"], 3) if stats["total"] else 0.0,
            "route_path": " -> ".join(_text(v) for v in cable.get("route_path", []) if _text(v)),
        })
    rows.append({
        "cable_id": "TOTAL", "name": "Physical fibre totals", "core_count": total_cores,
        "used_cores": total_used, "dark_fibre_count": total_dark,
        "dark_fibre_percent": round(100.0 * total_dark / total_cores, 3) if total_cores else 0.0,
    })
    return rows


def _external_network_schedule(data: dict) -> List[dict]:
    return sorted([
        {
            "id": _text(item.get("id")), "name": _text(item.get("name")),
            "network_type": _text(item.get("network_type")), "provider": _text(item.get("provider")),
            "asn": _text(item.get("asn")), "location_name": _text(item.get("location_name")),
            "demarcation_instance_id": _text(item.get("demarcation_instance_id")),
            "prefixes": ", ".join(_text(v) for v in item.get("prefixes", []) if _text(v)),
            "peer_instance_ids": ", ".join(_text(v) for v in item.get("peer_instance_ids", []) if _text(v)),
            "notes": _text(item.get("notes")),
        }
        for item in data.get("network_external_networks", []) if isinstance(item, dict)
    ], key=lambda row: row["id"])


def _ip_address_schedule(data: dict) -> List[dict]:
    return sorted([
        {
            "id": _text(item.get("id")), "instance_id": _text(item.get("instance_id")),
            "vlan_id": _text(item.get("vlan_id")), "address": _text(item.get("address")),
            "prefix_length": _int(item.get("prefix_length")), "gateway": _text(item.get("gateway")),
            "purpose": _text(item.get("purpose")), "notes": _text(item.get("notes")),
        }
        for item in data.get("network_ip_allocations", []) if isinstance(item, dict)
    ], key=lambda row: (row["vlan_id"], row["address"], row["instance_id"]))

def _configuration_summary(data: dict) -> List[dict]:
    summary = data.get("network_design_summary", {})
    if not isinstance(summary, dict):
        return []
    rows = []
    for key, value in summary.items():
        if key == "component_counts" and isinstance(value, dict):
            for role, quantity in value.items():
                rows.append({"section": "component_counts", "item": role, "value": quantity})
        elif key == "warnings" and isinstance(value, list):
            for index, warning in enumerate(value, start=1):
                rows.append({"section": "warnings", "item": f"warning_{index}", "value": warning})
        else:
            rows.append({"section": "summary", "item": key, "value": value})
    return rows


def write_network_schedules(data: dict, output_directory: Path, prefix: str = "network") -> List[Path]:
    output_directory = Path(output_directory)
    safe_prefix = _text(prefix) or "network"

    schedules = [
        (
            "switch_schedule",
            [
                "network_technology", "design_role", "instance_id", "instance_name", "asset_id", "asset_name",
                "asset_type", "manufacturer", "model", "location_name", "location_type", "floor",
                "rack_name", "rack_start_u", "rack_units", "logical_stack", "stack_member_count", "management_ip", "management_vlan",
                "number_of_ports", "assigned_endpoint_ports", "available_endpoint_ports", "port_utilisation_percent",
                "connections_in", "connections_out", "uplink_ports",
                "input_connection_type", "output_connection_type", "uplink_connection_type",
                "power_input_w", "poe_budget_w", "poe_load_w", "poe_headroom_w", "poe_utilisation_percent",
            ],
            _switch_schedule(data),
        ),
        (
            "port_schedule",
            [
                "instance_id", "instance_name", "asset_id", "asset_name", "asset_type", "location_name",
                "floor", "logical_stack", "stack_member_count", "port", "status", "connection_id", "connection_role", "medium",
                "connected_to_instance", "connected_to_port", "endpoint_name", "endpoint_port",
                "endpoint_asset_name", "department_id", "department_name", "poe_power_w", "copper_length_m",
                "vlan_ids", "cable_specification",
            ],
            _port_schedule(data),
        ),
        (
            "copper_patching_schedule",
            [
                "connection_id", "connection_role", "medium", "from_instance_id", "from_instance_name",
                "from_location", "from_port", "to_instance_id", "to_instance_name", "to_location",
                "to_port", "endpoint_name", "endpoint_port", "endpoint_asset_name", "department_id",
                "department_name", "poe_power_w", "length_m", "cable_specification", "fibre_count",
                "vlan_ids", "route_profile", "route_path", "redundancy_role", "protection_group", "notes",
            ],
            _patching_schedule(data, "copper"),
        ),
        (
            "fibre_patching_schedule",
            [
                "connection_id", "connection_role", "medium", "from_instance_id", "from_instance_name",
                "from_location", "from_port", "to_instance_id", "to_instance_name", "to_location",
                "to_port", "endpoint_name", "endpoint_port", "endpoint_asset_name", "department_id",
                "department_name", "poe_power_w", "length_m", "cable_specification", "fibre_count",
                "vlan_ids", "route_profile", "route_path", "redundancy_role", "protection_group", "notes",
            ],
            _patching_schedule(data, "fibre"),
        ),
        (
            "vlan_schedule",
            ["id", "vlan_id", "name", "purpose", "requested_hosts", "subnet", "subnet_mask", "prefix_length", "gateway", "dhcp_scope", "security_zone", "notes"],
            _vlan_schedule(data),
        ),
        (
            "routing_schedule",
            ["id", "source", "destination", "vlan_id", "protocol", "next_hop", "metric", "firewall_policy", "notes"],
            _routing_schedule(data),
        ),
        (
            "splitter_schedule",
            [
                "instance_id", "instance_name", "asset_id", "asset_name", "manufacturer", "model",
                "location_name", "location_type", "floor", "rack_name", "rack_start_u", "rack_units",
                "design_role", "split_ratio", "input_connection_type", "output_connection_type", "configured_inputs",
                "configured_outputs", "connected_inputs", "connected_outputs", "available_outputs",
                "primary_olt_instance", "primary_olt_port", "secondary_olt_instance", "secondary_olt_port",
                "protection_group", "failover_configured",
            ],
            _splitter_schedule(data),
        ),
        (
            "power_schedule",
            [
                "instance_id", "instance_name", "asset_id", "asset_name", "asset_type", "manufacturer", "model",
                "location_name", "location_type", "floor", "rack_name", "rack_start_u", "rack_units",
                "logical_stack", "stack_member_count", "design_role", "power_input_w", "poe_budget_w", "poe_load_w", "poe_headroom_w",
                "poe_utilisation_percent", "endpoint_ports_served", "power_feed", "ups_source", "notes",
            ],
            _power_schedule(data),
        ),
        (
            "physical_fibre_cable_schedule",
            ["cable_id", "name", "cable_type", "from_instance_id", "from_port", "to_instance_id", "to_port", "from_location", "to_location", "length_m", "slack_length_m", "core_count", "used_cores", "allocated_cores", "spliced_cores", "reserved_cores", "dark_fibre_count", "fault_cores", "route_path", "logical_connection_ids", "splice_ids", "installation_status", "drawing_layer", "sheath_colour", "owner", "notes"],
            _fibre_cable_schedule(data),
        ),
        (
            "physical_fibre_core_schedule",
            ["cable_id", "cable_name", "core_number", "core_colour", "tube_number", "tube_colour", "status", "circuit_id", "from_termination", "to_termination", "notes"],
            _fibre_core_schedule(data),
        ),
        (
            "physical_fibre_node_schedule",
            ["node_id", "name", "node_type", "location_name", "floor", "x", "y", "rack_name", "rack_start_u", "rack_units", "parent_node_id", "linked_instance_id", "splice_capacity", "configured_splices", "available_splice_capacity", "drawing_layer", "symbol", "notes"],
            _fibre_node_schedule(data),
        ),
        (
            "fibre_splice_schedule",
            ["splice_id", "node_id", "node_name", "node_type", "cassette_id", "incoming_cable_id", "incoming_core", "incoming_colour", "incoming_tube", "incoming_tube_colour", "outgoing_cable_id", "outgoing_core", "outgoing_colour", "outgoing_tube", "outgoing_tube_colour", "splice_type", "circuit_id", "loss_db", "notes"],
            _fibre_splice_schedule(data),
        ),
        (
            "dark_fibre_summary",
            ["cable_id", "name", "core_count", "used_cores", "dark_fibre_count", "dark_fibre_percent", "route_path"],
            _dark_fibre_summary(data),
        ),
        (
            "external_network_peering_schedule",
            ["id", "name", "network_type", "provider", "asn", "location_name", "demarcation_instance_id", "prefixes", "peer_instance_ids", "notes"],
            _external_network_schedule(data),
        ),
        (
            "ip_address_schedule",
            ["id", "instance_id", "vlan_id", "address", "prefix_length", "gateway", "purpose", "notes"],
            _ip_address_schedule(data),
        ),
        (
            "network_configuration_summary",
            ["section", "item", "value"],
            _configuration_summary(data),
        ),
    ]

    paths: List[Path] = []
    for suffix, fieldnames, rows in schedules:
        path = output_directory / f"{safe_prefix}_{suffix}.csv"
        _write_csv(path, fieldnames, rows)
        paths.append(path)
    return paths
