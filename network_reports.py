from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from network_services import (
    cable_core_statistics,
    calculate_optical_budgets,
    splice_arrangement_rows,
    network_traffic_loads,
)


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

    traffic = network_traffic_loads(data)
    carried = traffic.get("carried_by_instance", {})
    direct = traffic.get("direct_by_instance", {})
    rows = []
    switch_types = {
        "network_switch", "network_router", "firewall",
        "optical_line_terminal", "optical_network_terminal",
    }
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
        stack_members = max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1
        bandwidth_capacity_mbps = max(0.0, _float(asset.get("bandwidth_capacity_gbps"))) * 1000.0 * stack_members
        packet_capacity_pps = max(0.0, _float(asset.get("packet_throughput_mpps"))) * 1_000_000.0 * stack_members
        carried_row = carried.get(instance_id, {})
        direct_row = direct.get(instance_id, {})
        bandwidth_load = max(0.0, _float(carried_row.get("bandwidth_mbps")))
        packet_load = max(0.0, _float(carried_row.get("packet_rate_pps")))
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
                "power_input_w": _float(asset.get("power_input_w")) * stack_members,
                "bandwidth_capacity_mbps": round(bandwidth_capacity_mbps, 3),
                "expected_direct_bandwidth_mbps": round(max(0.0, _float(direct_row.get("bandwidth_mbps"))), 3),
                "expected_bandwidth_load_mbps": round(bandwidth_load, 3),
                "bandwidth_headroom_mbps": round(max(0.0, bandwidth_capacity_mbps - bandwidth_load), 3) if bandwidth_capacity_mbps else "",
                "bandwidth_utilisation_percent": round(100.0 * bandwidth_load / bandwidth_capacity_mbps, 3) if bandwidth_capacity_mbps else "",
                "packet_throughput_pps": round(packet_capacity_pps, 1),
                "expected_direct_packet_rate_pps": round(max(0.0, _float(direct_row.get("packet_rate_pps"))), 1),
                "expected_packet_load_pps": round(packet_load, 1),
                "packet_headroom_pps": round(max(0.0, packet_capacity_pps - packet_load), 1) if packet_capacity_pps else "",
                "packet_utilisation_percent": round(100.0 * packet_load / packet_capacity_pps, 3) if packet_capacity_pps else "",
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

    traffic_by_connection = network_traffic_loads(data).get("by_connection", {})
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
                        "link_speed_mbps": 0,
                        "connected_to_instance": "",
                        "connected_to_port": "",
                        "endpoint_name": "",
                        "endpoint_port": "",
                        "endpoint_asset_name": "",
                        "department_id": "",
                        "department_name": "",
                        "poe_power_w": 0.0,
                        "expected_bandwidth_mbps": 0.0,
                        "expected_packet_rate_pps": 0.0,
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
                        "link_speed_mbps": max(0, _int(assignment.get("link_speed_mbps"))),
                        "connected_to_instance": _text(assignment.get("endpoint_name")),
                        "connected_to_port": _text(assignment.get("endpoint_port")),
                        "endpoint_name": _text(assignment.get("endpoint_name")),
                        "endpoint_port": _text(assignment.get("endpoint_port")),
                        "endpoint_asset_name": _text(assignment.get("endpoint_asset_name")),
                        "department_id": _text(assignment.get("department_id")),
                        "department_name": _text(assignment.get("department_name")),
                        "poe_power_w": _float(assignment.get("poe_power_w")),
                        "expected_bandwidth_mbps": _float(assignment.get("expected_bandwidth_mbps")),
                        "expected_packet_rate_pps": _float(assignment.get("expected_packet_rate_pps")),
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
                        "link_speed_mbps": max(0, _int(connection.get("link_speed_mbps"))),
                        "connected_to_instance": _text(connection.get(f"{other_side}_instance_id")),
                        "connected_to_port": _text(connection.get(f"{other_side}_port")),
                        "endpoint_name": "",
                        "endpoint_port": "",
                        "endpoint_asset_name": "",
                        "department_id": "",
                        "department_name": "",
                        "poe_power_w": 0.0,
                        "expected_bandwidth_mbps": _float(traffic_by_connection.get(_text(connection.get("id")), {}).get("bandwidth_mbps")),
                        "expected_packet_rate_pps": _float(traffic_by_connection.get(_text(connection.get("id")), {}).get("packet_rate_pps")),
                        "copper_length_m": _float(connection.get("length_m")),
                        "vlan_ids": ", ".join(str(x) for x in connection.get("vlan_ids", []) if _text(x)),
                        "cable_specification": _text(connection.get("cable_specification")),
                    }
                )
    return sorted(rows, key=lambda row: (str(row["floor"]), row["instance_id"], _natural_port_key(row["port"]), row["status"]))


def _patching_schedule(data: dict, medium: str) -> List[dict]:
    assets, instances, locations = _maps(data)
    traffic_by_connection = network_traffic_loads(data).get("by_connection", {})
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
                "link_speed_mbps": max(0, _int(connection.get("link_speed_mbps"))),
                "from_optic_module_id": _text(connection.get("from_optic_module_id")),
                "to_optic_module_id": _text(connection.get("to_optic_module_id")),
                "from_instance_id": _text(connection.get("from_instance_id")),
                "from_instance_name": from_desc.get("instance_name", ""),
                "from_location": from_desc.get("location_name", ""),
                "from_port": _text(connection.get("from_port")),
                "logical_serving_instance_id": "",
                "logical_serving_instance_name": "",
                "logical_serving_port": "",
                "physical_termination_type": _text(connection.get("physical_segment")),
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
                "expected_bandwidth_mbps": _float(traffic_by_connection.get(_text(connection.get("id")), {}).get("bandwidth_mbps")),
                "expected_packet_rate_pps": _float(traffic_by_connection.get(_text(connection.get("id")), {}).get("packet_rate_pps")),
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
            logical_instance_id = _text(assignment.get("network_instance_id"))
            logical_instance = instances.get(logical_instance_id, {})
            logical_desc = (
                _instance_description(logical_instance, assets, locations)
                if logical_instance else {}
            )
            panel_instance_id = _text(assignment.get("physical_patch_panel_instance_id"))
            physical_instance = instances.get(panel_instance_id, {}) if panel_instance_id else logical_instance
            physical_desc = (
                _instance_description(physical_instance, assets, locations)
                if physical_instance else {}
            )
            physical_port = (
                _text(assignment.get("physical_patch_panel_port"))
                if panel_instance_id else _text(assignment.get("network_port"))
            )
            rows.append(
                {
                    "connection_id": _text(assignment.get("id")),
                    "connection_role": "output",
                    "medium": "copper",
                    "link_speed_mbps": max(0, _int(assignment.get("link_speed_mbps"))),
                    "from_optic_module_id": "",
                    "to_optic_module_id": "",
                    "from_instance_id": panel_instance_id or logical_instance_id,
                    "from_instance_name": physical_desc.get("instance_name", ""),
                    "from_location": physical_desc.get("location_name", ""),
                    "from_port": physical_port,
                    "logical_serving_instance_id": logical_instance_id,
                    "logical_serving_instance_name": logical_desc.get("instance_name", ""),
                    "logical_serving_port": _text(assignment.get("network_port")),
                    "physical_termination_type": _text(assignment.get("physical_termination_type")),
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
                    "expected_bandwidth_mbps": _float(assignment.get("expected_bandwidth_mbps")),
                    "expected_packet_rate_pps": _float(assignment.get("expected_packet_rate_pps")),
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
    type_map = {
        _text(row.get("id")): row
        for row in data.get("network_fibre_cable_types", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    rows = []
    for cable in data.get("network_fibre_cables", []):
        if not isinstance(cable, dict):
            continue
        stats = cable_core_statistics(cable)
        cable_type = type_map.get(_text(cable.get("cable_type_id")), {})
        designations = []
        for designation in cable.get("core_designations", []):
            if not isinstance(designation, dict):
                continue
            numbers = ",".join(str(_int(value)) for value in designation.get("core_numbers", []))
            designations.append(f"{_text(designation.get('circuit_id'))}:{numbers}")
        rows.append({
            "cable_id": _text(cable.get("id")),
            "name": _text(cable.get("name")),
            "cable_type_id": _text(cable.get("cable_type_id")),
            "cable_type": _text(cable.get("cable_type")) or _text(cable_type.get("name")),
            "fibre_standard": _text(cable_type.get("fibre_standard")),
            "routing_role": _text(cable.get("routing_role")) or "direct",
            "parent_cable_id": _text(cable.get("parent_cable_id")),
            "branch_node_id": _text(cable.get("branch_node_id")),
            "from_instance_id": _text(cable.get("from_instance_id")),
            "from_port": _text(cable.get("from_port")),
            "to_instance_id": _text(cable.get("to_instance_id")),
            "to_port": _text(cable.get("to_port")),
            "from_location": _text(cable.get("from_location")),
            "to_location": _text(cable.get("to_location")),
            "from_termination_method": _text(cable.get("from_termination_method")),
            "to_termination_method": _text(cable.get("to_termination_method")),
            "length_m": _float(cable.get("length_m")),
            "slack_length_m": _float(cable.get("slack_length_m")),
            "core_count": stats["total"],
            "used_cores": stats["used"],
            "allocated_cores": stats["allocated"],
            "spliced_cores": stats["spliced"],
            "reserved_cores": stats["reserved"],
            "dark_fibre_count": stats["dark"],
            "fault_cores": stats["fault"],
            "core_designations": "; ".join(designations),
            "attenuation_db_per_m": _float(cable.get("attenuation_db_per_m")),
            "connector_loss_db": _float(cable.get("connector_loss_db")),
            "reflection_loss_db": _float(cable.get("reflection_loss_db")),
            "splice_loss_db": _float(cable.get("splice_loss_db")),
            "wavelength_nm": _int(cable.get("wavelength_nm")),
            "connector_count": _int(cable.get("connector_count")),
            "splice_count": _int(cable.get("splice_count")),
            "estimated_attenuation_db": _float(cable.get("estimated_attenuation_db")),
            "estimated_connector_loss_db": _float(cable.get("estimated_connector_loss_db")),
            "estimated_splice_loss_db": _float(cable.get("estimated_splice_loss_db")),
            "estimated_total_loss_db": _float(cable.get("estimated_total_loss_db")),
            "minimum_return_loss_db": _float(cable.get("minimum_return_loss_db", cable.get("reflection_loss_db"))),
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


def _fibre_cable_type_schedule(data: dict) -> List[dict]:
    return sorted([
        {
            "cable_type_id": _text(row.get("id")),
            "name": _text(row.get("name")),
            "fibre_standard": _text(row.get("fibre_standard")),
            "manufacturer": _text(row.get("manufacturer")),
            "model": _text(row.get("model")),
            "core_count": _int(row.get("core_count")),
            "attenuation_db_per_m": _float(row.get("attenuation_db_per_m")),
            "connector_loss_db": _float(row.get("connector_loss_db")),
            "reflection_loss_db": _float(row.get("reflection_loss_db")),
            "splice_loss_db": _float(row.get("splice_loss_db")),
            "wavelength_nm": _int(row.get("wavelength_nm")),
            "construction": _text(row.get("construction")),
            "sheath_type": _text(row.get("sheath_type")),
            "notes": _text(row.get("notes")),
        }
        for row in data.get("network_fibre_cable_types", [])
        if isinstance(row, dict)
    ], key=lambda row: (row["core_count"], row["cable_type_id"]))

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
    rows = splice_arrangement_rows(data)
    by_id = {_text(row.get("id")): row for row in data.get("network_fibre_splices", []) if isinstance(row, dict)}
    for row in rows:
        source = by_id.get(_text(row.get("splice_id")), {})
        row.update({
            "pigtail": "Yes" if bool(source.get("pigtail")) else "No",
            "connectorised": "Yes" if bool(source.get("connectorised")) else "No",
            "termination_instance_id": _text(source.get("termination_instance_id")),
            "termination_port": _text(source.get("termination_port")),
        })
    return rows


def _splice_cassette_schedule(data: dict) -> List[dict]:
    nodes = {_text(row.get("id")): row for row in data.get("network_fibre_nodes", []) if isinstance(row, dict)}
    cables = {_text(row.get("id")): row for row in data.get("network_fibre_cables", []) if isinstance(row, dict)}
    rows = []
    for cassette in nodes.values():
        if _text(cassette.get("node_type")) != "splice_cassette":
            continue
        cassette_id = _text(cassette.get("id"))
        enclosure = nodes.get(_text(cassette.get("parent_node_id")), {})
        splices = [row for row in data.get("network_fibre_splices", []) if isinstance(row, dict) and _text(row.get("cassette_id")) == cassette_id]
        capacity = max(1, min(24, _int(cassette.get("max_splices_per_tray"), cassette.get("splice_capacity", 24))))
        incoming_id = _text(cassette.get("incoming_cable_id")) or next((_text(row.get("incoming_cable_id")) for row in splices if _text(row.get("incoming_cable_id"))), "")
        incoming = cables.get(incoming_id, {})
        rows.append({
            "enclosure_id": _text(enclosure.get("id")),
            "enclosure_name": _text(enclosure.get("name")),
            "cassette_id": cassette_id,
            "cassette_name": _text(cassette.get("name")),
            "tray_number": _int(cassette.get("tray_number"), 1),
            "floor": _int(cassette.get("floor", enclosure.get("floor"))),
            "location_name": _text(cassette.get("location_name")) or _text(enclosure.get("location_name")),
            "incoming_cable_id": incoming_id,
            "incoming_cable_core_count": _int(incoming.get("core_count")),
            "capacity": capacity,
            "used_positions": len(splices),
            "available_positions": max(0, capacity - len(splices)),
            "circuit_ids": ", ".join(sorted({_text(row.get("circuit_id")) for row in splices if _text(row.get("circuit_id"))})),
            "notes": _text(cassette.get("notes")),
        })
    return sorted(rows, key=lambda row: (row["floor"], row["enclosure_id"], row["tray_number"], row["cassette_id"]))


def _optic_module_schedule(data: dict) -> List[dict]:
    assets = {_text(row.get("id")): row for row in data.get("network_assets", []) if isinstance(row, dict)}
    instances = {_text(row.get("id")): row for row in data.get("network_asset_instances", []) if isinstance(row, dict)}
    rows = []
    for module in data.get("network_optic_modules", []):
        if not isinstance(module, dict):
            continue
        asset = assets.get(_text(module.get("asset_id")), {})
        host = instances.get(_text(module.get("host_instance_id")), {})
        rows.append({
            "module_id": _text(module.get("id")),
            "asset_id": _text(module.get("asset_id")),
            "optic_name": _text(asset.get("name")),
            "manufacturer": _text(asset.get("manufacturer")),
            "model": _text(asset.get("model")),
            "form_factor": _text(asset.get("optic_form_factor")),
            "connector_type": _text(asset.get("optic_connector_type")),
            "fibre_standard": _text(asset.get("optic_fibre_standard")),
            "supported_speeds_mbps": ", ".join(str(_int(value)) for value in asset.get("supported_speeds_mbps", []) if _int(value) > 0),
            "selected_speed_mbps": max(0, _int(module.get("link_speed_mbps"))),
            "host_instance_id": _text(module.get("host_instance_id")),
            "host_instance_name": _text(host.get("name")),
            "host_port": _text(module.get("host_port")),
            "connection_id": _text(module.get("connection_id")),
            "side": _text(module.get("side")),
            "transmit_power_dbm": asset.get("optical_tx_power_dbm", ""),
            "receiver_sensitivity_dbm": asset.get("optical_receiver_sensitivity_dbm", ""),
            "insertion_loss_db": asset.get("optical_insertion_loss_db", ""),
            "return_loss_db": asset.get("optical_return_loss_db", ""),
            "wavelength_nm": asset.get("optical_wavelength_nm", ""),
            "maximum_reach_m": asset.get("optic_reach_m", ""),
            "notes": _text(module.get("notes")),
        })
    return sorted(rows, key=lambda row: (row["host_instance_id"], row["host_port"], row["module_id"]))


def _optical_budget_schedule(data: dict) -> List[dict]:
    calculate_optical_budgets(data)
    return sorted([
        {
            "path_id": _text(row.get("id")),
            "source_instance_id": _text(row.get("source_instance_id")),
            "destination_instance_id": _text(row.get("destination_instance_id")),
            "source_optic_module_id": _text(row.get("source_optic_module_id")),
            "destination_optic_module_id": _text(row.get("destination_optic_module_id")),
            "link_speed_mbps": max(0, _int(row.get("link_speed_mbps"))),
            "wavelength_nm": max(0, _int(row.get("wavelength_nm"))),
            "connection_ids": ", ".join(_text(v) for v in row.get("connection_ids", []) if _text(v)),
            "fibre_cable_ids": ", ".join(_text(v) for v in row.get("fibre_cable_ids", []) if _text(v)),
            "transmit_power_dbm": "" if _text(row.get("transmit_power_dbm")) == "" else _float(row.get("transmit_power_dbm")),
            "receiver_sensitivity_dbm": "" if _text(row.get("receiver_sensitivity_dbm")) == "" else _float(row.get("receiver_sensitivity_dbm")),
            "cable_loss_db": _float(row.get("cable_loss_db")),
            "passive_loss_db": _float(row.get("passive_loss_db")),
            "active_optic_loss_db": _float(row.get("active_optic_loss_db")),
            "path_loss_db": _float(row.get("path_loss_db")),
            "available_budget_db": "" if _text(row.get("available_budget_db")) == "" else _float(row.get("available_budget_db")),
            "margin_db": "" if _text(row.get("margin_db")) == "" else _float(row.get("margin_db")),
            "minimum_return_loss_db": _float(row.get("minimum_return_loss_db")),
            "status": _text(row.get("status")),
            "missing_properties": _text(row.get("missing_properties")),
            "notes": _text(row.get("notes")),
        }
        for row in data.get("network_optical_paths", [])
        if isinstance(row, dict)
    ], key=lambda row: (row["status"], row["source_instance_id"], row["destination_instance_id"], row["path_id"]))


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
            "service_type": _text(item.get("service_type")),
            "redundant": "Yes" if bool(item.get("redundant")) else "No",
            "required_links": _int(item.get("required_links"), 1),
            "medium": _text(item.get("medium")),
            "bandwidth_mbps": _float(item.get("bandwidth_mbps")),
            "demarcation_instance_id": _text(item.get("demarcation_instance_id")),
            "prefixes": ", ".join(_text(v) for v in item.get("prefixes", []) if _text(v)),
            "peer_instance_ids": ", ".join(_text(v) for v in item.get("peer_instance_ids", []) if _text(v)),
            "redundancy_group_id": _text(item.get("redundancy_group_id")),
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
                "bandwidth_capacity_mbps", "expected_direct_bandwidth_mbps", "expected_bandwidth_load_mbps",
                "bandwidth_headroom_mbps", "bandwidth_utilisation_percent",
                "packet_throughput_pps", "expected_direct_packet_rate_pps", "expected_packet_load_pps",
                "packet_headroom_pps", "packet_utilisation_percent",
            ],
            _switch_schedule(data),
        ),
        (
            "port_schedule",
            [
                "instance_id", "instance_name", "asset_id", "asset_name", "asset_type", "location_name",
                "floor", "logical_stack", "stack_member_count", "port", "status", "connection_id", "connection_role", "medium", "link_speed_mbps",
                "connected_to_instance", "connected_to_port", "endpoint_name", "endpoint_port",
                "endpoint_asset_name", "department_id", "department_name", "poe_power_w",
                "expected_bandwidth_mbps", "expected_packet_rate_pps", "copper_length_m",
                "vlan_ids", "cable_specification",
            ],
            _port_schedule(data),
        ),
        (
            "copper_patching_schedule",
            [
                "connection_id", "connection_role", "medium", "link_speed_mbps", "from_optic_module_id", "to_optic_module_id", "from_instance_id", "from_instance_name",
                "from_location", "from_port", "logical_serving_instance_id", "logical_serving_instance_name",
                "logical_serving_port", "physical_termination_type", "to_instance_id", "to_instance_name", "to_location",
                "to_port", "endpoint_name", "endpoint_port", "endpoint_asset_name", "department_id",
                "department_name", "poe_power_w", "expected_bandwidth_mbps", "expected_packet_rate_pps",
                "length_m", "cable_specification", "fibre_count",
                "vlan_ids", "route_profile", "route_path", "redundancy_role", "protection_group", "notes",
            ],
            _patching_schedule(data, "copper"),
        ),
        (
            "fibre_patching_schedule",
            [
                "connection_id", "connection_role", "medium", "link_speed_mbps", "from_optic_module_id", "to_optic_module_id", "from_instance_id", "from_instance_name",
                "from_location", "from_port", "logical_serving_instance_id", "logical_serving_instance_name",
                "logical_serving_port", "physical_termination_type", "to_instance_id", "to_instance_name", "to_location",
                "to_port", "endpoint_name", "endpoint_port", "endpoint_asset_name", "department_id",
                "department_name", "poe_power_w", "expected_bandwidth_mbps", "expected_packet_rate_pps",
                "length_m", "cable_specification", "fibre_count",
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
            "physical_fibre_cable_type_schedule",
            ["cable_type_id", "name", "fibre_standard", "manufacturer", "model", "core_count", "attenuation_db_per_m", "connector_loss_db", "reflection_loss_db", "splice_loss_db", "wavelength_nm", "construction", "sheath_type", "notes"],
            _fibre_cable_type_schedule(data),
        ),
        (
            "physical_fibre_cable_schedule",
            ["cable_id", "name", "cable_type_id", "cable_type", "fibre_standard", "routing_role", "parent_cable_id", "branch_node_id", "from_instance_id", "from_port", "to_instance_id", "to_port", "from_location", "to_location", "from_termination_method", "to_termination_method", "length_m", "slack_length_m", "core_count", "used_cores", "allocated_cores", "spliced_cores", "reserved_cores", "dark_fibre_count", "fault_cores", "core_designations", "attenuation_db_per_m", "connector_loss_db", "reflection_loss_db", "splice_loss_db", "wavelength_nm", "connector_count", "splice_count", "estimated_attenuation_db", "estimated_connector_loss_db", "estimated_splice_loss_db", "estimated_total_loss_db", "minimum_return_loss_db", "route_path", "logical_connection_ids", "splice_ids", "installation_status", "drawing_layer", "sheath_colour", "owner", "notes"],
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
            ["splice_id", "node_id", "node_name", "node_type", "cassette_id", "incoming_cable_id", "incoming_core", "incoming_colour", "incoming_tube", "incoming_tube_colour", "outgoing_cable_id", "outgoing_core", "outgoing_colour", "outgoing_tube", "outgoing_tube_colour", "splice_type", "pigtail", "connectorised", "termination_instance_id", "termination_port", "circuit_id", "loss_db", "notes"],
            _fibre_splice_schedule(data),
        ),
        (
            "splice_cassette_schedule",
            ["enclosure_id", "enclosure_name", "cassette_id", "cassette_name", "tray_number", "floor", "location_name", "incoming_cable_id", "incoming_cable_core_count", "capacity", "used_positions", "available_positions", "circuit_ids", "notes"],
            _splice_cassette_schedule(data),
        ),
        (
            "optic_module_schedule",
            ["module_id", "asset_id", "optic_name", "manufacturer", "model", "form_factor", "connector_type", "fibre_standard", "supported_speeds_mbps", "selected_speed_mbps", "host_instance_id", "host_instance_name", "host_port", "connection_id", "side", "transmit_power_dbm", "receiver_sensitivity_dbm", "insertion_loss_db", "return_loss_db", "wavelength_nm", "maximum_reach_m", "notes"],
            _optic_module_schedule(data),
        ),
        (
            "optical_budget_schedule",
            ["path_id", "source_instance_id", "destination_instance_id", "source_optic_module_id", "destination_optic_module_id", "link_speed_mbps", "wavelength_nm", "connection_ids", "fibre_cable_ids", "transmit_power_dbm", "receiver_sensitivity_dbm", "cable_loss_db", "passive_loss_db", "active_optic_loss_db", "path_loss_db", "available_budget_db", "margin_db", "minimum_return_loss_db", "status", "missing_properties", "notes"],
            _optical_budget_schedule(data),
        ),
        (
            "dark_fibre_summary",
            ["cable_id", "name", "core_count", "used_cores", "dark_fibre_count", "dark_fibre_percent", "route_path"],
            _dark_fibre_summary(data),
        ),
        (
            "external_network_peering_schedule",
            ["id", "name", "network_type", "provider", "asn", "location_name", "service_type", "redundant", "required_links", "medium", "bandwidth_mbps", "demarcation_instance_id", "prefixes", "peer_instance_ids", "redundancy_group_id", "notes"],
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
