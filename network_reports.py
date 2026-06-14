from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


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
        "rack_units": _int(asset.get("rack_units")),
        "management_ip": _text(instance.get("management_ip")),
        "management_vlan": _text(instance.get("management_vlan")),
    }


def _switch_schedule(data: dict) -> List[dict]:
    assets, instances, locations = _maps(data)
    rows = []
    switch_types = {"network_switch", "optical_line_terminal"}
    technology = _text(data.get("network_settings", {}).get("technology"))
    for instance in instances.values():
        asset = assets.get(_text(instance.get("asset_id")), {})
        if _text(asset.get("asset_type")) not in switch_types:
            continue
        row = _instance_description(instance, assets, locations)
        row.update(
            {
                "network_technology": technology,
                "number_of_ports": _int(asset.get("number_of_ports")),
                "connections_in": _int(asset.get("connections_in")),
                "connections_out": _int(asset.get("connections_out")),
                "uplink_ports": _int(asset.get("uplink_ports")),
                "input_connection_type": _text(asset.get("input_connection_type")),
                "output_connection_type": _text(asset.get("output_connection_type")),
                "uplink_connection_type": _text(asset.get("uplink_connection_type")),
                "poe_budget_w": _float(asset.get("poe_budget_w")),
                "power_input_w": _float(asset.get("power_input_w")),
            }
        )
        rows.append(row)
    return sorted(rows, key=lambda row: (str(row["floor"]), row["location_name"], row["instance_id"]))


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

    rows = []
    for instance in instances.values():
        asset = assets.get(_text(instance.get("asset_id")), {})
        count = max(0, _int(asset.get("number_of_ports")))
        base = _instance_description(instance, assets, locations)
        for port_number in range(1, count + 1):
            port_name = str(port_number)
            linked = endpoint_map.get((_text(instance.get("id")), port_name), [])
            if not linked:
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
                        "vlan_ids": "",
                        "cable_specification": "",
                    }
                )
                continue
            for connection in linked:
                is_from = _text(connection.get("from_instance_id")) == _text(instance.get("id"))
                other_side = "to" if is_from else "from"
                rows.append(
                    {
                        **base,
                        "port": port_name,
                        "status": "Connected",
                        "connection_id": _text(connection.get("id")),
                        "connection_role": _text(connection.get("connection_role")),
                        "medium": _text(connection.get("medium")),
                        "connected_to_instance": _text(connection.get(f"{other_side}_instance_id")),
                        "connected_to_port": _text(connection.get(f"{other_side}_port")),
                        "vlan_ids": ", ".join(str(x) for x in connection.get("vlan_ids", []) if _text(x)),
                        "cable_specification": _text(connection.get("cable_specification")),
                    }
                )
    return sorted(rows, key=lambda row: (str(row["floor"]), row["instance_id"], int(row["port"]) if str(row["port"]).isdigit() else 999999))


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
                "cable_specification": _text(connection.get("cable_specification")),
                "fibre_count": _int(connection.get("fibre_count")),
                "vlan_ids": ", ".join(str(x) for x in connection.get("vlan_ids", []) if _text(x)),
                "route_profile": _text(connection.get("route_profile")),
                "route_path": " -> ".join(str(x) for x in connection.get("route_path", []) if _text(x)),
                "notes": _text(connection.get("notes")),
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
                "subnet": _text(vlan.get("subnet")),
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
        inbound = sum(1 for c in data.get("network_connections", []) if _text(c.get("to_instance_id")) == instance_id)
        outbound = sum(1 for c in data.get("network_connections", []) if _text(c.get("from_instance_id")) == instance_id)
        row = _instance_description(instance, assets, locations)
        row.update(
            {
                "split_ratio": _text(asset.get("split_ratio")),
                "input_connection_type": _text(asset.get("input_connection_type")),
                "output_connection_type": _text(asset.get("output_connection_type")),
                "configured_inputs": _int(asset.get("connections_in")),
                "configured_outputs": _int(asset.get("connections_out")),
                "connected_inputs": inbound,
                "connected_outputs": outbound,
                "available_outputs": max(0, _int(asset.get("connections_out")) - outbound),
            }
        )
        rows.append(row)
    return sorted(rows, key=lambda row: (str(row["floor"]), row["location_name"], row["instance_id"]))


def _power_schedule(data: dict) -> List[dict]:
    assets, instances, locations = _maps(data)
    rows = []
    total_input = 0.0
    total_poe = 0.0
    for instance in instances.values():
        asset = assets.get(_text(instance.get("asset_id")), {})
        power_input = _float(asset.get("power_input_w"))
        poe_budget = _float(asset.get("poe_budget_w"))
        total_input += power_input
        total_poe += poe_budget
        row = _instance_description(instance, assets, locations)
        row.update(
            {
                "power_input_w": power_input,
                "poe_budget_w": poe_budget,
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
            "poe_budget_w": round(total_poe, 3),
        }
    )
    return rows


def write_network_schedules(data: dict, output_directory: Path, prefix: str = "network") -> List[Path]:
    output_directory = Path(output_directory)
    safe_prefix = _text(prefix) or "network"

    schedules = [
        (
            "switch_schedule",
            [
                "network_technology", "instance_id", "instance_name", "asset_id", "asset_name",
                "asset_type", "manufacturer", "model", "location_name", "location_type", "floor",
                "rack_name", "rack_start_u", "rack_units", "management_ip", "management_vlan",
                "number_of_ports", "connections_in", "connections_out", "uplink_ports",
                "input_connection_type", "output_connection_type", "uplink_connection_type",
                "power_input_w", "poe_budget_w",
            ],
            _switch_schedule(data),
        ),
        (
            "port_schedule",
            [
                "instance_id", "instance_name", "asset_id", "asset_name", "asset_type", "location_name",
                "floor", "port", "status", "connection_id", "connection_role", "medium",
                "connected_to_instance", "connected_to_port", "vlan_ids", "cable_specification",
            ],
            _port_schedule(data),
        ),
        (
            "copper_patching_schedule",
            [
                "connection_id", "connection_role", "medium", "from_instance_id", "from_instance_name",
                "from_location", "from_port", "to_instance_id", "to_instance_name", "to_location",
                "to_port", "cable_specification", "fibre_count", "vlan_ids", "route_profile", "route_path", "notes",
            ],
            _patching_schedule(data, "copper"),
        ),
        (
            "fibre_patching_schedule",
            [
                "connection_id", "connection_role", "medium", "from_instance_id", "from_instance_name",
                "from_location", "from_port", "to_instance_id", "to_instance_name", "to_location",
                "to_port", "cable_specification", "fibre_count", "vlan_ids", "route_profile", "route_path", "notes",
            ],
            _patching_schedule(data, "fibre"),
        ),
        (
            "vlan_schedule",
            ["id", "vlan_id", "name", "purpose", "subnet", "gateway", "dhcp_scope", "security_zone", "notes"],
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
                "split_ratio", "input_connection_type", "output_connection_type", "configured_inputs",
                "configured_outputs", "connected_inputs", "connected_outputs", "available_outputs",
            ],
            _splitter_schedule(data),
        ),
        (
            "power_schedule",
            [
                "instance_id", "instance_name", "asset_id", "asset_name", "asset_type", "manufacturer", "model",
                "location_name", "location_type", "floor", "rack_name", "rack_start_u", "rack_units",
                "power_input_w", "poe_budget_w", "power_feed", "ups_source", "notes",
            ],
            _power_schedule(data),
        ),
    ]

    paths: List[Path] = []
    for suffix, fieldnames, rows in schedules:
        path = output_directory / f"{safe_prefix}_{suffix}.csv"
        _write_csv(path, fieldnames, rows)
        paths.append(path)
    return paths
