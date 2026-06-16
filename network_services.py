"""Shared services for physical fibre, circuit tracing and IP address planning."""
from __future__ import annotations

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


def sync_fibre_cables_from_connections(data: dict, replace_auto: bool = False) -> List[dict]:
    """Create or refresh physical cable records for logical fibre connections.

    The logical connection remains the authoritative network edge. The physical
    cable stores the graph route and individual cores so splice and dark-fibre
    reporting can be performed without exposing passive joints in logical views.
    """
    cables = data.setdefault("network_fibre_cables", [])
    if replace_auto:
        cables[:] = [item for item in cables if not bool(item.get("auto_generated"))]
    by_logical: Dict[str, dict] = {}
    for cable in cables:
        if not isinstance(cable, dict):
            continue
        for connection_id in cable.get("logical_connection_ids", []):
            if _text(connection_id):
                by_logical[_text(connection_id)] = cable

    settings = data.setdefault("network_settings", {})
    default_count = max(2, _int(settings.get("default_fibre_core_count"), 12))
    created: List[dict] = []
    routed_physical_parents = {
        _text(connection.get("parent_logical_connection_id"))
        for connection in data.get("network_connections", [])
        if isinstance(connection, dict)
        and _text(connection.get("medium")).lower() == "fibre"
        and _text(connection.get("physical_segment")).lower() == "fibre_backbone"
        and connection.get("route_path")
        and _text(connection.get("parent_logical_connection_id"))
    }
    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict) or _text(connection.get("medium")).lower() != "fibre":
            continue
        # Rack patch leads and zero-length panel jumpers are represented by
        # network_patch_leads/hidden physical segments, not as routed fibre
        # sheaths on the floor drawing.  Keep routed panel-to-panel backbones.
        cable_spec = _text(connection.get("cable_specification")).lower()
        route_path = [value for value in connection.get("route_path", []) if _text(value)]
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
        used_cores = max(1, _int(connection.get("fibre_count"), 2))
        declared_cable_cores = max(
            default_count,
            used_cores,
            _int(connection.get("cable_core_count"), 0),
        )
        cable = by_logical.get(logical_id)
        if cable is None:
            cable = {
                "id": next_record_id(cables, "FOC"),
                "name": f"Fibre cable for {connection_id}",
                "cable_type": _text(connection.get("cable_specification")) or "OS2 single-mode fibre",
                "from_instance_id": _text(connection.get("from_instance_id")),
                "from_port": _text(connection.get("from_port")),
                "to_instance_id": _text(connection.get("to_instance_id")),
                "to_port": _text(connection.get("to_port")),
                "from_location": _record_location(data, _text(connection.get("from_instance_id")))[0],
                "to_location": _record_location(data, _text(connection.get("to_instance_id")))[0],
                "route_path": list(connection.get("route_path", [])),
                "length_m": max(0.0, _float(connection.get("length_m"))),
                "core_count": declared_cable_cores,
                "logical_connection_ids": [logical_id],
                "splice_ids": [],
                "installation_status": "planned" if connection.get("auto_generated") else "installed",
                "owner": "",
                "notes": "Generated from the logical network connection.",
                "auto_generated": bool(connection.get("auto_generated", False)),
            }
            cable["cores"] = build_fibre_cores(cable["core_count"], used_cores, logical_id)
            cables.append(cable)
            by_logical[logical_id] = cable
            created.append(cable)
        else:
            cable["from_instance_id"] = _text(connection.get("from_instance_id"))
            cable["from_port"] = _text(connection.get("from_port"))
            cable["to_instance_id"] = _text(connection.get("to_instance_id"))
            cable["to_port"] = _text(connection.get("to_port"))
            cable["from_location"] = _record_location(data, cable["from_instance_id"])[0]
            cable["to_location"] = _record_location(data, cable["to_instance_id"])[0]
            cable["route_path"] = list(connection.get("route_path", []))
            cable["length_m"] = max(0.0, _float(connection.get("length_m")))
            cable["core_count"] = max(
                _int(cable.get("core_count"), default_count),
                declared_cable_cores,
            )
            cable["cores"] = build_fibre_cores(
                cable["core_count"], used_cores, logical_id, cable.get("cores", [])
            )
            ids = [_text(value) for value in cable.get("logical_connection_ids", []) if _text(value)]
            if logical_id not in ids:
                ids.append(logical_id)
            cable["logical_connection_ids"] = ids
    return created


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
        if any(_text(assets.get(_text(i.get("asset_id")), {}).get("asset_type")) == "wireless_access_point" for i in data.get("network_asset_instances", []) if isinstance(i, dict)):
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
        "symbol_scale": 0.32,
        "label_scale": 0.55,
        "cable_width_scale": 0.55,
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
    """Create physical terminations and splice cassettes for generated fibre ends.

    Passive nodes are deliberately stored outside ``network_asset_instances`` so
    they never appear in the logical topology.  They are linked to the active or
    patch-panel instance that terminates the cable and can be drawn on the
    independent physical-fibre map.
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
    created: List[dict] = []
    endpoint_ids: Set[str] = set()
    for cable in data.get("network_fibre_cables", []):
        if not isinstance(cable, dict):
            continue
        endpoint_ids.add(_text(cable.get("from_instance_id")))
        endpoint_ids.add(_text(cable.get("to_instance_id")))
    for instance_id in sorted(value for value in endpoint_ids if value):
        instance = instances.get(instance_id)
        if not instance:
            continue
        asset = assets.get(_text(instance.get("asset_id")), {})
        asset_type = _text(asset.get("asset_type")).lower()
        is_panel = asset_type == "patch_panel" and _text(asset.get("patch_panel_type")).lower() == "fibre"
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
        if is_panel and (instance_id, "splice_cassette") not in existing_by_instance_type:
            cassette = {
                "id": next_record_id(nodes, "FSC"),
                "name": f"{_text(instance.get('name')) or instance_id} cassette",
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
                "cassette_capacity": max(12, _int(asset.get("number_of_ports"), 12)),
                "drawing_layer": "NET-FIBRE-SPLICE",
                "notes": "Generated splice cassette associated with the fibre patch panel.",
                "auto_generated": True,
            }
            nodes.append(cassette)
            existing_by_instance_type[(instance_id, "splice_cassette")] = cassette
            created.append(cassette)
    return created


def ensure_physical_fibre_for_design(data: dict, replace_auto: bool = False) -> dict:
    """Synchronise logical fibre links into the separate physical-fibre layer."""
    created_cables = sync_fibre_cables_from_connections(data, replace_auto=replace_auto)
    created_nodes = sync_fibre_nodes_from_design(data, replace_auto=replace_auto)
    return {
        "created_cables": len(created_cables),
        "created_nodes": len(created_nodes),
        "cable_count": len(data.get("network_fibre_cables", [])),
        "node_count": len(data.get("network_fibre_nodes", [])),
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
    """Calculate expected bandwidth and packet load carried by active devices.

    Endpoint traffic is taken from ``network_endpoint_assignments`` and traffic
    generated by an active network device is taken from its asset definition.
    Each logical connected component is oriented from the highest-tier
    router/firewall/core device and descendant demand is accumulated upstream.
    Hidden physical patching records are deliberately excluded.
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

    direct: Dict[str, Dict[str, float]] = {
        instance_id: {"bandwidth_mbps": 0.0, "packet_rate_pps": 0.0}
        for instance_id in instances
    }
    for instance_id, instance in instances.items():
        asset = assets.get(_text(instance.get("asset_id")), {})
        members = (
            max(1, _int(instance.get("stack_member_count"), 1))
            if bool(instance.get("logical_stack"))
            else 1
        )
        direct[instance_id]["bandwidth_mbps"] += max(
            0.0, _float(asset.get("expected_bandwidth_mbps"))
        ) * members
        direct[instance_id]["packet_rate_pps"] += max(
            0.0, _float(asset.get("expected_packet_rate_pps"))
        ) * members

    for assignment in data.get("network_endpoint_assignments", []):
        if not isinstance(assignment, dict):
            continue
        instance_id = _text(assignment.get("network_instance_id"))
        if instance_id not in direct:
            continue
        direct[instance_id]["bandwidth_mbps"] += max(
            0.0, _float(assignment.get("expected_bandwidth_mbps"))
        )
        direct[instance_id]["packet_rate_pps"] += max(
            0.0, _float(assignment.get("expected_packet_rate_pps"))
        )

    adjacency: Dict[str, List[Tuple[str, str]]] = {key: [] for key in instances}
    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict):
            continue
        if bool(connection.get("topology_hidden")) or bool(
            connection.get("physical_connection")
        ):
            continue
        source = _text(connection.get("from_instance_id"))
        target = _text(connection.get("to_instance_id"))
        connection_id = _text(connection.get("id"))
        if source not in instances or target not in instances or source == target:
            continue
        adjacency[source].append((target, connection_id))
        adjacency[target].append((source, connection_id))

    def root_rank(instance_id: str) -> Tuple[int, int, str]:
        instance = instances[instance_id]
        asset = assets.get(_text(instance.get("asset_id")), {})
        asset_type = _text(asset.get("asset_type")).lower()
        role = _text(instance.get("design_role")).lower()
        if asset_type in {"network_router", "firewall", "telco_pop", "external_network"} or any(
            word in role for word in ("router", "gateway", "firewall", "telco", "peer")
        ):
            rank = 0
        elif "core" in role:
            rank = 1
        elif any(word in role for word in ("aggregation", "distribution")):
            rank = 2
        elif "access" in role or asset_type in {"network_switch", "optical_line_terminal"}:
            rank = 3
        else:
            rank = 4
        return rank, -len(adjacency.get(instance_id, [])), instance_id

    carried = {key: dict(value) for key, value in direct.items()}
    connection_loads: Dict[str, Dict[str, float]] = {}
    visited: Set[str] = set()

    for seed in sorted(instances):
        if seed in visited:
            continue
        component: Set[str] = {seed}
        queue = [seed]
        while queue:
            current = queue.pop(0)
            for neighbour, _connection_id in adjacency.get(current, []):
                if neighbour not in component:
                    component.add(neighbour)
                    queue.append(neighbour)
        visited.update(component)
        root = min(component, key=root_rank)
        parent: Dict[str, str] = {}
        parent_edge: Dict[str, str] = {}
        order: List[str] = [root]
        bfs = [root]
        discovered = {root}
        while bfs:
            current = bfs.pop(0)
            for neighbour, connection_id in sorted(
                adjacency.get(current, []), key=lambda row: root_rank(row[0])
            ):
                if neighbour not in component or neighbour in discovered:
                    continue
                discovered.add(neighbour)
                parent[neighbour] = current
                parent_edge[neighbour] = connection_id
                order.append(neighbour)
                bfs.append(neighbour)

        for node_id in reversed(order[1:]):
            upstream = parent[node_id]
            bandwidth = carried[node_id]["bandwidth_mbps"]
            packets = carried[node_id]["packet_rate_pps"]
            carried[upstream]["bandwidth_mbps"] += bandwidth
            carried[upstream]["packet_rate_pps"] += packets
            edge_id = parent_edge.get(node_id, "")
            if edge_id:
                connection_loads[edge_id] = {
                    "bandwidth_mbps": bandwidth,
                    "packet_rate_pps": packets,
                }

    return {
        "direct_by_instance": direct,
        "carried_by_instance": carried,
        "by_connection": connection_loads,
        "total_endpoint_bandwidth_mbps": sum(
            max(0.0, _float(item.get("expected_bandwidth_mbps")))
            for item in data.get("network_endpoint_assignments", [])
            if isinstance(item, dict)
        ),
        "total_endpoint_packet_rate_pps": sum(
            max(0.0, _float(item.get("expected_packet_rate_pps")))
            for item in data.get("network_endpoint_assignments", [])
            if isinstance(item, dict)
        ),
    }
