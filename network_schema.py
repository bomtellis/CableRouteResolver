"""Data model helpers for the Cable Routing Solver network-planning extension."""

from __future__ import annotations

from copy import deepcopy
import ipaddress
from typing import Dict, Iterable, List, Optional

from network_services import FIBRE_COLOURS, build_fibre_cores, fibre_layer_defaults, set_core_status_from_splices

NETWORK_DEFAULTS = {
    "network_settings": {
        "technology": "Traditional",
        "expected_mer_count": 2,
        "redundant_core": True,
        "topology_model": "collapsed_core",
        "independent_link_count": 2,
        "layer_connection_rules": [],
        "manufacturer_preferences": {},
        "auto_connect_new_manual_devices": True,
        "spare_capacity_percent": 15.0,
        "traditional_max_copper_m": 90.0,
        "polan_max_ont_copper_m": 30.0,
        "polan_max_onts_per_splitter": 8,
        "polan_max_splitter_ont_route_m": 120.0,
        "polan_olt_failover": True,
        "default_poe_power_w": 0.0,
        "default_expected_bandwidth_mbps": 0.0,
        "default_expected_packet_rate_pps": 0.0,
        "poe_power_defaults": {},
        "default_rack_size_u": 42,
        "default_fibre_core_count": 12,
        "ip_plan_base_cidr": "10.0.0.0/8",
        "physical_fibre_layer": fibre_layer_defaults(),
    },
    "network_assets": [],
    "network_asset_instances": [],
    "network_racks": [],
    "network_connections": [],
    "network_endpoint_assignments": [],
    "network_patch_leads": [],
    "network_redundancy_groups": [],
    "network_vlans": [],
    "network_routes": [],
    "network_ip_allocations": [],
    "network_external_networks": [],
    "network_fibre_cables": [],
    "network_fibre_nodes": [],
    "network_fibre_splices": [],
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
    "ups",
    "pdu",
    "power_device",
    "cable_management",
    "cable_manager",
    "telco_pop",
    "external_network",
    "other",
}

NETWORK_MEDIA = {"copper", "fibre", "wireless", "virtual", "stacking", "none"}
NETWORK_CONNECTION_ROLES = {"input", "output", "uplink"}
NETWORK_LOCATION_TYPES = {"mer", "polan", "telco_pop", "external_network", "fibre_joint"}

NETWORK_PORT_TYPES = {"rj45", "sfp", "sfp+", "qsfp", "qsfp28", "pon", "lc", "sc", "mpo", "usb", "console", "power", "other"}
NETWORK_PORT_USES = {"input", "output", "uplink", "downlink", "management", "console", "pon", "client", "patch", "stacking", "power", "spare", "other"}


NETWORK_TOPOLOGY_MODELS = {"collapsed_core", "three_tier"}
NETWORK_LAYERS = {"core", "aggregation", "access"}
NETWORK_LAYER_RULE_PAIRS = {
    ("core", "core"),
    ("core", "aggregation"),
    ("aggregation", "aggregation"),
    ("aggregation", "access"),
    ("core", "access"),
}

MANUFACTURER_PREFERENCE_COMPONENTS = {
    "access_switch": "Access switches",
    "aggregation_switch": "Aggregation/distribution switches",
    "core_switch": "Core switches",
    "edge_router": "Routers / WAN edge",
    "wireless_access_point": "Wireless access points",
    "optical_line_terminal": "Optical line terminals (OLT)",
    "optical_network_terminal": "Optical network terminals (ONT)",
    "fibre_splitter": "Fibre splitters",
    "copper_patch_panel": "Copper patch panels",
    "fibre_patch_panel": "Fibre patch panels",
    "rack_ups": "Rack UPS equipment",
    "cable_management": "Cable-management panels",
}


def default_layer_connection_rules(
    topology_model: str = "collapsed_core",
    redundant: bool = True,
    independent_link_count: int = 2,
) -> List[dict]:
    """Return deterministic defaults for the selected network hierarchy."""

    model = _text(topology_model).lower()
    if model not in NETWORK_TOPOLOGY_MODELS:
        model = "collapsed_core"
    redundant = bool(redundant)
    links = max(1, _as_int(independent_link_count, 2 if redundant else 1))
    if not redundant:
        links = 1
    distinct = min(2, links) if redundant else 1

    if model == "three_tier":
        rules = [
            {
                "id": "core_to_aggregation",
                "source_layer": "core",
                "target_layer": "aggregation",
                "links_per_target": links,
                "minimum_distinct_sources": distinct,
                "enabled": True,
            },
            {
                "id": "aggregation_to_access",
                "source_layer": "aggregation",
                "target_layer": "access",
                "links_per_target": links,
                "minimum_distinct_sources": distinct,
                "enabled": True,
            },
        ]
        if redundant:
            rules.insert(0, {
                "id": "core_peer",
                "source_layer": "core",
                "target_layer": "core",
                "links_per_target": 1,
                "minimum_distinct_sources": 1,
                "enabled": True,
            })
            rules.insert(2, {
                "id": "aggregation_peer",
                "source_layer": "aggregation",
                "target_layer": "aggregation",
                "links_per_target": 1,
                "minimum_distinct_sources": 1,
                "enabled": True,
            })
        return rules

    rules = [
        {
            "id": "core_to_access",
            "source_layer": "core",
            "target_layer": "access",
            "links_per_target": links,
            "minimum_distinct_sources": distinct,
            "enabled": True,
        }
    ]
    if redundant:
        rules.insert(0, {
            "id": "core_peer",
            "source_layer": "core",
            "target_layer": "core",
            "links_per_target": 1,
            "minimum_distinct_sources": 1,
            "enabled": True,
        })
    return rules


def normalise_layer_connection_rules(
    rules,
    topology_model: str = "collapsed_core",
    redundant: bool = True,
    independent_link_count: int = 2,
) -> List[dict]:
    """Normalise user-entered layer rules and discard unsupported layer pairs."""

    if not isinstance(rules, list) or not rules:
        return default_layer_connection_rules(
            topology_model, redundant, independent_link_count
        )

    result: List[dict] = []
    seen_ids: set[str] = set()
    for index, row in enumerate(rules, start=1):
        if not isinstance(row, dict):
            continue
        source = _text(row.get("source_layer")).lower()
        target = _text(row.get("target_layer")).lower()
        if (source, target) not in NETWORK_LAYER_RULE_PAIRS:
            continue
        links = max(1, min(16, _as_int(row.get("links_per_target"), 1)))
        distinct = max(1, min(links, _as_int(row.get("minimum_distinct_sources"), 1)))
        rule_id = _text(row.get("id")) or f"{source}_to_{target}"
        if rule_id in seen_ids:
            suffix = 2
            while f"{rule_id}_{suffix}" in seen_ids:
                suffix += 1
            rule_id = f"{rule_id}_{suffix}"
        seen_ids.add(rule_id)
        result.append(
            {
                "id": rule_id,
                "source_layer": source,
                "target_layer": target,
                "links_per_target": links,
                "minimum_distinct_sources": distinct,
                "enabled": bool(row.get("enabled", True)),
            }
        )

    return result or default_layer_connection_rules(
        topology_model, redundant, independent_link_count
    )


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


def _normalise_split_ratio(value) -> tuple[str, int, int]:
    """Normalise passive splitter ratios and allow one or two feeder inputs."""

    text = _text(value).lower().replace("×", ":").replace("x", ":")
    text = "".join(text.split())
    if ":" not in text:
        return "", 0, 0
    left, right = text.split(":", 1)
    try:
        inputs = int(left)
        outputs = int(right)
    except (TypeError, ValueError):
        return "", 0, 0
    if inputs not in {1, 2} or outputs < 1:
        return "", 0, 0
    return f"{inputs}:{outputs}", inputs, outputs


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


def normalise_manufacturer_preferences(value) -> Dict[str, dict]:
    """Return role-specific manufacturer priorities used by the auto-planner.

    Each component key stores an ordered manufacturer list and an optional
    strict flag.  Non-strict preferences rank matching equipment first but
    allow another manufacturer when the preferred library cannot meet the
    required capacity.  Strict preferences exclude all unlisted manufacturers.
    """

    source = value if isinstance(value, dict) else {}
    result: Dict[str, dict] = {}
    for component in MANUFACTURER_PREFERENCE_COMPONENTS:
        raw = source.get(component, {})
        strict = False
        preferred = []
        if isinstance(raw, dict):
            preferred = _normalise_string_list(
                raw.get("preferred_manufacturers", raw.get("manufacturers", []))
            )
            strict = bool(raw.get("strict", False))
        elif isinstance(raw, (list, tuple, str)):
            preferred = _normalise_string_list(raw)
        result[component] = {
            "preferred_manufacturers": preferred,
            "strict": strict,
        }
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
    settings.setdefault("topology_model", "collapsed_core")
    settings.setdefault("independent_link_count", 2)
    settings.setdefault("layer_connection_rules", [])
    settings.setdefault("manufacturer_preferences", {})
    settings.setdefault("auto_connect_new_manual_devices", True)
    settings.setdefault("spare_capacity_percent", 15.0)
    settings.setdefault("traditional_max_copper_m", 90.0)
    settings.setdefault("polan_max_ont_copper_m", 30.0)
    settings.setdefault("polan_max_onts_per_splitter", 8)
    settings.setdefault("polan_max_splitter_ont_route_m", 120.0)
    settings.setdefault("polan_olt_failover", True)
    settings.setdefault("default_poe_power_w", 0.0)
    settings.setdefault("default_expected_bandwidth_mbps", 0.0)
    settings.setdefault("default_expected_packet_rate_pps", 0.0)
    settings.setdefault("poe_power_defaults", {})
    settings.setdefault("default_rack_size_u", 42)
    settings.setdefault("default_fibre_core_count", 12)
    settings.setdefault("ip_plan_base_cidr", "10.0.0.0/8")
    settings.setdefault("physical_fibre_layer", fibre_layer_defaults())
    technology = _text(settings.get("technology")) or "Traditional"
    settings["technology"] = "PoLAN" if technology.lower() == "polan" else "Traditional"
    settings["expected_mer_count"] = max(
        1, _as_int(settings.get("expected_mer_count"), 2)
    )
    settings["redundant_core"] = bool(settings.get("redundant_core", True))
    topology_model = _text(settings.get("topology_model")).lower()
    settings["topology_model"] = (
        topology_model if topology_model in NETWORK_TOPOLOGY_MODELS else "collapsed_core"
    )
    settings["independent_link_count"] = max(
        1, min(16, _as_int(settings.get("independent_link_count"), 2))
    )
    settings["layer_connection_rules"] = normalise_layer_connection_rules(
        settings.get("layer_connection_rules"),
        settings["topology_model"],
        settings["redundant_core"],
        settings["independent_link_count"],
    )
    settings["manufacturer_preferences"] = normalise_manufacturer_preferences(
        settings.get("manufacturer_preferences")
    )
    settings["auto_connect_new_manual_devices"] = bool(
        settings.get("auto_connect_new_manual_devices", True)
    )
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
    settings["default_expected_bandwidth_mbps"] = max(
        0.0, _as_float(settings.get("default_expected_bandwidth_mbps"), 0.0)
    )
    settings["default_expected_packet_rate_pps"] = max(
        0.0, _as_float(settings.get("default_expected_packet_rate_pps"), 0.0)
    )
    settings["default_rack_size_u"] = max(
        1, _as_int(settings.get("default_rack_size_u"), 42)
    )
    settings["default_fibre_core_count"] = max(2, _as_int(settings.get("default_fibre_core_count"), 12))
    settings["ip_plan_base_cidr"] = _text(settings.get("ip_plan_base_cidr")) or "10.0.0.0/8"
    layer_settings = settings.get("physical_fibre_layer")
    if not isinstance(layer_settings, dict):
        layer_settings = {}
    merged_layer_settings = fibre_layer_defaults()
    merged_layer_settings.update(layer_settings)
    for flag in ("visible", "show_base_graph", "show_route_nodes", "show_core_counts", "show_dark_fibre", "show_splice_labels"):
        merged_layer_settings[flag] = bool(merged_layer_settings.get(flag, True))
    for field in ("name", "dxf_layer_prefix", "cable_layer", "node_layer", "splice_layer", "label_layer"):
        merged_layer_settings[field] = _text(merged_layer_settings.get(field)) or fibre_layer_defaults()[field]
    merged_layer_settings["symbol_scale"] = max(
        0.08, min(2.0, _as_float(merged_layer_settings.get("symbol_scale"), 0.32))
    )
    merged_layer_settings["label_scale"] = max(
        0.2, min(2.0, _as_float(merged_layer_settings.get("label_scale"), 0.55))
    )
    merged_layer_settings["cable_width_scale"] = max(
        0.2, min(2.0, _as_float(merged_layer_settings.get("cable_width_scale"), 0.55))
    )
    settings["physical_fibre_layer"] = merged_layer_settings
    if not isinstance(settings.get("poe_power_defaults"), dict):
        settings["poe_power_defaults"] = {}

    for key in (
        "network_assets",
        "network_asset_instances",
        "network_racks",
        "network_connections",
        "network_endpoint_assignments",
        "network_patch_leads",
        "network_redundancy_groups",
        "network_vlans",
        "network_routes",
        "network_ip_allocations",
        "network_external_networks",
        "network_fibre_cables",
        "network_fibre_nodes",
        "network_fibre_splices",
    ):
        if not isinstance(data.get(key), list):
            data[key] = []

    if not isinstance(data.get("network_design_summary"), dict):
        data["network_design_summary"] = {}

    # General project assets are the traffic-producing endpoints consumed by
    # the auto-planner. Store expected load on the existing asset records so
    # room-type quantities automatically contribute to switch/router demand.
    for endpoint_asset in data.get("assets", []):
        if not isinstance(endpoint_asset, dict):
            continue
        endpoint_asset["expected_bandwidth_mbps"] = max(
            0.0, _as_float(endpoint_asset.get("expected_bandwidth_mbps"))
        )
        endpoint_asset["expected_packet_rate_pps"] = max(
            0.0, _as_float(endpoint_asset.get("expected_packet_rate_pps"))
        )

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
        if asset["asset_type"] == "fibre_splitter":
            split_ratio, split_inputs, split_outputs = _normalise_split_ratio(
                asset.get("split_ratio")
            )
            if split_ratio:
                asset["split_ratio"] = split_ratio
                asset["split_input_count"] = split_inputs
                asset["split_output_count"] = split_outputs
            else:
                asset["split_input_count"] = max(0, _as_int(asset.get("split_input_count")))
                asset["split_output_count"] = max(0, _as_int(asset.get("split_output_count")))
        else:
            asset["split_input_count"] = 0
            asset["split_output_count"] = 0
        asset["frequencies"] = _normalise_string_list(asset.get("frequencies", []))
        asset["power_input_w"] = max(0.0, _as_float(asset.get("power_input_w")))
        asset["poe_budget_w"] = max(0.0, _as_float(asset.get("poe_budget_w")))
        asset["bandwidth_capacity_gbps"] = max(
            0.0,
            _as_float(
                asset.get(
                    "bandwidth_capacity_gbps",
                    asset.get(
                        "switching_capacity_gbps",
                        asset.get("routing_capacity_gbps", 0.0),
                    ),
                )
            ),
        )
        asset["packet_throughput_mpps"] = max(
            0.0,
            _as_float(
                asset.get(
                    "packet_throughput_mpps",
                    asset.get(
                        "packet_forwarding_rate_mpps",
                        asset.get("forwarding_rate_mpps", 0.0),
                    ),
                )
            ),
        )
        asset["expected_bandwidth_mbps"] = max(
            0.0, _as_float(asset.get("expected_bandwidth_mbps"))
        )
        asset["expected_packet_rate_pps"] = max(
            0.0, _as_float(asset.get("expected_packet_rate_pps"))
        )
        asset["number_of_ports"] = max(0, _as_int(asset.get("number_of_ports")))
        asset["connections_in"] = max(0, _as_int(asset.get("connections_in")))
        asset["connections_out"] = max(0, _as_int(asset.get("connections_out")))
        asset["uplink_ports"] = max(0, _as_int(asset.get("uplink_ports")))
        if asset["asset_type"] == "fibre_splitter" and asset["split_input_count"] > 0:
            asset["connections_in"] = asset["split_input_count"]
            asset["connections_out"] = asset["split_output_count"]
            asset["number_of_ports"] = (
                asset["split_input_count"] + asset["split_output_count"]
            )
            asset["input_connection_type"] = "fibre"
            asset["output_connection_type"] = "fibre"
            asset["uplink_connection_type"] = "fibre"
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

        # Structured physical ports are authoritative. Fibre splitters use a
        # canonical input/output layout so observed connection aliases never
        # create duplicate sockets in rack or device views.
        raw_port_definitions = asset.get("port_definitions", [])
        if not isinstance(raw_port_definitions, list):
            raw_port_definitions = []
        port_definitions = []
        for row in raw_port_definitions:
            if not isinstance(row, dict):
                continue
            port_type = _text(row.get("port_type")).lower() or "other"
            port_use = _text(row.get("port_use")).lower() or "other"
            names = row.get("explicit_names", [])
            if not isinstance(names, list):
                names = []
            port_definitions.append(
                {
                    "port_type": port_type if port_type in NETWORK_PORT_TYPES else "other",
                    "port_count": max(0, _as_int(row.get("port_count"))),
                    "port_use": port_use if port_use in NETWORK_PORT_USES else "other",
                    "name_prefix": _text(row.get("name_prefix")),
                    "explicit_names": [_text(value) for value in names if _text(value)],
                }
            )
        port_definitions = [row for row in port_definitions if row["port_count"] > 0]

        if asset["asset_type"] == "fibre_splitter" and asset["split_input_count"] > 0:
            input_names = (
                ["Input-1"]
                if asset["split_input_count"] == 1
                else ["Input-A", "Input-B"]
            )
            port_definitions = [
                {
                    "port_type": "lc",
                    "port_count": asset["split_input_count"],
                    "port_use": "input",
                    "name_prefix": "Input",
                    "explicit_names": input_names,
                },
                {
                    "port_type": "lc",
                    "port_count": asset["split_output_count"],
                    "port_use": "output",
                    "name_prefix": "Output",
                    "explicit_names": [],
                },
            ]
        elif not port_definitions and asset["number_of_ports"] > 0:
            default_type = (
                "pon"
                if asset["asset_type"] in {"optical_line_terminal", "optical_network_terminal"}
                else "lc"
                if asset.get("patch_panel_type") == "fibre"
                else "rj45"
            )
            default_use = "patch" if asset["asset_type"] == "patch_panel" else "client"
            port_definitions = [
                {
                    "port_type": default_type,
                    "port_count": asset["number_of_ports"],
                    "port_use": default_use,
                    "name_prefix": "",
                    "explicit_names": [],
                }
            ]
        asset["port_definitions"] = port_definitions
        if port_definitions:
            asset["number_of_ports"] = sum(row["port_count"] for row in port_definitions)

    # Recover planner-owned PoLAN placement rows if an older save path kept
    # generated instances but omitted their generated locations.  The instance
    # already carries the authoritative floor and coordinates, so recreating
    # AUTO-POLAN locations is deterministic and avoids false validation errors.
    existing_location_names = {
        _text(item.get("name"))
        for item in data.get("locations", [])
        if isinstance(item, dict) and _text(item.get("name"))
    }
    for instance in data.get("network_asset_instances", []):
        if not isinstance(instance, dict):
            continue
        location_name = _text(instance.get("location_name"))
        if (
            not location_name
            or not location_name.startswith("AUTO-POLAN-")
            or location_name in existing_location_names
        ):
            continue
        data["locations"].append(
            {
                "name": location_name,
                "floor": _as_int(instance.get("floor")),
                "x": _as_float(instance.get("x")),
                "y": _as_float(instance.get("y")),
                "kind": "polan",
                "department_id": "",
                "department_ids": [],
                "anchor_point_name": _text(instance.get("route_anchor")),
                "auto_network_location": True,
                "recovered_from_instance": True,
            }
        )
        existing_location_names.add(location_name)

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
        layer = _text(instance.get("network_layer") or instance.get("design_layer")).lower()
        layer = {
            "distribution": "aggregation",
            "distribution_switch": "aggregation",
            "aggregation_switch": "aggregation",
            "access_switch": "access",
            "core_switch": "core",
            "edge_router": "edge",
        }.get(layer, layer)
        instance["network_layer"] = (
            layer
            if layer in {"", "edge", "core", "aggregation", "access", "endpoint", "olt", "splitter", "ont"}
            else ""
        )
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
        instance.setdefault("external_network_id", "")
        if not isinstance(instance.get("router_ip_addresses"), list):
            instance["router_ip_addresses"] = []

    rack_ids: set[str] = set()
    for rack in data["network_racks"]:
        if not isinstance(rack, dict):
            continue
        rack.setdefault("id", "")
        rack.setdefault("name", rack.get("id", ""))
        rack.setdefault("location_name", "")
        linked_location = locations.get(_text(rack.get("location_name")), {})
        rack["floor"] = _as_int(rack.get("floor", linked_location.get("floor", 0)))
        rack["capacity_u"] = max(
            1, _as_int(rack.get("capacity_u"), settings["default_rack_size_u"])
        )
        rack.setdefault("manufacturer", "")
        rack.setdefault("model", "")
        rack.setdefault("notes", "")
        rack["auto_generated"] = bool(rack.get("auto_generated", False))
        rack_id = _text(rack.get("id"))
        if rack_id:
            rack_ids.add(rack_id)

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
        connection["auto_connected"] = bool(connection.get("auto_connected", False))
        connection["topology_hidden"] = bool(connection.get("topology_hidden", False))
        connection["physical_connection"] = bool(connection.get("physical_connection", False))
        connection.setdefault("physical_segment", "")
        connection.setdefault("parent_logical_connection_id", "")

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
        assignment.setdefault("physical_patch_panel_instance_id", "")
        assignment.setdefault("physical_patch_panel_port", "")
        assignment.setdefault("horizontal_cable_from_instance_id", "")
        assignment.setdefault("horizontal_cable_from_port", "")
        assignment.setdefault("horizontal_cable_to_endpoint", assignment.get("endpoint_name", ""))
        assignment.setdefault("horizontal_cable_to_port", assignment.get("endpoint_port", ""))
        assignment.setdefault("physical_termination_type", "")
        assignment["poe_power_w"] = max(0.0, _as_float(assignment.get("poe_power_w")))
        assignment["expected_bandwidth_mbps"] = max(
            0.0, _as_float(assignment.get("expected_bandwidth_mbps"))
        )
        assignment["expected_packet_rate_pps"] = max(
            0.0, _as_float(assignment.get("expected_packet_rate_pps"))
        )
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
            "source_layer",
            "target_layer",
        ):
            group.setdefault(field, "")
        group["source_instance_ids"] = _normalise_string_list(
            group.get("source_instance_ids", [])
        )
        group["source_core_instance_ids"] = _normalise_string_list(
            group.get("source_core_instance_ids", [])
        )
        group["required_distinct_sources"] = max(
            1, _as_int(group.get("required_distinct_sources"), 1)
        )
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
            "subnet_mask",
            "gateway",
            "dhcp_scope",
            "dhcp_start",
            "dhcp_end",
            "security_zone",
            "notes",
        ):
            vlan.setdefault(field, "")
        vlan["requested_hosts"] = max(2, _as_int(vlan.get("requested_hosts"), 254))
        vlan["prefix_length"] = max(0, min(32, _as_int(vlan.get("prefix_length"), 0)))

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
        route["auto_generated"] = bool(route.get("auto_generated", False))

    for allocation in data["network_ip_allocations"]:
        if not isinstance(allocation, dict):
            continue
        for field in ("id", "instance_id", "vlan_id", "address", "gateway", "purpose", "notes"):
            allocation.setdefault(field, "")
        allocation["prefix_length"] = max(0, min(32, _as_int(allocation.get("prefix_length"), 0)))
        allocation["auto_generated"] = bool(allocation.get("auto_generated", False))

    for external in data["network_external_networks"]:
        if not isinstance(external, dict):
            continue
        for field in ("id", "name", "network_type", "provider", "asn", "location_name", "demarcation_instance_id", "notes"):
            external.setdefault(field, "")
        external["prefixes"] = _normalise_string_list(external.get("prefixes", []))
        external["peer_instance_ids"] = _normalise_string_list(external.get("peer_instance_ids", []))

    for node in data["network_fibre_nodes"]:
        if not isinstance(node, dict):
            continue
        for field in ("id", "name", "node_type", "location_name", "rack_name", "parent_node_id", "linked_instance_id", "drawing_layer", "symbol", "label", "notes"):
            node.setdefault(field, "")
        node["floor"] = _as_int(node.get("floor"))
        node["x"] = _as_float(node.get("x"))
        node["y"] = _as_float(node.get("y"))
        node["rack_start_u"] = max(0, _as_int(node.get("rack_start_u")))
        node["rack_units"] = max(0, _as_int(node.get("rack_units"), 1))
        node["cassette_capacity"] = max(0, _as_int(node.get("cassette_capacity"), 12))
        node["splice_capacity"] = max(0, _as_int(node.get("splice_capacity"), node["cassette_capacity"]))
        node["drawing_layer"] = _text(node.get("drawing_layer")) or settings["physical_fibre_layer"]["node_layer"]
        node["symbol"] = _text(node.get("symbol")) or node.get("node_type", "fibre_joint")
        node["label"] = _text(node.get("label")) or _text(node.get("name"))
        node["auto_generated"] = bool(node.get("auto_generated", False))

    for cable in data["network_fibre_cables"]:
        if not isinstance(cable, dict):
            continue
        for field in ("id", "name", "cable_type", "from_instance_id", "from_port", "to_instance_id", "to_port", "from_location", "to_location", "installation_status", "owner", "drawing_layer", "sheath_colour", "label", "notes"):
            cable.setdefault(field, "")
        cable["route_path"] = _normalise_string_list(cable.get("route_path", []))
        cable["logical_connection_ids"] = _normalise_string_list(cable.get("logical_connection_ids", []))
        cable["splice_ids"] = _normalise_string_list(cable.get("splice_ids", []))
        cable["length_m"] = max(0.0, _as_float(cable.get("length_m")))
        cable["slack_length_m"] = max(0.0, _as_float(cable.get("slack_length_m")))
        cable["diameter_mm"] = max(0.0, _as_float(cable.get("diameter_mm")))
        cable["drawing_layer"] = _text(cable.get("drawing_layer")) or settings["physical_fibre_layer"]["cable_layer"]
        cable["sheath_colour"] = _text(cable.get("sheath_colour")) or "Black"
        cable["label"] = _text(cable.get("label")) or _text(cable.get("name"))
        cable["core_count"] = max(1, _as_int(cable.get("core_count"), settings["default_fibre_core_count"]))
        cable["cores"] = build_fibre_cores(cable["core_count"], 0, "", cable.get("cores", []))
        cable["auto_generated"] = bool(cable.get("auto_generated", False))

    for splice in data["network_fibre_splices"]:
        if not isinstance(splice, dict):
            continue
        for field in ("id", "node_id", "cassette_id", "incoming_cable_id", "outgoing_cable_id", "splice_type", "circuit_id", "drawing_layer", "label", "loss_db", "notes"):
            splice.setdefault(field, "")
        splice["incoming_core"] = max(1, _as_int(splice.get("incoming_core"), 1))
        splice["outgoing_core"] = max(1, _as_int(splice.get("outgoing_core"), 1))
        splice["loss_db"] = max(0.0, _as_float(splice.get("loss_db"), 0.1))
        splice["drawing_layer"] = _text(splice.get("drawing_layer")) or settings["physical_fibre_layer"]["splice_layer"]
        splice["label"] = _text(splice.get("label")) or _text(splice.get("id"))
        splice["auto_generated"] = bool(splice.get("auto_generated", False))

    set_core_status_from_splices(data)
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
    validate_unique_id("network_ip_allocations", "IP allocation")
    validate_unique_id("network_external_networks", "External network")
    fibre_cable_ids = validate_unique_id("network_fibre_cables", "Fibre cable")
    fibre_node_ids = validate_unique_id("network_fibre_nodes", "Fibre node")
    fibre_splice_ids = validate_unique_id("network_fibre_splices", "Fibre splice")

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
        if asset_type == "fibre_splitter":
            split_ratio, split_inputs, split_outputs = _normalise_split_ratio(
                asset.get("split_ratio")
            )
            if not split_ratio:
                messages.append(
                    f"Fibre splitter {asset_id} requires a valid 1:x or 2:x split ratio."
                )
            elif split_inputs not in {1, 2} or split_outputs < 1:
                messages.append(
                    f"Fibre splitter {asset_id} has an unsupported split ratio {asset.get('split_ratio')!r}."
                )
        if asset_type == "wireless_access_point" and not asset.get("frequencies"):
            messages.append(
                f"Wireless access point {asset_id} requires at least one frequency."
            )

    seen_rack_ids: set[str] = set()
    seen_rack_names: set[tuple[int, str, str]] = set()
    for rack in data.get("network_racks", []):
        if not isinstance(rack, dict):
            continue
        rack_id = _text(rack.get("id")) or "(unnamed)"
        if rack_id in seen_rack_ids:
            messages.append(f"Rack cabinet ID {rack_id!r} is duplicated.")
        seen_rack_ids.add(rack_id)
        location_name = _text(rack.get("location_name"))
        if location_name and location_name not in locations:
            messages.append(f"Rack cabinet {rack_id} references missing location {location_name!r}.")
        capacity = _as_int(rack.get("capacity_u"))
        if capacity < 1:
            messages.append(f"Rack cabinet {rack_id} must have a positive rack-unit capacity.")
        name_key = (_as_int(rack.get("floor")), location_name, _text(rack.get("name")).lower())
        if name_key in seen_rack_names:
            messages.append(
                f"Rack cabinet name {_text(rack.get('name'))!r} is duplicated at {location_name or 'the unassigned location'}."
            )
        seen_rack_names.add(name_key)

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

    rack_groups: Dict[tuple[int, str, str], List[tuple[str, int, int]]] = {}
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
        rack_groups.setdefault(key, []).append((instance_id, start_u, end_u))
        explicit_size = _as_int(instance.get("rack_size_u"))
        if explicit_size > 0:
            rack_sizes[key] = max(rack_sizes.get(key, 0), explicit_size)

    for key, rows in rack_groups.items():
        capacity = rack_sizes.get(key, default_rack_size)
        floor, location, rack = key
        rack_label = f"{location or 'Floor ' + str(floor)} / {rack}"
        for instance_id, start_u, end_u in rows:
            if end_u > capacity:
                messages.append(
                    f"Rack {rack_label} capacity is {capacity}U but {instance_id} occupies U{start_u}-U{end_u}."
                )
        ordered = sorted(rows, key=lambda row: (row[1], row[2], row[0]))
        for index, (instance_id, start_u, end_u) in enumerate(ordered):
            for other_id, other_start, other_end in ordered[index + 1 :]:
                if other_start > end_u:
                    break
                if other_end >= start_u:
                    messages.append(
                        f"Rack {rack_label} has overlapping rack units: {instance_id} U{start_u}-U{end_u} and {other_id} U{other_start}-U{other_end}."
                    )

    used_endpoints: set[tuple[str, str]] = set()
    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict):
            continue
        connection_id = _text(connection.get("id")) or "(unnamed)"
        is_physical_hidden = bool(connection.get("topology_hidden")) or bool(connection.get("physical_connection"))
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
            # A logical link and its hidden physical patching legitimately use
            # the same active-device port. Physical front/back panel
            # terminations are also represented separately, so only logical
            # topology connections participate in duplicate-endpoint checks.
            if not is_physical_hidden:
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
    bandwidth_loads: Dict[str, float] = {}
    packet_loads: Dict[str, float] = {}
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

        patch_panel_id = _text(assignment.get("physical_patch_panel_instance_id"))
        patch_panel_port = _text(assignment.get("physical_patch_panel_port"))
        if patch_panel_id:
            panel_instance = instances_by_id.get(patch_panel_id)
            if panel_instance is None:
                messages.append(
                    f"Endpoint assignment {assignment_id} references missing copper patch panel {patch_panel_id!r}."
                )
            else:
                panel_asset = assets_by_id.get(_text(panel_instance.get("asset_id")), {})
                if (
                    _text(panel_asset.get("asset_type")) != "patch_panel"
                    or _text(panel_asset.get("patch_panel_type")).lower() != "copper"
                ):
                    messages.append(
                        f"Endpoint assignment {assignment_id} physical termination {patch_panel_id!r} "
                        "is not a copper patch panel."
                    )
            if not patch_panel_port:
                messages.append(
                    f"Endpoint assignment {assignment_id} has a copper patch panel but no panel port."
                )
        elif patch_panel_port:
            messages.append(
                f"Endpoint assignment {assignment_id} has patch-panel port {patch_panel_port!r} "
                "but no patch-panel instance."
            )
        port_loads[instance_id] = port_loads.get(instance_id, 0) + 1
        poe_loads[instance_id] = poe_loads.get(instance_id, 0.0) + max(
            0.0, _as_float(assignment.get("poe_power_w"))
        )
        bandwidth_loads[instance_id] = bandwidth_loads.get(instance_id, 0.0) + max(
            0.0, _as_float(assignment.get("expected_bandwidth_mbps"))
        )
        packet_loads[instance_id] = packet_loads.get(instance_id, 0.0) + max(
            0.0, _as_float(assignment.get("expected_packet_rate_pps"))
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
        bandwidth_capacity = (
            max(0.0, _as_float(asset.get("bandwidth_capacity_gbps")))
            * 1000.0
            * stack_members
        )
        packet_capacity = (
            max(0.0, _as_float(asset.get("packet_throughput_mpps")))
            * 1_000_000.0
            * stack_members
        )
        bandwidth_load = bandwidth_loads.get(instance_id, 0.0)
        packet_load = packet_loads.get(instance_id, 0.0)
        if bandwidth_capacity and bandwidth_load > bandwidth_capacity + 1e-9:
            messages.append(
                f"Network instance {instance_id} has {bandwidth_load:.3f} Mbps expected traffic "
                f"but only {bandwidth_capacity:.3f} Mbps switching/routing capacity."
            )
        if packet_capacity and packet_load > packet_capacity + 1e-9:
            messages.append(
                f"Network instance {instance_id} has {packet_load:.0f} packets/s expected traffic "
                f"but only {packet_capacity:.0f} packets/s throughput."
            )

    # Core, aggregation and router devices carry descendant traffic even when
    # they have no directly assigned endpoints. Validate their aggregated load.
    try:
        from network_services import network_traffic_loads

        carried = network_traffic_loads(data).get("carried_by_instance", {})
        for instance_id, load in carried.items():
            instance = instances_by_id.get(instance_id, {})
            asset = assets_by_id.get(_text(instance.get("asset_id")), {})
            stack_members = (
                max(1, _as_int(instance.get("stack_member_count"), 1))
                if bool(instance.get("logical_stack"))
                else 1
            )
            bandwidth_capacity = (
                max(0.0, _as_float(asset.get("bandwidth_capacity_gbps")))
                * 1000.0
                * stack_members
            )
            packet_capacity = (
                max(0.0, _as_float(asset.get("packet_throughput_mpps")))
                * 1_000_000.0
                * stack_members
            )
            bandwidth_load = max(0.0, _as_float(load.get("bandwidth_mbps")))
            packet_load = max(0.0, _as_float(load.get("packet_rate_pps")))
            if bandwidth_capacity and bandwidth_load > bandwidth_capacity + 1e-9:
                messages.append(
                    f"Network instance {instance_id} carries {bandwidth_load:.3f} Mbps "
                    f"but only provides {bandwidth_capacity:.3f} Mbps switching/routing capacity."
                )
            if packet_capacity and packet_load > packet_capacity + 1e-9:
                messages.append(
                    f"Network instance {instance_id} carries {packet_load:.0f} packets/s "
                    f"but only provides {packet_capacity:.0f} packets/s throughput."
                )
    except Exception:
        # Validation must remain available in restricted runtimes even if a
        # service import fails; direct endpoint checks above still run.
        pass

    for group in data.get("network_redundancy_groups", []):
        if not isinstance(group, dict):
            continue
        group_id = _text(group.get("id")) or "(unnamed)"
        protected = _text(group.get("protected_instance_id"))
        if protected not in instance_ids:
            messages.append(
                f"Redundancy group {group_id} references missing protected instance {protected!r}."
            )
        if _text(group.get("protection_type")) == "independent_layer_uplinks":
            continue
        primary = _text(group.get("primary_olt_instance_id"))
        secondary = _text(group.get("secondary_olt_instance_id"))
        if primary not in instance_ids or secondary not in instance_ids:
            messages.append(
                f"Redundancy group {group_id} must reference both primary and secondary OLT instances."
            )
        elif primary == secondary:
            messages.append(
                f"Redundancy group {group_id} uses the same OLT for primary and secondary service."
            )

    settings = data.get("network_settings", {})
    if _text(settings.get("technology")) == "Traditional":
        topology_model = _text(settings.get("topology_model")).lower()
        enabled_pairs = {
            (
                _text(rule.get("source_layer")).lower(),
                _text(rule.get("target_layer")).lower(),
            )
            for rule in settings.get("layer_connection_rules", [])
            if isinstance(rule, dict) and bool(rule.get("enabled", True))
        }
        if topology_model == "collapsed_core" and ("core", "access") not in enabled_pairs:
            messages.append(
                "Traditional collapsed-core planning requires an enabled Core → Access layer rule."
            )
        if topology_model == "three_tier":
            if ("core", "aggregation") not in enabled_pairs:
                messages.append(
                    "Traditional three-tier planning requires an enabled Core → Aggregation layer rule."
                )
            if ("aggregation", "access") not in enabled_pairs:
                messages.append(
                    "Traditional three-tier planning requires an enabled Aggregation → Access layer rule."
                )

    for group in data.get("network_redundancy_groups", []):
        if not isinstance(group, dict):
            continue
        if _text(group.get("protection_type")) != "independent_layer_uplinks":
            continue
        required = max(1, _as_int(group.get("required_distinct_sources"), 1))
        source_ids = set(_normalise_string_list(group.get("source_instance_ids", [])))
        if len(source_ids) < required:
            messages.append(
                f"Redundancy group {_text(group.get('id')) or '(unnamed)'} requires "
                f"{required} distinct source devices but records only {len(source_ids)}."
            )
        if _text(group.get("target_layer")).lower() == "access" and required >= 2:
            core_ids = set(
                _normalise_string_list(group.get("source_core_instance_ids", []))
            )
            if len(core_ids) < 2:
                messages.append(
                    f"Redundancy group {_text(group.get('id')) or '(unnamed)'} does not "
                    "provide two different upstream core sources."
                )

    for external in data.get("network_external_networks", []):
        if not isinstance(external, dict):
            continue
        external_id = _text(external.get("id")) or "(unnamed)"
        demarcation = _text(external.get("demarcation_instance_id"))
        if demarcation and demarcation not in instance_ids:
            messages.append(f"External network {external_id} references missing demarcation instance {demarcation!r}.")
        for peer_id in external.get("peer_instance_ids", []):
            if _text(peer_id) not in instance_ids:
                messages.append(f"External network {external_id} references missing peering instance {_text(peer_id)!r}.")
        for prefix in external.get("prefixes", []):
            try:
                ipaddress.ip_network(_text(prefix), strict=False)
            except ValueError:
                messages.append(f"External network {external_id} contains invalid prefix {_text(prefix)!r}.")

    allocated_addresses: set[str] = set()
    for allocation in data.get("network_ip_allocations", []):
        if not isinstance(allocation, dict):
            continue
        allocation_id = _text(allocation.get("id")) or "(unnamed)"
        instance_id = _text(allocation.get("instance_id"))
        vlan_id = _text(allocation.get("vlan_id"))
        if instance_id and instance_id not in instance_ids:
            messages.append(f"IP allocation {allocation_id} references missing instance {instance_id!r}.")
        if vlan_id and vlan_id not in vlan_record_ids:
            messages.append(f"IP allocation {allocation_id} references missing VLAN {vlan_id!r}.")
        address = _text(allocation.get("address"))
        if address:
            try:
                ipaddress.ip_address(address)
            except ValueError:
                messages.append(f"IP allocation {allocation_id} has invalid address {address!r}.")
            if address in allocated_addresses:
                messages.append(f"IP address {address} is allocated more than once.")
            allocated_addresses.add(address)

    for cable in data.get("network_fibre_cables", []):
        if not isinstance(cable, dict):
            continue
        cable_id = _text(cable.get("id")) or "(unnamed)"
        core_count = _as_int(cable.get("core_count"))
        cores = [row for row in cable.get("cores", []) if isinstance(row, dict)]
        if core_count < 1:
            messages.append(f"Fibre cable {cable_id} must contain at least one core.")
        if len(cores) != core_count:
            messages.append(f"Fibre cable {cable_id} declares {core_count} cores but contains {len(cores)} core records.")
        if not cable.get("route_path"):
            messages.append(f"Advisory: fibre cable {cable_id} has no graph route path.")
        if not _text(cable.get("drawing_layer")):
            messages.append(f"Fibre cable {cable_id} has no physical drawing layer.")
        for field in ("from_instance_id", "to_instance_id"):
            endpoint_id = _text(cable.get(field))
            if endpoint_id and endpoint_id not in instance_ids:
                messages.append(f"Fibre cable {cable_id} references missing endpoint instance {endpoint_id!r}.")
        for logical_id in cable.get("logical_connection_ids", []):
            if _text(logical_id) not in connection_ids:
                messages.append(f"Fibre cable {cable_id} references missing logical connection {_text(logical_id)!r}.")
        numbers = [_as_int(row.get("number")) for row in cores]
        if len(numbers) != len(set(numbers)):
            messages.append(f"Fibre cable {cable_id} contains duplicate core numbers.")

    for node in data.get("network_fibre_nodes", []):
        if not isinstance(node, dict):
            continue
        node_id = _text(node.get("id")) or "(unnamed)"
        node_type = _text(node.get("node_type")).lower()
        if node_type not in {"splice_enclosure", "splice_cassette", "fibre_joint", "termination", "handhole", "chamber"}:
            messages.append(f"Fibre node {node_id} has unsupported type {node_type!r}.")
        parent = _text(node.get("parent_node_id"))
        if parent and parent not in fibre_node_ids:
            messages.append(f"Fibre node {node_id} references missing parent node {parent!r}.")
        linked_instance = _text(node.get("linked_instance_id"))
        if linked_instance and linked_instance not in instance_ids:
            messages.append(f"Fibre node {node_id} references missing linked network instance {linked_instance!r}.")
        location_name = _text(node.get("location_name"))
        if location_name and location_name not in locations:
            messages.append(f"Fibre node {node_id} references missing location {location_name!r}.")
        if not _text(node.get("drawing_layer")):
            messages.append(f"Fibre node {node_id} has no physical drawing layer.")
        splice_capacity = _as_int(node.get("splice_capacity", node.get("cassette_capacity")))
        used_splices = sum(
            1 for splice in data.get("network_fibre_splices", [])
            if _text(splice.get("node_id")) == node_id or _text(splice.get("cassette_id")) == node_id
        )
        if splice_capacity and used_splices > splice_capacity:
            messages.append(f"Fibre node {node_id} contains {used_splices} splices but has capacity for only {splice_capacity}.")

    cable_by_id = {_text(row.get("id")): row for row in data.get("network_fibre_cables", []) if isinstance(row, dict)}
    for splice in data.get("network_fibre_splices", []):
        if not isinstance(splice, dict):
            continue
        splice_id = _text(splice.get("id")) or "(unnamed)"
        node_id = _text(splice.get("node_id"))
        if node_id and node_id not in fibre_node_ids:
            messages.append(f"Fibre splice {splice_id} references missing fibre node {node_id!r}.")
        incoming = cable_by_id.get(_text(splice.get("incoming_cable_id")))
        outgoing = cable_by_id.get(_text(splice.get("outgoing_cable_id")))
        if incoming is None:
            messages.append(f"Fibre splice {splice_id} references a missing incoming cable.")
        elif _as_int(splice.get("incoming_core")) > _as_int(incoming.get("core_count")):
            messages.append(f"Fibre splice {splice_id} incoming core exceeds cable capacity.")
        if outgoing is None:
            messages.append(f"Fibre splice {splice_id} references a missing outgoing cable.")
        elif _as_int(splice.get("outgoing_core")) > _as_int(outgoing.get("core_count")):
            messages.append(f"Fibre splice {splice_id} outgoing core exceeds cable capacity.")

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
        subnet = _text(vlan.get("subnet"))
        if subnet:
            try:
                network = ipaddress.ip_network(subnet, strict=False)
                gateway = _text(vlan.get("gateway"))
                if gateway and ipaddress.ip_address(gateway) not in network:
                    messages.append(f"VLAN {_text(vlan.get('id')) or '(unnamed)'} gateway {gateway} is outside subnet {network}.")
            except ValueError:
                messages.append(f"VLAN {_text(vlan.get('id')) or '(unnamed)'} has invalid subnet {subnet!r}.")

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
