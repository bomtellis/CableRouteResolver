"""Data model helpers for the Cable Routing Solver network-planning extension."""

from __future__ import annotations

from copy import deepcopy
import ipaddress
from typing import Dict, Iterable, List, Optional

from network_services import FIBRE_COLOURS, build_fibre_cores, fibre_layer_defaults, set_core_status_from_splices


NETWORK_SCHEMA_VERSION = 13

# Cisco Catalyst 9600 line-card catalogue.  Port layouts and supervisor
# compatibility are taken from the 2025 Catalyst 9600 Series data sheet.
CATALYST_9600_LINE_CARDS = {
    "C9600X-LC-56YL4C": [("sfp56", 56, "downlink", [10000, 25000, 50000]), ("qsfp28", 4, "uplink", [40000, 100000])],
    "C9600X-LC-32CD": [("qsfp28", 30, "downlink", [40000, 100000]), ("qsfpdd", 2, "uplink", [100000, 200000, 400000])],
    "C9600-LC-40YL4CD": [("sfp56", 40, "downlink", [1000, 10000, 25000, 50000]), ("qsfp28", 2, "uplink", [40000, 100000]), ("qsfpdd", 2, "uplink", [200000, 400000])],
    "C9600-LC-24C": [("qsfp28", 24, "downlink", [40000, 100000])],
    "C9600-LC-48YL": [("sfp28", 48, "downlink", [1000, 10000, 25000, 50000])],
    "C9600-LC-48TX": [("rj45", 48, "downlink", [100, 1000, 2500, 5000, 10000])],
    "C9600-LC-48S": [("sfp", 48, "downlink", [100, 1000])],
}


def catalyst_9600_port_definitions(modules) -> List[dict]:
    """Expand installed line cards into the chassis' authoritative ports."""
    result: List[dict] = []
    if not isinstance(modules, list):
        return result
    for module in sorted(
        (row for row in modules if isinstance(row, dict)),
        key=lambda row: _as_int(row.get("slot"), 0),
    ):
        if _text(module.get("module_type")).lower() != "line_card":
            continue
        slot = max(1, _as_int(module.get("slot"), 1))
        model = _text(module.get("model"))
        for group_index, (port_type, count, port_use, speeds) in enumerate(
            CATALYST_9600_LINE_CARDS.get(model, []), start=1
        ):
            prefix = f"S{slot}-{port_type.upper()}"
            result.append(
                {
                    "port_type": port_type,
                    "port_count": count,
                    "port_use": port_use,
                    "name_prefix": prefix,
                    "explicit_names": [f"{prefix}-{number}" for number in range(1, count + 1)],
                    "supported_speeds_mbps": list(speeds),
                    "default_speed_mbps": max(speeds) if speeds else 0,
                    "chassis_slot": slot,
                    "module_model": model,
                    "module_port_group": group_index,
                }
            )
    return result


DEFAULT_FIBRE_CABLE_TYPES = [
    {
        "id": "OS2-2F",
        "name": "OS2 2-core fixed installation cable",
        "fibre_standard": "OS2",
        "core_count": 2,
        "attenuation_db_per_m": 0.00035,
        "connector_loss_db": 0.5,
        "reflection_loss_db": 55.0,
        "splice_loss_db": 0.1,
        "wavelength_nm": 1310,
        "notes": "Editable planning default for point-to-point and final spur fibre.",
    },
    {
        "id": "OS2-4F",
        "name": "OS2 4-core fixed installation cable",
        "fibre_standard": "OS2",
        "core_count": 4,
        "attenuation_db_per_m": 0.00035,
        "connector_loss_db": 0.5,
        "reflection_loss_db": 55.0,
        "splice_loss_db": 0.1,
        "wavelength_nm": 1310,
        "notes": "Editable planning default for reduced-count branch fibre.",
    },
    {
        "id": "OS2-8F",
        "name": "OS2 8-core fixed installation cable",
        "fibre_standard": "OS2",
        "core_count": 8,
        "attenuation_db_per_m": 0.00035,
        "connector_loss_db": 0.5,
        "reflection_loss_db": 55.0,
        "splice_loss_db": 0.1,
        "wavelength_nm": 1310,
        "notes": "Editable planning default for reduced-count branch fibre.",
    },
    {
        "id": "OS2-12F",
        "name": "OS2 12-core fixed installation cable",
        "fibre_standard": "OS2",
        "core_count": 12,
        "attenuation_db_per_m": 0.00035,
        "connector_loss_db": 0.5,
        "reflection_loss_db": 55.0,
        "splice_loss_db": 0.1,
        "wavelength_nm": 1310,
        "notes": "Editable planning default for fixed installation single-mode fibre.",
    },
    {
        "id": "OS2-24F",
        "name": "OS2 24-core fixed installation cable",
        "fibre_standard": "OS2",
        "core_count": 24,
        "attenuation_db_per_m": 0.00035,
        "connector_loss_db": 0.5,
        "reflection_loss_db": 55.0,
        "splice_loss_db": 0.1,
        "wavelength_nm": 1310,
        "notes": "Editable planning default for fixed installation single-mode fibre.",
    },
    {
        "id": "OS2-48F",
        "name": "OS2 48-core fixed installation cable",
        "fibre_standard": "OS2",
        "core_count": 48,
        "attenuation_db_per_m": 0.00035,
        "connector_loss_db": 0.5,
        "reflection_loss_db": 55.0,
        "splice_loss_db": 0.1,
        "wavelength_nm": 1310,
        "notes": "Editable planning default for spine fibre routes.",
    },
    {
        "id": "OS2-96F",
        "name": "OS2 96-core fixed installation cable",
        "fibre_standard": "OS2",
        "core_count": 96,
        "attenuation_db_per_m": 0.00035,
        "connector_loss_db": 0.5,
        "reflection_loss_db": 55.0,
        "splice_loss_db": 0.1,
        "wavelength_nm": 1310,
        "notes": "Editable planning default for high-capacity spine fibre routes.",
    },
]


DEFAULT_NETWORK_USAGE_PROFILES = [
    {
        "id": "light_iot_sensor",
        "name": "Light IoT sensor",
        "description": "Low-rate telemetry, status and control traffic.",
        "north_south_bandwidth_mbps": 0.25,
        "east_west_bandwidth_mbps": 0.25,
        "expected_packet_rate_pps": 100.0,
        "poe_power_w": 5.0,
    },
    {
        "id": "iot_radio_gateway",
        "name": "IoT radio gateway",
        "description": "Aggregated local radio traffic with cloud and building-system integration.",
        "north_south_bandwidth_mbps": 5.0,
        "east_west_bandwidth_mbps": 10.0,
        "expected_packet_rate_pps": 2500.0,
        "poe_power_w": 15.0,
    },
    {
        "id": "office_workstation",
        "name": "Office workstation",
        "description": "Typical productivity, collaboration and internal application use.",
        "north_south_bandwidth_mbps": 20.0,
        "east_west_bandwidth_mbps": 10.0,
        "expected_packet_rate_pps": 5000.0,
        "poe_power_w": 0.0,
    },
    {
        "id": "clinical_workstation",
        "name": "Clinical workstation",
        "description": "Clinical systems, imaging retrieval and internal application traffic.",
        "north_south_bandwidth_mbps": 25.0,
        "east_west_bandwidth_mbps": 35.0,
        "expected_packet_rate_pps": 10000.0,
        "poe_power_w": 0.0,
    },
    {
        "id": "wireless_access_point",
        "name": "Wireless access point",
        "description": "Busy enterprise access point carrying client internet and internal traffic.",
        "north_south_bandwidth_mbps": 150.0,
        "east_west_bandwidth_mbps": 100.0,
        "expected_packet_rate_pps": 40000.0,
        "poe_power_w": 25.0,
    },
    {
        "id": "hd_ip_camera",
        "name": "HD IP camera",
        "description": "Continuous local video stream to an NVR with limited external traffic.",
        "north_south_bandwidth_mbps": 0.5,
        "east_west_bandwidth_mbps": 8.0,
        "expected_packet_rate_pps": 3000.0,
        "poe_power_w": 12.0,
    },
    {
        "id": "voip_phone",
        "name": "VoIP telephone",
        "description": "Concurrent voice signalling and media allowance.",
        "north_south_bandwidth_mbps": 0.15,
        "east_west_bandwidth_mbps": 0.15,
        "expected_packet_rate_pps": 200.0,
        "poe_power_w": 7.0,
    },
    {
        "id": "application_server",
        "name": "Application server",
        "description": "Server workload with substantial internal east-west traffic.",
        "north_south_bandwidth_mbps": 250.0,
        "east_west_bandwidth_mbps": 1000.0,
        "expected_packet_rate_pps": 250000.0,
        "poe_power_w": 0.0,
    },
]


def default_physical_fibre_planning() -> dict:
    return {
        "routing_mode": "direct",
        "default_cable_type_id": "OS2-2F",
        "spine_cable_type_id": "OS2-48F",
        "spur_cable_type_id": "OS2-2F",
        "branch_termination_method": "spliced",
        "splitter_termination_method": "connectorised",
        "max_splices_per_cassette": 24,
        "spare_core_percent": 15.0,
        "minimum_optical_margin_db": 3.0,
        "splitter_pigtail": True,
        "extend_reduced_count_spine": True,
        "mpo_breakout_minimum_cores": 48,
        "mpo_breakout_connector": "mpo-24",
    }

NETWORK_DEFAULTS = {
    "network_settings": {
        "technology": "Traditional",
        "expected_mer_count": 2,
        "redundant_core": True,
        "topology_model": "collapsed_core",
        "independent_link_count": 2,
        "access_stacking_enabled": True,
        "access_stack_max_members": 8,
        "access_stack_topology": "ring",
        "layer_connection_rules": [],
        "manufacturer_preferences": {},
        "asset_model_preferences": {},
        "rack_deployment_model": "end_of_row",
        "aggregation_rack_mode": "dedicated",
        "tor_keep_final_connections_in_cabinet": True,
        "tor_allow_adjacent_cabinet_uplinks": True,
        "auto_connect_new_manual_devices": True,
        "auto_add_switches_for_bandwidth": True,
        "auto_planner_connected_data_points_only": False,
        "prevent_additional_equipment_rooms": False,
        "ignore_link_bandwidth_errors": False,
        "auto_planner_resolution_overrides": {},
        "spare_capacity_percent": 15.0,
        "traditional_max_copper_m": 90.0,
        "polan_max_ont_copper_m": 30.0,
        "polan_max_onts_per_splitter": 8,
        "polan_max_splitter_ont_route_m": 120.0,
        "polan_olt_failover": True,
        "external_network_redundancy": True,
        "external_network_link_count": 2,
        "default_poe_power_w": 0.0,
        "default_expected_bandwidth_mbps": 0.0,
        "default_north_south_bandwidth_mbps": 0.0,
        "default_east_west_bandwidth_mbps": 0.0,
        "default_expected_packet_rate_pps": 0.0,
        "poe_power_defaults": {},
        "default_rack_size_u": 42,
        "default_fibre_core_count": 12,
        "default_pdu_outlet_count": 42,
        "default_pdu_capacity_w": 7360.0,
        "auto_dual_power_critical_devices": True,
        "ip_plan_base_cidr": "10.0.0.0/8",
        "physical_fibre_layer": fibre_layer_defaults(),
        "physical_fibre_planning": default_physical_fibre_planning(),
    },
    "network_assets": [],
    "network_asset_instances": [],
    "network_racks": [],
    "network_connections": [],
    "network_power_connections": [],
    "network_endpoint_assignments": [],
    "network_patch_leads": [],
    "network_redundancy_groups": [],
    "network_vlans": [],
    "network_routes": [],
    "network_ip_allocations": [],
    "network_external_networks": [],
    "network_usage_profiles": deepcopy(DEFAULT_NETWORK_USAGE_PROFILES),
    "network_optic_modules": [],
    "network_optical_paths": [],
    "network_fibre_cable_types": deepcopy(DEFAULT_FIBRE_CABLE_TYPES),
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
    "wireless_device",
    "optical_line_terminal",
    "optical_network_terminal",
    "optical_transceiver",
    "ups",
    "pdu",
    "power_device",
    "cable_management",
    "cable_manager",
    "telco_pop",
    "external_network",
    "other",
}

NETWORK_ASSET_GROUPS = {
    "router",
    "core",
    "aggregation",
    "access",
    "switching",
    "wireless",
    "optical",
    "patching",
    "power",
    "cable_management",
    "external",
    "other",
}


def network_asset_group(asset: dict) -> str:
    """Return a stable library group, including for older ungrouped assets."""

    explicit = _text(asset.get("asset_group")).lower().replace("-", "_").replace(" ", "_")
    explicit = {
        "edge": "router",
        "routing": "router",
        "firewall": "router",
        "security": "router",
        "distribution": "aggregation",
        "distribution_switch": "aggregation",
        "aggregation_switch": "aggregation",
        "core_switch": "core",
        "access_switch": "access",
        "wireless_device": "wireless",
        "wireless_access_point": "wireless",
        "patch_panel": "patching",
        "optics": "optical",
        "power_device": "power",
        "external_network": "external",
    }.get(explicit, explicit)
    if explicit in NETWORK_ASSET_GROUPS:
        return explicit

    asset_type = _text(asset.get("asset_type")).lower()
    layer = _text(asset.get("network_layer") or asset.get("design_layer")).lower()
    layer = {
        "distribution": "aggregation",
        "distribution_switch": "aggregation",
        "aggregation_switch": "aggregation",
        "core_switch": "core",
        "access_switch": "access",
        "edge_router": "router",
        "edge": "router",
    }.get(layer, layer)
    if asset_type == "network_switch":
        if layer in {"core", "aggregation", "access"}:
            return layer
        name = _text(asset.get("name")).lower()
        if "aggregation" in name or "distribution" in name:
            return "aggregation"
        if "core" in name:
            return "core"
        if "access" in name:
            return "access"
        return "switching"
    if asset_type in {"network_router", "firewall"}:
        return "router"
    if asset_type in {"wireless_access_point", "wireless_device"}:
        return "wireless"
    if asset_type in {
        "fibre_splitter",
        "optical_line_terminal",
        "optical_network_terminal",
        "optical_transceiver",
    }:
        return "optical"
    if asset_type == "patch_panel":
        return "patching"
    if asset_type in {"ups", "pdu", "power_device"}:
        return "power"
    if asset_type in {"cable_management", "cable_manager"}:
        return "cable_management"
    if asset_type in {"telco_pop", "external_network"}:
        return "external"
    return "other"

NETWORK_MEDIA = {"copper", "fibre", "wireless", "virtual", "stacking", "none"}
NETWORK_CONNECTION_ROLES = {"input", "output", "uplink"}
NETWORK_LOCATION_TYPES = {"mer", "polan", "telco_pop", "external_network", "fibre_joint"}

NETWORK_PORT_TYPES = {"rj45", "sfp", "sfp+", "sfp28", "sfp56", "qsfp", "qsfp+", "qsfp28", "qsfp56", "qsfpdd", "osfp", "pon", "lc", "sc", "mpo", "usb", "console", "power", "other"}
NETWORK_PORT_USES = {"input", "output", "uplink", "downlink", "management", "console", "pon", "client", "patch", "stacking", "power", "spare", "other"}

MODULAR_PANEL_FRONT_CONNECTORS = {
    "lc_duplex": {"port_type": "lc", "label": "LC", "fibres_per_position": 2},
    "sc_simplex": {"port_type": "sc", "label": "SC", "fibres_per_position": 1},
    "sc_duplex": {"port_type": "sc", "label": "SC", "fibres_per_position": 2},
}
MODULAR_PANEL_TERMINATION_MODES = {"spliced", "connectorised"}
MODULAR_PANEL_REAR_CONNECTORS = {
    "mpo-12": 12,
    "mtp-12": 12,
    "mpo-24": 24,
    "mtp-24": 24,
}


NETWORK_PORT_SPEED_OPTIONS = [
    (10, "10 Mb/s"),
    (100, "100 Mb/s"),
    (1_000, "1 Gb/s"),
    (2_500, "2.5 Gb/s"),
    (5_000, "5 Gb/s"),
    (10_000, "10 Gb/s"),
    (25_000, "25 Gb/s"),
    (40_000, "40 Gb/s"),
    (50_000, "50 Gb/s"),
    (100_000, "100 Gb/s"),
    (200_000, "200 Gb/s"),
    (400_000, "400 Gb/s"),
    (800_000, "800 Gb/s"),
]

PLUGGABLE_OPTIC_PORT_TYPES = {
    "sfp", "sfp+", "sfp28", "sfp56", "qsfp", "qsfp+", "qsfp28",
    "qsfp56", "qsfpdd", "osfp",
}

_DEFAULT_PORT_SPEEDS = {
    "rj45": [10, 100, 1_000],
    "sfp": [100, 1_000],
    "sfp+": [1_000, 10_000],
    "sfp28": [1_000, 10_000, 25_000],
    "sfp56": [1_000, 10_000, 25_000, 50_000],
    "qsfp": [40_000],
    "qsfp+": [40_000],
    "qsfp28": [40_000, 100_000],
    "qsfp56": [50_000, 100_000, 200_000],
    "qsfpdd": [100_000, 200_000, 400_000, 800_000],
    "osfp": [100_000, 200_000, 400_000, 800_000],
    "pon": [1_000, 2_500, 10_000, 25_000],
}


def normalise_port_speeds(values) -> List[int]:
    """Return canonical Mbps values from lists or legacy comma-separated text."""
    if values is None:
        return []
    if isinstance(values, (int, float)):
        values = [values]
    elif isinstance(values, str):
        values = values.replace(";", ",").split(",")
    result: List[int] = []
    for value in values:
        text = _text(value).lower().replace("gbps", "g").replace("mbps", "m").replace(" ", "")
        try:
            if text.endswith("g"):
                speed = int(round(float(text[:-1]) * 1000.0))
            elif text.endswith("m"):
                speed = int(round(float(text[:-1])))
            else:
                speed = int(round(float(text)))
        except (TypeError, ValueError):
            continue
        if speed > 0 and speed not in result:
            result.append(speed)
    return sorted(result)


def default_port_speeds(port_type: str) -> List[int]:
    return list(_DEFAULT_PORT_SPEEDS.get(_text(port_type).lower(), []))


def port_speed_label(speed_mbps: int) -> str:
    speed = max(0, _as_int(speed_mbps))
    if speed >= 1000 and speed % 1000 == 0:
        return f"{speed // 1000} Gb/s"
    if speed >= 1000:
        return f"{speed / 1000.0:g} Gb/s"
    return f"{speed} Mb/s"


def compatible_port_speeds(left, right) -> List[int]:
    """Intersect declared speeds; an empty passive side is speed-transparent."""
    left_values = normalise_port_speeds(left)
    right_values = normalise_port_speeds(right)
    if left_values and right_values:
        return sorted(set(left_values) & set(right_values))
    return left_values or right_values


def optic_form_factors_for_cage(port_type: str) -> List[str]:
    cage = _text(port_type).lower()
    order = ["sfp", "sfp+", "sfp28", "sfp56", "qsfp", "qsfp+", "qsfp28", "qsfp56", "qsfpdd", "osfp"]
    compatibility = {
        "sfp": {"sfp"},
        "sfp+": {"sfp", "sfp+"},
        "sfp28": {"sfp", "sfp+", "sfp28"},
        "sfp56": {"sfp", "sfp+", "sfp28", "sfp56"},
        "qsfp": {"qsfp"},
        "qsfp+": {"qsfp", "qsfp+"},
        "qsfp28": {"qsfp", "qsfp+", "qsfp28"},
        "qsfp56": {"qsfp", "qsfp+", "qsfp28", "qsfp56"},
        "qsfpdd": {"qsfp", "qsfp+", "qsfp28", "qsfp56", "qsfpdd"},
        "osfp": {"osfp"},
    }
    allowed = compatibility.get(cage, {cage} if cage else set())
    return [value for value in order if value in allowed]

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
    "rack_pdu": "Rack power distribution units",
    "cable_management": "Cable-management panels",
}

# Exact-model preferences use asset IDs rather than manufacturer names.  The
# component keys deliberately mirror the manufacturer preference table so a
# model can be pinned at any generated layer without adding planner-specific
# fields to individual assets.
ASSET_MODEL_PREFERENCE_COMPONENTS = dict(MANUFACTURER_PREFERENCE_COMPONENTS)
RACK_DEPLOYMENT_MODELS = {"top_of_rack", "end_of_row"}
AGGREGATION_RACK_MODES = {"dedicated", "shared_eor"}


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


def normalise_asset_model_preferences(value) -> Dict[str, dict]:
    """Return ordered exact-model preferences keyed by planner component.

    ``preferred_asset_ids`` contains network-asset IDs in priority order.  A
    strict policy excludes all other models; a non-strict policy only ranks the
    selected models ahead of automatic alternatives.
    """

    source = value if isinstance(value, dict) else {}
    result: Dict[str, dict] = {}
    for component in ASSET_MODEL_PREFERENCE_COMPONENTS:
        raw = source.get(component, {})
        strict = False
        preferred: List[str] = []
        if isinstance(raw, dict):
            preferred = _normalise_string_list(
                raw.get("preferred_asset_ids", raw.get("asset_ids", []))
            )
            strict = bool(raw.get("strict", False))
        elif isinstance(raw, (list, tuple, str)):
            preferred = _normalise_string_list(raw)
        result[component] = {
            "preferred_asset_ids": preferred,
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
    settings.setdefault("access_stacking_enabled", True)
    settings.setdefault("access_stack_max_members", 8)
    settings.setdefault("access_stack_topology", "ring")
    settings.setdefault("layer_connection_rules", [])
    settings.setdefault("manufacturer_preferences", {})
    settings.setdefault("asset_model_preferences", {})
    settings.setdefault("rack_deployment_model", "end_of_row")
    settings.setdefault("aggregation_rack_mode", "dedicated")
    settings.setdefault("tor_keep_final_connections_in_cabinet", True)
    settings.setdefault("tor_allow_adjacent_cabinet_uplinks", True)
    settings.setdefault("auto_connect_new_manual_devices", True)
    settings.setdefault("auto_add_switches_for_bandwidth", True)
    settings.setdefault("auto_planner_connected_data_points_only", False)
    settings.setdefault("prevent_additional_equipment_rooms", False)
    settings.setdefault("ignore_link_bandwidth_errors", False)
    settings.setdefault("auto_planner_resolution_overrides", {})
    settings.setdefault("spare_capacity_percent", 15.0)
    settings.setdefault("traditional_max_copper_m", 90.0)
    settings.setdefault("polan_max_ont_copper_m", 30.0)
    settings.setdefault("polan_max_onts_per_splitter", 8)
    # Migrate the retired pre-v2 key before applying the canonical default.
    legacy_splitter_route = settings.pop("polan_max_splitter_to_ont_m", None)
    if "polan_max_splitter_ont_route_m" not in settings and legacy_splitter_route is not None:
        settings["polan_max_splitter_ont_route_m"] = legacy_splitter_route
    settings.setdefault("polan_max_splitter_ont_route_m", 120.0)
    settings.setdefault("polan_olt_failover", True)
    settings.setdefault("external_network_redundancy", True)
    settings.setdefault("external_network_link_count", 2)
    settings.setdefault("default_poe_power_w", 0.0)
    settings.setdefault("default_expected_bandwidth_mbps", 0.0)
    settings.setdefault(
        "default_north_south_bandwidth_mbps",
        settings.get("default_expected_bandwidth_mbps", 0.0),
    )
    settings.setdefault("default_east_west_bandwidth_mbps", 0.0)
    settings.setdefault("default_expected_packet_rate_pps", 0.0)
    settings.setdefault("poe_power_defaults", {})
    settings.setdefault("default_rack_size_u", 42)
    settings.setdefault("default_fibre_core_count", 12)
    settings.setdefault("default_pdu_outlet_count", 42)
    settings.setdefault("default_pdu_capacity_w", 7360.0)
    settings.setdefault("auto_dual_power_critical_devices", True)
    settings.setdefault("ip_plan_base_cidr", "10.0.0.0/8")
    settings.setdefault("physical_fibre_layer", fibre_layer_defaults())
    settings.setdefault("physical_fibre_planning", default_physical_fibre_planning())
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
    settings["access_stacking_enabled"] = bool(
        settings.get("access_stacking_enabled", True)
    )
    settings["access_stack_max_members"] = max(
        1, min(64, _as_int(settings.get("access_stack_max_members"), 8))
    )
    stack_topology = _text(settings.get("access_stack_topology")).lower()
    settings["access_stack_topology"] = (
        stack_topology if stack_topology in {"ring", "chain"} else "ring"
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
    settings["asset_model_preferences"] = normalise_asset_model_preferences(
        settings.get("asset_model_preferences")
    )
    rack_model = _text(settings.get("rack_deployment_model")).lower()
    settings["rack_deployment_model"] = (
        rack_model if rack_model in RACK_DEPLOYMENT_MODELS else "end_of_row"
    )
    aggregation_mode = _text(settings.get("aggregation_rack_mode")).lower()
    settings["aggregation_rack_mode"] = (
        aggregation_mode
        if aggregation_mode in AGGREGATION_RACK_MODES
        else "dedicated"
    )
    settings["tor_keep_final_connections_in_cabinet"] = bool(
        settings.get("tor_keep_final_connections_in_cabinet", True)
    )
    settings["tor_allow_adjacent_cabinet_uplinks"] = bool(
        settings.get("tor_allow_adjacent_cabinet_uplinks", True)
    )
    settings["auto_connect_new_manual_devices"] = bool(
        settings.get("auto_connect_new_manual_devices", True)
    )
    settings["auto_add_switches_for_bandwidth"] = bool(
        settings.get("auto_add_switches_for_bandwidth", True)
    )
    settings["ignore_link_bandwidth_errors"] = bool(
        settings.get("ignore_link_bandwidth_errors", False)
    )
    overrides = settings.get("auto_planner_resolution_overrides")
    if not isinstance(overrides, dict):
        overrides = {}

    access_assets = overrides.get("access_asset_by_location")
    access_assets = access_assets if isinstance(access_assets, dict) else {}
    access_assets = {
        _text(location): _text(asset_id)
        for location, asset_id in access_assets.items()
        if _text(location) and _text(asset_id)
    }

    upstream_assets = overrides.get("upstream_asset_by_layer")
    upstream_assets = upstream_assets if isinstance(upstream_assets, dict) else {}
    upstream_assets = {
        _text(layer).lower(): _text(asset_id)
        for layer, asset_id in upstream_assets.items()
        if _text(layer).lower() in {"core", "aggregation"} and _text(asset_id)
    }

    minimum_switches = overrides.get("minimum_access_switches_by_location")
    minimum_switches = minimum_switches if isinstance(minimum_switches, dict) else {}
    minimum_switches = {
        _text(location): max(1, _as_int(count, 1))
        for location, count in minimum_switches.items()
        if _text(location) and _as_int(count, 0) > 0
    }

    spare_modes = overrides.get("spare_capacity_mode_by_location")
    spare_modes = spare_modes if isinstance(spare_modes, dict) else {}
    spare_modes = {
        _text(location): _text(mode).lower()
        for location, mode in spare_modes.items()
        if _text(location) and _text(mode).lower() in {"defer", "new_rack"}
    }

    additional_ders = overrides.get("additional_der_by_location")
    additional_ders = additional_ders if isinstance(additional_ders, dict) else {}
    additional_ders = {
        _text(location): max(1, _as_int(count, 1))
        for location, count in additional_ders.items()
        if _text(location) and _as_int(count, 0) > 0
    }

    additional_der_assets = overrides.get("additional_der_asset_by_location")
    additional_der_assets = (
        additional_der_assets if isinstance(additional_der_assets, dict) else {}
    )
    additional_der_assets = {
        _text(location): _text(asset_id)
        for location, asset_id in additional_der_assets.items()
        if _text(location) and _text(asset_id)
    }

    settings["auto_planner_resolution_overrides"] = {
        "access_asset_by_location": access_assets,
        "upstream_asset_by_layer": upstream_assets,
        "minimum_access_switches_by_location": minimum_switches,
        "spare_capacity_mode_by_location": spare_modes,
        "additional_der_by_location": additional_ders,
        "additional_der_asset_by_location": additional_der_assets,
    }
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
    settings["external_network_redundancy"] = bool(settings.get("external_network_redundancy", True))
    settings["external_network_link_count"] = max(1, min(8, _as_int(settings.get("external_network_link_count"), 2)))
    settings["default_poe_power_w"] = max(
        0.0, _as_float(settings.get("default_poe_power_w"), 0.0)
    )
    settings["default_north_south_bandwidth_mbps"] = max(
        0.0,
        _as_float(
            settings.get("default_north_south_bandwidth_mbps"),
            settings.get("default_expected_bandwidth_mbps", 0.0),
        ),
    )
    settings["default_east_west_bandwidth_mbps"] = max(
        0.0, _as_float(settings.get("default_east_west_bandwidth_mbps"), 0.0)
    )
    settings["default_expected_bandwidth_mbps"] = round(
        settings["default_north_south_bandwidth_mbps"]
        + settings["default_east_west_bandwidth_mbps"],
        6,
    )
    settings["default_expected_packet_rate_pps"] = max(
        0.0, _as_float(settings.get("default_expected_packet_rate_pps"), 0.0)
    )
    settings["default_rack_size_u"] = max(
        1, _as_int(settings.get("default_rack_size_u"), 42)
    )
    settings["default_fibre_core_count"] = max(2, _as_int(settings.get("default_fibre_core_count"), 12))
    settings["default_pdu_outlet_count"] = max(1, _as_int(settings.get("default_pdu_outlet_count"), 42))
    settings["default_pdu_capacity_w"] = max(0.0, _as_float(settings.get("default_pdu_capacity_w"), 7360.0))
    settings["auto_dual_power_critical_devices"] = bool(settings.get("auto_dual_power_critical_devices", True))
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
    symbol_scale = _as_float(merged_layer_settings.get("symbol_scale"), 0.075)
    if any(abs(symbol_scale - legacy) <= 1e-9 for legacy in (0.32, 0.18, 0.12)):
        symbol_scale = 0.075
    merged_layer_settings["symbol_scale"] = max(0.035, min(2.0, symbol_scale))
    label_scale = _as_float(merged_layer_settings.get("label_scale"), 0.09)
    if any(abs(label_scale - legacy) <= 1e-9 for legacy in (0.55, 0.30, 0.18, 0.14)):
        label_scale = 0.09
    merged_layer_settings["label_scale"] = max(0.035, min(2.0, label_scale))
    merged_layer_settings["cable_width_scale"] = max(
        0.05, min(2.0, _as_float(merged_layer_settings.get("cable_width_scale"), 0.18))
    )
    settings["physical_fibre_layer"] = merged_layer_settings
    raw_fibre_planning = settings.get("physical_fibre_planning")
    if not isinstance(raw_fibre_planning, dict):
        raw_fibre_planning = {}
    fibre_planning = default_physical_fibre_planning()
    fibre_planning.update(raw_fibre_planning)
    mode = _text(fibre_planning.get("routing_mode")).lower()
    fibre_planning["routing_mode"] = mode if mode in {"direct", "spine_and_spur"} else "direct"
    for field, fallback in (
        ("default_cable_type_id", "OS2-12F"),
        ("spine_cable_type_id", "OS2-48F"),
        ("spur_cable_type_id", "OS2-12F"),
    ):
        fibre_planning[field] = _text(fibre_planning.get(field)) or fallback
    for field, fallback in (
        ("branch_termination_method", "spliced"),
        ("splitter_termination_method", "connectorised"),
    ):
        value = _text(fibre_planning.get(field)).lower()
        fibre_planning[field] = value if value in {"spliced", "connectorised"} else fallback
    fibre_planning["max_splices_per_cassette"] = max(1, min(24, _as_int(fibre_planning.get("max_splices_per_cassette"), 24)))
    fibre_planning["spare_core_percent"] = max(0.0, min(200.0, _as_float(fibre_planning.get("spare_core_percent"), 15.0)))
    fibre_planning["minimum_optical_margin_db"] = max(0.0, min(30.0, _as_float(fibre_planning.get("minimum_optical_margin_db"), 3.0)))
    fibre_planning["splitter_pigtail"] = bool(fibre_planning.get("splitter_pigtail", True))
    fibre_planning["extend_reduced_count_spine"] = bool(
        fibre_planning.get("extend_reduced_count_spine", True)
    )
    fibre_planning["mpo_breakout_minimum_cores"] = max(12, min(6912, _as_int(fibre_planning.get("mpo_breakout_minimum_cores"), 48)))
    mpo_connector = _text(fibre_planning.get("mpo_breakout_connector")).lower() or "mpo-24"
    fibre_planning["mpo_breakout_connector"] = mpo_connector if mpo_connector in MODULAR_PANEL_REAR_CONNECTORS else "mpo-24"
    settings["physical_fibre_planning"] = fibre_planning
    if not isinstance(settings.get("poe_power_defaults"), dict):
        settings["poe_power_defaults"] = {}

    for key in (
        "network_assets",
        "network_asset_instances",
        "network_racks",
        "network_connections",
        "network_power_connections",
        "network_endpoint_assignments",
        "network_patch_leads",
        "network_redundancy_groups",
        "network_vlans",
        "network_routes",
        "network_ip_allocations",
        "network_external_networks",
        "network_usage_profiles",
        "network_optic_modules",
        "network_optical_paths",
        "network_fibre_cable_types",
        "network_fibre_cables",
        "network_fibre_nodes",
        "network_fibre_splices",
    ):
        if not isinstance(data.get(key), list):
            data[key] = []

    if not isinstance(data.get("network_design_summary"), dict):
        data["network_design_summary"] = {}

    # General project assets are the traffic-producing endpoints consumed by
    # the auto-planner. Directional fields distinguish WAN/internet
    # north-south demand from internal east-west demand. Legacy projects that
    # only contain expected_bandwidth_mbps are migrated conservatively as
    # north-south traffic so their previous sizing is never reduced silently.
    for endpoint_asset in data.get("assets", []):
        if not isinstance(endpoint_asset, dict):
            continue
        legacy_bandwidth = max(
            0.0, _as_float(endpoint_asset.get("expected_bandwidth_mbps"))
        )
        has_directional = (
            "expected_north_south_bandwidth_mbps" in endpoint_asset
            or "expected_east_west_bandwidth_mbps" in endpoint_asset
        )
        north_south = max(
            0.0,
            _as_float(
                endpoint_asset.get("expected_north_south_bandwidth_mbps"),
                legacy_bandwidth if not has_directional else 0.0,
            ),
        )
        east_west = max(
            0.0,
            _as_float(endpoint_asset.get("expected_east_west_bandwidth_mbps"), 0.0),
        )
        endpoint_asset["usage_profile_id"] = _text(
            endpoint_asset.get("usage_profile_id")
        )
        endpoint_asset["north_south_concurrency_factor"] = min(
            1.0,
            max(
                0.0,
                _as_float(
                    endpoint_asset.get("north_south_concurrency_factor"), 1.0
                ),
            ),
        )
        endpoint_asset["east_west_concurrency_factor"] = min(
            1.0,
            max(
                0.0,
                _as_float(
                    endpoint_asset.get("east_west_concurrency_factor"), 1.0
                ),
            ),
        )
        endpoint_asset["expected_north_south_bandwidth_mbps"] = north_south
        endpoint_asset["expected_east_west_bandwidth_mbps"] = east_west
        endpoint_asset["expected_bandwidth_mbps"] = round(
            north_south + east_west, 6
        )
        endpoint_asset["expected_packet_rate_pps"] = max(
            0.0, _as_float(endpoint_asset.get("expected_packet_rate_pps"))
        )

    usage_profile_ids: set[str] = set()
    normalised_profiles: List[dict] = []
    for source_profile in list(data.get("network_usage_profiles", [])):
        if not isinstance(source_profile, dict):
            continue
        profile = source_profile
        profile_id = _text(profile.get("id"))
        if not profile_id or profile_id in usage_profile_ids:
            continue
        usage_profile_ids.add(profile_id)
        profile["id"] = profile_id
        profile["name"] = _text(profile.get("name")) or profile_id
        profile["description"] = _text(profile.get("description"))
        profile["north_south_bandwidth_mbps"] = max(
            0.0, _as_float(profile.get("north_south_bandwidth_mbps"))
        )
        profile["east_west_bandwidth_mbps"] = max(
            0.0, _as_float(profile.get("east_west_bandwidth_mbps"))
        )
        profile["north_south_concurrency_factor"] = min(
            1.0,
            max(
                0.0,
                _as_float(profile.get("north_south_concurrency_factor"), 1.0),
            ),
        )
        profile["east_west_concurrency_factor"] = min(
            1.0,
            max(
                0.0,
                _as_float(profile.get("east_west_concurrency_factor"), 1.0),
            ),
        )
        profile["expected_packet_rate_pps"] = max(
            0.0, _as_float(profile.get("expected_packet_rate_pps"))
        )
        profile["poe_power_w"] = max(
            0.0, _as_float(profile.get("poe_power_w"))
        )
        normalised_profiles.append(profile)
    for default_profile in DEFAULT_NETWORK_USAGE_PROFILES:
        if _text(default_profile.get("id")) not in usage_profile_ids:
            profile = deepcopy(default_profile)
            profile["north_south_concurrency_factor"] = min(
                1.0,
                max(
                    0.0,
                    _as_float(profile.get("north_south_concurrency_factor"), 1.0),
                ),
            )
            profile["east_west_concurrency_factor"] = min(
                1.0,
                max(
                    0.0,
                    _as_float(profile.get("east_west_concurrency_factor"), 1.0),
                ),
            )
            normalised_profiles.append(profile)
    data["network_usage_profiles"] = normalised_profiles

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
        asset["asset_group"] = network_asset_group(asset)
        wireless_category = _text(asset.get("wireless_device_category")).lower()
        if asset["asset_type"] == "wireless_access_point" and not wireless_category:
            wireless_category = "access_point"
        if wireless_category not in {
            "", "access_point", "iot_gateway", "radio_gateway",
            "wireless_bridge", "wireless_sensor", "other",
        }:
            wireless_category = "other"
        asset["wireless_device_category"] = wireless_category
        asset.setdefault("manufacturer", "")
        asset.setdefault("model", "")
        asset.setdefault("patch_panel_type", "")
        asset.setdefault("patch_panel_format", "fixed")
        inferred_modular_panel = bool(
            asset.get("modular_patch_panel", False)
            or _as_int(asset.get("patch_panel_cassette_count"), 0) > 0
        )
        patch_panel_format = _text(asset.get("patch_panel_format")).lower()
        if patch_panel_format in {"modular", "cassette", "modular_cassette"} or inferred_modular_panel:
            patch_panel_format = "modular_cassette"
        else:
            patch_panel_format = "fixed"
        is_modular_fibre_panel = bool(
            asset["asset_type"] == "patch_panel"
            and _text(asset.get("patch_panel_type")).lower() == "fibre"
            and patch_panel_format == "modular_cassette"
        )
        asset["patch_panel_format"] = "modular_cassette" if is_modular_fibre_panel else "fixed"
        asset["modular_patch_panel"] = is_modular_fibre_panel
        asset["patch_panel_cassette_count"] = (
            max(1, min(4, _as_int(asset.get("patch_panel_cassette_count"), 4)))
            if is_modular_fibre_panel
            else 0
        )
        # A modular panel accepts up to four field-replaceable cassettes.  Each
        # cassette exposes twelve front connector positions.  LC cassettes are
        # duplex by default, while SC cassettes can be simplex or duplex.
        asset["patch_panel_cassette_capacity"] = 12 if is_modular_fibre_panel else 0
        front_connector = _text(asset.get("patch_panel_cassette_front_connector")).lower() or "lc_duplex"
        if front_connector not in MODULAR_PANEL_FRONT_CONNECTORS:
            front_connector = "lc_duplex"
        termination_mode = _text(asset.get("patch_panel_cassette_termination_mode")).lower() or "spliced"
        if termination_mode not in MODULAR_PANEL_TERMINATION_MODES:
            termination_mode = "spliced"
        rear_connector = _text(asset.get("patch_panel_cassette_rear_connector")).lower() or "mpo-24"
        if rear_connector not in MODULAR_PANEL_REAR_CONNECTORS:
            rear_connector = "mpo-24"
        asset["patch_panel_cassette_front_connector"] = front_connector if is_modular_fibre_panel else ""
        asset["patch_panel_cassette_termination_mode"] = termination_mode if is_modular_fibre_panel else ""
        asset["patch_panel_cassette_rear_connector"] = rear_connector if is_modular_fibre_panel else ""
        asset["patch_panel_cassette_rear_connector_count"] = (
            max(1, min(4, _as_int(asset.get("patch_panel_cassette_rear_connector_count"), 1)))
            if is_modular_fibre_panel
            else 0
        )
        asset["patch_panel_mpo_breakout_minimum_cores"] = (
            max(12, min(6912, _as_int(asset.get("patch_panel_mpo_breakout_minimum_cores"), 48)))
            if is_modular_fibre_panel
            else 0
        )
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
        asset["power_capacity_w"] = max(
            0.0,
            _as_float(
                asset.get(
                    "power_capacity_w",
                    asset.get("rated_power_w", asset.get("output_capacity_w", 0.0)),
                )
            ),
        )
        asset["power_outlet_count"] = max(
            0,
            _as_int(
                asset.get(
                    "power_outlet_count",
                    asset.get("outlet_count", asset.get("connections_out", 0)),
                )
            ),
        )
        asset["power_feed_count"] = max(1, min(8, _as_int(asset.get("power_feed_count"), 1)))
        asset["redundant_power_supplies"] = bool(
            asset.get("redundant_power_supplies", asset["power_feed_count"] > 1)
        )
        asset["rack_mount_style"] = _text(asset.get("rack_mount_style")).lower()
        asset["rack_side"] = _text(asset.get("rack_side")).lower()
        asset["ups_backed_source"] = bool(asset.get("ups_backed_source", False))
        if asset["asset_type"] == "pdu":
            asset["rack_mount_style"] = asset["rack_mount_style"] or "vertical_side"
            if asset["rack_mount_style"] == "vertical_side":
                asset["rack_units"] = 0
            asset["power_outlet_count"] = max(
                1,
                asset["power_outlet_count"]
                or _as_int(settings.get("default_pdu_outlet_count"), 42),
            )
            asset["power_capacity_w"] = max(
                asset["power_capacity_w"],
                _as_float(settings.get("default_pdu_capacity_w"), 7360.0),
            )
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
        legacy_bandwidth = max(
            0.0, _as_float(asset.get("expected_bandwidth_mbps"))
        )
        has_directional = (
            "expected_north_south_bandwidth_mbps" in asset
            or "expected_east_west_bandwidth_mbps" in asset
        )
        asset["usage_profile_id"] = _text(asset.get("usage_profile_id"))
        asset["north_south_concurrency_factor"] = min(
            1.0,
            max(
                0.0,
                _as_float(asset.get("north_south_concurrency_factor"), 1.0),
            ),
        )
        asset["east_west_concurrency_factor"] = min(
            1.0,
            max(
                0.0,
                _as_float(asset.get("east_west_concurrency_factor"), 1.0),
            ),
        )
        asset["expected_north_south_bandwidth_mbps"] = max(
            0.0,
            _as_float(
                asset.get("expected_north_south_bandwidth_mbps"),
                legacy_bandwidth if not has_directional else 0.0,
            ),
        )
        asset["expected_east_west_bandwidth_mbps"] = max(
            0.0, _as_float(asset.get("expected_east_west_bandwidth_mbps"), 0.0)
        )
        asset["expected_bandwidth_mbps"] = round(
            asset["expected_north_south_bandwidth_mbps"]
            + asset["expected_east_west_bandwidth_mbps"],
            6,
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
        model_identity = " ".join(
            (_text(asset.get("manufacturer")), _text(asset.get("model")), _text(asset.get("name")))
        ).upper()
        is_catalyst_9600 = asset["asset_type"] == "network_switch" and (
            "C9606R" in model_identity or "CATALYST 9600" in model_identity
        )
        asset["modular_chassis"] = bool(asset.get("modular_chassis", is_catalyst_9600))
        modules = asset.get("chassis_modules")
        if not isinstance(modules, list):
            modules = []
        if is_catalyst_9600 and not modules:
            installed = "C9600X-LC-56YL4C"
            modules = [
                {"slot": slot, "module_type": "line_card", "model": installed}
                for slot in (1, 2, 5, 6)
            ] + [
                {"slot": 3, "module_type": "supervisor", "model": "C9600X-SUP-2"},
                {"slot": 4, "module_type": "supervisor", "model": "C9600X-SUP-2"},
            ]
        normalised_modules = []
        for row in modules:
            if not isinstance(row, dict):
                continue
            slot = max(1, _as_int(row.get("slot"), len(normalised_modules) + 1))
            module_type = _text(row.get("module_type")).lower() or "line_card"
            model = _text(row.get("model"))
            if module_type == "line_card" and model not in CATALYST_9600_LINE_CARDS:
                model = "C9600X-LC-56YL4C" if is_catalyst_9600 else model
            normalised_modules.append({"slot": slot, "module_type": module_type, "model": model})
        asset["chassis_modules"] = sorted(normalised_modules, key=lambda row: row["slot"])
        if asset["modular_chassis"] and any(
            row.get("module_type") == "line_card" for row in asset["chassis_modules"]
        ):
            asset["port_definitions"] = catalyst_9600_port_definitions(
                asset["chassis_modules"]
            )
        asset["rack_units"] = max(0, _as_int(asset.get("rack_units"), 1))
        asset["physical_width_mm"] = max(0.0, _as_float(asset.get("physical_width_mm"), 0.0))
        asset["physical_depth_mm"] = max(0.0, _as_float(asset.get("physical_depth_mm"), 0.0))
        asset["physical_height_mm"] = max(0.0, _as_float(asset.get("physical_height_mm"), 0.0))
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
        asset["supported_speeds_mbps"] = normalise_port_speeds(
            asset.get("supported_speeds_mbps", asset.get("supported_speeds", []))
        )
        asset["optic_form_factor"] = _text(asset.get("optic_form_factor")).lower()
        asset["optic_connector_type"] = _text(asset.get("optic_connector_type")).lower() or "lc"
        asset["optic_fibre_standard"] = _text(asset.get("optic_fibre_standard")) or _text(asset.get("optical_standard")) or "OS2"
        asset["optic_reach_m"] = max(0.0, _as_float(asset.get("optic_reach_m")))
        if asset["asset_type"] == "optical_transceiver":
            if asset["optic_form_factor"] not in PLUGGABLE_OPTIC_PORT_TYPES:
                asset["optic_form_factor"] = "sfp"
            if not asset["supported_speeds_mbps"]:
                asset["supported_speeds_mbps"] = default_port_speeds(asset["optic_form_factor"])
            asset["number_of_ports"] = 0
            asset["rack_units"] = 0

        # Optical characteristics are asset-level defaults. Blank values mean
        # the optic has not yet been configured and are not silently treated as 0 dBm.
        for field in ("optical_tx_power_dbm", "optical_receiver_sensitivity_dbm"):
            value = asset.get(field, "")
            asset[field] = "" if _text(value) == "" else _as_float(value)
        for field in ("optical_insertion_loss_db", "optical_return_loss_db"):
            value = asset.get(field, "")
            asset[field] = "" if _text(value) == "" else max(0.0, _as_float(value))
        asset["optical_wavelength_nm"] = max(0, _as_int(asset.get("optical_wavelength_nm"), 0))
        asset.setdefault("optical_standard", "")
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
                    "supported_speeds_mbps": (
                        normalise_port_speeds(
                            row.get("supported_speeds_mbps", row.get("supported_speeds", []))
                        )
                        if normalise_port_speeds(
                            row.get("supported_speeds_mbps", row.get("supported_speeds", []))
                        )
                        else ([] if asset["asset_type"] == "patch_panel" else default_port_speeds(port_type))
                    ),
                    "default_speed_mbps": max(0, _as_int(row.get("default_speed_mbps"))),
                    "transmit_power_dbm": "" if _text(row.get("transmit_power_dbm")) == "" else _as_float(row.get("transmit_power_dbm")),
                    "receiver_sensitivity_dbm": "" if _text(row.get("receiver_sensitivity_dbm")) == "" else _as_float(row.get("receiver_sensitivity_dbm")),
                    "insertion_loss_db": "" if _text(row.get("insertion_loss_db")) == "" else max(0.0, _as_float(row.get("insertion_loss_db"))),
                    "return_loss_db": "" if _text(row.get("return_loss_db")) == "" else max(0.0, _as_float(row.get("return_loss_db"))),
                    "wavelength_nm": max(0, _as_int(row.get("wavelength_nm"), 0)),
                }
            )
        port_definitions = [row for row in port_definitions if row["port_count"] > 0]

        if asset.get("modular_patch_panel"):
            cassette_count = max(1, min(4, _as_int(asset.get("patch_panel_cassette_count"), 4)))
            cassette_capacity = 12
            front_key = _text(asset.get("patch_panel_cassette_front_connector")).lower() or "lc_duplex"
            front = MODULAR_PANEL_FRONT_CONNECTORS.get(front_key, MODULAR_PANEL_FRONT_CONNECTORS["lc_duplex"])
            explicit_names = [
                f"C{cassette}-{front['label']}-{position:02d}"
                for cassette in range(1, cassette_count + 1)
                for position in range(1, cassette_capacity + 1)
            ]
            asset["patch_panel_cassette_count"] = cassette_count
            asset["patch_panel_cassette_capacity"] = cassette_capacity
            asset["number_of_ports"] = cassette_count * cassette_capacity
            asset["connections_in"] = asset["number_of_ports"]
            asset["connections_out"] = asset["number_of_ports"]
            asset["input_connection_type"] = "fibre"
            asset["output_connection_type"] = "fibre"
            port_definitions = [
                {
                    "port_type": front["port_type"],
                    "port_count": asset["number_of_ports"],
                    "port_use": "patch",
                    "name_prefix": front["label"],
                    "explicit_names": explicit_names,
                    "supported_speeds_mbps": [],
                    "default_speed_mbps": 0,
                }
            ]

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
                    "supported_speeds_mbps": [],
                    "default_speed_mbps": 0,
                },
                {
                    "port_type": "lc",
                    "port_count": asset["split_output_count"],
                    "port_use": "output",
                    "name_prefix": "Output",
                    "explicit_names": [],
                    "supported_speeds_mbps": [],
                    "default_speed_mbps": 0,
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
                    "supported_speeds_mbps": default_port_speeds(default_type),
                    "default_speed_mbps": 0,
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
    for location in locations.values():
        cabinet_type = _text(location.get("cabinet_type")).lower()
        location["cabinet_type"] = (
            cabinet_type if cabinet_type in {"standard", "slim_wall"} else "standard"
        )
        location["max_network_cabinets"] = max(
            0, _as_int(location.get("max_network_cabinets"), 0)
        )
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
        # Generated rack layouts may use half-U rails while the manual editor
        # remains intentionally whole-U.  Keep the exact physical slot in
        # separate integer metadata so normalisation cannot round it away.
        instance["rack_start_half_u"] = max(
            0, _as_int(instance.get("rack_start_half_u"))
        )
        instance["rack_height_half_u"] = max(
            0, _as_int(instance.get("rack_height_half_u"))
        )
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
        instance["route_anchor"] = _text(instance.get("route_anchor"))
        instance["wireless_device_layer"] = bool(instance.get("wireless_device_layer", False))
        instance["imported_wireless_device"] = bool(instance.get("imported_wireless_device", False))
        instance["wireless_import_source_file"] = _text(instance.get("wireless_import_source_file"))
        instance["wireless_import_source_id"] = _text(instance.get("wireless_import_source_id"))
        instance["wireless_import_source_floor"] = _text(instance.get("wireless_import_source_floor"))
        instance["wireless_import_profile"] = _text(instance.get("wireless_import_profile"))
        instance_legacy_bandwidth = max(
            0.0, _as_float(instance.get("expected_bandwidth_mbps"))
        )
        instance_has_directional = (
            "expected_north_south_bandwidth_mbps" in instance
            or "expected_east_west_bandwidth_mbps" in instance
        )
        instance["expected_north_south_bandwidth_mbps"] = max(
            0.0,
            _as_float(
                instance.get("expected_north_south_bandwidth_mbps"),
                instance_legacy_bandwidth if not instance_has_directional else 0.0,
            ),
        )
        instance["expected_east_west_bandwidth_mbps"] = max(
            0.0, _as_float(instance.get("expected_east_west_bandwidth_mbps"), 0.0)
        )
        instance["north_south_concurrency_factor"] = min(
            1.0,
            max(
                0.0,
                _as_float(instance.get("north_south_concurrency_factor"), 1.0),
            ),
        )
        instance["east_west_concurrency_factor"] = min(
            1.0,
            max(
                0.0,
                _as_float(instance.get("east_west_concurrency_factor"), 1.0),
            ),
        )
        instance["expected_bandwidth_mbps"] = round(
            instance["expected_north_south_bandwidth_mbps"]
            + instance["expected_east_west_bandwidth_mbps"],
            6,
        )
        instance["wireless_import_row_number"] = max(0, _as_int(instance.get("wireless_import_row_number")))
        instance["wireless_import_corridor_distance_m"] = max(
            0.0, _as_float(instance.get("wireless_import_corridor_distance_m"))
        )
        instance["wireless_import_graph_connected"] = bool(
            instance.get("wireless_import_graph_connected", bool(instance.get("route_anchor")))
        )
        instance["department_id"] = _text(instance.get("department_id"))
        instance["department_ids"] = _normalise_string_list(
            instance.get("department_ids", [instance["department_id"]] if instance["department_id"] else [])
        )
        if instance["department_id"] and instance["department_id"] not in instance["department_ids"]:
            instance["department_ids"].insert(0, instance["department_id"])
        instance["department_name"] = _text(instance.get("department_name"))
        instance["wireless_import_department_status"] = _text(
            instance.get("wireless_import_department_status")
        )
        instance["wireless_import_department_source"] = _text(
            instance.get("wireless_import_department_source")
        )
        instance["wireless_import_department_source_kind"] = _text(
            instance.get("wireless_import_department_source_kind")
        )
        instance["wireless_import_department_distance_m"] = max(
            0.0, _as_float(instance.get("wireless_import_department_distance_m"))
        )
        instance.setdefault("management_ip", "")
        instance.setdefault("management_vlan", "")
        instance.setdefault("power_feed", "")
        instance.setdefault("ups_source", "")
        instance["power_feed_count"] = max(1, min(8, _as_int(instance.get("power_feed_count"), 1)))
        instance["power_pdu_instance_ids"] = _normalise_string_list(instance.get("power_pdu_instance_ids", []))
        instance["upstream_power_source_id"] = _text(instance.get("upstream_power_source_id"))
        instance["rack_mount_style"] = _text(instance.get("rack_mount_style")).lower()
        instance["rack_side"] = _text(instance.get("rack_side")).lower()
        instance["side_mount_position"] = max(1, _as_int(instance.get("side_mount_position"), 1))
        instance["logical_stack"] = bool(instance.get("logical_stack", False))
        instance["stack_member_count"] = max(
            1, _as_int(instance.get("stack_member_count"), 1)
        )
        raw_member_slots = instance.get("stack_member_start_half_us", [])
        if not isinstance(raw_member_slots, list):
            raw_member_slots = []
        instance["stack_member_start_half_us"] = [
            max(1, _as_int(value, 1)) for value in raw_member_slots
        ][: instance["stack_member_count"]]
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
        asset = assets_by_id.get(_text(instance.get("asset_id")), {})
        raw_cassettes = instance.get("fibre_cassettes", [])
        if not isinstance(raw_cassettes, list):
            raw_cassettes = []
        normalised_cassettes = []
        if bool(asset.get("modular_patch_panel")):
            max_cassettes = max(1, min(4, _as_int(asset.get("patch_panel_cassette_count"), 4)))
            default_front = _text(asset.get("patch_panel_cassette_front_connector")).lower() or "lc_duplex"
            default_mode = _text(asset.get("patch_panel_cassette_termination_mode")).lower() or "spliced"
            default_rear = _text(asset.get("patch_panel_cassette_rear_connector")).lower() or "mpo-24"
            rows_by_position = {
                max(1, _as_int(row.get("position"), index + 1)): row
                for index, row in enumerate(raw_cassettes)
                if isinstance(row, dict)
            }
            for position in range(1, max_cassettes + 1):
                row = rows_by_position.get(position, {})
                front_key = _text(row.get("front_connector", default_front)).lower() or default_front
                if front_key not in MODULAR_PANEL_FRONT_CONNECTORS:
                    front_key = default_front
                mode = _text(row.get("termination_mode", default_mode)).lower() or default_mode
                if mode not in MODULAR_PANEL_TERMINATION_MODES:
                    mode = default_mode
                rear = _text(row.get("rear_connector", default_rear)).lower() or default_rear
                if rear not in MODULAR_PANEL_REAR_CONNECTORS:
                    rear = default_rear
                normalised_cassettes.append({
                    **row,
                    "position": position,
                    "front_connector": front_key,
                    "front_connector_capacity": 12,
                    "termination_mode": mode,
                    "rear_connector": rear if mode == "connectorised" else "splice",
                    "rear_connector_count": max(0, min(4, _as_int(row.get("rear_connector_count"), 1 if mode == "connectorised" else 0))),
                    "used_front_connectors": max(0, min(12, _as_int(row.get("used_front_connectors"), 0))),
                    "used_fibres": max(0, _as_int(row.get("used_fibres"), 0)),
                    "cable_ids": _normalise_string_list(row.get("cable_ids", [])),
                    "associated_instance_ids": _normalise_string_list(row.get("associated_instance_ids", [])),
                })
        instance["fibre_cassettes"] = normalised_cassettes
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
        cabinet_type = _text(rack.get("cabinet_type")).lower()
        rack["cabinet_type"] = (
            cabinet_type if cabinet_type in {"standard", "slim_wall"} else "standard"
        )
        rack["max_switches"] = 2 if rack["cabinet_type"] == "slim_wall" else max(
            0, _as_int(rack.get("max_switches"), 0)
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
        connection["cable_core_count"] = max(
            connection["fibre_count"],
            _as_int(connection.get("cable_core_count")),
        )
        for side in ("from", "to"):
            method_key = f"{side}_termination_method"
            method = _text(connection.get(method_key)).lower()
            connection[method_key] = (
                method if method in {"", "spliced", "connectorised"} else ""
            )
            connector_key = f"{side}_connector_type"
            connection[connector_key] = _text(connection.get(connector_key)).lower()
            allocation_key = f"{side}_panel_termination_allocations"
            raw_allocations = connection.get(allocation_key, [])
            connection[allocation_key] = [
                dict(row) for row in raw_allocations if isinstance(row, dict)
            ] if isinstance(raw_allocations, list) else []
        connection["connectorised_breakout"] = bool(
            connection.get("connectorised_breakout", False)
        )
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
        connection["link_speed_mbps"] = max(0, _as_int(connection.get("link_speed_mbps")))
        connection.setdefault("link_aggregation_group_id", "")
        connection.setdefault("link_aggregation_mode", "")
        connection["link_aggregation_member_index"] = max(
            0, _as_int(connection.get("link_aggregation_member_index"))
        )
        connection["link_aggregation_member_count"] = max(
            0, _as_int(connection.get("link_aggregation_member_count"))
        )
        connection["aggregate_link_speed_mbps"] = max(
            0, _as_int(connection.get("aggregate_link_speed_mbps"))
        )
        connection_legacy_bandwidth = max(
            0.0, _as_float(connection.get("expected_bandwidth_mbps"))
        )
        connection_has_directional = (
            "expected_north_south_bandwidth_mbps" in connection
            or "expected_east_west_bandwidth_mbps" in connection
        )
        connection["expected_north_south_bandwidth_mbps"] = max(
            0.0,
            _as_float(
                connection.get("expected_north_south_bandwidth_mbps"),
                connection_legacy_bandwidth if not connection_has_directional else 0.0,
            ),
        )
        connection["expected_east_west_bandwidth_mbps"] = max(
            0.0, _as_float(connection.get("expected_east_west_bandwidth_mbps"), 0.0)
        )
        connection["expected_bandwidth_mbps"] = round(
            connection["expected_north_south_bandwidth_mbps"]
            + connection["expected_east_west_bandwidth_mbps"],
            6,
        )
        connection["aggregate_expected_north_south_bandwidth_mbps"] = max(
            connection["expected_north_south_bandwidth_mbps"],
            _as_float(connection.get("aggregate_expected_north_south_bandwidth_mbps")),
        )
        connection["aggregate_expected_east_west_bandwidth_mbps"] = max(
            connection["expected_east_west_bandwidth_mbps"],
            _as_float(connection.get("aggregate_expected_east_west_bandwidth_mbps")),
        )
        connection["aggregate_expected_bandwidth_mbps"] = max(
            connection["expected_bandwidth_mbps"],
            _as_float(connection.get("aggregate_expected_bandwidth_mbps")),
        )
        connection["aggregate_expected_packet_rate_pps"] = max(
            0.0, _as_float(connection.get("aggregate_expected_packet_rate_pps"))
        )
        connection.setdefault("from_optic_module_id", "")
        connection.setdefault("to_optic_module_id", "")

    optic_module_ids: set[str] = set()
    for module in data["network_optic_modules"]:
        if not isinstance(module, dict):
            continue
        module.setdefault("id", "")
        module.setdefault("asset_id", "")
        module.setdefault("host_instance_id", "")
        module.setdefault("host_port", "")
        module.setdefault("connection_id", "")
        module.setdefault("side", "")
        module["link_speed_mbps"] = max(0, _as_int(module.get("link_speed_mbps")))
        module["auto_generated"] = bool(module.get("auto_generated", False))
        module.setdefault("notes", "")
        module_id = _text(module.get("id"))
        if module_id:
            optic_module_ids.add(module_id)

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
        assignment.setdefault("endpoint_instance_id", "")
        assignment.setdefault("endpoint_connection_id", "")
        assignment.setdefault("endpoint_network_port", "")
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
        assignment_legacy_bandwidth = max(
            0.0, _as_float(assignment.get("expected_bandwidth_mbps"))
        )
        assignment_has_directional = (
            "expected_north_south_bandwidth_mbps" in assignment
            or "expected_east_west_bandwidth_mbps" in assignment
        )
        assignment["expected_north_south_bandwidth_mbps"] = max(
            0.0,
            _as_float(
                assignment.get("expected_north_south_bandwidth_mbps"),
                assignment_legacy_bandwidth if not assignment_has_directional else 0.0,
            ),
        )
        assignment["expected_east_west_bandwidth_mbps"] = max(
            0.0, _as_float(assignment.get("expected_east_west_bandwidth_mbps"), 0.0)
        )
        assignment["expected_bandwidth_mbps"] = round(
            assignment["expected_north_south_bandwidth_mbps"]
            + assignment["expected_east_west_bandwidth_mbps"],
            6,
        )
        assignment["expected_packet_rate_pps"] = max(
            0.0, _as_float(assignment.get("expected_packet_rate_pps"))
        )
        assignment["link_speed_mbps"] = max(
            0, _as_int(assignment.get("link_speed_mbps"))
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

    for power_link in data["network_power_connections"]:
        if not isinstance(power_link, dict):
            continue
        for field in (
            "id", "from_instance_id", "from_port", "to_instance_id", "to_port",
            "feed_label", "phase", "notes",
        ):
            power_link.setdefault(field, "")
        power_link["voltage_v"] = max(0.0, _as_float(power_link.get("voltage_v"), 230.0))
        power_link["capacity_w"] = max(0.0, _as_float(power_link.get("capacity_w")))
        power_link["load_w"] = max(0.0, _as_float(power_link.get("load_w")))
        power_link["auto_generated"] = bool(power_link.get("auto_generated", False))

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
        for field in ("id", "name", "network_type", "provider", "asn", "location_name", "demarcation_instance_id", "notes", "service_type"):
            external.setdefault(field, "")
        external["prefixes"] = _normalise_string_list(external.get("prefixes", []))
        external["peer_instance_ids"] = _normalise_string_list(external.get("peer_instance_ids", []))
        external["redundant"] = bool(external.get("redundant", settings.get("external_network_redundancy", True)))
        external["required_links"] = max(1, min(8, _as_int(external.get("required_links"), settings.get("external_network_link_count", 2) if external["redundant"] else 1)))
        external["medium"] = _text(external.get("medium")).lower() or "fibre"
        if external["medium"] not in {"fibre", "copper", "wireless"}:
            external["medium"] = "fibre"
        external["planner_enabled"] = bool(external.get("planner_enabled", True))
        external["router_asset_id"] = _text(external.get("router_asset_id"))
        external["bandwidth_mbps"] = max(0.0, _as_float(external.get("bandwidth_mbps")))

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
        node["tray_number"] = max(0, _as_int(node.get("tray_number")))
        node["max_splices_per_tray"] = max(1, min(24, _as_int(node.get("max_splices_per_tray"), 24)))
        node["incoming_cable_id"] = _text(node.get("incoming_cable_id"))
        node["cassette_position"] = max(0, _as_int(node.get("cassette_position", node.get("tray_number"))))
        node["front_connector"] = _text(node.get("front_connector")).lower()
        node["front_connector_capacity"] = max(0, _as_int(node.get("front_connector_capacity"), 12))
        node["termination_mode"] = _text(node.get("termination_mode")).lower()
        node["rear_connector"] = _text(node.get("rear_connector")).lower()
        node["rear_connector_count"] = max(0, min(4, _as_int(node.get("rear_connector_count"))))
        node["used_front_connectors"] = max(0, _as_int(node.get("used_front_connectors")))
        node["used_front_connector_names"] = _normalise_string_list(
            node.get("used_front_connector_names", [])
        )
        node["used_fibres"] = max(0, _as_int(node.get("used_fibres")))
        node["cable_ids"] = _normalise_string_list(node.get("cable_ids", []))
        node["circuit_ids"] = _normalise_string_list(node.get("circuit_ids", []))
        node["auto_generated"] = bool(node.get("auto_generated", False))

    # Fixed-installation fibre cable definitions are reusable planner inputs.
    if not data.get("network_fibre_cable_types"):
        data["network_fibre_cable_types"] = deepcopy(DEFAULT_FIBRE_CABLE_TYPES)
    normalised_cable_types = []
    seen_cable_type_ids = set()
    for index, cable_type in enumerate(data.get("network_fibre_cable_types", []), start=1):
        if not isinstance(cable_type, dict):
            continue
        type_id = _text(cable_type.get("id")) or f"FCT{index}"
        if type_id in seen_cable_type_ids:
            suffix = 2
            while f"{type_id}-{suffix}" in seen_cable_type_ids:
                suffix += 1
            type_id = f"{type_id}-{suffix}"
        seen_cable_type_ids.add(type_id)
        normalised_cable_types.append({
            **cable_type,
            "id": type_id,
            "name": _text(cable_type.get("name")) or type_id,
            "fibre_standard": _text(cable_type.get("fibre_standard")) or _text(cable_type.get("cable_type")) or "OS2",
            "core_count": max(1, _as_int(cable_type.get("core_count"), 12)),
            "attenuation_db_per_m": max(0.0, _as_float(cable_type.get("attenuation_db_per_m"), 0.00035)),
            "connector_loss_db": max(0.0, _as_float(cable_type.get("connector_loss_db"), 0.5)),
            "reflection_loss_db": max(0.0, _as_float(cable_type.get("reflection_loss_db"), 55.0)),
            "splice_loss_db": max(0.0, _as_float(cable_type.get("splice_loss_db"), 0.1)),
            "wavelength_nm": max(0, _as_int(cable_type.get("wavelength_nm"), 1310)),
            "manufacturer": _text(cable_type.get("manufacturer")),
            "model": _text(cable_type.get("model")),
            "construction": _text(cable_type.get("construction")),
            "sheath_type": _text(cable_type.get("sheath_type")),
            "notes": _text(cable_type.get("notes")),
        })
    data["network_fibre_cable_types"] = normalised_cable_types or deepcopy(DEFAULT_FIBRE_CABLE_TYPES)
    cable_types_by_id = {_text(row.get("id")): row for row in data["network_fibre_cable_types"]}

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
        cable_type_id = _text(cable.get("cable_type_id"))
        cable_type = cable_types_by_id.get(cable_type_id, {})
        cable["cable_type_id"] = cable_type_id
        cable["core_count"] = max(1, _as_int(cable.get("core_count"), _as_int(cable_type.get("core_count"), settings["default_fibre_core_count"])))
        cable["cable_type"] = _text(cable.get("cable_type")) or _text(cable_type.get("name")) or _text(cable_type.get("fibre_standard")) or "OS2 single-mode fibre"
        cable["attenuation_db_per_m"] = max(0.0, _as_float(cable.get("attenuation_db_per_m"), _as_float(cable_type.get("attenuation_db_per_m"), 0.00035)))
        cable["connector_loss_db"] = max(0.0, _as_float(cable.get("connector_loss_db"), _as_float(cable_type.get("connector_loss_db"), 0.5)))
        cable["reflection_loss_db"] = max(0.0, _as_float(cable.get("reflection_loss_db"), _as_float(cable_type.get("reflection_loss_db"), 55.0)))
        cable["splice_loss_db"] = max(0.0, _as_float(cable.get("splice_loss_db"), _as_float(cable_type.get("splice_loss_db"), 0.1)))
        cable["wavelength_nm"] = max(0, _as_int(cable.get("wavelength_nm"), _as_int(cable_type.get("wavelength_nm"), 1310)))
        cable["routing_role"] = _text(cable.get("routing_role")) or "direct"
        cable["parent_cable_id"] = _text(cable.get("parent_cable_id"))
        cable["branch_node_id"] = _text(cable.get("branch_node_id"))
        cable["from_termination_method"] = _text(cable.get("from_termination_method")) or "connectorised"
        cable["to_termination_method"] = _text(cable.get("to_termination_method")) or "connectorised"
        cable["from_connector_type"] = _text(cable.get("from_connector_type")).lower()
        cable["to_connector_type"] = _text(cable.get("to_connector_type")).lower()
        cable["connectorised_breakout"] = bool(cable.get("connectorised_breakout", False))
        for side in ("from", "to"):
            allocation_key = f"{side}_panel_termination_allocations"
            raw_allocations = cable.get(allocation_key, [])
            cable[allocation_key] = [
                dict(row) for row in raw_allocations if isinstance(row, dict)
            ] if isinstance(raw_allocations, list) else []
        cable["connector_count"] = max(0, _as_int(cable.get("connector_count"), int(cable["from_termination_method"] == "connectorised") + int(cable["to_termination_method"] == "connectorised")))
        cable["splice_count"] = max(0, _as_int(cable.get("splice_count")))
        cable["estimated_attenuation_db"] = round(cable["length_m"] * cable["attenuation_db_per_m"], 6)
        cable["estimated_connector_loss_db"] = round(cable["connector_count"] * cable["connector_loss_db"], 6)
        cable["estimated_splice_loss_db"] = round(cable["splice_count"] * cable["splice_loss_db"], 6)
        cable["estimated_total_loss_db"] = round(cable["estimated_attenuation_db"] + cable["estimated_connector_loss_db"] + cable["estimated_splice_loss_db"], 6)
        cable["minimum_return_loss_db"] = max(0.0, _as_float(cable.get("minimum_return_loss_db"), cable["reflection_loss_db"]))
        cable["cores"] = build_fibre_cores(cable["core_count"], 0, "", cable.get("cores", []))
        cable["auto_generated"] = bool(cable.get("auto_generated", False))

    for splice in data["network_fibre_splices"]:
        if not isinstance(splice, dict):
            continue
        for field in ("id", "node_id", "cassette_id", "incoming_cable_id", "outgoing_cable_id", "splice_type", "circuit_id", "drawing_layer", "label", "loss_db", "notes", "termination_instance_id", "termination_port"):
            splice.setdefault(field, "")
        splice["incoming_core"] = max(1, _as_int(splice.get("incoming_core"), 1))
        splice["outgoing_core"] = max(1, _as_int(splice.get("outgoing_core"), 1))
        splice["loss_db"] = max(0.0, _as_float(splice.get("loss_db"), 0.1))
        splice["pigtail"] = bool(splice.get("pigtail", False))
        splice["connectorised"] = bool(splice.get("connectorised", False))
        splice["drawing_layer"] = _text(splice.get("drawing_layer")) or settings["physical_fibre_layer"]["splice_layer"]
        splice["label"] = _text(splice.get("label")) or _text(splice.get("id"))
        splice["auto_generated"] = bool(splice.get("auto_generated", False))

    normalised_optical_paths = []
    for path in data.get("network_optical_paths", []):
        if not isinstance(path, dict):
            continue
        normalised_optical_paths.append({
            **path,
            "id": _text(path.get("id")),
            "source_instance_id": _text(path.get("source_instance_id")),
            "destination_instance_id": _text(path.get("destination_instance_id")),
            "source_optic_module_id": _text(path.get("source_optic_module_id")),
            "destination_optic_module_id": _text(path.get("destination_optic_module_id")),
            "link_speed_mbps": max(0, _as_int(path.get("link_speed_mbps"))),
            "connection_ids": _normalise_string_list(path.get("connection_ids", [])),
            "fibre_cable_ids": _normalise_string_list(path.get("fibre_cable_ids", [])),
            "transmit_power_dbm": "" if _text(path.get("transmit_power_dbm")) == "" else _as_float(path.get("transmit_power_dbm")),
            "receiver_sensitivity_dbm": "" if _text(path.get("receiver_sensitivity_dbm")) == "" else _as_float(path.get("receiver_sensitivity_dbm")),
            "path_loss_db": max(0.0, _as_float(path.get("path_loss_db"))),
            "available_budget_db": "" if _text(path.get("available_budget_db")) == "" else _as_float(path.get("available_budget_db")),
            "margin_db": "" if _text(path.get("margin_db")) == "" else _as_float(path.get("margin_db")),
            "minimum_return_loss_db": max(0.0, _as_float(path.get("minimum_return_loss_db"))),
            "status": _text(path.get("status")) or "unconfigured",
            "missing_properties": _text(path.get("missing_properties")),
            "notes": _text(path.get("notes")),
        })
    data["network_optical_paths"] = normalised_optical_paths
    data["network_schema_version"] = NETWORK_SCHEMA_VERSION

    set_core_status_from_splices(data)
    return data


def next_network_id(items: Iterable[dict], prefix: str) -> str:
    used = {_text(item.get("id")) for item in items if isinstance(item, dict)}
    number = 1
    while f"{prefix}{number}" in used:
        number += 1
    return f"{prefix}{number}"


def network_assets_by_id(data: dict) -> Dict[str, dict]:
    if _as_int(data.get("network_schema_version"), 0) < NETWORK_SCHEMA_VERSION:
        ensure_network_schema(data)
    return {
        _text(item.get("id")): item
        for item in data.get("network_assets", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }


def network_instances_by_id(data: dict) -> Dict[str, dict]:
    if _as_int(data.get("network_schema_version"), 0) < NETWORK_SCHEMA_VERSION:
        ensure_network_schema(data)
    return {
        _text(item.get("id")): item
        for item in data.get("network_asset_instances", [])
        if isinstance(item, dict) and _text(item.get("id"))
    }


def network_instances_for_floor(data: dict, floor: int) -> Dict[str, dict]:
    if _as_int(data.get("network_schema_version"), 0) < NETWORK_SCHEMA_VERSION:
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
    validate_unique_id("network_power_connections", "Power connection")
    validate_unique_id("network_endpoint_assignments", "Network endpoint assignment")
    validate_unique_id("network_patch_leads", "Network patch lead")
    validate_unique_id("network_redundancy_groups", "Network redundancy group")
    vlan_record_ids = validate_unique_id("network_vlans", "VLAN record")
    validate_unique_id("network_routes", "Network route")
    validate_unique_id("network_ip_allocations", "IP allocation")
    validate_unique_id("network_external_networks", "External network")
    optic_module_ids = validate_unique_id("network_optic_modules", "Optic module")
    fibre_cable_type_ids = validate_unique_id("network_fibre_cable_types", "Fibre cable type")
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
    fibre_connected_instance_ids = {
        _text(connection.get(key))
        for connection in data.get("network_connections", [])
        if isinstance(connection, dict) and _text(connection.get("medium")).lower() == "fibre"
        for key in ("from_instance_id", "to_instance_id")
        if _text(connection.get(key))
    }
    fibre_connected_asset_ids = {
        _text(instances_by_id.get(instance_id, {}).get("asset_id"))
        for instance_id in fibre_connected_instance_ids
        if _text(instances_by_id.get(instance_id, {}).get("asset_id"))
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
        if bool(asset.get("modular_patch_panel")):
            cassette_count = _as_int(asset.get("patch_panel_cassette_count"), 0)
            cassette_capacity = _as_int(asset.get("patch_panel_cassette_capacity"), 0)
            front_connector = _text(asset.get("patch_panel_cassette_front_connector")).lower()
            termination_mode = _text(asset.get("patch_panel_cassette_termination_mode")).lower()
            rear_connector = _text(asset.get("patch_panel_cassette_rear_connector")).lower()
            rear_count = _as_int(asset.get("patch_panel_cassette_rear_connector_count"), 0)
            if _text(asset.get("patch_panel_type")).lower() != "fibre":
                messages.append(f"Modular patch panel {asset_id} must use fibre media.")
            if cassette_count < 1 or cassette_count > 4:
                messages.append(f"Modular patch panel {asset_id} must contain between one and four cassette positions.")
            if cassette_capacity != 12:
                messages.append(f"Modular patch panel {asset_id} cassettes must provide 12 front connector positions each.")
            if front_connector not in MODULAR_PANEL_FRONT_CONNECTORS:
                messages.append(f"Modular patch panel {asset_id} has an unsupported cassette front connector.")
            if termination_mode not in MODULAR_PANEL_TERMINATION_MODES:
                messages.append(f"Modular patch panel {asset_id} has an unsupported cassette termination mode.")
            if termination_mode == "connectorised":
                if rear_connector not in MODULAR_PANEL_REAR_CONNECTORS:
                    messages.append(f"Modular patch panel {asset_id} requires an MPO/MTP rear connector type.")
                if rear_count < 1 or rear_count > 4:
                    messages.append(f"Modular patch panel {asset_id} must accept between one and four rear MPO/MTP connectors per cassette.")
            expected_ports = max(0, cassette_count) * 12
            declared_ports = sum(
                max(0, _as_int(row.get("port_count")))
                for row in asset.get("port_definitions", [])
                if isinstance(row, dict)
            )
            if expected_ports and declared_ports != expected_ports:
                messages.append(
                    f"Modular patch panel {asset_id} declares {declared_ports} ports but "
                    f"{cassette_count} cassettes require {expected_ports} front connector positions."
                )
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
        if asset_type in {"wireless_access_point", "wireless_device"} and not asset.get("frequencies"):
            messages.append(
                f"Wireless device {asset_id} requires at least one frequency."
            )
        if asset_id in fibre_connected_asset_ids:
            is_passive_optic = asset_type == "fibre_splitter" or (
                asset_type == "patch_panel" and _text(asset.get("patch_panel_type")).lower() == "fibre"
            )
            if is_passive_optic:
                if _text(asset.get("optical_insertion_loss_db")) == "":
                    messages.append(f"Passive optical asset {asset_id} requires insertion loss in dB.")
                if _text(asset.get("optical_return_loss_db")) == "":
                    messages.append(f"Passive optical asset {asset_id} requires return loss in dB.")
            else:
                has_pluggable_cage = any(
                    _text(row.get("port_type")).lower() in PLUGGABLE_OPTIC_PORT_TYPES
                    for row in asset.get("port_definitions", [])
                    if isinstance(row, dict)
                )
                if not has_pluggable_cage:
                    integrated_rows = [
                        row
                        for row in asset.get("port_definitions", [])
                        if isinstance(row, dict)
                        and _text(row.get("port_type")).lower() in {"pon", "lc", "sc", "mpo"}
                    ]
                    has_tx = _text(asset.get("optical_tx_power_dbm")) != "" or any(
                        _text(row.get("transmit_power_dbm")) != "" for row in integrated_rows
                    )
                    has_rx = _text(asset.get("optical_receiver_sensitivity_dbm")) != "" or any(
                        _text(row.get("receiver_sensitivity_dbm")) != "" for row in integrated_rows
                    )
                    if not has_tx:
                        messages.append(f"Active optical asset {asset_id} requires transmit power in dBm.")
                    if not has_rx:
                        messages.append(f"Active optical asset {asset_id} requires receiver sensitivity in dBm.")

    for module in data.get("network_optic_modules", []):
        if not isinstance(module, dict):
            continue
        module_id = _text(module.get("id")) or "(unnamed)"
        asset = assets_by_id.get(_text(module.get("asset_id")), {})
        if _text(asset.get("asset_type")) != "optical_transceiver":
            messages.append(f"Optic module {module_id} must reference an optical transceiver asset.")
        if _text(module.get("host_instance_id")) not in instances_by_id:
            messages.append(f"Optic module {module_id} references a missing host instance.")
        if not _text(module.get("host_port")):
            messages.append(f"Optic module {module_id} has no host port.")
        speed = _as_int(module.get("link_speed_mbps"))
        supported = normalise_port_speeds(asset.get("supported_speeds_mbps"))
        if speed and supported and speed not in supported:
            messages.append(f"Optic module {module_id} does not support its selected {port_speed_label(speed)} speed.")

    def _speeds_for_connection_port(instance_id: str, observed_port: str) -> List[int]:
        instance = instances_by_id.get(instance_id, {})
        asset = assets_by_id.get(_text(instance.get("asset_id")), {})
        target = _text(observed_port).lower()
        if "/" in target:
            target = target.split("/", 1)[1]
        for row in asset.get("port_definitions", []):
            if not isinstance(row, dict):
                continue
            prefix = _text(row.get("name_prefix")).lower()
            explicit = [_text(value).lower() for value in row.get("explicit_names", []) if _text(value)]
            if target in explicit or (prefix and target.startswith(prefix)):
                return normalise_port_speeds(row.get("supported_speeds_mbps"))
        rows = [row for row in asset.get("port_definitions", []) if isinstance(row, dict)]
        if len(rows) == 1:
            return normalise_port_speeds(rows[0].get("supported_speeds_mbps"))
        return []

    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict):
            continue
        if not bool(connection.get("physical_connection")) and not bool(
            connection.get("topology_hidden")
        ):
            left_instance = instances_by_id.get(
                _text(connection.get("from_instance_id")), {}
            )
            right_instance = instances_by_id.get(
                _text(connection.get("to_instance_id")), {}
            )
            left_role = _text(left_instance.get("design_role")).lower()
            right_role = _text(right_instance.get("design_role")).lower()
            if "edge_router" in {left_role, right_role}:
                peer = right_instance if left_role == "edge_router" else left_instance
                peer_role = _text(peer.get("design_role")).lower()
                peer_asset = assets_by_id.get(_text(peer.get("asset_id")), {})
                peer_type = _text(peer_asset.get("asset_type")).lower()
                if peer_role != "core_switch" and peer_type != "external_network":
                    messages.append(
                        f"Edge-router connection {_text(connection.get('id'))} must terminate "
                        "at a core switch (or an external-network demarcation), not "
                        f"{peer_role or peer_type or 'an unclassified device'}."
                    )
        speed = _as_int(connection.get("link_speed_mbps"))
        if speed <= 0:
            continue
        left = _speeds_for_connection_port(_text(connection.get("from_instance_id")), _text(connection.get("from_port")))
        right = _speeds_for_connection_port(_text(connection.get("to_instance_id")), _text(connection.get("to_port")))
        if left and speed not in left:
            messages.append(f"Network connection {_text(connection.get('id'))} selects {port_speed_label(speed)} on a source port that does not support it.")
        if right and speed not in right:
            messages.append(f"Network connection {_text(connection.get('id'))} selects {port_speed_label(speed)} on a destination port that does not support it.")
        if left and right and speed not in set(left) & set(right):
            messages.append(f"Network connection {_text(connection.get('id'))} has no common port speed at {port_speed_label(speed)}.")

    lag_groups: Dict[str, List[dict]] = {}
    for connection in data.get("network_connections", []):
        if not isinstance(connection, dict):
            continue
        if bool(connection.get("physical_connection")):
            continue
        lag_id = _text(connection.get("link_aggregation_group_id"))
        if lag_id:
            lag_groups.setdefault(lag_id, []).append(connection)

    for lag_id, members in lag_groups.items():
        if len(members) < 2:
            messages.append(
                f"Link aggregation group {lag_id} contains only one member connection."
            )
            continue
        endpoint_pairs = {
            tuple(
                sorted(
                    (
                        _text(row.get("from_instance_id")),
                        _text(row.get("to_instance_id")),
                    )
                )
            )
            for row in members
        }
        media = {_text(row.get("medium")).lower() for row in members}
        speeds = {max(0, _as_int(row.get("link_speed_mbps"))) for row in members}
        if len(endpoint_pairs) != 1:
            messages.append(
                f"Link aggregation group {lag_id} spans more than one device pair."
            )
        if len(media) != 1:
            messages.append(
                f"Link aggregation group {lag_id} mixes connection media."
            )
        if len(speeds) != 1 or 0 in speeds:
            messages.append(
                f"Link aggregation group {lag_id} must use the same declared speed on every member."
            )
        indices = sorted(
            max(0, _as_int(row.get("link_aggregation_member_index")))
            for row in members
        )
        expected_indices = list(range(1, len(members) + 1))
        if indices != expected_indices:
            messages.append(
                f"Link aggregation group {lag_id} has invalid member indexes {indices}; "
                f"expected {expected_indices}."
            )
        declared_counts = {
            max(0, _as_int(row.get("link_aggregation_member_count")))
            for row in members
        }
        if declared_counts != {len(members)}:
            messages.append(
                f"Link aggregation group {lag_id} declares member counts "
                f"{sorted(declared_counts)} but contains {len(members)} connections."
            )
        member_speed = next(iter(speeds)) if len(speeds) == 1 else 0
        aggregate_capacity = member_speed * len(members)
        declared_aggregate = {
            max(0, _as_int(row.get("aggregate_link_speed_mbps")))
            for row in members
        }
        if declared_aggregate != {aggregate_capacity}:
            messages.append(
                f"Link aggregation group {lag_id} should declare "
                f"{port_speed_label(aggregate_capacity)} aggregate capacity."
            )
        expected_load = max(
            [
                max(0.0, _as_float(row.get("aggregate_expected_bandwidth_mbps")))
                for row in members
            ]
            or [0.0]
        )
        if aggregate_capacity and expected_load > aggregate_capacity + 1e-9:
            messages.append(
                f"Link aggregation group {lag_id} expects {expected_load:g} Mbps but "
                f"provides only {aggregate_capacity:g} Mbps aggregate capacity."
            )

    for assignment in data.get("network_endpoint_assignments", []):
        if not isinstance(assignment, dict):
            continue
        speed = _as_int(assignment.get("link_speed_mbps"))
        if speed <= 0:
            continue
        instance_id = _text(assignment.get("network_instance_id"))
        port = _text(assignment.get("network_port"))
        supported = _speeds_for_connection_port(instance_id, port)
        assignment_id = _text(assignment.get("id")) or "(unnamed)"
        if supported and speed not in supported:
            messages.append(
                f"Endpoint assignment {assignment_id} selects {port_speed_label(speed)} "
                "on a device port that does not support it."
            )
        expected = max(0.0, _as_float(assignment.get("expected_bandwidth_mbps")))
        if expected > speed + 1e-9:
            messages.append(
                f"Endpoint assignment {assignment_id} expects {expected:g} Mbps but its "
                f"selected port rate is only {port_speed_label(speed)}."
            )

    pdu_loads: Dict[str, float] = {}
    pdu_outlets: Dict[str, int] = {}
    for power_link in data.get("network_power_connections", []):
        if not isinstance(power_link, dict):
            continue
        link_id = _text(power_link.get("id")) or "(unnamed)"
        source_id = _text(power_link.get("from_instance_id"))
        target_id = _text(power_link.get("to_instance_id"))
        if source_id not in instance_ids:
            messages.append(f"Power connection {link_id} references missing source instance {source_id!r}.")
            continue
        if target_id not in instance_ids:
            messages.append(f"Power connection {link_id} references missing destination instance {target_id!r}.")
            continue
        source_asset = assets_by_id.get(_text(instances_by_id.get(source_id, {}).get("asset_id")), {})
        target_asset = assets_by_id.get(_text(instances_by_id.get(target_id, {}).get("asset_id")), {})
        source_type = _text(source_asset.get("asset_type")).lower()
        target_type = _text(target_asset.get("asset_type")).lower()
        if source_type not in {"ups", "pdu", "power_device"}:
            messages.append(f"Power connection {link_id} starts from non-power asset {source_id}.")
        if target_type == "pdu" and source_type not in {"ups", "power_device"}:
            messages.append(f"PDU {target_id} must be supplied by a UPS or UPS-backed power source.")
        if (
            target_type == "pdu"
            and source_type == "power_device"
            and not bool(source_asset.get("ups_backed_source", False))
        ):
            messages.append(
                f"PDU {target_id} is supplied by power source {source_id}, but that source is not marked as UPS-backed."
            )
        if source_type == "pdu":
            pdu_loads[source_id] = pdu_loads.get(source_id, 0.0) + max(0.0, _as_float(power_link.get("load_w")))
            pdu_outlets[source_id] = pdu_outlets.get(source_id, 0) + 1

    for pdu_id, outlet_count in pdu_outlets.items():
        pdu_instance = instances_by_id.get(pdu_id, {})
        pdu_asset = assets_by_id.get(_text(pdu_instance.get("asset_id")), {})
        declared_outlets = max(0, _as_int(pdu_asset.get("power_outlet_count")))
        declared_capacity = max(0.0, _as_float(pdu_asset.get("power_capacity_w")))
        if declared_outlets and outlet_count > declared_outlets:
            messages.append(f"PDU {pdu_id} uses {outlet_count} outlets but only {declared_outlets} are declared.")
        if declared_capacity and pdu_loads.get(pdu_id, 0.0) > declared_capacity + 1e-9:
            messages.append(
                f"PDU {pdu_id} carries {pdu_loads[pdu_id]:.1f} W but is rated for {declared_capacity:.1f} W."
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

        traffic = network_traffic_loads(data)
        carried = traffic.get("carried_by_instance", {})
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


        connection_loads = traffic.get("by_connection", {})
        for connection in data.get("network_connections", []):
            if not isinstance(connection, dict):
                continue
            if bool(connection.get("physical_connection")):
                continue
            connection_id = _text(connection.get("id"))
            speed = max(0, _as_int(connection.get("link_speed_mbps")))
            if not connection_id or speed <= 0:
                continue
            bandwidth_load = max(
                0.0,
                _as_float(
                    connection_loads.get(connection_id, {}).get("bandwidth_mbps")
                ),
            )
            if bandwidth_load > speed + 1e-9:
                messages.append(
                    f"Network connection {connection_id} carries "
                    f"{bandwidth_load:.3f} Mbps but its member speed is only "
                    f"{port_speed_label(speed)}."
                )

        for lag_id, load in traffic.get(
            "by_link_aggregation_group", {}
        ).items():
            aggregate_capacity = max(
                0.0, _as_float(load.get("aggregate_capacity_mbps"))
            )
            bandwidth_load = max(0.0, _as_float(load.get("bandwidth_mbps")))
            if (
                aggregate_capacity > 0.0
                and bandwidth_load > aggregate_capacity + 1e-9
            ):
                messages.append(
                    f"Link aggregation group {lag_id} carries "
                    f"{bandwidth_load:.3f} Mbps but provides only "
                    f"{aggregate_capacity:.3f} Mbps aggregate capacity."
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
        protection_type = _text(group.get("protection_type")).lower()
        if protection_type in {
            "independent_layer_uplinks",
            "independent_core_uplinks",
            "redundant_external_network_links",
        }:
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
        cable_type_id = _text(cable.get("cable_type_id"))
        if cable_type_id and cable_type_id not in fibre_cable_type_ids:
            messages.append(f"Fibre cable {cable_id} references missing cable type {cable_type_id!r}.")
        if _as_float(cable.get("attenuation_db_per_m")) <= 0.0:
            messages.append(f"Fibre cable {cable_id} requires a positive attenuation value in dB/m.")
        if _text(cable.get("reflection_loss_db")) == "":
            messages.append(f"Fibre cable {cable_id} requires a configured reflection/return-loss value.")
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
        termination_instance_id = _text(splice.get("termination_instance_id"))
        if outgoing is None and not termination_instance_id:
            messages.append(f"Fibre splice {splice_id} references a missing outgoing cable or termination.")
        elif outgoing is not None and _as_int(splice.get("outgoing_core")) > _as_int(outgoing.get("core_count")):
            messages.append(f"Fibre splice {splice_id} outgoing core exceeds cable capacity.")
        if termination_instance_id and termination_instance_id not in instance_ids:
            messages.append(f"Fibre splice {splice_id} references missing termination instance {termination_instance_id!r}.")

    for optical_path in data.get("network_optical_paths", []):
        if not isinstance(optical_path, dict):
            continue
        path_id = _text(optical_path.get("id")) or "(unnamed)"
        status = _text(optical_path.get("status")).lower()
        if status == "fail":
            messages.append(
                f"Optical path {path_id} fails its light budget with margin "
                f"{_as_float(optical_path.get('margin_db')):.3f} dB."
            )
        elif status == "unconfigured":
            missing = _text(optical_path.get("missing_properties")) or "optic/passive properties"
            messages.append(f"Optical path {path_id} is unconfigured: {missing}.")

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
