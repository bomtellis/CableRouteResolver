""" Network topology view for CableRouteResolver.

The view is deliberately read-only: network placement and connection editing remain
in the main graph.  This module turns the installed network records into a
hierarchical topology with expandable branches, redundant/failover links,
endpoint groups, utilisation indicators, and a device details panel.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import math
import re
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from PySide6.QtCore import QPoint, QPointF, QRectF, Qt, QTimer, Signal
from PySide6.QtGui import (
    QBrush,
    QColor,
    QFont,
    QFontMetrics,
    QPainter,
    QPainterPath,
    QPen,
)
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFrame,
    QGraphicsItem,
    QGraphicsObject,
    QGraphicsPathItem,
    QGraphicsScene,
    QGraphicsSimpleTextItem,
    QGraphicsView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QStyle,
    QStyleOptionGraphicsItem,
    QSplitter,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from network_schema import ensure_network_schema


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


def _human_number(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}k"
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.1f}"


def _asset_port_definitions(asset: dict) -> List[dict]:
    rows = [row for row in asset.get("port_definitions", []) if isinstance(row, dict) and _int(row.get("port_count")) > 0]
    if rows:
        return rows
    count = max(0, _int(asset.get("number_of_ports")))
    if count <= 0:
        return []
    asset_type = _text(asset.get("asset_type")).lower()
    port_type = "pon" if asset_type in {"optical_line_terminal", "optical_network_terminal"} else ("lc" if asset_type == "fibre_splitter" or _text(asset.get("patch_panel_type")).lower() == "fibre" else "rj45")
    return [{"port_type": port_type, "port_count": count, "port_use": "patch" if asset_type == "patch_panel" else "client", "name_prefix": ""}]

def _expanded_asset_ports(asset: dict) -> List[dict]:
    result = []
    counters: Dict[str, int] = defaultdict(int)
    for row in _asset_port_definitions(asset):
        port_type = _text(row.get("port_type")).lower() or "other"
        port_use = _text(row.get("port_use")).lower() or "other"
        explicit_names = [
            _text(value) for value in row.get("explicit_names", []) if _text(value)
        ] if isinstance(row.get("explicit_names", []), list) else []
        count = max(0, _int(row.get("port_count")))
        if explicit_names:
            for name in explicit_names[:count]:
                result.append({"name": name, "port_type": port_type, "port_use": port_use})
            count -= min(count, len(explicit_names))
        prefix = _text(row.get("name_prefix")) or ({"pon":"PON", "sfp":"SFP", "sfp+":"SFP+", "qsfp":"QSFP", "qsfp28":"QSFP28", "lc":"LC", "sc":"SC", "mpo":"MPO", "rj45":""}.get(port_type, port_type.upper()))
        for _ in range(count):
            counters[prefix] += 1
            # A one-port row whose prefix is already a complete physical name
            # should not be rendered as e.g. Input-A-1.
            name = prefix if count == 1 and not counters[prefix] > 1 and prefix.lower().startswith("input-") else (f"{prefix}-{counters[prefix]}" if prefix else str(counters[prefix]))
            result.append({"name": name, "port_type": port_type, "port_use": port_use})
    return result


def _canonical_port_name(asset: dict, observed_name: str, defined_ports: Sequence[dict]) -> str:
    """Map legacy/generated aliases onto the declared physical port list.

    Device views previously unioned observed connection labels with declared
    ports.  Names such as ``1`` versus ``Gi-1`` or ``In-1`` versus ``Input-A``
    therefore created extra physical sockets.  This function resolves those
    aliases without inventing additional ports.
    """
    observed = _text(observed_name)
    if not observed or observed == "Unspecified" or not defined_ports:
        return observed
    by_lower = {_text(row.get("name")).lower(): _text(row.get("name")) for row in defined_ports}
    exact = by_lower.get(observed.lower())
    if exact:
        return exact

    lowered = observed.lower()
    match = re.search(r"(\d+)(?!.*\d)", lowered)
    ordinal = int(match.group(1)) if match else 1
    wanted_use = ""
    wanted_type = ""
    if lowered.startswith(("input", "in-", "in ")):
        wanted_use = "input"
    elif lowered.startswith(("output", "out-", "out ")):
        wanted_use = "output"
    elif "uplink" in lowered:
        wanted_use = "uplink"
    elif "pon" in lowered:
        wanted_type = "pon"
    elif any(token in lowered for token in ("sfp", "qsfp")):
        wanted_type = "qsfp" if "qsfp" in lowered else "sfp"

    candidates = list(defined_ports)
    if wanted_use:
        filtered = [row for row in candidates if _text(row.get("port_use")).lower() == wanted_use]
        if filtered:
            candidates = filtered
    if wanted_type:
        filtered = [row for row in candidates if wanted_type in _text(row.get("port_type")).lower()]
        if filtered:
            candidates = filtered
    if not wanted_use and not wanted_type and lowered.isdigit():
        filtered = [row for row in candidates if _text(row.get("port_use")).lower() in {"client", "downlink", "patch", "output"}]
        if filtered:
            candidates = filtered
    if 1 <= ordinal <= len(candidates):
        return _text(candidates[ordinal - 1].get("name"))
    return observed

def _port_definition_for_name(asset: dict, port_name: str) -> dict:
    target = _text(port_name).lower()
    ports = _expanded_asset_ports(asset)
    for row in ports:
        if _text(row.get("name")).lower() == target:
            return row
    for row in ports:
        if _text(row.get("port_type")) in target or _text(row.get("port_use")) in target:
            return row
    return {"name": port_name, "port_type": "pon" if "pon" in target else ("sfp" if any(x in target for x in ("sfp","uplink","fibre","fiber")) else "rj45"), "port_use": "other"}


def _port_group_sort_key(port: dict, fallback_name: str = "") -> Tuple[int, int, str, str]:
    """Keep connector families together while preserving their physical order."""
    type_order = {
        "rj45": 0, "pon": 1, "lc": 2, "sc": 3, "mpo": 4,
        "sfp": 5, "sfp+": 6, "qsfp": 7, "qsfp28": 8,
        "console": 9, "usb": 10, "power": 11, "other": 12,
    }
    name = _text(port.get("name")) or _text(fallback_name)
    digits = "".join(ch for ch in name if ch.isdigit())
    return (type_order.get(_text(port.get("port_type")).lower(), 99),
            int(digits) if digits else 999999,
            _text(port.get("port_use")).lower(), name.lower())

def _role_rank(role: str, asset_type: str) -> int:
    role = role.lower()
    asset_type = asset_type.lower()
    if asset_type in {"firewall", "network_router"} or any(word in role for word in ("gateway", "router", "firewall")):
        return 0
    if "core" in role or "aggregation" in role:
        return 1
    if "distribution" in role:
        return 2
    if asset_type == "optical_line_terminal" or role.startswith("olt_"):
        return 2
    if asset_type == "network_switch" or "access_switch" in role:
        return 3
    if asset_type in {"fibre_splitter", "patch_panel"} or "splitter" in role:
        return 4
    if asset_type == "optical_network_terminal" or role == "ont":
        return 5
    if asset_type == "wireless_access_point":
        return 6
    return 7


def _type_label(asset_type: str, role: str) -> str:
    labels = {
        "patch_panel": "Patch panel",
        "fibre_splitter": "Fibre splitter",
        "network_switch": "Network switch",
        "network_router": "Router",
        "firewall": "Firewall",
        "wireless_access_point": "Wireless access point",
        "optical_line_terminal": "Optical line terminal",
        "optical_network_terminal": "Optical network terminal",
        "client_group": "Endpoint group",
        "client_device": "Client device",
        "site_group": "Installation",
        "other": "Network asset",
    }
    if role:
        role_labels = {
            "core_switch": "Core switch",
            "distribution_switch": "Distribution switch",
            "access_switch": "Access switch",
            "olt_primary": "Primary OLT",
            "olt_secondary": "Standby OLT",
            "protected_splitter": "Protected splitter",
            "splitter": "Fibre splitter",
            "ont": "ONT",
        }
        return role_labels.get(role.lower(), labels.get(asset_type, role.replace("_", " ").title()))
    return labels.get(asset_type, asset_type.replace("_", " ").title() or "Network asset")


def _icon_text(asset_type: str, role: str) -> str:
    role = role.lower()
    mapping = {
        "firewall": "FW",
        "network_router": "R",
        "network_switch": "SW",
        "optical_line_terminal": "OLT",
        "optical_network_terminal": "ONT",
        "fibre_splitter": "SPL",
        "wireless_access_point": "AP",
        "patch_panel": "PP",
        "client_group": "CL",
        "client_device": "END",
        "site_group": "SITE",
    }
    if "core" in role:
        return "CORE"
    if role == "olt_primary":
        return "OLT-A"
    if role == "olt_secondary":
        return "OLT-B"
    return mapping.get(asset_type, "NET")


@dataclass
class TopologyNode:
    node_id: str
    name: str
    asset_type: str
    role: str
    asset: dict = field(default_factory=dict)
    instance: dict = field(default_factory=dict)
    location: dict = field(default_factory=dict)
    floor: int = 0
    location_name: str = ""
    manufacturer: str = ""
    model: str = ""
    management_ip: str = ""
    port_capacity: int = 0
    ports_used: int = 0
    poe_budget_w: float = 0.0
    poe_used_w: float = 0.0
    connection_count: int = 0
    endpoint_count: int = 0
    endpoint_locations: int = 0
    pseudo: bool = False
    details: dict = field(default_factory=dict)

    @property
    def type_label(self) -> str:
        if bool(self.instance.get("logical_stack")):
            return "Switch stack"
        return _type_label(self.asset_type, self.role)

    @property
    def icon_text(self) -> str:
        return _icon_text(self.asset_type, self.role)

    @property
    def utilisation(self) -> float:
        if self.port_capacity <= 0:
            return 0.0
        return self.ports_used / self.port_capacity

    @property
    def poe_utilisation(self) -> float:
        if self.poe_budget_w <= 0:
            return 0.0
        return self.poe_used_w / self.poe_budget_w

    @property
    def state(self) -> str:
        if self.pseudo:
            return "online"
        if self.port_capacity and self.ports_used > self.port_capacity:
            return "error"
        if self.poe_budget_w and self.poe_used_w > self.poe_budget_w + 0.001:
            return "error"
        if self.connection_count == 0 and self.endpoint_count == 0:
            return "offline"
        if self.utilisation >= 0.90 or self.poe_utilisation >= 0.90:
            return "warning"
        return "online"


@dataclass
class TopologyEdge:
    edge_id: str
    source_id: str
    target_id: str
    medium: str
    source_port: str
    target_port: str
    length_m: float
    standby: bool
    redundancy_role: str
    protection_group: str
    connection: dict = field(default_factory=dict)

    @property
    def label(self) -> str:
        medium = self.medium.title() if self.medium else "Link"
        if self.length_m > 0.05:
            medium += f" · {self.length_m:.0f} m"
        if self.standby or self.redundancy_role.lower() in {"secondary", "standby"}:
            medium += " · Failover"
        return medium


class TopologyModel:
    """Create an orientation and spanning hierarchy from network connections."""

    def __init__(self, data: dict):
        ensure_network_schema(data)
        self.data = data
        self.assets = {
            _text(item.get("id")): item
            for item in data.get("network_assets", [])
            if isinstance(item, dict) and _text(item.get("id"))
        }
        self.locations = {
            _text(item.get("name")): item
            for item in data.get("locations", [])
            if isinstance(item, dict) and _text(item.get("name"))
        }
        self.nodes: Dict[str, TopologyNode] = {}
        self.edges: List[TopologyEdge] = []
        self.edges_by_id: Dict[str, TopologyEdge] = {}
        self.adjacency: Dict[str, List[Tuple[str, TopologyEdge]]] = defaultdict(list)
        self.parent: Dict[str, str] = {}
        self.parent_edge: Dict[str, str] = {}
        self.children: Dict[str, List[str]] = defaultdict(list)
        self.roots: List[str] = []
        self.level: Dict[str, int] = {}
        self.tree_edge_ids: Set[str] = set()
        self.client_groups: Dict[str, List[TopologyNode]] = defaultdict(list)
        self._descendant_cache: Dict[str, int] = {}
        self._cross_edges_cache: Optional[List[TopologyEdge]] = None
        self._build()

    def _build(self) -> None:
        assignments_by_instance: Dict[str, List[dict]] = defaultdict(list)
        for assignment in self.data.get("network_endpoint_assignments", []):
            if not isinstance(assignment, dict):
                continue
            instance_id = _text(assignment.get("network_instance_id"))
            network_port = _text(assignment.get("network_port"))
            endpoint_name = _text(assignment.get("endpoint_name"))
            if instance_id and endpoint_name and network_port and network_port != "0":
                assignments_by_instance[instance_id].append(assignment)

        connection_counts: Dict[str, int] = defaultdict(int)
        occupied_ports_by_instance: Dict[str, Set[str]] = defaultdict(set)
        for assignment in self.data.get("network_endpoint_assignments", []):
            if not isinstance(assignment, dict):
                continue
            instance_id = _text(assignment.get("network_instance_id"))
            network_port = _text(assignment.get("network_port"))
            if instance_id and network_port and network_port != "0":
                occupied_ports_by_instance[instance_id].add(network_port)

        for connection in self.data.get("network_connections", []):
            if not isinstance(connection, dict):
                continue
            source_id = _text(connection.get("from_instance_id"))
            target_id = _text(connection.get("to_instance_id"))
            if not source_id or not target_id or source_id == target_id:
                continue
            edge_id = _text(connection.get("id")) or f"link-{len(self.edges) + 1}"
            edge = TopologyEdge(
                edge_id=edge_id,
                source_id=source_id,
                target_id=target_id,
                medium=_text(connection.get("medium")).lower() or "copper",
                source_port=_text(connection.get("from_port")),
                target_port=_text(connection.get("to_port")),
                length_m=max(
                    0.0,
                    _float(connection.get("length_m")),
                    _float(connection.get("route_length_m")),
                    _float(connection.get("cable_length_m")),
                    _float(connection.get("calculated_length_m")),
                    _float(connection.get("total_length_m")),
                ),
                standby=bool(connection.get("standby", False)),
                redundancy_role=_text(connection.get("redundancy_role")),
                protection_group=_text(connection.get("protection_group")),
                connection=connection,
            )
            self.edges.append(edge)
            self.edges_by_id[edge_id] = edge
            self.adjacency[source_id].append((target_id, edge))
            self.adjacency[target_id].append((source_id, edge))
            connection_counts[source_id] += 1
            connection_counts[target_id] += 1
            source_port = _text(connection.get("from_port"))
            target_port = _text(connection.get("to_port"))
            if source_port and source_port != "0":
                occupied_ports_by_instance[source_id].add(source_port)
            if target_port and target_port != "0":
                occupied_ports_by_instance[target_id].add(target_port)

        for instance in self.data.get("network_asset_instances", []):
            if not isinstance(instance, dict):
                continue
            instance_id = _text(instance.get("id"))
            if not instance_id:
                continue
            asset = self.assets.get(_text(instance.get("asset_id")), {})
            location_name = _text(instance.get("location_name"))
            location = self.locations.get(location_name, {})
            assignments = assignments_by_instance.get(instance_id, [])
            endpoint_locations = {_text(item.get("endpoint_name")) for item in assignments if _text(item.get("endpoint_name"))}
            stack_members = max(1, _int(instance.get("stack_member_count"), 1)) if bool(instance.get("logical_stack")) else 1
            node = TopologyNode(
                node_id=instance_id,
                name=_text(instance.get("name")) or instance_id,
                asset_type=_text(asset.get("asset_type")).lower() or "other",
                role=_text(instance.get("design_role")).lower(),
                asset=asset,
                instance=instance,
                location=location,
                floor=_int(instance.get("floor", location.get("floor", 0))),
                location_name=location_name,
                manufacturer=_text(asset.get("manufacturer")),
                model=_text(asset.get("model")),
                management_ip=_text(instance.get("management_ip")),
                port_capacity=max(
                    sum(_int(row.get("port_count")) for row in _asset_port_definitions(asset)) * stack_members,
                    max(
                        0,
                        _int(asset.get("number_of_ports")),
                        _int(asset.get("connections_in"))
                        + _int(asset.get("connections_out"))
                        + _int(asset.get("uplink_ports")),
                        _int(asset.get("connections_out")) + _int(asset.get("uplink_ports")),
                        _int(asset.get("number_of_pon_ports")),
                        _int(asset.get("pon_ports")),
                        _int(asset.get("sfp_ports")) + _int(asset.get("rj45_ports")),
                    ) * stack_members,
                    len(occupied_ports_by_instance.get(instance_id, set())),
                ),
                ports_used=len(occupied_ports_by_instance.get(instance_id, set())),
                poe_budget_w=max(0.0, _float(asset.get("poe_budget_w"))) * stack_members,
                poe_used_w=sum(max(0.0, _float(item.get("poe_power_w"))) for item in assignments),
                connection_count=connection_counts.get(instance_id, 0),
                endpoint_count=len(assignments),
                endpoint_locations=len(endpoint_locations),
            )
            self.nodes[instance_id] = node

        # Ignore links to missing instances in the diagram while retaining them in validation.
        self.edges = [edge for edge in self.edges if edge.source_id in self.nodes and edge.target_id in self.nodes]
        self.edges_by_id = {edge.edge_id: edge for edge in self.edges}
        self.adjacency.clear()
        for edge in self.edges:
            self.adjacency[edge.source_id].append((edge.target_id, edge))
            self.adjacency[edge.target_id].append((edge.source_id, edge))

        self._build_hierarchy()
        self._add_installation_root()
        self._build_client_groups(assignments_by_instance)

    def _node_root_key(self, node_id: str) -> Tuple[int, int, int, str]:
        node = self.nodes[node_id]
        location_kind = _text(node.location.get("kind")).lower()
        return (
            _role_rank(node.role, node.asset_type),
            0 if location_kind == "mer" else 1,
            -len(self.adjacency.get(node_id, [])),
            node.name.lower(),
        )

    def _edge_sort_key(self, pair: Tuple[str, TopologyEdge]) -> Tuple[int, int, int, str]:
        neighbour_id, edge = pair
        neighbour = self.nodes.get(neighbour_id)
        return (
            1 if edge.standby else 0,
            1 if edge.redundancy_role.lower() in {"secondary", "standby"} else 0,
            _role_rank(neighbour.role, neighbour.asset_type) if neighbour else 99,
            neighbour.name.lower() if neighbour else neighbour_id,
        )

    def _hierarchy_rank(self, node_id: str) -> int:
        node = self.nodes[node_id]
        return _role_rank(node.role, node.asset_type)

    def _can_descend(self, parent_id: str, child_id: str) -> bool:
        parent = self.nodes[parent_id]
        child = self.nodes[child_id]
        parent_rank = self._hierarchy_rank(parent_id)
        child_rank = self._hierarchy_rank(child_id)

        if child_rank < parent_rank:
            return False

        parent_is_access = parent.role == "access_switch" or parent.asset_type == "wireless_access_point"
        child_is_access = child.role == "access_switch" or child.asset_type == "wireless_access_point"
        if parent_is_access and child_is_access:
            return False

        return True

    def _build_hierarchy(self) -> None:
        sorted_adjacency = {
            node_id: sorted(edges, key=self._edge_sort_key)
            for node_id, edges in self.adjacency.items()
        }
        unvisited = set(self.nodes)
        ordered_nodes = sorted(self.nodes, key=lambda node_id: self._node_root_key(node_id))
        for seed in ordered_nodes:
            if seed not in unvisited:
                continue
            component: Set[str] = set()
            queue = deque([seed])
            component.add(seed)
            while queue:
                current = queue.popleft()
                for neighbour, _edge in self.adjacency.get(current, []):
                    if neighbour not in component:
                        component.add(neighbour)
                        queue.append(neighbour)
            unvisited.difference_update(component)

            discovered: Set[str] = set()
            remaining = set(component)
            ordered_component = sorted(component, key=lambda node_id: self._node_root_key(node_id))
            for root in ordered_component:
                if root not in remaining:
                    continue
                self.roots.append(root)
                self.level[root] = 0
                bfs = deque([root])
                discovered.add(root)
                remaining.discard(root)
                while bfs:
                    current = bfs.popleft()
                    for neighbour, edge in sorted_adjacency.get(current, []):
                        if neighbour not in component or neighbour in discovered:
                            continue
                        if not self._can_descend(current, neighbour):
                            continue
                        discovered.add(neighbour)
                        remaining.discard(neighbour)
                        self.parent[neighbour] = current
                        self.parent_edge[neighbour] = edge.edge_id
                        self.children[current].append(neighbour)
                        self.level[neighbour] = self.level[current] + 1
                        self.tree_edge_ids.add(edge.edge_id)
                        bfs.append(neighbour)

        for parent_id, child_ids in self.children.items():
            child_ids.sort(
                key=lambda child_id: (
                    self.nodes[child_id].floor,
                    self.nodes[child_id].location_name.lower(),
                    _role_rank(self.nodes[child_id].role, self.nodes[child_id].asset_type),
                    self.nodes[child_id].name.lower(),
                )
            )
        self.roots.sort(key=lambda node_id: self._node_root_key(node_id))

    def _add_installation_root(self) -> None:
        """Group disconnected components under one installation card for a coherent overview."""
        if len(self.roots) == 1 and self.nodes.get(self.roots[0]) and self.nodes[self.roots[0]].asset_type == "site_group":
            return
        site_id = "topology::installation"
        project_name = _text(self.data.get("project", {}).get("name")) or "Network installation"
        original_roots = list(self.roots)
        if not original_roots:
            return
        self.nodes[site_id] = TopologyNode(
            node_id=site_id,
            name=project_name,
            asset_type="site_group",
            role="site_root",
            connection_count=len(original_roots),
            pseudo=True,
            details={"component_count": len(original_roots)},
        )
        self.roots = [site_id]
        self.level[site_id] = 0
        self.children[site_id] = original_roots
        for root_id in original_roots:
            self.parent[root_id] = site_id
            self.parent_edge[root_id] = ""
        for node_id in list(self.level):
            if node_id != site_id:
                self.level[node_id] += 1

    def _build_client_groups(self, assignments_by_instance: Dict[str, List[dict]]) -> None:
        for instance_id, assignments in assignments_by_instance.items():
            if instance_id not in self.nodes:
                continue
            grouped: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
            for assignment in assignments:
                department_id = _text(assignment.get("department_id")) or "UNASSIGNED"
                department_name = _text(assignment.get("department_name")) or department_id
                grouped[(department_id, department_name)].append(assignment)
            rows: List[TopologyNode] = []
            for (department_id, department_name), group in sorted(grouped.items(), key=lambda item: item[0][1].lower()):
                endpoint_names = {_text(item.get("endpoint_name")) for item in group if _text(item.get("endpoint_name"))}
                pseudo_id = f"client::{instance_id}::{department_id}"
                rows.append(
                    TopologyNode(
                        node_id=pseudo_id,
                        name=department_name if department_name != "UNASSIGNED" else "Unassigned endpoints",
                        asset_type="client_group",
                        role="client_group",
                        floor=self.nodes[instance_id].floor,
                        location_name=f"{len(endpoint_names)} data-point locations",
                        ports_used=len(group),
                        poe_used_w=sum(max(0.0, _float(item.get("poe_power_w"))) for item in group),
                        endpoint_count=len(group),
                        endpoint_locations=len(endpoint_names),
                        pseudo=True,
                        details={
                            "department_id": department_id,
                            "assignments": group,
                            "parent_instance_id": instance_id,
                        },
                    )
                )
            self.client_groups[instance_id] = rows

    def descendants(self, node_id: str) -> int:
        if node_id in self._descendant_cache:
            return self._descendant_cache[node_id]
        total = 0
        for child_id in self.children.get(node_id, []):
            total += 1 + self.descendants(child_id)
        self._descendant_cache[node_id] = total
        return total

    def cross_edges(self) -> List[TopologyEdge]:
        if self._cross_edges_cache is None:
            self._cross_edges_cache = [
                edge for edge in self.edges if edge.edge_id not in self.tree_edge_ids
            ]
        return self._cross_edges_cache


class TopologyCardItem(QGraphicsObject):
    activated = Signal(str)
    branchToggleRequested = Signal(str)

    WIDTH = 232.0
    HEIGHT = 98.0
    STACK_MEMBER_H = 34.0
    STACK_HEADER_H = 56.0

    def __init__(
        self,
        node: TopologyNode,
        hidden_descendants: int = 0,
        has_children: bool = False,
        expanded: bool = False,
        parent: Optional[QGraphicsItem] = None,
    ):
        super().__init__(parent)
        self.node = node
        self.hidden_descendants = hidden_descendants
        self.has_children = has_children
        self.expanded = expanded
        self.search_match = False
        self._height = self._calculate_height()
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setAcceptHoverEvents(True)
        self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
        self.setToolTip(self._tooltip())

    def boundingRect(self) -> QRectF:
        return QRectF(0.0, 0.0, self.WIDTH, self._height)

    @property
    def height(self) -> float:
        return self._height

    def _stack_member_count(self) -> int:
        if not bool(self.node.instance.get("logical_stack")):
            return 0
        return max(1, _int(self.node.instance.get("stack_member_count"), 1))

    def _calculate_height(self) -> float:
        members = self._stack_member_count()
        if members <= 1:
            return self.HEIGHT
        visible_members = min(members, 8)
        row_gap = 6.0
        frame_h = visible_members * self.STACK_MEMBER_H + row_gap * max(0, visible_members - 1) + 18.0
        return max(self.HEIGHT, self.STACK_HEADER_H + frame_h + 34.0)

    def _tooltip(self) -> str:
        node = self.node
        rows = [node.name, node.type_label]
        if node.manufacturer or node.model:
            rows.append(" ".join(value for value in (node.manufacturer, node.model) if value))
        if node.location_name:
            rows.append(f"{node.location_name} · Floor {node.floor}")
        if node.port_capacity:
            rows.append(f"Ports {node.ports_used}/{node.port_capacity}")
        elif node.endpoint_count:
            rows.append(f"Ports {node.endpoint_count}")
        if node.poe_budget_w:
            rows.append(f"PoE {node.poe_used_w:.1f}/{node.poe_budget_w:.1f} W")
        if self.hidden_descendants:
            rows.append(f"{self.hidden_descendants} hidden descendants")
        return "\n".join(rows)

    def set_search_match(self, match: bool) -> None:
        if self.search_match != match:
            self.search_match = match
            self.update()

    def paint(self, painter: QPainter, option, widget=None) -> None:  # noqa: ANN001
        painter.setRenderHint(QPainter.Antialiasing, True)
        rect = self.boundingRect()
        selected = self.isSelected()
        hovered = bool(option.state & QStyle.State_MouseOver)

        fill = QColor("#202832")
        if hovered:
            fill = QColor("#27323d")
        if self.node.pseudo:
            fill = QColor("#242a31")
        border = QColor("#3a4652")
        if selected:
            border = QColor("#5ca9ff")
        elif self.search_match:
            border = QColor("#f5bf42")

        painter.setPen(QPen(border, 2.0 if selected or self.search_match else 1.0))
        painter.setBrush(QBrush(fill))
        painter.drawRoundedRect(rect.adjusted(0.5, 0.5, -0.5, -0.5), 12.0, 12.0)

        # Large plans are normally opened at a very small scale. Drawing every
        # label, icon glyph and utilisation statistic at that scale creates a
        # large amount of text rasterisation work even though none of it is
        # readable. Keep only the card silhouette and status colour until the
        # user zooms in.
        lod = QStyleOptionGraphicsItem.levelOfDetailFromTransform(
            painter.worldTransform()
        )
        if lod < 0.12:
            state_colour = {
                "online": QColor("#55c98c"),
                "warning": QColor("#f3b84b"),
                "error": QColor("#ec6b65"),
                "offline": QColor("#7d8791"),
            }[self.node.state]
            painter.setPen(Qt.NoPen)
            painter.setBrush(state_colour)
            painter.drawRect(QRectF(4.0, 4.0, 7.0, max(5.0, rect.height() - 8.0)))
            return

        icon_rect = QRectF(12.0, 14.0, 46.0, 46.0)
        icon_fill = {
            "network_switch": QColor("#265f88"),
            "network_router": QColor("#7259a7"),
            "firewall": QColor("#a04f4f"),
            "optical_line_terminal": QColor("#4a5fa8"),
            "optical_network_terminal": QColor("#2d806d"),
            "fibre_splitter": QColor("#6e5c97"),
            "wireless_access_point": QColor("#9c6a31"),
            "patch_panel": QColor("#53616d"),
            "client_group": QColor("#48515a"),
            "client_device": QColor("#52606b"),
            "site_group": QColor("#3f596f"),
        }.get(self.node.asset_type, QColor("#53616d"))
        painter.setPen(Qt.NoPen)
        painter.setBrush(icon_fill)
        painter.drawRoundedRect(icon_rect, 9.0, 9.0)

        icon_font = QFont("Arial", 8)
        icon_font.setBold(True)
        painter.setFont(icon_font)
        painter.setPen(QColor("#f4f7fa"))
        painter.drawText(icon_rect, Qt.AlignCenter, self.node.icon_text)

        state_colour = {
            "online": QColor("#55c98c"),
            "warning": QColor("#f3b84b"),
            "error": QColor("#ec6b65"),
            "offline": QColor("#7d8791"),
        }[self.node.state]
        painter.setBrush(state_colour)
        painter.setPen(QPen(QColor("#202832"), 2.0))
        painter.drawEllipse(QPointF(54.0, 16.0), 5.5, 5.5)

        title_font = QFont("Arial", 10)
        title_font.setBold(True)
        painter.setFont(title_font)
        painter.setPen(QColor("#f2f5f7"))
        title_metrics = QFontMetrics(title_font)
        title = title_metrics.elidedText(self.node.name, Qt.ElideRight, 145)
        painter.drawText(QRectF(68.0, 13.0, 148.0, 20.0), Qt.AlignLeft | Qt.AlignVCenter, title)

        detail_font = QFont("Arial", 8)
        painter.setFont(detail_font)
        painter.setPen(QColor("#aeb8c2"))
        detail_metrics = QFontMetrics(detail_font)
        detail = self.node.type_label
        if self.node.model:
            detail = f"{detail} · {self.node.model}"
        detail = detail_metrics.elidedText(detail, Qt.ElideRight, 145)
        painter.drawText(QRectF(68.0, 34.0, 148.0, 17.0), Qt.AlignLeft | Qt.AlignVCenter, detail)

        location = self.node.location_name or (f"Floor {self.node.floor}" if not self.node.pseudo else "")
        if location and not self.node.pseudo:
            location = f"{location} · F{self.node.floor}"
        location = detail_metrics.elidedText(location, Qt.ElideRight, 145)
        painter.drawText(QRectF(68.0, 52.0, 148.0, 16.0), Qt.AlignLeft | Qt.AlignVCenter, location)

        stack_members = self._stack_member_count()
        if stack_members > 1:
            self._paint_stack_members(painter, stack_members)
            return

        stat_font = QFont("Arial", 8)
        stat_font.setBold(True)
        painter.setFont(stat_font)
        painter.setPen(QColor("#d7dee5"))
        if self.node.port_capacity:
            left_stat = f"Ports {self.node.ports_used}/{self.node.port_capacity}"
        elif self.node.endpoint_count:
            left_stat = f"Ports {self.node.endpoint_count}"
        else:
            left_stat = f"Links {self.node.connection_count}"
        painter.drawText(QRectF(12.0, 75.0, 104.0, 16.0), Qt.AlignLeft | Qt.AlignVCenter, left_stat)

        if self.node.poe_budget_w:
            right_stat = f"PoE {_human_number(self.node.poe_used_w)}/{_human_number(self.node.poe_budget_w)} W"
        elif self.node.endpoint_locations:
            right_stat = f"{self.node.endpoint_locations} points"
        else:
            right_stat = ""
        painter.drawText(QRectF(112.0, 75.0, 105.0, 16.0), Qt.AlignRight | Qt.AlignVCenter, right_stat)

        if self.has_children:
            badge_rect = QRectF(self.WIDTH - 31.0, self._height - 24.0, 22.0, 16.0)
            painter.setBrush(QColor("#34414d"))
            painter.setPen(Qt.NoPen)
            painter.drawRoundedRect(badge_rect, 7.0, 7.0)
            painter.setPen(QColor("#dbe4eb"))
            badge = "−" if self.expanded else (f"+{self.hidden_descendants}" if self.hidden_descendants < 100 else "+")
            painter.drawText(badge_rect, Qt.AlignCenter, badge)

    def _paint_stack_members(self, painter: QPainter, stack_members: int) -> None:
        visible_members = min(stack_members, 8)
        row_left = 24.0
        row_width = self.WIDTH - 48.0
        row_top = self.STACK_HEADER_H + 8.0
        row_gap = 6.0
        stack_links = max(0, _int(self.node.instance.get("stack_interconnect_count"), max(0, stack_members - 1)))

        frame_h = visible_members * self.STACK_MEMBER_H + row_gap * max(0, visible_members - 1) + 18.0
        stack_frame = QRectF(14.0, self.STACK_HEADER_H, self.WIDTH - 28.0, frame_h)
        painter.setBrush(QColor("#18222c"))
        painter.setPen(QPen(QColor("#526273"), 1.0))
        painter.drawRoundedRect(stack_frame, 8.0, 8.0)

        row_font = QFont("Arial", 8)
        row_font.setBold(True)
        painter.setFont(row_font)
        for index in range(visible_members):
            y = row_top + index * (self.STACK_MEMBER_H + row_gap)
            row_rect = QRectF(row_left, y, row_width, self.STACK_MEMBER_H)
            painter.setBrush(QColor("#263342"))
            painter.setPen(QPen(QColor("#7f8ea0"), 1.0))
            painter.drawRoundedRect(row_rect, 4.0, 4.0)
            painter.setPen(QColor("#edf3f7"))
            painter.drawText(row_rect.adjusted(10.0, 0.0, -10.0, 0.0), Qt.AlignLeft | Qt.AlignVCenter, f"Switch {index + 1}")

            if index < visible_members - 1:
                link_x = row_rect.right() - 14.0
                painter.setPen(QPen(QColor("#b779e3"), 2.0))
                painter.drawLine(QPointF(link_x, row_rect.bottom()), QPointF(link_x, row_rect.bottom() + row_gap))

        if stack_members > visible_members:
            painter.setPen(QColor("#b8c4ce"))
            painter.drawText(
                QRectF(row_left, stack_frame.bottom() - 18.0, row_width, 14.0),
                Qt.AlignCenter,
                f"+{stack_members - visible_members} more",
            )

        footer_rect = QRectF(14.0, self._height - 22.0, self.WIDTH - 28.0, 16.0)
        painter.setBrush(QColor("#3a2450"))
        painter.setPen(Qt.NoPen)
        painter.drawRoundedRect(footer_rect, 6.0, 6.0)
        painter.setFont(QFont("Arial", 7))
        painter.setPen(QColor("#e5c8ff"))
        painter.drawText(footer_rect, Qt.AlignCenter, f"{stack_links} stack links - 2 fibre uplinks")

    def mousePressEvent(self, event) -> None:  # noqa: ANN001
        if event.button() == Qt.LeftButton:
            super().mousePressEvent(event)
            self.activated.emit(self.node.node_id)
            event.accept()
            return

        super().mousePressEvent(event)

    def mouseDoubleClickEvent(self, event) -> None:  # noqa: ANN001
        self.branchToggleRequested.emit(self.node.node_id)
        event.accept()


class RackEquipmentItem(QGraphicsObject):
    activated = Signal(str)
    branchToggleRequested = Signal(str)
    portActivated = Signal(str)

    def __init__(
        self,
        node: TopologyNode,
        width: float,
        height: float,
        units: int,
        port_nodes: Sequence[TopologyNode] = (),
    ):
        super().__init__()
        self.node = node
        self._width = float(width)
        self._height = max(8.0, float(height))
        self.units = max(1, int(units))
        self.port_nodes = list(port_nodes)
        self._port_rects: Dict[str, QRectF] = {}
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setAcceptHoverEvents(True)
        self._build_port_rects()

    def boundingRect(self) -> QRectF:
        return QRectF(0.0, 0.0, self._width, self._height)

    @staticmethod
    def _port_kind(port_name: str) -> str:
        value = _text(port_name).lower()
        if 'pon' in value:
            return 'pon'
        if 'lc' in value:
            return 'lc'
        if 'sc' in value:
            return 'sc'
        if 'mpo' in value or 'mtp' in value:
            return 'mpo'
        if 'qsfp' in value:
            return 'qsfp'
        if any(token in value for token in ('sfp', 'uplink', 'fibre', 'fiber', 'optical')):
            return 'sfp'
        return 'rj45'

    def _build_port_rects(self) -> None:
        self._port_rects.clear()
        if not self.port_nodes or self._width < 42.0 or self._height < 14.0:
            return
        # The rack face uses a 482.6 mm-wide 19-inch panel. Shared OLT modules
        # retain that same scale after the face is divided into equal sections.
        scale = self._width / 482.6
        # Keep physical proportions but enforce a small clickable minimum when
        # an OLT module occupies only a fraction of the rack width.
        sizes = {
            'rj45': (max(5.0, 15.9 * scale), max(4.5, 13.5 * scale)),
            'sfp': (max(5.0, 14.0 * scale), max(3.5, 9.0 * scale)),
            'pon': (max(5.0, 13.0 * scale), max(3.5, 8.5 * scale)),
            'lc': (max(5.0, 12.0 * scale), max(3.5, 8.0 * scale)),
            'sc': (max(5.0, 13.0 * scale), max(4.0, 10.0 * scale)),
            'mpo': (max(6.0, 16.0 * scale), max(3.5, 7.0 * scale)),
            'qsfp': (max(6.0, 18.0 * scale), max(4.0, 10.0 * scale)),
        }
        ear_w = min(15.0, self._width * 0.045)
        left = ear_w + max(4.0, 5.0 * scale)
        right = self._width - ear_w - max(4.0, 5.0 * scale)
        top = max(10.0, self._height * 0.25)
        bottom = self._height - max(3.0, self._height * 0.10)
        usable_w = max(8.0, right - left)
        usable_h = max(5.0, bottom - top)
        max_w = max(value[0] for value in sizes.values())
        max_h = max(value[1] for value in sizes.values())
        gap_x = max(1.5, 2.5 * scale)
        gap_y = max(1.5, 2.0 * scale)
        per_row = max(1, int((usable_w + gap_x) // (max_w + gap_x)))
        rows = max(1, int(math.ceil(len(self.port_nodes) / per_row)))
        # If the declared ports cannot fit at physical scale, compress uniformly
        # while retaining the RJ45/SFP/PON aspect ratios and hit areas.
        required_h = rows * max_h + max(0, rows - 1) * gap_y
        if required_h > usable_h:
            factor = max(0.35, usable_h / required_h)
            sizes = {kind: (w * factor, h * factor) for kind, (w, h) in sizes.items()}
            max_w = max(value[0] for value in sizes.values())
            max_h = max(value[1] for value in sizes.values())
            gap_x *= factor
            gap_y *= factor
            per_row = max(1, int((usable_w + gap_x) // (max_w + gap_x)))
            rows = max(1, int(math.ceil(len(self.port_nodes) / per_row)))
        block_h = rows * max_h + max(0, rows - 1) * gap_y
        y0 = top + max(0.0, (usable_h - block_h) / 2.0)
        for index, port_node in enumerate(self.port_nodes):
            row, col = divmod(index, per_row)
            kind = self._port_kind(port_node.details.get('port_type') or port_node.details.get('port_name', ''))
            pw, ph = sizes[kind]
            x = left + col * (max_w + gap_x) + (max_w - pw) / 2.0
            y = y0 + row * (max_h + gap_y) + (max_h - ph) / 2.0
            if x + pw <= right + 0.5 and y + ph <= bottom + 0.5:
                self._port_rects[port_node.node_id] = QRectF(x, y, pw, ph)

    def paint(self, painter: QPainter, option, widget=None) -> None:
        rect = self.boundingRect()
        painter.setRenderHint(QPainter.Antialiasing, True)
        selected = self.isSelected()
        painter.setPen(QPen(QColor('#8fc7ff') if selected else QColor('#71808b'), 2.0 if selected else 1.2))
        painter.setBrush(QColor('#26323c'))
        painter.drawRoundedRect(rect.adjusted(1.0, 1.0, -1.0, -1.0), 3.0, 3.0)
        ear_w = min(15.0, self._width * 0.045)
        painter.setBrush(QColor('#151c22'))
        painter.drawRect(QRectF(1.0, 2.0, ear_w, max(2.0, rect.height()-4.0)))
        painter.drawRect(QRectF(rect.right()-ear_w-1.0, 2.0, ear_w, max(2.0, rect.height()-4.0)))

        painter.setPen(QColor('#dce5ea'))
        font = QFont('Arial', 7 if rect.height() >= 28 else 5)
        font.setBold(True)
        painter.setFont(font)
        name_rect = QRectF(ear_w + 4.0, 1.0, max(10.0, rect.width() - 2 * ear_w - 8.0), min(10.0, rect.height() * 0.28))
        painter.drawText(name_rect, Qt.AlignCenter, QFontMetrics(font).elidedText(self.node.name, Qt.ElideRight, int(name_rect.width())))

        for port_node in self.port_nodes:
            port_rect = self._port_rects.get(port_node.node_id)
            if port_rect is None:
                continue
            occupied = bool(port_node.details.get('occupied'))
            fill = QColor('#c94c4c') if occupied else QColor('#41a85f')
            kind = self._port_kind(port_node.details.get('port_type') or port_node.details.get('port_name', ''))
            painter.setPen(QPen(fill.darker(175), max(0.6, port_rect.width() * 0.06)))
            painter.setBrush(fill)
            if kind == 'rj45':
                painter.drawRoundedRect(port_rect, 1.0, 1.0)
                inner = port_rect.adjusted(port_rect.width()*0.16, port_rect.height()*0.18, -port_rect.width()*0.16, -port_rect.height()*0.22)
                painter.setBrush(fill.darker(145))
                painter.drawRect(inner)
                # Contact teeth make the symbol recognisable as an RJ45 socket.
                painter.setPen(QPen(fill.lighter(150), max(0.35, port_rect.width()*0.025)))
                for tooth in range(4):
                    tx = inner.left() + (tooth + 0.5) * inner.width() / 4.0
                    painter.drawLine(QPointF(tx, inner.top()), QPointF(tx, inner.top() + inner.height()*0.32))
            elif kind == 'pon':
                painter.drawRoundedRect(port_rect, port_rect.height()*0.28, port_rect.height()*0.28)
                painter.setBrush(fill.darker(150))
                painter.drawEllipse(port_rect.adjusted(port_rect.width()*0.27, port_rect.height()*0.22, -port_rect.width()*0.27, -port_rect.height()*0.22))
            elif kind == 'lc':
                painter.drawRoundedRect(port_rect, 1.0, 1.0)
                painter.setBrush(fill.darker(150))
                half = port_rect.width()/2.0
                painter.drawEllipse(QRectF(port_rect.left()+half*0.15, port_rect.top()+port_rect.height()*0.2, half*0.55, port_rect.height()*0.6))
                painter.drawEllipse(QRectF(port_rect.left()+half*1.15, port_rect.top()+port_rect.height()*0.2, half*0.55, port_rect.height()*0.6))
            elif kind == 'sc':
                painter.drawRect(port_rect)
                painter.setBrush(fill.darker(150)); painter.drawEllipse(port_rect.adjusted(port_rect.width()*0.28, port_rect.height()*0.2, -port_rect.width()*0.28, -port_rect.height()*0.2))
            elif kind in {'mpo', 'qsfp'}:
                painter.drawRect(port_rect)
                painter.setBrush(fill.darker(150)); painter.drawRoundedRect(port_rect.adjusted(port_rect.width()*0.12, port_rect.height()*0.25, -port_rect.width()*0.12, -port_rect.height()*0.25), 0.5, 0.5)
            else:
                painter.drawRoundedRect(port_rect, 0.8, 0.8)
                painter.setBrush(fill.darker(150))
                painter.drawRect(port_rect.adjusted(port_rect.width()*0.14, port_rect.height()*0.20, -port_rect.width()*0.14, -port_rect.height()*0.20))

        painter.setPen(QColor('#91a0aa'))
        painter.setFont(QFont('Arial', 5))
        painter.drawText(QRectF(ear_w+3.0, rect.bottom()-8.0, 28.0, 7.0), Qt.AlignLeft|Qt.AlignBottom, f'{self.units}U')

    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            point = event.pos()
            for node_id, rect in self._port_rects.items():
                if rect.adjusted(-1.5, -1.5, 1.5, 1.5).contains(point):
                    self.portActivated.emit(node_id)
                    event.accept()
                    return
            super().mousePressEvent(event)
            self.activated.emit(self.node.node_id)
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseDoubleClickEvent(self, event) -> None:
        self.branchToggleRequested.emit(self.node.node_id)
        event.accept()


class SwitchFrontPanelItem(QGraphicsObject):
    activated = Signal(str)
    portActivated = Signal(str)

    def __init__(self, node: TopologyNode, port_nodes: Sequence[TopologyNode], width: float, height: float):
        super().__init__()
        self.node = node
        self.port_nodes = list(port_nodes)
        self._width = float(width)
        self._height = float(height)
        self._port_rects: Dict[str, QRectF] = {}
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setAcceptHoverEvents(True)
        self._build_port_rects()

    def boundingRect(self) -> QRectF:
        return QRectF(0.0, 0.0, self._width, self._height)

    def _port_kind(self, port_name: str) -> str:
        value = _text(port_name).lower()
        if 'pon' in value:
            return 'pon'
        if 'lc' in value:
            return 'lc'
        if 'sc' in value:
            return 'sc'
        if 'mpo' in value or 'mtp' in value:
            return 'mpo'
        if 'qsfp' in value:
            return 'qsfp'
        if any(token in value for token in ('sfp', 'uplink', 'fibre', 'fiber', 'optical')):
            return 'sfp'
        return 'rj45'

    def _build_port_rects(self) -> None:
        self._port_rects.clear()
        if not self.port_nodes:
            return
        # 19-inch equipment width = 482.6 mm. Port sizes are proportional to that width.
        mm_scale = self._width / 482.6
        rj_w, rj_h = 15.9 * mm_scale, 13.5 * mm_scale
        sfp_w, sfp_h = 14.0 * mm_scale, 9.0 * mm_scale
        margin_x = 42.0
        margin_y = 18.0
        usable_w = max(20.0, self._width - margin_x * 2.0)
        max_w = max(rj_w, sfp_w)
        gap_x = max(3.0, 3.0 * mm_scale)
        gap_y = max(5.0, 4.0 * mm_scale)
        per_row = max(1, int((usable_w + gap_x) // (max_w + gap_x)))
        rows = max(1, int(math.ceil(len(self.port_nodes) / per_row)))
        row_pitch = max(rj_h, sfp_h) + gap_y
        block_h = rows * row_pitch - gap_y
        start_y = max(margin_y, (self._height - block_h) / 2.0)
        for index, port_node in enumerate(self.port_nodes):
            row = index // per_row
            col = index % per_row
            kind = self._port_kind(port_node.details.get('port_type') or port_node.details.get('port_name', ''))
            pw, ph = (sfp_w, sfp_h) if kind in {'sfp', 'pon'} else (rj_w, rj_h)
            x = margin_x + col * (max_w + gap_x) + (max_w - pw) / 2.0
            y = start_y + row * row_pitch + (max(rj_h, sfp_h) - ph) / 2.0
            self._port_rects[port_node.node_id] = QRectF(x, y, pw, ph)

    def paint(self, painter: QPainter, option, widget=None) -> None:
        rect = self.boundingRect()
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setPen(QPen(QColor('#8fc7ff') if self.isSelected() else QColor('#71808b'), 2.0))
        painter.setBrush(QColor('#222d35'))
        painter.drawRoundedRect(rect.adjusted(1.0, 1.0, -1.0, -1.0), 5.0, 5.0)
        painter.setBrush(QColor('#11181e'))
        painter.drawRect(QRectF(2.0, 4.0, 22.0, rect.height()-8.0))
        painter.drawRect(QRectF(rect.right()-24.0, 4.0, 22.0, rect.height()-8.0))
        painter.setFont(QFont('Arial', 9, QFont.Bold))
        painter.setPen(QColor('#dce5ea'))
        painter.drawText(QRectF(30.0, 4.0, rect.width()-60.0, 16.0), Qt.AlignCenter, self.node.name)
        for port_node in self.port_nodes:
            port_rect = self._port_rects.get(port_node.node_id)
            if port_rect is None:
                continue
            occupied = bool(port_node.details.get('occupied'))
            fill = QColor('#c94c4c') if occupied else QColor('#41a85f')
            kind = self._port_kind(port_node.details.get('port_type') or port_node.details.get('port_name', ''))
            painter.setPen(QPen(fill.darker(170), 1.2 if kind in {'sfp','qsfp','mpo'} else 1.0))
            painter.setBrush(fill)
            if kind == 'rj45':
                painter.drawRoundedRect(port_rect, 1.5, 1.5)
                inner = port_rect.adjusted(port_rect.width()*0.18, port_rect.height()*0.22, -port_rect.width()*0.18, -port_rect.height()*0.18)
                painter.setBrush(fill.darker(145)); painter.drawRect(inner)
            elif kind == 'pon':
                painter.drawRoundedRect(port_rect, port_rect.height()*0.3, port_rect.height()*0.3)
                painter.setBrush(fill.darker(145)); painter.drawEllipse(port_rect.adjusted(port_rect.width()*0.28, port_rect.height()*0.2, -port_rect.width()*0.28, -port_rect.height()*0.2))
            elif kind == 'lc':
                painter.drawRect(port_rect); painter.setBrush(fill.darker(145))
                painter.drawEllipse(QRectF(port_rect.left()+port_rect.width()*0.12, port_rect.top()+port_rect.height()*0.2, port_rect.width()*0.28, port_rect.height()*0.6))
                painter.drawEllipse(QRectF(port_rect.left()+port_rect.width()*0.60, port_rect.top()+port_rect.height()*0.2, port_rect.width()*0.28, port_rect.height()*0.6))
            elif kind == 'sc':
                painter.drawRect(port_rect); painter.setBrush(fill.darker(145)); painter.drawEllipse(port_rect.adjusted(port_rect.width()*0.3, port_rect.height()*0.2, -port_rect.width()*0.3, -port_rect.height()*0.2))
            else:
                painter.drawRect(port_rect); painter.setBrush(fill.darker(145)); painter.drawRect(port_rect.adjusted(port_rect.width()*0.12, port_rect.height()*0.22, -port_rect.width()*0.12, -port_rect.height()*0.22))
            painter.setFont(QFont('Arial', 5))
            painter.setPen(QColor('#ffffff'))
            painter.drawText(port_rect.adjusted(0, -10, 0, 0), Qt.AlignHCenter|Qt.AlignTop, _text(port_node.details.get('port_name')))

    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            point = event.pos()
            for node_id, rect in self._port_rects.items():
                if rect.contains(point):
                    self.portActivated.emit(node_id)
                    event.accept()
                    return
            self.activated.emit(self.node.node_id)
            event.accept()
            return
        super().mousePressEvent(event)


class SplitterFrontPanelItem(QGraphicsObject):
    """Dedicated 1U optical splitter face with a central prism."""
    activated = Signal(str)
    portActivated = Signal(str)

    def __init__(self, node: TopologyNode, port_nodes: Sequence[TopologyNode], width: float, height: float):
        super().__init__()
        self.node = node
        self.port_nodes = list(port_nodes)
        self._width = float(width)
        self._height = max(40.0, float(height))
        self._port_rects: Dict[str, QRectF] = {}
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setAcceptHoverEvents(True)
        self._build_port_rects()

    def boundingRect(self) -> QRectF:
        return QRectF(0.0, 0.0, self._width, self._height)

    def _build_port_rects(self) -> None:
        self._port_rects.clear()
        inputs = [p for p in self.port_nodes if _text(p.details.get("port_use")).lower() == "input"]
        outputs = [p for p in self.port_nodes if _text(p.details.get("port_use")).lower() == "output"]
        others = [p for p in self.port_nodes if p not in inputs and p not in outputs]
        outputs.extend(others)
        port_w = max(7.0, min(13.0, self._width * 0.026))
        port_h = max(6.0, min(11.0, self._height * 0.24))
        left_x = self._width * 0.10
        input_gap = max(5.0, port_h * 0.55)
        input_block_h = len(inputs) * port_h + max(0, len(inputs)-1) * input_gap
        iy = (self._height - input_block_h) / 2.0
        for p in inputs:
            self._port_rects[p.node_id] = QRectF(left_x, iy, port_w, port_h)
            iy += port_h + input_gap
        right_left = self._width * 0.60
        right_width = self._width * 0.32
        gap_x = max(2.0, port_w * 0.30)
        gap_y = max(2.0, port_h * 0.30)
        per_row = max(1, int((right_width + gap_x) // (port_w + gap_x)))
        rows = max(1, math.ceil(len(outputs) / per_row))
        total_h = rows * port_h + max(0, rows-1) * gap_y
        oy = max(16.0, (self._height - total_h) / 2.0)
        for i, p in enumerate(outputs):
            row, col = divmod(i, per_row)
            x = right_left + col * (port_w + gap_x)
            y = oy + row * (port_h + gap_y)
            self._port_rects[p.node_id] = QRectF(x, y, port_w, port_h)

    def paint(self, painter: QPainter, option, widget=None) -> None:
        rect = self.boundingRect()
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setPen(QPen(QColor('#8fc7ff') if self.isSelected() else QColor('#71808b'), 1.4))
        painter.setBrush(QColor('#222d35'))
        painter.drawRoundedRect(rect.adjusted(1,1,-1,-1), 4, 4)
        painter.setFont(QFont('Arial', 7, QFont.Bold))
        painter.setPen(QColor('#dce5ea'))
        painter.drawText(QRectF(8, 2, rect.width()-16, 13), Qt.AlignCenter, self.node.name)

        # Optical prism separating protected feeder inputs from the output bank.
        cx = rect.width() * 0.47
        cy = rect.height() * 0.56
        prism = QPainterPath()
        prism.moveTo(cx - 24, cy - 22)
        prism.lineTo(cx + 25, cy)
        prism.lineTo(cx - 24, cy + 22)
        prism.closeSubpath()
        painter.setPen(QPen(QColor('#9ac7ff'), 1.4))
        painter.setBrush(QColor('#405d79'))
        painter.drawPath(prism)
        painter.setPen(QColor('#bcdcff'))
        painter.setFont(QFont('Arial', 6, QFont.Bold))
        painter.drawText(QRectF(cx-22, cy-7, 40, 14), Qt.AlignCenter, 'OPTICAL')

        for port_node in self.port_nodes:
            pr = self._port_rects.get(port_node.node_id)
            if pr is None:
                continue
            occupied = bool(port_node.details.get('occupied'))
            fill = QColor('#c94c4c') if occupied else QColor('#41a85f')
            painter.setPen(QPen(fill.darker(170), 0.8))
            painter.setBrush(fill)
            painter.drawRoundedRect(pr, 1.2, 1.2)
            painter.setBrush(fill.darker(145))
            painter.drawEllipse(pr.adjusted(pr.width()*0.27, pr.height()*0.20, -pr.width()*0.27, -pr.height()*0.20))
            painter.setPen(QColor('#dce5ea'))
            painter.setFont(QFont('Arial', 5))
            label = _text(port_node.details.get('port_name'))
            painter.drawText(QRectF(pr.left()-16, pr.bottom()+1, pr.width()+32, 9), Qt.AlignHCenter|Qt.AlignTop, label)

    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            for node_id, rect in self._port_rects.items():
                if rect.adjusted(-2,-2,2,2).contains(event.pos()):
                    self.portActivated.emit(node_id)
                    event.accept(); return
            self.activated.emit(self.node.node_id)
            event.accept(); return
        super().mousePressEvent(event)


class LinkLabelItem(QGraphicsObject):
    def __init__(self, text: str, colour: QColor, parent: Optional[QGraphicsItem] = None):
        super().__init__(parent)
        self.text = text
        self.colour = colour
        self.font = QFont("Arial", 7)
        metrics = QFontMetrics(self.font)
        # Cable labels must show the complete medium, length and failover role.
        # The old 160 px cap clipped longer labels such as
        # "Fibre · 1,245 m · Failover".
        self._width = min(420.0, max(54.0, float(metrics.horizontalAdvance(text) + 18)))
        self._height = 20.0
        self.setZValue(0.35)

    def boundingRect(self) -> QRectF:
        return QRectF(-self._width / 2.0, -self._height / 2.0, self._width, self._height)

    def paint(self, painter: QPainter, option, widget=None) -> None:  # noqa: ANN001
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setPen(QPen(QColor("#38434d"), 1.0))
        painter.setBrush(QColor("#182028"))
        painter.drawRoundedRect(self.boundingRect(), 8.0, 8.0)
        painter.setFont(self.font)
        painter.setPen(self.colour.lighter(145))
        painter.drawText(self.boundingRect().adjusted(7.0, 0.0, -7.0, 0.0), Qt.AlignCenter, self.text)


class TopologyGraphicsView(QGraphicsView):
    nodeSelected = Signal(str)
    branchToggleRequested = Signal(str)

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)

        self.setRenderHints(
            QPainter.Antialiasing
            | QPainter.TextAntialiasing
        )

        # Updating the whole viewport on every pan/selection is expensive on
        # large topologies.  Restrict repaints to changed item bounds and cache
        # the static background instead.
        self.setViewportUpdateMode(QGraphicsView.BoundingRectViewportUpdate)
        self.setOptimizationFlag(QGraphicsView.DontSavePainterState, True)
        self.setOptimizationFlag(QGraphicsView.DontAdjustForAntialiasing, True)
        self.setCacheMode(QGraphicsView.CacheBackground)

        self.setTransformationAnchor(QGraphicsView.NoAnchor)
        self.setResizeAnchor(QGraphicsView.AnchorViewCenter)

        # Manual panning is used so device cards can still receive clicks.
        self.setDragMode(QGraphicsView.NoDrag)
        self.setInteractive(True)

        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        self.setBackgroundBrush(QColor("#111820"))
        self.setFrameShape(QFrame.NoFrame)
        self.setStyleSheet("QGraphicsView {" "border: 0;" "background: #111820;" "}")

        self.viewport().setMouseTracking(True)
        self.viewport().setFocusPolicy(Qt.StrongFocus)
        self.setFocusPolicy(Qt.StrongFocus)

        self._panning = False
        self._pan_button = Qt.NoButton
        self._pan_start = QPoint()
        self._navigation_margin = 5000.0
        self._min_zoom = 0.005
        self._max_zoom = 8.0
        self._normal_render_hints = self.renderHints()

    def mousePressEvent(self, event) -> None:  # noqa: ANN001
        position = event.position().toPoint()
        clicked_item = self.itemAt(position)

        # Middle mouse pans from anywhere.
        if event.button() == Qt.MiddleButton:
            self._start_pan(event, Qt.MiddleButton)
            return

        # Left mouse pans only when clicking empty scene space.
        if event.button() == Qt.LeftButton and clicked_item is None:
            self._start_pan(event, Qt.LeftButton)
            return

        # Forward clicks on cards and labels to the scene.
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event) -> None:  # noqa: ANN001
        if not self._panning:
            super().mouseMoveEvent(event)
            return

        current = event.position().toPoint()
        delta = current - self._pan_start
        self._pan_start = current

        self._pan_viewport_by(delta.x(), delta.y())

        event.accept()

    def mouseReleaseEvent(self, event) -> None:  # noqa: ANN001
        if self._panning and event.button() == self._pan_button:
            self._panning = False
            self._pan_button = Qt.NoButton
            self.viewport().unsetCursor()
            self.setRenderHints(self._normal_render_hints)
            self.viewport().update()
            event.accept()
            return

        super().mouseReleaseEvent(event)

    def leaveEvent(self, event) -> None:  # noqa: ANN001
        if self._panning:
            self._panning = False
            self._pan_button = Qt.NoButton
            self.viewport().unsetCursor()
            self.setRenderHints(self._normal_render_hints)
            self.viewport().update()

        super().leaveEvent(event)

    def wheelEvent(self, event) -> None:  # noqa: ANN001
        delta = event.angleDelta().y()
        if delta == 0:
            delta = event.pixelDelta().y()

        if delta == 0:
            event.ignore()
            return

        cursor_position = event.position().toPoint()
        old_scene_position = self.mapToScene(cursor_position)

        zoom_factor = 1.18 if delta > 0 else 1.0 / 1.18

        current_scale = abs(self.transform().m11())
        target_scale = current_scale * zoom_factor

        if current_scale <= 0.0:
            event.accept()
            return
        if delta > 0 and current_scale >= self._max_zoom:
            event.accept()
            return
        if delta < 0 and current_scale <= self._min_zoom:
            event.accept()
            return

        target_scale = min(self._max_zoom, max(self._min_zoom, target_scale))
        zoom_factor = target_scale / current_scale

        self.scale(zoom_factor, zoom_factor)

        # Keep the scene point beneath the mouse stationary.
        new_view_position = self.mapFromScene(old_scene_position)
        viewport_delta = new_view_position - cursor_position
        self._pan_viewport_by(-viewport_delta.x(), -viewport_delta.y())

        event.accept()

    def keyPressEvent(self, event) -> None:  # noqa: ANN001
        # Optional keyboard navigation.
        step = 35

        if event.key() == Qt.Key_Left:
            self._pan_viewport_by(step, 0)
            event.accept()
            return

        if event.key() == Qt.Key_Right:
            self._pan_viewport_by(-step, 0)
            event.accept()
            return

        if event.key() == Qt.Key_Up:
            self._pan_viewport_by(0, step)
            event.accept()
            return

        if event.key() == Qt.Key_Down:
            self._pan_viewport_by(0, -step)
            event.accept()
            return

        super().keyPressEvent(event)

    def _start_pan(self, event, button) -> None:  # noqa: ANN001
        self._panning = True
        self._pan_button = button
        # Text antialiasing across hundreds or thousands of cards is expensive
        # while the viewport is moving. Restore full quality immediately when
        # the drag ends.
        self.setRenderHints(QPainter.RenderHints())
        self._pan_start = event.position().toPoint()
        self.viewport().setCursor(Qt.ClosedHandCursor)
        self.viewport().setFocus()
        event.accept()

    def _pan_viewport_by(self, dx: int, dy: int) -> None:
        current_center = self.mapToScene(self.viewport().rect().center())
        next_center = self.mapToScene(self.viewport().rect().center() - QPoint(int(dx), int(dy)))
        self._center_on_unbounded(current_center + (next_center - current_center))

    def _center_on_unbounded(self, point: QPointF) -> None:
        self._ensure_scene_contains_view(point)
        self.centerOn(point)

    def _ensure_scene_contains_view(self, center: Optional[QPointF] = None) -> None:
        scene = self.scene()
        if scene is None:
            return

        current_rect = scene.sceneRect()
        if current_rect.isNull() or current_rect.isEmpty():
            current_rect = scene.itemsBoundingRect()
        if current_rect.isNull() or current_rect.isEmpty():
            current_rect = QRectF(-self._navigation_margin, -self._navigation_margin, self._navigation_margin * 2.0, self._navigation_margin * 2.0)

        visible_rect = self.mapToScene(self.viewport().rect()).boundingRect()
        required = visible_rect
        if center is not None:
            required = required.united(QRectF(center.x() - 1.0, center.y() - 1.0, 2.0, 2.0))

        margin = max(self._navigation_margin, visible_rect.width(), visible_rect.height())
        required = required.adjusted(-margin, -margin, margin, margin)
        if not current_rect.contains(required):
            scene.setSceneRect(current_rect.united(required))

    def focus_on_item(self, item: QGraphicsItem, target_scale: float = 1.35) -> None:
        """Centre the viewport on a topology item at a readable search zoom."""
        if item is None or self.scene() is None:
            return

        item_rect = item.sceneBoundingRect().adjusted(-70.0, -55.0, 70.0, 55.0)
        if item_rect.isEmpty():
            return

        self.resetTransform()
        self._ensure_scene_contains_view(item_rect.center())
        self.fitInView(item_rect, Qt.KeepAspectRatio)

        current_scale = abs(self.transform().m11())
        desired_scale = min(self._max_zoom, max(self._min_zoom, float(target_scale)))
        if current_scale > 0.0 and current_scale < desired_scale:
            factor = desired_scale / current_scale
            self.scale(factor, factor)

        self._center_on_unbounded(item_rect.center())
        self.viewport().update()

    def fit_topology(self) -> None:
        scene = self.scene()

        if scene is None:
            return

        rect = scene.itemsBoundingRect().adjusted(
            -60.0,
            -60.0,
            60.0,
            60.0,
        )

        if rect.isEmpty():
            return

        self.resetTransform()
        self._ensure_scene_contains_view(rect.center())
        self.fitInView(rect, Qt.KeepAspectRatio)

        # Avoid enlarging a small topology beyond its natural scale.
        if abs(self.transform().m11()) > 1.0:
            self.resetTransform()
            self._center_on_unbounded(rect.center())
        else:
            self._ensure_scene_contains_view(rect.center())


class NetworkTopologyDialog(QDialog):
    """Read-only topology hierarchy inspired by the UniFi Network topology view."""

    CARD_W = TopologyCardItem.WIDTH
    CARD_H = TopologyCardItem.HEIGHT

    # Horizontal distance between sibling branches.
    X_GAP = 90

    # Vertical distance between hierarchy levels.
    Y_GAP = 260

    # Distance between independent root trees.
    ROOT_GAP = 180

    # Maximum preferred width of a grouped floor band before wrapping it into
    # additional vertical rows.
    FLOOR_ROW_MAX_WIDTH = 2600.0

    def __init__(self, parent: Optional[QWidget], data: dict):
        super().__init__(parent)
        self.data = data
        ensure_network_schema(self.data)
        self.model = TopologyModel(self.data)
        self.explicit_expanded: Set[str] = set()
        self.explicit_collapsed: Set[str] = set()
        self.node_items: Dict[str, TopologyCardItem] = {}
        self.visible_nodes: Dict[str, TopologyNode] = {}
        self.visible_parent: Dict[str, str] = {}
        self.visible_parent_edge: Dict[str, str] = {}
        self._search_matches: List[str] = []
        self._search_index = -1
        self._fit_after_show = False
        self.rack_focus: Optional[Tuple[int, str, str]] = None
        self.switch_port_focus: Optional[str] = None
        self._rack_client_nodes_by_id: Dict[str, TopologyNode] = {}
        self._rack_port_nodes_by_id: Dict[str, TopologyNode] = {}
        self._switch_port_nodes_by_id: Dict[str, TopologyNode] = {}
        self._visible_synthetic_edges: Dict[str, TopologyEdge] = {}
        self._floor_match_cache: Dict[Tuple[str, int], bool] = {}
        self._logical_children_cache: Dict[str, Tuple[str, ...]] = {}
        self._collapsed_edge_cache: Dict[Tuple[str, str], TopologyEdge] = {}

        self.setWindowTitle("Network Topology")
        self.setWindowFlag(Qt.Window, True)
        self.setWindowFlag(Qt.WindowMinMaxButtonsHint, True)
        self.setWindowModality(Qt.NonModal)
        self.setMinimumSize(1180, 720)
        self.resize(1540, 900)
        self.setModal(False)
        self.setStyleSheet(
            "QDialog { background: #111820; color: #e9eef2; }"
            "QLabel { color: #dbe3e9; }"
            "QLineEdit, QComboBox { background: #202832; color: #eef3f6; border: 1px solid #3a4652; "
            "border-radius: 6px; padding: 6px; }"
            "QPushButton, QToolButton { background: #25303b; color: #e7edf1; border: 1px solid #3b4854; "
            "border-radius: 6px; padding: 6px 10px; }"
            "QPushButton:hover, QToolButton:hover { background: #303d49; }"
            "QCheckBox { color: #dbe3e9; spacing: 6px; }"
        )

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        # self.view must exist before _build_header() is called.
        self.scene = QGraphicsScene(self)
        self.view = TopologyGraphicsView(self)
        self.view.setScene(self.scene)

        root_layout.addWidget(self._build_header())

        splitter = QSplitter(Qt.Horizontal)
        splitter.setChildrenCollapsible(False)

        splitter.addWidget(self.view)
        splitter.addWidget(self._build_details_panel())
        splitter.setStretchFactor(0, 1)
        splitter.setSizes([1180, 330])
        root_layout.addWidget(splitter, 1)

        self.status_label = QLabel()
        self.status_label.setStyleSheet(
            "padding: 7px 12px; background: #182028; color: #9eabb5;"
        )
        root_layout.addWidget(self.status_label)

        self.scene.selectionChanged.connect(self._scene_selection_changed)
        self.rebuild_scene(fit=True)

    def showEvent(self, event) -> None:  # noqa: ANN001
        super().showEvent(event)
        if self._fit_after_show:
            self._fit_after_show = False
            self._schedule_fit_topology()

    def _build_header(self) -> QWidget:
        header = QWidget()
        header.setStyleSheet("background: #182028; border-bottom: 1px solid #2d3944;")
        layout = QHBoxLayout(header)
        layout.setContentsMargins(14, 10, 14, 10)
        layout.setSpacing(8)

        title = QLabel("Network topology")
        title_font = QFont("Arial", 13)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setStyleSheet("color: #f3f6f8; margin-right: 10px;")
        layout.addWidget(title)

        self.breadcrumb_button = QPushButton("Topology")
        self.breadcrumb_button.setToolTip("Return to the full topology")
        self.breadcrumb_button.clicked.connect(self._exit_rack_view)
        self.breadcrumb_button.hide()
        layout.addWidget(self.breadcrumb_button)

        technology = _text(self.data.get("network_settings", {}).get("technology")) or "Traditional"
        summary = self.data.get("network_design_summary", {}) or {}
        required_ports = _int(summary.get("required_ports"), sum(node.endpoint_count for node in self.model.nodes.values()))
        for text in (
            technology,
            f"{sum(1 for node in self.model.nodes.values() if not node.pseudo)} devices",
            f"{len(self.model.edges)} links",
            f"{_human_number(required_ports)} ports",
        ):
            chip = QLabel(text)
            chip.setStyleSheet("background: #26323d; color: #bfcbd4; border-radius: 8px; padding: 4px 8px;")
            layout.addWidget(chip)

        layout.addStretch(1)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search device, model or location")
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.setMinimumWidth(260)
        self.search_edit.textChanged.connect(self._apply_search_highlight)
        self.search_edit.returnPressed.connect(self._select_next_search_match)
        layout.addWidget(self.search_edit)

        self.floor_combo = QComboBox()
        self.floor_combo.addItem("All floors", None)
        floors = sorted({node.floor for node in self.model.nodes.values()})
        for floor in floors:
            self.floor_combo.addItem(f"Floor {floor}", floor)
        self.floor_combo.currentIndexChanged.connect(lambda _index: self.rebuild_scene(fit=False))
        layout.addWidget(self.floor_combo)

        self.show_clients_check = QCheckBox("Endpoint groups")
        self.show_clients_check.setToolTip("Show department endpoint groups beneath their serving switch or ONT")
        self.show_clients_check.setChecked(True)
        self.show_clients_check.toggled.connect(lambda _checked: self.rebuild_scene(fit=False))
        layout.addWidget(self.show_clients_check)

        self.show_redundant_check = QCheckBox("Failover links")
        self.show_redundant_check.setChecked(True)
        self.show_redundant_check.toggled.connect(lambda _checked: self.rebuild_scene(fit=False))
        layout.addWidget(self.show_redundant_check)

        self.show_link_labels_check = QCheckBox("Link labels")
        self.show_link_labels_check.setChecked(True)
        self.show_link_labels_check.toggled.connect(lambda _checked: self.rebuild_scene(fit=False))
        layout.addWidget(self.show_link_labels_check)

        overview_button = QPushButton("Overview")
        overview_button.setToolTip("Collapse dense branches to the default overview")
        overview_button.clicked.connect(self._overview)
        layout.addWidget(overview_button)

        fit_button = QPushButton("Fit")
        fit_button.clicked.connect(self.view.fit_topology)
        layout.addWidget(fit_button)

        refresh_button = QPushButton("Refresh")
        refresh_button.clicked.connect(self.refresh_from_data)
        layout.addWidget(refresh_button)
        return header

    def _build_details_panel(self) -> QWidget:
        panel = QWidget()
        panel.setMinimumWidth(310)
        panel.setStyleSheet("background: #182028; border-left: 1px solid #2d3944;")
        panel_layout = QVBoxLayout(panel)
        panel_layout.setContentsMargins(14, 14, 14, 14)
        panel_layout.setSpacing(10)

        heading = QLabel("Device details")
        heading_font = QFont("Arial", 12)
        heading_font.setBold(True)
        heading.setFont(heading_font)
        panel_layout.addWidget(heading)

        self.details_scroll = QScrollArea()
        self.details_scroll.setWidgetResizable(True)
        self.details_scroll.setFrameShape(QFrame.NoFrame)
        self.details_scroll.setStyleSheet("QScrollArea { background: transparent; } QWidget { background: transparent; }")
        self.details_container = QWidget()
        self.details_layout = QVBoxLayout(self.details_container)
        self.details_layout.setContentsMargins(0, 0, 0, 0)
        self.details_layout.setSpacing(7)
        self.details_scroll.setWidget(self.details_container)
        panel_layout.addWidget(self.details_scroll, 1)

        self.rack_view_button = QPushButton("Open rack view")
        self.rack_view_button.setEnabled(False)
        self.rack_view_button.clicked.connect(self._open_selected_rack_view)
        panel_layout.addWidget(self.rack_view_button)

        self.port_view_button = QPushButton("Open device port view")
        self.port_view_button.setToolTip("Show physical ports, direct connections and traced paths through passive equipment")
        self.port_view_button.setEnabled(False)
        self.port_view_button.clicked.connect(self._open_selected_switch_port_view)
        panel_layout.addWidget(self.port_view_button)

        self.branch_button = QPushButton("Expand branch")
        self.branch_button.setEnabled(False)
        self.rack_view_button.setEnabled(False)
        self.port_view_button.setEnabled(False)
        self.branch_button.clicked.connect(self._toggle_selected_branch)
        panel_layout.addWidget(self.branch_button)

        self._show_empty_details()
        return panel

    def _clear_details(self) -> None:
        while self.details_layout.count():
            item = self.details_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

    def _show_empty_details(self) -> None:
        self._clear_details()
        message = QLabel("Select a device to inspect its model, location, ports, PoE load and connections.\n\nDouble-click a card to expand or collapse that branch.")
        message.setWordWrap(True)
        message.setStyleSheet("color: #94a2ad; line-height: 1.4;")
        self.details_layout.addWidget(message)
        self.details_layout.addStretch(1)
        self.branch_button.setEnabled(False)
        self.rack_view_button.setEnabled(False)
        self.port_view_button.setEnabled(False)

    def _detail_row(self, label: str, value: str, emphasise: bool = False) -> None:
        row = QWidget()
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 3, 0, 3)
        key = QLabel(label)
        key.setStyleSheet("color: #8f9daa;")
        key.setMinimumWidth(98)
        val = QLabel(value or "—")
        val.setWordWrap(True)
        val.setTextInteractionFlags(Qt.TextSelectableByMouse)
        if emphasise:
            val.setStyleSheet("color: #f0f4f7; font-weight: 600;")
        else:
            val.setStyleSheet("color: #d6dee5;")
        layout.addWidget(key, 0)
        layout.addWidget(val, 1)
        self.details_layout.addWidget(row)

    def _show_node_details(self, node_id: str) -> None:
        node = self.visible_nodes.get(node_id)
        if node is None:
            self._show_empty_details()
            return
        self._clear_details()

        name = QLabel(node.name)
        name_font = QFont("Arial", 13)
        name_font.setBold(True)
        name.setFont(name_font)
        name.setWordWrap(True)
        name.setStyleSheet("color: #f1f5f7;")
        self.details_layout.addWidget(name)

        state = QLabel(node.state.title())
        state_colour = {"online": "#55c98c", "warning": "#f3b84b", "error": "#ec6b65", "offline": "#8a949d"}[node.state]
        state.setStyleSheet(f"color: {state_colour}; font-weight: 600; padding-bottom: 5px;")
        self.details_layout.addWidget(state)

        self._detail_row("Type", node.type_label, True)
        if node.manufacturer or node.model:
            self._detail_row("Product", " ".join(value for value in (node.manufacturer, node.model) if value))
        if not node.pseudo:
            self._detail_row("Instance ID", node.node_id)
            self._detail_row("Asset ID", _text(node.instance.get("asset_id")))
            if bool(node.instance.get("logical_stack")):
                members = max(1, _int(node.instance.get("stack_member_count"), 1))
                max_members = max(1, _int(node.instance.get("stack_max_members"), members))
                self._detail_row("Logical stack", f"{members} members / {max_members} maximum")
                self._detail_row("Fibre uplinks", str(node.connection_count))
                interconnects = max(0, _int(node.instance.get("stack_interconnect_count"), max(0, members - 1)))
                interconnect_medium = _text(node.instance.get("stack_interconnect_medium")) or "stacking"
                interconnect_spec = _text(node.instance.get("stack_interconnect_specification")) or "Switch stack interconnect"
                self._detail_row("Stack links", f"{interconnects} {interconnect_medium} links")
                self._detail_row("Stack link type", interconnect_spec)
            self._detail_row("Location", node.location_name)
            self._detail_row("Floor", str(node.floor))
            self._detail_row("Management IP", node.management_ip)
            self._detail_row("Rack", _text(node.instance.get("rack_name")))
            self._detail_row("Rack position", str(_int(node.instance.get("rack_start_u"))) if _int(node.instance.get("rack_start_u")) else "")
            key = self._switch_group_key(node.node_id)
            if key != (0, "", ""):
                self._detail_row("Rack use", f"{self._rack_used_for_key(key)} / {self._rack_capacity_for_key(key)}U")
                self._detail_row("Device rack use", f"{self._node_rack_units(node.node_id)}U")
            self._detail_row("Power feed", _text(node.instance.get("power_feed")))
            self._detail_row("UPS source", _text(node.instance.get("ups_source")))
        else:
            if node.role == "switch_port":
                self._detail_row("Port", _text(node.details.get("port_name")), True)
                self._detail_row("Port type", _text(node.details.get("port_type")).upper())
                self._detail_row("Port use", _text(node.details.get("port_use")).replace("_", " ").title())
                self._detail_row("Status", "Occupied" if node.details.get("occupied") else "Available")
                connected_devices = node.details.get("connected_devices", []) or []
                self._detail_row("Directly connected", "\n".join(str(value) for value in connected_devices) if connected_devices else "—")
                if not any(
                    key in node.details
                    for key in ("next_active_devices", "next_passive_patch_locations", "connection_paths")
                ):
                    parent_instance_id = _text(node.details.get("parent_instance_id"))
                    port_name = _text(node.details.get("port_name"))
                    if parent_instance_id and port_name:
                        node.details.update(self._port_trace_details(parent_instance_id, port_name))
                next_active = node.details.get("next_active_devices", []) or []
                if next_active:
                    self._detail_row("Next active device", "\n".join(str(value) for value in next_active))
                passive_locations = node.details.get("next_passive_patch_locations", []) or []
                if passive_locations:
                    self._detail_row("Next passive patch", "\n".join(str(value) for value in passive_locations))
                paths = node.details.get("connection_paths", []) or []
                if paths:
                    self._detail_row("Connection path", "\n\n".join(str(value) for value in paths))
                if node.poe_used_w:
                    self._detail_row("PoE load", f"{node.poe_used_w:.1f} W")
            elif node.asset_type == "client_device":
                assignment = node.details.get("assignment", {})
                self._detail_row("Endpoint", _text(assignment.get("endpoint_name")), True)
                self._detail_row("Endpoint port", str(_int(assignment.get("endpoint_port"), 1)))
                self._detail_row("Network port", _text(assignment.get("network_port")))
                self._detail_row("Endpoint asset", _text(assignment.get("endpoint_asset_name")))
                self._detail_row("Copper length", f"{_float(assignment.get('copper_length_m')):.1f} m")
            else:
                self._detail_row("Department", _text(node.details.get("department_id")))

        if node.port_capacity:
            self._detail_row("Ports", f"{node.ports_used} used / {node.port_capacity} available")
            self._detail_row("Port utilisation", f"{node.utilisation * 100:.1f}%")
        elif node.endpoint_count:
            self._detail_row("Assigned ports", str(node.endpoint_count))
        if node.poe_budget_w:
            self._detail_row("PoE", f"{node.poe_used_w:.1f} W / {node.poe_budget_w:.1f} W")
            self._detail_row("PoE utilisation", f"{node.poe_utilisation * 100:.1f}%")
        elif node.poe_used_w:
            self._detail_row("PoE demand", f"{node.poe_used_w:.1f} W")
        self._detail_row("Connections", str(node.connection_count))
        if node.endpoint_count:
            self._detail_row("Endpoint ports", str(node.endpoint_count))
            self._detail_row("Data-point locations", str(node.endpoint_locations))

        connected = []
        for neighbour_id, edge in self.model.adjacency.get(node.node_id, []):
            neighbour = self.model.nodes.get(neighbour_id)
            if neighbour is None:
                continue
            suffix = " (failover)" if edge.standby or edge.redundancy_role.lower() in {"secondary", "standby"} else ""
            connected.append(f"{neighbour.name} — {edge.medium.title()}{suffix}")
        if connected:
            separator = QFrame()
            separator.setFrameShape(QFrame.HLine)
            separator.setStyleSheet("color: #2d3944;")
            self.details_layout.addWidget(separator)
            links_title = QLabel("Connected devices")
            links_title.setStyleSheet("font-weight: 600; color: #e3e9ed;")
            self.details_layout.addWidget(links_title)
            for text in connected[:30]:
                item = QLabel(f"• {text}")
                item.setWordWrap(True)
                item.setStyleSheet("color: #aeb9c2;")
                self.details_layout.addWidget(item)
            if len(connected) > 30:
                item = QLabel(f"… and {len(connected) - 30} more")
                item.setStyleSheet("color: #84929d;")
                self.details_layout.addWidget(item)

        self.details_layout.addStretch(1)
        has_children = bool(self._children_for(node.node_id))
        self.branch_button.setEnabled(has_children and self.rack_focus is None and self.switch_port_focus is None)
        self.branch_button.setText("Collapse branch" if self._is_expanded(node.node_id) else "Expand branch")
        self.branch_button.setProperty("node_id", node.node_id)
        rack_key = self._rack_group_key(node.node_id)
        self.rack_view_button.setEnabled(not node.pseudo and rack_key != (0, "", ""))
        self.rack_view_button.setProperty("node_id", node.node_id)
        self.port_view_button.setEnabled(not node.pseudo and self._supports_port_view(node))
        self.port_view_button.setProperty("node_id", node.node_id)

    def _selected_floor(self) -> Optional[int]:
        if self.rack_focus is not None or self.switch_port_focus is not None:
            return None
        return self.floor_combo.currentData()

    def _node_matches_floor(self, node_id: str, floor: Optional[int]) -> bool:
        if floor is None:
            return True
        cache_key = (node_id, int(floor))
        cached = self._floor_match_cache.get(cache_key)
        if cached is not None:
            return cached
        node = self.model.nodes.get(node_id)
        if node is None or node.floor == floor:
            self._floor_match_cache[cache_key] = True
            return True
        # Keep ancestors visible when any descendant is on the selected floor.
        result = any(
            self._node_matches_floor(child_id, floor)
            for child_id in self.model.children.get(node_id, [])
        )
        self._floor_match_cache[cache_key] = result
        return result

    def _patch_panels_visible(self) -> bool:
        # Patch panels are physical rack components, not logical topology nodes.
        # They remain available to rack elevations, reports and port accounting.
        return False

    def _is_patch_panel(self, node_id: str) -> bool:
        node = self.model.nodes.get(node_id)
        return bool(node is not None and node.asset_type == "patch_panel")

    def _is_cable_management(self, node_id: str) -> bool:
        node = self.model.nodes.get(node_id)
        if node is None:
            return False
        asset_type = _text(node.asset_type).lower()
        role = _text(node.role).lower()
        name = _text(node.name).lower()
        return (
            asset_type in {"cable_management", "cable_manager"}
            or role in {"cable_management", "cable_manager"}
            or "cable management" in name
            or "cable-management" in name
        )

    def _is_active_topology_device(self, node_id: str) -> bool:
        """Return True only for devices that belong in the logical network hierarchy.

        Rack support, passive optical/copper components and power equipment stay
        available to rack and switch-port views, but are deliberately excluded
        from the overview topology because they add large numbers of cards and
        links without representing another logical network tier.
        """
        node = self.model.nodes.get(node_id)
        if node is None:
            return False
        if node.pseudo or node.asset_type in {"site_group", "client_group", "client_device"}:
            return True

        asset_type = _text(node.asset_type).lower()
        # Fibre splitters are passive, but they are an essential logical PoLAN
        # stage between OLTs and ONTs and must remain visible in the topology.
        if asset_type == "fibre_splitter":
            return True
        role = _text(node.role).lower()
        name = _text(node.name).lower()

        passive_or_power_tokens = (
            "patch", "coupler", "adapter", "splice",
            "cable management", "cable-management", "cable manager",
            "ups", "pdu", "power distribution", "power supply",
            "battery", "rectifier", "shelf", "blanking panel",
        )
        if asset_type in {
            "patch_panel", "cable_management",
            "cable_manager", "ups", "pdu", "power_device",
        }:
            return False
        if any(token in name or token in role for token in passive_or_power_tokens):
            return False

        if asset_type in {
            "network_router", "firewall", "network_switch",
            "wireless_access_point", "optical_line_terminal",
            "optical_network_terminal",
        }:
            return True

        logical_role_tokens = (
            "core", "distribution", "aggregation", "access",
            "gateway", "router", "firewall", "olt", "ont",
            "wireless", "client",
        )
        return any(token in role for token in logical_role_tokens)

    def _is_hidden_logical_component(self, node_id: str) -> bool:
        return not self._is_active_topology_device(node_id)

    def _children_for(self, node_id: str) -> List[str]:
        if self.rack_focus is None and self.switch_port_focus is None:
            cached = self._logical_children_cache.get(node_id)
            if cached is None:
                flattened: List[str] = []
                pending = deque(self.model.children.get(node_id, []))
                seen: Set[str] = set()
                while pending:
                    child_id = pending.popleft()
                    if child_id in seen:
                        continue
                    seen.add(child_id)
                    if self._is_hidden_logical_component(child_id):
                        pending.extendleft(
                            reversed(self.model.children.get(child_id, []))
                        )
                        continue
                    flattened.append(child_id)
                cached = tuple(flattened)
                self._logical_children_cache[node_id] = cached
            children = list(cached)
        else:
            children = list(self.model.children.get(node_id, []))
        if self.show_clients_check.isChecked():
            children.extend(
                node.node_id for node in self.model.client_groups.get(node_id, [])
            )
        return children

    def _visible_edge_between(self, parent_id: str, child_id: str) -> str:
        """Return an edge id, collapsing any hidden patch-panel chain."""
        direct_id = self.model.parent_edge.get(child_id, "")
        direct_parent = self.model.parent.get(child_id, "")
        if direct_parent == parent_id:
            return direct_id
        if self._patch_panels_visible():
            return direct_id

        chain: List[TopologyEdge] = []
        cursor = child_id
        guard = 0
        while cursor and cursor != parent_id and guard <= len(self.model.nodes):
            edge_id = self.model.parent_edge.get(cursor, "")
            edge = self.model.edges_by_id.get(edge_id)
            if edge is not None:
                chain.append(edge)
            cursor = self.model.parent.get(cursor, "")
            guard += 1
        if cursor != parent_id or not chain:
            return direct_id
        if len(chain) == 1:
            return chain[0].edge_id

        cache_key = (parent_id, child_id)
        cached_edge = self._collapsed_edge_cache.get(cache_key)
        if cached_edge is None:
            ordered = list(reversed(chain))
            first = ordered[0]
            last = ordered[-1]
            medium = (
                "fibre"
                if any(edge.medium == "fibre" for edge in ordered)
                else first.medium
            )
            synthetic_id = (
                f"collapsed::{'::'.join(edge.edge_id for edge in ordered)}"
            )
            cached_edge = TopologyEdge(
                edge_id=synthetic_id,
                source_id=parent_id,
                target_id=child_id,
                medium=medium,
                source_port=first.source_port,
                target_port=last.target_port,
                length_m=sum(edge.length_m for edge in ordered),
                standby=any(edge.standby for edge in ordered),
                redundancy_role=next(
                    (edge.redundancy_role for edge in ordered if edge.redundancy_role),
                    "",
                ),
                protection_group=next(
                    (edge.protection_group for edge in ordered if edge.protection_group),
                    "",
                ),
                connection={
                    "collapsed_patch_panel_path": [edge.edge_id for edge in ordered]
                },
            )
            self._collapsed_edge_cache[cache_key] = cached_edge
        self._visible_synthetic_edges[cached_edge.edge_id] = cached_edge
        return cached_edge.edge_id

    def _edge_by_id(self, edge_id: str) -> Optional[TopologyEdge]:
        return self.model.edges_by_id.get(edge_id) or self._visible_synthetic_edges.get(edge_id)

    def _node_for(self, node_id: str) -> Optional[TopologyNode]:
        if node_id in self.model.nodes:
            return self.model.nodes[node_id]
        if node_id.startswith("client::"):
            parent_id = node_id.split("::", 2)[1] if "::" in node_id else ""
            for node in self.model.client_groups.get(parent_id, []):
                if node.node_id == node_id:
                    return node
        if node_id.startswith("endpoint::"):
            return self._rack_client_nodes_by_id.get(node_id)
        if node_id.startswith("rackport::"):
            return self._rack_port_nodes_by_id.get(node_id)
        if node_id.startswith("port::"):
            return self._switch_port_nodes_by_id.get(node_id)
        return None

    def _is_expanded(self, node_id: str) -> bool:
        if node_id in self.explicit_expanded:
            return True
        if node_id in self.explicit_collapsed:
            return False
        children = self._children_for(node_id)
        if not children:
            return False
        node = self._node_for(node_id)
        descendants = self.model.descendants(node_id)
        if node is not None:
            if node.asset_type == "site_group":
                return True
            # Endpoint groups are aggregated and inexpensive. When the option is
            # enabled, automatically expose the serving device's endpoint groups
            # unless the user has explicitly collapsed that branch.
            if (
                self.show_clients_check.isChecked()
                and self.model.client_groups.get(node_id)
            ):
                return True
            # Automatically opening every OLT, core or distribution fan-out is
            # the main cause of very large projects creating thousands of
            # QGraphicsItems during dialog construction. Preserve the overview
            # behaviour for modest branches, but leave dense branches collapsed
            # until the user explicitly opens them.
            if node.asset_type in {"firewall", "network_router"}:
                return len(children) <= 12 and descendants <= 160
            if "core" in node.role or "distribution" in node.role:
                return len(children) <= 16 and descendants <= 240
            if (
                node.asset_type == "optical_line_terminal"
                or node.role.startswith("olt_")
            ):
                return len(children) <= 16 and descendants <= 240
        return len(children) <= 6 and descendants <= 32

    def _hidden_descendants(self, node_id: str) -> int:
        count = self.model.descendants(node_id)
        if self.show_clients_check.isChecked():
            count += sum(len(self.model.client_groups.get(desc_id, [])) for desc_id in self._subtree_ids(node_id))
        return count

    def _subtree_ids(self, node_id: str) -> Iterable[str]:
        yield node_id
        for child_id in self.model.children.get(node_id, []):
            yield from self._subtree_ids(child_id)

    def _collect_visible(self) -> List[str]:
        if self.switch_port_focus is not None:
            return self._collect_switch_port_visible()
        if self.rack_focus is not None:
            return self._collect_rack_visible()
        floor = self._selected_floor()
        visible: List[str] = []
        self.visible_parent.clear()
        self.visible_parent_edge.clear()
        self._visible_synthetic_edges.clear()

        def visit(node_id: str, parent_id: str = "", parent_edge: str = "") -> None:
            node = self._node_for(node_id)
            if node is None:
                return
            if self._is_hidden_logical_component(node_id):
                for child_id in self.model.children.get(node_id, []):
                    visit(child_id, parent_id, parent_edge)
                return
            if not node.pseudo and not self._node_matches_floor(node_id, floor):
                return
            visible.append(node_id)
            if parent_id:
                self.visible_parent[node_id] = parent_id
                if parent_edge:
                    self.visible_parent_edge[node_id] = parent_edge
            if not self._is_expanded(node_id):
                return
            for child_id in self._children_for(node_id):
                if child_id.startswith("client::"):
                    visit(child_id, node_id, "")
                    continue
                edge_id = self._visible_edge_between(node_id, child_id)
                visit(child_id, node_id, edge_id)

        for root_id in self.model.roots:
            visit(root_id)
        return visible

    def _collect_rack_visible(self) -> List[str]:
        visible: List[str] = []
        self.visible_parent.clear()
        self.visible_parent_edge.clear()
        self._rack_client_nodes_by_id.clear()
        self._rack_port_nodes_by_id.clear()
        key = self.rack_focus
        if key is None:
            return visible
        floor, location, _selected_rack = key
        # A rack view represents the equipment room/location, not just one rack.
        # Include every physical rack at the same floor/location so additional
        # racks are visible side by side rather than appearing to overflow the
        # selected rack.
        rack_names_at_location = {
            _text(node.instance.get("rack_name"))
            for node in self.model.nodes.values()
            if not node.pseudo
            and node.floor == floor
            and node.location_name == location
            and _text(node.instance.get("rack_name"))
        }
        rack_nodes = [
            node_id
            for node_id, node in self.model.nodes.items()
            if not node.pseudo
            and _text(node.instance.get("rack_name"))
            and (
                (node.floor == floor and node.location_name == location)
                or _text(node.instance.get("rack_name")) in rack_names_at_location
                or _text(node.instance.get("rack_name")) == _text(_selected_rack)
            )
        ]
        rack_nodes.sort(key=lambda node_id: (
            _text(self._node_for(node_id).instance.get("rack_name")) if self._node_for(node_id) else "",
            max(1, _int(self._node_for(node_id).instance.get("rack_start_u"), 1)) if self._node_for(node_id) else 1,
            self._node_for(node_id).name.lower() if self._node_for(node_id) else node_id,
        ))
        visible.extend(rack_nodes)
        return visible

    def _supports_port_view(self, node: TopologyNode) -> bool:
        return node.asset_type in {
            "network_switch",
            "network_router",
            "firewall",
            "optical_line_terminal",
            "optical_network_terminal",
            "wireless_access_point",
            "patch_panel",
            "fibre_splitter",
        }

    def _passive_patch_description(self, node: TopologyNode) -> str:
        parts = [node.name]
        if node.location_name:
            parts.append(node.location_name)
        rack = _text(node.instance.get("rack_name"))
        start_u = _int(node.instance.get("rack_start_u"))
        if rack:
            rack_text = rack + (f" at {start_u}U" if start_u else "")
            parts.append(rack_text)
        return " — ".join(parts)

    def _port_trace_details(self, device_id: str, port_name: str) -> dict:
        """Trace a physical port through passive equipment and active devices.

        The trace keeps direct patch-panel/splitter connections visible in port
        details while the overview topology continues to omit those components.
        Paths stop at a splitter, an endpoint-serving device, or a leaf device.
        """
        direct_edges: List[Tuple[str, TopologyEdge]] = []
        for neighbour_id, edge in self.model.adjacency.get(device_id, []):
            local_port = edge.source_port if edge.source_id == device_id else edge.target_port
            if _text(local_port) == _text(port_name):
                direct_edges.append((neighbour_id, edge))

        next_active: List[str] = []
        next_passive: List[str] = []
        paths: List[str] = []
        seen_path_labels: Set[str] = set()

        for neighbour_id, first_edge in direct_edges:
            queue = deque([(neighbour_id, device_id, [device_id, neighbour_id], 0, False, False)])
            visited_states: Set[Tuple[str, str]] = set()
            while queue and len(paths) < 40:
                current_id, previous_id, path_ids, depth, active_found, passive_found = queue.popleft()
                state = (current_id, previous_id)
                if state in visited_states or depth > 14:
                    continue
                visited_states.add(state)
                current = self.model.nodes.get(current_id)
                if current is None:
                    continue

                is_patch = current.asset_type == "patch_panel"
                is_splitter = current.asset_type == "fibre_splitter" or "splitter" in current.role
                is_active = self._is_active_topology_device(current_id) and not current.pseudo

                if is_patch and not passive_found:
                    description = self._passive_patch_description(current)
                    if description not in next_passive:
                        next_passive.append(description)
                    passive_found = True

                if is_active and current_id != device_id and not active_found:
                    description = current.name
                    if current.location_name:
                        description += f" — {current.location_name}"
                    if description not in next_active:
                        next_active.append(description)
                    active_found = True

                has_clients = current.endpoint_count > 0
                neighbours = [
                    (candidate_id, edge)
                    for candidate_id, edge in self.model.adjacency.get(current_id, [])
                    if candidate_id != previous_id and candidate_id not in path_ids
                ]
                terminal = is_splitter or has_clients or not neighbours or depth >= 14
                if terminal:
                    labels = []
                    for path_id in path_ids:
                        path_node = self.model.nodes.get(path_id)
                        labels.append(path_node.name if path_node is not None else path_id)
                    reason = "splitter" if is_splitter else ("client-serving device" if has_clients else "end device")
                    label = " → ".join(labels) + f"  [{reason}]"
                    if label not in seen_path_labels:
                        seen_path_labels.add(label)
                        paths.append(label)
                    continue

                for candidate_id, _edge in neighbours:
                    queue.append((
                        candidate_id,
                        current_id,
                        path_ids + [candidate_id],
                        depth + 1,
                        active_found,
                        passive_found,
                    ))

        return {
            "next_active_devices": next_active,
            "next_passive_patch_locations": next_passive,
            "connection_paths": paths,
        }

    def _device_port_records(self, device_id: str) -> Tuple[Dict[str, List[str]], Dict[str, float], Dict[str, dict]]:
        records: Dict[str, List[str]] = defaultdict(list)
        poe_by_port: Dict[str, float] = defaultdict(float)
        traces: Dict[str, dict] = {}
        for assignment in self.data.get("network_endpoint_assignments", []):
            if _text(assignment.get("network_instance_id")) != device_id:
                continue
            port = _text(assignment.get("network_port")) or "Unspecified"
            endpoint = _text(assignment.get("endpoint_name")) or "Endpoint"
            asset_name = _text(assignment.get("endpoint_asset_name"))
            records[port].append(endpoint + (f" — {asset_name}" if asset_name else ""))
            poe_by_port[port] += max(0.0, _float(assignment.get("poe_power_w")))
        for edge in self.model.edges:
            if edge.source_id == device_id:
                port = edge.source_port or "Unspecified"
                peer = self.model.nodes.get(edge.target_id)
                records[port].append(peer.name if peer else edge.target_id)
            elif edge.target_id == device_id:
                port = edge.target_port or "Unspecified"
                peer = self.model.nodes.get(edge.source_id)
                records[port].append(peer.name if peer else edge.source_id)
        device = self.model.nodes.get(device_id)
        defined_ports = _expanded_asset_ports(device.asset) if device is not None else []
        if defined_ports:
            canonical_records: Dict[str, List[str]] = defaultdict(list)
            canonical_poe: Dict[str, float] = defaultdict(float)
            for port_name, values in records.items():
                canonical = _canonical_port_name(device.asset, port_name, defined_ports)
                canonical_records[canonical].extend(values)
                canonical_poe[canonical] += poe_by_port.get(port_name, 0.0)
            records = canonical_records
            poe_by_port = canonical_poe
        # Trace details are intentionally calculated only when a user selects
        # a port. Eagerly tracing every port on every rack device would undo the
        # topology loading-performance improvements on large projects.
        return records, poe_by_port, traces

    def _collect_switch_port_visible(self) -> List[str]:
        visible: List[str] = []
        self.visible_parent.clear()
        self.visible_parent_edge.clear()
        self._switch_port_nodes_by_id.clear()
        switch_id = _text(self.switch_port_focus)
        switch = self.model.nodes.get(switch_id)
        if switch is None:
            return visible
        visible.append(switch_id)

        port_records, poe_by_port, port_traces = self._device_port_records(switch_id)

        capacity = max(0, switch.port_capacity)
        defined_ports = _expanded_asset_ports(switch.asset)
        names = {row["name"] for row in defined_ports} or {str(number) for number in range(1, capacity + 1)}
        names.update(port_records)
        port_definitions_by_name = {_text(row.get("name")): row for row in defined_ports}
        ordered_names = sorted(
            names,
            key=lambda value: _port_group_sort_key(
                port_definitions_by_name.get(value)
                or _port_definition_for_name(switch.asset, value),
                value,
            ),
        )
        for index, port in enumerate(ordered_names, start=1):
            connected = port_records.get(port, [])
            occupied = bool(connected)
            node_id = f"port::{switch_id}::{index}"
            node = TopologyNode(
                node_id=node_id,
                name=f"Port {port}",
                asset_type="client_device",
                role="switch_port",
                floor=switch.floor,
                location_name=switch.location_name,
                port_capacity=1,
                ports_used=1 if occupied else 0,
                poe_used_w=poe_by_port.get(port, 0.0),
                connection_count=len(connected),
                endpoint_count=len(connected),
                pseudo=True,
                details={
                    "parent_instance_id": switch_id,
                    "port_name": port,
                    "port_type": _port_definition_for_name(switch.asset, port).get("port_type", "other"),
                    "port_use": _port_definition_for_name(switch.asset, port).get("port_use", "other"),
                    "connected_devices": connected,
                    "occupied": occupied,
                    **port_traces.get(port, {}),
                },
            )
            self._switch_port_nodes_by_id[node_id] = node
            visible.append(node_id)
            self.visible_parent[node_id] = switch_id
        return visible

    def _rack_port_nodes(self, device_id: str) -> List[TopologyNode]:
        device = self.model.nodes.get(device_id)
        if device is None:
            return []
        records, poe_by_port, port_traces = self._device_port_records(device_id)

        capacity = max(0, device.port_capacity)
        defined_ports = _expanded_asset_ports(device.asset)
        names = {row["name"] for row in defined_ports} or {str(number) for number in range(1, capacity + 1)}
        names.update(records)
        port_definitions_by_name = {_text(row.get("name")): row for row in defined_ports}
        ordered_names = sorted(
            names,
            key=lambda value: _port_group_sort_key(
                port_definitions_by_name.get(value)
                or _port_definition_for_name(device.asset, value),
                value,
            ),
        )
        result: List[TopologyNode] = []
        for index, port in enumerate(ordered_names, start=1):
            connected = records.get(port, [])
            node_id = f"rackport::{device_id}::{index}"
            node = TopologyNode(
                node_id=node_id,
                name=f"Port {port}",
                asset_type="client_device",
                role="switch_port",
                floor=device.floor,
                location_name=device.location_name,
                port_capacity=1,
                ports_used=1 if connected else 0,
                poe_used_w=poe_by_port.get(port, 0.0),
                connection_count=len(connected),
                endpoint_count=len(connected),
                pseudo=True,
                details={
                    "parent_instance_id": device_id,
                    "port_name": port,
                    "port_type": _port_definition_for_name(device.asset, port).get("port_type", "other"),
                    "port_use": _port_definition_for_name(device.asset, port).get("port_use", "other"),
                    "connected_devices": connected,
                    "occupied": bool(connected),
                    **port_traces.get(port, {}),
                },
            )
            self._rack_port_nodes_by_id[node_id] = node
            result.append(node)
        return result

    def _rack_client_nodes(self, parent_id: str) -> List[TopologyNode]:
        parent = self.model.nodes.get(parent_id)
        if parent is None:
            return []
        assignments: List[dict] = []
        for group in self.model.client_groups.get(parent_id, []):
            assignments.extend(group.details.get("assignments", []))
        nodes: List[TopologyNode] = []
        for index, assignment in enumerate(
            sorted(
                assignments,
                key=lambda item: (
                    _text(item.get("endpoint_name")).lower(),
                    _int(item.get("endpoint_port"), 1),
                    _text(item.get("network_port")),
                ),
            ),
            start=1,
        ):
            assignment_id = _text(assignment.get("id")) or f"{parent_id}-{index}"
            endpoint_name = _text(assignment.get("endpoint_name")) or "Endpoint"
            endpoint_port = _int(assignment.get("endpoint_port"), 1)
            asset_name = _text(assignment.get("endpoint_asset_name"))
            network_port = _text(assignment.get("network_port"))
            nodes.append(
                TopologyNode(
                    node_id=f"endpoint::{parent_id}::{assignment_id}",
                    name=f"{endpoint_name}:{endpoint_port}",
                    asset_type="client_device",
                    role="client_device",
                    floor=_int(assignment.get("floor"), parent.floor),
                    location_name=asset_name or f"Port {network_port}",
                    ports_used=1,
                    poe_used_w=max(0.0, _float(assignment.get("poe_power_w"))),
                    endpoint_count=1,
                    endpoint_locations=1,
                    pseudo=True,
                    details={
                        "parent_instance_id": parent_id,
                        "assignment": assignment,
                    },
                )
            )
        return nodes

    def _layout_visible(self, visible_ids: Sequence[str]) -> Dict[str, QPointF]:
        if self.switch_port_focus is not None:
            return self._layout_switch_port_visible(visible_ids)
        if self.rack_focus is not None:
            return self._layout_rack_visible(visible_ids)
        visible_set = set(visible_ids)
        heights: Dict[str, float] = {}

        def card_height(node_id: str) -> float:
            node = self._node_for(node_id)
            if node is None:
                return self.CARD_H
            members = max(1, _int(node.instance.get("stack_member_count"), 1)) if bool(node.instance.get("logical_stack")) else 1
            if members <= 1:
                return self.CARD_H
            visible_members = min(members, 8)
            row_gap = 6.0
            frame_h = visible_members * TopologyCardItem.STACK_MEMBER_H + row_gap * max(0, visible_members - 1) + 18.0
            return max(self.CARD_H, TopologyCardItem.STACK_HEADER_H + frame_h + 34.0)

        def visible_children(node_id: str) -> List[str]:
            return [child_id for child_id in self._children_for(node_id) if child_id in visible_set and self.visible_parent.get(child_id) == node_id]

        def measure_height(node_id: str) -> float:
            children = visible_children(node_id)
            if not children:
                heights[node_id] = card_height(node_id)
                return heights[node_id]
            child_heights = [
                measure_height(child_id)
                for child_id in children
            ]

            branch_gap = 90.0
            if len(children) >= 4:
                branch_gap *= 1.25
            if len(children) >= 8:
                branch_gap *= 1.20

            total = (
                sum(child_heights)
                + branch_gap * max(0, len(children) - 1)
            )
            heights[node_id] = max(card_height(node_id), total)
            return heights[node_id]

        roots = [root_id for root_id in self.model.roots if root_id in visible_set]
        for root_id in roots:
            measure_height(root_id)

        positions: Dict[str, QPointF] = {}

        def place(node_id: str, level: int, top_y: float) -> None:
            height = heights[node_id]
            x = level * (self.CARD_W + self.X_GAP)
            y = top_y + (height - card_height(node_id)) / 2.0
            positions[node_id] = QPointF(x, y)
            children = visible_children(node_id)
            cursor_y = top_y
            branch_gap = 90.0

            if len(children) >= 4:
                branch_gap *= 1.25

            if len(children) >= 8:
                branch_gap *= 1.20

            for child_id in children:
                place(child_id, level + 1, cursor_y)
                cursor_y += heights[child_id] + branch_gap

        cursor_y = 0.0
        for root_id in roots:
            place(root_id, 0, cursor_y)
            cursor_y += heights[root_id] + self.ROOT_GAP
        self._pack_layered_topology(visible_ids, positions)
        self._space_recursive_subtrees(visible_ids, positions)
        self._recenter_group_nodes(visible_ids, positions)
        return positions


    def _space_recursive_subtrees(
        self, visible_ids: Sequence[str], positions: Dict[str, QPointF]
    ) -> None:
        """Allocate a distinct vertical band to every visible child subtree.

        A child's band is the larger of its own rendered card height and the
        combined height required by all of its descendants.  Sibling bands are
        then stacked without overlap and each parent is centred on the complete
        span of its children.  Consequently, opening children-of-children grows
        the ancestor branch and the whole topology instead of squeezing deeper
        columns into an independently centred layer.
        """

        visible_set = {node_id for node_id in visible_ids if node_id in positions}
        if not visible_set:
            return

        child_cache: Dict[str, List[str]] = {}

        def children_for(node_id: str) -> List[str]:
            cached = child_cache.get(node_id)
            if cached is not None:
                return cached
            rows = [
                child_id
                for child_id in self._children_for(node_id)
                if child_id in visible_set
                and self.visible_parent.get(child_id) == node_id
            ]
            rows.sort(
                key=lambda child_id: (
                    positions[child_id].y(),
                    self._node_for(child_id).name.lower()
                    if self._node_for(child_id) is not None
                    else child_id.lower(),
                )
            )
            child_cache[node_id] = rows
            return rows

        def sibling_gap(parent_id: str, child_ids: Sequence[str]) -> float:
            parent_layer = self._topology_layer(parent_id)
            child_layers = [self._topology_layer(child_id) for child_id in child_ids]
            deepest = max(child_layers, default=parent_layer)
            base = 72.0
            if parent_layer >= 2 or deepest >= 3:
                base = 96.0
            if deepest >= 4:
                base = 118.0
            if len(child_ids) >= 6:
                base += 18.0
            if len(child_ids) >= 12:
                base += 24.0
            return base

        band_height: Dict[str, float] = {}
        visiting: set[str] = set()

        def measure(node_id: str) -> float:
            if node_id in band_height:
                return band_height[node_id]
            if node_id in visiting:
                # Defensive guard for malformed cyclic topology data.
                return self._node_card_height(node_id)
            visiting.add(node_id)
            child_ids = children_for(node_id)
            own_height = self._node_card_height(node_id)
            if child_ids:
                gap = sibling_gap(node_id, child_ids)
                descendants_height = (
                    sum(measure(child_id) for child_id in child_ids)
                    + gap * max(0, len(child_ids) - 1)
                )
                value = max(own_height, descendants_height)
            else:
                value = own_height
            visiting.discard(node_id)
            band_height[node_id] = value
            return value

        roots = [
            node_id
            for node_id in self.model.roots
            if node_id in visible_set
            and self.visible_parent.get(node_id) not in visible_set
        ]
        # Include filtered/orphaned branches whose parent is not currently shown.
        for node_id in visible_ids:
            if node_id not in visible_set or node_id in roots:
                continue
            parent_id = self.visible_parent.get(node_id)
            if parent_id not in visible_set:
                roots.append(node_id)

        roots.sort(
            key=lambda node_id: (
                positions[node_id].y(),
                self._node_for(node_id).name.lower()
                if self._node_for(node_id) is not None
                else node_id.lower(),
            )
        )
        for root_id in roots:
            measure(root_id)

        def place(node_id: str, band_top: float) -> None:
            node_band = band_height.get(node_id, self._node_card_height(node_id))
            own_height = self._node_card_height(node_id)
            positions[node_id] = QPointF(
                positions[node_id].x(),
                band_top + (node_band - own_height) / 2.0,
            )
            child_ids = children_for(node_id)
            if not child_ids:
                return
            gap = sibling_gap(node_id, child_ids)
            child_total = (
                sum(band_height[child_id] for child_id in child_ids)
                + gap * max(0, len(child_ids) - 1)
            )
            cursor = band_top + max(0.0, (node_band - child_total) / 2.0)
            for child_id in child_ids:
                place(child_id, cursor)
                cursor += band_height[child_id] + gap

        root_gap = max(140.0, self.ROOT_GAP)
        total_height = (
            sum(band_height[root_id] for root_id in roots)
            + root_gap * max(0, len(roots) - 1)
        )
        cursor = -total_height / 2.0
        for root_id in roots:
            place(root_id, cursor)
            cursor += band_height[root_id] + root_gap

        # Rebuild layer bounds from the actual recursive positions so fitting and
        # layer backgrounds expand with the deepest visible descendant branch.
        if hasattr(self, "_layer_bounds"):
            by_layer: Dict[int, List[str]] = defaultdict(list)
            for node_id in visible_set:
                node = self._node_for(node_id)
                if node is None:
                    continue
                by_layer[self._topology_layer(node_id)].append(node_id)
            for layer, node_ids in by_layer.items():
                left = min(positions[node_id].x() for node_id in node_ids)
                right = max(positions[node_id].x() + self.CARD_W for node_id in node_ids)
                top = min(positions[node_id].y() for node_id in node_ids)
                bottom = max(
                    positions[node_id].y() + self._node_card_height(node_id)
                    for node_id in node_ids
                )
                self._layer_bounds[layer] = QRectF(
                    left, top, max(self.CARD_W, right - left), max(self.CARD_H, bottom - top)
                )


    def _align_upstream_to_downstream(
        self, visible_ids: Sequence[str], positions: Dict[str, QPointF]
    ) -> None:
        """Centre upstream cards on the vertical span of their visible descendants.

        The layered packer gives each layer an independent top-to-bottom order.
        That is compact, but it can bunch OLT/core/distribution rows together even
        when one device feeds a much larger downstream branch.  This pass derives
        each upstream card's preferred Y position from the complete visible
        downstream subtree, then resolves collisions within that layer while
        preserving the downstream order.
        """

        visible_set = set(visible_ids)
        real_ids = [
            node_id
            for node_id in visible_ids
            if node_id in positions
            and self._node_for(node_id) is not None
            and not self._node_for(node_id).pseudo
        ]
        if not real_ids:
            return

        child_cache: Dict[str, List[str]] = {}

        def visible_children(node_id: str) -> List[str]:
            cached = child_cache.get(node_id)
            if cached is not None:
                return cached
            children = [
                child_id
                for child_id in self._children_for(node_id)
                if child_id in visible_set
                and child_id in positions
                and self.visible_parent.get(child_id) == node_id
            ]
            child_cache[node_id] = children
            return children

        span_cache: Dict[str, Tuple[float, float]] = {}

        def subtree_span(node_id: str) -> Tuple[float, float]:
            cached = span_cache.get(node_id)
            if cached is not None:
                return cached
            children = visible_children(node_id)
            if not children:
                top = positions[node_id].y()
                span = (top, top + self._node_card_height(node_id))
                span_cache[node_id] = span
                return span
            child_spans = [subtree_span(child_id) for child_id in children]
            span = (
                min(value[0] for value in child_spans),
                max(value[1] for value in child_spans),
            )
            span_cache[node_id] = span
            return span

        preferred_top: Dict[str, float] = {}
        for node_id in real_ids:
            children = visible_children(node_id)
            if not children:
                preferred_top[node_id] = positions[node_id].y()
                continue
            top, bottom = subtree_span(node_id)
            preferred_top[node_id] = (top + bottom - self._node_card_height(node_id)) / 2.0

        # Work from the deepest upstream layer back towards the root so a
        # distribution row follows its access/ONT branch and an OLT/core row
        # subsequently follows the already-spaced distribution rows.
        by_layer: Dict[int, List[str]] = defaultdict(list)
        for node_id in real_ids:
            layer = self._topology_layer(node_id)
            if layer < 4:
                by_layer[layer].append(node_id)

        min_gap = 54.0
        for layer in sorted(by_layer, reverse=True):
            ordered = sorted(
                by_layer[layer],
                key=lambda node_id: (
                    preferred_top.get(node_id, positions[node_id].y()),
                    positions[node_id].y(),
                    self._node_for(node_id).name.lower(),
                ),
            )
            if not ordered:
                continue

            placed_tops: Dict[str, float] = {}
            cursor_bottom: Optional[float] = None
            for node_id in ordered:
                target_top = preferred_top.get(node_id, positions[node_id].y())
                if cursor_bottom is not None:
                    target_top = max(target_top, cursor_bottom + min_gap)
                placed_tops[node_id] = target_top
                cursor_bottom = target_top + self._node_card_height(node_id)

            # Pull the whole layer back towards its preferred centre when the
            # forward collision pass introduced avoidable downward drift.
            first_id = ordered[0]
            last_id = ordered[-1]
            actual_centre = (
                placed_tops[first_id]
                + placed_tops[last_id]
                + self._node_card_height(last_id)
            ) / 2.0
            preferred_centre = (
                preferred_top.get(first_id, placed_tops[first_id])
                + preferred_top.get(last_id, placed_tops[last_id])
                + self._node_card_height(last_id)
            ) / 2.0
            shift = preferred_centre - actual_centre
            for node_id in ordered:
                positions[node_id] = QPointF(
                    positions[node_id].x(), placed_tops[node_id] + shift
                )

            # Rebuild spans so the next upstream layer uses the corrected rows.
            span_cache.clear()
            for node_id in real_ids:
                children = visible_children(node_id)
                if children:
                    top, bottom = subtree_span(node_id)
                    preferred_top[node_id] = (
                        top + bottom - self._node_card_height(node_id)
                    ) / 2.0
                else:
                    preferred_top[node_id] = positions[node_id].y()

    def _centralise_layers_by_height(
        self, visible_ids: Sequence[str], positions: Dict[str, QPointF]
    ) -> None:
        """Centre every topology layer in one shared vertical drawing span.

        The topology remains left-to-right along the X axis.  Within each X-axis
        layer, cards are ordered by their downstream-derived position and the
        common available height is divided evenly between them.  Real rendered
        card heights are used, including logical-stack cards, so tall elements
        reserve more space and the layer remains visually centred.
        """

        by_layer: Dict[int, List[str]] = defaultdict(list)
        for node_id in visible_ids:
            if node_id not in positions:
                continue
            node = self._node_for(node_id)
            if node is None or node.pseudo:
                continue
            by_layer[self._topology_layer(node_id)].append(node_id)

        if not by_layer:
            return

        layer_gap_min = 54.0
        layer_heights: Dict[int, float] = {}

        def minimum_gap_for_layer(layer: int, node_ids: Sequence[str]) -> float:
            base = {0: 70.0, 1: 82.0, 2: 108.0, 3: 126.0, 4: 108.0, 5: 78.0}.get(layer, layer_gap_min)
            max_children = max(
                (
                    len([
                        child_id
                        for child_id in self._children_for(node_id)
                        if child_id in positions and self.visible_parent.get(child_id) == node_id
                    ])
                    for node_id in node_ids
                ),
                default=0,
            )
            # Expanding a busy branch reserves additional row separation before
            # the cards are centred across the common layer height.
            return base + min(150.0, max_children * 12.0)
        ordered_layers: Dict[int, List[str]] = {}

        for layer, node_ids in by_layer.items():
            ordered = sorted(
                node_ids,
                key=lambda node_id: (
                    positions[node_id].y(),
                    self._node_for(node_id).name.lower(),
                ),
            )
            ordered_layers[layer] = ordered
            total_cards = sum(self._node_card_height(node_id) for node_id in ordered)
            adaptive_gap = minimum_gap_for_layer(layer, ordered)
            layer_heights[layer] = total_cards + adaptive_gap * max(0, len(ordered) - 1)

        shared_height = max(layer_heights.values())
        # Give sparse layers enough breathing room to divide the drawing height
        # evenly rather than collapsing into a tight group at the centre.
        shared_height = max(shared_height, self.CARD_H * 2.0)
        shared_top = -shared_height / 2.0

        for layer, ordered in ordered_layers.items():
            if not ordered:
                continue
            total_cards = sum(self._node_card_height(node_id) for node_id in ordered)
            if len(ordered) > 1:
                even_gap = max(
                    minimum_gap_for_layer(layer, ordered),
                    (shared_height - total_cards) / float(len(ordered) - 1),
                )
            else:
                even_gap = 0.0

            occupied_height = total_cards + even_gap * max(0, len(ordered) - 1)
            cursor_y = shared_top + (shared_height - occupied_height) / 2.0
            for node_id in ordered:
                positions[node_id] = QPointF(positions[node_id].x(), cursor_y)
                cursor_y += self._node_card_height(node_id) + even_gap

            if hasattr(self, "_layer_bounds") and layer in self._layer_bounds:
                rect = self._layer_bounds[layer]
                self._layer_bounds[layer] = QRectF(
                    rect.x(), shared_top, rect.width(), shared_height
                )

    def _layout_rack_visible(self, visible_ids: Sequence[str]) -> Dict[str, QPointF]:
        self._layer_bounds = {}
        self._location_bus_by_node = {}
        self._failover_bus_by_node = {}
        self._main_bus_column_x = None
        self._failover_bus_column_x = None
        positions: Dict[str, QPointF] = {}
        key = self.rack_focus
        if key is None:
            return positions
        floor, location, _selected_rack = key
        rack_names = self._rack_names_for_location(floor, location)
        rack_left_start = 92.0
        rack_width = 518.6
        rack_gap = 90.0
        rack_top = 90.0
        unit_pitch = 44.45
        rack_x_by_name = {
            rack_name: rack_left_start + index * (rack_width + rack_gap)
            for index, rack_name in enumerate(rack_names)
        }
        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None:
                continue
            rack_name = _text(node.instance.get("rack_name"))
            rack_key = (floor, location, rack_name)
            capacity = self._rack_capacity_for_key(rack_key)
            start_u = max(1, _int(node.instance.get("rack_start_u"), 1))
            units = max(1, self._node_rack_units(node_id))
            # Invalid legacy placements are kept visible but constrained to the
            # rack drawing. New auto-planned equipment is prevented from creating
            # these positions by network_auto_planner.py.
            top_u = min(capacity, start_u + units - 1)
            y = rack_top + (capacity - top_u) * unit_pitch
            rack_left = rack_x_by_name.get(rack_name, rack_left_start)
            shared_capacity = max(1, _int(node.instance.get("shared_rack_unit_capacity"), 1))
            shared_position = max(1, _int(node.instance.get("shared_rack_unit_position"), 1))
            if _text(node.instance.get("shared_rack_unit_group")) and shared_capacity > 1:
                face_width = (482.6 - 8.0) / shared_capacity
                x = rack_left + 18.0 + (shared_position - 1) * face_width
            else:
                x = rack_left + 18.0
            positions[node_id] = QPointF(x, y + 2.0)
        return positions

    def _layout_switch_port_visible(self, visible_ids: Sequence[str]) -> Dict[str, QPointF]:
        self._layer_bounds = {}
        self._location_bus_by_node = {}
        self._failover_bus_by_node = {}
        self._main_bus_column_x = None
        self._failover_bus_column_x = None
        positions: Dict[str, QPointF] = {}
        switch_id = _text(self.switch_port_focus)
        if switch_id not in visible_ids:
            return positions
        # The switch front panel owns the clickable port geometry; pseudo port
        # nodes remain available for the details panel but are not drawn as cards.
        positions[switch_id] = QPointF(40.0, 80.0)
        return positions

    def _topology_layer(self, node_id: str) -> int:
        """Return the fixed left-to-right visual column for a node.

        PoLAN needs one more visual stage than a traditional LAN.  Splitters and
        access switches remain in the access column, ONTs/downstream network
        devices are placed in their own column, and endpoint groups occupy the
        final client column.  Keeping these stages separate prevents expanded
        splitter branches from being drawn on top of the access layer.
        """
        node = self._node_for(node_id)
        if node is None:
            return 9
        if node.asset_type == "site_group":
            return 0
        if node.asset_type in {"client_group", "client_device"}:
            return 5

        role = _text(node.role).lower()
        asset_type = _text(node.asset_type).lower()
        if asset_type in {"firewall", "network_router"} or any(
            word in role for word in ("gateway", "router", "firewall", "core", "aggregation")
        ):
            return 1
        if "distribution" in role or asset_type == "optical_line_terminal" or role.startswith("olt_"):
            return 2
        if asset_type in {"fibre_splitter", "patch_panel"} or "splitter" in role:
            return 3
        if asset_type == "network_switch" or "access_switch" in role:
            return 3
        if asset_type in {"optical_network_terminal", "wireless_access_point"} or role == "ont":
            return 4

        rank = self.model._hierarchy_rank(node_id)
        if rank <= 1:
            return 1
        if rank == 2:
            return 2
        if rank <= 4:
            return 3
        if rank <= 6:
            return 4
        return 5

    def _pack_layered_topology(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        self._layer_bounds: Dict[int, QRectF] = {}
        self._location_bus_by_node: Dict[str, Tuple[QPointF, float, float]] = {}
        self._failover_bus_by_node: Dict[str, Tuple[QPointF, float, float]] = {}
        self._main_bus_column_x: Optional[float] = None
        self._failover_bus_column_x: Optional[float] = None
        grouped: Dict[int, Dict[Tuple[int, str, str], List[str]]] = defaultdict(lambda: defaultdict(list))
        pseudo_ids: List[str] = []
        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None or node_id not in positions:
                continue
            if node.pseudo and node.asset_type not in {"site_group", "client_group"}:
                pseudo_ids.append(node_id)
                continue
            rack = _text(node.instance.get("rack_name"))
            location = node.location_name or f"Floor {node.floor}"
            grouped[self._topology_layer(node_id)][(node.floor, location, rack)].append(node_id)

        if not grouped:
            return

        layer_gap = 420.0

        # Extra horizontal room is reserved around the distribution/access
        # drill-down.  The allowance grows with the number of visible elements
        # in the downstream layer so opening a large splitter branch cannot
        # force cards and link labels into the neighbouring column.
        layer_item_counts = {
            layer: sum(len(node_ids) for node_ids in groups.values())
            for layer, groups in grouped.items()
        }

        def gap_after(layer: int) -> float:
            next_count = layer_item_counts.get(layer + 1, 0)
            expansion_allowance = min(300.0, max(0, next_count - 1) * 14.0)
            if layer == 2:
                return 520.0 + expansion_allowance
            if layer == 3:
                return 560.0 + expansion_allowance
            if layer == 4:
                return 480.0 + expansion_allowance
            return layer_gap + min(160.0, expansion_allowance)
        bus_lane_h = 96.0
        group_gap_y = 150.0
        # Distribution devices (including OLTs) are stacked vertically so each
        # physical device occupies its own row instead of forming one long row.
        distribution_rows_per_column = 10_000
        distribution_row_gap = 58.0
        all_layers = sorted(grouped)
        column_widths: Dict[int, float] = {}

        def group_layout_size(layer: int, node_ids: Sequence[str]) -> Tuple[float, float, int, int, float]:
            heights = [self._node_card_height(node_id) for node_id in node_ids]
            max_height = max(heights) if heights else self.CARD_H
            if layer == 2 and len(node_ids) > 1:
                rows = min(distribution_rows_per_column, len(node_ids))
                columns = int(math.ceil(len(node_ids) / rows))
                width = columns * self.CARD_W + max(0, columns - 1) * self.X_GAP
                height = rows * max_height + max(0, rows - 1) * distribution_row_gap
                return width, height, rows, columns, max_height
            width = len(node_ids) * self.CARD_W + max(0, len(node_ids) - 1) * self.X_GAP
            return width, max_height, 1, len(node_ids), max_height

        for layer in all_layers:
            max_width = self.CARD_W
            for (_floor, _location, _rack), node_ids in grouped[layer].items():
                group_width, _group_height, _rows, _columns, _max_height = group_layout_size(layer, node_ids)
                max_width = max(max_width, group_width)
            column_widths[layer] = max_width

        column_lefts: Dict[int, float] = {}
        cursor_x = 0.0
        for layer in all_layers:
            column_lefts[layer] = cursor_x
            cursor_x += column_widths[layer] + gap_after(layer)
        access_layer = 3 if 3 in column_lefts else max(all_layers)
        # Reserve distinct X-axis routing lanes.  Primary fibre uses the lane
        # nearest the access layer; failover fibre uses a separate lane further
        # upstream so the blue solid and orange dashed trunks never overlap.
        self._main_bus_column_x = column_lefts[access_layer] - 170.0
        self._failover_bus_column_x = column_lefts[access_layer] - 310.0

        for layer in all_layers:
            group_top = 0.0
            layer_groups = sorted(
                grouped[layer].items(),
                key=lambda item: (-item[0][0], item[0][1].lower(), item[0][2].lower()),
            )
            for (_floor, _location, _rack), node_ids in layer_groups:
                ordered = sorted(
                    node_ids,
                    key=lambda node_id: (
                        0 if bool(self._node_for(node_id).instance.get("logical_stack")) else 1,
                        self._node_for(node_id).name.lower(),
                    ),
                )
                group_width, group_height, grid_rows, _grid_columns, grid_card_h = group_layout_size(layer, ordered)
                group_left = column_lefts[layer] + max(0.0, (column_widths[layer] - group_width) / 2.0)
                is_switch_group = layer == 3 and any((self._node_for(node_id) and self._node_for(node_id).asset_type == "network_switch") for node_id in ordered)
                card_top = group_top + (bus_lane_h if is_switch_group else 0.0)
                for index, node_id in enumerate(ordered):
                    if layer == 2 and len(ordered) > 1:
                        column = index // grid_rows
                        row = index % grid_rows
                        y_offset = row * (grid_card_h + distribution_row_gap)
                        positions[node_id] = QPointF(group_left + column * (self.CARD_W + self.X_GAP), card_top + y_offset)
                    else:
                        positions[node_id] = QPointF(group_left + index * (self.CARD_W + self.X_GAP), card_top)
                if is_switch_group:
                    bus_y = group_top + 34.0
                    failover_bus_y = group_top + 58.0
                    bus_left = group_left + self.CARD_W / 2.0
                    bus_right = group_left + group_width - self.CARD_W / 2.0
                    for node_id in ordered:
                        positions[node_id] = QPointF(positions[node_id].x(), card_top)
                        drop_x = positions[node_id].x() + self.CARD_W / 2.0
                        self._location_bus_by_node[node_id] = (QPointF(drop_x, bus_y), bus_left, bus_right)
                        self._failover_bus_by_node[node_id] = (QPointF(drop_x, failover_bus_y), bus_left, bus_right)
                group_top += group_height + (bus_lane_h if is_switch_group else 0.0) + group_gap_y
            self._layer_bounds[layer] = QRectF(
                column_lefts[layer],
                0.0,
                column_widths[layer],
                max(self.CARD_H, group_top - group_gap_y),
            )

        pseudo_by_layer: Dict[int, List[str]] = defaultdict(list)
        for node_id in pseudo_ids:
            pseudo_by_layer[self._topology_layer(node_id)].append(node_id)
        for layer, node_ids in pseudo_by_layer.items():
            left = column_lefts.get(layer, cursor_x)
            for index, node_id in enumerate(node_ids):
                positions[node_id] = QPointF(
                    left, index * (self.CARD_H + group_gap_y)
                )

    def _stack_same_rack_logical_stacks(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        grouped: Dict[Tuple[str, Tuple[int, str, str]], List[str]] = defaultdict(list)
        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None or not bool(node.instance.get("logical_stack")):
                continue
            parent_id = self.visible_parent.get(node_id, "")
            key = self._switch_group_key(node_id)
            if parent_id and key != (0, "", ""):
                grouped[(parent_id, key)].append(node_id)

        stack_gap = max(self.X_GAP, 80.0)
        for (_parent_id, _key), node_ids in grouped.items():
            if len(node_ids) < 2:
                continue
            ordered = sorted(
                node_ids,
                key=lambda value: (
                    positions[value].x(),
                    self._node_for(value).name.lower() if self._node_for(value) else value,
                ),
            )
            left_x = min(positions[node_id].x() for node_id in ordered)
            top_y = min(positions[node_id].y() for node_id in ordered)
            for index, node_id in enumerate(ordered):
                positions[node_id] = QPointF(left_x + index * (self.CARD_W + stack_gap), top_y)

    def _separate_overlapping_cards(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        grouped: Dict[int, List[str]] = defaultdict(list)
        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None or node.pseudo or node_id not in positions:
                continue
            grouped[node.floor].append(node_id)

        min_gap = 36.0
        for _floor, node_ids in grouped.items():
            placed: List[QRectF] = []
            ordered = sorted(node_ids, key=lambda node_id: (positions[node_id].y(), positions[node_id].x()))
            for node_id in ordered:
                rect = self._card_rect(node_id, positions)
                while any(rect.adjusted(-8.0, -8.0, 8.0, 8.0).intersects(existing) for existing in placed):
                    blocking_bottom = max(
                        existing.bottom()
                        for existing in placed
                        if rect.adjusted(-8.0, -8.0, 8.0, 8.0).intersects(existing)
                    )
                    positions[node_id] = QPointF(positions[node_id].x(), blocking_bottom + min_gap)
                    rect = self._card_rect(node_id, positions)
                placed.append(rect)

    def _recenter_group_nodes(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        visible_set = set(visible_ids)

        def visible_children(node_id: str) -> List[str]:
            return [
                child_id
                for child_id in self._children_for(node_id)
                if child_id in visible_set and self.visible_parent.get(child_id) == node_id and child_id in positions
            ]

        for node_id in reversed(visible_ids):
            node = self._node_for(node_id)
            if node is None or not node.pseudo or node.asset_type == "site_group" or node_id not in positions:
                continue
            children = visible_children(node_id)
            if not children:
                continue
            left = min(positions[child_id].x() for child_id in children)
            right = max(positions[child_id].x() + self.CARD_W for child_id in children)
            positions[node_id] = QPointF((left + right - self.CARD_W) / 2.0, positions[node_id].y())

    def rebuild_scene(self, fit: bool = False) -> None:
        selected_id = ""
        selected_items = self.scene.selectedItems()
        if selected_items and isinstance(selected_items[0], TopologyCardItem):
            selected_id = selected_items[0].node.node_id

        self.scene.clear()
        self.node_items.clear()
        self.visible_nodes.clear()
        visible_ids = self._collect_visible()
        positions = self._layout_visible(visible_ids)
        self._update_breadcrumb()
        self._visible_failover_bus_nodes = set()
        visible_cross_edges = (
            self._visible_cross_edges(positions)
            if self.rack_focus is None and self.switch_port_focus is None
            else []
        )
        if self.rack_focus is None:
            for edge in visible_cross_edges:
                if edge.source_id in positions and edge.target_id in positions:
                    _source_id, target_id = self._cross_link_origin_target(edge)
                    self._visible_failover_bus_nodes.add(target_id)

        if self.rack_focus is not None and self.switch_port_focus is None:
            self._add_rack_elevation(visible_ids, positions)
        else:
            self._add_location_groups(visible_ids, positions)
        if self.rack_focus is None and self.switch_port_focus is None:
            self._add_layer_headers(visible_ids, positions)

        # Tree links first so cards sit above them.
        children_by_parent: Dict[str, List[str]] = defaultdict(list)
        for child_id, parent_id in self.visible_parent.items():
            children_by_parent[parent_id].append(child_id)

        if self.rack_focus is not None and self.switch_port_focus is None:
            children_by_parent.clear()

        for parent_id, child_ids in children_by_parent.items():
            if len(child_ids) >= 2:
                self._add_link_rail(parent_id, child_ids, positions)
                continue
            child_id = child_ids[0]
            edge = None if child_id.startswith("client::") else self._edge_by_id(self.visible_parent_edge.get(child_id, ""))
            self._add_link(parent_id, child_id, positions, edge, client_link=child_id.startswith("client::"))

        if self.show_redundant_check.isChecked() and self.rack_focus is None and self.switch_port_focus is None:
            failover_groups: Dict[str, List[Tuple[str, str, TopologyEdge]]] = defaultdict(list)
            for edge in visible_cross_edges:
                if edge.source_id in positions and edge.target_id in positions:
                    source_id, target_id = self._cross_link_origin_target(edge)
                    failover_groups[self._failover_group_key(edge, source_id)].append(
                        (source_id, target_id, edge)
                    )
            for rows in failover_groups.values():
                unique_sources = {source_id for source_id, _target_id, _edge in rows}
                if len(rows) >= 2 and len(unique_sources) >= 1:
                    self._add_shared_failover_trunk(rows, positions)
                    continue
                source_id, target_id, edge = rows[0]
                self._add_link(source_id, target_id, positions, edge, cross_link=True)

        if self.switch_port_focus is not None:
            switch_id = _text(self.switch_port_focus)
            node = self._node_for(switch_id)
            if node is not None and switch_id in positions:
                self.visible_nodes[switch_id] = node
                port_nodes = [self._switch_port_nodes_by_id[node_id] for node_id in visible_ids if node_id in self._switch_port_nodes_by_id]
                units = max(1, self._node_rack_units(switch_id))
                panel_width = 965.2
                panel_height = max(88.9, units * 88.9)
                item = (
                    SplitterFrontPanelItem(node, port_nodes, panel_width, 88.9)
                    if node.asset_type == "fibre_splitter"
                    else SwitchFrontPanelItem(node, port_nodes, panel_width, panel_height)
                )
                item.setPos(positions[switch_id])
                item.setZValue(1.0)
                item.activated.connect(self._card_activated)
                item.portActivated.connect(self._card_activated)
                self.scene.addItem(item)
                self.node_items[switch_id] = item
                for port_node in port_nodes:
                    self.visible_nodes[port_node.node_id] = port_node
        else:
            for node_id in visible_ids:
                node = self._node_for(node_id)
                if node is None:
                    continue
                self.visible_nodes[node_id] = node
                if self.rack_focus is not None:
                    units = max(1, self._node_rack_units(node_id))
                    shared_capacity = max(
                        1,
                        _int(node.instance.get("shared_rack_unit_capacity"), 1),
                    )
                    shared_group = _text(
                        node.instance.get("shared_rack_unit_group")
                    )
                    if shared_group and shared_capacity > 1:
                        # Several independent OLTs share one physical 1U mounting
                        # position.  Draw each functional unit as its own equal-width
                        # section of the 19-inch face rather than as a full-width 1U
                        # item.  The layout position already offsets each member by
                        # shared_rack_unit_position.
                        equipment_width = (482.6 - 8.0) / shared_capacity
                        equipment_height = 44.45 - 4.0
                        display_units = 1
                    else:
                        equipment_width = 482.6
                        equipment_height = units * 44.45 - 4.0
                        display_units = units
                    port_nodes = self._rack_port_nodes(node_id)
                    item = (
                        SplitterFrontPanelItem(node, port_nodes, equipment_width, 44.45 - 4.0)
                        if node.asset_type == "fibre_splitter"
                        else RackEquipmentItem(
                            node,
                            equipment_width,
                            equipment_height,
                            display_units,
                            port_nodes,
                        )
                    )
                else:
                    children = self._children_for(node_id)
                    has_children = bool(children)
                    expanded = self._is_expanded(node_id)
                    hidden = 0 if expanded else self._hidden_descendants(node_id)
                    item = TopologyCardItem(node, hidden, has_children, expanded)
                item.setPos(positions[node_id])
                item.setZValue(1.0)
                item.activated.connect(self._card_activated)
                if isinstance(item, (RackEquipmentItem, SplitterFrontPanelItem)):
                    item.portActivated.connect(self._card_activated)
                item.branchToggleRequested.connect(self._card_double_clicked)
                self.scene.addItem(item)
                self.node_items[node_id] = item

        navigation_margin = getattr(self.view, "_navigation_margin", 5000.0)
        scene_rect = self.scene.itemsBoundingRect().adjusted(
            -navigation_margin,
            -navigation_margin,
            navigation_margin,
            navigation_margin,
        )
        self.scene.setSceneRect(scene_rect)
        self._apply_search_highlight(self.search_edit.text())

        if selected_id in self.node_items:
            self.node_items[selected_id].setSelected(True)
            self._show_node_details(selected_id)
        elif not self.node_items:
            self._show_empty_details()

        self.status_label.setText(self._status_text())
        if fit:
            if self.isVisible() and self.view.viewport().width() > 0 and self.view.viewport().height() > 0:
                self._schedule_fit_topology()
            else:
                self._fit_after_show = True

    def _schedule_fit_topology(self) -> None:
        QTimer.singleShot(0, self.view.fit_topology)

    def _status_text(self) -> str:
        if self.switch_port_focus is not None:
            node = self.model.nodes.get(self.switch_port_focus)
            occupied = sum(1 for item in self._switch_port_nodes_by_id.values() if item.ports_used)
            total = len(self._switch_port_nodes_by_id)
            return f"Device port view: {node.name if node else self.switch_port_focus} · {occupied:,}/{total:,} ports occupied · Red = occupied, green = free · Click a port for details"
        if self.rack_focus is not None:
            equipment_count = sum(1 for node in self.visible_nodes.values() if not node.pseudo)
            return (
                f"Rack view: {equipment_count:,} installed equipment items shown at their rack U positions"
                " · Red ports are occupied, green ports are free · Click a port for details · Double-click a switch for its port view · Use the breadcrumb to return · Drag to pan · Wheel to zoom"
            )
        hidden_patch_panels = (
            sum(1 for node in self.model.nodes.values() if node.asset_type == "patch_panel")
            if not self._patch_panels_visible()
            else 0
        )
        hidden_cable_management = sum(
            1
            for node in self.model.nodes.values()
            if (
                _text(node.asset_type).lower() in {"cable_management", "cable_manager"}
                or _text(node.role).lower() in {"cable_management", "cable_manager"}
                or "cable management" in _text(node.name).lower()
                or "cable-management" in _text(node.name).lower()
            )
        )
        return (
            f"Showing {sum(1 for node in self.visible_nodes.values() if not node.pseudo):,} of "
            f"{sum(1 for node in self.model.nodes.values() if not node.pseudo):,} network assets"
            + (f" · {hidden_patch_panels:,} patch panels omitted from topology" if hidden_patch_panels else "")
            + (f" · {hidden_cable_management:,} cable-management items omitted from topology" if hidden_cable_management else "")
            + (f" on Floor {self._selected_floor()}" if self._selected_floor() is not None else "")
            + " · Double-click a rack switch to drill into its rack · Drag to pan · Wheel to zoom"
        )

    def _node_card_height(self, node_id: str) -> float:
        node = self._node_for(node_id)
        if node is None:
            return self.CARD_H
        members = max(1, _int(node.instance.get("stack_member_count"), 1)) if bool(node.instance.get("logical_stack")) else 1
        if members <= 1:
            return self.CARD_H
        visible_members = min(members, 8)
        row_gap = 6.0
        frame_h = visible_members * TopologyCardItem.STACK_MEMBER_H + row_gap * max(0, visible_members - 1) + 18.0
        return max(self.CARD_H, TopologyCardItem.STACK_HEADER_H + frame_h + 34.0)

    def _card_rect(self, node_id: str, positions: Dict[str, QPointF]) -> QRectF:
        pos = positions[node_id]
        if self.rack_focus is not None and self.switch_port_focus is None:
            node = self._node_for(node_id)
            shared_capacity = max(1, _int(node.instance.get("shared_rack_unit_capacity"), 1)) if node is not None else 1
            width = (482.6 - 8.0) / shared_capacity if shared_capacity > 1 else 482.6
            return QRectF(pos.x(), pos.y(), width, max(1, self._node_rack_units(node_id)) * 44.45 - 4.0)
        if self.switch_port_focus is not None and node_id == _text(self.switch_port_focus):
            units = max(1, self._node_rack_units(node_id))
            return QRectF(pos.x(), pos.y(), 965.2, max(88.9, units * 88.9))
        return QRectF(pos.x(), pos.y(), self.CARD_W, self._node_card_height(node_id))

    def _add_layer_headers(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        labels = {
            0: "Site",
            1: "Core layer",
            2: "Distribution layer",
            3: "Access / splitter layer",
            4: "Downstream devices",
            5: "Client devices",
        }
        layer_rects: Dict[int, QRectF] = {}
        for node_id in visible_ids:
            if node_id not in positions:
                continue
            node = self._node_for(node_id)
            if node is None:
                continue
            layer = self._topology_layer(node_id)
            rect = self._card_rect(node_id, positions)
            layer_rects[layer] = rect if layer not in layer_rects else layer_rects[layer].united(rect)

        for layer, rect in sorted(layer_rects.items()):
            text = labels.get(layer, "Network")
            label = QGraphicsSimpleTextItem(text)
            font = QFont("Arial", 11)
            font.setBold(True)
            label.setFont(font)
            label.setBrush(QBrush(QColor("#d9e4ec")))
            label_rect = label.boundingRect()
            label.setPos(rect.center().x() - label_rect.width() / 2.0, rect.top() - 46.0)
            label.setZValue(-0.2)
            self.scene.addItem(label)

            underline = QPainterPath(QPointF(rect.left(), rect.top() - 16.0))
            underline.lineTo(QPointF(rect.right(), rect.top() - 16.0))
            item = QGraphicsPathItem(underline)
            item.setPen(QPen(QColor("#34424e"), 1.2))
            item.setZValue(-1.5)
            self.scene.addItem(item)

    def _rack_group_key(self, node_id: str) -> Tuple[int, str, str]:
        node = self._node_for(node_id)
        if node is None or node.pseudo:
            return (0, "", "")
        rack = _text(node.instance.get("rack_name"))
        location = node.location_name
        if not rack:
            return (0, "", "")
        return (node.floor, location, rack)

    def _switch_group_key(self, node_id: str) -> Tuple[int, str, str]:
        node = self._node_for(node_id)
        if node is None or not self._supports_port_view(node):
            return (0, "", "")
        rack = _text(node.instance.get("rack_name"))
        location = node.location_name
        if not location and not rack:
            return (0, "", "")
        return (node.floor, location, rack)

    def _node_rack_units(self, node_id: str) -> int:
        node = self._node_for(node_id)
        if node is None:
            return 0
        members = max(1, _int(node.instance.get("stack_member_count"), 1)) if bool(node.instance.get("logical_stack")) else 1
        if node.asset_type == "network_switch":
            allowance = max(0, _int(node.asset.get("switch_rack_unit_allowance"), _int(node.asset.get("rack_units"), 1)))
            return max(1, allowance) * members
        return max(0, _int(node.asset.get("rack_units"), 1))

    def _rack_names_for_location(self, floor: int, location: str) -> List[str]:
        names = {
            _text(node.instance.get("rack_name"))
            for node in self.model.nodes.values()
            if not node.pseudo
            and node.floor == int(floor)
            and node.location_name == location
            and _text(node.instance.get("rack_name"))
        }
        # Support equipment generated into the same named rack can have legacy
        # or blank location metadata. Keep it visible with the active equipment.
        if self.rack_focus is not None and self.rack_focus[2]:
            names.add(_text(self.rack_focus[2]))
        return sorted(names, key=lambda value: value.lower())

    def _rack_capacity_for_key(self, key: Tuple[int, str, str]) -> int:
        default_capacity = max(1, _int(self.data.get("network_settings", {}).get("default_rack_size_u"), 42))
        explicit_capacity = 0
        for node in self.model.nodes.values():
            if node.pseudo or not _text(node.instance.get("rack_name")):
                continue
            node_key = (node.floor, node.location_name, _text(node.instance.get("rack_name")))
            if node_key == key:
                explicit_capacity = max(explicit_capacity, _int(node.instance.get("rack_size_u"), 0))
        return explicit_capacity or default_capacity

    def _rack_used_for_key(self, key: Tuple[int, str, str]) -> int:
        used = 0
        for node_id, node in self.model.nodes.items():
            if node.pseudo or not _text(node.instance.get("rack_name")):
                continue
            node_key = (node.floor, node.location_name, _text(node.instance.get("rack_name")))
            if node_key == key:
                start_u = max(1, _int(node.instance.get("rack_start_u"), 1))
                used = max(used, start_u + max(1, self._node_rack_units(node_id)) - 1)
        return used

    def _add_rack_elevation(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        key = self.rack_focus
        if key is None:
            return
        floor, location, selected_rack = key
        rack_names = self._rack_names_for_location(floor, location)
        if not rack_names:
            rack_names = [selected_rack]
        rack_left_start = 92.0
        rack_top = 90.0
        rack_width = 518.6
        rack_gap = 90.0
        unit_pitch = 44.45

        for rack_index, rack_name in enumerate(rack_names):
            rack_key = (floor, location, rack_name)
            capacity = self._rack_capacity_for_key(rack_key)
            rack_left = rack_left_start + rack_index * (rack_width + rack_gap)
            rack_height = capacity * unit_pitch
            frame_path = QPainterPath()
            frame_path.addRoundedRect(QRectF(rack_left, rack_top, rack_width, rack_height), 8.0, 8.0)
            frame = QGraphicsPathItem(frame_path)
            frame.setPen(QPen(QColor("#7f95a5") if rack_name == selected_rack else QColor("#5d6b76"), 2.4 if rack_name == selected_rack else 2.0))
            frame.setBrush(QBrush(QColor("#141c23")))
            frame.setZValue(-2.0)
            self.scene.addItem(frame)
            for u in range(1, capacity + 1):
                y = rack_top + (capacity - u) * unit_pitch
                line = QPainterPath(QPointF(rack_left, y))
                line.lineTo(QPointF(rack_left + rack_width, y))
                item = QGraphicsPathItem(line)
                item.setPen(QPen(QColor("#2f3b45"), 0.8))
                item.setZValue(-1.8)
                self.scene.addItem(item)
                label = QGraphicsSimpleTextItem(f"{u}U")
                label.setFont(QFont("Arial", 8))
                label.setBrush(QBrush(QColor("#8997a2")))
                label.setPos(rack_left - 36.0, y + 7.0)
                label.setZValue(-1.0)
                self.scene.addItem(label)
            used = self._rack_used_for_key(rack_key)
            title = QGraphicsSimpleTextItem(f"{rack_name} — {used}/{capacity}U")
            font = QFont("Arial", 12)
            font.setBold(True)
            title.setFont(font)
            title.setBrush(QBrush(QColor("#e5edf2")))
            title.setPos(rack_left, rack_top - 42.0)
            title.setZValue(-1.0)
            self.scene.addItem(title)

        room_title = QGraphicsSimpleTextItem(
            f"Rack elevation — {location or 'Unassigned location'} ({len(rack_names)} rack{'s' if len(rack_names) != 1 else ''})"
        )
        room_font = QFont("Arial", 14)
        room_font.setBold(True)
        room_title.setFont(room_font)
        room_title.setBrush(QBrush(QColor("#ffffff")))
        room_title.setPos(rack_left_start, 24.0)
        room_title.setZValue(-1.0)
        self.scene.addItem(room_title)

    def _add_location_groups(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        grouped: Dict[Tuple[int, str, str], List[str]] = defaultdict(list)
        for node_id in visible_ids:
            key = self._switch_group_key(node_id)
            if key != (0, "", ""):
                grouped[key].append(node_id)

        for (_floor, location, rack), node_ids in grouped.items():
            rect: Optional[QRectF] = None
            for node_id in node_ids:
                if node_id not in positions:
                    continue
                card_rect = self._card_rect(node_id, positions)
                rect = card_rect if rect is None else rect.united(card_rect)
            if rect is None:
                continue
            bus_nodes = [node_id for node_id in node_ids if node_id in getattr(self, "_location_bus_by_node", {})]
            rect = rect.adjusted(-20.0, -98.0 if bus_nodes else -34.0, 20.0, 22.0)
            path = QPainterPath()
            path.addRoundedRect(rect, 10.0, 10.0)
            item = QGraphicsPathItem(path)
            item.setBrush(QColor(29, 40, 50, 150))
            item.setPen(QPen(QColor("#344653"), 1.0, Qt.DashLine))
            item.setZValue(-1.5)
            self.scene.addItem(item)

            label_text = rack or location
            if rack and location:
                label_text = f"{location} / {rack}"
            key = (_floor, location, rack)
            capacity = self._rack_capacity_for_key(key)
            used = self._rack_used_for_key(key)
            if capacity:
                label_text = f"{label_text} · {used}/{capacity}U"
            label = QGraphicsSimpleTextItem(label_text)
            label.setFont(QFont("Arial", 8))
            label.setBrush(QBrush(QColor("#9fb0bd")))
            label.setPos(rect.left() + 10.0, rect.top() + 8.0)
            label.setZValue(-1.4)
            self.scene.addItem(label)

            if bus_nodes:
                bus_anchor, bus_left, bus_right = self._location_bus_by_node[bus_nodes[0]]
                fail_anchor, fail_left, fail_right = self._failover_bus_by_node.get(bus_nodes[0], (bus_anchor, bus_left, bus_right))
                main_bus = QPainterPath(QPointF(bus_left, bus_anchor.y()))
                main_bus.lineTo(QPointF(bus_right, bus_anchor.y()))
                main_item = QGraphicsPathItem(main_bus)
                main_item.setPen(QPen(QColor("#6f8dff"), 2.2))
                main_item.setZValue(-0.98)
                self.scene.addItem(main_item)

                failover_nodes = getattr(self, "_visible_failover_bus_nodes", set())
                if any(node_id in failover_nodes for node_id in bus_nodes):
                    fail_bus = QPainterPath(QPointF(fail_left, fail_anchor.y()))
                    fail_bus.lineTo(QPointF(fail_right, fail_anchor.y()))
                    fail_item = QGraphicsPathItem(fail_bus)
                    fail_pen = QPen(QColor("#d68f52"), 2.0, Qt.DashLine)
                    fail_item.setPen(fail_pen)
                    fail_item.setZValue(-0.86)
                    self.scene.addItem(fail_item)

    def _link_colour(self, medium: str) -> QColor:
        return {
            "fibre": QColor("#6f8dff"),
            "copper": QColor("#4fbfa3"),
            "wireless": QColor("#e3a34e"),
            "virtual": QColor("#6f7b86"),
            "stacking": QColor("#b779e3"),
            "none": QColor("#6f7b86"),
        }.get(medium, QColor("#6f7b86"))

    @staticmethod
    def _edge_is_failover(edge: Optional[TopologyEdge]) -> bool:
        if edge is None:
            return False
        return bool(
            edge.standby
            or _text(edge.redundancy_role).lower() in {"secondary", "standby", "failover"}
            or bool(edge.connection.get("standby", False))
            or _text(edge.connection.get("redundancy_role")).lower()
            in {"secondary", "standby", "failover"}
        )

    def _edge_colour(self, edge: Optional[TopologyEdge], medium: str = "") -> QColor:
        if self._edge_is_failover(edge):
            return QColor("#d68f52")
        return self._link_colour(medium or (edge.medium if edge else ""))

    def _link_label_required_span(self, edge: Optional[TopologyEdge]) -> float:
        """Return the horizontal branch length needed to show a full link label."""
        if edge is None:
            return 0.0
        font = QFont("Arial", 7)
        width = float(QFontMetrics(font).horizontalAdvance(edge.label) + 34)
        return min(520.0, max(150.0, width + 24.0))

    @staticmethod
    def _failover_group_key(edge: TopologyEdge, source_id: str) -> str:
        group = _text(edge.protection_group)
        return group or f"source::{source_id}"

    def _primary_fibre_lane_x(self, bus_left: float, required_span: float) -> float:
        """Return the dedicated X lane for primary fibre trunks."""
        preferred = (
            self._main_bus_column_x
            if self._main_bus_column_x is not None
            else bus_left - 170.0
        )
        return min(preferred, bus_left - required_span - 34.0)

    def _failover_fibre_lane_x(self, bus_left: float, required_span: float) -> float:
        """Return a failover lane kept clear of the primary fibre lane."""
        primary_lane = self._primary_fibre_lane_x(bus_left, required_span)
        preferred = (
            self._failover_bus_column_x
            if self._failover_bus_column_x is not None
            else primary_lane - 140.0
        )
        # Keep at least 120 scene units between the two vertical trunks.  This
        # remains true when a long label pushes the primary lane further left.
        return min(
            preferred,
            primary_lane - 120.0,
            bus_left - required_span - 154.0,
        )

    def _add_link_label(self, text: str, colour: QColor, point: QPointF) -> None:
        if not text:
            return
        label = LinkLabelItem(text, colour)
        # Keep failover labels clear of the primary fibre label and bus line.
        if "failover" in text.lower() or "standby" in text.lower():
            point = QPointF(point.x(), point.y() - 14.0)
        label.setPos(point)
        self.scene.addItem(label)

    def _bus_for_node(self, node_id: str, failover: bool = False) -> Optional[Tuple[QPointF, float, float]]:
        if failover:
            return getattr(self, "_failover_bus_by_node", {}).get(node_id)
        return getattr(self, "_location_bus_by_node", {}).get(node_id)

    def _add_bus_drop(
        self,
        node_id: str,
        positions: Dict[str, QPointF],
        colour: QColor,
        failover: bool = False,
        z_value: float = -0.95,
    ) -> None:
        bus = self._bus_for_node(node_id, failover=failover)
        target_pos = positions.get(node_id)
        if bus is None or target_pos is None:
            return
        bus_anchor, _bus_left, _bus_right = bus
        top = QPointF(target_pos.x() + self.CARD_W / 2.0, target_pos.y())
        path = QPainterPath(bus_anchor)
        path.lineTo(top)
        item = QGraphicsPathItem(path)
        pen = QPen(colour, 2.0)
        if failover:
            pen.setStyle(Qt.DashLine)
        item.setPen(pen)
        item.setZValue(z_value)
        self.scene.addItem(item)

    def _add_link_rail(self, source_id: str, child_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        if source_id not in positions:
            return

        source_node = self._node_for(source_id)
        # OLT and distribution outputs represent separate physical PON/uplink
        # ports. Draw one routed link per child rather than merging all outputs
        # into a shared rail, which made multiple devices look like one feeder.
        if source_node is not None and (
            source_node.asset_type == "optical_line_terminal"
            or source_node.role.startswith("olt_")
            or "distribution" in source_node.role
        ):
            for child_id in child_ids:
                if child_id not in positions:
                    continue
                edge = None if child_id.startswith("client::") else self._edge_by_id(
                    self.visible_parent_edge.get(child_id, "")
                )
                self._add_link(
                    source_id,
                    child_id,
                    positions,
                    edge,
                    client_link=child_id.startswith("client::"),
                )
            return

        grouped: Dict[str, List[str]] = defaultdict(list)
        for child_id in child_ids:
            node = self._node_for(child_id)
            location = node.location_name if node is not None else ""
            grouped[location or child_id].append(child_id)

        for _location, group_child_ids in grouped.items():
            source_pos = positions[source_id]
            source_start = QPointF(source_pos.x() + self.CARD_W, source_pos.y() + self._node_card_height(source_id) / 2.0)
            endpoints = []
            for child_id in group_child_ids:
                target_pos = positions.get(child_id)
                if target_pos is None:
                    continue
                edge = None if child_id.startswith("client::") else self._edge_by_id(self.visible_parent_edge.get(child_id, ""))
                bus = self._bus_for_node(child_id)
                endpoint = bus[0] if bus else QPointF(target_pos.x(), target_pos.y() + self._node_card_height(child_id) / 2.0)
                endpoints.append((child_id, endpoint, edge))
            endpoints.sort(key=lambda item: (item[1].y(), item[1].x()))
            if len(endpoints) < 2:
                if endpoints:
                    child_id, _end, edge = endpoints[0]
                    self._add_link(source_id, child_id, positions, edge, client_link=child_id.startswith("client::"))
                continue

            bus_endpoints = [(child_id, point, edge) for child_id, point, edge in endpoints if self._bus_for_node(child_id)]
            if len(bus_endpoints) == len(endpoints):
                bus_left = min(self._bus_for_node(child_id)[1] for child_id, _point, _edge in endpoints)
                bus_y = min(point.y() for _child_id, point, _edge in endpoints)
                required_span = max(
                    self._link_label_required_span(edge)
                    for _child_id, _point, edge in endpoints
                    if edge is not None
                )
                entry_x = self._primary_fibre_lane_x(bus_left, required_span)
                trunk_colour = QColor("#60717f")
                trunk_pen = QPen(trunk_colour, 2.0)
                trunk = QPainterPath(source_start)
                trunk.lineTo(QPointF(entry_x, source_start.y()))
                trunk.lineTo(QPointF(entry_x, bus_y))
                trunk.lineTo(QPointF(bus_left, bus_y))
                trunk_item = QGraphicsPathItem(trunk)
                trunk_item.setPen(trunk_pen)
                trunk_item.setZValue(-1.0)
                self.scene.addItem(trunk_item)
                for child_id, end, edge in endpoints:
                    failover = self._edge_is_failover(edge)
                    colour = self._edge_colour(edge, edge.medium if edge else "fibre")
                    self._add_bus_drop(
                        child_id, positions, colour, failover=failover,
                        z_value=-0.80 if failover else -0.95,
                    )
                    if edge and self.show_link_labels_check.isChecked():
                        self._add_link_label(edge.label, colour, QPointF((entry_x + end.x()) / 2.0, end.y() - 14.0))
                continue

            rail_x = min(point.x() for _child_id, point, _edge in endpoints) - 52.0
            rail_top = min(point.y() for _child_id, point, _edge in endpoints)
            rail_bottom = max(point.y() for _child_id, point, _edge in endpoints)
            trunk_colour = QColor("#60717f")
            trunk_pen = QPen(trunk_colour, 2.0)

            trunk = QPainterPath(source_start)
            trunk.lineTo(QPointF(rail_x, source_start.y()))
            trunk.moveTo(QPointF(rail_x, rail_top))
            trunk.lineTo(QPointF(rail_x, rail_bottom))
            trunk_item = QGraphicsPathItem(trunk)
            trunk_item.setPen(trunk_pen)
            trunk_item.setZValue(-1.0)
            self.scene.addItem(trunk_item)

            for child_id, end, edge in endpoints:
                client_link = child_id.startswith("client::")
                medium = edge.medium if edge else ("virtual" if client_link else "copper")
                standby = self._edge_is_failover(edge)
                colour = QColor("#d68f52") if standby else self._link_colour(medium)
                pen = QPen(colour, 2.0 if not client_link else 1.3)
                if standby or client_link:
                    pen.setStyle(Qt.DashLine)
                branch = QPainterPath(QPointF(rail_x, end.y()))
                branch.lineTo(end)
                branch_item = QGraphicsPathItem(branch)
                branch_item.setPen(pen)
                branch_item.setZValue(-0.95)
                self.scene.addItem(branch_item)
                if edge and self.show_link_labels_check.isChecked() and not client_link:
                    self._add_link_label(edge.label, colour, QPointF((rail_x + end.x()) / 2.0, end.y()))

    def _add_vertical_link_rail(
        self,
        start: QPointF,
        endpoints: Sequence[Tuple[str, QPointF, Optional[TopologyEdge]]],
        trunk_colour: QColor,
        z_value: float,
        cross_link: bool = False,
        rail_x: Optional[float] = None,
    ) -> None:
        if rail_x is None:
            rail_x = min(point.x() for _child_id, point, _edge in endpoints) - self.CARD_W / 2.0 - 34.0
        rail_top = min(start.y(), min(point.y() for _child_id, point, _edge in endpoints))
        rail_bottom = max(point.y() for _child_id, point, _edge in endpoints)
        trunk_pen = QPen(trunk_colour, 2.0)
        if cross_link:
            trunk_pen.setStyle(Qt.DashLine)

        trunk = QPainterPath(start)
        trunk.lineTo(QPointF(rail_x, start.y()))
        trunk.lineTo(QPointF(rail_x, rail_top))
        trunk.moveTo(QPointF(rail_x, rail_top))
        trunk.lineTo(QPointF(rail_x, rail_bottom))
        trunk_item = QGraphicsPathItem(trunk)
        trunk_item.setPen(trunk_pen)
        trunk_item.setZValue(z_value)
        self.scene.addItem(trunk_item)

        for child_id, end, edge in endpoints:
            client_link = child_id.startswith("client::")
            medium = edge.medium if edge else ("virtual" if client_link else "copper")
            standby = cross_link or self._edge_is_failover(edge)
            colour = QColor("#d68f52") if standby else self._link_colour(medium)
            pen = QPen(colour, 2.0 if not client_link else 1.3)
            if standby or client_link or cross_link:
                pen.setStyle(Qt.DashLine)
            branch_y = end.y()
            branch = QPainterPath(QPointF(rail_x, branch_y))
            branch.lineTo(end)
            branch_item = QGraphicsPathItem(branch)
            branch_item.setPen(pen)
            branch_item.setZValue(z_value + 0.05)
            self.scene.addItem(branch_item)
            if edge and self.show_link_labels_check.isChecked() and not client_link:
                self._add_link_label(edge.label, colour, QPointF((rail_x + end.x()) / 2.0, branch_y))

    def _active_descendants_for_hidden(
        self, node_id: str, positions: Dict[str, QPointF]
    ) -> List[str]:
        """Return the first visible active nodes below a hidden passive node.

        Passive splitters, patch panels and rack support items are omitted from
        the overview. A standby fibre commonly terminates on one of those hidden
        nodes, so the failover link must continue to the first visible active
        descendant rather than disappearing with the passive card.
        """
        result: List[str] = []
        pending = deque(self.model.children.get(node_id, []))
        seen: Set[str] = set()
        while pending:
            candidate_id = pending.popleft()
            if candidate_id in seen:
                continue
            seen.add(candidate_id)
            if candidate_id in positions and self._is_active_topology_device(candidate_id):
                result.append(candidate_id)
                continue
            pending.extend(self.model.children.get(candidate_id, []))
        return result

    def _tree_length_between(self, ancestor_id: str, descendant_id: str) -> float:
        """Return the saved cable length along a parent-chain segment."""
        total = 0.0
        cursor = descendant_id
        guard = 0
        while cursor and cursor != ancestor_id and guard <= len(self.model.nodes):
            edge = self.model.edges_by_id.get(self.model.parent_edge.get(cursor, ""))
            if edge is not None:
                total += max(0.0, edge.length_m)
            cursor = self.model.parent.get(cursor, "")
            guard += 1
        return total if cursor == ancestor_id else 0.0

    def _visible_cross_edges(
        self, positions: Dict[str, QPointF]
    ) -> List[TopologyEdge]:
        """Return cross/failover links mapped onto visible active devices.

        Direct cross-links are retained unchanged. Standby links ending on a
        hidden splitter or patch panel are expanded to the first visible active
        descendants, preserving the failover relationship in the active-only
        topology.
        """
        visible: List[TopologyEdge] = []
        seen: Set[Tuple[str, str, str, str]] = set()
        for edge in self.model.cross_edges():
            source_visible = edge.source_id in positions
            target_visible = edge.target_id in positions
            if source_visible and target_visible:
                key = (edge.source_id, edge.target_id, edge.protection_group, edge.edge_id)
                if key not in seen:
                    seen.add(key)
                    visible.append(edge)
                continue

            is_failover = bool(
                edge.standby
                or edge.redundancy_role.lower() in {"secondary", "standby", "failover"}
                or edge.protection_group
            )
            if not is_failover:
                continue

            mappings: List[Tuple[str, str, float]] = []
            if source_visible and not target_visible:
                for target_id in self._active_descendants_for_hidden(edge.target_id, positions):
                    mappings.append(
                        (edge.source_id, target_id, self._tree_length_between(edge.target_id, target_id))
                    )
            elif target_visible and not source_visible:
                for source_id in self._active_descendants_for_hidden(edge.source_id, positions):
                    mappings.append(
                        (source_id, edge.target_id, self._tree_length_between(edge.source_id, source_id))
                    )

            for source_id, target_id, extra_length in mappings:
                if source_id == target_id or source_id not in positions or target_id not in positions:
                    continue
                pair = tuple(sorted((source_id, target_id)))
                key = (pair[0], pair[1], edge.protection_group, edge.edge_id)
                if key in seen:
                    continue
                seen.add(key)
                synthetic = TopologyEdge(
                    edge_id=f"visible-failover::{edge.edge_id}::{source_id}::{target_id}",
                    source_id=source_id,
                    target_id=target_id,
                    medium=edge.medium or "fibre",
                    source_port=edge.source_port,
                    target_port=edge.target_port,
                    length_m=max(0.0, edge.length_m) + max(0.0, extra_length),
                    standby=True,
                    redundancy_role=edge.redundancy_role or "standby",
                    protection_group=edge.protection_group,
                    connection={
                        **edge.connection,
                        "collapsed_passive_failover": edge.edge_id,
                    },
                )
                self._visible_synthetic_edges[synthetic.edge_id] = synthetic
                visible.append(synthetic)
        return visible

    def _cross_link_origin_target(self, edge: TopologyEdge) -> Tuple[str, str]:
        source_rank = self.model._hierarchy_rank(edge.source_id)
        target_rank = self.model._hierarchy_rank(edge.target_id)
        if target_rank < source_rank:
            return edge.target_id, edge.source_id
        return edge.source_id, edge.target_id

    def _add_shared_failover_trunk(
        self,
        rows: Sequence[Tuple[str, str, TopologyEdge]],
        positions: Dict[str, QPointF],
    ) -> None:
        """Draw one common orange dashed trunk for a protected failover group."""
        valid = [row for row in rows if row[0] in positions and row[1] in positions]
        if not valid:
            return

        colour = QColor("#d68f52")
        sources = sorted({source_id for source_id, _target_id, _edge in valid})
        target_rows: List[Tuple[str, QPointF, TopologyEdge]] = []
        for _source_id, target_id, edge in valid:
            target_pos = positions[target_id]
            bus = self._bus_for_node(target_id, failover=True)
            point = bus[0] if bus else QPointF(
                target_pos.x(),
                self._incoming_fibre_connection_y(target_id, positions, edge, cross_link=True),
            )
            target_rows.append((target_id, point, edge))

        all_target_x = [point.x() for _target_id, point, _edge in target_rows]
        min_target_x = min(all_target_x)
        max_label_span = max(self._link_label_required_span(edge) for _t, _p, edge in target_rows)
        trunk_x = self._failover_fibre_lane_x(
            min_target_x,
            max(150.0, max_label_span + 38.0),
        )

        y_values: List[float] = []
        source_points: List[Tuple[str, QPointF]] = []
        for source_id in sources:
            source_pos = positions[source_id]
            point = QPointF(
                source_pos.x() + self.CARD_W,
                source_pos.y() + self._node_card_height(source_id) / 2.0,
            )
            source_points.append((source_id, point))
            y_values.append(point.y())
        y_values.extend(point.y() for _target_id, point, _edge in target_rows)

        trunk = QPainterPath(QPointF(trunk_x, min(y_values)))
        trunk.lineTo(QPointF(trunk_x, max(y_values)))
        trunk_item = QGraphicsPathItem(trunk)
        trunk_item.setPen(QPen(colour, 2.2, Qt.DashLine))
        trunk_item.setZValue(-0.84)
        self.scene.addItem(trunk_item)

        # Join every standby source to the same vertical trunk.
        for _source_id, source_point in source_points:
            branch = QPainterPath(source_point)
            branch.lineTo(QPointF(trunk_x, source_point.y()))
            item = QGraphicsPathItem(branch)
            item.setPen(QPen(colour, 2.0, Qt.DashLine))
            item.setZValue(-0.83)
            self.scene.addItem(item)

        # Join every protected branch from the common trunk to its destination.
        for target_id, target_point, edge in target_rows:
            bus = self._bus_for_node(target_id, failover=True)
            required = self._link_label_required_span(edge)
            branch_end_x = target_point.x()
            if branch_end_x - trunk_x < required:
                # Route left first to create a dedicated horizontal label section.
                label_lane_x = branch_end_x - required
                branch = QPainterPath(QPointF(trunk_x, target_point.y()))
                branch.lineTo(QPointF(label_lane_x, target_point.y()))
                branch.lineTo(target_point)
                label_point = QPointF((label_lane_x + branch_end_x) / 2.0, target_point.y() - 14.0)
            else:
                branch = QPainterPath(QPointF(trunk_x, target_point.y()))
                branch.lineTo(target_point)
                label_point = QPointF((trunk_x + branch_end_x) / 2.0, target_point.y() - 14.0)
            item = QGraphicsPathItem(branch)
            item.setPen(QPen(colour, 2.0, Qt.DashLine))
            item.setZValue(-0.82)
            self.scene.addItem(item)
            if bus is not None:
                self._add_bus_drop(target_id, positions, colour, failover=True, z_value=-0.8)
            if self.show_link_labels_check.isChecked():
                self._add_link_label(edge.label, colour, label_point)

    def _add_cross_link_rail(
        self,
        source_id: str,
        targets: Sequence[Tuple[str, TopologyEdge]],
        positions: Dict[str, QPointF],
    ) -> None:
        source_pos = positions[source_id]
        endpoints = []
        for target_id, edge in targets:
            target_pos = positions.get(target_id)
            if target_pos is None:
                continue
            bus = self._bus_for_node(target_id, failover=True)
            endpoint = bus[0] if bus else QPointF(
                target_pos.x() + self.CARD_W / 2.0,
                self._incoming_fibre_connection_y(target_id, positions, edge, cross_link=True),
            )
            endpoints.append((target_id, endpoint, edge))
        endpoints.sort(key=lambda item: (item[1].y(), item[1].x()))
        if len(endpoints) < 2:
            if endpoints:
                _target_id, _end, edge = endpoints[0]
                self._add_link(*self._cross_link_origin_target(edge), positions, edge, cross_link=True)
            return

        bus_endpoints = [(target_id, point, edge) for target_id, point, edge in endpoints if self._bus_for_node(target_id, failover=True)]
        if len(bus_endpoints) == len(endpoints):
            bus_left = min(self._bus_for_node(target_id, failover=True)[1] for target_id, _point, _edge in endpoints)
            bus_right = max(self._bus_for_node(target_id, failover=True)[2] for target_id, _point, _edge in endpoints)
            bus_y = min(point.y() for _target_id, point, _edge in endpoints)
            source_start = QPointF(source_pos.x() + self.CARD_W + 8.0, source_pos.y() + self._node_card_height(source_id) / 2.0)
            if source_start.x() <= bus_left:
                required_span = max(
                    self._link_label_required_span(edge)
                    for _target_id, _point, edge in endpoints
                )
                entry_x = self._failover_fibre_lane_x(bus_left, required_span)
                bus_entry_x = bus_left
            else:
                entry_x = bus_right + 68.0
                bus_entry_x = bus_right
            trunk = QPainterPath(source_start)
            trunk.lineTo(QPointF(entry_x, source_start.y()))
            trunk.lineTo(QPointF(entry_x, bus_y))
            trunk.lineTo(QPointF(bus_entry_x, bus_y))
            trunk_item = QGraphicsPathItem(trunk)
            trunk_item.setPen(QPen(QColor("#d68f52"), 2.0, Qt.DashLine))
            trunk_item.setZValue(-0.82)
            self.scene.addItem(trunk_item)
            for target_id, end, edge in endpoints:
                self._add_bus_drop(target_id, positions, QColor("#d68f52"), failover=True, z_value=-0.8)
                if self.show_link_labels_check.isChecked():
                    self._add_link_label(edge.label, QColor("#d68f52"), QPointF((entry_x + end.x()) / 2.0, end.y() - 14.0))
            return

        average_target_x = sum(point.x() for _target_id, point, _edge in endpoints) / len(endpoints)
        route_right = average_target_x >= source_pos.x() + self.CARD_W / 2.0
        if route_right:
            start = QPointF(source_pos.x() + self.CARD_W + 8.0, source_pos.y() + self._node_card_height(source_id) / 2.0)
            rail_x = source_pos.x() + self.CARD_W + 52.0
        else:
            start = QPointF(source_pos.x() - 8.0, source_pos.y() + self._node_card_height(source_id) / 2.0)
            rail_x = source_pos.x() - 52.0

        self._add_vertical_link_rail(start, endpoints, QColor("#a76a42"), z_value=-0.82, cross_link=True, rail_x=rail_x)


    def _incoming_fibre_connection_y(
        self,
        node_id: str,
        positions: Dict[str, QPointF],
        edge: Optional[TopologyEdge],
        cross_link: bool = False,
    ) -> float:
        """Return an evenly spaced vertical connection point for fibre inputs.

        Primary and failover fibres often terminate on the same splitter or
        distribution card.  Using the card centre for both makes the final
        horizontal sections overlap.  Reserve one vertical slot per visible
        incoming fibre and centre the slots around the card midpoint.
        """
        pos = positions[node_id]
        card_height = self._node_card_height(node_id)
        centre_y = pos.y() + card_height / 2.0
        if edge is None or _text(edge.medium).lower() != "fibre":
            return centre_y

        incoming = []
        visible_ids = set(positions)
        for candidate in self.model.edges:
            if candidate.target_id != node_id:
                continue
            if candidate.source_id not in visible_ids:
                continue
            if _text(candidate.medium).lower() != "fibre":
                continue
            standby = bool(
                candidate.standby
                or candidate.redundancy_role.lower() in {"secondary", "standby"}
            )
            incoming.append((1 if standby else 0, candidate.edge_id, candidate))

        if len(incoming) <= 1:
            return centre_y

        incoming.sort(key=lambda row: (row[0], row[1]))
        selected_index = 0
        for index, (_standby, _edge_id, candidate) in enumerate(incoming):
            if candidate.edge_id == edge.edge_id:
                selected_index = index
                break

        # Fit all connection points inside the card with a sensible maximum
        # pitch.  Two fibres therefore sit evenly above and below centre.
        usable_height = max(8.0, card_height - 20.0)
        pitch = min(18.0, usable_height / max(1, len(incoming) - 1))
        first_y = centre_y - pitch * (len(incoming) - 1) / 2.0
        return first_y + selected_index * pitch

    def _add_link(
        self,
        source_id: str,
        target_id: str,
        positions: Dict[str, QPointF],
        edge: Optional[TopologyEdge],
        cross_link: bool = False,
        client_link: bool = False,
    ) -> None:
        source_pos = positions[source_id]
        target_pos = positions[target_id]
        standby = self._edge_is_failover(edge)
        failover_style = cross_link or standby
        target_bus = self._bus_for_node(target_id, failover=failover_style)
        if failover_style:
            source_center = QPointF(source_pos.x() + self.CARD_W / 2.0, source_pos.y() + self._node_card_height(source_id) / 2.0)
            target_center = QPointF(
                target_pos.x() + self.CARD_W / 2.0,
                self._incoming_fibre_connection_y(target_id, positions, edge, cross_link=True),
            )
            if target_center.x() >= source_center.x():
                start = QPointF(source_pos.x() + self.CARD_W + 8.0, source_center.y())
                end = QPointF(target_pos.x() - 8.0, target_center.y())
            else:
                start = QPointF(source_pos.x() - 8.0, source_center.y())
                end = QPointF(target_pos.x() + self.CARD_W + 8.0, target_center.y())
        else:
            start = QPointF(source_pos.x() + self.CARD_W, source_pos.y() + self._node_card_height(source_id) / 2.0)
            end = QPointF(
                target_pos.x(),
                self._incoming_fibre_connection_y(target_id, positions, edge, cross_link=False),
            )
        if target_bus is not None and not client_link:
            bus_anchor, bus_left, bus_right = target_bus
            target_right_of_source = bus_anchor.x() >= start.x()
            required_span = self._link_label_required_span(edge)
            if target_right_of_source and failover_style:
                entry_x = self._failover_fibre_lane_x(bus_left, required_span)
            elif target_right_of_source:
                entry_x = self._primary_fibre_lane_x(bus_left, required_span)
            else:
                # Right-to-left links use mirrored, independently separated
                # lanes on the far side of the destination bus.
                primary_right = bus_right + required_span + 34.0
                entry_x = primary_right + (120.0 if failover_style else 0.0)
            bus_entry_x = bus_left if target_right_of_source else bus_right
            path = QPainterPath(start)
            path.lineTo(QPointF(entry_x, start.y()))
            path.lineTo(QPointF(entry_x, bus_anchor.y()))
            path.lineTo(QPointF(bus_entry_x, bus_anchor.y()))

            medium = edge.medium if edge else "fibre"
            standby = cross_link or self._edge_is_failover(edge)
            colour = QColor("#d68f52") if standby else self._link_colour(medium)
            pen = QPen(colour, 2.0)
            if failover_style:
                pen.setStyle(Qt.DashLine)
            path_item = QGraphicsPathItem(path)
            path_item.setPen(pen)
            path_item.setZValue(-1.0 if not failover_style else -0.8)
            self.scene.addItem(path_item)
            self._add_bus_drop(target_id, positions, colour, failover=failover_style, z_value=-0.95 if not failover_style else -0.8)
            if edge and self.show_link_labels_check.isChecked():
                self._add_link_label(edge.label, colour, QPointF((entry_x + bus_anchor.x()) / 2.0, bus_anchor.y() - (14.0 if failover_style else 12.0)))
            return
        path = QPainterPath(start)
        if failover_style:
            bend = max(60.0, abs(end.x() - start.x()) * 0.20)
            direction = -1.0 if end.x() >= start.x() else 1.0
            control_x = (start.x() + end.x()) / 2.0 + direction * bend
            path.lineTo(QPointF(control_x, start.y()))
            path.lineTo(QPointF(control_x, end.y()))
            path.lineTo(end)
        else:
            mid_x = (start.x() + end.x()) / 2.0
            path.lineTo(QPointF(mid_x, start.y()))
            path.lineTo(QPointF(mid_x, end.y()))
            path.lineTo(end)

        medium = edge.medium if edge else ("virtual" if client_link else "copper")
        colour = QColor("#d68f52") if failover_style else self._link_colour(medium)
        pen = QPen(colour, 2.0 if not client_link else 1.3)
        if failover_style or client_link:
            pen.setStyle(Qt.DashLine)
        path_item = QGraphicsPathItem(path)
        path_item.setPen(pen)
        path_item.setZValue(-1.0 if not failover_style else -0.8)
        self.scene.addItem(path_item)

        if edge and self.show_link_labels_check.isChecked() and not client_link:
            # Keep cable-length labels on a horizontal section of the
            # orthogonal topology link. Using pointAtPercent(0.50) can place
            # the label on the vertical riser when cards have different Y positions.
            if failover_style:
                first_length = abs(control_x - start.x())
                last_length = abs(end.x() - control_x)
                if last_length >= first_length:
                    label_point = QPointF((control_x + end.x()) / 2.0, end.y())
                else:
                    label_point = QPointF((start.x() + control_x) / 2.0, start.y())
            else:
                first_length = abs(mid_x - start.x())
                last_length = abs(end.x() - mid_x)
                if last_length >= first_length:
                    label_point = QPointF((mid_x + end.x()) / 2.0, end.y())
                else:
                    label_point = QPointF((start.x() + mid_x) / 2.0, start.y())
            self._add_link_label(edge.label, colour, label_point)

    def _card_activated(self, node_id: str) -> None:
        item = self.node_items.get(node_id)
        if item is not None and not item.isSelected():
            self.scene.clearSelection()
            item.setSelected(True)
        self._show_node_details(node_id)

    def _scene_selection_changed(self) -> None:
        selected = self.scene.selectedItems()
        if not selected:
            return
        item = selected[0]
        if isinstance(item, TopologyCardItem):
            self._show_node_details(item.node.node_id)

    def _rack_label(self, key: Tuple[int, str, str]) -> str:
        floor, location, rack = key
        if location and rack:
            return f"{location} / {rack}"
        if rack:
            return rack
        if location:
            return location
        return f"Floor {floor}"

    def _update_breadcrumb(self) -> None:
        if self.switch_port_focus is not None:
            node = self.model.nodes.get(self.switch_port_focus)
            label = node.name if node is not None else self.switch_port_focus
            if self.rack_focus is not None:
                self.breadcrumb_button.setText(f"Topology / {self._rack_label(self.rack_focus)} / {label} ports")
            else:
                self.breadcrumb_button.setText(f"Topology / {label} ports")
            self.breadcrumb_button.show()
            self.floor_combo.setEnabled(False)
            self.show_clients_check.setEnabled(False)
            return
        if self.rack_focus is None:
            self.breadcrumb_button.hide()
            self.floor_combo.setEnabled(True)
            self.show_clients_check.setEnabled(True)
            return
        self.breadcrumb_button.setText(f"Topology / {self._rack_label(self.rack_focus)}")
        self.breadcrumb_button.show()
        self.floor_combo.setEnabled(False)
        self.show_clients_check.setEnabled(False)

    def _exit_rack_view(self) -> None:
        if self.switch_port_focus is not None:
            self.switch_port_focus = None
            self.rebuild_scene(fit=True)
            return
        if self.rack_focus is None:
            return
        self.rack_focus = None
        self.rebuild_scene(fit=True)

    def _open_selected_rack_view(self) -> None:
        node_id = _text(self.rack_view_button.property("node_id"))
        key = self._rack_group_key(node_id)
        if key == (0, "", ""):
            return
        self.switch_port_focus = None
        self.rack_focus = key
        self.rebuild_scene(fit=True)

    def _open_selected_switch_port_view(self) -> None:
        node_id = _text(self.port_view_button.property("node_id"))
        node = self.model.nodes.get(node_id)
        if node is None or not self._supports_port_view(node):
            return
        self.switch_port_focus = node_id
        self.rebuild_scene(fit=True)

    def _card_double_clicked(self, node_id: str) -> None:
        if self.switch_port_focus is not None:
            return
        if self.rack_focus is not None:
            node = self._node_for(node_id)
            if node is not None and self._supports_port_view(node):
                self.switch_port_focus = node_id
                self.rebuild_scene(fit=True)
            return
        node = self._node_for(node_id)
        if node is not None and node.asset_type in {"client_group", "client_device"}:
            parent_id = _text(node.details.get("parent_instance_id"))
            parent = self._node_for(parent_id)
            if parent is not None:
                key = self._switch_group_key(parent_id)
                if key != (0, "", ""):
                    self.rack_focus = key
                    self.rebuild_scene(fit=True)
                    return
        if node is not None and node.asset_type == "network_switch":
            key = self._switch_group_key(node_id)
            if key != (0, "", ""):
                self.rack_focus = key
                self.rebuild_scene(fit=True)
                return
        self.toggle_branch(node_id)

    def toggle_branch(self, node_id: str) -> None:
        if not self._children_for(node_id):
            return
        if self._is_expanded(node_id):
            self.explicit_collapsed.add(node_id)
            self.explicit_expanded.discard(node_id)
        else:
            self.explicit_expanded.add(node_id)
            self.explicit_collapsed.discard(node_id)
        self.rebuild_scene(fit=False)
        item = self.node_items.get(node_id)
        if item is not None:
            item.setSelected(True)
            self.view.centerOn(item)

    def _toggle_selected_branch(self) -> None:
        node_id = _text(self.branch_button.property("node_id"))
        if node_id:
            self.toggle_branch(node_id)

    def _overview(self) -> None:
        self.explicit_expanded.clear()
        self.explicit_collapsed.clear()
        self.rebuild_scene(fit=True)

    def _apply_search_highlight(self, text: str) -> None:
        query = _text(text).lower()
        self._search_matches = []
        self._search_index = -1
        for node_id, item in self.node_items.items():
            node = item.node
            haystack = " ".join(
                (
                    node.node_id,
                    node.name,
                    node.type_label,
                    node.manufacturer,
                    node.model,
                    node.location_name,
                    node.management_ip,
                )
            ).lower()
            match = bool(query and query in haystack)
            item.set_search_match(match)
            if match:
                self._search_matches.append(node_id)
        if query and self._search_matches:
            self.status_label.setText(f"{len(self._search_matches)} visible topology matches · Press Enter to step through results")

    def _select_next_search_match(self) -> None:
        if not self._search_matches:
            return
        self._search_index = (self._search_index + 1) % len(self._search_matches)
        node_id = self._search_matches[self._search_index]
        item = self.node_items.get(node_id)
        if item is None:
            return
        self.scene.clearSelection()
        item.setSelected(True)
        self.view.focus_on_item(item)
        self._show_node_details(node_id)

    def refresh_from_data(self) -> None:
        ensure_network_schema(self.data)
        self.model = TopologyModel(self.data)
        self._floor_match_cache.clear()
        self._logical_children_cache.clear()
        self._collapsed_edge_cache.clear()
        self.explicit_expanded.clear()
        self.explicit_collapsed.clear()
        self.rebuild_scene(fit=True)


__all__ = ["NetworkTopologyDialog", "TopologyModel"]
