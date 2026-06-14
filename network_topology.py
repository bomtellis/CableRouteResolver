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
                length_m=max(0.0, _float(connection.get("length_m"))),
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
                port_capacity=max(0, _int(asset.get("number_of_ports"))) * stack_members,
                ports_used=len(assignments),
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
        unvisited = set(self.nodes)
        while unvisited:
            seed = min(unvisited, key=lambda node_id: self._node_root_key(node_id))
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
            while remaining:
                root = min(remaining, key=lambda node_id: self._node_root_key(node_id))
                self.roots.append(root)
                self.level[root] = 0
                bfs = deque([root])
                discovered.add(root)
                remaining.discard(root)
                while bfs:
                    current = bfs.popleft()
                    for neighbour, edge in sorted(self.adjacency.get(current, []), key=self._edge_sort_key):
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
        if len(self.roots) <= 1:
            return
        site_id = "topology::installation"
        project_name = _text(self.data.get("project", {}).get("name")) or "Network installation"
        original_roots = list(self.roots)
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
        return [edge for edge in self.edges if edge.edge_id not in self.tree_edge_ids]


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
        return max(self.HEIGHT, self.STACK_HEADER_H + visible_members * self.STACK_MEMBER_H + 34.0)

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
        if self.has_children:
            self.branchToggleRequested.emit(self.node.node_id)
            event.accept()
            return
        super().mouseDoubleClickEvent(event)


class LinkLabelItem(QGraphicsObject):
    def __init__(self, text: str, colour: QColor, parent: Optional[QGraphicsItem] = None):
        super().__init__(parent)
        self.text = text
        self.colour = colour
        self.font = QFont("Arial", 7)
        metrics = QFontMetrics(self.font)
        self._width = min(160.0, max(42.0, float(metrics.horizontalAdvance(text) + 14)))
        self._height = 18.0
        self.setZValue(-0.25)

    def boundingRect(self) -> QRectF:
        return QRectF(-self._width / 2.0, -self._height / 2.0, self._width, self._height)

    def paint(self, painter: QPainter, option, widget=None) -> None:  # noqa: ANN001
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setPen(QPen(QColor("#38434d"), 1.0))
        painter.setBrush(QColor("#182028"))
        painter.drawRoundedRect(self.boundingRect(), 8.0, 8.0)
        painter.setFont(self.font)
        painter.setPen(self.colour.lighter(145))
        painter.drawText(self.boundingRect(), Qt.AlignCenter, QFontMetrics(self.font).elidedText(self.text, Qt.ElideRight, int(self._width - 10)))


class TopologyGraphicsView(QGraphicsView):
    nodeSelected = Signal(str)
    branchToggleRequested = Signal(str)

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)

        self.setRenderHints(
            QPainter.Antialiasing
            | QPainter.TextAntialiasing
            | QPainter.SmoothPixmapTransform
        )

        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        self.setOptimizationFlag(QGraphicsView.DontSavePainterState, True)

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
            event.accept()
            return

        super().mouseReleaseEvent(event)

    def leaveEvent(self, event) -> None:  # noqa: ANN001
        if self._panning:
            self._panning = False
            self._pan_button = Qt.NoButton
            self.viewport().unsetCursor()

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
        self.show_clients_check.toggled.connect(lambda _checked: self.rebuild_scene(fit=False))
        layout.addWidget(self.show_clients_check)

        self.show_redundant_check = QCheckBox("Failover links")
        self.show_redundant_check.setChecked(True)
        self.show_redundant_check.toggled.connect(lambda _checked: self.rebuild_scene(fit=False))
        layout.addWidget(self.show_redundant_check)

        self.group_floors_check = QCheckBox("Group floors")
        self.group_floors_check.setToolTip("Group visible locations by floor level before location and rack groups")
        self.group_floors_check.setChecked(True)
        self.group_floors_check.toggled.connect(lambda _checked: self.rebuild_scene(fit=False))
        layout.addWidget(self.group_floors_check)

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

        self.branch_button = QPushButton("Expand branch")
        self.branch_button.setEnabled(False)
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
            self._detail_row("Power feed", _text(node.instance.get("power_feed")))
            self._detail_row("UPS source", _text(node.instance.get("ups_source")))
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
        self.branch_button.setEnabled(has_children)
        self.branch_button.setText("Collapse branch" if self._is_expanded(node.node_id) else "Expand branch")
        self.branch_button.setProperty("node_id", node.node_id)

    def _selected_floor(self) -> Optional[int]:
        return self.floor_combo.currentData()

    def _node_matches_floor(self, node_id: str, floor: Optional[int]) -> bool:
        if floor is None:
            return True
        node = self.model.nodes.get(node_id)
        if node is None:
            return True
        if node.floor == floor:
            return True
        # Keep ancestors visible when any descendant is on the selected floor.
        return any(self._node_matches_floor(child_id, floor) for child_id in self.model.children.get(node_id, []))

    def _children_for(self, node_id: str) -> List[str]:
        children = list(self.model.children.get(node_id, []))
        if self.show_clients_check.isChecked():
            children.extend(node.node_id for node in self.model.client_groups.get(node_id, []))
        return children

    def _node_for(self, node_id: str) -> Optional[TopologyNode]:
        if node_id in self.model.nodes:
            return self.model.nodes[node_id]
        if node_id.startswith("client::"):
            parent_id = node_id.split("::", 2)[1] if "::" in node_id else ""
            for node in self.model.client_groups.get(parent_id, []):
                if node.node_id == node_id:
                    return node
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
            if node.asset_type in {"firewall", "network_router"}:
                return True
            if "core" in node.role or "distribution" in node.role:
                return True
            if node.asset_type == "optical_line_terminal" or node.role.startswith("olt_"):
                return True
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
        floor = self._selected_floor()
        visible: List[str] = []
        self.visible_parent.clear()
        self.visible_parent_edge.clear()

        def visit(node_id: str, parent_id: str = "", parent_edge: str = "") -> None:
            node = self._node_for(node_id)
            if node is None:
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
                edge_id = self.model.parent_edge.get(child_id, "")
                visit(child_id, node_id, edge_id)

        for root_id in self.model.roots:
            visit(root_id)
        return visible

    def _layout_visible(self, visible_ids: Sequence[str]) -> Dict[str, QPointF]:
        visible_set = set(visible_ids)
        widths: Dict[str, float] = {}

        def card_height(node_id: str) -> float:
            node = self._node_for(node_id)
            if node is None:
                return self.CARD_H
            members = max(1, _int(node.instance.get("stack_member_count"), 1)) if bool(node.instance.get("logical_stack")) else 1
            if members <= 1:
                return self.CARD_H
            return max(self.CARD_H, TopologyCardItem.STACK_HEADER_H + min(members, 8) * TopologyCardItem.STACK_MEMBER_H + 34.0)

        def visible_children(node_id: str) -> List[str]:
            return [child_id for child_id in self._children_for(node_id) if child_id in visible_set and self.visible_parent.get(child_id) == node_id]

        def measure(node_id: str) -> float:
            children = visible_children(node_id)
            if not children:
                widths[node_id] = self.CARD_W
                return self.CARD_W
            child_widths = [
                measure(child_id)
                for child_id in children
            ]

            # Add slightly more spacing when a device has a large number of children,
            # such as an OLT, splitter or core switch.
            branch_gap = self.X_GAP

            if len(children) >= 4:
                branch_gap *= 1.25

            if len(children) >= 8:
                branch_gap *= 1.20

            total = (
                sum(child_widths)
                + branch_gap * max(0, len(children) - 1)
            )
            widths[node_id] = max(self.CARD_W, total)
            return widths[node_id]

        roots = [root_id for root_id in self.model.roots if root_id in visible_set]
        for root_id in roots:
            measure(root_id)

        positions: Dict[str, QPointF] = {}

        def place(node_id: str, left: float, top_y: float) -> None:
            width = widths[node_id]
            x = left + (width - self.CARD_W) / 2.0
            positions[node_id] = QPointF(x, top_y)
            children = visible_children(node_id)
            child_top = top_y + card_height(node_id) + self.Y_GAP
            cursor = left
            branch_gap = self.X_GAP

            if len(children) >= 4:
                branch_gap *= 1.25

            if len(children) >= 8:
                branch_gap *= 1.20

            for child_id in children:
                place(child_id, cursor, child_top)
                cursor += widths[child_id] + branch_gap

        cursor = 0.0
        for root_id in roots:
            place(root_id, cursor, 0.0)
            cursor += widths[root_id] + self.ROOT_GAP
        if self.group_floors_check.isChecked():
            self._pack_floor_location_groups(visible_ids, positions)
        else:
            self._stack_same_rack_logical_stacks(visible_ids, positions)
            self._separate_overlapping_cards(visible_ids, positions)
        self._recenter_group_nodes(visible_ids, positions)
        return positions

    def _pack_floor_location_groups(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        grouped: Dict[int, Dict[int, Dict[Tuple[str, str], List[str]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        pseudo_ids: List[str] = []
        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None or node_id not in positions:
                continue
            if node.pseudo:
                pseudo_ids.append(node_id)
                continue
            rack = _text(node.instance.get("rack_name"))
            location = node.location_name or f"Floor {node.floor}"
            rank = self.model._hierarchy_rank(node_id)
            grouped[node.floor][rank][(location, rack)].append(node_id)

        if not grouped:
            return

        rank_gap = 300.0
        group_gap_y = 90.0
        floor_gap = 280.0
        floor_tops: Dict[int, float] = {}
        all_ranks = sorted({rank for rank_groups in grouped.values() for rank in rank_groups})
        column_widths: Dict[int, float] = {}
        for rank in all_ranks:
            max_width = self.CARD_W
            for rank_groups in grouped.values():
                for (_location, _rack), node_ids in rank_groups.get(rank, {}).items():
                    group_width = len(node_ids) * self.CARD_W + max(0, len(node_ids) - 1) * self.X_GAP
                    max_width = max(max_width, group_width)
            column_widths[rank] = max_width

        column_lefts: Dict[int, float] = {}
        cursor_x = 0.0
        for rank in all_ranks:
            column_lefts[rank] = cursor_x
            cursor_x += column_widths[rank] + rank_gap

        cursor_y = 0.0

        for floor in sorted(grouped, reverse=True):
            floor_tops[floor] = cursor_y
            floor_height = self.CARD_H
            for rank in all_ranks:
                group_top = cursor_y
                floor_groups = sorted(
                    grouped[floor].get(rank, {}).items(),
                    key=lambda item: (item[0][0].lower(), item[0][1].lower()),
                )
                for (_location, _rack), node_ids in floor_groups:
                    ordered = sorted(
                        node_ids,
                        key=lambda node_id: (
                            0 if bool(self._node_for(node_id).instance.get("logical_stack")) else 1,
                            self._node_for(node_id).name.lower(),
                        ),
                    )
                    group_width = len(ordered) * self.CARD_W + max(0, len(ordered) - 1) * self.X_GAP
                    group_left = column_lefts[rank] + max(0.0, (column_widths[rank] - group_width) / 2.0)
                    group_height = max(self._node_card_height(node_id) for node_id in ordered)
                    for index, node_id in enumerate(ordered):
                        positions[node_id] = QPointF(group_left + index * (self.CARD_W + self.X_GAP), group_top)
                    group_top += group_height + group_gap_y
                floor_height = max(floor_height, group_top - cursor_y - group_gap_y)
            cursor_y += floor_height + floor_gap

        if pseudo_ids:
            first_floor_top = min(floor_tops.values())
            non_pseudo = [
                node_id
                for rank_groups in grouped.values()
                for floor_groups in rank_groups.values()
                for nodes in floor_groups.values()
                for node_id in nodes
            ]
            if non_pseudo:
                left = min(positions[node_id].x() for node_id in non_pseudo)
                for index, node_id in enumerate(pseudo_ids):
                    positions[node_id] = QPointF(left - self.CARD_W - rank_gap, first_floor_top + index * (self.CARD_H + 90.0))

    def _arrange_floor_rows(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        if not self.group_floors_check.isChecked() or self._selected_floor() is not None:
            return

        floor_rects: Dict[int, QRectF] = {}
        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None or node.pseudo or node_id not in positions:
                continue
            rect = self._card_rect(node_id, positions)
            floor_rects[node.floor] = rect if node.floor not in floor_rects else floor_rects[node.floor].united(rect)

        if len(floor_rects) < 2:
            return

        row_gap = 220.0
        row_tops: Dict[int, float] = {}
        cursor_y = 0.0
        for floor in sorted(floor_rects, reverse=True):
            rect = floor_rects[floor]
            row_tops[floor] = cursor_y
            cursor_y += rect.height() + row_gap

        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None or node.pseudo or node_id not in positions:
                continue
            rect = floor_rects[node.floor]
            pos = positions[node_id]
            positions[node_id] = QPointF(pos.x(), row_tops[node.floor] + (pos.y() - rect.top()))

    def _wrap_floor_rows(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        if not self.group_floors_check.isChecked():
            return

        grouped: Dict[int, List[str]] = defaultdict(list)
        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None or node.pseudo or node_id not in positions:
                continue
            grouped[node.floor].append(node_id)

        row_gap = 80.0
        for floor, node_ids in grouped.items():
            rect: Optional[QRectF] = None
            for node_id in node_ids:
                card_rect = self._card_rect(node_id, positions)
                rect = card_rect if rect is None else rect.united(card_rect)
            if rect is None or rect.width() <= self.FLOOR_ROW_MAX_WIDTH:
                continue

            ordered = sorted(node_ids, key=lambda node_id: (positions[node_id].x(), positions[node_id].y()))
            base_left = rect.left()
            row_top = rect.top()
            row_height = 0.0
            cursor_x = base_left
            for node_id in ordered:
                pos = positions[node_id]
                if cursor_x > base_left and cursor_x + self.CARD_W - base_left > self.FLOOR_ROW_MAX_WIDTH:
                    cursor_x = base_left
                    row_top += row_height + row_gap
                    row_height = 0.0
                positions[node_id] = QPointF(cursor_x, row_top + (pos.y() - rect.top()))
                cursor_x += self.CARD_W + self.X_GAP
                row_height = max(row_height, self._node_card_height(node_id) + (pos.y() - rect.top()))

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
            if node is None or not node.pseudo or node_id not in positions:
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

        self._add_location_groups(visible_ids, positions)

        # Tree links first so cards sit above them.
        children_by_parent: Dict[str, List[str]] = defaultdict(list)
        for child_id, parent_id in self.visible_parent.items():
            children_by_parent[parent_id].append(child_id)

        for parent_id, child_ids in children_by_parent.items():
            if len(child_ids) >= 2:
                self._add_link_rail(parent_id, child_ids, positions)
                continue
            child_id = child_ids[0]
            edge = None if child_id.startswith("client::") else self.model.edges_by_id.get(self.visible_parent_edge.get(child_id, ""))
            self._add_link(parent_id, child_id, positions, edge, client_link=child_id.startswith("client::"))

        if self.show_redundant_check.isChecked():
            cross_by_source: Dict[str, List[Tuple[str, TopologyEdge]]] = defaultdict(list)
            for edge in self.model.cross_edges():
                if edge.source_id in positions and edge.target_id in positions:
                    source_id, target_id = self._cross_link_origin_target(edge)
                    cross_by_source[source_id].append((target_id, edge))
            for source_id, targets in cross_by_source.items():
                if len(targets) >= 2:
                    self._add_cross_link_rail(source_id, targets, positions)
                    continue
                target_id, edge = targets[0]
                self._add_link(source_id, target_id, positions, edge, cross_link=True)

        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None:
                continue
            self.visible_nodes[node_id] = node
            children = self._children_for(node_id)
            has_children = bool(children)
            expanded = self._is_expanded(node_id)
            hidden = 0 if expanded else self._hidden_descendants(node_id)
            item = TopologyCardItem(node, hidden, has_children, expanded)
            item.setPos(positions[node_id])
            item.setZValue(1.0)
            item.activated.connect(self._card_activated)
            item.branchToggleRequested.connect(self.toggle_branch)
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

        self.status_label.setText(
            f"Showing {sum(1 for node in self.visible_nodes.values() if not node.pseudo):,} of "
            f"{sum(1 for node in self.model.nodes.values() if not node.pseudo):,} network assets"
            + (f" on Floor {self._selected_floor()}" if self._selected_floor() is not None else "")
            + " · Double-click a device to expand or collapse its branch · Drag to pan · Wheel to zoom"
        )
        if fit:
            if self.isVisible() and self.view.viewport().width() > 0 and self.view.viewport().height() > 0:
                self._schedule_fit_topology()
            else:
                self._fit_after_show = True

    def _schedule_fit_topology(self) -> None:
        QTimer.singleShot(0, self.view.fit_topology)

    def _node_card_height(self, node_id: str) -> float:
        node = self._node_for(node_id)
        if node is None:
            return self.CARD_H
        members = max(1, _int(node.instance.get("stack_member_count"), 1)) if bool(node.instance.get("logical_stack")) else 1
        if members <= 1:
            return self.CARD_H
        return max(self.CARD_H, TopologyCardItem.STACK_HEADER_H + min(members, 8) * TopologyCardItem.STACK_MEMBER_H + 34.0)

    def _card_rect(self, node_id: str, positions: Dict[str, QPointF]) -> QRectF:
        pos = positions[node_id]
        return QRectF(pos.x(), pos.y(), self.CARD_W, self._node_card_height(node_id))

    def _switch_group_key(self, node_id: str) -> Tuple[int, str, str]:
        node = self._node_for(node_id)
        if node is None or node.asset_type != "network_switch":
            return (0, "", "")
        rack = _text(node.instance.get("rack_name"))
        location = node.location_name
        if not location and not rack:
            return (0, "", "")
        return (node.floor if self.group_floors_check.isChecked() else 0, location, rack)

    def _add_location_groups(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        if self.group_floors_check.isChecked():
            self._add_floor_groups(visible_ids, positions)

        grouped: Dict[Tuple[int, str, str], List[str]] = defaultdict(list)
        for node_id in visible_ids:
            key = self._switch_group_key(node_id)
            if key != (0, "", ""):
                grouped[key].append(node_id)

        for (_floor, location, rack), node_ids in grouped.items():
            if len(node_ids) < 2:
                continue
            rect: Optional[QRectF] = None
            for node_id in node_ids:
                if node_id not in positions:
                    continue
                card_rect = self._card_rect(node_id, positions)
                rect = card_rect if rect is None else rect.united(card_rect)
            if rect is None:
                continue
            rect = rect.adjusted(-20.0, -34.0, 20.0, 22.0)
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
            label = QGraphicsSimpleTextItem(label_text)
            label.setFont(QFont("Arial", 8))
            label.setBrush(QBrush(QColor("#9fb0bd")))
            label.setPos(rect.left() + 10.0, rect.top() + 8.0)
            label.setZValue(-1.4)
            self.scene.addItem(label)

    def _add_floor_groups(self, visible_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        grouped: Dict[int, List[str]] = defaultdict(list)
        for node_id in visible_ids:
            node = self._node_for(node_id)
            if node is None or node.pseudo:
                continue
            if node_id in positions:
                grouped[node.floor].append(node_id)

        for floor, node_ids in grouped.items():
            if len(node_ids) < 2:
                continue
            rect: Optional[QRectF] = None
            for node_id in node_ids:
                if node_id not in positions:
                    continue
                card_rect = self._card_rect(node_id, positions)
                rect = card_rect if rect is None else rect.united(card_rect)
            if rect is None:
                continue
            rect = rect.adjusted(-42.0, -58.0, 42.0, 42.0)
            path = QPainterPath()
            path.addRoundedRect(rect, 14.0, 14.0)
            item = QGraphicsPathItem(path)
            item.setBrush(QColor(20, 29, 37, 115))
            item.setPen(QPen(QColor("#2d3b47"), 1.2))
            item.setZValue(-2.0)
            self.scene.addItem(item)

            label = QGraphicsSimpleTextItem(f"Floor {floor}")
            label.setFont(QFont("Arial", 9))
            label.setBrush(QBrush(QColor("#b9c6cf")))
            label.setPos(rect.left() + 12.0, rect.top() + 10.0)
            label.setZValue(-1.9)
            self.scene.addItem(label)

    def _link_colour(self, medium: str) -> QColor:
        return {
            "fibre": QColor("#6f8dff"),
            "copper": QColor("#4fbfa3"),
            "wireless": QColor("#e3a34e"),
            "virtual": QColor("#6f7b86"),
            "stacking": QColor("#b779e3"),
            "none": QColor("#6f7b86"),
        }.get(medium, QColor("#6f7b86"))

    def _add_link_label(self, text: str, colour: QColor, point: QPointF) -> None:
        if not text:
            return
        label = LinkLabelItem(text, colour)
        label.setPos(point)
        self.scene.addItem(label)

    def _add_link_rail(self, source_id: str, child_ids: Sequence[str], positions: Dict[str, QPointF]) -> None:
        if source_id not in positions:
            return
        source_pos = positions[source_id]
        start = QPointF(source_pos.x() + self.CARD_W / 2.0, source_pos.y() + self._node_card_height(source_id))
        endpoints = []
        for child_id in child_ids:
            target_pos = positions.get(child_id)
            if target_pos is None:
                continue
            edge = None if child_id.startswith("client::") else self.model.edges_by_id.get(self.visible_parent_edge.get(child_id, ""))
            endpoints.append((child_id, QPointF(target_pos.x() + self.CARD_W / 2.0, target_pos.y()), edge))
        if len(endpoints) < 2:
            if endpoints:
                child_id, _end, edge = endpoints[0]
                self._add_link(source_id, child_id, positions, edge, client_link=child_id.startswith("client::"))
            return

        if max(point.x() for _child_id, point, _edge in endpoints) - min(point.x() for _child_id, point, _edge in endpoints) < 8.0:
            self._add_vertical_link_rail(start, endpoints, QColor("#60717f"), z_value=-1.0)
            return

        rail_y = min(point.y() for _child_id, point, _edge in endpoints) - 32.0
        rail_left = min(point.x() for _child_id, point, _edge in endpoints)
        rail_right = max(point.x() for _child_id, point, _edge in endpoints)
        trunk_colour = QColor("#60717f")
        trunk_pen = QPen(trunk_colour, 2.0)

        trunk = QPainterPath(start)
        trunk.lineTo(QPointF(start.x(), rail_y))
        if rail_left != rail_right:
            trunk.moveTo(QPointF(rail_left, rail_y))
            trunk.lineTo(QPointF(rail_right, rail_y))
        trunk_item = QGraphicsPathItem(trunk)
        trunk_item.setPen(trunk_pen)
        trunk_item.setZValue(-1.0)
        self.scene.addItem(trunk_item)

        for child_id, end, edge in endpoints:
            client_link = child_id.startswith("client::")
            medium = edge.medium if edge else ("virtual" if client_link else "copper")
            colour = self._link_colour(medium)
            standby = bool(edge and (edge.standby or edge.redundancy_role.lower() in {"secondary", "standby"}))
            pen = QPen(colour, 2.0 if not client_link else 1.3)
            if standby or client_link:
                pen.setStyle(Qt.DashLine)
            branch = QPainterPath(QPointF(end.x(), rail_y))
            branch.lineTo(end)
            branch_item = QGraphicsPathItem(branch)
            branch_item.setPen(pen)
            branch_item.setZValue(-0.95)
            self.scene.addItem(branch_item)
            if edge and self.show_link_labels_check.isChecked() and not client_link:
                self._add_link_label(edge.label, colour, QPointF(end.x(), (rail_y + end.y()) / 2.0))

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
            colour = QColor("#d68f52") if cross_link else self._link_colour(medium)
            standby = bool(edge and (edge.standby or edge.redundancy_role.lower() in {"secondary", "standby"}))
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

    def _cross_link_origin_target(self, edge: TopologyEdge) -> Tuple[str, str]:
        source_rank = self.model._hierarchy_rank(edge.source_id)
        target_rank = self.model._hierarchy_rank(edge.target_id)
        if target_rank < source_rank:
            return edge.target_id, edge.source_id
        return edge.source_id, edge.target_id

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
            endpoints.append((target_id, QPointF(target_pos.x() + self.CARD_W / 2.0, target_pos.y() + self._node_card_height(target_id) / 2.0), edge))
        if len(endpoints) < 2:
            if endpoints:
                _target_id, _end, edge = endpoints[0]
                self._add_link(*self._cross_link_origin_target(edge), positions, edge, cross_link=True)
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
        if cross_link:
            source_center = QPointF(source_pos.x() + self.CARD_W / 2.0, source_pos.y() + self._node_card_height(source_id) / 2.0)
            target_center = QPointF(target_pos.x() + self.CARD_W / 2.0, target_pos.y() + self._node_card_height(target_id) / 2.0)
            if target_center.x() >= source_center.x():
                start = QPointF(source_pos.x() + self.CARD_W + 8.0, source_center.y())
                end = QPointF(target_pos.x() - 8.0, target_center.y())
            else:
                start = QPointF(source_pos.x() - 8.0, source_center.y())
                end = QPointF(target_pos.x() + self.CARD_W + 8.0, target_center.y())
        else:
            start = QPointF(source_pos.x() + self.CARD_W / 2.0, source_pos.y() + self._node_card_height(source_id))
            end = QPointF(target_pos.x() + self.CARD_W / 2.0, target_pos.y())
        mid_y = (start.y() + end.y()) / 2.0
        path = QPainterPath(start)
        if cross_link:
            bend = max(60.0, abs(end.x() - start.x()) * 0.20)
            direction = -1.0 if end.x() >= start.x() else 1.0
            control_x = (start.x() + end.x()) / 2.0 + direction * bend
            path.cubicTo(QPointF(control_x, start.y()), QPointF(control_x, end.y()), end)
        else:
            path.cubicTo(QPointF(start.x(), mid_y), QPointF(end.x(), mid_y), end)

        medium = edge.medium if edge else ("virtual" if client_link else "copper")
        colour = QColor("#d68f52") if cross_link else self._link_colour(medium)
        standby = bool(edge and (edge.standby or edge.redundancy_role.lower() in {"secondary", "standby"}))
        pen = QPen(colour, 2.0 if not client_link else 1.3)
        if standby or cross_link or client_link:
            pen.setStyle(Qt.DashLine)
        path_item = QGraphicsPathItem(path)
        path_item.setPen(pen)
        path_item.setZValue(-1.0 if not cross_link else -0.8)
        self.scene.addItem(path_item)

        if edge and self.show_link_labels_check.isChecked() and not client_link:
            point = path.pointAtPercent(0.50)
            self._add_link_label(edge.label, colour, point)

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
        self.view.centerOn(item)
        self._show_node_details(node_id)

    def refresh_from_data(self) -> None:
        ensure_network_schema(self.data)
        self.model = TopologyModel(self.data)
        self.explicit_expanded.clear()
        self.explicit_collapsed.clear()
        self.rebuild_scene(fit=True)


__all__ = ["NetworkTopologyDialog", "TopologyModel"]
