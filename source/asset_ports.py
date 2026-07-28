"""Typed endpoint-asset ports, connection recipes and planner demand helpers."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Mapping


ASSET_PORT_TYPES = ("network", "peripheral", "video", "power")
ASSET_PORT_TYPE_LABELS = {
    "network": "Network",
    "peripheral": "Peripheral",
    "video": "Video",
    "power": "Power",
}

_PORT_TYPE_ALIASES = {
    "data": "network",
    "ethernet": "network",
    "lan": "network",
    "network": "network",
    "usb": "peripheral",
    "serial": "peripheral",
    "control": "peripheral",
    "peripheral": "peripheral",
    "display": "video",
    "hdmi": "video",
    "video": "video",
    "mains": "power",
    "dc": "power",
    "poe": "power",
    "power": "power",
}


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _nonnegative_int(value, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return max(0, int(default))


def normalise_asset_port_type(value, default: str = "network") -> str:
    """Return a stable endpoint-port type used by the UI and planner."""
    text = _text(value).casefold().replace("_", " ").replace("-", " ")
    if not text:
        return default
    return _PORT_TYPE_ALIASES.get(text, text if text in ASSET_PORT_TYPES else default)


def clean_asset_port_definitions(
    definitions,
    direction: str,
    *,
    legacy_count: int = 0,
) -> list[dict]:
    """Normalise grouped input/output port rows.

    A definition represents one named port family and a quantity, for example
    ``Network / LAN / RJ45 / 1``.  Legacy scalar counts become one Network row.
    """
    direction = "output" if _text(direction).casefold().startswith("out") else "input"
    rows = definitions if isinstance(definitions, (list, tuple)) else []
    result: list[dict] = []
    used_ids: set[str] = set()
    for index, value in enumerate(rows, start=1):
        if not isinstance(value, Mapping):
            continue
        quantity = _nonnegative_int(
            value.get("qty", value.get("quantity", value.get("count", 1))),
            1,
        )
        if quantity <= 0:
            continue
        port_type = normalise_asset_port_type(
            value.get("port_type", value.get("type", value.get("signal_type", "")))
        )
        name = _text(value.get("name", value.get("label")))
        if not name:
            name = ASSET_PORT_TYPE_LABELS.get(port_type, port_type.title())
        port_id = _text(value.get("id", value.get("port_id")))
        if not port_id:
            port_id = f"{direction}-{port_type}-{index}"
        base_id = port_id
        suffix = 2
        while port_id in used_ids:
            port_id = f"{base_id}-{suffix}"
            suffix += 1
        used_ids.add(port_id)
        result.append(
            {
                "id": port_id,
                "name": name,
                "port_type": port_type,
                "connector_type": _text(
                    value.get(
                        "connector_type",
                        value.get("connector", value.get("interface", "")),
                    )
                ),
                "qty": quantity,
            }
        )
    if not result and legacy_count > 0:
        result.append(
            {
                "id": f"{direction}-network",
                "name": "Network",
                "port_type": "network",
                "connector_type": "RJ45",
                "qty": _nonnegative_int(legacy_count),
            }
        )
    return result


def asset_port_definitions(asset: Mapping | None, direction: str) -> list[dict]:
    """Return typed port definitions, accepting the established scalar fields."""
    asset = asset or {}
    direction = "output" if _text(direction).casefold().startswith("out") else "input"
    field = f"{direction}_port_definitions"
    alias = f"{direction}s"
    explicit = asset.get(field)
    if not isinstance(explicit, (list, tuple)):
        explicit = asset.get(alias)
    if direction == "input":
        legacy = _nonnegative_int(
            asset.get(
                "data_points",
                asset.get(
                    "input_ports",
                    asset.get("data_points_each", asset.get("cables", 1)),
                ),
            ),
            1,
        )
    else:
        legacy = _nonnegative_int(asset.get("output_ports", 0), 0)
    return clean_asset_port_definitions(explicit, direction, legacy_count=legacy)


def asset_port_count(
    asset: Mapping | None,
    direction: str,
    port_type: str | None = None,
) -> int:
    rows = asset_port_definitions(asset, direction)
    if port_type is not None:
        wanted = normalise_asset_port_type(port_type)
        rows = [row for row in rows if row["port_type"] == wanted]
    return sum(_nonnegative_int(row.get("qty")) for row in rows)


def asset_input_ports(asset: Mapping | None) -> int:
    """Return Network inputs on one asset (the legacy upstream-data count)."""
    asset = asset or {}
    has_typed = isinstance(
        asset.get("input_port_definitions"), (list, tuple)
    ) or isinstance(asset.get("inputs"), (list, tuple))
    if not has_typed and "data_points" in asset:
        return _nonnegative_int(
            asset.get(
                "data_points",
                asset.get("data_points_each", asset.get("input_ports", 1)),
            ),
            1,
        )
    return asset_port_count(asset, "input", "network")


def asset_output_ports(asset: Mapping | None) -> int:
    """Return Network outputs on one asset (the legacy daisy-chain count)."""
    return asset_port_count(asset, "output", "network")


def is_connection_asset(asset: Mapping | None) -> bool:
    """Return whether an asset is intended to be used between two ports."""
    asset = asset or {}
    if bool(asset.get("is_connection_asset", asset.get("connection_accessory", False))):
        return True
    text = " ".join(
        _text(asset.get(field)).casefold()
        for field in ("name", "Group", "group", "category_name", "asset_role")
    )
    return any(
        token in text
        for token in ("patch lead", "patch cable", "cable", "lead", "adaptor", "adapter")
    )


def set_asset_network_port_count(asset: dict, direction: str, count: int) -> dict:
    """Update the Network port total without discarding other typed ports."""
    if not isinstance(asset, dict):
        return asset
    direction = "output" if _text(direction).casefold().startswith("out") else "input"
    count = _nonnegative_int(count)
    field = f"{direction}_port_definitions"
    alias = f"{direction}s"
    has_typed = isinstance(asset.get(field), (list, tuple)) or isinstance(
        asset.get(alias), (list, tuple)
    )
    if has_typed:
        rows = asset_port_definitions(asset, direction)
        updated = []
        network_written = False
        for row in rows:
            if row["port_type"] != "network":
                updated.append(row)
                continue
            if network_written:
                continue
            network_written = True
            if count > 0:
                updated.append({**row, "qty": count})
        if not network_written and count > 0:
            updated.insert(
                0,
                {
                    "id": f"{direction}-network",
                    "name": "Network",
                    "port_type": "network",
                    "connector_type": "RJ45",
                    "qty": count,
                },
            )
        asset[field] = updated
    if direction == "input":
        asset["input_ports"] = count
        asset["data_points"] = count
    else:
        asset["output_ports"] = count
    return asset


def clean_asset_connections(connections, valid_asset_ids=None) -> list[dict]:
    """Return de-duplicated typed output-to-input connection recipes.

    ``from_asset_instance`` and ``to_asset_instance`` are optional, one-based
    occurrence numbers.  They let two copies of the same asset participate in
    different connection chains without inventing duplicate asset-library IDs.
    Older recipes without occurrence numbers keep their quantity-based
    first-available behaviour.
    """
    valid = (
        {
            _text(asset_id)
            for asset_id in valid_asset_ids
            if _text(asset_id)
        }
        if valid_asset_ids is not None
        else None
    )
    result: list[dict] = []
    by_key: dict[tuple, dict] = {}
    for value in connections or []:
        if not isinstance(value, Mapping):
            continue
        from_id = _text(
            value.get(
                "from_asset_id",
                value.get("from_asset", value.get("from", "")),
            )
        )
        to_id = _text(
            value.get(
                "to_asset_id",
                value.get("to_asset", value.get("to", "")),
            )
        )
        external_source = bool(
            value.get("external_source", value.get("untracked_source", False))
        ) or not from_id
        if (
            not to_id
            or (
                valid is not None
                and (
                    to_id not in valid
                    or (from_id and from_id not in valid)
                )
            )
        ):
            continue
        quantity = _nonnegative_int(value.get("qty", 1), 1)
        if quantity <= 0:
            continue
        port_type = normalise_asset_port_type(
            value.get("port_type", value.get("connection_type", "network"))
        )
        from_port_id = _text(
            value.get("from_output_id", value.get("from_port_id", value.get("from_port")))
        )
        to_port_id = _text(
            value.get("to_input_id", value.get("to_port_id", value.get("to_port")))
        )
        connection_asset_id = _text(
            value.get(
                "connection_asset_id",
                value.get("connector_asset_id", value.get("link_asset_id")),
            )
        )
        from_instance = _nonnegative_int(
            value.get(
                "from_asset_instance",
                value.get("from_instance", value.get("source_instance", 0)),
            )
        )
        to_instance = _nonnegative_int(
            value.get(
                "to_asset_instance",
                value.get("to_instance", value.get("target_instance", 0)),
            )
        )
        key = (
            from_id,
            from_instance,
            from_port_id,
            to_id,
            to_instance,
            to_port_id,
            port_type,
            connection_asset_id,
        )
        if key in by_key:
            by_key[key]["qty"] += quantity
            continue
        row = {
            "from_asset_id": from_id,
            "to_asset_id": to_id,
            "port_type": port_type,
            "qty": quantity,
        }
        if external_source:
            row["external_source"] = True
        if from_port_id:
            row["from_output_id"] = from_port_id
        if from_id and from_instance:
            row["from_asset_instance"] = from_instance
        if to_port_id:
            row["to_input_id"] = to_port_id
        if to_instance:
            row["to_asset_instance"] = to_instance
        if connection_asset_id:
            row["connection_asset_id"] = connection_asset_id
        by_key[key] = row
        result.append(row)
    return result


def _assignment_quantities(assignments) -> dict[str, int]:
    quantities: dict[str, int] = defaultdict(int)
    for assignment in assignments or []:
        if not isinstance(assignment, Mapping):
            continue
        asset_id = _text(assignment.get("asset_id", assignment.get("id")))
        if asset_id:
            quantities[asset_id] += _nonnegative_int(assignment.get("qty", 1), 1)
    return dict(quantities)


def room_asset_network_demands(
    assignments,
    assets_by_id: Mapping,
    connections=None,
) -> list[dict]:
    """Expand a room recipe into the Network inputs that need switch ports.

    Each result represents a true upstream network input.  Its ``asset_nodes``
    and ``asset_connections`` describe any phone/dock/display-style downstream
    chain served through that input.
    """
    quantities = _assignment_quantities(assignments)
    nodes: dict[str, dict] = {}
    inputs: list[dict] = []
    outputs: list[dict] = []
    for asset_id, quantity in quantities.items():
        asset = assets_by_id.get(asset_id, {}) or {}
        input_rows = [
            row
            for row in asset_port_definitions(asset, "input")
            if row["port_type"] == "network"
        ]
        output_rows = [
            row
            for row in asset_port_definitions(asset, "output")
            if row["port_type"] == "network"
        ]
        for instance_number in range(1, quantity + 1):
            node_id = f"{asset_id}#{instance_number}"
            nodes[node_id] = {
                "node_id": node_id,
                "asset_id": asset_id,
                "asset_name": _text(asset.get("name")) or asset_id,
                "instance_number": instance_number,
            }
            for row in input_rows:
                for ordinal in range(1, _nonnegative_int(row.get("qty")) + 1):
                    inputs.append(
                        {
                            "socket_id": f"{node_id}:in:{row['id']}:{ordinal}",
                            "node_id": node_id,
                            "asset_id": asset_id,
                            "port_id": row["id"],
                            "port_name": row["name"],
                            "port_type": "network",
                            "connector_type": row["connector_type"],
                            "ordinal": ordinal,
                        }
                    )
            for row in output_rows:
                for ordinal in range(1, _nonnegative_int(row.get("qty")) + 1):
                    outputs.append(
                        {
                            "socket_id": f"{node_id}:out:{row['id']}:{ordinal}",
                            "node_id": node_id,
                            "asset_id": asset_id,
                            "port_id": row["id"],
                            "port_name": row["name"],
                            "port_type": "network",
                            "connector_type": row["connector_type"],
                            "ordinal": ordinal,
                        }
                    )

    used_outputs: set[str] = set()
    used_inputs: set[str] = set()
    effective_links: list[dict] = []
    recipes = clean_asset_connections(connections, quantities)
    for recipe in [row for row in recipes if not row.get("external_source")]:
        if recipe["port_type"] != "network":
            continue
        from_candidates = [
            row
            for row in outputs
            if row["asset_id"] == recipe["from_asset_id"]
            and (
                not recipe.get("from_asset_instance")
                or nodes[row["node_id"]]["instance_number"]
                == recipe["from_asset_instance"]
            )
            and (
                not recipe.get("from_output_id")
                or row["port_id"] == recipe["from_output_id"]
            )
            and row["socket_id"] not in used_outputs
        ]
        to_candidates = [
            row
            for row in inputs
            if row["asset_id"] == recipe["to_asset_id"]
            and (
                not recipe.get("to_asset_instance")
                or nodes[row["node_id"]]["instance_number"]
                == recipe["to_asset_instance"]
            )
            and (
                not recipe.get("to_input_id")
                or row["port_id"] == recipe["to_input_id"]
            )
            and row["socket_id"] not in used_inputs
        ]
        pairs = min(recipe["qty"], len(from_candidates), len(to_candidates))
        connection_asset = assets_by_id.get(recipe.get("connection_asset_id", ""), {}) or {}
        for index in range(pairs):
            source = from_candidates[index]
            target = to_candidates[index]
            if source["node_id"] == target["node_id"]:
                replacement_index = next(
                    (
                        candidate_index
                        for candidate_index in range(index + 1, len(to_candidates))
                        if to_candidates[candidate_index]["node_id"] != source["node_id"]
                    ),
                    None,
                )
                if replacement_index is not None:
                    to_candidates[index], to_candidates[replacement_index] = (
                        to_candidates[replacement_index],
                        to_candidates[index],
                    )
                    target = to_candidates[index]
            used_outputs.add(source["socket_id"])
            used_inputs.add(target["socket_id"])
            effective_links.append(
                {
                    "from_socket_id": source["socket_id"],
                    "from_node_id": source["node_id"],
                    "from_asset_id": source["asset_id"],
                    "from_output_id": source["port_id"],
                    "from_output_name": source["port_name"],
                    "to_socket_id": target["socket_id"],
                    "to_node_id": target["node_id"],
                    "to_asset_id": target["asset_id"],
                    "to_input_id": target["port_id"],
                    "to_input_name": target["port_name"],
                    "port_type": "network",
                    "connection_asset_id": recipe.get("connection_asset_id", ""),
                    "connection_asset_name": _text(connection_asset.get("name")),
                }
            )

    external_by_input: dict[str, dict] = {}
    claimed_external_inputs: set[str] = set()
    for recipe in [row for row in recipes if row.get("external_source")]:
        if recipe["port_type"] != "network":
            continue
        to_candidates = [
            row
            for row in inputs
            if row["asset_id"] == recipe["to_asset_id"]
            and (
                not recipe.get("to_asset_instance")
                or nodes[row["node_id"]]["instance_number"]
                == recipe["to_asset_instance"]
            )
            and (
                not recipe.get("to_input_id")
                or row["port_id"] == recipe["to_input_id"]
            )
            and row["socket_id"] not in used_inputs
            and row["socket_id"] not in claimed_external_inputs
        ]
        connection_asset = assets_by_id.get(
            recipe.get("connection_asset_id", ""), {}
        ) or {}
        for target in to_candidates[: recipe["qty"]]:
            claimed_external_inputs.add(target["socket_id"])
            external_by_input[target["socket_id"]] = {
                "external_source": True,
                "to_node_id": target["node_id"],
                "to_asset_id": target["asset_id"],
                "to_input_id": target["port_id"],
                "to_input_name": target["port_name"],
                "port_type": "network",
                "connection_asset_id": recipe.get("connection_asset_id", ""),
                "connection_asset_name": _text(connection_asset.get("name")),
            }

    outgoing: dict[str, list[dict]] = defaultdict(list)
    incoming_nodes: set[str] = set()
    for link in effective_links:
        outgoing[link["from_node_id"]].append(link)
        incoming_nodes.add(link["to_node_id"])

    roots = [row for row in inputs if row["socket_id"] not in used_inputs]
    # A malformed circular recipe must still generate one upstream demand.
    if not roots and inputs:
        roots = [inputs[0]]

    results: list[dict] = []
    covered_nodes: set[str] = set()
    for root in roots:
        queue = deque([root["node_id"]])
        chain_nodes: list[dict] = []
        chain_links: list[dict] = []
        seen_nodes: set[str] = set()
        while queue:
            node_id = queue.popleft()
            if node_id in seen_nodes:
                continue
            seen_nodes.add(node_id)
            if node_id in nodes:
                chain_nodes.append(dict(nodes[node_id]))
            for link in outgoing.get(node_id, []):
                chain_links.append(dict(link))
                queue.append(link["to_node_id"])
        covered_nodes.update(seen_nodes)
        head = nodes.get(root["node_id"], {})
        input_port_statuses = [
            {
                "socket_id": root["socket_id"],
                "node_id": root["node_id"],
                "asset_id": root["asset_id"],
                "asset_name": _text(head.get("asset_name")) or root["asset_id"],
                "input_port_id": root["port_id"],
                "input_port_name": root["port_name"],
                "input_connector_type": root["connector_type"],
                "status": "upstream_required",
                "counted_upstream": True,
                "reason": (
                    "Requires one upstream network port; the source device is not "
                    "tracked, so the selected connection cable is still counted."
                    if root["socket_id"] in external_by_input
                    else "Requires one upstream network port."
                ),
            }
        ]
        for link in chain_links:
            target_node = nodes.get(link["to_node_id"], {})
            source_node = nodes.get(link["from_node_id"], {})
            source_label = (
                _text(source_node.get("asset_name"))
                or _text(link.get("from_asset_id"))
                or "upstream asset"
            )
            source_port = _text(link.get("from_output_name"))
            cable_label = _text(link.get("connection_asset_name"))
            served_by = " ".join(value for value in (source_label, source_port) if value)
            via_cable = f" via {cable_label}" if cable_label else ""
            input_port_statuses.append(
                {
                    "socket_id": _text(link.get("to_socket_id")),
                    "node_id": _text(link.get("to_node_id")),
                    "asset_id": _text(link.get("to_asset_id")),
                    "asset_name": (
                        _text(target_node.get("asset_name"))
                        or _text(link.get("to_asset_id"))
                    ),
                    "input_port_id": _text(link.get("to_input_id")),
                    "input_port_name": _text(link.get("to_input_name")),
                    "input_connector_type": "",
                    "status": "deferred_internal",
                    "counted_upstream": False,
                    "reason": (
                        f"Served by {served_by}{via_cable}; no separate upstream "
                        "network port is required."
                    ),
                }
            )
        results.append(
            {
                "asset_id": head.get("asset_id", root["asset_id"]),
                "asset_name": head.get("asset_name", root["asset_id"]),
                "input_port_id": root["port_id"],
                "input_port_name": root["port_name"],
                "input_connector_type": root["connector_type"],
                "upstream_connection": dict(
                    external_by_input.get(root["socket_id"], {})
                ),
                "asset_nodes": chain_nodes,
                "asset_connections": chain_links,
                "input_port_statuses": input_port_statuses,
            }
        )

    # Include disconnected cycles/components that were not reachable from a root.
    for node_id in sorted(set(nodes) - covered_nodes):
        node_inputs = [row for row in inputs if row["node_id"] == node_id]
        if not node_inputs:
            continue
        root = node_inputs[0]
        head = nodes[node_id]
        results.append(
            {
                "asset_id": head["asset_id"],
                "asset_name": head["asset_name"],
                "input_port_id": root["port_id"],
                "input_port_name": root["port_name"],
                "input_connector_type": root["connector_type"],
                "upstream_connection": dict(
                    external_by_input.get(root["socket_id"], {})
                ),
                "asset_nodes": [dict(head)],
                "asset_connections": [],
                "input_port_statuses": [
                    {
                        "socket_id": root["socket_id"],
                        "node_id": root["node_id"],
                        "asset_id": root["asset_id"],
                        "asset_name": _text(head.get("asset_name"))
                        or root["asset_id"],
                        "input_port_id": root["port_id"],
                        "input_port_name": root["port_name"],
                        "input_connector_type": root["connector_type"],
                        "status": "upstream_required",
                        "counted_upstream": True,
                        "reason": (
                            "Requires one upstream network port because the "
                            "connection recipe is circular or incomplete."
                        ),
                    }
                ],
            }
        )
    return results


def room_asset_port_summary(
    assignments,
    assets_by_id: Mapping,
    connections=None,
) -> dict:
    """Calculate typed ports, effective links and upstream Network demand."""
    quantities = _assignment_quantities(assignments)
    inputs_by_type: dict[str, int] = defaultdict(int)
    outputs_by_type: dict[str, int] = defaultdict(int)
    for asset_id, quantity in quantities.items():
        asset = assets_by_id.get(asset_id, {}) or {}
        for row in asset_port_definitions(asset, "input"):
            inputs_by_type[row["port_type"]] += quantity * _nonnegative_int(row["qty"])
        for row in asset_port_definitions(asset, "output"):
            outputs_by_type[row["port_type"]] += quantity * _nonnegative_int(row["qty"])

    input_capacity: dict[tuple[str, int, str, str], int] = defaultdict(int)
    output_capacity: dict[tuple[str, int, str, str], int] = defaultdict(int)
    for asset_id, quantity in quantities.items():
        asset = assets_by_id.get(asset_id, {}) or {}
        for instance_number in range(1, quantity + 1):
            for row in asset_port_definitions(asset, "input"):
                input_capacity[
                    (asset_id, instance_number, row["id"], row["port_type"])
                ] += _nonnegative_int(row["qty"])
            for row in asset_port_definitions(asset, "output"):
                output_capacity[
                    (asset_id, instance_number, row["id"], row["port_type"])
                ] += _nonnegative_int(row["qty"])
    effective_by_type: dict[str, int] = defaultdict(int)
    external_effective_by_type: dict[str, int] = defaultdict(int)
    effective_connection_assets: dict[str, int] = defaultdict(int)
    for recipe in clean_asset_connections(connections, quantities):
        port_type = recipe["port_type"]
        output_keys = [
            key
            for key in output_capacity
            if key[0] == recipe["from_asset_id"]
            and (
                not recipe.get("from_asset_instance")
                or key[1] == recipe["from_asset_instance"]
            )
            and key[3] == port_type
            and (
                not recipe.get("from_output_id")
                or key[2] == recipe["from_output_id"]
            )
        ]
        input_keys = [
            key
            for key in input_capacity
            if key[0] == recipe["to_asset_id"]
            and (
                not recipe.get("to_asset_instance")
                or key[1] == recipe["to_asset_instance"]
            )
            and key[3] == port_type
            and (
                not recipe.get("to_input_id")
                or key[2] == recipe["to_input_id"]
            )
        ]
        available_outputs = (
            recipe["qty"]
            if recipe.get("external_source")
            else sum(output_capacity[key] for key in output_keys)
        )
        available_inputs = sum(input_capacity[key] for key in input_keys)
        effective = min(recipe["qty"], available_outputs, available_inputs)
        if effective <= 0:
            continue
        effective_by_type[port_type] += effective
        connection_asset_id = _text(recipe.get("connection_asset_id"))
        if connection_asset_id:
            effective_connection_assets[connection_asset_id] += effective
        if recipe.get("external_source"):
            external_effective_by_type[port_type] += effective
        remaining = effective
        if not recipe.get("external_source"):
            for key in output_keys:
                used = min(remaining, output_capacity[key])
                output_capacity[key] -= used
                remaining -= used
                if remaining <= 0:
                    break
        remaining = effective
        for key in input_keys:
            used = min(remaining, input_capacity[key])
            input_capacity[key] -= used
            remaining -= used
            if remaining <= 0:
                break

    network_demands = room_asset_network_demands(assignments, assets_by_id, connections)
    upstream_ports = len(network_demands)
    network_input_ports: list[dict] = []
    seen_network_input_sockets: set[str] = set()
    for demand in network_demands:
        for status in demand.get("input_port_statuses", []):
            if not isinstance(status, dict):
                continue
            socket_id = _text(status.get("socket_id"))
            if socket_id and socket_id in seen_network_input_sockets:
                continue
            if socket_id:
                seen_network_input_sockets.add(socket_id)
            network_input_ports.append(dict(status))
    network_inputs = inputs_by_type.get("network", 0)
    effective_network_links = min(
        effective_by_type.get("network", 0),
        max(0, network_inputs - upstream_ports),
    )
    connection_assets = dict(effective_connection_assets)

    return {
        # Established keys remain Network-specific for compatibility.
        "input_ports": network_inputs,
        "output_ports": outputs_by_type.get("network", 0),
        "daisy_chain_links": effective_network_links,
        "upstream_ports": upstream_ports,
        # New typed detail is available to dialogs, reports and planners.
        "inputs_by_type": {
            port_type: inputs_by_type.get(port_type, 0)
            for port_type in ASSET_PORT_TYPES
        },
        "outputs_by_type": {
            port_type: outputs_by_type.get(port_type, 0)
            for port_type in ASSET_PORT_TYPES
        },
        "connections_by_type": {
            port_type: (
                effective_network_links
                + external_effective_by_type.get("network", 0)
                if port_type == "network"
                else effective_by_type.get(port_type, 0)
            )
            for port_type in ASSET_PORT_TYPES
        },
        "connection_assets": connection_assets,
        "network_demands": network_demands,
        # Every physical Network input remains visible. Internally served ports
        # are explicitly deferred instead of silently disappearing from demand.
        "network_input_ports": network_input_ports,
    }
