"""Endpoint-asset port helpers, including daisy-chain demand calculation."""

from __future__ import annotations

from typing import Mapping


def _nonnegative_int(value, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return max(0, int(default))


def asset_input_ports(asset: Mapping | None) -> int:
    """Return the input ports on one asset, accepting legacy data-point fields."""
    asset = asset or {}
    return _nonnegative_int(
        asset.get(
            "input_ports",
            asset.get(
                "data_points",
                asset.get("data_points_each", asset.get("cables", 1)),
            ),
        ),
        1,
    )


def asset_output_ports(asset: Mapping | None) -> int:
    """Return the downstream output ports on one asset."""
    asset = asset or {}
    return _nonnegative_int(asset.get("output_ports", 0), 0)


def clean_asset_connections(connections, valid_asset_ids=None) -> list[dict]:
    """Return de-duplicated output-to-input connection rows."""
    valid = (
        {
            str(asset_id or "").strip()
            for asset_id in valid_asset_ids
            if str(asset_id or "").strip()
        }
        if valid_asset_ids is not None
        else None
    )
    result = []
    by_pair = {}
    for value in connections or []:
        if not isinstance(value, Mapping):
            continue
        from_id = str(
            value.get(
                "from_asset_id",
                value.get("from_asset", value.get("from", "")),
            )
            or ""
        ).strip()
        to_id = str(
            value.get(
                "to_asset_id",
                value.get("to_asset", value.get("to", "")),
            )
            or ""
        ).strip()
        if (
            not from_id
            or not to_id
            or (valid is not None and (from_id not in valid or to_id not in valid))
        ):
            continue
        quantity = _nonnegative_int(value.get("qty", 1), 1)
        if quantity <= 0:
            continue
        pair = (from_id, to_id)
        if pair in by_pair:
            by_pair[pair]["qty"] += quantity
            continue
        row = {
            "from_asset_id": from_id,
            "to_asset_id": to_id,
            "qty": quantity,
        }
        by_pair[pair] = row
        result.append(row)
    return result


def room_asset_port_summary(
    assignments,
    assets_by_id: Mapping,
    connections=None,
) -> dict:
    """Calculate input, output and upstream port totals for one room.

    Every valid explicit output-to-input connection removes one upstream port.
    Connections are limited by the assigned asset quantities and their declared
    capacities. Each connected component retains at least one chain head, so a
    circular definition can never reduce its upstream demand to zero.
    """
    total_inputs = 0
    total_outputs = 0
    assignment_qty = {}
    inputs_by_id = {}
    input_each_by_id = {}
    output_capacity_by_id = {}

    for assignment in assignments or []:
        if not isinstance(assignment, Mapping):
            continue
        asset_id = str(
            assignment.get("asset_id", assignment.get("id", "")) or ""
        ).strip()
        asset = assets_by_id.get(asset_id, {}) if asset_id else {}
        quantity = _nonnegative_int(assignment.get("qty", 1), 1)
        inputs_each = asset_input_ports(asset)
        outputs_each = asset_output_ports(asset)
        inputs = quantity * inputs_each
        outputs = quantity * outputs_each
        total_inputs += inputs
        total_outputs += outputs
        assignment_qty[asset_id] = assignment_qty.get(asset_id, 0) + quantity
        inputs_by_id[asset_id] = inputs_by_id.get(asset_id, 0) + inputs
        input_each_by_id[asset_id] = inputs_each

        connection_type = str(
            asset.get(
                "connection_type",
                asset.get("type_of_connection", "wired"),
            )
            or "wired"
        ).strip().casefold()
        if connection_type == "wired" and inputs_each > 0 and outputs_each > 0:
            output_capacity_by_id[asset_id] = (
                output_capacity_by_id.get(asset_id, 0) + outputs
            )

    remaining_outputs = dict(output_capacity_by_id)
    remaining_inputs = dict(inputs_by_id)
    effective_connections = []
    adjacency = {}
    for connection in clean_asset_connections(
        connections,
        assignment_qty,
    ):
        from_id = connection["from_asset_id"]
        to_id = connection["to_asset_id"]
        from_asset = assets_by_id.get(from_id, {})
        to_asset = assets_by_id.get(to_id, {})
        from_type = str(
            from_asset.get(
                "connection_type",
                from_asset.get("type_of_connection", "wired"),
            )
            or "wired"
        ).strip().casefold()
        to_type = str(
            to_asset.get(
                "connection_type",
                to_asset.get("type_of_connection", "wired"),
            )
            or "wired"
        ).strip().casefold()
        if from_type != "wired" or to_type != "wired":
            continue
        effective = min(
            connection["qty"],
            remaining_outputs.get(from_id, 0),
            remaining_inputs.get(to_id, 0),
        )
        if effective <= 0:
            continue
        remaining_outputs[from_id] -= effective
        remaining_inputs[to_id] -= effective
        effective_connections.append((from_id, to_id, effective))
        adjacency.setdefault(from_id, set()).add(to_id)
        adjacency.setdefault(to_id, set()).add(from_id)

    connected_ids = set(adjacency)
    upstream_ports = sum(
        inputs
        for asset_id, inputs in inputs_by_id.items()
        if asset_id not in connected_ids
    )
    seen = set()
    for start in connected_ids:
        if start in seen:
            continue
        stack = [start]
        component = set()
        while stack:
            asset_id = stack.pop()
            if asset_id in component:
                continue
            component.add(asset_id)
            stack.extend(adjacency.get(asset_id, ()))
        seen.update(component)
        component_inputs = sum(inputs_by_id.get(asset_id, 0) for asset_id in component)
        component_links = sum(
            quantity
            for from_id, to_id, quantity in effective_connections
            if from_id in component and to_id in component
        )
        chain_head_inputs = min(
            (
                input_each_by_id.get(asset_id, 0)
                for asset_id in component
                if input_each_by_id.get(asset_id, 0) > 0
            ),
            default=0,
        )
        upstream_ports += max(
            chain_head_inputs,
            component_inputs - component_links,
        )

    return {
        "input_ports": total_inputs,
        "output_ports": total_outputs,
        "daisy_chain_links": max(0, total_inputs - upstream_ports),
        "upstream_ports": max(0, upstream_ports),
    }
