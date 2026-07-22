"""Atomic room-type condensation and verification-RFI creation."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import re


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _safe_int(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _room_asset_rows(room_type: dict) -> list[dict]:
    rows = []
    seen = set()
    for raw in room_type.get("assets", []) or []:
        if not isinstance(raw, dict):
            continue
        asset_id = _text(raw.get("asset_id", raw.get("id")))
        if not asset_id or asset_id in seen:
            continue
        rows.append({"asset_id": asset_id, "qty": max(1, _safe_int(raw.get("qty", 1), 1))})
        seen.add(asset_id)
    for raw_asset_id in room_type.get("asset_ids", []) or []:
        asset_id = _text(raw_asset_id)
        if asset_id and asset_id not in seen:
            rows.append({"asset_id": asset_id, "qty": 1})
            seen.add(asset_id)
    return rows


def _room_port_count(data: dict, room_type: dict) -> tuple[int, int]:
    assets = {
        _text(asset.get("id")): asset
        for asset in data.get("assets", []) or []
        if isinstance(asset, dict) and _text(asset.get("id"))
    }
    rows = _room_asset_rows(room_type)
    total = 0
    for row in rows:
        asset = assets.get(row["asset_id"], {})
        ports_each = max(
            0,
            _safe_int(
                asset.get(
                    "data_points",
                    asset.get("data_points_each", asset.get("cables", 1)),
                ),
                1,
            ),
        )
        total += row["qty"] * ports_each
    return len(rows), total


def _next_rfi_number(queries) -> int:
    highest = 0
    for query in queries or []:
        if not isinstance(query, dict):
            continue
        match = re.search(r"(\d+)$", _text(query.get("id")))
        if match:
            highest = max(highest, int(match.group(1)))
    return highest + 1


def condense_room_types(
    data: dict, main_room_type_id: str, condensed_room_type_ids, reason: str
) -> dict:
    """Replace selected room types with one retained type throughout active data."""
    main_id = _text(main_room_type_id)
    source_ids = []
    seen = set()
    for value in condensed_room_type_ids or []:
        room_type_id = _text(value)
        if room_type_id and room_type_id not in seen:
            source_ids.append(room_type_id)
            seen.add(room_type_id)
    reason = _text(reason)
    room_types = [
        row for row in data.get("room_types", []) or [] if isinstance(row, dict)
    ]
    by_id = {
        _text(row.get("id")): row for row in room_types if _text(row.get("id"))
    }
    if not main_id or main_id not in by_id:
        raise ValueError("Select a valid main room type.")
    if main_id in seen:
        raise ValueError("The main room type cannot be condensed into itself.")
    if not source_ids:
        raise ValueError("Select at least one room type to condense.")
    missing = [room_type_id for room_type_id in source_ids if room_type_id not in by_id]
    if missing:
        raise ValueError("Unknown room type ID(s): " + ", ".join(missing))
    if not reason:
        raise ValueError("A database commit note is required.")

    source_set = set(source_ids)
    placement_counts = {room_type_id: 0 for room_type_id in source_ids}
    for point in data.get("data_points", []) or []:
        if not isinstance(point, dict):
            continue
        old_id = _text(point.get("room_type_id"))
        if old_id in source_set:
            point["room_type_id"] = main_id
            placement_counts[old_id] += 1

    endpoint_assignment_count = 0
    for assignment in data.get("network_endpoint_assignments", []) or []:
        if not isinstance(assignment, dict):
            continue
        if _text(assignment.get("room_type_id")) in source_set:
            assignment["room_type_id"] = main_id
            endpoint_assignment_count += 1

    for group in data.get("room_type_scenario_groups", []) or []:
        if not isinstance(group, dict):
            continue
        remapped = []
        group_seen = set()
        for raw_id in group.get("room_type_ids", []) or []:
            room_type_id = _text(raw_id)
            if room_type_id in source_set:
                room_type_id = main_id
            if room_type_id and room_type_id not in group_seen:
                remapped.append(room_type_id)
                group_seen.add(room_type_id)
        group["room_type_ids"] = remapped

    review_state = data.get("room_type_asset_review", {})
    if isinstance(review_state, dict):
        review_state.pop(main_id, None)
        for room_type_id in source_ids:
            review_state.pop(room_type_id, None)

    rfi_state = data.setdefault("room_type_asset_rfi", {})
    if not isinstance(rfi_state, dict):
        rfi_state = {}
        data["room_type_asset_rfi"] = rfi_state
    queries = rfi_state.setdefault("queries", [])
    history = rfi_state.setdefault("history", [])
    if not isinstance(queries, list):
        queries = []
        rfi_state["queries"] = queries
    if not isinstance(history, list):
        history = []
        rfi_state["history"] = history
    main_name = _text(by_id[main_id].get("name", main_id))
    for query in queries:
        if (
            isinstance(query, dict)
            and _text(query.get("room_type_id")) in source_set
            and _text(query.get("status") or "outstanding").casefold() != "resolved"
        ):
            query["room_type_id"] = main_id
            query["room_type_name"] = main_name

    asset_count, port_count = _room_port_count(data, by_id[main_id])
    timestamp = datetime.now().replace(microsecond=0).isoformat(sep=" ")
    next_rfi = _next_rfi_number(queries)
    created_rfis = []
    for source_id in source_ids:
        source = by_id[source_id]
        source_name = _text(source.get("name", source_id))
        rfi_id = f"RFI-{next_rfi:04d}"
        next_rfi += 1
        query_reason = (
            f"Room type condensation reassigned {placement_counts[source_id]} placed "
            f"room(s)/data point(s) from {source_id} ({source_name}) to {main_id} "
            f"({main_name}). Check that the retained room type is correct and verify "
            f"its asset/port definition: {asset_count} asset assignment(s), "
            f"{port_count} total port(s). Condensation reason: {reason}"
        )
        query = {
            "id": rfi_id,
            "room_type_id": main_id,
            "room_type_name": main_name,
            "asset_id": "",
            "asset_name": "",
            "reason": query_reason,
            "status": "outstanding",
            "created_at": timestamp,
            "updated_at": timestamp,
            "resolution": "",
            "resolved_at": "",
        }
        queries.append(query)
        history.append(
            {
                "timestamp": timestamp,
                "action": "room_query_raised",
                "rfi_id": rfi_id,
                "room_type_id": main_id,
                "room_type_name": main_name,
                "asset_id": "",
                "asset_name": "",
                "note": query_reason,
            }
        )
        created_rfis.append(query)

    data["room_types"] = [
        row for row in room_types if _text(row.get("id")) not in source_set
    ]
    return {
        "main_room_type_id": main_id,
        "main_room_type_name": main_name,
        "removed_room_types": [deepcopy(by_id[room_type_id]) for room_type_id in source_ids],
        "placement_counts": placement_counts,
        "endpoint_assignment_count": endpoint_assignment_count,
        "created_rfis": deepcopy(created_rfis),
        "asset_count": asset_count,
        "port_count": port_count,
    }
