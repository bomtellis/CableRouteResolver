"""Safe, auditable project-asset condensation helpers."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import re


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _quantity(value) -> int:
    try:
        return max(1, int(value or 1))
    except (TypeError, ValueError):
        return 1


def _port_count(value) -> int:
    try:
        return max(0, int(value if value is not None else 1))
    except (TypeError, ValueError):
        return 1


def _assignment_rows(room_type: dict) -> list[dict]:
    rows = []
    seen = set()
    for raw in room_type.get("assets", []) or []:
        if not isinstance(raw, dict):
            continue
        asset_id = _text(raw.get("asset_id", raw.get("id")))
        if not asset_id or asset_id in seen:
            continue
        row = {"asset_id": asset_id, "qty": _quantity(raw.get("qty", 1))}
        requested_by = _text(raw.get("requested_by"))
        if requested_by:
            row["requested_by"] = requested_by
        rows.append(row)
        seen.add(asset_id)
    for raw_asset_id in room_type.get("asset_ids", []) or []:
        asset_id = _text(raw_asset_id)
        if asset_id and asset_id not in seen:
            rows.append({"asset_id": asset_id, "qty": 1})
            seen.add(asset_id)
    return rows


def _combine_requesters(values) -> str:
    combined = []
    seen = set()
    for value in values:
        value = _text(value)
        if value and value.casefold() not in seen:
            combined.append(value)
            seen.add(value.casefold())
    return "; ".join(combined)


def condense_assets(data: dict, main_asset_id: str, condensed_asset_ids) -> dict:
    """Replace selected project assets with one main asset throughout room types."""
    main_id = _text(main_asset_id)
    source_ids = []
    seen_sources = set()
    for value in condensed_asset_ids or []:
        asset_id = _text(value)
        if asset_id and asset_id not in seen_sources:
            source_ids.append(asset_id)
            seen_sources.add(asset_id)

    assets = [row for row in data.get("assets", []) or [] if isinstance(row, dict)]
    assets_by_id = {
        _text(row.get("id")): row for row in assets if _text(row.get("id"))
    }
    if not main_id or main_id not in assets_by_id:
        raise ValueError("Select a valid main asset.")
    if main_id in seen_sources:
        raise ValueError("The main asset cannot also be condensed into itself.")
    if not source_ids:
        raise ValueError("Select at least one asset to condense.")
    missing = [asset_id for asset_id in source_ids if asset_id not in assets_by_id]
    if missing:
        raise ValueError("Unknown asset ID(s): " + ", ".join(missing))

    source_set = set(source_ids)
    affected_ids = source_set | {main_id}
    room_changes = []
    for room_type in data.get("room_types", []) or []:
        if not isinstance(room_type, dict):
            continue
        before = _assignment_rows(room_type)
        if not any(row["asset_id"] in source_set for row in before):
            continue

        replacement_qty = sum(
            row["qty"] for row in before if row["asset_id"] in affected_ids
        )
        replacement_requesters = _combine_requesters(
            row.get("requested_by", "")
            for row in before
            if row["asset_id"] in affected_ids
        )
        after = []
        inserted_main = False
        for row in before:
            asset_id = row["asset_id"]
            if asset_id not in affected_ids:
                after.append(dict(row))
                continue
            if inserted_main:
                continue
            replacement = {"asset_id": main_id, "qty": replacement_qty}
            if replacement_requesters:
                replacement["requested_by"] = replacement_requesters
            after.append(replacement)
            inserted_main = True

        room_type["assets"] = after
        room_type["asset_ids"] = [row["asset_id"] for row in after]
        room_changes.append(
            {
                "room_type_id": _text(room_type.get("id")),
                "room_type_name": _text(room_type.get("name")),
                "before": deepcopy(before),
                "after": deepcopy(after),
            }
        )

    data["assets"] = [
        row for row in assets if _text(row.get("id")) not in source_set
    ]

    for group in data.get("asset_scenario_groups", []) or []:
        if not isinstance(group, dict):
            continue
        remapped = []
        seen = set()
        for raw_asset_id in group.get("asset_ids", []) or []:
            asset_id = _text(raw_asset_id)
            if asset_id in source_set:
                asset_id = main_id
            if asset_id and asset_id not in seen:
                remapped.append(asset_id)
                seen.add(asset_id)
        group["asset_ids"] = remapped

    return {
        "main_asset_id": main_id,
        "main_asset_name": _text(assets_by_id[main_id].get("name", main_id)),
        "removed_assets": [deepcopy(assets_by_id[asset_id]) for asset_id in source_ids],
        "room_changes": room_changes,
    }


def expand_asset(data: dict, source_asset_id: str, replacement_assets) -> dict:
    """Replace one asset with two assets while retaining each room quantity."""
    source_id = _text(source_asset_id)
    assets = [row for row in data.get("assets", []) or [] if isinstance(row, dict)]
    assets_by_id = {
        _text(row.get("id")): row for row in assets if _text(row.get("id"))
    }
    if not source_id or source_id not in assets_by_id:
        raise ValueError("Select a valid asset to expand.")

    replacements = [
        deepcopy(row) for row in (replacement_assets or []) if isinstance(row, dict)
    ]
    if len(replacements) != 2:
        raise ValueError("Asset expansion requires exactly two replacement assets.")
    replacement_ids = [_text(row.get("id")) for row in replacements]
    if any(not asset_id for asset_id in replacement_ids):
        raise ValueError("Each replacement asset requires an ID.")
    if len(set(replacement_ids)) != 2:
        raise ValueError("Replacement asset IDs must be different.")
    if source_id in replacement_ids:
        raise ValueError("A replacement asset cannot reuse the expanded asset ID.")
    conflicts = [asset_id for asset_id in replacement_ids if asset_id in assets_by_id]
    if conflicts:
        raise ValueError("Replacement asset ID already exists: " + ", ".join(conflicts))
    for replacement in replacements:
        replacement["id"] = _text(replacement.get("id"))
        replacement["name"] = _text(replacement.get("name"))
        if not replacement["name"]:
            raise ValueError("Each replacement asset requires a name.")

    room_changes = []
    review_state = data.get("room_type_asset_review", {})
    for room_type in data.get("room_types", []) or []:
        if not isinstance(room_type, dict):
            continue
        before = _assignment_rows(room_type)
        if not any(row["asset_id"] == source_id for row in before):
            continue
        after = []
        for row in before:
            if row["asset_id"] != source_id:
                after.append(dict(row))
                continue
            for replacement_id in replacement_ids:
                replacement_row = {
                    "asset_id": replacement_id,
                    "qty": row["qty"],
                }
                requested_by = _text(row.get("requested_by"))
                if requested_by:
                    replacement_row["requested_by"] = requested_by
                after.append(replacement_row)
        room_type["assets"] = after
        room_type["asset_ids"] = [row["asset_id"] for row in after]
        room_type_id = _text(room_type.get("id"))
        if isinstance(review_state, dict):
            review_state.pop(room_type_id, None)
        room_changes.append(
            {
                "room_type_id": room_type_id,
                "room_type_name": _text(room_type.get("name")),
                "before": deepcopy(before),
                "after": deepcopy(after),
            }
        )

    expanded_assets = []
    for asset in assets:
        if _text(asset.get("id")) == source_id:
            expanded_assets.extend(deepcopy(replacements))
        else:
            expanded_assets.append(asset)
    data["assets"] = expanded_assets

    def expand_ids(values):
        expanded = []
        seen = set()
        for raw_asset_id in values or []:
            asset_ids = replacement_ids if _text(raw_asset_id) == source_id else [_text(raw_asset_id)]
            for asset_id in asset_ids:
                if asset_id and asset_id not in seen:
                    expanded.append(asset_id)
                    seen.add(asset_id)
        return expanded

    for group in data.get("asset_scenario_groups", []) or []:
        if isinstance(group, dict):
            group["asset_ids"] = expand_ids(group.get("asset_ids", []))
    for scenario in data.get("room_type_asset_scenarios", []) or []:
        if not isinstance(scenario, dict):
            continue
        for key in ("asset_ids", "replacement_asset_ids", "preferred_asset_ids"):
            if key in scenario:
                scenario[key] = expand_ids(scenario.get(key, []))

    return {
        "source_asset": deepcopy(assets_by_id[source_id]),
        "replacement_assets": deepcopy(replacements),
        "room_changes": room_changes,
    }


def create_condensation_rfis(data: dict, condensation: dict, reason: str) -> list[dict]:
    """Raise one main-asset verification RFI for every affected room type."""
    state = data.setdefault("room_type_asset_rfi", {})
    if not isinstance(state, dict):
        state = {}
        data["room_type_asset_rfi"] = state
    queries = state.setdefault("queries", [])
    history = state.setdefault("history", [])
    if not isinstance(queries, list):
        queries = []
        state["queries"] = queries
    if not isinstance(history, list):
        history = []
        state["history"] = history

    highest = 0
    for query in queries:
        if not isinstance(query, dict):
            continue
        match = re.search(r"(\d+)$", _text(query.get("id")))
        if match:
            highest = max(highest, int(match.group(1)))

    main_id = _text(condensation.get("main_asset_id"))
    main_name = _text(condensation.get("main_asset_name"))
    main_asset = next(
        (
            row
            for row in data.get("assets", []) or []
            if isinstance(row, dict) and _text(row.get("id")) == main_id
        ),
        {},
    )
    ports_each = _port_count(
        main_asset.get(
            "data_points",
            main_asset.get("data_points_each", main_asset.get("cables", 1)),
        )
    )
    removed_ids = [
        _text(asset.get("id"))
        for asset in condensation.get("removed_assets", []) or []
        if isinstance(asset, dict) and _text(asset.get("id"))
    ]
    timestamp = datetime.now().replace(microsecond=0).isoformat(sep=" ")
    created = []
    review_state = data.get("room_type_asset_review", {})

    for change in condensation.get("room_changes", []) or []:
        if not isinstance(change, dict):
            continue
        main_row = next(
            (
                row
                for row in change.get("after", []) or []
                if isinstance(row, dict) and _text(row.get("asset_id")) == main_id
            ),
            {},
        )
        assigned_qty = _quantity(main_row.get("qty", 1))
        port_count = assigned_qty * ports_each
        replaced_in_room = [
            asset_id
            for asset_id in removed_ids
            if any(
                isinstance(row, dict) and _text(row.get("asset_id")) == asset_id
                for row in change.get("before", []) or []
            )
        ]
        room_type_id = _text(change.get("room_type_id"))
        room_type_name = _text(change.get("room_type_name"))
        highest += 1
        rfi_id = f"RFI-{highest:04d}"
        query_reason = (
            f"Asset condensation replaced {', '.join(replaced_in_room)} with {main_id}. "
            f"Check that {main_id} ({main_name or 'unnamed asset'}) is the correct "
            f"asset and verify its port count: {assigned_qty} assigned x "
            f"{ports_each} port(s) each = {port_count} port(s). "
            f"Condensation reason: {_text(reason)}"
        )
        query = {
            "id": rfi_id,
            "room_type_id": room_type_id,
            "room_type_name": room_type_name,
            "asset_id": main_id,
            "asset_name": main_name,
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
                "action": "query_raised",
                "rfi_id": rfi_id,
                "room_type_id": room_type_id,
                "room_type_name": room_type_name,
                "asset_id": main_id,
                "asset_name": main_name,
                "note": query_reason,
            }
        )
        if isinstance(review_state, dict):
            review_state.pop(room_type_id, None)
        created.append(query)
    return created
