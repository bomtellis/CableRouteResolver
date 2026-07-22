"""Coalesced, database-serialisable staging for room-type asset changes."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from time import monotonic


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def room_type_matches_filter(room_type, filter_text) -> bool:
    """Match every typeahead term against a room type's ID or name."""
    if not isinstance(room_type, dict):
        return False
    terms = [term for term in _text(filter_text).casefold().split() if term]
    if not terms:
        return True
    searchable = " ".join(
        (
            _text(room_type.get("id")),
            _text(room_type.get("name")),
        )
    ).casefold()
    return all(term in searchable for term in terms)


def should_mirror_rfi_audit_to_revision(item) -> bool:
    """Keep assignment edits in RFI audit history without logging them twice."""
    if not isinstance(item, dict):
        return False
    action = _text(item.get("action")).casefold()
    return action != "assignment_values_updated"


def remember_room_type_revision_change(
    fingerprints: dict,
    source,
    room_type_id,
    details,
    *,
    timestamp=None,
    duplicate_window_seconds=2.0,
) -> bool:
    """Return False for an identical repeated event for the same source and room.

    The short window catches repeated UI signal submissions while allowing a later
    genuine recurrence, including after undo and reapply.
    """
    cleaned_details = tuple(
        _text(detail) for detail in (details or []) if _text(detail)
    )
    if not cleaned_details:
        return False
    key = (_text(source).casefold(), _text(room_type_id).casefold())
    recorded_at = float(monotonic() if timestamp is None else timestamp)
    previous = fingerprints.get(key)
    fingerprints[key] = (cleaned_details, recorded_at)
    if (
        isinstance(previous, tuple)
        and len(previous) == 2
        and previous[0] == cleaned_details
        and recorded_at - float(previous[1])
        <= max(0.0, float(duplicate_window_seconds))
    ):
        return False
    return True


def clean_assignment_rows(rows) -> list[dict]:
    cleaned = []
    seen = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        asset_id = _text(row.get("asset_id", row.get("id", "")))
        if not asset_id or asset_id in seen:
            continue
        try:
            qty = max(1, int(row.get("qty", 1) or 1))
        except (TypeError, ValueError):
            qty = 1
        value = {"asset_id": asset_id, "qty": qty}
        requested_by = _text(row.get("requested_by"))
        if requested_by:
            value["requested_by"] = requested_by
        cleaned.append(value)
        seen.add(asset_id)
    return sorted(cleaned, key=lambda row: row["asset_id"].casefold())


def _rows_by_id(rows) -> dict[str, dict]:
    return {row["asset_id"]: row for row in clean_assignment_rows(rows)}


def staged_changes(staging) -> list[dict]:
    state = staging if isinstance(staging, dict) else {}
    changes = []
    for room_id, record in (state.get("rooms", {}) or {}).items():
        if not isinstance(record, dict):
            continue
        before = _rows_by_id(record.get("before", []))
        after = _rows_by_id(record.get("after", []))
        for asset_id in sorted(set(before) | set(after), key=str.casefold):
            if asset_id not in before:
                change_type = "added"
            elif asset_id not in after:
                change_type = "removed"
            elif before[asset_id] != after[asset_id]:
                change_type = "changed"
            else:
                continue
            changes.append(
                {
                    "change_type": change_type,
                    "scope": "assignment",
                    "room_type_id": _text(room_id),
                    "room_type_name": _text(record.get("room_type_name")),
                    "asset_id": asset_id,
                    "asset_name": _text(
                        (record.get("asset_names", {}) or {}).get(asset_id)
                    ),
                    "before": deepcopy(before.get(asset_id)),
                    "after": deepcopy(after.get(asset_id)),
                }
            )
    for asset_id, record in (state.get("assets", {}) or {}).items():
        if not isinstance(record, dict):
            continue
        before = int(record.get("before_data_points", 0) or 0)
        after = int(record.get("after_data_points", 0) or 0)
        if before == after:
            continue
        changes.append(
            {
                "change_type": "changed",
                "scope": "asset",
                "room_type_id": _text(record.get("room_type_id")),
                "room_type_name": _text(record.get("room_type_name")),
                "asset_id": _text(asset_id),
                "asset_name": _text(record.get("asset_name")),
                "before": {"data_points": before},
                "after": {"data_points": after},
            }
        )
    return sorted(
        changes,
        key=lambda row: (
            {"added": 0, "changed": 1, "removed": 2}.get(row["change_type"], 9),
            row["room_type_name"].casefold(),
            row["room_type_id"].casefold(),
            row["asset_name"].casefold(),
            row["asset_id"].casefold(),
        ),
    )


def update_staging(
    staging,
    *,
    room_type_id,
    room_type_name,
    before_rows,
    after_rows,
    before_ports,
    after_ports,
    asset_names,
) -> dict:
    state = deepcopy(staging) if isinstance(staging, dict) else {}
    state.setdefault("started_at", datetime.now(timezone.utc).isoformat())
    rooms = state.setdefault("rooms", {})
    assets = state.setdefault("assets", {})
    room_id = _text(room_type_id)
    original = rooms.get(room_id) if isinstance(rooms.get(room_id), dict) else None
    record = {
        "room_type_name": _text(room_type_name),
        "before": clean_assignment_rows(
            original.get("before", []) if original else before_rows
        ),
        "after": clean_assignment_rows(after_rows),
        "asset_names": {
            _text(key): _text(value) for key, value in (asset_names or {}).items()
        },
    }
    if record["before"] == record["after"]:
        rooms.pop(room_id, None)
    else:
        rooms[room_id] = record

    before_values = {
        _text(key): max(0, int(value or 0)) for key, value in (before_ports or {}).items()
    }
    for raw_asset_id, raw_after in (after_ports or {}).items():
        asset_id = _text(raw_asset_id)
        if not asset_id:
            continue
        after_value = max(0, int(raw_after or 0))
        existing = assets.get(asset_id) if isinstance(assets.get(asset_id), dict) else None
        before_value = (
            max(0, int(existing.get("before_data_points", 0) or 0))
            if existing
            else before_values.get(asset_id, after_value)
        )
        if before_value == after_value:
            assets.pop(asset_id, None)
            continue
        assets[asset_id] = {
            "asset_name": _text((asset_names or {}).get(asset_id)),
            "room_type_id": room_id,
            "room_type_name": _text(room_type_name),
            "before_data_points": before_value,
            "after_data_points": after_value,
        }

    state["changes"] = staged_changes(state)
    if not state["changes"]:
        return {}
    return state


def build_commit(staging, message, *, commit_id) -> dict:
    changes = staged_changes(staging)
    if not changes:
        raise ValueError("There are no staged room-type asset changes to commit.")
    commit = {
        "id": _text(commit_id),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": _text(message),
        "changes": changes,
    }
    rollback_of = [
        _text(value)
        for value in (staging.get("rollback_of", []) if isinstance(staging, dict) else [])
        if _text(value)
    ]
    if rollback_of:
        commit["rollback_of"] = rollback_of
    return commit


def resolve_rfis_with_commit(
    rfi_state, rfi_ids, *, commit_id, message, timestamp=None
) -> tuple[dict, list[str]]:
    """Resolve selected outstanding RFIs and append commit-linked audit events."""
    state = deepcopy(rfi_state) if isinstance(rfi_state, dict) else {}
    queries = state.setdefault("queries", [])
    history = state.setdefault("history", [])
    if not isinstance(queries, list):
        queries = []
        state["queries"] = queries
    if not isinstance(history, list):
        history = []
        state["history"] = history
    wanted = {_text(value) for value in rfi_ids or [] if _text(value)}
    resolved = []
    resolved_at = _text(timestamp) or datetime.now(timezone.utc).isoformat()
    resolution = f"Resolved by asset commit {_text(commit_id)}: {_text(message)}"
    for query in queries:
        if not isinstance(query, dict):
            continue
        rfi_id = _text(query.get("id"))
        if (
            not rfi_id
            or rfi_id not in wanted
            or _text(query.get("status") or "outstanding").casefold() == "resolved"
        ):
            continue
        query.update(
            status="resolved",
            resolution=resolution,
            resolved_at=resolved_at,
            updated_at=resolved_at,
        )
        history.append(
            {
                "timestamp": resolved_at,
                "action": "query_resolved",
                "rfi_id": rfi_id,
                "room_type_id": _text(query.get("room_type_id")),
                "room_type_name": _text(query.get("room_type_name")),
                "asset_id": _text(query.get("asset_id")),
                "asset_name": _text(query.get("asset_name")),
                "note": resolution,
                "commit_id": _text(commit_id),
            }
        )
        resolved.append(rfi_id)
    return state, resolved
