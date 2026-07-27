"""Standalone reusable asset bundles for room-type assignment workflows."""

from __future__ import annotations

from copy import deepcopy


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def clean_asset_rows(rows) -> list[dict]:
    """Return valid, de-duplicated assignment rows in their original order."""

    result: list[dict] = []
    by_id: dict[str, dict] = {}
    for value in rows or []:
        if isinstance(value, dict):
            asset_id = _text(value.get("asset_id", value.get("id")))
            quantity = value.get("qty", 1)
            requested_by = _text(value.get("requested_by"))
        else:
            asset_id = _text(value)
            quantity = 1
            requested_by = ""
        if not asset_id:
            continue
        try:
            quantity = max(1, int(quantity or 1))
        except (TypeError, ValueError):
            quantity = 1
        if asset_id in by_id:
            by_id[asset_id]["qty"] += quantity
            continue
        row = {"asset_id": asset_id, "qty": quantity}
        if requested_by:
            row["requested_by"] = requested_by
        by_id[asset_id] = row
        result.append(row)
    return result


def normalise_asset_bundles(bundles, valid_asset_ids=None) -> list[dict]:
    """Normalise persisted bundles without involving scenario-group data."""

    valid = (
        {_text(asset_id) for asset_id in valid_asset_ids if _text(asset_id)}
        if valid_asset_ids is not None
        else None
    )
    result = []
    used_ids = set()
    for index, value in enumerate(bundles or [], start=1):
        if not isinstance(value, dict):
            continue
        bundle_id = _text(value.get("id")) or f"AB{index}"
        if bundle_id in used_ids:
            continue
        rows = clean_asset_rows(value.get("assets", value.get("asset_ids", [])))
        if valid is not None:
            rows = [row for row in rows if row["asset_id"] in valid]
        used_ids.add(bundle_id)
        result.append(
            {
                "id": bundle_id,
                "name": _text(value.get("name")) or bundle_id,
                "description": _text(value.get("description")),
                "assets": rows,
            }
        )
    return result


def clean_bundle_assignments(assignments, valid_bundle_ids=None) -> list[dict]:
    """Return de-duplicated bundle references and their saved multipliers."""
    valid = (
        {_text(bundle_id) for bundle_id in valid_bundle_ids if _text(bundle_id)}
        if valid_bundle_ids is not None
        else None
    )
    result = []
    by_id = {}
    for value in assignments or []:
        if not isinstance(value, dict):
            continue
        bundle_id = _text(value.get("bundle_id", value.get("id")))
        if not bundle_id or (valid is not None and bundle_id not in valid):
            continue
        try:
            quantity = max(
                1,
                int(value.get("qty", value.get("bundle_qty", 1)) or 1),
            )
        except (TypeError, ValueError):
            quantity = 1
        if bundle_id in by_id:
            by_id[bundle_id]["qty"] += quantity
            continue
        row = {"bundle_id": bundle_id, "qty": quantity}
        by_id[bundle_id] = row
        result.append(row)
    return result


def merge_bundle_assignments(existing_assignments, selected_bundles) -> list[dict]:
    """Record selected bundle multipliers alongside a room-type assignment."""
    additions = [
        {
            "bundle_id": bundle.get("id"),
            "qty": bundle.get("bundle_qty", 1),
        }
        for bundle in selected_bundles or []
        if isinstance(bundle, dict)
    ]
    return clean_bundle_assignments(
        [*clean_bundle_assignments(existing_assignments), *additions]
    )


def unlink_bundle_assignment(assignments, bundle_id) -> list[dict]:
    """Remove one bundle relationship without changing current asset rows."""
    unlinked_id = _text(bundle_id)
    return [
        assignment
        for assignment in clean_bundle_assignments(assignments)
        if assignment["bundle_id"] != unlinked_id
    ]


def merge_asset_assignments(existing_rows, added_rows) -> list[dict]:
    """Add bundle quantities to current room assignments.

    Existing row order and requester values are preserved. New assets are
    appended in bundle order, and repeated assets have their quantities added.
    """

    result = deepcopy(clean_asset_rows(existing_rows))
    by_id = {row["asset_id"]: row for row in result}
    for added in clean_asset_rows(added_rows):
        asset_id = added["asset_id"]
        if asset_id in by_id:
            by_id[asset_id]["qty"] += added["qty"]
            continue
        row = deepcopy(added)
        result.append(row)
        by_id[asset_id] = row
    return result


def merge_selected_bundles(existing_rows, bundles) -> list[dict]:
    """Apply selected bundle recipes and their instance multipliers in order."""

    result = clean_asset_rows(existing_rows)
    for bundle in bundles or []:
        if isinstance(bundle, dict):
            try:
                bundle_qty = max(1, int(bundle.get("bundle_qty", 1) or 1))
            except (TypeError, ValueError):
                bundle_qty = 1
            scaled_rows = [
                {
                    **row,
                    "qty": int(row.get("qty", 1) or 1) * bundle_qty,
                }
                for row in clean_asset_rows(bundle.get("assets", []))
            ]
            result = merge_asset_assignments(result, scaled_rows)
    return result


def sync_room_types_for_bundle_updates(
    room_types,
    old_bundles,
    new_bundles,
) -> list[str]:
    """Apply changed bundle recipes to room types that explicitly use them.

    Each room keeps a bundle ID and multiplier. Synchronisation subtracts the
    old recipe contribution and adds the new one, preserving any quantities
    that were added manually or by other bundles.
    """
    old_by_id = {
        bundle["id"]: bundle
        for bundle in normalise_asset_bundles(old_bundles)
    }
    new_by_id = {
        bundle["id"]: bundle
        for bundle in normalise_asset_bundles(new_bundles)
    }
    changed_room_ids = []

    for room_index, room_type in enumerate(room_types or [], start=1):
        if not isinstance(room_type, dict):
            continue
        assignments = clean_bundle_assignments(
            room_type.get("asset_bundle_assignments", [])
        )
        if not assignments:
            continue

        deltas = {}
        for assignment in assignments:
            bundle_id = assignment["bundle_id"]
            multiplier = assignment["qty"]
            old_bundle = old_by_id.get(bundle_id)
            new_bundle = new_by_id.get(bundle_id)
            old_assets = {
                row["asset_id"]: row["qty"]
                for row in clean_asset_rows(
                    old_bundle.get("assets", []) if old_bundle else []
                )
            }
            new_assets = {
                row["asset_id"]: row["qty"]
                for row in clean_asset_rows(
                    new_bundle.get("assets", []) if new_bundle else []
                )
            }
            for asset_id in set(old_assets) | set(new_assets):
                delta = (
                    new_assets.get(asset_id, 0) - old_assets.get(asset_id, 0)
                ) * multiplier
                if delta:
                    deltas[asset_id] = deltas.get(asset_id, 0) + delta

        rows = clean_asset_rows(room_type.get("assets", []))
        rows_by_id = {row["asset_id"]: row for row in rows}
        before = deepcopy(rows)
        for asset_id, delta in deltas.items():
            existing = rows_by_id.get(asset_id)
            if existing is None:
                if delta > 0:
                    row = {"asset_id": asset_id, "qty": delta}
                    rows.append(row)
                    rows_by_id[asset_id] = row
                continue
            updated_quantity = int(existing.get("qty", 1) or 1) + delta
            if updated_quantity > 0:
                existing["qty"] = updated_quantity
            else:
                rows.remove(existing)
                rows_by_id.pop(asset_id, None)

        remaining_assignments = clean_bundle_assignments(
            assignments,
            new_by_id,
        )
        room_type["asset_bundle_assignments"] = remaining_assignments
        if rows != before:
            room_type["assets"] = rows
            room_type["asset_ids"] = [row["asset_id"] for row in rows]
        if rows != before or remaining_assignments != assignments:
            changed_room_ids.append(
                _text(room_type.get("id")) or f"Room type {room_index}"
            )

    return changed_room_ids
