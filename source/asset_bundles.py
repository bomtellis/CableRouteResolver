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
