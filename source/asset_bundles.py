"""Standalone reusable asset bundles for room-type assignment workflows."""

from __future__ import annotations

from copy import deepcopy

from asset_ports import clean_asset_connections


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


def clean_bundle_asset_exclusions(values, valid_asset_ids=None) -> list[str]:
    """Return unique room-specific asset IDs suppressed from bundle recipes."""

    if isinstance(values, dict):
        values = [
            asset_id
            for asset_id, excluded in values.items()
            if bool(excluded)
        ]
    valid = (
        {_text(asset_id) for asset_id in valid_asset_ids if _text(asset_id)}
        if valid_asset_ids is not None
        else None
    )
    result = []
    for value in values or []:
        asset_id = _text(
            value.get("asset_id", value.get("id"))
            if isinstance(value, dict)
            else value
        )
        if (
            not asset_id
            or asset_id in result
            or (valid is not None and asset_id not in valid)
        ):
            continue
        result.append(asset_id)
    return result


def bundle_without_excluded_assets(bundle, excluded_asset_ids) -> dict:
    """Copy a bundle recipe without room-excluded assets or connections."""

    excluded = set(clean_bundle_asset_exclusions(excluded_asset_ids))
    if not isinstance(bundle, dict) or not excluded:
        return deepcopy(bundle) if isinstance(bundle, dict) else {}
    result = deepcopy(bundle)
    result["assets"] = [
        row
        for row in clean_asset_rows(bundle.get("assets", []))
        if row["asset_id"] not in excluded
    ]
    result["connections"] = [
        connection
        for connection in clean_asset_connections(
            bundle.get("connections", bundle.get("asset_connections", []))
        )
        if not any(
            _text(connection.get(field)) in excluded
            for field in (
                "from_asset_id",
                "to_asset_id",
                "connection_asset_id",
            )
        )
    ]
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
        row_ids = {row["asset_id"] for row in rows}
        used_ids.add(bundle_id)
        result.append(
            {
                "id": bundle_id,
                "name": _text(value.get("name")) or bundle_id,
                "description": _text(value.get("description")),
                "assets": rows,
                "placeholder": not bool(rows),
                "connections": clean_asset_connections(
                    value.get("connections", value.get("asset_connections", [])),
                    row_ids,
                ),
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


def _bundle_asset_contributions(
    assignments,
    bundles,
    excluded_asset_ids=None,
) -> dict[str, list[dict]]:
    excluded = set(clean_bundle_asset_exclusions(excluded_asset_ids))
    normalised_bundles = normalise_asset_bundles(bundles)
    bundles_by_id = {
        bundle["id"]: bundle for bundle in normalised_bundles
    }
    overlaps_by_asset = {}
    for assignment in clean_bundle_assignments(
        assignments,
        bundles_by_id,
    ):
        bundle_id = assignment["bundle_id"]
        bundle = bundles_by_id.get(bundle_id)
        if bundle is None:
            continue
        bundle_qty = int(assignment.get("qty", 1) or 1)
        for asset_row in clean_asset_rows(bundle.get("assets", [])):
            asset_id = asset_row["asset_id"]
            if asset_id in excluded:
                continue
            asset_qty = int(asset_row.get("qty", 1) or 1)
            contribution = {
                "source_type": "bundle",
                "bundle_id": bundle_id,
                "bundle_name": _text(bundle.get("name")) or bundle_id,
                "bundle_qty": bundle_qty,
                "asset_qty": asset_qty,
                "total_qty": bundle_qty * asset_qty,
            }
            overlaps_by_asset.setdefault(asset_id, []).append(contribution)
    return overlaps_by_asset


def room_asset_source_labels(room_type, bundles) -> dict[str, str]:
    """Return display labels for each saved room asset's assignment source.

    Linked bundle quantities are materialised into the room's saved asset
    quantities. Any saved quantity beyond those linked contributions is a
    manual assignment. Assets can therefore legitimately list more than one
    bundle and ``Manual`` in the same cell.
    """

    room_type = room_type if isinstance(room_type, dict) else {}
    saved_rows = clean_asset_rows(
        room_type.get("assets", [])
        or room_type.get("asset_ids", [])
    )
    contributions_by_asset = _bundle_asset_contributions(
        room_type.get("asset_bundle_assignments", []),
        bundles,
        room_type.get("asset_bundle_excluded_asset_ids", []),
    )
    result = {}
    for row in saved_rows:
        asset_id = row["asset_id"]
        contributions = contributions_by_asset.get(asset_id, [])
        labels = []
        bundle_total = 0
        for contribution in contributions:
            bundle_id = _text(contribution.get("bundle_id"))
            bundle_name = _text(contribution.get("bundle_name")) or bundle_id
            label = (
                bundle_id
                if bundle_name.casefold() == bundle_id.casefold()
                else f"{bundle_name} [{bundle_id}]"
            )
            if label and label not in labels:
                labels.append(label)
            bundle_total += max(
                0,
                int(contribution.get("total_qty", 0) or 0),
            )
        saved_quantity = max(1, int(row.get("qty", 1) or 1))
        if not labels or saved_quantity > bundle_total:
            labels.append("Manual")
        result[asset_id] = "; ".join(labels)
    return result


def inferred_room_bundle_asset_exclusions(room_type, bundles) -> list[str]:
    """Preserve bundle assets that are already absent from a room type.

    Bundle assignments are materialised into ``room_type["assets"]``. If an
    assigned bundle contains an asset that is not present in those saved rows,
    the missing asset represents a room-specific omission and must be recorded
    so later bundle quantity or recipe changes do not silently restore it.
    """

    room_type = room_type if isinstance(room_type, dict) else {}
    existing = clean_bundle_asset_exclusions(
        room_type.get("asset_bundle_excluded_asset_ids", [])
    )
    expected_asset_ids = set(
        _bundle_asset_contributions(
            room_type.get("asset_bundle_assignments", []),
            bundles,
        )
    )
    present_asset_ids = {
        row["asset_id"]
        for row in clean_asset_rows(
            room_type.get("assets", [])
            or room_type.get("asset_ids", [])
        )
    }
    return clean_bundle_asset_exclusions(
        [
            *existing,
            *sorted(expected_asset_ids - present_asset_ids, key=str.casefold),
        ]
    )


def bundle_asset_overlaps(assignments, bundles) -> list[dict]:
    """Describe assets contributed by more than one assigned bundle.

    Bundle and asset quantities are included in each contribution so callers
    can show the combined room quantity without changing the established
    additive merge behaviour.
    """

    result = []
    for asset_id, contributions in _bundle_asset_contributions(
        assignments,
        bundles,
    ).items():
        if len(contributions) < 2:
            continue
        result.append(
            {
                "asset_id": asset_id,
                "bundle_ids": [
                    contribution["bundle_id"]
                    for contribution in contributions
                ],
                "contributions": contributions,
                "total_qty": sum(
                    contribution["total_qty"]
                    for contribution in contributions
                ),
            }
        )
    return result


def room_bundle_asset_overlaps(
    room_type,
    assignments,
    bundles,
) -> list[dict]:
    """Find bundle/bundle and bundle/manual asset overlaps for one room.

    Saved room asset quantities already contain linked bundle contributions.
    The manual quantity is therefore the saved total less the contribution
    from the room's current bundle assignments. Requested assignments are then
    checked against that stable manual remainder.
    """

    room_type = room_type if isinstance(room_type, dict) else {}
    excluded_asset_ids = clean_bundle_asset_exclusions(
        room_type.get("asset_bundle_excluded_asset_ids", [])
    )
    current_assignments = room_type.get("asset_bundle_assignments", [])
    current_contributions = _bundle_asset_contributions(
        current_assignments,
        bundles,
        excluded_asset_ids,
    )
    requested_contributions = _bundle_asset_contributions(
        assignments,
        bundles,
        excluded_asset_ids,
    )
    current_bundle_totals = {
        asset_id: sum(row["total_qty"] for row in contributions)
        for asset_id, contributions in current_contributions.items()
    }
    manual_quantities = {}
    for row in clean_asset_rows(room_type.get("assets", [])):
        asset_id = row["asset_id"]
        manual_qty = max(
            0,
            int(row.get("qty", 1) or 1)
            - current_bundle_totals.get(asset_id, 0),
        )
        if manual_qty:
            manual_quantities[asset_id] = manual_qty

    result = []
    asset_ids = list(requested_contributions)
    asset_ids.extend(
        asset_id
        for asset_id in manual_quantities
        if asset_id not in requested_contributions
    )
    for asset_id in asset_ids:
        bundle_contributions = [
            dict(row)
            for row in requested_contributions.get(asset_id, [])
        ]
        manual_qty = manual_quantities.get(asset_id, 0)
        overlap_types = []
        if len(bundle_contributions) > 1:
            overlap_types.append("multiple_bundles")
        if bundle_contributions and manual_qty:
            overlap_types.append("bundle_and_manual")
        if not overlap_types:
            continue
        contributions = list(bundle_contributions)
        if manual_qty:
            contributions.append(
                {
                    "source_type": "manual",
                    "bundle_id": "",
                    "bundle_name": "Manually added",
                    "bundle_qty": 1,
                    "asset_qty": manual_qty,
                    "total_qty": manual_qty,
                }
            )
        result.append(
            {
                "asset_id": asset_id,
                "bundle_ids": [
                    contribution["bundle_id"]
                    for contribution in bundle_contributions
                ],
                "contributions": contributions,
                "manual_qty": manual_qty,
                "overlap_types": overlap_types,
                "total_qty": sum(
                    contribution["total_qty"]
                    for contribution in contributions
                ),
            }
        )
    return result


def resolve_room_bundle_asset_overlaps(room_type, asset_ids) -> dict:
    """Keep one room asset and suppress future bundle re-addition for it."""

    if not isinstance(room_type, dict):
        raise ValueError("A room type is required.")
    result = deepcopy(room_type)
    resolved_asset_ids = clean_bundle_asset_exclusions(asset_ids)
    exclusions = clean_bundle_asset_exclusions(
        [
            *clean_bundle_asset_exclusions(
                result.get("asset_bundle_excluded_asset_ids", [])
            ),
            *resolved_asset_ids,
        ]
    )
    rows = clean_asset_rows(result.get("assets", []))
    rows_by_id = {row["asset_id"]: row for row in rows}
    for asset_id in resolved_asset_ids:
        row = rows_by_id.get(asset_id)
        if row is None:
            row = {"asset_id": asset_id, "qty": 1}
            rows.append(row)
            rows_by_id[asset_id] = row
        else:
            row["qty"] = 1
    result["assets"] = rows
    result["asset_ids"] = [row["asset_id"] for row in rows]
    result["asset_bundle_excluded_asset_ids"] = exclusions
    result["asset_connections"] = clean_asset_connections(
        result.get("asset_connections", result.get("connections", [])),
        result["asset_ids"],
    )
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


def reconcile_bundle_assignments(room_type, bundles, assignments) -> dict:
    """Apply the delta between current and requested linked bundle quantities."""

    if not isinstance(room_type, dict):
        raise ValueError("A room type is required.")
    normalised_bundles = normalise_asset_bundles(bundles)
    bundles_by_id = {
        bundle["id"]: bundle for bundle in normalised_bundles
    }
    excluded_asset_ids = clean_bundle_asset_exclusions(
        room_type.get("asset_bundle_excluded_asset_ids", [])
    )
    current_assignments = clean_bundle_assignments(
        room_type.get("asset_bundle_assignments", []),
        bundles_by_id,
    )
    requested_assignments = clean_bundle_assignments(
        assignments,
        bundles_by_id,
    )

    def selected_bundle_rows(values):
        selected = []
        for assignment in values:
            bundle = bundles_by_id.get(assignment["bundle_id"])
            if bundle:
                bundle = bundle_without_excluded_assets(
                    bundle,
                    excluded_asset_ids,
                )
                selected.append(
                    {**bundle, "bundle_qty": assignment["qty"]}
                )
        return selected

    def asset_contributions(values):
        contributions = {}
        for assignment in values:
            bundle = bundles_by_id.get(assignment["bundle_id"], {})
            bundle = bundle_without_excluded_assets(
                bundle,
                excluded_asset_ids,
            )
            multiplier = int(assignment.get("qty", 1) or 1)
            for row in clean_asset_rows(bundle.get("assets", [])):
                asset_id = row["asset_id"]
                contributions[asset_id] = contributions.get(asset_id, 0) + (
                    int(row.get("qty", 1) or 1) * multiplier
                )
        return contributions

    current_assets = asset_contributions(current_assignments)
    requested_assets = asset_contributions(requested_assignments)
    rows = deepcopy(clean_asset_rows(room_type.get("assets", [])))
    rows_by_id = {row["asset_id"]: row for row in rows}
    for asset_id in set(current_assets) | set(requested_assets):
        delta = requested_assets.get(asset_id, 0) - current_assets.get(
            asset_id, 0
        )
        existing = rows_by_id.get(asset_id)
        if existing is None:
            if delta > 0:
                added = {"asset_id": asset_id, "qty": delta}
                rows.append(added)
                rows_by_id[asset_id] = added
            continue
        updated = int(existing.get("qty", 1) or 1) + delta
        if updated > 0:
            existing["qty"] = updated
        else:
            rows.remove(existing)
            rows_by_id.pop(asset_id, None)
    rows = clean_asset_rows(rows)

    current_connections = {
        _connection_key(row): row
        for row in merge_selected_bundle_connections(
            [], selected_bundle_rows(current_assignments)
        )
    }
    requested_connections = {
        _connection_key(row): row
        for row in merge_selected_bundle_connections(
            [], selected_bundle_rows(requested_assignments)
        )
    }
    connections = clean_asset_connections(
        room_type.get("asset_connections", room_type.get("connections", []))
    )
    connections_by_key = {
        _connection_key(row): row for row in connections
    }
    for key in set(current_connections) | set(requested_connections):
        old_row = current_connections.get(key, {})
        new_row = requested_connections.get(key, {})
        delta = int(new_row.get("qty", 0) or 0) - int(
            old_row.get("qty", 0) or 0
        )
        existing = connections_by_key.get(key)
        if existing is None:
            if delta > 0:
                added = dict(new_row)
                added["qty"] = delta
                connections.append(added)
                connections_by_key[key] = added
            continue
        updated = int(existing.get("qty", 1) or 1) + delta
        if updated > 0:
            existing["qty"] = updated
        else:
            connections.remove(existing)
            connections_by_key.pop(key, None)
    connections = clean_asset_connections(
        connections,
        [row["asset_id"] for row in rows],
    )

    result = deepcopy(room_type)
    result["assets"] = rows
    result["asset_ids"] = [row["asset_id"] for row in rows]
    result["asset_bundle_assignments"] = requested_assignments
    result["asset_bundle_excluded_asset_ids"] = excluded_asset_ids
    result["asset_connections"] = connections
    return result


def force_room_bundle_assignments(room_type, bundles) -> dict:
    """Rebuild a room exclusively from its assigned bundle definitions.

    Unlike delta reconciliation, this intentionally discards room-level asset
    and connection edits, restores excluded bundle assets, and reapplies each
    saved bundle multiplier to the current recipe.
    """

    if not isinstance(room_type, dict):
        raise ValueError("A room type is required.")
    normalised_bundles = normalise_asset_bundles(bundles)
    bundles_by_id = {
        bundle["id"]: bundle for bundle in normalised_bundles
    }
    # Force update treats the saved quantity as authoritative. Historical
    # projects can contain repeated rows for one bundle; the normal additive
    # cleaner would sum those rows and duplicate every bundled asset. Collapse
    # them with last-write-wins semantics instead.
    assignments_by_id = {}
    assignment_order = []
    for raw_assignment in room_type.get("asset_bundle_assignments", []) or []:
        cleaned = clean_bundle_assignments(
            [raw_assignment],
            bundles_by_id,
        )
        if not cleaned:
            continue
        assignment = cleaned[0]
        bundle_id = assignment["bundle_id"]
        if bundle_id not in assignments_by_id:
            assignment_order.append(bundle_id)
        assignments_by_id[bundle_id] = assignment
    assignments = [
        assignments_by_id[bundle_id] for bundle_id in assignment_order
    ]
    if not assignments:
        raise ValueError("This room type has no valid assigned asset bundles.")

    selected_bundles = [
        {
            **bundles_by_id[assignment["bundle_id"]],
            "bundle_qty": assignment["qty"],
        }
        for assignment in assignments
    ]
    quantities_by_asset_id = {}
    asset_order = []
    for bundle in selected_bundles:
        bundle_qty = int(bundle.get("bundle_qty", 1) or 1)
        for asset_row in clean_asset_rows(bundle.get("assets", [])):
            asset_id = asset_row["asset_id"]
            if asset_id not in quantities_by_asset_id:
                asset_order.append(asset_id)
                quantities_by_asset_id[asset_id] = 0
            quantities_by_asset_id[asset_id] += (
                int(asset_row.get("qty", 1) or 1) * bundle_qty
            )
    rows = [
        {
            "asset_id": asset_id,
            "qty": quantities_by_asset_id[asset_id],
        }
        for asset_id in asset_order
    ]
    connections = merge_selected_bundle_connections(
        [],
        selected_bundles,
        existing_asset_rows=[],
    )

    result = deepcopy(room_type)
    result["assets"] = rows
    result["asset_ids"] = [row["asset_id"] for row in rows]
    result["asset_bundle_assignments"] = assignments
    result["asset_bundle_excluded_asset_ids"] = []
    result["asset_connections"] = clean_asset_connections(
        connections,
        result["asset_ids"],
    )
    return result


def replace_bundle_assignment(
    room_type,
    bundles,
    replaced_bundle_id,
    replacement_bundle_id,
    replacement_qty=1,
) -> dict:
    """Replace one linked bundle while preserving unrelated room quantities."""

    if not isinstance(room_type, dict):
        raise ValueError("A room type is required.")
    bundles_by_id = {
        bundle["id"]: bundle for bundle in normalise_asset_bundles(bundles)
    }
    excluded_asset_ids = clean_bundle_asset_exclusions(
        room_type.get("asset_bundle_excluded_asset_ids", [])
    )
    old_id = _text(replaced_bundle_id)
    new_id = _text(replacement_bundle_id)
    old_bundle = bundles_by_id.get(old_id)
    new_bundle = bundles_by_id.get(new_id)
    if old_bundle is None:
        raise ValueError(f"Linked bundle {old_id or '(blank)'} was not found.")
    if new_bundle is None:
        raise ValueError(f"Replacement bundle {new_id or '(blank)'} was not found.")
    old_bundle = bundle_without_excluded_assets(
        old_bundle,
        excluded_asset_ids,
    )
    new_bundle = bundle_without_excluded_assets(
        new_bundle,
        excluded_asset_ids,
    )
    try:
        new_qty = max(1, int(replacement_qty or 1))
    except (TypeError, ValueError):
        new_qty = 1

    assignments = clean_bundle_assignments(
        room_type.get("asset_bundle_assignments", []),
        bundles_by_id,
    )
    old_assignment = next(
        (
            assignment
            for assignment in assignments
            if assignment["bundle_id"] == old_id
        ),
        None,
    )
    if old_assignment is None:
        raise ValueError(f"Room type is not linked to bundle {old_id}.")
    old_qty = int(old_assignment.get("qty", 1) or 1)

    old_assets = {
        row["asset_id"]: int(row.get("qty", 1) or 1) * old_qty
        for row in clean_asset_rows(old_bundle.get("assets", []))
    }
    new_assets = {
        row["asset_id"]: int(row.get("qty", 1) or 1) * new_qty
        for row in clean_asset_rows(new_bundle.get("assets", []))
    }
    rows = deepcopy(clean_asset_rows(room_type.get("assets", [])))
    rows_by_id = {row["asset_id"]: row for row in rows}
    for asset_id in set(old_assets) | set(new_assets):
        delta = new_assets.get(asset_id, 0) - old_assets.get(asset_id, 0)
        existing = rows_by_id.get(asset_id)
        if existing is None:
            if delta > 0:
                added = {"asset_id": asset_id, "qty": delta}
                rows.append(added)
                rows_by_id[asset_id] = added
            continue
        updated = int(existing.get("qty", 1) or 1) + delta
        if updated > 0:
            existing["qty"] = updated
        else:
            rows.remove(existing)
            rows_by_id.pop(asset_id, None)
    rows = clean_asset_rows(rows)

    old_connections = {
        _connection_key(row): row
        for row in merge_selected_bundle_connections(
            [],
            [{**old_bundle, "bundle_qty": old_qty}],
        )
    }
    new_connections = {
        _connection_key(row): row
        for row in merge_selected_bundle_connections(
            [],
            [{**new_bundle, "bundle_qty": new_qty}],
        )
    }
    connections = clean_asset_connections(
        room_type.get("asset_connections", room_type.get("connections", []))
    )
    connections_by_key = {
        _connection_key(row): row for row in connections
    }
    for key in set(old_connections) | set(new_connections):
        old_connection = old_connections.get(key, {})
        new_connection = new_connections.get(key, {})
        delta = int(new_connection.get("qty", 0) or 0) - int(
            old_connection.get("qty", 0) or 0
        )
        existing = connections_by_key.get(key)
        if existing is None:
            if delta > 0:
                added = dict(new_connection)
                added["qty"] = delta
                connections.append(added)
                connections_by_key[key] = added
            continue
        updated = int(existing.get("qty", 1) or 1) + delta
        if updated > 0:
            existing["qty"] = updated
        else:
            connections.remove(existing)
            connections_by_key.pop(key, None)
    connections = clean_asset_connections(
        connections,
        [row["asset_id"] for row in rows],
    )

    remaining = [
        assignment
        for assignment in assignments
        if assignment["bundle_id"] != old_id
    ]
    replacement_assignment = {"bundle_id": new_id, "qty": new_qty}
    updated_assignments = clean_bundle_assignments(
        [*remaining, replacement_assignment],
        bundles_by_id,
    )

    result = deepcopy(room_type)
    result["assets"] = rows
    result["asset_ids"] = [row["asset_id"] for row in rows]
    result["asset_bundle_assignments"] = updated_assignments
    result["asset_bundle_excluded_asset_ids"] = excluded_asset_ids
    result["asset_connections"] = connections
    return result


def merge_asset_connections(existing_connections, added_connections) -> list[dict]:
    """Add connection quantities while preserving their first-seen order."""
    return clean_asset_connections(
        [
            *clean_asset_connections(existing_connections),
            *clean_asset_connections(added_connections),
        ]
    )


def merge_selected_bundle_connections(
    existing_connections,
    bundles,
    existing_asset_rows=None,
) -> list[dict]:
    """Apply selected bundle connection recipes and multipliers.

    Instance-aware rows are copied once per bundle occurrence and their local
    occurrence numbers are offset into the room's combined asset list. Legacy
    rows without occurrence numbers retain the established quantity scaling.
    ``existing_asset_rows`` should describe the room before the new bundles are
    inserted, so new occurrences do not collide with existing ones.
    """
    result = clean_asset_connections(existing_connections)
    instance_offsets = {
        row["asset_id"]: int(row.get("qty", 1) or 1)
        for row in clean_asset_rows(existing_asset_rows)
    }
    for bundle in bundles or []:
        if not isinstance(bundle, dict):
            continue
        try:
            bundle_qty = max(1, int(bundle.get("bundle_qty", 1) or 1))
        except (TypeError, ValueError):
            bundle_qty = 1
        asset_counts = {
            row["asset_id"]: int(row.get("qty", 1) or 1)
            for row in clean_asset_rows(bundle.get("assets", []))
        }
        recipes = clean_asset_connections(
            bundle.get("connections", bundle.get("asset_connections", []))
        )
        scaled = []
        for connection in recipes:
            if not connection.get("from_asset_instance") and not connection.get(
                "to_asset_instance"
            ):
                # "All instances / automatic" is a per-occurrence recipe.
                # Prefer the target count (for example one power lead per
                # powered asset); fall back to the source count when only the
                # source is a tracked bundle asset.
                automatic_instances = asset_counts.get(
                    _text(connection.get("to_asset_id")),
                    asset_counts.get(
                        _text(connection.get("from_asset_id")),
                        1,
                    ),
                )
                scaled.append(
                    {
                        **connection,
                        "qty": int(connection.get("qty", 1) or 1)
                        * max(1, int(automatic_instances or 1))
                        * bundle_qty,
                    }
                )
        for bundle_index in range(bundle_qty):
            for connection in recipes:
                from_instance = int(
                    connection.get("from_asset_instance", 0) or 0
                )
                to_instance = int(connection.get("to_asset_instance", 0) or 0)
                if not from_instance and not to_instance:
                    continue
                row = dict(connection)
                from_asset_id = _text(row.get("from_asset_id"))
                to_asset_id = _text(row.get("to_asset_id"))
                automatic_instances = (
                    asset_counts.get(to_asset_id, 1)
                    if to_asset_id and not to_instance
                    else (
                        asset_counts.get(from_asset_id, 1)
                        if from_asset_id and not from_instance
                        else 1
                    )
                )
                row["qty"] = int(row.get("qty", 1) or 1) * max(
                    1, int(automatic_instances or 1)
                )
                if from_asset_id and from_instance:
                    row["from_asset_instance"] = (
                        instance_offsets.get(from_asset_id, 0)
                        + (bundle_index * asset_counts.get(from_asset_id, 0))
                        + from_instance
                    )
                if to_asset_id and to_instance:
                    row["to_asset_instance"] = (
                        instance_offsets.get(to_asset_id, 0)
                        + (bundle_index * asset_counts.get(to_asset_id, 0))
                        + to_instance
                    )
                scaled.append(row)
        result = merge_asset_connections(result, scaled)
        for asset_id, quantity in asset_counts.items():
            instance_offsets[asset_id] = (
                instance_offsets.get(asset_id, 0) + (quantity * bundle_qty)
            )
    return result


def _connection_key(connection: dict) -> tuple:
    return (
        _text(connection.get("from_asset_id")),
        int(connection.get("from_asset_instance", 0) or 0),
        _text(connection.get("from_output_id")),
        _text(connection.get("to_asset_id")),
        int(connection.get("to_asset_instance", 0) or 0),
        _text(connection.get("to_input_id")),
        _text(connection.get("port_type")),
        _text(connection.get("connection_asset_id")),
    )


def resolve_room_type_asset_connections(room_type, asset_bundles=None) -> list[dict]:
    """Return the effective saved and bundle-derived connection recipes.

    Current room types normally contain materialised copies of their bundle
    connections. Older projects can contain only the bundle assignment. Bundle
    quantities therefore act as a minimum for each identical connection,
    rather than being added again to an already materialised room recipe.
    """

    if not isinstance(room_type, dict):
        return []
    direct = clean_asset_connections(
        room_type.get("asset_connections", room_type.get("connections", []))
    )
    bundles = normalise_asset_bundles(asset_bundles)
    bundles_by_id = {bundle["id"]: bundle for bundle in bundles}
    excluded_asset_ids = clean_bundle_asset_exclusions(
        room_type.get("asset_bundle_excluded_asset_ids", [])
    )
    selected = []
    for assignment in clean_bundle_assignments(
        room_type.get("asset_bundle_assignments", []),
        bundles_by_id,
    ):
        bundle = bundles_by_id.get(assignment["bundle_id"])
        if bundle:
            bundle = bundle_without_excluded_assets(
                bundle,
                excluded_asset_ids,
            )
            selected.append({**bundle, "bundle_qty": assignment["qty"]})
    expected = merge_selected_bundle_connections([], selected)
    if not expected:
        return direct

    result = [dict(connection) for connection in direct]
    result_by_key = {_connection_key(connection): connection for connection in result}
    for connection in expected:
        key = _connection_key(connection)
        current = result_by_key.get(key)
        if current is None:
            current = dict(connection)
            result.append(current)
            result_by_key[key] = current
        else:
            current["qty"] = max(
                int(current.get("qty", 0) or 0),
                int(connection.get("qty", 0) or 0),
            )
    return clean_asset_connections(result)


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
        excluded_asset_ids = clean_bundle_asset_exclusions(
            room_type.get("asset_bundle_excluded_asset_ids", [])
        )

        deltas = {}
        old_selected_bundles = []
        new_selected_bundles = []
        for assignment in assignments:
            bundle_id = assignment["bundle_id"]
            multiplier = assignment["qty"]
            old_bundle = old_by_id.get(bundle_id)
            new_bundle = new_by_id.get(bundle_id)
            old_bundle = bundle_without_excluded_assets(
                old_bundle,
                excluded_asset_ids,
            )
            new_bundle = bundle_without_excluded_assets(
                new_bundle,
                excluded_asset_ids,
            )
            if old_bundle:
                old_selected_bundles.append(
                    {**old_bundle, "bundle_qty": multiplier}
                )
            if new_bundle:
                new_selected_bundles.append(
                    {**new_bundle, "bundle_qty": multiplier}
                )
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

        old_bundle_connections = {
            _connection_key(row): row
            for row in merge_selected_bundle_connections(
                [], old_selected_bundles
            )
        }
        new_bundle_connections = {
            _connection_key(row): row
            for row in merge_selected_bundle_connections(
                [], new_selected_bundles
            )
        }
        connection_deltas = {}
        connection_templates = {}
        for key in set(old_bundle_connections) | set(new_bundle_connections):
            old_row = old_bundle_connections.get(key, {})
            new_row = new_bundle_connections.get(key, {})
            delta = int(new_row.get("qty", 0) or 0) - int(
                old_row.get("qty", 0) or 0
            )
            if delta:
                connection_deltas[key] = delta
                connection_templates[key] = dict(new_row or old_row)

        rows = clean_asset_rows(room_type.get("assets", []))
        rows_by_id = {row["asset_id"]: row for row in rows}
        before = deepcopy(rows)
        connections = clean_asset_connections(
            room_type.get("asset_connections", room_type.get("connections", []))
        )
        connections_by_key = {
            _connection_key(row): row
            for row in connections
        }
        before_connections = deepcopy(connections)
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

        for key, delta in connection_deltas.items():
            existing = connections_by_key.get(key)
            if existing is None:
                if delta > 0:
                    row = dict(connection_templates[key])
                    row["qty"] = delta
                    connections.append(row)
                    connections_by_key[key] = row
                continue
            updated_quantity = int(existing.get("qty", 1) or 1) + delta
            if updated_quantity > 0:
                existing["qty"] = updated_quantity
            else:
                connections.remove(existing)
                connections_by_key.pop(key, None)

        connections = clean_asset_connections(
            connections,
            [row["asset_id"] for row in rows],
        )
        remaining_assignments = clean_bundle_assignments(
            assignments,
            new_by_id,
        )
        room_type["asset_bundle_assignments"] = remaining_assignments
        if rows != before:
            room_type["assets"] = rows
            room_type["asset_ids"] = [row["asset_id"] for row in rows]
        if connections != before_connections:
            room_type["asset_connections"] = connections
        if (
            rows != before
            or connections != before_connections
            or remaining_assignments != assignments
        ):
            changed_room_ids.append(
                _text(room_type.get("id")) or f"Room type {room_index}"
            )

    return changed_room_ids
