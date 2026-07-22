"""Portable JSON import/export for project and network asset libraries."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Iterable, Optional


ASSET_PACK_FORMAT = "cable-route-resolver-asset-pack"
ASSET_PACK_VERSION = 1
SUPPORTED_LIBRARY_TYPES = {"assets", "network_assets"}
IMPORT_ACTION_MAP = "map"
IMPORT_ACTION_CREATE = "create"
IMPORT_ACTION_REJECT = "reject"
SUPPORTED_IMPORT_ACTIONS = {
    IMPORT_ACTION_MAP,
    IMPORT_ACTION_CREATE,
    IMPORT_ACTION_REJECT,
}


class AssetPackError(ValueError):
    """Raised when an asset pack cannot be validated."""


def build_asset_pack(
    library_type: str,
    assets: Iterable[dict],
    *,
    name: str = "",
    related: Optional[dict] = None,
    metadata: Optional[dict] = None,
) -> dict:
    library_type = str(library_type or "").strip()
    if library_type not in SUPPORTED_LIBRARY_TYPES:
        raise AssetPackError(f"Unsupported asset library type: {library_type}")
    rows = [deepcopy(row) for row in assets if isinstance(row, dict)]
    return {
        "format": ASSET_PACK_FORMAT,
        "version": ASSET_PACK_VERSION,
        "library_type": library_type,
        "name": str(name or "").strip(),
        "assets": rows,
        "related": deepcopy(related or {}),
        "metadata": deepcopy(metadata or {}),
    }


def write_asset_pack(
    path,
    library_type: str,
    assets: Iterable[dict],
    *,
    name: str = "",
    related: Optional[dict] = None,
    metadata: Optional[dict] = None,
) -> dict:
    payload = build_asset_pack(
        library_type,
        assets,
        name=name,
        related=related,
        metadata=metadata,
    )
    Path(path).write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return payload


def read_asset_pack(path, expected_library_type: str = "") -> dict:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AssetPackError(f"Could not read asset pack: {exc}") from exc
    if not isinstance(payload, dict):
        raise AssetPackError("Asset pack must contain a JSON object.")
    if payload.get("format") != ASSET_PACK_FORMAT:
        raise AssetPackError(
            "This is not a Cable Route Resolver asset-pack file."
        )
    try:
        version = int(payload.get("version", 0) or 0)
    except (TypeError, ValueError):
        version = 0
    if version != ASSET_PACK_VERSION:
        raise AssetPackError(
            f"Unsupported asset-pack version {version}; expected {ASSET_PACK_VERSION}."
        )
    library_type = str(payload.get("library_type", "") or "").strip()
    if library_type not in SUPPORTED_LIBRARY_TYPES:
        raise AssetPackError(f"Unsupported asset library type: {library_type}")
    if expected_library_type and library_type != expected_library_type:
        raise AssetPackError(
            f"This pack contains {library_type}, not {expected_library_type}."
        )
    rows = payload.get("assets", [])
    if not isinstance(rows, list):
        raise AssetPackError("Asset pack 'assets' must be a list.")
    cleaned = []
    seen = set()
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise AssetPackError(f"Asset row {index} is not an object.")
        asset_id = str(row.get("id", "") or "").strip()
        if not asset_id:
            raise AssetPackError(f"Asset row {index} has no ID.")
        if asset_id in seen:
            raise AssetPackError(f"Asset ID {asset_id} occurs more than once.")
        seen.add(asset_id)
        cleaned.append(deepcopy(row))
    payload["assets"] = cleaned
    if not isinstance(payload.get("related"), dict):
        payload["related"] = {}
    return payload


def merge_asset_rows(existing, incoming, *, replace_existing: bool) -> tuple:
    """Merge by asset ID, retaining order and returning merge counts."""
    merged = [deepcopy(row) for row in existing if isinstance(row, dict)]
    positions = {
        str(row.get("id", "") or "").strip(): index
        for index, row in enumerate(merged)
        if str(row.get("id", "") or "").strip()
    }
    added = replaced = skipped = 0
    imported_ids = []
    for source in incoming:
        row = deepcopy(source)
        asset_id = str(row.get("id", "") or "").strip()
        if asset_id in positions:
            if replace_existing:
                merged[positions[asset_id]] = row
                replaced += 1
                imported_ids.append(asset_id)
            else:
                skipped += 1
            continue
        positions[asset_id] = len(merged)
        merged.append(row)
        added += 1
        imported_ids.append(asset_id)
    return merged, {
        "added": added,
        "replaced": replaced,
        "skipped": skipped,
        "imported_ids": imported_ids,
    }


def marshal_asset_rows(existing, incoming, resolutions) -> tuple:
    """Apply explicit per-row import resolutions without mutating the inputs.

    ``resolutions`` contains one row per incoming asset with ``source_id``,
    ``action`` and, for map/create actions, ``target_id``. Mapping acknowledges
    that the imported definition is represented by an existing local asset;
    creating copies the incoming definition under the selected local ID.
    """
    existing_rows = [deepcopy(row) for row in existing if isinstance(row, dict)]
    incoming_rows = [deepcopy(row) for row in incoming if isinstance(row, dict)]
    existing_ids = {
        str(row.get("id", "") or "").strip()
        for row in existing_rows
        if str(row.get("id", "") or "").strip()
    }
    resolution_by_source = {}
    for raw in resolutions:
        if not isinstance(raw, dict):
            raise AssetPackError("Each asset import resolution must be an object.")
        source_id = str(raw.get("source_id", "") or "").strip()
        action = str(raw.get("action", "") or "").strip().lower()
        target_id = str(raw.get("target_id", "") or "").strip()
        if not source_id:
            raise AssetPackError("An asset import resolution has no source ID.")
        if source_id in resolution_by_source:
            raise AssetPackError(f"Asset {source_id} has more than one import resolution.")
        if action not in SUPPORTED_IMPORT_ACTIONS:
            raise AssetPackError(f"Asset {source_id} has an unsupported import action: {action}")
        resolution_by_source[source_id] = {
            "source_id": source_id,
            "action": action,
            "target_id": target_id,
        }

    incoming_ids = {
        str(row.get("id", "") or "").strip() for row in incoming_rows
    }
    missing = sorted(incoming_ids - set(resolution_by_source))
    unexpected = sorted(set(resolution_by_source) - incoming_ids)
    if missing:
        raise AssetPackError(
            "No import resolution was supplied for: " + ", ".join(missing)
        )
    if unexpected:
        raise AssetPackError(
            "Import resolutions reference unknown assets: " + ", ".join(unexpected)
        )

    created_targets = set()
    for source_id, resolution in resolution_by_source.items():
        action = resolution["action"]
        target_id = resolution["target_id"]
        if action == IMPORT_ACTION_REJECT:
            continue
        if not target_id:
            raise AssetPackError(f"Asset {source_id} has no target asset ID.")
        if action == IMPORT_ACTION_MAP:
            if target_id not in existing_ids:
                raise AssetPackError(
                    f"Asset {source_id} maps to unknown existing asset {target_id}."
                )
            continue
        if target_id in existing_ids or target_id in created_targets:
            raise AssetPackError(
                f"New asset ID {target_id} is already used by another asset."
            )
        created_targets.add(target_id)

    merged = list(existing_rows)
    created_ids = []
    created_source_ids = []
    mapped_ids = []
    rejected_ids = []
    source_to_target = {}
    for row in incoming_rows:
        source_id = str(row.get("id", "") or "").strip()
        resolution = resolution_by_source[source_id]
        action = resolution["action"]
        if action == IMPORT_ACTION_REJECT:
            rejected_ids.append(source_id)
            continue
        target_id = resolution["target_id"]
        source_to_target[source_id] = target_id
        if action == IMPORT_ACTION_MAP:
            mapped_ids.append(target_id)
            continue
        row["id"] = target_id
        merged.append(row)
        created_ids.append(target_id)
        created_source_ids.append(source_id)

    return merged, {
        "added": len(created_ids),
        "mapped": len(mapped_ids),
        "rejected": len(rejected_ids),
        "created_ids": created_ids,
        "created_source_ids": created_source_ids,
        "mapped_ids": mapped_ids,
        "rejected_ids": rejected_ids,
        "accepted_source_ids": list(source_to_target),
        "source_to_target": source_to_target,
        "imported_ids": created_ids + mapped_ids,
    }
