"""Portable JSON import/export for project and network asset libraries."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Iterable, Optional


ASSET_PACK_FORMAT = "cable-route-resolver-asset-pack"
ASSET_PACK_VERSION = 1
SUPPORTED_LIBRARY_TYPES = {"assets", "network_assets"}


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
