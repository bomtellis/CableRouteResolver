#!/usr/bin/env python3
"""Create or validate Cable Route Resolver asset-pack JSON files."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys


FORMAT = "cable-route-resolver-asset-pack"
VERSION = 1
LIBRARY_TYPES = {"assets", "network_assets"}


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load_rows(path: Path) -> list[dict]:
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    payload = load_json(path)
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get("assets"), list):
        rows = payload["assets"]
    else:
        raise ValueError("Input JSON must be an asset array or contain an 'assets' array.")
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError("Every input asset must be an object.")
    return rows


def integer(value, default: int, minimum: int) -> int:
    if value in (None, ""):
        return default
    result = int(float(value))
    if result < minimum:
        raise ValueError(f"Expected an integer >= {minimum}, got {value!r}.")
    return result


def factor(value) -> float:
    if value in (None, ""):
        return 1.0
    result = float(value)
    if not 0.0 <= result <= 1.0:
        raise ValueError(f"Concurrency factor must be between 0 and 1, got {value!r}.")
    return result


def capability_list(value) -> list[str]:
    if isinstance(value, list):
        values = value
    else:
        values = str(value or "").replace("\n", ";").replace(",", ";").split(";")
    output = []
    seen = set()
    for item in values:
        label = str(item or "").strip()
        key = label.casefold()
        if label and key not in seen:
            output.append(label)
            seen.add(key)
    return output


def normalize_project_asset(source: dict) -> dict:
    row = dict(source)
    row["id"] = str(row.get("id", "") or "").strip()
    row["name"] = str(row.get("name", row["id"]) or row["id"]).strip()
    row["ADB_Code"] = str(row.get("ADB_Code", row.get("adb_code", "")) or "").strip()
    row["Group"] = str(row.get("Group", row.get("group", "")) or "").strip()
    row["qty"] = integer(row.get("qty"), 1, 1)
    row["data_points"] = integer(
        row.get("data_points", row.get("data_points_each", row.get("cables"))), 1, 0
    )
    method = str(row.get("connection_type", row.get("connection_method", "wired")) or "wired")
    wireless_tokens = ("wireless", "wi-fi", "wifi", "w-fi")
    row["connection_type"] = (
        "wireless" if any(token in method.casefold() for token in wireless_tokens) else "wired"
    )
    row["category_id"] = str(row.get("category_id", row.get("category", "")) or "").strip()
    values = capability_list(row.get("capabilities", row.get("capability_keywords", "")))
    row["capabilities"] = values
    row["capability_keywords"] = "; ".join(values)
    row["north_south_concurrency_factor"] = factor(row.get("north_south_concurrency_factor"))
    row["east_west_concurrency_factor"] = factor(row.get("east_west_concurrency_factor"))
    return row


def validate(payload: dict, expected_library_type: str = "") -> dict:
    if not isinstance(payload, dict) or payload.get("format") != FORMAT:
        raise ValueError("This is not a Cable Route Resolver asset-pack object.")
    if int(payload.get("version", 0) or 0) != VERSION:
        raise ValueError(f"Unsupported asset-pack version; expected {VERSION}.")
    library_type = str(payload.get("library_type", "") or "").strip()
    if library_type not in LIBRARY_TYPES:
        raise ValueError(f"Unsupported library_type: {library_type!r}.")
    if expected_library_type and library_type != expected_library_type:
        raise ValueError(f"Pack contains {library_type}, not {expected_library_type}.")
    rows = payload.get("assets")
    if not isinstance(rows, list):
        raise ValueError("Pack 'assets' must be a list.")
    seen = set()
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            raise ValueError(f"Asset row {index} is not an object.")
        asset_id = str(row.get("id", "") or "").strip()
        if not asset_id:
            raise ValueError(f"Asset row {index} has no ID.")
        if asset_id in seen:
            raise ValueError(f"Asset ID {asset_id} occurs more than once.")
        seen.add(asset_id)
        if library_type == "assets":
            if not str(row.get("name", "") or "").strip():
                raise ValueError(f"Project asset {asset_id} has no name.")
            integer(row.get("qty"), 1, 1)
            integer(row.get("data_points", row.get("data_points_each", row.get("cables"))), 1, 0)
            connection_type = str(row.get("connection_type", "wired") or "wired").strip()
            if connection_type not in {"wired", "wireless"}:
                raise ValueError(f"Project asset {asset_id} has invalid connection_type {connection_type!r}.")
            factor(row.get("north_south_concurrency_factor"))
            factor(row.get("east_west_concurrency_factor"))
    if library_type == "assets":
        related = payload.get("related", {})
        category_rows = related.get("asset_categories", []) if isinstance(related, dict) else []
        category_ids = [str(row.get("id", "") or "").strip() for row in category_rows if isinstance(row, dict)]
        if len(category_ids) != len(set(category_ids)):
            raise ValueError("Related asset category IDs must be unique.")
        defined = set(category_ids)
        missing = sorted({str(row.get("category_id", "") or "").strip() for row in rows} - defined - {""})
        if missing:
            raise ValueError(f"Undefined asset category IDs: {', '.join(missing)}.")
    return payload


def load_categories(path: Path | None) -> list[dict]:
    if path is None:
        return []
    payload = load_json(path)
    if isinstance(payload, dict):
        payload = payload.get("asset_categories", payload.get("categories"))
    if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
        raise ValueError("Categories must be a JSON array or an object containing one.")
    return payload


def create_pack(args) -> None:
    rows = load_rows(args.input)
    if args.library_type == "assets":
        rows = [normalize_project_asset(row) for row in rows]
    payload = {
        "format": FORMAT,
        "version": VERSION,
        "library_type": args.library_type,
        "name": args.name.strip(),
        "assets": rows,
        "related": {"asset_categories": load_categories(args.categories)} if args.categories else {},
        "metadata": {"source_file": args.input.name},
    }
    validate(payload, args.library_type)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"path": str(args.output.resolve()), "library_type": args.library_type, "assets": len(rows)}, indent=2))


def validate_file(args) -> None:
    payload = validate(load_json(args.path), args.library_type)
    print(json.dumps({"path": str(args.path.resolve()), "library_type": payload["library_type"], "assets": len(payload["assets"])}, indent=2))


def build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    create_parser = commands.add_parser("create", help="Create a pack from normalized CSV or JSON")
    create_parser.add_argument("--input", type=Path, required=True)
    create_parser.add_argument("--output", type=Path, required=True)
    create_parser.add_argument("--library-type", choices=sorted(LIBRARY_TYPES), required=True)
    create_parser.add_argument("--name", default="")
    create_parser.add_argument("--categories", type=Path)
    create_parser.set_defaults(func=create_pack)
    validate_parser = commands.add_parser("validate", help="Validate an existing pack")
    validate_parser.add_argument("path", type=Path)
    validate_parser.add_argument("--library-type", choices=sorted(LIBRARY_TYPES), default="")
    validate_parser.set_defaults(func=validate_file)
    return root


def main() -> int:
    args = build_parser().parse_args()
    try:
        args.func(args)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
