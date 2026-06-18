"""Command-line migration and verification utility for CableRouteResolver."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

from models import JsonStore
from project_sqlite import DEFAULT_EXTENSION, SQLiteProjectFile


def _default_destination(source: Path) -> Path:
    if source.suffix.lower() == ".json":
        return source.with_suffix(DEFAULT_EXTENSION)
    return source.with_name(source.stem + "_converted" + DEFAULT_EXTENSION)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Convert CableRouteResolver JSON projects to SQLite or verify/export an existing SQLite project."
    )
    parser.add_argument("source", type=Path, help="Source .json or .crsdb project")
    parser.add_argument("destination", type=Path, nargs="?", help="Destination .crsdb path")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing destination")
    parser.add_argument("--export-json", type=Path, help="Also export a JSON copy after loading")
    parser.add_argument("--verify-only", action="store_true", help="Verify an existing SQLite project without converting it")
    args = parser.parse_args(argv)

    source = args.source.expanduser().resolve()
    if not source.exists():
        parser.error(f"Source project does not exist: {source}")

    if args.verify_only:
        errors = SQLiteProjectFile(source).verify()
        if errors:
            for error in errors:
                print(f"ERROR: {error}", file=sys.stderr)
            return 1
        stats = SQLiteProjectFile(source).statistics()
        print(f"Verified: {source}")
        print(f"Schema version: {stats['schema_version']}")
        print(f"Indexed records: {stats['indexed_records']}")
        print(f"File size: {stats['file_size_bytes']} bytes")
        return 0

    destination = (args.destination or _default_destination(source)).expanduser().resolve()
    if destination.suffix.lower() != DEFAULT_EXTENSION:
        destination = destination.with_suffix(DEFAULT_EXTENSION)
    if destination.exists() and not args.force:
        parser.error(f"Destination already exists: {destination} (use --force to replace it)")

    store = JsonStore.from_file(str(source))
    store.save(str(destination))
    errors = SQLiteProjectFile(destination).verify()
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(f"Converted: {source}")
    print(f"SQLite project: {destination}")
    statistics = store.last_save_statistics
    if statistics is not None:
        print(f"Changed chunks: {statistics.changed_chunks}")
        print(f"Indexed records: {statistics.indexed_records}")
        print(f"File size: {statistics.file_size_bytes} bytes")

    if args.export_json:
        export_path = args.export_json.expanduser().resolve()
        store.save(str(export_path))
        print(f"JSON export: {export_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
