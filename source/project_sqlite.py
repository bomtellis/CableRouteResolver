"""SQLite-backed project persistence for CableRouteResolver.

The application still works with the established in-memory ``dict`` model, but
projects are persisted as compressed, indexed collection chunks in SQLite.  A
chunked layout keeps startup close to compact-JSON performance while allowing
unchanged chunks to be left untouched on save and providing indexed floor/
viewport queries for future lazy-loading work.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
import zlib

FORMAT_NAME = "CableRouteResolver SQLite Project"
SCHEMA_VERSION = 2
DEFAULT_EXTENSION = ".crsdb"
SQLITE_HEADER = b"SQLite format 3\x00"
DEFAULT_CHUNK_SIZE = 512
AUTO_COMPACT_MIN_FREE_BYTES = 16 * 1024 * 1024
AUTO_COMPACT_MIN_FREE_RATIO = 0.20


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _float_or_none(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_sqlite_project(path: str | Path) -> bool:
    """Return ``True`` when *path* is an SQLite project database."""

    candidate = Path(path)
    if not candidate.is_file():
        return False
    try:
        with candidate.open("rb") as handle:
            return handle.read(len(SQLITE_HEADER)) == SQLITE_HEADER
    except OSError:
        return False


def _json_bytes(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=True,
    ).encode("utf-8")


def _pack(value) -> Tuple[bytes, str]:
    raw = _json_bytes(value)
    digest = hashlib.sha256(raw).hexdigest()
    return sqlite3.Binary(zlib.compress(raw, level=1)), digest


def _unpack(payload: bytes):
    return json.loads(zlib.decompress(payload).decode("utf-8"))


def _chunks(values: Sequence, size: int) -> Iterator[Tuple[int, Sequence]]:
    chunk_size = max(1, int(size or DEFAULT_CHUNK_SIZE))
    if not values:
        yield 0, []
        return
    for start in range(0, len(values), chunk_size):
        yield start // chunk_size, values[start : start + chunk_size]


def _section_rows(data: dict, chunk_size: int) -> Iterator[Tuple[str, str, int, object]]:
    """Yield ``(section_key, kind, chunk_index, value)`` rows.

    Top-level lists are chunked.  ``corridors.nodes`` and ``corridors.edges``
    are separated from the surrounding dictionary because they are commonly
    among the largest collections in a project.
    """

    for key, value in data.items():
        if key == "corridors" and isinstance(value, dict):
            metadata = {
                child_key: child_value
                for child_key, child_value in value.items()
                if child_key not in {"nodes", "edges"}
            }
            yield "corridors", "scalar", 0, metadata
            for child_key in ("nodes", "edges"):
                rows = value.get(child_key, [])
                if not isinstance(rows, list):
                    rows = []
                for chunk_index, chunk in _chunks(rows, chunk_size):
                    yield f"corridors.{child_key}", "list", chunk_index, chunk
            continue

        if isinstance(value, list):
            for chunk_index, chunk in _chunks(value, chunk_size):
                yield key, "list", chunk_index, chunk
        else:
            yield key, "scalar", 0, value


def _record_index_values(item: object) -> Tuple[str, str, Optional[int], str, Optional[float], Optional[float], str]:
    if not isinstance(item, dict):
        return "", "", None, "", None, None, ""
    record_id = _text(
        item.get("id", item.get("public_id", item.get("name", item.get("key", ""))))
    )
    record_name = _text(item.get("name", item.get("label", record_id)))
    floor = _int_or_none(item.get("floor"))
    kind = _text(
        item.get(
            "kind",
            item.get(
                "asset_type",
                item.get("node_type", item.get("design_role", item.get("type", ""))),
            ),
        )
    )
    x = _float_or_none(item.get("x"))
    y = _float_or_none(item.get("y"))
    parent_id = _text(
        item.get(
            "parent_id",
            item.get(
                "parent_node_id",
                item.get("from_instance_id", item.get("from", item.get("source", ""))),
            ),
        )
    )
    return record_id, record_name, floor, kind, x, y, parent_id


def _configure_connection(connection: sqlite3.Connection, *, writable: bool) -> None:
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 10000")
    connection.execute("PRAGMA temp_store = MEMORY")
    connection.execute("PRAGMA cache_size = -65536")
    connection.execute("PRAGMA mmap_size = 268435456")
    if writable:
        # A rollback journal retains the expected single-file project behaviour
        # once the transaction completes.  WAL is not useful here because the
        # application opens short-lived connections for load/save operations.
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("PRAGMA synchronous = NORMAL")


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS project_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS project_sections (
            section_key TEXT NOT NULL,
            section_kind TEXT NOT NULL CHECK(section_kind IN ('scalar', 'list')),
            chunk_index INTEGER NOT NULL,
            record_count INTEGER NOT NULL,
            payload BLOB NOT NULL,
            payload_hash TEXT NOT NULL,
            PRIMARY KEY(section_key, chunk_index)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS project_record_index (
            section_key TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            item_index INTEGER NOT NULL,
            ordinal INTEGER NOT NULL,
            record_id TEXT NOT NULL DEFAULT '',
            record_name TEXT NOT NULL DEFAULT '',
            floor INTEGER,
            kind TEXT NOT NULL DEFAULT '',
            x REAL,
            y REAL,
            parent_id TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(section_key, chunk_index, item_index),
            FOREIGN KEY(section_key, chunk_index)
                REFERENCES project_sections(section_key, chunk_index)
                ON DELETE CASCADE
        ) WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS idx_project_record_section_floor
            ON project_record_index(section_key, floor);
        CREATE INDEX IF NOT EXISTS idx_project_record_section_kind
            ON project_record_index(section_key, kind);
        CREATE INDEX IF NOT EXISTS idx_project_record_id
            ON project_record_index(record_id);
        CREATE INDEX IF NOT EXISTS idx_project_record_name
            ON project_record_index(record_name);
        CREATE INDEX IF NOT EXISTS idx_project_record_parent
            ON project_record_index(parent_id);
        CREATE INDEX IF NOT EXISTS idx_project_record_xy
            ON project_record_index(section_key, floor, x, y);

        CREATE TABLE IF NOT EXISTS project_revisions (
            revision_number INTEGER PRIMARY KEY,
            created_utc TEXT NOT NULL,
            notes TEXT NOT NULL,
            changed_chunks INTEGER NOT NULL DEFAULT 0,
            unchanged_chunks INTEGER NOT NULL DEFAULT 0,
            deleted_chunks INTEGER NOT NULL DEFAULT 0,
            indexed_records INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS project_revision_blobs (
            payload_hash TEXT PRIMARY KEY,
            section_kind TEXT NOT NULL CHECK(section_kind IN ('scalar', 'list')),
            record_count INTEGER NOT NULL,
            payload BLOB NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS project_revision_sections (
            revision_number INTEGER NOT NULL,
            section_key TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            payload_hash TEXT NOT NULL,
            PRIMARY KEY(revision_number, section_key, chunk_index),
            FOREIGN KEY(revision_number)
                REFERENCES project_revisions(revision_number)
                ON DELETE CASCADE,
            FOREIGN KEY(payload_hash)
                REFERENCES project_revision_blobs(payload_hash)
        ) WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS idx_project_revision_sections_hash
            ON project_revision_sections(payload_hash);
        """
    )


@dataclass(frozen=True)
class DatabaseSpaceStatistics:
    path: str
    file_size_bytes: int
    page_size_bytes: int
    page_count: int
    free_page_count: int
    reclaimable_bytes: int
    free_ratio: float


@dataclass(frozen=True)
class CompactionStatistics:
    path: str
    compacted: bool
    file_size_before_bytes: int
    file_size_after_bytes: int
    reclaimed_bytes: int
    reclaimable_before_bytes: int
    free_ratio_before: float


@dataclass(frozen=True)
class SaveStatistics:
    path: str
    revision_number: int
    revision_notes: str
    revision_created: bool
    changed_chunks: int
    unchanged_chunks: int
    deleted_chunks: int
    indexed_records: int
    file_size_bytes: int
    compacted: bool = False
    reclaimed_bytes: int = 0
    reclaimable_before_bytes: int = 0
    free_ratio_before: float = 0.0
    compaction_error: str = ""


class SQLiteProjectFile:
    """Read, write and query a CableRouteResolver SQLite project."""

    def __init__(self, path: str | Path, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.path = Path(path)
        self.chunk_size = max(32, int(chunk_size or DEFAULT_CHUNK_SIZE))

    @staticmethod
    def _section_label(section_key: str) -> str:
        labels = {
            "project": "project details",
            "building": "building settings",
            "departments": "departments",
            "room_types": "room types",
            "room_type_asset_review": "room type asset review",
            "room_type_asset_staging": "staged room type asset changes",
            "room_type_asset_commits": "room type asset commits",
            "room_type_asset_rfi": "room type asset RFI list",
            "revision_change_log": "detailed change log",
            "room_type_scenario_groups": "room type scenario groups",
            "asset_scenario_groups": "asset scenario groups",
            "room_type_asset_scenarios": "room type asset scenarios",
            "asset_bundles": "asset bundles",
            "asset_categories": "asset categories",
            "assets": "assets",
            "retired_asset_ids": "retired asset IDs",
            "locations": "locations",
            "equipment_room_placement_zones": "equipment room placement zones",
            "data_points": "data points",
            "corridors": "corridor settings",
            "corridors.nodes": "corridor nodes",
            "corridors.edges": "corridor edges",
            "transitions": "transitions",
            "floor_dxf_files": "floor DXF mapping",
            "connections": "connections",
            "route_profiles": "route profiles",
        }
        if section_key in labels:
            return labels[section_key]
        return section_key.replace("_", " ").replace(".", " ")

    @classmethod
    def _revision_notes(
        cls,
        *,
        had_existing_project: bool,
        changed_sections: Iterable[str],
        deleted_sections: Iterable[str],
        changed_chunks: int,
        deleted_chunks: int,
        detailed_changes: Iterable[str] = (),
        commit_message: str = "Project saved",
    ) -> str:
        changed = sorted({cls._section_label(section) for section in changed_sections})
        deleted = sorted({cls._section_label(section) for section in deleted_sections})
        exact = [_text(item) for item in detailed_changes if _text(item)]
        if not had_existing_project:
            return "Initial commit"

        parts = []
        if changed:
            shown = ", ".join(changed[:8])
            if len(changed) > 8:
                shown += f", and {len(changed) - 8} more section(s)"
            parts.append(f"Updated {shown}")
        if deleted:
            shown = ", ".join(deleted[:6])
            if len(deleted) > 6:
                shown += f", and {len(deleted) - 6} more removed section(s)"
            parts.append(f"Removed {shown}")
        chunk_text = f"{changed_chunks} changed chunk(s)"
        if deleted_chunks:
            chunk_text += f", {deleted_chunks} deleted chunk(s)"
        summary = "; ".join(parts)
        base = f"{summary}. {chunk_text}." if summary else f"{chunk_text}."
        message = _text(commit_message) or "Project saved"
        return " | ".join([message] + exact + [base])

    @staticmethod
    def _snapshot_revision(connection: sqlite3.Connection, revision_number: int) -> None:
        """Attach a deduplicated, restorable project snapshot to a revision."""

        connection.execute(
            """
            INSERT OR IGNORE INTO project_revision_blobs(
                payload_hash, section_kind, record_count, payload
            )
            SELECT payload_hash, section_kind, record_count, payload
            FROM project_sections
            """
        )
        connection.execute(
            """
            INSERT INTO project_revision_sections(
                revision_number, section_key, chunk_index, payload_hash
            )
            SELECT ?, section_key, chunk_index, payload_hash
            FROM project_sections
            """,
            (int(revision_number),),
        )

    @classmethod
    def _insert_revision(
        cls,
        connection: sqlite3.Connection,
        *,
        revision_number: int,
        created_utc: str,
        notes: str,
        changed_chunks: int,
        unchanged_chunks: int,
        deleted_chunks: int,
        indexed_records: int,
    ) -> None:
        connection.execute(
            """
            INSERT INTO project_revisions(
                revision_number, created_utc, notes,
                changed_chunks, unchanged_chunks, deleted_chunks, indexed_records
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(revision_number),
                _text(created_utc),
                _text(notes),
                int(changed_chunks),
                int(unchanged_chunks),
                int(deleted_chunks),
                int(indexed_records),
            ),
        )
        cls._snapshot_revision(connection, revision_number)

    @staticmethod
    def _data_from_section_rows(rows: Iterable[Tuple[str, str, int, bytes]]) -> dict:
        data: Dict[str, object] = {}
        list_sections: Dict[str, List[object]] = {}
        for section_key, section_kind, _chunk_index, payload in rows:
            value = _unpack(payload)
            if section_kind == "list":
                target = list_sections.setdefault(section_key, [])
                if isinstance(value, list):
                    target.extend(value)
            else:
                data[section_key] = value

        for section_key, section_rows in list_sections.items():
            if section_key == "corridors.nodes":
                corridors = data.setdefault("corridors", {})
                if not isinstance(corridors, dict):
                    corridors = {}
                    data["corridors"] = corridors
                corridors["nodes"] = section_rows
            elif section_key == "corridors.edges":
                corridors = data.setdefault("corridors", {})
                if not isinstance(corridors, dict):
                    corridors = {}
                    data["corridors"] = corridors
                corridors["edges"] = section_rows
            else:
                data[section_key] = section_rows
        return data

    def load(self) -> dict:
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        if not is_sqlite_project(self.path):
            raise ValueError(f"Not an SQLite project: {self.path}")

        connection = sqlite3.connect(str(self.path))
        try:
            _configure_connection(connection, writable=False)
            meta = dict(connection.execute("SELECT key, value FROM project_meta"))
            if meta.get("format_name") != FORMAT_NAME:
                raise ValueError(
                    "The SQLite file is not a CableRouteResolver project database."
                )
            schema_version = int(meta.get("schema_version", "0") or 0)
            if schema_version > SCHEMA_VERSION:
                raise ValueError(
                    f"Project schema version {schema_version} is newer than this application supports ({SCHEMA_VERSION})."
                )

            cursor = connection.execute(
                """
                SELECT section_key, section_kind, chunk_index, payload
                FROM project_sections
                ORDER BY section_key, chunk_index
                """
            )
            return self._data_from_section_rows(cursor)
        finally:
            connection.close()

    def load_revision(self, revision_number: int) -> dict:
        """Load a restorable revision without changing the current project."""

        revision_number = int(revision_number)
        connection = sqlite3.connect(str(self.path))
        try:
            _configure_connection(connection, writable=False)
            exists = connection.execute(
                "SELECT 1 FROM project_revisions WHERE revision_number = ?",
                (revision_number,),
            ).fetchone()
            if not exists:
                raise ValueError(f"Revision {revision_number} was not found.")
            cursor = connection.execute(
                """
                SELECT sections.section_key, blobs.section_kind,
                       sections.chunk_index, blobs.payload
                FROM project_revision_sections AS sections
                JOIN project_revision_blobs AS blobs
                  ON blobs.payload_hash = sections.payload_hash
                WHERE sections.revision_number = ?
                ORDER BY sections.section_key, sections.chunk_index
                """,
                (revision_number,),
            )
            rows = cursor.fetchall()
            if not rows:
                raise ValueError(
                    f"Revision {revision_number} predates restorable project commits."
                )
            return self._data_from_section_rows(rows)
        except sqlite3.OperationalError as exc:
            if "project_revision_" in str(exc):
                raise ValueError(
                    f"Revision {revision_number} predates restorable project commits."
                ) from exc
            raise
        finally:
            connection.close()

    def restore_revision(self, revision_number: int) -> Tuple[dict, SaveStatistics]:
        """Restore a revision and record that rollback as a new commit."""

        revision_number = int(revision_number)
        current = self.load()
        restored = self.load_revision(revision_number)
        active_asset_ids = {
            _text(asset.get("id"))
            for asset in restored.get("assets", []) or []
            if isinstance(asset, dict) and _text(asset.get("id"))
        }
        retired_asset_ids = {
            _text(asset_id)
            for payload in (current, restored)
            for asset_id in payload.get("retired_asset_ids", []) or []
            if _text(asset_id)
        }
        restored["retired_asset_ids"] = sorted(
            retired_asset_ids - active_asset_ids, key=str.casefold
        )
        for room_type in restored.get("room_types", []) or []:
            if not isinstance(room_type, dict):
                continue
            rows = [
                row
                for row in room_type.get("assets", []) or []
                if isinstance(row, dict)
                and _text(row.get("asset_id", row.get("id"))) in active_asset_ids
            ]
            legacy_ids = [
                _text(asset_id)
                for asset_id in room_type.get("asset_ids", []) or []
                if _text(asset_id) in active_asset_ids
            ]
            if "assets" in room_type:
                room_type["assets"] = rows
                room_type["asset_ids"] = [
                    _text(row.get("asset_id", row.get("id"))) for row in rows
                ]
            else:
                room_type["asset_ids"] = legacy_ids
        statistics = self.save(
            restored,
            source_path=str(self.path),
            commit_message=f"Rollback to revision {revision_number}",
        )
        return restored, statistics

    def space_usage(self) -> DatabaseSpaceStatistics:
        """Return file and free-page information for maintenance decisions."""

        if not self.path.exists() or not is_sqlite_project(self.path):
            return DatabaseSpaceStatistics(
                path=str(self.path),
                file_size_bytes=self.path.stat().st_size if self.path.exists() else 0,
                page_size_bytes=0,
                page_count=0,
                free_page_count=0,
                reclaimable_bytes=0,
                free_ratio=0.0,
            )
        connection = sqlite3.connect(str(self.path))
        try:
            _configure_connection(connection, writable=False)
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0] or 0)
            page_count = int(connection.execute("PRAGMA page_count").fetchone()[0] or 0)
            free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0] or 0)
        finally:
            connection.close()
        reclaimable = max(0, page_size * free_pages)
        return DatabaseSpaceStatistics(
            path=str(self.path),
            file_size_bytes=self.path.stat().st_size,
            page_size_bytes=page_size,
            page_count=page_count,
            free_page_count=free_pages,
            reclaimable_bytes=reclaimable,
            free_ratio=(free_pages / page_count) if page_count else 0.0,
        )

    @staticmethod
    def _meets_compaction_threshold(
        usage: DatabaseSpaceStatistics,
        *,
        min_free_bytes: int,
        min_free_ratio: float,
    ) -> bool:
        return (
            usage.reclaimable_bytes >= max(0, int(min_free_bytes))
            and usage.free_ratio >= max(0.0, float(min_free_ratio))
        )

    def compact(
        self,
        *,
        force: bool = False,
        min_free_bytes: int = AUTO_COMPACT_MIN_FREE_BYTES,
        min_free_ratio: float = AUTO_COMPACT_MIN_FREE_RATIO,
    ) -> CompactionStatistics:
        """VACUUM the project when requested or when thresholds are exceeded."""

        before = self.space_usage()
        should_run = force or self._meets_compaction_threshold(
            before,
            min_free_bytes=min_free_bytes,
            min_free_ratio=min_free_ratio,
        )
        if not should_run or not self.path.exists():
            return CompactionStatistics(
                path=str(self.path),
                compacted=False,
                file_size_before_bytes=before.file_size_bytes,
                file_size_after_bytes=before.file_size_bytes,
                reclaimed_bytes=0,
                reclaimable_before_bytes=before.reclaimable_bytes,
                free_ratio_before=before.free_ratio,
            )

        connection = sqlite3.connect(str(self.path))
        try:
            _configure_connection(connection, writable=True)
            connection.execute("VACUUM")
            connection.execute("PRAGMA optimize")
        finally:
            connection.close()
        after = self.space_usage()
        return CompactionStatistics(
            path=str(self.path),
            compacted=True,
            file_size_before_bytes=before.file_size_bytes,
            file_size_after_bytes=after.file_size_bytes,
            reclaimed_bytes=max(0, before.file_size_bytes - after.file_size_bytes),
            reclaimable_before_bytes=before.reclaimable_bytes,
            free_ratio_before=before.free_ratio,
        )

    def save(
        self,
        data: dict,
        *,
        source_path: str = "",
        commit_message: str = "Project saved",
        auto_compact: bool = True,
        compact_min_free_bytes: int = AUTO_COMPACT_MIN_FREE_BYTES,
        compact_min_free_ratio: float = AUTO_COMPACT_MIN_FREE_RATIO,
    ) -> SaveStatistics:
        if not isinstance(data, dict):
            raise TypeError("Project data must be a dictionary")
        self.path.parent.mkdir(parents=True, exist_ok=True)

        connection = sqlite3.connect(str(self.path))
        changed_chunks = 0
        unchanged_chunks = 0
        indexed_records = 0
        deleted_chunks = 0
        changed_sections: set[str] = set()
        deleted_sections: set[str] = set()
        existing_change_ids: set[str] = set()
        detailed_changes: List[str] = []
        revision_number = 0
        revision_notes = ""
        revision_created = False
        try:
            _configure_connection(connection, writable=True)
            _create_schema(connection)
            existing_hashes = {
                (section_key, int(chunk_index)): payload_hash
                for section_key, chunk_index, payload_hash in connection.execute(
                    "SELECT section_key, chunk_index, payload_hash FROM project_sections"
                )
            }
            row = connection.execute(
                "SELECT COALESCE(MAX(revision_number), 0) FROM project_revisions"
            ).fetchone()
            revision_number = int(row[0] or 0)
            has_restorable_revisions = bool(
                connection.execute(
                    "SELECT 1 FROM project_revision_sections LIMIT 1"
                ).fetchone()
            )
            initial_revision_created = False
            for (payload,) in connection.execute(
                """
                SELECT payload FROM project_sections
                WHERE section_key = 'revision_change_log'
                ORDER BY chunk_index
                """
            ):
                rows = _unpack(payload)
                if not isinstance(rows, list):
                    continue
                existing_change_ids.update(
                    _text(item.get("id"))
                    for item in rows
                    if isinstance(item, dict) and _text(item.get("id"))
                )
            for item in data.get("revision_change_log", []) or []:
                if not isinstance(item, dict):
                    continue
                event_id = _text(item.get("id"))
                if event_id and event_id in existing_change_ids:
                    continue
                summary = _text(item.get("summary"))
                if not summary:
                    continue
                source = _text(item.get("source"))
                detailed_changes.append(f"{source}: {summary}" if source else summary)
            seen: set[Tuple[str, int]] = set()
            ordinal_by_section: Dict[str, int] = {}

            with connection:
                # Databases written before restorable commits were introduced
                # already contain the current project but no snapshot. Preserve
                # that state before applying the next save so it can be recovered.
                if existing_hashes and not has_restorable_revisions:
                    revision_number += 1
                    initial_created_utc = _utc_now()
                    existing_indexed_records = int(
                        connection.execute(
                            "SELECT COALESCE(SUM(record_count), 0) FROM project_sections"
                        ).fetchone()[0]
                        or 0
                    )
                    self._insert_revision(
                        connection,
                        revision_number=revision_number,
                        created_utc=initial_created_utc,
                        notes="Initial commit",
                        changed_chunks=len(existing_hashes),
                        unchanged_chunks=0,
                        deleted_chunks=0,
                        indexed_records=existing_indexed_records,
                    )
                    initial_revision_created = True

                for section_key, section_kind, chunk_index, value in _section_rows(
                    data, self.chunk_size
                ):
                    identity = (section_key, int(chunk_index))
                    seen.add(identity)
                    payload, payload_hash = _pack(value)
                    rows = value if section_kind == "list" and isinstance(value, list) else []
                    record_count = len(rows)
                    base_ordinal = ordinal_by_section.get(section_key, 0)
                    ordinal_by_section[section_key] = base_ordinal + record_count

                    if existing_hashes.get(identity) == payload_hash:
                        unchanged_chunks += 1
                        indexed_records += record_count
                        continue

                    connection.execute(
                        """
                        INSERT INTO project_sections(
                            section_key, section_kind, chunk_index,
                            record_count, payload, payload_hash
                        ) VALUES(?, ?, ?, ?, ?, ?)
                        ON CONFLICT(section_key, chunk_index) DO UPDATE SET
                            section_kind = excluded.section_kind,
                            record_count = excluded.record_count,
                            payload = excluded.payload,
                            payload_hash = excluded.payload_hash
                        """,
                        (
                            section_key,
                            section_kind,
                            int(chunk_index),
                            record_count,
                            payload,
                            payload_hash,
                        ),
                    )
                    connection.execute(
                        "DELETE FROM project_record_index WHERE section_key = ? AND chunk_index = ?",
                        (section_key, int(chunk_index)),
                    )
                    if rows:
                        index_rows = []
                        for item_index, item in enumerate(rows):
                            (
                                record_id,
                                record_name,
                                floor,
                                kind,
                                x,
                                y,
                                parent_id,
                            ) = _record_index_values(item)
                            index_rows.append(
                                (
                                    section_key,
                                    int(chunk_index),
                                    item_index,
                                    base_ordinal + item_index,
                                    record_id,
                                    record_name,
                                    floor,
                                    kind,
                                    x,
                                    y,
                                    parent_id,
                                )
                            )
                        connection.executemany(
                            """
                            INSERT INTO project_record_index(
                                section_key, chunk_index, item_index, ordinal,
                                record_id, record_name, floor, kind, x, y, parent_id
                            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            index_rows,
                        )
                        indexed_records += len(index_rows)
                    changed_chunks += 1
                    changed_sections.add(section_key)

                stale = [identity for identity in existing_hashes if identity not in seen]
                if stale:
                    connection.executemany(
                        "DELETE FROM project_sections WHERE section_key = ? AND chunk_index = ?",
                        stale,
                    )
                    deleted_chunks = len(stale)
                    deleted_sections.update(section_key for section_key, _chunk_index in stale)

                existing_meta = dict(
                    connection.execute("SELECT key, value FROM project_meta")
                )
                created_utc = existing_meta.get("created_utc") or _utc_now()
                modified_utc = _utc_now()
                has_project_data_changes = bool(
                    changed_chunks or deleted_chunks or not existing_hashes
                )
                meta_rows = {
                    "format_name": FORMAT_NAME,
                    "schema_version": str(SCHEMA_VERSION),
                    "created_utc": created_utc,
                    "modified_utc": modified_utc,
                    "chunk_size": str(self.chunk_size),
                    "source_path": _text(source_path),
                    "application": "CableRouteResolver",
                }

                connection.executemany(
                    """
                    INSERT INTO project_meta(key, value) VALUES(?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    list(meta_rows.items()),
                )

                # Every explicit project save creates a restorable commit. When
                # an older database has just received its missing initial commit
                # and no data changed, that initial commit is the save commit.
                if has_project_data_changes or not initial_revision_created:
                    revision_number += 1
                    revision_created = True
                    revision_notes = self._revision_notes(
                        had_existing_project=bool(existing_hashes),
                        changed_sections=changed_sections,
                        deleted_sections=deleted_sections,
                        changed_chunks=changed_chunks,
                        deleted_chunks=deleted_chunks,
                        detailed_changes=detailed_changes,
                        commit_message=commit_message,
                    )
                    self._insert_revision(
                        connection,
                        revision_number=revision_number,
                        created_utc=meta_rows["modified_utc"],
                        notes=revision_notes,
                        changed_chunks=changed_chunks,
                        unchanged_chunks=unchanged_chunks,
                        deleted_chunks=deleted_chunks,
                        indexed_records=indexed_records,
                    )
                else:
                    revision_created = True
                    revision_notes = "Initial commit"

            connection.execute("PRAGMA optimize")
        finally:
            connection.close()

        compaction = CompactionStatistics(
            path=str(self.path),
            compacted=False,
            file_size_before_bytes=self.path.stat().st_size if self.path.exists() else 0,
            file_size_after_bytes=self.path.stat().st_size if self.path.exists() else 0,
            reclaimed_bytes=0,
            reclaimable_before_bytes=0,
            free_ratio_before=0.0,
        )
        compaction_error = ""
        if auto_compact:
            try:
                compaction = self.compact(
                    min_free_bytes=compact_min_free_bytes,
                    min_free_ratio=compact_min_free_ratio,
                )
            except Exception as exc:
                # The project transaction has already committed. Maintenance
                # failure must not turn a successful save into data loss.
                compaction_error = str(exc)

        return SaveStatistics(
            path=str(self.path),
            revision_number=revision_number,
            revision_notes=revision_notes,
            revision_created=revision_created,
            changed_chunks=changed_chunks,
            unchanged_chunks=unchanged_chunks,
            deleted_chunks=deleted_chunks,
            indexed_records=indexed_records,
            file_size_bytes=self.path.stat().st_size if self.path.exists() else 0,
            compacted=compaction.compacted,
            reclaimed_bytes=compaction.reclaimed_bytes,
            reclaimable_before_bytes=compaction.reclaimable_before_bytes,
            free_ratio_before=compaction.free_ratio_before,
            compaction_error=compaction_error,
        )

    def verify(self) -> List[str]:
        """Return validation errors; an empty list means the database is sound."""

        errors: List[str] = []
        if not is_sqlite_project(self.path):
            return ["File is not an SQLite database."]
        connection = sqlite3.connect(str(self.path))
        try:
            _configure_connection(connection, writable=False)
            quick_check = connection.execute("PRAGMA quick_check").fetchone()
            if not quick_check or str(quick_check[0]).lower() != "ok":
                errors.append(f"SQLite quick check failed: {quick_check}")
            meta = dict(connection.execute("SELECT key, value FROM project_meta"))
            if meta.get("format_name") != FORMAT_NAME:
                errors.append("Project format marker is missing or invalid.")
            if not connection.execute(
                "SELECT 1 FROM project_sections LIMIT 1"
            ).fetchone():
                errors.append("Project contains no stored sections.")
            if connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'project_revisions'"
            ).fetchone():
                try:
                    connection.execute("SELECT 1 FROM project_revisions LIMIT 1").fetchone()
                except sqlite3.DatabaseError as exc:
                    errors.append(f"Revision history is not readable: {exc}")
            if connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'project_revision_sections'"
            ).fetchone():
                missing_blobs = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM project_revision_sections AS sections
                    LEFT JOIN project_revision_blobs AS blobs
                      ON blobs.payload_hash = sections.payload_hash
                    WHERE blobs.payload_hash IS NULL
                    """
                ).fetchone()[0]
                if missing_blobs:
                    errors.append(
                        f"Revision history has {int(missing_blobs)} missing snapshot chunk(s)."
                    )
        except sqlite3.DatabaseError as exc:
            errors.append(str(exc))
        finally:
            connection.close()
        return errors

    def query_records(
        self,
        section_key: str,
        *,
        floor: Optional[int] = None,
        kind: str = "",
        bounds: Optional[Tuple[float, float, float, float]] = None,
        record_id: str = "",
        parent_id: str = "",
    ) -> List[dict]:
        """Return indexed records without decoding unrelated collection chunks.

        ``bounds`` is ``(min_x, min_y, max_x, max_y)``.  This method is ready
        for floor and viewport lazy-loading while the current UI continues to
        consume the complete in-memory dictionary.
        """

        clauses = ["section_key = ?"]
        parameters: List[object] = [section_key]
        if floor is not None:
            clauses.append("floor = ?")
            parameters.append(int(floor))
        if kind:
            clauses.append("kind = ?")
            parameters.append(str(kind))
        if record_id:
            clauses.append("record_id = ?")
            parameters.append(str(record_id))
        if parent_id:
            clauses.append("parent_id = ?")
            parameters.append(str(parent_id))
        if bounds is not None:
            min_x, min_y, max_x, max_y = bounds
            clauses.extend(("x >= ?", "x <= ?", "y >= ?", "y <= ?"))
            parameters.extend((float(min_x), float(max_x), float(min_y), float(max_y)))

        connection = sqlite3.connect(str(self.path))
        try:
            _configure_connection(connection, writable=False)
            matches = connection.execute(
                f"""
                SELECT chunk_index, item_index, ordinal
                FROM project_record_index
                WHERE {' AND '.join(clauses)}
                ORDER BY ordinal
                """,
                parameters,
            ).fetchall()
            if not matches:
                return []
            by_chunk: Dict[int, List[Tuple[int, int]]] = {}
            for chunk_index, item_index, ordinal in matches:
                by_chunk.setdefault(int(chunk_index), []).append(
                    (int(item_index), int(ordinal))
                )
            ordered: List[Tuple[int, dict]] = []
            for chunk_index, positions in by_chunk.items():
                row = connection.execute(
                    """
                    SELECT payload FROM project_sections
                    WHERE section_key = ? AND chunk_index = ?
                    """,
                    (section_key, chunk_index),
                ).fetchone()
                if row is None:
                    continue
                chunk = _unpack(row[0])
                if not isinstance(chunk, list):
                    continue
                for item_index, ordinal in positions:
                    if 0 <= item_index < len(chunk) and isinstance(chunk[item_index], dict):
                        ordered.append((ordinal, chunk[item_index]))
            ordered.sort(key=lambda pair: pair[0])
            return [item for _ordinal, item in ordered]
        finally:
            connection.close()

    def statistics(self) -> dict:
        connection = sqlite3.connect(str(self.path))
        try:
            _configure_connection(connection, writable=False)
            meta = dict(connection.execute("SELECT key, value FROM project_meta"))
            section_count, record_count, compressed_bytes = connection.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(record_count), 0),
                       COALESCE(SUM(LENGTH(payload)), 0)
                FROM project_sections
                """
            ).fetchone()
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0] or 0)
            page_count = int(connection.execute("PRAGMA page_count").fetchone()[0] or 0)
            free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0] or 0)
            reclaimable = max(0, page_size * free_pages)
            return {
                "path": str(self.path),
                "schema_version": int(meta.get("schema_version", "0") or 0),
                "created_utc": meta.get("created_utc", ""),
                "modified_utc": meta.get("modified_utc", ""),
                "section_chunks": int(section_count or 0),
                "indexed_records": int(record_count or 0),
                "compressed_payload_bytes": int(compressed_bytes or 0),
                "file_size_bytes": self.path.stat().st_size,
                "page_size_bytes": page_size,
                "page_count": page_count,
                "free_page_count": free_pages,
                "reclaimable_bytes": reclaimable,
                "free_ratio": (free_pages / page_count) if page_count else 0.0,
            }
        finally:
            connection.close()

    def revision_history(self, limit: Optional[int] = None) -> List[dict]:
        connection = sqlite3.connect(str(self.path))
        try:
            _configure_connection(connection, writable=False)
            has_snapshot_table = bool(
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'project_revision_sections'
                    """
                ).fetchone()
            )
            restorable_sql = (
                """
                EXISTS(
                    SELECT 1 FROM project_revision_sections AS sections
                    WHERE sections.revision_number = project_revisions.revision_number
                )
                """
                if has_snapshot_table
                else "0"
            )
            sql = f"""
                SELECT revision_number, created_utc, notes,
                       changed_chunks, unchanged_chunks, deleted_chunks, indexed_records,
                       {restorable_sql} AS restorable
                FROM project_revisions
                ORDER BY revision_number DESC
            """
            parameters: Tuple[object, ...] = ()
            if limit is not None:
                sql += " LIMIT ?"
                parameters = (max(1, int(limit)),)
            rows = connection.execute(sql, parameters).fetchall()
            return [
                {
                    "revision_number": int(revision_number or 0),
                    "created_utc": created_utc or "",
                    "notes": notes or "",
                    "changed_chunks": int(changed_chunks or 0),
                    "unchanged_chunks": int(unchanged_chunks or 0),
                    "deleted_chunks": int(deleted_chunks or 0),
                    "indexed_records": int(indexed_records or 0),
                    "restorable": bool(restorable),
                }
                for (
                    revision_number,
                    created_utc,
                    notes,
                    changed_chunks,
                    unchanged_chunks,
                    deleted_chunks,
                    indexed_records,
                    restorable,
                ) in rows
            ]
        except sqlite3.OperationalError as exc:
            if "project_revisions" in str(exc):
                return []
            raise
        finally:
            connection.close()


def export_json_atomic(data: dict, path: str | Path, *, indent: int = 2) -> None:
    """Export project data to JSON using an atomic file replacement."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="\n",
        dir=str(destination.parent),
        prefix=f".{destination.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(data, handle, ensure_ascii=False, indent=indent, allow_nan=True)
        handle.write("\n")
    try:
        temporary.replace(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def load_json(path: str | Path) -> dict:
    with Path(path).open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Project JSON root must be an object.")
    return payload


def migrate_json_to_sqlite(
    source_path: str | Path,
    destination_path: str | Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> SaveStatistics:
    source = Path(source_path)
    payload = load_json(source)
    project = SQLiteProjectFile(destination_path, chunk_size=chunk_size)
    statistics = project.save(payload, source_path=str(source))
    errors = project.verify()
    if errors:
        raise ValueError("SQLite project verification failed: " + "; ".join(errors))
    return statistics
