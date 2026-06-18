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
SCHEMA_VERSION = 1
DEFAULT_EXTENSION = ".crsdb"
SQLITE_HEADER = b"SQLite format 3\x00"
DEFAULT_CHUNK_SIZE = 512


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
        """
    )


@dataclass(frozen=True)
class SaveStatistics:
    path: str
    changed_chunks: int
    unchanged_chunks: int
    deleted_chunks: int
    indexed_records: int
    file_size_bytes: int


class SQLiteProjectFile:
    """Read, write and query a CableRouteResolver SQLite project."""

    def __init__(self, path: str | Path, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.path = Path(path)
        self.chunk_size = max(32, int(chunk_size or DEFAULT_CHUNK_SIZE))

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

            data: Dict[str, object] = {}
            list_sections: Dict[str, List[object]] = {}
            cursor = connection.execute(
                """
                SELECT section_key, section_kind, chunk_index, payload
                FROM project_sections
                ORDER BY section_key, chunk_index
                """
            )
            for section_key, section_kind, _chunk_index, payload in cursor:
                value = _unpack(payload)
                if section_kind == "list":
                    target = list_sections.setdefault(section_key, [])
                    if isinstance(value, list):
                        target.extend(value)
                else:
                    data[section_key] = value

            for section_key, rows in list_sections.items():
                if section_key == "corridors.nodes":
                    corridors = data.setdefault("corridors", {})
                    if not isinstance(corridors, dict):
                        corridors = {}
                        data["corridors"] = corridors
                    corridors["nodes"] = rows
                elif section_key == "corridors.edges":
                    corridors = data.setdefault("corridors", {})
                    if not isinstance(corridors, dict):
                        corridors = {}
                        data["corridors"] = corridors
                    corridors["edges"] = rows
                else:
                    data[section_key] = rows

            return data
        finally:
            connection.close()

    def save(self, data: dict, *, source_path: str = "") -> SaveStatistics:
        if not isinstance(data, dict):
            raise TypeError("Project data must be a dictionary")
        self.path.parent.mkdir(parents=True, exist_ok=True)

        connection = sqlite3.connect(str(self.path))
        changed_chunks = 0
        unchanged_chunks = 0
        indexed_records = 0
        deleted_chunks = 0
        try:
            _configure_connection(connection, writable=True)
            _create_schema(connection)
            existing_hashes = {
                (section_key, int(chunk_index)): payload_hash
                for section_key, chunk_index, payload_hash in connection.execute(
                    "SELECT section_key, chunk_index, payload_hash FROM project_sections"
                )
            }
            seen: set[Tuple[str, int]] = set()
            ordinal_by_section: Dict[str, int] = {}

            with connection:
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

                stale = [identity for identity in existing_hashes if identity not in seen]
                if stale:
                    connection.executemany(
                        "DELETE FROM project_sections WHERE section_key = ? AND chunk_index = ?",
                        stale,
                    )
                    deleted_chunks = len(stale)

                existing_meta = dict(
                    connection.execute("SELECT key, value FROM project_meta")
                )
                created_utc = existing_meta.get("created_utc") or _utc_now()
                meta_rows = {
                    "format_name": FORMAT_NAME,
                    "schema_version": str(SCHEMA_VERSION),
                    "created_utc": created_utc,
                    "modified_utc": _utc_now(),
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

            connection.execute("PRAGMA optimize")
        finally:
            connection.close()

        return SaveStatistics(
            path=str(self.path),
            changed_chunks=changed_chunks,
            unchanged_chunks=unchanged_chunks,
            deleted_chunks=deleted_chunks,
            indexed_records=indexed_records,
            file_size_bytes=self.path.stat().st_size if self.path.exists() else 0,
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
            return {
                "path": str(self.path),
                "schema_version": int(meta.get("schema_version", "0") or 0),
                "created_utc": meta.get("created_utc", ""),
                "modified_utc": meta.get("modified_utc", ""),
                "section_chunks": int(section_count or 0),
                "indexed_records": int(record_count or 0),
                "compressed_payload_bytes": int(compressed_bytes or 0),
                "file_size_bytes": self.path.stat().st_size,
            }
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
