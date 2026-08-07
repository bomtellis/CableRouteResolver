"""Workbook schema mapping, assembly expansion, comparison, and export."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from pathlib import Path
import re
from typing import Iterable, Mapping, Sequence

from xlsx_workbook import (
    SheetData,
    WorkbookData,
    WorksheetSpec,
    XlsxError,
    write_xlsx,
)


@dataclass(frozen=True)
class FieldDefinition:
    key: str
    label: str
    aliases: tuple[str, ...]
    required: bool = False


MAIN_FIELDS = (
    FieldDefinition(
        "room_code",
        "Room code",
        ("room code", "room id", "room type id", "room type code", "room number"),
    ),
    FieldDefinition("room_name", "Room name", ("room name", "room type", "room", "space name")),
    FieldDefinition(
        "asset_code",
        "Asset code",
        ("code", "asset id", "asset code", "equipment code", "item code", "adb code"),
        True,
    ),
    FieldDefinition("description", "Description", ("description", "asset description", "equipment description", "item description")),
    FieldDefinition(
        "quantity",
        "Quantity",
        ("count", "quantity", "qty", "qty per room", "asset quantity", "item quantity"),
        True,
    ),
    FieldDefinition("assembly_id", "Assembly ID", ("assembly id", "assembly", "assembly identifier")),
    FieldDefinition("assembly_parent_code", "Assembly parent code", ("parent code", "assembly code", "parent asset code")),
)

ASSEMBLY_FIELDS = (
    FieldDefinition("assembly_id", "Assembly ID", ("assembly id", "assembly", "assembly identifier"), True),
    FieldDefinition("item_code", "Item code", ("item code", "asset code", "equipment code", "code", "adb code"), True),
    FieldDefinition("item_description", "Item description", ("item description", "description", "asset description", "equipment description")),
    FieldDefinition("item_quantity", "Item quantity", ("item quantity", "quantity", "qty", "count"), True),
    FieldDefinition("parent_code", "Parent code", ("parent code", "assembly code", "parent asset code")),
    FieldDefinition("room_code", "Room code", ("room code", "room id", "room type code", "room number")),
    FieldDefinition("room_name", "Room name", ("room name", "room type", "room", "space name")),
)


def normalise(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def display_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


@dataclass
class SchemaMapping:
    main_sheet: str
    main_header_row: int
    main_columns: dict[str, int | None]
    assembly_sheet: str | None = None
    assembly_header_row: int = 0
    assembly_columns: dict[str, int | None] = field(default_factory=dict)


@dataclass(frozen=True)
class RoomRecord:
    key: str
    code: str
    name: str

    @property
    def label(self) -> str:
        if self.code and self.name:
            return f"{self.code} — {self.name}"
        return self.name or self.code or "(blank room)"


@dataclass(frozen=True)
class RoomMappingGroup:
    rooms_a: tuple[str, ...]
    rooms_b: tuple[str, ...]
    label: str = ""


@dataclass(frozen=True)
class AssetEntry:
    room_key: str
    room_code: str
    room_name: str
    asset_code: str
    description: str
    quantity: Decimal
    assembly_id: str = ""
    assembly_parent_code: str = ""
    source_row: int = 0

    @property
    def asset_key(self) -> str:
        code = normalise(self.asset_code)
        return f"code:{code}" if code else f"description:{normalise(self.description)}"

    @property
    def assembly_key(self) -> str:
        if not self.assembly_id:
            return "__direct__"
        parent = normalise(self.assembly_parent_code)
        return f"parent:{parent}" if parent else f"assembly:{normalise(self.assembly_id)}"


@dataclass(frozen=True)
class ComparisonRow:
    level: str
    assembly_group: str
    assembly_ids_a: str
    assembly_ids_b: str
    asset_code: str
    description_a: str
    description_b: str
    quantity_a: Decimal
    quantity_b: Decimal
    difference: Decimal
    status: str


@dataclass
class MappingComparison:
    label: str
    rooms_a: tuple[RoomRecord, ...]
    rooms_b: tuple[RoomRecord, ...]
    total_rows: list[ComparisonRow]
    assembly_rows: list[ComparisonRow]
    warnings: list[str] = field(default_factory=list)


@dataclass
class ComparisonResult:
    mappings: list[MappingComparison]
    source_a: str
    source_b: str

    @property
    def difference_count(self) -> int:
        return sum(
            row.status != "Same"
            for mapping in self.mappings
            for row in mapping.total_rows
        )


def _headers(sheet: SheetData, row_index: int) -> list[str]:
    if not 0 <= row_index < len(sheet.rows):
        return []
    return [display_value(value) for value in sheet.rows[row_index]]


def _alias_score(header: str, definition: FieldDefinition) -> float:
    candidate = normalise(header)
    if not candidate:
        return 0.0
    aliases = [normalise(value) for value in definition.aliases]
    if candidate in aliases:
        return 1.0
    return max(
        (SequenceMatcher(None, candidate, alias).ratio() for alias in aliases),
        default=0.0,
    )


def _alias_rank(header: str, definition: FieldDefinition) -> int:
    candidate = normalise(header)
    aliases = [normalise(value) for value in definition.aliases]
    try:
        return aliases.index(candidate)
    except ValueError:
        return len(aliases)


def suggest_columns(
    headers: Sequence[str], definitions: Sequence[FieldDefinition]
) -> dict[str, int | None]:
    result: dict[str, int | None] = {}
    used: set[int] = set()
    for definition in definitions:
        candidates = max(
            (
                (
                    _alias_score(header, definition),
                    -_alias_rank(header, definition),
                    -index,
                    index,
                )
                for index, header in enumerate(headers)
                if index not in used
            ),
            default=(0.0, 0, 0, -1),
        )
        score, _rank, _position, index = candidates
        if score >= 0.72:
            result[definition.key] = index
            used.add(index)
        else:
            result[definition.key] = None
    return result


def suggest_header(
    sheet: SheetData, definitions: Sequence[FieldDefinition]
) -> tuple[int, dict[str, int | None], float]:
    best = (0, {definition.key: None for definition in definitions}, -1.0)
    for row_index in range(min(30, len(sheet.rows))):
        headers = _headers(sheet, row_index)
        columns = suggest_columns(headers, definitions)
        score = sum(
            2 if definition.required and columns[definition.key] is not None else
            1 if columns[definition.key] is not None else 0
            for definition in definitions
        )
        if score > best[2]:
            best = (row_index, columns, float(score))
    return best


def suggest_schema(workbook: WorkbookData) -> SchemaMapping:
    assembly_options = [
        (suggest_header(sheet, ASSEMBLY_FIELDS), sheet.name)
        for sheet in workbook.sheets
    ]
    assembly_sheet = None
    assembly_header = 0
    assembly_columns: dict[str, int | None] = {
        definition.key: None for definition in ASSEMBLY_FIELDS
    }
    if assembly_options:
        (candidate_header, candidate_columns, candidate_score), candidate_sheet = max(
            assembly_options,
            key=lambda item: (
                item[0][2]
                + (6 if "assembl" in normalise(item[1]) else 0)
                + (2 if "item" in normalise(item[1]) else 0)
            ),
        )
        required_found = all(
            candidate_columns[definition.key] is not None
            for definition in ASSEMBLY_FIELDS
            if definition.required
        )
        name_hint = "assembl" in normalise(candidate_sheet)
        if len(workbook.sheets) > 1 and required_found and (
            candidate_score >= 6 or name_hint
        ):
            assembly_sheet = candidate_sheet
            assembly_header = candidate_header
            assembly_columns = candidate_columns
    main_options = [
        (suggest_header(sheet, MAIN_FIELDS), sheet.name)
        for sheet in workbook.sheets
        if sheet.name != assembly_sheet
    ]
    (main_header, main_columns, _score), main_sheet = max(
        main_options,
        key=lambda item: (
            item[0][2]
            + (
                4
                if any(
                    hint in normalise(item[1])
                    for hint in ("equipment", "assetlist", "assetdetail", "schedule")
                )
                else 0
            )
            - (6 if "assembl" in normalise(item[1]) else 0)
        ),
    )
    return SchemaMapping(
        main_sheet=main_sheet,
        main_header_row=main_header,
        main_columns=main_columns,
        assembly_sheet=assembly_sheet,
        assembly_header_row=assembly_header,
        assembly_columns=assembly_columns,
    )


def validate_schema(workbook: WorkbookData, schema: SchemaMapping) -> list[str]:
    errors = []
    if schema.main_sheet not in workbook.sheet_names:
        return [f'Main worksheet "{schema.main_sheet}" was not found.']
    if not (
        schema.main_columns.get("room_code") is not None
        or schema.main_columns.get("room_name") is not None
    ):
        errors.append("Map at least one of Room code or Room name.")
    for key, label in (("asset_code", "Asset code"), ("quantity", "Quantity")):
        if schema.main_columns.get(key) is None:
            errors.append(f"Map the required main-sheet field: {label}.")
    if schema.assembly_sheet is not None:
        if schema.assembly_sheet not in workbook.sheet_names:
            errors.append(
                f'Assembly worksheet "{schema.assembly_sheet}" was not found.'
            )
        for definition in ASSEMBLY_FIELDS:
            if definition.required and schema.assembly_columns.get(definition.key) is None:
                errors.append(
                    f"Map the required assembly-sheet field: {definition.label}."
                )
        if schema.main_columns.get("assembly_id") is None:
            errors.append(
                "Map Assembly ID on the main sheet, or select No assembly sheet."
            )
    return errors


def _value(row: Sequence[object], column: int | None) -> object:
    if column is None or column < 0 or column >= len(row):
        return ""
    return row[column]


def _room_from_row(
    row: Sequence[object], columns: Mapping[str, int | None]
) -> RoomRecord | None:
    code = display_value(_value(row, columns.get("room_code")))
    name = display_value(_value(row, columns.get("room_name")))
    if not code and not name:
        return None
    key = f"{normalise(code)}|{normalise(name)}"
    return RoomRecord(key, code, name)


def extract_rooms(
    workbook: WorkbookData, schema: SchemaMapping
) -> dict[str, RoomRecord]:
    sheet = workbook.sheet(schema.main_sheet)
    result = {}
    for row in sheet.rows[schema.main_header_row + 1 :]:
        room = _room_from_row(row, schema.main_columns)
        if room is not None:
            result.setdefault(room.key, room)
    return dict(sorted(result.items(), key=lambda item: item[1].label.casefold()))


def auto_map_rooms(
    rooms_a: Mapping[str, RoomRecord], rooms_b: Mapping[str, RoomRecord]
) -> list[RoomMappingGroup]:
    unmatched_b = set(rooms_b)
    groups = []
    for key_a, room_a in rooms_a.items():
        best_key = None
        best_score = 0.0
        for key_b in unmatched_b:
            room_b = rooms_b[key_b]
            code_equal = bool(
                normalise(room_a.code)
                and normalise(room_a.code) == normalise(room_b.code)
            )
            name_equal = bool(
                normalise(room_a.name)
                and normalise(room_a.name) == normalise(room_b.name)
            )
            if code_equal or name_equal:
                score = 1.0
            else:
                score = SequenceMatcher(
                    None, normalise(room_a.name), normalise(room_b.name)
                ).ratio()
            if score > best_score:
                best_key, best_score = key_b, score
        if best_key is not None and best_score >= 0.92:
            groups.append(RoomMappingGroup((key_a,), (best_key,)))
            unmatched_b.remove(best_key)
    return groups


def _decimal(value: object, default: Decimal = Decimal(1)) -> Decimal:
    text = display_value(value).replace(",", "")
    if not text:
        return default
    try:
        return Decimal(text)
    except InvalidOperation:
        return default


def _assembly_lookup(
    workbook: WorkbookData, schema: SchemaMapping
) -> dict[str, list[tuple[Sequence[object], int]]]:
    result: dict[str, list[tuple[Sequence[object], int]]] = defaultdict(list)
    if schema.assembly_sheet is None:
        return result
    sheet = workbook.sheet(schema.assembly_sheet)
    column = schema.assembly_columns.get("assembly_id")
    for row_number, row in enumerate(
        sheet.rows[schema.assembly_header_row + 1 :],
        start=schema.assembly_header_row + 2,
    ):
        assembly_id = normalise(_value(row, column))
        if assembly_id:
            result[assembly_id].append((row, row_number))
    return result


def expand_assets(
    workbook: WorkbookData, schema: SchemaMapping, room_keys: Iterable[str]
) -> tuple[list[AssetEntry], list[str]]:
    selected = set(room_keys)
    main = workbook.sheet(schema.main_sheet)
    assembly_rows = _assembly_lookup(workbook, schema)
    assets = []
    warnings = []
    for row_number, row in enumerate(
        main.rows[schema.main_header_row + 1 :],
        start=schema.main_header_row + 2,
    ):
        room = _room_from_row(row, schema.main_columns)
        if room is None or room.key not in selected:
            continue
        code = display_value(_value(row, schema.main_columns.get("asset_code")))
        description = display_value(
            _value(row, schema.main_columns.get("description"))
        )
        quantity = _decimal(_value(row, schema.main_columns.get("quantity")))
        assembly_id = display_value(
            _value(row, schema.main_columns.get("assembly_id"))
        )
        parent_code = display_value(
            _value(row, schema.main_columns.get("assembly_parent_code"))
        ) or code
        matched_items = (
            assembly_rows.get(normalise(assembly_id), [])
            if schema.assembly_sheet is not None and assembly_id
            else []
        )
        if matched_items and (
            schema.assembly_columns.get("room_code") is not None
            or schema.assembly_columns.get("room_name") is not None
        ):
            room_matches = [
                item
                for item in matched_items
                if (
                    _room_from_row(item[0], schema.assembly_columns) is not None
                    and _room_from_row(item[0], schema.assembly_columns).key
                    == room.key
                )
            ]
            if room_matches:
                matched_items = room_matches
        if matched_items:
            for item_row, _item_row_number in matched_items:
                item_code = display_value(
                    _value(item_row, schema.assembly_columns.get("item_code"))
                )
                item_description = display_value(
                    _value(item_row, schema.assembly_columns.get("item_description"))
                )
                item_quantity = _decimal(
                    _value(item_row, schema.assembly_columns.get("item_quantity"))
                )
                if not item_code and not item_description:
                    continue
                item_parent = display_value(
                    _value(item_row, schema.assembly_columns.get("parent_code"))
                ) or parent_code
                assets.append(
                    AssetEntry(
                        room.key,
                        room.code,
                        room.name,
                        item_code,
                        item_description,
                        quantity * item_quantity,
                        assembly_id,
                        item_parent,
                        row_number,
                    )
                )
        else:
            if not code and not description:
                continue
            if assembly_id and schema.assembly_sheet is not None:
                warnings.append(
                    f'{schema.main_sheet} row {row_number}: assembly "{assembly_id}" '
                    "was not found on the mapped assembly sheet; its parent row "
                    "was retained as a direct asset."
                )
            assets.append(
                AssetEntry(
                    room.key,
                    room.code,
                    room.name,
                    code,
                    description,
                    quantity,
                    "",
                    "",
                    row_number,
                )
            )
    return assets, warnings


def _describe(entries: Sequence[AssetEntry]) -> str:
    values = []
    for entry in entries:
        if entry.description and entry.description not in values:
            values.append(entry.description)
    return " | ".join(values)


def _comparison_row(
    level: str,
    assembly_group: str,
    entries_a: Sequence[AssetEntry],
    entries_b: Sequence[AssetEntry],
) -> ComparisonRow:
    quantity_a = sum((entry.quantity for entry in entries_a), Decimal(0))
    quantity_b = sum((entry.quantity for entry in entries_b), Decimal(0))
    code = next(
        (entry.asset_code for entry in (*entries_a, *entries_b) if entry.asset_code),
        "",
    )
    description_a = _describe(entries_a)
    description_b = _describe(entries_b)
    if quantity_a == 0 and quantity_b != 0:
        status = "Added"
    elif quantity_b == 0 and quantity_a != 0:
        status = "Removed"
    elif quantity_a != quantity_b:
        status = "Quantity changed"
    elif normalise(description_a) != normalise(description_b):
        status = "Changed"
    else:
        status = "Same"
    return ComparisonRow(
        level,
        assembly_group,
        ", ".join(sorted({entry.assembly_id for entry in entries_a if entry.assembly_id})),
        ", ".join(sorted({entry.assembly_id for entry in entries_b if entry.assembly_id})),
        code,
        description_a,
        description_b,
        quantity_a,
        quantity_b,
        quantity_b - quantity_a,
        status,
    )


def _compare_entries(
    assets_a: Sequence[AssetEntry], assets_b: Sequence[AssetEntry]
) -> tuple[list[ComparisonRow], list[ComparisonRow]]:
    totals_a: dict[str, list[AssetEntry]] = defaultdict(list)
    totals_b: dict[str, list[AssetEntry]] = defaultdict(list)
    for entry in assets_a:
        totals_a[entry.asset_key].append(entry)
    for entry in assets_b:
        totals_b[entry.asset_key].append(entry)
    total_rows = [
        _comparison_row("Total asset list", "All direct and expanded assets", totals_a.get(key, []), totals_b.get(key, []))
        for key in sorted(set(totals_a) | set(totals_b))
    ]

    grouped_a: dict[tuple[str, str], list[AssetEntry]] = defaultdict(list)
    grouped_b: dict[tuple[str, str], list[AssetEntry]] = defaultdict(list)
    for entry in assets_a:
        grouped_a[(entry.assembly_key, entry.asset_key)].append(entry)
    for entry in assets_b:
        grouped_b[(entry.assembly_key, entry.asset_key)].append(entry)
    assembly_rows = []
    for assembly_key, asset_key in sorted(set(grouped_a) | set(grouped_b)):
        entries_a = grouped_a.get((assembly_key, asset_key), [])
        entries_b = grouped_b.get((assembly_key, asset_key), [])
        sample = next(iter((*entries_a, *entries_b)))
        if assembly_key == "__direct__":
            label = "Direct assets"
        else:
            parent = sample.assembly_parent_code
            label = f"Assembly {parent}" if parent else "Assembly"
        assembly_rows.append(
            _comparison_row(label, label, entries_a, entries_b)
        )
    return total_rows, assembly_rows


def compare_workbooks(
    workbook_a: WorkbookData,
    schema_a: SchemaMapping,
    workbook_b: WorkbookData,
    schema_b: SchemaMapping,
    groups: Sequence[RoomMappingGroup],
    *,
    include_unmapped: bool = True,
) -> ComparisonResult:
    rooms_a = extract_rooms(workbook_a, schema_a)
    rooms_b = extract_rooms(workbook_b, schema_b)
    work_groups = list(groups)
    used_a = {key for group in work_groups for key in group.rooms_a}
    used_b = {key for group in work_groups for key in group.rooms_b}
    if include_unmapped:
        work_groups.extend(
            RoomMappingGroup((key,), (), f"Only in A: {room.label}")
            for key, room in rooms_a.items()
            if key not in used_a
        )
        work_groups.extend(
            RoomMappingGroup((), (key,), f"Only in B: {room.label}")
            for key, room in rooms_b.items()
            if key not in used_b
        )

    comparisons = []
    for index, group in enumerate(work_groups, start=1):
        selected_a = tuple(rooms_a[key] for key in group.rooms_a if key in rooms_a)
        selected_b = tuple(rooms_b[key] for key in group.rooms_b if key in rooms_b)
        assets_a, warnings_a = expand_assets(workbook_a, schema_a, group.rooms_a)
        assets_b, warnings_b = expand_assets(workbook_b, schema_b, group.rooms_b)
        total_rows, assembly_rows = _compare_entries(assets_a, assets_b)
        label = group.label.strip() or f"Mapping {index}"
        comparisons.append(
            MappingComparison(
                label,
                selected_a,
                selected_b,
                total_rows,
                assembly_rows,
                warnings_a + warnings_b,
            )
        )
    return ComparisonResult(
        comparisons, str(Path(workbook_a.path)), str(Path(workbook_b.path))
    )


def _source_rows(
    label: str, workbook: WorkbookData, schema: SchemaMapping
) -> tuple[list[list[object]], set[int], set[int], set[int]]:
    rows: list[list[object]] = [
        [f"{label} — Original workbook data"],
        [f"File: {workbook.path}"],
        [f"Main sheet: {schema.main_sheet}"],
    ]
    title_rows = {0}
    section_rows = {2}
    header_rows = {len(rows)}
    rows.extend([list(row) for row in workbook.sheet(schema.main_sheet).rows])
    if schema.assembly_sheet is not None:
        rows.append([])
        section_rows.add(len(rows))
        rows.append([f"Assembly sheet: {schema.assembly_sheet}"])
        header_rows.add(len(rows))
        rows.extend([list(row) for row in workbook.sheet(schema.assembly_sheet).rows])
    else:
        rows.append([])
        section_rows.add(len(rows))
        rows.append(["Assembly sheet: Not available / not used"])
    return rows, title_rows, section_rows, header_rows


def export_comparison(
    path: str | Path,
    result: ComparisonResult,
    workbook_a: WorkbookData,
    schema_a: SchemaMapping,
    workbook_b: WorkbookData,
    schema_b: SchemaMapping,
) -> str:
    original_a, titles_a, sections_a, headers_a = _source_rows(
        "Workbook A", workbook_a, schema_a
    )
    original_b, titles_b, sections_b, headers_b = _source_rows(
        "Workbook B", workbook_b, schema_b
    )
    difference_rows: list[list[object]] = [
        ["Workbook asset comparison — differences"],
        [f"Workbook A: {result.source_a}"],
        [f"Workbook B: {result.source_b}"],
        [],
        [
            "Mapping",
            "Rooms A",
            "Rooms B",
            "Comparison level",
            "Assembly group",
            "Assembly IDs A",
            "Assembly IDs B",
            "Asset code",
            "Description A",
            "Description B",
            "Quantity A",
            "Quantity B",
            "Difference (B - A)",
            "Status",
        ],
    ]
    for mapping in result.mappings:
        rooms_a = "; ".join(room.label for room in mapping.rooms_a)
        rooms_b = "; ".join(room.label for room in mapping.rooms_b)
        for row in (*mapping.total_rows, *mapping.assembly_rows):
            if row.status == "Same":
                continue
            difference_rows.append(
                [
                    mapping.label,
                    rooms_a,
                    rooms_b,
                    row.level,
                    row.assembly_group,
                    row.assembly_ids_a,
                    row.assembly_ids_b,
                    row.asset_code,
                    row.description_a,
                    row.description_b,
                    row.quantity_a,
                    row.quantity_b,
                    row.difference,
                    row.status,
                ]
            )
    if len(difference_rows) == 5:
        difference_rows.append(
            ["No differences found", "", "", "", "", "", "", "", "", "", 0, 0, 0, "Same"]
        )
    return write_xlsx(
        path,
        [
            WorksheetSpec(
                "Original A",
                original_a,
                title_rows=titles_a,
                section_rows=sections_a,
                header_rows=headers_a,
                freeze_row=4 + schema_a.main_header_row,
            ),
            WorksheetSpec(
                "Original B",
                original_b,
                title_rows=titles_b,
                section_rows=sections_b,
                header_rows=headers_b,
                freeze_row=4 + schema_b.main_header_row,
            ),
            WorksheetSpec(
                "Differences",
                difference_rows,
                title_rows={0},
                header_rows={4},
                status_column=13,
                freeze_row=5,
                auto_filter_row=4,
            ),
        ],
    )
