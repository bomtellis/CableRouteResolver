---
name: create-asset-packs
description: Create, convert, repair, and validate native Cable Route Resolver `.asset-pack.json` files from spreadsheets, CSVs, JSON, or existing project data. Use for project asset libraries (`assets`), network asset libraries (`network_assets`), category metadata, or troubleshooting packs rejected by the resolver import dialogs.
---

# Create Cable Route Resolver Asset Packs

Produce import-ready packs that match the software's current schema and preserve source traceability.

## Workflow

1. Find the repository root and read `source/asset_library_io.py`. Treat its format name, version, supported library types, and validation behavior as authoritative.
2. For project assets, inspect the normalization and editor fields in `source/models.py` and `source/dialogs.py`. For network assets, inspect `source/network_schema.py` and the network asset editor in `source/network_dialogs.py`.
3. Inspect the input before mapping it. When the source is a workbook, use the spreadsheet skill and visually verify every relevant sheet before extracting rows.
4. Normalize source rows without discarding useful source-specific fields. Read [references/schema.md](references/schema.md) for the stable container and project-asset mappings.
5. Write a filename ending in `.asset-pack.json`, normally under the repository's `asset_packs/` directory. Do not overwrite an existing pack unless the user requested replacement.
6. Validate with the repository's `read_asset_pack(path, expected_library_type)` function. Also check counts, unique IDs, category references, numeric bounds, and wired/wireless normalization.
7. Report the pack path, asset count, category count, and any rows retained with unresolved technical-review flags.

## Deterministic CLI

Use [scripts/asset_pack_cli.py](scripts/asset_pack_cli.py) when the source has already been normalized to CSV or JSON.

```powershell
python .codex/skills/create-asset-packs/scripts/asset_pack_cli.py create `
  --input normalized-assets.json `
  --output asset_packs/example.asset-pack.json `
  --library-type assets `
  --name "Example Asset Library" `
  --categories asset-categories.json

python .codex/skills/create-asset-packs/scripts/asset_pack_cli.py validate `
  asset_packs/example.asset-pack.json --library-type assets
```

The CLI preserves unknown row fields, so source notes and engineering attributes survive round trips.

## Mapping Rules

- Require a non-empty, unique `id` for every asset.
- Use `qty: 1` as the project-library default unless the source explicitly defines a reusable library quantity. Room-type assignments control deployed quantities.
- Map cable demand to non-negative integer `data_points`; accept legacy `data_points_each` or `cables` only as source aliases.
- Normalize `connection_type` to `wired` or `wireless`. Keep detailed methods such as dual-homed Ethernet, nurse-call cabling, or Wi-Fi in an additional `connection_method` field.
- Store category IDs on asset rows and include matching category definitions in `related.asset_categories`.
- Preserve ADB codes as identifiers, including leading zeroes.
- Keep technical alternatives and review flags in explicit fields; do not silently resolve engineering ambiguity.
- Default concurrency factors to `1.0` only when the source provides no better assumption.

## Safety and Quality

- Never create duplicate IDs or silently replace existing library definitions.
- Do not infer that `data_points: 0` means wireless; connection method and cable count are separate facts.
- Do not copy source totals into `qty` when totals represent deployment occurrences across rooms.
- Prefer current repository code over this skill if the schema changes.
- Keep generated pack JSON UTF-8, indented, and terminated with a newline.
