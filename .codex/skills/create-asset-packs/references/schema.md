# Cable Route Resolver Asset-Pack Schema

## Container

```json
{
  "format": "cable-route-resolver-asset-pack",
  "version": 1,
  "library_type": "assets",
  "name": "Library name",
  "assets": [],
  "related": {},
  "metadata": {}
}
```

`library_type` is either `assets` or `network_assets`. The importer requires every asset row to be an object with a unique, non-empty `id`. Unknown asset fields are retained.

Project asset packs may also contain reusable bundle definitions in
`related.asset_bundles`. Network asset packs cannot contain bundles.

## Project Asset Fields

| Field | Type | Mapping |
|---|---|---|
| `id` | text | Required stable key; preserve leading zeroes |
| `name` | text | Human-readable name; normally required by the editor |
| `ADB_Code` | text | ADB identifier; preserve as text |
| `Group` | text | Display group or functional family |
| `capability_keywords` | text | Semicolon-separated searchable capabilities |
| `capabilities` | text array | Normalized equivalent of `capability_keywords` |
| `connection_type` | text | Exactly `wired` or `wireless` |
| `category_id` | text | References `related.asset_categories[].id` |
| `qty` | integer | Library default, normally `1` |
| `data_points` | integer | Non-negative cable/data-point demand per item |
| `north_south_concurrency_factor` | number | Between `0` and `1` |
| `east_west_concurrency_factor` | number | Between `0` and `1` |

Useful optional traceability fields include `description`, `connection_method`, `power_method`, `source_row_count`, `source_total_asset_qty`, `source_categories`, `alternative_connections`, `alternative_power_methods`, `review_status`, and `notes`.

## Related Categories

```json
{
  "related": {
    "asset_categories": [
      { "id": "CAT-CLIN", "name": "Clinical Equipment" }
    ]
  }
}
```

Include only category definitions referenced by the exported rows. Keep category IDs unique.

## Related Asset Bundles

```json
{
  "related": {
    "asset_bundles": [
      {
        "id": "AB1",
        "name": "Consulting room",
        "description": "Standard room endpoint set",
        "assets": [
          { "asset_id": "A100", "qty": 1 },
          { "asset_id": "A200", "qty": 2 }
        ],
        "connections": [
          {
            "from_asset_id": "A100",
            "to_asset_id": "A200",
            "connection_asset_id": "C100",
            "port_type": "network",
            "qty": 1
          }
        ]
      }
    ]
  }
}
```

- Bundle IDs must be non-empty and unique within the pack.
- Bundle asset rows use `asset_id` and a positive integer `qty`.
- Every asset referenced by a bundle asset row, connection endpoint, or
  `connection_asset_id` must also appear in the pack's top-level `assets`.
- Use `connections` for bundle connection recipes. `from_asset_id` may be blank
  only for an explicitly external source; `to_asset_id` identifies a bundled
  asset.
- Empty placeholder bundles are valid in a full-library pack.
- During import, bundle references follow the user's asset mappings. If any
  referenced source asset is rejected, the entire bundle is skipped.

## Room-Type Bundle Overlap State

Room assignments are project data and are not stored in an asset pack. A room
type can contain `asset_bundle_assignments` plus a room-specific
`asset_bundle_excluded_asset_ids` array. The latter records assets that were
reduced to one retained room item during overlap review. Bundle application,
replacement, recipe synchronisation, and derived connections must ignore those
asset IDs for that room so the removed overlap is not recreated. Do not add
this field to `related.asset_bundles`.

## Validation Checklist

- Container format and version match `source/asset_library_io.py`.
- Expected `library_type` matches the destination import dialog.
- Asset IDs are non-empty and unique.
- Category IDs resolve or are deliberately blank.
- Bundle IDs are unique and every bundle dependency resolves to an included asset.
- Project-asset quantities and data-point counts are integers in valid ranges.
- Connection types are normalized.
- Source-specific engineering details are preserved in extra fields.
- `read_asset_pack` accepts the final file.
