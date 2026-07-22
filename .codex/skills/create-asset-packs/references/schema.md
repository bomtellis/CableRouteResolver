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

## Validation Checklist

- Container format and version match `source/asset_library_io.py`.
- Expected `library_type` matches the destination import dialog.
- Asset IDs are non-empty and unique.
- Category IDs resolve or are deliberately blank.
- Project-asset quantities and data-point counts are integers in valid ranges.
- Connection types are normalized.
- Source-specific engineering details are preserved in extra fields.
- `read_asset_pack` accepts the final file.
