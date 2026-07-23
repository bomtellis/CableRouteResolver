from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
import re
import shutil
from uuid import uuid4

from project_sqlite import (
    AUTO_COMPACT_MIN_FREE_BYTES,
    AUTO_COMPACT_MIN_FREE_RATIO,
    DEFAULT_EXTENSION,
    SQLiteProjectFile,
    export_json_atomic,
    is_sqlite_project,
    load_json,
)
from room_type_asset_staging import staged_changes as room_type_asset_staged_changes
from asset_bundles import normalise_asset_bundles


DEFAULT_JSON = {
    "project": {
        "name": "Cable Routing Project",
    },
    "building": {
        "floor_height_m": 4.0,
    },
    "departments": [],
    "room_types": [],
    "room_type_asset_review": {},
    "room_type_asset_staging": {},
    "room_type_asset_commits": [],
    "room_type_asset_rfi": {
        "queries": [],
        "history": [],
    },
    "room_type_scenario_groups": [],
    "asset_scenario_groups": [],
    "room_type_asset_scenarios": [],
    "asset_bundles": [],
    "asset_categories": [],
    "assets": [],
    "retired_asset_ids": [],
    "locations": [],
    "equipment_room_placement_zones": [],
    "data_points": [],
    "corridors": {
        "nodes": [],
        "edges": [],
        "auto_connect": False,
    },
    "transitions": [],
    "floor_dxf_files": [],
    "floor_plan_pdf_settings": {
        "paper_size": "A1",
        "scale": 100,
    },
    "pdf_report_studio_settings": {},
    "network_pdf_snippet_templates": [],
    "pdf_report_page_templates": [],
    "connections": [],
    "route_profiles": {
        "default": {
            "allowed_transitions": [],
            "allowed_nodes": [],
            "allowed_edges": [],
        }
    },
}


class JsonStore:
    """Compatibility store backed by JSON in memory and SQLite on disk.

    The class name is retained because the editor and its extensions already
    depend on it.  New project saves use ``.crsdb`` SQLite databases; JSON is
    supported as an import/export interchange format.
    """

    def __init__(self, data: Optional[dict] = None):
        self.data = deepcopy(DEFAULT_JSON)
        self.storage_path: Optional[str] = None
        self.storage_format = "memory"
        self.last_save_statistics = None
        self.last_compaction_statistics = None
        if data:
            self._load_from_payload(data)

    def _load_from_payload(self, payload: dict) -> None:
        payload = deepcopy(payload)

        # Backwards compatibility for the AMR editor base project.
        if "transitions" not in payload and "lifts" in payload:
            payload["transitions"] = []
            for lift in payload.get("lifts", []):
                payload["transitions"].append(
                    {
                        "id": lift.get("id", "Transition-1"),
                        "floors": lift.get("served_floors", []),
                        "cable_limit": lift.get("capacity_size_units", 0),
                        "floor_locations": lift.get("floor_locations", {}),
                    }
                )
        if "connections" not in payload and "tasks" in payload:
            payload["connections"] = []
            for task in payload.get("tasks", []):
                payload["connections"].append(
                    {
                        "id": task.get("id", "C1"),
                        "from": task.get("pickup", ""),
                        "to": task.get("dropoff", ""),
                        "qty": task.get("qty", 1),
                        "route_profile": task.get("route_profile", ""),
                    }
                )
        for profile in payload.get("route_profiles", {}).values():
            if "allowed_transitions" not in profile and "allowed_lifts" in profile:
                profile["allowed_transitions"] = list(profile.get("allowed_lifts", []))
            profile.setdefault("allowed_transitions", [])
            profile.setdefault("allowed_nodes", [])
            profile.setdefault("allowed_edges", [])

        self.data.update(deepcopy(DEFAULT_JSON))
        for key, value in payload.items():
            self.data[key] = value

        self.data.setdefault("departments", [])
        self.data.setdefault("room_types", [])
        if not isinstance(self.data.get("room_type_asset_review"), dict):
            self.data["room_type_asset_review"] = {}
        self.data.setdefault("room_type_asset_review", {})
        if not isinstance(self.data.get("room_type_asset_staging"), dict):
            self.data["room_type_asset_staging"] = {}
        if not isinstance(self.data.get("room_type_asset_commits"), list):
            self.data["room_type_asset_commits"] = []
        else:
            self.data["room_type_asset_commits"] = [
                dict(item)
                for item in self.data["room_type_asset_commits"]
                if isinstance(item, dict)
            ]
        rfi_state = self.data.setdefault("room_type_asset_rfi", {})
        if not isinstance(rfi_state, dict):
            rfi_state = {}
            self.data["room_type_asset_rfi"] = rfi_state
        for key in ("queries", "history"):
            if not isinstance(rfi_state.get(key), list):
                rfi_state[key] = []
            else:
                rfi_state[key] = [
                    dict(item) for item in rfi_state[key] if isinstance(item, dict)
                ]
        revision_change_log = self.data.get("revision_change_log")
        if revision_change_log is not None and not isinstance(revision_change_log, list):
            self.data.pop("revision_change_log", None)
        elif isinstance(revision_change_log, list):
            self.data["revision_change_log"] = [
                dict(item) for item in revision_change_log if isinstance(item, dict)
            ]
        retired_asset_ids = self.data.get("retired_asset_ids", [])
        if not isinstance(retired_asset_ids, list):
            retired_asset_ids = []
        active_asset_ids = {
            str(asset.get("id", "") or "").strip()
            for asset in self.data.get("assets", []) or []
            if isinstance(asset, dict) and str(asset.get("id", "") or "").strip()
        }
        self.data["retired_asset_ids"] = sorted(
            {
                str(asset_id or "").strip()
                for asset_id in retired_asset_ids
                if str(asset_id or "").strip()
                and str(asset_id or "").strip() not in active_asset_ids
            },
            key=str.casefold,
        )
        self.data.setdefault("room_type_scenario_groups", [])
        self.data.setdefault("asset_scenario_groups", [])
        self.data.setdefault("room_type_asset_scenarios", [])
        self.data.setdefault("asset_bundles", [])
        self.data.setdefault("asset_categories", [])
        self.data.setdefault("assets", [])
        self.data["asset_bundles"] = normalise_asset_bundles(
            self.data.get("asset_bundles", []),
            [
                asset.get("id")
                for asset in self.data.get("assets", [])
                if isinstance(asset, dict)
            ],
        )
        self.data.setdefault("locations", [])
        self.data.setdefault("equipment_room_placement_zones", [])
        self.data.setdefault("data_points", [])
        self.data.setdefault("corridors", {}).setdefault("nodes", [])
        self.data.setdefault("corridors", {}).setdefault("edges", [])
        self.data.setdefault("transitions", [])
        self.data.setdefault("floor_dxf_files", [])
        settings = self.data.setdefault("floor_plan_pdf_settings", {})
        if not isinstance(settings, dict):
            settings = {}
            self.data["floor_plan_pdf_settings"] = settings
        paper_size = str(settings.get("paper_size", "A1") or "A1").upper()
        settings["paper_size"] = paper_size if paper_size in {"A0", "A1", "A2"} else "A1"
        try:
            settings["scale"] = max(10, min(5000, int(settings.get("scale", 100) or 100)))
        except (TypeError, ValueError):
            settings["scale"] = 100

        self.data.setdefault("connections", [])
        self.data.setdefault("route_profiles", deepcopy(DEFAULT_JSON["route_profiles"]))

        for location in self.data.get("locations", []):
            if not isinstance(location, dict):
                continue
            cabinet_type = str(
                location.get("cabinet_type", "standard") or "standard"
            ).strip().lower()
            location["cabinet_type"] = (
                cabinet_type
                if cabinet_type in {"standard", "slim_wall"}
                else "standard"
            )
            try:
                location["max_network_cabinets"] = max(
                    0, int(location.get("max_network_cabinets", 0) or 0)
                )
            except (TypeError, ValueError):
                location["max_network_cabinets"] = 0

        normalised_zones = []
        for index, zone in enumerate(
            self.data.get("equipment_room_placement_zones", []), start=1
        ):
            if not isinstance(zone, dict):
                continue
            try:
                x1 = float(zone.get("min_x", zone.get("x1", 0.0)))
                x2 = float(zone.get("max_x", zone.get("x2", 0.0)))
                y1 = float(zone.get("min_y", zone.get("y1", 0.0)))
                y2 = float(zone.get("max_y", zone.get("y2", 0.0)))
                floor = int(zone.get("floor", 0))
            except (TypeError, ValueError):
                continue
            allow_comms = bool(zone.get("allow_comms_room", True))
            allow_der = bool(
                zone.get("allow_distributed_equipment_room", True)
            )
            try:
                max_comms_rooms = max(
                    0, int(zone.get("max_comms_rooms", 0) or 0)
                )
            except (TypeError, ValueError):
                max_comms_rooms = 0
            try:
                max_der_rooms = max(
                    0,
                    int(zone.get("max_distributed_equipment_rooms", 0) or 0),
                )
            except (TypeError, ValueError):
                max_der_rooms = 0
            if not allow_comms and not allow_der:
                continue
            normalised_zones.append(
                {
                    **zone,
                    "id": str(zone.get("id", f"ZONE-{index}") or f"ZONE-{index}"),
                    "name": str(
                        zone.get("name", zone.get("id", f"Zone {index}"))
                        or f"Zone {index}"
                    ),
                    "floor": floor,
                    "min_x": round(min(x1, x2), 3),
                    "max_x": round(max(x1, x2), 3),
                    "min_y": round(min(y1, y2), 3),
                    "max_y": round(max(y1, y2), 3),
                    "allow_comms_room": allow_comms,
                    "allow_distributed_equipment_room": allow_der,
                    "max_comms_rooms": max_comms_rooms,
                    "max_distributed_equipment_rooms": max_der_rooms,
                }
            )
        self.data["equipment_room_placement_zones"] = normalised_zones

        for dept in self.data.get("departments", []):
            dept.setdefault("id", "")
            dept.setdefault("name", dept.get("id", ""))
            dept.setdefault("floor", 0)
            dept.setdefault("x", 0.0)
            dept.setdefault("y", 0.0)

        # in _load_from_payload(), before asset normalisation
        for category in self.data.get("asset_categories", []):
            category.setdefault("id", "")
            category.setdefault("name", category.get("id", ""))

        # replace the existing asset normalisation block
        for asset in self.data.get("assets", []):
            asset.setdefault("id", "")
            asset.setdefault("name", asset.get("id", ""))
            asset.setdefault("qty", 1)
            asset["data_points"] = int(
                asset.get(
                    "data_points",
                    asset.get("data_points_each", asset.get("cables", 1)),
                )
            )
            asset.setdefault("connection_type", asset.get("type_of_connection", "wired"))
            asset.setdefault("category_id", asset.get("category", ""))
            asset.setdefault("ADB_Code", asset.get("adb_code", ""))
            asset.setdefault("Group", asset.get("group", ""))
            asset["ADB_Code"] = str(asset.get("ADB_Code", "") or "").strip()
            asset["Group"] = str(asset.get("Group", "") or "").strip()
            capability_keywords = self.parse_capability_keywords(
                asset.get(
                    "capability_keywords",
                    asset.get("capabilities", asset.get("function_keywords", "")),
                )
            )
            asset["capabilities"] = capability_keywords
            asset["capability_keywords"] = "; ".join(capability_keywords)
            legacy_asset_group = str(
                asset.get(
                    "scenario_group",
                    asset.get("asset_scenario_group", asset.get("scenario_group_name", "")),
                )
                or ""
            ).strip()
            if legacy_asset_group:
                asset["scenario_group"] = legacy_asset_group

        def normalise_name_list(value):
            if isinstance(value, (list, tuple, set)):
                raw_values = list(value)
            elif value in (None, ""):
                raw_values = []
            else:
                text = str(value or "").strip()
                raw_values = [part.strip() for part in text.split(";")] if ";" in text else [text]
            names = []
            seen = set()
            for item in raw_values:
                name = str(item or "").strip()
                if name and name.casefold() not in seen:
                    names.append(name)
                    seen.add(name.casefold())
            return names

        normalised_scenarios = []
        for idx, scenario in enumerate(self.data.get("room_type_asset_scenarios", []) or [], start=1):
            if not isinstance(scenario, dict):
                continue
            name = str(scenario.get("name", scenario.get("id", f"Scenario {idx}")) or f"Scenario {idx}").strip()
            room_groups = normalise_name_list(scenario.get("room_groups"))
            if not room_groups:
                room_groups = normalise_name_list(scenario.get("room_group", scenario.get("scenario_group", "")))
            asset_groups = normalise_name_list(scenario.get("asset_groups"))
            if not asset_groups:
                asset_groups = normalise_name_list(scenario.get("asset_group", scenario.get("asset_scenario_group", "")))
            replacement_asset_groups = normalise_name_list(scenario.get("replacement_asset_groups"))
            if not replacement_asset_groups:
                replacement_asset_groups = normalise_name_list(
                    scenario.get(
                        "replacement_asset_group",
                        scenario.get("replacement_group", scenario.get("target_asset_group", "")),
                    )
                )
            scenario_type = str(
                scenario.get("scenario_type", scenario.get("type", scenario.get("kind", "standard"))) or "standard"
            ).strip().lower()
            if scenario_type.startswith("rep") or scenario_type in {"replace_asset", "asset_replacement"}:
                scenario_type = "replacement"
            else:
                scenario_type = "standard"
            mode = str(scenario.get("mode", "add") or "add").strip().lower()
            if mode not in {"add", "minimum", "replace"}:
                mode = "add"
            qty = self._safe_int(scenario.get("qty", 1), 1)
            normalised_scenarios.append(
                {
                    "name": name,
                    "enabled": bool(scenario.get("enabled", True)),
                    "scenario_type": scenario_type,
                    "room_group": room_groups[0] if room_groups else "",
                    "room_groups": room_groups,
                    "asset_group": asset_groups[0] if asset_groups else "",
                    "asset_groups": asset_groups,
                    "replacement_asset_group": replacement_asset_groups[0] if replacement_asset_groups else "",
                    "replacement_asset_groups": replacement_asset_groups,
                    "qty": max(1, qty),
                    "mode": mode,
                    "notes": str(scenario.get("notes", "") or "").strip(),
                }
            )
        self.data["room_type_asset_scenarios"] = normalised_scenarios

        for room_type in self.data.get("room_types", []):
            room_type.setdefault("id", "")
            room_type.setdefault("name", room_type.get("id", ""))
            legacy_room_group = str(
                room_type.get(
                    "scenario_group",
                    room_type.get("deployment_group", room_type.get("room_type_group", "")),
                )
                or ""
            ).strip()
            if legacy_room_group:
                room_type["scenario_group"] = legacy_room_group
            if "assets" not in room_type:
                room_type["assets"] = [
                    {
                        "asset_id": str(asset_id).strip(),
                        "qty": 1,
                    }
                    for asset_id in room_type.get("asset_ids", [])
                    if str(asset_id).strip()
                ]
            else:
                cleaned_assets = []
                for row in room_type.get("assets", []) or []:
                    if not isinstance(row, dict):
                        continue
                    asset_id = str(row.get("asset_id", row.get("id", ""))).strip()
                    if not asset_id:
                        continue
                    cleaned_row = {
                        "asset_id": asset_id,
                        "qty": int(row.get("qty", 1) or 1),
                    }
                    requested_by = str(row.get("requested_by", "") or "").strip()
                    if requested_by:
                        cleaned_row["requested_by"] = requested_by
                    cleaned_assets.append(cleaned_row)
                room_type["assets"] = cleaned_assets

            room_type["asset_ids"] = [
                row["asset_id"]
                for row in room_type.get("assets", [])
                if str(row.get("asset_id", "")).strip()
            ]

            for location in self.data.get("locations", []):
                if "department_ids" not in location:
                    legacy = str(location.get("department_id", "")).strip()
                    location["department_ids"] = [legacy] if legacy else []
                elif not isinstance(location.get("department_ids"), list):
                    legacy_value = location.get("department_ids")
                    if legacy_value in (None, ""):
                        location["department_ids"] = []
                    else:
                        location["department_ids"] = [str(legacy_value).strip()]
                location["department_ids"] = [
                    str(x).strip()
                    for x in location.get("department_ids", [])
                    if str(x).strip()
                ]

        for point in self.data.get("data_points", []):
            if "department_ids" not in point:
                legacy = str(point.get("department_id", "")).strip()
                point["department_ids"] = [legacy] if legacy else []
            elif not isinstance(point.get("department_ids"), list):
                legacy_value = point.get("department_ids")
                if legacy_value in (None, ""):
                    point["department_ids"] = []
                else:
                    point["department_ids"] = [str(legacy_value).strip()]
            point["department_ids"] = [
                str(x).strip()
                for x in point.get("department_ids", [])
                if str(x).strip()
            ]

        self._normalise_scenario_group_definitions()
        self.purge_missing_asset_assignments(record_change=True)

    def record_revision_change(
        self,
        source: str,
        summary: str,
        *,
        room_type_id: str = "",
        details=None,
    ) -> dict:
        """Append an exact, persistent change event for the next saved revision."""
        entry = {
            "id": uuid4().hex,
            "created_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "source": str(source or "Project").strip() or "Project",
            "summary": str(summary or "").strip(),
            "room_type_id": str(room_type_id or "").strip(),
            "details": [
                str(detail).strip()
                for detail in (details or [])
                if str(detail).strip()
            ],
        }
        self.data.setdefault("revision_change_log", []).append(entry)
        return entry

    def retired_asset_ids(self) -> set:
        return {
            str(asset_id or "").strip()
            for asset_id in self.data.get("retired_asset_ids", []) or []
            if str(asset_id or "").strip()
        }

    def next_asset_id(self, prefix: str = "A", extra_reserved=()) -> str:
        used = self.retired_asset_ids()
        used.update(
            str(asset.get("id", "") or "").strip()
            for asset in self.data.get("assets", []) or []
            if isinstance(asset, dict) and str(asset.get("id", "") or "").strip()
        )
        used.update(
            str(asset_id or "").strip()
            for asset_id in extra_reserved or []
            if str(asset_id or "").strip()
        )
        number = 1
        while f"{prefix}{number}" in used:
            number += 1
        return f"{prefix}{number}"

    def purge_missing_asset_assignments(self, *, record_change: bool = True) -> dict:
        valid_asset_ids = {
            str(asset.get("id", "") or "").strip()
            for asset in self.data.get("assets", []) or []
            if isinstance(asset, dict) and str(asset.get("id", "") or "").strip()
        }
        purged = []
        for room_type in self.data.get("room_types", []) or []:
            if not isinstance(room_type, dict):
                continue
            room_type_id = str(room_type.get("id", "") or "").strip()
            room_name = str(
                room_type.get("name", room_type_id) or room_type_id
            ).strip()
            rows = self.room_type_asset_rows(room_type)
            kept_rows = [
                row for row in rows if str(row.get("asset_id", "") or "").strip() in valid_asset_ids
            ]
            removed_ids = [
                str(row.get("asset_id", "") or "").strip()
                for row in rows
                if str(row.get("asset_id", "") or "").strip() not in valid_asset_ids
            ]
            if not removed_ids:
                continue
            room_type["assets"] = kept_rows
            room_type["asset_ids"] = [row["asset_id"] for row in kept_rows]
            purged.append(
                {
                    "room_type_id": room_type_id,
                    "room_type_name": room_name,
                    "asset_ids": removed_ids,
                }
            )

        staging_purged_count = 0
        staging = self.data.get("room_type_asset_staging", {})
        if isinstance(staging, dict) and staging:
            staging = deepcopy(staging)
            rooms = staging.get("rooms", {})
            if not isinstance(rooms, dict):
                rooms = {}
            for room_type_id in list(rooms):
                record = rooms.get(room_type_id)
                if not isinstance(record, dict):
                    rooms.pop(room_type_id, None)
                    continue
                for key in ("before", "after"):
                    original = self.room_type_asset_rows({"assets": record.get(key, [])})
                    filtered = [
                        row for row in original if row.get("asset_id") in valid_asset_ids
                    ]
                    staging_purged_count += len(original) - len(filtered)
                    record[key] = filtered
                asset_names = record.get("asset_names", {})
                record["asset_names"] = {
                    str(asset_id): name
                    for asset_id, name in (
                        asset_names.items() if isinstance(asset_names, dict) else []
                    )
                    if str(asset_id) in valid_asset_ids
                }
                if record.get("before") == record.get("after"):
                    rooms.pop(room_type_id, None)
            assets = staging.get("assets", {})
            if not isinstance(assets, dict):
                assets = {}
            for asset_id in list(assets):
                if str(asset_id) not in valid_asset_ids:
                    assets.pop(asset_id, None)
                    staging_purged_count += 1
            staging["rooms"] = rooms
            staging["assets"] = assets
            staging["changes"] = room_type_asset_staged_changes(staging)
            self.data["room_type_asset_staging"] = staging if staging["changes"] else {}

        if (purged or staging_purged_count) and record_change:
            self.record_revision_change(
                "Asset Library",
                f"Purged {sum(len(row['asset_ids']) for row in purged)} missing asset assignment(s) from room types",
                details=[
                    f"{row['room_type_id'] or row['room_type_name']}: removed "
                    + ", ".join(row["asset_ids"])
                    for row in purged
                ]
                + (
                    [
                        f"Removed {staging_purged_count} missing asset reference(s) from staged room-type changes"
                    ]
                    if staging_purged_count
                    else []
                ),
            )
        return {
            "purged": purged,
            "count": sum(len(row["asset_ids"]) for row in purged),
            "staging_purged_count": staging_purged_count,
        }

    def replace_assets(self, items) -> dict:
        new_assets = [deepcopy(item) for item in items or [] if isinstance(item, dict)]
        new_ids = [
            str(asset.get("id", "") or "").strip()
            for asset in new_assets
            if str(asset.get("id", "") or "").strip()
        ]
        if len(new_ids) != len(new_assets):
            raise ValueError("Every asset must have a non-empty ID.")
        if len(new_ids) != len(set(new_ids)):
            raise ValueError("Asset IDs must be unique.")

        previous_ids = {
            str(asset.get("id", "") or "").strip()
            for asset in self.data.get("assets", []) or []
            if isinstance(asset, dict) and str(asset.get("id", "") or "").strip()
        }
        retired_ids = self.retired_asset_ids()
        newly_added_ids = set(new_ids) - previous_ids
        reused_ids = sorted(newly_added_ids & retired_ids, key=str.casefold)
        if reused_ids:
            raise ValueError(
                "Retired asset IDs cannot be reused: " + ", ".join(reused_ids)
            )

        removed_ids = previous_ids - set(new_ids)
        retired_ids.update(removed_ids)
        self.data["retired_asset_ids"] = sorted(retired_ids, key=str.casefold)
        self.data["assets"] = new_assets

        for group in self.data.get("asset_scenario_groups", []) or []:
            if isinstance(group, dict):
                group["asset_ids"] = [
                    str(asset_id).strip()
                    for asset_id in group.get("asset_ids", []) or []
                    if str(asset_id).strip() in set(new_ids)
                ]

        purge_result = self.purge_missing_asset_assignments(record_change=False)
        if removed_ids or purge_result["count"]:
            details = [f"Retired asset ID {asset_id}" for asset_id in sorted(removed_ids)]
            details.extend(
                f"{row['room_type_id'] or row['room_type_name']}: removed "
                + ", ".join(row["asset_ids"])
                for row in purge_result["purged"]
            )
            summary_parts = []
            if removed_ids:
                summary_parts.append(
                    "Deleted and retired asset IDs: "
                    + ", ".join(sorted(removed_ids, key=str.casefold))
                )
            if purge_result["count"]:
                summary_parts.append(
                    f"purged {purge_result['count']} missing room-type assignment(s)"
                )
            self.record_revision_change(
                "Asset Library",
                "; ".join(summary_parts),
                details=details,
            )
        return {
            "removed_ids": sorted(removed_ids, key=str.casefold),
            "retired_ids": sorted(retired_ids, key=str.casefold),
            "purged_assignments": purge_result["purged"],
            "purged_count": purge_result["count"],
        }

    def _normalise_group_collection(self, collection_key: str, member_key: str, valid_member_ids, legacy_members_by_group=None) -> None:
        valid_ids = {str(member_id).strip() for member_id in valid_member_ids if str(member_id).strip()}
        legacy_members_by_group = legacy_members_by_group or {}
        groups_by_name = {}
        display_order = []

        def normalise_members(value):
            if isinstance(value, (list, tuple, set)):
                raw_members = value
            elif value in (None, ""):
                raw_members = []
            else:
                raw_members = [value]
            members = []
            seen = set()
            for member_id in raw_members:
                member_id = str(member_id).strip()
                if not member_id or member_id in seen:
                    continue
                if valid_ids and member_id not in valid_ids:
                    continue
                seen.add(member_id)
                members.append(member_id)
            return members

        def add_group(name, members=None, notes=""):
            name = str(name or "").strip()
            if not name:
                return
            key = name.casefold()
            if key not in groups_by_name:
                groups_by_name[key] = {"name": name, member_key: [], "notes": str(notes or "").strip()}
                display_order.append(key)
            elif notes and not groups_by_name[key].get("notes"):
                groups_by_name[key]["notes"] = str(notes or "").strip()
            existing = set(groups_by_name[key].get(member_key, []) or [])
            for member_id in normalise_members(members):
                if member_id not in existing:
                    groups_by_name[key].setdefault(member_key, []).append(member_id)
                    existing.add(member_id)

        for raw_group in self.data.get(collection_key, []) or []:
            if isinstance(raw_group, dict):
                name = raw_group.get("name", raw_group.get("id", raw_group.get("group", raw_group.get("group_name", ""))))
                members = raw_group.get(member_key)
                if members is None:
                    members = raw_group.get("member_ids", raw_group.get("members", raw_group.get("ids", [])))
                add_group(name, members, raw_group.get("notes", raw_group.get("description", "")))
            else:
                add_group(raw_group, [])

        for group_name, member_ids in legacy_members_by_group.items():
            add_group(group_name, member_ids)

        self.data[collection_key] = [groups_by_name[key] for key in display_order]

    def _normalise_scenario_group_definitions(self) -> None:
        room_ids = [
            str(room_type.get("id", "") or "").strip()
            for room_type in self.data.get("room_types", []) or []
            if str(room_type.get("id", "") or "").strip()
        ]
        asset_ids = [
            str(asset.get("id", "") or "").strip()
            for asset in self.data.get("assets", []) or []
            if str(asset.get("id", "") or "").strip()
        ]

        legacy_room_groups = {}
        for room_type in self.data.get("room_types", []) or []:
            room_type_id = str(room_type.get("id", "") or "").strip()
            group = str(room_type.get("scenario_group", room_type.get("deployment_group", room_type.get("room_type_group", ""))) or "").strip()
            if room_type_id and group:
                legacy_room_groups.setdefault(group, []).append(room_type_id)

        legacy_asset_groups = {}
        for asset in self.data.get("assets", []) or []:
            asset_id = str(asset.get("id", "") or "").strip()
            group = str(asset.get("scenario_group", asset.get("asset_scenario_group", "")) or "").strip()
            if asset_id and group:
                legacy_asset_groups.setdefault(group, []).append(asset_id)

        self._normalise_group_collection(
            "room_type_scenario_groups",
            "room_type_ids",
            room_ids,
            legacy_room_groups,
        )
        self._normalise_group_collection(
            "asset_scenario_groups",
            "asset_ids",
            asset_ids,
            legacy_asset_groups,
        )

    @classmethod
    def from_file(cls, path: str) -> "JsonStore":
        source = Path(path)
        if is_sqlite_project(source):
            payload = SQLiteProjectFile(source).load()
            store = cls(payload)
            store.storage_path = str(source)
            store.storage_format = "sqlite"
            return store

        payload = load_json(source)
        store = cls(payload)
        store.storage_path = str(source)
        store.storage_format = "json"
        return store

    def save(self, path: str) -> None:
        destination = Path(path)
        if destination.suffix.lower() == ".json":
            self.export_json(str(destination))
            return
        self.save_sqlite(str(destination))

    def save_sqlite(
        self,
        path: str,
        *,
        commit_message: str = "Project saved",
        auto_compact: bool = True,
        compact_min_free_bytes: int = AUTO_COMPACT_MIN_FREE_BYTES,
        compact_min_free_ratio: float = AUTO_COMPACT_MIN_FREE_RATIO,
    ) -> None:
        self.sync_all_room_type_quantities()
        destination = Path(path)
        if not destination.suffix:
            destination = destination.with_suffix(DEFAULT_EXTENSION)
        source = Path(self.storage_path) if self.storage_path else None
        if (
            source is not None
            and source.exists()
            and is_sqlite_project(source)
            and source.resolve() != destination.resolve()
        ):
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary_copy = destination.with_name(
                f".{destination.name}.save-as-{uuid4().hex}.tmp"
            )
            try:
                shutil.copy2(source, temporary_copy)
                temporary_copy.replace(destination)
            finally:
                if temporary_copy.exists():
                    temporary_copy.unlink()
        project = SQLiteProjectFile(destination)
        self.last_save_statistics = project.save(
            self.data,
            source_path=self.storage_path or "",
            commit_message=commit_message,
            auto_compact=auto_compact,
            compact_min_free_bytes=compact_min_free_bytes,
            compact_min_free_ratio=compact_min_free_ratio,
        )
        errors = project.verify()
        if errors:
            raise ValueError("SQLite project verification failed: " + "; ".join(errors))
        self.storage_path = str(destination)
        self.storage_format = "sqlite"

    def export_json(self, path: str) -> None:
        self.sync_all_room_type_quantities()
        export_json_atomic(self.data, path)

    def database_statistics(self) -> dict:
        if self.storage_format != "sqlite" or not self.storage_path:
            return {}
        return SQLiteProjectFile(self.storage_path).statistics()

    def revision_history(self, limit: Optional[int] = None) -> List[dict]:
        if self.storage_format != "sqlite" or not self.storage_path:
            return []
        return SQLiteProjectFile(self.storage_path).revision_history(limit=limit)

    def revision_data(self, revision_number: int) -> dict:
        if self.storage_format != "sqlite" or not self.storage_path:
            raise ValueError("Save this project as a .crsdb database before checking out a revision.")
        return SQLiteProjectFile(self.storage_path).load_revision(revision_number)

    def restore_revision(self, revision_number: int):
        if self.storage_format != "sqlite" or not self.storage_path:
            raise ValueError("Save this project as a .crsdb database before rolling it back.")
        project = SQLiteProjectFile(self.storage_path)
        restored, statistics = project.restore_revision(revision_number)
        errors = project.verify()
        if errors:
            raise ValueError("SQLite project verification failed: " + "; ".join(errors))
        self.data = deepcopy(DEFAULT_JSON)
        self._load_from_payload(restored)
        self.last_save_statistics = statistics
        return statistics

    def database_space_usage(self) -> dict:
        if self.storage_format != "sqlite" or not self.storage_path:
            return {}
        usage = SQLiteProjectFile(self.storage_path).space_usage()
        return {
            "path": usage.path,
            "file_size_bytes": usage.file_size_bytes,
            "page_size_bytes": usage.page_size_bytes,
            "page_count": usage.page_count,
            "free_page_count": usage.free_page_count,
            "reclaimable_bytes": usage.reclaimable_bytes,
            "free_ratio": usage.free_ratio,
        }

    def compact_database(self, *, force: bool = True):
        if self.storage_format != "sqlite" or not self.storage_path:
            return None
        self.last_compaction_statistics = SQLiteProjectFile(
            self.storage_path
        ).compact(force=force)
        return self.last_compaction_statistics

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
        """Query indexed SQLite records, with an in-memory fallback.

        The current editor continues to expose the complete data dictionary,
        but renderers and reports can use this method to adopt floor/viewport
        lazy loading without another file-format migration.
        """

        if self.storage_format == "sqlite" and self.storage_path:
            return SQLiteProjectFile(self.storage_path).query_records(
                section_key,
                floor=floor,
                kind=kind,
                bounds=bounds,
                record_id=record_id,
                parent_id=parent_id,
            )

        if section_key == "corridors.nodes":
            rows = self.data.get("corridors", {}).get("nodes", [])
        elif section_key == "corridors.edges":
            rows = self.data.get("corridors", {}).get("edges", [])
        else:
            rows = self.data.get(section_key, [])
        if not isinstance(rows, list):
            return []
        result = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if floor is not None:
                try:
                    if int(row.get("floor")) != int(floor):
                        continue
                except (TypeError, ValueError):
                    continue
            row_kind = str(
                row.get(
                    "kind",
                    row.get("asset_type", row.get("node_type", row.get("design_role", ""))),
                )
                or ""
            )
            if kind and row_kind != kind:
                continue
            if record_id and str(row.get("id", row.get("name", ""))) != record_id:
                continue
            if parent_id and str(
                row.get("parent_id", row.get("parent_node_id", row.get("from_instance_id", row.get("from", ""))))
            ) != parent_id:
                continue
            if bounds is not None:
                try:
                    x = float(row.get("x"))
                    y = float(row.get("y"))
                except (TypeError, ValueError):
                    continue
                min_x, min_y, max_x, max_y = bounds
                if not (min_x <= x <= max_x and min_y <= y <= max_y):
                    continue
            result.append(row)
        return result

    def floor_dxf_path(self, floor: int) -> Optional[str]:
        for entry in self.data.get("floor_dxf_files", []):
            try:
                if int(entry.get("floor")) == int(floor):
                    path = (entry.get("filepath") or "").strip()
                    return path or None
            except Exception:
                continue
        return None

    def set_floor_dxf_path(self, floor: int, filepath: str) -> None:
        entries = self.data.setdefault("floor_dxf_files", [])
        payload = {"floor": int(floor), "filepath": str(filepath)}
        for entry in entries:
            try:
                if int(entry.get("floor")) == int(floor):
                    entry.clear()
                    entry.update(payload)
                    return
            except Exception:
                continue
        entries.append(payload)
        entries.sort(key=lambda item: int(item.get("floor", 0)))

    def clear_floor_dxf_path(self, floor: int) -> None:
        self.data["floor_dxf_files"] = [
            entry
            for entry in self.data.get("floor_dxf_files", [])
            if int(entry.get("floor", -(10**9))) != int(floor)
        ]

    def names_in_use(self) -> set:
        names = set()
        for point in self.all_points().values():
            names.add(point["name"])
        return names

    def department_ids(self) -> set:
        return {
            str(item.get("id", "")).strip()
            for item in self.data.get("departments", [])
            if str(item.get("id", "")).strip()
        }

    def departments_for_floor(self, floor: int) -> Dict[str, dict]:
        result = {}
        for item in self.data.get("departments", []):
            if int(item.get("floor", 0)) != int(floor):
                continue
            dept_id = str(item.get("id", "")).strip()
            if not dept_id:
                continue
            result[dept_id] = {
                **item,
                "id": dept_id,
                "name": str(item.get("name", dept_id)),
                "floor": int(item.get("floor", 0)),
                "x": float(item.get("x", 0.0)),
                "y": float(item.get("y", 0.0)),
                "kind": "department",
            }
        return result

    def add_department(
        self,
        name: str,
        floor: int,
        x: float,
        y: float,
        department_id: Optional[str] = None,
    ) -> str:
        if department_id is None or not str(department_id).strip():
            department_id = self.suggest_next_department_id()
        department_id = str(department_id).strip()

        self.data["departments"].append(
            {
                "id": department_id,
                "name": str(name).strip(),
                "floor": int(floor),
                "x": round(x, 3),
                "y": round(y, 3),
            }
        )
        return department_id

    def set_department_position(self, department_id: str, x: float, y: float) -> None:
        x = round(x, 3)
        y = round(y, 3)
        for item in self.data.get("departments", []):
            if str(item.get("id", "")).strip() == str(department_id).strip():
                item["x"] = x
                item["y"] = y
                return

    def all_points(self) -> Dict[str, dict]:
        result: Dict[str, dict] = {}

        for item in self.data.get("locations", []):
            kind = str(item.get("kind", "location") or "location")
            result[item["name"]] = {**item, "kind": kind}

        for item in self.data.get("data_points", []):
            result[item["name"]] = {**item, "kind": "data_point"}

        for item in self.data.get("corridors", {}).get("nodes", []):
            result[item["name"]] = {**item, "kind": "corridor_node"}

        for transition in self.data.get("transitions", []):
            transition_id = transition["id"]
            for floor_str, pos in transition.get("floor_locations", {}).items():
                result[f"{transition_id}-F{floor_str}"] = {
                    "name": f"{transition_id}-F{floor_str}",
                    "floor": int(floor_str),
                    "x": float(pos["x"]),
                    "y": float(pos["y"]),
                    "kind": "transition_node",
                    "transition_id": transition_id,
                }

        return result

    def points_for_floor(self, floor: int) -> Dict[str, dict]:
        return {
            name: point
            for name, point in self.all_points().items()
            if int(point["floor"]) == int(floor)
        }

    def edges_for_floor(self, floor: int) -> List[dict]:
        points = self.all_points()
        edges = []
        for edge in self.data.get("corridors", {}).get("edges", []):
            a = points.get(edge["from"])
            b = points.get(edge["to"])
            if (
                a
                and b
                and int(a["floor"]) == int(floor)
                and int(b["floor"]) == int(floor)
            ):
                edges.append(edge)
        return edges

    def add_corridor_node(
        self,
        name: str,
        floor: int,
        x: float,
        y: float,
        height_affl_m: float = 0.0,
        cable_limit: int = 0,
    ) -> None:
        self.data["corridors"]["nodes"].append(
            {
                "name": name,
                "floor": floor,
                "x": round(x, 3),
                "y": round(y, 3),
                "height_affl_m": float(height_affl_m),
                "cable_limit": int(cable_limit),
            }
        )

    def add_location(
        self,
        name: str,
        floor: int,
        x: float,
        y: float,
        kind: str = "location",
        department_ids: Optional[List[str]] = None,
        max_cable_length_m: float = 90.0,
    ) -> None:
        self.data["locations"].append(
            {
                "name": name,
                "floor": floor,
                "x": round(x, 3),
                "y": round(y, 3),
                "kind": kind,
                "department_ids": [
                    str(x).strip() for x in (department_ids or []) if str(x).strip()
                ],
                "max_cable_length_m": max(0.1, float(max_cable_length_m)),
            }
        )

    def add_data_point(
        self,
        name: str,
        floor: int,
        x: float,
        y: float,
        qty: int = 1,
        extension_distance_m: float = 0.0,
        department_ids: Optional[List[str]] = None,
        room_type_id: str = "",
    ) -> None:
        room_type_id = str(room_type_id or "").strip()
        resolved_qty = (
            self.room_type_cable_qty(room_type_id) if room_type_id else int(qty)
        )

        self.data["data_points"].append(
            {
                "name": name,
                "floor": floor,
                "x": round(x, 3),
                "y": round(y, 3),
                "qty": int(resolved_qty),
                "room_type_id": room_type_id,
                "extension_distance_m": float(extension_distance_m),
                "department_ids": [
                    str(x).strip() for x in (department_ids or []) if str(x).strip()
                ],
            }
        )

    def add_edge(self, from_name: str, to_name: str) -> None:
        edges = self.data["corridors"]["edges"]
        if not any(e["from"] == from_name and e["to"] == to_name for e in edges):
            edges.append({"from": from_name, "to": to_name})

    def remove_edge(self, from_name: str, to_name: str) -> None:
        self.data["corridors"]["edges"] = [
            e
            for e in self.data["corridors"]["edges"]
            if not (e["from"] == from_name and e["to"] == to_name)
        ]

    def set_point_position(self, name: str, x: float, y: float) -> None:
        x = round(x, 3)
        y = round(y, 3)

        for item in self.data.get("departments", []):
            if str(item.get("id", "")).strip() == str(name).strip():
                item["x"] = x
                item["y"] = y
                return

        for item in self.data.get("locations", []):
            if item["name"] == name:
                item["x"] = x
                item["y"] = y
                return
        for item in self.data.get("data_points", []):
            if item["name"] == name:
                item["x"] = x
                item["y"] = y
                return
        for item in self.data.get("corridors", {}).get("nodes", []):
            if item["name"] == name:
                item["x"] = x
                item["y"] = y
                return
        if "-F" in name:
            transition_id, floor_text = name.rsplit("-F", 1)
            for transition in self.data.get("transitions", []):
                if transition["id"] == transition_id and floor_text in transition.get(
                    "floor_locations", {}
                ):
                    transition["floor_locations"][floor_text]["x"] = x
                    transition["floor_locations"][floor_text]["y"] = y
                    return

    def move_transition_from_floor_up(
        self,
        transition_id: str,
        from_floor: int,
        new_x: float,
        new_y: float,
    ) -> int:
        transition_id = str(transition_id).strip()
        from_floor = int(from_floor)
        new_x = round(float(new_x), 3)
        new_y = round(float(new_y), 3)

        for transition in self.data.get("transitions", []):
            if str(transition.get("id", "")).strip() != transition_id:
                continue

            floor_locations = transition.setdefault("floor_locations", {})
            current = floor_locations.get(str(from_floor))
            if current is None:
                current = floor_locations.get(from_floor)

            if not isinstance(current, dict):
                return 0

            old_x = float(current.get("x", 0.0))
            old_y = float(current.get("y", 0.0))

            dx = new_x - old_x
            dy = new_y - old_y

            moved_count = 0

            for floor_key, pos in list(floor_locations.items()):
                try:
                    floor = int(floor_key)
                except Exception:
                    continue

                if floor < from_floor:
                    continue

                if not isinstance(pos, dict):
                    continue

                pos["x"] = round(float(pos.get("x", 0.0)) + dx, 3)
                pos["y"] = round(float(pos.get("y", 0.0)) + dy, 3)
                moved_count += 1

            return moved_count

        return 0

    def rename_point(self, old_name: str, new_name: str) -> None:
        if old_name == new_name:
            return
        for collection in (
            self.data.get("locations", []),
            self.data.get("data_points", []),
            self.data.get("corridors", {}).get("nodes", []),
        ):
            for item in collection:
                if item["name"] == old_name:
                    item["name"] = new_name

        for edge in self.data.get("corridors", {}).get("edges", []):
            if edge["from"] == old_name:
                edge["from"] = new_name
            if edge["to"] == old_name:
                edge["to"] = new_name

        for connection in self.data.get("connections", []):
            if connection.get("from") == old_name:
                connection["from"] = new_name
            if connection.get("to") == old_name:
                connection["to"] = new_name

        for profile in self.data.get("route_profiles", {}).values():
            profile["allowed_nodes"] = [
                new_name if x == old_name else x
                for x in profile.get("allowed_nodes", [])
            ]
            profile["allowed_edges"] = [
                [new_name if part == old_name else part for part in edge_pair]
                for edge_pair in profile.get("allowed_edges", [])
            ]

    def delete_point(self, name: str) -> None:
        self.data["locations"] = [
            x for x in self.data.get("locations", []) if x["name"] != name
        ]
        self.data["data_points"] = [
            x for x in self.data.get("data_points", []) if x["name"] != name
        ]
        self.data["corridors"]["nodes"] = [
            x
            for x in self.data.get("corridors", {}).get("nodes", [])
            if x["name"] != name
        ]
        self.data["corridors"]["edges"] = [
            e
            for e in self.data.get("corridors", {}).get("edges", [])
            if e["from"] != name and e["to"] != name
        ]
        self.data["connections"] = [
            c
            for c in self.data.get("connections", [])
            if c.get("from") != name and c.get("to") != name
        ]
        for profile in self.data.get("route_profiles", {}).values():
            profile["allowed_nodes"] = [
                x for x in profile.get("allowed_nodes", []) if x != name
            ]
            profile["allowed_edges"] = [
                pair for pair in profile.get("allowed_edges", []) if name not in pair
            ]

    def rename_department(
        self, old_id: str, new_id: str, new_name: Optional[str] = None
    ) -> None:
        old_id = str(old_id).strip()
        new_id = str(new_id).strip()
        for item in self.data.get("departments", []):
            if str(item.get("id", "")).strip() == old_id:
                item["id"] = new_id
                if new_name is not None:
                    item["name"] = str(new_name).strip()
                break

        for location in self.data.get("locations", []):
            updated = []
            for department_id in location.get("department_ids", []):
                updated.append(
                    new_id
                    if str(department_id).strip() == old_id
                    else str(department_id).strip()
                )
            location["department_ids"] = updated

    def delete_department(self, department_id: str) -> None:
        department_id = str(department_id).strip()
        self.data["departments"] = [
            item
            for item in self.data.get("departments", [])
            if str(item.get("id", "")).strip() != department_id
        ]

        for location in self.data.get("locations", []):
            location["department_ids"] = [
                str(x).strip()
                for x in location.get("department_ids", [])
                if str(x).strip() != department_id
            ]

    def upsert_transition(
        self,
        transition_id: str,
        floors: List[int],
        floor_locations: Dict[int, Tuple[float, float]],
        cable_limit: int = 0,
    ) -> None:
        transition = None
        for existing in self.data["transitions"]:
            if existing["id"] == transition_id:
                transition = existing
                break
        payload = {
            "id": transition_id,
            "floors": sorted(int(f) for f in floors),
            "cable_limit": int(cable_limit),
            "floor_locations": {
                str(f): {"x": round(pos[0], 3), "y": round(pos[1], 3)}
                for f, pos in floor_locations.items()
            },
        }
        if transition is None:
            self.data["transitions"].append(payload)
        else:
            transition.clear()
            transition.update(payload)

    def delete_transition(self, transition_id: str) -> None:
        names_to_delete = {
            f"{transition_id}-F{floor}"
            for transition in self.data.get("transitions", [])
            if transition["id"] == transition_id
            for floor in transition.get("floor_locations", {}).keys()
        }
        self.data["transitions"] = [
            x for x in self.data.get("transitions", []) if x["id"] != transition_id
        ]
        self.data["corridors"]["edges"] = [
            e
            for e in self.data.get("corridors", {}).get("edges", [])
            if e["from"] not in names_to_delete and e["to"] not in names_to_delete
        ]
        for profile in self.data.get("route_profiles", {}).values():
            profile["allowed_transitions"] = [
                x for x in profile.get("allowed_transitions", []) if x != transition_id
            ]
            profile["allowed_nodes"] = [
                x for x in profile.get("allowed_nodes", []) if x not in names_to_delete
            ]
            profile["allowed_edges"] = [
                pair
                for pair in profile.get("allowed_edges", [])
                if not any(name in pair for name in names_to_delete)
            ]

    def validate(self) -> List[str]:
        errors = []
        names = self.names_in_use()
        department_ids = self.department_ids()
        route_profile_names = set(self.data.get("route_profiles", {}).keys())
        transition_ids = {x.get("id") for x in self.data.get("transitions", [])}

        for edge in self.data.get("corridors", {}).get("edges", []):
            if edge.get("from") not in names:
                errors.append(f"Unknown edge start: {edge.get('from')}")
            if edge.get("to") not in names:
                errors.append(f"Unknown edge end: {edge.get('to')}")

        for connection in self.data.get("connections", []):
            connection_id = connection.get("id", "-")
            if connection.get("from") not in names:
                errors.append(
                    f"Connection {connection_id} start not found: {connection.get('from')}"
                )
            if connection.get("to") not in names:
                errors.append(
                    f"Connection {connection_id} end not found: {connection.get('to')}"
                )
            rp = connection.get("route_profile", "")
            if rp and rp not in route_profile_names:
                errors.append(
                    f"Connection {connection_id} route profile not found: {rp}"
                )
            try:
                qty = int(connection.get("qty", 1))
                if qty < 0:
                    errors.append(
                        f"Connection {connection_id} qty cannot be negative"
                    )
            except Exception:
                errors.append(f"Connection {connection_id} has invalid qty")

        for profile_name, profile in self.data.get("route_profiles", {}).items():
            for transition_id in profile.get("allowed_transitions", []):
                if transition_id not in transition_ids:
                    errors.append(
                        f"Route profile {profile_name} has unknown transition: {transition_id}"
                    )
            for node_name in profile.get("allowed_nodes", []):
                if node_name not in names:
                    errors.append(
                        f"Route profile {profile_name} has unknown node: {node_name}"
                    )
            for edge_pair in profile.get("allowed_edges", []):
                if len(edge_pair) != 2:
                    errors.append(
                        f"Route profile {profile_name} has invalid edge pair: {edge_pair}"
                    )
                    continue
                if edge_pair[0] not in names or edge_pair[1] not in names:
                    errors.append(
                        f"Route profile {profile_name} has unknown edge endpoint: {edge_pair}"
                    )

        for node in self.data.get("corridors", {}).get("nodes", []):
            if not str(node.get("name", "")).strip():
                errors.append("Corridor node has no name")

        for point in self.data.get("data_points", []):
            try:
                qty = int(point.get("qty", 1))
                if qty < 0:
                    errors.append(
                        f"Data point {point.get('name', '-')} qty cannot be negative"
                    )
            except Exception:
                errors.append(f"Data point {point.get('name', '-')} has invalid qty")

            raw_ids = point.get("department_ids", [])
            if not isinstance(raw_ids, list):
                errors.append(
                    f"Data point {point.get('name', '-')} has invalid department_ids"
                )
                continue

            for department_id in raw_ids:
                department_id = str(department_id).strip()
                if department_id and department_id not in self.department_ids():
                    errors.append(
                        f"Data point {point.get('name', '-')} references unknown department: {department_id}"
                    )

        seen_floors = set()
        for entry in self.data.get("floor_dxf_files", []):
            if not isinstance(entry, dict):
                errors.append(f"Invalid floor_dxf_files entry: {entry}")
                continue
            try:
                floor = int(entry.get("floor"))
            except Exception:
                errors.append(f"DXF mapping has invalid floor: {entry.get('floor')}")
                continue
            filepath = str(entry.get("filepath") or "").strip()
            if not filepath:
                errors.append(f"DXF mapping for floor {floor} has empty filepath")
            if floor in seen_floors:
                errors.append(f"Duplicate DXF mapping for floor {floor}")
            seen_floors.add(floor)

        seen_department_ids = set()
        for dept in self.data.get("departments", []):
            dept_id = str(dept.get("id", "")).strip()
            if not dept_id:
                errors.append("Department has no ID")
                continue
            if dept_id in seen_department_ids:
                errors.append(f"Duplicate department ID: {dept_id}")
            seen_department_ids.add(dept_id)

        for location in self.data.get("locations", []):
            raw_ids = location.get("department_ids", [])
            if not isinstance(raw_ids, list):
                errors.append(
                    f"Location {location.get('name', '-')} has invalid department_ids"
                )
                continue

            for department_id in raw_ids:
                department_id = str(department_id).strip()
                if department_id and department_id not in department_ids:
                    errors.append(
                        f"Location {location.get('name', '-')} references unknown department: {department_id}"
                    )
        return errors

    def invalid_connections_and_routes(self) -> dict:
        """Return structurally invalid cable connections and routing edges.

        The check is deliberately read-only and limited to ``connections`` and
        ``corridors.edges``. A connection is invalid when its endpoints/profile/
        quantity are invalid or no permitted graph route joins its endpoints.
        """
        points = self.all_points()
        invalid_routes = []
        valid_edges = []
        for index, edge in enumerate(self.data.get("corridors", {}).get("edges", [])):
            start = str(edge.get("from", "") or "").strip()
            end = str(edge.get("to", "") or "").strip()
            reason = ""
            if start not in points or end not in points:
                reason = "missing route endpoint"
            elif start == end:
                reason = "route starts and ends at the same point"
            elif int(points[start].get("floor", 0)) != int(points[end].get("floor", 0)):
                reason = "cross-floor route does not use a transition"
            if reason:
                invalid_routes.append(
                    {"index": index, "from": start, "to": end, "reason": reason}
                )
            else:
                valid_edges.append((start, end))

        graph = {name: set() for name in points}
        for start, end in valid_edges:
            graph[start].add(end)
            graph[end].add(start)
        for transition in self.data.get("transitions", []):
            transition_id = str(transition.get("id", "") or "").strip()
            names = [
                f"{transition_id}-F{floor}"
                for floor in (transition.get("floor_locations", {}) or {})
                if f"{transition_id}-F{floor}" in points
            ]
            for first_index, start in enumerate(names):
                for end in names[first_index + 1 :]:
                    graph[start].add(end)
                    graph[end].add(start)

        profiles = self.data.get("route_profiles", {}) or {}

        def has_route(start, end, profile):
            allowed_nodes = set(profile.get("allowed_nodes", []) or [])
            allowed_edges = {
                tuple(pair)
                for pair in (profile.get("allowed_edges", []) or [])
                if isinstance(pair, (list, tuple)) and len(pair) == 2
            }
            allowed_transitions = set(profile.get("allowed_transitions", []) or [])

            def transition_id(name):
                point = points.get(name, {})
                if str(point.get("kind", "")) != "transition_node" or "-F" not in name:
                    return ""
                return name.rsplit("-F", 1)[0]

            def node_allowed(name):
                point = points.get(name, {})
                if allowed_nodes and name not in allowed_nodes and str(
                    point.get("kind", "")
                ) != "transition_node":
                    return False
                node_transition = transition_id(name)
                return not (
                    allowed_transitions
                    and node_transition
                    and node_transition not in allowed_transitions
                )

            if not node_allowed(start) or not node_allowed(end):
                return False
            pending = [start]
            visited = {start}
            while pending:
                current = pending.pop()
                if current == end:
                    return True
                for neighbour in graph.get(current, set()):
                    if neighbour in visited or not node_allowed(neighbour):
                        continue
                    if allowed_edges and (current, neighbour) not in allowed_edges:
                        same_transition = (
                            transition_id(current)
                            and transition_id(current) == transition_id(neighbour)
                        )
                        if not same_transition:
                            continue
                    visited.add(neighbour)
                    pending.append(neighbour)
            return False

        invalid_connections = []
        for index, connection in enumerate(self.data.get("connections", [])):
            connection_id = str(connection.get("id", "") or "").strip() or f"row {index + 1}"
            start = str(connection.get("from", "") or "").strip()
            end = str(connection.get("to", "") or "").strip()
            profile_name = str(connection.get("route_profile", "") or "").strip()
            reason = ""
            if start not in points or end not in points:
                reason = "missing connection endpoint"
            elif start == end:
                reason = "connection starts and ends at the same point"
            else:
                try:
                    if int(connection.get("qty", 1)) < 0:
                        reason = "quantity is negative"
                except (TypeError, ValueError):
                    reason = "quantity is invalid"
            if not reason and profile_name and profile_name not in profiles:
                reason = "route profile does not exist"
            if not reason:
                profile = profiles.get(profile_name, {}) if profile_name else {}
                if not has_route(start, end, profile):
                    reason = "no permitted graph route joins the endpoints"
            if reason:
                invalid_connections.append(
                    {
                        "index": index,
                        "id": connection_id,
                        "from": start,
                        "to": end,
                        "reason": reason,
                    }
                )

        return {"connections": invalid_connections, "routes": invalid_routes}

    def remove_invalid_connections_and_routes(self) -> dict:
        """Remove only invalid cable connections and corridor routing edges."""
        invalid = self.invalid_connections_and_routes()
        connection_indexes = {row["index"] for row in invalid["connections"]}
        route_indexes = {row["index"] for row in invalid["routes"]}
        self.data["connections"] = [
            row for index, row in enumerate(self.data.get("connections", []))
            if index not in connection_indexes
        ]
        self.data.setdefault("corridors", {})["edges"] = [
            row
            for index, row in enumerate(self.data.get("corridors", {}).get("edges", []))
            if index not in route_indexes
        ]
        return invalid

    def suggest_next_department_id(self) -> str:
        nums = []
        for item in self.data.get("departments", []):
            dept_id = str(item.get("id", "")).strip()
            if dept_id.isdigit():
                nums.append(int(dept_id))
        return str(max(nums, default=0) + 1)

    def suggest_next_corridor_name(self, floor: int) -> str:
        prefix = f"C{floor}-"
        nums = []
        for item in self.data.get("corridors", {}).get("nodes", []):
            name = item["name"]
            if name.startswith(prefix):
                tail = name[len(prefix) :]
                if tail.isdigit():
                    nums.append(int(tail))
        return f"C{floor}-{max(nums, default=0) + 1}"

    def suggest_next_data_point_name(self, floor: int) -> str:
        prefix = f"DP{floor}-"
        nums = []
        for item in self.data.get("data_points", []):
            name = str(item.get("name", ""))
            if name.startswith(prefix):
                tail = name[len(prefix) :]
                if tail.isdigit():
                    nums.append(int(tail))
        return f"DP{floor}-{max(nums, default=0) + 1}"

    def suggest_next_transition_id(self) -> str:
        nums = []
        for item in self.data.get("transitions", []):
            transition_id = str(item.get("id", ""))
            if transition_id.startswith("TR-") and transition_id[3:].isdigit():
                nums.append(int(transition_id[3:]))
        return f"TR-{max(nums, default=0) + 1}"

    def suggest_next_connection_id(self) -> str:
        nums = []
        for connection in self.data.get("connections", []):
            connection_id = str(connection.get("id", ""))
            if connection_id.startswith("C") and connection_id[1:].isdigit():
                nums.append(int(connection_id[1:]))
        return f"C{max(nums, default=0) + 1}"

    def _point_record_by_name(self, name: str):
        name = str(name).strip()

        for item in self.data.get("corridors", {}).get("nodes", []):
            if str(item.get("name", "")).strip() == name:
                return "corridor_node", item

        for item in self.data.get("locations", []):
            if str(item.get("name", "")).strip() == name:
                kind = str(item.get("kind", "location") or "location").strip()
                if kind in {"comms_room", "distributed_equipment_room"}:
                    return kind, item

        for item in self.data.get("data_points", []):
            if str(item.get("name", "")).strip() == name:
                return "data_point", item

        return None, None

    def _suggest_next_comms_room_name_for_floor(
        self, floor: int, used_names: set, kind: str
    ) -> str:
        prefix = "DER" if kind == "distributed_equipment_room" else "CR"
        floor = int(floor)
        pattern = re.compile(rf"^{re.escape(prefix)}(\d+)-F{floor}$", re.IGNORECASE)

        nums = []
        for name in used_names:
            match = pattern.match(str(name).strip())
            if match:
                nums.append(int(match.group(1)))

        return f"{prefix}{max(nums, default=0) + 1}-F{floor}"

    def _suggest_next_corridor_name_for_floor(self, floor: int, used_names: set) -> str:
        prefix = f"C{int(floor)}-"
        nums = []
        for name in used_names:
            if str(name).startswith(prefix):
                tail = str(name)[len(prefix) :]
                if tail.isdigit():
                    nums.append(int(tail))
        return f"C{int(floor)}-{max(nums, default=0) + 1}"

    def _suggest_next_data_point_name_for_floor(
        self, floor: int, used_names: set
    ) -> str:
        prefix = f"DP{int(floor)}-"
        nums = []
        for name in used_names:
            if str(name).startswith(prefix):
                tail = str(name)[len(prefix) :]
                if tail.isdigit():
                    nums.append(int(tail))
        return f"DP{int(floor)}-{max(nums, default=0) + 1}"

    def clone_template_between_floors(
        self,
        source_names: List[str],
        target_floor: int,
        include_internal_edges: bool = True,
        offset_x: float = 0.0,
        offset_y: float = 0.0,
    ) -> dict:
        selected_names = [
            str(x).strip() for x in (source_names or []) if str(x).strip()
        ]
        if not selected_names:
            raise ValueError("Select one or more corridor nodes or data points")

        used_names = set(self.names_in_use())
        selected_set = set(selected_names)

        id_map = {}
        created_corridors = []
        created_data_points = []
        created_locations = []
        skipped = []

        for old_name in selected_names:
            kind, record = self._point_record_by_name(old_name)
            if kind is None or record is None:
                skipped.append(old_name)
                continue

            if kind == "corridor_node":
                new_name = self._suggest_next_corridor_name_for_floor(
                    target_floor, used_names
                )
                used_names.add(new_name)

                new_record = {
                    "name": new_name,
                    "floor": int(target_floor),
                    "x": round(float(record.get("x", 0.0)) + float(offset_x), 3),
                    "y": round(float(record.get("y", 0.0)) + float(offset_y), 3),
                    "height_affl_m": float(record.get("height_affl_m", 0.0) or 0.0),
                    "cable_limit": int(record.get("cable_limit", 0) or 0),
                    "restricted": bool(record.get("restricted", False)),
                }
                self.data.setdefault("corridors", {}).setdefault("nodes", []).append(
                    new_record
                )
                id_map[old_name] = new_name
                created_corridors.append(new_name)

            elif kind in {"comms_room", "distributed_equipment_room"}:
                new_name = self._suggest_next_comms_room_name_for_floor(
                    target_floor, used_names, kind
                )
                used_names.add(new_name)

                new_record = {
                    "name": new_name,
                    "floor": int(target_floor),
                    "x": round(float(record.get("x", 0.0)) + float(offset_x), 3),
                    "y": round(float(record.get("y", 0.0)) + float(offset_y), 3),
                    "kind": kind,
                    "department_ids": [],
                    "max_cable_length_m": max(
                        0.1, float(record.get("max_cable_length_m", 90.0) or 90.0)
                    ),
                }

                self.data.setdefault("locations", []).append(new_record)
                id_map[old_name] = new_name
                created_locations.append(new_name)

            elif kind == "data_point":
                new_name = self._suggest_next_data_point_name_for_floor(
                    target_floor, used_names
                )
                used_names.add(new_name)

                new_record = {
                    "name": new_name,
                    "floor": int(target_floor),
                    "x": round(float(record.get("x", 0.0)) + float(offset_x), 3),
                    "y": round(float(record.get("y", 0.0)) + float(offset_y), 3),
                    "qty": int(record.get("qty", 1) or 1),
                    "extension_distance_m": float(
                        record.get("extension_distance_m", 0.0) or 0.0
                    ),
                    "department_ids": [],
                    "created_locations": created_locations,
                }
                self.data.setdefault("data_points", []).append(new_record)
                id_map[old_name] = new_name
                created_data_points.append(new_name)

        created_edges = []
        if include_internal_edges and id_map:
            existing_edges = self.data.setdefault("corridors", {}).setdefault(
                "edges", []
            )
            existing_pairs = {
                (str(edge.get("from", "")).strip(), str(edge.get("to", "")).strip())
                for edge in existing_edges
            }

            for edge in list(existing_edges):
                start = str(edge.get("from", "")).strip()
                end = str(edge.get("to", "")).strip()

                if start not in selected_set or end not in selected_set:
                    continue
                if start not in id_map or end not in id_map:
                    continue

                new_start = id_map[start]
                new_end = id_map[end]
                pair = (new_start, new_end)

                if pair in existing_pairs:
                    continue

                existing_edges.append({"from": new_start, "to": new_end})
                existing_pairs.add(pair)
                created_edges.append({"from": new_start, "to": new_end})

        return {
            "id_map": id_map,
            "created_corridors": created_corridors,
            "created_data_points": created_data_points,
            "created_edges": created_edges,
            "skipped": skipped,
        }

    @staticmethod
    def _safe_int(value, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return int(default)

    def room_type_asset_rows(self, room_type: dict) -> List[dict]:
        """Return normalised asset/quantity rows for a room type.

        Older projects stored room-type assets as an ``asset_ids`` list.  Newer
        projects store ``assets`` rows so that each room type can hold multiple
        units of the same endpoint asset.
        """
        rows: List[dict] = []
        seen = set()

        for row in room_type.get("assets", []) or []:
            if not isinstance(row, dict):
                continue
            asset_id = str(row.get("asset_id", row.get("id", ""))).strip()
            if not asset_id:
                continue
            qty = max(1, self._safe_int(row.get("qty", 1), 1))
            normalised = {"asset_id": asset_id, "qty": qty}
            requested_by = str(row.get("requested_by", "") or "").strip()
            if requested_by:
                normalised["requested_by"] = requested_by
            rows.append(normalised)
            seen.add(asset_id)

        for asset_id in room_type.get("asset_ids", []) or []:
            asset_id = str(asset_id).strip()
            if asset_id and asset_id not in seen:
                rows.append({"asset_id": asset_id, "qty": 1})
                seen.add(asset_id)

        return rows

    def restore_historical_assets(
        self,
        historical_data: dict,
        asset_ids,
        *,
        source_revision: int = 0,
    ) -> dict:
        """Recover selected historical assets and their missing room assignments.

        Existing asset definitions and assignment rows belong to the current
        project and are retained. A definition is copied from history only when
        that asset no longer exists, and historical assignment metadata is used
        only when adding a missing room-type assignment.
        """

        selected_ids = []
        seen = set()
        for value in asset_ids or []:
            asset_id = str(value or "").strip()
            if asset_id and asset_id not in seen:
                selected_ids.append(asset_id)
                seen.add(asset_id)

        historical_assets = {
            str(asset.get("id", "") or "").strip(): asset
            for asset in historical_data.get("assets", []) or []
            if isinstance(asset, dict) and str(asset.get("id", "") or "").strip()
        }
        current_assets = self.data.setdefault("assets", [])
        current_assets_by_id = {
            str(asset.get("id", "") or "").strip(): asset
            for asset in current_assets
            if isinstance(asset, dict) and str(asset.get("id", "") or "").strip()
        }
        current_rooms = self.room_type_lookup()
        historical_rooms = [
            room
            for room in historical_data.get("room_types", []) or []
            if isinstance(room, dict)
        ]

        result = {
            "selected_asset_ids": selected_ids,
            "restored_asset_ids": [],
            "retained_latest_asset_ids": [],
            "restored_assignments": [],
            "existing_assignments": [],
            "missing_room_types": [],
            "missing_asset_ids": [],
            "asset_id_map": {},
        }

        for asset_id in selected_ids:
            historical_asset = historical_assets.get(asset_id)
            if historical_asset is None:
                result["missing_asset_ids"].append(asset_id)
                continue

            definition_restored = asset_id not in current_assets_by_id
            target_asset_id = asset_id
            if definition_restored:
                retired_ids = self.retired_asset_ids()
                retired_ids.add(asset_id)
                self.data["retired_asset_ids"] = sorted(retired_ids, key=str.casefold)
                target_asset_id = self.next_asset_id()
                restored_asset = deepcopy(historical_asset)
                restored_asset["id"] = target_asset_id
                current_assets.append(restored_asset)
                current_assets_by_id[target_asset_id] = restored_asset
                result["restored_asset_ids"].append(target_asset_id)
                result["asset_id_map"][asset_id] = target_asset_id
            else:
                result["retained_latest_asset_ids"].append(asset_id)
                result["asset_id_map"][asset_id] = asset_id

            restored_rooms = []
            skipped_rooms = []
            for historical_room in historical_rooms:
                historical_row = next(
                    (
                        row
                        for row in self.room_type_asset_rows(historical_room)
                        if row.get("asset_id") == asset_id
                    ),
                    None,
                )
                if historical_row is None:
                    continue
                room_type_id = str(historical_room.get("id", "") or "").strip()
                room_name = str(
                    historical_room.get("name", room_type_id) or room_type_id
                ).strip()
                current_room = current_rooms.get(room_type_id)
                if current_room is None:
                    missing = {
                        "asset_id": target_asset_id,
                        "source_asset_id": asset_id,
                        "room_type_id": room_type_id,
                        "room_type_name": room_name,
                    }
                    result["missing_room_types"].append(missing)
                    skipped_rooms.append(room_name or room_type_id)
                    continue

                current_rows = self.room_type_asset_rows(current_room)
                if any(row.get("asset_id") == target_asset_id for row in current_rows):
                    result["existing_assignments"].append(
                        {"asset_id": target_asset_id, "room_type_id": room_type_id}
                    )
                    continue

                restored_row = deepcopy(historical_row)
                restored_row["asset_id"] = target_asset_id
                current_rows.append(restored_row)
                current_room["assets"] = current_rows
                current_room["asset_ids"] = [
                    row["asset_id"] for row in current_rows if row.get("asset_id")
                ]
                assignment = {
                    "asset_id": target_asset_id,
                    "source_asset_id": asset_id,
                    "room_type_id": room_type_id,
                    "room_type_name": room_name,
                    "qty": int(restored_row.get("qty", 1) or 1),
                    "requested_by": str(
                        restored_row.get("requested_by", "") or ""
                    ).strip(),
                }
                result["restored_assignments"].append(assignment)
                restored_rooms.append(room_name or room_type_id)

            if definition_restored or restored_rooms:
                asset_name = str(
                    historical_asset.get("name", asset_id) or asset_id
                ).strip()
                details = []
                if definition_restored:
                    details.append(
                        f"Recovered the asset definition from history under new ID {target_asset_id}"
                    )
                else:
                    details.append("Retained the latest current asset definition")
                if restored_rooms:
                    details.append(
                        "Reapplied to room types: " + ", ".join(restored_rooms)
                    )
                if skipped_rooms:
                    details.append(
                        "Skipped room types missing from current project: "
                        + ", ".join(skipped_rooms)
                    )
                revision_text = (
                    f" from revision {int(source_revision)}" if source_revision else ""
                )
                self.record_revision_change(
                    "Historical asset restore",
                    (
                        f"Restored historical asset {asset_id} as {target_asset_id} - "
                        f"{asset_name}{revision_text} to the current project"
                        if definition_restored
                        else f"Restored asset {asset_id} - {asset_name}{revision_text} to the current project"
                    ),
                    details=details,
                )

        result["changed"] = bool(
            result["restored_asset_ids"] or result["restored_assignments"]
        )
        return result

    def room_type_lookup(self) -> Dict[str, dict]:
        return {
            str(item.get("id", "")).strip(): item
            for item in self.data.get("room_types", [])
            if str(item.get("id", "")).strip()
        }

    @staticmethod
    def parse_capability_keywords(value) -> List[str]:
        """Normalise asset capability/function keywords for replacement testing.

        Keywords can be entered as a comma, semicolon, pipe, slash or newline
        separated text field, or loaded from a legacy list value.  Matching is
        case-insensitive, while the stored display value is title-neutral lower
        case so equivalent entries collapse into one capability.
        """
        if isinstance(value, (list, tuple, set)):
            raw_values = list(value)
        elif value in (None, ""):
            raw_values = []
        else:
            text = str(value or "")
            for sep in ["\r", "\n", ",", "|", "/"]:
                text = text.replace(sep, ";")
            raw_values = text.split(";")

        keywords: List[str] = []
        seen = set()
        for item in raw_values:
            keyword = str(item or "").strip()
            if not keyword:
                continue
            keyword = re.sub(r"\s+", " ", keyword).strip().lower()
            if keyword and keyword not in seen:
                keywords.append(keyword)
                seen.add(keyword)
        return keywords

    def asset_capability_keywords(self, asset) -> List[str]:
        if not isinstance(asset, dict):
            return []
        return self.parse_capability_keywords(
            asset.get(
                "capability_keywords",
                asset.get("capabilities", asset.get("function_keywords", "")),
            )
        )

    def _scenario_group_lookup(self, collection_key: str, member_key: str) -> Dict[str, List[str]]:
        lookup: Dict[str, List[str]] = {}
        for group in self.data.get(collection_key, []) or []:
            if not isinstance(group, dict):
                continue
            name = str(group.get("name", "") or "").strip()
            if not name:
                continue
            members: List[str] = []
            seen = set()
            for member_id in group.get(member_key, []) or []:
                member_id = str(member_id).strip()
                if member_id and member_id not in seen:
                    members.append(member_id)
                    seen.add(member_id)
            lookup[name] = members
        return lookup

    def room_type_scenario_groups(self) -> List[str]:
        groups = {
            str(item.get("name", "") or "").strip()
            for item in self.data.get("room_type_scenario_groups", []) or []
            if isinstance(item, dict)
        }
        # Backwards compatibility for projects edited before group definitions.
        groups.update(
            str(item.get("scenario_group", "") or "").strip()
            for item in self.data.get("room_types", []) or []
            if str(item.get("scenario_group", "") or "").strip()
        )
        return sorted((group for group in groups if group), key=str.casefold)

    def asset_scenario_groups(self) -> List[str]:
        groups = {
            str(item.get("name", "") or "").strip()
            for item in self.data.get("asset_scenario_groups", []) or []
            if isinstance(item, dict)
        }
        # Backwards compatibility for projects edited before group definitions.
        groups.update(
            str(item.get("scenario_group", "") or "").strip()
            for item in self.data.get("assets", []) or []
            if str(item.get("scenario_group", "") or "").strip()
        )
        return sorted((group for group in groups if group), key=str.casefold)

    def room_type_ids_for_scenario_group(self, group_name: str) -> List[str]:
        group_name = str(group_name or "").strip()
        if not group_name:
            return []
        lookup = self._scenario_group_lookup("room_type_scenario_groups", "room_type_ids")
        for name, member_ids in lookup.items():
            if name.casefold() == group_name.casefold():
                return list(member_ids)
        return [
            str(room_type.get("id", "") or "").strip()
            for room_type in self.data.get("room_types", []) or []
            if str(room_type.get("id", "") or "").strip()
            and str(room_type.get("scenario_group", "") or "").strip().casefold() == group_name.casefold()
        ]

    def asset_ids_for_scenario_group(self, group_name: str) -> List[str]:
        group_name = str(group_name or "").strip()
        if not group_name:
            return []
        lookup = self._scenario_group_lookup("asset_scenario_groups", "asset_ids")
        for name, member_ids in lookup.items():
            if name.casefold() == group_name.casefold():
                return list(member_ids)
        return [
            str(asset.get("id", "") or "").strip()
            for asset in self.data.get("assets", []) or []
            if str(asset.get("id", "") or "").strip()
            and str(asset.get("scenario_group", "") or "").strip().casefold() == group_name.casefold()
        ]

    def scenario_definitions(self) -> List[dict]:
        return [dict(item) for item in self.data.get("room_type_asset_scenarios", []) or []]

    def placed_room_type_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for point in self.data.get("data_points", []) or []:
            room_type_id = str(point.get("room_type_id", "") or "").strip()
            if room_type_id:
                counts[room_type_id] = counts.get(room_type_id, 0) + 1
        return counts

    def asset_deployment_summary(self) -> Dict[str, dict]:
        """Count deployed endpoint assets from placed room-type assignments.

        ``deployed_items`` counts physical asset units.  ``deployed_data_points``
        multiplies those units by the asset's data-points-per-item value.
        """
        assets_by_id = {
            str(asset.get("id", "")).strip(): asset
            for asset in self.data.get("assets", [])
            if str(asset.get("id", "")).strip()
        }
        room_types_by_id = self.room_type_lookup()
        placed_counts = self.placed_room_type_counts()
        summary: Dict[str, dict] = {
            asset_id: {
                "deployed_rooms": 0,
                "deployed_items": 0,
                "deployed_data_points": 0,
                "room_type_ids": set(),
            }
            for asset_id in assets_by_id
        }

        for room_type_id, placed_rooms in placed_counts.items():
            room_type = room_types_by_id.get(room_type_id)
            if not room_type or placed_rooms <= 0:
                continue

            for row in self.room_type_asset_rows(room_type):
                asset_id = str(row.get("asset_id", "") or "").strip()
                if not asset_id:
                    continue
                asset = assets_by_id.get(asset_id, {})
                data_points_each = max(
                    0,
                    self._safe_int(
                        asset.get(
                            "data_points",
                            asset.get("data_points_each", asset.get("cables", 1)),
                        ),
                        1,
                    ),
                )
                qty_per_room = max(1, self._safe_int(row.get("qty", 1), 1))
                deployed_items = placed_rooms * qty_per_room
                record = summary.setdefault(
                    asset_id,
                    {
                        "deployed_rooms": 0,
                        "deployed_items": 0,
                        "deployed_data_points": 0,
                        "room_type_ids": set(),
                    },
                )
                record["deployed_rooms"] += placed_rooms
                record["deployed_items"] += deployed_items
                record["deployed_data_points"] += deployed_items * data_points_each
                record["room_type_ids"].add(room_type_id)

        for record in summary.values():
            room_type_ids = record.get("room_type_ids", set())
            if isinstance(room_type_ids, set):
                record["room_type_ids"] = sorted(room_type_ids, key=str.casefold)

        return summary

    def asset_deployment_locations(self) -> Dict[str, List[dict]]:
        """Return placed rooms/data-points where each endpoint asset is deployed.

        Deployment is derived from the standard room-type asset assignments and
        the placed data points that reference those room types.  Each row is a
        room/data-point instance, so it can be shown directly in the asset
        viewer and navigated to on the canvas.
        """
        assets_by_id = {
            str(asset.get("id", "")).strip(): asset
            for asset in self.data.get("assets", [])
            if str(asset.get("id", "")).strip()
        }
        room_types_by_id = self.room_type_lookup()
        locations: Dict[str, List[dict]] = {asset_id: [] for asset_id in assets_by_id}

        for point in self.data.get("data_points", []) or []:
            if not isinstance(point, dict):
                continue

            room_type_id = str(point.get("room_type_id", "") or "").strip()
            if not room_type_id:
                continue

            room_type = room_types_by_id.get(room_type_id)
            if not room_type:
                continue

            room_type_name = str(room_type.get("name", room_type_id) or room_type_id).strip()
            room_name = str(point.get("name", "") or "").strip()
            if not room_name:
                continue

            for asset_row in self.room_type_asset_rows(room_type):
                asset_id = str(asset_row.get("asset_id", "") or "").strip()
                if not asset_id:
                    continue

                asset = assets_by_id.get(asset_id, {})
                data_points_each = max(
                    0,
                    self._safe_int(
                        asset.get(
                            "data_points",
                            asset.get("data_points_each", asset.get("cables", 1)),
                        ),
                        1,
                    ),
                )
                qty_per_room = max(1, self._safe_int(asset_row.get("qty", 1), 1))

                try:
                    floor = int(point.get("floor", 0))
                except (TypeError, ValueError):
                    floor = 0

                locations.setdefault(asset_id, []).append(
                    {
                        "asset_id": asset_id,
                        "asset_name": str(asset.get("name", asset_id) or asset_id).strip(),
                        "room_name": room_name,
                        "floor": floor,
                        "x": point.get("x", ""),
                        "y": point.get("y", ""),
                        "room_type_id": room_type_id,
                        "room_type_name": room_type_name,
                        "qty_per_room": qty_per_room,
                        "deployed_items": qty_per_room,
                        "deployed_data_points": qty_per_room * data_points_each,
                        "data_points_each": data_points_each,
                        "department_ids": [
                            str(department_id).strip()
                            for department_id in point.get("department_ids", []) or []
                            if str(department_id).strip()
                        ],
                    }
                )

        for rows in locations.values():
            rows.sort(
                key=lambda row: (
                    int(row.get("floor", 0) or 0),
                    str(row.get("room_name", "") or "").casefold(),
                    str(row.get("room_type_name", "") or "").casefold(),
                )
            )

        return locations

    def asset_capability_overlap_rows(self) -> List[dict]:
        """Build a capability matrix showing where asset functions overlap.

        A capability overlaps when two or more assets share the same capability
        keyword.  A deployed overlap is more specific: two or more assets with
        the same keyword are assigned to the same room type, and placed rooms
        exist for that room type.
        """
        assets_by_id = {
            str(asset.get("id", "") or "").strip(): asset
            for asset in self.data.get("assets", []) or []
            if str(asset.get("id", "") or "").strip()
        }
        capability_assets: Dict[str, List[str]] = {}
        for asset_id, asset in assets_by_id.items():
            for keyword in self.asset_capability_keywords(asset):
                capability_assets.setdefault(keyword, []).append(asset_id)

        room_types_by_id = self.room_type_lookup()
        locations_by_asset = self.asset_deployment_locations()

        overlap_room_types: Dict[str, dict] = {}
        for room_type_id, room_type in room_types_by_id.items():
            assigned_asset_ids = [
                str(row.get("asset_id", "") or "").strip()
                for row in self.room_type_asset_rows(room_type)
                if str(row.get("asset_id", "") or "").strip() in assets_by_id
            ]
            if not assigned_asset_ids:
                continue
            assigned_set = set(assigned_asset_ids)
            for capability, asset_ids in capability_assets.items():
                matching_ids = [asset_id for asset_id in asset_ids if asset_id in assigned_set]
                if len(matching_ids) < 2:
                    continue
                room_type_name = str(room_type.get("name", room_type_id) or room_type_id).strip()
                label = f"{room_type_id} - {room_type_name}" if room_type_id != room_type_name else room_type_id
                entry = overlap_room_types.setdefault(capability, {"room_types": [], "rooms": [], "room_type_ids": set()})
                if room_type_id not in entry["room_type_ids"]:
                    entry["room_type_ids"].add(room_type_id)
                    entry["room_types"].append(label)
                for point in self.data.get("data_points", []) or []:
                    if str(point.get("room_type_id", "") or "").strip() != room_type_id:
                        continue
                    room_name = str(point.get("name", "") or "").strip()
                    if room_name:
                        try:
                            floor = int(point.get("floor", 0))
                        except (TypeError, ValueError):
                            floor = 0
                        room_label = f"F{floor}: {room_name}"
                        if room_label not in entry["rooms"]:
                            entry["rooms"].append(room_label)

        rows: List[dict] = []
        for capability, asset_ids in sorted(capability_assets.items(), key=lambda item: item[0].casefold()):
            unique_asset_ids = []
            seen = set()
            for asset_id in asset_ids:
                if asset_id in seen:
                    continue
                unique_asset_ids.append(asset_id)
                seen.add(asset_id)
            asset_labels = []
            deployed_asset_ids = []
            for asset_id in unique_asset_ids:
                asset = assets_by_id.get(asset_id, {})
                asset_name = str(asset.get("name", asset_id) or asset_id).strip()
                asset_labels.append(f"{asset_id} - {asset_name}" if asset_name != asset_id else asset_id)
                if locations_by_asset.get(asset_id):
                    deployed_asset_ids.append(asset_id)
            overlap = overlap_room_types.get(capability, {"room_types": [], "rooms": []})
            rows.append(
                {
                    "capability": capability,
                    "asset_ids": unique_asset_ids,
                    "asset_labels": asset_labels,
                    "asset_count": len(unique_asset_ids),
                    "deployed_asset_ids": deployed_asset_ids,
                    "deployed_asset_count": len(deployed_asset_ids),
                    "overlap_room_types": sorted(overlap.get("room_types", []) or [], key=str.casefold),
                    "overlap_rooms": sorted(overlap.get("rooms", []) or [], key=str.casefold),
                }
            )
        return rows

    def sync_all_room_type_quantities(self) -> int:
        changed = 0
        for point in self.data.get("data_points", []) or []:
            name = str(point.get("name", "")).strip()
            if name:
                previous_point_qty = point.get("qty", 1)
                previous_connection_qty = [
                    connection.get("qty", 1)
                    for connection in self.data.get("connections", []) or []
                    if str(connection.get("to", "")).strip() == name
                    or str(connection.get("from", "")).strip() == name
                ]
                self.sync_connection_qty_for_data_point(name)
                if point.get("qty", 1) != previous_point_qty:
                    changed += 1
                current_connection_qty = [
                    connection.get("qty", 1)
                    for connection in self.data.get("connections", []) or []
                    if str(connection.get("to", "")).strip() == name
                    or str(connection.get("from", "")).strip() == name
                ]
                changed += sum(
                    before != after
                    for before, after in zip(
                        previous_connection_qty, current_connection_qty
                    )
                )
        return changed

    def room_type_options(self) -> List[Tuple[str, str]]:
        return [
            (
                str(item.get("id", "")).strip(),
                str(item.get("name", item.get("id", ""))).strip(),
            )
            for item in self.data.get("room_types", [])
            if str(item.get("id", "")).strip()
        ]

    def room_type_cable_qty(self, room_type_id: str) -> int:
        room_type_id = str(room_type_id or "").strip()
        if not room_type_id:
            return 0

        room_type = next(
            (
                item
                for item in self.data.get("room_types", [])
                if str(item.get("id", "")).strip() == room_type_id
            ),
            None,
        )
        if not room_type:
            return 0

        assets_by_id = {
            str(asset.get("id", "")).strip(): asset
            for asset in self.data.get("assets", [])
            if str(asset.get("id", "")).strip()
        }

        total = 0
        for row in self.room_type_asset_rows(room_type):
            asset = assets_by_id.get(row["asset_id"])
            if not asset:
                continue

            room_asset_qty = int(row.get("qty", 1) or 1)
            data_points = int(
                asset.get(
                    "data_points",
                    asset.get("data_points_each", asset.get("cables", 1)),
                )
            )
            total += room_asset_qty * data_points

        return max(0, int(total))

    def data_point_required_port_count(self, point: dict) -> int:
        """Return manual or room-type asset port demand, whichever is greater."""
        if not isinstance(point, dict):
            return 0
        try:
            manual_ports = max(0, int(point.get("qty", 1) or 0))
        except (TypeError, ValueError):
            manual_ports = 1
        room_type_id = str(point.get("room_type_id", "") or "").strip()
        asset_ports = self.room_type_cable_qty(room_type_id) if room_type_id else 0
        return max(manual_ports, asset_ports)

    def count_deployed_data_points(self, point_names=None) -> int:
        """Return deployed port demand for the named placed data-point records.

        A placed marker can represent several physical data points through its
        manual quantity or its assigned room type.  Passing ``None`` counts the
        whole project; passing an iterable limits the count to those markers.
        """
        selected_names = None
        if point_names is not None:
            selected_names = {
                str(name or "").strip()
                for name in point_names
                if str(name or "").strip()
            }
            if not selected_names:
                return 0

        total = 0
        for point in self.data.get("data_points", []) or []:
            if not isinstance(point, dict):
                continue
            name = str(point.get("name", "") or "").strip()
            if not name or (selected_names is not None and name not in selected_names):
                continue
            total += self.data_point_required_port_count(point)
        return max(0, int(total))

    def access_switch_capacity_profile(self) -> dict:
        """Return the configured access-switch endpoint and rack-space capacity."""
        candidates = []
        excluded_uses = {
            "uplink", "input", "pon", "stacking", "power", "console", "management"
        }
        for asset in self.data.get("network_assets", []) or []:
            if not isinstance(asset, dict):
                continue
            asset_type = str(
                asset.get("asset_type", asset.get("type", "")) or ""
            ).strip().lower()
            output_type = str(asset.get("output_connection_type", "") or "").strip().lower()
            name = str(asset.get("name", asset.get("id", "")) or "").strip()
            if (
                asset_type != "network_switch"
                or output_type != "copper"
                or any(word in name.lower() for word in ("core", "distribution", "aggregation"))
            ):
                continue
            definitions = [
                row
                for row in asset.get("port_definitions", []) or []
                if isinstance(row, dict)
            ]
            endpoint_ports = sum(
                max(0, int(row.get("port_count", 0) or 0))
                for row in definitions
                if str(row.get("port_use", "") or "").strip().lower()
                not in excluded_uses
            )
            if endpoint_ports <= 0:
                endpoint_ports = max(0, int(asset.get("number_of_ports", 0) or 0))
            if endpoint_ports <= 0:
                continue
            candidates.append(
                {
                    "id": str(asset.get("id", "") or "").strip(),
                    "name": name or str(asset.get("id", "") or "").strip(),
                    "ports": endpoint_ports,
                    "rack_units": max(1, int(asset.get("rack_units", 1) or 1)),
                }
            )

        settings = self.data.get("network_settings", {}) or {}
        preferred_ids = (
            settings.get("asset_model_preferences", {})
            .get("access_switch", {})
            .get("preferred_asset_ids", [])
        )
        selected = None
        for preferred_id in preferred_ids if isinstance(preferred_ids, list) else []:
            selected = next(
                (row for row in candidates if row["id"] == str(preferred_id).strip()),
                None,
            )
            if selected is not None:
                break
        if selected is None and candidates:
            selected = max(candidates, key=lambda row: (row["ports"], -row["rack_units"]))
        if selected is None:
            selected = {
                "id": "",
                "name": "Default 48-port access switch",
                "ports": 48,
                "rack_units": 1,
            }
        rack_size_u = max(1, int(settings.get("default_rack_size_u", 42) or 42))
        return {
            **selected,
            "rack_size_u": rack_size_u,
            "switches_per_full_cabinet": max(
                1, rack_size_u // max(1, int(selected["rack_units"]))
            ),
        }
        
    def asset_category_options(self) -> List[Tuple[str, str]]:
        return [
            (str(item.get("id", "")).strip(), str(item.get("name", "")).strip())
            for item in self.data.get("asset_categories", [])
            if str(item.get("id", "")).strip()
        ]

    def asset_options(self) -> List[Tuple[str, str]]:
        return [
            (
                str(item.get("id", "")).strip(),
                str(item.get("name", item.get("id", ""))).strip(),
            )
            for item in self.data.get("assets", [])
            if str(item.get("id", "")).strip()
        ]

    def sync_data_point_qty_from_room_type(self, data_point_name: str) -> int:
        data_point_name = str(data_point_name or "").strip()
        for point in self.data.get("data_points", []):
            if str(point.get("name", "")).strip() != data_point_name:
                continue

            room_type_id = str(point.get("room_type_id", "") or "").strip()
            if room_type_id:
                point["qty"] = max(0, self.room_type_cable_qty(room_type_id))
            return max(0, int(point.get("qty", 1) or 0))

        return 1

    def sync_connection_qty_for_data_point(self, data_point_name: str) -> int:
        qty = self.sync_data_point_qty_from_room_type(data_point_name)
        for connection in self.data.get("connections", []):
            if str(connection.get("to", "")).strip() == data_point_name:
                connection["qty"] = qty
            elif str(connection.get("from", "")).strip() == data_point_name:
                connection["qty"] = qty
        return qty

    @staticmethod
    def basename(path: Optional[str]) -> str:
        if not path:
            return "New file"
        return Path(path).name
