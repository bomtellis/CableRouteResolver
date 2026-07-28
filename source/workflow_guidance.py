"""Project-state guidance for the main Cable Route Resolver workflow."""

from __future__ import annotations

from collections.abc import Mapping


def _text(value) -> str:
    return str(value or "").strip()


def _rows(data: Mapping, key: str) -> list[dict]:
    return [
        dict(row)
        for row in data.get(key, []) or []
        if isinstance(row, Mapping)
    ]


def _room_has_assets(room_type: Mapping) -> bool:
    return any(
        isinstance(row, Mapping)
        and _text(row.get("asset_id", row.get("id")))
        for row in room_type.get("assets", []) or []
    ) or any(_text(value) for value in room_type.get("asset_ids", []) or [])


def _required_data_points(data_points: list[dict]) -> list[dict]:
    result = []
    for row in data_points:
        try:
            quantity = int(row.get("qty", 1) or 0)
        except (TypeError, ValueError):
            quantity = 0
        if quantity > 0:
            result.append(row)
    return result


def project_workflow_state(data: Mapping | None) -> dict:
    """Return the recommended action and compact workflow counts."""

    data = data if isinstance(data, Mapping) else {}
    assets = _rows(data, "assets")
    room_types = _rows(data, "room_types")
    data_points = _required_data_points(_rows(data, "data_points"))
    route_connections = _rows(data, "connections")
    network_instances = _rows(data, "network_asset_instances")
    network_assignments = _rows(data, "network_endpoint_assignments")

    counts = {
        "assets": len(assets),
        "room_types": len(room_types),
        "data_points": len(data_points),
        "route_connections": len(route_connections),
        "network_instances": len(network_instances),
        "network_assignments": len(network_assignments),
    }
    counts_text = (
        f"{counts['assets']} assets · {counts['room_types']} room types · "
        f"{counts['data_points']} data points · "
        f"{counts['route_connections']} routes · "
        f"{counts['network_instances']} network devices"
    )

    if not assets:
        recommendation = {
            "stage": 1,
            "title": "Define the assets used by the project",
            "detail": (
                "Create or import the equipment library before assigning room "
                "requirements."
            ),
            "action_text": "Open Assets",
            "action_name": "manage_assets",
        }
    elif not room_types:
        recommendation = {
            "stage": 1,
            "title": "Create room types and assign their assets",
            "detail": (
                "Room types turn the asset library into repeatable room "
                "requirements and final network-port demand."
            ),
            "action_text": "Open Room Types",
            "action_name": "manage_room_types",
        }
    elif any(not _room_has_assets(room_type) for room_type in room_types):
        recommendation = {
            "stage": 1,
            "title": "Complete the asset assignments for each room type",
            "detail": (
                "At least one room type has no assets. Review the assignments "
                "before placing rooms or solving the network."
            ),
            "action_text": "Review Assignments",
            "action_name": "show_room_type_asset_review_wizard",
        }
    elif not data_points:
        recommendation = {
            "stage": 2,
            "title": "Place or import the project data points",
            "detail": (
                "Add room/data-point instances after the reusable room "
                "requirements are ready."
            ),
            "action_text": "Open Data Points",
            "action_name": "manage_data_points",
        }
    else:
        unassigned = [
            row
            for row in data_points
            if not _text(row.get("room_type_id"))
            and not row.get("assets")
            and not row.get("asset_ids")
        ]
        if unassigned:
            recommendation = {
                "stage": 2,
                "title": f"Assign room types to {len(unassigned)} data point(s)",
                "detail": (
                    "Unassigned points cannot produce reliable asset, cable, "
                    "or network requirements."
                ),
                "action_text": "Find Unassigned",
                "action_name": "show_unassigned_data_point_navigator",
            }
        else:
            routed_names = {
                _text(row.get("to"))
                for row in route_connections
                if _text(row.get("to"))
            }
            unrouted = [
                row
                for row in data_points
                if _text(row.get("name"))
                and _text(row.get("name")) not in routed_names
            ]
            if unrouted:
                recommendation = {
                    "stage": 2,
                    "title": f"Route {len(unrouted)} outstanding data point(s)",
                    "detail": (
                        "Connect the points to equipment rooms before running "
                        "the network planner."
                    ),
                    "action_text": "Autoroute",
                    "action_name": "autoroute_data_points",
                }
            elif not network_instances or not network_assignments:
                recommendation = {
                    "stage": 3,
                    "title": "Generate and review the network design",
                    "detail": (
                        "The room demand and routes are ready for equipment, "
                        "rack, port, fibre, and power planning."
                    ),
                    "action_text": "Open Network Planner",
                    "action_name": "open_network_planner",
                }
            else:
                recommendation = {
                    "stage": 4,
                    "title": "Validate the project and prepare deliverables",
                    "detail": (
                        "Review topology and schedules, then export the required "
                        "project reports."
                    ),
                    "action_text": "Validate Project",
                    "action_name": "validate_json",
                }

    return {
        **recommendation,
        "counts": counts,
        "counts_text": counts_text,
    }
