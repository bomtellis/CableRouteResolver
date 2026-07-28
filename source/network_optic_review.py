"""Installed optical-transceiver review used by the network planning UI."""
from __future__ import annotations

from typing import Dict, List, Optional

from network_schema import (
    optic_form_factors_for_cage,
    normalise_port_speeds,
    port_speed_label,
)
from network_services import calculate_optical_budgets


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value) -> Optional[float]:
    if _text(value) == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _port_definition(asset: dict, observed_port: str) -> dict:
    target = _text(observed_port).lower()
    # Logical stacks prefix the physical port with the member number.
    if "/" in target:
        stack_member, unstacked_name = target.split("/", 1)
        if stack_member.isdigit():
            target = unstacked_name
    rows = [
        row
        for row in asset.get("port_definitions", [])
        if isinstance(row, dict)
    ]
    for row in rows:
        explicit = [
            _text(value).lower()
            for value in row.get("explicit_names", [])
            if _text(value)
        ]
        prefix = _text(row.get("name_prefix")).lower()
        if target in explicit or (prefix and target.startswith(prefix)):
            return row
    if len(rows) == 1:
        return rows[0]
    return {}


def _connection_length(data: dict, connection: dict) -> float:
    for key in ("route_length_m", "length_m", "estimated_length_m"):
        if _text(connection.get(key)) != "":
            return max(0.0, _float(connection.get(key)))
    connection_id = _text(connection.get("id"))
    lengths = [
        max(0.0, _float(cable.get("length_m")))
        for cable in data.get("network_fibre_cables", [])
        if isinstance(cable, dict)
        and connection_id in {
            _text(value) for value in cable.get("logical_connection_ids", [])
        }
    ]
    return sum(lengths)


def installed_optic_module_review_rows(data: dict) -> List[dict]:
    """Return actionable validation rows for every installed pluggable optic.

    The calculation deliberately uses the same installed module records and
    optical-budget service as the planner and reports the configured design
    margin, rather than merely checking that an optic asset ID exists.
    """

    paths = calculate_optical_budgets(data)
    assets = {
        _text(row.get("id")): row
        for row in data.get("network_assets", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    instances = {
        _text(row.get("id")): row
        for row in data.get("network_asset_instances", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    connections = {
        _text(row.get("id")): row
        for row in data.get("network_connections", [])
        if isinstance(row, dict) and _text(row.get("id"))
    }
    modules = [
        row
        for row in data.get("network_optic_modules", [])
        if isinstance(row, dict)
    ]
    modules_by_id = {
        _text(row.get("id")): row for row in modules if _text(row.get("id"))
    }
    required_margin = max(
        0.0,
        _float(
            data.get("network_settings", {})
            .get("physical_fibre_planning", {})
            .get("minimum_optical_margin_db", 3.0),
            3.0,
        ),
    )

    result: List[dict] = []
    for source_index, module in enumerate(modules):
        module_id = _text(module.get("id"))
        optic_asset = assets.get(_text(module.get("asset_id")), {})
        host = instances.get(_text(module.get("host_instance_id")), {})
        host_asset = assets.get(_text(host.get("asset_id")), {})
        connection = connections.get(_text(module.get("connection_id")), {})
        port = _port_definition(host_asset, _text(module.get("host_port")))
        cage = _text(port.get("port_type")).lower()
        form_factor = _text(optic_asset.get("optic_form_factor")).lower()
        selected_speed = max(
            0,
            int(
                module.get(
                    "link_speed_mbps", connection.get("link_speed_mbps", 0)
                )
                or 0
            ),
        )

        failures: List[str] = []
        incomplete: List[str] = []
        if not host:
            failures.append("Host switch is missing")
        if not connection:
            failures.append("Logical connection is missing")
        if _text(optic_asset.get("asset_type")).lower() != "optical_transceiver":
            failures.append("Installed module does not reference an optical-transceiver model")
        if not port:
            failures.append("Host port is not present on the switch model")
        else:
            allowed_forms = optic_form_factors_for_cage(cage)
            if form_factor not in allowed_forms:
                failures.append(
                    f"{form_factor.upper() or 'Unspecified optic'} is not compatible "
                    f"with the {cage.upper() or 'unknown'} cage"
                )

        supported_speeds = normalise_port_speeds(
            optic_asset.get("supported_speeds_mbps")
        )
        if selected_speed and supported_speeds and selected_speed not in supported_speeds:
            failures.append(
                f"Optic does not support the selected {port_speed_label(selected_speed)} rate"
            )
        connection_speed = max(0, int(connection.get("link_speed_mbps", 0) or 0))
        if selected_speed and connection_speed and selected_speed != connection_speed:
            failures.append(
                f"Installed rate {port_speed_label(selected_speed)} differs from "
                f"the link rate {port_speed_label(connection_speed)}"
            )

        length_m = _connection_length(data, connection)
        reach_m = max(0.0, _float(optic_asset.get("optic_reach_m")))
        if reach_m and length_m > reach_m + 1e-9:
            failures.append(
                f"Route length {length_m:g} m exceeds the optic reach {reach_m:g} m"
            )

        if _optional_float(optic_asset.get("optical_tx_power_dbm")) is None:
            incomplete.append("transmit power")
        if _optional_float(
            optic_asset.get("optical_receiver_sensitivity_dbm")
        ) is None:
            incomplete.append("receiver sensitivity")

        related_paths = [
            path
            for path in paths
            if module_id
            and module_id
            in {
                _text(path.get("source_optic_module_id")),
                _text(path.get("destination_optic_module_id")),
            }
        ]
        peer_ids = set()
        for path in related_paths:
            for key in (
                "source_optic_module_id",
                "destination_optic_module_id",
            ):
                peer_id = _text(path.get(key))
                if peer_id and peer_id != module_id:
                    peer_ids.add(peer_id)
        # A direct connection can still identify the peer while the optical
        # budget remains incomplete.
        if not peer_ids and connection:
            peer_ids.update(
                _text(candidate.get("id"))
                for candidate in modules
                if candidate is not module
                and _text(candidate.get("connection_id"))
                == _text(module.get("connection_id"))
                and _text(candidate.get("id"))
            )

        peer_models: List[str] = []
        for peer_id in sorted(peer_ids):
            peer_module = modules_by_id.get(peer_id, {})
            peer_asset = assets.get(_text(peer_module.get("asset_id")), {})
            peer_models.append(
                _text(peer_asset.get("name"))
                or _text(peer_asset.get("id"))
                or peer_id
            )
            left_connector = _text(
                optic_asset.get("optic_connector_type")
            ).lower()
            right_connector = _text(peer_asset.get("optic_connector_type")).lower()
            if (
                left_connector
                and right_connector
                and left_connector != right_connector
            ):
                failures.append(
                    f"Connector mismatch: {left_connector.upper()} / "
                    f"{right_connector.upper()}"
                )
            left_standard = _text(optic_asset.get("optic_fibre_standard")).upper()
            right_standard = _text(peer_asset.get("optic_fibre_standard")).upper()
            if left_standard and right_standard and left_standard != right_standard:
                failures.append(
                    f"Fibre-standard mismatch: {left_standard} / {right_standard}"
                )
            left_wavelength = int(
                optic_asset.get("optical_wavelength_nm", 0) or 0
            )
            right_wavelength = int(
                peer_asset.get("optical_wavelength_nm", 0) or 0
            )
            if (
                left_wavelength
                and right_wavelength
                and left_wavelength != right_wavelength
            ):
                failures.append(
                    f"Wavelength mismatch: {left_wavelength} / "
                    f"{right_wavelength} nm"
                )

        path_missing = [
            _text(path.get("missing_properties"))
            for path in related_paths
            if _text(path.get("status")).lower() == "unconfigured"
            and _text(path.get("missing_properties"))
        ]
        incomplete.extend(path_missing)
        margins = [
            _float(path.get("margin_db"))
            for path in related_paths
            if _text(path.get("status")).lower() in {"pass", "fail"}
            and _text(path.get("margin_db")) != ""
        ]
        worst_margin = min(margins) if margins else None
        if worst_margin is not None and worst_margin + 1e-9 < required_margin:
            failures.append(
                f"Calculated margin {worst_margin:.3f} dB is below the "
                f"required {required_margin:.3f} dB"
            )
        if not related_paths and not failures:
            incomplete.append("complete active optical path")

        failures = list(dict.fromkeys(value for value in failures if value))
        incomplete = list(dict.fromkeys(value for value in incomplete if value))
        if failures:
            status = "fail"
            detail = "; ".join(failures)
            if incomplete:
                detail += "; also configure " + ", ".join(incomplete)
        elif incomplete:
            status = "unconfigured"
            detail = "Configure " + ", ".join(incomplete)
        else:
            status = "pass"
            detail = (
                f"Validated at or above the required {required_margin:.3f} dB margin"
            )

        result.append(
            {
                "source_index": source_index,
                "status": status,
                "module_id": module_id,
                "host": _text(host.get("name"))
                or _text(host.get("id"))
                or _text(module.get("host_instance_id")),
                "host_port": _text(module.get("host_port")),
                "optic_asset_id": _text(optic_asset.get("id"))
                or _text(module.get("asset_id")),
                "optic_model": _text(optic_asset.get("name"))
                or _text(optic_asset.get("id"))
                or _text(module.get("asset_id")),
                "connection_id": _text(module.get("connection_id")),
                "side": _text(module.get("side")),
                "speed_mbps": selected_speed,
                "cage": cage,
                "form_factor": form_factor,
                "connector": _text(optic_asset.get("optic_connector_type")),
                "fibre_standard": _text(
                    optic_asset.get("optic_fibre_standard")
                ),
                "reach_m": reach_m,
                "tx_dbm": optic_asset.get("optical_tx_power_dbm", ""),
                "rx_dbm": optic_asset.get(
                    "optical_receiver_sensitivity_dbm", ""
                ),
                "insertion_loss_db": optic_asset.get(
                    "optical_insertion_loss_db", ""
                ),
                "wavelength_nm": optic_asset.get("optical_wavelength_nm", ""),
                "peer_models": ", ".join(peer_models),
                "margin_db": "" if worst_margin is None else worst_margin,
                "required_margin_db": required_margin,
                "detail": detail,
            }
        )

    return sorted(
        result,
        key=lambda row: (
            row["connection_id"].casefold(),
            row["side"].casefold(),
            row["host"].casefold(),
            row["host_port"].casefold(),
            row["module_id"].casefold(),
        ),
    )
