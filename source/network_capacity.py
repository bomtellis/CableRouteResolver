"""Simple physical-capacity estimates used by network planning summaries."""

from __future__ import annotations

import math


ACCESS_SWITCH_PORTS = 48
ACCESS_SWITCHES_PER_CABINET = 16
PORTS_PER_CABINET = ACCESS_SWITCH_PORTS * ACCESS_SWITCHES_PER_CABINET

# Nominal 42U cabinet envelope used by the existing room-layout reports.
CABINET_WIDTH_MM = 600
CABINET_DEPTH_MM = 1070
CABINET_HEIGHT_MM = 2000


def rough_cabinet_requirement(port_count: int) -> dict:
    """Return a rough single-row cabinet footprint for an endpoint port count.

    This is deliberately a space-planning estimate. It assumes 16 48-port
    access switches per 42U cabinet and excludes working/access clearances,
    patching, power, uplinks and other rack-mounted equipment.
    """

    ports = max(0, int(port_count or 0))
    cabinet_count = math.ceil(ports / PORTS_PER_CABINET) if ports else 0
    row_width_mm = cabinet_count * CABINET_WIDTH_MM
    return {
        "cabinet_count": cabinet_count,
        "ports_per_cabinet": PORTS_PER_CABINET,
        "switches_per_cabinet": ACCESS_SWITCHES_PER_CABINET,
        "ports_per_switch": ACCESS_SWITCH_PORTS,
        "rack_size_u": 42,
        "cabinet_width_mm": CABINET_WIDTH_MM,
        "cabinet_depth_mm": CABINET_DEPTH_MM,
        "cabinet_height_mm": CABINET_HEIGHT_MM,
        "row_width_mm": row_width_mm,
        "row_depth_mm": CABINET_DEPTH_MM if cabinet_count else 0,
        "footprint_area_m2": row_width_mm * CABINET_DEPTH_MM / 1_000_000.0,
    }
