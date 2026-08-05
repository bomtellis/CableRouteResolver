import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent / "source"))

from room_asset_detail_export import (
    _CATEGORISED_BREAKDOWN_HEADERS,
    ROOM_ASSET_DETAIL_HEADERS,
    categorised_room_asset_breakdowns,
    export_room_asset_detail_xlsx,
)
from xlsx_workbook import read_xlsx


class RoomAssetDetailExportTests(unittest.TestCase):
    def setUp(self):
        self.room_types = [
            {
                "id": "RT1",
                "name": "Consulting room",
                "assets": [
                    {"asset_id": "A1", "qty": 1},
                    {"asset_id": "A2", "qty": 2},
                ],
                "asset_connections": [
                    {
                        "from_asset_id": "A1",
                        "to_asset_id": "A2",
                        "qty": 2,
                    }
                ],
            }
        ]
        self.assets_by_id = {
            "A1": {
                "id": "A1",
                "name": "Daisy-chain display",
                "category_id": "AV",
                "input_ports": 1,
                "output_ports": 2,
            },
            "A2": {
                "id": "A2",
                "name": "Downstream display",
                "category_id": "AV",
                "input_ports": 1,
            },
        }

    def test_categorised_breakdown_retains_final_network_port_count(self):
        room = categorised_room_asset_breakdowns(
            self.room_types,
            self.assets_by_id,
            {"AV": "Audio visual"},
        )[0]

        self.assertEqual(room["final_network_ports_per_room"], 1)
        self.assertEqual(room["assets"][0]["inputs_per_device"], 1)
        self.assertEqual(room["assets"][0]["outputs_per_device"], 2)
        self.assertEqual(room["assets"][0]["connected_to_device"], "")
        self.assertEqual(room["assets"][0]["final_network_ports"], 1)
        self.assertEqual(room["assets"][1]["inputs_per_device"], 1)
        self.assertEqual(room["assets"][1]["outputs_per_device"], 0)
        self.assertEqual(room["assets"][1]["connected_to_device"], "A1")
        self.assertEqual(room["assets"][1]["final_network_ports"], 0)

    def test_xlsx_categorised_sheet_includes_final_network_ports(self):
        with tempfile.TemporaryDirectory() as folder:
            destination, _ = export_room_asset_detail_xlsx(
                Path(folder) / "room-assets.xlsx",
                self.room_types,
                self.assets_by_id,
                {"AV": "Audio visual"},
            )
            workbook = read_xlsx(destination)
            sheet = workbook.sheet("Categorised Breakdown")
            detail_sheet = workbook.sheet("Room Asset Detail")

        header_row = next(
            row for row in sheet.rows if row == _CATEGORISED_BREAKDOWN_HEADERS
        )
        self.assertEqual(
            header_row[-6:],
            (
                "Qty per room",
                "Inputs per device",
                "Outputs per device",
                "Connected to device",
                "Asset total",
                "Final network port total",
            ),
        )
        final_port_column = header_row.index("Final network port total")
        connected_column = header_row.index("Connected to device")
        asset_row = next(row for row in sheet.rows if row[:1] == ("A2",))
        total_row = next(row for row in sheet.rows if row[:1] == ("Total",))
        self.assertEqual(asset_row[connected_column], "A1")
        self.assertEqual(asset_row[final_port_column], 0)
        self.assertEqual(total_row[final_port_column], 1)
        self.assertEqual(detail_sheet.rows[0], ROOM_ASSET_DETAIL_HEADERS)
        self.assertEqual(
            detail_sheet.rows[0][-6:],
            (
                "Qty per room",
                "Inputs per device",
                "Outputs per device",
                "Connected to device",
                "Network Port Detail",
                "Final network port total",
            ),
        )


if __name__ == "__main__":
    unittest.main()
