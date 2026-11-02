import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("geography", ROOT / "pipelines/medallion/transformations/geography.py")
geography = importlib.util.module_from_spec(spec)
spec.loader.exec_module(geography)

BRAZILIAN_STATES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB",
    "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}


class UfCentersTest(unittest.TestCase):
    def test_every_state_has_exactly_one_point(self):
        self.assertEqual(BRAZILIAN_STATES, set(geography.UF_CENTERS))

    def test_points_fall_inside_brazil(self):
        for uf, (latitude, longitude) in geography.UF_CENTERS.items():
            self.assertTrue(-34.0 <= latitude <= 5.5, uf)
            self.assertTrue(-74.0 <= longitude <= -34.0, uf)

    def test_points_are_distinct(self):
        points = list(geography.UF_CENTERS.values())
        self.assertEqual(len(points), len(set(points)))

    def test_center_rows_are_sorted_and_complete(self):
        rows = geography.center_rows()
        self.assertEqual(27, len(rows))
        self.assertEqual(sorted(row[0] for row in rows), [row[0] for row in rows])


class PipelineContractTest(unittest.TestCase):
    """A aba `address` percorre as três camadas."""

    def test_bronze_declares_the_address_sheet_with_minimal_columns(self):
        bronze = (ROOT / "pipelines/medallion/transformations/bronze.py").read_text(encoding="utf-8")
        self.assertIn('"address": ("id", "state", "city")', bronze)

    def test_silver_publishes_the_addresses_table(self):
        silver = (ROOT / "pipelines/medallion/transformations/silver.py").read_text(encoding="utf-8")
        self.assertIn('"addresses": ("address"', silver)

    def test_the_workbook_has_the_address_sheet(self):
        import openpyxl

        book = openpyxl.load_workbook(ROOT / "inputs/admins.xlsx", read_only=True)
        self.assertIn("address", book.sheetnames)
        rows = list(book["address"].iter_rows(values_only=True))
        self.assertEqual(("id", "state", "city"), rows[0])
        self.assertGreater(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
