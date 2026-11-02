import unittest

import export_inputs


class ExportInputsTest(unittest.TestCase):
    def test_reads_the_bronze_contract_without_importing_the_notebook(self):
        sheets = export_inputs.read_constant("SHEETS")
        self.assertEqual(set(sheets), {"fornecedores", "admins"})
        self.assertIn("purchase_order", sheets["fornecedores"])
        self.assertIn("order_item", sheets["fornecedores"])

    def test_company_plan_keeps_its_plural_source_sheet(self):
        self.assertEqual(export_inputs.read_constant("SOURCE_SHEETS")["company_plan"], "company_plans")

    def test_cells_are_written_as_text(self):
        from datetime import datetime
        from decimal import Decimal

        self.assertEqual(export_inputs.cell(True), "true")
        self.assertEqual(export_inputs.cell(Decimal("12.50")), "12.50")
        self.assertEqual(export_inputs.cell(datetime(2026, 11, 2, 10, 0)), "2026-11-02T10:00:00")
        self.assertIsNone(export_inputs.cell(None))


if __name__ == "__main__":
    unittest.main()
