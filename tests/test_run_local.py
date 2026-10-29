"""Contratos da entrada Excel e da exportação JSON do executor local."""

from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run_local import LocalReader, LocalSpark, json_value


class LocalRunnerTest(unittest.TestCase):
    def test_json_preserves_decimal_null_and_date(self):
        result = json.loads(json.dumps({"amount": Decimal("1234567890123456.78"),
                                        "date": date(2026, 10, 29), "missing": None},
                                       default=json_value))
        self.assertEqual(result["amount"], "1234567890123456.78")
        self.assertEqual(result["date"], "2026-10-29")
        self.assertIsNone(result["missing"])

    def test_excel_text_is_encoded_as_data_and_empty_cells_are_preserved(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            path = Path(folder) / "input.xlsx"
            book = Workbook()
            sheet = book.active
            sheet.title = "offer"
            sheet.append(["id", "name", "optional"])
            text = "O'Brien \\ test; DROP TABLE offer"
            sheet.append(["a", text, None])
            book.save(path)
            book.close()
            session = Mock()
            session.sql.return_value.cache.return_value.collect.return_value = [("a", text, None)]
            LocalReader(session).format("excel").option("dataAddress", "offer").load(path)
        query = session.sql.call_args.args[0]
        self.assertNotIn(text, query)
        self.assertIn(text.encode("utf-8").hex(), query)
        self.assertIn("CAST(NULL AS STRING)", query)

    def test_missing_sheet_fails_explicitly(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            path = Path(folder) / "input.xlsx"
            book = Workbook()
            book.save(path)
            book.close()
            with self.assertRaisesRegex(ValueError, "aba ausente"):
                LocalReader(Mock()).format("excel").option("dataAddress", "offer").load(path)

    def test_local_catalog_uses_only_registered_temporary_tables(self):
        session = Mock()
        local = LocalSpark(session)
        self.assertFalse(local.catalog.tableExists("local_gold_example"))
        session.catalog.tableExists.assert_not_called()
        with self.assertRaisesRegex(ValueError, "bloqueada"):
            local.sql("DROP TABLE example")
        session.sql.assert_not_called()


if __name__ == "__main__":
    unittest.main()
