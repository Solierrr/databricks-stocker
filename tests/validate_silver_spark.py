"""Exercise Silver conversion and validation with local Spark SQL values.

Run explicitly with: python tests/validate_silver_spark.py
"""

import os
import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession


TRANSFORMATIONS = Path(__file__).resolve().parents[1] / "pipelines/00_medallion/transformations"


class SilverSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.spark = (SparkSession.builder.master("local[1]")
                     .config("spark.ui.enabled", "false")
                     .appName("silver-validation")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        runtime = types.ModuleType("databricks.sdk.runtime")
        runtime.spark = cls.spark
        catalog = types.ModuleType("note_catalog")
        catalog.BRONZE = "bronze_core"
        catalog.table = lambda schema, name: f"workspace.{schema}.{name}"
        with patch.dict(sys.modules, {"databricks.sdk.runtime": runtime,
                                      "note_catalog": catalog}):
            module = runpy.run_path(str(TRANSFORMATIONS / "note_silver.py"))
        cls.validate = staticmethod(module["_validate_source"])
        cls.value = staticmethod(module["_value"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_valid_mixed_source_types(self):
        frame = self.spark.sql("""
            SELECT * FROM VALUES
              ('a', 12.30, 2, true, DATE'2024-01-01'),
              ('b', 0.00, 0, false, DATE'2024-02-29')
            AS t(id, unit_price, quantity, is_active, due_date)
        """)
        columns = [("id", "offer_id", "raw"), ("unit_price", "unit_price", "raw"),
                   ("quantity", "quantity", "raw"), ("is_active", "is_active", "raw"),
                   ("due_date", "due_date", "raw")]
        self.validate(frame, "mixed", columns)
        values = frame.select(self.value("unit_price", "unit_price").alias("price"),
                              self.value("is_active", "is_active").alias("active")).collect()
        self.assertEqual(str(values[0]["price"]), "12.30")
        self.assertTrue(values[0]["active"])

    def test_rejects_silent_numeric_rounding_and_date_truncation(self):
        frame = self.spark.sql("""
            SELECT * FROM VALUES
              ('a', '1.000000000001', '2024-01-01 12:30:00'),
              ('b', '1e-12', '2024-01-02'),
              ('c', '1.2300', '2024-01-03 00:00:00'),
              ('d', '1e-1001', '2024-01-04')
            AS t(id, unit_price, due_date)
        """)
        columns = [("id", "offer_id", "raw"), ("unit_price", "unit_price", "raw"),
                   ("due_date", "due_date", "raw")]
        with patch("builtins.print") as output:
            cleaned = self.validate(frame, "example", columns)
        self.assertEqual(cleaned.count(), 1)
        self.assertIn("'unit_price': 3", output.call_args.args[0])
        self.assertIn("'due_date': 1", output.call_args.args[0])

    def test_exact_scientific_values_and_blank_optionals(self):
        frame = self.spark.sql("""
            SELECT * FROM VALUES
              ('a', '1e2', '2.00', 'true', '2024-01-01 00:00:00'),
              ('b', '', '', '', '')
            AS t(id, unit_price, quantity, is_active, due_date)
        """)
        columns = [("id", "offer_id", "raw"), ("unit_price", "unit_price", "raw"),
                   ("quantity", "quantity", "raw"), ("is_active", "is_active", "raw"),
                   ("due_date", "due_date", "raw")]
        self.validate(frame, "example", columns)

    def test_rejects_fractional_integer_and_duplicate_ids(self):
        frame = self.spark.sql("""
            SELECT * FROM VALUES ('a', '1.000000000001') AS t(id, quantity)
        """)
        with patch("builtins.print") as output:
            cleaned = self.validate(frame, "inventory", [("id", "inventory_id", "raw"),
                                                          ("quantity", "quantity", "raw")])
        self.assertEqual(cleaned.count(), 0)
        self.assertIn("'quantity': 1", output.call_args.args[0])

        duplicates = self.spark.sql("""
            SELECT * FROM VALUES (' a '), ('a') AS t(id)
        """)
        with patch("builtins.print") as output:
            cleaned = self.validate(duplicates, "example", [("id", "inventory_id", "raw")])
        self.assertEqual(cleaned.count(), 0)
        self.assertIn("2 linhas", output.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
