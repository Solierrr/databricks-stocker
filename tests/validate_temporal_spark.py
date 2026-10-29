"""Validação temporal da Silver com Spark local: python tests/validate_temporal_spark.py."""

import os
import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession


class TemporalValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").appName("temporal-validation").getOrCreate()
        runtime = types.ModuleType("databricks.sdk.runtime")
        runtime.spark = cls.spark
        catalog = types.ModuleType("note_catalog")
        catalog.BRONZE = "bronze_core"
        catalog.table = lambda schema, name: f"workspace.{schema}.{name}"
        path = Path(__file__).resolve().parents[1] / "pipelines/00_medallion/transformations/note_silver.py"
        with patch.dict(sys.modules, {"databricks.sdk.runtime": runtime, "note_catalog": catalog}):
            cls.validate = staticmethod(runpy.run_path(str(path))["_validate_source"])
        cls.columns = [("id", "service_id", "raw"), ("created_at", "created_at", "raw"),
                       ("accepted_at", "accepted_at", "raw"), ("ends_at", "ends_at", "raw")]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_accepts_valid_equal_and_missing_dates(self):
        frame = self.spark.sql("""SELECT * FROM VALUES
          ('a', '2024-01-01 12:00:00', '2024-01-01 12:00:00', '2024-01-02 12:00:00'),
          ('b', '2024-01-01 12:00:00', NULL, NULL)
          AS t(id, created_at, accepted_at, ends_at)""")
        self.validate(frame, "technical_service", self.columns)

    def test_rejects_events_out_of_order(self):
        frame = self.spark.sql("""SELECT * FROM VALUES
          ('a', '2024-01-02 12:00:00', '2024-01-01 12:00:00', '2024-01-01 10:00:00'),
          ('b', '2024-01-01 12:00:00', '2024-01-03 12:00:00', '2024-01-02 12:00:00')
          AS t(id, created_at, accepted_at, ends_at)""")
        with patch("builtins.print") as output:
            cleaned = self.validate(frame, "technical_service", self.columns)
        self.assertEqual(cleaned.count(), 0)
        message = output.call_args.args[0]
        for field in ("accepted_at_before_created_at", "ends_at_before_created_at", "ends_at_before_accepted_at"):
            self.assertIn(field, message)


    def test_inventory_excludes_invalid_and_duplicate_rows_but_keeps_zero(self):
        frame = self.spark.sql("""SELECT * FROM VALUES
          ('zero', '0'), ('good', '2'), ('negative', '-1'), ('blank', NULL),
          ('dup', '3'), ('dup', '4'), ('bad', 'not-a-number')
          AS t(id, quantity)""")
        columns = [("id", "inventory_id", "raw"), ("quantity", "quantity", "raw")]
        with patch("builtins.print"):
            cleaned = self.validate(frame, "inventory", columns)
        self.assertEqual({row.id for row in cleaned.select("id").collect()}, {"zero", "good"})

    def test_unknown_status_is_excluded_without_removing_valid_rows(self):
        frame = self.spark.sql("""SELECT * FROM VALUES
          ('a', 'accepted'), ('b', 'UNKNOWN'), ('c', NULL)
          AS t(id, status)""")
        columns = [("id", "proposal_id", "raw"), ("status", "proposal_status", "upper")]
        with patch("builtins.print"):
            cleaned = self.validate(frame, "proposal", columns)
        self.assertEqual({row.id for row in cleaned.select("id").collect()}, {"a", "c"})


if __name__ == "__main__":
    unittest.main()
