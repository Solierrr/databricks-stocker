"""Local contract checks for the Databricks notebook modules."""

import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch


TRANSFORMATIONS = Path(__file__).resolve().parents[1] / "pipelines/00_medallion/transformations"


def runtime_modules(spark, dbutils=None):
    databricks = types.ModuleType("databricks")
    sdk = types.ModuleType("databricks.sdk")
    runtime = types.ModuleType("databricks.sdk.runtime")
    runtime.spark = spark
    runtime.dbutils = dbutils
    return {"databricks": databricks, "databricks.sdk": sdk,
            "databricks.sdk.runtime": runtime}


class BronzeContractTest(unittest.TestCase):
    def setUp(self):
        self.spark = Mock()
        self.frame = Mock()
        self.frame.columns = ["id", "name"]
        self.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = self.frame
        self.modules = patch.dict(sys.modules, runtime_modules(self.spark))
        self.modules.start()
        self.addCleanup(self.modules.stop)
        self.read_bronze = runpy.run_path(str(TRANSFORMATIONS / "note_bronze.py"))["read_bronze"]

    def test_explicit_fourth_argument_is_used(self):
        result = self.read_bronze("admins", "company", ("id",), "/Volumes/admins.xlsx")
        self.assertIs(result, self.frame)
        self.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.assert_called_once_with(
            "/Volumes/admins.xlsx"
        )
        self.spark.conf.get.assert_not_called()

    def test_recovery_headers_are_renamed_for_silver(self):
        self.frame.columns = ["id", "fk_company", "status", "business_type"]
        normalized = self.read_bronze(
            "fornecedores", "supplier", ("id", "company_id"), "/Volumes/fornecedores.xlsx"
        )
        self.frame.toDF.assert_called_once_with("id", "company_id", "status", "business_type")
        self.assertIs(normalized, self.frame.toDF.return_value)

    def test_company_plan_reads_plural_recovery_sheet(self):
        self.frame.columns = ["id", "name", "value", "cycle"]
        self.read_bronze("admins", "company_plan", ("id", "price"), "/Volumes/admins.xlsx")
        self.spark.read.format.return_value.option.return_value.option.assert_any_call(
            "dataAddress", "company_plans"
        )

    def test_missing_columns_fail_before_publication(self):
        with self.assertRaisesRegex(ValueError, "missing columns .*status"):
            self.read_bronze("admins", "company", ("id", "status"), "/Volumes/admins.xlsx")


class SilverContractTest(unittest.TestCase):
    def setUp(self):
        pyspark = types.ModuleType("pyspark")
        sql = types.ModuleType("pyspark.sql")
        sql.functions = MagicMock()
        sql.functions.col.return_value.__gt__.return_value = MagicMock()
        catalog = types.ModuleType("note_catalog")
        catalog.BRONZE = "bronze_core"
        catalog.table = lambda schema, name: f"workspace.{schema}.{name}"
        with patch.dict(sys.modules, {**runtime_modules(Mock()), "pyspark": pyspark,
                                      "pyspark.sql": sql, "note_catalog": catalog}):
            self.validate = runpy.run_path(str(TRANSFORMATIONS / "note_silver.py"))["_validate_source"]

    def test_missing_source_column_is_reported(self):
        frame = Mock(columns=["id"])
        with self.assertRaisesRegex(ValueError, "Bronze offer: missing Silver columns .*unit_price"):
            self.validate(frame, "offer", [("id", "offer_id", "raw"), ("unit_price", "unit_price", "raw")])
        frame.agg.assert_not_called()

    def test_invalid_numeric_value_is_reported_and_filtered(self):
        frame = Mock(columns=["id", "unit_price"])
        frame.agg.return_value.first.return_value.asDict.return_value = {"id": 0, "unit_price": 2}
        frame.join.return_value.count.return_value = 0
        with patch("builtins.print") as output:
            result = self.validate(frame, "offer", [("id", "offer_id", "raw"), ("unit_price", "unit_price", "raw")])
        output.assert_called_once()
        self.assertIn("'unit_price': 2", output.call_args.args[0])
        frame.filter.assert_called_once()
        self.assertIs(result, frame.filter.return_value.join.return_value)

    def test_duplicate_ids_are_reported_and_filtered(self):
        frame = Mock(columns=["id"])
        frame.agg.return_value.first.return_value.asDict.return_value = {"id": 0}
        frame.join.return_value.count.return_value = 2
        with patch("builtins.print") as output:
            self.validate(frame, "offer", [("id", "offer_id", "raw")])
        self.assertIn("2 linhas", output.call_args.args[0])
        self.assertEqual(frame.filter.return_value.join.call_args.args[-1], "left_anti")


class NotebookContractTest(unittest.TestCase):
    def test_run_passes_workbook_path_to_bronze_reader(self):
        spark = Mock()
        spark.catalog.tableExists.return_value = False
        dbutils = Mock()
        parameters = {"catalog": "analytics", "source_mode": "manual", "fornecedores_path": "/Volumes/fornecedores.xlsx",
                      "admins_path": "/Volumes/admins.xlsx", "allow_replace_existing": "false"}
        dbutils.widgets.get.side_effect = parameters.__getitem__
        bronze = types.ModuleType("note_bronze")
        bronze.SHEETS = {"fornecedores": {"offer": ("id",)}, "admins": {"company": ("id",)}}
        bronze.read_bronze = Mock(return_value=Mock())
        silver = types.ModuleType("note_silver")
        silver.SPECS = {"companies": None}
        silver.silver_frame = Mock(return_value=Mock())
        gold = types.ModuleType("note_gold")
        gold.GOLD_BUILDERS = {"kpi_admin_cadastros": Mock(return_value=Mock())}
        gold.supplier_filter_name = Mock(return_value="analytics.gold_security.can_read_supplier")
        gold.SUPPLIER_TABLES = frozenset()
        catalog = types.ModuleType("note_catalog")
        catalog.BRONZE = "bronze_core"
        catalog.SILVER = "silver"
        catalog.GOLD = "gold"
        catalog.DEFAULT_CATALOG = "analytics"
        catalog.catalog = Mock(return_value="analytics")
        catalog.set_catalog = Mock()
        catalog.table = lambda schema, name: f"analytics.{schema}.{name}"
        with patch.dict(sys.modules, {**runtime_modules(spark, dbutils), "note_bronze": bronze,
                                      "note_silver": silver, "note_gold": gold, "note_catalog": catalog}):
            notebook = runpy.run_path(str(TRANSFORMATIONS / "my_transformation.py"))
            notebook["run"]()
        catalog.set_catalog.assert_called_once_with("analytics")
        spark.conf.set.assert_not_called()
        self.assertEqual(spark.sql.call_args_list, [
            unittest.mock.call("CREATE SCHEMA IF NOT EXISTS analytics.silver"),
            unittest.mock.call("CREATE SCHEMA IF NOT EXISTS analytics.gold"),
        ])
        self.assertEqual(bronze.read_bronze.call_args_list, [
            unittest.mock.call("fornecedores", "offer", ("id",), "/Volumes/fornecedores.xlsx"),
            unittest.mock.call("admins", "company", ("id",), "/Volumes/admins.xlsx"),
        ])

        # A previous partial run leaves managed Delta targets behind. Rerunning
        # must proceed, while a view with the same name remains protected.
        spark.catalog.tableExists.return_value = True
        spark.catalog.getTable.return_value.tableType = "MANAGED"
        spark.sql.return_value.first.return_value = {"format": "delta"}
        notebook["run"]()
        self.assertGreater(bronze.read_bronze.call_count, 2)

        spark.catalog.getTable.side_effect = lambda name: types.SimpleNamespace(
            tableType="VIEW" if name.endswith(".offer") else "MANAGED"
        )
        previous_reads = bronze.read_bronze.call_count
        with self.assertRaisesRegex(RuntimeError, "workspace|analytics.bronze_core.offer"):
            notebook["run"]()
        self.assertEqual(bronze.read_bronze.call_count, previous_reads)

        spark.catalog.getTable.side_effect = None
        spark.sql.return_value.first.return_value = {"format": "parquet"}
        with self.assertRaisesRegex(RuntimeError, "parquet"):
            notebook["run"]()
        self.assertEqual(bronze.read_bronze.call_count, previous_reads)


if __name__ == "__main__":
    unittest.main()
