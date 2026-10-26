"""Local contract checks for the Databricks notebook modules."""

import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


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

    def test_empty_path_fails_before_reading(self):
        with self.assertRaisesRegex(ValueError, "Set a path for admins.xlsx"):
            self.read_bronze("admins", "company", ("id",), "")
        self.spark.read.format.assert_not_called()

    def test_missing_columns_fail_before_publication(self):
        with self.assertRaisesRegex(ValueError, "missing columns .*status"):
            self.read_bronze("admins", "company", ("id", "status"), "/Volumes/admins.xlsx")


class NotebookContractTest(unittest.TestCase):
    def test_run_passes_workbook_path_to_bronze_reader(self):
        spark = Mock()
        spark.catalog.tableExists.return_value = False
        dbutils = Mock()
        parameters = {"source_mode": "manual", "fornecedores_path": "/Volumes/fornecedores.xlsx",
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
        gold.SUPPLIER_FILTER = "workspace.gold_security.can_read_supplier"
        gold.SUPPLIER_TABLES = frozenset()
        with patch.dict(sys.modules, {**runtime_modules(spark, dbutils), "note_bronze": bronze,
                                      "note_silver": silver, "note_gold": gold}):
            notebook = runpy.run_path(str(TRANSFORMATIONS / "my_transformation.py"))
            notebook["run"]()
        self.assertEqual(bronze.read_bronze.call_args_list, [
            unittest.mock.call("fornecedores", "offer", ("id",), "/Volumes/fornecedores.xlsx"),
            unittest.mock.call("admins", "company", ("id",), "/Volumes/admins.xlsx"),
        ])


if __name__ == "__main__":
    unittest.main()
