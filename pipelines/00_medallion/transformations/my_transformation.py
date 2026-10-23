# Databricks notebook source
"""One notebook job: Excel -> Bronze -> Silver -> Gold managed Delta tables.

Run only after the old pipeline-owned objects have been migrated. This module
does not run as Lakeflow source code and has no network or API calls.
"""

from databricks.sdk.runtime import dbutils, spark

from note_bronze import SHEETS, read_bronze
from note_gold import GOLD_BUILDERS, SUPPLIER_FILTER, SUPPLIER_TABLES
from note_silver import SPECS, silver_frame


def _parameter(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name).strip()


def _targets() -> list[str]:
    bronze = [f"workspace.bronze_core.{name}" for sheets in SHEETS.values() for name in sheets]
    silver = [f"workspace.silver_core.{name}" for name in (*SPECS, "technicians")]
    gold = [f"workspace.gold_core.{name}" for name in GOLD_BUILDERS]
    return bronze + silver + gold


def _write_table(name: str, frame, *, supplier_filter: bool = False) -> None:
    if supplier_filter:
        # The only SQL here is table DDL. All KPI calculations remain PySpark.
        # Applying the filter in CREATE makes publication atomic and keeps the
        # supplier policy on later replacements.
        temporary = f"_solaria_{name.rsplit('.', 1)[-1]}"
        frame.createOrReplaceTempView(temporary)
        try:
            spark.sql(
                f"CREATE OR REPLACE TABLE {name} USING DELTA "
                f"WITH ROW FILTER {SUPPLIER_FILTER} ON (supplier_id) "
                f"AS SELECT * FROM `{temporary}`"
            )
        finally:
            spark.catalog.dropTempView(temporary)
        return

    writer = frame.writeTo(name).using("delta")
    if name == "workspace.silver_core.technical_projects":
        writer = writer.tableProperty("delta.feature.timestampNtz", "supported")
    writer.createOrReplace()


def run() -> None:
    source_mode = _parameter("source_mode", "manual").lower()
    if source_mode != "manual":
        raise ValueError("Only source_mode=manual is implemented; automatic CoreDB ingestion is not configured")

    source_paths = {
        "fornecedores": _parameter("fornecedores_path", "/Volumes/workspace/bronze_core/manual_ingestion/fornecedores.xlsx"),
        "admins": _parameter("admins_path", "/Volumes/workspace/bronze_core/manual_ingestion/admins.xlsx"),
    }
    allow_replace = _parameter("allow_replace_existing", "false").lower() == "true"
    if not all(source_paths.values()):
        raise ValueError("Both workbook paths are required")

    targets = _targets()
    existing = [name for name in targets if spark.catalog.tableExists(name)]
    if existing and not allow_replace:
        raise RuntimeError(
            "Target objects already exist. Complete the cutover and explicitly set "
            "allow_replace_existing=true before replacing managed Delta tables. "
            f"Existing objects: {existing}"
        )

    for workbook, sheets in SHEETS.items():
        spark.conf.set(f"solaria.excel.{workbook}", source_paths[workbook])
        for sheet, required in sheets.items():
            _write_table(f"workspace.bronze_core.{sheet}", read_bronze(workbook, sheet, required))

    for name in (*SPECS, "technicians"):
        _write_table(f"workspace.silver_core.{name}", silver_frame(name))

    for name, build in GOLD_BUILDERS.items():
        _write_table(
            f"workspace.gold_core.{name}",
            build(),
            supplier_filter=name in SUPPLIER_TABLES,
        )


if __name__ == "__main__":
    run()
