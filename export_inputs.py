"""Gera as duas planilhas de entrada a partir das views do schema export do banco de negócio.

Cada aba segue o contrato de colunas de `bronze.py` (SHEETS), então a pipeline lê o resultado sem ajustes.
A conexão usa DB_POSTGRES_HOST, DB_POSTGRES_PORT, DB_POSTGRES_USER, DB_POSTGRES_PASSWORD e DB_POSTGRES_CORE
(opcionalmente DB_POSTGRES_SSLMODE). O banco é lido sem alterações.

    python export_inputs.py [--output-dir inputs]
"""

import argparse
import ast
from datetime import date, datetime
from decimal import Decimal
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
BRONZE = ROOT / "pipelines/medallion/transformations/bronze.py"
ENVIRONMENT = ("DB_POSTGRES_HOST", "DB_POSTGRES_PORT", "DB_POSTGRES_USER", "DB_POSTGRES_PASSWORD", "DB_POSTGRES_CORE")


def read_constant(name: str):
    """Lê uma constante literal de bronze.py sem importá-lo, porque ele depende do runtime do Databricks."""
    for node in ast.parse(BRONZE.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(getattr(target, "id", None) == name for target in node.targets):
            return ast.literal_eval(node.value)
    raise ValueError(f"{name} não encontrado em {BRONZE.name}")


def cell(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def export(output_dir: Path) -> None:
    import psycopg2
    from openpyxl import Workbook

    missing = [key for key in ENVIRONMENT if not os.environ.get(key)]
    if missing:
        raise SystemExit(f"Variáveis ausentes: {', '.join(missing)}")
    sheets = read_constant("SHEETS")
    source_sheets = read_constant("SOURCE_SHEETS")
    connection = psycopg2.connect(
        host=os.environ["DB_POSTGRES_HOST"],
        port=os.environ["DB_POSTGRES_PORT"],
        dbname=os.environ["DB_POSTGRES_CORE"],
        user=os.environ["DB_POSTGRES_USER"],
        password=os.environ["DB_POSTGRES_PASSWORD"],
        sslmode=os.environ.get("DB_POSTGRES_SSLMODE", "require"),
        application_name="export-inputs",
    )
    connection.set_session(readonly=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        for workbook_name, entities in sheets.items():
            book = Workbook()
            book.remove(book.active)
            for entity, columns in entities.items():
                names = ", ".join(f'"{column}"' for column in columns)
                with connection.cursor() as cursor:
                    cursor.execute(f'SELECT {names} FROM export."{entity}" ORDER BY 1')
                    rows = cursor.fetchall()
                sheet = book.create_sheet(source_sheets.get(entity, entity))
                sheet.append(list(columns))
                for row in rows:
                    sheet.append([cell(value) for value in row])
                print(f"{workbook_name}.{entity}: {len(rows)} linhas", flush=True)
            path = output_dir / f"{workbook_name}.xlsx"
            book.save(path)
            print(f"Salvo em {path}", flush=True)
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "inputs")
    args = parser.parse_args()
    export(args.output_dir.resolve())
    return 0


if __name__ == "__main__":
    sys.exit(main())
