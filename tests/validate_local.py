"""Offline medallion check. Run: python tests/validate_local.py

Checks repository tests and the recovery Excel files. It does not access
Databricks, Unity Catalog, volumes, or consume Databricks credits.
"""

import ast
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

try:
    import openpyxl
except ImportError:
    raise SystemExit("Instale openpyxl: python -m pip install openpyxl")


ROOT = Path(__file__).resolve().parents[1]
TRANSFORMATIONS = ROOT / "pipelines/00_medallion/transformations"


def constant(module: str, name: str):
    tree = ast.parse((TRANSFORMATIONS / module).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise ValueError(f"Constante {name} não encontrada em {module}")


def validate_value(value, output, numeric_types, boolean_columns, date_columns, timestamp_columns):
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    if output in numeric_types:
        try:
            number = Decimal(raw)
        except InvalidOperation:
            return "número inválido"
        if not number.is_finite():
            return "número não finito"
        kind = numeric_types[output]
        if kind == "bigint":
            if number != number.to_integral_value() or not -(2**63) <= number < 2**63:
                return "inteiro inválido ou fora do limite"
        else:
            precision, scale = map(int, kind.removeprefix("decimal(").removesuffix(")").split(","))
            if number != number.quantize(Decimal(1).scaleb(-scale)):
                return f"perderia casas ao converter para {kind}"
            if abs(number) >= Decimal(10) ** (precision - scale):
                return f"fora do limite de {kind}"
    elif output in boolean_columns and raw.lower() not in {"t", "true", "1", "f", "false", "0"}:
        return "booleano inválido"
    elif output in date_columns | timestamp_columns:
        if isinstance(value, (date, datetime)):
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if output in date_columns and parsed.time().isoformat() != "00:00:00":
                return "data contém horário que seria perdido"
        except ValueError:
            return "data/hora inválida"
    return None


def validate_workbooks():
    sheets = constant("note_bronze.py", "SHEETS")
    source_sheets = constant("note_bronze.py", "SOURCE_SHEETS")
    aliases = constant("note_bronze.py", "COLUMN_ALIASES")
    specs = constant("note_silver.py", "SPECS")
    numeric_types = constant("note_silver.py", "NUMERIC_TYPES")
    boolean_columns = constant("note_silver.py", "BOOLEAN_COLUMNS")
    date_columns = constant("note_silver.py", "DATE_COLUMNS")
    timestamp_columns = constant("note_silver.py", "TIMESTAMP_COLUMNS")
    by_source = {source: columns for source, columns in specs.values()}
    by_source.update({
        "technician": [("id", "technician_id", "raw"), ("person_id", "person_id", "raw"), ("slug", "technician_slug", "clean")],
        "person": [("id", "person_id", "raw"), ("user_id", "user_id", "raw")],
    })
    problems = []
    rows_checked = 0
    for workbook_name, definitions in sheets.items():
        path = ROOT / "recovery" / f"{workbook_name}.xlsx"
        if not path.is_file():
            problems.append(f"Arquivo ausente: {path}")
            continue
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            for source, required in definitions.items():
                sheet_name = source_sheets.get(source, source)
                if sheet_name not in workbook:
                    problems.append(f"{path.name}: aba ausente {sheet_name}")
                    continue
                rows = workbook[sheet_name].iter_rows(values_only=True)
                header = next(rows, None)
                if not header:
                    problems.append(f"{path.name}/{sheet_name}: cabeçalho ausente")
                    continue
                original = [str(value).strip() if value is not None else "" for value in header]
                if len(original) != len(set(original)):
                    problems.append(f"{path.name}/{sheet_name}: cabeçalhos duplicados")
                names = [aliases.get(source, {}).get(value, value)
                         if aliases.get(source, {}).get(value, value) not in original else value
                         for value in original]
                expected = set(required) | {column for column, _, _ in by_source[source]}
                missing = sorted(expected - set(names))
                if missing:
                    problems.append(f"{path.name}/{sheet_name}: colunas ausentes {missing}")
                    continue
                index = {name: position for position, name in enumerate(names)}
                seen_ids = set()
                for row_number, row in enumerate(rows, start=2):
                    rows_checked += 1
                    row_id = row[index["id"]] if index["id"] < len(row) else None
                    if row_id is None or str(row_id).strip() == "":
                        problems.append(f"{path.name}/{sheet_name}:{row_number}: id vazio")
                    elif str(row_id).strip() in seen_ids:
                        problems.append(f"{path.name}/{sheet_name}:{row_number}: id duplicado {row_id}")
                    else:
                        seen_ids.add(str(row_id).strip())
                    for field, output, _ in by_source[source]:
                        position = index[field]
                        value = row[position] if position < len(row) else None
                        problem = validate_value(value, output, numeric_types, boolean_columns,
                                                 date_columns, timestamp_columns)
                        if problem:
                            problems.append(f"{path.name}/{sheet_name}:{row_number} {field}: {problem} ({value!r})")
        finally:
            workbook.close()
    return rows_checked, problems


def main():
    print("Executando testes do projeto...", flush=True)
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    print("Conferindo recovery/admins.xlsx e recovery/fornecedores.xlsx...", flush=True)
    rows, problems = validate_workbooks()
    print(f"Linhas conferidas: {rows}; problemas encontrados: {len(problems)}")
    for problem in problems[:30]:
        print("-", problem)
    if len(problems) > 30:
        print(f"... mais {len(problems) - 30} problemas")
    if not result.wasSuccessful() or problems:
        return 1
    print("OK local. A execução no Databricks e os arquivos enviados ao volume ainda precisam ser conferidos lá.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
