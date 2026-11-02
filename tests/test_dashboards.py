"""Confere a estrutura dos painéis Lakeview versionados, sem precisar do Databricks.

Os testes não provam que o Databricks renderiza o painel, mas pegam os erros que quebram a importação ou deixam
um widget sem dados: dataset inexistente, coluna que não existe no dataset, widget sobreposto, nome repetido e
tabela Gold que não existe no código da pipeline.
"""

import ast
import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = sorted((ROOT / "dashboards").glob("*.lvdash.json"))
GOLD_SOURCE = (ROOT / "pipelines/medallion/transformations/gold.py").read_text(encoding="utf-8")
GRID_COLUMNS = 12
IMPLICIT_FIELDS = {"*", "associative_filter_predicate_group"}
KNOWN_WIDGET_TYPES = {
    "counter", "table", "bar", "line", "histogram", "box", "symbol-map", "filter-multi-select",
    "filter-single-select", "filter-date-range-picker",
}
FILTER_TYPES = {"filter-multi-select", "filter-single-select", "filter-date-range-picker"}


def gold_tables():
    tree = ast.parse(GOLD_SOURCE)
    return {node.name for node in tree.body if isinstance(node, ast.FunctionDef) and node.name.startswith("kpi_")}


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def widgets(document):
    for page in document["pages"]:
        for item in page["layout"]:
            yield page, item


def chart_widgets(document):
    for page, item in widgets(document):
        spec = item["widget"].get("spec")
        if spec:
            yield page, item, spec


def encoded_fields(spec):
    """Nomes de campo citados pelas codificações de um widget."""
    found = []

    def walk(node):
        if isinstance(node, dict):
            if "fieldName" in node:
                found.append(node["fieldName"])
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(spec.get("encodings", {}))
    return found


class DashboardStructureTest(unittest.TestCase):
    def test_there_are_two_dashboards(self):
        self.assertEqual(["admin-overview.lvdash.json", "supplier-overview.lvdash.json"], [p.name for p in DASHBOARDS])

    def test_datasets_have_unique_names_and_read_existing_gold_tables(self):
        existing = gold_tables()
        for path in DASHBOARDS:
            datasets = load(path)["datasets"]
            self.assertEqual(len(datasets), len({d["name"] for d in datasets}), path.name)
            self.assertEqual(len(datasets), len({d["displayName"] for d in datasets}), path.name)
            for dataset in datasets:
                table = dataset["config"]["source"].rsplit(".", 1)[-1]
                self.assertEqual(f"workspace.gold.{dataset['displayName']}", dataset["config"]["source"], path.name)
                self.assertIn(table, existing, f"{path.name}: {table} não existe em gold.py")

    def test_page_names_and_widget_names_are_unique(self):
        for path in DASHBOARDS:
            document = load(path)
            pages = [p["name"] for p in document["pages"]]
            self.assertEqual(len(pages), len(set(pages)), path.name)
            names = [item["widget"]["name"] for _, item in widgets(document)]
            self.assertEqual(len(names), len(set(names)), f"{path.name}: nome de widget repetido")

    def test_widgets_fit_the_grid_and_do_not_overlap(self):
        for path in DASHBOARDS:
            for page in load(path)["pages"]:
                boxes = []
                for item in page["layout"]:
                    p = item["position"]
                    self.assertTrue(all(isinstance(p[k], int) for k in ("x", "y", "width", "height")), item["widget"]["name"])
                    self.assertGreaterEqual(p["x"], 0)
                    self.assertGreaterEqual(p["y"], 0)
                    self.assertGreater(p["width"], 0)
                    self.assertGreater(p["height"], 0)
                    self.assertLessEqual(p["x"] + p["width"], GRID_COLUMNS, f"{path.name}/{item['widget']['name']}")
                    boxes.append((item["widget"]["name"], p))
                for i, (name_a, a) in enumerate(boxes):
                    for name_b, b in boxes[i + 1:]:
                        separated = (a["x"] + a["width"] <= b["x"] or b["x"] + b["width"] <= a["x"]
                                     or a["y"] + a["height"] <= b["y"] or b["y"] + b["height"] <= a["y"])
                        self.assertTrue(separated, f"{path.name}/{page['name']}: {name_a} sobrepõe {name_b}")

    def test_queries_use_existing_datasets_and_columns(self):
        for path in DASHBOARDS:
            document = load(path)
            columns = {d["name"]: {c["name"] for c in d["config"]["dimensions"] + d["config"].get("measures", [])}
                       for d in document["datasets"]}
            for _, item in widgets(document):
                for query in item["widget"].get("queries", []):
                    dataset = query["query"]["datasetName"]
                    self.assertIn(dataset, columns, f"{path.name}/{item['widget']['name']}")
                    for field in query["query"].get("fields", []):
                        for column in re.findall(r"`([^`]+)`", field["expression"]):
                            if column in IMPLICIT_FIELDS:
                                continue
                            self.assertIn(column, columns[dataset],
                                          f"{path.name}/{item['widget']['name']}: coluna {column} não existe em {dataset}")

    def test_encodings_point_to_fields_of_their_own_query(self):
        for path in DASHBOARDS:
            for _, item, spec in chart_widgets(load(path)):
                widget = item["widget"]
                if spec["widgetType"] in FILTER_TYPES:
                    continue
                names = {f["name"] for q in widget["queries"] for f in q["query"]["fields"]}
                for field in encoded_fields(spec):
                    self.assertIn(field, names, f"{path.name}/{widget['name']}: campo {field} sem consulta")

    def test_filters_are_bound_to_their_query(self):
        for path in DASHBOARDS:
            for _, item, spec in chart_widgets(load(path)):
                if spec["widgetType"] not in FILTER_TYPES:
                    continue
                widget = item["widget"]
                queries = {q["name"]: {f["name"] for f in q["query"].get("fields", [])} for q in widget["queries"]}
                for field in spec["encodings"]["fields"]:
                    self.assertIn(field["queryName"], queries, widget["name"])
                    self.assertIn(field["fieldName"], queries[field["queryName"]], widget["name"])
                    self.assertTrue(field["displayName"], widget["name"])

    def test_every_widget_type_is_known(self):
        for path in DASHBOARDS:
            for _, item, spec in chart_widgets(load(path)):
                self.assertIn(spec["widgetType"], KNOWN_WIDGET_TYPES, f"{path.name}/{item['widget']['name']}")

    def test_files_are_stable_when_reserialized(self):
        for path in DASHBOARDS:
            text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
            self.assertEqual(text, json.dumps(json.loads(text), indent=2, ensure_ascii=False) + "\n", path.name)


class BiRequirementsTest(unittest.TestCase):
    """Os requisitos de BI: filtros, histograma, boxplot e mapa."""

    @staticmethod
    def types_of(name):
        return [spec["widgetType"] for _, _, spec in chart_widgets(load(ROOT / "dashboards" / name))]

    def test_both_dashboards_have_interactive_filters(self):
        for name in ("admin-overview.lvdash.json", "supplier-overview.lvdash.json"):
            self.assertGreaterEqual(sum(t in FILTER_TYPES for t in self.types_of(name)), 3, name)
            self.assertIn("filter-date-range-picker", self.types_of(name), name)

    def test_both_dashboards_show_distributions(self):
        for name in ("admin-overview.lvdash.json", "supplier-overview.lvdash.json"):
            types = self.types_of(name)
            self.assertGreaterEqual(types.count("histogram"), 2, name)
            self.assertGreaterEqual(types.count("box"), 1, name)

    def test_the_admin_dashboard_has_a_point_map_by_state(self):
        self.assertIn("symbol-map", self.types_of("admin-overview.lvdash.json"))

    def test_histogram_and_box_read_the_offer_prices_of_the_dataset(self):
        for name in ("admin-overview.lvdash.json", "supplier-overview.lvdash.json"):
            document = load(ROOT / "dashboards" / name)
            expressions = " ".join(
                f["expression"] for _, item, spec in chart_widgets(document)
                if spec["widgetType"] in ("histogram", "box")
                for q in item["widget"]["queries"] for f in q["query"]["fields"])
            self.assertIn("BIN_FLOOR(`unit_price`", expressions)
            self.assertIn("PERCENTILE(`unit_price`, 0.25)", expressions)
            self.assertIn("MEDIAN(`unit_price`)", expressions)


class DemandByStateTest(unittest.TestCase):
    COLUMNS = ["state", "state_latitude", "state_longitude", "registered_local_units", "requested_proposals",
               "accepted_proposals", "requested_units"]

    def test_gold_table_and_dashboard_dataset_expose_the_same_columns(self):
        document = load(ROOT / "dashboards/admin-overview.lvdash.json")
        dataset = next(d for d in document["datasets"] if d["displayName"] == "kpi_admin_demand_by_state")
        self.assertEqual(self.COLUMNS, [c["name"] for c in dataset["config"]["dimensions"]])
        start = GOLD_SOURCE.index("def kpi_admin_demand_by_state")
        end = GOLD_SOURCE.index("\ndef ", start + 10)
        body = GOLD_SOURCE[start:end]
        for column in self.COLUMNS:
            self.assertRegex(body, rf'["\']{column}["\']', column)

    def test_the_new_table_is_a_gold_builder(self):
        # A exportação para o analyticsdb usa a lista de indicadores do databricks-sync (gold_export_manifest.json).
        self.assertIn("kpi_admin_demand_by_state", gold_tables())


if __name__ == "__main__":
    unittest.main()
