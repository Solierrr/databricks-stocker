from databricks.sdk.runtime import spark


CATALOG_KEY = "lake.catalog"
DEFAULT_CATALOG = "analytics"

BRONZE = "bronze_core"
SILVER = "silver"
GOLD = "gold"
SECURITY = "gold_security"


def catalog() -> str:
    return spark.conf.get(CATALOG_KEY, DEFAULT_CATALOG)


def table(schema: str, name: str) -> str:
    return f"{catalog()}.{schema}.{name}"
