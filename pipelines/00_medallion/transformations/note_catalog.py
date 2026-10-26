DEFAULT_CATALOG = "workspace"

BRONZE = "bronze_core"
SILVER = "silver"
GOLD = "gold"
SECURITY = "gold_security"

_catalog = DEFAULT_CATALOG


def set_catalog(name: str) -> None:
    global _catalog
    _catalog = name


def catalog() -> str:
    return _catalog


def table(schema: str, name: str) -> str:
    return f"{catalog()}.{schema}.{name}"
