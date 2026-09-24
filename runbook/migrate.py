# Databricks notebook source
# MAGIC %md
# MAGIC # Migracao manual Postgres -> Databricks
# MAGIC
# MAGIC Runbook para popular os schemas `core` e `auth` no catalogo `workspace` a
# MAGIC partir do Postgres, usando as credenciais guardadas no secret scope
# MAGIC `solaria-db`. Alternativa manual ao `databricks-sync` (que roda
# MAGIC automaticamente via GitHub Actions) para popular os dados sob demanda,
# MAGIC direto do workspace do Databricks.


import time


CATALOG = "workspace"
SECRET_SCOPE = "solaria-db"


SOURCES = [
    {"secret_prefix": "core", "dbx_schema": "core"},
    {"secret_prefix": "auth", "dbx_schema": "auth"},
]


def pg_secret(prefix: str, key: str) -> str:
    return dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{prefix}-{key}")


def jdbc_config(prefix: str) -> tuple[str, dict]:
    host = pg_secret(prefix, "host")
    port = pg_secret(prefix, "port")
    dbname = pg_secret(prefix, "name")
    url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    props = {
        "user": pg_secret(prefix, "user"),
        "password": pg_secret(prefix, "password"),
        "driver": "org.postgresql.Driver",
        "sslmode": "require",
    }
    return url, props


def get_tables(jdbc_url: str, jdbc_props: dict) -> list[str]:
    query = (
        "(SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name) AS tables_list"
    )
    rows = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_props).collect()
    return [r["table_name"] for r in rows]


def migrate_table(jdbc_url: str, jdbc_props: dict, table: str, dbx_schema: str) -> None:
    df = spark.read.jdbc(url=jdbc_url, table=f'public."{table}"', properties=jdbc_props)
    full_name = f"`{CATALOG}`.`{dbx_schema}`.`{table}`"
    df.write.mode("overwrite").saveAsTable(full_name)


def migrate_source(secret_prefix: str, dbx_schema: str) -> None:
    jdbc_url, jdbc_props = jdbc_config(secret_prefix)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{dbx_schema}`")

    tables = get_tables(jdbc_url, jdbc_props)
    print(f"Migrando {len(tables)} tabelas de {secret_prefix} para {CATALOG}.{dbx_schema} ...")
    for table in tables:
        try:
            migrate_table(jdbc_url, jdbc_props, table, dbx_schema)
            print(f"  ok  {table}")
        except Exception as exc:
            print(f"  FALHOU {table}: {exc}")


start_time = time.time()

for source in SOURCES:
    migrate_source(source["secret_prefix"], source["dbx_schema"])

elapsed = time.time() - start_time
minutes, seconds = divmod(int(elapsed), 60)
print(f"Migração concluída com sucesso em {minutes:02d}:{seconds:02d}s.")
