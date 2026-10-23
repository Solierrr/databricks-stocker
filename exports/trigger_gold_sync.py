# Databricks notebook source
"""Final job task: call the sync API only after Silver and Gold succeed."""

import json
from urllib.request import Request, urlopen

from databricks.sdk.runtime import dbutils


scope = "solaria-sync"
endpoint = dbutils.secrets.get(scope=scope, key="endpoint-url").rstrip("/")
token = dbutils.secrets.get(scope=scope, key="api-token")

request = Request(
    f"{endpoint}/sync/gold",
    headers={"Authorization": f"Bearer {token}"},
    data=b"",
    method="POST",
)
with urlopen(request, timeout=1800) as response:
    result = json.load(response)
if result.get("status") != "completed":
    raise RuntimeError("Gold export did not complete")
print(f"Exported {result['tables_synced']} Gold KPI tables")
