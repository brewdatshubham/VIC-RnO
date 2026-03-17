from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class DatabricksConfig:
    host: str
    http_path: str
    token: str


def query_databricks(cfg: DatabricksConfig, sql: str) -> pd.DataFrame:
    """
    Execute SQL against Databricks SQL Warehouse and return a DataFrame.
    Requires `databricks-sql-connector` installed.
    """
    from databricks import sql as dbsql

    with dbsql.connect(
        server_hostname=cfg.host,
        http_path=cfg.http_path,
        access_token=cfg.token,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in (cur.description or [])]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def sql_escape(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_in(col: str, values: list) -> str:
    if not values:
        return ""
    if len(values) == 1:
        return f"{col} = {sql_escape(values[0])}"
    inner = ", ".join(sql_escape(v) for v in values)
    return f"{col} IN ({inner})"

