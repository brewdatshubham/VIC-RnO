from __future__ import annotations

import streamlit as st

from app.databricks_client import DatabricksConfig


def get_databricks_config() -> DatabricksConfig:
    """
    Reads Databricks settings from Streamlit secrets.

    Put these in `.streamlit/secrets.toml`:
      [databricks]
      host = "..."
      http_path = "..."
      token = "..."
    """
    db = st.secrets.get("databricks", {})
    host = str(db.get("host", "")).strip()
    http_path = str(db.get("http_path", "")).strip()
    token = str(db.get("token", "")).strip()
    if not host or not http_path or not token:
        raise RuntimeError("Missing Databricks config in Streamlit secrets (`[databricks]`).")
    return DatabricksConfig(host=host, http_path=http_path, token=token)

