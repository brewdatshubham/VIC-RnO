from __future__ import annotations

import streamlit as st
import pandas as pd

from app.config import get_databricks_config
from app.databricks_client import query_databricks
from app.stored_procedures.get_vilc_summary import VilcSummary
from app.stored_procedures.get_spend import SpendKPIs
from app.stored_procedures.get_cost_per_hl import SCHEMA
from app.analytics.clustering import cluster_volume_1d


st.set_page_config(page_title="VIC RnO", page_icon="📊", layout="wide")
st.title("VIC RnO")


try:
    db_cfg = get_databricks_config()
except Exception as e:
    st.error(
        "Databricks config missing. Create `.streamlit/secrets.toml` from "
        "`.streamlit/secrets.example.toml`.\n\nDetails: " + str(e)
    )
    st.stop()


def _to_list(val: str):
    if not val or val.strip().lower() == "all":
        return None
    return [v.strip() for v in val.split(",") if v.strip()]


ZONES  = ["APAC", "NAZ", "MAZ", "SAZ", "AFR", "EUR"]
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
PLANT_GROUPBY = ["plant", "zone", "country", "year", "month"]
JOIN_KEYS     = ["plant", "zone", "country", "year", "month"]


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Filters")
    sel_zones   = st.multiselect("Zone",  ZONES,  placeholder="All")
    country     = st.text_input("Country", value="All")
    year        = st.text_input("Year",    value="2025")
    sel_months  = st.multiselect("Month", MONTHS, placeholder="All")
    period_type = st.radio("Period type", ["MTD", "YTD"], horizontal=True)
    n_clusters  = st.slider("Clusters", min_value=2, max_value=8, value=3)
    fetch_btn   = st.button("Fetch Data", type="primary", use_container_width=True)

zone  = ",".join(sel_zones)  if sel_zones  else "All"
month = ",".join(sel_months) if sel_months else "All"


# ---------------------------------------------------------------------------
# Cached fetchers
# ---------------------------------------------------------------------------
@st.cache_data(ttl=900)
def _fetch_vilc(zone, country, year, month, period_type):
    sql = VilcSummary().get_vilc_summary(
        zone=_to_list(zone), country=_to_list(country),
        year=_to_list(year), month=_to_list(month),
        period_type=period_type, groupby_column=PLANT_GROUPBY,
        beverage_category=["Beer"],
    )
    return query_databricks(db_cfg, sql)


@st.cache_data(ttl=900)
def _fetch_spend(zone, country, year, month, period_type):
    month_list = _to_list(month)
    groupby = [c for c in PLANT_GROUPBY if not (c == "month" and month_list)]
    sql = SpendKPIs().get_spend(
        zone=_to_list(zone), country=_to_list(country),
        year=_to_list(year), month=month_list,
        period_type=period_type, groupby_column=groupby,
        beverage_category=["Beer"],
    )
    return query_databricks(db_cfg, sql)


@st.cache_data(ttl=900)
def _fetch_volume(zone, country, year, month):
    z, c, y, m = _to_list(zone), _to_list(country), _to_list(year), _to_list(month)

    def _q(col, vals):
        inner = ", ".join("'" + v.replace("'", "''") + "'" for v in vals)
        return f"{col} IN ({inner})"

    where_parts = []
    if z: where_parts.append(_q("dl.zone", z))
    if c: where_parts.append(_q("dl.country", c))
    if y: where_parts.append(_q("dt.year", y))
    if m: where_parts.append(_q("dt.month", m))
    where = ("\nWHERE " + "\n    AND ".join(where_parts)) if where_parts else ""

    sql = f"""
SELECT
    dl.zone, dl.country, dl.plant, dt.year, dt.month,
    SUM(fv.filling_volume_in_hl) AS volume_hl
FROM {SCHEMA}.FACT_VOLUME fv
JOIN {SCHEMA}.DIM_TIME     dt ON fv.time_key     = dt.time_key
JOIN {SCHEMA}.DIM_LOCATION dl ON fv.location_key = dl.location_key
JOIN {SCHEMA}.DIM_SKU      ds ON fv.sku_key      = ds.sku_key
{where}
GROUP BY dl.zone, dl.country, dl.plant, dt.year, dt.month
ORDER BY volume_hl DESC
LIMIT 1000
"""
    return query_databricks(db_cfg, sql)


# ---------------------------------------------------------------------------
# Fetch on button click → store in session_state
# ---------------------------------------------------------------------------
if fetch_btn:
    with st.spinner("Fetching SCFD 2.0 — VILC…"):
        try:
            st.session_state["df_vilc"] = _fetch_vilc(zone, country, year, month, period_type)
        except Exception as e:
            st.error(f"VILC query failed: {e}")
            st.session_state["df_vilc"] = pd.DataFrame()

    with st.spinner("Fetching SCFD 2.0 — Spend…"):
        try:
            st.session_state["df_spend"] = _fetch_spend(zone, country, year, month, period_type)
        except Exception as e:
            st.error(f"Spend query failed: {e}")
            st.session_state["df_spend"] = pd.DataFrame()

    with st.spinner("Fetching SCFD 3.0 — Volume…"):
        try:
            st.session_state["df_vol"] = _fetch_volume(zone, country, year, month)
        except Exception as e:
            st.error(f"Volume query failed: {e}")
            st.session_state["df_vol"] = pd.DataFrame()

    st.session_state["clusters"] = None   # reset clusters on new fetch


# ---------------------------------------------------------------------------
# Show data if available in session
# ---------------------------------------------------------------------------
if "df_vilc" not in st.session_state:
    st.info("Set filters and click **Fetch Data**.")
    st.stop()


# ── SCFD 2.0 accordion ─────────────────────────────────────────────────────
df_vilc  = st.session_state.get("df_vilc",  pd.DataFrame())
df_spend = st.session_state.get("df_spend", pd.DataFrame())

# Merge VILC + Spend on common plant-level keys
if not df_vilc.empty and not df_spend.empty:
    keys = [k for k in JOIN_KEYS if k in df_vilc.columns and k in df_spend.columns]
    df_20 = pd.merge(df_vilc, df_spend, on=keys, how="outer")
elif not df_vilc.empty:
    df_20 = df_vilc
else:
    df_20 = df_spend

with st.expander(f"📊 SCFD 2.0 — Plant Level (VILC + Spend)  ·  {len(df_20):,} rows", expanded=True):
    st.dataframe(df_20, use_container_width=True, height=380)


# ── SCFD 3.0 accordion ─────────────────────────────────────────────────────
df_vol = st.session_state.get("df_vol", pd.DataFrame())

with st.expander(f"🍺 SCFD 3.0 — Volume  ·  {len(df_vol):,} rows", expanded=True):
    st.dataframe(df_vol, use_container_width=True, height=380)


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------
import os
import numpy as np

MAPPING_FILE = os.path.join(os.path.dirname(__file__), "plant_mapping_master.csv")

st.markdown("---")
cluster_btn = st.button("⚙️ Start Clustering", type="primary")

if cluster_btn:
    if df_vol.empty or "plant" not in df_vol.columns:
        st.warning("Need volume data. Fetch data first.")
    elif not os.path.exists(MAPPING_FILE):
        st.warning("plant_mapping_master.csv not found. Run `build_mapping.py` first.")
    else:
        mapping = pd.read_csv(MAPPING_FILE)

        # Plants present in volume (3.0)
        vol_plants = (
            df_vol.groupby("plant", as_index=False)["volume_hl"]
            .sum()
            .dropna(subset=["volume_hl"])
            .rename(columns={"plant": "scfd3_plant"})
        )

        # Plants present in 2.0
        scfd2_col = next((c for c in df_20.columns if c.lower() == "plant"), None)
        scfd2_plants = set(df_20[scfd2_col].dropna().unique()) if scfd2_col else set()

        # Join vol_plants → mapping on scfd3_plant
        mapped = vol_plants.merge(
            mapping[["scfd3_plant", "scfd2_plant", "canonical_name"]].dropna(subset=["scfd3_plant"]),
            on="scfd3_plant", how="left"
        )

        # ── Mapped: has scfd2 match AND that scfd2 is in the actual 2.0 result ──
        df_mapped = mapped[
            mapped["scfd2_plant"].notna() &
            mapped["scfd2_plant"].isin(scfd2_plants)
        ].copy()

        # ── Unmapped: everything else ──
        df_unmapped = mapped[
            ~(mapped["scfd2_plant"].notna() & mapped["scfd2_plant"].isin(scfd2_plants))
        ].copy()

        # Tag reason for unmapped
        def _reason(row):
            if pd.isna(row["scfd2_plant"]):
                name = str(row["scfd3_plant"]).upper()
                if any(k in name for k in ["DC ", " DC", "DEPOT", "DISTRIBUTION", "CSD", "WATER"]):
                    return "Distribution Center / Non-Brewery"
                return "No mapping found"
            return "Not in current 2.0 filter"

        df_unmapped["reason"] = df_unmapped.apply(_reason, axis=1)

        # ── Cluster mapped plants by volume ──
        vols   = df_mapped["volume_hl"].to_numpy()
        labels, centers = cluster_volume_1d(vols, n_clusters=n_clusters)

        cluster_names = {
            i: ("Small" if i == 0 else "Large" if i == n_clusters - 1 else "Medium")
            for i in range(n_clusters)
        }
        df_mapped["cluster"]       = labels
        df_mapped["cluster_label"] = df_mapped["cluster"].map(
            lambda i: f"Cluster {i} — {cluster_names[i]}"
        )
        df_mapped["center_hl"] = df_mapped["cluster"].map(
            {i: round(float(c), 2) for i, c in enumerate(centers.tolist())}
        )
        df_mapped = df_mapped.sort_values(["cluster", "volume_hl"], ascending=[True, False])

        st.session_state["clusters"]   = df_mapped
        st.session_state["unmapped"]   = df_unmapped
        st.session_state["n_clusters"] = n_clusters


if st.session_state.get("clusters") is not None:
    df_clust   = st.session_state["clusters"]
    df_unmap   = st.session_state.get("unmapped", pd.DataFrame())
    saved_k    = st.session_state.get("n_clusters", n_clusters)

    # ── Cluster results ────────────────────────────────────────────────────
    st.success(
        f"Clustered **{len(df_clust):,} matched plants** into **{saved_k} clusters**  |  "
        f"**{len(df_unmap):,} plants** excluded (not mapped)"
    )

    with st.expander("🔵 Cluster Results", expanded=True):
        # Summary cards
        summary = (
            df_clust.groupby(["cluster", "cluster_label"])
            .agg(plants=("scfd3_plant", "count"),
                 total_vol_hl=("volume_hl", "sum"),
                 center_hl=("center_hl", "first"))
            .reset_index().sort_values("cluster")
        )
        cols = st.columns(len(summary))
        for col, (_, row) in zip(cols, summary.iterrows()):
            col.metric(
                label=row["cluster_label"],
                value=f"{int(row['plants'])} plants",
                delta=f"{row['total_vol_hl']:,.0f} HL total",
            )

        st.markdown("**Plant detail**")
        st.dataframe(
            df_clust[["scfd3_plant", "scfd2_plant", "canonical_name",
                       "volume_hl", "cluster_label", "center_hl"]],
            use_container_width=True, height=400, hide_index=True,
        )

    # ── Unmapped plants ────────────────────────────────────────────────────
    if not df_unmap.empty:
        reason_counts = df_unmap["reason"].value_counts()
        unmap_label = "  ·  ".join(f"{v} {k}" for k, v in reason_counts.items())

        with st.expander(f"⚠️ Excluded Plants  ·  {len(df_unmap):,} total  |  {unmap_label}", expanded=False):
            for reason, grp in df_unmap.groupby("reason"):
                st.markdown(f"**{reason}** — {len(grp)} plants")
                st.dataframe(
                    grp[["scfd3_plant", "scfd2_plant", "volume_hl"]].sort_values(
                        "volume_hl", ascending=False
                    ),
                    use_container_width=True,
                    height=min(200, 38 + len(grp) * 35),
                    hide_index=True,
                )
                st.markdown("")
