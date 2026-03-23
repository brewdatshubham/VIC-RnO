from __future__ import annotations

import streamlit as st
import pandas as pd

from app.config import get_databricks_config
from app.databricks_client import query_databricks
from app.stored_procedures.get_vilc_summary import VilcSummary
from app.stored_procedures.get_spend import SpendKPIs
from app.stored_procedures.get_cost_per_hl import SCHEMA
from app.stored_procedures.get_beerometer_kpi import BeerometerKPIs
from app.analytics.clustering import cluster_volume_1d

import os
import numpy as np

MAPPING_FILE = os.path.join(os.path.dirname(__file__), "plant_mapping_master.csv")

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
    fetch_btn   = st.button("Fetch Data", type="primary", width='stretch')

zone  = ",".join(sel_zones)  if sel_zones  else "All"
month = ",".join(sel_months) if sel_months else "All"


VIC_PACKAGES = [
    "Stock Impacts",
    "Packaging materials",
    "Raw materials",
    "Buy-Sell Ops",
    "Auxiliary materials",
    "Direct Wages and Salaries",
    "Co-Products",
    "Footprint / Sourcing",
    "Imports / Exports",
    "Non-Supply VIC Package",
    "SD Kit",
    "Maintenance-RCM",
    "Direct Energy & Fluids",
    "Non-Quality",
    "Services / Taxes / Other Ops",
    "Reconciliation",
]

PACKAGE_KPI_MAP = {
    "Direct Energy & Fluids":      "Total Purchased Energy per hLN",
    "Raw materials":               "Brewery Total Extract Losses",
    "Direct Wages and Salaries":   "Total Technical Productivity",
}

KPI_LABEL = {
    "Direct Energy & Fluids":    "TPE (kWh/hL)",
    "Raw materials":             "Extract Loss (%)",
    "Direct Wages and Salaries": "Tech Productivity",
}

# Packages where a HIGHER KPI value is better (benchmark = max)
# All others default to lower-is-better (benchmark = min)
KPI_HIGHER_IS_BETTER = {"Direct Wages and Salaries"}


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


@st.cache_data(ttl=900)
def _fetch_opportunity(plants: tuple, zone, country, year, month, period_type) -> pd.DataFrame:
    """Fetch price & performance per plant per VIC package for a given set of plants."""
    sql = VilcSummary().get_vilc_summary(
        plant=list(plants),
        zone=_to_list(zone),
        country=_to_list(country),
        year=_to_list(year),
        month=_to_list(month),
        period_type=period_type,
        groupby_column=["plant", "package"],
        beverage_category=["Beer"],
    )
    return query_databricks(db_cfg, sql)


@st.cache_data(ttl=900)
def _fetch_spend_actual(
    plants: tuple, package: str,
    zone: str, country: str, year: str, month: str,
) -> pd.DataFrame:
    """Fetch actual spend ($K) per plant for a specific package, year, month."""
    month_list = _to_list(month)
    # Keep month out of groupby so the month WHERE filter fires
    sql = SpendKPIs().get_spend(
        plant=list(plants),
        package=[package],
        zone=_to_list(zone),
        country=_to_list(country),
        year=_to_list(year),
        month=month_list,
        period_type="MTH",
        groupby_column=["plant"],
        beverage_category=["Beer"],
    )
    sql = sql.replace("LIMIT 1000", "LIMIT 5000")
    df = query_databricks(db_cfg, sql)
    df.columns = [c.lower() for c in df.columns]
    if not df.empty and "actual_spend" in df.columns and "plant" in df.columns:
        df = df.groupby("plant", as_index=False)["actual_spend"].sum()
    return df


@st.cache_data(ttl=900)
def _fetch_beerometer_snapshot(
    beerometer_plants: tuple, kpi_name: str,
    zone: str, country: str, year: str, month: str,
) -> pd.DataFrame:
    """Fetch beerometer KPI per plant for the user-selected year/month (AC scenario)."""
    sql = BeerometerKPIs().get_beerometer_kpis(
        plant=list(beerometer_plants),
        kpi_name=[kpi_name],
        zone=_to_list(zone),
        country=_to_list(country),
        year=_to_list(year),
        month=_to_list(month),
        period_type="MTH",
        groupby_column=["plant", "year", "month"],
    )
    sql = sql.replace("LIMIT 1000", "LIMIT 5000")
    df = query_databricks(db_cfg, sql)
    df.columns = [c.lower() for c in df.columns]
    if "scenario" in df.columns:
        df = df[df["scenario"].str.upper() == "AC"]
    if not df.empty and "kpi_value" in df.columns and "plant" in df.columns:
        df = df.groupby("plant", as_index=False)["kpi_value"].mean()
    return df


@st.cache_data(ttl=900)
def _fetch_beerometer_for_corr(beerometer_plants: tuple, kpi_name: str, zone: str, country: str) -> pd.DataFrame:
    """Fetch AC beerometer KPI data for all available years (for correlation)."""
    sql = BeerometerKPIs().get_beerometer_kpis(
        plant=list(beerometer_plants),
        kpi_name=[kpi_name],
        zone=_to_list(zone),
        country=_to_list(country),
        year=["2022", "2023", "2024", "2025"],
        period_type="MTH",
        groupby_column=["plant", "year", "month"],
    )
    sql = sql.replace("LIMIT 1000", "LIMIT 20000")
    df = query_databricks(db_cfg, sql)
    df.columns = [c.lower() for c in df.columns]
    if "scenario" in df.columns:
        df = df[df["scenario"].str.upper() == "AC"]
    return df


@st.cache_data(ttl=900)
def _fetch_vilc_for_corr(scfd2_plants: tuple, package: str, zone: str, country: str) -> pd.DataFrame:
    """Fetch VILC performance data for all available years (for correlation)."""
    sql = VilcSummary().get_vilc_summary(
        plant=list(scfd2_plants),
        package=[package],
        zone=_to_list(zone),
        country=_to_list(country),
        year=["2022", "2023", "2024", "2025"],
        period_type="MTD",
        groupby_column=["plant", "year", "month"],
        beverage_category=["Beer"],
    )
    sql = sql.replace("LIMIT 1000", "LIMIT 20000")
    return query_databricks(db_cfg, sql)


def _compute_plant_correlations(
    df_beer: pd.DataFrame,
    df_vilc_corr: pd.DataFrame,
    mapping_df: pd.DataFrame,
    scfd2_plants: list,
) -> dict:
    """Compute Pearson r between beerometer KPI and VILC performance per plant.
    Returns {scfd2_plant: pearson_r}."""
    # Build beerometer_plant → scfd2_plant lookup (restricted to this cluster)
    beer_to_scfd2 = (
        mapping_df[mapping_df["scfd2_plant"].isin(scfd2_plants)]
        [["beerometer_plant", "scfd2_plant"]]
        .dropna()
        .drop_duplicates("beerometer_plant")
        .set_index("beerometer_plant")["scfd2_plant"]
        .to_dict()
    )

    if df_beer.empty or df_vilc_corr.empty or not beer_to_scfd2:
        return {}

    df_beer = df_beer.copy()
    df_vilc_corr = df_vilc_corr.copy()
    df_beer.columns = [c.lower() for c in df_beer.columns]
    df_vilc_corr.columns = [c.lower() for c in df_vilc_corr.columns]

    beer_plant_col = next((c for c in df_beer.columns if c == "plant"), None)
    if not beer_plant_col or "kpi_value" not in df_beer.columns:
        return {}
    if "performance" not in df_vilc_corr.columns or "plant" not in df_vilc_corr.columns:
        return {}

    df_beer["scfd2_plant"] = df_beer[beer_plant_col].map(beer_to_scfd2)
    df_beer = df_beer.dropna(subset=["scfd2_plant"])

    correlations = {}
    for plant in scfd2_plants:
        b = df_beer[df_beer["scfd2_plant"] == plant]
        v = df_vilc_corr[df_vilc_corr["plant"] == plant]
        if b.empty or v.empty:
            continue

        merged = pd.merge(
            b[["year", "month", "kpi_value"]],
            v[["year", "month", "performance"]],
            on=["year", "month"],
            how="inner",
        ).dropna(subset=["kpi_value", "performance"])

        if len(merged) < 2:
            continue

        x = merged["kpi_value"].to_numpy(dtype=float)
        y = merged["performance"].to_numpy(dtype=float)
        if x.std() == 0 or y.std() == 0:
            continue

        r = np.corrcoef(x, y)[0, 1]
        correlations[plant] = round(float(r), 3)

    return correlations


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

    # Auto-cluster immediately after fetching
    _vol = st.session_state.get("df_vol", pd.DataFrame())
    if not _vol.empty and "plant" in _vol.columns and os.path.exists(MAPPING_FILE):
        mapping = pd.read_csv(MAPPING_FILE)
        vol_plants = (
            _vol.groupby("plant", as_index=False)["volume_hl"]
            .sum()
            .dropna(subset=["volume_hl"])
            .rename(columns={"plant": "scfd3_plant"})
        )
        mapped = vol_plants.merge(
            mapping[["scfd3_plant", "scfd2_plant", "canonical_name"]].dropna(subset=["scfd3_plant"]),
            on="scfd3_plant", how="left",
        )
        df_mapped   = mapped[mapped["scfd2_plant"].notna()].copy()
        df_unmapped = mapped[mapped["scfd2_plant"].isna()].copy()

        def _reason(row):
            name = str(row["scfd3_plant"]).upper()
            if any(k in name for k in ["DC ", " DC", "DEPOT", "DISTRIBUTION", "CSD", "WATER",
                                        "AMBEV REFRIGERANTES", "ADM.", "TECNOLOGICO"]):
                return "Distribution Center / Non-Brewery"
            return "No mapping found"
        df_unmapped["reason"] = df_unmapped.apply(_reason, axis=1)

        vols = df_mapped["volume_hl"].to_numpy()
        labels, _ = cluster_volume_1d(vols, n_clusters=n_clusters)
        cluster_names = {
            i: ("Small" if i == 0 else "Large" if i == n_clusters - 1 else "Medium")
            for i in range(n_clusters)
        }
        df_mapped["cluster"]       = labels
        df_mapped["cluster_label"] = df_mapped["cluster"].map(lambda i: f"Cluster {i} — {cluster_names[i]}")
        df_mapped = df_mapped.sort_values(["cluster", "volume_hl"], ascending=[True, False])
        df_mapped["volume_khl"] = (df_mapped["volume_hl"] / 1000).round(0).astype(int)

        cluster_ranges = (
            df_mapped.groupby("cluster")["volume_khl"]
            .agg(lo="min", hi="max")
            .sort_index()
        )
        def _bucket(cid):
            lo, hi = int(cluster_ranges.loc[cid, "lo"]), int(cluster_ranges.loc[cid, "hi"])
            prev = cluster_ranges.index[cluster_ranges.index < cid]
            nxt  = cluster_ranges.index[cluster_ranges.index > cid]
            if len(prev) == 0:  return f"< {hi:,} K HL"
            elif len(nxt) == 0: return f"> {lo:,} K HL"
            else:               return f"{lo:,} – {hi:,} K HL"
        df_mapped["volume_bucket"] = df_mapped["cluster"].map(_bucket)

        st.session_state["clusters"]   = df_mapped
        st.session_state["unmapped"]   = df_unmapped
        st.session_state["n_clusters"] = n_clusters
    else:
        st.session_state["clusters"] = None


# ---------------------------------------------------------------------------
# Guard — nothing to show until first fetch
# ---------------------------------------------------------------------------
if "df_vilc" not in st.session_state:
    st.info("Set filters and click **Fetch Data**.")
    st.stop()

if st.session_state.get("clusters") is None:
    st.warning("Could not build clusters — check filters or ensure `plant_mapping_master.csv` exists.")
    st.stop()

df_vilc  = st.session_state.get("df_vilc",  pd.DataFrame())
df_spend = st.session_state.get("df_spend", pd.DataFrame())
df_vol   = st.session_state.get("df_vol",   pd.DataFrame())
df_clust = st.session_state["clusters"]
df_unmap = st.session_state.get("unmapped", pd.DataFrame())
saved_k  = st.session_state.get("n_clusters", n_clusters)

if not df_vilc.empty and not df_spend.empty:
    keys = [k for k in JOIN_KEYS if k in df_vilc.columns and k in df_spend.columns]
    df_20 = pd.merge(df_vilc, df_spend, on=keys, how="outer")
elif not df_vilc.empty:
    df_20 = df_vilc
else:
    df_20 = df_spend


# ── Per-cluster Opportunity Analysis ────────────────────────────────────────
st.markdown("### 🎯 Opportunity Analysis")

for cluster_id in sorted(df_clust["cluster"].unique(), reverse=True):
    cdf      = df_clust[df_clust["cluster"] == cluster_id]
    clabel   = cdf["cluster_label"].iloc[0]
    cbucket  = cdf["volume_bucket"].iloc[0]
    c_plants = tuple(sorted(cdf["scfd2_plant"].dropna().unique().tolist()))
    opp_key  = f"opp_{cluster_id}"

    with st.expander(f"📦 {clabel}  ·  {cbucket}  ·  {len(c_plants)} plants", expanded=False):

        if st.button("Load Analysis", key=f"opp_btn_{cluster_id}", type="primary"):
            with st.spinner(f"Fetching package-level performance for {clabel}…"):
                try:
                    st.session_state[opp_key] = _fetch_opportunity(
                        c_plants, zone, country, year, month, period_type
                    )
                    for k in [k for k in st.session_state
                              if k.startswith(f"corr_{cluster_id}_")
                              or k.startswith(f"snap_{cluster_id}_")
                              or k.startswith(f"spend_{cluster_id}_")]:
                        del st.session_state[k]
                except Exception as e:
                    st.error(f"Failed to fetch opportunity data: {e}")

        if opp_key not in st.session_state:
            st.caption("Click **Load Analysis** to fetch package-level price & performance.")
            continue

        df_opp = st.session_state[opp_key]

        if df_opp.empty:
            st.warning("No data returned for this cluster with current filters.")
            continue

        df_opp.columns = [c.lower() for c in df_opp.columns]
        pkg_col   = next((c for c in df_opp.columns if "package" in c), None)
        perf_col  = "performance"
        price_col = "price"
        pp_col    = "price_and_performance"
        plant_col = "plant"

        if not pkg_col or perf_col not in df_opp.columns:
            st.warning(f"Expected columns not found. Got: {list(df_opp.columns)}")
            continue

        pkg_agg = (
            df_opp.groupby(pkg_col)[perf_col]
            .sum().reset_index().sort_values(perf_col).reset_index(drop=True)
        )
        worst_pkg   = pkg_agg[pkg_col].iloc[0]
        avail_pkgs  = pkg_agg[pkg_col].tolist()

        selected_pkg = st.selectbox(
            "VIC Package",
            options=avail_pkgs,
            index=avail_pkgs.index(worst_pkg),
            key=f"pkg_sel_{cluster_id}",
            help="Sorted worst → best performance. Default = lowest performing package for this cluster.",
        )

        # Plant × selected package — one row per plant
        df_display = (
            df_opp[df_opp[pkg_col] == selected_pkg]
            .groupby(plant_col, as_index=False)[[price_col, perf_col, pp_col]]
            .sum().sort_values(perf_col).reset_index(drop=True)
        )
        df_display.columns = ["plant", "price ($K)", "performance ($K)", "price + perf ($K)"]
        for col in ["price ($K)", "performance ($K)", "price + perf ($K)"]:
            df_display[col] = df_display[col].round(2)

        # Enrich with zone, canonical_name, volume
        df_vol_ss = st.session_state.get("df_vol", pd.DataFrame())
        vol_zone  = (
            df_vol_ss[["plant", "zone"]].drop_duplicates("plant").rename(columns={"plant": "scfd3_plant"})
            if not df_vol_ss.empty else pd.DataFrame(columns=["scfd3_plant", "zone"])
        )
        cdf_meta = (
            cdf[["scfd2_plant", "scfd3_plant", "canonical_name", "volume_khl"]]
            .dropna(subset=["scfd2_plant"])
            .merge(vol_zone, on="scfd3_plant", how="left")
        )
        df_display = df_display.merge(
            cdf_meta[["scfd2_plant", "zone", "canonical_name", "volume_khl"]]
            .rename(columns={"scfd2_plant": "plant", "volume_khl": "volume (K HL)"}),
            on="plant", how="left",
        )

        # Per-cluster benchmark controls (only for mapped packages)
        if selected_pkg in PACKAGE_KPI_MAP:
            _cn_map = cdf.set_index("scfd2_plant")["canonical_name"].to_dict()
            c_plant_display = {p: (_cn_map.get(p) or p) for p in c_plants}
            bm_opts = ["auto"] + list(c_plants)
            col_bm, col_kpi = st.columns([3, 1])
            with col_kpi:
                manual_bm_kpi_val = st.number_input(
                    "Override benchmark KPI",
                    value=0.0, step=0.1, format="%.2f",
                    help="If non-zero, use this as the benchmark KPI value directly (simulate any target).",
                    key=f"bm_kpi_{cluster_id}",
                )
            manual_bm_kpi = float(manual_bm_kpi_val) if manual_bm_kpi_val != 0.0 else None
            with col_bm:
                sel_bm_plant = st.selectbox(
                    "Benchmark Brewery",
                    options=bm_opts,
                    format_func=lambda p: "Auto (best KPI score)" if p == "auto" else c_plant_display.get(p, p),
                    key=f"bm_plant_{cluster_id}",
                    disabled=manual_bm_kpi is not None,
                )

        # Benchmark analysis (only for mapped packages)
        if selected_pkg in PACKAGE_KPI_MAP:
            target_kpi = PACKAGE_KPI_MAP[selected_pkg]
            kpi_label  = KPI_LABEL[selected_pkg]
            snap_key   = f"snap_{cluster_id}_{selected_pkg}"
            corr_key   = f"corr_{cluster_id}_{selected_pkg}"

            mapping_df  = pd.read_csv(MAPPING_FILE)
            beer_plants = tuple(sorted(
                mapping_df[mapping_df["scfd2_plant"].isin(list(c_plants))]
                ["beerometer_plant"].dropna().unique().tolist()
            ))

            if snap_key not in st.session_state:
                if beer_plants:
                    with st.spinner(f"Fetching {kpi_label} ({year})…"):
                        try:
                            st.session_state[snap_key] = _fetch_beerometer_snapshot(
                                beer_plants, target_kpi, zone, country, year, month
                            )
                        except Exception as e:
                            st.error(f"KPI snapshot failed: {e}")
                            st.session_state[snap_key] = pd.DataFrame()
                else:
                    st.session_state[snap_key] = pd.DataFrame()

            if corr_key not in st.session_state:
                if beer_plants:
                    with st.spinner("Computing correlation (2022–2025)…"):
                        try:
                            df_beer_corr = _fetch_beerometer_for_corr(beer_plants, target_kpi, zone, country)
                            df_vilc_corr = _fetch_vilc_for_corr(c_plants, selected_pkg, zone, country)
                            st.session_state[corr_key] = _compute_plant_correlations(
                                df_beer_corr, df_vilc_corr, mapping_df, list(c_plants)
                            )
                        except Exception as e:
                            st.error(f"Correlation failed: {e}")
                            st.session_state[corr_key] = {}
                else:
                    st.session_state[corr_key] = {}

            df_snap = st.session_state.get(snap_key, pd.DataFrame())
            if not df_snap.empty and "plant" in df_snap.columns and "kpi_value" in df_snap.columns:
                b2s = (
                    mapping_df[mapping_df["scfd2_plant"].isin(list(c_plants))]
                    [["beerometer_plant", "scfd2_plant"]].dropna()
                    .drop_duplicates("beerometer_plant")
                    .set_index("beerometer_plant")["scfd2_plant"].to_dict()
                )
                snap_mapped = df_snap.copy()
                snap_mapped["plant"] = snap_mapped["plant"].map(b2s)
                snap_mapped = snap_mapped.dropna(subset=["plant"])
                df_display = df_display.merge(
                    snap_mapped[["plant", "kpi_value"]].rename(columns={"kpi_value": kpi_label}),
                    on="plant", how="left",
                )
                df_display[kpi_label] = df_display[kpi_label].round(2)
            else:
                df_display[kpi_label] = None

            correlations = st.session_state.get(corr_key, {})
            df_display["perf_corr (r)"] = df_display["plant"].map(correlations)

            # Benchmark determination
            if sel_bm_plant != "auto":
                bm_row = df_display[df_display["plant"] == sel_bm_plant]
                _cn = bm_row["canonical_name"].iloc[0] if not bm_row.empty else None
                benchmark_name = (_cn if pd.notna(_cn) else sel_bm_plant) if not bm_row.empty else sel_bm_plant
                benchmark_kpi  = bm_row[kpi_label].iloc[0] if (not bm_row.empty and kpi_label in df_display.columns) else None
            else:
                valid_kpi = df_display[kpi_label].dropna() if kpi_label in df_display.columns else pd.Series(dtype=float)
                if not valid_kpi.empty:
                    higher_is_better = selected_pkg in KPI_HIGHER_IS_BETTER
                    benchmark_idx  = df_display[kpi_label].idxmax() if higher_is_better else df_display[kpi_label].idxmin()
                    _cn = df_display.loc[benchmark_idx, "canonical_name"]
                    benchmark_name = _cn if pd.notna(_cn) else df_display.loc[benchmark_idx, "plant"]
                    benchmark_kpi  = df_display.loc[benchmark_idx, kpi_label]
                else:
                    benchmark_name, benchmark_kpi = None, None

            if manual_bm_kpi is not None:
                benchmark_kpi = manual_bm_kpi

            df_display["benchmark"] = benchmark_name

            spend_key = f"spend_{cluster_id}_{selected_pkg}"
            if spend_key not in st.session_state:
                with st.spinner(f"Fetching actual spend ({selected_pkg})…"):
                    try:
                        st.session_state[spend_key] = _fetch_spend_actual(
                            c_plants, selected_pkg, zone, country, year, month
                        )
                    except Exception as e:
                        st.error(f"Spend fetch failed: {e}")
                        st.session_state[spend_key] = pd.DataFrame()

            df_spend_act = st.session_state.get(spend_key, pd.DataFrame())
            if not df_spend_act.empty and "actual_spend" in df_spend_act.columns:
                df_display = df_display.merge(df_spend_act[["plant", "actual_spend"]], on="plant", how="left")
                df_display["spend (Mil $)"] = df_display["actual_spend"].round(3)
                df_display.drop(columns=["actual_spend"], inplace=True)
            else:
                df_display["spend (Mil $)"] = None

            if benchmark_kpi is not None and not pd.isna(benchmark_kpi) and benchmark_kpi != 0:
                raw_delta = (df_display[kpi_label] - benchmark_kpi) / benchmark_kpi
                if selected_pkg in KPI_HIGHER_IS_BETTER:
                    # Flip sign: opportunity = how far BELOW the benchmark the plant is.
                    # Positive  → plant underperforms (kpi < benchmark) → has opportunity.
                    # Negative  → plant already exceeds benchmark         → no opportunity.
                    kpi_delta = -raw_delta
                else:
                    # Lower-is-better: positive delta means plant is worse than benchmark.
                    kpi_delta = raw_delta
                df_display["kpi delta %"] = kpi_delta.round(4)
                # clip(lower=0): zero opportunity when plant already beats the benchmark
                # clip(upper=1.0): opportunity cannot exceed total actual spend
                df_display["opp (Mil $)"] = (
                    df_display["spend (Mil $)"] * kpi_delta.clip(lower=0, upper=1.0)
                ).round(3)
            else:
                df_display["kpi delta %"] = None
                df_display["opp (Mil $)"]  = None

        # Column ordering and display
        base_cols = ["zone", "plant", "canonical_name", "performance ($K)"]
        kpi_cols  = (
            [KPI_LABEL[selected_pkg], "volume (K HL)", "perf_corr (r)",
             "benchmark", "kpi delta %", "spend (Mil $)", "opp (Mil $)"]
            if selected_pkg in PACKAGE_KPI_MAP else ["volume (K HL)"]
        )
        show_cols = [c for c in base_cols + kpi_cols if c in df_display.columns]
        df_show   = df_display[show_cols]

        color_cols = [c for c in ["performance ($K)", "opp (Mil $)"] if c in df_show.columns]
        st.dataframe(
            df_show.style.applymap(
                lambda v: "color: red"   if isinstance(v, (int, float)) and v < 0 else
                          "color: green" if isinstance(v, (int, float)) and v > 0 else "",
                subset=color_cols,
            ),
            width='stretch',
            height=min(600, 40 + len(df_show) * 35),
            hide_index=True,
        )

        if selected_pkg in PACKAGE_KPI_MAP:
            corr_key = f"corr_{cluster_id}_{selected_pkg}"
            covered  = len(st.session_state.get(corr_key, {}))
            bm_direction = "highest" if selected_pkg in KPI_HIGHER_IS_BETTER else "lowest"
            st.caption(
                f"Benchmark = {bm_direction} KPI score · "
                f"kpi delta % = (plant − benchmark) / benchmark · "
                f"opp = spend × kpi delta · "
                f"perf_corr (r): Pearson r vs *{PACKAGE_KPI_MAP[selected_pkg]}* "
                f"(AC, 2022–2025, {covered}/{len(c_plants)} plants)"
            )


# ── Debug accordion (collapsed by default) ──────────────────────────────────
with st.expander("🔧 Debug", expanded=False):
    st.subheader(f"SCFD 2.0 — VILC + Spend  ·  {len(df_20):,} rows")
    st.dataframe(df_20, width='stretch', height=350)

    st.subheader(f"SCFD 3.0 — Volume  ·  {len(df_vol):,} rows")
    st.dataframe(df_vol, width='stretch', height=350)

    st.subheader("Cluster Results")
    st.caption(f"{len(df_clust):,} matched plants · {saved_k} clusters · {len(df_unmap):,} excluded")
    _summary = (
        df_clust.groupby(["cluster", "cluster_label", "volume_bucket"])
        .agg(plants=("scfd3_plant", "count"), total_vol_khl=("volume_khl", "sum"))
        .reset_index().sort_values("cluster")
    )
    _cols = st.columns(len(_summary))
    for _col, (_, row) in zip(_cols, _summary.iterrows()):
        _col.metric(
            label=f"{row['cluster_label']}\n{row['volume_bucket']}",
            value=f"{int(row['plants'])} plants",
            delta=f"{row['total_vol_khl']:,} K HL total",
        )
    st.dataframe(
        df_clust[["scfd3_plant", "scfd2_plant", "canonical_name", "volume_khl", "cluster_label", "volume_bucket"]]
        .rename(columns={"volume_khl": "volume (K HL)", "volume_bucket": "bucket"}),
        width='stretch', height=400, hide_index=True,
    )

    if not df_unmap.empty:
        reason_counts = df_unmap["reason"].value_counts()
        st.subheader("Excluded Plants  ·  " + "  ·  ".join(f"{v} {k}" for k, v in reason_counts.items()))
        for reason, grp in df_unmap.groupby("reason"):
            st.markdown(f"**{reason}** — {len(grp)} plants")
            grp = grp.copy()
            grp["volume (K HL)"] = (grp["volume_hl"] / 1000).round(0).astype("Int64")
            st.dataframe(
                grp[["scfd3_plant", "scfd2_plant", "volume (K HL)"]].sort_values("volume (K HL)", ascending=False),
                width='stretch', height=min(200, 38 + len(grp) * 35), hide_index=True,
            )
