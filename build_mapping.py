"""
build_mapping.py
----------------
Fetches distinct plant names from SCFD 2.0 and SCFD 3.0 for every zone,
fuzzy-matches them, merges existing beerometer mappings, and writes:

    plant_mapping_master.csv
    Columns: zone | country | scfd2_plant | scfd3_plant | match_score
             | canonical_name | beerometer_plant

Usage:
    python build_mapping.py
"""
from __future__ import annotations

import os
import sys
import difflib

import pandas as pd

from app.stored_procedures.get_vilc_summary import VilcSummary
from app.stored_procedures.get_cost_per_hl import CostPerHLKPIs

ZONES = ["APAC", "MAZ", "NAZ", "SAZ", "EUR", "AFR"]
EXISTING_MAPPING = "Plant_Mapping.csv"
OUTPUT_FILE = "plant_mapping_master.csv"


# ---------------------------------------------------------------------------
# Databricks connection
# ---------------------------------------------------------------------------
def _get_config():
    secrets_path = os.path.join(".streamlit", "secrets.toml")
    if not os.path.exists(secrets_path):
        sys.exit(f"ERROR: {secrets_path} not found.")

    # Parse secrets.toml — try stdlib tomllib (Py 3.11+) then fall back to toml package
    try:
        import tomllib
        with open(secrets_path, "rb") as f:
            data = tomllib.load(f)
    except ImportError:
        import toml
        data = toml.load(secrets_path)

    cfg = data.get("databricks", {})
    host = cfg.get("host", "").strip()
    http_path = cfg.get("http_path", "").strip()
    token = cfg.get("token", "").strip()
    if not host or not http_path or not token:
        sys.exit("ERROR: Databricks credentials incomplete in secrets.toml")
    return host, http_path, token


def _query(sql: str) -> pd.DataFrame:
    from databricks import sql as dbsql
    host, http_path, token = _get_config()
    with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in (cur.description or [])]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# Fetch distinct plant names per zone using the stored procedure classes
# ---------------------------------------------------------------------------
def fetch_scfd2_plants(zone: str) -> pd.DataFrame:
    """Uses VilcSummary (same query builder as dashboard) — groupby zone+country+plant."""
    sql = VilcSummary().get_vilc_summary(
        zone=[zone],
        groupby_column=["zone", "country", "plant"],
        beverage_category=["Beer"],
    )
    df = _query(sql)
    # keep only the dimension columns, drop metric columns
    dim_cols = [c for c in df.columns if c not in ("price", "performance", "price_and_performance")]
    df = df[dim_cols].drop_duplicates()
    # normalise column name to scfd2_plant
    plant_col = next((c for c in df.columns if c.lower() == "plant"), None)
    if plant_col:
        df = df.rename(columns={plant_col: "scfd2_plant"})
    return df


def fetch_scfd3_plants(zone: str) -> pd.DataFrame:
    """Uses CostPerHLKPIs (same query builder as dashboard) — groupby zone+country+plant."""
    sql = CostPerHLKPIs().get_cost_per_hl(
        zone=[zone],
        groupby_column=["zone", "country", "plant"],
    )
    df = _query(sql)
    # keep only dimension columns
    dim_cols = [c for c in df.columns if c not in ("volume", "cost_per_hl", "total_cost")]
    df = df[dim_cols].drop_duplicates()
    plant_col = next((c for c in df.columns if c.lower() == "plant"), None)
    if plant_col:
        df = df.rename(columns={plant_col: "scfd3_plant"})
    return df


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Normalise a raw plant name for better fuzzy matching
# Strips known prefixes/suffixes that differ between SCFD 2.0 and 3.0
# ---------------------------------------------------------------------------
import re

_STRIP_PREFIXES = [
    r"^cervejaria\s+",          # CERVEJARIA RIO DE JANEIRO → RIO DE JANEIRO
    r"^c\.\s+",                 # C. PONTA GROSSA PROD-DC  → PONTA GROSSA PROD-DC
    r"^f\.\s+",                 # F. NOVA MINAS PROD-DC    → NOVA MINAS PROD-DC
    r"^ambev refrigerantes\s+", # AMBEV REFRIGERANTES X    → X
    r"^planta\s+",              # Planta Zacapa            → Zacapa
]
_STRIP_SUFFIXES = [
    r"\s+prod-dc$",             # F. NOVA MINAS PROD-DC    → F. NOVA MINAS
    r"\s+prod$",
    r"\s+brewery$",
    r"\s+beer$",
    r"\s+central$",
]
_STRIP_WORDS = [
    r"\bnova\b",                # NOVA MINAS → MINAS  (often "nova" is not in the 2.0 name)
    r"\bda\b", r"\bde\b", r"\bdo\b", r"\bdos\b",  # Portuguese articles
]

def _normalise(name: str) -> str:
    s = name.strip().lower()
    for pat in _STRIP_PREFIXES:
        s = re.sub(pat, "", s)
    for pat in _STRIP_SUFFIXES:
        s = re.sub(pat, "", s)
    for pat in _STRIP_WORDS:
        s = re.sub(pat, " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _best_match(p3: str, candidates: list[str]) -> tuple[str | None, float | None]:
    """Try normalised fuzzy match first; fall back to raw match."""
    norm3 = _normalise(p3)
    norm_cands = {_normalise(c): c for c in candidates}

    # 1. exact normalised match
    if norm3 in norm_cands:
        orig = norm_cands[norm3]
        return orig, 1.0

    # 2. fuzzy on normalised strings
    matches = difflib.get_close_matches(norm3, list(norm_cands.keys()), n=1, cutoff=0.4)
    if matches:
        orig = norm_cands[matches[0]]
        score = round(difflib.SequenceMatcher(None, norm3, matches[0]).ratio(), 3)
        return orig, score

    # 3. keyword containment — if every word in the normalised scfd3 name
    #    appears in a normalised scfd2 candidate (or vice versa)
    words3 = set(norm3.split())
    for nc, orig in norm_cands.items():
        words2 = set(nc.split())
        if words3 and words3.issubset(words2):
            score = round(difflib.SequenceMatcher(None, norm3, nc).ratio(), 3)
            return orig, score
        if words2 and words2.issubset(words3):
            score = round(difflib.SequenceMatcher(None, norm3, nc).ratio(), 3)
            return orig, score

    return None, None


# ---------------------------------------------------------------------------
# Fuzzy match scfd3 → scfd2 within same country (fallback: any in zone)
# ---------------------------------------------------------------------------
def fuzzy_match_zone(zone: str, df2: pd.DataFrame, df3: pd.DataFrame) -> pd.DataFrame:
    records = []
    matched_scfd2 = set()

    for _, r3 in df3.iterrows():
        country, p3 = r3["country"], r3["scfd3_plant"]

        if not p3 or (isinstance(p3, float)):
            continue

        candidates_df = df2[df2["country"] == country]
        if candidates_df.empty:
            candidates_df = df2   # fallback to all plants in zone

        candidates = [c for c in candidates_df["scfd2_plant"].tolist() if c and not isinstance(c, float)]
        best, score = _best_match(p3, candidates)
        if best:
            matched_scfd2.add(best)

        records.append({
            "zone": zone,
            "country": country,
            "scfd2_plant": best,
            "scfd3_plant": p3,
            "match_score": score,
            "canonical_name": best if best else p3,
            "beerometer_plant": None,
        })

    # scfd2 plants with no scfd3 counterpart
    for _, r2 in df2.iterrows():
        if not r2["scfd2_plant"] or isinstance(r2["scfd2_plant"], float):
            continue
        if r2["scfd2_plant"] not in matched_scfd2:
            records.append({
                "zone": zone,
                "country": r2["country"],
                "scfd2_plant": r2["scfd2_plant"],
                "scfd3_plant": None,
                "match_score": None,
                "canonical_name": r2["scfd2_plant"],
                "beerometer_plant": None,
            })

    return pd.DataFrame(records, columns=[
        "zone", "country", "scfd2_plant", "scfd3_plant",
        "match_score", "canonical_name", "beerometer_plant",
    ])


# ---------------------------------------------------------------------------
# Merge existing beerometer_plant column from Plant_Mapping.csv
# ---------------------------------------------------------------------------
def merge_beerometer(master: pd.DataFrame) -> pd.DataFrame:
    if not os.path.exists(EXISTING_MAPPING):
        print(f"  WARNING: {EXISTING_MAPPING} not found — beerometer column left blank.")
        return master

    existing = pd.read_csv(EXISTING_MAPPING)
    if "beerometer_plant" not in existing.columns or "vilc_plant" not in existing.columns:
        print("  WARNING: existing mapping missing columns — beerometer column left blank.")
        return master

    # vilc_plant (= scfd2 canonical) → beerometer_plant
    beerometer_map = dict(zip(
        existing["vilc_plant"].astype(str).str.strip(),
        existing["beerometer_plant"].astype(str).str.strip(),
    ))
    master["beerometer_plant"] = master["canonical_name"].map(beerometer_map)
    return master


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    all_zones = []

    for zone in ZONES:
        print(f"\n── {zone} ──────────────────────────")
        df2 = fetch_scfd2_plants(zone)
        df3 = fetch_scfd3_plants(zone)
        print(f"  SCFD 2.0 unique plants: {len(df2)}")
        print(f"  SCFD 3.0 unique plants: {len(df3)}")

        if df2.empty and df3.empty:
            print("  No data — skipping.")
            continue

        df_zone = fuzzy_match_zone(zone, df2, df3)

        low_conf = df_zone["match_score"].notna() & (df_zone["match_score"] < 0.8)
        print(f"  Matched pairs          : {df_zone['match_score'].notna().sum()}")
        print(f"  Low confidence (<0.80) : {low_conf.sum()}  ← review these")

        all_zones.append(df_zone)

    if not all_zones:
        print("\nNo data returned from any zone. Check credentials / filters.")
        return

    master = pd.concat(all_zones, ignore_index=True)
    master = merge_beerometer(master)
    master.sort_values(["zone", "country", "canonical_name"], na_position="last", inplace=True)
    master.to_csv(OUTPUT_FILE, index=False)

    total = len(master)
    needs_review = (master["match_score"].notna() & (master["match_score"] < 0.8)).sum()
    no_match = master["scfd2_plant"].isna() | master["scfd3_plant"].isna()

    print(f"\n{'='*50}")
    print(f"  Written : {OUTPUT_FILE}")
    print(f"  Total rows           : {total}")
    print(f"  Needs review (<0.80) : {needs_review}")
    print(f"  One-sided (no pair)  : {no_match.sum()}")
    print(f"\nNext steps:")
    print(f"  1. Open {OUTPUT_FILE}")
    print(f"  2. Fix 'canonical_name' for low-confidence / one-sided rows")
    print(f"  3. Save and use as your master mapping in the dashboard")


if __name__ == "__main__":
    main()
