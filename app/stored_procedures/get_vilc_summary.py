from typing import Optional, List



SCHEMA = "brewdat_uc_gbgai_dev.gai_copilot_procurement_brewgpt_ghq"
DELIMITER = "|||#$#|||"

COLUMN_TABLE_MAP = {
    "zone": "dl.zone",
    "country": "dl.country",
    "beverage_category": "fvpp.beverage_category",
    "year": "dt.year",
    "month": "dt.month",
    "package": "dal.package",
    "subpackage_group": "dal.subpackage_group",
    "subpackage_lowest_level": "dal.subpackage_lowest_level",
    "pnl_code": "dal.pnl_code",
    "kpi_category": "dal.kpi_category",
    "kpi_name_account_2": "dal.kpi_name_account_2",
    "plant_type": "dl.plant_type",
    "plant_code": "dl.plant_code",
    "plant": "dl.plant",
}

class VilcSummary:
    """Builds the SQL query for VILC Summary (port of get_vilc_summary stored procedure)."""

    @staticmethod
    def _escape(value: str) -> str:
        if value is None:
            return "NULL"
        return "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def _to_list(param) -> Optional[List[str]]:
        if param is None:
            return None
        if isinstance(param, list):
            flat: List[str] = []
            for item in param:
                if item is None:
                    continue
                s = str(item).strip()
                if DELIMITER in s:
                    flat.extend([v.strip() for v in s.split(DELIMITER) if v.strip()])
                elif s:
                    flat.append(s)
            return flat if flat else None
        s = str(param).strip()
        if not s or s == "NULL":
            return None
        if s.startswith("'") and s.endswith("'"):
            s = s[1:-1]
        if DELIMITER in s:
            return [v.strip() for v in s.split(DELIMITER) if v.strip()]
        return [s] if s else None

    @staticmethod
    def _in_clause(column: str, values: List[str]) -> str:
        escaped = ", ".join([VilcSummary._escape(v) for v in values])
        if len(values) == 1:
            return f"{column} = {VilcSummary._escape(values[0])}"
        return f"{column} IN ({escaped})"

    @staticmethod
    def _case_insensitive_in_clause(column: str, values: List[str]) -> str:
        if not values:
            return ""
        upper_values = [v.upper() for v in values]
        normal_in = ", ".join([VilcSummary._escape(v) for v in values])
        upper_in = ", ".join([VilcSummary._escape(v) for v in upper_values])
        if len(values) == 1:
            return f"({column} = {VilcSummary._escape(values[0])} OR UPPER({column}) = {VilcSummary._escape(upper_values[0])})"
        else:
            return f"({column} IN ({normal_in}) OR UPPER({column}) IN ({upper_in}))"

    def get_vilc_summary(
        self,
        year=None,
        month=None,
        period_type=None,
        zone=None,
        country=None,
        plant_type=None,
        plant_code=None,
        plant=None,
        pnl_code=None,
        kpi_category=None,
        kpi_name_account_2=None,
        package=None,
        subpackage_group=None,
        subpackage_lowest_level=None,
        groupby_column=None,
        beverage_category=None,
        
    ) -> str:
        zone = self._to_list(zone)
        country = self._to_list(country)
        beverage_category = self._to_list(beverage_category)
        year = self._to_list(year)
        month = self._to_list(month)
        period_type_list = self._to_list(period_type)
        package = self._to_list(package)
        subpackage_group = self._to_list(subpackage_group)
        subpackage_lowest_level = self._to_list(subpackage_lowest_level)
        pnl_code = self._to_list(pnl_code)
        kpi_category = self._to_list(kpi_category)
        kpi_name_account_2 = self._to_list(kpi_name_account_2)
        plant_type = self._to_list(plant_type)
        plant_code = self._to_list(plant_code)
        plant = self._to_list(plant)
        groupby_column = self._to_list(groupby_column)

        period_type_val = period_type_list[0].upper() if period_type_list else ""

        # Aggregated columns based on period type (COALESCE is Databricks equivalent of ISNULL)
        if period_type_val == "YTD":
            agg_columns = (
                "ROUND(SUM(COALESCE(fvpp.ytd_price_usd, 0)) / 1000.0, 2) AS price,\n"
                "        ROUND(SUM(COALESCE(fvpp.ytd_perf_usd, 0)) / 1000.0, 2) AS performance,\n"
                "        ROUND(SUM(COALESCE(fvpp.ytd_price_usd, 0) + COALESCE(fvpp.ytd_perf_usd, 0)) / 1000.0, 2) AS price_and_performance"
            )
            order_expr = "SUM(COALESCE(fvpp.ytd_price_and_performance_usd, 0)) DESC"
        else:
            agg_columns = (
                "ROUND(SUM(COALESCE(fvpp.mtd_price_usd, 0)) / 1000.0, 2) AS price,\n"
                "        ROUND(SUM(COALESCE(fvpp.mtd_perf_usd, 0)) / 1000.0, 2) AS performance,\n"
                "        ROUND(SUM(COALESCE(fvpp.mtd_price_usd, 0) + COALESCE(fvpp.mtd_perf_usd, 0)) / 1000.0, 2) AS price_and_performance"
            )
            order_expr = "SUM(COALESCE(fvpp.mtd_price_usd, 0) + COALESCE(fvpp.mtd_perf_usd, 0)) DESC"

        # ---- Build WHERE conditions ----
        where_parts: List[str] = []

        filter_map = [
            (zone, "dl.zone"),
            (country, "dl.country"),
            (year, "dt.year"),
            (month, "dt.month"),
            (package, "dal.package"),
            (subpackage_group, "dal.subpackage_group"),
            (subpackage_lowest_level, "dal.subpackage_lowest_level"),
            (pnl_code, "dal.pnl_code"),
            (kpi_category, "dal.kpi_category"),
            (kpi_name_account_2, "dal.kpi_name_account_2"),
            (plant_type, "dl.plant_type"),
            (plant_code, "dl.plant_code"),
            (plant, "dl.plant"),
            (beverage_category, "fvpp.beverage_category"),
        ]

        for vals, col in filter_map:
            if vals:
                where_parts.append(self._case_insensitive_in_clause(col, vals))

        where_clause = "\n    WHERE " + "\n        AND ".join(where_parts)

        # ---- Build SELECT / GROUP BY columns ----
        select_cols: List[str] = []
        group_cols: List[str] = []

        def _add_col(col_ref: str):
            if col_ref not in select_cols:
                select_cols.append(col_ref)
            if col_ref not in group_cols:
                group_cols.append(col_ref)

        param_col_pairs = [
            (zone, "dl.zone"),
            (country, ["dl.country", "fvpp.beverage_category"]),
            (year, "dt.year"),
            (month, "dt.month"),
            (package, "dal.package"),
            (subpackage_group, "dal.subpackage_group"),
            (subpackage_lowest_level, "dal.subpackage_lowest_level"),
            (pnl_code, "dal.pnl_code"),
            (kpi_category, "dal.kpi_category"),
            (kpi_name_account_2, "dal.kpi_name_account_2"),
            (plant_type, "dl.plant_type"),
            (plant_code, "dl.plant_code"),
            (plant, "dl.plant"),
        ]

        for param_val, cols in param_col_pairs:
            if param_val:
                if isinstance(cols, list):
                    for c in cols:
                        _add_col(c)
                else:
                    _add_col(cols)

        # Handle groupby_col additions
        if groupby_column:
            for col in groupby_column:
                col_lower = col.strip().lower()
                mapped = COLUMN_TABLE_MAP.get(col_lower)
                if mapped:
                    _add_col(mapped)
                    if col_lower == "country" and "fvpp.beverage_category" not in select_cols:
                        _add_col("fvpp.beverage_category")

        # ---- Build FROM / JOINs ----
        from_clause = (
            f"FROM {SCHEMA}.FACT_VIC_PRICE_PERFORMANCE fvpp\n"
            f"    LEFT JOIN {SCHEMA}.DIM_TIME dt ON fvpp.time_key = dt.time_key\n"
            f"    LEFT JOIN {SCHEMA}.DIM_LOCATION dl ON fvpp.location_key = dl.location_key\n"
            f"    LEFT JOIN {SCHEMA}.DIM_VIC_PRICE_PERFORMANCE dal ON fvpp.account_level_key = dal.account_level_key\n"
            f"    LEFT JOIN {SCHEMA}.DIM_RATE dr ON fvpp.rate_key = dr.rate_key"
        )

        # ---- Assemble query ----
        if select_cols:
            select_str = ", ".join(select_cols)
            select_clause = f"SELECT {select_str},\n        {agg_columns}"
            group_by_clause = f"\n    GROUP BY {', '.join(group_cols)}"
        else:
            select_clause = f"SELECT {agg_columns}"
            group_by_clause = ""

        order_clause = f"\n    ORDER BY {order_expr}"

        query = f"{select_clause}\n    {from_clause}{where_clause}{group_by_clause}{order_clause}\n    LIMIT 1000"

        return query


if __name__ == "__main__":
    handler = VilcSummary()
    q = handler.get_vilc_summary(
        zone=["APAC","EUR","NAZ"],
        year=["2025","2025"],
        month=["Jan","Feb","Mar"],
        pnl_code=["VIC", "VLC"],
        period_type="YTD",
        # groupby_col=["subpackage_lowest_level"]
    )
    print(q)
