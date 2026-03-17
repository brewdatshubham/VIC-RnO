from typing import Optional, List



SCHEMA = "brewdat_uc_gbgai_dev.gai_copilot_procurement_brewgpt_ghq"


class SpendKPIs:
    """Builds the SQL query for Spend analysis (port of get_spend stored procedure)."""

    DELIMITER = "|||#$#|||"

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
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
                if SpendKPIs.DELIMITER in s:
                    flat.extend([v.strip() for v in s.split(SpendKPIs.DELIMITER) if v.strip()])
                elif s:
                    flat.append(s)
            return flat if flat else None

        s = str(param).strip()
        if not s:
            return None

        if SpendKPIs.DELIMITER in s:
            return [v.strip() for v in s.split(SpendKPIs.DELIMITER) if v.strip()]

        return [s]

    @staticmethod
    def _in_clause(column: str, values: List[str]) -> str:
        escaped = ", ".join([SpendKPIs._escape(v) for v in values])
        if len(values) == 1:
            return f"{column} = {SpendKPIs._escape(values[0])}"
        return f"{column} IN ({escaped})"

    @staticmethod
    def _case_insensitive_in_clause(column: str, values: List[str]) -> str:
        if not values:
            return ""
        upper_values = [v.upper() for v in values]
        normal_in = ", ".join([SpendKPIs._escape(v) for v in values])
        upper_in = ", ".join([SpendKPIs._escape(v) for v in upper_values])
        if len(values) == 1:
            return f"({column} = {SpendKPIs._escape(values[0])} OR UPPER({column}) = {SpendKPIs._escape(upper_values[0])})"
        else:
            return f"({column} IN ({normal_in}) OR UPPER({column}) IN ({upper_in}))"

    # ------------------------------------------------------------------
    # public entry point
    # ------------------------------------------------------------------
    def get_spend(
        self,
        year=None,
        month=None,
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
        beverage_category=None,
        groupby_column=None,
        sortby_value=None,
        period_type=None,
    ) -> str:

        # Convert to lists
        year = self._to_list(year)
        month = self._to_list(month)
        zone = self._to_list(zone)
        country = self._to_list(country)
        plant_type = self._to_list(plant_type)
        plant_code = self._to_list(plant_code)
        plant = self._to_list(plant)
        pnl_code = self._to_list(pnl_code)
        kpi_category = self._to_list(kpi_category)
        kpi_name_account_2 = self._to_list(kpi_name_account_2)
        package = self._to_list(package)
        subpackage_group = self._to_list(subpackage_group)
        subpackage_lowest_level = self._to_list(subpackage_lowest_level)
        beverage_category = self._to_list(beverage_category)
        groupby_column = self._to_list(groupby_column)
        sortby_value_list = self._to_list(sortby_value)
        period_type_list = self._to_list(period_type)

        # Handle country normalization
        if country and len(country) == 1:
            country_val = country[0].upper()
            if country_val not in ["US"]:
                if "USA" in country_val or "UNITED STATES" in country_val:
                    country = ["US"]

        # If package has value → ensure subpackage_group in groupby
        if package:
            if not groupby_column:
                groupby_column = ["subpackage_group"]
            else:
                has_subpackage = any("subpackage_group" in col.lower() for col in groupby_column)
                if not has_subpackage:
                    groupby_column.append("subpackage_group")

        # Default period type
        determined_period_type = "MTH"
        if period_type_list:
            determined_period_type = period_type_list[0]

        # Default year
        if not year:
            year = ["2025"]

        # Validate sort direction
        sort_direction = "DESC"
        if sortby_value_list and sortby_value_list[0].upper() in ("ASC", "DESC"):
            sort_direction = sortby_value_list[0].upper()

        # ---- Build FROM / JOINs ----
        base_query = (
            f"FROM {SCHEMA}.FACT_VIC_PRICE_PERFORMANCE fvpp\n"
            f"LEFT JOIN {SCHEMA}.DIM_TIME dt ON fvpp.time_key = dt.time_key\n"
            f"LEFT JOIN {SCHEMA}.DIM_LOCATION dl ON fvpp.location_key = dl.location_key\n"
            f"LEFT JOIN {SCHEMA}.DIM_VIC_PRICE_PERFORMANCE dal ON fvpp.account_level_key = dal.account_level_key\n"
            f"LEFT JOIN {SCHEMA}.DIM_RATE dr ON fvpp.rate_key = dr.rate_key"
        )

        # ---- Build WHERE clause ----
        where_parts: List[str] = []

        # Check if month is part of groupby
        groupby_has_month = False
        if groupby_column:
            groupby_has_month = any(col.strip().lower() == "month" for col in groupby_column)

        # Custom month filtering (case-insensitive)
        if month and not groupby_has_month:
            upper_months = [m.upper() for m in month]

            if len(month) == 1:
                where_parts.append(
                    f"(dt.month = {self._escape(month[0])} "
                    f"OR UPPER(dt.month) = {self._escape(upper_months[0])})"
                )
            else:
                normal_in = ", ".join([self._escape(m) for m in month])
                upper_in = ", ".join([self._escape(m) for m in upper_months])

                where_parts.append(
                    f"(dt.month IN ({normal_in}) "
                    f"OR UPPER(dt.month) IN ({upper_in}))"
                )

        # Other filters (month removed from here)
        filter_map = [
            (year, "dt.year"),
            (zone, "dl.zone"),
            (country, "dl.country"),
            (plant_type, "dl.plant_type"),
            (plant_code, "dl.plant_code"),
            (plant, "dl.plant"),
            (pnl_code, "dal.pnl_code"),
            (kpi_category, "dal.kpi_category"),
            (kpi_name_account_2, "dal.kpi_name_account_2"),
            (package, "dal.package"),
            (subpackage_group, "dal.subpackage_group"),
            (subpackage_lowest_level, "dal.subpackage_lowest_level"),
            (beverage_category, "fvpp.beverage_category"),
        ]

        for vals, col in filter_map:
            if vals:
                where_parts.append(self._case_insensitive_in_clause(col, vals))

        where_clause = ""
        if where_parts:
            where_clause = "\nWHERE " + "\n    AND ".join(where_parts)

        # ---- Build SELECT columns ----
        select_cols: List[str] = []

        if year:
            select_cols.append("dt.year")
        if month:
            select_cols.append("dt.month")
        if zone:
            select_cols.append("dl.zone")
        if country:
            select_cols.append("dl.country")
        if plant_type:
            select_cols.append("dl.plant_type")
        if plant_code:
            select_cols.append("dl.plant_code")
        if plant:
            select_cols.append("dl.plant")
        if pnl_code:
            select_cols.append("dal.pnl_code")
        if kpi_category:
            select_cols.append("dal.kpi_category")
        if kpi_name_account_2:
            select_cols.append("dal.kpi_name_account_2")
        if package:
            select_cols.append("dal.package")
        if subpackage_group:
            select_cols.append("dal.subpackage_group")
        if subpackage_lowest_level:
            select_cols.append("dal.subpackage_lowest_level")
        if beverage_category:
            select_cols.append("fvpp.beverage_category")

        column_mapping = {
            "year": "dt.year",
            "month": "dt.month",
            "zone": "dl.zone",
            "country": "dl.country",
            "plant_type": "dl.plant_type",
            "plant_code": "dl.plant_code",
            "plant": "dl.plant",
            "pnl_code": "dal.pnl_code",
            "kpi_category": "dal.kpi_category",
            "kpi_name_account_2": "dal.kpi_name_account_2",
            "package": "dal.package",
            "subpackage_group": "dal.subpackage_group",
            "subpackage_lowest_level": "dal.subpackage_lowest_level",
            "beverage_category": "fvpp.beverage_category",
        }

        if groupby_column:
            for col in groupby_column:
                mapped = column_mapping.get(col.strip().lower())
                if mapped and mapped not in select_cols:
                    select_cols.append(mapped)

        # Aggregate columns
        select_cols.extend([
            "SUM(fvpp.mtd_bu_usd)/1000.0 AS budgeted_spend",
            "SUM(fvpp.mtd_ac_usd)/1000.0 AS actual_spend"
        ])

        # ---- GROUP BY ----
        group_by_cols = [col for col in select_cols if not col.startswith("SUM(")]
        group_by_clause = ""
        if group_by_cols:
            group_by_clause = f"\nGROUP BY " + ", ".join(group_by_cols)

        # ---- ORDER BY ----
        order_clause = f"\nORDER BY budgeted_spend {sort_direction}"

        # ---- Final query ----
        select_str = ",\n    ".join(select_cols)

        query = (
            f"SELECT\n    {select_str}\n"
            f"{base_query}"
            f"{where_clause}"
            f"{group_by_clause}"
            f"{order_clause}\n"
            f"LIMIT 1000"
        )

        return query

if __name__ == "__main__":
    handler = SpendKPIs()

    q = handler.get_spend(
        country=["china"],
        year=["2025"],
        groupby_column=["country", "beverage_category"],
        sortby_value=["DESC"]
    )

    print(q)