from typing import Optional, List


SCHEMA = "brewdat_uc_gbgai_dev.gai_copilot_procurement_brewgpt_ghq"


class CostPerHLKPIs:
    """Builds SQL for Cost Per Hectoliter analysis."""

    DELIMITER = "|||#$#|||"

    # -----------------------------------------------------
    # Helpers
    # -----------------------------------------------------
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
            flat = []
            for item in param:
                if item is None:
                    continue
                s = str(item).strip()
                if CostPerHLKPIs.DELIMITER in s:
                    flat.extend([v.strip() for v in s.split(CostPerHLKPIs.DELIMITER) if v.strip()])
                elif s:
                    flat.append(s)
            return flat if flat else None

        s = str(param).strip()
        if not s:
            return None

        if CostPerHLKPIs.DELIMITER in s:
            return [v.strip() for v in s.split(CostPerHLKPIs.DELIMITER) if v.strip()]

        return [s]

    @staticmethod
    def _in_clause(column: str, values: List[str]) -> str:
        escaped = ", ".join([CostPerHLKPIs._escape(v) for v in values])
        if len(values) == 1:
            return f"{column} = {CostPerHLKPIs._escape(values[0])}"
        return f"{column} IN ({escaped})"

    @staticmethod
    def _case_insensitive_in_clause(column: str, values: List[str]) -> str:
        if not values:
            return ""
        upper_values = [v.upper() for v in values]
        normal_in = ", ".join([CostPerHLKPIs._escape(v) for v in values])
        upper_in = ", ".join([CostPerHLKPIs._escape(v) for v in upper_values])
        if len(values) == 1:
            return f"({column} = {CostPerHLKPIs._escape(values[0])} OR UPPER({column}) = {CostPerHLKPIs._escape(upper_values[0])})"
        else:
            return f"({column} IN ({normal_in}) OR UPPER({column}) IN ({upper_in}))"

    # -----------------------------------------------------
    # Main Query Builder
    # -----------------------------------------------------
    def get_cost_per_hl(
        self,
        country=None,
        zone=None,
        plant_code=None,
        plant=None,
        year=None,
        month=None,
        scenario=None,
        brand=None,
        subbrand=None,
        pack_type=None,
        pack_size=None,
        package=None,
        subpackage=None,
        groupby_column=None,
        sortby_value=None,
    ) -> str:

        # Convert params to lists
        country = self._to_list(country)
        zone = self._to_list(zone)
        plant_code = self._to_list(plant_code)
        plant = self._to_list(plant)
        year = self._to_list(year)
        month = self._to_list(month)
        scenario = self._to_list(scenario)
        brand = self._to_list(brand)
        subbrand = self._to_list(subbrand)
        pack_type = self._to_list(pack_type)
        pack_size = self._to_list(pack_size)
        package = self._to_list(package)
        subpackage = self._to_list(subpackage)
        groupby_column = self._to_list(groupby_column)

        if not scenario:
            scenario = ["AC"]

        # -------------------------------------------------
        # Column Mapping
        # -------------------------------------------------
        column_mapping = {
            "country": "dl.country",
            "zone": "dl.zone",
            "plant_code": "dl.plant_code",
            "plant": "dl.plant",
            "year": "dt.year",
            "month": "dt.month",
            "brand": "ds.brand",
            "subbrand": "ds.subbrand",
            "pack_type": "ds.pack_type",
            "pack_size": "ds.pack_size",
            "package": "dm.package",       # material only
            "subpackage": "dm.subpackage", # material only
        }

        material_cols = {"dm.package", "dm.subpackage"}

        # -------------------------------------------------
        # Build SELECT / GROUP BY
        # -------------------------------------------------
        selected_cols = ["dt.year"]

        # Map params → column
        param_to_column = {
            "country": (country, "dl.country"),
            "zone": (zone, "dl.zone"),
            "plant_code": (plant_code, "dl.plant_code"),
            "plant": (plant, "dl.plant"),
            "month": (month, "dt.month"),
            "brand": (brand, "ds.brand"),
            "subbrand": (subbrand, "ds.subbrand"),
            "pack_type": (pack_type, "ds.pack_type"),
            "pack_size": (pack_size, "ds.pack_size"),
            "package": (package, "dm.package"),
            "subpackage": (subpackage, "dm.subpackage"),
        }

        # Add only provided parameters
        for _, (param_value, col_name) in param_to_column.items():
            if param_value and col_name not in selected_cols:
                selected_cols.append(col_name)

        # Add explicit groupby columns (if any)
        if groupby_column:
            for col in groupby_column:
                mapped = column_mapping.get(col.lower())
                if mapped and mapped not in selected_cols:
                    selected_cols.append(mapped)

        # Material columns exist only in cost table
        material_cols = {"dm.package", "dm.subpackage"}

        volume_cols = [c for c in selected_cols if c not in material_cols]
        cost_cols = selected_cols.copy()

        # Build SELECT
        volume_select = ["fv.scenario"] + volume_cols
        cost_select = ["fmu.scenario"] + cost_cols

        volume_group = volume_select.copy()
        cost_group = cost_select.copy()

        volume_select_str = ",\n        ".join(volume_select)
        cost_select_str = ",\n        ".join(cost_select)

        volume_group_str = ",\n        ".join(volume_group)
        cost_group_str = ",\n        ".join(cost_group)

                

        # -------------------------------------------------
        # WHERE builder
        # -------------------------------------------------
        def build_where(scenario_col: str, include_material_filters: bool):
            where_parts = []

            # detect if month is part of groupby
            groupby_has_month = (
                groupby_column
                and any("month" in g.lower() for g in groupby_column)
            )

            filter_map = [
                (country, "dl.country"),
                (zone, "dl.zone"),
                (plant_code, "dl.plant_code"),
                (plant, "dl.plant"),
                (year, "dt.year"),
                (scenario, scenario_col),
                (brand, "ds.brand"),
                (subbrand, "ds.subbrand"),
                (pack_type, "ds.pack_type"),
                (pack_size, "ds.pack_size"),
            ]

            if include_material_filters:
                # Only include material filters if values are provided (non-empty list)
                if package:
                    filter_map.append((package, "dm.package"))
                if subpackage:
                    filter_map.append((subpackage, "dm.subpackage"))

            for vals, col in filter_map:
                if vals:
                    where_parts.append(self._case_insensitive_in_clause(col, vals))
                    
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

            if not where_parts:
                return ""

            return "\nWHERE " + "\n    AND ".join(where_parts)

        volume_where = build_where("fv.scenario", include_material_filters=False)
        cost_where = build_where("fmu.scenario", include_material_filters=True)

        # -------------------------------------------------
        # Join keys (only common columns)
        # -------------------------------------------------
        volume_keys = [c.split(".")[-1] for c in volume_group]
        cost_keys = [c.split(".")[-1] for c in cost_group]

        join_keys = list(set(volume_keys).intersection(cost_keys))

        join_condition = " AND ".join([f"ca.{k} = va.{k}" for k in join_keys])

        # -------------------------------------------------
        # Final SQL
        # -------------------------------------------------
        query = f"""
        WITH volume_agg AS (
            SELECT
                {volume_select_str},
                SUM(fv.filling_volume_in_hl) AS volume
            FROM {SCHEMA}.FACT_VOLUME fv
            JOIN {SCHEMA}.DIM_TIME dt ON fv.time_key = dt.time_key
            JOIN {SCHEMA}.DIM_LOCATION dl ON fv.location_key = dl.location_key
            JOIN {SCHEMA}.DIM_SKU ds ON fv.sku_key = ds.sku_key
            {volume_where}
            GROUP BY
                {volume_group_str}
        ),

        cost_agg AS (
            SELECT
                {cost_select_str},
                SUM(fmu.cost_spend_in_usd) AS total_cost
            FROM {SCHEMA}.FACT_MATERIAL_USAGE fmu
            JOIN {SCHEMA}.DIM_TIME dt ON fmu.time_key = dt.time_key
            JOIN {SCHEMA}.DIM_LOCATION dl ON fmu.location_key = dl.location_key
            JOIN {SCHEMA}.DIM_SKU ds ON fmu.sku_key = ds.sku_key
            JOIN {SCHEMA}.DIM_MATERIAL dm ON fmu.material_key = dm.material_key
            {cost_where}
            GROUP BY
                {cost_group_str}
        ),

        vol_cost_agg AS (
            SELECT
                {", ".join([f"ca.{k}" for k in cost_keys])},
                va.volume,
                CASE WHEN va.volume > 0 THEN ca.total_cost / va.volume END AS cost_per_hl
            FROM cost_agg ca
            LEFT JOIN volume_agg va
                ON {join_condition}
        )

        SELECT *
        FROM vol_cost_agg
        LIMIT 1000;
        """

        return query.strip()


# Example usage
if __name__ == "__main__":
    handler = CostPerHLKPIs()

    q1 = handler.get_cost_per_hl(
        zone=["APAC"],
        # country=["China"],
        year=["2025"]
        # month=["Jan"],
        # groupby_column=["package"]
    )

    print(q1)
