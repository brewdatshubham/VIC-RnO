from typing import Optional, List


SCHEMA = "brewdat_uc_gbgai_dev.gai_copilot_procurement_brewgpt_ghq"

COLUMN_TABLE_MAP = {
    "line": "dl.plant_lines",
    "lines": "dl.plant_lines",
    "plant_line": "dl.plant_lines",
    "plant_lines": "dl.plant_lines",
    "zone": "dl.zone",
    "country": "dl.country",
    "plant": "dl.plant",
    "plant_type": "dl.plant_type",
    "country_group": "dl.country_group",
    "plant_group": "dl.plant_group",
    "scenario": "fbk.scenario",
    "business_area": "fbk.business_area",
    "kpi_code": "dbk.kpi_code",
    "kpi_name": "dbk.kpi_name",
    "kpi_classification": "dbk.kpi_classification",
    "kpi_group": "dbk.kpi_group",
    "kpi_definition": "dbk.kpi_definition",
    "kpi_owner": "dbk.kpi_owner",
    "formula": "dbk.formula",
    "year": "dt.year",
    "month": "dt.month",
}

LOCATION_HIERARCHY = ["line", "plant", "plant_group", "country_group", "country", "zone"]
LOCATION_TYPE_FOR_PARAM = {
    "line": "LINE",
    "plant": "PLANT",
    "plant_group": "PLANT_GROUP",
    "country_group": "COUNTRY_GROUP",
    "country": "COUNTRY",
    "zone": "ZONE",
}
GROUPBY_LOCATION_KEYWORDS = {
    "line": "LINE", "lines": "LINE", "plant_line": "LINE", "plant_lines": "LINE",
    "plant": "PLANT",
    "country": "COUNTRY",
    "zone": "ZONE",
}


class BeerometerKPIs:
    """Builds the SQL query for Beerometer KPI analysis (port of get_beerometer_kpis stored procedure)."""

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
                if BeerometerKPIs.DELIMITER in s:
                    flat.extend([v.strip() for v in s.split(BeerometerKPIs.DELIMITER) if v.strip()])
                elif s:
                    flat.append(s)
            return flat if flat else None
        s = str(param).strip()
        if not s:
            return None
        if BeerometerKPIs.DELIMITER in s:
            return [v.strip() for v in s.split(BeerometerKPIs.DELIMITER) if v.strip()]
        return [s]

    @staticmethod
    def _in_clause(column: str, values: List[str]) -> str:
        escaped = ", ".join([BeerometerKPIs._escape(v) for v in values])
        if len(values) == 1:
            return f"{column} = {BeerometerKPIs._escape(values[0])}"
        return f"{column} IN ({escaped})"

    @staticmethod
    def _case_insensitive_in_clause(column: str, values: List[str]) -> str:
        if not values:
            return ""
        upper_values = [v.upper() for v in values]
        normal_in = ", ".join([BeerometerKPIs._escape(v) for v in values])
        upper_in = ", ".join([BeerometerKPIs._escape(v) for v in upper_values])
        if len(values) == 1:
            return f"({column} = {BeerometerKPIs._escape(values[0])} OR UPPER({column}) = {BeerometerKPIs._escape(upper_values[0])})"
        else:
            return f"({column} IN ({normal_in}) OR UPPER({column}) IN ({upper_in}))"

    # ------------------------------------------------------------------
    # location type determination
    # ------------------------------------------------------------------
    @staticmethod
    def _determine_location_type(
        location_type, groupby_column,
        line, plant, plant_group, country_group, country, zone,
    ) -> str:
        param_location = ""
        for param_name, param_val in [
            ("line", line), ("plant", plant), ("plant_group", plant_group),
            ("country_group", country_group), ("country", country), ("zone", zone),
        ]:
            if param_val:
                param_location = LOCATION_TYPE_FOR_PARAM[param_name]
                break
        if not param_location:
            param_location = "GLOBAL"

        groupby_location = ""
        if groupby_column:
            for col in groupby_column:
                col_lower = col.lower()
                for keyword, loc_type in GROUPBY_LOCATION_KEYWORDS.items():
                    if keyword in col_lower:
                        groupby_location = loc_type
                        break
                if groupby_location:
                    break

        if location_type:
            return location_type[0] if isinstance(location_type, list) else str(location_type)
        if groupby_location:
            return groupby_location
        if param_location:
            return param_location
        return "GLOBAL"

    # ------------------------------------------------------------------
    # public entry point
    # ------------------------------------------------------------------
    def get_beerometer_kpis(
        self,
        scenario=None,
        zone=None,
        country=None,
        country_group=None,
        plant=None,
        plant_group=None,
        line=None,
        business_area=None,
        color=None,
        kpi_code=None,
        kpi_name=None,
        kpi_classification=None,
        kpi_group=None,
        month=None,
        year=None,
        period_type=None,
        uom=None,
        groupby_column=None,
        sortby_value=None,
        location_type=None,
        plant_type=None,
        kpi_owner=None,
        kpi_definition=None,
        formula=None,
    ) -> str:
        scenario = self._to_list(scenario)
        zone = self._to_list(zone)
        country = self._to_list(country)
        country_group = self._to_list(country_group)
        plant = self._to_list(plant)
        plant_group = self._to_list(plant_group)
        line = self._to_list(line)
        business_area = self._to_list(business_area)
        color = self._to_list(color)
        kpi_code = self._to_list(kpi_code)
        kpi_name = self._to_list(kpi_name)
        kpi_classification = self._to_list(kpi_classification)
        kpi_group = self._to_list(kpi_group)
        month = self._to_list(month)
        year = self._to_list(year)
        period_type_list = self._to_list(period_type)
        uom = self._to_list(uom)
        groupby_column = self._to_list(groupby_column)
        sortby_value_list = self._to_list(sortby_value)
        location_type = self._to_list(location_type)
        plant_type = self._to_list(plant_type)
        kpi_owner = self._to_list(kpi_owner)
        kpi_definition = self._to_list(kpi_definition)
        formula = self._to_list(formula)


        params = locals().copy()
        # Belgium case-insensitive handling
        if country and len(country) == 1 and country[0].upper() == "BELGIUM":
            country = ["belgium_PR", "belgium"]

        # Default scenario
        scenario = ["BU", "AC"]

        # Default period type
        determined_period_type = "MTH"
        if period_type_list:
            determined_period_type = period_type_list[0]

        determined_month = month

        # Determine location type
        determined_location_type = self._determine_location_type(
            location_type, groupby_column, line, plant, plant_group, country_group, country, zone
        )

        # ---- Build FROM / JOINs ----
        base_query = (
            f"FROM {SCHEMA}.FACT_BEEROMETER_KPI fbk\n"
            f"INNER JOIN {SCHEMA}.DIM_TIME dt ON fbk.time_key = dt.time_key\n"
            f"INNER JOIN {SCHEMA}.DIM_LOCATION dl ON fbk.location_key = dl.location_key\n"
            f"INNER JOIN {SCHEMA}.DIM_BEEROMETER_KPI dbk ON fbk.beerometer_kpi_key = dbk.beerometer_kpi_key"
        )

        # ---- Build WHERE ----
        where_parts: List[str] = [
            f"fbk.period_type = {self._escape(determined_period_type)}",
            f"fbk.location_type = {self._escape(determined_location_type)}",
        ]

        filter_map = [
            (scenario, "fbk.scenario"),
            (zone, "dl.zone"),
            (country, "dl.country"),
            (plant, "dl.plant"),
            (line, "dl.plant_lines"),
            (business_area, "fbk.business_area"),
            (kpi_code, "dbk.kpi_code"),
            (kpi_name, "dbk.kpi_name"),
            (kpi_classification, "dbk.kpi_classification"),
            (kpi_group, "dbk.kpi_group"),
            (kpi_owner, "dbk.kpi_owner"),
            (kpi_definition, "dbk.kpi_definition"),
            (formula, "dbk.formula"),
            (plant_type, "dl.plant_type"),
            (year, "dt.year"),
        ]

        for vals, col in filter_map:
            if vals:
                where_parts.append(self._case_insensitive_in_clause(col, vals))

        # Month filter — skip if grouping by month
        groupby_has_month = groupby_column and any("month" in g.lower() for g in groupby_column)
        if determined_month and not groupby_has_month:
            upper_months = [m.upper() for m in determined_month]
            if len(determined_month) == 1:
                where_parts.append(
                    f"(dt.month = {self._escape(determined_month[0])} OR UPPER(dt.month) = {self._escape(upper_months[0])})"
                )
            else:
                normal_in = ", ".join([self._escape(m) for m in determined_month])
                upper_in = ", ".join([self._escape(m) for m in upper_months])
                where_parts.append(
                    f"(dt.month IN ({normal_in}) OR UPPER(dt.month) IN ({upper_in}))"
                )

        where_clause = "\nWHERE " + "\n    AND ".join(where_parts)

        # ---- Build SELECT columns ----
        select_cols: List[str] = [
            "dt.year",
            "dt.month",
            "fbk.period_type",
            "fbk.location_type",
        ]

        def _add_if_absent(col: str):
            if col not in select_cols:
                select_cols.append(col)

        # Add location columns based on filter params
        if zone:
            _add_if_absent("dl.zone")
        if country:
            _add_if_absent("dl.country")
        if plant:
            _add_if_absent("dl.plant")
        if line:
            _add_if_absent("dl.plant_lines")
        if plant_type:
            _add_if_absent("dl.plant_type")

        # Add location columns based on determined location_type
        if determined_location_type == "LINE":
            _add_if_absent("dl.plant")
            _add_if_absent("dl.plant_lines")
        elif determined_location_type == "PLANT":
            _add_if_absent("dl.plant")
        elif determined_location_type == "COUNTRY":
            _add_if_absent("dl.country")
        elif determined_location_type == "ZONE":
            _add_if_absent("dl.zone")

        # KPI columns (always present)
        select_cols.extend([
            "fbk.scenario",
            "dbk.kpi_name",
            "dbk.kpi_owner",
            "dbk.kpi_definition",
            "dbk.formula",
        ])

        # Optional KPI columns
        if business_area:
            _add_if_absent("fbk.business_area")
        if kpi_classification:
            _add_if_absent("dbk.kpi_classification")
        if kpi_group:
            _add_if_absent("dbk.kpi_group")
        if kpi_code:
            _add_if_absent("dbk.kpi_code")

        # Handle groupby_column additions
        if groupby_column:
            for col in groupby_column:
                col_lower = col.strip().lower()
                mapped = COLUMN_TABLE_MAP.get(col_lower)
                if mapped:
                    _add_if_absent(mapped)

        # kpi_value always last
        select_cols.append("fbk.kpi_value")

        # ---- Sort ----
        sort_dir = "DESC"
        if sortby_value_list and sortby_value_list[0].upper() in ("ASC", "DESC"):
            sort_dir = sortby_value_list[0].upper()

        order_clause = f"ORDER BY dt.year ASC, fbk.kpi_value {sort_dir}"

        select_str = ",\n    ".join(select_cols)
        query = f"SELECT {select_str}\n{base_query}{where_clause}\n{order_clause}\nLIMIT 1000"

        return query


if __name__ == "__main__":
    handler = BeerometerKPIs()
    q = handler.get_beerometer_kpis(
        zone=["APAC"],
        kpi_name=["GLY"],
        year=["2025"],
        groupby_column=["country"]
        # month=["Jan"]
    )
    print(q)
