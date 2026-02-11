"""Streamlit dashboard for San Diego traffic & transportation analysis."""

from __future__ import annotations

from pathlib import Path

import duckdb
import plotly.express as px
import streamlit as st

# ── Parquet paths ──
_AGG = "data/aggregated"
_root = Path(__file__).resolve().parent.parent
if (_root / _AGG).exists():
    _AGG = str(_root / _AGG)

st.set_page_config(
    page_title="San Diego Traffic & Transportation",
    page_icon="\U0001f68c",
    layout="wide",
)

CHART_COLOR = "#83c9ff"


def query(sql: str, params: list | None = None):
    """Run SQL against parquet files and return a pandas DataFrame."""
    con = duckdb.connect()
    return con.execute(sql, params or []).fetchdf()


# ── Sidebar filters ──
st.sidebar.title("Filters")


@st.cache_data(ttl=3600)
def _sidebar_options():
    # Year range across all datasets
    years = set()
    for pq in ["ridership_trends", "vmt_trends", "collision_severity",
                "city_collision_trends", "traffic_volume_trends"]:
        path = f"{_AGG}/{pq}.parquet"
        try:
            yrs = query(f"SELECT DISTINCT year FROM '{path}' WHERE year IS NOT NULL")["year"].tolist()
            years.update(yrs)
        except Exception:
            pass

    # Peak periods from travel times
    try:
        peaks = sorted(query(f"""
            SELECT DISTINCT peak FROM '{_AGG}/travel_time_trends.parquet'
            WHERE peak IS NOT NULL ORDER BY peak
        """)["peak"].tolist())
    except Exception:
        peaks = []

    # Collision severity from SWITRS
    try:
        severities = sorted(query(f"""
            SELECT DISTINCT collision_severity FROM '{_AGG}/collision_severity.parquet'
            WHERE collision_severity IS NOT NULL ORDER BY collision_severity
        """)["collision_severity"].tolist())
    except Exception:
        severities = []

    return sorted(years), peaks, severities


all_years, all_peaks, all_severities = _sidebar_options()

if all_years:
    year_range = st.sidebar.slider(
        "Year Range",
        min_value=int(min(all_years)),
        max_value=int(max(all_years)),
        value=(max(int(min(all_years)), 2019), int(max(all_years))),
    )
else:
    year_range = (2019, 2024)

peak_period = st.sidebar.selectbox(
    "Peak Period",
    options=["All"] + all_peaks,
    index=0,
    help="Filter congestion data by AM/PM peak. Applies to Congestion tab.",
)

collision_modes = st.sidebar.multiselect(
    "Collision Mode",
    options=["Bicycle", "Pedestrian", "Motorcycle"],
    default=None,
    placeholder="All modes",
    help="Filter collision data by involved mode. Applies to Safety and Map tabs.",
)

st.sidebar.caption(
    "**Year Range** applies to all tabs. **Peak Period** applies to Congestion. "
    "**Collision Mode** applies to Safety and Map tabs."
)


def _year_where(table_alias: str = "") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    return f"{prefix}year BETWEEN {year_range[0]} AND {year_range[1]}"


def _mode_where() -> str:
    """Build WHERE fragment for collision mode filters."""
    if not collision_modes:
        return ""
    clauses = []
    if "Bicycle" in collision_modes:
        clauses.append("is_bicycle = TRUE")
    if "Pedestrian" in collision_modes:
        clauses.append("is_pedestrian = TRUE")
    if "Motorcycle" in collision_modes:
        clauses.append("is_motorcycle = TRUE")
    return " AND (" + " OR ".join(clauses) + ")"


# ── Header ──
st.title("San Diego Traffic & Transportation")
st.markdown(
    "Explore traffic congestion, transit ridership recovery, collision safety, "
    "and new mobility programs across the San Diego region. Data from "
    "[SANDAG](https://opendata.sandag.org) and the "
    "[City of San Diego](https://data.sandiego.gov) open data portals."
)

# ==================================================================
# Tabs
# ==================================================================
tab_overview, tab_congestion, tab_transit, tab_safety, tab_map, tab_deep = st.tabs(
    ["Overview", "Congestion", "Transit", "Safety", "Collision Map", "Deep Dive"]
)

# ══════════════════════════════════════════════════════════════════
# TAB 1: Overview
# ══════════════════════════════════════════════════════════════════
with tab_overview:
    st.subheader("Key Indicators")

    # KPIs
    latest_ridership = query(f"""
        SELECT year, total_weekday_boardings FROM '{_AGG}/ridership_trends.parquet'
        WHERE {_year_where()} ORDER BY year DESC LIMIT 1
    """)
    ridership_2019 = query(f"""
        SELECT total_weekday_boardings FROM '{_AGG}/ridership_trends.parquet'
        WHERE year = 2019
    """)
    latest_vmt = query(f"""
        SELECT year, SUM(vmt) AS total_vmt FROM '{_AGG}/vmt_trends.parquet'
        WHERE {_year_where()} GROUP BY year ORDER BY year DESC LIMIT 1
    """)
    latest_collisions = query(f"""
        SELECT SUM(num_collisions) AS total FROM '{_AGG}/collision_severity.parquet'
        WHERE {_year_where()}
    """)
    latest_fatal = query(f"""
        SELECT SUM(num_collisions) AS total FROM '{_AGG}/collision_severity.parquet'
        WHERE {_year_where()} AND collision_severity = 'Fatal'
    """)

    c1, c2, c3, c4, c5 = st.columns(5)

    if not latest_ridership.empty:
        boardings = latest_ridership["total_weekday_boardings"].iloc[0]
        yr = int(latest_ridership["year"].iloc[0])
        c1.metric(f"Weekday Boardings ({yr})", f"{boardings:,.0f}")

        if not ridership_2019.empty:
            base = ridership_2019["total_weekday_boardings"].iloc[0]
            if base and base > 0:
                pct = (boardings - base) / base * 100
                c2.metric("vs 2019", f"{pct:+.1f}%")

    if not latest_vmt.empty:
        vmt_val = latest_vmt["total_vmt"].iloc[0]
        c3.metric("Total VMT", f"{vmt_val:,.0f}")

    if not latest_collisions.empty:
        c4.metric("Total Collisions", f"{int(latest_collisions['total'].iloc[0] or 0):,}")

    if not latest_fatal.empty:
        c5.metric("Fatal Collisions", f"{int(latest_fatal['total'].iloc[0] or 0):,}")

    # Ridership recovery line
    st.subheader("Transit Ridership Recovery")
    ridership_trend = query(f"""
        SELECT year, total_weekday_boardings AS "Weekday Boardings"
        FROM '{_AGG}/ridership_trends.parquet'
        WHERE {_year_where()} ORDER BY year
    """)
    if not ridership_trend.empty:
        ridership_trend["year"] = ridership_trend["year"].astype(str)
        st.line_chart(ridership_trend.set_index("year"), color=CHART_COLOR, y_label="Avg Weekday Boardings")

    # VMT trend
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Vehicle Miles Traveled")
        vmt_trend = query(f"""
            SELECT year, SUM(vmt) AS "Total VMT"
            FROM '{_AGG}/vmt_trends.parquet'
            WHERE {_year_where()} GROUP BY year ORDER BY year
        """)
        if not vmt_trend.empty:
            vmt_trend["year"] = vmt_trend["year"].astype(str)
            st.line_chart(vmt_trend.set_index("year"), color=CHART_COLOR)

    # Collision severity stacked area
    with col_r:
        st.subheader("Collision Severity Trends")
        sev_trend = query(f"""
            SELECT year, collision_severity, num_collisions
            FROM '{_AGG}/collision_severity.parquet'
            WHERE {_year_where()} ORDER BY year
        """)
        if not sev_trend.empty:
            pivot = sev_trend.pivot_table(
                index="year", columns="collision_severity", values="num_collisions", fill_value=0
            )
            pivot.index = pivot.index.astype(str)
            st.area_chart(pivot)

# ══════════════════════════════════════════════════════════════════
# TAB 2: Congestion
# ══════════════════════════════════════════════════════════════════
with tab_congestion:
    st.subheader("Congestion Analysis")

    peak_filter = ""
    if peak_period != "All":
        safe_peak = peak_period.replace("'", "''")
        peak_filter = f" AND peak = '{safe_peak}'"

    # KPIs
    worst_corridor = query(f"""
        SELECT route, mean_minutes FROM '{_AGG}/travel_time_trends.parquet'
        WHERE {_year_where()}{peak_filter}
        ORDER BY mean_minutes DESC LIMIT 1
    """)
    vmt_yoy = query(f"""
        SELECT year, SUM(vmt) AS total_vmt FROM '{_AGG}/vmt_trends.parquet'
        WHERE {_year_where()}{peak_filter}
        GROUP BY year ORDER BY year
    """)
    avg_travel = query(f"""
        SELECT AVG(mean_minutes) AS avg_min FROM '{_AGG}/travel_time_trends.parquet'
        WHERE {_year_where()}{peak_filter}
    """)

    c1, c2, c3 = st.columns(3)
    if not worst_corridor.empty:
        c1.metric("Worst Corridor", worst_corridor["route"].iloc[0],
                   f"{worst_corridor['mean_minutes'].iloc[0]:.0f} min")
    if not avg_travel.empty and avg_travel["avg_min"].iloc[0]:
        c2.metric("Avg Peak Travel Time", f"{avg_travel['avg_min'].iloc[0]:.1f} min")
    if len(vmt_yoy) >= 2:
        latest = vmt_yoy["total_vmt"].iloc[-1]
        prev = vmt_yoy["total_vmt"].iloc[-2]
        if prev and prev > 0:
            change = (latest - prev) / prev * 100
            c3.metric("YoY VMT Change", f"{change:+.1f}%")

    # Travel times by route
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Travel Times by Route")
        tt_routes = query(f"""
            SELECT route AS "Route", AVG(mean_minutes) AS "Avg Minutes"
            FROM '{_AGG}/travel_time_trends.parquet'
            WHERE {_year_where()}{peak_filter}
            GROUP BY route ORDER BY "Avg Minutes" DESC
        """)
        if not tt_routes.empty:
            st.bar_chart(tt_routes.set_index("Route"), horizontal=True, color=CHART_COLOR)

    with col_r:
        st.subheader("VMT by Freeway")
        vmt_fw = query(f"""
            SELECT freeway AS "Freeway", SUM(vmt) AS "Total VMT"
            FROM '{_AGG}/vmt_trends.parquet'
            WHERE {_year_where()}{peak_filter}
            GROUP BY freeway ORDER BY "Total VMT" DESC
        """)
        if not vmt_fw.empty:
            st.bar_chart(vmt_fw.set_index("Freeway"), horizontal=True, color=CHART_COLOR)

    # Travel time trends (top 5 worst)
    st.subheader("Travel Time Trends (Top 5 Worst Routes)")
    tt_trend = query(f"""
        WITH worst AS (
            SELECT route FROM '{_AGG}/travel_time_trends.parquet'
            WHERE {_year_where()}{peak_filter}
            GROUP BY route ORDER BY AVG(mean_minutes) DESC LIMIT 5
        )
        SELECT t.year, t.route, t.mean_minutes
        FROM '{_AGG}/travel_time_trends.parquet' t
        JOIN worst w ON t.route = w.route
        WHERE {_year_where('t')}{peak_filter.replace('peak', 't.peak') if peak_filter else ''}
        ORDER BY t.year
    """)
    if not tt_trend.empty:
        pivot = tt_trend.pivot_table(index="year", columns="route", values="mean_minutes", fill_value=0)
        pivot.index = pivot.index.astype(str)
        st.line_chart(pivot)

    # Street volume trends
    st.subheader("City Traffic Volume Trends")
    vol_trend = query(f"""
        SELECT year, avg_daily_traffic AS "Avg Daily Traffic"
        FROM '{_AGG}/traffic_volume_trends.parquet'
        WHERE {_year_where()} ORDER BY year
    """)
    if not vol_trend.empty:
        vol_trend["year"] = vol_trend["year"].astype(str)
        st.line_chart(vol_trend.set_index("year"), color=CHART_COLOR)

# ══════════════════════════════════════════════════════════════════
# TAB 3: Transit
# ══════════════════════════════════════════════════════════════════
with tab_transit:
    st.subheader("Transit Ridership & New Mobility")

    # KPIs
    route_count = query(f"""
        SELECT COUNT(DISTINCT route) AS n FROM '{_AGG}/ridership_by_route.parquet'
        WHERE {_year_where()}
    """)
    top_route = query(f"""
        SELECT route, SUM(avg_weekday_boardings) AS total
        FROM '{_AGG}/ridership_by_route.parquet'
        WHERE {_year_where()}
        GROUP BY route ORDER BY total DESC LIMIT 1
    """)
    yop_total = query(f"""
        SELECT SUM(total_rides) AS total FROM '{_AGG}/youth_pass_trends.parquet'
    """)
    flex_total = query(f"""
        SELECT SUM(total_value) AS total FROM '{_AGG}/flex_fleet_trends.parquet'
        WHERE category = 'Total Rides'
    """)

    c1, c2, c3, c4 = st.columns(4)
    if not route_count.empty:
        c1.metric("Routes Tracked", int(route_count["n"].iloc[0]))
    if not top_route.empty:
        c2.metric("Highest Ridership Route", top_route["route"].iloc[0])
    if not yop_total.empty and yop_total["total"].iloc[0]:
        c3.metric("YOP Total Rides", f"{yop_total['total'].iloc[0]:,.0f}")
    if not flex_total.empty and flex_total["total"].iloc[0]:
        c4.metric("Flex Fleet Rides", f"{flex_total['total'].iloc[0]:,.0f}")

    # Top 15 routes by boardings
    st.subheader("Top 15 Routes by Weekday Boardings")
    top_routes = query(f"""
        SELECT route AS "Route", SUM(avg_weekday_boardings) AS "Total Boardings"
        FROM '{_AGG}/ridership_by_route.parquet'
        WHERE {_year_where()}
        GROUP BY route ORDER BY "Total Boardings" DESC LIMIT 15
    """)
    if not top_routes.empty:
        st.bar_chart(top_routes.set_index("Route"), horizontal=True, color=CHART_COLOR)

    # Route recovery multi-line (top 10)
    st.subheader("Route Recovery Trends (Top 10)")
    recovery = query(f"""
        WITH top10 AS (
            SELECT route FROM '{_AGG}/ridership_by_route.parquet'
            GROUP BY route ORDER BY SUM(avg_weekday_boardings) DESC LIMIT 10
        )
        SELECT r.year, r.route, r.avg_weekday_boardings
        FROM '{_AGG}/ridership_by_route.parquet' r
        JOIN top10 t ON r.route = t.route
        ORDER BY r.year
    """)
    if not recovery.empty:
        pivot = recovery.pivot_table(index="year", columns="route", values="avg_weekday_boardings", fill_value=0)
        pivot.index = pivot.index.astype(str)
        st.line_chart(pivot)

    # Youth Opportunity Pass
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Youth Opportunity Pass — Monthly Trend")
        yop_trend = query(f"""
            SELECT month, total_rides AS "Total Rides"
            FROM '{_AGG}/youth_pass_trends.parquet'
            ORDER BY month
        """)
        if not yop_trend.empty:
            yop_trend["month"] = yop_trend["month"].astype(str).str[:7]
            st.line_chart(yop_trend.set_index("month"), color=CHART_COLOR, y_label="Rides")

    with col_r:
        st.subheader("Flexible Fleet by Location")
        flex_loc = query(f"""
            SELECT location_name AS "Location", SUM(total_value) AS "Total Rides"
            FROM '{_AGG}/flex_fleet_trends.parquet'
            WHERE category = 'Total Rides'
            GROUP BY location_name ORDER BY "Total Rides" DESC
        """)
        if not flex_loc.empty:
            st.bar_chart(flex_loc.set_index("Location"), horizontal=True, color=CHART_COLOR)

# ══════════════════════════════════════════════════════════════════
# TAB 4: Safety
# ══════════════════════════════════════════════════════════════════
with tab_safety:
    st.subheader("Collision Safety Trends")

    mode_filter = _mode_where()

    # KPIs
    fatal_total = query(f"""
        SELECT SUM(num_collisions) AS total FROM '{_AGG}/collision_severity.parquet'
        WHERE {_year_where()} AND collision_severity = 'Fatal'
    """)
    bike_ped = query(f"""
        SELECT COUNT(*) AS total FROM '{_AGG}/collision_by_type.parquet'
        WHERE {_year_where()} AND (is_bicycle = TRUE OR is_pedestrian = TRUE)
    """)
    # YoY fatality change
    fatal_yoy = query(f"""
        SELECT year, SUM(num_collisions) AS total FROM '{_AGG}/collision_severity.parquet'
        WHERE collision_severity = 'Fatal' AND {_year_where()}
        GROUP BY year ORDER BY year
    """)
    # Injury rate
    injury_rate = query(f"""
        SELECT
            SUM(CASE WHEN collision_severity = 'Injury (Severe)' OR collision_severity = 'Injury (Complaint of Pain)'
                      OR collision_severity = 'Injury (Other Visible)' THEN num_collisions ELSE 0 END) AS injuries,
            SUM(num_collisions) AS total
        FROM '{_AGG}/collision_severity.parquet'
        WHERE {_year_where()}
    """)

    c1, c2, c3, c4 = st.columns(4)
    if not fatal_total.empty:
        c1.metric("Fatal Collisions", f"{int(fatal_total['total'].iloc[0] or 0):,}")
    if not bike_ped.empty:
        c2.metric("Bike/Ped Collision Types", f"{int(bike_ped['total'].iloc[0] or 0):,}")
    if len(fatal_yoy) >= 2:
        latest_f = fatal_yoy["total"].iloc[-1]
        prev_f = fatal_yoy["total"].iloc[-2]
        if prev_f and prev_f > 0:
            change = (latest_f - prev_f) / prev_f * 100
            c3.metric("YoY Fatality Change", f"{change:+.1f}%")
    if not injury_rate.empty:
        inj = injury_rate["injuries"].iloc[0] or 0
        tot = injury_rate["total"].iloc[0] or 1
        c4.metric("Injury Rate", f"{inj / tot * 100:.0f}%")

    # Collision severity trend (stacked area)
    st.subheader("Collision Severity Trend (SWITRS 2006-2024)")
    sev = query(f"""
        SELECT year, collision_severity, num_collisions
        FROM '{_AGG}/collision_severity.parquet'
        ORDER BY year
    """)
    if not sev.empty:
        pivot = sev.pivot_table(index="year", columns="collision_severity", values="num_collisions", fill_value=0)
        pivot.index = pivot.index.astype(str)
        st.area_chart(pivot)

    # Bike/ped collision trends
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Bicycle & Pedestrian Collisions")
        bp_trend = query(f"""
            SELECT year,
                SUM(CASE WHEN is_bicycle THEN num_collisions ELSE 0 END) AS "Bicycle",
                SUM(CASE WHEN is_pedestrian THEN num_collisions ELSE 0 END) AS "Pedestrian"
            FROM '{_AGG}/collision_by_type.parquet'
            WHERE {_year_where()}
            GROUP BY year ORDER BY year
        """)
        if not bp_trend.empty:
            bp_trend["year"] = bp_trend["year"].astype(str)
            st.line_chart(bp_trend.set_index("year"))

    with col_r:
        st.subheader("Collisions by Type")
        by_type = query(f"""
            SELECT type_of_collision AS "Type", SUM(num_collisions) AS "Count"
            FROM '{_AGG}/collision_by_type.parquet'
            WHERE {_year_where()} AND type_of_collision IS NOT NULL{mode_filter}
            GROUP BY type_of_collision ORDER BY "Count" DESC LIMIT 10
        """)
        if not by_type.empty:
            st.bar_chart(by_type.set_index("Type"), horizontal=True, color=CHART_COLOR)

    # Weather and lighting breakdowns
    col_l2, col_r2 = st.columns(2)
    with col_l2:
        st.subheader("By Weather")
        by_weather = query(f"""
            SELECT weather AS "Weather", SUM(num_collisions) AS "Count"
            FROM '{_AGG}/collision_by_type.parquet'
            WHERE {_year_where()} AND weather IS NOT NULL{mode_filter}
            GROUP BY weather ORDER BY "Count" DESC LIMIT 8
        """)
        if not by_weather.empty:
            st.bar_chart(by_weather.set_index("Weather"), horizontal=True, color=CHART_COLOR)

    with col_r2:
        st.subheader("By Lighting")
        by_lighting = query(f"""
            SELECT lighting AS "Lighting", SUM(num_collisions) AS "Count"
            FROM '{_AGG}/collision_by_type.parquet'
            WHERE {_year_where()} AND lighting IS NOT NULL{mode_filter}
            GROUP BY lighting ORDER BY "Count" DESC LIMIT 8
        """)
        if not by_lighting.empty:
            st.bar_chart(by_lighting.set_index("Lighting"), horizontal=True, color=CHART_COLOR)

    # City collision overlay
    st.subheader("City of SD Reported Collisions")
    city_col = query(f"""
        SELECT year, num_collisions AS "Collisions", total_injured AS "Injured", total_killed AS "Killed"
        FROM '{_AGG}/city_collision_trends.parquet'
        WHERE {_year_where()} ORDER BY year
    """)
    if not city_col.empty:
        city_col["year"] = city_col["year"].astype(str)
        st.line_chart(city_col.set_index("year"))

# ══════════════════════════════════════════════════════════════════
# TAB 5: Collision Map
# ══════════════════════════════════════════════════════════════════
with tab_map:
    st.subheader("Collision Heatmap")

    map_severity = st.selectbox(
        "Severity Filter",
        options=["All"] + all_severities,
        key="map_severity",
    )
    sev_filter = ""
    if map_severity != "All":
        safe_sev = map_severity.replace("'", "''")
        sev_filter = f" AND collision_severity = '{safe_sev}'"

    mode_filter_map = _mode_where()

    map_data = query(f"""
        SELECT latitude AS lat, longitude AS lon
        FROM '{_AGG}/collision_map_points.parquet'
        WHERE {_year_where()}{sev_filter}{mode_filter_map}
    """)

    if map_data.empty:
        st.info("No collision points for the selected filters.")
    else:
        st.caption(f"{len(map_data):,} collision points")

        import pydeck as pdk

        layer = pdk.Layer(
            "HeatmapLayer",
            data=map_data,
            get_position=["lon", "lat"],
            radiusPixels=30,
            intensity=1,
            threshold=0.05,
        )
        view = pdk.ViewState(
            latitude=32.85,
            longitude=-117.15,
            zoom=10,
            pitch=0,
        )
        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=view,
            map_style="mapbox://styles/mapbox/dark-v10",
        ))

# ══════════════════════════════════════════════════════════════════
# TAB 6: Deep Dive
# ══════════════════════════════════════════════════════════════════
with tab_deep:
    # Route ridership lookup
    st.subheader("Route Ridership Lookup")
    all_routes = query(f"""
        SELECT DISTINCT route FROM '{_AGG}/ridership_by_route.parquet'
        ORDER BY route
    """)["route"].tolist()

    selected_route = st.selectbox("Select Route", options=all_routes, key="route_lookup")
    if selected_route:
        route_data = query(f"""
            SELECT year, avg_weekday_boardings AS "Avg Weekday Boardings"
            FROM '{_AGG}/ridership_by_route.parquet'
            WHERE route = $1
            ORDER BY year
        """, [selected_route])
        if not route_data.empty:
            route_data["year"] = route_data["year"].astype(str)
            st.line_chart(route_data.set_index("year"), color=CHART_COLOR)
        else:
            st.info(f"No data for route {selected_route}")

    # Top traffic volume streets
    st.subheader("Top Traffic Volume Streets")
    top_streets = query(f"""
        SELECT street_name AS "Street", limits AS "Limits",
               MAX(total_count) AS "Peak Daily Count", MAX(year) AS "Year"
        FROM '{_AGG}/traffic_volume_streets.parquet'
        WHERE {_year_where()}
        GROUP BY street_name, limits
        ORDER BY "Peak Daily Count" DESC
        LIMIT 25
    """)
    if not top_streets.empty:
        st.dataframe(top_streets, use_container_width=True, hide_index=True,
                      column_config={"Peak Daily Count": st.column_config.NumberColumn(format="%d")})

    # City collision by violation type
    st.subheader("City Collisions by Year")
    city_detail = query(f"""
        SELECT year, num_collisions AS "Collisions",
               total_injured AS "Injured", total_killed AS "Killed"
        FROM '{_AGG}/city_collision_trends.parquet'
        WHERE {_year_where()}
        ORDER BY year
    """)
    if not city_detail.empty:
        st.dataframe(city_detail, use_container_width=True, hide_index=True)

    # Data source documentation
    with st.expander("Data Sources"):
        st.markdown("""
**SANDAG Open Data** (opendata.sandag.org):
- Transit Ridership by Route (2019-2024)
- Vehicle Miles Traveled / PeMS (2013-2024)
- Highway Travel Times (2019-2024)
- SWITRS Collision Summary (2006-2024)
- SWITRS Detailed Collisions (2013-2022) — includes lat/lon for mapping
- Youth Opportunity Pass Ridership (Apr 2022-Sep 2025)
- Flexible Fleet / Microtransit (Jan 2022-Sep 2025)

**City of San Diego** (seshat.datasd.org):
- Traffic Volume Counts (2005-2022)
- Police-Reported Traffic Collisions (2015-2026)
- Transit Route Reference Table
""")
