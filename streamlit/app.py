"""
NZ Electricity Generation Dashboard

5-page Streamlit app connecting to Snowflake mart tables.
Connects via snowflake-connector-python with reader role + dashboard warehouse.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector

# ──────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────

st.set_page_config(
    page_title="NZ Electricity Generation",
    page_icon="⚡",
    layout="wide",
)

# ──────────────────────────────────────────────
# Snowflake connection
# ──────────────────────────────────────────────

def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        database=st.secrets["snowflake"]["database"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        role=st.secrets["snowflake"]["role"],
        schema="ANALYTICS",
    )


@st.cache_data(ttl=86400)
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


# ──────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────

@st.cache_data(ttl=86400)
def load_monthly():
    return run_query("""
        select year_month, fuel_type,
               total_generation_kwh, total_generation_gwh,
               generator_count, active_days
        from analytics.mart_generation_monthly
        order by year_month, fuel_type
    """)


@st.cache_data(ttl=86400)
def load_daily():
    return run_query("""
        select trading_date, fuel_type,
               total_generation_kwh, total_generation_gwh,
               generator_count
        from analytics.mart_generation_daily
        order by trading_date, fuel_type
    """)


@st.cache_data(ttl=86400)
def load_renewable():
    return run_query("""
        select year_month, total_gwh, renewable_gwh, renewable_pct
        from analytics.mart_renewable_ratio
        order by year_month
    """)


@st.cache_data(ttl=86400)
def load_ranking():
    return run_query("""
        select year_month, site_code,
               total_generation_gwh, monthly_rank, primary_fuel_type
        from analytics.mart_plant_ranking
        order by year_month desc, monthly_rank
    """)


@st.cache_data(ttl=86400)
def load_seasonal():
    return run_query("""
        select season_year, season, fuel_type,
               total_generation_gwh, avg_generation_gwh
        from analytics.mart_seasonal_pattern
        order by season_year, season, fuel_type
    """)


# ──────────────────────────────────────────────
# Sidebar navigation
# ──────────────────────────────────────────────

page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Fuel Trends", "Plant Ranking", "Renewable Share", "Seasonal Analysis"],
)

# ──────────────────────────────────────────────
# Page 1: Overview
# ──────────────────────────────────────────────

if page == "Overview":
    st.title("NZ Electricity Generation — Overview")

    monthly = load_monthly()
    renewable = load_renewable()

    if monthly.empty:
        st.warning("No data available.")
        st.stop()

    # KPI cards
    latest_month = monthly["YEAR_MONTH"].max()
    prev_year_month = str(int(latest_month) - 100)

    latest_total = monthly.loc[
        monthly["YEAR_MONTH"] == latest_month, "TOTAL_GENERATION_GWH"
    ].sum()
    prev_total = monthly.loc[
        monthly["YEAR_MONTH"] == prev_year_month, "TOTAL_GENERATION_GWH"
    ].sum()
    yoy_growth = ((latest_total - prev_total) / prev_total * 100) if prev_total else 0

    latest_renewable = renewable.loc[
        renewable["YEAR_MONTH"] == latest_month, "RENEWABLE_PCT"
    ]
    renewable_pct = latest_renewable.values[0] if not latest_renewable.empty else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Generation (latest month)", f"{latest_total:,.1f} GWh")
    col2.metric("Renewable Share", f"{renewable_pct:.1f}%")
    col3.metric("YoY Growth", f"{yoy_growth:+.1f}%")

    # Fuel mix donut for latest month
    st.subheader(f"Generation Mix — {latest_month}")
    latest_mix = monthly[monthly["YEAR_MONTH"] == latest_month][
        ["FUEL_TYPE", "TOTAL_GENERATION_GWH"]
    ]
    fig = px.pie(
        latest_mix,
        values="TOTAL_GENERATION_GWH",
        names="FUEL_TYPE",
        hole=0.4,
    )
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)

# ──────────────────────────────────────────────
# Page 2: Fuel Trends
# ──────────────────────────────────────────────

elif page == "Fuel Trends":
    st.title("Fuel Trends — How has NZ's generation mix evolved?")

    monthly = load_monthly()
    renewable = load_renewable()

    if monthly.empty:
        st.warning("No data available.")
        st.stop()

    # Date range filter
    all_months = sorted(monthly["YEAR_MONTH"].unique())
    col1, col2 = st.columns(2)
    start = col1.selectbox("From", all_months, index=0)
    end = col2.selectbox("To", all_months, index=len(all_months) - 1)

    filtered = monthly[
        (monthly["YEAR_MONTH"] >= start) & (monthly["YEAR_MONTH"] <= end)
    ]

    # Stacked area chart
    st.subheader("Monthly Generation by Fuel Type")
    fig = px.area(
        filtered,
        x="YEAR_MONTH",
        y="TOTAL_GENERATION_GWH",
        color="FUEL_TYPE",
        labels={"YEAR_MONTH": "Month", "TOTAL_GENERATION_GWH": "GWh", "FUEL_TYPE": "Fuel"},
    )
    fig.update_layout(height=500, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

    # Renewable % trend line
    st.subheader("Renewable Percentage Trend")
    ren_filtered = renewable[
        (renewable["YEAR_MONTH"] >= start) & (renewable["YEAR_MONTH"] <= end)
    ]
    fig2 = px.line(
        ren_filtered,
        x="YEAR_MONTH",
        y="RENEWABLE_PCT",
        labels={"YEAR_MONTH": "Month", "RENEWABLE_PCT": "Renewable %"},
    )
    fig2.update_layout(height=400, xaxis_tickangle=-45)
    st.plotly_chart(fig2, use_container_width=True)

# ──────────────────────────────────────────────
# Page 3: Plant Ranking
# ──────────────────────────────────────────────

elif page == "Plant Ranking":
    st.title("Plant Ranking — Top generators each month")

    ranking = load_ranking()

    if ranking.empty:
        st.warning("No data available.")
        st.stop()

    # Month selector
    all_months = sorted(ranking["YEAR_MONTH"].unique(), reverse=True)
    selected_month = st.selectbox("Select Month", all_months)

    month_data = ranking[ranking["YEAR_MONTH"] == selected_month].sort_values(
        "MONTHLY_RANK"
    )

    # Top 10 horizontal bar
    st.subheader(f"Top 10 Power Stations — {selected_month}")
    top10 = month_data.head(10)
    fig = px.bar(
        top10,
        x="TOTAL_GENERATION_GWH",
        y="SITE_CODE",
        color="PRIMARY_FUEL_TYPE",
        orientation="h",
        labels={
            "TOTAL_GENERATION_GWH": "Generation (GWh)",
            "SITE_CODE": "Station",
            "PRIMARY_FUEL_TYPE": "Primary Fuel",
        },
    )
    fig.update_layout(height=450, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)

    # Sortable table
    st.subheader("Full Ranking")
    st.dataframe(
        month_data[
            ["MONTHLY_RANK", "SITE_CODE", "PRIMARY_FUEL_TYPE", "TOTAL_GENERATION_GWH"]
        ].reset_index(drop=True),
        use_container_width=True,
        hide_index=True,
    )

# ──────────────────────────────────────────────
# Page 4: Renewable Share
# ──────────────────────────────────────────────

elif page == "Renewable Share":
    st.title("Renewable Share — Monthly trend over the past decade")

    renewable = load_renewable()

    if renewable.empty:
        st.warning("No data available.")
        st.stop()

    fig = px.line(
        renewable,
        x="YEAR_MONTH",
        y="RENEWABLE_PCT",
        labels={"YEAR_MONTH": "Month", "RENEWABLE_PCT": "Renewable %"},
    )
    fig.update_layout(height=500, xaxis_tickangle=-45)
    fig.add_hline(
        y=renewable["RENEWABLE_PCT"].mean(),
        line_dash="dash",
        line_color="grey",
        annotation_text=f"Average: {renewable['RENEWABLE_PCT'].mean():.1f}%",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Summary stats
    col1, col2, col3 = st.columns(3)
    col1.metric("Average", f"{renewable['RENEWABLE_PCT'].mean():.1f}%")
    col2.metric("Min", f"{renewable['RENEWABLE_PCT'].min():.1f}%")
    col3.metric("Max", f"{renewable['RENEWABLE_PCT'].max():.1f}%")

# ──────────────────────────────────────────────
# Page 5: Seasonal Analysis
# ──────────────────────────────────────────────

elif page == "Seasonal Analysis":
    st.title("Seasonal Analysis — Summer vs Winter generation mix")

    seasonal = load_seasonal()
    monthly = load_monthly()

    if seasonal.empty:
        st.warning("No data available.")
        st.stop()

    # Year range filter
    all_years = sorted(seasonal["SEASON_YEAR"].unique())
    col1, col2 = st.columns(2)
    start_year = col1.selectbox("From Year", all_years, index=0)
    end_year = col2.selectbox("To Year", all_years, index=len(all_years) - 1)

    filtered = seasonal[
        (seasonal["SEASON_YEAR"] >= start_year)
        & (seasonal["SEASON_YEAR"] <= end_year)
    ]

    # Grouped bar: season x fuel
    st.subheader("Generation by Season and Fuel Type")
    season_totals = (
        filtered.groupby(["SEASON", "FUEL_TYPE"])["TOTAL_GENERATION_GWH"]
        .sum()
        .reset_index()
    )
    season_order = ["Summer", "Autumn", "Winter", "Spring"]
    season_totals["SEASON"] = pd.Categorical(
        season_totals["SEASON"], categories=season_order, ordered=True
    )
    fig = px.bar(
        season_totals.sort_values("SEASON"),
        x="SEASON",
        y="TOTAL_GENERATION_GWH",
        color="FUEL_TYPE",
        barmode="group",
        labels={
            "SEASON": "Season",
            "TOTAL_GENERATION_GWH": "Total GWh",
            "FUEL_TYPE": "Fuel",
        },
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    # Heatmap: month x fuel (from monthly data)
    st.subheader("Monthly Generation Heatmap by Fuel Type")
    monthly_copy = monthly.copy()
    monthly_copy["MONTH"] = monthly_copy["YEAR_MONTH"].str[4:6].astype(int)
    heatmap_data = (
        monthly_copy.groupby(["MONTH", "FUEL_TYPE"])["TOTAL_GENERATION_GWH"]
        .mean()
        .reset_index()
    )
    pivot = heatmap_data.pivot(
        index="FUEL_TYPE", columns="MONTH", values="TOTAL_GENERATION_GWH"
    ).fillna(0)

    month_labels = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    fig2 = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=month_labels,
            y=pivot.index.tolist(),
            colorscale="YlOrRd",
            colorbar_title="Avg GWh",
        )
    )
    fig2.update_layout(height=400)
    st.plotly_chart(fig2, use_container_width=True)
