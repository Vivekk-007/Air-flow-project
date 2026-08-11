from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Flight Operations Intelligence",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

GOLD_DIR = PROJECT_ROOT / "data" / "gold"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"


# ============================================================
# CUSTOM CSS
# ============================================================

st.markdown(
    """
    <style>

    .main-title {
        font-size: 40px;
        font-weight: 700;
        margin-bottom: 0px;
    }

    .subtitle {
        font-size: 17px;
        color: #6b7280;
        margin-bottom: 25px;
    }

    .section-title {
        font-size: 28px;
        font-weight: 650;
        margin-top: 20px;
        margin-bottom: 15px;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_latest_file(directory: Path, pattern: str):
    """Return the most recently modified file matching pattern."""

    if not directory.exists():
        return None

    files = list(directory.glob(pattern))

    if not files:
        return None

    return max(
        files,
        key=lambda file: file.stat().st_mtime,
    )


def number(value):
    """Format a number safely."""

    try:
        return f"{int(float(value)):,}"
    except (ValueError, TypeError):
        return "0"


def percentage(value):
    """Format percentage safely."""

    try:
        return f"{float(value):.1f}%"
    except (ValueError, TypeError):
        return "0.0%"


def safe_percentage(numerator, denominator):
    """Calculate percentage without division by zero."""

    if denominator == 0:
        return 0.0

    return (numerator / denominator) * 100


# ============================================================
# LOAD GOLD DATA
# ============================================================

@st.cache_data(ttl=60)
def load_gold_data():

    country_file = get_latest_file(
        GOLD_DIR,
        "country_flight_analytics_*.csv",
    )

    velocity_file = get_latest_file(
        GOLD_DIR,
        "velocity_analytics_*.csv",
    )

    status_file = get_latest_file(
        GOLD_DIR,
        "flight_status_analytics_*.csv",
    )

    overall_file = get_latest_file(
        GOLD_DIR,
        "overall_flight_metrics_*.csv",
    )

    missing = []

    if country_file is None:
        missing.append("country_flight_analytics")

    if velocity_file is None:
        missing.append("velocity_analytics")

    if status_file is None:
        missing.append("flight_status_analytics")

    if overall_file is None:
        missing.append("overall_flight_metrics")

    if missing:
        return None, missing

    try:

        country_df = pd.read_csv(country_file)

        velocity_df = pd.read_csv(velocity_file)

        status_df = pd.read_csv(status_file)

        overall_df = pd.read_csv(overall_file)

    except Exception as error:

        return None, [str(error)]

    return {
        "country": country_df,
        "velocity": velocity_df,
        "status": status_df,
        "overall": overall_df,
        "country_file": country_file,
        "velocity_file": velocity_file,
        "status_file": status_file,
        "overall_file": overall_file,
    }, []


data, loading_errors = load_gold_data()


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.title("✈️ Flight Intelligence")

    st.markdown("---")

    st.subheader("Navigation")

    page = st.radio(
        "Select dashboard section",
        [
            "Executive Overview",
            "Country Analysis",
            "Velocity Analysis",
            "Operational Insights",
            "Data Quality",
            "Gold Data",
        ],
    )

    st.markdown("---")

    st.subheader("Pipeline Architecture")

    st.write("🟤 Bronze")
    st.write("↓")
    st.write("⚪ Silver")
    st.write("↓")
    st.write("🟡 Gold")
    st.write("↓")
    st.write("📊 Dashboard")

    st.markdown("---")

    if st.button(
        "🔄 Refresh Dashboard",
        use_container_width=True,
    ):

        st.cache_data.clear()
        st.rerun()


# ============================================================
# CHECK DATA
# ============================================================

if data is None:

    st.title("✈️ Flight Operations Intelligence")

    st.error(
        "Gold layer data is not available."
    )

    if loading_errors:

        st.write("Problem:")

        for error in loading_errors:
            st.code(str(error))

    st.info(
        "Run the Airflow pipeline first to generate "
        "the Gold datasets."
    )

    st.code(
        "docker exec airflow-scheduler "
        "airflow dags trigger flights_ops_medallion_pipe",
        language="cmd",
    )

    st.stop()


# ============================================================
# DATA REFERENCES
# ============================================================

country_df = data["country"].copy()

velocity_df = data["velocity"].copy()

status_df = data["status"].copy()

overall_df = data["overall"].copy()


# ============================================================
# CHECK OVERALL DATA
# ============================================================

if overall_df.empty:

    st.error(
        "overall_flight_metrics file is empty."
    )

    st.stop()


metrics = overall_df.iloc[0]


# ============================================================
# GLOBAL KPI VARIABLES
#
# IMPORTANT:
# These are defined BEFORE all dashboard pages so that
# every page can use them.
# ============================================================

total_aircraft = int(
    float(metrics.get("total_aircraft", 0))
)

total_countries = int(
    float(metrics.get("total_countries", 0))
)

airborne_count = int(
    float(metrics.get("airborne_count", 0))
)

grounded_count = int(
    float(metrics.get("grounded_count", 0))
)

airborne_percentage = float(
    metrics.get("airborne_percentage", 0)
)

grounded_percentage = float(
    metrics.get("grounded_percentage", 0)
)

average_velocity = float(
    metrics.get("average_velocity_kmh", 0)
)

average_velocity_ms = float(
    metrics.get("average_velocity", 0)
)

minimum_velocity = float(
    metrics.get("minimum_velocity", 0)
)

maximum_velocity = float(
    metrics.get("maximum_velocity", 0)
)

median_velocity = float(
    metrics.get("median_velocity", 0)
)

high_speed_count = int(
    float(metrics.get("high_speed_aircraft", 0))
)


# ============================================================
# LAST UPDATED
# ============================================================

gold_files = [
    data["country_file"],
    data["velocity_file"],
    data["status_file"],
    data["overall_file"],
]

latest_modified = max(
    file.stat().st_mtime
    for file in gold_files
)

last_updated = datetime.fromtimestamp(
    latest_modified
)


# ============================================================
# HEADER
# ============================================================

st.markdown(
    '<div class="main-title">'
    "✈️ Flight Operations Intelligence"
    "</div>",
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="subtitle">'
    "Aircraft snapshot analytics powered by "
    "Apache Airflow + Medallion Architecture"
    "</div>",
    unsafe_allow_html=True,
)


# ============================================================
# SNAPSHOT INFORMATION
# ============================================================

info1, info2, info3, info4 = st.columns(4)

with info1:

    st.info(
        """
        **Data Source**

        Flight-state snapshot
        """
    )

with info2:

    st.info(
        """
        **Architecture**

        Bronze → Silver → Gold
        """
    )

with info3:

    st.info(
        f"""
        **Last Updated**

        {last_updated.strftime("%d %b %Y, %H:%M")}
        """
    )

with info4:

    st.info(
        """
        **Pipeline Schedule**

        Every 30 minutes
        """
    )


# ============================================================
# EXECUTIVE OVERVIEW
# ============================================================

if page == "Executive Overview":

    st.markdown(
        '<div class="section-title">'
        "📊 Executive Overview"
        "</div>",
        unsafe_allow_html=True,
    )

    st.write(
        """
        This section provides a high-level view of the current
        aircraft-state snapshot and the most important
        operational indicators.
        """
    )

    # --------------------------------------------------------
    # MAIN KPIs
    # --------------------------------------------------------

    col1, col2, col3, col4 = st.columns(4)

    with col1:

        st.metric(
            "Observed Aircraft",
            number(total_aircraft),
        )

    with col2:

        st.metric(
            "Countries",
            number(total_countries),
        )

    with col3:

        st.metric(
            "Airborne Rate",
            percentage(airborne_percentage),
        )

    with col4:

        st.metric(
            "Grounded Rate",
            percentage(grounded_percentage),
        )

    # --------------------------------------------------------
    # SECONDARY KPIs
    # --------------------------------------------------------

    col1, col2, col3, col4 = st.columns(4)

    with col1:

        st.metric(
            "Airborne Aircraft",
            number(airborne_count),
        )

    with col2:

        st.metric(
            "Grounded Aircraft",
            number(grounded_count),
        )

    with col3:

        st.metric(
            "Average Velocity",
            f"{average_velocity:.1f} km/h",
        )

    with col4:

        st.metric(
            "High-Speed Aircraft",
            number(high_speed_count),
        )

    st.divider()

    # --------------------------------------------------------
    # AIRBORNE VS GROUNDED
    # --------------------------------------------------------

    st.subheader(
        "✈️ Aircraft Operational Status"
    )

    col1, col2 = st.columns(2)

    with col1:

        if not status_df.empty:

            fig = px.pie(
                status_df,
                names="flight_status",
                values="aircraft_count",
                hole=0.50,
                title="Airborne vs Grounded",
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

    with col2:

        if not status_df.empty:

            fig = px.bar(
                status_df,
                x="flight_status",
                y="aircraft_count",
                text="aircraft_count",
                title="Aircraft by Operational Status",
            )

            fig.update_traces(
                textposition="outside"
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

    # --------------------------------------------------------
    # TOP COUNTRIES
    # --------------------------------------------------------

    st.subheader(
        "🌍 Top Countries by Observed Aircraft"
    )

    if not country_df.empty:

        top_countries = (
            country_df
            .sort_values(
                "total_aircraft",
                ascending=False,
            )
            .head(10)
            .sort_values(
                "total_aircraft"
            )
        )

        fig = px.bar(
            top_countries,
            x="total_aircraft",
            y="origin_country",
            orientation="h",
            text="total_aircraft",
            title="Top 10 Countries",
        )

        fig.update_traces(
            textposition="outside"
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    # --------------------------------------------------------
    # QUICK INTERPRETATION
    # --------------------------------------------------------

    st.subheader(
        "💡 Quick Interpretation"
    )

    st.write(
        f"""
        The current snapshot contains **{total_aircraft:,} observed
        aircraft** across **{total_countries:,} countries**.

        **{airborne_percentage:.1f}%** of observed aircraft are
        airborne, while **{grounded_percentage:.1f}%** are grounded.

        The average observed velocity is approximately
        **{average_velocity:.1f} km/h**.
        """
    )

    st.warning(
        """
        **Important:** These are observed aircraft states in a
        snapshot. They should not be interpreted as the number
        of completed flights.
        """
    )


# ============================================================
# COUNTRY ANALYSIS
# ============================================================

elif page == "Country Analysis":

    st.markdown(
        '<div class="section-title">'
        "🌍 Country Analysis"
        "</div>",
        unsafe_allow_html=True,
    )

    st.write(
        """
        Analyze geographic distribution, operational activity,
        grounding and velocity across countries.
        """
    )

    if country_df.empty:

        st.warning(
            "Country analytics dataset is empty."
        )

        st.stop()

    # --------------------------------------------------------
    # FILTER
    # --------------------------------------------------------

    top_n = st.slider(
        "Number of countries to display",
        min_value=5,
        max_value=min(25, len(country_df)),
        value=min(10, len(country_df)),
    )

    top_df = (
        country_df
        .sort_values(
            "total_aircraft",
            ascending=False,
        )
        .head(top_n)
    )

    # --------------------------------------------------------
    # AIRCRAFT BY COUNTRY
    # --------------------------------------------------------

    st.subheader(
        "Aircraft Distribution"
    )

    col1, col2 = st.columns(2)

    with col1:

        plot_df = top_df.sort_values(
            "total_aircraft"
        )

        fig = px.bar(
            plot_df,
            x="total_aircraft",
            y="origin_country",
            orientation="h",
            text="total_aircraft",
            title="Observed Aircraft by Country",
        )

        fig.update_traces(
            textposition="outside"
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    with col2:

        plot_df = top_df.sort_values(
            "avg_velocity_kmh"
        )

        fig = px.bar(
            plot_df,
            x="avg_velocity_kmh",
            y="origin_country",
            orientation="h",
            text="avg_velocity_kmh",
            title="Average Velocity by Country",
        )

        fig.update_traces(
            textposition="outside"
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    # --------------------------------------------------------
    # AIRBORNE RATE
    # --------------------------------------------------------

    st.subheader(
        "✈️ Airborne Utilization"
    )

    utilization = (
        country_df
        .sort_values(
            "airborne_percentage",
            ascending=False,
        )
        .head(top_n)
        .sort_values(
            "airborne_percentage"
        )
    )

    fig = px.bar(
        utilization,
        x="airborne_percentage",
        y="origin_country",
        orientation="h",
        text="airborne_percentage",
        title="Airborne Rate by Country",
    )

    fig.update_traces(
        texttemplate="%{text:.1f}%",
        textposition="outside",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    # --------------------------------------------------------
    # GROUNDING RATE
    # --------------------------------------------------------

    st.subheader(
        "🛬 Grounding Analysis"
    )

    grounding = (
        country_df
        .sort_values(
            "grounded_percentage",
            ascending=False,
        )
        .head(top_n)
        .sort_values(
            "grounded_percentage"
        )
    )

    fig = px.bar(
        grounding,
        x="grounded_percentage",
        y="origin_country",
        orientation="h",
        text="grounded_percentage",
        title="Grounding Rate by Country",
    )

    fig.update_traces(
        texttemplate="%{text:.1f}%",
        textposition="outside",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    # --------------------------------------------------------
    # COUNTRY PERFORMANCE TABLE
    # --------------------------------------------------------

    st.subheader(
        "📋 Country Performance"
    )

    columns = [
        "origin_country",
        "total_aircraft",
        "airborne_count",
        "grounded_count",
        "airborne_percentage",
        "grounded_percentage",
        "avg_velocity_kmh",
        "high_speed_aircraft",
        "traffic_rank",
        "velocity_rank",
    ]

    available_columns = [
        column
        for column in columns
        if column in country_df.columns
    ]

    table = (
        country_df[available_columns]
        .sort_values(
            "traffic_rank"
            if "traffic_rank" in available_columns
            else "total_aircraft",
            ascending=True
            if "traffic_rank" in available_columns
            else False,
        )
    )

    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# VELOCITY ANALYSIS
# ============================================================

elif page == "Velocity Analysis":

    st.markdown(
        '<div class="section-title">'
        "🚀 Velocity Analysis"
        "</div>",
        unsafe_allow_html=True,
    )

    st.write(
        """
        Analyze aircraft movement profiles, velocity categories
        and high-speed observations.
        """
    )

    # --------------------------------------------------------
    # VELOCITY CATEGORY
    # --------------------------------------------------------

    if not velocity_df.empty:

        col1, col2 = st.columns(2)

        with col1:

            fig = px.bar(
                velocity_df,
                x="velocity_category",
                y="aircraft_count",
                text="aircraft_count",
                title="Aircraft by Velocity Category",
            )

            fig.update_traces(
                textposition="outside"
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

        with col2:

            fig = px.bar(
                velocity_df,
                x="velocity_category",
                y="avg_velocity_kmh",
                text="avg_velocity_kmh",
                title="Average Velocity by Category",
            )

            fig.update_traces(
                textposition="outside"
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

    # --------------------------------------------------------
    # VELOCITY KPIs
    # --------------------------------------------------------

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Average",
        f"{average_velocity:.1f} km/h",
    )

    col2.metric(
        "Median",
        f"{median_velocity * 3.6:.1f} km/h",
    )

    col3.metric(
        "Minimum",
        f"{minimum_velocity * 3.6:.1f} km/h",
    )

    col4.metric(
        "Maximum",
        f"{maximum_velocity * 3.6:.1f} km/h",
    )

    # --------------------------------------------------------
    # HIGH-SPEED AIRCRAFT
    # --------------------------------------------------------

    if not country_df.empty:

        st.subheader(
            "⚡ High-Speed Aircraft"
        )

        high_speed = (
            country_df
            .sort_values(
                "high_speed_aircraft",
                ascending=False,
            )
            .head(10)
            .sort_values(
                "high_speed_aircraft"
            )
        )

        fig = px.bar(
            high_speed,
            x="high_speed_aircraft",
            y="origin_country",
            orientation="h",
            text="high_speed_aircraft",
            title="High-Speed Aircraft by Country",
        )

        fig.update_traces(
            textposition="outside"
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    # --------------------------------------------------------
    # TRAFFIC VS VELOCITY
    # --------------------------------------------------------

    if not country_df.empty:

        st.subheader(
            "🌍 Aircraft Volume vs Velocity"
        )

        fig = px.scatter(
            country_df,
            x="total_aircraft",
            y="avg_velocity_kmh",
            size="airborne_count",
            hover_name="origin_country",
            hover_data=[
                "airborne_percentage",
                "grounded_percentage",
                "high_speed_aircraft",
            ],
            title="Observed Aircraft vs Average Velocity",
            labels={
                "total_aircraft": "Observed Aircraft",
                "avg_velocity_kmh":
                    "Average Velocity (km/h)",
            },
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )


# ============================================================
# OPERATIONAL INSIGHTS
# ============================================================

elif page == "Operational Insights":

    st.markdown(
        '<div class="section-title">'
        "💡 Operational & Business Insights"
        "</div>",
        unsafe_allow_html=True,
    )

    st.write(
        """
        This section converts Gold-layer metrics into
        understandable operational findings.
        """
    )

    if country_df.empty:

        st.warning(
            "Country analytics are not available."
        )

        st.stop()

    # --------------------------------------------------------
    # LEADERS
    # --------------------------------------------------------

    traffic_leader = (
        country_df
        .sort_values(
            "total_aircraft",
            ascending=False,
        )
        .iloc[0]
    )

    velocity_leader = (
        country_df
        .sort_values(
            "avg_velocity_kmh",
            ascending=False,
        )
        .iloc[0]
    )

    airborne_leader = (
        country_df
        .sort_values(
            "airborne_percentage",
            ascending=False,
        )
        .iloc[0]
    )

    grounding_leader = (
        country_df
        .sort_values(
            "grounded_percentage",
            ascending=False,
        )
        .iloc[0]
    )

    # --------------------------------------------------------
    # TRAFFIC CONCENTRATION
    # --------------------------------------------------------

    total_observed = (
        country_df["total_aircraft"].sum()
    )

    top5_observed = (
        country_df
        .nlargest(
            min(5, len(country_df)),
            "total_aircraft",
        )["total_aircraft"]
        .sum()
    )

    top5_share = safe_percentage(
        top5_observed,
        total_observed,
    )

    # --------------------------------------------------------
    # LEADER CARDS
    # --------------------------------------------------------

    col1, col2 = st.columns(2)

    with col1:

        st.success(
            f"""
### 🌍 Traffic Leader

**{traffic_leader["origin_country"]}**

Observed aircraft:

**{number(traffic_leader["total_aircraft"])}**

Largest observed aircraft population in
the current snapshot.
"""
        )

    with col2:

        st.info(
            f"""
### 🚀 Velocity Leader

**{velocity_leader["origin_country"]}**

Average velocity:

**{velocity_leader["avg_velocity_kmh"]:.1f} km/h**

Highest average observed velocity.
"""
        )

    col1, col2 = st.columns(2)

    with col1:

        st.success(
            f"""
### ✈️ Highest Airborne Rate

**{airborne_leader["origin_country"]}**

Airborne rate:

**{airborne_leader["airborne_percentage"]:.1f}%**
"""
        )

    with col2:

        st.warning(
            f"""
### 🛬 Highest Grounding Rate

**{grounding_leader["origin_country"]}**

Grounding rate:

**{grounding_leader["grounded_percentage"]:.1f}%**
"""
        )

    # --------------------------------------------------------
    # TRAFFIC CONCENTRATION
    # --------------------------------------------------------

    st.subheader(
        "📊 Traffic Concentration"
    )

    col1, col2, col3 = st.columns(3)

    with col1:

        st.metric(
            "Top 5 Observed Aircraft",
            number(top5_observed),
        )

    with col2:

        st.metric(
            "Top 5 Share",
            f"{top5_share:.1f}%",
        )

    with col3:

        if top5_share >= 60:

            concentration_level = "High"

        elif top5_share >= 40:

            concentration_level = "Moderate"

        else:

            concentration_level = "Low"

        st.metric(
            "Concentration Level",
            concentration_level,
        )

    # --------------------------------------------------------
    # AUTOMATIC FINDINGS
    # --------------------------------------------------------

    st.subheader(
        "🧠 Key Findings"
    )

    findings = []

    findings.append(
        f"The snapshot contains **{total_aircraft:,} observed "
        f"aircraft** across **{total_countries:,} countries**."
    )

    findings.append(
        f"**{airborne_percentage:.1f}%** of observed aircraft "
        f"are airborne and **{grounded_percentage:.1f}%** "
        f"are grounded."
    )

    findings.append(
        f"The top five countries represent approximately "
        f"**{top5_share:.1f}%** of observed aircraft."
    )

    findings.append(
        f"**{traffic_leader['origin_country']}** is the "
        f"largest country by observed aircraft."
    )

    findings.append(
        f"**{velocity_leader['origin_country']}** has "
        f"the highest average observed velocity."
    )

    findings.append(
        f"**{grounding_leader['origin_country']}** has "
        f"the highest grounding rate."
    )

    for finding in findings:

        st.markdown(
            f"- {finding}"
        )

    # --------------------------------------------------------
    # INTERPRETATION WARNING
    # --------------------------------------------------------

    st.warning(
        """
        **Data interpretation**

        This project analyzes aircraft-state snapshots.
        "Observed aircraft" does not mean completed flights.

        Airborne and grounded percentages describe aircraft
        states at the time represented by the snapshot.
        """
    )


# ============================================================
# DATA QUALITY
# ============================================================

elif page == "Data Quality":

    st.markdown(
        '<div class="section-title">'
        "🧹 Data Quality"
        "</div>",
        unsafe_allow_html=True,
    )

    st.write(
        """
        The Silver layer cleans, validates and transforms
        the Bronze data before producing business-ready
        Gold datasets.
        """
    )

    # --------------------------------------------------------
    # FILE COUNTS
    # --------------------------------------------------------

    bronze_files = (
        list(BRONZE_DIR.glob("*"))
        if BRONZE_DIR.exists()
        else []
    )

    silver_files = (
        list(SILVER_DIR.glob("*"))
        if SILVER_DIR.exists()
        else []
    )

    gold_files = (
        list(GOLD_DIR.glob("*"))
        if GOLD_DIR.exists()
        else []
    )

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Bronze Files",
        len(bronze_files),
    )

    col2.metric(
        "Silver Files",
        len(silver_files),
    )

    col3.metric(
        "Gold Files",
        len(gold_files),
    )

    st.divider()

    # --------------------------------------------------------
    # QUALITY PROCESS
    # --------------------------------------------------------

    quality_checks = pd.DataFrame(
        [
            {
                "Data Quality Check":
                    "Duplicate ICAO24",
                "Silver Layer Action":
                    "Removed duplicate aircraft records",
                "Status":
                    "Completed",
            },
            {
                "Data Quality Check":
                    "Missing Country",
                "Silver Layer Action":
                    "Filled missing values with Unknown",
                "Status":
                    "Completed",
            },
            {
                "Data Quality Check":
                    "Velocity Validation",
                "Silver Layer Action":
                    "Converted to numeric and removed invalid values",
                "Status":
                    "Completed",
            },
            {
                "Data Quality Check":
                    "on_ground",
                "Silver Layer Action":
                    "Converted to boolean",
                "Status":
                    "Completed",
            },
            {
                "Data Quality Check":
                    "Country Normalization",
                "Silver Layer Action":
                    "Trimmed and normalized country names",
                "Status":
                    "Completed",
            },
            {
                "Data Quality Check":
                    "Derived Fields",
                "Silver Layer Action":
                    "Created flight status and velocity indicators",
                "Status":
                    "Completed",
            },
        ]
    )

    st.dataframe(
        quality_checks,
        use_container_width=True,
        hide_index=True,
    )

    # --------------------------------------------------------
    # DATASET SIZES
    # --------------------------------------------------------

    st.subheader(
        "📦 Gold Dataset Sizes"
    )

    dataset_sizes = pd.DataFrame(
        [
            {
                "Gold Dataset":
                    "Country Analytics",
                "Rows":
                    len(country_df),
                "Columns":
                    len(country_df.columns),
            },
            {
                "Gold Dataset":
                    "Velocity Analytics",
                "Rows":
                    len(velocity_df),
                "Columns":
                    len(velocity_df.columns),
            },
            {
                "Gold Dataset":
                    "Flight Status Analytics",
                "Rows":
                    len(status_df),
                "Columns":
                    len(status_df.columns),
            },
            {
                "Gold Dataset":
                    "Overall Metrics",
                "Rows":
                    len(overall_df),
                "Columns":
                    len(overall_df.columns),
            },
        ]
    )

    st.dataframe(
        dataset_sizes,
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# GOLD DATA
# ============================================================

elif page == "Gold Data":

    st.markdown(
        '<div class="section-title">'
        "📋 Gold Layer Data"
        "</div>",
        unsafe_allow_html=True,
    )

    st.write(
        """
        These are the business-ready analytical datasets
        generated by the Gold layer of the Airflow pipeline.
        """
    )

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Country Analytics",
            "Velocity Analytics",
            "Status Analytics",
            "Overall Metrics",
        ]
    )

    with tab1:

        st.caption(
            "Country-level operational analytics"
        )

        st.dataframe(
            country_df,
            use_container_width=True,
            hide_index=True,
        )

    with tab2:

        st.caption(
            "Velocity-category analytics"
        )

        st.dataframe(
            velocity_df,
            use_container_width=True,
            hide_index=True,
        )

    with tab3:

        st.caption(
            "Airborne vs grounded analytics"
        )

        st.dataframe(
            status_df,
            use_container_width=True,
            hide_index=True,
        )

    with tab4:

        st.caption(
            "Overall snapshot metrics"
        )

        st.dataframe(
            overall_df,
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Flight Operations Intelligence | "
    "Apache Airflow • Python • Pandas • Plotly • Streamlit"
)