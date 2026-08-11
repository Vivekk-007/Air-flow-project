from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# PAGE CONFIGURATION
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

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


# ============================================================
# CUSTOM CSS
# ============================================================

st.markdown(
    """
    <style>

    .main-title {
        font-size: 42px;
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
        margin-top: 25px;
        margin-bottom: 15px;
    }

    .info-box {
        padding: 18px;
        border-radius: 10px;
        border: 1px solid #d9d9d9;
        margin-bottom: 10px;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_latest_file(directory: Path, pattern: str):

    files = list(directory.glob(pattern))

    if not files:
        return None

    return max(
        files,
        key=lambda file: file.stat().st_mtime
    )


def format_number(value):

    return f"{int(value):,}"


def format_percentage(value):

    return f"{float(value):.1f}%"


def safe_divide(a, b):

    if b == 0:
        return 0

    return (a / b) * 100


# ============================================================
# LOAD GOLD DATA
# ============================================================

@st.cache_data(ttl=60)
def load_gold_data():

    country_file = get_latest_file(
        GOLD_DIR,
        "country_flight_analytics_*.csv"
    )

    velocity_file = get_latest_file(
        GOLD_DIR,
        "velocity_analytics_*.csv"
    )

    status_file = get_latest_file(
        GOLD_DIR,
        "flight_status_analytics_*.csv"
    )

    overall_file = get_latest_file(
        GOLD_DIR,
        "overall_flight_metrics_*.csv"
    )

    if not all(
        [
            country_file,
            velocity_file,
            status_file,
            overall_file,
        ]
    ):
        return None

    country_df = pd.read_csv(country_file)

    velocity_df = pd.read_csv(velocity_file)

    status_df = pd.read_csv(status_file)

    overall_df = pd.read_csv(overall_file)

    return {
        "country": country_df,
        "velocity": velocity_df,
        "status": status_df,
        "overall": overall_df,
        "country_file": country_file,
        "velocity_file": velocity_file,
        "status_file": status_file,
        "overall_file": overall_file,
    }


data = load_gold_data()


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.title("✈️ Flight Intelligence")

    st.markdown("---")

    st.subheader("Navigation")

    page = st.radio(
        "Go to",
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

    st.subheader("Pipeline")

    st.write("🟤 Bronze")
    st.write("↓")
    st.write("⚪ Silver")
    st.write("↓")
    st.write("🟡 Gold")
    st.write("↓")
    st.write("📊 Dashboard")

    st.markdown("---")

    if st.button("🔄 Refresh Data"):

        st.cache_data.clear()

        st.rerun()


# ============================================================
# DATA AVAILABILITY CHECK
# ============================================================

if data is None:

    st.title("✈️ Flight Operations Intelligence")

    st.error(
        "Gold layer data is not available."
    )

    st.info(
        "Run the Airflow DAG first:"
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

country_df = data["country"]

velocity_df = data["velocity"]

status_df = data["status"]

overall_df = data["overall"]


metrics = overall_df.iloc[0]


# ============================================================
# HEADER
# ============================================================

st.markdown(
    '<div class="main-title">'
    '✈️ Flight Operations Intelligence'
    '</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="subtitle">'
    'Aircraft snapshot analytics powered by '
    'Apache Airflow and Medallion Architecture'
    '</div>',
    unsafe_allow_html=True,
)


# ============================================================
# SNAPSHOT INFORMATION
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


col1, col2, col3, col4 = st.columns(4)

with col1:
    st.info(
        f"""
        **Data Source**

        Flight state snapshot
        """
    )

with col2:
    st.info(
        f"""
        **Pipeline**

        Bronze → Silver → Gold
        """
    )

with col3:
    st.info(
        f"""
        **Last Updated**

        {last_updated.strftime("%d %b %Y, %H:%M")}
        """
    )

with col4:
    st.info(
        """
        **Schedule**

        Every 30 minutes
        """
    )


# ============================================================
# EXECUTIVE OVERVIEW
# ============================================================

if page == "Executive Overview":

    st.markdown(
        '<div class="section-title">'
        '📊 Executive Overview'
        '</div>',
        unsafe_allow_html=True,
    )

    # --------------------------------------------------------
    # KPI VALUES
    # --------------------------------------------------------

    total_aircraft = int(
        metrics["total_aircraft"]
    )

    total_countries = int(
        metrics["total_countries"]
    )

    airborne_count = int(
        metrics["airborne_count"]
    )

    grounded_count = int(
        metrics["grounded_count"]
    )

    airborne_percentage = float(
        metrics["airborne_percentage"]
    )

    grounded_percentage = float(
        metrics["grounded_percentage"]
    )

    average_velocity = float(
        metrics["average_velocity_kmh"]
    )

    high_speed_count = int(
        metrics["high_speed_aircraft"]
    )

    # --------------------------------------------------------
    # MAIN KPIs
    # --------------------------------------------------------

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Observed Aircraft",
        format_number(total_aircraft),
    )

    col2.metric(
        "Countries",
        format_number(total_countries),
    )

    col3.metric(
        "Airborne Rate",
        format_percentage(
            airborne_percentage
        ),
    )

    col4.metric(
        "Grounded Rate",
        format_percentage(
            grounded_percentage
        ),
    )

    st.markdown("")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Airborne Aircraft",
        format_number(airborne_count),
    )

    col2.metric(
        "Grounded Aircraft",
        format_number(grounded_count),
    )

    col3.metric(
        "Average Velocity",
        f"{average_velocity:,.1f} km/h",
    )

    col4.metric(
        "High-Speed Aircraft",
        format_number(high_speed_count),
    )

    st.divider()

    # --------------------------------------------------------
    # AIRBORNE VS GROUNDED
    # --------------------------------------------------------

    st.subheader(
        "✈️ Current Aircraft Status"
    )

    col1, col2 = st.columns(2)

    with col1:

        fig = px.pie(
            status_df,
            names="flight_status",
            values="aircraft_count",
            hole=0.55,
            title="Airborne vs Grounded",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    with col2:

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


# ============================================================
# COUNTRY ANALYSIS
# ============================================================

elif page == "Country Analysis":

    st.markdown(
        '<div class="section-title">'
        '🌍 Country Analysis'
        '</div>',
        unsafe_allow_html=True,
    )

    st.write(
        """
        This section analyzes the geographic distribution
        and operational activity of observed aircraft.
        """
    )

    top_n = st.slider(
        "Number of countries to display",
        5,
        25,
        10,
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

    col1, col2 = st.columns(2)

    with col1:

        fig = px.bar(
            top_df.sort_values(
                "total_aircraft"
            ),
            x="total_aircraft",
            y="origin_country",
            orientation="h",
            text="total_aircraft",
            title="Observed Aircraft by Country",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    with col2:

        fig = px.bar(
            top_df.sort_values(
                "avg_velocity_kmh"
            ),
            x="avg_velocity_kmh",
            y="origin_country",
            orientation="h",
            text="avg_velocity_kmh",
            title="Average Velocity by Country",
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
    )

    fig = px.bar(
        utilization.sort_values(
            "airborne_percentage"
        ),
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
    )

    fig = px.bar(
        grounding.sort_values(
            "grounded_percentage"
        ),
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
    # COUNTRY TABLE
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

    columns = [
        col
        for col in columns
        if col in country_df.columns
    ]

    st.dataframe(
        country_df[columns]
        .sort_values("traffic_rank"),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# VELOCITY ANALYSIS
# ============================================================

elif page == "Velocity Analysis":

    st.markdown(
        '<div class="section-title">'
        '🚀 Velocity Analysis'
        '</div>',
        unsafe_allow_html=True,
    )

    st.write(
        """
        Velocity analysis describes the movement profile
        of aircraft observed in the current snapshot.
        """
    )

    col1, col2 = st.columns(2)

    with col1:

        fig = px.bar(
            velocity_df,
            x="velocity_category",
            y="aircraft_count",
            text="aircraft_count",
            title="Aircraft by Velocity Category",
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

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    # --------------------------------------------------------
    # HIGH SPEED
    # --------------------------------------------------------

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

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    # --------------------------------------------------------
    # VELOCITY COUNTRY SCATTER
    # --------------------------------------------------------

    st.subheader(
        "🌍 Traffic vs Velocity"
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
        title="Aircraft Volume vs Average Velocity",
        labels={
            "total_aircraft":
                "Observed Aircraft",
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
        '💡 Operational & Business Insights'
        '</div>',
        unsafe_allow_html=True,
    )

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

    total_observed = country_df[
        "total_aircraft"
    ].sum()

    top5_observed = (
        country_df
        .nlargest(
            5,
            "total_aircraft",
        )["total_aircraft"]
        .sum()
    )

    concentration = safe_divide(
        top5_observed,
        total_observed,
    )

    # --------------------------------------------------------
    # INSIGHT CARDS
    # --------------------------------------------------------

    col1, col2 = st.columns(2)

    with col1:

        st.success(
            f"""
            ### 🌍 Traffic Leader

            **{traffic_leader['origin_country']}**

            Observed aircraft:
            **{int(traffic_leader['total_aircraft']):,}**

            This country has the largest observed
            aircraft population in the current snapshot.
            """
        )

    with col2:

        st.info(
            f"""
            ### 🚀 Velocity Leader

            **{velocity_leader['origin_country']}**

            Average velocity:
            **{velocity_leader['avg_velocity_kmh']:.1f} km/h**

            This country has the highest average
            observed velocity.
            """
        )

    col1, col2 = st.columns(2)

    with col1:

        st.success(
            f"""
            ### ✈️ Highest Airborne Rate

            **{airborne_leader['origin_country']}**

            Airborne rate:
            **{airborne_leader['airborne_percentage']:.1f}%**
            """
        )

    with col2:

        st.warning(
            f"""
            ### 🛬 Highest Grounding Rate

            **{grounding_leader['origin_country']}**

            Grounding rate:
            **{grounding_leader['grounded_percentage']:.1f}%**
            """
        )

    # --------------------------------------------------------
    # CONCENTRATION
    # --------------------------------------------------------

    st.subheader(
        "📊 Traffic Concentration"
    )

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Top 5 Aircraft",
        f"{int(top5_observed):,}",
    )

    col2.metric(
        "Top 5 Share",
        f"{concentration:.1f}%",
    )

    if concentration >= 60:
        level = "High"
    elif concentration >= 40:
        level = "Moderate"
    else:
        level = "Low"

    col3.metric(
        "Concentration Level",
        level,
    )

    # --------------------------------------------------------
    # AUTOMATIC BUSINESS INSIGHTS
    # --------------------------------------------------------

    st.subheader(
        "🧠 Key Findings"
    )

    findings = []

    findings.append(
        f"The dataset currently contains "
        f"**{total_aircraft:,} observed aircraft** "
        f"across **{total_countries:,} countries**."
    )

    findings.append(
        f"**{airborne_percentage:.1f}%** of observed "
        f"aircraft are currently airborne, while "
        f"**{grounded_percentage:.1f}%** are grounded."
    )

    findings.append(
        f"The top five countries represent approximately "
        f"**{concentration:.1f}%** of all observed aircraft."
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
    # IMPORTANT INTERPRETATION
    # --------------------------------------------------------

    st.warning(
        """
        **Interpretation note**

        This dashboard represents an aircraft-state snapshot.
        "Observed aircraft" should not be interpreted as the
        number of completed flights.

        Airborne and grounded percentages describe the state
        of aircraft at the time represented by the snapshot.
        """
    )


# ============================================================
# DATA QUALITY
# ============================================================

elif page == "Data Quality":

    st.markdown(
        '<div class="section-title">'
        '🧹 Data Quality'
        '</div>',
        unsafe_allow_html=True,
    )

    st.write(
        """
        The Silver layer cleans and validates the raw flight
        data before it reaches the Gold layer.
        """
    )

    # --------------------------------------------------------
    # FILE COUNTS
    # --------------------------------------------------------

    bronze_files = list(
        BRONZE_DIR.glob("*")
    )

    silver_files = list(
        SILVER_DIR.glob("*")
    )

    gold_files = list(
        GOLD_DIR.glob("*")
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
    # DATA QUALITY CHECKS
    # --------------------------------------------------------

    quality_checks = {
        "Duplicate ICAO24": "Removed in Silver",
        "Missing Country": "Filled as Unknown",
        "Invalid Velocity": "Validated in Silver",
        "on_ground": "Converted to boolean",
        "Country Names": "Normalized",
        "Derived Fields": "Created in Silver",
    }

    quality_df = pd.DataFrame(
        [
            {
                "Check": check,
                "Status": status,
            }
            for check, status
            in quality_checks.items()
        ]
    )

    st.dataframe(
        quality_df,
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# GOLD DATA
# ============================================================

elif page == "Gold Data":

    st.markdown(
        '<div class="section-title">'
        '📋 Gold Layer Data'
        '</div>',
        unsafe_allow_html=True,
    )

    st.write(
        """
        The Gold layer contains business-ready analytical
        datasets generated by the Airflow pipeline.
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

        st.dataframe(
            country_df,
            use_container_width=True,
            hide_index=True,
        )

    with tab2:

        st.dataframe(
            velocity_df,
            use_container_width=True,
            hide_index=True,
        )

    with tab3:

        st.dataframe(
            status_df,
            use_container_width=True,
            hide_index=True,
        )

    with tab4:

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
    "Apache Airflow + Python + Pandas + Streamlit + Plotly"
)