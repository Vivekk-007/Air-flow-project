import pandas as pd
from pathlib import Path


def run_gold_layer(**context):

    # 1. Get Silver file from XCom
    silver_file = context["ti"].xcom_pull(
        key="silver_file",
        task_ids="silver_transform"
    )

    if not silver_file:
        raise ValueError(
            "Silver file path not found in XCom"
        )
    # 2. Read Silver dataset
    df = pd.read_csv(silver_file)

    if df.empty:
        raise ValueError(
            "Silver dataset is empty"
        )

    # 3. Validate required columns


    required_columns = [
        "icao24",
        "origin_country",
        "velocity",
        "velocity_kmh",
        "on_ground",
        "is_airborne",
        "flight_status",
        "velocity_category",
        "is_high_speed",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns in Silver dataset: "
            f"{missing_columns}"
        )
    # 4. Convert required columns to correct types


    df["velocity"] = pd.to_numeric(
        df["velocity"],
        errors="coerce"
    )

    df["velocity_kmh"] = pd.to_numeric(
        df["velocity_kmh"],
        errors="coerce"
    )

    df["on_ground"] = (
        df["on_ground"]
        .astype(str)
        .str.lower()
        .map({
            "true": True,
            "false": False
        })
        .fillna(False)
        .astype(bool)
    )

    df["is_airborne"] = (
        df["is_airborne"]
        .astype(str)
        .str.lower()
        .map({
            "true": True,
            "false": False
        })
        .fillna(False)
        .astype(bool)
    )

    df["is_high_speed"] = (
        df["is_high_speed"]
        .astype(str)
        .str.lower()
        .map({
            "true": True,
            "false": False
        })
        .fillna(False)
        .astype(bool)
    )
    # GOLD ANALYTICS 1
    # Country-level flight analytics

    country_analytics = (
        df.groupby("origin_country")
        .agg(
            total_aircraft=(
                "icao24",
                "nunique"
            ),

            airborne_count=(
                "is_airborne",
                "sum"
            ),

            grounded_count=(
                "on_ground",
                "sum"
            ),

            avg_velocity=(
                "velocity",
                "mean"
            ),

            median_velocity=(
                "velocity",
                "median"
            ),

            min_velocity=(
                "velocity",
                "min"
            ),

            max_velocity=(
                "velocity",
                "max"
            ),

            avg_velocity_kmh=(
                "velocity_kmh",
                "mean"
            ),

            high_speed_aircraft=(
                "is_high_speed",
                "sum"
            ),
        )
        .reset_index()
    )

    # 5. Calculate percentages
    country_analytics["airborne_percentage"] = (
        country_analytics["airborne_count"]
        / country_analytics["total_aircraft"]
        * 100
    )

    country_analytics["grounded_percentage"] = (
        country_analytics["grounded_count"]
        / country_analytics["total_aircraft"]
        * 100
    )

    # 6. Country traffic ranking

    country_analytics["traffic_rank"] = (
        country_analytics["total_aircraft"]
        .rank(
            method="dense",
            ascending=False
        )
        .astype(int)
    )

    # 7. Country velocity ranking

    country_analytics["velocity_rank"] = (
        country_analytics["avg_velocity"]
        .rank(
            method="dense",
            ascending=False
        )
        .astype(int)
    )

    # Round numeric values

    numeric_columns = (
        country_analytics
        .select_dtypes(include="number")
        .columns
    )

    country_analytics[numeric_columns] = (
        country_analytics[numeric_columns]
        .round(2)
    )

    # GOLD ANALYTICS 2
    # Velocity-category analytics

    velocity_analytics = (
        df.groupby("velocity_category")
        .agg(
            aircraft_count=(
                "icao24",
                "nunique"
            ),

            avg_velocity=(
                "velocity",
                "mean"
            ),

            avg_velocity_kmh=(
                "velocity_kmh",
                "mean"
            ),

            airborne_count=(
                "is_airborne",
                "sum"
            ),

            grounded_count=(
                "on_ground",
                "sum"
            ),
        )
        .reset_index()
    )

    velocity_analytics["percentage"] = (
        velocity_analytics["aircraft_count"]
        / velocity_analytics["aircraft_count"].sum()
        * 100
    )

    velocity_analytics = (
        velocity_analytics
        .round(2)
    )

    # GOLD ANALYTICS 3
    # Flight-status analytics

    status_analytics = (
        df.groupby("flight_status")
        .agg(
            aircraft_count=(
                "icao24",
                "nunique"
            ),

            avg_velocity=(
                "velocity",
                "mean"
            ),

            avg_velocity_kmh=(
                "velocity_kmh",
                "mean"
            ),
        )
        .reset_index()
    )

    status_analytics["percentage"] = (
        status_analytics["aircraft_count"]
        / status_analytics["aircraft_count"].sum()
        * 100
    )

    status_analytics = (
        status_analytics
        .round(2)
    )

    # GOLD ANALYTICS 4
    # Overall flight statistics

    total_aircraft = df["icao24"].nunique()

    airborne_count = int(
        df["is_airborne"].sum()
    )

    grounded_count = int(
        df["on_ground"].sum()
    )

    overall_metrics = pd.DataFrame(
        [{
            "total_aircraft": total_aircraft,

            "total_countries": (
                df["origin_country"]
                .nunique()
            ),

            "airborne_count": airborne_count,

            "grounded_count": grounded_count,

            "airborne_percentage": (
                airborne_count
                / total_aircraft
                * 100
            ),

            "grounded_percentage": (
                grounded_count
                / total_aircraft
                * 100
            ),

            "average_velocity": (
                df["velocity"].mean()
            ),

            "median_velocity": (
                df["velocity"].median()
            ),

            "minimum_velocity": (
                df["velocity"].min()
            ),

            "maximum_velocity": (
                df["velocity"].max()
            ),

            "average_velocity_kmh": (
                df["velocity_kmh"].mean()
            ),

            "high_speed_aircraft": int(
                df["is_high_speed"].sum()
            ),
        }]
    )

    overall_metrics = (
        overall_metrics
        .round(2)
    )

    # 5. Create Gold directory

    gold_path = Path("/opt/airflow/data/gold")

    gold_path.mkdir(
        parents=True,
        exist_ok=True
    )

    # Get execution date from context

    execution_date = context["ds_nodash"]

    # 6. Save Gold datasets

    country_file = (
        gold_path
        / f"country_flight_analytics_{execution_date}.csv"
    )

    velocity_file = (
        gold_path
        / f"velocity_analytics_{execution_date}.csv"
    )

    status_file = (
        gold_path
        / f"flight_status_analytics_{execution_date}.csv"
    )

    overall_file = (
        gold_path
        / f"overall_flight_metrics_{execution_date}.csv"
    )

    country_analytics.to_csv(
        country_file,
        index=False
    )

    velocity_analytics.to_csv(
        velocity_file,
        index=False
    )

    status_analytics.to_csv(
        status_file,
        index=False
    )

    overall_metrics.to_csv(
        overall_file,
        index=False
    )

    # 7. XCom outputs

    context["ti"].xcom_push(
        key="gold_file",
        value=str(country_file)
    )

    context["ti"].xcom_push(
        key="gold_country_file",
        value=str(country_file)
    )

    context["ti"].xcom_push(
        key="gold_velocity_file",
        value=str(velocity_file)
    )

    context["ti"].xcom_push(
        key="gold_status_file",
        value=str(status_file)
    )

    context["ti"].xcom_push(
        key="gold_overall_file",
        value=str(overall_file)
    )

    # 8. Logging

    print(
        f"Country analytics saved to: {country_file}"
    )

    print(
        f"Velocity analytics saved to: {velocity_file}"
    )

    print(
        f"Status analytics saved to: {status_file}"
    )

    print(
        f"Overall metrics saved to: {overall_file}"
    )