import json
import pandas as pd
from pathlib import Path


def run_silver_transform(**context):

    execution_date = context["ds_nodash"]

    # ---------------------------------------------------------
    # 1. Get Bronze file from XCom
    # ---------------------------------------------------------

    bronze_file = context["ti"].xcom_pull(
        key="bronze_file",
        task_ids="bronze_ingest"
    )

    if not bronze_file:
        raise ValueError(
            "Bronze file path not found in XCom"
        )

    # ---------------------------------------------------------
    # 2. Create Silver directory
    # ---------------------------------------------------------

    silver_path = Path("/opt/airflow/data/silver")

    silver_path.mkdir(
        parents=True,
        exist_ok=True
    )

    # ---------------------------------------------------------
    # 3. Read Bronze JSON
    # ---------------------------------------------------------

    with open(bronze_file, "r") as f:
        raw = json.load(f)

    if "states" not in raw:
        raise ValueError(
            "Bronze JSON does not contain 'states'"
        )

    if not raw["states"]:
        raise ValueError(
            "Bronze dataset contains no flight records"
        )

    # ---------------------------------------------------------
    # 4. Convert raw states into DataFrame
    # ---------------------------------------------------------

    df_raw = pd.DataFrame(raw["states"])

    df_raw.columns = [
        "icao24",
        "callsign",
        "origin_country",
        "time_position",
        "last_contact",
        "longitude",
        "latitude",
        "baro_altitude",
        "on_ground",
        "velocity",
        "true_track",
        "vertical_rate",
        "sensors",
        "geo_altitude",
        "squawk",
        "spi",
        "position_source"
    ]

    # ---------------------------------------------------------
    # 5. Select required columns
    # ---------------------------------------------------------

    df = df_raw[
        [
            "icao24",
            "origin_country",
            "velocity",
            "on_ground"
        ]
    ].copy()

    # =========================================================
    # SILVER DATA CLEANING & TRANSFORMATION
    # =========================================================

    # ---------------------------------------------------------
    # 6. Clean ICAO24
    # ---------------------------------------------------------

    df["icao24"] = (
        df["icao24"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    # Remove empty ICAO24 values
    df = df[
        df["icao24"].notna()
        & (df["icao24"] != "")
        & (df["icao24"] != "nan")
    ]

    # ---------------------------------------------------------
    # 7. Normalize origin_country
    # ---------------------------------------------------------

    df["origin_country"] = (
        df["origin_country"]
        .astype("string")
        .str.strip()
    )

    # Replace missing/empty country values
    df["origin_country"] = (
        df["origin_country"]
        .replace(
            {
                "": pd.NA,
                "nan": pd.NA,
                "None": pd.NA,
                "null": pd.NA
            }
        )
        .fillna("Unknown")
    )

    # Normalize whitespace
    df["origin_country"] = (
        df["origin_country"]
        .str.replace(
            r"\s+",
            " ",
            regex=True
        )
        .str.title()
    )

    # ---------------------------------------------------------
    # 8. Convert velocity to numeric
    # ---------------------------------------------------------

    df["velocity"] = pd.to_numeric(
        df["velocity"],
        errors="coerce"
    )

    # ---------------------------------------------------------
    # 9. Validate velocity
    # ---------------------------------------------------------

    # Negative velocity is invalid
    df.loc[
        df["velocity"] < 0,
        "velocity"
    ] = pd.NA

    # Remove unrealistic/null velocity values
    df = df[
        df["velocity"].notna()
    ]

    # ---------------------------------------------------------
    # 10. Convert on_ground to boolean
    # ---------------------------------------------------------

    def convert_to_bool(value):

        if pd.isna(value):
            return False

        if isinstance(value, bool):
            return value

        if isinstance(value, str):

            value = value.strip().lower()

            if value in {
                "true",
                "1",
                "yes",
                "y"
            }:
                return True

            if value in {
                "false",
                "0",
                "no",
                "n"
            }:
                return False

        if isinstance(value, (int, float)):
            return bool(value)

        return False

    df["on_ground"] = (
        df["on_ground"]
        .apply(convert_to_bool)
        .astype(bool)
    )

    # ---------------------------------------------------------
    # 11. Remove duplicate ICAO24 records
    # ---------------------------------------------------------

    before_duplicates = len(df)

    df = df.drop_duplicates(
        subset=["icao24"],
        keep="last"
    )

    duplicates_removed = (
        before_duplicates - len(df)
    )

    # ---------------------------------------------------------
    # 12. Add derived flight status
    # ---------------------------------------------------------

    df["flight_status"] = df["on_ground"].map(
        {
            True: "Grounded",
            False: "Airborne"
        }
    )

    # ---------------------------------------------------------
    # 13. Add velocity category
    # ---------------------------------------------------------

    df["velocity_category"] = pd.cut(
        df["velocity"],
        bins=[
            -float("inf"),
            50,
            150,
            250,
            float("inf")
        ],
        labels=[
            "Very Low",
            "Low",
            "Normal",
            "High"
        ]
    )

    # ---------------------------------------------------------
    # 14. Add velocity in km/h
    #
    # OpenSky velocity is normally provided in m/s.
    # ---------------------------------------------------------

    df["velocity_kmh"] = (
        df["velocity"] * 3.6
    ).round(2)

    # ---------------------------------------------------------
    # 15. Add high-speed indicator
    # ---------------------------------------------------------

    df["is_high_speed"] = (
        df["velocity"] > 250
    )

    # ---------------------------------------------------------
    # 16. Add ground/air indicator
    # ---------------------------------------------------------

    df["is_airborne"] = (
        ~df["on_ground"]
    )

    # ---------------------------------------------------------
    # 17. Sort data
    # ---------------------------------------------------------

    df = df.sort_values(
        by="origin_country"
    ).reset_index(drop=True)

    # ---------------------------------------------------------
    # 18. Final column order
    # ---------------------------------------------------------

    df = df[
        [
            "icao24",
            "origin_country",
            "velocity",
            "velocity_kmh",
            "on_ground",
            "is_airborne",
            "flight_status",
            "velocity_category",
            "is_high_speed"
        ]
    ]

    # ---------------------------------------------------------
    # 19. Save Silver dataset
    # ---------------------------------------------------------

    output_file = (
        silver_path
        / f"flights_silver_{execution_date}.csv"
    )

    df.to_csv(
        output_file,
        index=False
    )

    # ---------------------------------------------------------
    # 20. Create data-quality summary
    # ---------------------------------------------------------

    quality_report = {
        "execution_date": execution_date,
        "input_records": len(df_raw),
        "output_records": len(df),
        "duplicates_removed": duplicates_removed,
        "missing_icao24_removed": (
            len(df_raw)
            - len(
                df_raw[
                    df_raw["icao24"].notna()
                ]
            )
        ),
        "missing_country_filled": int(
            (
                df["origin_country"]
                == "Unknown"
            ).sum()
        ),
        "airborne_records": int(
            df["is_airborne"].sum()
        ),
        "grounded_records": int(
            df["on_ground"].sum()
        ),
        "high_speed_records": int(
            df["is_high_speed"].sum()
        )
    }

    quality_file = (
        silver_path
        / f"silver_quality_{execution_date}.json"
    )

    with open(
        quality_file,
        "w"
    ) as f:

        json.dump(
            quality_report,
            f,
            indent=4
        )

    # ---------------------------------------------------------
    # 21. Push outputs to XCom
    # ---------------------------------------------------------

    context["ti"].xcom_push(
        key="silver_file",
        value=str(output_file)
    )

    context["ti"].xcom_push(
        key="silver_quality_file",
        value=str(quality_file)
    )

    print(
        f"Silver dataset saved to: {output_file}"
    )

    print(
        f"Silver quality report saved to: {quality_file}"
    )

    print(
        f"Input records: {len(df_raw)}"
    )

    print(
        f"Output records: {len(df)}"
    )

    print(
        f"Duplicates removed: {duplicates_removed}"
    )