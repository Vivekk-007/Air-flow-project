import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator



# Make project root available to Airflow DAG parser
AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))



# Import pipeline layers


from scripts.bronze_layer import run_bronze_ingestion
from scripts.silver_layer import run_silver_transform
from scripts.gold_layer import run_gold_layer


# Default DAG arguments

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}



# DAG Definition


with DAG(
    dag_id="flights_ops_medallion_pipe",
    default_args=default_args,
    start_date=datetime(2026, 8, 11),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["flights", "medallion", "etl"],
    description=(
        "Flight data Bronze to Silver to Gold "
        "Medallion ETL pipeline"
    ),
) as dag:


    # Bronze Layer
    # Raw flight data ingestion


    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
    )


    # Silver Layer
    # Cleaning + validation + transformation


    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver_transform,
    )


    # Gold Layer
    # Analytics + aggregations


    gold = PythonOperator(
        task_id="gold_analytics",
        python_callable=run_gold_layer,
    )


    # Pipeline dependency

    bronze >> silver >> gold