# airflow/dags/retail_pipeline.py
from __future__ import annotations
from datetime import timedelta,datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")


from ingestion.generate_data import generate_customers, generate_products, generate_orders
from ingestion.load_to_staging import load_orders, load_customers, load_products
from quality.checks import run_staging_checks, run_model_checks, run_mart_checks

from sqlalchemy import create_engine, text

DB_URL = "postgresql://retail:retail123@postgres:5432/retail_db"


def run_sql_dir(directory: str):
    """Runs all .sql files in a folder in alphabetical order."""
    engine = create_engine(DB_URL)
    sql_files = sorted(Path(f"/opt/airflow/{directory}").glob("*.sql"))
    
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in {directory}")
    
    for sql_file in sql_files:
        print(f"Running {sql_file}...")
        with engine.begin() as conn:
            conn.execute(text(sql_file.read_text()))
    print(f"All SQL files in {sql_file.name} executed successfully.")
    
    
# default args for DAG
default_args = {
    'owner': 'retail_analytics_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

with DAG(
    dag_id="retail_pipeline",
    default_args=default_args,
    description="Retail sales analytics pipeline",
    schedule="0 6 * * *",      # runs daily at 6am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "etl"],
) as dag:
    
    # 1. Generate raw data (simulates new data arriving each day)
    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=lambda: [generate_customers(), generate_products(), generate_orders()]
    )
    
    # 2. Ingest raw data into staging tables
    ingest_data = PythonOperator(
        task_id="ingest_to_staging",
        python_callable=lambda: [load_orders(), load_customers(), load_products()]
    )
    
    # 3. Run data quality checks on staging data
    staging_checks = PythonOperator(
        task_id="staging_quality_checks",
        python_callable=run_staging_checks
    )   
    
    # 4. Run SQL scripts to transform staging data into dimensional models
    run_models = PythonOperator(
        task_id="run_models",
        python_callable=lambda: run_sql_dir("models")
    )   
    
    # 5. Run data quality checks on models
    model_checks = PythonOperator(
        task_id="model_quality_checks",
        python_callable=run_model_checks
    )   
    
    # 6. Run SQL scripts to populate marts
    run_marts = PythonOperator(
        task_id="run_marts",
        python_callable=lambda: run_sql_dir("marts")
    )   
    
    # 7. Run data quality checks on marts
    mart_checks = PythonOperator(
        task_id="mart_quality_checks",
        python_callable=run_mart_checks     
    )
    
    # Define task dependencies
    # Pipeline order — this is the whole architecture in one line
    generate_data >> ingest_data >> staging_checks >> run_models >> model_checks >> run_marts >> mart_checks
















