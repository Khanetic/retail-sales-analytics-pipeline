# airflow/dags/retail_pipeline.py
from __future__ import annotations
from datetime import timedelta,datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")





























