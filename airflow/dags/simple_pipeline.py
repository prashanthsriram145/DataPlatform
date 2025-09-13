from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Step 1: Define simple Python functions for tasks
def start_task():
    print("Pipeline started")

def process_task():
    print("Processing data... (dummy step)")

def end_task():
    print("Pipeline finished")

# Step 2: Define DAG
with DAG(
    dag_id="simple_local_pipeline",
    description="A simple Apache Airflow DAG for local machine",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # Run manually
    catchup=False
) as dag:

    # Step 3: Define tasks
    start = PythonOperator(
        task_id="start",
        python_callable=start_task
    )

    process = PythonOperator(
        task_id="process",
        python_callable=process_task
    )

    end = PythonOperator(
        task_id="end",
        python_callable=end_task
    )

    # Step 4: Define task pipeline (order)
    start >> process >> end
