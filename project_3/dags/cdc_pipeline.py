from airflow import DAG
import subprocess
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import json
import logging

# 1. The Python function that acts as the "Circuitbreaker"
def check_spark_offsets():
       
    
    # Use the same packages you use in your Bronze/Silver scripts
    cmd = f"docker exec jupyter spark-submit /home/jovyan/project/work/check_offsets.py"
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    # Log it so you can see why it might be failing
    print(f"STDOUT: {result.stdout}")
    print(f"STDERR: {result.stderr}")
    
    return "DATA_FOUND" in result.stdout

def register_connector():
    connect_url = 'http://connect:8083/connectors'
    connector_id = 'pg-cdc-connector'

    # 1. Check if the connector is already registered
    status = requests.get(f'{connect_url}/{connector_id}/status')
    if status.status_code == 200:
        logging.info(f"Connector '{connector_id}' is already registered and running.")
        return

    # 2. If not found, register the connector
    logging.info("Connector not found. Registering...")
    cfg = {
        'name': connector_id,
        'config': {
            'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',
            'database.hostname': 'postgres',
            'database.port': '5432',
            'database.user': 'cdc_user',
            'database.password': 'admin',
            'database.dbname': 'sourcedb',
            'topic.prefix': 'dbserver1',
            'table.include.list': 'public.customers',
            'plugin.name': 'pgoutput',
            'snapshot.mode': 'initial',
            'key.converter.schemas.enable': 'false',
            'value.converter.schemas.enable': 'false',
        }
    }
    
    response = requests.post(
        connect_url,
        headers={'Content-Type': 'application/json'},
        data=json.dumps(cfg)
    )
    
    # 3. Raise an exception if it fails (This tells Airflow to mark the task as FAILED)
    response.raise_for_status()
    logging.info(f"Successfully created connector: {response.status_code}")

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'sla': timedelta(minutes=30),
}
# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    'cdc_iceberg_pipeline',
    start_date=datetime(2026, 4, 29),
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    
    PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
    )


    # Task 1: Setup Debezium
    setup_cdc = BashOperator(
        task_id='register_debezium',
        bash_command=f'docker exec jupyter python /home/jovyan/project/work/register_debezium.py'
    )
    

    run_bronze = BashOperator(
        task_id='spark_bronze',
        bash_command=f'docker exec jupyter spark-submit --packages {PACKAGES} /home/jovyan/project/work/spark_bronze.py'
    )

    run_silver = BashOperator(
        task_id='spark_silver',
        bash_command=f'docker exec jupyter spark-submit --packages {PACKAGES} /home/jovyan/project/work/spark_silver.py'
    )

    # Define Dependencies
    setup_cdc >> run_bronze >> run_silver