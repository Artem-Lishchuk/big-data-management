import json
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator


LOGGER = logging.getLogger(__name__)
JUPYTER_CONTAINER = os.environ.get("PROJECT3_JUPYTER_CONTAINER", "jupyter")
PIPELINE_ENTRYPOINT = "/home/jovyan/project/jobs/pipeline.py"
SCHEDULE = "*/10 * * * *"


def send_failure_alert(context):
    task_instance = context["task_instance"]
    message = {
        "dag_id": context["dag"].dag_id,
        "task_id": task_instance.task_id,
        "run_id": context["run_id"],
        "try_number": task_instance.try_number,
        "logical_date": str(context.get("logical_date")),
        "log_url": task_instance.log_url,
    }

    LOGGER.error("Project 3 pipeline task failed: %s", json.dumps(message))

    webhook_url = os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook_url:
        return

    response = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"text": json.dumps(message)}),
        timeout=10,
    )
    response.raise_for_status()


def jupyter_python_command(*args):
    command = " ".join(args)
    return (
        f"docker exec {JUPYTER_CONTAINER} "
        f"python {PIPELINE_ENTRYPOINT} {command}"
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": send_failure_alert,
    "sla": timedelta(minutes=30),
}


with DAG(
    dag_id="project3_lakehouse_pipeline",
    description="End-to-end CDC + taxi medallion pipeline for Project 3.",
    start_date=datetime(2026, 5, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=40),
    default_args=default_args,
    tags=["project-3", "cdc", "iceberg", "airflow"],
) as dag:
    register_connector = BashOperator(
        task_id="register_connector",
        bash_command=jupyter_python_command("register-debezium"),
    )

    connector_health_check = BashOperator(
        task_id="connector_health_check",
        bash_command=jupyter_python_command("connector-health-check"),
    )

    bronze_cdc = BashOperator(
        task_id="bronze_cdc",
        bash_command=jupyter_python_command("bronze-cdc"),
    )

    bronze_taxi = BashOperator(
        task_id="bronze_taxi",
        bash_command=jupyter_python_command("bronze-taxi"),
    )

    silver_cdc = BashOperator(
        task_id="silver_cdc",
        bash_command=jupyter_python_command("silver-cdc"),
    )

    silver_taxi = BashOperator(
        task_id="silver_taxi",
        bash_command=jupyter_python_command("silver-taxi"),
    )

    gold_taxi = BashOperator(
        task_id="gold_taxi",
        bash_command=jupyter_python_command("gold-taxi"),
    )

    validation = BashOperator(
        task_id="validation",
        bash_command=jupyter_python_command("validate"),
    )

    register_connector >> connector_health_check
    connector_health_check >> [bronze_cdc, bronze_taxi]
    bronze_cdc >> silver_cdc
    bronze_taxi >> silver_taxi >> gold_taxi
    [silver_cdc, gold_taxi] >> validation