from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from rico_pipeline.tasks.embed_image import run_embed_image
from rico_pipeline.tasks.ingest import run_ingest
from rico_pipeline.tasks.init import init_run
from rico_pipeline.tasks.parse import run_parse
from rico_pipeline.tasks.finalize import finalize_run

with DAG(
    dag_id="project_4_dag",
    start_date=datetime(2026, 5, 24),
    schedule=None,
    catchup=False,
    params = {"limit":10},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
) as dag:

    init = PythonOperator(
        task_id = "init_run",
        python_callable = init_run
    )

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=run_ingest,
    )

    parse = PythonOperator(
        task_id ="parse",
        python_callable=run_parse,
    )

    embed_image = PythonOperator(
        task_id="embed_image",
        python_callable=run_embed_image,
    )

    finalize = PythonOperator(
        task_id = "finalize",
        python_callable = finalize_run,
        trigger_rule = "all_done"
    )

    init >> ingest >> parse >> embed_image >> finalize