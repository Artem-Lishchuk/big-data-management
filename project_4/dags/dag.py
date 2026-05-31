"""
RICO pipeline DAG.

Stages (per PDF):
    init -> ingest -> parse -> [embed_image, embed_text, llm_extract]
         -> audit -> eval -> finalize

* finalize uses trigger_rule='all_done' so it runs even when audit fails
  (the run still needs to be marked failed, metrics still collected, Slack
  still notified).
* eval depends only on a passing audit; if audit fails, eval is skipped.
* LIMIT controls how many screens to ingest. Defaults to 5 (dev).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from rico_pipeline.tasks.audit import run_audit
from rico_pipeline.tasks.embed_image import run_embed_image
from rico_pipeline.tasks.embed_text import run_embed_text
from rico_pipeline.tasks.eval import run_eval
from rico_pipeline.tasks.finalize import finalize_run
from rico_pipeline.tasks.ingest import run_ingest
from rico_pipeline.tasks.init import init_run
from rico_pipeline.tasks.llm_extract import run_llm_extract
from rico_pipeline.tasks.parse import run_parse


with DAG(
    dag_id="project_4_dag",
    start_date=datetime(2026, 5, 24),
    schedule=None,
    catchup=False,
    params={"limit": 5},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["rico", "project_4"],
) as dag:

    init = PythonOperator(task_id="init_run", python_callable=init_run)
    ingest = PythonOperator(task_id="ingest", python_callable=run_ingest)
    parse = PythonOperator(task_id="parse", python_callable=run_parse)
    embed_image = PythonOperator(task_id="embed_image", python_callable=run_embed_image)
    embed_text = PythonOperator(task_id="embed_text", python_callable=run_embed_text)
    llm_extract = PythonOperator(task_id="llm_extract", python_callable=run_llm_extract)
    audit = PythonOperator(
        task_id="audit",
        python_callable=run_audit,
        retries=0,  # a duplicate doesn't disappear by retrying
    )
    evaluate = PythonOperator(task_id="eval", python_callable=run_eval)
    finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize_run,
        trigger_rule="all_done",
    )

    init >> ingest >> parse
    parse >> [embed_image, embed_text, llm_extract] >> audit >> evaluate >> finalize
