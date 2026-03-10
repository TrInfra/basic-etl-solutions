from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

from alerting.consumer.airflow_callback import airflow_task_failure_callback, airflow_pipeline_success_callback
from src.script.extract.extraction import extract_products, extract_users, extract_carts
from src.script.transformation.silver_transformation import transform_products, transform_users, transform_carts


def run_extractions(**kwargs) -> None:
    extract_products(**kwargs)
    extract_users(**kwargs)
    extract_carts(**kwargs)


def run_transformations(**kwargs) -> None:
    transform_products(**kwargs)
    transform_users(**kwargs)
    transform_carts(**kwargs)


def alert_on_completion(**kwargs):
    dag_run = kwargs['dag_run']
    
    failed_tasks = dag_run.get_task_instances(state='failed')

    if len(failed_tasks) == 0:
        print("Sucesso: Nenhuma falha detectada. Enviando alerta de sucesso...")
        return airflow_pipeline_success_callback(**kwargs)
    else:
        print(f"Falha: Detectadas {len(failed_tasks)} tarefas com erro. Enviando alerta de falha...")
        return airflow_task_failure_callback(**kwargs)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 3, 1),
    "on_failure_callback": airflow_task_failure_callback,
    "on_success_callback": airflow_pipeline_success_callback,
}

with DAG(
    dag_id="etl_pipeline",
    description="ETL: FakeStore API → Bronze → Silver → Gold (dbt)",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "fakestore"],
) as dag:

    start_dag = DummyOperator(task_id="start_dag")
    
    t_extract = PythonOperator(
        task_id="extract_all",
        python_callable=run_extractions,
    )

    # ── Transform ────────────────────────────────────────────
    t_transform = PythonOperator(
        task_id="transform_all",
        python_callable=run_transformations,
    )

    alert_task = PythonOperator(
        task_id="alert_on_completion",
        python_callable=alert_on_completion,
        trigger_rule="all_done",
        provide_context=True
    )
    
    end_dag = DummyOperator(task_id="end_dag")
    
    # ── Gold (dbt) ───────────────────────────────────────────
    t_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
        env={
            **os.environ, 
            "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", "admin"),
            "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", "12345678"),
        }
    )

    # ── Dependências ─────────────────────────────────────────
    start_dag >> t_extract >> t_transform >> t_dbt_gold >> alert_task >> end_dag