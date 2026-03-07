from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.session import create_session
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.models import DagBag


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
    ti = kwargs.get('task_instance') or kwargs.get('ti')
    dag_run = kwargs.get('dag_run')
    dag_id = getattr(dag_run, "dag_id", None)
    run_id = getattr(dag_run, "run_id", None)

    dag_bag = DagBag()
    dag_obj = dag_bag.get_dag(dag_id)
    task_obj = dag_obj.get_task('alert_on_completion')

    upstream_task_ids = {t.task_id for t in task_obj.get_direct_relatives(upstream=True)}

    with create_session() as session:
        tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.task_id.in_(upstream_task_ids)  # Filtra apenas upstream
        ).all()

    states = {t.task_id: t.state for t in tis}
    print(f"[DEBUG] Upstream task states: {states}")
    all_success = all(s == State.SUCCESS for s in states.values())

    print(f"[DEBUG] All upstream success: {all_success}")
    if all_success:
        airflow_pipeline_success_callback(**kwargs)
    else:
        airflow_task_failure_callback(**kwargs)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 3, 1),
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

    t_transform = PythonOperator(
        task_id="transform_all",
        python_callable=run_transformations,
    )
    

    alert_task = PythonOperator(
        task_id="alert_on_completion",
        python_callable=alert_on_completion,
        trigger_rule="all_done",
    )
    end_dag = DummyOperator(task_id="end_dag")
    
    # ── Gold (dbt) - será configurado depois ─────────────────
    # t_dbt_run = BashOperator(
    #     task_id="dbt_run",
    #     bash_command="dbt run --project-dir /opt/airflow/dbt",
    # )

    # ── Dependências ─────────────────────────────────────────
    start_dag >> t_extract >> t_transform >> alert_task >> end_dag