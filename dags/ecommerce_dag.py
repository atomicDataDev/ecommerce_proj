from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.airflow_tasks import extract_db, extract_files, load, transform

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="ecommerce_daily_report",
    default_args=default_args,
    description="Daily ETL pipeline for e-commerce sales reporting",
    schedule="@daily",
    start_date=datetime(2026, 5, 28),
    catchup=False,
    tags=["ecommerce", "etl"],
)
def default_dag():
    @task(task_id="extract_files_task")
    def extract_files_task(**context) -> str:
        current_run_date = context["ds"]
        return extract_files(run_date=current_run_date)

    @task(task_id="extract_db_task", multiple_outputs=True)
    def extract_db_task(**context) -> dict:
        current_run_date = context["ds"]
        return extract_db(run_date=current_run_date)

    @task(task_id="transform_task")
    def transform_task(
        events_path: str, customers_path: str, products_path: str, **context
    ) -> str:
        current_run_date = context["ds"]
        return transform(
            run_date=current_run_date,
            events_path=events_path,
            customers_path=customers_path,
            products_path=products_path,
        )

    @task(task_id="load_task")
    def load_task(report_path: str, **context) -> None:
        load(report_path)

    @task(task_id="cleanup_task", trigger_rule="all_success")
    def cleanup_task(**context) -> None:
        """Remove per-run temporary directory after DAG completes.

        This task uses the logical date (``ds``) to identify the run
        directory. It will remove the directory even if previous tasks
        failed.

        :param context: Airflow task context (injected automatically).
        :type context: dict
        :return: None
        :rtype: None
        """
        import shutil
        from pathlib import Path

        from pipeline.config import settings

        PROJECT_ROOT = Path(__file__).resolve().parents[1]
        run_date = context["ds"]
        safe = run_date.replace(":", "_").replace("/", "_").replace(" ", "_")
        run_dir = PROJECT_ROOT / settings.tmp_dir / safe
        if run_dir.exists():
            shutil.rmtree(run_dir)

    events_path = extract_files_task()
    db_outputs = extract_db_task()
    customers_path = db_outputs["customers_path"]
    products_path = db_outputs["products_path"]
    report_path = transform_task(events_path, customers_path, products_path)
    load_op = load_task(report_path)
    cleanup_op = cleanup_task()
    load_op >> cleanup_op


dag = default_dag()
