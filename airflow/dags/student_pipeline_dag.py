from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "bigdata-student",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="student_learning_pipeline",
    default_args=default_args,
    description="Pipeline Big Data pour analyse des comportements d'apprentissage",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # 1️⃣ Unifier les datasets
    unify_task = BashOperator(
        task_id="unify_students",
        bash_command="python src/processing/unify_students.py"
    )

    # 2️⃣ Nettoyer les données
    clean_task = BashOperator(
        task_id="clean_students",
        bash_command="python src/processing/clean_students.py"
    )

    # 3️⃣ Calculer les KPIs
    kpi_task = BashOperator(
        task_id="compute_kpis",
        bash_command="python src/analytics/compute_kpis.py"
    )

    unify_task >> clean_task >> kpi_task
