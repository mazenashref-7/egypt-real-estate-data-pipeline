from datetime import timedelta
from pathlib import Path

import pendulum
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


PROJECT_DIR = Path("/app")
SPARK_OPTIONS = (
    "--master 'local[2]' "
    "--driver-memory 1g "
    "--conf spark.sql.shuffle.partitions=8 "
    "--conf spark.jars.ivy=/tmp/ivy "
    "--packages org.postgresql:postgresql:42.7.3"

)
CLEANERS = [
    "clean_egypt_locations.py",
    "clean_inflation_monthly.py",
    "clean_usd_egp_monthly.py",
    "clean_egypt_real_estate.py",
    "clean_propertyfinder.py",
    "clean_listings_stream.py",
]

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/dbt --profiles-dir /opt/dbt/profiles"
def spark_submit(script_name):
    script_path = PROJECT_DIR / "silver_cleaners" / script_name
    return (
        "set -euo pipefail\n"
        f"spark-submit {SPARK_OPTIONS} {script_path}"
    )


def apply_silver_constraints():
    sql_path = PROJECT_DIR / "constraints.sql"
    sql = sql_path.read_text(encoding="utf-8")
    with psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="realestate",
        user="postgres",
        password="postgres",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)


def prepare_silver_rebuild():
    sql_path = PROJECT_DIR / "prepare_silver_rebuild.sql"
    sql = sql_path.read_text(encoding="utf-8")
    with psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="realestate",
        user="postgres",
        password="postgres",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="real_estate_silver_pipeline",
    description="Clean Bronze sources, build Silver tables, and profile the results.",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Africa/Cairo"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["real-estate", "silver", "spark"],
) as dag:
    previous_task = None

    for cleaner in CLEANERS:
        task = BashOperator(
            task_id=f"run_{Path(cleaner).stem}",
            bash_command=spark_submit(cleaner),
            execution_timeout=timedelta(hours=2),
        )
        if previous_task:
            previous_task >> task
        previous_task = task

    prepare_rebuild = PythonOperator(
        task_id="prepare_silver_rebuild",
        python_callable=prepare_silver_rebuild,
    )

    build_star_schema = BashOperator(
        task_id="build_silver_star_schema",
        bash_command=(
            "set -euo pipefail\n"
            f"spark-submit {SPARK_OPTIONS} "
            f"{PROJECT_DIR / 'build_silver_star_schema.py'}"
        ),
        execution_timeout=timedelta(hours=2),
    )

    apply_constraints = PythonOperator(
        task_id="apply_silver_constraints",
        python_callable=apply_silver_constraints,
    )

    profile_tables = BashOperator(
        task_id="profile_bronze_and_silver",
        bash_command=(
            "set -euo pipefail\n"
            f"spark-submit {SPARK_OPTIONS} "
            f"{PROJECT_DIR / 'Profile_All_tables.py'}"
        ),
        execution_timeout=timedelta(hours=2),
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"set -euo pipefail\n{DBT} run {DBT_ARGS}",
        execution_timeout=timedelta(hours=1),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"set -euo pipefail\n{DBT} test {DBT_ARGS}",
        execution_timeout=timedelta(hours=1),
    )

previous_task >> prepare_rebuild >> build_star_schema >> apply_constraints
apply_constraints >> dbt_run >> dbt_test >> profile_tables