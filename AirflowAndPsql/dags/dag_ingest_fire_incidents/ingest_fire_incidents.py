"""
Get daily yesterday data from Public-Safety Fire-Incidents

# Source
 - https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data
"""
from pathlib import Path

import pandas
from airflow import DAG
from airflow.decorators import task
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from pendulum import datetime

from config import WORKSPACE_PATH, CONN_ID__RECALLS_DB, DBT_PROFILES_PATH
from utils.postgres import execute_query


default_args = {
    "owner": "DE_TEAM",
    "depends_on_past": True,
    "catchup": True,
    "wait_for_downstream": True,
}


with DAG(
    dag_id=Path(__file__).stem,
    default_args=default_args,
    start_date=datetime(2024, 10, 22),  # For testing purposes
    end_date=datetime(2024, 10, 24),  # For testing purposes
    template_searchpath=Path(__file__).parent.absolute().as_posix(),
    schedule_interval="0 3 * * *",  # It should be after data loaded to the Web Page
    tags=["CSV", "DAILY"],
) as dag:
    dag_path = Path(__file__).parent

    @task(task_id="download_yesterday_data")
    def download_yesterday_sales(**context):
        """Download daily data from the webpage and saves it as a CSV file."""
        # TODO
        # Only for testing purposes:
        file_saved_in = (
            WORKSPACE_PATH.joinpath("wr8u-xric_version_5503.csv")
            if context["ds"] == "2024-10-22"
            else WORKSPACE_PATH.joinpath("wr8u-xric_version_5505.csv")
        )
        return file_saved_in.absolute().as_posix()


    dag_load_yesterday_data_to_postgres_task_name = "load_yesterday_data_to_postgres"
    @task(task_id=dag_load_yesterday_data_to_postgres_task_name)
    def load_yesterday_sales_to_postgres(**context):
        from sqlalchemy import create_engine

        raw_table_name = "raw.incidents"
        raw_table = f'public."{raw_table_name}"'
        ds = context["ds"]
        incident_date_postfix = "T00:00:00.000"
        already_exists = execute_query(
            query=f"SELECT 1 FROM {raw_table} WHERE incident_date = {ds} || '{incident_date_postfix}' LIMIT 1",
        )
        if already_exists:
            raise Exception(f"The data already exists for the date `{ds}`")  # Or delete and load

        downloaded_file_absolute_path =  context["ti"].xcom_pull(task_ids=dag_load_yesterday_data_to_postgres_task_name)
        df = pandas.read_csv(downloaded_file_absolute_path)
        df = df[df["incident_date"] == context["ds"] + incident_date_postfix]  # In Airflow ds is yesterday so in 2024-10-25 it will be 24
        engine = create_engine("postgresql://admin:admin@recalls_db:5432/recalls_db")  # Should use Airflow Connections
        with engine.connect() as conn:
            print(df.to_sql(schema="public", name=raw_table_name, con=conn, if_exists="append", index=False))

        # Recommended alternative (being allowed to create the table from the documentation)
        # Depending on the infrastructure could be better filter before load or in a temporary table after load
        # COPY table_name
        # FROM 'file_path'
        # (FORMAT format_name [OPTIONS]);

    dag_recalls_db_conn_conf: Connection = Connection.get_connection_from_secrets(conn_id=CONN_ID__RECALLS_DB)
    dag_dbt_profiles_dir = DBT_PROFILES_PATH.absolute().as_posix()
    dag_dbt_project_dir = Path().parent.joinpath("dbt")
    dag_dbt_daily_incidents_agg_vars = """--vars '{"ds":"{{ ds }}"}'"""
    task_dbt_daily_incidents_agg = BashOperator(
        task_id="dbt_daily_incidents_agg",
        bash_command=" ".join((
            f"dbt run ",
            f"--project-dir {dag_dbt_project_dir}",
            f"--profiles-dir {dag_dbt_profiles_dir}"
            f"{dag_dbt_daily_incidents_agg_vars}",
        )),
        env={
            "RECALLS_DB_USERNAME": dag_recalls_db_conn_conf.login,
            "RECALLS_DB_PASSWORD": dag_recalls_db_conn_conf.password,
        },
    )

    download_yesterday_sales() >> load_yesterday_sales_to_postgres() >> task_dbt_daily_incidents_agg
