from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.logging import log


def execute_query(query: str, fetch_one: bool = True, commit: bool = False, conn_id: str = "recalls_db"):
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    log.info(f"Executing the following query\n{query}")
    return postgres_hook.run(sql=query, autocommit=commit, return_last=fetch_one)
