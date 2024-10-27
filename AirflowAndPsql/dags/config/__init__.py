from pathlib import Path


CONN_ID__RECALLS_DB = "recalls_db"  # Should be configured via Airflow UI (or Airflow CI/CD)

DBT_CORE_PATH = Path("/home/airflow/.local/bin/dbt")
DBT_PROFILES_PATH = Path().parent.parent.parent.joinpath("config/.dbt")

WORKSPACE_PATH = Path(__file__).parent.parent.joinpath("WorkSpace")
