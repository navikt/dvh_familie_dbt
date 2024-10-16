import requests
import os
import time
import json
import logging
from pathlib import Path
import shlex
from google.cloud import secretmanager


from dbt.cli.main import dbtRunner, dbtRunnerResult


DBT_BASE_COMMAND = ["--no-use-colors", "--log-format-file", "json"]


def get_dbt_log(log_path) -> str:
    with open(log_path) as log:
        return log.read()


def set_secrets_as_envs(secret_name: str):
    secrets = secretmanager.SecretManagerServiceClient()
    resource_name = f"{os.environ['KNADA_TEAM_SECRET']}/versions/latest"
    secret = secrets.access_secret_version(name=resource_name)
    secret_str = secret.payload.data.decode('UTF-8')
    secrets = json.loads(secret_str)
    os.environ.update(secrets)

logging.basicConfig(
    format='{"msg":"%(message)s", "time":"%(asctime)s", "level":"%(levelname)s"}',
    force=True,
    level=logging.getLevelName("INFO"),
)

if __name__ == "__main__":
    set_secrets_as_envs()
    log_path = Path(__file__).parent / "logs/dbt.log"

    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()

    schema = os.getenv("DB_SCHEMA")
    os.environ["DBT_ORCL_USER_PROXY"] = f"{os.environ['DB_USER']}[{schema}]"
    os.environ["DBT_ORCL_SCHEMA"] = schema
    #os.environ["DBT_DB_DSN"] = os.environ["DB_DSN"]
    os.environ["DBT_ORCL_PASS"] = os.environ["DB_PASSWORD"]
    logging.info("DBT miljøvariabler er lastet inn")

    # default dbt kommando er build
    command = shlex.split(os.getenv("DBT_COMMAND", "run"))
    if dbt_models := os.getenv("DBT_MODELS", None):
        command = command + ["--select", dbt_models]

    dbt = dbtRunner()
    dbt_deps = dbt.invoke(DBT_BASE_COMMAND + ["deps"])
    output: dbtRunnerResult = dbt.invoke(DBT_BASE_COMMAND + command)

    # Exit code 2, feil utenfor DBT
    if output.exception:
        raise output.exception
    # Exit code 1, feil i dbt (test eller under kjøring)
    if not output.success:
        raise Exception(output.result)

    if "docs" in command:
        dbt_project = os.environ["DBT_DOCS_PROJECT_NAME"]
        logging.info("publiserer dbt docs")

    # Legg til logikk for å skrive logg til xcom