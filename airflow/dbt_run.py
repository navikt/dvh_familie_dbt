import subprocess
import os
import time
import json
import sys
import logging
from typing import List
from google.cloud import secretmanager
import shlex
from dbt.cli.main import dbtRunner, dbtRunnerResult

DBT_BASE_COMMAND = ["--no-use-colors", "--log-format-file", "json"]

def set_secrets_as_envs():
  secrets = secretmanager.SecretManagerServiceClient()
  resource_name = f"{os.environ['KNADA_TEAM_SECRET']}/versions/latest"
  secret = secrets.access_secret_version(name=resource_name)
  secret_str = secret.payload.data.decode('UTF-8')
  secrets = json.loads(secret_str)
  os.environ.update(secrets)


def write_to_xcom_push_file(content: List[dict]):
    with open('/airflow/xcom/return.json', 'w') as xcom_file:
        json.dump(content, xcom_file)


def filter_logs(file_path: str) -> List[dict]:
    logs = []
    with open(file_path) as logfile:
      for log in logfile:
        logs.append(json.loads(log))

    dbt_codes = [
      'Q009', #PASS
      'Q010', #WARN
      'Q011', #FAIL
      'Q019', #Freshness WARN
      'Q020', #Freshness PASS
      'Z021', #Info about warning in tests
      'Z022', #Info about failing tests
      'E040', #Total runtime
    ]

    filtered_logs = [log for log in logs if log['info']['code'] in dbt_codes]

    return filtered_logs


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    stream_handler = logging.StreamHandler(sys.stdout)
    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()
    profiles_dir = str(sys.path[0])
    command = shlex.split(os.environ["DBT_COMMAND"])
    dbt = dbtRunner()
    dbt_deps = dbt.invoke(DBT_BASE_COMMAND + ["deps"])
    output: dbtRunnerResult = dbt.invoke(DBT_BASE_COMMAND + command)

    #[c.replace('\\', '') for c in os.environ["DBT_COMMAND"].split()]
    log_level = os.getenv("LOG_LEVEL")
    schema = os.getenv("DB_SCHEMA")

    set_secrets_as_envs() #get secrets from gcp

    if not log_level: log_level = 'INFO'
    logger.setLevel(log_level)
    logger.addHandler(stream_handler)

    #logger.debug(f"dbt command: {command}")
    #logger.debug(f"db schema: {schema}")

    def dbt_logg(my_path) -> str:
      with open(my_path + "/logs/dbt.log") as log: return log.read()

    # setter miljø og korrekt skjema med riktig proxy
    os.environ['DBT_ORCL_USER_PROXY'] = f"{os.environ['DBT_ORCL_USER']}" + (f"[{schema}]" if schema else '')
    os.environ['DBT_ORCL_SCHEMA'] = (schema if schema else os.environ['DBT_ORCL_USER']) # samme som i Vault

    logger.info(f"bruker: {os.environ['DBT_ORCL_USER_PROXY']}")

    project_path = os.path.dirname(os.getcwd())
    logger.info(f"Prosjekt path er: {project_path}")

   # Exit code 2, feil utenfor DBT
    if output.exception:
        raise output.exception
    # Exit code 1, feil i dbt (test eller under kjøring)
    if not output.success:
        raise Exception(output.result)


