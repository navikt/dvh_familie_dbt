import subprocess
import os
import time
import json
import sys
import logging
from typing import List
from google.cloud import secretmanager
import shlex

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
        'Q009', 'Q010', 'Q011', 'Q019', 'Q020', 'Z021', 'Z022', 'E040',
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


    log_level = os.getenv("LOG_LEVEL")
    schema = os.getenv("DB_SCHEMA")

    set_secrets_as_envs() #get secrets from gcp

    if not log_level: log_level = 'INFO'
    logger.setLevel(log_level)
    logger.addHandler(stream_handler)

    def dbt_logg(my_path) -> str:
      with open(my_path + "/logs/dbt.log") as log: return log.read()

    os.environ['DBT_ORCL_USER_PROXY'] = f"{os.environ['DBT_ORCL_USER']}" + (f"[{schema}]" if schema else '')
    os.environ['DBT_ORCL_SCHEMA'] = schema if schema else os.environ['DBT_ORCL_USER']

    logger.info(f"bruker: {os.environ['DBT_ORCL_USER_PROXY']}")
    project_path = os.path.dirname(os.getcwd())
    logger.info(f"Prosjekt path er: {project_path}")

    try:
        # Start dbt deps process
        process_deps = subprocess.Popen(['dbt', 'deps'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Run the main dbt command
        output = subprocess.run(
            ["dbt", "--no-use-colors", "--log-format", "json"] + command +
            ["--profiles-dir", profiles_dir, "--project-dir", project_path],
            check=True, capture_output=True
        )

        # Wait for dbt deps to complete
        stdout, stderr = process_deps.communicate()
        logger.info(stdout.decode("utf-8"))
        if stderr:
            logger.error(stderr.decode("utf-8"))

        logger.info(output.stdout.decode("utf-8"))

    except subprocess.CalledProcessError as err:
        logger.error(dbt_logg(project_path))
        logger.error(err.stderr.decode("utf-8"))
        raise
