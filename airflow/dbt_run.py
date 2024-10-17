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

    log_level = os.getenv("LOG_LEVEL", 'INFO')
    schema = os.getenv("DB_SCHEMA")

    set_secrets_as_envs()

    logger.setLevel(log_level)
    logger.addHandler(stream_handler)

    os.environ['DBT_ORCL_USER_PROXY'] = f"{os.environ['DBT_ORCL_USER']}" + (f"[{schema}]" if schema else '')
    os.environ['DBT_ORCL_SCHEMA'] = schema if schema else os.environ['DBT_ORCL_USER']

    logger.info(f"bruker: {os.environ['DBT_ORCL_USER_PROXY']}")
    project_path = os.path.dirname(os.getcwd())
    logger.info(f"Prosjekt path er: {project_path}")

    try:
        # Start dbt deps and wait for it to complete
        logger.info("Running dbt deps...")
        process_deps = subprocess.run(
            ['dbt', 'deps'],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        logger.info(process_deps.stdout.decode("utf-8"))
        if process_deps.stderr:
            logger.error(process_deps.stderr.decode("utf-8"))

        # Run the main dbt command
        logger.info("Running dbt command...")
        output = subprocess.run(
            ["dbt", "--no-use-colors", "--log-format", "json"] + command +
            ["--profiles-dir", profiles_dir, "--project-dir", project_path],
            check=True,
            capture_output=True
        )

        logger.info(output.stdout.decode("utf-8"))

    except subprocess.CalledProcessError as err:
        logger.error("An error occurred while running dbt.")
        logger.error(err.stderr.decode("utf-8"))
        raise

    # Uncomment if you need to filter logs
    # filtered_logs = filter_logs(f"{project_path}/logs/dbt.log")
    # write_to_xcom_push_file(filtered_logs)
