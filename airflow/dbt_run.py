import os, time, json, sys, logging, subprocess
from typing import List
from google.cloud import secretmanager

def set_secrets_as_envs():
  secrets = secretmanager.SecretManagerServiceClient()
  resource_name = f"{os.environ['KNADA_TEAM_SECRET']}/versions/latest"
  secret = secrets.access_secret_version(name=resource_name)
  secret_str = secret.payload.data.decode('UTF-8')
  secrets = json.loads(secret_str)
  os.environ.update(secrets)





# we pass a list of dictionary parameter, and write a json output to the file (xcom_file). This file doesn't exist but will be created when the dag runs
def write_to_xcom_push_file(content: List[dict]):
    with open('/airflow/xcom/return.json', 'w') as xcom_file:
        json.dump(content, xcom_file)


# the log file created by dbt when a model/folder/project runs will
# this method returns a dict
def filter_logs(file_path: str) -> List[dict]:
    logs = []
    with open(file_path) as logfile:
      for log in logfile:
        logs.append(json.loads(log)) # json.loads converts a json string into a python dictionary.

    # logs will look like something like that --> logs = [ {"code":"A001"}, {"code":"A002"}, {"code":"A003"}, {"code":"Q011"} ]
    # which means filtered_logs will return only {"code":"Q011"} (Just an example)

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

    filtered_logs = [log for log in logs if log['code'] in dbt_codes]

    return filtered_logs

# in our case dvh_familie_dbt/logs/dbt.log. we read the dbt.log file
def dbt_logg(my_path) -> str:
  with open(my_path + "/logs/dbt.log") as log:
    return log.read()


if __name__ == "__main__":

    # the 3 below comes from the airflow dag (they are passed to the task the runs the dbt model/s)
  command = os.environ["DBT_COMMAND"].split(' ',3)
  log_level = os.getenv("LOG_LEVEL")
  schema = os.getenv("DB_SCHEMA")
  os.environ["TZ"] = "Europe/Oslo"
  time.tzset()
  set_secrets_as_envs() #get secrets from gcp


  #set_secrets_as_envs() #get secrets from vault

  profiles_dir = str(sys.path[0]) #dvh_familie_dbt/airflow (directory containing the profiles.yml)
  logger = logging.getLogger(__name__)
  stream_handler = logging.StreamHandler(sys.stdout)

  # if log_level not passed as a parameter in the dag, then make the log_level = 'DEBUG'
  if not log_level:
    log_level = 'DEBUG'
  logger.setLevel(log_level)
  logger.addHandler(stream_handler) ########## need more readings

  #logger.debug(f"dbt command: {command}") #prints the whole dbt command
  #logger.debug(f"db schema: {schema}") #prints the used schema

  # setter miljø og korrekt skjema med riktig proxy
  # we create a proxy user, we get the DBT_ORCL_USER from vault and schema (DB_SCHEMA) if we passed in the dag
  os.environ['DBT_ORCL_USER_PROXY'] = f"{os.environ['DBT_ORCL_USER']}" + (f"[{schema}]" if schema else '')
  os.environ['DBT_ORCL_SCHEMA'] = (schema if schema else os.environ['DBT_ORCL_USER']) # if there is a schema passed in the dag then use it, else get it from vault DBT_ORCL_SCHEMA
  logger.info(f"bruker: {os.environ['DBT_ORCL_USER_PROXY']}") # print DBT_ORCL_USER_PROXY to the airflow log after the dag is run

  project_path = os.path.dirname(os.getcwd()) # path to the project (dvh_familie_dbt)
  logger.info(f"Prosjekt path er: {project_path}") # print project_path to the airflow log after the dag is run


  # we run the dbt command in the background   command +
  try:
      output = subprocess.run(
          (
            ["dbt", "--log-format", "json"] + ["run",  "--select", "Barnetrygd_utpakking.*", "--vars '{dag_interval_start: '2023-06-22 12:00:00', dag_interval_end: '2023-06-22 13:00:00'}'"] +
            ["--profiles-dir", profiles_dir, "--project-dir", project_path]
          ),
          check=True, capture_output=True
      )
      logger.info(output.stdout.decode("utf-8"))
      logger.debug(dbt_logg(project_path))
  except subprocess.CalledProcessError as err:
      raise Exception(logger.error(dbt_logg(project_path)),
                      err.stdout.decode("utf-8"))

  # after running dbt model/s we run this 2 methods the filter_logs() will filter the log file and save only code-s we are interested in
  # the second method will save the output of the first method as an xcom key, value in airflow (see airflow/admin/XComs)

  filtered_logs = filter_logs(f"{project_path}/logs/dbt.log")
  write_to_xcom_push_file(filtered_logs)