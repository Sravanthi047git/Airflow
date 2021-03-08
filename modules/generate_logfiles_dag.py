#generate_logfiles_dag.py
#from airflow import DAG
#import airflow

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

import os
from s3_utils import copy_to_s3
from system_utils import execute_local, del_local_file
import datetime


###############################################################
#  constants and configs
###############################################################
location = 's3://twinkle-de-playground/raw/access-log/'

log_generator_repo_path = "/Users//BigData_ETL_Project/Fake-Apache-Log-Generator/"

###############################################################
#  Utility functions
###############################################################


def run(**kwargs):
    date = str(kwargs['ds'])
    log_files = generate_data()
    copy_to_s3(location + date + '/', log_files)
    del_local_file(log_files)


def generate_data():
    os.chdir(log_generator_repo_path)

    #cmd = ["./venv/bin/python", "apache-fake-log-gen.py", "-n", "1000", "-o", "GZ"]
    cmd = ["/usr/local/bin/python3.8", "generate_fake_profiles.py"]
    execute_local(cmd)

    # List files in current directory
    files = os.listdir(os.curdir)
    # particular files from a directories
    list = [log_generator_repo_path + k for k in files if 'access_log' in k]
    print(list)
    return list

###############################################################
#  Airflow dag
###############################################################

args = {
    'owner': 'twinkle',
    'start_date': datetime.datetime(2021, 1, 22),
}

dag = DAG(
    'dags',
    schedule_interval="20 5 * * *",
    default_args=args
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

python_task = PythonOperator(
    task_id='generate_data',
    provide_context=True,
    python_callable=run,
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

start.set_downstream(python_task)
python_task.set_downstream(end)
