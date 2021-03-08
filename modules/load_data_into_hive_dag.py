from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator

#import airflow
import os
import datetime
from airflow_job_config import key_path, ip
from hive_custom_operator import HiveCustomOperator

dir_path = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(dir_path, "../scripts/initial_load_with_param.hql")

args = {
    'owner': 'twinkle',
    'start_date': datetime.datetime(2021, 1, 22),
}

dag = DAG(
    'load_data_into_hive',
    schedule_interval="20 5 * * *",
    default_args=args
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

hive_task = HiveCustomOperator(
    task_id='run_on_hive',
    script_path=file_path,
    run_date='{{ds}}',
    pem_file_path=key_path,
    emr_master_ip=ip,
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

start.set_downstream(hive_task)
hive_task.set_downstream(end)