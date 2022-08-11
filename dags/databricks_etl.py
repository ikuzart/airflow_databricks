from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

from databricks_api_extensions.dbfs import DBFS

default_args = {
    "owner": "admin",
    "email": [
    ],
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "my_dag_for_databricks_jobs",
    default_args=default_args,
    description="Example DAG for databricks jobs",
    schedule_interval=None,
    concurrency=30,
) as dag:

    LOCAL_INIT_FILE_PATH = "dags/databricks_init_scripts/databricks_cluster_init.sh"
    INIT_FILE_DBFS_PATH = "/user/admin/databricks_cluster_init.sh"

    def copy_from_local_to_dbfs():
        fs = DBFS()
        fs.cp_to_dbfs(LOCAL_INIT_FILE_PATH, INIT_FILE_DBFS_PATH, overwrite="true")

    update_init_configs = PythonOperator(
        task_id="update_init_configs",
        python_callable=copy_from_local_to_dbfs,
    )

    new_cluster = {
        'spark_version': '9.1.x-scala2.12',
        'node_type_id': 'r3.xlarge',
        'aws_attributes': {'availability': 'ON_DEMAND'},
        'num_workers': 2,
        "init_scripts": [
            {"dbfs": {"destination": "dbfs:/user/admin/databricks_cluster_init.sh"}},
        ],
    }

    databricks_job_1_task = DatabricksSubmitRunOperator(
        task_id='job_1',
        new_cluster=new_cluster,
        spark_python_task={
            "python_file": "dbfs:/myjobs/job_1.py",
            "parameters": [],
        }
    )
    databricks_job_2_task = DatabricksSubmitRunOperator(
        task_id='job_2',
        new_cluster=new_cluster,
        spark_python_task={
            "python_file": "dbfs:/myjobs/job_2.py",
            "parameters": [],
        }
    )
    update_init_configs >> databricks_job_1_task >> databricks_job_2_task
