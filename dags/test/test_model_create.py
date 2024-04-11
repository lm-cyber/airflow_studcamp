"""DAG to functional test amazon S3 """
from datetime import timedelta
from random import randint

import backoff
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
import torch
from airflow.utils.dates import days_ago
from botocore.exceptions import (
    ConnectTimeoutError,
    EndpointConnectionError,
    ConnectionError,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

cid = "s3_connection"

DEFAULT_ARGS = {
    "owner": "Alan",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "test_torch",
    tags=["mlops"],
    catchup=False,
    start_date=days_ago(2),
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)


def torch_test() -> None:
    torch.tensor([1, 2, 3])




# Create a task to call your processing function
t1 = PythonOperator(
    task_id="test_torch",
    provide_context=True,
    python_callable=torch_test,
    dag=dag,
)

