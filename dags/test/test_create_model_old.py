"""DAG to functional test amazon S3 """
from datetime import timedelta
from random import randint

import backoff
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging

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
    "create_pipe_old",
    tags=["pipe"],
    catchup=False,
    start_date=days_ago(2),
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)

def moex_parser():
    pass

def moex_processing():
    pass

def news_parser():
    pass
def news_processing():
    pass
def join_processing():
    pass

def create_model():
    pass

t1 = PythonOperator(
    task_id="moex_parser",
    provide_context=True,
    python_callable=moex_parser,
    dag=dag,
)

t2 = PythonOperator(
    task_id="moex_processing",
    provide_context=True,
    python_callable=moex_processing,
    dag=dag,
)

t3 = PythonOperator(
    task_id="news_parser",
    provide_context=True,
    python_callable=news_parser,
    dag=dag,
)


t4 = PythonOperator(
    task_id="news_processing",
    provide_context=True,
    python_callable=news_processing,
    dag=dag,
)

t5 = PythonOperator(
    task_id="join_processing",
    provide_context=True,
    python_callable=join_processing,
    dag=dag,
)

t6 = PythonOperator(
    task_id="create_model",
    provide_context=True,
    python_callable=create_model,
    dag=dag,
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6