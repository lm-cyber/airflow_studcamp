import requests
import json
from bs4 import BeautifulSoup
import os
import logging
from datetime import timedelta
import time
import datetime
from datetime import date
import pandas as pd

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
import tempfile

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

cid = "s3_connection"

DEFAULT_ARGS = {
    "owner": "Dmitrii",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "parser_news_data",
    tags=["mlops"],
    catchup=False,
    start_date=days_ago(2),
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)


def parse_one_news(url):
    response = requests.get(url)
    if not response.ok:
        logging.info(f'News loading error. Code: {response.status_code}\n')
        return None
    tree = BeautifulSoup(response.content, 'html.parser')
    text = tree.find('div', {'class': 'article_text_wrapper'})
    if text is None:
        return None
    return text.text


def parse_kommersant(date, title_code):
    time_sleep_stamps = [0.01, 0.05, 0.07]
    final_date = datetime.date(2015, 1, 1)

    while date != final_date:
        news_text = []
        news_date = []
        news_title = []

        request = f'https://www.kommersant.ru/archive/rubric/{title_code}/day/'

        text_date = date.strftime('%Y-%m-%d')
        try:
            response = requests.get(request + text_date)
        except Exception as e:
            return date
        if not response.ok:
            logging.info("response error\n")
            return date
        tree = BeautifulSoup(response.content, 'html.parser')
        news_cards = tree.find_all('a', {'class': 'uho__link'})
        try:
            for news in news_cards:
                resp = parse_one_news('https://www.kommersant.ru' + news.attrs['href'])
                if resp is None:
                    continue

                news_text.append(resp)
                news_date.append(text_date)
                news_title.append(news.text)

        except Exception as e:
            logging.info('parsing error\n')
            logging.info(e)
            break
        if len(news_cards) == 0:
            logging.info("No news that day\n")
            date -= timedelta(days=1)
            continue

        out_txt = f'out{title_code}.csv'
        out_df = pd.DataFrame(columns=['date', 'text', 'title'])
        out_df['date'] = news_date
        out_df['text'] = news_text
        out_df['title'] = news_title
        out_df.to_csv(out_txt, mode='a', header=not os.path.exists(out_txt), index=False)
        logging.info(f'date: {date}\n')
        date -= timedelta(days=1)
    return date


def parse() -> None:
    title_code = {"economic": 3, "politics": 2, "finance": 40}  # {3: экономика, 2: политика, 40: финансы}
    for key, value in title_code.items():
        data = parse_kommersant(date.today(), value)
        data.to_csv(f"/tmp/data.csv", index=False)

        # Upload generated file to Minio
        s3 = S3Hook(
            cid,
            transfer_config_args={
                "use_threads": False,
            },
        )
        s3.load_file(
            "/tmp/data.csv", key=f"/news_data/{key}.csv", bucket_name="airflow"
        )
        os.remove("/tmp/data.csv")
        logger.info("File loaded")


t1 = PythonOperator(
    task_id="parse",
    provide_context=True,
    python_callable=parse,
    dag=dag,
)
