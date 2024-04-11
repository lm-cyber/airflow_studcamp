"""DAG to functional test amazon S3 """
from datetime import timedelta
from random import randint

import backoff
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
import torch
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
from transformers import AutoTokenizer, AutoModel
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from itertools import product
from sklearn.metrics import make_scorer
from sklearn.model_selection import cross_val_score
import re
from sklearn.model_selection import train_test_split
import torch
from sklearn.decomposition import PCA
from airflow.utils.dates import days_ago
from botocore.exceptions import (
    ConnectTimeoutError,
    EndpointConnectionError,
    ConnectionError,
)

from catboost import CatBoostRegressor
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
    "create_embs",
    tags=["mlops"],
    catchup=False,
    start_date=days_ago(2),
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)




def create_embedding():
    tokenizer = AutoTokenizer.from_pretrained("cointegrated/rubert-tiny2")
    model = AutoModel.from_pretrained("cointegrated/rubert-tiny2")

    def embed_bert_cls(text, model, tokenizer):
        t = tokenizer(text, padding=True, truncation=True, return_tensors='pt')
        with torch.no_grad():
            model_output = model(**{k: v.to(model.device) for k, v in t.items()})
        embeddings = model_output.last_hidden_state[:, 0, :]
        embeddings = torch.nn.functional.normalize(embeddings)
        return embeddings[0].cpu().numpy()

    def process_headlines(name):
        data = pd.read_csv(name)
        data.drop_duplicates('title', inplace=True)
        data.reset_index(inplace=True, drop=True)

        bert_embs = data.cleaned.progress_apply(lambda x: embed_bert_cls(x, model, tokenizer))
        data = pd.concat([data, pd.DataFrame(bert_embs.to_list(), columns=[i for i in range(312)])], axis=1)
        data.date = pd.to_datetime(data.date)
        return data
    news_path ="news_data"
    s3 = S3Hook(
        cid,
        transfer_config_args={
            "use_threads": False,
        },
    )

    s3.download_file(
        "kommersant_economics_2015_2024.csv", key=f"{news_path}/kommersant_economics_2015_2024.csv", bucket_name="airflow"
    )
    s3.download_file(
        "kommersant_financial_2015_2024.csv", key=f"kommersant_financial_2015_2024.csv", bucket_name="airflow"
    )
    s3.download_file(
        "kommersant_politics_2015_2024.csv", key=f"kommersant_politics_2015_2024.csv", bucket_name="airflow"
    )


    economics_headlines = process_headlines('kommersant_economics_2015_2024.csv')
    financial_headlines = process_headlines('kommersant_financial_2015_2024.csv')
    politics_headlines = process_headlines('kommersant_politics_2015_2024.csv')
    news = pd.concat([economics_headlines, financial_headlines, politics_headlines], axis=0).reset_index(drop=True)
    news.to_csv("embeddings.csv", index=False)
    s3.load_file(
        "embeddings.csv", key=f"{news_path}/embeddings.csv", bucket_name="airflow"
    )
    model = None
    import gc
    tokenizer = None
    gc.collect()

# Create a task to call your processing function
gen_emmbeding = PythonOperator(
    task_id="create_embedding",
    provide_context=True,
    python_callable=create_embedding,
    dag=dag,
)

