"""DAG to functional test amazon S3 """
from datetime import timedelta
from random import randint

import backoff
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
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
import joblib
from catboost import CatBoostRegressor
import numpy as np
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
    "create_embedding_and_preroccesor",
    tags=["pipe"],
    catchup=False,
    start_date=days_ago(2),
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)


def create_embedding():
    n_comp = 20
    tokenizer = AutoTokenizer.from_pretrained("cointegrated/rubert-tiny2", cache_dir="/tmp/")
    model = AutoModel.from_pretrained("cointegrated/rubert-tiny2", cache_dir="/tmp/")

    def embed_bert_cls(text, model, tokenizer):
        t = tokenizer(text, padding=True, truncation=True, return_tensors='pt')
        with torch.no_grad():
            model_output = model(**{k: v.to(model.device) for k, v in t.items()})
        embeddings = model_output.last_hidden_state[:, 0, :]
        embeddings = torch.nn.functional.normalize(embeddings)
        return embeddings[0].cpu().numpy()

    def process_headlines(data):

        bert_embs = data.title.apply(lambda x: embed_bert_cls(x, model, tokenizer))
        data = pd.concat([data, pd.DataFrame(bert_embs.to_list(), columns=[str(i) for i in range(312)])], axis=1)
        return data

    news_path = "news_data"
    s3 = S3Hook(
        cid,
        transfer_config_args={
            "use_threads": False,
        },
    )

    s3.download_file(
        local_path="/tmp/", preserve_file_name=True, use_autogenerated_subdir=False,
        key=f"{news_path}/kommersant_economics_2015_2024.csv", bucket_name="airflow"
    )
    s3.download_file(
        local_path="/tmp/", preserve_file_name=True, use_autogenerated_subdir=False,
        key=f"{news_path}/kommersant_financial_2015_2024.csv", bucket_name="airflow"
    )
    s3.download_file(
        local_path="/tmp/", preserve_file_name=True, use_autogenerated_subdir=False,
        key=f"{news_path}/kommersant_politics_2015_2024.csv", bucket_name="airflow"
    )

    economics_headlines = pd.read_csv('/tmp/kommersant_economics_2015_2024.csv')
    financial_headlines = pd.read_csv('/tmp/kommersant_financial_2015_2024.csv')
    politics_headlines = pd.read_csv('/tmp/kommersant_politics_2015_2024.csv')
    news = pd.concat([economics_headlines, financial_headlines, politics_headlines], axis=0).reset_index(drop=True)

    def date_transform(data):
        data.date = pd.to_datetime(data.date)
        return data

    def drop_dublicat(data):
        data.drop_duplicates('title', inplace=True)
        data.reset_index(inplace=True, drop=True)
        return data

    def avg(data):
        if 'title' in data.columns:
            data = data.drop("title", axis=1)

        unique_dates = pd.Series(data.date.unique(), name='TRADEDATE')
        news_embeddings = data.drop(['date'], axis=1)
        date_embeddings = pd.DataFrame(columns=news_embeddings.columns)
        for date in unique_dates:
            embs = news_embeddings[data.date == date]
            date_embedding = embs
            if date.dayofweek == 0:
                for j in [1, 2]:
                    weekend_news = news_embeddings[data.date == date - timedelta(days=j)]
                    date_embedding = pd.concat([date_embedding, weekend_news], axis=0)
            date_embedding = date_embedding.mean(axis=0).to_numpy().reshape(1, -1)

            # print(date_embedding)
            # print(date_embeddings.shape)
            date_embeddings = pd.concat(
                [date_embeddings, pd.DataFrame(date_embedding, columns=date_embeddings.columns)], axis=0)
            # print(date_embeddings)

        date_embeddings.reset_index()
        date_embeddings.index = [i for i in range(len(date_embeddings))]
        date_embeddings = pd.concat([pd.DataFrame({"TRADEDATE": unique_dates}), date_embeddings], axis=1)
        date_embeddings.set_index('TRADEDATE', inplace=True)
        return date_embeddings

    class PCAWrapper:
        def __init__(self, n_comp):
            self.n_comp = n_comp
            self.pca = PCA(n_comp)

        def fit(self, X, y):
            self.pca.fit(X[[str(i) for i in range(312)]])
            return self

        def transform(self, X):
            X1 = X.drop([str(i) for i in range(312)], axis=1)
            X1[[str(i) for i in range(self.n_comp)]] = self.pca.transform(X[[str(i) for i in range(312)]])

            return X1

        def fit_transform(self, X, y):
            return self.fit(X, y).transform(X)

    pipe = Pipeline(
        [

            ("drop_dublicat", FunctionTransformer(drop_dublicat)),
            ("date_transform", FunctionTransformer(date_transform)),
            ("emmbenig", FunctionTransformer(process_headlines)),
            ('PCAWrapper', PCAWrapper(n_comp)),
            ('avg', FunctionTransformer(avg))
        ]
    )
    news1 = news.copy()
    pipe.fit(news)
    news = pipe.transform(news1)
    news.to_csv("embeddings.csv", index=False)
    s3.load_file(
        "embeddings.csv", key=f"{news_path}/embeddings.csv", bucket_name="airflow"
    )
    joblib.dump(pipe, "emmbeding_pipeline.plk")
    s3.load_file(
        "emmbeding_pipeline.plk", key=f"{news_path}/emmbeding_pipeline.plk", bucket_name="airflow"
    )
# Create a task to call your processing function
gen_emmbeding = PythonOperator(
    task_id="create_embedding_and_preroccesor",
    provide_context=True,
    python_callable=create_embedding,
    dag=dag,
)

