from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import boto3
from bs4 import BeautifulSoup as BS
import json
import hashlib


def format_execution_date(ds):
    exec_date = str(ds).split('T')[0]
    date = datetime.strptime(exec_date, '%Y-%m-%d')
    return str(f'{date.day}-{date.month}-{date.year}')


def fetch_edition_data(url):
    try:
        html = requests.get(url).text
        soup = BS(html, 'html.parser')
        script = soup.find("script", id="params")
        j = json.loads(script.text)
        return j
    except:
        return None


def save_data_into_s3(bucket, date, data, file_name):
    s3 = boto3.client('s3')
    s3.put_object(Body=(bytes(json.dumps(data).encode('UTF-8'))),
    Bucket=bucket,
    Key=f"{date.split('-')[2]}/{date.split('-')[1]}/{date.split('-')[0]}/{file_name}.json"
    )


class ArticleChoper:

    @staticmethod
    def print_ds(ds):
        print(ds)

    @staticmethod
    def chop_articles_and_save(ds):
        date = format_execution_date(ds)
        conn = boto3.client('s3')
        objects_list = conn.list_objects(Bucket='dou-sections-bucket',
                                         Prefix=f"{date.split('-')[2]}/{date.split('-')[1]}/{date.split('-')[0]}")['Contents']
        for obj in objects_list:
            s3 = boto3.resource('s3')
            s3_object = s3.Object('dou-sections-bucket', obj['Key'])
            file_content = s3_object.get()['Body'].read().decode('utf-8')
            json_data = json.loads(file_content)
            for article in json_data:
                try:
                    article_title_hash = hashlib.sha256(str(article).encode('utf-8')).hexdigest()
                    save_data_into_s3('dou-articles-bucket', date, article, article_title_hash)
                except Exception as e:
                    print(f"[+] Erro no artigo {article['urlTile']} {e}")


    @staticmethod
    def inspect_minio_and_log(ds):
        # date = format_execution_date(ds)
        # print(f'[+] EXECUTION DATE: {date}')
        # objects = list_objects_in_bucket('articles', f'/{date}')
        # total_articles = 0
        # for _ in objects:
        #     total_articles += 1
        # print(f"[+] ARTICLES FOUND: {total_articles}")
        pass


default_args = {
    "owner": "dou",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 0,
}

with DAG("article_chopper_dag",
         start_date=datetime(2022, 6, 10),
         schedule_interval=None,
         default_args=default_args,
         catchup=True) as dag:

    chop_articles_and_save = PythonOperator(
        task_id='chop_articles_and_save',
        python_callable=ArticleChoper.chop_articles_and_save
    )

    inspect_minio_and_log = PythonOperator(
        task_id='inspect_minio_and_log',
        python_callable=ArticleChoper.inspect_minio_and_log
    )

chop_articles_and_save >> inspect_minio_and_log