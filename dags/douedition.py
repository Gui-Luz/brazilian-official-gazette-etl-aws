from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import boto3
import requests
from bs4 import BeautifulSoup as BS
import json


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


class EditionFinder:

    @staticmethod
    def check_if_sections_were_already_saved(ds):
        date = format_execution_date(ds)
        conn = boto3.client('s3')
        try:
            for key in conn.list_objects(Bucket=f'dou-sections-bucket', Prefix=f"{date.split('-')[2]}/{date.split('-')[1]}/{date.split('-')[0]}")['Contents']:
                print(key['Key'])
            return False
        except:
            return True

    @staticmethod
    def fetch_section_do1(ds):
        date = format_execution_date(ds)
        edition_data = fetch_edition_data(f"https://www.in.gov.br/leiturajornal?data={date}&secao=do1")
        return edition_data

    @staticmethod
    def check_if_edition_exists(ti):
        edition_data = ti.xcom_pull(key='return_value', task_ids='fetch_section_do1')
        if edition_data['jsonArray']:
            return True
        else:
            return False

    @staticmethod
    def check_which_extra_sections_exists(ti):
        edition_data = ti.xcom_pull(key='return_value', task_ids='fetch_section_do1')
        sections = []
        for key in edition_data['typeNormDay']:
            if edition_data['typeNormDay'][key]:
                sections.append(key.lower())
        return sections

    @staticmethod
    def save_section_do1_into_minio(ti, ds):
        date = format_execution_date(ds)
        edition_data = ti.xcom_pull(key='return_value', task_ids='fetch_section_do1')
        section_data = edition_data['jsonArray']
        save_data_into_s3('dou-sections-bucket', date, section_data, 'do1')

    @staticmethod
    def fetch_and_save_section_do2_into_minio(ds):
        date = format_execution_date(ds)
        edition_data = fetch_edition_data(f"https://www.in.gov.br/leiturajornal?data={date}&secao=do2")
        section_data = edition_data['jsonArray']
        save_data_into_s3('dou-sections-bucket', date, section_data, 'do2')

    @staticmethod
    def fetch_and_save_section_do3_into_minio(ds):
        date = format_execution_date(ds)
        edition_data = fetch_edition_data(f"https://www.in.gov.br/leiturajornal?data={date}&secao=do3")
        section_data = edition_data['jsonArray']
        save_data_into_s3('dou-sections-bucket', date, section_data, 'do3')

    @staticmethod
    def fetch_and_save_extra_editions_into_minio(ds, ti):
        date = format_execution_date(ds)
        sections = ti.xcom_pull(key='return_value', task_ids='check_which_extra_sections_exists')
        for section in sections:
            edition_data = fetch_edition_data(f"https://www.in.gov.br/leiturajornal?data={date}&secao={section}")
            save_data_into_s3('dou-sections-bucket', date, edition_data, section)

    @staticmethod
    def inspect_minio_and_final_log(ds):
        date = format_execution_date(ds)
        print(f'[+] EXECUTION DATE: {date}')
        conn = boto3.client('s3')
        objects_list = conn.list_objects(Bucket='dou-sections-bucket', Prefix=f"{date.split('-')[2]}/{date.split('-')[1]}/{date.split('-')[0]}")['Contents']
        edition_total_size = 0
        for obj in objects_list:
            s3 = boto3.resource('s3')
            s3_object = s3.Object('dou-sections-bucket',
                               obj['Key'])
            file_content = s3_object.get()['Body'].read().decode('utf-8')
            json_data = json.loads(file_content)
            edition_total_size += len(json_data)
            print(f"[+] section {obj['Key'].split('/')[3].strip('.json')} - {len(json_data)} articles")
        print(f"[+] TOTAL EDITION SIZE: {edition_total_size} articles")


default_args = {
    "owner": "dou",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 0,
}

with DAG("douedition_dag",
         start_date=datetime(2022, 6, 23),
         schedule_interval="@daily",
         default_args=default_args,
         catchup=True) as dag:

    check_if_sections_were_already_saved = ShortCircuitOperator(
        task_id='check_if_sections_were_already_saved',
        python_callable=EditionFinder.check_if_sections_were_already_saved
    )

    check_if_dou_site_is_reachable = HttpSensor(
        task_id='check_if_dou_site_is_reachable',
        http_conn_id='dou_website',
        endpoint='leiturajornal',
        response_check=lambda response: 'jsonArray' in response.text,
        poke_interval=5
    )

    fetch_section_do1 = PythonOperator(
        task_id='fetch_section_do1',
        python_callable=EditionFinder.fetch_section_do1,
    )

    check_if_edition_exists = ShortCircuitOperator(
        task_id='check_if_edition_exists',
        python_callable=EditionFinder.check_if_edition_exists
    )

    check_which_extra_sections_exists = PythonOperator(
        task_id='check_which_extra_sections_exists',
        python_callable=EditionFinder.check_which_extra_sections_exists
    )

    save_section_do1_into_minio = PythonOperator(
        task_id='save_section_do1_into_minio',
        python_callable=EditionFinder.save_section_do1_into_minio
    )

    fetch_and_save_section_do2_into_minio = PythonOperator(
        task_id='fetch_and_save_section_do2_into_minio',
        python_callable=EditionFinder.fetch_and_save_section_do2_into_minio
    )

    fetch_and_save_section_do3_into_minio = PythonOperator(
        task_id='fetch_and_save_section_do3_into_minio',
        python_callable=EditionFinder.fetch_and_save_section_do3_into_minio
    )

    fetch_and_save_extra_editions_into_minio = PythonOperator(
        task_id='fetch_and_save_extra_editions_into_minio',
        python_callable=EditionFinder.fetch_and_save_extra_editions_into_minio
    )

    inspect_minio_and_final_log = PythonOperator(
        task_id='inspect_minio_and_final_log',
        python_callable=EditionFinder.inspect_minio_and_final_log
    )

    trigger_article_chopper_dag = TriggerDagRunOperator(
        task_id='trigger_article_chopper_dag',
        trigger_dag_id='article_chopper_dag',
        reset_dag_run=True,
        execution_date='{{ ds }}'
    )

check_if_sections_were_already_saved >> check_if_dou_site_is_reachable
check_if_dou_site_is_reachable >> fetch_section_do1 >> check_if_edition_exists
check_if_edition_exists >> [check_which_extra_sections_exists, save_section_do1_into_minio,
                            fetch_and_save_section_do2_into_minio, fetch_and_save_section_do3_into_minio]
check_which_extra_sections_exists >> fetch_and_save_extra_editions_into_minio
[save_section_do1_into_minio, fetch_and_save_section_do2_into_minio, fetch_and_save_section_do3_into_minio,
 fetch_and_save_extra_editions_into_minio] >> inspect_minio_and_final_log >> trigger_article_chopper_dag