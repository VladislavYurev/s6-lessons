from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import boto3
import pendulum

def fetch_s3_file(bucket: str, key: str):
    AWS_ACCESS_KEY_ID = "YCAJEiyNFq4wiOe_eMCMCXmQP"
    AWS_SECRET_ACCESS_KEY = "YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/home/vladislav/data/{key}'
    )

# эту команду надо будет поправить, чтобы она выводила
# первые десять строк каждого файла
bash_command_tmpl = """
echo {{ params.files }}
"""

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_get_data():
    bucket_files = ['users.csv','groups.csv','dialogs.csv']
    task1 = PythonOperator(
        task_id=f'fetch_groups.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'groups.csv'},
    )

    task2 = PythonOperator(
        task_id=f'fetch_dialogs.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs.csv'},
    )

    task2 = PythonOperator(
        task_id=f'fetch_users.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'users.csv'},
    )


    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': [f'/home/vladislav/Рабочий стол/s6-lessons/Тема 3.  Разработка аналитической базы данных/2. Изучим исходные данные/Задание 1/data/{f}' for f in bucket_files]}
    )

_ = sprint6_dag_get_data()