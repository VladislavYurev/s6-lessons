from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import pandas as pd
import vertica_python
import contextlib
from typing import List, Optional, Dict


def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len(df)
    vertica_conn = vertica_python.connect(
        host='localhost',
        port=5433,
        user='dbadmin',
    )
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_upload_data_to_vertica_dag():
    upload_groups = PythonOperator(
        task_id=f'fetch_groups.csv',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={'dataset_path': '/data/groups.csv', 'schema': 'STV2025032024__STAGING', 'table': 'groups'},)
    
    upload_users = PythonOperator(
        task_id=f'fetch_groups.csv',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={'dataset_path': '/data/users.csv', 'schema': 'STV2025032024__STAGING', 'table': 'users'},)
    
    upload_dialogs = PythonOperator(
        task_id=f'fetch_groups.csv',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={'dataset_path': '/data/dialogs.csv', 'schema': 'STV2025032024__STAGING', 'table': 'dialogs'},)
    
    [upload_groups, upload_dialogs, upload_users]    
    
_ = sprint6_upload_data_to_vertica_dag
    
    

    
