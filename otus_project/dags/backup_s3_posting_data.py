from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from clickhouse_connect import get_client
import boto3
from urllib.error import URLError
from botocore.exceptions import ClientError


# Функция: ежедневный бэкап за последние сутки
def daily_backup_s3_posting_data(**kwargs) -> None:
    conn_id = kwargs.get('conn_id')
    s3_con = kwargs.get('s3_con')

    connection_clickhouse = BaseHook.get_connection(conn_id)
    connection_s3_con = BaseHook.get_connection(s3_con)

    conn_data = {
        'host': connection_clickhouse.host,
        'port': connection_clickhouse.port,
        'user': connection_clickhouse.login,
        'password': connection_clickhouse.password,
        'database': connection_clickhouse.schema,
    }


    start_time = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    file_name = f"backup/posting_data_{datetime.now().strftime('%Y%m%d')}.parquet.gz"

    s3_con_data = {
        "s3_access_key": connection_s3_con.login,
        "s3_secret_key": connection_s3_con.password,
        "file_name": file_name,
        "start_time": start_time,
    }
    query_set = f"""SET s3_create_new_file_on_insert = 1"""
    query_daily = """
    INSERT INTO FUNCTION s3(
        'http://minio:9000/otus-project/{file_name}',
        '{s3_access_key}',
        '{s3_secret_key}',
        'Parquet'
    )
    SELECT * FROM otus.posting_data
    WHERE clickhoue_timestamp >= '{start_time}'
    """

    client = get_client(**conn_data)
    client.command(query_set)
    client.command(query_daily.format(**s3_con_data))


# Функция: недельный бэкап (по понедельникам)
def weekly_backup_s3_posting_data(**kwargs) -> None:
    conn_id = kwargs.get('conn_id')
    s3_con = kwargs.get('s3_con')

    connection_clickhouse = BaseHook.get_connection(conn_id)
    connection_s3_con = BaseHook.get_connection(s3_con)

    conn_data = {
        'host': connection_clickhouse.host,
        'port': connection_clickhouse.port,
        'user': connection_clickhouse.login,
        'password': connection_clickhouse.password,
        'database': connection_clickhouse.schema,
    }

    s3_access_key = connection_s3_con.login
    s3_secret_key = connection_s3_con.password

    query_set = f"""SET s3_create_new_file_on_insert = 1"""
    query_weekly = """

    INSERT INTO FUNCTION s3(
        'http://minio:9000/otus-project/backup/weekly_backup.parquet.gz',
        '{s3_access_key}',
        '{s3_secret_key}',
        'Parquet'
    )
    SELECT * FROM otus.posting_data
    WHERE clickhoue_timestamp >= today() - 7
    """

    s3_con_data = {
        "s3_access_key": connection_s3_con.login,
        "s3_secret_key": connection_s3_con.password,
    }
    client = get_client(**conn_data)
    client.command(query_set)
    client.command(query_weekly.format(**s3_con_data))


# Функция: удаление файлов старше N дней
def delete_old_backups_from_s3(**kwargs):
    s3_con = kwargs.get('s3_con')
    connection_s3_con = BaseHook.get_connection(s3_con)

    s3_access_key = connection_s3_con.login
    s3_secret_key = connection_s3_con.password

    bucket_name = 'otus-project'
    folder_prefix = 'backup/'
    days_to_keep = 7

    # Создаем клиент S3
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        region_name='us-east-1'
    )

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)

        for page in page_iterator:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                file_name = obj['Key']
                last_modified = obj['LastModified'].replace(tzinfo=None)
                age_days = (datetime.now() - last_modified).days

                if age_days > days_to_keep and 'posting_data_' in file_name and file_name.endswith('.parquet.gz'):
                    print(f"Deleting old backup: {file_name}, Last modified: {last_modified}, Age: {age_days} days")
                    s3_client.delete_object(Bucket=bucket_name, Key=file_name)

    except ClientError as e:
        raise Exception(f"Failed to delete old backups from S3: {e}")


with DAG(
    dag_id='backup_s3_posting_data',
    schedule_interval='0 2 * * *',  # Ежедневно в 2:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_daily_backup = PythonOperator(
        task_id='daily_backup_s3_posting_data',
        python_callable=daily_backup_s3_posting_data,
        op_kwargs={
            'conn_id': 'clickhouse_con',
            "s3_con": "s3_con",
        },
    )

    task_weekly_backup = PythonOperator(
        task_id='weekly_backup_s3_posting_data',
        python_callable=weekly_backup_s3_posting_data,
        op_kwargs={
            'conn_id': 'clickhouse_con',
            "s3_con": "s3_con",
        },
        trigger_rule='all_success',
    )

    task_delete_old_backups = PythonOperator(
        task_id='delete_old_backups_from_s3',
        python_callable=delete_old_backups_from_s3,
        op_kwargs={
            "s3_con": "s3_con",
        },
    )

    task_daily_backup >> task_weekly_backup >> task_delete_old_backups