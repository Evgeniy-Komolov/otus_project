from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
from clickhouse_connect import get_client

# Функция для получения данных из соединения
def write_data_to_clickhouse(**kwargs) -> dict:
    # Получаем ID соединения из параметров
    conn_id = kwargs.get('conn_id')

    # Извлекаем соединение
    connection = BaseHook.get_connection(conn_id)

    # Получаем данные соединения
    conn_data = {
        'host': connection.host,
        'port': connection.port,
        'user': connection.login,
        'password': connection.password,
        'database': connection.schema,
    }

    client = get_client(**conn_data)
    df_from = client.query_df("""SELECT * FROM otus.kafka_messages""")
    print("Таблица из которой мы читаем данные имеет размерность: ", df_from.shape)
    #
    # df_in = client.query_df("""SELECT * FROM otus.kafka_test""")
    # print("Таблица в которую данные будут записаны ДО записи имеет размерность: ", df_in.shape)

    # client.insert_df(
    #     table="otus.write_in",
    #     df=df_from
    # )
    #
    # df_in_after = client.query_df("""SELECT * FROM otus.write_in""")
    # print("Таблица в которую записались данные ПОСЛЕ записи имеет размерность: ", df_in_after.shape)


# Определяем DAG
with DAG(
        dag_id='write_data_to_clickhouse',
        schedule_interval='@daily',
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:
    # Определяем задачу
    task = PythonOperator(
        task_id='write_data_to_clickhouse_task',
        python_callable=write_data_to_clickhouse,
        op_kwargs={'conn_id': 'clickhouse_con'},  # Замените на ваш ID соединения
    )

    task
