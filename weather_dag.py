import datetime
import time
import requests
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

token = '7fad09c1fb274810889134232232205'
warsaw_timezone = 3
local_timezone = 4
horizont_hours = 48

args = {
    'owner': 'me',
    'start_date': datetime.datetime(2018, 11, 1),
    'provide_context': True
}


def extract_data(**kwargs):
    ti = kwargs['ti']
    params = {
        'q': 'Warsaw',
        'tp': 1,
        'num_of_days': 2,
        'date': 'today',
        'format': 'json',
        'key': token
    }

    response = requests.get('https://api.worldweatheronline.com/premium/v1/weather.ashx',
                            params=params)

    if response.status_code == 200:
        json_data = response.json()
        print(json_data)
        ti.xcom_push(key='weather_json', value=json_data)


def transform_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='weather_json', task_ids=['extract_data'])[0]

    start_warsaw = datetime.datetime.utcnow() + datetime.timedelta(hours=warsaw_timezone)
    start_station = datetime.datetime.utcnow() + datetime.timedelta(hours=local_timezone)
    end_station = start_station + datetime.timedelta(hours=horizont_hours)

    date_list = []
    value_list = []
    weather_data = json_data['data']['weather']
    for weather_count in range(len(weather_data)):
        temp_date = weather_data[weather_count]['date']
        hourly_values = weather_data[weather_count]['hourly']
        for i in range(len(hourly_values)):
            date_time_str = '{} {:02d}:00:00'.format(temp_date, int(hourly_values[i]['time']) // 100)
            date_list.append(date_time_str)
            value_list.append(hourly_values[i]['cloudcover'])

    res_df = pd.DataFrame(value_list, columns=['cloud_cover'])
    # Prediction time
    res_df['date_to'] = date_list
    res_df['date_to'] = pd.to_datetime(res_df['date_to'])
    # Prediction 48 interval definition
    res_df = res_df[res_df['date_to'].between(start_station, end_station, inclusive=True)]
    # Prediction time(Warsaw timezone)
    res_df['date_to'] = res_df['date_to'] + datetime.timedelta(hours=warsaw_timezone - local_timezone)
    res_df['date_to'] = res_df['date_to'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # Time when sent the request (Warsaw timezone)
    res_df['date_from'] = start_warsaw
    res_df['date_from'] = pd.to_datetime(res_df['date_from']).dt.strftime('%Y-%m-%d %H:%M:%S')
    # Time when got the answer (UTC)
    res_df['processing_date'] = res_df['date_from']
    print(res_df.head())
    ti.xcom_push(key='weather_df', value=res_df)


with DAG('load_weather', description='weather load', schedule_interval='@once', catchup=False,
         default_args=args) as dag:
    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)
    create_cloud_table = PostgresOperator(
        task_id='create_clouds_table',
        postgres_conn_id='database_PG',
        sql="""
            CREATE TABLE IF NOT EXISTS clouds_value (
            clouds FLOAT NOT NULL,
            date_to TIMESTAMP NOT NULL,
            date_from TIMESTAMP NOT NULL,
            processing_date TIMESTAMP NOT NULL);
            """,
    )

    insert_in_table = PostgresOperator(
        task_id='insert_clouds_table',
        postgres_conn_id='database_PG',
        sql=[f"""INSERT INTO clouds_value VALUES(
             {{{{ti.xcom_pull(key='weather_df', task_ids=['transform_data'])[0].iloc[{i}]['cloud_cover']}}}}
            '{{{{ti.xcom_pull(key='weather_df', task_ids=['transform_data'])[0].iloc[{i}]['date_to']}}}}',
            '{{{{ti.xcom_pull(key='weather_df', task_ids=['transform_data'])[0].iloc[{i}]['date_from']}}}}',
            '{{{{ti.xcom_pull(key='weather_df', task_ids=['transform_data'])[0].iloc[{i}]['processing_date']}}}}')
            """ for i in range(5)]
    )

extract_data >> transform_data >> create_cloud_table >> insert_in_table
