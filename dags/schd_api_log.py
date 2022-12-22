import pandas as pd
import requests
import json
import datetime
import os
import pendulum

from google.cloud import storage
from currency_converter import CurrencyConverter
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

local_tz = "Asia/Jakarta"

# get data api
def get_json(url):
    get_api = requests.get(url)
    json_data = get_api.json()
    return json_data

def filename():
    file_name = 'log_api_'+ pendulum.now(local_tz).strftime('%Y%m%d')+'.csv'
    return file_name

#env variable to load data bigquery
GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="prj-narasio-final"
BUCKET_NAME = 'finalprj_data_log'
DATASET = "dt_narasio_api"
FILENAME = filename()

#convert currency USD to IDR
def conver_currency(x):
    cc = CurrencyConverter()
    curr_convert = format(round(cc.convert(x, 'USD', 'IDR'), 2))
    return curr_convert

# modify format time
def format_date(x):
    x_conv = pd.to_datetime(x)
    x_str = x_conv[0].strftime('%Y-%m-%d %H:%M:%S')
    return x_str
    
def load_to_gcs(x_data, x_file_name):
    #upload to google clode storage
    #path_to_private_key = 'prj-narasio-final-c0d2c6d03c58.json'
    #client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
    client = storage.Client()
    # The bucket on GCS in which to write the CSV file
    bucket = client.get_bucket(BUCKET_NAME)
    # The name assigned to the CSV file on GCS
    blob = bucket.blob(x_file_name)
    blob.upload_from_string(x_data.to_csv(index=False), 'text/csv')    
      
# main process
def generate_api():
    # get data API
    url = 'https://api.coindesk.com/v1/bpi/currentprice.json'
    data = get_json(url)
    print(f"[INFO] Data Transform Start .....")

    # get json to pandas
    df = pd.json_normalize(data)
    # Rename Column dataframe
    df_rm = df.rename(columns={
        "chartName": "chart_name",
        "time.updated": "time_updated",
        "time.updatedISO": "time_updated_iso",
        "bpi.USD.code": "bpi_usd_code",
        "bpi.USD.rate_float": "bpi_usd_rate_float",
        "bpi.USD.description": "bpi_usd_description",
        "bpi.GBP.code": "bpi_gdp_code",
        "bpi.GBP.rate_float": "bpi_gdp_rate_float",
        "bpi.GBP.description": "bpi_gdp_description",
        "bpi.EUR.code": "bpi_eur_code",
        "bpi.EUR.rate_float": "bpi_eur_rate_float",
        "bpi.EUR.description": "bpi_eur_description"
    })
    # Drop cloumn df
    df_data = df_rm.drop(columns={
        "time.updateduk",
        "bpi.USD.symbol",
        "bpi.USD.rate",
        "bpi.GBP.symbol",
        "bpi.GBP.rate",
        "bpi.EUR.symbol",
        "bpi.EUR.rate"
    })

    # df order column
    df_data = df_data.loc[:,
                          ['disclaimer', 'chart_name', 'time_updated', 'time_updated_iso', 'bpi_usd_code',
                           'bpi_usd_rate_float', 'bpi_usd_description', 'bpi_gdp_code', 'bpi_gdp_rate_float',
                           'bpi_gdp_description', 'bpi_eur_code', 'bpi_eur_rate_float', 'bpi_eur_description'
                           ]
                          ]

    #add new column bpi_idr_rate_float
    df_data["bpi_idr_rate_float"] = conver_currency(
        df_data["bpi_usd_rate_float"])

    # modify time_updated & time_updated_iso
    df_data['time_updated'] = format_date(df_data['time_updated'])
    df_data['time_updated_iso'] = format_date(df_data['time_updated_iso'])

    # get date now
    df_data['last_updated'] = pendulum.now(local_tz).strftime('%Y-%m-%d %H:%M:%S')
    #load data to csv file
    load_to_gcs(df_data, FILENAME)   
    print(f"[INFO] Transform & Load Data to Google Cloud Storage Success .....")


# default_args = {
#     'owner': 'Novian Azi Saiful Anwar',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'start_date':  datetime(2022,12,1, tz="WIB"),
#     'retry_delay': timedelta("@hourly"),
# }

with DAG(dag_id="schedule_log_api", start_date=pendulum.datetime(2022,12,1, tz=local_tz), 
    schedule_interval="@hourly", catchup=False, ) as dag:

    start = BashOperator(
        task_id="start_run",
        bash_command='echo start')

    generate_api = PythonOperator(
        task_id="load_api_gcs",
        python_callable=generate_api)

    load_bigquery = GCSToBigQueryOperator(
        task_id = 'load_dataset_api',
        bucket = BUCKET_NAME,
        source_objects = [FILENAME], #ini dari google cloude storage
        destination_project_dataset_table = f'{PROJECT_ID}:{DATASET}.tbl_log_api',
        write_disposition='WRITE_APPEND',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'disclaimer', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'chart_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'time_updated', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'time_updated_iso', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'bpi_usd_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'bpi_usd_rate_float', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'bpi_usd_description', 'type': 'STRING', 'mode': 'NULLABLE'},        
        {'name': 'bpi_gdp_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'bpi_gdp_rate_float', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'bpi_gdp_description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'bpi_eur_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'bpi_eur_rate_float', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'bpi_eur_description', 'type': 'STRING', 'mode': 'NULLABLE'},        
        {'name': 'bpi_idr_rate_float', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'last_updated', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},        
            ]
        )
    
    end = BashOperator(
        task_id="end_run",
        bash_command='echo end')

    start >> generate_api >> load_bigquery >> end
       