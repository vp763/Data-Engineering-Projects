from airflow import DAG
import datetime
import requests
import json
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

file_path = '/opt/airflow/data/raw_products.json'

def extract(**kwargs):
    data = requests.get('https://fakestoreapi.com/products').json()
    with open(file_path,'w',encoding="utf-8") as json_file:
        json.dump(data,json_file,indent=4)

def transform(**kwargs):
    spark = SparkSession.builder.appName("ETL").getOrCreate()
    df = spark.read.option("multiline", "true").json(file_path)
    df = df.filter(df['price'] > 50)
    df.show()
    df.write.mode('overwrite').parquet('/opt/airflow/data/transformed_products.parquet')

def load(**kwargs):
    spark = SparkSession.builder.appName("ETL").getOrCreate()
    df = spark.read.parquet('/opt/airflow/data/transformed_products.parquet')
    df.write.mode('overwrite').parquet('/opt/airflow/data/production_products.parquet')

with DAG(
     dag_id="fakestore_etl",
     start_date=datetime.datetime(2021, 1, 1),
     schedule="@daily",
     catchup = False

    
 ) as dag:
    extract_data = PythonOperator(
                task_id = 'extract_data',
                python_callable = extract)
    transform_data = PythonOperator(
                task_id = 'transform_data',
                python_callable = transform)
    load_data = PythonOperator(
                task_id = 'load_data',
                python_callable = load)

    extract_data >> transform_data >> load_data


