import csv
from airflow import DAG
from datetime import datetime
from airflow.contrib.operators import gcs_to_bq


default_args = {
    'owner': 'airflow',
    'start_date': datetime.date,
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG('gcs_bigquery', schedule_interval='@daily', default_args=default_args,
         catchup=False, tags=['naveen']) as dag:
    GCS_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        # Replace ":" with valid character for Airflow task
        task_id='GCS_to_Bigquery_table',
        bucket="gs://naveentest",
        source_objects=["gs://naveentest/iris_data.csv"],
        destination_project_dataset_table="second-pursuit-350605.test",
        source_format='csv',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

GCS_to_BQ
