import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import smtplib


def success_email():
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login("naveen@saturam.com", "Saturam$15")
    message = "This is a test message to tell you that the DAG has run successfully"
    try:
        s.sendmail("naveen@saturam.com", "naveenbharathic@gmail.com", message)
        print('Mail sent successfully')
    except SMTPException:
        print('Mail not sent successfully')

    s.quit()


def failure_email():
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login("naveen@saturam.com", "Saturam$15")
    message = "This is a test message to tell you that the DAG has failed miserably"
    try:
        s.sendmail("naveen@saturam.com", "naveenbharathic@gmail.com", message)
        print('Mail sent successfully')
    except SMTPException:
        print('Mail not sent successfully')

    s.quit()


def get_iris_data():
    try:
        sql_stmt = "SELECT * FROM iris"
        pg_hook = PostgresHook(
            postgres_conn_id='postgres_default',
            schema='falcon'
        )
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(sql_stmt)
        return cursor.fetchall()
    except:
        return ['failure_mail']


def process_iris_data(ti):
    try:
        iris = ti.xcom_pull(task_ids=['get_iris_data'])
        if not iris:
            raise Exception('No data.')

        iris = pd.DataFrame(
            data=iris[0],
            columns=['iris_id', 'iris_sepal_length', 'iris_sepal_width',
                     'iris_petal_length', 'iris_petal_width', 'iris_variety']
        )
        print(iris.agg(['sum', 'min']))
        iris = iris[
            (iris['iris_sepal_length'] > 5) &
            (iris['iris_sepal_width'] == 3) &
            (iris['iris_petal_length'] > 3) &
            (iris['iris_petal_width'] == 1.5)
        ]
        iris = iris.drop('iris_id', axis=1)
        iris.to_csv(index=False, path_or_buf='/home/naveen/af2/iris_data.csv')
    except:
        return ['failure_mail']


with DAG(
    dag_id='postgres_db_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=2, day=1),
    catchup=False
) as dag:

    task_copy_data = task_get_iris_data = PostgresOperator(
        task_id='copy_iris_data',
        postgres_conn_id='postgres_default',
        sql="""COPY iris(iris_sepal_length, iris_sepal_width, iris_petal_length, iris_petal_width, iris_variety)
            FROM '/home/naveen/af2/iris.csv'
            DELIMITER ','
            CSV HEADER;"""

    )
    task_get_iris_data = PythonOperator(
        task_id='get_iris_data',
        python_callable=get_iris_data,
        do_xcom_push=True
    )
    task_process_iris_data = PythonOperator(
        task_id='process_iris_data',
        python_callable=process_iris_data
    )
    task_truncate_table = PostgresOperator(
        task_id='truncate_tgt_table',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE iris_tgt"
    )

    task_send_success_mail = PythonOperator(
        task_id='success_email',
        python_callable=success_email
    )

    task_send_failure_mail = PythonOperator(
        task_id='failure_email',
        python_callable=failure_email
    )


task_copy_data >> task_get_iris_data >> task_process_iris_data >> task_truncate_table >> task_send_success_mail >> task_send_failure_mail
