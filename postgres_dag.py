import datetime

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import random
import airflow
# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'end_date': datetime.now(),
    'depends_on_past': False,
    'email': ['naveen@saturam.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1, 'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_operator_dag",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    x = 0

    def my_func():
        x = random.choice([1, 2])
        print(x)
        return x

    random_number = PythonOperator(
        task_id='random_number',
        python_callable=my_func
    )

    # create_odd_table = PostgresOperator(
    # task_id='odd_table',
    # sql="""
    #     CREATE TABLE IF NOT EXISTS ODD (
    #     pet_id SERIAL PRIMARY KEY,
    #     name VARCHAR NOT NULL,
    #     pet_type VARCHAR NOT NULL,
    #     birth_date DATE NOT NULL,
    #     OWNER VARCHAR NOT NULL);
    #     """,
    #     postgres_conn_id="postgres_default"
    # )
    # create_even_table = PostgresOperator(
    # task_id='even_table',
    # sql="""
    #     CREATE TABLE IF NOT EXISTS Even (
    #     pet_id SERIAL PRIMARY KEY,
    #     name VARCHAR NOT NULL,
    #     pet_type VARCHAR NOT NULL,
    #     birth_date DATE NOT NULL,
    #     OWNER VARCHAR NOT NULL);
    #     """,
    #     postgres_conn_id="postgres_default"
    # )

    # employee_task = PostgresOperator(
    #     task_id='emp_table',
    #     sql="""
    #     CREATE TABLE employees(
    #     emp_id SERIAL PRIMARY KEY,
    #     first_name VARCHAR(50),
    #     last_name VARCHAR(50),
    #     dob DATE,
    #     city VARCHAR(40));
    #     """,
    #     postgres_conn_id="postgres_default"
    # )
    csv_load = PostgresOperator(
        task_id='csv_load',
        sql="""
        INSERT INTO employees
        VALUES (3, 'Adhi', 'Sundar','1995-05-01', 'Coimbatore')""",
        postgres_conn_id="postgres_default"
    )

    send_email = EmailOperator(
        task_id='send_email',
        to='naveenbharathic@gmail.com',
        subject='ingestion complete - Email Operator',
        html_content=f"""<br>This is to inform that DAG has run successfully</br>""",
        dag=dag)


random_number >> csv_load >> send_email
