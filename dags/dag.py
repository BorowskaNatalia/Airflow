from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Funkcja przykładowa, którą wykonamy w PythonOperatorze
def my_function():
    print("Hello from Airflow!")

# Definiowanie DAG-a
with DAG(
        "my_first_dag",
        default_args={"owner": "airflow", "start_date": datetime(2023, 1, 1)},
        schedule_interval="@daily",
        catchup=False,
) as dag:

    start = DummyOperator(task_id="start")
    python_task = PythonOperator(task_id="python_task", python_callable=my_function)
    end = DummyOperator(task_id="end")

    start >> python_task >> end
