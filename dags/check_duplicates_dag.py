import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

# URL do datasetu
DATASET_URL = "https://vincentarelbundock.github.io/Rdatasets/csv/AER/CollegeDistance.csv"
CSV_FILE_PATH = "/opt/airflow/dags/files/CollegeDistance.csv"

# Funkcja do pobierania datasetu
def download_dataset():
    response = requests.get(DATASET_URL)
    with open(CSV_FILE_PATH, "wb") as file:
        file.write(response.content)
    print(f"Dataset downloaded to {CSV_FILE_PATH}")

# Funkcja do sprawdzania i usuwania duplikatów
def check_and_remove_duplicates():
    # Wczytaj dane
    df = pd.read_csv(CSV_FILE_PATH)

    # Sprawdź duplikaty
    num_duplicates = df.duplicated().sum()
    if num_duplicates > 0:
        print(f"Found {num_duplicates} duplicates. Removing them.")
        df = df.drop_duplicates()
        df.to_csv(CSV_FILE_PATH, index=False)
    else:
        print("No duplicates found.")

    print(f"Data processed and saved to {CSV_FILE_PATH}")

# Konfiguracja DAG-a
with DAG(
        "check_duplicates_dag",
        #start_date=datetime(2023, 1, 1),
        schedule_interval=None,
        catchup=False,
) as dag:

    # Zadanie: Pobierz dataset
    download_task = PythonOperator(
        task_id="download_dataset",
        python_callable=download_dataset
    )

    # Zadanie: Sprawdź duplikaty i usuń je
    check_duplicates_task = PythonOperator(
        task_id="check_and_remove_duplicates",
        python_callable=check_and_remove_duplicates
    )

    # Definiowanie przepływu zależności
    download_task >> check_duplicates_task
