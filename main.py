from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sklearn.model_selection import train_test_split
import gspread
from google.oauth2.service_account import Credentials

# Funkcja do pobrania danych z Google Sheets
def download_data_from_google_sheets(**kwargs):
    # Użycie podanego sposobu autoryzacji
    json_path = "C:\Users\Natalia\Desktop\airflow-442316-8c5dfa0cf9c0.json"
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(json_path, scopes=scopes)
    client = gspread.authorize(creds)

    try:
        sheet = client.open("Airflow").sheet1
        data = pd.DataFrame(sheet.get_all_records())
        kwargs['ti'].xcom_push(key='data', value=data.to_json())
    except gspread.exceptions.SpreadsheetNotFound:
        print("Arkusz nie został znaleziony. Sprawdź nazwę i dostęp.")
    except gspread.exceptions.APIError as e:
        print(f"Problem z API: {e}")
    except Exception as e:
        print(f"Nieoczekiwany błąd: {e}")

# Funkcja do podziału danych na treningowe i testowe
def split_data(**kwargs):
    ti = kwargs['ti']
    data = pd.read_json(ti.xcom_pull(key='data'))
    train, test = train_test_split(data, test_size=0.3, random_state=42)
    kwargs['ti'].xcom_push(key='train', value=train.to_json())
    kwargs['ti'].xcom_push(key='test', value=test.to_json())

# Funkcja do zapisu danych do Google Sheets
def upload_data_to_google_sheets(**kwargs):
    # Użycie podanego sposobu autoryzacji
    json_path = "airflow-442316-8c5dfa0cf9c0.json"
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",  # Zakres dla Google Sheets
        "https://www.googleapis.com/auth/drive"          # Zakres dla Google Drive
    ]
    creds = Credentials.from_service_account_file(json_path, scopes=scopes)
    client = gspread.authorize(creds)

    train = pd.read_json(kwargs['ti'].xcom_pull(key='train'))
    test = pd.read_json(kwargs['ti'].xcom_pull(key='test'))

    try:
        # Zapis zbioru treningowego
        train_sheet = client.create("Zbiór modelowy").sheet1
        train_sheet.update([train.columns.values.tolist()] + train.values.tolist())

        # Zapis zbioru testowego
        test_sheet = client.create("Zbiór douczeniowy").sheet1
        test_sheet.update([test.columns.values.tolist()] + test.values.tolist())
    except gspread.exceptions.APIError as e:
        print(f"Problem z API: {e}")
    except Exception as e:
        print(f"Nieoczekiwany błąd: {e}")

# Definicja DAG
default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}
with DAG(
    dag_id='dag_1_data_split',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    task_download = PythonOperator(
        task_id='download_data_from_google_sheets',
        python_callable=download_data_from_google_sheets
    )

    task_split = PythonOperator(
        task_id='split_data',
        python_callable=split_data
    )

    task_upload = PythonOperator(
        task_id='upload_to_google_sheets',
        python_callable=upload_data_to_google_sheets
    )

    task_download >> task_split >> task_upload