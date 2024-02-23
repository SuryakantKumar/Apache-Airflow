from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import csv
import json
import requests


def download_rates():
    ''' Download forex rates according to the currencies we want to watch '''
    
    BASE_URL = "https://gist.github.com/SuryakantKumar/b38be3c4e51846151929d023b1e4c758/raw/"
    ENDPOINTS = {"USD": "api_forex_exchange_usd.json", "EUR": "api_forex_exchange_eur.json"}
    
    with open("/opt/airflow/dags/files/forex_currencies.csv") as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter = ";")
        
        for idx, row in enumerate(reader):
            base = row["base"]
            with_pairs = row["with_pairs"].split(" ")
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {"base": base, "rates": {}, "last_update": indata["date"]}
            
            for pair in with_pairs:
                outdata["rates"][pair] = indata["rates"][pair]
            
            with open("/opt/airflow/dags/files/forex_rates.json", "a") as outfile:
                json.dump(outdata, outfile)
                outfile.write("\n")
                
default_args = {
    "owner": "suryakant",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "suryakant.kumar@icloud.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(dag_id = "forex_data_pipeline", start_date = datetime(2024, 2, 15), schedule_interval = "@daily", default_args = default_args, catchup = False) as dag:
    
    is_forex_rates_available = HttpSensor(task_id = "is_forex_rates_available",
                                          http_conn_id = "forex_api",
                                          endpoint = "SuryakantKumar/b38be3c4e51846151929d023b1e4c758",
                                          response_check = lambda response: "rates" in response.text,
                                          poke_interval = 5,
                                          timeout = 20)
    
    is_forex_currencies_file_available = FileSensor(task_id = "is_forex_currencies_file_available",
                                                    fs_conn_id = "forex_path",
                                                    filepath = "forex_currencies.csv",
                                                    poke_interval = 5,
                                                    timeout = 20)
    
    download_forex_rates = PythonOperator(task_id = "download_forex_rates",
                                          python_callable = download_rates)
    
    save_forex_rates = BashOperator(task_id = "save_forex_rates",
                                    bash_command = """
                                        hdfs dfs -mkdir -p /forex && \
                                        hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
                                    """)