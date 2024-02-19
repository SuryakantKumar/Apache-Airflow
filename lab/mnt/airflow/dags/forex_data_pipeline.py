from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor

from datetime import datetime, timedelta

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