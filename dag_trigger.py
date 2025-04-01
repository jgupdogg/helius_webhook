# dag_trigger.py
import os
import logging
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta

# Global variable to track the time of the last DAG trigger.
last_trigger_time = None

def trigger_helius_dag(conf={}):
    """
    Triggers the Airflow DAG by calling its REST API endpoint.
    """
    dag_run_endpoint = "http://localhost:8080/api/v1/dags/helius_webhook_notification_dag/dagRuns"
    airflow_username = os.getenv("AIRFLOW_USERNAME", "admin")
    airflow_password = os.getenv("AIRFLOW_PASSWORD", "password")
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Generate a unique dag_run_id with the current UTC time.
    time_str = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    dag_run_id = f"webhook_trigger_{time_str}"
    
    data = {
        "dag_run_id": dag_run_id,
        "conf": conf
    }
    
    try:
        response = requests.post(
            dag_run_endpoint,
            json=data,
            headers=headers,
            auth=HTTPBasicAuth(airflow_username, airflow_password),
        )
        if response.status_code in [200, 201]:
            logging.info("Successfully triggered Airflow DAG")
        else:
            logging.error("Failed to trigger Airflow DAG. Status code: %s, response: %s",
                          response.status_code, response.text)
    except Exception as e:
        logging.error("Exception while triggering Airflow DAG: %s", e)

def maybe_trigger_dag(conf={}):
    """
    Triggers the DAG only if at least one minute has passed since the last trigger.
    """
    global last_trigger_time
    now = datetime.utcnow()
    if last_trigger_time is not None and (now - last_trigger_time) < timedelta(minutes=1):
        logging.info("DAG trigger skipped: triggered less than a minute ago.")
        return
    last_trigger_time = now
    trigger_helius_dag(conf)
