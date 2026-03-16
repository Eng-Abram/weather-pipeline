from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/workspaces/weather-pipeline/scripts')

default_args = {
    'owner' : 'Abram',
    'retries' : 2,
    'retry_delay': timedelta(minutes=1, seconds= 30)
}

with DAG (
    dag_id= 'weather_update_1',
    description= 'This dag is responsible of updatng the date from the weather API',
    default_args= default_args,
    start_date= datetime(2025,1,1),
    schedule= '*/10 * * * *', # or timedelta(minutes=7)
    catchup= False,
) as dag :
    
    fetch_task = BashOperator(
    task_id='fetch_1',
    bash_command = 'source /workspaces/weather-pipeline/.venv/bin/activate && python /workspaces/weather-pipeline/scripts/fetch_weather.py'
    )

    dbt_run_task = BashOperator(
        task_id = 'dbt_run_1',
        bash_command = 'cd /workspaces/weather-pipeline/weather_dbt && dbt run'
    )

    dbt_test_task = BashOperator(
        task_id = 'dbt_test_1',
        bash_command = 'cd /workspaces/weather-pipeline/weather_dbt && dbt test'
    )

    fetch_task >> dbt_run_task >> dbt_test_task