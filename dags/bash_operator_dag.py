from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
	'owner':'emeli',
	'retries':2,
	'retry_delay':timedelta(minutes=1)

}

with DAG(
	dag_id='simple_bash_dag',
	description='This is a dag to write hello world!',
	default_args=default_args,
	start_date=datetime(2025,2,10,9),
	schedule_interval='@daily',
	catchup=True

	) as dag:
		task1 = BashOperator(
			task_id='first_task',
			bash_command='echo Hello, World!'
			)

		task1 
