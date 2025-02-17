from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
	'owner':'emeli',
	'retries':2,
	'retry_delay':timedelta(minutes=2)

}

def python_function():
	#import pandas as pd
	#print(f"pandas version: {pd.__version__}")
	return("Hello from python_function")

def pytorch_version():
	import torch
	print(f"pytorch version: {torch.__version__}")

with DAG(
	dag_id='python_external_library_dag',
	description='This dag uses pandas library!',
	default_args=default_args,
	start_date=datetime(2025,2,10),
	schedule_interval='@daily',

	) as dag:
		task1 = PythonOperator(
			task_id='say_hello',
			python_callable=python_function
			)

		task2 = PythonOperator(
			task_id='print_pytorch_version',
			python_callable=pytorch_version
			)

		task1
		task2