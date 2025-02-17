from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
	'owner':'emeli',
	'retries':2,
	'retry_delay':timedelta(minutes=2)

}

def say_hello():
	value = "Hello, World! This is a Python function!"
	print(value)
	return value

with DAG(
	dag_id='python_dag',
	description='This is a dag to write hello world!',
	default_args=default_args,
	start_date=datetime(2025,2,10,9),
	schedule_interval='@daily',
	catchup=True

	) as dag:
		task1 = BashOperator(
			task_id='hello_from_bash_first',
			bash_command='echo Hello, World! I\'m first'
			)

		task2 = BashOperator(
			task_id='hello_from_bash_second',
			bash_command='echo Hello, World! I\'m second'
			)

		task3 = PythonOperator(
			task_id='hello_from_python',
			python_callable=say_hello
			)

		[task1, task2] >> task3