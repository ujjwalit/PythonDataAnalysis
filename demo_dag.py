from tracemalloc import start
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1)
}

dag = DAG('dag_demo', default_args=default_args, schedule_interval=timedelta(days=1))

task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Hello World"',
    dag=dag
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=lambda: print('Hello World'),
    dag=dag
)


start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)


start >> task1 >> task2 >> end


