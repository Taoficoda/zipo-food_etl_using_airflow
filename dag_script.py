from datetime import datetime , timedelta

from airflow import DAG
from airflow.operator.python_operator import PythonOperator
from Extraction import run_extraction
from Transformation import run_transformation
from loading import run_loading



# Set default airflow argument
default_args = {
    'owner': airflow,
    'depends_on_past' : False,
    'start_date':  datetime(2024,11,8),
    'email' : "taofeekaribisala@gmail.com",
    'email_on_retry': True,
    'retires' : 1,
    'retries_delay' : timedelta(minutes=1)
)

dag = DAG(
    'zipco_food_pipeline',
    default_args =default_args,
    description= 'This represents Zipco Foods Data Management pipeline'
)

extraction= PythonOperator(
    task_id = 'extraction_layer',
    python_callable = run_extraction,
    dag=dag
)


transformation= PythonOperator(
    task_id = 'transformation',
    python_callable = run_transformation,
    dag=dag,
)

loading= PythonOperator(
    task_id = 'loading',
    python_callable = run_loading,
    dag=dag,
)

extraction >> transformation >> loading