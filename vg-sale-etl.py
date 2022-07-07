# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

def extract_data():
    df = pd.read_csv('gs://vg-data-set/source/vgsales.csv')
    return df

def transform_data(data):
    data = data.dropna(subset=['Year'])
    data['Year'] = data['Year'].apply(int)
    df = data.groupby(['Year','Genre'])['Global_Sales'].sum()
    return df

def load_data(data):
    data.to_csv('gs://vg-data-set/load/vgsales.csv')
    
# extract data 
extracted_data = extract_data()
# transform data
transformed_data = transform_data(extracted_data)
# load data
load_data(transformed_data)



#defining DAG arguments
default_args = {
    'owner': 'Muhammad Eatesaam',
    'start_date': days_ago(0),
    'email': ['eatesaam@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'vg_sales_dag',
    default_args=default_args,
    description='VG Sales ETL DAG',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the extract task
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

# define the transfoem task
transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

# define the loadtask
load = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

# task pipeline
extract >> transform >> load