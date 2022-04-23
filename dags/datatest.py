import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 23),
    'email': ['mark@coder.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('datafile', default_args=default_args)


def task_read_la_911_2021_data():
  print("reading data from Los Angeles 911 activity")
  la_911_data=pd.read_json("https://data.lacity.org/resource/cibt-wiru.json")
  la_911_data.to_csv("../sets/la_911_2021_data.csv")

def task_read_la_911_2020_data():
  print("reading data from Los Angeles 911 activity")
  la_911_data=pd.read_json("https://data.lacity.org/resource/84iq-i2r6.json")
  la_911_data.to_csv("../sets/la_911_2020_data.csv")

 
def task_merge():
   print("2021 911 call types")
   data_2021=pd.read_csv('../sets/la_911_2021_data.csv')
   calltype_2021=data_2021['call_type_text'].value_counts(sort=True, ascending=True).to_frame()
   print("2020 911 call types")
   data_2020=pd.read_csv('../sets/la_911_2020_data.csv')
   calltype_2020=data_2021['call_type_text'].value_counts(sort=True, ascending=True).to_frame()
   m=pd.concat([calltype_2021,calltype_2020],axis=1)
   m.columns=['year2021','year2020']
   m.to_csv('../sets/la_911_2021_2020_calltypes.csv')


t1 = BashOperator(
    task_id='read_json_2021',
    python_callable=task_read_la_911_2021_data(),
    bash_command='python3 ~/airflow/dags/datatest.py',
    dag=dag)

t2 = BashOperator(
    task_id='read_json_2020',
    python_callable=task_read_la_911_2020_data(),
    bash_command='python3 ~/airflow/dags/datatest.py',
    dag=dag)

t3 = BashOperator(
    task_id='merge',
    python_callable=task_merge(),
    bash_command='python3 ~/airflow/dags/datatest.py',
    dag=dag)

t3.set_upstream(t1)
t3.set_upstream(t2)