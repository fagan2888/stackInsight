from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os



srcDir = '/home/ubuntu/stackInsights/s3'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 6),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'stackins', default_args=default_args)

downloadData= BashOperator(
    task_id='download-urls-list',
    bash_command='python ' + srcDir + 'urls_retrieve.py' ,
    dag=dag)
loaddatatobucket = BashOperator(
    task_id='load-zip-files',
    bash_command=   srcDir + 'transfer.sh ' ,
    dag=dag)

downloadData.set_downstream(loaddatatobucket)
filesconversion = BashOperator(
     task_id='convert-to-xml',
     bash_command='python ' + srcDir + 's3_xml.py',
     dag=dag)
loaddatatobucket.set_downstream(filesconversion)
