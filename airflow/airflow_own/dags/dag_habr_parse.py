import sys

sys.path.append('/opt/airflow/plugins/')
import TRANSFORM 
import WRITER 
import PARSER 
from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.utils.dates import days_ago
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
import ast


def habr_parser(path_out):
  dict_content = PARSER.RSS_HABR_PARSER().parse_hab() 
  with open(path_out, 'w') as file:
    file.write('{}'.format(dict_content))

def data_transform(path, path_out):
  with open(path, 'r') as file:
    dict_content = ast.literal_eval(file.read())
  TRANSFORM.DATA_PREPARE_TRANSFORM(dict_content).write_to_file(path_out)

def data_to_db(path):
  out = WRITER.POSTGREE_WRITER().sql_start(path)
  update_db_result(out)
  #'./habr_news/'

    
def on_success_callback(context):
    send_message = TelegramOperator(
    task_id = 'send_message_telegram',
    telegram_conn_id = 'telegram_id',
    chat_id = '240216872',
    text='Airflow task is failure: {}'.format(context.get('task_instance').task_id),
    dag=dag)
    return send_message.execute(context=context)
    
    
def update_db_result(context):
    send_message = TelegramOperator(
    task_id = 'send_message_telegram',
    telegram_conn_id = 'telegram_id',
    chat_id = '240216872',
    text='RESULT, BY DB: {}'.format(context),
    dag=dag)
    return send_message.execute(context=context)


with DAG("dag_habr_parse",
         default_args={'owner': 'airflow',
         'reties':'5',
         "retry_delay": timedelta(minutes=5),
         'trigger_rule':'all_success',
         'on_failure_callback':on_success_callback},
         schedule_interval='0 * * * *',
         max_active_runs=1,
         start_date=datetime(2021, 12, 5, 18,25)) as dag:
    habr_parser = PythonOperator(
        task_id='habr_parser',
        python_callable=habr_parser,
        op_kwargs={'path_out':'/opt/airflow/tmp_dir/tmp_file/temp_file'},
    )
    data_transform = PythonOperator(
        task_id='data_transform',
        python_callable=data_transform,
        op_kwargs={'path': '/opt/airflow/tmp_dir/tmp_file/temp_file', 'path_out':'/opt/airflow/tmp_dir/habr_news/file'},
    )
    data_to_db = PythonOperator(
        task_id='data_to_db',
        python_callable=data_to_db,
        op_kwargs={'path':'/opt/airflow/tmp_dir/habr_news/'},
    )

    habr_parser >> data_transform >> data_to_db 