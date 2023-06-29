from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_sql_operator import SparkSqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from airflow.operators.email_operator import EmailOperator

def handle_failure(context):
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    execution_date = task_instance.execution_date

    email_subject = f"Task Failed: {task_id}"
    email_body = f"The task '{task_id}' failed on {execution_date}."
    to_email = 'hareshbaskaran.work@gmail.com'

    email_task = EmailOperator(
        task_id='send_failure_email',
        to=to_email,
        subject=email_subject,
        html_content=email_body
    )
    email_task.execute(context)

def handle_success(context):
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    execution_date = task_instance.execution_date

    email_subject = f"Task Succeeded: {task_id}"
    email_body = f"The task '{task_id}' succeeded on {execution_date}."
    to_email = 'hareshbaskaran.work@gmail.com'

    email_task = EmailOperator(
        task_id='send_success_email',
        to=to_email,
        subject=email_subject,
        html_content=email_body
    )
    email_task.execute(context)


default_args = {
    'start_date': datetime(2023, 6, 24, 1),
    'owner': 'airflow',
    'on_failure_callback': handle_failure,
    'on_success_callback': handle_success
}

with DAG(dag_id='sample_360',
         schedule_interval=timedelta(hours=3),
         default_args=default_args) as dag:

    start = DummyOperator(task_id='start_etl_airflow')

    extract = DatabricksRunNowOperator(
        task_id='extract_bigquery',
        databricks_conn_id='databricks_default',
        job_id="309433312561047",
        notebook_params={
            'input_table': 'ga_sessions_*',
        },
        dag=dag
    )

    transform = DatabricksRunNowOperator(
        task_id='transform_ts',
        databricks_conn_id='databricks_default',
        job_id="302678166028164",
        notebook_params={
            'transform': 'timeseries'
        },
        dag=dag
    )

    load = DatabricksRunNowOperator(
        task_id='load_postgre',
        databricks_conn_id='databricks_default',
        job_id="853797615576758",
        notebook_params={
            'output_table': 'sample_analytics'
        },
        dag=dag
    )
    handle_failure_task = PythonOperator(
        task_id='handle_failure',
        python_callable=handle_failure,
        provide_context=True
    )

    handle_success_task = PythonOperator(
        task_id='handle_success',
        python_callable=handle_success,
        provide_context=True
    )

    stop = DummyOperator(task_id='loaded_gcpmysql')
    start >> extract >> transform >> load >> handle_success_task >> stop
    extract >> handle_failure_task
    transform>> handle_failure_task
    load>> handle_failure_task


