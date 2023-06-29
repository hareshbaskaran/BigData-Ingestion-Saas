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
#setup smtm connections in airflow.cfg for evaluation to your account and your device
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
#THE SCHEDULE IS GIVEN IN 4 HOUR INTERVAL SO IT WILL JUNK UP THE MAIL FOR SUCCESS
#HAVE IMPLEMENTED EMAIL OPERATOR ONLY FOR ANY FAILURE

start_date = datetime(2023, 6, 29, 6,30)
#USE THIS FOR DATETIME UNSPECIFIC 
#start_date = datetime.now()
default_args = {
  'start_date': start_date,
  'owner': 'airflow'
}

with DAG(dag_id='sample_analytics',
         schedule_interval=timedelta(hours=4),
         default_args=default_args) as dag:

    start = DummyOperator(task_id='start_etl_airflow')

    extract = DatabricksRunNowOperator(
        task_id='extract_bigquery',
        databricks_conn_id='databricks_default',
        job_id="372533610331023",
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
        job_id="432818082145600",
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
    stop = DummyOperator(task_id='handle_success')
    start >> extract >> handle_failure_task
    extract >> transform >> handle_failure_task
    transform >> load >> stop

       