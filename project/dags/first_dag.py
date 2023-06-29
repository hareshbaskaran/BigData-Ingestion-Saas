from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_sql_operator import SparkSqlOperator
from airflow.operators.dummy_operator import DummyOperator
default_args = {
  'start_date': datetime(2023, 6, 24, 1),
  'owner': 'airflow'
}
with DAG(dag_id='sample_360',
          schedule_interval=timedelta(hours=2),
          default_args=default_args) as dag:
  start = DummyOperator(task_id='start_etl_airflow')
  extract = DatabricksRunNowOperator(
    task_id = 'extract_bigquery',
    databricks_conn_id = 'databricks_default',
    job_id = "309433312561047",
    notebook_params={
        'input_table': 'ga_sessions_*',
    },
    dag=dag
  )
  transform = DatabricksRunNowOperator(
    task_id = 'transform_ts',
    databricks_conn_id = 'databricks_default',
    job_id = "302678166028164",
    notebook_params={
        'transform': 'timeseries'
    },
    dag=dag
  )
  load = DatabricksRunNowOperator(
    task_id = 'load_postgre',
    databricks_conn_id = 'databricks_default',
    job_id = "853797615576758",
    notebook_params={
        'output_table': 'sample_analytics'
    },
    dag=dag
  )
  stop = DummyOperator(task_id='loaded_postgresql')
  start>>extract>>transform>>load>>stop