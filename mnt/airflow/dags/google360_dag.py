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

  etl_pipeline = DatabricksRunNowOperator(
    task_id = 'spark_aggregation',
    databricks_conn_id = 'databricks_default',
    job_id = "556758442665706",
    notebook_params={
        'input_table': 'ga_sessions_*',
    },
    dag=dag
  )
  db_instance = DatabricksRunNowOperator(
    task_id = 'mysql_gcp',
    databricks_conn_id = 'databricks_default',
    job_id = "863822731568199",
    notebook_params={
        'output_table': 'ga_daily'
    },
    dag=dag
  )
  etl_pipeline>>db_instance