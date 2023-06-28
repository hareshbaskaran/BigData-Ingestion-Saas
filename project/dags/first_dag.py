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