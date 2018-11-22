import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)


args = {
    "owner": "Catia",
    "start_date": airflow.utils.dates.days_ago(3),
}

dag = DAG(dag_id="exercise4",
          default_args=args,
          schedule_interval="0 0 * * *",
)


#Postgres to GCS implementation - step 1
pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="airflow-training-catia",
    filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
    postgres_conn_id="catia_airflow_training",
    dag=dag,
)