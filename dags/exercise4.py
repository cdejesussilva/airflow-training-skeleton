from locale import currency

import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)

from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)

from other import HttpToGcsOperator


args = {
    "owner": "Catia",
    "start_date": airflow.utils.dates.days_ago(3),
}

dag = DAG(dag_id="exercise4",
          default_args=args,
          schedule_interval="0 0 * * *",
)


#Postgres to GCS implementation
pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="airflow-training-catia",
    filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
    postgres_conn_id="catia_airflow_training",
    dag=dag,
)


HttpToGcsOperator(
    task_id="get_currency_" + currency(),
    method="GET",
    endpoint="/airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=" + currency(),
    http_conn_id="airflow-training-currency-http",
    gcs_path="currency/{{ ds }}-" + currency() + ".json",
    gcs_bucket="airflow-training-data",
    dag=dag,
)



#Compute aggregates with Dataproc
dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-9c7b4dddd139902f",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

compute_aggregates = DataProcPySparkOperator(
    task_id="compute_aggregates",
    main="../other/build_statistics.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=["{{ ds }}"],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-9c7b4dddd139902f",
    dag=dag,
)


[pgsl_to_gcs,HttpToGcsOperator] >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster


