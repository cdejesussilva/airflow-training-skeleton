from locale import currency
from tempfile import NamedTemporaryFile

import airflow
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import DAG, BaseOperator
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)

from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)
from airflow.utils.decorators import apply_defaults

args = {
    "owner": "Catia",
    "start_date": airflow.utils.dates.days_ago(3),
}

dag = DAG(dag_id="exercise4",
          default_args=args,
          schedule_interval="0 0 * * *",
)


class HttpToGcsOperator(BaseOperator):
    """    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """
    template_fields = ("endpoint", "gcs_path")
    template_ext = ()
    ui_color = "#f4a460"

    @apply_defaults
    def __init__(
            self,
            endpoint,
            gcs_bucket,
            gcs_path,
            method="GET",
            http_conn_id="http_default",
            gcs_conn_id="google_cloud_default",
            *args,
            **kwargs
    ):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.gcs_conn_id = gcs_conn_id

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint)

        with NamedTemporaryFile() as tmp_file_handle:
            tmp_file_handle.write(response.content)
            tmp_file_handle.flush()

            hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)
            hook.upload(
                bucket=self.gcs_bucket,
                object=self.gcs_path,
                filename=tmp_file_handle.name,
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


get_currency_EUR = HttpToGcsOperator(
    task_id="get_currency_EUR",
    method="GET",
    endpoint="/airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=EUR",
    http_conn_id="https://europe-west1-gdd-airflow-training.cloudfunctions.net",
    gcs_path="currency/{{ ds }}-EUR.json",
    gcs_bucket="airflow-training-catia",
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
    main="gs://airflow-training-catia/build_statistics.py",
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


[pgsl_to_gcs,get_currency_EUR] >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster


