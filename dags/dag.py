import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import date

dag = DAG(
    dag_id="hello_airflow",
    default_args={
        "owner": "godatadriven",
        "start_date": date.today(),
    },
)

BashOperator(
    task_id="print_exec_date", bash_command="echo 'My new dag!' ", dag=dag
)
