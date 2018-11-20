import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


args = {"owner": "cdejesussilva", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise2",
    default_args=args,
)

BashOperator(
    task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
)

BashOperator(
    task_id="wait_5", bash_command="sleep 5", dag=dag
)

BashOperator(
    task_id="wait_1", bash_command="sleep 1", dag=dag
)

BashOperator(
    task_id="wait_10", bash_command="sleep 10", dag=dag
)

DummyOperator(
            task_id='the_end',
            dag=dag,
        )


print_exec_date >> [wait_5,wait_1,wait_10] >> dummy_1


