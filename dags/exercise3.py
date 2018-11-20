import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
import datetime

args = {"owner": "cdejesussilva", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise3",
    default_args=args,
)

weekday_person_to_email = {
  0: "Bob", #Monday
  1: "Joe", #Tuesday
  2: "Alice", #Wednesday
  3: "Joe", #Thursday
  4: "Alice", #Friday
  5: "Alice", #Saturday
  6: "Alice", #Sunday
}
    
def _get_weekday(execution_date,**context):
    return execution_date.strftime("%a")
    
    
print_weekday = PythonOperator(
    task_id='print_weekday',
    provide_context=True,
    python_callable=_get_weekday,
    dag=dag)
   
    
branching = BranchPythonOperator(task_id="branching",python_callable=_get_weekday,provide_context=True,dag=dag)

for task in weekday_person_to_email.values():
  print_weekday >> branching >> DummyOperator(task_id=task,dag=dag)
