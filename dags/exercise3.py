import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
import datetime

args = {"owner": "cdejesussilva", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise3",
    default_args=args,
)

weekday_person_to_email = {
  0: "Bob", #Monday
  1: "Joe", #Tuesday
  2: "Alice1", #Wednesday
  3: "Joe1", #Thursday
  4: "Alice2", #Friday
  5: "Alice3", #Saturday
  6: "Alice4", #Sunday
}
    
def _get_weekday(execution_date,**context):
    return execution_date.strftime("%a")
    
    
print_weekday = PythonOperator(
    task_id='print_weekday',
    provide_context=True,
    python_callable=_get_weekday,
    dag=dag)
   
branching = BranchPythonOperator(task_id="branching",python_callable=_get_weekday,provide_context=True,dag=dag)
print_weekday >> branching
for task in weekday_person_to_email.values():
  #print_weekday >> branching >> DummyOperator(task_id=task,dag=dag)
  branching >> DummyOperator(task_id=task,dag=dag,trigger_rule = TriggerRule.ONE_SUCCESS)
