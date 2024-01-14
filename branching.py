from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
import datetime as dt

default_args = {
    "owner": 'airflow',
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=1)
}

dag = DAG(
    default_args=default_args,
    dag_id = 'branching',
    start_date=dt.datetime(2023,5,30),
    schedule=None
)

condition_date = datetime(2023,5,15)

def _before_cond_date():
    print("No Penalty on credit card payment!")

def _after_cond_date():
    print("Penalty of late payment is INR 100 excluding GST")

before_cond_date = PythonOperator(
    task_id='before_cond_date',
    python_callable=_before_cond_date,
    dag = dag
)

after_cond_date = PythonOperator(
    task_id='after_cond_date',
    python_callable=_after_cond_date,
    dag = dag
)

def _pick_process_based_on_date(**context):
    if context['execution_date'] <= condition_date:
        return "before_cond_date"
    else:
        return "after_cond_date"
    
pick_process = BranchPythonOperator(
    task_id = 'pick_process',
    python_callable = _pick_process_based_on_date,
    dag = dag
)

start = EmptyOperator(task_id='start')
end = EmptyOperator(task_id='end', trigger_rule='none_failed')

start >> pick_process >> [before_cond_date, after_cond_date] >> end