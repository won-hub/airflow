from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException
import pendulum
from datetime import timedelta
from airflow.models import Variable

#DAG 파라미터 :  DAG 단위로 적용(개별 오퍼레이터에 적용되지 않음) / default_args에 전달하면 안됨

#Base 오퍼레이터 파라미터 : 개별 Task 단위로 적용 
#Task마다 선언해줄 수 있지만 DAG 하위 모든 오퍼레이터에 적용 필요시 default_args를 통해 전달 가능

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_email_on_failure',
    start_date=pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
    catchup=False,
    schedule='0 1 * * *',
    dagrun_timeout=timedelta(minutes=2),
    default_args={
        'email_on_failure': True,
        'email': email_lst
    }
) as dag:
    @task(task_id='python_fail')
    def python_task_func():
        raise AirflowException('에러 발생')
    python_task_func()

    bash_fail = BashOperator(
        task_id='bash_fail',
        bash_command='exit 1',
    )

    bash_success = BashOperator(
        task_id='bash_success',
        bash_command='exit 0',
    )