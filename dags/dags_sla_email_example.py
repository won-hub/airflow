from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from airflow.models import Variable

#DAG 파라미터 :  DAG 단위로 적용(개별 오퍼레이터에 적용되지 않음) / default_args에 전달하면 안됨

#Base 오퍼레이터 파라미터 : 개별 Task 단위로 적용 
#Task마다 선언해줄 수 있지만 DAG 하위 모든 오퍼레이터에 적용 필요시 default_args를 통해 전달 가능

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_sla_email_example',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    schedule='*/10 * * * *',
    catchup=False,
    default_args={
        'sla': timedelta(seconds=70),
        'email': email_lst
    }
) as dag:
    
    task_slp_30s_sla_70s = BashOperator(
        task_id='task_slp_30s_sla_70s',
        bash_command='sleep 30'
    )
    
    task_slp_60_sla_70s = BashOperator(
        task_id='task_slp_60_sla_70s',
        bash_command='sleep 60'
    )

    task_slp_10s_sla_70s = BashOperator(
        task_id='task_slp_10s_sla_70s',
        bash_command='sleep 10'
    )

    task_slp_10s_sla_30s = BashOperator(
        task_id='task_slp_10s_sla_30s',
        bash_command='sleep 10',
        sla=timedelta(seconds=30)
    )

    task_slp_30s_sla_70s >> task_slp_60_sla_70s >> task_slp_10s_sla_70s >> task_slp_10s_sla_30s