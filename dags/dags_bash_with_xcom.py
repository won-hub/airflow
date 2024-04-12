from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

#Bash 오퍼레이터 : env, bash_command 파라미터에서 Template을 이용하여 push/pull

#Bash_command에 의해 출력된 값은 자동으로 return_value로 저장(마지막 출력 문장만)
#return_value를 꺼낼 때는 xcom_pull에서 task_ids 값만 줘도 됨
#키가 지정된 xcom값을 꺼낼 때는 key값만 줘도 됨(단, 다른 task에서 동일 key로 push하지 않았을 때)

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_push = BashOperator(
    task_id='bash_push',
    bash_command="echo START && "
                 "echo XCOM_PUSHED "
                 "{{ ti.xcom_push(key='bash_pushed',value='first_bash_message') }} && "
                 "echo COMPLETE" #마지막 출력문은 자동으로 return_value에 저장
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed') }}",
            'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push') }}"}, #task_ids만 지정하면 key='return_value'를 의미
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE ",
        do_xcom_push=False
    )

    bash_push >> bash_pull