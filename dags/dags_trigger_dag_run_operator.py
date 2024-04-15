# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

#DAG 간 의존관계 설정
#1.TriggerDagRun 오퍼레이터 : 실행할 다른 DAG의 ID를 지정하여 수행 -> Trigger 되는 DAG의 선행 DAG이 하나만 있을 경우 사용
#2.ExternalTask 센서 : 본 Task가 수행되기 전 다른 DAG의 완료를 기다린 후 수행 -> Trigger 되는 DAG의 선행 DAG이 2개 이상인 경우 사용

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"',
    )

    #TriggerDagRun 오퍼레이터 - run_id : DAG의 수행 방식과 시간을 유일하게 식별해주는 키 
    #같은 시간이라 해도 수행 방식(Schedule, manual, Backfill)에 따라 키가 달라짐
    #스케줄에 의해 실행된 경우 scheduled__{{data_interval_start}} 값을 가짐

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task', #task_id, trigger_dag_id : 필수값
        trigger_dag_id='dags_python_operator', #어떤 DAG을 트립할 것인지 
        trigger_run_id=None, #run_id 값 직접 지정점
        execution_date='{{data_interval_start}}', #manual_{{execution_date}}로 수행
        reset_dag_run=True, #이미 run_id 값이 있는 경우에도 재수행할 것인지
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
        )

    start_task >> trigger_dag_task