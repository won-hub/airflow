from airflow import DAG
import pendulum
import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_conn_test",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    t1 = EmptyOperator(
        task_id="t1"
    )

    t2 = EmptyOperator(
        task_id="t2"
    )

    t3 = EmptyOperator(
        task_id="t3"
    )

    t4 = EmptyOperator(
        task_id="t4"
    )

    t5 = EmptyOperator(
        task_id="t5"
    )

    t6 = EmptyOperator(
        task_id="t6"
    )

    t7 = EmptyOperator(
        task_id="t7"
    )

    t8 = EmptyOperator(
        task_id="t8"
    )

    #Task 연결 방법
    #1.>>, << (Airflow 공식 추천 방식)
    #2.함수 사용하기

    t1 >> [t2, t3] >> t4
    t5 >> t4 
    [t4, t7] >> t6 >> t8