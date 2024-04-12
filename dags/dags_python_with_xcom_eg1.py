from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

#Xcom(Cross Communication) :
#1.Airflow DAG 안 Task 간 데이터 공유를 위해 사용되는 기술
#ex)Task1의 수행 중 내용이나 결과를 Task2에서 사용 또는 입력으로 주고 싶은 경우
#2.주로 작은 규모의 데이터 공유를 위해 사용
#(Xcom 내용은 메타 DB의 xcom 테이블에 값이 저장됨)
#1GB 이상의 대용량 데이터 공유를 위해서는 외부 솔루션 사용 필요(AWS S3, HDFS 등)

#사용 방법
#1.**kwargs에 존재하는 ti(task_instance) 객체 활용

with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_1")
        ti.xcom_push(key="result2", value=[1,2,3])

    @task(task_id='python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2", value=[1,2,3,4])

    @task(task_id='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key="result1")
        value2 = ti.xcom_pull(key="result2", task_ids='python_xcom_push_task1')
        print(value1)
        print(value2)


    xcom_push1() >> xcom_push2() >> xcom_pull() #Task 데커레이터 사용 시 함수 입력/출력 관계만으로 Task flow가 정의됨
