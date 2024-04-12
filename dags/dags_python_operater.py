from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator #from 패키지 명 import 오퍼레이터 명(클래스) : 정의된 파이썬 함수를 실행시킴
import random

#Airplow : 
#데이터 파이프라인을 관리하기 위한 오픈 소스 플랫폼 -> 데이터 파이프라인 : 데이터의 흐름을 관리하고 데이터 처리 작업을 자동화
#작업(Task)들 간의 종속성을 정의하고 예약된 작업을 실행, 작업의 상태와 진행 상황을 모니터링

#airflow.operators.python 패키지의 Operater
#1.PythonOperator : 어떤 파이썬 함수를 실행시키기 위한 Operater.
#2.BranchPythonOperator : 파이썬 함수 실행 결과에 따라 Task를 선택적으로 실행
#3.ShortCircuitOperator : 파이썬 함수 실행 결과에 따라 후행 Task를 실행하지 않고 종료
#4.PythonVirtualenvOperator : 파이썬 가상환경 생성 후 Job 수행하고 마무리되면 가상 환경을 삭제
#5.ExternalPythonOperator : 기존에 존재하는 파이썬 가상환경에서 Job를 수행하게 함

#Operater : 특정 행위를 할 수 있는 기능을 모아놓은 클래스 설계도
#Task : operater에서 객체화 실행 가능한 Object 

#DAG(Directed Acyclic Graph) : Airflow의 workflow, Task의 집합체

#Task의 수행 주체 : 
#1.스케줄러 : DAG Parsing 후 DB에 정보 저장,  DAG 시작 시간 결정
#2.워커 : 실제 작업 수행

#Cron 스케줄 : 
#Task가 실행되어야 하는 시간(주기)을 정하기 위한 다섯 개의 필드로 구성된 문자열
#{분} {시} {일} {월} {요일} 

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    def select_fruit():
        fruit = ["APPLE", "BANANA", "ORANGE", "AVOCADO"] 
        rand_int = random.randint(0,3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id="py_t1",
        python_callable=select_fruit
    )

    py_t1