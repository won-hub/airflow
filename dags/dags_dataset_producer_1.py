from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

#Dataset은 Pub/sub 구조로 DAG간 의존관계를 주는 방법
#Unique한 String 형태의 Key 값을 부여하여 Dataset Publish 
#Dataset을 Consume하는 DAG은 스케줄을 별도로 주지 않고 리스트 형태로 구독할 Dataset 요소들을 명시
#Dataset에 의해 시작된 DAG의 Run_id는 dataset_triggered__{trigger된 시간}으로 표현됨.
#Airflow 화면 메뉴중 Datasets 메뉴를 통해 별도로 Dataset 현황 모니터링 가능

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")

with DAG(
        dag_id='dags_dataset_producer_1',
        schedule='0 7 * * *',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        catchup=False
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_dags_dataset_producer_1],
        bash_command='echo "producer_1 수행 완료"'
    )