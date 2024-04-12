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
#2.파이썬 함수의 return 값 활용(1안)

with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success' #return 한 값은 자동으로 xcom에 key='return_value', task_ids=task_id 로 저장됨


    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return') #return 한 값을 꺼낼 때는 key를 명시하지 않아도 됨. (자동으로 key=return_value를 찾음)
        print('xcom_pull 메서드로 직접 찾은 리턴 값:' + value1)

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status)


    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()