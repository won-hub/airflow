from airflow import DAG
import pendulum
from airflow.decorators import task

#Task Decorator : 원래의 함수를 감싸서(Wrapping) 바깥에 추가 기능을 덧붙이는 방법

#Python : 
#1.함수 안에 함수를 선언하는 것이 가능
#2.함수의 인자로 함수를 전달하는 것이 가능
#3.함수 자체를 리턴하는 것이 가능

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task_decorator 실행')