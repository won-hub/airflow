from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

#Task Group(데커레이터&클래스 사용) :
#UI Graph탭에서 Task들을 Group화하여 보여줌 -> Tooltip파라미터를 이용해 UI 화면에서 Task Group에 대한 설명 가능
#Task Group 안에 Task Group을 중첩하여 구성 가능

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or '' 
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번째 그룹입니다. ''' #''' ''' -> docstring : tooltip

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫 번째 TaskGroup내 두 번쨰 task입니다.'}
        )

        inner_func1() >> inner_function2

    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다') as group_2:
        ''' 여기에 적은 docstring은 표시되지 않습니다''' #class 이용 시 docstring이 표시되지 않음
        @task(task_id='inner_function1') #Task Group이 다르면 task_id가 같아도 무방
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': '두 번째 TaskGroup내 두 번째 task입니다.'}
        )
        inner_func1() >> inner_function2

    #Task Group 간에도 Flow 정의 가능       
    group_1() >> group_2