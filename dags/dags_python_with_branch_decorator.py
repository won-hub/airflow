from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task

#Task 분기 처리 : 
#2.task.branch 데커레이터
#공통적으로 리턴 값으로 후행 Task의 id 를 str 또는 list로 리턴

with DAG(
    dag_id='dags_python_with_branch_decorator',
    start_date=datetime(2023,4,1),
    schedule=None,
    catchup=False
) as dag:
    @task.branch(task_id='python_branch_task')
    def select_random():
        import random
        item_lst = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B','C']:
            return ['task_b','task_c']
    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    select_random() >> [task_a, task_b, task_c]