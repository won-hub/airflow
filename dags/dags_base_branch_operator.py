from airflow import DAG
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

#Task 분기 처리 : 
#3.BaseBranchOperator 상속하여 직접 개발(3보다 1,2 방법을 주로 사용)
#공통적으로 리턴 값으로 후행 Task의 id 를 str 또는 list로 리턴

with DAG(
    dag_id='dags_base_branch_operator',  
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    class CustomBranchOperator(BaseBranchOperator): #BaseBranchOperator 사용 시 choose_branch 함수 구현
        def choose_branch(self, context): #choose_branch -> context 파라미터 사용(이름 변경X)
            import random
            print(context)
            
            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B','C']:
                return ['task_b','task_c']

    
    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')

    
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

    custom_branch_operator >> [task_a, task_b, task_c]