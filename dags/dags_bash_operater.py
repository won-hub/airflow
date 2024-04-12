from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

#Bash Operater : 쉘 스크립트 명령을 수행하는 operater
#Bash Operater가 Template를 사용할 수 있는 파라미터 : 
#-> bash_command (str)
#-> env (dict[str, str] | None)
#-> append_env (bool)
#-> output_encoding (str)
#-> skip_exit_code (int)
#-> cwd (str | None)

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo #HOSTNAME",
    )

    bash_t1 >> bash_t2