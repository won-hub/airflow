from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

#전역변수 Variable :
#Airflow 홈페이지 Admin -> Variable 등록 : 실제 Variable의 Key, Value 값은 메타 DB에 저장됨(Variable 테이블)
#협업 환경에서 표준화된 DAG을 만들기 위해 사용 -> 주로 상수(CONST)로 지정해서 사용할 변수들 셋팅
#e.g) base_sh_dir = /opt/airflow/plugins/shell
#e.g) base_file_dir = /opt/airflow/plugins/files
#e.g) email, Alert 메시지를 받을 담당자의 email 주소정보

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    #1.Variable 라이브러리 이용, 파이썬 문법을 이용해 미리 가져오기 : 
    #문제점 -> 스케줄러의 주기적 DAG 파싱 시 Variable.get개수만큼 DB연결을 일으켜 불필요한 부하 발생(스케줄러 과부하 원인 중 하나)
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
    task_id="bash_var_1",
    bash_command=f"echo variable:{var_value}"
    )

    #2.Jinja 템플릿 이용, 오퍼레이터 내부에서 가져오기(권고)
    bash_var_2 = BashOperator(
    task_id="bash_var_2",
    bash_command="echo variable:{{var.value.sample_key}}"
    )