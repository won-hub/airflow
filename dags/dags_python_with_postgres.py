from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

#postgres insert(문제점 → 해결 방법):
#접속 정보 노출(postgres DB에 대한 User, Password 등) 위험 → Variable 이용 (User, Password 등을 Variable에 등록하고 꺼내오기)
#접속 정보 변경시 대응 어려움 → Hook 이용 (Variable 등록 필요없음)

#Connection : Airflow UI 화면에서 등록한 커넥션 정보

#Hook의 개념 : Airflow에서 외부 솔루션의 기능을 사용할 수 있도록 미리 구현된 메서드를 가진 클래스
#Hook의 특징 :
#(1) Connection 정보를 통해 생성되는 객체 → 접속 정보를 Connection을 통해 받아오므로 접속 정보가 코드상 노출되지 않음
#(2) 특정 솔루션을 다룰 수 있는 메서드가 구현되어 있음.
#(3) 오퍼레이터나 센서와는 달리 Hook은 task를 만들어내지 못하므로 Custom 오퍼레이터 안에서나 Python 오퍼레이터 내 함수에서 사용됨

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    
    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['172.28.0.3', '5432', 'hjkim', 'hjkim', 'hjkim']
    )
        
    insrt_postgres