from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook

#Custom Hook 개발
#1.get_conn 메서드 구현
# → DB와의 연결 세션 객체인 conn을 리턴하도록 구현
#   (Airflow에서 등록한 Connection 정보를 담은 conn이 아님)
# → BaseHook의 추상 메서드, 자식 클래스에서 구현 필요
#2.bulk_load 메서드 구현 → 사용자 지정

with DAG(
        dag_id='dags_python_with_custom_hook_bulk_load',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'TbCorona19CountStatus_bulk2',
                   'file_nm':'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'}
    )