from airflow.sensors.bash import BashSensor
from airflow.operators.bash import BashOperator
from airflow import DAG
import pendulum

#sensor의 개념 : 
#1.일종의 특화된 오퍼레이터
#2.특정 조건이 만족되기를 기다리고 만족되면 True를 반환하는 Task
#3.모든 센서는 BaseSensorOperator를 상속하여 구현되며(BaseSensorOperator는 BaseOperator를 상속함)
#  상속시에는 __init()__ 함수와 poke(context) 함수 재정의 필요
#4.센싱하는 로직은 poke 함수에 정의

#https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/base/index.html

#Pool : 
#모든 Task는 특정 Pool에서 수행되며 Pool은 Slot이라는 것을 가지고 있음.
#기본적으로 Task 1개당 Slot 1개를 점유하며 Pool을 지정하지 않으면 default_pool에서 수행

with DAG(
    dag_id='dags_bash_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='0 6 * * *',
    catchup=False
) as dag:

    sensor_task_by_poke = BashSensor(
        task_id='sensor_task_by_poke',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command=f'''echo $FILE && 
                        if [ -f $FILE ]; then 
                              exit 0
                        else 
                              exit 1
                        fi''',
        poke_interval=30,      #30초
        timeout=60*2,          #2분
        mode='poke',
        soft_fail=False
    )

    sensor_task_by_reschedule = BashSensor(
        task_id='sensor_task_by_reschedule',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command=f'''echo $FILE && 
                        if [ -f $FILE ]; then 
                              exit 0
                        else 
                              exit 1
                        fi''',
        poke_interval=60*3,    # 3분
        timeout=60*9,          #9분
        mode='reschedule',
        soft_fail=True
    )

    bash_task = BashOperator(
        task_id='bash_task',
        env={'FILE': '/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command='echo "건수: `cat $FILE | wc -l`"',
    )

    [sensor_task_by_poke,sensor_task_by_reschedule] >> bash_task