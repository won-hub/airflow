from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

#Email 오퍼레이터 

#1.사전 설정 작업(Google)
#구글메일서버사용 
#G-mail → 설정 → 모든 설정 보기 → 전달 및 POP/IMAP →  IMAP사용
#구글계정관리 → 보안 → 2단계 인증 → 앱 비밀번호 셋팅

#2.사전 설정 작업(Airflow)
#Docker-compose.yaml 편집 (environment 항목에 추가)
#AIRFLOW__SMTP__SMTP_HOST : 'smtp.gmail.com'
#AIRFLOW__SMTP__SMTP_USER : '{gmail계정}'
#AIRFLOW__SMTP__SMTP_PASSWORD : '{앱비밀번호}'
#AIRFLOW__SMTP__SMTP_PORT : 587
#AIRFLOW__SMTP__SMTP_MAIL_FROM : '{gmail 계정}'

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='moxnox764@gmail.com',
        subject='Airflow 성공메일',
        html_content='Airflow 작업이 완료되었습니다'
    )