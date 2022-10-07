from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from fetch_youtube_channel import get_videos
from crawler_class import GlueTriggerCrawlerOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

default_args = {
    'owner':'airflow-oumaima',
    'depends_on_past':'false',
    'start_date':datetime(2022,9,8),
    'email':['oumaimaidhika2001@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes = 5),
    'schedule_interval': '@weekly',
    "depends_on_past": True 

}

dag = DAG(
 'main_dag',
 default_args = default_args,
 description = 'analytics data pipeline'
)

fetch_channel_1 = PythonOperator(
 task_id="fetch_channel_1",
 python_callable=get_videos,
 op_kwargs={
 "channel_name": "Tina Huang",
 "channel_id": "UC2UXDak6o7rBm23k3Vv5dww",
 },
 dag = dag
)
fetch_channel_2 = PythonOperator(
 task_id="fetch_channel_2",
 python_callable=get_videos,
 op_kwargs={
 "channel_name": "Kin Jee",
 "channel_id": "UCiT9RITQ9PW6BhXK0y2jaeg",
 },
 dag = dag
)

trigger_crawler = GlueTriggerCrawlerOperator(
 aws_conn_id="s3_connection",
 task_id="trigger_crawler",
 crawler_name="input_data_crawler",
 region_name = "us-east-1",
 dag =dag
)

query_data = AthenaOperator(
 task_id='query_data',
 aws_conn_id='s3_connection',
 database='channels_db',
 query= 'SELECT channel_name , video_id,video_title,view_count,like_count , comment_count FROM "channels_db"."input_data" WHERE view_count >= 1000  ORDER BY view_count DESC ',
 output_location='s3://youtube-fetched-data/output_data/',
 dag = dag
)

fetch_channel_1 >> fetch_channel_2 >> trigger_crawler >> query_data


