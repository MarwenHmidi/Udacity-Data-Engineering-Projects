from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


region = 'us-west-2'
redshift_conn_id = 'redshift_conn'
aws_credentials_id = 'aws_credentials'
s3_bucket = 'udacity-dend'
s3_song_key = 'song_data/A/A/A'
s3_log_key = 'log_data/{execution_date.year}/{execution_date.month}'
log_json_path = f's3://{s3_bucket}/log_json_path.json'
data_quality_checks = [
  {'check_query': "SELECT COUNT(*) FROM songplays", 'expected_result': 0, 'comparison': '>'}
  {'check_query': "SELECT COUNT(*) FROM songs", 'expected_result': 0, 'comparison': '>'}
  {'check_query': "SELECT COUNT(*) FROM artists", 'expected_result': 0, 'comparison': '>'}
  {'check_query': "SELECT COUNT(*) FROM time", 'expected_result': 0, 'comparison': '>'}
  {'check_query': "SELECT COUNT(*) FROM users", 'expected_result': 0, 'comparison': '>'}
]


# Default args 
default_args = {
    'owner': 'Marwen',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
    
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *' #or schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id=aws_credentials_id,
    table='staging_events',
    s3_bucket=s3_bucket,
    s3_key=s3_song_key,
    region=region,
    data_format=f"JSON '{log_json_path}'",
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id=aws_credentials_id,
    table='staging_songs',
    s3_bucket=s3_bucket,
    s3_key=s3_song_key,
    region=region,
    data_format="auto",
)




load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,    
    redshift_conn_id=redshift_conn_id,
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)




load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_table',
    dag=dag,   
    redshift_conn_id=redshift_conn_id,
    table="songs",
    sql=SqlQueries.song_table_insert
)


load_user_dimension_table  = LoadDimensionOperator(
    task_id='Load_users_table',
    dag=dag,   
    redshift_conn_id=redshift_conn_id,
    table="users",
    sql=SqlQueries.user_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_table',
    dag=dag,   
    redshift_conn_id=redshift_conn_id,
    table="artists",
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_table',
    dag=dag,   
    redshift_conn_id=redshift_conn_id,
    table="time",
    sql=SqlQueries.time_table_insert
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    checks = data_quality_checks
   
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)





start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator


