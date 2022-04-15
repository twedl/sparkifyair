from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                        LoadDimensionOperator, DataQualityOperator)
from helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 12, hour = 0),
    'catchup': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    jsonpath="log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    jsonpath="",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    dag=dag,
    sql=SqlQueries.songplay_table_insert
) 

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    source_table="staging_events",
    sql=SqlQueries.user_table_insert,
    primary_key="userid",
    delete_or_upsert="delete",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    source_table="staging_songs",
    sql=SqlQueries.song_table_insert,
    primary_key="song_id",
    delete_or_upsert="delete",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    source_table="staging_songs",
    sql=SqlQueries.artist_table_insert,
    primary_key="artist_id",
    delete_or_upsert="upsert",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    source_table="songplays",
    sql=SqlQueries.time_table_insert,
    primary_key="start_time",
    delete_or_upsert="upsert",
    dag=dag
)

run_user_quality_checks = DataQualityOperator(
    task_id='Run_user_quality_checks',
    redshift_conn_id="redshift",
    table="users",
    dag=dag
)

run_song_quality_checks = DataQualityOperator(
    task_id='Run_song_quality_checks',
    redshift_conn_id="redshift",
    table="songs",
    dag=dag
)

run_artist_quality_checks = DataQualityOperator(
    task_id='Run_artist_quality_checks',
    redshift_conn_id="redshift",
    table="artists",
    dag=dag
)

run_time_quality_checks = DataQualityOperator(
    task_id='Run_time_quality_checks',
    redshift_conn_id="redshift",
    table="time",
    dag=dag
)

run_songplays_quality_checks = DataQualityOperator(
    task_id='Run_songplays_quality_checks',
    redshift_conn_id="redshift",
    table="songplays",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> run_songplays_quality_checks
run_songplays_quality_checks >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
load_user_dimension_table >> run_user_quality_checks
load_song_dimension_table >> run_song_quality_checks
load_artist_dimension_table >> run_artist_quality_checks
load_time_dimension_table >> run_time_quality_checks
run_user_quality_checks >> end_operator
run_song_quality_checks >> end_operator
run_artist_quality_checks >> end_operator
run_time_quality_checks >> end_operator