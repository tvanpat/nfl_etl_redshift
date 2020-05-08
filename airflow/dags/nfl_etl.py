from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'NFL-CAP',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_Query',  dag=dag)

coach_s3_operator = DummyOperator(task_id='Coaches_JSON_saved_s3',  dag=dag)

team_s3_operator = DummyOperator(task_id='Teams_JSON_saved_s3',  dag=dag)

schedule_s3_operator = DummyOperator(task_id='Schedule_Details_JSON_saved_s3',  dag=dag)

gameid_s3_operator = DummyOperator(task_id='Game_ID_JSON_saved_s3',  dag=dag)

statid_s3_operator = DummyOperator(task_id='Stat_ID_JSON_saved_s3',  dag=dag)

gamesheet_s3_operator = DummyOperator(task_id='Game_Record_JSON_saved_s3',  dag=dag)

gameinfo_s3_operator = DummyOperator(task_id='Game_Info_JSON_saved_s3',  dag=dag)

playerid_s3_operator = DummyOperator(task_id='Player_ID_JSON_saved_s3',  dag=dag)

player_s3_operator = DummyOperator(task_id='Player_JSON_saved_s3',  dag=dag)

gameplays_s3_operator = DummyOperator(task_id='Game_Play_JSON_saved_s3',  dag=dag)


stage_coach_to_redshift = StageToRedshiftOperator(
    task_id='staging_coach',
    dag=dag,
    table='staging_coaches',
    redshift='redshift',
    aws_credentials='aws_credentials',
    s3_bucket="nfl-cap",
    s3_key="coaches",
    file_type="json",
    append_table = False
)

stage_team_to_redshift = StageToRedshiftOperator(
    task_id='staging_team',
    dag=dag,
    table='staging_team',
    redshift='redshift',
    aws_credentials='aws_credentials',
    s3_bucket="nfl-cap",
    s3_key="teams",
    file_type="json",
    append_table = False
)

stage_schedule_details_to_redshift = StageToRedshiftOperator(
    task_id='staging_schedule_details',
    dag=dag,
    table='staging_schedule',
    redshift='redshift',
    aws_credentials='aws_credentials',
    s3_bucket="nfl-cap",
    s3_key="schedules_details",
    file_type="json",
    append_table = False
)

stage_game_info_to_redshift = StageToRedshiftOperator(
    task_id='staging_game_info',
    dag=dag,
    table='staging_gameinfo',
    redshift='redshift',
    aws_credentials='aws_credentials',
    s3_bucket="nfl-cap",
    s3_key="gameinfo",
    file_type="json",
    append_table = False
)

stage_statid_to_redshift = StageToRedshiftOperator(
    task_id='staging_statid',
    dag=dag,
    table='staging_statid',
    redshift='redshift',
    aws_credentials='aws_credentials',
    s3_bucket="nfl-cap",
    s3_key="statids",
    file_type="csv",
    append_table = False
)

stage_players_to_redshift = StageToRedshiftOperator(
    task_id='staging_players',
    dag=dag,
    table='staging_player',
    redshift='redshift',
    aws_credentials='aws_credentials',
    s3_bucket="nfl-cap",
    s3_key="player_info",
    file_type="json",
    append_table = False
)

stage_gameplays_to_redshift = StageToRedshiftOperator(
    task_id='staging_gameplays',
    dag=dag,
    table='staging_gameplays',
    redshift='redshift',
    aws_credentials='aws_credentials',
    s3_bucket="nfl-cap",
    s3_key="gameplays",
    json_path="s3://udacity-dend/log_json_path.json",
    file_type="json",
    append_table = False
)


load_gameplays_table = LoadFactOperator(
    task_id='Load_gameplay_fact_table',
    dag=dag,
    table='play_fact',
    sql_statement= SqlQueries.gameplay_table_insert,
    redshift='redshift',
    append_only = False
)

load_coach_dimension_table = LoadDimensionOperator(
    task_id='Load_coach_dim_table',
    dag=dag,
    redshift = 'redshift',
    table= 'coaches_dim',
    sql_statement= SqlQueries.coach_table_insert,
    append_only = 'False'
)

load_team_dimension_table = LoadDimensionOperator(
    task_id='Load_team_dim_table',
    dag=dag,
    redshift = 'redshift',
    table= 'team_dim',
    sql_statement= SqlQueries.team_table_insert,
    append_only = 'False'
)

load_game_dimension_table = LoadDimensionOperator(
    task_id='Load_game_dim_table',
    dag=dag,
    redshift = 'redshift',
    table= 'game_dim',
    sql_statement= SqlQueries.game_table_insert,
    append_only = 'False'
)

load_players_dimension_table = LoadDimensionOperator(
    task_id='Load_players_dim_table',
    dag=dag,
    redshift = 'redshift',
    table= 'players_dim',
    sql_statement= SqlQueries.player_table_insert,
    append_only = 'False'
)


load_statid_dimension_table = LoadDimensionOperator(
    task_id='Load_statid_dim_table',
    dag=dag,
    redshift = 'redshift',
    table= 'statid_dim',
    sql_statement= SqlQueries.statid_table_insert,
    append_only = 'False'
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag, 
    redshift='redshift',
    tables=['play_fact', 'coaches_dim', 'players_dim', ' game_dim', 'team_dim', 'statid_dim']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Dependencies


start_operator >> coach_s3_operator
start_operator >> team_s3_operator
start_operator >> schedule_s3_operator
start_operator >> gameid_s3_operator
start_operator >> statid_s3_operator

coach_s3_operator >> stage_coach_to_redshift
team_s3_operator >> stage_team_to_redshift
schedule_s3_operator >> stage_schedule_details_to_redshift
statid_s3_operator >> stage_statid_to_redshift

gameid_s3_operator >> gamesheet_s3_operator

gamesheet_s3_operator >> gameinfo_s3_operator
gamesheet_s3_operator >> playerid_s3_operator
gamesheet_s3_operator >> gameplays_s3_operator

gameinfo_s3_operator >> stage_game_info_to_redshift

playerid_s3_operator >> player_s3_operator

player_s3_operator >> stage_players_to_redshift
gameplays_s3_operator >> stage_gameplays_to_redshift

stage_coach_to_redshift >> load_coach_dimension_table
stage_team_to_redshift >> load_team_dimension_table
stage_statid_to_redshift >> load_statid_dimension_table
stage_players_to_redshift >> load_players_dimension_table
stage_gameplays_to_redshift >> load_gameplays_table

stage_schedule_details_to_redshift >> load_game_dimension_table
stage_game_info_to_redshift >> load_game_dimension_table


load_game_dimension_table >> run_quality_checks
load_coach_dimension_table >> run_quality_checks
load_team_dimension_table >> run_quality_checks
load_statid_dimension_table >> run_quality_checks
load_players_dimension_table >> run_quality_checks
load_gameplays_table >> run_quality_checks


run_quality_checks >> end_operator