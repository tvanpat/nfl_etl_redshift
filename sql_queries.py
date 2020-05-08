import configparser
import os
from dotenv import load_dotenv
load_dotenv()

# Load CONFIG File Not sure if this is required
config = configparser.ConfigParser()
config.read_file(open('vpconfig.cfg'))

# Load Parameters
iam_role = config.get("IAM_ROLE", "ARN")

coach_data = os.getenv('coach_data')
gameinfo_data = os.getenv('gameinfo_data')
gameplays_data = os.getenv('gameplays_data')
player_info_data = os.getenv('player_info_data')
statid_codes_data = os.getenv('statid_codes_data')
schedules_details_data = os.getenv('schedules_details_data')
team_data = os.getenv('team_data')

# DROP TABLES

# Staging Tables
staging_coaches_table_drop = "DROP TABLE IF EXISTS staging_coaches"
staging_gameinfo_table_drop = "DROP TABLE IF EXISTS staging_gameinfo"
staging_gameplays_table_drop = "DROP TABLE IF EXISTS staging_gameplays"
staging_player_table_drop = "DROP TABLE IF EXISTS staging_player"
staging_statid_table_drop = "DROP TABLE IF EXISTS staging_statid"
staging_schedule_table_drop = "DROP TABLE IF EXISTS staging_schedule"
staging_team_table_drop = "DROP TABLE IF EXISTS staging_team"

# Drop Dim and Fact TABLES
coaches_table_drop = "DROP TABLE IF EXISTS coaches_dim"
players_table_drop = "DROP TABLE IF EXISTS players_dim"
game_table_drop = "DROP TABLE IF EXISTS game_dim"
team_table_drop = "DROP TABLE IF EXISTS team_dim"
statid_table_drop = "DROP TABLE IF EXISTS statid_dim"
play_table_drop = "DROP TABLE IF EXISTS play_fact"

# Create staging tables

staging_coaches_table_create = ("""CREATE TABLE IF NOT EXISTS staging_coaches(
coach_id VARCHAR,
season INTEGER,
week VARCHAR,
display_name VARCHAR,
first_name VARCHAR,
last_name VARCHAR,
esbid VARCHAR,
status VARCHAR,
birthdate DATE,
hometown VARCHAR,
college VARCHAR,
team_id VARCHAR,
isdeceased VARCHAR,
pic_url VARCHAR
);
""")

staging_gameinfo_table_create = ("""CREATE TABLE IF NOT EXISTS staging_gameinfo(
game_id VARCHAR,
season INTEGER,
season_type VARCHAR,
week VARCHAR,
game_key VARCHAR,
game_date DATE,
game_time_iso TIMESTAMP,
vis_points_total INTEGER,
vis_points_q1 INTEGER,
vis_points_q2 INTEGER,
vis_points_q3 INTEGER,
vis_points_q4 INTEGER,
vis_points_ot INTEGER,
home_points_total INTEGER,
home_points_q1 INTEGER,
home_points_q2 INTEGER,
home_points_q3 INTEGER,
home_points_q4 INTEGER,
home_points_ot INTEGER,
win_team VARCHAR,
lose_team VARCHAR,
site_id VARCHAR,
site_city VARCHAR,
site_full_name VARCHAR,
site_state VARCHAR,
roof_type VARCHAR,
phase VARCHAR
);
""")

staging_gameplays_table_create = ("""CREATE TABLE IF NOT EXISTS staging_gameplays(
game_id VARCHAR,
week VARCHAR,
drive_seq VARCHAR,
play_id VARCHAR,
play_stat_seq VARCHAR,
season INTEGER,
home_team VARCHAR,
vis_team VARCHAR,
off_team VARCHAR,
def_team VARCHAR,
penalty VARCHAR,
scoring VARCHAR,
scoring_team VARCHAR,
play_type VARCHAR,
quarter VARCHAR,
down INTEGER,
yard_to_go NUMERIC,
first_down VARCHAR,
play_descript VARCHAR(MAX),
play_vid VARCHAR,
stat_id VARCHAR,
yards INTEGER,
player_id VARCHAR,
player_team VARCHAR
);
""")

staging_player_table_create = ("""CREATE TABLE IF NOT EXISTS staging_player(
nfl_id VARCHAR,
esb_id VARCHAR,
gsis_id VARCHAR,
status VARCHAR,
display_name VARCHAR,
first_name VARCHAR,
last_name VARCHAR,
middle_name VARCHAR,
suffix VARCHAR,
birth_date DATE,
home_town VARCHAR,
college_id VARCHAR,
college_name VARCHAR,
position_group VARCHAR,
position VARCHAR,
jersey_number VARCHAR,
height INTEGER,
weight INTEGER,
current_team VARCHAR,
player_pic_url VARCHAR
);
""")

staging_statid_table_create = ("""CREATE TABLE IF NOT EXISTS staging_statid(
stat_id VARCHAR,
name VARCHAR,
comment VARCHAR
);
""")

staging_schedule_table_create = ("""CREATE TABLE IF NOT EXISTS staging_schedule(
game_id VARCHAR,
season INTEGER,
season_type VARCHAR,
week VARCHAR,
game_key VARCHAR,
home_id VARCHAR,
vis_id VARCHAR,
game_type VARCHAR,
week_name_abbr VARCHAR,
week_name VARCHAR
);
""")

staging_team_table_create = ("""CREATE TABLE IF NOT EXISTS staging_team(
team_id VARCHAR,
season INTEGER,
abbr VARCHAR,
citystate VARCHAR,
full_name VARCHAR,
nick VARCHAR,
team_type VARCHAR,
conference_abbr VARCHAR,
division_abbr VARCHAR,
year_found INTEGER,
stadium_name VARCHAR
);
""")

# Create Fact and Dim tables

coach_table_create = ("""CREATE TABLE IF NOT EXISTS coaches_dim(
coach_id VARCHAR,
season INTEGER,
week VARCHAR,
display_name VARCHAR,
first_name VARCHAR,
last_name VARCHAR,
esbid VARCHAR,
status VARCHAR,
birthdate DATE,
hometown VARCHAR,
college  VARCHAR,
team_id VARCHAR,
isdeceased VARCHAR,
pic_url VARCHAR,
PRIMARY KEY (coach_id, season, week)
);
""")

player_table_create = ("""CREATE TABLE IF NOT EXISTS players_dim(
nflid VARCHAR,
esbid VARCHAR,
gsisid VARCHAR,
status VARCHAR,
display_name VARCHAR,
first_name VARCHAR,
last_name VARCHAR,
middle_name VARCHAR,
suffix VARCHAR,
birthdate DATE,
hometown VARCHAR,
college_id VARCHAR,
college VARCHAR,
position_group VARCHAR,
position VARCHAR,
jersey_number VARCHAR,
height INTEGER,
weight INTEGER,
current_team VARCHAR,
player_pic_url VARCHAR,
PRIMARY KEY (nflid)
);
""")

game_table_create = ("""CREATE TABLE IF NOT EXISTS game_dim(
game_id VARCHAR,
season INTEGER,
season_type VARCHAR,
week VARCHAR,
game_key VARCHAR,
game_date DATE,
game_time_iso TIMESTAMP,
vis_points_total INTEGER,
vis_points_q1 INTEGER,
vis_points_q2 INTEGER,
vis_points_q3 INTEGER,
vis_points_q4 INTEGER,
vis_points_ot INTEGER,
home_points_total INTEGER,
home_points_q1 INTEGER,
home_points_q2 INTEGER,
home_points_q3 INTEGER,
home_points_q4 INTEGER,
home_points_ot INTEGER,
win_team VARCHAR,
lose_team VARCHAR,
site_id VARCHAR,
site_city VARCHAR,
site_full_name VARCHAR,
site_state VARCHAR,
roof_type VARCHAR,
game_phase VARCHAR,
week_name_abbr VARCHAR,
week_name VARCHAR,
game_type VARCHAR,
home_id VARCHAR,
away_id VARCHAR,
PRIMARY KEY (game_id)
);
""")

team_table_create = ("""CREATE TABLE IF NOT EXISTS team_dim(
team_id VARCHAR,
season INTEGER,
abbr VARCHAR,
citystate VARCHAR,
full_name VARCHAR,
nick VARCHAR,
team_type VARCHAR,
conference_abbr VARCHAR,
division_abbr VARCHAR,
year_found INTEGER,
stadium_name VARCHAR,
PRIMARY KEY (team_id, season)
);
""")

statid_table_create = ("""CREATE TABLE IF NOT EXISTS statid_dim(
stat_id VARCHAR,
name VARCHAR,
comment VARCHAR,
PRIMARY KEY (stat_id)
);
""")

play_table_create = ("""CREATE TABLE IF NOT EXISTS play_fact(
guid INTEGER IDENTITY(0,1),
game_id VARCHAR,
week VARCHAR,
drive_seq VARCHAR,
play_id VARCHAR,
play_stat_seq VARCHAR,
season INTEGER,
off_team VARCHAR,
def_team VARCHAR,
penalty VARCHAR,
scoring VARCHAR,
scoring_team VARCHAR,
play_type VARCHAR,
quarter VARCHAR,
down INTEGER,
yard_to_go INTEGER,
first_down VARCHAR,
play_descript VARCHAR (MAX),
play_vid VARCHAR,
stat_id VARCHAR,
yards INT,
player_id VARCHAR,
player_team VARCHAR,
PRIMARY KEY (guid)
);
""")

# Copy from S3 to staging
staging_coaches_copy = ("""
    DELETE FROM staging_coaches;
    COPY  staging_coaches FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF STATUPDATE OFF
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto';
""").format(coach_data, iam_role)

staging_gameinfo_copy = ("""
    DELETE FROM staging_gameinfo;
    COPY  staging_gameinfo FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    TIMEFORMAT as 'epochmillisecs'
    COMPUPDATE OFF STATUPDATE OFF
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto';
""").format(gameinfo_data, iam_role)

staging_gameplays_copy = ("""
    DELETE FROM staging_gameplays;
    COPY  staging_gameplays FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF STATUPDATE OFF
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto';
""").format(gameplays_data, iam_role)

staging_player_copy = ("""
    DELETE FROM staging_player;
    COPY  staging_player FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF STATUPDATE OFF
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto';
""").format(player_info_data, iam_role)

staging_statid_copy = ("""
    DELETE FROM staging_statid;
    COPY  staging_statid FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF STATUPDATE OFF
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    CSV;
""").format(statid_codes_data, iam_role)

staging_schedule_copy = ("""
    DELETE FROM staging_schedule;
    COPY  staging_schedule FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF STATUPDATE OFF
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto';
""").format(schedules_details_data, iam_role)

staging_team_copy = ("""
    DELETE FROM staging_team;
    COPY  staging_team FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF STATUPDATE OFF
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto';
""").format(team_data, iam_role)

# Insert into dim tables

coach_table_insert = ('''
INSERT INTO coaches_dim(coach_id, season, week, display_name, first_name, last_name, esbid, status, birthdate, hometown, college,team_id, isdeceased, pic_url)
SELECT DISTINCT coach_id as coach_id,
season as season,
week as week,
display_name as display_name,
first_name  as first_name,
last_name as last_name,
esbid as esbid,
status as status,
birthdate as birthdate,
hometown as hometown,
college as college,
team_id as team_id,
isdeceased as isdeceased,
pic_url as pic_url
FROM staging_coaches
where coach_id IS NOT NULL;
''')

player_table_insert = ('''
INSERT INTO players_dim(nflid, esbid, gsisid, status, display_name, first_name, last_name, middle_name, suffix, birthdate, hometown, college_id, college, position_group, position, jersey_number, height, weight, current_team, player_pic_url)
SELECT DISTINCT nfl_id as nflid,
esb_id as esbid,
gsis_id as gsisid,
status as status,
display_name as display_name,
first_name as first_name,
last_name as last_name,
middle_name as middle_name,
suffix as suffix,
birth_date as birthdate,
home_town as hometown,
college_id as college_id,
college_name  as college_name,
position_group as position_group,
position as position,
jersey_number as jersey_number,
height as height,
weight as weight,
current_team as current_team,
player_pic_url as player_pic
FROM  staging_player
where nflid IS NOT NULL;
''')

statid_table_insert = ('''
INSERT INTO  statid_dim(stat_id, name, comment)
SELECT DISTINCT stat_id as stat_id,
name as name,
comment as comment
FROM  staging_statid
where stat_id IS NOT NULL;
''')

team_table_insert = ('''
INSERT INTO  team_dim(team_id, season, abbr, citystate, full_name, nick, team_type, conference_abbr, division_abbr, year_found, stadium_name)
SELECT DISTINCT team_id as team_id,
season as season,
abbr as abbr,
citystate as citystate,
full_name as full_name,
nick as nick,
team_type as team_type,
conference_abbr as conference_abbr,
division_abbr as division_abbr,
year_found as year_found,
stadium_name as stadium_name
FROM  staging_team
where team_id IS NOT NULL;
''')

game_table_insert = ('''
    INSERT INTO game_dim
    (game_id, season, season_type, week, game_key, game_date, game_time_iso, vis_points_total, vis_points_q1, vis_points_q2, vis_points_q3, vis_points_q4, vis_points_ot, home_points_total, home_points_q1, home_points_q2,
    home_points_q3, home_points_q4, home_points_ot, win_team, lose_team, site_id, site_city, site_full_name, site_state, roof_type, game_phase, week_name_abbr, week_name, game_type, home_id, away_id)
    SELECT DISTINCT sg.game_id as game_id,
    sg.season as season,
    sg.season_type as season_type,
    sg.week as week,
    sg.game_key as game_key,
    sg.game_date as game_date,
    sg.game_time_iso as game_time_iso,
    sg.vis_points_total as vis_points_total,
    sg.vis_points_q1 as vis_points_q1,
    sg.vis_points_q2 as vis_points_q2,
    sg.vis_points_q3 as vis_points_q3,
    sg.vis_points_q4 as vis_points_q4,
    sg.vis_points_ot as vis_points_ot,
    sg.home_points_total as home_points_total,
    sg.home_points_q1 as home_points_q1,
    sg.home_points_q2 as home_points_q2,
    sg.home_points_q3 as home_points_q3,
    sg.home_points_q4 as home_points_q4,
    sg.home_points_ot as home_points_ot,
    sg.win_team as win_team,
    sg.lose_team as lose_team,
    sg.site_id as site_id,
    sg.site_city as site_city,
    sg.site_full_name as site_full_name,
    sg.site_state as site_state,
    sg.roof_type as roof_type,
    sg.phase as game_phase,
    sd.week_name_abbr as week_name_abbr,
    sd.week_name as week_name,
    sd.game_type as game_type,
    sd.home_id as home_id,
    sd.vis_id as away_id
    FROM staging_gameinfo sg
    JOIN staging_schedule sd ON sg.game_id = sd.game_id;
''')

gameplay_table_insert = ('''
INSERT INTO  play_fact(game_id, week, drive_seq, play_id, play_stat_seq, season, off_team, def_team, penalty, scoring, scoring_team, play_type,
    quarter, down, yard_to_go, first_down, play_descript, play_vid, stat_id, yards, player_id, player_team)
SELECT  game_id as game_id,
week as week,
drive_seq as drive_seq,
play_id as play_id,
play_stat_seq as play_stat_seq,
season as season,
off_team as off_team,
def_team as def_team,
penalty as penalty,
scoring as scoring,
 scoring_team as scoring_team,
 play_type as play_type,
 quarter as quarter,
 down as down,
 yard_to_go as yard_to_go,
 first_down as first_down,
 play_descript as play_descript,
 play_vid as play_vid,
 stat_id as stat_id,
 yards as yards,
 player_id as player_id,
 player_team as player_team
FROM  staging_gameplays
where game_id IS NOT NULL;
''')


# Drop Table List

drop_table_queries = [staging_coaches_table_drop, staging_gameinfo_table_drop,
                      staging_gameplays_table_drop, staging_player_table_drop, staging_statid_table_drop,
                      staging_schedule_table_drop, staging_team_table_drop, coaches_table_drop,
                      players_table_drop, game_table_drop, team_table_drop, statid_table_drop, play_table_drop]

# Create Table List
create_table_queries = [staging_coaches_table_create, staging_gameinfo_table_create,
                        staging_gameplays_table_create, staging_player_table_create,
                        staging_statid_table_create, staging_schedule_table_create,
                        staging_team_table_create, coach_table_create, player_table_create,
                        game_table_create, team_table_create, statid_table_create, play_table_create]

# Copy to Staging Table list

copy_table_queries = [staging_coaches_copy, staging_gameinfo_copy,
                      staging_gameplays_copy, staging_player_copy, staging_statid_copy,
                      staging_schedule_copy, staging_schedule_copy, staging_team_copy]

# Insert Queries

insert_table_queries = [coach_table_insert, player_table_insert, statid_table_insert,
                        team_table_insert, game_table_insert, gameplay_table_insert]
