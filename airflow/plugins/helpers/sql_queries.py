class SqlQueries:
    
    gameplay_table_insert = ('''
        (game_id, week, drive_seq, play_id, play_stat_seq, season, off_team, def_team, penalty, scoring, scoring_team, play_type,
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
    
    coach_table_insert = ('''
        (coach_id, season, week, display_name, first_name, last_name, esbid, 
        status, birthdate, hometown, college,team_id, isdeceased, pic_url)
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
        (nflid, esbid, gsisid, status, display_name, first_name, last_name, 
        middle_name, suffix, birthdate, hometown, college_id, college, position_group, position, jersey_number, 
        height, weight, current_team, player_pic_url)
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
        (stat_id, name, comment)
        SELECT DISTINCT stat_id as stat_id,
        name as name,
        comment as comment
        FROM  staging_statid
        where stat_id IS NOT NULL;
        ''')
    
    team_table_insert = ('''
        (team_id, season, abbr, citystate, full_name, nick, team_type, conference_abbr, division_abbr, year_found, stadium_name)
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
        (game_id, season, season_type, week, game_key, game_date, game_time_iso, vis_points_total, 
        vis_points_q1, vis_points_q2, vis_points_q3, vis_points_q4, vis_points_ot, home_points_total, 
        home_points_q1, home_points_q2, home_points_q3, home_points_q4, home_points_ot, win_team, 
        lose_team, site_id, site_city, site_full_name, site_state, roof_type, game_phase, week_name_abbr, 
        week_name, game_type, home_id, away_id)
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
    
    