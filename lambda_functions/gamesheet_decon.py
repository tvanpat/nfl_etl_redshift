import json
import json
import boto3
import urllib.request
from urllib.parse import unquote_plus
import os
from datetime import datetime
import uuid


awskey = os.environ['AWS_KEY']
awssecret = os.environ['AWS_SECRET']

s3_client = boto3.client('s3',
                         aws_access_key_id=awskey,
                         aws_secret_access_key=awssecret
                         )
s3 = boto3.resource('s3',
                    region_name="us-east-1",
                    aws_access_key_id=awskey,
                    aws_secret_access_key=awssecret
                    )


def save_to_bucket(data, filepath, bucket):
    '''
    Save file to bucket
    '''
    s3object = s3.Object(bucket, filepath)
    s3object.put(
        Body=(bytes(json.dumps(data).encode('UTF-8')))
    )


def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        file_object = s3.Object(bucket, key)
        file_content = file_object.get()['Body'].read().decode('utf-8')
        game_data = json.loads(file_content)

    game_schedule = game_data.get("gameSchedule", "")
    game_score = game_data.get("score", "")
    season = game_schedule.get("season", "")
    game_id = game_schedule.get("gameId", "")
    home_team = game_schedule.get("homeTeamId", "")
    vis_team = game_schedule.get("visitorTeamId", "")

    season_type = game_schedule.get("seasonType", "")
    week = game_schedule.get("week", "")
    game_key = game_schedule.get("gameKey", "")
    gamed = game_schedule.get("gameDate", "")
    dateobj = datetime.strptime(gamed, '%m/%d/%Y')
    game_date = dateobj.strftime("%Y,%m,%d")
    game_time_iso = game_schedule.get("isoTime", "")
    vis_points = game_score.get("visitorTeamScore", "")
    vis_points_total = vis_points.get("pointTotal", "")
    vis_points_q1 = vis_points.get("pointQ1", "")
    vis_points_q2 = vis_points.get("pointQ2", "")
    vis_points_q3 = vis_points.get("pointQ3", "")
    vis_points_q4 = vis_points.get("pointQ4", "")
    vis_points_ot = vis_points.get("pointOT", "")
    home_points = game_score.get("homeTeamScore", "")
    home_points_total = home_points.get("pointTotal", "")
    home_points_q1 = home_points.get("pointQ1", "")
    home_points_q2 = home_points.get("pointQ2", "")
    home_points_q3 = home_points.get("pointQ3", "")
    home_points_q4 = home_points.get("pointQ4", "")
    home_points_ot = home_points.get("pointOT", "")
    phase = game_score.get("phase", "")
    if home_points_total > vis_points_total:
        win_team = home_team
    else:
        win_team = vis_team
    if home_points_total < vis_points_total:
        lose_team = home_team
    else:
        lose_team = vis_team
    site = game_schedule.get("site", "")
    site_id = site.get("siteId", "")
    site_city = site.get("siteCity", "")
    site_full_name = site.get("siteFullname", "")
    site_state = site.get("siteState", "")
    roof_type = site.get("roofType", "")

    game_info = {'game_id': game_id, 'season': season, 'season_type': season_type, 'week': week, 'game_key': game_key, 'game_date': game_date, 'game_time_iso': game_time_iso,
                 'vis_points_total': vis_points_total, 'vis_points_q1': vis_points_q1, 'vis_points_q2': vis_points_q2, 'vis_points_q3': vis_points_q3, 'vis_points_q4': vis_points_q4,
                 'vis_points_ot': vis_points_ot, 'home_points_total': home_points_total, 'home_points_q1': home_points_q1, 'home_points_q2': home_points_q2, 'home_points_q3': home_points_q3,
                 'home_points_q4': home_points_q4, 'home_points_ot': home_points_ot, 'win_team': win_team, 'lose_team': lose_team, 'site_id': site_id, 'site_city': site_city, 'site_full_name': site_full_name,
                 'site_state': site_state, 'roof_type': roof_type, 'phase': phase}

    filepath = f'gameinfo/{game_id}.json'
    save_to_bucket(game_info, filepath, bucket)

    team_dic = {game_schedule['homeTeamAbbr']: home_team,
                game_schedule['visitorTeamAbbr']: vis_team}
    player_list = []
    drives = game_data.get("drives", "")
    count = 0
    for drive in drives:
        drive_seq = drive.get("sequence", "")
        plays = drive.get("plays", "")
        for play in plays:
            play_id = play.get("playId", "")
            off_team = play.get("teamEid", "")
            if off_team == home_team:
                def_team = vis_team
            else:
                def_team = home_team

            penalty = play.get("penalty", "")
            scoring = play.get("scoring", "")
            scoring_team = play.get("scoringTeamEid", "")
            scoring_type = play.get("scoringType", "")
            play_type = play.get("playType", "")
            quarter = play.get("quarter", "")
            down = play.get("down", "")
            yard_to_go = play.get("yardsToGo", "")
            first_or_touch = play.get("firstDownOrTouchdown", "")
            if scoring == False and first_or_touch == True:
                first_down = True
            else:
                first_down = False
            play_descrip = play.get("playDescription", "")
            play_vid = 'no-vid'
            highlight_dict = play.get("highlightVideo", 'no-vid')
            if highlight_dict != 'no-vid':
                temp_vid = highlight_dict.get("videoBitRates", None)[0]
                play_vid = temp_vid.get("videoPath", 'no-vid')
            playstats = play.get("playStats", "no-player-stats")
            if playstats == 'no-player-stats' or playstats == None:
                play_stat_seq = ''
                statid = ''
                yards = ''
                player_id = ''
                player_team = ''
            else:
                for ps in playstats:
                    play_stat_seq = ps.get("playStatSeq", "")
                    statid = ps.get("statId", "")
                    yards = ps.get("yards", "")
                    player_id = ''
                    player_dict = ps.get("player", "")
                    if player_dict != None:
                        player_id = player_dict.get("nflId", "")
                    player_team_abbr = ps.get("teamAbbr", "no-team")
                    player_team = ''
                    if player_team_abbr != 'no-team':
                        player_team = team_dic[player_team_abbr]

                    if player_id != '':
                        if player_id not in player_list:
                            player_list.append(player_id)

                    play_dict = {'game_id': game_id, 'week': week, 'drive_seq': drive_seq, 'play_id': play_id, 'play_stat_seq': play_stat_seq, 'season': season,
                                 'home_team': home_team, 'def_team': def_team, 'vis_team': vis_team, 'drive_seq': drive_seq, 'play_id': play_id, 'off_team': off_team,
                                 'penalty': penalty, 'scoring': scoring, 'scoring_team': scoring_team, 'scoring_type': scoring_type,
                                 'play_type': play_type, 'quarter': quarter, 'down': down, 'yard_to_go': yard_to_go, 'first_down': first_down,
                                 'play_descript': play_descrip, 'play_vid': play_vid, 'stat_id': statid, 'yards': yards, 'player_id': player_id, 'player_team': player_team
                                 }

                    file_str = f'{game_id}_{drive_seq}_{play_id}_{play_stat_seq}_{statid}_{player_id}'
                    filepath = filepath = f'gameplays/{file_str}.json'
                    save_to_bucket(play_dict, filepath, bucket)

    for player in player_list:
        filepath = f'playerids/{player}.json'
        save_to_bucket(player, filepath, bucket)
