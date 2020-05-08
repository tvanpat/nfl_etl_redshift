import urllib.request
import json
import glob
import os
import boto3
import botocore
from multiprocessing.dummy import Pool as ThreadPool

from dotenv import load_dotenv
load_dotenv()


awskey = os.getenv('aws_key')
awssecret = os.getenv('aws_secret')
s3_bucket = os.getenv('s3_bucket')

s3 = boto3.resource('s3',
                    region_name="us-east-1",
                    aws_access_key_id=awskey,
                    aws_secret_access_key=awssecret
                    )

bucket = s3.Bucket(s3_bucket)


def hyper_down_game(g_list):
    gameid = g_list[0]
    season_to_get = g_list[1]
    game_url = f'https://www.nfl.com/feeds-rs/boxscorePbp/{str(gameid)}.json'
    with urllib.request.urlopen(game_url) as url:
        data = json.loads(url.read().decode())
    filepath = f'{season_to_get}/gamesheets/{gameid}.json'
    save_to_bucket(data, filepath)


def hyper_down_player(player):
    player_url = f'https://www.nfl.com/feeds-rs/player/{player}.json'
    player_stat_url = f'https://www.nfl.com/feeds-rs/playerStats/{player}.json'

    with urllib.request.urlopen(player_url) as url:
        data = json.loads(url.read().decode())
    filepath = f'{season_to_get}/playersheets/{player}.json'
    save_to_bucket(data, filepath)

    filepath = f'{season_to_get}/player_stat_sheets/{player}.json'
    with urllib.request.urlopen(player_stat_url) as url:
        data = json.loads(url.read().decode())
    save_to_bucket(data, filepath)


def save_to_bucket(data, filepath):
    '''
    Save file to bucket
    '''
    s3object = s3.Object(s3_bucket, filepath)
    s3object.put(
        Body=(bytes(json.dumps(data).encode('UTF-8')))
    )


def download_season_schedule(season_to_get):
    '''
    This function takes the
    '''
    game_schedule_url = f'https://www.nfl.com/feeds-rs/schedules/{season_to_get}.json'
    with urllib.request.urlopen(game_schedule_url) as url:
        data = json.loads(url.read().decode())

    filepath = f'{season_to_get}/{season_to_get}_schedule.json'
    save_to_bucket(data, filepath)


def download_gamesheet(season_to_get):
    '''
    This function dowloads all game files
    '''
    filepath = f'{season_to_get}/{season_to_get}_schedule.json'
    file_object = s3.Object(s3_bucket, filepath)
    file_content = file_object.get()['Body'].read().decode('utf-8')
    schedule = json.loads(file_content)

    game_id = []
    for s in schedule['gameSchedules']:
        game_id.append((s['gameId'], season_to_get))

    pool = ThreadPool(4)
    pool.map(hyper_down_game, game_id)
    pool.close()
    pool.join()


def download_player(season_to_get):
    path = f'{season_to_get}/gamesheets'
    gamesheets = []
    for object_summary in bucket.objects.filter(Prefix=path):
        gamesheets.append(object_summary.key)

    nflid_list = []
    for g in gamesheets:
        file_object = s3.Object(s3_bucket, g)
        file_content = file_object.get()['Body'].read().decode('utf-8')
        game_data = json.loads(file_content)
        for i in game_data['drives']:
            for j in i['plays']:
                if j['playStats'] != None:
                    for k in j['playStats']:
                        if k['player'] != None:
                            if k['player']['nflId'] not in nflid_list:
                                nflid_list.append(k['player']['nflId'])

    pool = ThreadPool(50)
    pool.map(hyper_down_player, nflid_list)
    pool.close()
    pool.join()


def download_coach(season_to_get):
    '''
    This does X
    '''
    filepath = f'{season_to_get}/{season_to_get}_schedule.json'
    file_object = s3.Object(s3_bucket, filepath)
    file_content = file_object.get()['Body'].read().decode('utf-8')
    schedule = json.loads(file_content)

    team_id = []
    for s in schedule['gameSchedules']:
        if s['visitorTeamId'] not in team_id:
            team_id.append(s['visitorTeamId'])
        if s['homeTeamId'] not in team_id:
            team_id.append(s['homeTeamId'])
    team_id.remove('8700')
    team_id.remove('8600')

    for t in team_id:
        coach_url = f'https://www.nfl.com/feeds-rs/coach/byTeam/{t}/{season_to_get}.json'
        with urllib.request.urlopen(coach_url) as url:
            data = json.loads(url.read().decode())
        filepath = f'{season_to_get}/coach/{t}.json'
        save_to_bucket(data, filepath)


def download_team(season_to_get):
    game_schedule_url = f'https://www.nfl.com/feeds-rs/teams/{season_to_get}.json'
    with urllib.request.urlopen(game_schedule_url) as url:
        data = json.loads(url.read().decode())

    filepath = f'{season_to_get}/team/{season_to_get}_team.json'
    save_to_bucket(data, filepath)


def main():
    '''
    '''
    season_to_get = input(str('Enter Season to download: '))
    download_season_schedule(season_to_get)
    download_gamesheet(season_to_get)
    # download_player(season_to_get)
    # download_coach(season_to_get)
    # download_team(season_to_get)


if __name__ == "__main__":
    main()
