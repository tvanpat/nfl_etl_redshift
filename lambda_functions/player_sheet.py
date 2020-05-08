import json
import json
import boto3
import urllib.request
from urllib.parse import unquote_plus
import os
from datetime import datetime


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
        data = json.loads(file_content)
    player_url = f'https://www.nfl.com/feeds-rs/playerStats/{data}.json'
    with urllib.request.urlopen(player_url) as url:
        player_data = json.loads(url.read().decode())
    player = player_data.get("teamPlayer", "")
    nfl_id = player.get("nflId", "")
    esb_id = player.get("esbId", "")
    gsis_id = player.get("gsisId", "")
    status = player.get("status", "")
    display_name = player.get("displayName", "")
    first_name = player.get("firstName", "")
    last_name = player.get("lastName", "")
    middle_name = player.get("middleName", "")
    suffix = player.get("suffix", "")
    temp_birth_date = player.get("birthDate", "")
    dateobj = datetime.strptime(temp_birth_date, '%m/%d/%Y')
    birth_date = dateobj.strftime("%Y,%m,%d")
    home_town = player.get("homeTown", "")
    college_id = player.get("collegeId", "")
    college_name = player.get("collegeName", "")
    position_group = player.get("positionGroup", "")
    position = player.get("position", "")
    jersey_number = player.get("jerseyNumber", "")
    temp_height = player.get("height", "")
    temp_list = temp_height.split('-')
    height = int(temp_list[0]) * 12 + int(temp_list[1])
    weight = player.get("weight", "")
    current_team = player.get("teamId", "")
    player_pic_url = f'http://static.nfl.com/static/content/public/static/img/fantasy/transparent/200x200/{esb_id}.png'
    if status != 'ACT':
        current_team = ''

    player_dict = {'nfl_id': nfl_id, 'esb_id': esb_id, 'gsis_id': gsis_id, 'status': status, 'display_name': display_name, 'first_name': first_name, 'last_name': last_name, 'middle_name': middle_name,
                   'suffix': suffix, 'birth_date': birth_date, 'home_town': home_town, 'college_id': college_id, 'college_name': college_name, 'position_group': position_group, 'position': position,
                   'jersey_number': jersey_number, 'height': height, 'weight': weight, 'current_team': current_team, 'player_pic_url': player_pic_url}
    filepath = f'player_info/{nfl_id}.json'
    save_to_bucket(player_dict, filepath, bucket)
