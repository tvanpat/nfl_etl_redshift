import json
import boto3
import urllib.request
from urllib.parse import unquote_plus
import os


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


def save_to_bucket(teamjson, filepath, bucket):
    '''
    Save file to bucket
    '''
    s3object = s3.Object(bucket, filepath)
    s3object.put(
        Body=(bytes(json.dumps(teamjson).encode('UTF-8')))
    )


def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        file_object = s3.Object(bucket, key)
        file_content = file_object.get()['Body'].read().decode('utf-8')
        schedule = json.loads(file_content)
        season_to_get = schedule['season']
        game_schedule_url = f'https://www.nfl.com/feeds-rs/teams/{season_to_get}.json'
        with urllib.request.urlopen(game_schedule_url) as url:
            data = json.loads(url.read().decode())

        season = schedule['season']
        for s in schedule['gameSchedules']:
            gameid = s['gameId']
            filepath = f'gameids/{gameid}.json'
            save_to_bucket(gameid, filepath, bucket)
