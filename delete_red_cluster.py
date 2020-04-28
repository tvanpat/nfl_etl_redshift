import pandas as pd
import boto3
import json
import configparser
import time
import os

from dotenv import load_dotenv
load_dotenv()

aws_key = os.getenv('aws_key')
aws_secret = os.getenv('aws_secret')

red_cluster_identifier = os.getenv('red_cluster_identifier')

red_iam_role_name = os.getenv('red_iam_role_name')

ec2 = boto3.resource('ec2',
                     region_name="us-east-1",
                     aws_access_key_id=aws_key,
                     aws_secret_access_key=aws_secret
                     )

s3 = boto3.resource('s3',
                    region_name="us-east-1",
                    aws_access_key_id=aws_key,
                    aws_secret_access_key=aws_secret
                    )

iam = boto3.client('iam', aws_access_key_id=aws_key,
                   aws_secret_access_key=aws_secret,
                   region_name='us-east-1'
                   )

redshift = boto3.client('redshift',
                        region_name="us-east-1",
                        aws_access_key_id=aws_key,
                        aws_secret_access_key=aws_secret
                        )


def main():

    # CAREFUL!!
    # -- Uncomment & run to delete the created resources
    redshift.delete_cluster(ClusterIdentifier=red_cluster_identifier,
                            SkipFinalClusterSnapshot=True)
    # CAREFUL!!

    # This will check the status every 10 seconds until the cluster is deleted.  If you wish to change the amount of time change the time_sleep to the desired number of seconds.
    time_sleep = 30
    status = 'deleting'
    while status == 'deleting':
        try:
            myClusterProps = redshift.describe_clusters(
                ClusterIdentifier=red_cluster_identifier)['Clusters'][0]
            redstatus = prettyRedshiftProps(myClusterProps)
            if redstatus.loc[2, 'Value'] == status:
                print('Cluster is being deleted.')
                time.sleep(time_sleep)
            else:
                print(f'Clustredstatus.loc[2,"Value"]')
                status = redstatus.loc[2, 'Value']

        except:
            print('Cluster is deleted')
            status = 'deleted'

    # CAREFUL!!
    # -- Uncomment & run to delete the created resources
    iam.detach_role_policy(RoleName=red_iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=red_iam_role_name)
    # CAREFUL!!

    red_endpoint = ''
    red_role_arn = ''

    config = configparser.ConfigParser()
    config.read_file(open('vpconfig.cfg'))
    config.set('RED', 'red_endpoint', red_endpoint)
    config.set('IAM_ROLE', 'arn', red_role_arn)

    with open('vpconfig.cfg', 'w') as configfile:
        config.write(configfile)


if __name__ == "__main__":
    main()
