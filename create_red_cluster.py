import pandas as pd
import boto3
import json
import configparser
import time
import os

from dotenv import load_dotenv
load_dotenv()


def main():
    aws_key = os.getenv('aws_key')
    aws_secret = os.getenv('aws_secret')

    red_cluster_type = os.getenv('red_cluster_type')
    red_num_modes = os.getenv('red_num_modes')
    red_node_type = os.getenv('red_node_type')

    red_cluster_identifier = os.getenv('red_cluster_identifier')
    red_db = os.getenv('red_db')
    red_db_user = os.getenv('red_db_user')
    red_db_password = os.getenv('red_db_password')
    red_port = os.getenv('red_port')

    red_iam_role_name = os.getenv('red_iam_role_name')

    (red_db_user, red_db_password, red_db)

    pd.DataFrame({"Param":
                  ["RED_CLUSTER_TYPE", "RED_NUM_NODES", "RED_NODE_TYPE", "RED_CLUSTER_IDENTIFIER",
                      "RED_DB", "RED_DB_USER", "RED_DB_PASSWORD", "RED_PORT", "RED_IAM_ROLE_NAME"],
                  "Value":
                      [red_cluster_type, red_num_modes, red_node_type, red_cluster_identifier,
                          red_db, red_db_user, red_db_password, red_port, red_iam_role_name]
                  })

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
    try:
        redRole = iam.create_role(
            Path='/',
            RoleName=red_iam_role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)
        pass

    # 3.2 Attach Policy
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=red_iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=red_iam_role_name)['Role']['Arn']

    print('IAM Policy Attached')

    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=red_cluster_type,
            NodeType=red_node_type,
            NumberOfNodes=int(red_num_modes),

            #Identifiers & Credentials
            DBName=red_db,
            ClusterIdentifier=red_cluster_identifier,
            MasterUsername=red_db_user,
            MasterUserPassword=red_db_password,

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)

    time_sleep = 30

    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', None)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                      "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k, v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    status = 'creating'
    while status == 'creating':
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=red_cluster_identifier)['Clusters'][0]
        redstatus = prettyRedshiftProps(myClusterProps)
        if redstatus.loc[2, 'Value'] == 'creating':
            print('Cluster is creating')
            time.sleep(time_sleep)
        else:
            status = redstatus.loc[2, 'Value']
            print(f'Cluster is {status}')

    red_endpoint = myClusterProps['Endpoint']['Address']
    red_role_arn = myClusterProps['IamRoles'][0]['IamRoleArn']
    print('red_endpoint Created')
    print("red_role_arn Created")

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print('TCP Connection Open')
        defaultSg.authorize_ingress(
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(red_port),
            ToPort=int(red_port)
        )

    except Exception as e:
        print(e)
        pass

    config = configparser.ConfigParser()
    config.read_file(open('vpconfig.cfg'))

    config.set('RED', 'red_endpoint', red_endpoint)
    config.set('IAM_ROLE', 'arn', red_role_arn)

    with open('vpconfig.cfg', 'w') as configfile:
        config.write(configfile)
    print('Process Complete')


if __name__ == "__main__":
    main()
