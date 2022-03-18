# The Udacity Data Engineering Nanodegree capstone project which showcases some
# of the skills I've learned through the program. Namely, skills around data modeling,
# creating data pipelines and ETL processes, as well as using cloud
# technologies (AWS, in this case) to transform and organize data so that it
# can be used for analysis.

# Standard libraries
from datetime import datetime, timedelta
import pandas as pd
import json
import configparser
import logging
import time
import os
# Airflow
from airflow import DAG, settings
from airflow.models import Connection
# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.operators.python import PythonOperator
from operators import (
    S3ToRedshiftOperator,
    RedshiftTableOperator,
    DataQualityOperator
)
from helpers import sql_queries
# AWS boto3
import boto3
from botocore.exceptions import ClientError


# Config vars
config = configparser.ConfigParser()
config.read_file(open(os.path.join(os.path.dirname(__file__), os.pardir, 'dwh.cfg')))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
S3_BUCKET = config.get('AWS', 'S3_BUCKET')

DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH', 'DWH_DB')
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT = config.get('DWH', 'DWH_PORT')
DWH_IAM_ROLE_NAME = config.get('DWH', 'DWH_IAM_ROLE_NAME')


def create_aws_clients(region='us-east-1'):
    """Creates an S3, IAM, and Redshift client to interact with.

    Parameters
    ----------
    region : str
        The aws region to create each client (default 'us-east-1').

    Returns
    -------
    ec3
        A boto3 ec2 resource.
    s3
        A boto3 s3 resource.
    iam
        A boto3 iam client.
    redshift
        A boto3 redshift client.
    """

    ec2 = boto3.resource(
        'ec2',
        region_name=region,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    s3 = boto3.resource(
        's3',
        region_name=region,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
    iam = boto3.client(
        'iam',
        region_name=region,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
    redshift = boto3.client(
        'redshift',
        region_name=region,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
    
    return ec2, s3, iam, redshift


def create_iam_role(iam_client):
    """Creates an IAM role to allow Redshift clusters to call AWS services.

    Parameters
    ----------
    iam_client : obj
        The boto3 client to interact with AWS IAM.

    Returns
    -------
    role_arn
        The AWS Resource Name (ARN) of the IAM role.
    """

    try:
        dwhRole = iam_client.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        return None
    
    iam_client.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    role_arn = iam_client.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    return role_arn


def create_redshift_cluster(redshift_client, role_arn):
    """Creates an AWS Redshift Cluster.

    Parameters
    ----------
    redshift_client : obj
        The boto3 client to interact with AWS Redshift.
    role_arn : str
        The AWS Resource Name (ARN) of the IAM Role.

    Returns
    -------
    response
        The response sent from creating the redshift cluster.
    """

    try:
        response = redshift_client.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[role_arn]  
        )
        return response
    except Exception as e:
        print(e)
        response = None
        return response


def describe_redshift_cluster(redshift_client):
    """Describes the state of a redshift cluster.

    Parameters
    ----------
    redshift_client : obj
        The boto3 client to interact with AWS Redshift.

    Returns
    -------
    cluster_description
        A dictionary that describes the state of the cluster.
    """

    try:
        cluster_description = redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        return cluster_description
    except redshift_client.exceptions.ClusterNotFoundFault as e:
        print(e)
        cluster_description = {}
        return cluster_description


def aws_startup():
    """Spins up a Redshift cluster and the boto3 clients to do so.

    Returns
    -------
    bool
        Returns True once the redshift cluster is available.
    """

    ec2, _, iam, redshift = create_aws_clients()
    role_arn = create_iam_role(iam)
    if role_arn is None:
        role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    cluster = create_redshift_cluster(redshift, role_arn)
    while True:
        cluster_description = describe_redshift_cluster(redshift)
        if cluster_description.get('ClusterStatus') == 'available':
            try:
                vpc = ec2.Vpc(id=cluster_description['VpcId'])
                defaultSg = list(vpc.security_groups.all())[0]
                
                defaultSg.authorize_ingress(
                    GroupName= 'default',  
                    CidrIp='0.0.0.0/0',  
                    IpProtocol='TCP',  
                    FromPort=int(DWH_PORT),
                    ToPort=int(DWH_PORT)
                )
            except Exception as e:
                print(e)
            return True
        else:
            time.sleep(30)


def destroy_aws_resources():
    """Destroys Redshift cluster and IAM role.

    Returns
    -------
    bool
        Returns True once the Redshift cluster and IAM role are deleted.
    """
    _, _, iam, redshift = create_aws_clients()
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    while True:
        cluster_description = describe_redshift_cluster(redshift)
        if cluster_description == {}:
            break
        else:
            time.sleep(30)
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    return True


def add_airflow_redshift_connection():
    """Adds Redshift Connection to Airflow.

    Returns
    -------
    bool
        Returns True once the Redshift connection is added.
    """

    # Describe cluster for Connection values
    _, _, _, redshift = create_aws_clients()
    cluster_description = describe_redshift_cluster(redshift)
    conn_id = 'redshift'
    conn_type = 'postgres'
    host = cluster_description['Endpoint']['Address']
    schema = cluster_description['DBName']
    login = cluster_description['MasterUsername']
    password = DWH_DB_PASSWORD
    port = cluster_description['Endpoint']['Port']
    # Define connection
    conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            schema=schema,
            login=login,
            password=password,
            port=port
    )
    session = settings.Session()
    # Check to make sure the connection doesn't already exist
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    if str(conn_name) == str(conn.conn_id):
        logging.warning(f"Connection {conn.conn_id} already exists")
        return None
    # Add connection to airflow
    session.add(conn)
    session.commit()
    return True


def add_airflow_aws_connection():
    """Adds AWS Connection to Airflow.

    Returns
    -------
    bool
        Returns True once the AWS connection is added.
    """

    # Define connection values
    conn_id = 'aws_credentials'
    conn_type = 'Amazon Web Services'
    login = KEY
    password = SECRET
    # Define connection
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        login=login,
        password=password
    )
    session = settings.Session()
    # Check to make sure the connection doesn't already exist
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    if str(conn_name) == str(conn.conn_id):
        logging.warning(f"Connection {conn.conn_id} already exists")
        return None
    # Add connection to airflow
    session.add(conn)
    session.commit()
    return True


# Default arguments for the DAG & Operators
default_args = {
    'owner': 'jacobtomlinson',
    'depends_on_past': False,
    'email': ['jacob.tomlinson21@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'sla': timedelta(hours=4)
}


with DAG(
    'udacity_capstone',
    default_args=default_args,
    description='''An ETL pipeline which includes data on yelp businesses, 
        yelp reviews, yelp users, U.S. demographics, and the U.S. tax climate.''',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['udacity']
) as dag:
    connection_complete = DummyOperator(task_id='connection_complete')
    drop_tables_complete = DummyOperator(task_id='drop_tables_complete')
    create_tables_complete = DummyOperator(task_id='create_tables_complete')
    copy_complete = DummyOperator(task_id='copy_complete')
    insert_tables_complete = DummyOperator(task_id='insert_tables_complete')
    unload_to_s3_complete = DummyOperator(task_id='unload_to_s3_complete')
    
    startup_aws = PythonOperator(
        task_id='aws_startup',
        python_callable=aws_startup
    )
    
    add_redshift_connection = PythonOperator(
        task_id='add_redshift_connection',
        python_callable=add_airflow_redshift_connection
    )

    add_aws_connection = PythonOperator(
        task_id='add_aws_connection',
        python_callable=add_airflow_aws_connection
    )

    for i, sql_statement in enumerate(sql_queries.table_drop_statements):
        drop_table = RedshiftTableOperator(
            task_id='{}_drop_table'.format(i),
            redshift_conn_id='redshift', 
            sql=sql_statement
        )
        add_redshift_connection >> connection_complete >> drop_table >> drop_tables_complete
        add_aws_connection >> connection_complete >> drop_table >> drop_tables_complete
    
    for i, sql_statement in enumerate(sql_queries.table_create_statements):
        create_table = RedshiftTableOperator(
            task_id='{}_create_table'.format(i),
            redshift_conn_id='redshift',
            sql=sql_statement
        )
        drop_tables_complete >> create_table >> create_tables_complete

    copy_yelp_business_data = S3ToRedshiftOperator(
        task_id='copy_yelp_business_data',
        redshift_conn_id='redshift',
        role_name=DWH_IAM_ROLE_NAME,
        region='us-east-1',
        key=KEY,
        secret=SECRET,
        s3_bucket=S3_BUCKET,
        s3_key='yelp_academic_dataset_business.json',
        table='yelp_businesses',
        format=f'json \'s3://{S3_BUCKET}/yelp_business_jsonpath.json\''
    )

    copy_yelp_review_data = S3ToRedshiftOperator(
        task_id='copy_yelp_review_data',
        redshift_conn_id='redshift',
        role_name=DWH_IAM_ROLE_NAME,
        region='us-east-1',
        key=KEY,
        secret=SECRET,
        s3_bucket=S3_BUCKET,
        s3_key='yelp_academic_dataset_review.json',
        table='yelp_reviews',
        format='json \'auto\''
    )

    copy_yelp_user_data = S3ToRedshiftOperator(
        task_id='copy_yelp_user_data',
        redshift_conn_id='redshift',
        role_name=DWH_IAM_ROLE_NAME,
        region='us-east-1',
        key=KEY,
        secret=SECRET,
        s3_bucket=S3_BUCKET,
        s3_key='yelp_academic_dataset_user.json',
        table='yelp_users',
        format=f'json \'s3://{S3_BUCKET}/yelp_user_jsonpath.json\''
    )

    copy_tax_climate_data = S3ToRedshiftOperator(
        task_id='copy_tax_climate_data',
        redshift_conn_id='redshift',
        role_name=DWH_IAM_ROLE_NAME,
        region='us-east-1',
        key=KEY,
        secret=SECRET,
        s3_bucket=S3_BUCKET,
        s3_key='state_business_tax_climate.csv',
        table='tax_climate',
        format='csv ignoreheader 1'
    )

    copy_demographic_data = S3ToRedshiftOperator(
        task_id='copy_demographic_data',
        redshift_conn_id='redshift',
        role_name=DWH_IAM_ROLE_NAME,
        region='us-east-1',
        key=KEY,
        secret=SECRET,
        s3_bucket=S3_BUCKET,
        s3_key='us-cities-demographics.json',
        table='demographics',
        format=f'json \'auto\''
    )

    data_quality_checks = DataQualityOperator(
        task_id='data_quality_checks',
        redshift_conn_id='redshift',
        dq_checks=sql_queries.dq_checks
    )

    for i, sql_statement in enumerate(sql_queries.table_insert_statements):
        insert_table = RedshiftTableOperator(
            task_id='{}_insert_table'.format(i),
            redshift_conn_id='redshift',
            sql=sql_statement
        )
        data_quality_checks >> insert_table >> insert_tables_complete
    
    tax_and_yelp_business_to_s3 = RedshiftToS3Operator(
        task_id='tax_and_yelp_business_to_s3',
        s3_bucket=S3_BUCKET,
        s3_key='tax_and_yelp_business',
        schema='public',
        table='tax_and_yelp_business',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        autocommit=True,
        include_header=True,
        table_as_file_name=True
    )

    demographic_and_yelp_business_to_s3 = RedshiftToS3Operator(
        task_id='demographic_and_yelp_business_to_s3',
        s3_bucket=S3_BUCKET,
        s3_key='demographic_and_yelp_business',
        schema='public',
        table='demographic_and_yelp_business',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        autocommit=True,
        include_header=True,
        table_as_file_name=True
    )

    aws_shutdown = PythonOperator(
        task_id='aws_shutdown',
        python_callable=destroy_aws_resources
    )

    startup_aws >> [add_redshift_connection, add_aws_connection]
    create_tables_complete >> [copy_yelp_business_data, copy_tax_climate_data, copy_demographic_data, copy_yelp_review_data, copy_yelp_user_data] >> copy_complete
    copy_complete >> data_quality_checks
    insert_tables_complete >> [tax_and_yelp_business_to_s3, demographic_and_yelp_business_to_s3] >> unload_to_s3_complete
    unload_to_s3_complete >> aws_shutdown
