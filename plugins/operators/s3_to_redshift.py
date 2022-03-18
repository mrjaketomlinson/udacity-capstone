from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3


class S3ToRedshiftOperator(BaseOperator):
    """Copy S3 data to Redshift.

    Attributes
    ----------
    redshift_conn_id : str
        Redshift connection string.
    region : str
            Region fo the IAM client (default: 'us-east-1').
    key : str
        AWS Access Key.
    secret : str
        AWS Secret Key.
    s3_bucket : str
        S3 bucket to copy data from.
    s3_key : str
        S3 path to copy data from.
    table : str
        Redshift table to copy data to.
    format : str
        Format of the data to use in COPY statement.
    
    Methods
    -------
    execute(context)
        Runs COPY statement to copy S3 data to Redshift table.
    """

    template_fields = ('s3_key',)
    copy_sql = """
        COPY {table}
        FROM '{bucket}'
        IAM_ROLE '{role_arn}'
        FORMAT AS {format};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 role_name='',
                 region='us-east-1',
                 key='',
                 secret='',
                 s3_bucket='',
                 s3_key='',
                 table='',
                 format='',
                 *args, **kwargs):
        """
        Parameters
        ----------
        redshift_conn_id : str
            Redshift connection string.
        role_name : str
            AWS IAM Role Name.
        region : str
            Region fo the IAM client (default: 'us-east-1').
        key : str
            AWS Access Key.
        secret : str
            AWS Secret Key.
        s3_bucket : str
            S3 bucket to copy data from.
        s3_key : str
            S3 path to copy data from.
        table : str
            Redshift table to copy data to.
        format : str
            Format of the data to use in COPY statement.
        """

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.role_name = role_name
        self.region = region
        self.key = key
        self.secret = secret
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.format = format

    def execute(self, context):
        """Runs COPY statement to copy S3 data to Redshift table.

        Parameters
        ----------
        context : dict, optional
            Context provided to the operator; used to format s3_path.
        """

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        iam = boto3.client('iam', region_name=self.region, aws_access_key_id=self.key, aws_secret_access_key=self.secret)
        role_arn = iam.get_role(RoleName=self.role_name)['Role']['Arn']
        
        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            table=self.table,
            bucket=s3_path,
            role_arn = role_arn,
            format=self.format
        )
        redshift.run(formatted_sql)