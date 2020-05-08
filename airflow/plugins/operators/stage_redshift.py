from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


    
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    
    json_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT as 'epochmillisecs'
        JSON 'auto'
        COMPUPDATE OFF
    """
    csv_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        COMPUPDATE OFF STATUPDATE OFF
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        CSV
    """
    
    


    @apply_defaults
    def __init__(self,
                 table="",
                 redshift="",
                 aws_credentials="",
                 s3_bucket="",
                 s3_key="",
                 file_type="",
                 append_table = 'False',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift = redshift
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type
        self.append_table = append_table


    def execute(self, context):
        aws = AwsHook(self.aws_credentials)
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift)
        self.log.info(f'Starting transfer of staging data from {self.s3_bucket} / {self.s3_key} to {self.table}')
        if not self.append_table:
            self.log.info(f"Deleting data from {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.file_type == "json":
            self.log.info(f'JSON data from {self.s3_bucket} / {self.s3_key} to {self.table}')
            formatted_sql = StageToRedshiftOperator.json_template.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key  
            )
        
        elif self.file_type == "csv":
            self.log.info(f'CAV data from {self.s3_bucket} / {self.s3_key} to {self.table}')
            formatted_sql = StageToRedshiftOperator.csv_template.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
        
        
        redshift.run(formatted_sql)
