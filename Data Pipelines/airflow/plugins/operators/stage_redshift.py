from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    '''
    This operator loads JSON formatted files from S3 to  Redshift.
    You need to provide aws_credentials, redshift_conn_id, table and s3_path.
    '''
    
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {} REGION '{}'
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 data_format='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.data_format = data_format

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()


        # SQL query parameters
        rendered_s3_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_s3_key}'

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, s3_path, credentials.access_key,
            credentials.secret_key, self.data_format, self.region,
        )

        self.log.info('Copy data from {s3_path} to Redshift table {self.table}')
        redshift.run(formatted_sql)       
        
        
        
        
        
        
        



