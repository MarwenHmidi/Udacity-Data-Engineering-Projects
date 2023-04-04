from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 checks = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks
        

    def execute(self, context):
        self.log.info('DataQualityOperator started')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        for i, dq_check in enumerate(self.checks):
            records = redshift_hook.get_records(dq_check['check_query'])
            if not dq_check['expected_results'] == records[0][0]:
                           raise ValueError(f"Data quality check #{i} failed. {put explanation of the error here}")


        