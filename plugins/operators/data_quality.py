from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Perform data quality checks on Redshift tables.

    Attributes
    ----------
    redshift_conn_id : str
        Redshift connection string.
    dq_checks : list of dictionaries
        Data quality checks, with 'sql' and 'expected_result' as keys
        in the dictionary. Example:
        [{'sql': 'SELECT * FROM table', 'expected_result': 1000}, {...}]
    
    Methods
    -------
    execute(context)
        Runs COPY statement to copy S3 data to Redshift table.
    
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dq_checks=[],
                 *args, **kwargs):
        """
        Parameters
        ----------
        redshift_conn_id : str
            Redshift connection string.
        dq_checks : list of dictionaries
            Data quality checks, with 'sql' and 'expected_result' as keys
            in the dictionary. Example:
            [{'sql': 'SELECT * FROM table', 'expected_result': 1000}, {...}]
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        """Perform data quality checks.

        Parameters
        ----------
        context : dict, optional
            Context provided to the operator; used to format s3_path.
        
        Raises
        ------
        ValueError
            If the result is not what is expected, raise an error.
        """
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for check in self.dq_checks:
            sql = check.get('sql')
            expected_result = check.get('expected_result')
            
            records = redshift.get_records(sql)[0][0]
            self.log.info(f'Data check records: {records}')
            
            if records != expected_result:
                raise ValueError(f'Data quality check failed. SQL: {sql}.\n Records: {records}.\n Expected Result: {expected_result}.')
            
            self.log.info(f'Data quality check passed: {check}')
        
        self.log.info('DataQualityOperator finished.')