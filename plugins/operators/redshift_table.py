from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftTableOperator(BaseOperator):
    """Execute SQL query on Redshift table(s).

    Attributes
    ----------
    redshift_conn_id : str
        Redshift connection string.
    sql : str
        SQL query to be performed.
    
    Methods
    -------
    execute(context)
        Runs SQL in Redshift Cluster.
    
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql='',
                 *args, **kwargs):
        """
        Parameters
        ----------
        redshift_conn_id : str
            Redshift connection string.
        sql : str
            SQL query to be performed.
        """

        super(RedshiftTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        """Runs SQL in Redshift Cluster.

        Parameters
        ----------
        context : dict, optional
            Context provided to the operator.
        """
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.sql)