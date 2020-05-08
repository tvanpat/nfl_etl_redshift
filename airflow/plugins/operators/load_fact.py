from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql ='''
                INSERT INTO {}
                {};
                '''
   
    @apply_defaults
    def __init__(self,
                 redshift = "",
                 table = "",
                 sql_statement = "",
                 append_only = False,
                 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift
        self.table = table
        self.sql_statement = sql_statement
        self.append_only = append_only


    def execute(self, context):
        self.log.info(f"Openning data transfer process to {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift)
        if not self.append_only:
            self.log.info(f"Deleting data from {self.table} fact table")
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info(f"Insert data from staging tables into {self.table} fact table")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_statement
        )
        redshift.run(formatted_sql)
