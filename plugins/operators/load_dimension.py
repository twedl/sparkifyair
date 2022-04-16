from this import d
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 source_table="",
                 sql="",
                 primary_key="",
                 delete_existing=False
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.source_table = source_table
        self.sql = sql
        self.primary_key = primary_key
        self.delete_existing = delete_existing

    def execute(self, context):
        self.log.info('Running LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_existing == True:
            self.log.info(f"Deleting and re-inserting all data into {self.table}")
            redshift.run(f"""
                DELETE FROM {self.table};
            """)
        else:
            self.log.info(f"Appending into {self.table}")

        redshift.run(f"""
            INSERT INTO {self.table} ({self.sql});
        """)

        self.log.info(f"Finished loading {self.table}")