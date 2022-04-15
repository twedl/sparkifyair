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
                 delete_or_upsert="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.source_table = source_table
        self.sql = sql
        self.primary_key = primary_key
        self.delete_or_upsert = delete_or_upsert

    def execute(self, context):
        self.log.info('Running LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_or_upsert == "delete":
            self.log.info(f"Deleting and re-inserting all data into {self.table}")
            redshift.run(f"DELETE FROM {self.table}") 
            redshift.run(f"INSERT INTO {self.table} ({self.sql})")
        elif self.delete_or_upsert == "upsert":
            self.log.info(f"Updating existing data and appending into {self.table}")

            redshift.run(f"""
                CREATE TABLE upsert_stage (LIKE {self.table});
                INSERT INTO upsert_stage ({self.sql});
                DELETE FROM {self.table} 
                    USING upsert_stage
                    WHERE {self.table}.{self.primary_key} = upsert_stage.{self.primary_key};
                INSERT INTO {self.table}
                    (SELECT * FROM upsert_stage);
                DROP TABLE upsert_stage;
            """)
        self.log.info(f"Finished loading {self.table}")