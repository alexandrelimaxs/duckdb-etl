import polars as pl
import os

from .duckdb_hook import DuckDBHook

class DuckDBBaseETL():

    def __init__(self, source_table:str, table_name:str, primary_key:str):

        self.hook = DuckDBHook()
        self.source_table = source_table
        self.table_name = table_name
        self.primary_key = primary_key
        self.path = f"main/temp_files/{table_name}_tmp.parquet"
        self.source_path = f"raw_data/{source_table}.csv"

    def execute(self):
        print(f"Starting {self.table_name} ETL process...")

        df = self.load_transform()
        self.export_transformed_data(df)
        if self.check_if_table_exists():
            self.load_temp_table()
            self.upsert_table()
            self.delete_temp_table()
        else:
            self.load_temp_table(create_table=True)

    def load_transform(self) -> pl.DataFrame:
        print("Loading and transforming data...")

        df = pl.read_csv(self.source_path)

        return df

    def export_transformed_data(self, df:pl.DataFrame):
        print("Exporting transformed data...")

        self.columns = list(df.columns)
        self.columns.remove(self.primary_key.upper())
        df.write_parquet(self.path)

    def load_temp_table(self, create_table:bool = False):
        print("Loading temporary transformed data...")

        if not create_table:
            table_name = self.table_name + '_temp'

        query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{self.path}')"

        self.hook.sql(query)

    def upsert_table(self):
        print("Merging transformed data into target table...")

        update_columns = []

        for source_columns, target_columns in zip(self.columns, self.columns):
            join = f'{target_columns} = EXCLUDED.{source_columns}'
            update_columns.append(join)

        update_columns = '\nAND\n'.join(update_columns)

        query = f"""
        INSERT INTO 
            {self.table_name}
        BY NAME 
            (SELECT * FROM {self.table_name}_temp)
        ON CONFLICT DO UPDATE SET {update_columns} WHERE {self.primary_key} = EXCLUDED.{self.primary_key} AND {self.primary_key} > EXCLUDED.{self.primary_key}
        ;
        """

        self.hook.sql(query)

    def delete_temp_table(self):
        print("Deleting temporary files and tables...")
        
        self.hook.sql(f"DROP TABLE {self.table_name}_temp")

        os.remove(self.path)

    def check_if_table_exists(self) -> bool:
        print(f"Checking if {self.source_table} exists...")

        query = f"""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.table_name.split('.')[1]}'
        """

        tabela = self.hook.sql(query).pl()

        if tabela.shape[0] == 0:
            print(f"Table {self.table_name} don't exists.")
            return False
        else:
            print(f"Table {self.table_name} exists.")
            return True



