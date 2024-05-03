from polars.dataframe import DataFrame
from base_files.duckdb_base_etl import DuckDBBaseETL

class CustomersETL(DuckDBBaseETL):

    def __init__(self, source_table: str, table_name: str, primary_key: list):
        super().__init__(source_table, table_name, primary_key)


dim_customers = CustomersETL('customers_export','sales.dim_customers',['customer_id'])

dim_customers.execute()