from base_files.duckdb_base_etl import DuckDBBaseETL

class SalesrepETL(DuckDBBaseETL):

    def __init__(self, source_table: str, table_name: str, primary_key: list):
        super().__init__(source_table, table_name, primary_key)

dim_salesrep = SalesrepETL('salesrep_export','sales.dim_salesrep',['employee_id'])

dim_salesrep.execute()