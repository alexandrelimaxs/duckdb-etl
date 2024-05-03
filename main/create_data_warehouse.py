from main.base_files.duckdb_base_etl import DuckDBHook

banco = DuckDBHook()

banco.execute_dump_sql('main/sql_files/create_data_warehouse.sql')