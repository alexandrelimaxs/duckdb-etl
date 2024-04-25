from duckdb_hook import duckdb_hook

banco = duckdb_hook()

banco.execute_dump_sql('main/sql_files/create_data_warehouse.sql')