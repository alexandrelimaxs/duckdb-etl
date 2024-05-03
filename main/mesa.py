from base_files.duckdb_hook import DuckDBHook
import polars as pl


hook = DuckDBHook()


query = """
select * from sales.sales
"""

print(hook.sql(query))