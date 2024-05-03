from base_files.duckdb_hook import DuckDBHook
import polars as pl


hook = DuckDBHook()


query = """

"""

print(hook.sql(query))