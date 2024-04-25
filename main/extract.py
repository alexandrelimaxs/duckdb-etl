import duckdb_hook
import polars as pl


csv = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/customers_export.csv')

print(csv)