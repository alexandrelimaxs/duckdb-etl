import main.etls.duckdb_hook as duckdb_hook
import polars as pl


customers = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/customers_export.csv')
salesrep = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/salesrep_export.csv')
orders = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/orders_export.csv')
products = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/products_export.csv')
promotions = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/promotions_export.csv')
customers_store_nyc = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/Customers_Store_NYC.csv')
orders_store_nyc = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/Orders_Store_NYC.csv')

print(orders.columns)