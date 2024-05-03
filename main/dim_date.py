import polars as pl
from base_files.duckdb_base_etl import DuckDBBaseETL

class DateETL(DuckDBBaseETL):

    def __init__(self, source_table: str, table_name: str, primary_key: list):
        super().__init__(source_table, table_name, primary_key)

    def load_transform(self) -> pl.DataFrame:

        df = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/orders_export.csv')

        df = df.sql('select distinct ORDER_DATE AS SALES_DATE from self')

        df = df.with_columns(
            pl.col("SALES_DATE").str.to_date("%d-%b-%y"),
        )

        df = df.with_columns(
            pl.col("SALES_DATE").dt.year().alias("SALES_YEAR"),
            pl.col("SALES_DATE").dt.month().alias("SALES_MONTH"),
            pl.col("SALES_DATE").dt.day().alias("SALES_DAY"),
            pl.col("SALES_DATE").dt.quarter().alias("SALES_QUARTER"),
            pl.col("SALES_DATE").dt.strftime("%B").alias("SALES_MONTH_NAME"),  
            pl.col("SALES_DATE").dt.ordinal_day().alias("SALES_DAY_OF_YEAR"),  
            pl.col("SALES_DATE").dt.timestamp("ms").alias("SALES_DATE_ID"),
        )
        
        return df
