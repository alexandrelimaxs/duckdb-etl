import polars as pl
from base_files.duckdb_base_etl import DuckDBBaseETL

class SalesETL(DuckDBBaseETL):

    def __init__(self, source_table: str, table_name: str, primary_key: str):
        super().__init__(source_table, table_name, primary_key)

    def load_transform(self) -> pl.DataFrame:

        df = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/orders_export.csv')

        df = df.sql("""
                    select 
                        ORDER_DATE,
                        ORDER_ID,
                        CUSTOMER_ID AS CUSTOMER_DIM_ID,
                        SALES_REP_ID AS SALESREP_DIM_ID,
                        PROMO_ID AS PROMO_DIM_ID,
                        PRODUCT_ID AS PRODUCT_DIM_ID,
                        UNIT_PRICE,
                        QUANTITY
                    from
                        self                
                    """)

        df = df.with_columns(
            pl.col("ORDER_DATE").str.to_date("%d-%b-%y"),
        )

        df = df.with_columns(
            pl.col("ORDER_DATE").dt.timestamp("ms").alias("SALES_DATE_DIM_ID"),
        )

        df = df.sql("""
            select 
                CUSTOMER_DIM_ID,
                SALESREP_DIM_ID,
                PROMO_DIM_ID,
                PRODUCT_DIM_ID,
                SALES_DATE_DIM_ID,
                SUM(UNIT_PRICE) AS DOLLARS_SOLD,
                SUM(QUANTITY) AS QUANTITY_SOLD
            from
                self
            group by
                CUSTOMER_DIM_ID,
                SALESREP_DIM_ID,
                PROMO_DIM_ID,
                PRODUCT_DIM_ID,
                SALES_DATE_DIM_ID
            """)

        return df

sales = SalesETL('orders_export','sales.sales',['CUSTOMER_DIM_ID','SALESREP_DIM_ID','PROMO_DIM_ID','PRODUCT_DIM_ID','SALES_DATE_DIM_ID'])

sales.execute()