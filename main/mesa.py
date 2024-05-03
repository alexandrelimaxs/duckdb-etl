from base_files.duckdb_hook import DuckDBHook
import polars as pl


hook = DuckDBHook()

# query = '''SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) AS TABLE_NAME FROM INFORMATION_SCHEMA.TABLES'''
# query = '''SELECT * FROM SALES.dim_promotions'''
# query = 'DROP TABLE sales.dim_promotions_temp'
# query = """
# create or replace table sales.dim_products (
#     PRODUCT_ID INTEGER PRIMARY KEY,
#     PRODUCT_NAME VARCHAR,
#     LANGUAGE_ID VARCHAR,
#     MIN_PRICE FLOAT,
#     LIST_PRICE FLOAT,
#     PRODUCT_STATUS VARCHAR,
#     SUPPLIER_ID INTEGER,
#     WARRANTY_PERIOD INTEGER,
#     WEIGHT_CLASS INTEGER,
#     PRODUCT_DESCRIPTION VARCHAR,
#     CATEGORY_ID INTEGER,
#     CATALOG_URL VARCHAR,
#     SUB_CATEGORY_NAME VARCHAR,
#     SUB_CATEGORY_DESCRIPTION VARCHAR,
#     PARENT_CATEGORY_ID INTEGER,
#     CATEGORY_NAME VARCHAR
# );
# """

# print(hook.sql(query))


df = pl.read_csv('/home/alexandre/faculdade/duckdb-etl/raw_data/orders_export.csv')

df = df.sql('select distinct ORDER_DATE from self')

df = 

# df['ORDER_DATE'] = df['ORDER_DATE'].str.replace('-', '/')

print(df)



# schema = df.schema

# for row in schema:
#     print(f'{row} STRING,')