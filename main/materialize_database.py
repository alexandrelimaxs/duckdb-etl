from base_files.duckdb_base_etl import DuckDBHook
from dim_customers import CustomersETL
from dim_date import DateETL
from dim_products import ProductsETL
from dim_promotions import PromotionsETL
from dim_salesrep import SalesrepETL
from sales import SalesETL

banco = DuckDBHook()

### Cria as tabelas do banco.

banco.execute_dump_sql('main/sql_files/create_data_warehouse.sql')

### Popula todas as tabelas do banco.

dim_customers = CustomersETL('customers_export','sales.dim_customers',['customer_id'])
dim_customers.execute()

dim_date = DateETL('orders_export','sales.dim_date',['sales_date_id'])
dim_date.execute()

dim_products = ProductsETL('products_export','sales.dim_products',['product_id'])
dim_products.execute()

dim_promotions = PromotionsETL('promotions_export','sales.dim_promotions',['promo_id'])
dim_promotions.execute()

dim_salesrep = SalesrepETL('salesrep_export','sales.dim_salesrep',['employee_id'])
dim_salesrep.execute()

sales = SalesETL('orders_export','sales.sales',['CUSTOMER_DIM_ID','SALESREP_DIM_ID','PROMO_DIM_ID','PRODUCT_DIM_ID','SALES_DATE_DIM_ID'])
sales.execute()