from base_files.duckdb_base_etl import DuckDBBaseETL

class PromotionsETL(DuckDBBaseETL):

    def __init__(self, source_table: str, table_name: str, primary_key: list):
        super().__init__(source_table, table_name, primary_key)

dim_promotions = PromotionsETL('promotions_export','sales.dim_promotions',['promo_id'])

dim_promotions.execute()