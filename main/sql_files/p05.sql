select
    CONCAT(sales_year,'/',sales_month) as month,
    category_name,
    sum(dollars_sold) as dollars_sold,
from 
    sales.sales as s
left join
    sales.dim_date as d
on
    s.sales_date_dim_id = d.sales_date_id
inner join
    sales.dim_promotions as p
on
    s.promo_dim_id = p.promo_id
left join
    sales.dim_products as pr
on
    s.product_dim_id = pr.product_id
group by all
order by 3 desc