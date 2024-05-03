with total_sales as (
select
    CONCAT(sales_year,'/',sales_month) as month,
    sum(dollars_sold) as    total_dollars_sold,
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
group by all
),
category_sales as (
select
    CONCAT(sales_year,'/',sales_month) as month,
    sum(dollars_sold) as dollars_sold,
    CATEGORY_NAME,
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
)
select
    month,
    category_name,
    round(dollars_sold/total_dollars_sold,2) as percentage
from
    category_sales as cs
left join
    total_sales as ts using(month)
order by 1
