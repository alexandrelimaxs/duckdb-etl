with group_table as (
select
    CONCAT(sales_year,'/',sales_month) as month,
    product_name,
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
)
select
    g.month,
    g.product_name,
    g.dollars_sold,
    rank() over (partition by month order by dollars_sold desc) as month_rank
from
    group_table as g
order by 1,4