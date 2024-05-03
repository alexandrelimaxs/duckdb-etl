with total_sold as (
select
    1 as join_col,
    sum(dollars_sold) as total_dollars_sold
from 
    sales.sales as s
inner join
    sales.dim_promotions as p
on
    s.promo_dim_id = p.promo_id
),
year_sales as (
select
    1 as join_col,
    first_name,
    sales_year,
    sum(dollars_sold) as dollars_sold,
from 
    sales.sales as s 
left join 
    sales.dim_salesrep as sr 
on 
    s.salesrep_dim_id = sr.employee_id
left join
    sales.dim_date as d
on
    s.sales_date_dim_id = d.sales_date_id
inner join
    sales.dim_promotions as p
on
    s.promo_dim_id = p.promo_id
group by all
order by 1,2
)
select
    first_name,
    sales_year,
    dollars_sold,
    round(dollars_sold/total_dollars_sold, 2) as percentage
from
    year_sales as y
left join
    total_sold as t
on
    t.join_col = y.join_col