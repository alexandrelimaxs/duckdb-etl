select 
    sales_year,
    sales_quarter,
    sales_month,
    sum(dollars_sold) as dollars_sold
from 
    sales.sales as s 
left join 
    sales.dim_date as d 
on 
    s.sales_date_dim_id = d.sales_date_id
group by all