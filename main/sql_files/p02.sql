select 
    sales_year,
    sales_quarter,
    sales_month,
    sum(quantity_sold) as quantity_sold
from 
    sales.sales as s 
left join 
    sales.dim_date as d 
on 
    s.sales_date_dim_id = d.sales_date_id
group by all