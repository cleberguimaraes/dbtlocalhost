with
    validation as (
        select sum(quantity) as sum_val
        from {{( 'dbt_northwind.fact_order_details' )}}
        where order_date  BETWEEN '2006-03-01' and '2006-03-31'
    )
select * from validation  where sum_val != 1092   