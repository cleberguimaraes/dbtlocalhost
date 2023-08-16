with
    source as (
        select 
           id
          , order_id
          , product_id::varchar
          , purchase_order_id
          , inventory_id
          , status_id
          , quantity
          , unit_price
          , discount
          , date_allocated
        from {{source('northwind','order_details')}}
    )
select * from source