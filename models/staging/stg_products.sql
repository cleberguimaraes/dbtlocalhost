with
    source as (
        select 
            id
            , supplier_ids
            , product_id
            , product_name
            , "description"
            , category
            , reorder_level
            , list_price
            , 
              case 
                when discontinued then True
                else False
              end as is_discontinued  
            , standard_cost
            , target_level
            , quantity_per_unit
            , minimum_reorder_quantity
            , attachments 
        from {{source('northwind','products')}}
    )
select * from source