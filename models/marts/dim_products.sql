with
    selected as (
        select 
            id
            , supplier_ids
            ,  product_id
            , product_name
            , "description"
            , category
            , reorder_level
            , list_price
            , is_discontinued  
            , standard_cost
            , target_level
            , quantity_per_unit
        from {{ ref('stg_products')}}
    )
    , transformed as (
        select 
           row_number() over (order by id) as products_sk
           , *
           from selected
    )
select * from transformed