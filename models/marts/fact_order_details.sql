with
    customers as (
        select *
        from {{ ref('dim_customers')}}
    )
    , employees as (
        select *
        from {{ ref('dim_employees')}}
    )
    , products as (
        select *
        from {{ ref('dim_products')}}
    )
    , shippers as (
        select *
        from {{ ref('dim_shippers')}}
    )
    , suppliers as (
        select *
        from {{ ref('dim_suppliers')}}
    )
    , orders_with_sk as (
        select 
            orders.id as order_id
            , employees.employees_sk as employee_fk
            , customers.customers_sk as customer_fk
            , shippers.shippers_sk as shipper_fk
            , orders.ship_name
            , orders.ship_address
            , orders.ship_city
            , orders.ship_zip_postal_code
            , orders.ship_country_region
            , orders.shipping_fee
            , orders.order_date
            , orders.shipped_date
            , orders.paid_date     
        from  {{ref ('stg_orders') }}  as orders   
        left join employees on orders.employee_id = employees.id
        left join customers on orders.customer_id = customers.id
        left join shippers on orders.shipper_id = shippers.id
    )
    , order_details_with_sk as (
        select 
            order_details.order_id
            , products.products_sk as product_fk
            , purchase_order_id
            , inventory_id
            , status_id
            , order_details.quantity
            , order_details.unit_price
            , order_details.discount
            , order_details.date_allocated
        from  {{ref('stg_order_details')}}  as order_details
        left join products on order_details.product_id = products.product_id
    )
    , final as (
        select 
            owsk.order_id
            , owsk.employee_fk
            , owsk.customer_fk
            , owsk.shipper_fk
            , owsk.order_date
            , owsk.ship_name
            , owsk.ship_address
            , owsk.ship_city
            , owsk.ship_zip_postal_code
            , owsk.ship_country_region
            , owsk.shipping_fee
            , owsk.shipped_date
            , owsk.paid_date  
            , odwsk.product_fk
            , odwsk.discount
            , odwsk.unit_price
            , odwsk.quantity
        from orders_with_sk owsk
        left join order_details_with_sk odwsk on owsk.order_id = odwsk.order_id
    )
select * from final