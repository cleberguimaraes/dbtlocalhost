with
    source as (
        select 
          order_id
          ,  employee_id
          ,  customer_id
          ,  status_id
          ,  shipper_id
          ,  ship_name
          ,  ship_address
          ,  ship_city
          ,  ship_state_province
          ,  ship_zip_postal_code
          ,  ship_country_region
          ,  shipping_fee
          ,  taxes
          ,  payment_type
          ,  order_date
          ,  shipped_date
          ,  paid_date          
          ,  notes
          ,  tax_rate
          ,  tax_status_id
        from {{source('northwind','orders')}}
    )
select * from source