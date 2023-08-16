with
    selected as (
        select 
          id
          , company
          , first_name || ' ' || last_name as contact_name
          , email_address
          , business_phone
          , fax_number
          , "address"
          , city
          , state_province
          , country_region          
        from {{ ref('stg_customers')}}
    )
    , transformed as (
        select 
           row_number() over (order by id) as customers_sk
           , *
           from selected
    )
select * from transformed