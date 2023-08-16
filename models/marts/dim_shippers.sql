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
          , zip_postal_code
          , country_region
        from {{ ref('stg_shippers')}}
    )
    , transformed as (
        select 
           row_number() over (order by id) as shippers_sk
           , *
           from selected
    )
select * from transformed