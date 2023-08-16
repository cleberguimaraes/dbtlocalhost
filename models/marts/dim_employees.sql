with
    selected as (
        select 
            id
            , company
            , first_name || ' ' || last_name as contact_name
            , email_address
            , job_title
            , business_phone
            , fax_number
            , "address"
            , city
            , state_province
            , zip_postal_code
            , country_region
        from {{ ref('stg_employees')}}
    )
    , transformed as (
        select 
           row_number() over (order by id) as employees_sk
           , *
           from selected
    )
select * from transformed