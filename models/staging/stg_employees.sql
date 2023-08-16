with
    source as (
        select 
            id
            , company
            , last_name
            , first_name
            , email_address
            , job_title
            , business_phone
            , home_phone
            , mobile_phone
            , fax_number
            , "address"
            , city
            , state_province
            , zip_postal_code
            , country_region
            , web_page
            , notes
            , attachments
        from {{source('northwind','employees')}}
    )
select * from source