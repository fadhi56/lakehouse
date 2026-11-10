with raw_customer_data AS (

select * from {{ source('bronze_customers', 'customers')}}

)
select * from raw_customer_data