with customer_data AS (

select * from {{ ref('customers_ids')}}

)
select * from customer_data