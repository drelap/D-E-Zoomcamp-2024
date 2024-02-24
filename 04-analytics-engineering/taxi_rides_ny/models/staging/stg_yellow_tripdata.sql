{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by vendor_id, pickup_datetime) as rn
  from {{ source('staging','yellow_tripdata') }}
  where vendor_id is not null 
)
select  
-- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id','pickup_datetime']) }} as tripid,
    cast(vendor_id as integer) as vendorid,
    -- cast(rate_code as integer) as ratecodeid,
    rate_code as ratecodeid,
    cast(pickup_location_id as integer) as  pickup_locationid,
    cast(dropoff_location_id as integer) as dropoff_locationid,
    
    -- timestamps
    pickup_datetime,
    dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    '1.0' as trip_type,
    
    -- payment info
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    0 as ehail_fee,
    imp_surcharge as improvement_surcharge,
    total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description
    -- cast(congestion_surcharge as numeric) as congestion_surcharge

from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
