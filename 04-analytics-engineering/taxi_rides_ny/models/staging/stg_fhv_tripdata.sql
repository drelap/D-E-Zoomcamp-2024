{{ config(materialized='view') }}

select 
  -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num','pickup_datetime']) }} as tripid,
  -- timestamps
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    sr_flag,
    affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
where EXTRACT(YEAR FROM pickup_datetime) = 2019

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}