{{ config(materialized="view") }}

select 
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    cast(sr_flag as integer) as is_shared_ride,
    affiliated_base_number
from {{ source("staging", "fhv_trip_external") }}

{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}