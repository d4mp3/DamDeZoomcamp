with 

source as (

    select * from {{ source('staging', 'fhv_tripdata') }}
    where pickup_datetime is not null and dropoff_datetime is not null

),

renamed as (

    select
        int64_field_0,
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        cast(pulocationid as numeric) as pulocationid,
        cast(dolocationid as numeric) as dolocationid,
        sr_flag,
        affiliated_base_number

    from source
    where pickup_datetime >= '2019-01-01' and pickup_datetime < '2020-01-01'

)

select * from renamed


-- dbt build --select <model.sql> --vars '{'is_test_run': 'false'}'
-- dbt build --select stg_fhv_tripdata --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
