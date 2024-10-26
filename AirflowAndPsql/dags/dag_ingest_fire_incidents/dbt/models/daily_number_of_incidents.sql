{{
    config(
        materialized='incremental',
        unique_key='incident_date'
    )
}}

SELECT '{{ ds }}'::date as incident_date, count(*) as quantity
FROM {{ source('raw_incidents', 'raw_incidents') }}

{% if is_incremental() %}

WHERE incident_date = '{{ ds }}' || 'T00:00:00.000'

{% endif %}