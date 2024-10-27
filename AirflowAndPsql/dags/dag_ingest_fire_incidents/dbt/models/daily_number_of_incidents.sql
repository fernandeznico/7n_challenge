{{
    config(
        materialized='incremental',
        unique_key='incident_date'
    )
}}

SELECT incident_date::date as incident_date, count(*) as quantity
FROM {{ source('raw_incidents', 'raw_incidents') }}
    {% if is_incremental() %}
    WHERE incident_date = '{{ var("ds") }}' || 'T00:00:00.000'
    {% endif %}
GROUP BY incident_date::date