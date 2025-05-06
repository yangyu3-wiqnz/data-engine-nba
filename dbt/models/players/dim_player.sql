

{{ config(materialized='table') }}

with source_api.ext_players as (

    select 1 as id
    union all
    select 2 as full_name
    union all
    select 3 as first_name
    union all
    select 4 as last_name
    union all
    select 5 as is_active

)

select *
from source_api.ext_players


