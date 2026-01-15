{{ 
    config(
        materialized='incremental', 
        unique_key=['dim_person_id','fact_date'], 
        incremental_strategy='append'
    ) 
}}

with last_persons as (
select 
    dbt_scd_id as dim_person_id, 
    tmdb_id,
    imdb_id,
    adult,
    biography,
    gender,
    known_for_department,
    name,
    also_known_as,
    place_of_birth,
    popularity,
    profile_path,
    load_id,
    load_date,
    dlt_id,
    deathday,
    homepage,    
    dbt_valid_from,
    dbt_valid_to,
    ROW_NUMBER() OVER(PARTITION BY tmdb_id ORDER BY dbt_updated_at DESC) as rn
from {{ref('int_last_person_changes_snapshot')}}
)
select     
    dim_person_id,
    popularity,
    dbt_valid_from::DATE as fact_date
from last_persons
{% if is_incremental() %}
where dbt_valid_from > (select max(fact_date) from {{this}})
{% endif %}
