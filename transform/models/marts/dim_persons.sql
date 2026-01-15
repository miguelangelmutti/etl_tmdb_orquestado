{{ config(materialized='table') }}
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
    case when rn = 1 then 1 else 0 end as is_current,
    dbt_valid_from::DATE as valid_from,
    case when rn = 1 then '9999-12-31'::DATE else(dbt_valid_to::DATE - interval 1 day)::DATE end as valid_to
from last_persons