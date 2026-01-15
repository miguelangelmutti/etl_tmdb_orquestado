{{ config(materialized='view') }}

with tmdb_persons_and_changes as (
select 
    stg_changes.tmdb_id,
    stg_changes.imdb_id,
    stg_changes.adult,
    stg_changes.biography,
    stg_changes.gender,
    stg_changes.known_for_department,
    stg_changes.name,
    stg_changes.also_known_as,
    stg_changes.place_of_birth,
    stg_changes.popularity,
    stg_changes.profile_path,
    stg_changes.load_id,
    stg_changes.load_date,
    stg_changes.dlt_id,
    stg_changes.deathday,
    stg_changes.homepage    
from {{ref('stg_persons_changes')}} as stg_changes
union all
select 
    stg_persons.tmdb_id,
    stg_persons.imdb_id,
    stg_persons.adult,
    stg_persons.biography,
    stg_persons.gender,
    stg_persons.known_for_department,
    stg_persons.name,
    stg_persons.also_known_as,
    stg_persons.place_of_birth,
    stg_persons.popularity,
    stg_persons.profile_path,
    stg_persons.load_id,
    stg_persons.load_date,
    stg_persons.dlt_id,
    stg_persons.deathday,
    stg_persons.homepage    
from {{ref('stg_persons')}} as stg_persons
 ), last_person_changes as (
 select 
    tmdb_id,
    imdb_id,
    ROW_NUMBER() OVER(PARTITION BY tmdb_id ORDER BY  load_id DESC) as row_num,
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
    homepage    
from tmdb_persons_and_changes
qualify row_num = 1)

select 
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
from last_person_changes