
{{config(materialized='table')}}

with last_credits_movies as (
select 
    dbt_scd_id as dim_movie_id,
    tmdb_id,
    credits,
    dlt_id, 
    dbt_valid_from,
    dbt_valid_to,
    ROW_NUMBER() OVER(PARTITION BY tmdb_id ORDER BY dbt_updated_at DESC) as rn
from {{ref('int_last_movie_changes_snapshot')}}
),
flatten_credits as (
select 
    dim_movie_id,
    tmdb_id as tmdb_movie_id,	    
    unnest(from_json(credits.cast, '[{"id":"INTEGER"}]')).id as tmdb_person_id,    
    unnest(from_json(credits.cast, '[{"credit_id":"VARCHAR"}]')).credit_id as credit_id,    
    unnest(from_json(credits.cast, '[{"character":"VARCHAR"}]')).character as character, 
    'Acting' as department,
    'Acting' as Job,
    case when rn = 1 then 1 else 0 end as is_current,
    dbt_valid_from::DATE as valid_from,
    case when rn = 1 then '9999-12-31'::DATE else(dbt_valid_to::DATE - interval 1 day)::DATE end as valid_to
from last_credits_movies
union all 
select 
	dim_movie_id,
    tmdb_id as tmdb_movie_id,	 
    unnest(from_json(credits.crew, '[{"id":"INTEGER"}]')).id as tmdb_person_id,
    unnest(from_json(credits.crew, '[{"credit_id":"VARCHAR"}]')).credit_id as credit_id,
    null as character,
    unnest(from_json(credits.crew, '[{"department":"VARCHAR"}]')).department as department,
	unnest(from_json(credits.crew, '[{"job":"VARCHAR"}]')).job as Job,            
    case when rn = 1 then 1 else 0 end as is_current,
    dbt_valid_from::DATE as valid_from,
    case when rn = 1 then '9999-12-31'::DATE else(dbt_valid_to::DATE - interval 1 day)::DATE end as valid_to
from last_credits_movies
)
select
    md5(flatten_credits.dim_movie_id || flatten_credits.tmdb_person_id || flatten_credits.credit_id) as dim_credit_id,
    flatten_credits.dim_movie_id,
    flatten_credits.tmdb_movie_id,
    flatten_credits.tmdb_person_id,
    flatten_credits.credit_id,
    flatten_credits.character,
    flatten_credits.department,
    flatten_credits.job,
    flatten_credits.is_current,
    flatten_credits.valid_from,
    flatten_credits.valid_to
from flatten_credits