
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
)
flatten_credits as (
select 
    dim_movie_id,
    tmdb_id,
	unnest(from_json(credits, '[{"adult":"BOOLEAN"}]')).adult as adult,
    unnest(from_json(credits, '[{"gender":"INTEGER"}]')).gender as gender,
    unnest(from_json(credits, '[{"id":"INTEGER"}]')).id as id,
    unnest(from_json(credits, '[{"known_for_department":"VARCHAR"}]')).known_for_department as known_for_department,
    unnest(from_json(credits, '[{"name":"VARCHAR"}]')).name as name,
    unnest(from_json(credits, '[{"original_name":"VARCHAR"}]')).original_name as original_name,
    unnest(from_json(credits, '[{"popularity":"FLOAT"}]')).popularity as popularity,
    unnest(from_json(credits, '[{"profile_path":"VARCHAR"}]')).profile_path as profile_path,
    unnest(from_json(credits, '[{"cast_id":"INTEGER"}]')).cast_id as cast_id,
    unnest(from_json(credits, '[{"character":"VARCHAR"}]')).character as character,
    unnest(from_json(credits, '[{"credit_id":"VARCHAR"}]')).credit_id as credit_id,
    unnest(from_json(credits, '[{"order":"INTEGER"}]')).order as order,
    case when rn = 1 then 1 else 0 end as is_current,
    dbt_valid_from::DATE as valid_from,
    case when rn = 1 then '9999-12-31'::DATE else(dbt_valid_to::DATE - interval 1 day)::DATE end as valid_to
from last_credits_movies
)
select
    md5(flatten_credits.dim_movie_id || flatten_credits.id) as dim_credit_id,
    flatten_credits.dim_movie_id,
    flatten_credits.tmdb_id,
    flatten_credits.credit_id,
    flatten_credits.adult,
    flatten_credits.gender,
    flatten_credits.id,
    flatten_credits.known_for_department,
    flatten_credits.name,
    flatten_credits.original_name,
    flatten_credits.popularity,
    flatten_credits.profile_path,
    flatten_credits.cast_id,
    flatten_credits.character,
    flatten_credits.order,
    flatten_credits.is_current,
    flatten_credits.valid_from,
    flatten_credits.valid_to
from flatten_credits