{{config(materialized='incremental', unique_key=['dim_movie_genre_id','genre_id'])}}
with movie_genres as (
select  m.dbt_scd_id,
		unnest(from_json(genres, '[{"id":"INTEGER"}]')).id as genre_id,
        (1.00 / json_array_length(genres)) as genre_weight,
        m.dbt_valid_from,
        m.dbt_valid_to,
        ROW_NUMBER() OVER(PARTITION BY m.tmdb_id ORDER BY m.dbt_updated_at DESC) as rn
from {{ref('int_last_movie_changes_snapshot')}} m
)
select 
    md5(mg.dbt_scd_id || mg.genre_id) as dim_movie_genre_id,
    mg.dbt_scd_id as dim_movie_id,
    mg.genre_id,
    mg.genre_weight,
    mg.dbt_valid_from,
    mg.dbt_valid_to,
    case when mg.rn = 1 then 1 else 0 end as is_current,
    mg.dbt_valid_from::DATE as valid_from,
    case when mg.rn = 1 then '9999-12-31'::DATE else(mg.dbt_valid_to::DATE - interval 1 day)::DATE end as valid_to
    from movie_genres mg