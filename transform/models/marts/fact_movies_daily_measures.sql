{{
    config(
        materialized='incremental',
        unique_key=['dim_movie_id'],
        incremental_strategy='merge'
    )
}}
with last_movies as (
select 
    dbt_scd_id as dim_movie_id,
    tmdb_id,
    imdb_id,
    status,
    original_title,
    release_date,
    budget,
    revenue,
    runtime,
    popularity,
    vote_average,
    vote_count,
    original_language,
    genres,
    production_companies,
    production_countries,
    spoken_languages,
    origin_country,
    credits,
    overview,
    poster_path,
    tagline,
    title,
    video,
    belongs_to_collection__id,
    belongs_to_collection__name,
    belongs_to_collection__poster_path,
    belongs_to_collection__backdrop_path,
    adult,
    backdrop_path,
    load_id,
    load_date,
    dlt_id, 
    dbt_valid_from,
    dbt_valid_to,
    ROW_NUMBER() OVER(PARTITION BY tmdb_id ORDER BY dbt_updated_at DESC) as rn
from {{ref('int_last_movie_changes_snapshot')}}
),
new_movies_measures as (
select 
    dim_movie_id,
    tmdb_id,        
    budget,
    revenue,
    popularity,
    vote_average,
    vote_count,        
    {% if is_incremental() %}CURRENT_DATE as fact_date{% else %}dbt_valid_from::DATE as fact_date{% endif %}
from last_movies
{% if is_incremental() %} 
where rn = 1
{% endif %}
)
{% if is_incremental() %}
,
last_snapshot AS (
        SELECT
        ls.dim_movie_id,
        lm.tmdb_id,        
        ls.budget,
        ls.revenue,
        ls.popularity,
        ls.vote_average,
        ls.vote_count,        
        ls.fact_date
    FROM {{ this }} ls 
        JOIN {{ref('int_last_movie_changes_snapshot')}} lm ON ls.dim_movie_id = lm.dbt_scd_id
    WHERE ls.fact_date = (SELECT MAX(fact_date) FROM {{ this }})
)
{% endif %}
SELECT
    n.dim_movie_id,    
    n.fact_date,
    n.popularity,
    n.vote_average,
    n.vote_count,
    n.budget,
    n.revenue
FROM new_movies_measures n
{% if is_incremental() %}
LEFT JOIN last_snapshot l ON n.tmdb_id = l.tmdb_id
WHERE
    l.tmdb_id IS NULL
    OR n.vote_count != l.vote_count
    OR n.vote_average != l.vote_average
    OR n.budget != l.budget
    OR n.revenue != l.revenue
    OR ABS(n.popularity - l.popularity) > 0.01
{% endif %}