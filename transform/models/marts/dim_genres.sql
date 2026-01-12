{{config(materialized='incremental', unique_key='genre_id')}}

SELECT distinct 
       unnest(from_json(genres, '[{"id":"INTEGER"}]')).id as genre_id,
       unnest(from_json(genres, '[{"name":"VARCHAR"}]')).name as genre_name,       
       CURRENT_DATE as load_date
from {{ref('int_last_movie_changes')}}
{% if is_incremental() %}
where load_date > (select max(load_date) from {{ this }})
{% endif %}