{{ config(materialized='view') }}

select
        adult,
        biography,
        birthday,
        gender,
        id as tmdb_id,
        imdb_id,
        known_for_department,
        name,
        also_known_as,
        place_of_birth,
        popularity,
        profile_path,
        _entity_type,
        _dlt_load_id as load_id,
        to_timestamp(_dlt_load_id::DOUBLE) AT TIME ZONE 'America/Argentina/Buenos_Aires' as load_date,
        _dlt_id as dlt_id,
        deathday,
        homepage
from {{source('shared_movies', 'tmdb_persons')}}