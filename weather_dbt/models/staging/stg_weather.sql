with source as (
    select 
        cast(date as DATE) as day_date,
        weather_code,
        temperature_2m_max as max_temp_f,
        temperature_2m_min as min_temp_f,
        cast(sunrise as time) as sunrise,
        cast(sunset as time) as sunset,
        round(wind_speed_10m_max, 2) as wind_speed_kmh,
        CAST(s.loaded_at AS date) as loaded_at
    from {{source('raw', 'weather_raw')}}
), f_to_c as (
    select 
        day_date,
        ROUND((max_temp_f - 32) * 5/9, 2) as max_temp_c,
        ROUND((min_temp_f - 32) * 5/9, 2) as min_temp_c
    from source
), weather_description as (
    select 
        day_date,
        CASE s.weather_code
        WHEN 0  THEN 'Clear sky'
        WHEN 1  THEN 'Mainly clear'
        WHEN 2  THEN 'Partly cloudy'
        WHEN 3  THEN 'Overcast'
        WHEN 45 THEN 'Foggy'
        WHEN 51 THEN 'Light drizzle'
        WHEN 61 THEN 'Slight rain'
        WHEN 71 THEN 'Slight snow'
        WHEN 80 THEN 'Slight showers'
        WHEN 95 THEN 'Thunderstorm'
        ELSE 'Other'
        END 
        as weather_state,
        round(fc.max_temp_c - fc.min_temp_c, 2) as temp_range

    from source as s, f_to_c as fc
)
select
    s.day_date,
    fc.max_temp_c,
    fc.min_temp_c,
    wd.temp_range,
    s.wind_speed_kmh,
    s.sunrise,
    s.sunset,
    wd.weather_state,
    s.loaded_at
from source as s, f_to_c as fc, weather_description as wd