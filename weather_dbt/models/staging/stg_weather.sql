with source as (
    select 
        cast(date as DATE)              as day_date,
        weather_code,
        max_temp_f,
        min_temp_f,
        sunrise,
        sunset,
        round(wind_speed_kmh, 2) as wind_speed_kmh,
        cast(loaded_at as DATE) as loaded_at
    from {{ source('raw', 'weather_raw') }}
),

transformed as (
    select
        day_date,
        ROUND((max_temp_f - 32) * 5/9, 2) as max_temp_c,
        ROUND((min_temp_f - 32) * 5/9, 2) as min_temp_c,

        ROUND(((max_temp_f - 32) * 5/9) - ((min_temp_f - 32) * 5/9), 2) as temp_range_c,
        
        wind_speed_kmh,

        CAST(sunrise AS TIME) as sunrise,
        CAST(sunset  AS TIME) as sunset,

        CASE weather_code
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
        loaded_at

    from source
)

select * from transformed