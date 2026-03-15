with daily as (
    select * from {{ ref('stg_weather') }}
),

weekly_summary as (
    select
        DATE_TRUNC('week', weather_date)   as week_start,
        COUNT(*)                           as days_with_data,
        
        ROUND(AVG(temp_max_c), 1)          as avg_max_temp_c,
        ROUND(MAX(temp_max_c), 1)          as hottest_day_c,
        ROUND(MIN(temp_min_c), 1)          as coldest_night_c,
        
        ROUND(SUM(precipitation_mm), 1)   as total_rain_mm,
        ROUND(AVG(windspeed_max_kmh), 1)  as avg_wind_kmh,
        
        -- was this a rainy week?
        CASE
            WHEN SUM(precipitation_mm) > 20 THEN 'Rainy'
            WHEN SUM(precipitation_mm) > 5  THEN 'Some rain'
            ELSE 'Dry'
        END                                as rain_category

    from daily
    group by DATE_TRUNC('week', weather_date)
    order by week_start
)

select * from weekly_summary