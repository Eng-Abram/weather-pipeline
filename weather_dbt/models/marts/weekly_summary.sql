with daily as (
    select * from {{ ref('stg_weather') }}
),
weekly_summary as (
    select
        DATE_TRUNC('week', day_date) as week_start,
        COUNT(*) as days_with_data,
        
        ROUND(AVG(max_temp_c), 1) as avg_max_temp_c,
        ROUND(MAX(max_temp_c), 1) as hottest_day_c,
        ROUND(MIN(min_temp_c), 1) as coldest_night_c,
        
        ROUND(AVG(wind_speed_kmh), 1) as avg_wind_kmh,

    from daily
    group by DATE_TRUNC('week', day_date)
    order by week_start
)

select * from weekly_summary