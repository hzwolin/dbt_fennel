WITH joining_day_location AS (
    SELECT * 
    FROM {{ref('prep_forecast_day')}}
    LEFT JOIN {{ref('staging_location')}}
    USING(city, region, country)
),
adding_features AS (
    SELECT 
        *,
        CONCAT('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](', condition_icon, '?width=35)') AS condition_icon_md,
        CASE WHEN max_temp_c < 20 OR max_temp_c > 30 THEN 0 ELSE 1 END AS good_temp,
        CASE WHEN avg_temp_c >= 20 THEN 1 ELSE 0 END AS good_avg,
        CASE WHEN total_precip_mm = 0 THEN 1 ELSE 0 END AS no_rain,
        CASE WHEN daily_will_it_rain = 0 THEN 1 ELSE 0 END AS good_rain,
        CASE WHEN max_wind_kph > 5 AND max_wind_kph < 12 THEN 1 ELSE 0 END AS good_wind,
        CASE WHEN avg_vis_km > 8 THEN 1 ELSE 0 END AS good_vis,
        CASE WHEN uv >= 3 THEN 1 ELSE 0 END AS sunscreen_needed
    FROM joining_day_location
),
filtering_ordering_features AS (
    SELECT 
        date,
        city,
        country,
        month_of_year,
        max_temp_c,
        min_temp_c,
        avg_temp_c,
        total_precip_mm,
        daily_will_it_rain,
        daily_chance_of_rain,
        condition_text,
        condition_code,
        max_wind_kph,
        avg_vis_km,
        uv,
        good_temp,
        good_avg,
        no_rain,
        good_rain,
        good_wind,
        good_vis,
        sunscreen_needed,
        lat,
        lon,
        country
    FROM adding_features
)
SELECT * 
FROM filtering_ordering_features