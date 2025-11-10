-- Business Intelligence Queries

-- 1. Average temperature by city
SELECT 
    dc.city_name,
    dc.country_code,
    AVG(fw.temperature) as avg_temperature,
    MAX(fw.temperature) as max_temperature,
    MIN(fw.temperature) as min_temperature
FROM fact_weather fw
JOIN dim_city dc ON fw.city_id = dc.city_id
GROUP BY dc.city_name, dc.country_code
ORDER BY avg_temperature DESC;

-- 2. Weather patterns by month
SELECT 
    dd.month,
    dd.year,
    AVG(fw.temperature) as avg_temperature,
    AVG(fw.humidity) as avg_humidity
FROM fact_weather fw
JOIN dim_date dd ON fw.date_id = dd.date_id
GROUP BY dd.month, dd.year
ORDER BY dd.year, dd.month;

-- 3. Cities with highest population and their weather
SELECT 
    dc.city_name,
    dc.country_code,
    dc.population,
    AVG(fw.temperature) as avg_temperature
FROM dim_city dc
JOIN fact_weather fw ON dc.city_id = fw.city_id
WHERE dc.population IS NOT NULL
GROUP BY dc.city_name, dc.country_code, dc.population
ORDER BY dc.population DESC
LIMIT 10;

-- 4. Daily weather summary
SELECT 
    dd.full_date,
    dc.city_name,
    COUNT(*) as readings_count,
    AVG(fw.temperature) as avg_temperature
FROM fact_weather fw
JOIN dim_date dd ON fw.date_id = dd.date_id
JOIN dim_city dc ON fw.city_id = dc.city_id
GROUP BY dd.full_date, dc.city_name
ORDER BY dd.full_date DESC, dc.city_name;