-- Staging tables
CREATE TABLE IF NOT EXISTS staging_weather (
    city VARCHAR(100),
    country VARCHAR(10),
    timestamp TIMESTAMP,
    temperature DECIMAL(5,2),
    humidity INTEGER,
    pressure INTEGER,
    wind_speed DECIMAL(5,2),
    weather_description VARCHAR(100),
    extraction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_population (
    city VARCHAR(100),
    country VARCHAR(10),
    population BIGINT,
    year INTEGER,
    extraction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Warehouse Tables
CREATE TABLE IF NOT EXISTS dim_city (
    city_id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_name VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    population BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city_name, country_code)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    day_of_week INTEGER
);

CREATE TABLE IF NOT EXISTS fact_weather (
    weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id INTEGER REFERENCES dim_city(city_id),
    date_id INTEGER REFERENCES dim_date(date_id),
    temperature DECIMAL(5,2),
    humidity INTEGER,
    pressure INTEGER,
    wind_speed DECIMAL(5,2),
    weather_condition VARCHAR(100),
    recorded_time TIMESTAMP,
    loaded_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_fact_weather_city_date ON fact_weather(city_id, date_id);
CREATE INDEX IF NOT EXISTS idx_dim_city_name_country ON dim_city(city_name, country_code);
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);