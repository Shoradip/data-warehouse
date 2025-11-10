import pandas as pd
from sqlalchemy import text
from utils import setup_logging, get_db_connection

logger = setup_logging()

class DataLoader:
    def __init__(self, config):
        self.config = config
        self.engine = get_db_connection(config)
        self.tables = config['tables']
    
    def load_to_staging(self, df, table_name):
        """Load data to staging tables"""
        try:
            if df.empty:
                logger.warning(f"No data to load to {table_name}")
                return False
                
            with self.engine.connect() as conn:
                df.to_sql(
                    table_name, 
                    conn, 
                    if_exists='replace', 
                    index=False,
                    method='multi'
                )
            logger.info(f"Successfully loaded {len(df)} records to staging table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error loading data to staging table {table_name}: {e}")
            return False
    
    def load_to_warehouse(self):
        """Load data from staging to data warehouse"""
        try:
            with self.engine.connect() as conn:
                # Check if we're using SQLite
                is_sqlite = 'sqlite' in str(self.engine.url)
                
                # Load dim_city
                if is_sqlite:
                    # SQLite version - use INSERT OR REPLACE
                    conn.execute(text("""
                        INSERT OR REPLACE INTO dim_city (city_name, country_code, population)
                        SELECT 
                            sw.city as city_name,
                            sw.country as country_code,
                            sp.population
                        FROM staging_weather sw
                        LEFT JOIN staging_population sp 
                            ON sw.city = sp.city AND sw.country = sp.country
                    """))
                else:
                    # PostgreSQL version
                    conn.execute(text("""
                        INSERT INTO dim_city (city_name, country_code, population)
                        SELECT 
                            sw.city as city_name,
                            sw.country as country_code,
                            sp.population
                        FROM staging_weather sw
                        LEFT JOIN staging_population sp 
                            ON sw.city = sp.city AND sw.country = sp.country
                        ON CONFLICT (city_name, country_code) 
                        DO UPDATE SET 
                            population = EXCLUDED.population,
                            last_updated = CURRENT_TIMESTAMP
                    """))
                
                # Load dim_date (if not exists)
                if is_sqlite:
                    # SQLite version - use SQLite date functions
                    conn.execute(text("""
                        INSERT OR IGNORE INTO dim_date (full_date, year, month, day, quarter, day_of_week)
                        SELECT DISTINCT
                            date(sw.timestamp) as full_date,
                            CAST(strftime('%Y', sw.timestamp) AS INTEGER) as year,
                            CAST(strftime('%m', sw.timestamp) AS INTEGER) as month,
                            CAST(strftime('%d', sw.timestamp) AS INTEGER) as day,
                            CAST((strftime('%m', sw.timestamp) - 1) / 3 + 1 AS INTEGER) as quarter,
                            CAST(strftime('%w', sw.timestamp) AS INTEGER) + 1 as day_of_week
                        FROM staging_weather sw
                    """))
                else:
                    # PostgreSQL version
                    conn.execute(text("""
                        INSERT INTO dim_date (full_date, year, month, day, quarter, day_of_week)
                        SELECT DISTINCT
                            DATE(sw.timestamp) as full_date,
                            EXTRACT(YEAR FROM sw.timestamp) as year,
                            EXTRACT(MONTH FROM sw.timestamp) as month,
                            EXTRACT(DAY FROM sw.timestamp) as day,
                            EXTRACT(QUARTER FROM sw.timestamp) as quarter,
                            EXTRACT(DOW FROM sw.timestamp) + 1 as day_of_week
                        FROM staging_weather sw
                        ON CONFLICT (full_date) DO NOTHING
                    """))
                
                # Load fact_weather
                conn.execute(text("""
                    INSERT INTO fact_weather (city_id, date_id, temperature, humidity, pressure, wind_speed, weather_condition, recorded_time)
                    SELECT 
                        dc.city_id,
                        dd.date_id,
                        sw.temperature,
                        sw.humidity,
                        sw.pressure,
                        sw.wind_speed,
                        sw.weather_description,
                        sw.timestamp
                    FROM staging_weather sw
                    JOIN dim_city dc ON sw.city = dc.city_name AND sw.country = dc.country_code
                    JOIN dim_date dd ON date(sw.timestamp) = dd.full_date
                """))
                
                conn.commit()
            
            logger.info("Successfully loaded data to data warehouse")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data to warehouse: {e}")
            return False
    
    def create_tables(self):
        """Create database tables"""
        try:
            with self.engine.connect() as conn:
                with open('sql/create_tables.sql', 'r') as file:
                    sql_script = file.read()
                
                # Execute SQL script
                for statement in sql_script.split(';'):
                    if statement.strip():
                        conn.execute(text(statement))
                conn.commit()
            
            logger.info("Successfully created database tables")
            return True
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False