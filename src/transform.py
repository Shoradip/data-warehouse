import pandas as pd
from utils import setup_logging

logger = setup_logging()

class DataTransformer:
    def __init__(self):
        self.logger = logger
    
    def clean_weather_data(self, df):
        """Clean and transform weather data"""
        try:
            if df.empty:
                self.logger.warning("No weather data to clean")
                return df
            
            # Remove duplicates
            df = df.drop_duplicates()
            
            # Handle missing values
            numeric_columns = ['temperature', 'humidity', 'pressure', 'wind_speed']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].fillna(df[col].mean())
            
            # Standardize text data
            if 'weather_description' in df.columns:
                df['weather_description'] = df['weather_description'].str.lower().str.strip()
            if 'city' in df.columns:
                df['city'] = df['city'].str.title().str.strip()
            if 'country' in df.columns:
                df['country'] = df['country'].str.upper().str.strip()
            
            # Add extraction timestamp
            df['extraction_time'] = pd.Timestamp.now()
            
            self.logger.info(f"Successfully cleaned {len(df)} weather records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error cleaning weather data: {e}")
            return pd.DataFrame()
    
    def clean_population_data(self, df):
        """Clean and transform population data"""
        try:
            if df.empty:
                self.logger.warning("No population data to clean")
                return df
            
            # Rename columns for consistency
            column_mapping = {
                'City': 'city',
                'Country': 'country', 
                'Population': 'population',
                'Year': 'year'
            }
            
            # Apply renaming for columns that exist
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # Handle missing values
            if 'population' in df.columns:
                df['population'] = pd.to_numeric(df['population'], errors='coerce')
                df = df.dropna(subset=['population'])
            
            # Standardize text data
            if 'city' in df.columns:
                df['city'] = df['city'].str.title().str.strip()
            if 'country' in df.columns:
                df['country'] = df['country'].str.upper().str.strip()
            
            # Filter for latest year data if year column exists
            if 'year' in df.columns and not df.empty:
                latest_year = df['year'].max()
                df = df[df['year'] == latest_year]
            
            # Add extraction timestamp
            df['extraction_time'] = pd.Timestamp.now()
            
            self.logger.info(f"Successfully cleaned {len(df)} population records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error cleaning population data: {e}")
            return pd.DataFrame()
    
    def create_dim_date(self, start_date='2020-01-01', end_date='2025-12-31'):
        """Create date dimension table"""
        try:
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            dim_date = pd.DataFrame({'full_date': date_range})
            
            dim_date['date_id'] = dim_date.index + 1
            dim_date['year'] = dim_date['full_date'].dt.year
            dim_date['month'] = dim_date['full_date'].dt.month
            dim_date['day'] = dim_date['full_date'].dt.day
            dim_date['quarter'] = dim_date['full_date'].dt.quarter
            dim_date['day_of_week'] = dim_date['full_date'].dt.dayofweek + 1
            
            self.logger.info("Successfully created date dimension")
            return dim_date[['date_id', 'full_date', 'year', 'month', 'day', 'quarter', 'day_of_week']]
            
        except Exception as e:
            self.logger.error(f"Error creating date dimension: {e}")
            return pd.DataFrame()