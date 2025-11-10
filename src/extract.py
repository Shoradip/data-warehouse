import requests
import pandas as pd
from utils import setup_logging, load_config
import time
from datetime import datetime
import random

logger = setup_logging()

class DataExtractor:
    def __init__(self, config):
        self.config = config
        self.api_config = config['api']
        self.data_sources = config['data_sources']
    
    def extract_weather_data(self, cities):
        """Extract weather data from OpenWeatherMap API with fallback"""
        weather_data = []
        api_key = self.api_config.get('weather_api_key')
        
        if not api_key or api_key == 'your_api_key_here':
            logger.warning("No valid API key found, using mock data")
            return self._get_mock_weather_data(cities)
        
        for city in cities:
            try:
                params = {
                    'q': f"{city['city']},{city['country']}",
                    'appid': api_key,
                    'units': 'metric'
                }
                
                response = requests.get(
                    self.api_config['weather_base_url'],
                    params=params,
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    weather_data.append({
                        'city': city['city'],
                        'country': city['country'],
                        'timestamp': pd.to_datetime(data['dt'], unit='s'),
                        'temperature': data['main']['temp'],
                        'humidity': data['main']['humidity'],
                        'pressure': data['main']['pressure'],
                        'wind_speed': data['wind']['speed'],
                        'weather_description': data['weather'][0]['description']
                    })
                    logger.info(f"Successfully extracted weather data for {city['city']}")
                else:
                    logger.warning(f"API failed for {city['city']} (Status: {response.status_code}), using mock data")
                    weather_data.extend(self._get_mock_weather_data([city]))
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error extracting weather data for {city['city']}: {e}")
                weather_data.extend(self._get_mock_weather_data([city]))
        
        return pd.DataFrame(weather_data)
    
    def _get_mock_weather_data(self, cities):
        """Generate mock weather data for testing"""
        mock_data = []
        
        for city in cities:
            mock_data.append({
                'city': city['city'],
                'country': city['country'],
                'timestamp': datetime.now(),
                'temperature': round(random.uniform(5, 30), 2),
                'humidity': random.randint(30, 90),
                'pressure': random.randint(980, 1030),
                'wind_speed': round(random.uniform(0, 15), 2),
                'weather_description': random.choice(['clear sky', 'few clouds', 'scattered clouds', 'broken clouds', 'light rain'])
            })
        return mock_data
    
    def extract_population_data(self):
        """Extract population data from CSV URL with fallback"""
        try:
            df = pd.read_csv(self.data_sources['csv_url'])
            logger.info("Successfully extracted population data from URL")
            return df
        except Exception as e:
            logger.warning(f"Failed to extract population data from URL: {e}")
            logger.info("Using mock population data")
            return self._get_mock_population_data()
    
    def _get_mock_population_data(self):
        """Generate mock population data for testing"""
        mock_data = {
            'City': ['London', 'New York', 'Tokyo', 'Sydney', 'Berlin'],
            'Country': ['GB', 'US', 'JP', 'AU', 'DE'],
            'Population': [8982000, 8419000, 13960000, 5312000, 3645000],
            'Year': [2023, 2023, 2023, 2023, 2023]
        }
        return pd.DataFrame(mock_data)