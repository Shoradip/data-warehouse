from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils import load_config, setup_logging
import schedule
import time

logger = setup_logging()

class ETLPipeline:
    def __init__(self):
        self.config = load_config()
        self.extractor = DataExtractor(self.config)
        self.transformer = DataTransformer()
        self.loader = DataLoader(self.config)
    
    def run_pipeline(self):
        """Execute complete ETL pipeline"""
        logger.info("Starting ETL Pipeline")
        
        try:
            # Get cities from config
            cities = self.config.get('cities', [
                {'city': 'London', 'country': 'GB'},
                {'city': 'New York', 'country': 'US'},
                {'city': 'Tokyo', 'country': 'JP'},
                {'city': 'Sydney', 'country': 'AU'},
                {'city': 'Berlin', 'country': 'DE'}
            ])
            
            # Extract
            logger.info("Extraction phase started")
            weather_data = self.extractor.extract_weather_data(cities)
            population_data = self.extractor.extract_population_data()
            
            if weather_data.empty:
                logger.error("No weather data extracted. Pipeline stopped.")
                return False
            
            # Transform
            logger.info("Transformation phase started")
            clean_weather = self.transformer.clean_weather_data(weather_data)
            clean_population = self.transformer.clean_population_data(population_data)
            
            # Load to staging
            logger.info("Loading phase started")
            self.loader.load_to_staging(clean_weather, 'staging_weather')
            self.loader.load_to_staging(clean_population, 'staging_population')
            
            # Load to warehouse
            success = self.loader.load_to_warehouse()
            
            if success:
                logger.info("ETL Pipeline completed successfully")
                return True
            else:
                logger.error("ETL Pipeline failed at warehouse loading")
                return False
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}")
            return False
    
    def initialize_database(self):
        """Initialize database tables"""
        logger.info("Initializing database")
        return self.loader.create_tables()

def main():
    """Main function to run the ETL pipeline"""
    pipeline = ETLPipeline()
    
    # Initialize database (run once)
    print("🗄️ Initializing database...")
    pipeline.initialize_database()
    
    # Run immediately
    print("🚀 Running ETL pipeline...")
    pipeline.run_pipeline()
    
    # Schedule to run daily at 8 AM
    schedule.every().day.at("08:00").do(pipeline.run_pipeline)
    
    logger.info("ETL Pipeline scheduled to run daily at 8:00 AM")
    print("⏰ ETL Pipeline scheduled to run daily at 8:00 AM")
    print("💡 Press Ctrl+C to stop the scheduler")
    
    # Keep the script running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n👋 ETL Pipeline stopped")

if __name__ == "__main__":
    main()