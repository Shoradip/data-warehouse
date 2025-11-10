import sys
import os

# Add src to path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils import load_config, setup_logging

def test_complete_pipeline():
    print("🚀 Testing Complete ETL Pipeline")
    print("=" * 60)
    
    # Setup
    logger = setup_logging()
    config = load_config()
    
    if not config:
        print("❌ Failed to load configuration")
        return False
    
    print("✅ Configuration loaded")
    
    try:
        # 1. EXTRACT
        print("\n📥 STEP 1: EXTRACT")
        print("-" * 30)
        extractor = DataExtractor(config)
        
        cities = config.get('cities', [
            {'city': 'London', 'country': 'GB'},
            {'city': 'New York', 'country': 'US'}
        ])
        
        weather_data = extractor.extract_weather_data(cities)
        population_data = extractor.extract_population_data()
        
        print(f"✅ Extracted {len(weather_data)} weather records")
        print(f"✅ Extracted {len(population_data)} population records")
        
        if weather_data.empty:
            print("❌ No weather data extracted")
            return False
        
        # 2. TRANSFORM
        print("\n🔄 STEP 2: TRANSFORM")
        print("-" * 30)
        transformer = DataTransformer()
        
        clean_weather = transformer.clean_weather_data(weather_data)
        clean_population = transformer.clean_population_data(population_data)
        
        print(f"✅ Cleaned {len(clean_weather)} weather records")
        print(f"✅ Cleaned {len(clean_population)} population records")
        
        # 3. LOAD
        print("\n📤 STEP 3: LOAD")
        print("-" * 30)
        loader = DataLoader(config)
        
        # Initialize database
        print("🗄️ Creating database tables...")
        loader.create_tables()
        
        # Load to staging
        print("📥 Loading to staging tables...")
        loader.load_to_staging(clean_weather, 'staging_weather')
        loader.load_to_staging(clean_population, 'staging_population')
        
        # Load to warehouse
        print("🏢 Loading to data warehouse...")
        success = loader.load_to_warehouse()
        
        if success:
            print("✅ Data successfully loaded to warehouse")
        else:
            print("❌ Failed to load data to warehouse")
        
        print("\n🎉 ETL Pipeline Test Completed!")
        return success
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_complete_pipeline()