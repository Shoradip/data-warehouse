import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils import load_config

def test_fixed_pipeline():
    print("🚀 Testing Fixed ETL Pipeline")
    print("=" * 50)
    
    config = load_config()
    if not config:
        print("❌ Failed to load configuration")
        return False
    
    print("✅ Configuration loaded")
    
    try:
        # Extract
        print("\n📥 EXTRACTING DATA...")
        extractor = DataExtractor(config)
        cities = config.get('cities')
        
        weather_data = extractor.extract_weather_data(cities)
        population_data = extractor.extract_population_data()
        
        print(f"   Weather records: {len(weather_data)}")
        print(f"   Population records: {len(population_data)}")
        
        # Transform
        print("\n🔄 TRANSFORMING DATA...")
        transformer = DataTransformer()
        clean_weather = transformer.clean_weather_data(weather_data)
        clean_population = transformer.clean_population_data(population_data)
        
        print(f"   Cleaned weather: {len(clean_weather)}")
        print(f"   Cleaned population: {len(clean_population)}")
        
        # Load
        print("\n📤 LOADING DATA...")
        loader = DataLoader(config)
        
        print("   Creating tables...")
        loader.create_tables()
        
        print("   Loading to staging...")
        loader.load_to_staging(clean_weather, 'staging_weather')
        loader.load_to_staging(clean_population, 'staging_population')
        
        print("   Loading to warehouse...")
        success = loader.load_to_warehouse()
        
        if success:
            print("\n🎉 SUCCESS: ETL Pipeline completed!")
            
            # Show sample data
            print("\n📊 SAMPLE WEATHER DATA:")
            print(clean_weather[['city', 'temperature', 'humidity', 'weather_description']].head())
            
            print("\n📊 SAMPLE POPULATION DATA:")
            print(clean_population[['city', 'country', 'population']].head())
            
        else:
            print("\n❌ Pipeline failed at loading stage")
            
        return success
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_fixed_pipeline()