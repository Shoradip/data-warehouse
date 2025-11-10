import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils import load_config

def test_simple_sqlite():
    print("🚀 SIMPLE SQLITE ETL TEST")
    print("=" * 50)
    
    print("📋 Loading configuration...")
    config = load_config()
    
    if not config:
        print("❌ Config failed")
        return False
    
    print("✅ Config loaded")
    
    try:
        # 1. EXTRACT
        print("\n🌤️ EXTRACTING WEATHER DATA...")
        extractor = DataExtractor(config)
        cities = config['cities']
        
        weather_data = extractor.extract_weather_data(cities)
        print(f"✅ Got weather for {len(weather_data)} cities")
        
        # Show what we extracted
        for _, row in weather_data.iterrows():
            print(f"   {row['city']}: {row['temperature']}°C, {row['weather_description']}")
        
        # 2. TRANSFORM  
        print("\n🔄 CLEANING DATA...")
        transformer = DataTransformer()
        clean_weather = transformer.clean_weather_data(weather_data)
        print(f"✅ Cleaned {len(clean_weather)} records")
        
        # 3. LOAD
        print("\n💾 LOADING TO DATABASE...")
        loader = DataLoader(config)
        
        # Create tables
        print("   Creating tables...")
        success = loader.create_tables()
        if success:
            print("   ✅ Tables created")
        else:
            print("   ❌ Table creation failed")
            return False
        
        # Load to staging
        print("   Loading to staging...")
        loader.load_to_staging(clean_weather, 'staging_weather')
        print("   ✅ Data staged")
        
        # Load to warehouse
        print("   Loading to warehouse...")
        loader.load_to_warehouse()
        print("   ✅ Data in warehouse")
        
        print("\n🎉 SUCCESS! ETL pipeline completed with SQLite!")
        print("💾 Database file created: data_warehouse.db")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_simple_sqlite()