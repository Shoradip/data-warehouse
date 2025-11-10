import sys
import os
import sqlite3

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils import load_config

def test_final():
    print("🚀 FINAL ETL PIPELINE TEST")
    print("=" * 50)
    
    print("📋 Loading configuration...")
    config = load_config()
    
    if not config:
        print("❌ Config failed")
        return False
    
    print("✅ Config loaded")
    
    try:
        # 1. EXTRACT
        print("\n🌤️ EXTRACTING DATA...")
        extractor = DataExtractor(config)
        weather_data = extractor.extract_weather_data(config['cities'])
        population_data = extractor.extract_population_data()
        
        print(f"✅ Weather: {len(weather_data)} cities")
        print(f"✅ Population: {len(population_data)} records")
        
        # 2. TRANSFORM  
        print("\n🔄 TRANSFORMING DATA...")
        transformer = DataTransformer()
        clean_weather = transformer.clean_weather_data(weather_data)
        clean_population = transformer.clean_population_data(population_data)
        print(f"✅ Cleaned {len(clean_weather)} weather records")
        
        # 3. LOAD
        print("\n💾 LOADING TO DATABASE...")
        loader = DataLoader(config)
        
        print("   Creating tables...")
        loader.create_tables()
        print("   ✅ Tables created")
        
        print("   Loading to staging...")
        loader.load_to_staging(clean_weather, 'staging_weather')
        loader.load_to_staging(clean_population, 'staging_population')
        print("   ✅ Data staged")
        
        print("   Loading to warehouse...")
        success = loader.load_to_warehouse()
        if success:
            print("   ✅ Data in warehouse")
        else:
            print("   ❌ Warehouse loading failed")
            return False
        
        # 4. VERIFY
        print("\n🔍 VERIFYING DATA...")
        verify_data()
        
        print("\n🎉 SUCCESS! COMPLETE ETL PIPELINE WORKING!")
        print("💾 SQLite database: data_warehouse.db")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_data():
    """Verify data was loaded correctly"""
    try:
        conn = sqlite3.connect('data_warehouse.db')
        cursor = conn.cursor()
        
        # Check counts
        cursor.execute("SELECT COUNT(*) FROM dim_city")
        city_count = cursor.fetchone()[0]
        print(f"   📍 Cities in warehouse: {city_count}")
        
        cursor.execute("SELECT COUNT(*) FROM dim_date")
        date_count = cursor.fetchone()[0]
        print(f"   📅 Dates in warehouse: {date_count}")
        
        cursor.execute("SELECT COUNT(*) FROM fact_weather")
        weather_count = cursor.fetchone()[0]
        print(f"   🌡️ Weather facts: {weather_count}")
        
        # Show sample data
        cursor.execute("""
            SELECT c.city_name, c.country_code, AVG(f.temperature), c.population
            FROM fact_weather f
            JOIN dim_city c ON f.city_id = c.city_id
            GROUP BY c.city_name, c.country_code, c.population
        """)
        
        print("\n📊 SAMPLE ANALYSIS:")
        for row in cursor.fetchall():
            print(f"   {row[0]}, {row[1]}: {row[2]:.1f}°C avg, Pop: {row[3]:,}")
        
        conn.close()
        
    except Exception as e:
        print(f"   ❌ Verification failed: {e}")

if __name__ == "__main__":
    test_final()