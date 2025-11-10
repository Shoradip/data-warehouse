import os
import requests
from dotenv import load_dotenv

load_dotenv()

def test_weather_api_simple():
    print("🌤️ Testing Weather API (Simple Version)...")
    print("=" * 50)
    
    api_key = os.getenv('WEATHER_API_KEY')
    
    if not api_key or api_key == 'your_api_key_here':
        print("❌ API Key not set in .env file")
        return False
    
    try:
        # Test API with London
        params = {
            'q': 'London,GB',
            'appid': api_key,
            'units': 'metric'
        }
        
        print("🌐 Making API request...")
        response = requests.get(
            'https://api.openweathermap.org/data/2.5/weather',
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ API Test Successful!")
            print(f"📍 City: {data['name']}")
            print(f"🌡️ Temperature: {data['main']['temp']}°C")
            print(f"💧 Humidity: {data['main']['humidity']}%")
            print(f"🌤️ Condition: {data['weather'][0]['description']}")
            return True
        else:
            print(f"❌ API Test Failed. Status Code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ API Test Error: {e}")
        return False

def test_imports():
    print("📦 Testing Imports...")
    print("=" * 50)
    
    try:
        import pandas as pd
        import sqlalchemy
        import yaml
        from src.utils import load_config
        
        print("✅ All imports successful!")
        
        # Test config loading
        config = load_config()
        if config:
            print("✅ Config loaded successfully")
            print(f"🗄️ Database: {config['database']['database']}")
            return True
        else:
            print("❌ Config loading failed")
            return False
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Simple Data Warehouse Test")
    print("=" * 50)
    
    imports_ok = test_imports()
    print()
    api_ok = test_weather_api_simple()
    
    print("\n" + "=" * 50)
    if imports_ok and api_ok:
        print("🎉 SUCCESS: Everything is working!")
        print("Next: We'll create the database tables")
    elif api_ok:
        print("✅ Weather API is working!")
        print("⚠️ Some imports/config issues to fix")
    else:
        print("❌ Some tests failed")