import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

load_dotenv()

def test_environment():
    print("🔐 Testing Environment Variables...")
    print("=" * 40)
    
    api_key = os.getenv('WEATHER_API_KEY')
    db_password = os.getenv('DB_PASSWORD')
    
    print(f"WEATHER_API_KEY: {'✅ Set' if api_key and api_key != 'your_api_key_here' else '❌ Not set properly'}")
    print(f"DB_PASSWORD: {'✅ Set' if db_password and db_password != 'your_postgres_password_here' else '❌ Not set properly'}")
    
    if api_key and api_key != 'your_api_key_here':
        print("🎉 Weather API is ready to use!")
    
    return api_key and api_key != 'your_api_key_here'

def test_weather_api():
    print("\n🌤️ Testing Weather API...")
    print("=" * 40)
    
    try:
        import requests
        from utils import load_config
        
        config = load_config()
        api_key = os.getenv('WEATHER_API_KEY')
        
        if api_key and api_key != 'your_api_key_here':
            # Test API with London
            params = {
                'q': 'London,GB',
                'appid': api_key,
                'units': 'metric'
            }
            
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
                return True
            else:
                print(f"❌ API Test Failed: {response.status_code}")
                return False
        else:
            print("❌ API Key not properly set")
            return False
            
    except Exception as e:
        print(f"❌ API Test Error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Testing Data Warehouse Setup...")
    print("=" * 50)
    
    env_ok = test_environment()
    
    if env_ok:
        api_ok = test_weather_api()
        
        print("\n" + "=" * 50)
        if api_ok:
            print("🎉 SUCCESS: Weather API is working!")
            print("💡 Next: We'll set up the database")
        else:
            print("❌ Weather API test failed")
    else:
        print("❌ Environment variables not set properly")