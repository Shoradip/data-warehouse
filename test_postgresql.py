import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

load_dotenv()

def test_postgresql_connection():
    print("🗄️ Testing PostgreSQL Connection...")
    print("=" * 50)
    
    from utils import load_config, get_db_connection
    
    # Check if password is set
    db_password = os.getenv('DB_PASSWORD')
    if not db_password or db_password == 'your_postgres_password_here':
        print("❌ DB_PASSWORD not set in .env file")
        print("💡 Please update your .env file with your PostgreSQL password")
        return False
    
    config = load_config()
    if not config:
        print("❌ Failed to load configuration")
        return False
    
    try:
        engine = get_db_connection(config)
        
        # Test connection
        with engine.connect() as conn:
            # Test basic connection
            result = conn.execute("SELECT version();")
            version = result.fetchone()
            print(f"✅ PostgreSQL Connected!")
            print(f"📊 Version: {version[0]}")
            
            # Check if database exists
            result = conn.execute("SELECT datname FROM pg_database WHERE datname = 'data_warehouse';")
            db_exists = result.fetchone()
            
            if db_exists:
                print("✅ Database 'data_warehouse' exists")
            else:
                print("❌ Database 'data_warehouse' does not exist")
                print("💡 Let's create it...")
                return False
                
            return True
            
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        print("\n🔧 Troubleshooting steps:")
        print("1. Make sure PostgreSQL is running")
        print("2. Check your password in .env file")
        print("3. Verify database 'data_warehouse' exists")
        print("4. Check if PostgreSQL service is started")
        return False

def check_postgresql_service():
    print("\n🔍 Checking PostgreSQL Service...")
    print("=" * 30)
    
    try:
        import subprocess
        # Check if PostgreSQL service is running (Windows)
        result = subprocess.run(['sc', 'query', 'postgresql'], 
                              capture_output=True, text=True, timeout=10)
        
        if 'RUNNING' in result.stdout:
            print("✅ PostgreSQL service is running")
            return True
        else:
            print("❌ PostgreSQL service is not running")
            print("💡 Start it from: Services → PostgreSQL")
            return False
            
    except Exception as e:
        print(f"⚠️ Could not check service status: {e}")
        return True  # Continue anyway

if __name__ == "__main__":
    print("🚀 PostgreSQL Setup Test")
    print("=" * 50)
    
    service_ok = check_postgresql_service()
    if service_ok:
        connection_ok = test_postgresql_connection()
        
        if connection_ok:
            print("\n🎉 PostgreSQL is ready!")
        else:
            print("\n❌ PostgreSQL setup needs attention")
    else:
        print("\n❌ Please start PostgreSQL service first")