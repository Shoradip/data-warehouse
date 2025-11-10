import logging
import yaml
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Remove emojis from log format for Windows compatibility
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/etl_pipeline.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config():
    """Load configuration from YAML file"""
    try:
        with open('config/config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        # Replace with environment variables for security
        if 'DB_PASSWORD' in os.environ:
            config['database']['password'] = os.getenv('DB_PASSWORD')
        if 'WEATHER_API_KEY' in os.environ:
            config['api']['weather_api_key'] = os.getenv('WEATHER_API_KEY')
        
        logger = setup_logging()
        logger.info("✅ Configuration loaded successfully")
        return config
        
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return None

def get_db_connection(config):
    """Create database connection - uses SQLite"""
    try:
        db_config = config['database']
        
        # Always use SQLite for now
        sqlite_db = db_config['database']
        connection_string = f"sqlite:///{sqlite_db}"
        print(f"🔗 Using SQLite database: {sqlite_db}")
        engine = create_engine(connection_string)
        return engine
            
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        # Ultimate fallback
        print("🔄 Using default SQLite...")
        connection_string = "sqlite:///data_warehouse.db"
        engine = create_engine(connection_string)
        return engine

def test_db_connection(config):
    """Test database connection"""
    try:
        engine = get_db_connection(config)
        if engine:
            with engine.connect() as conn:
                result = conn.execute("SELECT 1")
                print("✅ Database connection successful!")
                return True
        return False
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False