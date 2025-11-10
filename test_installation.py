try:
    import requests
    import pandas as pd
    import sqlalchemy
    import yaml
    from dotenv import load_dotenv
    import schedule
    print("✅ All packages imported successfully!")
    
    # Test specific versions
    print(f"✅ Pandas version: {pd.__version__}")
    print(f"✅ Requests version: {requests.__version__}")
    
except ImportError as e:
    print(f"❌ Import error: {e}")