import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def quick_import_test():
    print("🔍 Quick Import Test")
    print("=" * 40)
    
    try:
        from src.utils import load_config
        print("✅ utils.py imports work")
        
        config = load_config()
        if config:
            print("✅ Config loaded successfully")
        else:
            print("❌ Config failed to load")
            return False
        
        from src.extract import DataExtractor
        print("✅ DataExtractor import works")
        
        from src.transform import DataTransformer
        print("✅ DataTransformer import works")
        
        from src.load import DataLoader
        print("✅ DataLoader import works")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Other error: {e}")
        return False

if __name__ == "__main__":
    if quick_import_test():
        print("\n🎉 All imports working! Ready to run ETL pipeline.")
    else:
        print("\n❌ There are import issues to fix.")