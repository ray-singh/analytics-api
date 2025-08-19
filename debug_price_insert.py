import asyncio
import json
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics")
# Test function to ensure timestamp conversion works
async def test_timestamp_conversion():
    print("Testing timestamp conversion...")
    
    # Import the actual function from the repository
    sys.path.append('/app')
    from services.persistence.src.repositories.price_repository import PriceRepository
    
    # Create a test repository
    repo = PriceRepository()
    
    # Test with various timestamp formats
    test_cases = [
        "2025-08-18T23:43:05",
        "2025-08-18T23:43:05Z", 
        "2025-08-18T23:43:05.000Z",
        "2025-08-18 23:43:05"
    ]
    
    for ts in test_cases:
        try:
            # Try to parse timestamp
            if isinstance(ts, str):
                try:
                    parsed = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    print(f"SUCCESS: Converted '{ts}' to {parsed}")
                except ValueError:
                    # Try parsing with datetime.strptime if fromisoformat fails
                    try:
                        parsed = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
                        print(f"SUCCESS: Converted '{ts}' to {parsed} using strptime")
                    except ValueError:
                        try:
                            # Add more formats as needed
                            parsed = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                            print(f"SUCCESS: Converted '{ts}' to {parsed} using alternate strptime")
                        except ValueError:
                            print(f"FAILED: Could not convert '{ts}' to datetime")
        except Exception as e:
            print(f"ERROR: Exception when testing '{ts}': {e}")

if __name__ == "__main__":
    import sys
    asyncio.run(test_timestamp_conversion())