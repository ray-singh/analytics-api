"""Environment check script to validate the setup"""

import os
import sys
import psycopg2
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    """Main function to run environment checks"""
    print("🔍 Running environment checks for analytics-api")
    print("==================================================")
    
    # Track if any checks fail
    has_failures = False
    
    # Check if we're running as part of the build process
    is_build_check = os.environ.get("BUILD_CHECK", "false").lower() == "true"
    
    # Check required environment variables
    print("Checking environment variables...")
    required_vars = ["DATABASE_URL", "KAFKA_BOOTSTRAP_SERVERS"]
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"  - {var}")
        print("")
        has_failures = True
    else:
        print("✅ All required environment variables are set")
        print("")
    
    # Skip connection checks during build
    if is_build_check:
        print("ℹ️ Skipping connection checks during build process")
        return 0
    
    # Check database connection
    print("Checking database connection...")
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        try:
            # Parse connection parameters from URL
            conn_parts = db_url.replace("postgresql://", "").split("@")
            auth = conn_parts[0].split(":")
            host_parts = conn_parts[1].split("/")
            host_port = host_parts[0].split(":")
            
            conn_params = {
                "user": auth[0],
                "password": auth[1] if len(auth) > 1 else "",
                "host": host_port[0],
                "port": host_port[1] if len(host_port) > 1 else 5432,
                "dbname": host_parts[1] if len(host_parts) > 1 else "postgres"
            }
            
            # Try to connect
            conn = psycopg2.connect(**conn_params)
            conn.close()
            print("✅ Successfully connected to database")
            print("")
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            print("")
            has_failures = True
    
    # Check Kafka connection
    print("Checking Kafka connection...")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_bootstrap:
        try:
            # Create admin client to test connection
            admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap})
            
            # List topics to test connection
            topics = admin_client.list_topics(timeout=5)
            print("✅ Successfully connected to Kafka")
            print(f"  Available topics: {len(topics.topics)}")
            print("")
        except Exception as e:
            print(f"❌ Cannot connect to Kafka at {kafka_bootstrap}")
            print("")
            has_failures = True
    
    print("==================================================")
    if has_failures:
        print("⚠️ Some checks failed. Please fix the issues above.")
        return 1
    else:
        print("✅ All checks passed! Environment is ready.")
        return 0

if __name__ == "__main__":
    sys.exit(main())