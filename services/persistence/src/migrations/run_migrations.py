"""
Script to run database migrations.
This script should be executed when the service starts or manually for schema updates.
"""
import os
import logging
from dotenv import load_dotenv
from initial_schema import run_migration

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    """Run all migrations in the correct order."""
    # Load environment variables
    load_dotenv()
    
    # Get database URL from environment variables
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    logger.info("Starting database migrations...")
    
    # Run initial schema migration
    run_migration(db_url)
    
    # Future migrations would be called here in sequence
    logger.info("All migrations completed successfully")

if __name__ == "__main__":
    main()