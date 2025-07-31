#!/usr/bin/env python3
"""
Database migration script to update schema if needed.
"""

import sys
from pathlib import Path
import duckdb

# Add the src directory to the path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from crawleb.database.database import Database

def migrate_database():
    """Migrate database to new schema with sequences."""
    db_path = "data/crawleb.db"
    
    print("Starting database migration...")
    
    try:
        with duckdb.connect(db_path) as conn:
            # Check if sequences already exist
            result = conn.execute("SELECT sequence_name FROM duckdb_sequences()").fetchall()
            sequence_names = [row[0] for row in result]
            
            if 'crawl_registry_id_seq' not in sequence_names:
                print("Creating sequences...")
                
                # Create sequences
                conn.execute("CREATE SEQUENCE crawl_registry_id_seq;")
                conn.execute("CREATE SEQUENCE articles_id_seq;")
                conn.execute("CREATE SEQUENCE companies_id_seq;")
                conn.execute("CREATE SEQUENCE topics_id_seq;")
                
                # Update table defaults
                try:
                    conn.execute("ALTER TABLE crawl_registry ALTER COLUMN id SET DEFAULT nextval('crawl_registry_id_seq');")
                    conn.execute("ALTER TABLE articles ALTER COLUMN article_id SET DEFAULT nextval('articles_id_seq');")
                    conn.execute("ALTER TABLE companies ALTER COLUMN company_id SET DEFAULT nextval('companies_id_seq');")
                    conn.execute("ALTER TABLE topics ALTER COLUMN topic_id SET DEFAULT nextval('topics_id_seq');")
                    
                    print("Migration completed successfully!")
                except Exception as e:
                    print(f"Warning: Could not update table defaults: {e}")
                    print("Consider deleting the database file to recreate with new schema.")
            else:
                print("Sequences already exist, no migration needed.")
                
    except Exception as e:
        print(f"Migration failed: {e}")
        print("You may need to delete the database file and restart the application.")

if __name__ == "__main__":
    migrate_database()