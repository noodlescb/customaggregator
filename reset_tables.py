#!/usr/bin/env python3
"""
Reset articles, topics, companies and their association tables.
Keeps crawl_registry and config intact.
"""

import sys
from pathlib import Path
import duckdb

# Add the src directory to the path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

def reset_tables():
    """Reset articles, topics, companies and association tables."""
    db_path = "data/crawleb.db"
    
    print("🔄 Resetting articles, topics, and companies tables...")
    
    try:
        with duckdb.connect(db_path) as conn:
            # Delete data from association tables first (due to relationships)
            print("  - Clearing article_topics...")
            conn.execute("DELETE FROM article_topics")
            
            print("  - Clearing article_companies...")
            conn.execute("DELETE FROM article_companies")
            
            # Delete data from main tables
            print("  - Clearing articles...")
            conn.execute("DELETE FROM articles")
            
            print("  - Clearing companies...")
            conn.execute("DELETE FROM companies")
            
            print("  - Clearing topics...")
            conn.execute("DELETE FROM topics")
            
            # Reset sequences to start from 1 again
            print("  - Resetting sequences...")
            try:
                conn.execute("ALTER SEQUENCE articles_id_seq RESTART WITH 1")
                conn.execute("ALTER SEQUENCE companies_id_seq RESTART WITH 1") 
                conn.execute("ALTER SEQUENCE topics_id_seq RESTART WITH 1")
            except:
                # If sequences don't exist, that's okay
                pass
            
            print("✅ Successfully reset all tables!")
            print("📋 Crawl registry and configuration preserved.")
            
    except Exception as e:
        print(f"❌ Error resetting tables: {e}")
        return False
    
    return True

if __name__ == "__main__":
    reset_tables()