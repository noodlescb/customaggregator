#!/usr/bin/env python3
"""
Migration script to add theme-related tables to existing database.
This will add the themes and article_themes tables without affecting existing data.
"""

import sys
from pathlib import Path
import duckdb

# Add the src directory to the path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

def migrate_themes():
    """Add themes and article_themes tables to existing database."""
    db_path = "data/crawleb.db"
    
    print("🔄 Adding theme-related tables to database...")
    
    try:
        with duckdb.connect(db_path) as conn:
            # Check if trending_reports table exists and create if needed
            print("  - Ensuring trending_reports table exists...")
            try:
                conn.execute("""
                    CREATE SEQUENCE IF NOT EXISTS trending_reports_id_seq;
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS trending_reports (
                        report_id INTEGER PRIMARY KEY DEFAULT nextval('trending_reports_id_seq'),
                        days INTEGER NOT NULL,
                        generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        article_count INTEGER,
                        results_json TEXT
                    )
                """)
            except Exception as e:
                print(f"    Warning: {e}")
            
            # Create themes table
            print("  - Creating themes table...")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS themes (
                    theme_id INTEGER PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    explanation TEXT,
                    insights TEXT,
                    report_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (report_id) REFERENCES trending_reports(report_id)
                )
            """)
            
            # Create article_themes join table
            print("  - Creating article_themes table...")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS article_themes (
                    article_id INTEGER,
                    theme_id INTEGER,
                    relevance_score DOUBLE,
                    PRIMARY KEY (article_id, theme_id),
                    FOREIGN KEY (article_id) REFERENCES articles(article_id),
                    FOREIGN KEY (theme_id) REFERENCES themes(theme_id)
                )
            """)
            
            print("✅ Successfully added theme tables!")
            print("📋 All existing data preserved.")
            
    except Exception as e:
        print(f"❌ Error adding theme tables: {e}")
        return False
    
    return True

if __name__ == "__main__":
    migrate_themes()