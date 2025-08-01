#!/usr/bin/env python3
"""
Clean up failed article entries from the database.
Removes articles with extraction failure indicators.
"""

import sys
from pathlib import Path
import duckdb

# Add the src directory to the path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from crawleb.database.database import Database

def cleanup_failed_articles():
    """Remove articles with failed extraction indicators."""
    db = Database()
    
    print("🧹 Cleaning up failed article entries...")
    
    with duckdb.connect(str(db.db_path)) as conn:
        # Count total articles before cleanup
        total_before = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        print(f"Total articles before cleanup: {total_before}")
        
        # Define failure indicators to search for
        failed_indicators = [
            'Failed to extract title',
            'Content extraction failed',
            'No title extracted', 
            'No content extracted',
            'extraction failed',
            'Error:'
        ]
        
        articles_to_delete = set()
        
        # Find all articles with failure indicators
        for indicator in failed_indicators:
            # Find by title
            title_results = conn.execute(
                'SELECT article_id FROM articles WHERE title LIKE ?', 
                [f'%{indicator}%']
            ).fetchall()
            
            for (article_id,) in title_results:
                articles_to_delete.add(article_id)
            
            # Find by content
            content_results = conn.execute(
                'SELECT article_id FROM articles WHERE content LIKE ?', 
                [f'%{indicator}%']
            ).fetchall()
            
            for (article_id,) in content_results:
                articles_to_delete.add(article_id)
        
        print(f"Found {len(articles_to_delete)} articles to delete")
        
        if articles_to_delete:
            # Delete from association tables first (to maintain referential integrity)
            print("  - Removing article-topic associations...")
            for article_id in articles_to_delete:
                conn.execute("DELETE FROM article_topics WHERE article_id = ?", [article_id])
            
            print("  - Removing article-company associations...")  
            for article_id in articles_to_delete:
                conn.execute("DELETE FROM article_companies WHERE article_id = ?", [article_id])
            
            print("  - Removing article-theme associations...")
            for article_id in articles_to_delete:
                try:
                    conn.execute("DELETE FROM article_themes WHERE article_id = ?", [article_id])
                except:
                    # Table might not exist in older databases
                    pass
            
            # Delete the articles themselves
            print("  - Removing failed articles...")
            for article_id in articles_to_delete:
                conn.execute("DELETE FROM articles WHERE article_id = ?", [article_id])
            
            # Count articles after cleanup
            total_after = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
            deleted_count = total_before - total_after
            
            print(f"✅ Cleanup completed!")
            print(f"   Articles removed: {deleted_count}")
            print(f"   Articles remaining: {total_after}")
            
            # Show some stats about remaining articles
            print(f"\n📊 Remaining articles summary:")
            
            # Articles with topics
            with_topics = conn.execute("""
                SELECT COUNT(DISTINCT a.article_id) 
                FROM articles a 
                JOIN article_topics at ON a.article_id = at.article_id
            """).fetchone()[0]
            
            # Articles with companies  
            with_companies = conn.execute("""
                SELECT COUNT(DISTINCT a.article_id) 
                FROM articles a 
                JOIN article_companies ac ON a.article_id = ac.article_id
            """).fetchone()[0]
            
            print(f"   Articles with topics: {with_topics}")
            print(f"   Articles with companies: {with_companies}")
            
        else:
            print("✅ No failed articles found to clean up!")

if __name__ == "__main__":
    cleanup_failed_articles()