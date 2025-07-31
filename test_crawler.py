#!/usr/bin/env python3
import sys
import asyncio
import logging
from pathlib import Path

# Setup
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

from crawleb.database.database import Database
from crawleb.llm.databricks_client import DatabricksLLMClient
from crawleb.crawler.crawler import WebCrawler

async def test_crawler():
    try:
        # Initialize database
        db = Database()
        print("✓ Database initialized")
        
        # Get configuration - you'll need to set this up first via the web interface
        config = db.get_config()
        if not config:
            print("❌ No configuration found. Please set up Databricks config first via web interface.")
            return
        
        print("✓ Configuration loaded")
        
        # Initialize LLM client
        llm_client = DatabricksLLMClient(
            workspace_url=config.databricks_workspace_url,
            api_key=config.databricks_api_key,
            endpoint_name=config.llm_endpoint_name
        )
        
        print("✓ LLM client created")
        
        # Test single URL crawl
        crawler = WebCrawler(db, llm_client)
        test_url = "https://www.artificialintelligence-news.com/resources/governing-generative-ai-securely-and-safely-across-emea/"
        
        print(f"🔍 Testing URL: {test_url}")
        
        result = await crawler.crawl_single_url(test_url, extract_topics=True, extract_companies=True)
        
        print("📊 Results:")
        print(f"  Success: {result['success']}")
        print(f"  Article ID: {result.get('article_id')}")
        print(f"  Topics: {result.get('topics', [])}")
        print(f"  Companies: {result.get('companies', [])}")
        if result.get('error'):
            print(f"  Error: {result['error']}")
            
    except Exception as e:
        import traceback
        print(f"❌ Error: {e}")
        print(f"Full traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_crawler())