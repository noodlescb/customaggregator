#!/usr/bin/env python3
"""
CLI tool for running News-dles crawler independently.
Can be used for cron jobs or manual crawling.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add the src directory to the path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from crawleb.database.database import Database
from crawleb.llm.databricks_client import DatabricksLLMClient
from crawleb.crawler.crawler import WebCrawler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawleb_crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


async def run_crawler():
    """Run the crawler using configuration from the database."""
    try:
        # Initialize database
        db = Database()
        logger.info("Database initialized")
        
        # Get configuration
        config = db.get_config()
        if not config:
            logger.error("No configuration found. Please configure the application through the web interface.")
            return False
        
        logger.info("Configuration loaded")
        
        # Initialize LLM client
        llm_client = DatabricksLLMClient(
            workspace_url=config.databricks_workspace_url,
            api_key=config.databricks_api_key,
            endpoint_name=config.llm_endpoint_name
        )
        
        # Test connection
        logger.info("Testing LLM connection...")
        connection_ok = await llm_client.test_connection()
        if not connection_ok:
            logger.error("Failed to connect to Databricks LLM endpoint")
            return False
        
        logger.info("LLM connection successful")
        
        # Initialize crawler
        crawler = WebCrawler(db, llm_client)
        
        # Run crawl
        logger.info("Starting crawl...")
        results = await crawler.run_crawl()
        
        # Log results
        logger.info(f"Crawl completed with results: {results}")
        
        if results.get('errors'):
            logger.warning(f"Crawl completed with {len(results['errors'])} errors:")
            for error in results['errors']:
                logger.warning(f"  - {error}")
        
        return True
        
    except Exception as e:
        logger.error(f"Critical error in crawler: {e}")
        return False


async def crawl_single_url(url: str, extract_topics: bool = True, extract_companies: bool = True):
    """Crawl a single URL."""
    try:
        # Initialize database
        db = Database()
        logger.info("Database initialized")
        
        # Get configuration
        config = db.get_config()
        if not config:
            logger.error("No configuration found. Please configure the application through the web interface.")
            return False
        
        # Initialize LLM client
        llm_client = DatabricksLLMClient(
            workspace_url=config.databricks_workspace_url,
            api_key=config.databricks_api_key,
            endpoint_name=config.llm_endpoint_name
        )
        
        # Test connection
        logger.info("Testing LLM connection...")
        connection_ok = await llm_client.test_connection()
        if not connection_ok:
            logger.error("Failed to connect to Databricks LLM endpoint")
            return False
        
        # Initialize crawler
        crawler = WebCrawler(db, llm_client)
        
        # Crawl single URL
        logger.info(f"Crawling single URL: {url}")
        result = await crawler.crawl_single_url(url, extract_topics, extract_companies)
        
        logger.info(f"Single URL crawl result: {result}")
        
        return result.get('success', False)
        
    except Exception as e:
        logger.error(f"Error crawling single URL: {e}")
        return False


def main():
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python crawler_cli.py run                    # Run full crawl")
        print("  python crawler_cli.py single <url>          # Crawl single URL")
        print("  python crawler_cli.py single <url> --no-topics --no-companies  # Crawl single URL without extraction")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "run":
        # Run full crawl
        success = asyncio.run(run_crawler())
        sys.exit(0 if success else 1)
        
    elif command == "single":
        if len(sys.argv) < 3:
            print("Error: URL required for single crawl")
            sys.exit(1)
        
        url = sys.argv[2]
        extract_topics = "--no-topics" not in sys.argv
        extract_companies = "--no-companies" not in sys.argv
        
        success = asyncio.run(crawl_single_url(url, extract_topics, extract_companies))
        sys.exit(0 if success else 1)
        
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()