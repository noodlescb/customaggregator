import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor

from ..database.database import Database
from ..database.models import Article, Company, Topic
from ..llm.databricks_client import DatabricksLLMClient
from .extractor import ContentExtractor

logger = logging.getLogger(__name__)


class WebCrawler:
    def __init__(self, database: Database, llm_client: DatabricksLLMClient):
        self.db = database
        self.llm_client = llm_client
        self.extractor = ContentExtractor()
        self.executor = ThreadPoolExecutor(max_workers=4)  # For concurrent processing
    
    async def run_crawl(self) -> dict:
        """
        Main crawl method that processes all URLs in the crawl registry.
        Returns a summary of the crawl results.
        """
        results = {
            'total_urls': 0,
            'new_articles': 0,
            'existing_articles': 0,
            'failed_extractions': 0,
            'topics_added': 0,
            'companies_added': 0,
            'errors': []
        }
        
        try:
            # Get all active URLs from crawl registry
            registry_entries = self.db.get_crawl_registry()
            active_entries = [entry for entry in registry_entries if entry.active]
            
            results['total_urls'] = len(active_entries)
            logger.info(f"Starting crawl of {len(active_entries)} URLs")
            
            for registry_entry in active_entries:
                try:
                    await self._process_registry_entry(registry_entry, results)
                except Exception as e:
                    error_msg = f"Error processing {registry_entry.url}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            logger.info(f"Crawl completed: {results}")
            return results
        
        except Exception as e:
            logger.error(f"Critical error in crawl process: {e}")
            results['errors'].append(f"Critical error: {str(e)}")
            return results
    
    async def _process_registry_entry(self, registry_entry, results: dict):
        """Process a single entry from the crawl registry."""
        logger.info(f"Processing registry entry: {registry_entry.url}")
        
        # Extract articles from the page (could be multiple if it's a news homepage)
        try:
            article_urls = self.extractor.extract_articles_from_page(registry_entry.url)
        except Exception as e:
            logger.error(f"Error extracting article URLs from {registry_entry.url}: {e}")
            article_urls = [registry_entry.url]  # Use original URL as fallback
        
        for article_url in article_urls:
            try:
                # Check if article already exists
                if self.db.article_exists(article_url):
                    logger.info(f"Article already exists: {article_url}")
                    results['existing_articles'] += 1
                    continue
                
                # Extract article content
                try:
                    article_data = self.extractor.extract_article_content(article_url)
                except Exception as e:
                    logger.error(f"Error extracting content from {article_url}: {e}")
                    results['failed_extractions'] += 1
                    continue
                
                # Validate extracted content
                if not self.extractor.is_valid_article(article_data):
                    logger.warning(f"Invalid article data for {article_url}")
                    results['failed_extractions'] += 1
                    continue
                
                # Create Article object and save to database
                article = Article(
                    url=article_data['url'],
                    title=article_data['title'],
                    author=article_data['author'],
                    description=article_data['description'],
                    publication_date=article_data['publication_date'],
                    crawl_date=datetime.now(timezone.utc),
                    content=article_data['content']
                )
                
                # Generate summary using LLM
                if article_data['content']:
                    summary = await self.llm_client.summarize_article(
                        article_data['content'], 
                        article_data['title'] or ""
                    )
                    article.summary = summary if summary else "Summary generation failed"
                
                # Save article to database
                article_id = self.db.add_article(article)
                results['new_articles'] += 1
                logger.info(f"Added new article: {article_id} - {article.title}")
                
                # Process topics if requested
                if registry_entry.extract_topics and article_data['content']:
                    await self._process_topics(article_id, article_data, results)
                
                # Process companies if requested
                if registry_entry.extract_companies and article_data['content']:
                    await self._process_companies(article_id, article_data, results)
                
            except Exception as e:
                error_msg = f"Error processing article {article_url}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['failed_extractions'] += 1
    
    async def _process_topics(self, article_id: int, article_data: dict, results: dict):
        """Extract and process topics for an article."""
        try:
            topics = await self.llm_client.extract_topics(
                article_data['content'], 
                article_data['title'] or ""
            )
            
            for topic_name in topics:
                if not topic_name.strip():
                    continue
                
                # Check if topic exists, create if not
                topic = self.db.get_topic_by_name(topic_name)
                if not topic:
                    topic_obj = Topic(name=topic_name)
                    topic_id = self.db.add_topic(topic_obj)
                    results['topics_added'] += 1
                    logger.info(f"Added new topic: {topic_name}")
                else:
                    topic_id = topic.topic_id
                
                # Link article to topic
                self.db.link_article_topic(article_id, topic_id, 1.0)
        
        except Exception as e:
            logger.error(f"Error processing topics for article {article_id}: {e}")
    
    async def _process_companies(self, article_id: int, article_data: dict, results: dict):
        """Extract and process companies for an article."""
        try:
            companies = await self.llm_client.extract_companies(
                article_data['content'], 
                article_data['title'] or ""
            )
            
            for company_name in companies:
                if not company_name.strip():
                    continue
                
                # Check if company exists, create if not
                company = self.db.get_company_by_name(company_name)
                if not company:
                    # Research company information using LLM
                    company_info = await self.llm_client.research_company(company_name)
                    
                    company_obj = Company(
                        name=company_name,
                        website_url=company_info.get('website_url'),
                        summary=company_info.get('summary'),
                        founded_year=company_info.get('founded_year'),
                        employee_count=company_info.get('employee_count', 'Unknown')
                    )
                    
                    company_id = self.db.add_company(company_obj)
                    results['companies_added'] += 1
                    logger.info(f"Added new company: {company_name}")
                else:
                    company_id = company.company_id
                
                # Link article to company
                self.db.link_article_company(article_id, company_id, 1.0)
        
        except Exception as e:
            logger.error(f"Error processing companies for article {article_id}: {e}")
    
    async def crawl_single_url(self, url: str, extract_topics: bool = True, 
                              extract_companies: bool = True) -> dict:
        """
        Crawl a single URL immediately (for testing or one-off crawls).
        """
        results = {
            'url': url,
            'success': False,
            'article_id': None,
            'topics': [],
            'companies': [],
            'error': None
        }
        
        try:
            # Check if article already exists
            if self.db.article_exists(url):
                results['error'] = "Article already exists"
                return results
            
            # Extract article content
            try:
                article_data = self.extractor.extract_article_content(url)
            except Exception as e:
                logger.error(f"Error extracting content from {url}: {e}")
                results['error'] = f"Content extraction failed: {str(e)}"
                return results
            
            # Validate extracted content
            if not self.extractor.is_valid_article(article_data):
                results['error'] = "Invalid or insufficient article content"
                return results
            
            # Create Article object
            article = Article(
                url=article_data['url'],
                title=article_data['title'],
                author=article_data['author'],
                description=article_data['description'],
                publication_date=article_data['publication_date'],
                crawl_date=datetime.now(timezone.utc),
                content=article_data['content']
            )
            
            # Generate summary
            if article_data['content']:
                summary = await self.llm_client.summarize_article(
                    article_data['content'], 
                    article_data['title'] or ""
                )
                article.summary = summary if summary else "Summary generation failed"
            
            # Save article
            article_id = self.db.add_article(article)
            results['article_id'] = article_id
            results['success'] = True
            
            # Process topics
            if extract_topics and article_data['content']:
                topics = await self.llm_client.extract_topics(
                    article_data['content'], 
                    article_data['title'] or ""
                )
                
                for topic_name in topics:
                    if not topic_name.strip():
                        continue
                    
                    topic = self.db.get_topic_by_name(topic_name)
                    if not topic:
                        topic_obj = Topic(name=topic_name)
                        topic_id = self.db.add_topic(topic_obj)
                    else:
                        topic_id = topic.topic_id
                    
                    self.db.link_article_topic(article_id, topic_id, 1.0)
                    results['topics'].append(topic_name)
            
            # Process companies
            if extract_companies and article_data['content']:
                companies = await self.llm_client.extract_companies(
                    article_data['content'], 
                    article_data['title'] or ""
                )
                
                for company_name in companies:
                    if not company_name.strip():
                        continue
                    
                    company = self.db.get_company_by_name(company_name)
                    if not company:
                        company_info = await self.llm_client.research_company(company_name)
                        
                        company_obj = Company(
                            name=company_name,
                            website_url=company_info.get('website_url'),
                            summary=company_info.get('summary'),
                            founded_year=company_info.get('founded_year'),
                            employee_count=company_info.get('employee_count', 'Unknown')
                        )
                        
                        company_id = self.db.add_company(company_obj)
                    else:
                        company_id = company.company_id
                    
                    self.db.link_article_company(article_id, company_id, 1.0)
                    results['companies'].append(company_name)
            
            return results
            
        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            results['error'] = str(e)
            return results