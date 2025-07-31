import duckdb
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from contextlib import contextmanager
import logging

from .models import CrawlRegistry, Article, Company, Topic, ArticleTopic, ArticleCompany, Config

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = "data/crawleb.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        self._init_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = duckdb.connect(str(self.db_path))
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_database(self):
        """Initialize the database and create tables if they don't exist."""
        with duckdb.connect(str(self.db_path)) as conn:
            # Create crawl_registry table
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS crawl_registry_id_seq;
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS crawl_registry (
                    id INTEGER PRIMARY KEY DEFAULT nextval('crawl_registry_id_seq'),
                    url VARCHAR NOT NULL UNIQUE,
                    extract_topics BOOLEAN DEFAULT TRUE,
                    extract_companies BOOLEAN DEFAULT TRUE,
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create articles table
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS articles_id_seq;
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    article_id INTEGER PRIMARY KEY DEFAULT nextval('articles_id_seq'),
                    url VARCHAR NOT NULL UNIQUE,
                    title VARCHAR,
                    author VARCHAR,
                    description TEXT,
                    publication_date TIMESTAMP,
                    crawl_date TIMESTAMP NOT NULL,
                    summary TEXT,
                    content TEXT
                )
            """)
            
            # Create companies table
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS companies_id_seq;
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    company_id INTEGER PRIMARY KEY DEFAULT nextval('companies_id_seq'),
                    name VARCHAR NOT NULL UNIQUE,
                    website_url VARCHAR,
                    summary TEXT,
                    founded_year INTEGER,
                    employee_count VARCHAR,
                    logo_url VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create topics table
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS topics_id_seq;
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS topics (
                    topic_id INTEGER PRIMARY KEY DEFAULT nextval('topics_id_seq'),
                    name VARCHAR NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create article_topics join table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS article_topics (
                    article_id INTEGER,
                    topic_id INTEGER,
                    relevance_score DOUBLE,
                    PRIMARY KEY (article_id, topic_id)
                )
            """)
            
            # Create article_companies join table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS article_companies (
                    article_id INTEGER,
                    company_id INTEGER,
                    relevance_score DOUBLE,
                    PRIMARY KEY (article_id, company_id)
                )
            """)
            
            # Create config table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS config (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR NOT NULL
                )
            """)
    
    # Crawl Registry methods
    def add_crawl_url(self, registry: CrawlRegistry) -> int:
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute("""
                INSERT INTO crawl_registry (url, extract_topics, extract_companies, active)
                VALUES (?, ?, ?, ?)
                RETURNING id
            """, [registry.url, registry.extract_topics, registry.extract_companies, registry.active])
            return result.fetchone()[0]
    
    def get_crawl_registry(self) -> List[CrawlRegistry]:
        with duckdb.connect(str(self.db_path)) as conn:
            results = conn.execute("""
                SELECT id, url, extract_topics, extract_companies, active, created_at
                FROM crawl_registry ORDER BY created_at DESC
            """).fetchall()
            return [CrawlRegistry(
                id=row[0], url=row[1], extract_topics=row[2], 
                extract_companies=row[3], active=row[4], created_at=row[5]
            ) for row in results]
    
    def update_crawl_registry(self, registry: CrawlRegistry) -> bool:
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("""
                UPDATE crawl_registry 
                SET extract_topics = ?, extract_companies = ?, active = ?
                WHERE id = ?
            """, [registry.extract_topics, registry.extract_companies, registry.active, registry.id])
            return True
    
    def delete_crawl_registry(self, registry_id: int) -> bool:
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("DELETE FROM crawl_registry WHERE id = ?", [registry_id])
            return True
    
    # Article methods
    def article_exists(self, url: str) -> bool:
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute("SELECT COUNT(*) FROM articles WHERE url = ?", [url])
            return result.fetchone()[0] > 0
    
    def add_article(self, article: Article) -> int:
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute("""
                INSERT INTO articles (url, title, author, description, publication_date, crawl_date, summary, content)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING article_id
            """, [article.url, article.title, article.author, article.description, 
                  article.publication_date, article.crawl_date, article.summary, article.content])
            return result.fetchone()[0]
    
    def get_articles(self, limit: int = 10, offset: int = 0, topic_id: Optional[int] = None, 
                    company_id: Optional[int] = None) -> List[Dict[str, Any]]:
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                if topic_id:
                    # Get articles associated with a specific topic
                    # First get article IDs for this topic
                    topic_articles = conn.execute("""
                        SELECT article_id FROM article_topics WHERE topic_id = ?
                        ORDER BY article_id DESC
                    """, [topic_id]).fetchall()
                    
                    if not topic_articles:
                        return []
                    
                    article_ids = [row[0] for row in topic_articles]
                    # Create placeholders for the IN clause
                    placeholders = ','.join(['?' for _ in article_ids])
                    
                    query = f"""
                        SELECT article_id, url, title, author, description, 
                               publication_date, crawl_date, summary
                        FROM articles
                        WHERE article_id IN ({placeholders})
                        ORDER BY COALESCE(publication_date, crawl_date) DESC, crawl_date DESC
                        LIMIT ? OFFSET ?
                    """
                    results = conn.execute(query, article_ids + [limit, offset]).fetchall()
                    
                elif company_id:
                    # Get articles associated with a specific company
                    # First get article IDs for this company
                    company_articles = conn.execute("""
                        SELECT article_id FROM article_companies WHERE company_id = ?
                        ORDER BY article_id DESC
                    """, [company_id]).fetchall()
                    
                    if not company_articles:
                        return []
                    
                    article_ids = [row[0] for row in company_articles]
                    # Create placeholders for the IN clause
                    placeholders = ','.join(['?' for _ in article_ids])
                    
                    query = f"""
                        SELECT article_id, url, title, author, description, 
                               publication_date, crawl_date, summary
                        FROM articles
                        WHERE article_id IN ({placeholders})
                        ORDER BY COALESCE(publication_date, crawl_date) DESC, crawl_date DESC
                        LIMIT ? OFFSET ?
                    """
                    results = conn.execute(query, article_ids + [limit, offset]).fetchall()
                    
                else:
                    # Get all articles
                    query = """
                        SELECT article_id, url, title, author, description, 
                               publication_date, crawl_date, summary
                        FROM articles
                        ORDER BY COALESCE(publication_date, crawl_date) DESC, crawl_date DESC
                        LIMIT ? OFFSET ?
                    """
                    results = conn.execute(query, [limit, offset]).fetchall()
                
                articles = []
                for row in results:
                    article = {
                        'article_id': row[0], 'url': row[1], 'title': row[2], 'author': row[3],
                        'description': row[4], 'publication_date': row[5], 'crawl_date': row[6], 'summary': row[7]
                    }
                    # Get associated topics and companies
                    article['topics'] = self.get_article_topics(row[0])
                    article['companies'] = self.get_article_companies(row[0])
                    articles.append(article)
                
                return articles
                
            except Exception as e:
                logger.error(f"Error getting articles: {e}")
                return []
    
    # Company methods
    def get_company_by_name(self, name: str) -> Optional[Company]:
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute("""
                SELECT company_id, name, website_url, summary, founded_year, employee_count, logo_url, created_at
                FROM companies WHERE name = ?
            """, [name]).fetchone()
            
            if result:
                return Company(
                    company_id=result[0], name=result[1], website_url=result[2],
                    summary=result[3], founded_year=result[4], employee_count=result[5],
                    logo_url=result[6], created_at=result[7]
                )
            return None
    
    def add_company(self, company: Company) -> int:
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute("""
                INSERT INTO companies (name, website_url, summary, founded_year, employee_count, logo_url)
                VALUES (?, ?, ?, ?, ?, ?)
                RETURNING company_id
            """, [company.name, company.website_url, company.summary, 
                  company.founded_year, company.employee_count, company.logo_url])
            return result.fetchone()[0]
    
    def get_companies(self) -> List[Dict[str, Any]]:
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                # Simple query first to get all companies
                results = conn.execute("""
                    SELECT company_id, name, website_url, summary, founded_year, 
                           employee_count, logo_url, created_at
                    FROM companies
                    ORDER BY name
                """).fetchall()
                
                companies = []
                for row in results:
                    # Count articles for each company separately
                    try:
                        count_result = conn.execute("""
                            SELECT COUNT(*) FROM article_companies WHERE company_id = ?
                        """, [row[0]]).fetchone()
                        article_count = count_result[0] if count_result else 0
                    except:
                        article_count = 0
                    
                    companies.append({
                        'company_id': row[0], 'name': row[1], 'website_url': row[2], 'summary': row[3],
                        'founded_year': row[4], 'employee_count': row[5], 'logo_url': row[6],
                        'created_at': row[7], 'article_count': article_count
                    })
                
                # Sort by article count descending, then by name
                companies.sort(key=lambda x: (-x['article_count'], x['name']))
                return companies
                
            except Exception as e:
                logger.error(f"Error getting companies: {e}")
                return []
    
    # Topic methods
    def get_topic_by_name(self, name: str) -> Optional[Topic]:
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute("""
                SELECT topic_id, name, created_at FROM topics WHERE name = ?
            """, [name]).fetchone()
            
            if result:
                return Topic(topic_id=result[0], name=result[1], created_at=result[2])
            return None
    
    def add_topic(self, topic: Topic) -> int:
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute("""
                INSERT INTO topics (name) VALUES (?) RETURNING topic_id
            """, [topic.name])
            return result.fetchone()[0]
    
    def get_topics(self) -> List[Dict[str, Any]]:
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                # Simple query first to get all topics
                results = conn.execute("""
                    SELECT topic_id, name, created_at
                    FROM topics
                    ORDER BY name
                """).fetchall()
                
                topics = []
                for row in results:
                    # Count articles for each topic separately
                    try:
                        count_result = conn.execute("""
                            SELECT COUNT(*) FROM article_topics WHERE topic_id = ?
                        """, [row[0]]).fetchone()
                        article_count = count_result[0] if count_result else 0
                    except:
                        article_count = 0
                    
                    topics.append({
                        'topic_id': row[0], 
                        'name': row[1], 
                        'created_at': row[2], 
                        'article_count': article_count
                    })
                
                # Sort by article count descending, then by name
                topics.sort(key=lambda x: (-x['article_count'], x['name']))
                return topics
                
            except Exception as e:
                logger.error(f"Error getting topics: {e}")
                return []
    
    # Association methods
    def link_article_topic(self, article_id: int, topic_id: int, relevance_score: float = 1.0):
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                conn.execute("""
                    INSERT INTO article_topics (article_id, topic_id, relevance_score)
                    VALUES (?, ?, ?)
                """, [article_id, topic_id, relevance_score])
            except Exception:
                # Link already exists, ignore
                pass
    
    def link_article_company(self, article_id: int, company_id: int, relevance_score: float = 1.0):
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                conn.execute("""
                    INSERT INTO article_companies (article_id, company_id, relevance_score)
                    VALUES (?, ?, ?)
                """, [article_id, company_id, relevance_score])
            except Exception:
                # Link already exists, ignore
                pass
    
    def get_article_topics(self, article_id: int) -> List[Dict[str, Any]]:
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                # First get topic IDs and relevance scores for this article
                topic_links = conn.execute("""
                    SELECT topic_id, relevance_score FROM article_topics 
                    WHERE article_id = ?
                    ORDER BY relevance_score DESC
                """, [article_id]).fetchall()
                
                if not topic_links:
                    return []
                
                # Then get topic details for each topic ID
                topics = []
                for topic_id, relevance_score in topic_links:
                    topic_result = conn.execute("""
                        SELECT topic_id, name FROM topics WHERE topic_id = ?
                    """, [topic_id]).fetchone()
                    
                    if topic_result:
                        topics.append({
                            'topic_id': topic_result[0],
                            'name': topic_result[1],
                            'relevance_score': relevance_score
                        })
                
                return topics
                
            except Exception as e:
                logger.error(f"Error getting article topics for article_id {article_id}: {e}")
                return []
    
    def get_article_companies(self, article_id: int) -> List[Dict[str, Any]]:
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                # First get company IDs and relevance scores for this article
                company_links = conn.execute("""
                    SELECT company_id, relevance_score FROM article_companies 
                    WHERE article_id = ?
                    ORDER BY relevance_score DESC
                """, [article_id]).fetchall()
                
                if not company_links:
                    return []
                
                # Then get company details for each company ID
                companies = []
                for company_id, relevance_score in company_links:
                    company_result = conn.execute("""
                        SELECT company_id, name, website_url FROM companies WHERE company_id = ?
                    """, [company_id]).fetchone()
                    
                    if company_result:
                        companies.append({
                            'company_id': company_result[0],
                            'name': company_result[1],
                            'website_url': company_result[2],
                            'relevance_score': relevance_score
                        })
                
                return companies
                
            except Exception as e:
                logger.error(f"Error getting article companies for article_id {article_id}: {e}")
                return []
    
    # Config methods
    def save_config(self, config: Config):
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("DELETE FROM config")  # Clear existing config
            conn.execute("INSERT INTO config (key, value) VALUES (?, ?)", 
                        ['databricks_workspace_url', config.databricks_workspace_url])
            conn.execute("INSERT INTO config (key, value) VALUES (?, ?)", 
                        ['databricks_api_key', config.databricks_api_key])
            conn.execute("INSERT INTO config (key, value) VALUES (?, ?)", 
                        ['llm_endpoint_name', config.llm_endpoint_name])
            conn.execute("INSERT INTO config (key, value) VALUES (?, ?)", 
                        ['max_articles_per_page', str(config.max_articles_per_page)])
    
    def get_config(self) -> Optional[Config]:
        with duckdb.connect(str(self.db_path)) as conn:
            results = conn.execute("SELECT key, value FROM config").fetchall()
            config_dict = {row[0]: row[1] for row in results}
            
            if not config_dict:
                return None
                
            return Config(
                databricks_workspace_url=config_dict.get('databricks_workspace_url', ''),
                databricks_api_key=config_dict.get('databricks_api_key', ''),
                llm_endpoint_name=config_dict.get('llm_endpoint_name', ''),
                max_articles_per_page=int(config_dict.get('max_articles_per_page', '10'))
            )
    
    # Trending analysis methods
    def get_articles_by_date_range(self, days: int) -> List[Dict[str, Any]]:
        """Get articles within the last N days based on publication date."""
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                query = f"""
                    SELECT article_id, url, title, author, description, 
                           publication_date, crawl_date, summary, content
                    FROM articles
                    WHERE publication_date >= CURRENT_DATE - INTERVAL {days} DAY
                    ORDER BY COALESCE(publication_date, crawl_date) DESC
                """
                results = conn.execute(query).fetchall()
                
                articles = []
                for row in results:
                    article = {
                        'article_id': row[0], 'url': row[1], 'title': row[2], 'author': row[3],
                        'description': row[4], 'publication_date': row[5], 'crawl_date': row[6], 
                        'summary': row[7], 'content': row[8]
                    }
                    # Get associated topics and companies
                    article['topics'] = self.get_article_topics(row[0])
                    article['companies'] = self.get_article_companies(row[0])
                    articles.append(article)
                
                return articles
                
            except Exception as e:
                logger.error(f"Error getting articles by date range: {e}")
                return []
    
    def get_trending_topics_by_date_range(self, days: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top topics by article count within the last N days."""
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                # First, get articles in the date range
                article_query = f"""
                    SELECT article_id FROM articles 
                    WHERE publication_date >= CURRENT_DATE - INTERVAL {days} DAY
                """
                article_results = conn.execute(article_query).fetchall()
                
                if not article_results:
                    return []
                
                article_ids = [row[0] for row in article_results]
                placeholders = ','.join(['?' for _ in article_ids])
                
                # Then get topic counts for those articles
                topic_query = f"""
                    SELECT topics.topic_id, topics.name, COUNT(article_topics.article_id) as article_count
                    FROM topics
                    JOIN article_topics ON topics.topic_id = article_topics.topic_id
                    WHERE article_topics.article_id IN ({placeholders})
                    GROUP BY topics.topic_id, topics.name
                    ORDER BY article_count DESC
                    LIMIT {limit}
                """
                results = conn.execute(topic_query, article_ids).fetchall()
                
                return [{
                    'topic_id': row[0],
                    'name': row[1],
                    'article_count': row[2]
                } for row in results]
                
            except Exception as e:
                logger.error(f"Error getting trending topics: {e}")
                return []
    
    def get_trending_companies_by_date_range(self, days: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top companies by article count within the last N days."""
        with duckdb.connect(str(self.db_path)) as conn:
            try:
                # First, get articles in the date range
                article_query = f"""
                    SELECT article_id FROM articles 
                    WHERE publication_date >= CURRENT_DATE - INTERVAL {days} DAY
                """
                article_results = conn.execute(article_query).fetchall()
                
                if not article_results:
                    return []
                
                article_ids = [row[0] for row in article_results]
                placeholders = ','.join(['?' for _ in article_ids])
                
                # Then get company counts for those articles
                company_query = f"""
                    SELECT companies.company_id, companies.name, companies.website_url, COUNT(article_companies.article_id) as article_count
                    FROM companies
                    JOIN article_companies ON companies.company_id = article_companies.company_id
                    WHERE article_companies.article_id IN ({placeholders})
                    GROUP BY companies.company_id, companies.name, companies.website_url
                    ORDER BY article_count DESC
                    LIMIT {limit}
                """
                results = conn.execute(company_query, article_ids).fetchall()
                
                return [{
                    'company_id': row[0],
                    'name': row[1],
                    'website_url': row[2],
                    'article_count': row[3]
                } for row in results]
                
            except Exception as e:
                logger.error(f"Error getting trending companies: {e}")
                return []