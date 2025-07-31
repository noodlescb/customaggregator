import logging
import requests
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin, urlparse
import trafilatura
from newspaper import Article as NewspaperArticle
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)


class ContentExtractor:
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def extract_articles_from_page(self, url: str) -> List[str]:
        """
        Extract article URLs from a page (e.g., news homepage, blog index).
        Returns a list of article URLs found on the page.
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            article_urls = set()
            
            # Look for common article link patterns
            selectors = [
                'a[href*="/article/"]',
                'a[href*="/news/"]',
                'a[href*="/blog/"]',
                'a[href*="/post/"]',
                'article a',
                '.article-link',
                '.news-link',
                '.post-link',
                'h1 a', 'h2 a', 'h3 a',  # Headlines
            ]
            
            base_domain = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            
            for selector in selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href:
                        # Convert relative URLs to absolute
                        full_url = urljoin(url, href)
                        
                        # Filter out non-article URLs
                        if self._is_likely_article_url(full_url):
                            article_urls.add(full_url)
            
            # If no specific article links found, try generic links from the same domain
            if not article_urls:
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href')
                    if href:
                        full_url = urljoin(url, href)
                        if (urlparse(full_url).netloc == urlparse(url).netloc and 
                            self._is_likely_article_url(full_url)):
                            article_urls.add(full_url)
            
            logger.info(f"Found {len(article_urls)} potential article URLs from {url}")
            return list(article_urls)
        
        except Exception as e:
            logger.error(f"Error extracting articles from {url}: {e}")
            return [url]  # Return the original URL as a fallback
    
    def _is_likely_article_url(self, url: str) -> bool:
        """Determine if a URL is likely to be an article."""
        # Skip common non-article paths
        skip_patterns = [
            '/category/', '/tag/', '/author/', '/search/', '/page/',
            '/contact', '/about', '/privacy', '/terms', '/sitemap',
            '.pdf', '.jpg', '.png', '.gif', '.css', '.js',
            '/feed/', '/rss/', '/admin/', '/login', '/register'
        ]
        
        url_lower = url.lower()
        for pattern in skip_patterns:
            if pattern in url_lower:
                return False
        
        # Look for positive article indicators
        article_patterns = [
            '/article/', '/news/', '/blog/', '/post/', '/story/',
            '/content/', '/read/', '/view/'
        ]
        
        for pattern in article_patterns:
            if pattern in url_lower:
                return True
        
        # If URL has date patterns, it's likely an article
        date_pattern = r'/\d{4}(/\d{1,2})?(/\d{1,2})?/'
        if re.search(date_pattern, url):
            return True
        
        # If URL ends with number or has article-like structure
        if re.search(r'/[\w-]+(-\d+)?/?$', url):
            return True
        
        return False
    
    def extract_article_content(self, url: str) -> Dict[str, Any]:
        """
        Extract article content, metadata, and other details from a single article URL.
        """
        article_data = {
            'url': url,
            'title': None,
            'author': None,
            'description': None,
            'publication_date': None,
            'content': None,
            'extracted_at': datetime.now(timezone.utc)
        }
        
        try:
            # Try with trafilatura first (usually better for content extraction)
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            html_content = response.text
            
            # Extract main content with trafilatura
            content = trafilatura.extract(html_content, include_comments=False, 
                                        include_tables=True, include_links=False)
            
            if content:
                article_data['content'] = content
            
            # Try newspaper3k for better metadata extraction
            try:
                newspaper_article = NewspaperArticle(url)
                newspaper_article.download()
                newspaper_article.parse()
                
                if newspaper_article.title:
                    article_data['title'] = newspaper_article.title
                
                if newspaper_article.authors:
                    article_data['author'] = ', '.join(newspaper_article.authors[:3])  # Limit to 3 authors
                
                if newspaper_article.meta_description:
                    article_data['description'] = newspaper_article.meta_description
                
                if newspaper_article.publish_date:
                    article_data['publication_date'] = newspaper_article.publish_date
                
                # Use newspaper3k content if trafilatura didn't work
                if not article_data['content'] and newspaper_article.text:
                    article_data['content'] = newspaper_article.text
                    
            except Exception as e:
                logger.warning(f"Newspaper3k extraction failed for {url}: {e}")
            
            # Fallback: manual extraction with BeautifulSoup
            if not any([article_data['title'], article_data['content']]):
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Extract title
                title_selectors = ['h1', 'title', '.article-title', '.post-title', '.entry-title']
                for selector in title_selectors:
                    title_elem = soup.select_one(selector)
                    if title_elem:
                        article_data['title'] = title_elem.get_text().strip()
                        break
                
                # Extract content
                content_selectors = [
                    '.article-content', '.post-content', '.entry-content',
                    '.article-body', '.post-body', 'article', '.content'
                ]
                
                for selector in content_selectors:
                    content_elem = soup.select_one(selector)
                    if content_elem:
                        # Remove script and style elements
                        for elem in content_elem(['script', 'style', 'nav', 'aside', 'footer']):
                            elem.decompose()
                        article_data['content'] = content_elem.get_text().strip()
                        break
                
                # Extract meta description if not found
                if not article_data['description']:
                    meta_desc = soup.find('meta', {'name': 'description'})
                    if meta_desc:
                        article_data['description'] = meta_desc.get('content')
                
                # Extract author from meta tags
                if not article_data['author']:
                    author_selectors = [
                        'meta[name="author"]',
                        'meta[property="article:author"]',
                        '.author', '.byline'
                    ]
                    
                    for selector in author_selectors:
                        author_elem = soup.select_one(selector)
                        if author_elem:
                            if author_elem.name == 'meta':
                                article_data['author'] = author_elem.get('content')
                            else:
                                article_data['author'] = author_elem.get_text().strip()
                            break
            
            # Clean up the content
            if article_data['content']:
                # Remove excessive whitespace
                article_data['content'] = re.sub(r'\s+', ' ', article_data['content']).strip()
                # Limit content length to prevent database issues
                if len(article_data['content']) > 50000:
                    article_data['content'] = article_data['content'][:50000] + "..."
            
            # Ensure we have at least a title
            if not article_data['title']:
                # Generate a title from URL
                path = urlparse(url).path
                title = path.split('/')[-1].replace('-', ' ').replace('_', ' ').title()
                article_data['title'] = title or "Untitled Article"
            
            logger.info(f"Successfully extracted content from {url}")
            return article_data
            
        except Exception as e:
            import traceback
            logger.error(f"Error extracting content from {url}: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            # Return minimal data even if extraction fails
            article_data['title'] = article_data.get('title') or "Failed to extract title"
            article_data['content'] = f"Content extraction failed for {url}: {str(e)}"
            return article_data
    
    def is_valid_article(self, article_data: Dict[str, Any]) -> bool:
        """
        Validate if the extracted article data is sufficient for processing.
        """
        # Must have URL and either title or content
        if not article_data.get('url'):
            return False
        
        if not article_data.get('title') and not article_data.get('content'):
            return False
        
        # Content should be substantial (at least 100 characters)
        content = article_data.get('content', '')
        if len(content.strip()) < 100:
            return False
        
        return True