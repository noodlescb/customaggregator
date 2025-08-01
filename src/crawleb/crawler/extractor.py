import logging
import requests
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin, urlparse
import trafilatura
from newspaper import Article as NewspaperArticle
from bs4 import BeautifulSoup
import re
import time
import random

logger = logging.getLogger(__name__)


class ContentExtractor:
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.last_request_time = 0
        self.min_delay = 1.0  # Minimum delay between requests in seconds
        self.max_delay = 3.0  # Maximum delay between requests in seconds
        
        # Rotate between different realistic user agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        
        # Set up session with better headers
        self._setup_session()
    
    def _setup_session(self):
        """Set up the requests session with realistic browser headers."""
        headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
        self.session.headers.update(headers)
    
    def _make_request(self, url: str) -> requests.Response:
        """Make a rate-limited HTTP request with anti-detection measures."""
        # Implement rate limiting
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_delay:
            delay = random.uniform(self.min_delay, self.max_delay)
            logger.debug(f"Rate limiting: waiting {delay:.2f}s before request to {url}")
            time.sleep(delay)
        
        # Rotate user agent occasionally
        if random.random() < 0.3:  # 30% chance to rotate
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
        
        # Add some randomization to headers
        if random.random() < 0.5:
            self.session.headers['Accept-Language'] = random.choice([
                'en-US,en;q=0.9', 
                'en-US,en;q=0.8,es;q=0.7', 
                'en-GB,en;q=0.9,en-US;q=0.8'
            ])
        
        try:
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            self.last_request_time = time.time()
            
            # Log the response for debugging
            logger.debug(f"Request to {url}: {response.status_code}")
            
            # Handle specific error cases
            if response.status_code == 403:
                logger.warning(f"403 Forbidden for {url} - site may be blocking automated requests")
                # Try multiple strategies to get around the block
                retry_attempts = 3
                for attempt in range(retry_attempts):
                    logger.info(f"Retry attempt {attempt + 1} for {url}")
                    
                    # Progressive delay
                    delay = random.uniform(3.0, 8.0) * (attempt + 1)
                    time.sleep(delay)
                    
                    # Create a completely fresh session
                    old_session = self.session
                    self.session = requests.Session()
                    self._setup_session()
                    
                    # Add additional headers that might help
                    extra_headers = {
                        'Referer': 'https://www.google.com/',
                        'Sec-Ch-Ua': '"Chromium";v="120", "Not_A Brand";v="24", "Google Chrome";v="120"',
                        'Sec-Ch-Ua-Mobile': '?0',
                        'Sec-Ch-Ua-Platform': '"macOS"'
                    }
                    self.session.headers.update(extra_headers)
                    
                    try:
                        response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
                        self.last_request_time = time.time()
                        
                        if response.status_code != 403:
                            logger.info(f"Retry successful for {url} on attempt {attempt + 1}")
                            break
                    except Exception as e:
                        logger.debug(f"Retry attempt {attempt + 1} failed: {e}")
                        if attempt == retry_attempts - 1:
                            # Restore old session and raise the original error
                            self.session = old_session
                            response.raise_for_status()
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
    
    def extract_articles_from_page(self, url: str) -> List[str]:
        """
        Extract article URLs from a page (e.g., news homepage, blog index).
        Returns a list of article URLs found on the page.
        """
        try:
            response = self._make_request(url)
            
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
                # GeekWire specific patterns
                'a[href*="/startups/"]',
                'a[href*="/biotech/"]',
                'a[href*="/enterprise/"]',
                'a[href*="/transportation/"]',
                'a[href*="/cloud/"]',
                'a[href*="/ai/"]',
                '.post-title a',
                '.entry-title a',
                '.headline a',
                '.story-headline a'
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
            '/content/', '/read/', '/view/',
            # GeekWire specific patterns
            '/startups/', '/biotech/', '/enterprise/', '/transportation/',
            '/cloud/', '/ai/', '/fintech/', '/gaming/', '/mobile/',
            '/venture-capital/', '/hiring/', '/legal/', '/real-estate/'
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
            response = self._make_request(url)
            
            html_content = response.text
            
            # Extract main content with trafilatura
            content = trafilatura.extract(html_content, include_comments=False, 
                                        include_tables=True, include_links=False)
            
            if content:
                article_data['content'] = content
            
            # Try newspaper3k for better metadata extraction
            try:
                newspaper_article = NewspaperArticle(url)
                # Set the HTML content we already have instead of downloading again
                newspaper_article.set_html(html_content)
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
            
            # Try a fallback approach using newspaper3k's native download method
            try:
                logger.info(f"Trying fallback extraction method for {url}")
                time.sleep(random.uniform(2.0, 5.0))  # Wait before fallback
                
                newspaper_article = NewspaperArticle(url)
                newspaper_article.download()
                newspaper_article.parse()
                
                if newspaper_article.title or newspaper_article.text:
                    article_data['title'] = newspaper_article.title or "No title extracted"
                    article_data['content'] = newspaper_article.text or "No content extracted"
                    article_data['author'] = ', '.join(newspaper_article.authors[:3]) if newspaper_article.authors else None
                    article_data['description'] = newspaper_article.meta_description
                    article_data['publication_date'] = newspaper_article.publish_date
                    
                    logger.info(f"Fallback extraction successful for {url}")
                    return article_data
                else:
                    logger.warning(f"Fallback extraction returned no content for {url}")
            except Exception as fallback_error:
                logger.error(f"Fallback extraction also failed for {url}: {fallback_error}")
            
            # If all extraction methods fail, raise an exception instead of returning bad data
            logger.error(f"All extraction methods failed for {url}")
            raise Exception(f"Content extraction failed for {url}: {str(e)}")
    
    def is_valid_article(self, article_data: Dict[str, Any]) -> bool:
        """
        Validate if the extracted article data is sufficient for processing.
        """
        # Must have URL
        if not article_data.get('url'):
            return False
        
        # Check for failed extraction indicators
        title = article_data.get('title', '')
        content = article_data.get('content', '')
        
        # Reject articles with error indicators in title or content
        error_indicators = [
            'failed to extract',
            'content extraction failed',
            'no title extracted',
            'no content extracted',
            'extraction failed',
            'error:'
        ]
        
        title_lower = title.lower()
        content_lower = content.lower()
        
        for indicator in error_indicators:
            if indicator in title_lower or indicator in content_lower:
                logger.info(f"Rejecting article {article_data.get('url')} due to extraction failure indicator")
                return False
        
        # Must have meaningful title and content
        if not title or not content:
            return False
        
        # Content should be substantial (at least 100 characters)
        if len(content.strip()) < 100:
            logger.info(f"Rejecting article {article_data.get('url')} due to insufficient content length: {len(content.strip())} chars")
            return False
        
        # Title should be meaningful (not just whitespace or very short)
        if len(title.strip()) < 10:
            logger.info(f"Rejecting article {article_data.get('url')} due to insufficient title length: '{title}'")
            return False
        
        return True