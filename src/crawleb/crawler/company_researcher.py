import logging
import asyncio
from typing import Dict, Any, List, Optional
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse

from ..llm.databricks_client import DatabricksLLMClient
from ..database.database import Database

logger = logging.getLogger(__name__)


class CompanyResearcher:
    def __init__(self, llm_client: DatabricksLLMClient, database: Database):
        self.llm_client = llm_client
        self.db = database
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    async def research_companies_with_missing_info(self) -> Dict[str, Any]:
        """Research companies that have missing information."""
        results = {
            'total_companies': 0,
            'researched_companies': 0,
            'updated_companies': 0,
            'failed_companies': 0,
            'errors': []
        }
        
        try:
            # Get companies with missing information
            companies = self.db.get_companies()
            companies_to_research = []
            
            for company in companies:
                if self._needs_research(company):
                    companies_to_research.append(company)
            
            results['total_companies'] = len(companies_to_research)
            logger.info(f"Found {len(companies_to_research)} companies needing research")
            
            for company in companies_to_research:
                try:
                    logger.info(f"Researching company: {company['name']}")
                    
                    # Try LLM research first
                    company_info = await self._research_with_llm(company['name'])
                    
                    # If LLM research failed or incomplete, try web search
                    if not self._is_complete_info(company_info):
                        logger.info(f"LLM research incomplete for {company['name']}, trying web search")
                        web_info = await self._research_with_web_search(company['name'])
                        company_info = self._merge_company_info(company_info, web_info)
                    
                    # Update the company if we got better information
                    if self._is_better_info(company, company_info):
                        await self._update_company(company['company_id'], company_info)
                        results['updated_companies'] += 1
                        logger.info(f"Updated company: {company['name']}")
                    
                    results['researched_companies'] += 1
                    
                except Exception as e:
                    error_msg = f"Error researching {company['name']}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['failed_companies'] += 1
                    
                # Small delay to be respectful to APIs and websites
                await asyncio.sleep(1)
            
            return results
            
        except Exception as e:
            logger.error(f"Critical error in company research: {e}")
            results['errors'].append(f"Critical error: {str(e)}")
            return results

    def _needs_research(self, company: Dict[str, Any]) -> bool:
        """Determine if a company needs additional research."""
        # Check if summary is missing or looks like an error message
        summary = company.get('summary', '')
        if not summary or 'could not be retrieved' in summary.lower():
            return True
        
        # Check if other important fields are missing
        if not company.get('website_url'):
            return True
        
        if not company.get('founded_year'):
            return True
            
        return False

    async def _research_with_llm(self, company_name: str) -> Dict[str, Any]:
        """Use LLM to research company information."""
        try:
            prompt = f"""
            Research the company "{company_name}" and provide comprehensive information in the following JSON format:
            {{
                "website_url": "official company website URL",
                "summary": "detailed description of what the company does, their main products/services, and market position (3-4 sentences)",
                "founded_year": year_as_integer_or_null,
                "employee_count": "estimated employee count range (e.g., '100-500', '1000+', 'Unknown')",
                "industry": "primary industry or sector",
                "headquarters": "city, country of headquarters",
                "key_products": "main products or services offered"
            }}
            
            Please provide accurate, up-to-date information. If you cannot find reliable information for any field, use null or "Unknown" as appropriate.
            Return only the JSON object, no other text.
            
            Company: {company_name}
            """
            
            response = await self.llm_client.generate_response(prompt, max_tokens=500, temperature=0.1)
            
            try:
                import json
                company_info = json.loads(response.strip())
                logger.info(f"LLM research successful for {company_name}")
                return company_info
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse LLM response for {company_name}")
                return {}
                
        except Exception as e:
            logger.error(f"LLM research failed for {company_name}: {e}")
            return {}

    async def _research_with_web_search(self, company_name: str) -> Dict[str, Any]:
        """Use web search to find company information."""
        try:
            # Search for the company's official website
            search_results = await self._search_company_website(company_name)
            
            if search_results:
                # Try to extract information from the company's website
                website_info = await self._extract_from_website(search_results['url'], company_name)
                if website_info:
                    return website_info
            
            # Fallback: try to find information from other sources
            return await self._search_company_info(company_name)
            
        except Exception as e:
            logger.error(f"Web search failed for {company_name}: {e}")
            return {}

    async def _search_company_website(self, company_name: str) -> Optional[Dict[str, Any]]:
        """Search for company's official website using DuckDuckGo."""
        try:
            # Use DuckDuckGo instant answer API (no API key required)
            search_query = f"{company_name} official website"
            url = f"https://api.duckduckgo.com/?q={search_query}&format=json&no_html=1&skip_disambig=1"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for instant answer with website
            if data.get('Answer'):
                # Look for URLs in the answer
                import re
                urls = re.findall(r'https?://[^\s<>"]+', data['Answer'])
                if urls:
                    return {'url': urls[0], 'source': 'duckduckgo_instant'}
            
            # Check abstract sources
            if data.get('AbstractURL'):
                return {'url': data['AbstractURL'], 'source': 'duckduckgo_abstract'}
            
            # Check related topics
            for topic in data.get('RelatedTopics', []):
                if isinstance(topic, dict) and topic.get('FirstURL'):
                    return {'url': topic['FirstURL'], 'source': 'duckduckgo_related'}
            
            return None
            
        except Exception as e:
            logger.error(f"Website search failed for {company_name}: {e}")
            return None

    async def _extract_from_website(self, url: str, company_name: str) -> Dict[str, Any]:
        """Extract company information from their website."""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract basic information
            company_info = {
                'website_url': url,
                'summary': None,
                'founded_year': None,
                'employee_count': None
            }
            
            # Try to extract company description
            description = self._extract_company_description(soup)
            if description:
                company_info['summary'] = description
            
            # Try to extract founded year
            founded_year = self._extract_founded_year(soup)
            if founded_year:
                company_info['founded_year'] = founded_year
            
            logger.info(f"Extracted website info for {company_name}")
            return company_info
            
        except Exception as e:
            logger.error(f"Website extraction failed for {url}: {e}")
            return {'website_url': url}  # At least return the URL

    def _extract_company_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract company description from website."""
        # Try various selectors for company descriptions
        selectors = [
            'meta[name="description"]',
            'meta[property="og:description"]',
            '.company-description',
            '.about-description',
            '#about p',
            '.hero-description',
            '.intro-text'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                if elem.name == 'meta':
                    text = elem.get('content', '')
                else:
                    text = elem.get_text().strip()
                
                if text and len(text) > 50:  # Must be substantial
                    return text[:500]  # Limit length
        
        return None

    def _extract_founded_year(self, soup: BeautifulSoup) -> Optional[int]:
        """Extract founded year from website."""
        # Look for founded/established patterns in text
        text = soup.get_text().lower()
        
        patterns = [
            r'founded in (\d{4})',
            r'established in (\d{4})',
            r'since (\d{4})',
            r'founded (\d{4})',
            r'established (\d{4})',
            r'©\s*(\d{4})'  # Copyright year as fallback
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            if matches:
                year = int(matches[0])
                # Reasonable year range
                if 1800 <= year <= 2024:
                    return year
        
        return None

    async def _search_company_info(self, company_name: str) -> Dict[str, Any]:
        """Search for general company information."""
        # This could be expanded to search other sources like:
        # - Crunchbase API
        # - LinkedIn company pages
        # - Wikipedia
        # For now, return basic structure
        return {
            'summary': f"Company information for {company_name} needs manual research.",
            'website_url': None,
            'founded_year': None,
            'employee_count': "Unknown"
        }

    def _is_complete_info(self, company_info: Dict[str, Any]) -> bool:
        """Check if company information is reasonably complete."""
        if not company_info:
            return False
        
        # At minimum, we want summary and website_url
        return (
            company_info.get('summary') and 
            len(company_info.get('summary', '')) > 50 and
            company_info.get('website_url')
        )

    def _is_better_info(self, current_company: Dict[str, Any], new_info: Dict[str, Any]) -> bool:
        """Check if new information is better than current."""
        if not new_info:
            return False
        
        # Check if we have a better summary
        current_summary = current_company.get('summary', '')
        new_summary = new_info.get('summary', '')
        
        if new_summary and ('could not be retrieved' in current_summary or len(new_summary) > len(current_summary)):
            return True
        
        # Check if we're filling in missing fields
        if not current_company.get('website_url') and new_info.get('website_url'):
            return True
        
        if not current_company.get('founded_year') and new_info.get('founded_year'):
            return True
        
        return False

    def _merge_company_info(self, llm_info: Dict[str, Any], web_info: Dict[str, Any]) -> Dict[str, Any]:
        """Merge information from LLM and web search, prioritizing better data."""
        merged = {}
        
        # Take the best summary
        llm_summary = llm_info.get('summary', '')
        web_summary = web_info.get('summary', '')
        
        if len(llm_summary) > len(web_summary):
            merged['summary'] = llm_summary
        elif web_summary:
            merged['summary'] = web_summary
        else:
            merged['summary'] = llm_summary
        
        # Prefer web-found website URL as it's more likely to be accurate
        merged['website_url'] = web_info.get('website_url') or llm_info.get('website_url')
        
        # Take any founded year we can get
        merged['founded_year'] = web_info.get('founded_year') or llm_info.get('founded_year')
        
        # Take LLM employee count as it might have more recent data
        merged['employee_count'] = llm_info.get('employee_count') or web_info.get('employee_count')
        
        return merged

    async def _update_company(self, company_id: int, company_info: Dict[str, Any]):
        """Update company in database with new information."""
        with self.db.get_connection() as conn:
            updates = []
            params = []
            
            if company_info.get('summary'):
                updates.append("summary = ?")
                params.append(company_info['summary'])
            
            if company_info.get('website_url'):
                updates.append("website_url = ?")
                params.append(company_info['website_url'])
            
            if company_info.get('founded_year'):
                updates.append("founded_year = ?")
                params.append(company_info['founded_year'])
            
            if company_info.get('employee_count'):
                updates.append("employee_count = ?")
                params.append(company_info['employee_count'])
            
            if updates:
                query = f"UPDATE companies SET {', '.join(updates)} WHERE company_id = ?"
                params.append(company_id)
                conn.execute(query, params)