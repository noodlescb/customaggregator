import json
import logging
from typing import List, Dict, Any, Optional
import httpx

logger = logging.getLogger(__name__)


class DatabricksLLMClient:
    def __init__(self, workspace_url: str, api_key: str, endpoint_name: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.api_key = api_key
        self.endpoint_name = endpoint_name
        self.base_url = f"{self.workspace_url}/serving-endpoints/{endpoint_name}/invocations"
        
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    async def generate_response(self, prompt: str, max_tokens: int = 1000, temperature: float = 0.1) -> str:
        """Generate a response from the Databricks LLM endpoint."""
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.base_url,
                    headers=self.headers,
                    json=payload,
                    timeout=60.0
                )
                response.raise_for_status()
                
                result = response.json()
                return result.get("choices", [{}])[0].get("message", {}).get("content", "")
        
        except Exception as e:
            logger.error(f"Error calling Databricks LLM: {e}")
            return ""
    
    async def summarize_article(self, content: str, title: str = "") -> str:
        """Summarize an article to 500 words or less."""
        prompt = f"""
        Please summarize the following article in 500 words or less. Focus on the key points, main arguments, and important details.
        
        Title: {title}
        
        Content:
        {content[:8000]}  # Limit content to prevent token overflow
        
        Summary:
        """
        
        return await self.generate_response(prompt, max_tokens=700)
    
    async def extract_topics(self, content: str, title: str = "") -> List[str]:
        """Extract up to 5 main topics from the article content."""
        prompt = f"""
        Analyze the following article and extract up to 5 main topics. Each topic should be 1-3 words long and represent key subjects discussed in the article.
        
        Return only the topics as a JSON array of strings, like: ["AI", "Machine Learning", "Healthcare", "Technology"]
        
        Title: {title}
        
        Content:
        {content[:6000]}
        
        Topics:
        """
        
        response = await self.generate_response(prompt, max_tokens=100, temperature=0.1)
        
        try:
            # Try to parse the JSON response
            topics = json.loads(response.strip())
            if isinstance(topics, list):
                return [topic.strip() for topic in topics[:5]]  # Limit to 5 topics
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse topics JSON: {response}")
            # Fallback: try to extract topics from plain text response
            topics = []
            for line in response.split('\n'):
                line = line.strip()
                if line and not line.startswith('[') and not line.startswith(']'):
                    # Remove quotes and common prefixes
                    topic = line.strip('"').strip("'").strip('-').strip()
                    if topic and len(topic.split()) <= 3:
                        topics.append(topic)
            return topics[:5]
        
        return []
    
    async def extract_companies(self, content: str, title: str = "") -> List[str]:
        """Extract up to 5 companies mentioned in the article."""
        prompt = f"""
        Analyze the following article and extract up to 5 company names that are mentioned or discussed.
        Focus on well-known companies, startups, or organizations that are central to the article's content.
        
        Return only the company names as a JSON array of strings, like: ["Apple", "Google", "Microsoft"]
        
        Title: {title}
        
        Content:
        {content[:6000]}
        
        Companies:
        """
        
        response = await self.generate_response(prompt, max_tokens=150, temperature=0.1)
        
        try:
            companies = json.loads(response.strip())
            if isinstance(companies, list):
                return [company.strip() for company in companies[:5]]
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse companies JSON: {response}")
            # Fallback parsing
            companies = []
            for line in response.split('\n'):
                line = line.strip()
                if line and not line.startswith('[') and not line.startswith(']'):
                    company = line.strip('"').strip("'").strip('-').strip()
                    if company:
                        companies.append(company)
            return companies[:5]
        
        return []
    
    async def research_company(self, company_name: str) -> Dict[str, Any]:
        """Research a company and return profile information."""
        prompt = f"""
        Research the company "{company_name}" and provide information in the following JSON format:
        {{
            "website_url": "company homepage URL",
            "summary": "brief description of what the company does (2-3 sentences)",
            "founded_year": year_as_integer_or_null,
            "employee_count": "estimated employee count as string (e.g., '1000-5000', '50-100', 'Unknown')"
        }}
        
        If you cannot find reliable information for any field, use null or "Unknown" as appropriate.
        Return only the JSON object, no other text.
        
        Company: {company_name}
        """
        
        response = await self.generate_response(prompt, max_tokens=300, temperature=0.1)
        
        try:
            company_info = json.loads(response.strip())
            return {
                "website_url": company_info.get("website_url"),
                "summary": company_info.get("summary"),
                "founded_year": company_info.get("founded_year"),
                "employee_count": company_info.get("employee_count", "Unknown")
            }
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse company research JSON for {company_name}: {response}")
            return {
                "website_url": None,
                "summary": f"Information about {company_name} could not be retrieved.",
                "founded_year": None,
                "employee_count": "Unknown"
            }
    
    async def test_connection(self) -> bool:
        """Test the connection to the Databricks LLM endpoint."""
        try:
            response = await self.generate_response("Hello, please respond with 'OK' if you can see this message.", max_tokens=10)
            return "OK" in response or "ok" in response.lower()
        except Exception as e:
            logger.error(f"Failed to test Databricks connection: {e}")
            return False