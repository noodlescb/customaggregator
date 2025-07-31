import asyncio
import logging
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, Request, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ..database.database import Database
from ..database.models import CrawlRegistry, Config
from ..llm.databricks_client import DatabricksLLMClient
from ..crawler.crawler import WebCrawler
from ..crawler.company_researcher import CompanyResearcher
from ..crawler.trending_analyzer import TrendingAnalyzer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="CrawlEB - Web Article Crawler", version="1.0.0")

# Setup static files and templates
static_path = Path(__file__).parent.parent.parent.parent / "static"
templates_path = Path(__file__).parent.parent.parent.parent / "templates"

app.mount("/static", StaticFiles(directory=str(static_path)), name="static")
templates = Jinja2Templates(directory=str(templates_path))

# Initialize database
db = Database()

# Global variables for LLM client and crawler (will be initialized when config is set)
llm_client: Optional[DatabricksLLMClient] = None
crawler: Optional[WebCrawler] = None


class CrawlRegistryForm(BaseModel):
    url: str
    extract_topics: bool = True
    extract_companies: bool = True
    active: bool = True


class ConfigForm(BaseModel):
    databricks_workspace_url: str
    databricks_api_key: str
    llm_endpoint_name: str
    max_articles_per_page: int = 10


def get_llm_client_and_crawler():
    """Get initialized LLM client and crawler, or None if not configured."""
    global llm_client, crawler
    
    if llm_client is None or crawler is None:
        config = db.get_config()
        if config:
            llm_client = DatabricksLLMClient(
                workspace_url=config.databricks_workspace_url,
                api_key=config.databricks_api_key,
                endpoint_name=config.llm_endpoint_name
            )
            crawler = WebCrawler(db, llm_client)
    
    return llm_client, crawler


@app.get("/", response_class=HTMLResponse)
async def home(request: Request, page: int = 1, topic_id: Optional[int] = None, 
               company_id: Optional[int] = None):
    """Home page showing articles in descending order of crawl date."""
    config = db.get_config()
    articles_per_page = config.max_articles_per_page if config else 10
    
    offset = (page - 1) * articles_per_page
    articles = db.get_articles(
        limit=articles_per_page, 
        offset=offset, 
        topic_id=topic_id, 
        company_id=company_id
    )
    
    # Get filter info for display
    filter_info = {}
    if topic_id:
        topics = db.get_topics()
        topic = next((t for t in topics if t['topic_id'] == topic_id), None)
        filter_info['topic'] = topic['name'] if topic else f"Topic {topic_id}"
    
    if company_id:
        companies = db.get_companies()
        company = next((c for c in companies if c['company_id'] == company_id), None)
        filter_info['company'] = company['name'] if company else f"Company {company_id}"
    
    return templates.TemplateResponse("home.html", {
        "request": request,
        "articles": articles,
        "page": page,
        "has_more": len(articles) == articles_per_page,
        "filter_info": filter_info,
        "topic_id": topic_id,
        "company_id": company_id
    })


@app.get("/registry", response_class=HTMLResponse)
async def registry_page(request: Request):
    """Crawl registry management page."""
    registry_entries = db.get_crawl_registry()
    return templates.TemplateResponse("registry.html", {
        "request": request,
        "entries": registry_entries
    })


@app.post("/registry/add")
async def add_registry_entry(
    url: str = Form(...),
    extract_topics: bool = Form(False),
    extract_companies: bool = Form(False),
    active: bool = Form(False)
):
    """Add a new entry to the crawl registry."""
    try:
        registry = CrawlRegistry(
            url=url,
            extract_topics=extract_topics,
            extract_companies=extract_companies,
            active=active
        )
        db.add_crawl_url(registry)
        return RedirectResponse(url="/registry", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/registry/update/{entry_id}")
async def update_registry_entry(
    entry_id: int,
    extract_topics: bool = Form(False),
    extract_companies: bool = Form(False),
    active: bool = Form(False)
):
    """Update an existing registry entry."""
    try:
        # Get existing entry
        entries = db.get_crawl_registry()
        entry = next((e for e in entries if e.id == entry_id), None)
        if not entry:
            raise HTTPException(status_code=404, detail="Entry not found")
        
        # Update the entry
        entry.extract_topics = extract_topics
        entry.extract_companies = extract_companies
        entry.active = active
        
        db.update_crawl_registry(entry)
        return RedirectResponse(url="/registry", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/registry/delete/{entry_id}")
async def delete_registry_entry(entry_id: int):
    """Delete a registry entry."""
    try:
        db.delete_crawl_registry(entry_id)
        return RedirectResponse(url="/registry", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/companies", response_class=HTMLResponse)
async def companies_page(request: Request, search: Optional[str] = None):
    """Companies page showing all companies and their article counts."""
    companies = db.get_companies()
    
    # Filter companies by search term if provided
    if search:
        search_lower = search.lower()
        companies = [
            company for company in companies 
            if (search_lower in company['name'].lower() or 
                (company.get('summary') and search_lower in company['summary'].lower()))
        ]
    
    return templates.TemplateResponse("companies.html", {
        "request": request,
        "companies": companies,
        "search": search or ""
    })


@app.get("/companies/{company_id}", response_class=HTMLResponse)
async def company_profile(request: Request, company_id: int, page: int = 1):
    """Company profile page showing articles associated with the company."""
    companies = db.get_companies()
    company = next((c for c in companies if c['company_id'] == company_id), None)
    
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    config = db.get_config()
    articles_per_page = config.max_articles_per_page if config else 10
    
    offset = (page - 1) * articles_per_page
    articles = db.get_articles(
        limit=articles_per_page, 
        offset=offset, 
        company_id=company_id
    )
    
    return templates.TemplateResponse("company_profile.html", {
        "request": request,
        "company": company,
        "articles": articles,
        "page": page,
        "has_more": len(articles) == articles_per_page
    })


@app.get("/topics", response_class=HTMLResponse)
async def topics_page(request: Request):
    """Topics page showing all topics and their article counts."""
    topics = db.get_topics()
    return templates.TemplateResponse("topics.html", {
        "request": request,
        "topics": topics
    })


@app.get("/trending", response_class=HTMLResponse)
async def trending_page(request: Request, days: int = 7):
    """Trending page showing trending topics and companies for the specified time period."""
    # Validate days parameter
    if days not in [7, 30, 90]:
        days = 7
    
    return templates.TemplateResponse("trending.html", {
        "request": request,
        "days": days,
        "trending_data": None  # Will be populated when analysis is run
    })


@app.get("/config", response_class=HTMLResponse)
async def config_page(request: Request):
    """Configuration page for Databricks settings."""
    config = db.get_config()
    return templates.TemplateResponse("config.html", {
        "request": request,
        "config": config
    })


@app.post("/config/save")
async def save_config(
    databricks_workspace_url: str = Form(...),
    databricks_api_key: str = Form(...),
    llm_endpoint_name: str = Form(...),
    max_articles_per_page: int = Form(10)
):
    """Save configuration settings."""
    global llm_client, crawler
    
    try:
        config = Config(
            databricks_workspace_url=databricks_workspace_url,
            databricks_api_key=databricks_api_key,
            llm_endpoint_name=llm_endpoint_name,
            max_articles_per_page=max_articles_per_page
        )
        
        # Test the configuration
        test_client = DatabricksLLMClient(
            workspace_url=databricks_workspace_url,
            api_key=databricks_api_key,
            endpoint_name=llm_endpoint_name
        )
        
        connection_test = await test_client.test_connection()
        if not connection_test:
            raise HTTPException(status_code=400, detail="Failed to connect to Databricks LLM endpoint")
        
        # Save configuration
        db.save_config(config)
        
        # Reinitialize global objects
        llm_client = test_client
        crawler = WebCrawler(db, llm_client)
        
        return RedirectResponse(url="/config?success=1", status_code=303)
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def run_crawl_background():
    """Background task to run the crawler."""
    global crawler
    _, crawler = get_llm_client_and_crawler()
    
    if not crawler:
        logger.error("Crawler not initialized - check configuration")
        return
    
    try:
        await crawler.run_crawl()
        logger.info("Background crawl completed")
    except Exception as e:
        logger.error(f"Background crawl failed: {e}")


@app.post("/crawl/run")
async def trigger_crawl(background_tasks: BackgroundTasks):
    """Trigger a manual crawl."""
    _, crawler = get_llm_client_and_crawler()
    
    if not crawler:
        raise HTTPException(status_code=400, detail="Crawler not configured - please set up Databricks configuration first")
    
    # Run crawl in background
    background_tasks.add_task(run_crawl_background)
    
    return {"message": "Crawl started in background"}


@app.post("/crawl/single")
async def crawl_single_url(
    url: str = Form(...),
    extract_topics: bool = Form(False),
    extract_companies: bool = Form(False)
):
    """Crawl a single URL immediately."""
    _, crawler = get_llm_client_and_crawler()
    
    if not crawler:
        raise HTTPException(status_code=400, detail="Crawler not configured - please set up Databricks configuration first")
    
    try:
        result = await crawler.crawl_single_url(url, extract_topics, extract_companies)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def run_company_research_background():
    """Background task to research companies with missing information."""
    llm_client, _ = get_llm_client_and_crawler()
    
    if not llm_client:
        logger.error("Company research not configured - check configuration")
        return
    
    try:
        researcher = CompanyResearcher(llm_client, db)
        results = await researcher.research_companies_with_missing_info()
        logger.info(f"Company research completed: {results}")
    except Exception as e:
        logger.error(f"Company research failed: {e}")


@app.post("/companies/research")
async def research_companies(background_tasks: BackgroundTasks):
    """Trigger company research for companies with missing information."""
    llm_client, _ = get_llm_client_and_crawler()
    
    if not llm_client:
        raise HTTPException(status_code=400, detail="Company research not configured - please set up Databricks configuration first")
    
    # Run research in background
    background_tasks.add_task(run_company_research_background)
    
    return {"message": "Company research started in background"}


async def run_trending_analysis_background(days: int):
    """Background task to analyze trending topics."""
    llm_client, _ = get_llm_client_and_crawler()
    
    if not llm_client:
        logger.error("Trending analysis not configured - check configuration")
        return None
    
    try:
        analyzer = TrendingAnalyzer(db, llm_client)
        results = await analyzer.analyze_trending_topics(days)
        logger.info(f"Trending analysis completed for {days} days: {len(results.get('ai_trending_topics', []))} AI topics found")
        return results
    except Exception as e:
        logger.error(f"Trending analysis failed: {e}")
        return None


@app.post("/trending/analyze")
async def analyze_trending(days: int = Form(7)):
    """Trigger trending analysis for the specified time period."""
    llm_client, _ = get_llm_client_and_crawler()
    
    if not llm_client:
        raise HTTPException(status_code=400, detail="Trending analysis not configured - please set up Databricks configuration first")
    
    # Validate days parameter
    if days not in [7, 30, 90]:
        days = 7
    
    try:
        # Run analysis synchronously to return results immediately
        analyzer = TrendingAnalyzer(db, llm_client)
        results = await analyzer.analyze_trending_topics(days)
        
        return {
            "success": True,
            "data": results
        }
        
    except Exception as e:
        logger.error(f"Trending analysis failed: {e}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    config = db.get_config()
    configured = config is not None
    
    llm_client_ready = False
    if configured:
        _, crawler = get_llm_client_and_crawler()
        llm_client_ready = llm_client is not None
    
    return {
        "status": "healthy",
        "configured": configured,
        "llm_client_ready": llm_client_ready
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)