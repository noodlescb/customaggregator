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
from .job_status import job_tracker, JobStatus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="News-dles - Web Article Crawler", version="1.0.0")

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
    
    # Load cached trending report if available
    cached_report = db.get_latest_trending_report(days)
    
    return templates.TemplateResponse("trending.html", {
        "request": request,
        "days": days,
        "trending_data": cached_report
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
    job_tracker.start_job("crawl", "Initializing crawler...")
    
    try:
        _, crawler = get_llm_client_and_crawler()
        
        if not crawler:
            job_tracker.fail_job("crawl", "Crawler not initialized - check configuration")
            return
        
        job_tracker.update_job_step("crawl", "Crawling registry URLs...")
        await crawler.run_crawl()
        
        job_tracker.complete_job("crawl", {"message": "Crawl completed successfully"})
        logger.info("Background crawl completed")
    except Exception as e:
        error_msg = f"Background crawl failed: {e}"
        job_tracker.fail_job("crawl", error_msg)
        logger.error(error_msg)


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
    job_tracker.start_job("research", "Initializing company research...")
    
    try:
        llm_client, _ = get_llm_client_and_crawler()
        
        if not llm_client:
            job_tracker.fail_job("research", "Company research not configured - check configuration")
            return
        
        job_tracker.update_job_step("research", "Researching companies with missing info...")
        researcher = CompanyResearcher(llm_client, db)
        results = await researcher.research_companies_with_missing_info()
        
        job_tracker.complete_job("research", results)
        logger.info(f"Company research completed: {results}")
    except Exception as e:
        error_msg = f"Company research failed: {e}"
        job_tracker.fail_job("research", error_msg)
        logger.error(error_msg)


@app.post("/companies/research")
async def research_companies(background_tasks: BackgroundTasks):
    """Trigger company research for companies with missing information."""
    llm_client, _ = get_llm_client_and_crawler()
    
    if not llm_client:
        raise HTTPException(status_code=400, detail="Company research not configured - please set up Databricks configuration first")
    
    # Run research in background
    background_tasks.add_task(run_company_research_background)
    
    return {"message": "Company research started in background"}


async def run_trending_analysis_background(days: int = 7):
    """Background task to analyze trending topics."""
    job_tracker.start_job("trending", f"Initializing trending analysis for {days} days...")
    
    try:
        llm_client, _ = get_llm_client_and_crawler()
        
        if not llm_client:
            job_tracker.fail_job("trending", "Trending analysis not configured - check configuration")
            return None
        
        job_tracker.update_job_step("trending", f"Analyzing trending topics for last {days} days...")
        analyzer = TrendingAnalyzer(db, llm_client)
        results = await analyzer.analyze_trending_topics(days)
        
        job_tracker.complete_job("trending", {
            "days": days,
            "article_count": results.get("article_count", 0),
            "ai_topics_found": len(results.get('ai_trending_topics', []))
        })
        logger.info(f"Trending analysis completed for {days} days: {len(results.get('ai_trending_topics', []))} AI topics found")
        return results
    except Exception as e:
        error_msg = f"Trending analysis failed: {e}"
        job_tracker.fail_job("trending", error_msg)
        logger.error(error_msg)
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


@app.get("/theme/{theme_id}/articles", response_class=HTMLResponse)
async def theme_articles_page(request: Request, theme_id: int, page: int = 1):
    """Page showing all articles for a specific theme."""
    try:
        # Get theme details
        theme = db.get_theme_by_id(theme_id)
        if not theme:
            raise HTTPException(status_code=404, detail="Theme not found")
        
        # Get articles for this theme with pagination
        per_page = 20
        offset = (page - 1) * per_page
        articles = db.get_articles_by_theme(theme_id, limit=per_page, offset=offset)
        
        # Calculate pagination info
        has_more = len(articles) == per_page
        
        return templates.TemplateResponse("theme_articles.html", {
            "request": request,
            "theme": theme,
            "articles": articles,
            "page": page,
            "has_more": has_more,
            "prev_page": page - 1 if page > 1 else None,
            "next_page": page + 1 if has_more else None
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error loading theme articles: {e}")
        raise HTTPException(status_code=500, detail="Failed to load theme articles")


@app.get("/api/theme/find")
async def find_theme_by_name(theme_name: str, report_id: int = None):
    """Find theme ID by name and optional report ID."""
    try:
        if report_id:
            theme = db.get_theme_by_name_and_report(theme_name, report_id)
        else:
            # If no report_id provided, get the most recent theme with this name
            import duckdb
            with duckdb.connect(str(db.db_path)) as conn:
                result = conn.execute("""
                    SELECT theme_id, name, explanation, insights, report_id, created_at
                    FROM themes 
                    WHERE name = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                """, [theme_name]).fetchone()
                
                if result:
                    theme = {
                        'theme_id': result[0], 'name': result[1], 'explanation': result[2],
                        'insights': result[3], 'report_id': result[4], 'created_at': result[5]
                    }
                else:
                    theme = None
        
        if not theme:
            raise HTTPException(status_code=404, detail="Theme not found")
        
        return {"theme_id": theme['theme_id'], "name": theme['name']}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error finding theme: {e}")
        raise HTTPException(status_code=500, detail="Failed to find theme")


async def run_refresh_all_background():
    """Background task to run all three jobs in sequence: crawl, research, trending."""
    job_tracker.start_job("refresh_all", "Starting comprehensive data refresh...")
    
    try:
        # Step 1: Run crawl
        job_tracker.update_job_step("refresh_all", "Step 1/3: Running article crawler", 1)
        await run_crawl_background()
        
        # Check if crawl succeeded
        crawl_status = job_tracker.get_status("crawl")
        if crawl_status["status"] != JobStatus.COMPLETED:
            job_tracker.fail_job("refresh_all", f"Crawl step failed: {crawl_status.get('error', 'Unknown error')}")
            return
        
        # Step 2: Run company research
        job_tracker.update_job_step("refresh_all", "Step 2/3: Researching missing company info", 2)
        await run_company_research_background()
        
        # Check if research succeeded
        research_status = job_tracker.get_status("research")
        if research_status["status"] != JobStatus.COMPLETED:
            job_tracker.fail_job("refresh_all", f"Research step failed: {research_status.get('error', 'Unknown error')}")
            return
        
        # Step 3: Run trending analysis for all time periods
        job_tracker.update_job_step("refresh_all", "Step 3/3: Generating trending reports", 3)
        
        # Generate trending reports for 7, 30, and 90 days
        for days in [7, 30, 90]:
            await run_trending_analysis_background(days)
            # Don't fail the whole process if one trending report fails
            trending_status = job_tracker.get_status("trending")
            if trending_status["status"] != JobStatus.COMPLETED:
                logger.warning(f"Trending analysis for {days} days failed: {trending_status.get('error')}")
        
        # Collect final results
        final_results = {
            "crawl_results": job_tracker.get_status("crawl")["results"],
            "research_results": job_tracker.get_status("research")["results"],
            "trending_completed": True,
            "message": "All jobs completed successfully"
        }
        
        job_tracker.complete_job("refresh_all", final_results)
        logger.info("Refresh All background job completed successfully")
        
    except Exception as e:
        error_msg = f"Refresh All failed: {e}"
        job_tracker.fail_job("refresh_all", error_msg)
        logger.error(error_msg)


@app.post("/refresh-all")
async def trigger_refresh_all(background_tasks: BackgroundTasks):
    """Trigger the comprehensive refresh of all data."""
    # Check if any job is already running
    if job_tracker.is_any_job_running():
        raise HTTPException(status_code=409, detail="Another job is already running. Please wait for it to complete.")
    
    # Check configuration
    llm_client, crawler = get_llm_client_and_crawler()
    if not llm_client or not crawler:
        raise HTTPException(status_code=400, detail="System not configured - please set up Databricks configuration first")
    
    # Reset all job statuses
    for job_name in ["refresh_all", "crawl", "research", "trending"]:
        job_tracker.reset_job(job_name)
    
    # Start the background task
    background_tasks.add_task(run_refresh_all_background)
    
    return {"message": "Comprehensive refresh started in background"}


@app.get("/job-status/{job_name}")
async def get_job_status(job_name: str):
    """Get the status of a specific job."""
    valid_jobs = ["refresh_all", "crawl", "research", "trending"]
    if job_name not in valid_jobs:
        raise HTTPException(status_code=404, detail=f"Job not found. Valid jobs: {valid_jobs}")
    
    status = job_tracker.get_status(job_name)
    
    # Convert enum to string for JSON serialization
    if "status" in status:
        status["status"] = status["status"].value
    
    # Convert datetime to ISO string
    for field in ["started_at", "completed_at"]:
        if status.get(field):
            status[field] = status[field].isoformat()
    
    return status


@app.get("/job-status")
async def get_all_job_status():
    """Get the status of all jobs."""
    all_status = job_tracker.get_all_status()
    
    # Convert enums and datetimes for JSON serialization
    for job_status in all_status.values():
        if "status" in job_status:
            job_status["status"] = job_status["status"].value
        
        for field in ["started_at", "completed_at"]:
            if job_status.get(field):
                job_status[field] = job_status[field].isoformat()
    
    return all_status


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