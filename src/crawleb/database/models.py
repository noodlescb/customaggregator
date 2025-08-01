from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, HttpUrl


class CrawlRegistry(BaseModel):
    id: Optional[int] = None
    url: str
    extract_topics: bool = True
    extract_companies: bool = True
    active: bool = True
    created_at: Optional[datetime] = None


class Article(BaseModel):
    article_id: Optional[int] = None
    url: str
    title: Optional[str] = None
    author: Optional[str] = None
    description: Optional[str] = None
    publication_date: Optional[datetime] = None
    crawl_date: datetime
    summary: Optional[str] = None
    content: Optional[str] = None


class Company(BaseModel):
    company_id: Optional[int] = None
    name: str
    website_url: Optional[str] = None
    summary: Optional[str] = None
    founded_year: Optional[int] = None
    employee_count: Optional[str] = None
    logo_url: Optional[str] = None
    created_at: Optional[datetime] = None


class Topic(BaseModel):
    topic_id: Optional[int] = None
    name: str
    created_at: Optional[datetime] = None


class ArticleTopic(BaseModel):
    article_id: int
    topic_id: int
    relevance_score: Optional[float] = None


class ArticleCompany(BaseModel):
    article_id: int
    company_id: int
    relevance_score: Optional[float] = None


class Config(BaseModel):
    databricks_workspace_url: str
    databricks_api_key: str
    llm_endpoint_name: str
    max_articles_per_page: int = 10


class Theme(BaseModel):
    theme_id: Optional[int] = None
    name: str
    explanation: Optional[str] = None
    insights: Optional[str] = None
    report_id: Optional[int] = None  # Links to the trending report that identified this theme
    created_at: Optional[datetime] = None


class ArticleTheme(BaseModel):
    article_id: int
    theme_id: int
    relevance_score: Optional[float] = None


class TrendingReport(BaseModel):
    report_id: Optional[int] = None
    days: int = 7
    generated_at: Optional[datetime] = None
    article_count: Optional[int] = None
    results: Optional[dict] = None