import logging
from typing import List, Dict, Any, Optional
from collections import Counter
import re

from ..database.database import Database
from ..llm.databricks_client import DatabricksLLMClient

logger = logging.getLogger(__name__)


class TrendingAnalyzer:
    def __init__(self, db: Database, llm_client: DatabricksLLMClient):
        self.db = db
        self.llm_client = llm_client
    
    async def analyze_trending_topics(self, days: int = 7) -> Dict[str, Any]:
        """
        Analyze trending topics for the given time period.
        
        Returns:
        - top_topics: Top 10 topics by article count
        - top_companies: Top 10 companies by article count  
        - ai_trending_topics: AI-identified trending topics with summaries
        """
        logger.info(f"Analyzing trending topics for the last {days} days")
        
        # Get articles from the specified date range
        articles = self.db.get_articles_by_date_range(days)
        
        if not articles:
            logger.warning(f"No articles found for the last {days} days")
            return {
                'top_topics': [],
                'top_companies': [],
                'ai_trending_topics': [],
                'article_count': 0,
                'days': days
            }
        
        # Get top topics and companies by article count
        top_topics = self.db.get_trending_topics_by_date_range(days, limit=10)
        top_companies = self.db.get_trending_companies_by_date_range(days, limit=10)
        
        # Analyze content for AI-powered trending topics
        ai_trending_topics = await self._identify_ai_trending_topics(articles)
        
        return {
            'top_topics': top_topics,
            'top_companies': top_companies, 
            'ai_trending_topics': ai_trending_topics,
            'article_count': len(articles),
            'days': days
        }
    
    async def _identify_ai_trending_topics(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Use AI to identify trending topics from article content.
        """
        try:
            # Extract article summaries and titles for analysis
            content_samples = []
            for article in articles[:50]:  # Limit to first 50 articles for performance
                sample = ""
                if article.get('title'):
                    sample += f"Title: {article['title']}\n"
                if article.get('summary'):
                    sample += f"Summary: {article['summary'][:200]}...\n"
                elif article.get('description'):
                    sample += f"Description: {article['description'][:200]}...\n"
                
                if sample:
                    content_samples.append(sample)
            
            if not content_samples:
                return []
            
            # Combine content for AI analysis
            combined_content = "\n---\n".join(content_samples[:20])  # Use top 20 for analysis
            
            prompt = f"""
            Analyze the following news articles from the last few days and identify the top 10 trending topics or themes. 
            For each trending topic, provide:
            1. Topic name (2-4 words)
            2. Brief explanation (1-2 sentences) of why it's trending
            3. Key insights or implications
            
            Articles:
            {combined_content}
            
            Format your response as JSON with this structure:
            {{
                "trending_topics": [
                    {{
                        "name": "Topic Name",
                        "explanation": "Why this is trending...",
                        "insights": "Key insights about this trend..."
                    }}
                ]
            }}
            """
            
            # Get AI analysis
            response = await self.llm_client.generate_response(prompt, max_tokens=1500)
            
            # Parse JSON response
            import json
            try:
                parsed_response = json.loads(response)
                trending_topics = parsed_response.get('trending_topics', [])
                
                # Add article references for each trending topic
                enhanced_topics = []
                for topic in trending_topics[:10]:  # Limit to top 10
                    # Find articles related to this topic
                    related_articles = self._find_related_articles(articles, topic['name'])
                    
                    enhanced_topics.append({
                        'name': topic['name'],
                        'explanation': topic['explanation'],
                        'insights': topic['insights'],
                        'related_article_count': len(related_articles),
                        'related_articles': related_articles[:5]  # Show top 5 related articles
                    })
                
                return enhanced_topics
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse AI trending topics response: {e}")
                # Fallback: extract topics from existing article topics
                return self._fallback_trending_analysis(articles)
                
        except Exception as e:
            logger.error(f"Error in AI trending topics analysis: {e}")
            return self._fallback_trending_analysis(articles)
    
    def _find_related_articles(self, articles: List[Dict[str, Any]], topic_name: str) -> List[Dict[str, Any]]:
        """
        Find articles related to a trending topic by keyword matching.
        """
        related = []
        topic_keywords = topic_name.lower().split()
        
        for article in articles:
            # Check title, summary, and description for keyword matches
            content_to_check = []
            if article.get('title'):
                content_to_check.append(article['title'].lower())
            if article.get('summary'):
                content_to_check.append(article['summary'].lower())
            if article.get('description'):
                content_to_check.append(article['description'].lower())
            
            content_text = " ".join(content_to_check)
            
            # Count keyword matches
            match_count = sum(1 for keyword in topic_keywords if keyword in content_text)
            
            if match_count >= max(1, len(topic_keywords) // 2):  # At least half the keywords match
                related.append({
                    'article_id': article['article_id'],
                    'title': article.get('title', 'Untitled'),
                    'url': article['url'],
                    'publication_date': article.get('publication_date'),
                    'match_score': match_count
                })
        
        # Sort by match score and publication date
        related.sort(key=lambda x: (x['match_score'], x['publication_date'] or ''), reverse=True)
        return related
    
    def _fallback_trending_analysis(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Fallback method to identify trending topics when AI analysis fails.
        """
        # Extract common words/phrases from titles and summaries
        all_text = []
        for article in articles:
            if article.get('title'):
                all_text.append(article['title'])
            if article.get('summary'):
                all_text.append(article['summary'][:100])  # First 100 chars
        
        combined_text = " ".join(all_text).lower()
        
        # Remove common stop words and extract meaningful phrases
        stop_words = {'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'a', 'an'}
        words = re.findall(r'\b[a-zA-Z]{3,}\b', combined_text)
        meaningful_words = [w for w in words if w not in stop_words]
        
        # Count word frequency
        word_counts = Counter(meaningful_words)
        common_words = word_counts.most_common(20)
        
        # Create trending topics from most common words
        trending_topics = []
        for word, count in common_words[:10]:
            if count >= 2:  # Must appear at least twice
                related_articles = self._find_related_articles(articles, word)
                
                trending_topics.append({
                    'name': word.title(),
                    'explanation': f"This topic appears frequently ({count} times) in recent articles.",
                    'insights': f"Based on article analysis, {word} is mentioned across {len(related_articles)} articles.",
                    'related_article_count': len(related_articles),
                    'related_articles': related_articles[:5]
                })
        
        return trending_topics