import logging
from typing import List, Dict, Any, Optional
from collections import Counter
import re

from ..database.database import Database
from ..llm.databricks_client import DatabricksLLMClient
from ..database.models import Theme

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
        - ai_trending_topics: AI-identified trending themes with shared messages
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
        
        # Analyze content for AI-powered trending themes
        ai_trending_topics = await self._identify_ai_trending_themes(articles)
        
        results = {
            'top_topics': top_topics,
            'top_companies': top_companies, 
            'ai_trending_topics': ai_trending_topics,
            'article_count': len(articles),
            'days': days
        }
        
        # Save results to database
        try:
            report_id = self.db.save_trending_report(days, len(articles), results)
            results['report_id'] = report_id
            logger.info(f"Saved trending analysis to database with report ID {report_id}")
            
            # Store themes and their article associations
            logger.info(f"About to store {len(ai_trending_topics)} themes to database")
            self._store_themes_to_database(ai_trending_topics, report_id, articles)
            
        except Exception as e:
            logger.error(f"Failed to save trending analysis to database: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        return results
    
    async def _identify_ai_trending_themes(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Use AI to identify trending themes - shared messages and arguments from article content.
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
            Analyze the following news articles from the last few days and identify the top 8-10 recurring THEMES - not just topics, but shared messages, arguments, and narratives that articles are trying to convey.

            Look for:
            - Common arguments or viewpoints being made across multiple articles
            - Shared concerns, warnings, or predictions
            - Similar conclusions or recommendations
            - Recurring narratives about cause and effect
            - Consistent messaging about implications or consequences

            For each theme, provide:
            1. Theme name (3-6 words describing the message/argument)
            2. What this theme represents - the core message or argument being made
            3. Key messages and arguments - what the articles are specifically saying about this theme

            Articles:
            {combined_content}
            
            IMPORTANT: Return ONLY valid JSON in the exact format below. Do not include any markdown formatting, code blocks, or additional text:

            {{
                "trending_topics": [
                    {{
                        "name": "Theme Name",
                        "explanation": "What this theme represents and the core message...",
                        "insights": "Specific arguments and messages the articles are making..."
                    }}
                ]
            }}
            """
            
            # Get AI analysis
            response = await self.llm_client.generate_response(prompt, max_tokens=1500)
            
            # Log the raw response for debugging
            logger.info(f"AI response for theme analysis: {response[:500]}...")
            
            # Check for empty response
            if not response or not response.strip():
                logger.warning("AI returned empty response for theme analysis")
                return self._fallback_trending_analysis(articles)
            
            # Clean the response in case it's wrapped in markdown code blocks
            cleaned_response = response.strip()
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response[7:]  # Remove ```json
            if cleaned_response.startswith('```'):
                cleaned_response = cleaned_response[3:]   # Remove ```
            if cleaned_response.endswith('```'):
                cleaned_response = cleaned_response[:-3]  # Remove trailing ```
            cleaned_response = cleaned_response.strip()
            
            # Parse JSON response
            import json
            try:
                parsed_response = json.loads(cleaned_response)
                trending_topics = parsed_response.get('trending_topics', [])
                
                if not trending_topics:
                    logger.warning("AI returned valid JSON but no trending topics found")
                    return self._fallback_trending_analysis(articles)
                
                # Add article references for each trending theme and store in database
                enhanced_topics = []
                for topic in trending_topics[:10]:  # Limit to top 10
                    # Find articles related to this theme
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
                logger.error(f"Failed to parse AI trending themes response: {e}")
                logger.error(f"Raw response was: {response}")
                logger.error(f"Cleaned response was: {cleaned_response}")
                # Fallback: extract themes from existing article content
                return self._fallback_trending_analysis(articles)
                
        except Exception as e:
            logger.error(f"Error in AI trending themes analysis: {e}")
            return self._fallback_trending_analysis(articles)
    
    def _find_related_articles(self, articles: List[Dict[str, Any]], theme_name: str) -> List[Dict[str, Any]]:
        """
        Find articles related to a trending theme by keyword matching.
        """
        related = []
        theme_keywords = theme_name.lower().split()
        
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
            match_count = sum(1 for keyword in theme_keywords if keyword in content_text)
            
            if match_count >= max(1, len(theme_keywords) // 2):  # At least half the keywords match
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
        Fallback method to identify trending themes when AI analysis fails.
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
        
        # Create trending themes from most common words
        trending_themes = []
        for word, count in common_words[:10]:
            if count >= 2:  # Must appear at least twice
                related_articles = self._find_related_articles(articles, word)
                
                trending_themes.append({
                    'name': word.title(),
                    'explanation': f"This theme appears frequently ({count} times) in recent articles as a recurring message.",
                    'insights': f"Based on article analysis, {word} represents a common narrative across {len(related_articles)} articles.",
                    'related_article_count': len(related_articles),
                    'related_articles': related_articles[:5]
                })
        
        return trending_themes
    
    def _store_themes_to_database(self, themes: List[Dict[str, Any]], report_id: int, articles: List[Dict[str, Any]]):
        """Store themes and their article associations to the database."""
        try:
            logger.info(f"Storing {len(themes)} themes to database for report {report_id}")
            for theme_data in themes:
                # Check if theme already exists for this report
                existing_theme = self.db.get_theme_by_name_and_report(theme_data['name'], report_id)
                
                if existing_theme:
                    theme_id = existing_theme['theme_id']
                    # Clear existing associations for refresh
                    self.db.clear_theme_articles(theme_id)
                else:
                    # Create new theme
                    theme = Theme(
                        name=theme_data['name'],
                        explanation=theme_data['explanation'],
                        insights=theme_data['insights'],
                        report_id=report_id
                    )
                    theme_id = self.db.add_theme(theme)
                
                # Find and link related articles
                related_articles = self._find_related_articles(articles, theme_data['name'])
                for article in related_articles:
                    self.db.link_article_theme(
                        article['article_id'], 
                        theme_id, 
                        article.get('match_score', 1.0)
                    )
                
                logger.info(f"Stored theme '{theme_data['name']}' with {len(related_articles)} related articles")
                
        except Exception as e:
            logger.error(f"Error storing themes to database: {e}")