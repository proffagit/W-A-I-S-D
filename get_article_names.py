#!/usr/bin/env python3
"""
Wikipedia Article Scraper

This script scrapes all Wikipedia article names from the Special:AllPages page
and saves them to a SQLite database using BeautifulSoup.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import logging
from urllib.parse import urljoin, parse_qs, urlparse
import sys
from typing import List, Optional
from funcs import create_sqlite_db


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class WikipediaScraper:
    """Scraper for Wikipedia article names from Special:AllPages"""
    
    def __init__(self, db_name: str = "wikipedia_articles.db", delay: float = 1.0):
        """
        Initialize the scraper
        
        Args:
            db_name: Name of the SQLite database file
            delay: Delay between requests in seconds (be respectful to Wikipedia)
        """
        self.db_name = db_name
        self.delay = delay
        self.base_url = "https://en.wikipedia.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Wikipedia Article Scraper (Educational Purpose)'
        })
        
        # Initialize database
        self._setup_database()
        
    def _setup_database(self):
        """Setup the SQLite database with required tables"""
        table_schema = {
            'articles': {
                'id': {'type': 'INTEGER', 'primary_key': True, 'not_null': True},
                'title': {'type': 'TEXT', 'not_null': True, 'unique': True}
            }
        }
        
        success = create_sqlite_db(self.db_name, table_schema)
        if not success:
            raise Exception("Failed to setup database")
        
        logger.info(f"Database '{self.db_name}' setup completed")
    
    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch a page and return BeautifulSoup object
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Add delay to be respectful
            time.sleep(self.delay)
            
            return BeautifulSoup(response.content, 'html.parser')
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
    
    def _extract_articles_from_page(self, soup: BeautifulSoup) -> List[dict]:
        """
        Extract article names and URLs from a Special:AllPages page
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of dictionaries with article info
        """
        articles = []
        
        # Find the content div containing the article list
        content_div = soup.find('div', {'class': 'mw-allpages-body'})
        if not content_div:
            logger.warning("Could not find article list container")
            return articles
        
        # Find all article links
        article_links = content_div.find_all('a')
        
        for link in article_links:
            title = link.get('title')
            href = link.get('href')
            
            if title and href and href.startswith('/wiki/'):
                articles.append({
                    'title': title
                })
        
        logger.info(f"Extracted {len(articles)} articles from page")
        return articles
    
    def _find_next_page_url(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Find the URL for the next page of results
        
        Args:
            soup: BeautifulSoup object of the current page
            
        Returns:
            URL of next page or None if no next page
        """
        # Look for "Next page" link
        next_links = soup.find_all('a', string=lambda text: text and 'next' in text.lower())
        
        for link in next_links:
            href = link.get('href')
            if href and 'Special:AllPages' in href:
                return urljoin(self.base_url, href)
        
        # Alternative: look for navigation links
        nav_div = soup.find('div', {'class': 'mw-allpages-nav'})
        if nav_div:
            links = nav_div.find_all('a')
            for link in links:
                if 'next' in link.get_text().lower():
                    href = link.get('href')
                    if href:
                        return urljoin(self.base_url, href)
        
        return None
    
    def _save_articles_to_db(self, articles: List[dict]):
        """
        Save articles to the database
        
        Args:
            articles: List of article dictionaries
        """
        if not articles:
            return
        
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            # Insert articles (ignore duplicates)
            cursor.executemany(
                "INSERT OR IGNORE INTO articles (title) VALUES (?)",
                [(article['title'],) for article in articles]
            )
            
            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(f"Saved {rows_affected} new articles to database")
            
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
    
    def get_total_articles_count(self) -> int:
        """Get the total number of articles in the database"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM articles")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except sqlite3.Error:
            return 0
    
    def get_last_article_title(self) -> Optional[str]:
        """Get the last article title from the database (alphabetically)"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT title FROM articles ORDER BY title DESC LIMIT 1")
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else None
        except sqlite3.Error:
            return None
    
    def create_resume_url(self, last_title: str) -> str:
        """Create a resume URL from the last article title"""
        from urllib.parse import quote_plus
        # URL encode the title for use in the from parameter
        encoded_title = quote_plus(last_title)
        return f"https://en.wikipedia.org/w/index.php?title=Special:AllPages&from={encoded_title}"
    
    def scrape_all_articles(self, start_url: str = None, max_pages: int = None):
        """
        Scrape all Wikipedia articles starting from the given URL
        
        Args:
            start_url: Starting URL (default: Special:AllPages from '!')
            max_pages: Maximum number of pages to scrape (None for all pages)
        """
        if start_url is None:
            start_url = "https://en.wikipedia.org/w/index.php?title=Special:AllPages&from=%21"
        
        current_url = start_url
        pages_scraped = 0
        total_articles = 0
        
        logger.info(f"Starting Wikipedia article scraping from: {start_url}")
        
        while current_url:
            if max_pages and pages_scraped >= max_pages:
                logger.info(f"Reached maximum page limit: {max_pages}")
                break
            
            soup = self._get_page(current_url)
            if not soup:
                logger.error(f"Failed to fetch page, stopping scraping")
                break
            
            # Extract articles from current page
            articles = self._extract_articles_from_page(soup)
            if not articles:
                logger.warning("No articles found on this page, stopping")
                break
            
            # Save articles to database
            self._save_articles_to_db(articles)
            
            pages_scraped += 1
            total_articles += len(articles)
            
            # Get current count from database
            db_count = self.get_total_articles_count()
            logger.info(f"Page {pages_scraped} completed. Total articles in DB: {db_count}")
            
            # Find next page
            current_url = self._find_next_page_url(soup)
            if not current_url:
                logger.info("No more pages found, scraping completed")
                break
        
        final_count = self.get_total_articles_count()
        logger.info(f"Scraping completed! Total pages scraped: {pages_scraped}")
        logger.info(f"Total articles in database: {final_count}")


def main():
    """Main function to run the scraper"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Wikipedia Article Scraper')
    parser.add_argument('--start-from', type=str, help='Starting page/article name to scrape from (e.g., "2004DW")')
    parser.add_argument('--resume', action='store_true', help='Automatically resume from the last article in database')
    parser.add_argument('--max-pages', type=int, help='Maximum number of pages to scrape')
    args = parser.parse_args()
    
    try:
        # Create scraper instance
        scraper = WikipediaScraper(db_name="wikipedia_articles.db", delay=1.0)
        
        # Check if we already have articles
        existing_count = scraper.get_total_articles_count()
        if existing_count > 0:
            logger.info(f"Found {existing_count} existing articles in database")
            
            # Get the last article title
            last_title = scraper.get_last_article_title()
            if last_title:
                logger.info(f"Last article in database: '{last_title}'")
        
        # Determine starting URL
        start_url = None
        if args.start_from:
            start_url = f"https://en.wikipedia.org/w/index.php?title=Special:AllPages&from={args.start_from}"
            logger.info(f"Starting from article: {args.start_from}")
        elif args.resume and existing_count > 0:
            last_title = scraper.get_last_article_title()
            if last_title:
                start_url = scraper.create_resume_url(last_title)
                logger.info(f"Resuming from last article: '{last_title}'")
            else:
                logger.warning("Could not get last article title, starting from beginning")
        else:
            if existing_count > 0:
                response = input("Continue scraping from where we left off? (y/n): ").lower()
                if response == 'y':
                    last_title = scraper.get_last_article_title()
                    if last_title:
                        start_url = scraper.create_resume_url(last_title)
                        logger.info(f"Resuming from last article: '{last_title}'")
                elif response != 'n':
                    logger.info("Scraping cancelled by user")
                    return
        
        # Start scraping all pages
        logger.info("Starting Wikipedia article scraping...")
        logger.info("This will scrape Wikipedia articles. Press Ctrl+C to stop.")
        
        scraper.scrape_all_articles(start_url=start_url, max_pages=args.max_pages)
        
        # Show final statistics
        total_count = scraper.get_total_articles_count()
        print(f"\n=== Scraping Summary ===")
        print(f"Total articles saved: {total_count}")
        print(f"Database file: {scraper.db_name}")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()