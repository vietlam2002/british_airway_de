import os
from pathlib import Path
import logging
import pandas as pd
import requests
import time, random
import json
from typing import Dict, List
from bs4 import BeautifulSoup

# Config Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class AirlineReviewScraper:
    """
    Extracts review data from British Airways reviews on AirlineQuality.com.

    Args:
        number_of_pages (int): The number of pages to scrape.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted review data.
    """

    BASE_URL = "https://www.airlinequality.com/airline-reviews/british-airways"
    PAGE_SIZE = 100

    def __init__(self, num_pages: int, output_path: str = "/opt/airflow/data/raw_data.csv"):
        self.num_pages = num_pages
        self.output_path = output_path
        self.reviews_data = []
        self.session = requests.Session()
        
    def scrape(self) -> pd.DataFrame:
        # More diverse and realistic user agents
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.58 Safari/537.36 Edg/123.0.2420.65",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/108.0.0.0"
        ]
        
        # More realistic referers
        referers = [
            "https://www.google.com/search?q=british+airways+reviews",
            "https://www.bing.com/search?q=airline+quality+british+airways",
            "https://duckduckgo.com/?q=british+airways+passenger+reviews",
            "https://www.airlinequality.com/",
            "https://www.airlinequality.com/airline-reviews/",
            None
        ]
        
        # Track any failed pages for retry
        failed_pages = []
        
        # First attempt for all pages
        for page in range(1, self.num_pages + 1):
            if not self._scrape_page(page, user_agents, referers):
                failed_pages.append(page)
            
            # More human-like pauses between requests
            time.sleep(random.uniform(3.0, 15.0))
        
        # Retry failed pages with different approach
        if failed_pages:
            logging.info(f"Retrying {len(failed_pages)} failed pages with different approach")
            for page in failed_pages:
                # Wait longer before retry
                time.sleep(random.uniform(15.0, 25.0))
                
                # Use a completely different user agent and approach
                self._scrape_page(page, user_agents, referers, retry=True)
        
        # Save scraped data
        if self.reviews_data:
            df = pd.DataFrame(self.reviews_data)
            self.save_to_csv(df)
            return df
        else:
            logging.error("No review data was collected.")
            return pd.DataFrame()
    
    def _scrape_page(self, page: int, user_agents: List[str], referers: List[str], retry: bool = False) -> bool:
        """
        Scrapes a single page with anti-blocking measures
        
        Args:
            page (int): Page number to scrape
            user_agents (List[str]): List of user agents to choose from
            referers (List[str]): List of referers to choose from
            retry (bool): Whether this is a retry attempt
            
        Returns:
            bool: True if scraping was successful, False otherwise
        """
        # Select a random user agent and referer
        user_agent = random.choice(user_agents)
        referer = random.choice(referers)
        
        # Custom headers with more browser-like values
        headers = {
            "User-Agent": user_agent,
            "Accept-Language": f"en-US,en;q=0.{random.randint(7, 9)}",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none" if not referer else "cross-site",
            "Sec-Fetch-User": "?1",
            "DNT": "1"
        }
        
        if referer:
            headers["Referer"] = referer
            
        url = f"{self.BASE_URL}/page/{page}/?sortby=post_date%3ADesc&pagesize={self.PAGE_SIZE}"
        
        if retry:
            # On retry, use a slightly different URL format or query parameters
            url = f"{self.BASE_URL}/page/{page}/?pagesize={self.PAGE_SIZE}"
            
            # Clear cookies before retry
            self.session = requests.Session()
        
        # Log with reduced detail to avoid detection patterns
        logging.info(f"Processing page {page}")
        
        try:
            # Use session for cookies consistency
            response = self.session.get(
                url, 
                headers=headers, 
                timeout=20,
                allow_redirects=True
            )
            
            if response.status_code != 200:
                logging.error(f"Failed to fetch page {page}: {response.status_code}")
                return False
                
            # Verify we got meaningful content
            if len(response.content) < 5000:  # Very small response usually means blocked
                logging.error(f"Page {page} returned suspicious small content ({len(response.content)} bytes)")
                return False
                
            soup = BeautifulSoup(response.content, "html.parser")
            reviews = soup.select('article[class*="comp_media-review-rated"]')
            
            if not reviews:
                logging.error(f"No reviews found on page {page}")
                return False
                
            logging.info(f"Found {len(reviews)} reviews on page {page}")
            
            for review in reviews:
                review_data = self.extract_review_data(review)
                if review_data:
                    self.reviews_data.append(review_data)
                    
            return True
            
        except requests.RequestException as e:
            logging.error(f"Request error on page {page}: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error processing page {page}: {e}")
            return False
    
    def extract_review_data(self, review: BeautifulSoup) -> Dict[str, str]:
        """
        Extracts relevant data from a single review.

        Args:
            review (BeautifulSoup): The parsed HTML of a single review.

        Returns:
            Dict[str, str]: A dictionary containing extracted review data.
        """
        review_data = {
            "date": self.extract_text(review, "time", itemprop="datePublished"),
            "customer_name": self.extract_text(review, "span", itemprop="name"),
            "country": self.extract_country(review),
            "review_body": self.extract_text(review, "div", itemprop="reviewBody")
        }

        self.extract_ratings(review, review_data)
        return review_data

    def extract_text(self, element: BeautifulSoup, tag: str, **attrs) -> str:
        """
        Extracts text from a BeautifulSoup element.

        Args:
            element (BeautifulSoup): The BeautifulSoup element to search within.
            tag (str): The HTML tag to look for.
            **attrs: Additional attributes to filter the search.

        Returns:
            str: The extracted text, or None if not found.
        """
        found = element.find(tag, attrs)
        return found.text.strip() if found else None

    def extract_country(self, review: BeautifulSoup) -> str:
        """
        Extracts the country from a review.

        Args:
            review (BeautifulSoup): The parsed HTML of a single review.

        Returns:
            str: The extracted country, or None if not found.
        """
        country = review.find(string=lambda text: text and "(" in text and ")" in text)
        return country.strip("()") if country else None

    def extract_ratings(self, review: BeautifulSoup, review_data: Dict[str, str]) -> None:
        """
        Extracts ratings from a review and adds them to the review_data dictionary.

        Args:
            review (BeautifulSoup): The parsed HTML of a single review.
            review_data (Dict[str, str]): The dictionary to update with extracted ratings.
        """
        ratings_table = review.find("table", class_="review-ratings")
        if not ratings_table:
            return

        for row in ratings_table.find_all("tr"):
            header = row.find("td", class_="review-rating-header")
            if not header:
                continue

            header_text = header.text.strip()
            stars_td = row.find("td", class_="review-rating-stars")

            if stars_td:
                stars = stars_td.find_all("span", class_="star fill")
                review_data[header_text] = len(stars)
            else:
                value_td = row.find("td", class_="review-value")
                if value_td:
                    review_data[header_text] = value_td.text.strip()

    def save_to_csv(self, df: pd.DataFrame) -> None:
        """
        Saves a DataFrame to a CSV file.

        Args:
            df (pd.DataFrame): The DataFrame to save.
            file_path (str): The path where the CSV file will be saved.
        """
        # Create robust data saving with fallback
        # try:
        #     # Save data incrementally to prevent complete loss if interrupted
        #     os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        #     df.to_csv(self.output_path, index=False)
        #     logging.info(f"Data saved to {self.output_path}")
        # except Exception as e:
        #     logging.error(f"Error saving to primary path: {e}")
        try:
            # Also save locally as a backup
            project_root = Path(__file__).resolve().parents[3]
            data_dir = project_root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            
            local_path = data_dir / "raw_data.csv"
            df.to_csv(local_path, index=False)
            logging.info(f"Data saved to {local_path}")
            
            # Also save a backup with timestamp to prevent data loss
            # backup_path = data_dir / f"raw_data_backup_{int(time.time())}.csv"
            # df.to_csv(backup_path, index=False)
            # logging.info(f"Backup saved to {backup_path}")
        except Exception as e:
            logging.error(f"Error saving to local path: {e}")

if __name__ == "__main__":
    # More robust execution with error handling and recovery
    try:
        # Start with fewer pages for initial run
        scraper = AirlineReviewScraper(num_pages=30)
        df = scraper.scrape()
        
        if df.empty:
            logging.warning("No data was collected in the first attempt. Retrying with reduced scope.")
            # If first attempt fails completely, try again with reduced scope
            time.sleep(60)  # Wait a minute before retry
            scraper = AirlineReviewScraper(num_pages=15)
            scraper.scrape()
    except Exception as e:
        logging.critical(f"Critical error in scraper execution: {e}")
        # Still try to save any collected data before exiting
        if hasattr(scraper, 'reviews_data') and scraper.reviews_data:
            try:
                df = pd.DataFrame(scraper.reviews_data)
                scraper.save_to_csv(df)
                logging.info(f"Saved {len(scraper.reviews_data)} reviews despite errors")
            except Exception as save_error:
                logging.critical(f"Failed to save partial data: {save_error}")