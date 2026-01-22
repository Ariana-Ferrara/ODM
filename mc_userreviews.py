import scrapy
from scrapy.spiders import CrawlSpider
from MetacriticNEW.items import UserReviewItem
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

class UserReviewsSpider(CrawlSpider):
    name = "mc_user_reviews"
    allowed_domains = ["metacritic.com"] 
    start_urls = ["https://www.metacritic.com/browse/movie/"]

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.google.com/',
        },
        'FEEDS': {
            'user_reviews_dataMC.csv': {
                'format': 'csv',
                'item_classes': ['MetacriticNEW.items.UserReviewItem'],
                'overwrite': True,
                'encoding': 'utf8',
            },
        }
    }

    def start_requests(self):
        # Generate requests for pages 1-59
        # Selenium used via middleware for browser interaction
        for page in range(1, 60):
            url = f"https://www.metacritic.com/browse/movie/?releaseYearMin=2023&releaseYearMax=2025&page={page}"
            yield scrapy.Request(
                url,
                callback=self.parse_listing,
                dont_filter=True
            )

    def parse_listing(self, response):
        # Selenium driver available for JavaScript interactions
        driver = response.meta.get('driver')
        
        movie_links = response.css('a[href*="/movie/"]::attr(href)').getall()
        
        for link in movie_links:
            if '/movie/' in link and '/browse/' not in link:
                # Extract movie_id from link
                url_parts = link.split('/')
                movie_id = [part for part in url_parts if part][-1]
                
                # Convert to user-reviews URL
                user_reviews_url = f"https://www.metacritic.com/movie/{movie_id}/user-reviews/"
                
                # Pass movie_id in meta for use in parse_reviews callback
                yield scrapy.Request(
                    user_reviews_url,
                    callback=self.parse_reviews,
                    meta={'movie_id': movie_id},
                    dont_filter=True
                )

    def parse_reviews(self, response):
        # Selenium driver used for clicking "Read More" buttons on spoiler reviews
        driver = response.meta.get('driver')
        movie_id = response.meta['movie_id']
        
        # Get all review containers
        review_containers = response.css('div.c-siteReview')
        
        # Limit to 20 reviews maximum
        review_containers = review_containers[:20]
        
        for index, review in enumerate(review_containers):
            item = UserReviewItem()
            
            # Movie ID
            item['movie_id'] = movie_id
            
            # Username
            username = review.css("a.c-siteReviewHeader_username::text").get()
            item['user_name'] = username.strip() if username else None
            
            # User score
            item['user_score'] = review.css("div.c-siteReviewScore_user span::text").get()
            
            # Review date
            review_date = review.css("div.c-siteReview_reviewDate::text").get()
            item['review_date'] = review_date.strip() if review_date else None
            
            # Review text
            # Check if spoiler alert present
            review_text = review.css("div.c-siteReview_quote span::text").getall()
            
            # Spoiler reviews require clicking "Read More" button with Selenium
            # Cannot get spoiler text from regular HTML
            if review_text and '[SPOILER ALERT: This review contains spoilers.]' in ''.join(review_text):
                # Check if driver is available
                if driver is None:
                    item['review_text'] = None
                else:
                    try:
                        # Find all "Read More" buttons using Selenium
                        # Use normalize-space to handle whitespace around text
                        read_more_buttons = driver.find_elements(By.XPATH, '//button[contains(normalize-space(.), "Read More")]')
                        
                        if index < len(read_more_buttons):
                            # Click Read More button for this review
                            read_more_buttons[index].click()
                            
                            # Wait for modal to appear (up to 3 seconds)
                            wait = WebDriverWait(driver, 3)
                            modal = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.c-siteReviewReadMore_content')))
                            
                            # Extract text from modal
                            modal_text_element = driver.find_element(By.CSS_SELECTOR, 'div.c-siteReviewReadMore_wrapper')
                            modal_text = modal_text_element.text
                            
                            # Clean up newlines and extra whitespace
                            if modal_text:
                                item['review_text'] = ' '.join(modal_text.split())
                            else:
                                item['review_text'] = None
                            
                            # Close modal by pressing Escape key
                            from selenium.webdriver.common.keys import Keys
                            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                            time.sleep(0.5)
                        else:
                            item['review_text'] = None
                    except Exception as e:
                        item['review_text'] = None
            else:
                # Regular review - text already in HTML
                # No Selenium needed
                cleaned_text = ' '.join([text.strip() for text in review_text if text.strip()])
                # Remove extra newlines and whitespace
                item['review_text'] = ' '.join(cleaned_text.split()) if cleaned_text else None
            
            yield item