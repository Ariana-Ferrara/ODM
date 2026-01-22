import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ODM_Group1.items import MovieItem, ReviewItem
from seleniumwire import webdriver as wire_webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapy.http import HtmlResponse
import time
import re
from scrapy.exceptions import DropItem, CloseSpider
import json
from datetime import date
from calendar import monthrange

#Chris
#Since the url has too many movies (21.000), and imdb doesn't allow endless scroling, we implemented a url builder, 
#that devides the year into 12 url's so the scraper can scrape everything.
def build_imdb_urls(year):
    urls = []

    for month in range(1, 13):               #Here we define the builder based on the imported calendar
        last_day = monthrange(year, month)[1]
        start_date = date(year, month, 1).isoformat()
        end_date = date(year, month, last_day).isoformat()

        for start in range(1, 1000, 50):
            urls.append(
                f"https://www.imdb.com/search/title/"     #Main url
                f"?title_type=feature"
                f"&release_date={start_date},{end_date}"  #Here it builds the 12 different urls like batch sizes
                f"&start={start}"
            )

    return urls

#chris and ariana
class CrawlingSpider(scrapy.Spider):  #create a custom class (CrawlingSpider) which inherits all the automated navigation features of Scrapy's built-in CrawlSpider
    name = "mycrawler"  #unique name used to run the spider in the terminal
    allowed_domains = ["www.imdb.com"]  #prevents the spider from accidentally wandering off to external websites

    #Ariana
    def __init__(self, *args, **kwargs):
        super(CrawlingSpider, self).__init__(*args, **kwargs)
        self.yielded_ids = set()
        self.movie_count = 0  # Fixed variable name
        self.max_movies = 20000 

    #Chris
    def start_requests(self):
        for url in build_imdb_urls(2023):   #Starts the request based on the urls that we build before
            yield scrapy.Request(
                url=url,
                callback=self.parse_search_results,   #The results will be pushed to pars search results, wich outputs it in the file you want
                dont_filter=True
            )

    #Chris
    def parse_search_results(self, response):
        driver = response.meta.get('driver') 
        
        if not driver:
            self.logger.error("!!! CRITICAL: Driver still not found in meta !!!")   #if the middleware is working, this will NOT be None
            return

        for i in range(5):       #here we tell the driver to click on the button 'get 50 more movies' with selenium 
            try:
                btn = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.ipc-see-more__button")) #Specify the button
                )
                driver.execute_script("arguments[0].click();", btn)
                self.logger.info(f"Loaded batch {i+1}")
                time.sleep(7)     #After clicking do nothing for 7 seconds, to prevent blocking from imdb 
            except:
                break
    
        html = driver.page_source   #Gets the full html sourse code from the selenium browser

        response = HtmlResponse(   #Creates a scrapy friendly response
            url=driver.current_url,  #Gets the web adress browser 
            body=html,     #Uses the html sourse from above 
            encoding='utf-8'   #Sets the text format, so that also special characters will be shown correctly
        )

        for href in response.css('a.ipc-title-link-wrapper::attr(href)').getall():   #Now, we extract the movie links to scrape 
            yield response.follow(href, callback=self.parse_item)   #For every link found, we scrape that page
        
    #chris and ariana
    rules = (
        Rule(LinkExtractor(
            allow=r'/title/tt\d+/',
            deny=(r'/technical', r'/companycredits', r'/fullcredits', r'/parentalguide')
        ), callback="parse_item", follow=False),
        
        Rule(LinkExtractor(
            allow=r'/search/title/',
            restrict_css='a.ipc-title-link-wrapper, a.next-page, section div.ipc-page-grid a, span.ipc-see-more-button',
            tags=('a', 'span', 'button'), 
            attrs=('href', 'data-testid')  
        ), follow=True),
        )

#In the next comments, we commented out the custom settings. We did this, because we saw that the custom setting were overwrighting the
#settings itself. But we keep them in case we might need them in the future:

    #chris
    # custom_settings = {
    #     # REMOVED THE PIPELINE - testing if that's the issue
    #     'DOWNLOAD_DELAY': 3,       #Adds a 3-second wait between every request to mimic human browsing behavior and prevent the server from getting overwhelmed
    #     'AUTOTHROTTLE_ENABLED': True,   #Automatically adjusts the crawling speed based on how fast the IMDb server is responding
    #     'CONCURRENT_REQUESTS_PER_DOMAIN': 1,  #forces the spider to finish downloading one page from IMDb before starting the next to avoid triggering IMDb's anti-bot alarms
    #     'AUTOTHROTTLE_START_DELAY': 5,  #When the spider first starts, it will wait 5 seconds for its very first request before letting the AUTOTHROTTLE logic take over and adjust the speed based on server health
    #     'AUTOTHROTTLE_TARGET_CONCURRENCY': 2.0,
    #     'RANDOMIZE_DOWNLOAD_DELAY': True,
    #     'ROBOTSTXT_OBEY': True,      
    #     'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',   #Disguises the spider as a standard Chrome browser on Windows to avoid being flagged as a bot
    #     'REDIRECT_ENABLED': False,      #Set to False to prevent the spider from following automatic redirects, which leads our spideraway from the movie data we want
    #     'DEFAULT_REQUEST_HEADERS': {    #Sends additional technical info (like language and referrer) with every request to make the bot appear more like a legitimate user coming from Google
    #         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    #         'Accept-Language': 'en-US,en;q=0.9',
    #         'Referer': 'https://www.google.com/',
    #     },
    #     #This section specifically targets how Scrapy handles page redirections. We set the standard RedirectMiddleware and MetaRefreshMiddleware to None, disbaling them
    #     #our meta data proxy is still in effect
    #     'DOWNLOADER_MIDDLEWARES': {
    #         'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,
    #         'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': None,
    #     },
    #     'METAREFRESH_ENABLED': False,   #Disables Scrapy's ability to follow "Meta Refresh" tags to keep the spider from wandering off to irrelevant pages
    #     'HTTPERROR_ALLOWED_CODES': [301, 302],  #tells Scrapy to log these codes rather than treat them as a fatal crash
    # }
    
    #ariana   
    def parse_item(self, response):
        # Ensure this matches the name in __init__
        if self.movie_count >= self.max_movies:
            raise CloseSpider(reason=f"Reached {self.max_movies} movies")
        
        
        #ariana
        # Primary Key extraction: Identifies the unique IMDb ID from the URL (e.g.,'tt32642706') to use as a unique identifier in the relational database
        movie_id_match = re.search(r'tt\d+', response.url)
        movie_id = movie_id_match.group() if movie_id_match else None
        
        #ariana
        # duplication check: Check the current session's memory (self.yielded_ids) to ensure this specific movie hasn't already been processed
        if movie_id and movie_id in self.yielded_ids:
            self.logger.info(f"Skipping duplicate in current session: {movie_id}")
            return
        
        #ariana
        # Add the new ID to our session set immediately to ensure any subsequent links to this movie are ignored during this run
        if movie_id:
            self.yielded_ids.add(movie_id)
        
        #chris and ariana
        # initialize the item and start filling it
        item = MovieItem()

        #chris
        # --- movieID ---
        if movie_id:
            item['movie_id'] = movie_id
        
        #chris
        #------ movie title ------
        item['title'] = response.css('[data-testid="hero__primary-text"]::text').get()
        
        #ariana
        #------ movie director ------
        item['director'] = response.css('a.ipc-metadata-list-item__list-content-item--link::text').get()

        #ariana
        #------ movie writer ------
        writers_raw = response.css('li[data-testid="title-pc-principal-credit"]:contains("Writer") a.ipc-metadata-list-item__list-content-item--link::text').getall()
        item['writer'] = list(set(writers_raw)) if writers_raw else None
        
        #chris
        #------ release date ------
        raw_date = response.css('[data-testid="title-details-releasedate"] a.ipc-metadata-list-item__list-content-item--link::text').get()
        if raw_date:
            item["release_date"] = raw_date.split('(')[0].strip() # This splits the string at the first "(" and takes the part before it
        
        #chris
        #------ movie duration ------
        item["duration"] = response.css('[data-testid="title-techspec_runtime"] .ipc-metadata-list-item__list-content-item::text').get()
        
        #ariana
        #------ production company ------
        companies = response.css('[data-testid="title-details-companies"] li a::text').getall()
        item['production_company'] = companies  # Yields: ['Fox 2000', 'Regency']

        #ariana
        #------ genre ------
        # Extracted from JSON-LD to prevent empty results; the standard CSS selectors failed here as IMDb loads this specific data dynamically via scripts
        json_text = response.xpath('//script[@type="application/ld+json"]/text()').get()    #grab XPath to locate a <script> tag on the IMDb page that contains structured metadata in JSON format
        if json_text:
            data = json.loads(json_text)    #convert the raw string into a Python dictionary
            item['genres'] = data.get('genre', [])  #This looks for the genre key in the JSON data; if it exists, it saves the list to your MovieItem, otherwise it returns an empty list [] to prevent errors.
            rating_data = data.get('aggregateRating', {})   #Inside the main JSON, there is a sub-dictionary called aggregateRating which holds the score details; this line extracts that specific section.
            item['rating'] = rating_data.get('ratingValue') #pulls the actual numerical score from the aggregateRating section
        else:
            item['genres'] = []  #If no JSON found, initialize the genres field as an empty list
    
        #ariana
        #------ character names ------
        roles_raw = response.css('div[class*="characters-list"] span::text').getall()
        item['roles'] = list(set(roles_raw)) if roles_raw else []

        #ariana
        #------ actor names ------
        stars_raw = response.css('a[data-testid="title-cast-item__actor"]::text').getall()
        item['stars'] = list(set(stars_raw)) if stars_raw else []

        #chris
        #------ Sales information ------
        item['budget'] = response.css('[data-testid="title-boxoffice-budget"] .ipc-metadata-list-item__list-content-item::text').get()
        item['grossworldwide'] = response.css('[data-testid="title-boxoffice-cumulativeworldwidegross"] .ipc-metadata-list-item__list-content-item::text').get()
        item['openingweekend'] = response.css('[data-testid="title-boxoffice-openingweekenddomestic"] .ipc-metadata-list-item__list-content-item::text').get()

        #ariana
        #Now that were finished with the Movie table, update the counter and yield the MovieItem immediately
        self.movie_count += 1
        yield item

        # Now move to the reviews table: trigger the review scraping for this specific movie
        reviews_url = f"https://www.imdb.com/title/{item['movie_id']}/reviews/" #uses an f-string to create the full web address for the reviews page by inserting the unique movie_id directly into the standard IMDb review URL structure
        yield response.follow(  #schedule a new request to visit the URL I just created
            reviews_url, # specifies the destination URL for the new request
            callback=self.parse_reviews, #send the page to my parse_reviews method to extract the individual comments
            meta={'movie_id': item['movie_id']} # "tags" this new request with the movie_id; ensures every review extracted later can be linked back as a Foreign Key to the correct movie in our database
        )

    #ariana
    #Here, I extract the individual user feedback for each movie
    def parse_reviews(self, response):  #defines the function that Scrapy calls once it has finished loading the IMDb reviews page for a specific movie
        movie_id = response.meta['movie_id']    #retrieves the unique ID "tagged" onto the request in the above function, linking these reviews to the correct movie in our database
        
        # loop through every individual review on the page, identifying each one by its CSS class user-review-item
        for article in response.css('article.user-review-item'):
            review = ReviewItem() #create a new instance of ReviewItem class for each review
            review['movie_id'] = movie_id   #save the carried-over ID into the item, serving as the Foreign Key in our relational database structure
            review['reviewer_name'] = article.css('a[data-testid="author-link"]::text').get()   #extracts the reviewer's username
            review['review_date'] = article.css('li.review-date::text').get()   #extracts the date the review was posted
            review['review_score'] = article.css('span.ipc-rating-star--rating::text').get()    #extracts the user's score fo the movie
            review['review_text'] = article.css('div.ipc-html-content-inner-div::text').get()   #extracts the actual text of the review
            
            yield review    #send the completed review item to my json file