import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import MovieItem, ReviewItem
import re
from scrapy.exceptions import DropItem, CloseSpider
import json

#chris and ariana
class CrawlingSpider(CrawlSpider):
    name = "mycrawler"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/search/title/?title_type=feature&release_date=2025-01-01,2026-01-31"]
    
    #chris
    def __init__(self, *args, **kwargs):
        super(CrawlingSpider, self).__init__(*args, **kwargs)
        self.yielded_ids = set()
        self.item_count = 0
        self.max_items = 20
    #chris and ariana
    rules = (
        # Match the main title page, even with 'ref' tags, but STOP there
        Rule(LinkExtractor(
            allow=r'/title/tt\d+/', 
            deny=(r'/technical', r'/companycredits', r'/fullcredits', r'/parentalguide')
        ), callback="parse_item", follow=False),
        
        # Follow search result pagination
        Rule(LinkExtractor(allow=r'/search/title/'), follow=True),
    )
    #chris
    custom_settings = {
        # REMOVED THE PIPELINE - testing if that's the issue
        'DOWNLOAD_DELAY': 10,
        'AUTOTHROTTLE_ENABLED': True,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'AUTOTHROTTLE_START_DELAY': 5,
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'REDIRECT_ENABLED': False,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.google.com/',
        },
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,
            'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': None,
        },
        'METAREFRESH_ENABLED': False,
        'HTTPERROR_ALLOWED_CODES': [301, 302],
    }
    #ariana
    def parse_item(self, response):
        # close spider if we hit the limit
        if self.item_count >= self.max_items:
            CloseSpider
        #ariana
        # Extract the ID first to check for duplicates
        movie_id_match = re.search(r'tt\d+', response.url)
        movie_id = movie_id_match.group() if movie_id_match else None
        
        #ariana
        # Handle duplicates (Only skip if seen in THIS run)
        if movie_id and movie_id in self.yielded_ids:
            self.logger.info(f"Skipping duplicate in current session: {movie_id}")
            return
        #ariana
        if movie_id:
            self.yielded_ids.add(movie_id)
        
        #chris and ariana
        # initialize the item and start filling it
        item = MovieItem()

        #chris
        # --- movieID ---
        if movie_id:
            # REMOVE: self.yielded_ids.add(movie_id) 
            # REMOVE: self.item_count += 1
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
            # Extract JSON-LD metadata for reliable data collection
        json_text = response.xpath('//script[@type="application/ld+json"]/text()').get()
        if json_text:
            data = json.loads(json_text)
            
            # 1. Genres as a list
            item['genres'] = data.get('genre', [])
            
            # 2. Rating (More reliable than CSS)
            rating_data = data.get('aggregateRating', {})
            item['rating'] = rating_data.get('ratingValue')
        else:
            item['genres'] = []       
    
        #ariana
        #------ character names ------
        roles_raw = response.css('div[class*="characters-list"] span::text').getall()
        item['roles'] = list(set(roles_raw)) if roles_raw else []

        #ariana
        # --- final extraction step (e.g., actor names) ---
        stars_raw = response.css('a[data-testid="title-cast-item__actor"]::text').getall()
        item['stars'] = list(set(stars_raw)) if stars_raw else []

        #Budget - by Chris
        item['budget'] = response.css('[data-testid="title-boxoffice-budget"] .ipc-metadata-list-item__list-content-item::text').get()
        item['grossworldwide'] = response.css('[data-testid="title-boxoffice-cumulativeworldwidegross"] .ipc-metadata-list-item__list-content-item::text').get()
        item['openingweekend'] = response.css('[data-testid="title-boxoffice-openingweekenddomestic"] .ipc-metadata-list-item__list-content-item::text').get()



        #ariana
        # --- NOW HANDLE THE STAR LINK ---
        star_links = response.css('a[data-testid="title-cast-item__actor"]::attr(href)').getall()
        if star_links:
            for link in star_links:
                # We yield a request for EVERY actor found
                yield response.follow(
                    link, 
                    callback=self.parse_star_details, 
                    meta={'item': item.copy()}, # .copy() ensures each actor gets their own item instance
                    dont_filter=True 
                )
                
            # --- ADD THIS AFTER THE FOR LOOP ENDS (Line 149) ---
            # This triggers the review scraping exactly once per movie
            reviews_url = f"https://www.imdb.com/title/{item['movie_id']}/reviews/"
            yield response.follow(reviews_url, callback=self.parse_reviews, meta={'movie_id': item['movie_id']})

        else:
            # If no star link, we MUST yield and jump to reviews here
            yield item
            reviews_url = f"https://www.imdb.com/title/{item['movie_id']}/reviews/"
            yield response.follow(reviews_url, callback=self.parse_reviews, meta={'movie_id': item['movie_id']})

    #ariana
    def parse_star_details(self, response):
        item = response.meta['item']
        
        # 1. Pull the specific actor's name from the page header
        actor_name = response.css('span.hero__primary-text::text').get()
        item['actor_name'] = actor_name.strip() if actor_name else "Unknown"

        # 2. Targeted selector for the rank (from image_07c4a8.jpg)
        rank = response.css('span.starmeter-current-rank::text').get()
        item['starmeter'] = rank.strip() if rank else None

        # 3. Finalize and Yield: This now creates one unique row per actor
        self.item_count += 1
        yield item

    #ariana
    def parse_reviews(self, response):
        movie_id = response.meta['movie_id']
        
        # 7. Loop through the containers found in shell
        for article in response.css('article.user-review-item'):
            # Use the ReviewItem class for clean CSV rows
            review = ReviewItem() 
            review['movie_id'] = movie_id
            review['reviewer_name'] = article.css('a[data-testid="author-link"]::text').get()
            review['review_date'] = article.css('li.review-date::text').get()
            review['review_score'] = article.css('span.ipc-rating-star--rating::text').get()
            review['review_text'] = article.css('div.ipc-html-content-inner-div::text').get()
            
            yield review
