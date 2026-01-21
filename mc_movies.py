import scrapy
from scrapy.spiders import CrawlSpider
from MetacriticNEW.items import MovieItem
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class CrawlingSpider(CrawlSpider):
    name = "mc_movies"
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
            'movies_dataMC.csv': {
                'format': 'csv',
                'item_classes': ['MetacriticNEW.items.MovieItem'],
                'overwrite': True,
                'encoding': 'utf8',
            },
        }
    }

    # Starts scraper
    def start_requests(self):
        for page in range(1, 60):
            url = f"https://www.metacritic.com/browse/movie/?releaseYearMin=2023&releaseYearMax=2025&page={page}"
            # NOTE: Using Selenium instead of Playwright
            # The 'dont_filter=True' allows revisiting same URLs if needed
            yield scrapy.Request(
                url,
                callback=self.parse_listing,
                dont_filter=True
            )

    # Extracts movie links
    def parse_listing(self, response):
        # NOTE: With Selenium middleware, response.meta['driver'] gives us access to the Selenium WebDriver
        # This allows us to interact with the browser if needed (clicking, scrolling, etc.)
        driver = response.meta.get('driver')
        
        # Extract movie links using CSS selectors (same as before)
        movie_links = response.css('a[href*="/movie/"]::attr(href)').getall()
        
        for link in movie_links:
            if '/movie/' in link and '/browse/' not in link:
                full_url = response.urljoin(link)
                # NOTE: Regular Scrapy request for movie detail pages
                # Selenium is only needed for pages that require JavaScript/browser interaction
                yield scrapy.Request(full_url, callback=self.parse_movie)

    def parse_movie(self, response):
        # NOTE: This method doesn't need Selenium since the movie detail page loads normally
        # All data is available in the HTML response
        item = MovieItem()
        
        # MovieID from link
        url_parts = response.url.split('/')
        item['movie_id'] = [part for part in url_parts if part][-1]

        # ReleaseDate
        item['release_date'] = response.xpath('//span[contains(text(), "Release Date")]/following-sibling::span/text()').get()
       
        # MovieTitle
        item["title"] = response.css("h1::text").get()
        if item["title"]:
            item["title"] = item["title"].strip()
        
        # ProductionCompany (joining them into single string and removing white spaces)
        item["production_company"] = ", ".join([company.strip() for company in response.xpath('//span[contains(text(), "Production Company")]/following-sibling::ul//span/text()').getall()])

        # Duration
        item["duration"] = response.xpath('//span[contains(text(), "Duration")]/following-sibling::span/text()').get()

        # Rating
        item["rating"] = response.xpath('//span[contains(text(), "Rating")]/following-sibling::span/text()').get()

        # Genres (Specifically targeting the buttons, removing white spaces and joining them into single string)
        item["genres"] = ", ".join([genre.strip() for genre in response.xpath('//span[contains(text(), "Genres")]/following-sibling::ul//span[@class="c-globalButton_label"]/text()').getall()])
    
        # Director
        # NOTE: strip(',') removes commas that are sometimes included in the HTML text nodes
        directors = response.xpath('//p[contains(., "Directed By")]//a[@href[contains(., "/person/")]]/text()').getall()
        directors = list(dict.fromkeys([d.strip().strip(',').strip() for d in directors if d and d.strip()]))
        item["director"] = ", ".join(directors) if directors else None
        
        # Writer  
        writers = response.xpath('//p[contains(., "Written By")]//a[@href[contains(., "/person/")]]/text()').getall()
        writers = list(dict.fromkeys([w.strip().strip(',').strip() for w in writers if w and w.strip()]))
        item["writer"] = ", ".join(writers) if writers else None
    
        # Taglines
        item["tagline"] = response.xpath('//span[contains(text(), "Tagline")]/following-sibling::span/text()').get()

        # Website
        item["website"] = response.xpath('//span[contains(text(), "Website")]/following-sibling::span/a/@href').get()

        # Awards (Only getting name of the awards they have actually won, not nominations)
        awards = []
        award_divs = response.css('div.c-productionAwardSummary_award')

        for award_div in award_divs:
            festival = award_div.css('div.g-text-bold::text').get()
            count_text = award_div.xpath('./div[2]//text()').get()
            
            # Check if there's at least 1 win and festival is not empty
            if festival and festival.strip() and count_text and 'Win' in count_text:
                awards.append(festival.strip())

        # NOTE: Join outside the loop to avoid creating the string multiple times
        item["awards"] = ", ".join(awards) if awards else None
        
        # Metascore
        item["metascore"] = response.css('div.c-siteReviewScore span::text').get()

        # UserScore
        item["user_score"] = response.css('div.c-siteReviewScore_user span::text').get()

        # Clean empty values - set to None instead of empty strings or empty lists
        for key in item.keys():
            if not item[key] or item[key] == []:
                item[key] = None

        yield item
