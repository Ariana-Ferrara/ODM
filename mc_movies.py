import scrapy
from scrapy.spiders import CrawlSpider, Rule
from Metacritic.items import MovieItem
from scrapy_playwright.page import PageMethod

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
            'movies_data.json': {
                'format': 'json',
                'item_classes': ['Metacritic.items.MovieItem'],
                'overwrite': True,
                'encoding': 'utf8',
                'indent': 4, # This makes the JSON pretty and readable
            },
        }
        
    }


    #Starts scraper
    def start_requests(self):
        for page in range(1, 21):
            url = f"https://www.metacritic.com/browse/movie/?releaseYearMin=2025&releaseYearMax=2026&page={page}"
            yield scrapy.Request(
                url,
                meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                ),
                callback=self.parse_listing,
                errback=self.errback,
            )

    #Extracts movielinks
    async def parse_listing(self, response):
        page = response.meta["playwright_page"]
        await page.close()
        
        movie_links = response.css('a[href*="/movie/"]::attr(href)').getall()
        
        for link in movie_links:
            if '/movie/' in link and '/browse/' not in link:
                full_url = response.urljoin(link)
                yield scrapy.Request(full_url, callback=self.parse_movie)

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()

    def parse_movie(self, response):
        item = MovieItem()
        #MovieID from link
        url_parts = response.url.split('/')
        item['movie_id'] = [part for part in url_parts if part][-1]

        #ReleaseDate (see if can be changed to only release_date)
        item['release_date'] = response.xpath('//span[contains(text(), "Release Date")]/following-sibling::span/text()').get()
       
        #MovieTitle
        item["title"] = response.css("h1::text").get()
        
        if item["title"]:
            item["title"] = item["title"].strip()
        
        #ProductionCompany (joining them into single strip and removing white spaces)
        item["production_company"] = ", ".join([company.strip() for company in response.xpath('//span[contains(text(), "Production Company")]/following-sibling::ul//span/text()').getall()] )

        #Duration
        item["duration"] = response.xpath('//span[contains(text(), "Duration")]/following-sibling::span/text()').get()

        #Rating
        item["rating"] = response.xpath('//span[contains(text(), "Rating")]/following-sibling::span/text()').get()

        #Genres (Specifically targeting the buttons, removing white spaces and joining them into single strip)
        item["genres"] =  ", ".join([genre.strip() for genre in response.xpath('//span[contains(text(), "Genres")]/following-sibling::ul//span[@class="c-globalButton_label"]/text()').getall()])
    
        #Taglines
        item["tagline"] = response.xpath('//span[contains(text(), "Tagline")]/following-sibling::span/text()').get()

        #Website
        item["website"] = response.xpath('//span[contains(text(), "Website")]/following-sibling::span/a/@href').get()

        #Awards (Only getting name of the rewards they have actually won since there is also nominations)
        awards = []
        award_divs = response.css('div.c-productionAwardSummary_award')

        for award_div in award_divs:
            festival = award_div.css('div.g-text-bold::text').get()
            count_text = award_div.xpath('./div[2]//text()').get()
            
            # Check if there's at least 1 win
            if festival and count_text and 'Win' in count_text:
                awards.append(festival.strip())

            # Join all awards into a single string
            item["awards"] = ", ".join(awards) if awards else None
        
        #Metascore
        item["metascore"] = response.css('div.c-siteReviewScore span::text').get()

        #UserScore
        item["user_score"] = response.css('div.c-siteReviewScore_user span::text').get()

        # Clean empty values
        for key in item.keys():
            if not item[key] or item[key] == []:
                item[key] = None

        yield item
