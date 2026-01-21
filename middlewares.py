# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.http import HtmlResponse
from selenium import webdriver
from seleniumwire import webdriver as wire_webdriver  # Selenium-wire for proxy auth
import logging

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class MetacriticnewSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class MetacriticnewDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class SeleniumBrightDataMiddleware:
    # Selenium middleware with BrightData proxy support
    # Creates Chrome WebDriver with BrightData proxy authentication
    # Loads each URL in the browser and returns page source as Scrapy response
    # Provides access to driver via response.meta['driver']
    
    def __init__(self):
        # Initialize middleware with empty driver
        self.driver = None
    
    @classmethod
    def from_crawler(cls, crawler):
        # Scrapy calls this method to create the middleware instance
        # Connects to spider_opened and spider_closed signals for setup/cleanup
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware
    
    def spider_opened(self, spider):
        # Creates Selenium WebDriver when spider opens
        # Configures BrightData proxy with authentication
        # Uses selenium-wire for authenticated proxy support
        
        # BrightData proxy configuration
        proxy_url = 'http://brd-customer-hl_79cc5ce7-zone-proxygroup1:lv505da0ax0k@brd.superproxy.io:33335'
        
        # Seleniumwire proxy configuration
        # Extends Selenium to support authenticated proxies
        seleniumwire_options = {
            'proxy': {
                'http': proxy_url,
                'https': proxy_url,
                'no_proxy': 'localhost,127.0.0.1'
            },
            'connection_timeout': 300,
            'read_timeout': 300
        }
        
        # Chrome options for browser configuration
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')  # Required for some environments
        chrome_options.add_argument('--disable-dev-shm-usage')  # Overcome limited resources
        chrome_options.add_argument('--disable-gpu')  # Disable GPU acceleration
        chrome_options.add_argument('--window-size=1920,1080')  # Set window size
        
        # User agent matching Scrapy settings
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Create Chrome driver with seleniumwire and proxy
        self.driver = wire_webdriver.Chrome(
            options=chrome_options,
            seleniumwire_options=seleniumwire_options
        )
        
        # Set page load and script timeouts
        self.driver.set_page_load_timeout(120)  # 2 minutes for page load
        self.driver.set_script_timeout(120)  # 2 minutes for scripts
    
    def spider_closed(self, spider):
        # Closes browser when spider finishes
        if self.driver:
            self.driver.quit()
    
    def process_request(self, request, spider):
        # Processes each request through Selenium browser
        # Loads URL, waits for page load, returns page source
        # Driver passed in response.meta['driver'] for spider access
        
        if self.driver is None:
            return None
        
        try:
            # Load page in browser
            self.driver.get(request.url)
            
            # Wait for page elements to load
            self.driver.implicitly_wait(30)
            
            # Get rendered page source after JavaScript execution
            body = self.driver.page_source
            
            # Create Scrapy response from rendered HTML
            # Driver available in meta for additional interactions
            return HtmlResponse(
                url=request.url,
                body=body,
                encoding='utf-8',
                request=request,
                status=200,
                meta={'driver': self.driver}
            )
            
        except Exception as e:
            # Return None on error, allows Scrapy to retry or skip
            return None
    
    def process_response(self, request, response, spider):
        # Returns response as-is
        # All processing done in process_request
        return response
    
    def process_exception(self, request, exception, spider):
        # Returns None to let Scrapy handle exceptions
        return None