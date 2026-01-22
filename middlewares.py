# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy import signals
from scrapy.http import HtmlResponse
from selenium import webdriver
from seleniumwire import webdriver as wire_webdriver  # Selenium-wire for proxy auth
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from seleniumwire.utils import decode
import logging
import time

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class OdmGroup1SpiderMiddleware:
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


class OdmGroup1DownloaderMiddleware:
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
        proxy_url = 'http://brd-customer-hl_79cc5ce7-zone-proxygroup1:lv505da0ax0k@brd.superproxy.io:33335'
        
        seleniumwire_options = {
            'proxy': {
                'http': proxy_url,
                'https': proxy_url,
            },
            'verify_ssl': False,
            'suppress_connection_errors': True,
        }
        
        chrome_options = webdriver.ChromeOptions()
        
        chrome_options.add_argument('--ignore-certificate-errors') # Fixes "Not Secure"
        chrome_options.add_argument('--ignore-ssl-errors')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--allow-running-insecure-content')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--ignore-urlfetcher-cert-requests')
        #chrome_options.add_argument('--headless=new')  #Comment this out if you want to see the screen and selenium action


        service = Service(ChromeDriverManager().install())

        self.driver = wire_webdriver.Chrome(
            service=service,
            options=chrome_options,
            seleniumwire_options=seleniumwire_options
        )

    
    def spider_closed(self, spider):
        # Closes browser when spider finishes
        if self.driver:
            self.driver.quit()
    
    def process_request(self, request, spider):
        if not self.driver:
            return None
        
        try:
            # Tell the driver to load the URL
            self.driver.get(request.url)
            
            # Waits for Bright Data to tunnel the connection
            time.sleep(5) 
            self.driver.implicitly_wait(20)
            
            # Create the response and manually attach the driver
            response = HtmlResponse(
                url=request.url,
                body=self.driver.page_source,
                encoding='utf-8',
                request=request,
                status=200,
            )
            
            response.meta['driver'] = self.driver   #Places the driver into meta
            return response
            
        except Exception as e:
            spider.logger.error(f"Middleware critical failure: {e}")
            return None
        
    def process_response(self, request, response, spider):
        # Returns response as-is
        # All processing done in process_request
        return response
    
    def process_exception(self, request, exception, spider):
        # Returns None to let Scrapy handle exceptions
        return None