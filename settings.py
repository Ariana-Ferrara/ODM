# Scrapy settings for MetacriticNEW project

BOT_NAME = "MetacriticNEW"

SPIDER_MODULES = ["MetacriticNEW.spiders"]
NEWSPIDER_MODULE = "MetacriticNEW.spiders"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Maximum concurrent requests
CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 16

# Download delay between requests (seconds)
# Helps avoid getting blocked
DOWNLOAD_DELAY = 2 
RANDOMIZE_DOWNLOAD_DELAY = True

# Disable cookies
#COOKIES_ENABLED = False

# Disable Telnet Console
#TELNETCONSOLE_ENABLED = False

# Override default request headers
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Spider middlewares
#SPIDER_MIDDLEWARES = {
#    "MetacriticNEW.middlewares.MetacriticnewSpiderMiddleware": 543,
#}

# Downloader middlewares
# Custom Selenium middleware with BrightData proxy authentication
# Creates Chrome browser with BrightData proxy
# Processes all requests through browser
# Handles proxy authentication automatically
# Makes driver available via response.meta['driver']
DOWNLOADER_MIDDLEWARES = {
    'MetacriticNEW.middlewares.SeleniumBrightDataMiddleware': 800,
}

# Required packages:
# pip install selenium
# pip install selenium-wire (for proxy authentication support)
# Install chromedriver: https://chromedriver.chromium.org/downloads
# Or use: pip install webdriver-manager (auto-manages chromedriver)

# Item pipelines
#ITEM_PIPELINES = {
#    "MetacriticNEW.pipelines.MetacriticnewPipeline": 300,
#}

# AutoThrottle extension
# Useful with Selenium to avoid overloading browser
AUTOTHROTTLE_ENABLED = False
#AUTOTHROTTLE_START_DELAY = 5
#AUTOTHROTTLE_MAX_DELAY = 60
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
#AUTOTHROTTLE_DEBUG = False

# HTTP caching
# Useful during development to avoid repeated requests
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Feed export encoding
# UTF-8 ensures proper encoding of special characters in CSV/JSON
FEED_EXPORT_ENCODING = "utf-8"
# Logging
# NOTE: Set to DEBUG to see detailed Selenium operations, INFO for normal operation
LOG_LEVEL = 'WARNING'
