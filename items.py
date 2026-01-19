# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

###ttest
# class BoxmojoItem(scrapy.Item):
#     # define the fields for your item here like:
#     # name = scrapy.Field()
#     pass
#tessstt

class MovieItem(scrapy.Item):
    movie_id = scrapy.Field()
    release_date = scrapy.Field()
    title = scrapy.Field()
    production_company = scrapy.Field()
    duration = scrapy.Field()
    rating = scrapy.Field()
    genres = scrapy.Field()
    tagline = scrapy.Field()
    website = scrapy.Field()
    awards = scrapy.Field()

    #sales = scrapy.Field()

    #all from ERD 

#Ahmed------------------------------------------
# class SalesItem(scrapy.Item):  
#     movie_id = scrapy.Field()          #FK → Movie.movie_id///# master key (tt...)
#     budget = scrapy.Field()            #float/int
#     opening_weekend = scrapy.Field()   #float/int
#     gross_worldwide = scrapy.Field()   #float/int
#     source_url = scrapy.Field()        #BoxOfficeMojo URL


#-------------------------------------------------------------------Ahmed

import scrapy

class SalesItem(scrapy.Item):
    input_title = scrapy.Field()
    input_year = scrapy.Field()

    bom_title = scrapy.Field()
    source_url = scrapy.Field()

    budget = scrapy.Field()
    opening_weekend = scrapy.Field()

    gross_domestic = scrapy.Field()
    gross_international = scrapy.Field()
    gross_worldwide = scrapy.Field()

    genres = scrapy.Field()
    runtime_minutes = scrapy.Field()
    release_date = scrapy.Field()

    filmmakers = scrapy.Field()  # list of {name, role}
    cast = scrapy.Field()        # list of {name, role}