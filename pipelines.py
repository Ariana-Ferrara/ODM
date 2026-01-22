# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from ODM_Group1.items import MovieItem, ReviewItem
from scrapy.exceptions import DropItem

class DuplicatesPipeline:
    def __init__(self):
        self.seen_movies = set()

    def process_item(self, item, spider):

        if isinstance(item, MovieItem):
            movie_id = item.get('movie_id')
            if movie_id in self.seen_movies:
                raise DropItem(f"Duplicate movie: {movie_id}")
            self.seen_movies.add(movie_id)

        return item
