# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        # Category & Product Type ---> Switch to lowercase
        lowercase_keys = ['category','product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()
        
        # Price ---> Convert to float
        price_keys = ['price','tax','price_excl_tax','price_incl_tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£','')
            adapter[price_key] = float(value)

        # Availability ---> Extract number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = availability_array[0]

        # Reviews ---> Convert string to number
        num_review_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_review_string)

        # Stars ---> Convert text to number
        stars_string = adapter.get('stars')
        split_stars_array = stars_string.split(' ')
        stars_text_value = split_stars_array[1].lower()
        if stars_text_value == "zero":
            adapter['stars'] = 0
        elif stars_text_value == "one":
            adapter['stars'] = 1
        elif stars_text_value == "two":
            adapter['stars'] = 2
        elif stars_text_value == "three":
            adapter['stars'] = 3
        elif stars_text_value == "four":
            adapter['stars'] = 4
        elif stars_text_value == "five":
            adapter['stars'] = 5

        return item

import pymongo
from pymongo import MongoClient


class SaveToMongoDBPipeline:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017")
        self.db = self.client["WebScraping"]
        self.col = self.db["Books"]
    
    def process_item(self,item,spider):
        self.col.insert_one({"url":item["url"],"title":item["title"],"product_type":item["product_type"],"price_excl_tax":item["price_excl_tax"],"price_incl_tax":item["price_incl_tax"],"tax":item["tax"],"price":item["price"],"availability":item["availability"],"num_reviews":item["num_reviews"],"stars":item["stars"],"category":item["category"],"description":str(item["description"][0])})
        
        return item