# db.py
from pymongo import MongoClient
import urllib.parse

class MongoDB:
    def __init__(self, collection_name):
        username = urllib.parse.quote_plus('admin')
        password = urllib.parse.quote_plus('admin')

        self.client = MongoClient(f'mongodb://{username}:{password}@mongoservice')
        self.db = self.client['gamesanalyst']
        self.collection = self.db[collection_name]