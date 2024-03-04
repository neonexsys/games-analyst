# db.py
from pymongo import MongoClient
import urllib.parse
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.


class MongoDB:
  def __init__(self, collection_name):
    username = urllib.parse.quote_plus(os.getenv("MONGODB_DB_USERNAME"))
    password = urllib.parse.quote_plus(os.getenv("MONGODB_DB_PASSWORD"))
    host = os.getenv("MONGODB_DB_HOST")
    port = os.getenv("MONGODB_DB_PORT")

    self.client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}")
    self.db = self.client["gamesanalyst"]
    self.collection = self.db[collection_name]
