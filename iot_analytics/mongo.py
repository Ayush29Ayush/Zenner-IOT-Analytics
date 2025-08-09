from decouple import config
from pymongo import MongoClient

MONGO_URI = config("MONGODB_URI")
MONGO_DB = config("MONGODB_DB")

_client = None

def get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    return _client[MONGO_DB]
