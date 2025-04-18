from pymongo import MongoClient
from django.conf import settings

client = MongoClient(settings.MONGO_DB_URI)
mongo_db = client[settings.MONGO_DB_NAME]

transactions_collection = mongo_db['transaction']
summary_collection = mongo_db['transaction_summary']

