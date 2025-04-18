
from utils.mongodb import mongo_db
from datetime import datetime

class Transaction:
    collection = mongo_db["transaction"]

    def __init__(self, transaction_id, amount, timestamp=None, status="pending"):
        self.transaction_id = transaction_id
        self.amount = amount
        self.timestamp = timestamp or datetime.utcnow()
        self.status = status

    def to_dict(self):
        return {
            "transaction_id": self.transaction_id,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "status": self.status,
        }

    def save(self):
        self.collection.insert_one(self.to_dict())

    @classmethod
    def all(cls):
        return list(cls.collection.find())

    @classmethod
    def find_by_id(cls, transaction_id):
        return cls.collection.find_one({"transaction_id": transaction_id})

    def __str__(self):
        return self.transaction_id

