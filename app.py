from pymongo import MongoClient
from datetime import datetime

def connect_to_mongodb(uri="mongodb://localhost:27017/"):
    client = MongoClient(uri)
    return client['Deal_Transactions']

def insert_deal_and_valuation(db, deal_id, keyword, date, volume, price, amount):
    # Insert into Deals collection
    deal = {
        "DealUID": deal_id,
        "Keyword": keyword,
        "Date": datetime.strptime(date, "%Y-%m-%d"),
        "Volume": volume,
        "Price": price,
        "Amount": amount,
        "VersionSEQ": 0,
        "InactiveFlag": "N",
    }
    deal_inserted = db.Deals.insert_one(deal)

    # Insert into Valuations collection using the DealID from the Deals collection
    valuation = {
        "DealUID": deal_id,
        "Date": datetime.strptime(date, "%Y-%m-%d"),
        "Volume": volume,
        "Price": price,
        "Amount": amount,
    }
    db.Valuations.insert_one(valuation)

    # Insert into Transactions collection using the DealID from the Deals collection
    transaction = {
        "DealUID": deal_id,
        "Type": "Buy",
        "Date": datetime.strptime(date, "%Y-%m-%d"),
        "Volume": volume,
        "Price": price,
        "Amount": amount,
    }
    db.Transactions.insert_one(transaction)

# Example usage
db = connect_to_mongodb()

insert_deal_and_valuation(db, 1, "Bitcoin", "2023-03-21", 0.01, 28000, 280)