from pymongo import MongoClient
from datetime import datetime
from configparser import ConfigParser

# Load configuration file
config = ConfigParser()
config.read('config.ini')

# Setup MongoDB Client
#client = MongoClient(config.get('DEAL_NEW_CREATION', 'MongoClient'))
client = MongoClient("mongodb://localhost:27017/")
db = client['Deal_Transactions']

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
        "Keyword": keyword,
        "Date": datetime.strptime(date, "%Y-%m-%d"),
        "Volume": volume,
        "Price": price,
        "Amount": amount,
        "Init_Volume": volume,
        "Init_Price": price,
        "Sold_Volume": 0,
        "Sold_Amount": 0,
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

insert_deal_and_valuation(db, 1, "Bitcoin", "2023-01-01", 0.01, 25000, 250)
insert_deal_and_valuation(db, 2, "Bitcoin", "2023-01-01", 0.01, 25500, 255)
insert_deal_and_valuation(db, 3, "Bitcoin", "2023-01-01", 0.01, 26000, 260)
insert_deal_and_valuation(db, 4, "Bitcoin", "2023-01-01", 0.01, 26500, 265)
insert_deal_and_valuation(db, 5, "Bitcoin", "2023-01-01", 0.01, 27000, 270)
insert_deal_and_valuation(db, 6, "Bitcoin", "2023-01-01", 0.01, 27500, 275)
insert_deal_and_valuation(db, 7, "Bitcoin", "2023-01-01", 0.01, 28000, 280)
insert_deal_and_valuation(db, 8, "Bitcoin", "2023-01-01", 0.01, 28500, 285)
insert_deal_and_valuation(db, 9, "Bitcoin", "2023-01-01", 0.01, 29000, 290)
insert_deal_and_valuation(db, 10, "Bitcoin", "2023-01-01", 0.01, 29500, 295)

insert_deal_and_valuation(db, 11, "Ethereum", "2023-01-01", 0.01, 1500, 15)
insert_deal_and_valuation(db, 12, "Ethereum", "2023-01-01", 0.01, 1550, 15.5)
insert_deal_and_valuation(db, 13, "Ethereum", "2023-01-01", 0.01, 1600, 16)
insert_deal_and_valuation(db, 14, "Ethereum", "2023-01-01", 0.01, 1650, 16.5)
insert_deal_and_valuation(db, 15, "Ethereum", "2023-01-01", 0.01, 1700, 17)
insert_deal_and_valuation(db, 16, "Ethereum", "2023-01-01", 0.01, 1750, 17.5)
insert_deal_and_valuation(db, 17, "Ethereum", "2023-01-01", 0.01, 1800, 18)
insert_deal_and_valuation(db, 18, "Ethereum", "2023-01-01", 0.01, 1850, 18.5)
insert_deal_and_valuation(db, 19, "Ethereum", "2023-01-01", 0.01, 1900, 19)
insert_deal_and_valuation(db, 20, "Ethereum", "2023-01-01", 0.01, 1950, 19.5)