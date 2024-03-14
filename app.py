from pymongo import MongoClient
from datetime import datetime
from configparser import ConfigParser
from binance.client import Client
import os

# Load configuration file
config = ConfigParser()
config.read('config.ini')

# Setup MongoDB Client
client = MongoClient("mongodb://localhost:27017/")
db = client['Deal_Transactions']

# Binance API Key and Secret
api_key = os.environ.get(config.get('DEAL_NEW_CREATION', 'APIKey'))
api_secret = os.environ.get(config.get('DEAL_NEW_CREATION', 'APISecret'))
binance_client = Client(api_key, api_secret)

def get_binance_price(keyword, datetime_str):
    symbol_map = {
        "Bitcoin": "BTCUSDT",
        "Ethereum": "ETHUSDT"
    }
    symbol = symbol_map.get(keyword)
    if not symbol:
        raise ValueError(f"Invalid cryptocurrency: {keyword}")

    specific_time = datetime.fromisoformat(datetime_str)
    specific_time_ms = int(specific_time.timestamp() * 1000)  # Convert to milliseconds

    klines = binance_client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, str(specific_time_ms), str(specific_time_ms+60000))

    if klines:
        close_price = klines[0][4]
        return float(close_price)
    else:
        return "No data available for the specified time"

def insert_deal_and_valuation(db, deal_id, keyword, date_str, volume, init_price, factor):
    date = datetime.strptime(date_str, "%Y-%m-%d")

    datetime_str = date.strftime('%Y-%m-%dT00:00:00')
    price = get_binance_price(keyword, datetime_str)

    # Insert into Deals collection
    deal = {
        "DealUID": deal_id,
        "Keyword": keyword,
        "Date": date,
        "Volume": volume,
        "Price": init_price,
        "Amount": volume*init_price,
        "Factor": factor,
        "VersionSEQ": 0,
        "InactiveFlag": "N",
    }
    db.Deals.insert_one(deal)

    # Insert into Valuations collection using the DealID from the Deals collection
    valuation = {
        "DealUID": deal_id,
        "Keyword": keyword,
        "Date": date,
        "Volume": volume,
        "Price": price,
        "Amount": volume*price,
        "Init_Volume": volume,
        "Init_Price": init_price, 
        "Sold_Volume": 0,
        "Sold_Amount": 0,
    }
    db.Valuations.insert_one(valuation)

    # Insert into Transactions collection using the DealID from the Deals collection
    transaction = {
        "DealUID": deal_id,
        "Type": "Buy",
        "Date": date,
        "Volume": volume,
        "Price": init_price,  
        "Amount": volume*init_price,
    }
    db.Transactions.insert_one(transaction)

insert_deal_and_valuation(db, 1, "Bitcoin", "2023-01-01", 0.01, 25000, 0.1)
insert_deal_and_valuation(db, 2, "Bitcoin", "2023-01-01", 0.01, 25500, 0.1)
insert_deal_and_valuation(db, 3, "Bitcoin", "2023-01-01", 0.01, 26000, 0.1)
insert_deal_and_valuation(db, 4, "Bitcoin", "2023-01-01", 0.01, 26500, 0.1)
insert_deal_and_valuation(db, 5, "Bitcoin", "2023-01-01", 0.01, 27000, 0.1)
insert_deal_and_valuation(db, 6, "Bitcoin", "2023-01-01", 0.01, 27500, 0.1)
insert_deal_and_valuation(db, 7, "Bitcoin", "2023-01-01", 0.01, 28000, 0.1)
insert_deal_and_valuation(db, 8, "Bitcoin", "2023-01-01", 0.01, 28500, 0.1)
insert_deal_and_valuation(db, 9, "Bitcoin", "2023-01-01", 0.01, 29000, 0.1)
insert_deal_and_valuation(db, 10, "Bitcoin", "2023-01-01", 0.01, 29500, 0.1)

insert_deal_and_valuation(db, 11, "Ethereum", "2023-01-01", 0.1, 1500, 0.1)
insert_deal_and_valuation(db, 12, "Ethereum", "2023-01-01", 0.1, 1550, 0.1)
insert_deal_and_valuation(db, 13, "Ethereum", "2023-01-01", 0.1, 1600, 0.1)
insert_deal_and_valuation(db, 14, "Ethereum", "2023-01-01", 0.1, 1650, 0.1)
insert_deal_and_valuation(db, 15, "Ethereum", "2023-01-01", 0.1, 1700, 0.1)
insert_deal_and_valuation(db, 16, "Ethereum", "2023-01-01", 0.1, 1750, 0.1)
insert_deal_and_valuation(db, 17, "Ethereum", "2023-01-01", 0.1, 1800, 0.1)
insert_deal_and_valuation(db, 18, "Ethereum", "2023-01-01", 0.1, 1850, 0.1)
insert_deal_and_valuation(db, 19, "Ethereum", "2023-01-01", 0.1, 1900, 0.1)
insert_deal_and_valuation(db, 20, "Ethereum", "2023-01-01", 0.1, 1950, 0.1)