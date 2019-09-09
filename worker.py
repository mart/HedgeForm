from strategy.edgar import update_filings, update_gains
from strategy.backtest import backtest
from strategy.stockdata import update_trading_days
from pymongo import MongoClient
from os import environ
from datetime import datetime
import argparse

client = MongoClient(environ['MONGODB_URI'])
db = client.get_database()
update_list = db.cik.find_one()['cik']
num_stock_list = [5, 15, 50]
update_trading_days()


parser = argparse.ArgumentParser(description="pull forms and/or backtest")
parser.add_argument('-f', action="store_true")
parser.add_argument('-b', action="store_true")
args = parser.parse_args()

if args.f:
    for cik in update_list:
        if update_filings(cik, 40):
            update_gains(cik)

if args.b:
    for cik in update_list:
        for num in num_stock_list:
            num_stocks, result = backtest(cik, '2014-01-01', datetime.today().strftime("%Y-%m-%d"), num, 1000000)
            if result['return'] != -100.0:
                db.backtest.update_one({'cik': cik}, {'$set': {num_stocks: result}}, upsert=True)
