from strategy.edgar import update_filings, update_gains
from strategy.backtest import backtest
from strategy.stockdata import update_trading_days
from pymongo import MongoClient
from os import environ

client = MongoClient(environ['MONGO'])
db = client.form13f
update_list = db.cik.find_one()['cik']
num_stock_list = [5, 15, 50]
update_trading_days()


for cik in update_list:
    update_filings(cik, 40)
    update_gains(cik)
    for num in num_stock_list:
        backtest(cik, '2014-01-01', num, 1000000)
