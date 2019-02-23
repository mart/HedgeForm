from strategy.edgar import update_filings, update_gains
from strategy.backtest import backtest
from pymongo import MongoClient
from os import environ

client = MongoClient(environ['MONGO'])
db = client.form13f
update_list = db.cik.find_one()['cik']

for cik in update_list:
    update_filings(cik)
    update_gains(cik)
    for num in [5, 10, 30]:
        backtest(cik, '2014-01-01', num, 1000000)
