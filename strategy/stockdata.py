import requests
from os import environ
from datetime import datetime, timedelta
from pymongo import MongoClient
import logging


DATA_API_URL = 'https://www.alphavantage.co/query'
API_KEY = environ['AV_API']
TRADING_CANARIES = ['AAPL', 'WMT']
MIN_DATE = '2014-01-01'
MAX_DATE = '2019-12-31'


class DataError(RuntimeError):
    pass


class APIError(ConnectionError):
    pass


def data_request(params):
    data = requests.get(DATA_API_URL, params=params).json()
    if data.get("message") is not None and "request limit" in data["message"]:
        raise APIError("Over API limit.")
    if data.get("Message") is not None or data.get('name') is None:
        raise APIError("Something's wrong with the API request: " + data.get("Message"))
    return data


def recent_open(ticker, date, db):
    data = db.stockdata.find_one({'name': ticker})
    if data is None:
        return None
    if data['history'].get(date) is not None:
        return data['history'][date]['open']
    dates = [hist_date for hist_date in data['history'].keys() if hist_date < date]
    before_date = max(dates)
    return data['history'][before_date]['open']     # The closest open price before the supplied date


def get_historical_price(db, ticker):
    params = {'api_token': API_KEY, 'symbol': ticker}
    try:
        data = data_request(params)
    except APIError:
        return False
    db.stockdata.replace_one({'name': ticker}, data, upsert=True)
    return True


def already_in_db(ticker, date, db):
    query = db.stockdata.find_one({'$and': [{'name': ticker},
                                            {'history.' + date: {"$exists": True}}]})
    return query is not None


def update_stock_db(tickers, date, db):
    base_params = {'api_token': API_KEY}
    trimmed = [ticker for ticker in tickers if not already_in_db(ticker, date, db)]
    failed = []
    for ticker in trimmed:
        base_params.update({'symbol': ticker})
        try:
            data = data_request(base_params)
            db.stockdata.replace_one({'name': ticker}, data, upsert=True)
            if data['history'].get(date) is None:
                failed.append(ticker)
        except APIError:
            found_historical = get_historical_price(db, ticker)
            failed.append(ticker)
            if found_historical:
                logging.info("Found historical data for '" + ticker + "'.")
            else:
                logging.warning("Could not get data for '" + ticker + "'.")
    return failed


def update_trading_days():
    client = MongoClient(environ['MONGO'])
    db = client.form13f
    for canary in TRADING_CANARIES:
        params = {'api_token': API_KEY, 'symbol': canary}
        data = data_request(params)
        db.stockdata.replace_one({'name': canary}, data, upsert=True)


def next_trading_day(date, db):
    next_date = datetime.strptime(date, '%Y-%m-%d')
    for i in range(7):
        next_date += timedelta(days=1)
        for canary in TRADING_CANARIES:
            query = db.stockdata.find_one({'$and': [{'name': canary},
                                                    {'history.' + next_date.strftime('%Y-%m-%d'): {"$exists": True}}]})
            if query is not None:
                return next_date.strftime('%Y-%m-%d')
    raise DataError("Either the stock market disappeared or you should update the trading days manually.")


def get_data(tickers, date):
    client = MongoClient(environ['MONGO'])
    db = client.form13f
    return update_stock_db(tickers, date, db)       # Returns any tickers that failed data retrieval
