import requests
from os import environ
from datetime import datetime, timedelta
from pymongo import MongoClient

DATA_API_URL = 'https://api.tiingo.com/tiingo/daily/'
API_KEY = environ['TIINGO_API']
TRADING_CANARIES = ['AAPL', 'WMT']
MIN_DATE = '2014-01-01'
client = MongoClient(environ['MONGODB_URI'])
db = client.get_database()


class DataError(RuntimeError):
    pass


class APIError(ConnectionError):
    pass


def specific_open(ticker, date):
    data = db.stockdata.find_one({'$and': [{'name': ticker}, {'history.' + date: {"$exists": True}}]})
    if data is not None:
        return data['history'].get(date)
    else:
        return None


def data_request(params, ticker):
    url = DATA_API_URL + ticker + "/prices"
    raw_data = requests.get(url, params=params).json()
    print("REQUEST/API: Stock data for " + ticker)
    if isinstance(raw_data, list) and len(raw_data) < 2:
        print("WARNING/SD: Error with " + ticker + " request: " + raw_data[0][:20] if len(raw_data) > 0 else None)
        raise APIError()
    if isinstance(raw_data, dict) and raw_data.get("detail") is not None:
        print(" WARNING/SD: Error with " + ticker + " request: " + raw_data.get("detail"))
        raise APIError()
    data = {'name': ticker, 'history': {}}
    for datum in raw_data:
        data['history'][datum['date'][:10]] = datum['adjOpen']
    return data


def recent_open(ticker, date):
    data = db.stockdata.find_one({'name': ticker})
    if data is None:
        return None
    if data['history'].get(date) is not None:
        return data['history'][date]
    dates = [hist_date for hist_date in data['history'].keys() if hist_date < date]
    if dates:
        before_date = max(dates)
        return data['history'].get(before_date)    # The closest open price before the supplied date
    else:
        return None


def already_in_db(ticker, date):
    query = db.stockdata.find_one({'$and': [{'name': ticker},
                                            {'history.' + date: {"$exists": True}}]})
    return query is not None


def update_stock_db(tickers, date):
    params = {'token': API_KEY, 'startDate': MIN_DATE}
    trimmed = [ticker for ticker in tickers if not already_in_db(ticker, date)]
    failed = []
    for ticker in trimmed:
        try:
            data = data_request(params, ticker)
            db.stockdata.replace_one({'name': ticker}, data, upsert=True)
            if data['history'].get(date) is None:
                print(" WARNING/SD: Could not get data for " + ticker + " on: " + date)
                failed.append(ticker)
        except APIError:
            failed.append(ticker)
    return failed


def update_trading_days():
    for canary in TRADING_CANARIES:
        params = {'token': API_KEY, 'startDate': MIN_DATE}
        data = data_request(params, canary)
        db.stockdata.replace_one({'name': canary}, data, upsert=True)


def next_trading_day(date):
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
    return update_stock_db(tickers, date)       # Returns any tickers that failed data retrieval
