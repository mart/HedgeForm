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


def data_request(params, ticker):
    url = DATA_API_URL + ticker + "/prices"
    raw_data = requests.get(url, params=params).json()
    print("REQUEST/API: Stock data for " + ticker)
    if isinstance(raw_data, list) and len(raw_data) < 2:
        raise APIError("Error with " + ticker + " request: " + raw_data[0][:20] if len(raw_data) > 0 else None)
    if isinstance(raw_data, dict) and raw_data.get("detail") is not None:
        raise APIError("Error with " + ticker + " request: " + raw_data.get("detail"))
    data = {'name': ticker, 'history': {}}
    for datum in raw_data:
        data['history'][datum['date'][:10]] = datum
    return data


def recent_open(ticker, date):
    data = db.stockdata.find_one({'name': ticker})
    if data is None:
        return None
    if data['history'].get(date) is not None:
        return data['history'][date]['adjOpen']
    dates = [hist_date for hist_date in data['history'].keys() if hist_date < date]
    before_date = max(dates)
    return data['history'][before_date]['adjOpen']     # The closest open price before the supplied date


def get_historical_price(ticker):
    params = {'token': API_KEY, 'startDate': MIN_DATE}
    try:
        data = data_request(params, ticker)
    except APIError:
        return False
    db.stockdata.replace_one({'name': ticker}, data, upsert=True)
    return True


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
                failed.append(ticker)
        except APIError as e:
            print("WARNING/SD: " + str(e))
            found_historical = get_historical_price(ticker)
            failed.append(ticker)
            if found_historical:
                print("STOCKDATA: Found historical data for '" + ticker + "'.")
            else:
                print("STOCKDATA: Could not get data for '" + ticker + "'.")
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
