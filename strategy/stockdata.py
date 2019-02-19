import requests
from os import environ
from datetime import datetime, timedelta
from time import sleep
from pymongo import MongoClient
from pandas.tseries.holiday import USFederalHolidayCalendar


MAX_HISTORICAL_DAYS_WEB = 365
MAX_HISTORICAL_DAYS_LOCAL = 365
DATA_API_URL = 'https://www.worldtradingdata.com/api/v1/history_multi_single_day'
API_KEY = environ['WTD_API']


def db_get_recent_open(holding, date, db):
    next_date = datetime.strptime(date, '%Y-%m-%d')
    for i in range(MAX_HISTORICAL_DAYS_LOCAL):
        query = db.stockdata.find_one({'$and': [{'date': next_date.strftime('%Y-%m-%d')},
                                                {'data.' + holding + '.open': {"$exists": True}}]})
        if query is not None and query.get('data') is not None and query['data'].get(holding) is not None:
            return query['data'][holding]['open']
        next_date -= timedelta(days=1)
    return None


def db_add_date(next_date, db):
    if db.stockdata.find_one({'date': next_date}) is None:
        db.stockdata.insert_one({'date': next_date, 'data': {}})
    return db.stockdata.find_one({'date': next_date}).get('_id')


def db_add_trading_day(date, next_date, db):
    if db.tradingday.find_one({'date': date}) is None:
        db.tradingday.insert_one({'date': date, 'next_trading_day': next_date})


def first_open_backwards(delta, next_date, holidays):
    day = next_date - timedelta(days=delta)
    while day.weekday() > 4 or day in holidays:
        day -= timedelta(days=1)
    return day


def stock_trading_on(day, params):
    params.update({'date': day.strftime('%Y-%m-%d')})
    for i in range(2):
        try:
            r = requests.get(DATA_API_URL, params=params).json()
            if r.get('data') is not None:
                print("API hit: testing stock trading on day")
                return True
            return False
        except ConnectionError as e:
            if i == 1:
                print("Connection to WTD failed twice.")
                raise e
            sleep(10)


def get_historical_price(next_date, db, ticker):
    params = {'api_token': environ['WTD_API'], 'symbol': ticker}
    holidays = USFederalHolidayCalendar().holidays(start='2014-01-01', end='2019-12-31').to_pydatetime()
    lo, hi = 1, 365
    while hi > lo:
        mid = (hi + lo) // 2
        trading_day = first_open_backwards(mid, next_date, holidays)
        if stock_trading_on(trading_day, params):
            hi = mid
        else:
            lo = mid
        if hi - lo == 1:
            break
    for delta in [hi, lo]:
        last_trading_day = first_open_backwards(delta, next_date, holidays)
        params.update({'date': last_trading_day.strftime('%Y-%m-%d')})
        r = requests.get(DATA_API_URL, params=params).json()
        if r.get('data') is not None:
            print("API hit: testing stock trading on day")
            doc_id = db_add_date(last_trading_day.strftime('%Y-%m-%d'), db)
            db.stockdata.update_one({'_id': doc_id}, {'$set': {'data.' + ticker: r['data'][ticker]}})
            return True
    return False


def db_try_update(doc_id, ticker, r, db, date):
    try:
        db.stockdata.update_one({'_id': doc_id}, {'$set': {'data.' + ticker: r['data'][ticker]}})
        return None
    except KeyError:
        next_date = datetime.strptime(date, '%Y-%m-%d')
        found_historical = get_historical_price(next_date, db, ticker)
        if found_historical:
            print("Found historical WTD data for '" + ticker + "'.")
            return ticker
        else:
            print("Could not get WTD data for '" + ticker + "'.")
            return ticker


def already_in_db(ticker, next_date, db):
    query = db.stockdata.find_one({'$and': [{'date': next_date}, {'data.' + ticker + '.open': {"$exists": True}}]})
    return query is not None


def update_stock_db(tickers, next_date, db):
    base_params = {'date': next_date, 'api_token': API_KEY}
    doc_id = db_add_date(next_date, db)
    trimmed = [ticker for ticker in tickers if not already_in_db(ticker, next_date, db)]
    iterator = iter(trimmed)
    ticker_group = list(zip(iterator, iterator, iterator, iterator, iterator))
    failed = []
    for group in ticker_group:
        symbols = ""
        for i in range(5):
            symbols += group[i] + ","
        base_params.update({'symbol': symbols[:-1]})
        r = requests.get(DATA_API_URL, params=base_params).json()
        print("API hit: getting OHLC data")
        for i in range(5):
            failed.append(db_try_update(doc_id, group[i], r, db, next_date))
    if len(trimmed) % 5 != 0:
        missed_tickers = len(trimmed) % 5
        group = trimmed[missed_tickers * -1:]
        symbols = ""
        for i in range(missed_tickers):
            symbols += group[i] + ","
        base_params.update({'symbol': symbols[:-1]})
        r = requests.get(DATA_API_URL, params=base_params).json()
        print("API hit: getting OHLC data")
        for i in range(missed_tickers):
            failed.append(db_try_update(doc_id, group[i], r, db, next_date))
    return [ticker for ticker in failed if ticker is not None]


def get_next_trading_day(date, db):
    db_date = db.tradingday.find_one({'date': date})
    if db_date is not None:
        return db_date['next_trading_day']
    next_date = datetime.strptime(date, '%Y-%m-%d')
    params = {'api_token': environ['WTD_API'], 'symbol': 'AAPL'}
    r = {"Message": "Error! No data was found."}
    attempts = 0
    while r.get("Message") is not None:
        if r.get("message") is not None and "request limit" in r["message"]:
            raise ConnectionError("Over API limit.")
        if attempts > 7:
            msg = "Can't find next trading day after " + date + \
                  ". Either the stock market disappeared or you have internet problems."
            raise ConnectionError(msg)
        next_date += timedelta(days=1)
        params.update({'date': next_date.strftime('%Y-%m-%d')})
        r = requests.get(DATA_API_URL, params=params).json()
        print("API hit: getting valid trading day")
        attempts += 1
    db_add_trading_day(date, next_date.strftime('%Y-%m-%d'), db)
    return next_date.strftime('%Y-%m-%d')


def get_data(tickers, date):
    client = MongoClient(environ['MONGO'])
    db = client.form13f
    next_date = get_next_trading_day(date, db)
    return update_stock_db(tickers, next_date, db)
