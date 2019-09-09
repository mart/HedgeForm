import strategy.stockdata as sd
from pytest import approx
from os import environ

API_KEY = environ['TIINGO_API']
MIN_DATE = '2014-01-01'
sd.get_data('SHPG', '2019-02-11')


def test_specific_open():
    assert sd.specific_open('AMZN', '2019-07-29') == approx(1930)


def test_specific_open_fail():
    assert sd.specific_open('AAPL', '2219-02-11') is None


def test_data_request():
    req = sd.data_request({'token': API_KEY, 'startDate': '2018-01-12', 'endDate': '2018-01-18'}, 'SHPG')
    assert req == {'name': 'SHPG',
                   'history': {'2018-01-12': 147.2010065855,
                               '2018-01-16': 144.7101111056,
                               '2018-01-17': 144.6207961283,
                               '2018-01-18': 142.040585671}}


def test_data_request_bad_param():
    try:
        req = sd.data_request({'token': API_KEY, 'startDate': 'abc', 'endDate': '2018-01-18'}, 'SHPG')
        assert False
    except sd.APIError:
        pass    # expected


def test_data_request_other_api_error():
    try:
        req = sd.data_request({'token': 111, 'startDate': '2018-01-12', 'endDate': '2018-01-18'}, 'SHPG')
        assert False
    except sd.APIError:
        pass    # expected


def test_recent_open_exact():
    assert sd.recent_open('SHPG', '2019-02-07') == approx(179.2)


def test_recent_open_recent():
    assert sd.recent_open('SHPG', '2019-01-06') == approx(176.6)


def test_recent_open_future():
    assert sd.recent_open('SHPG', '2218-01-15') == approx(179.2)


def test_recent_open_fail():
    assert sd.recent_open('FPACU', '2018-01-15') is None


def test_already_in_db():
    assert sd.already_in_db('SHPG', '2019-01-07')


def test_update_stock_db_nofail():
    assert sd.update_stock_db(['SHPG', 'AAPL'], '2019-01-08') == []


def test_update_stock_db_1fail():
    assert sd.update_stock_db(['SHPG', 'FPACU', 'AAPL'], '2019-01-08') == ['FPACU']


def test_update_stock_db_2fail():
    assert sd.update_stock_db(['SHPG', 'FPACU', 'AAPL'], '2019-01-09') == ['SHPG', 'FPACU']


def test_next_trading_day_weekday():
    assert sd.next_trading_day('2019-01-08') == '2019-01-09'


def test_next_trading_day_weekend():
    assert sd.next_trading_day('2019-02-01') == '2019-02-04'


def test_next_trading_day_holiday():
    assert sd.next_trading_day('2018-12-31') == '2019-01-02'
