import strategy.backtest as bt
import strategy.stockdata as sd
from pytest import approx


def test_value_to_weight_simple():
    holdings = {"AAPL": 30, "WMT": 20, "BA": 22, "AIG": 28}
    weights = {"AAPL": 0.30, "WMT": 0.20, "BA": 0.22, "AIG": 0.28}
    assert bt.value_to_weight(holdings) == approx(weights, abs=1e-15)


def test_value_to_weight():
    holdings = {"AAPL": 50.30, "WMT": 20.08, "BA": 22.82, "AIG": 14.17}
    weights = {"AAPL": 50.30 / 107.37, "WMT": 20.08 / 107.37, "BA": 22.82 / 107.37, "AIG": 14.17 / 107.37}
    assert bt.value_to_weight(holdings) == approx(weights, abs=1e-15)


def test_db_get_forms_two_old():
    form_dates = {'000108514614001202': '2014-05-15', '000108514614000711': '2014-02-14'}
    result = bt.db_get_forms("0001040273", '2014-01-01', '2014-06-01')
    assert result == form_dates


def test_db_get_forms_one_new():
    form_dates = {'000108514619000438': '2019-02-08'}
    result = bt.db_get_forms("0001040273", '2019-01-01', '2019-03-01')
    assert result == form_dates


def test_db_get_form_holdings():
    holdings = {'ADBE': 203616, 'AXP': 276428, 'BAX': 1842960, 'CPB': 692790, 'CI': 109204, 'STZ': 281435,
                'DHR': 320703, 'DOV': 43634, 'DWDP': 516360, 'FPAC': 158337, 'FPACU': 40360, 'IQV': 189357, 'SHY': 3336,
                'KDMN': 19568, 'MRK': 212038, 'PYPL': 338462, 'SPGI': 135952, 'CRM': 160940, 'SHPG': 299247,
                'BID': 264732, 'V': 197910, 'WP': 201011}
    assert bt.db_get_form_holdings("000108514619000438", "0001040273") == holdings


def test_find_valid_tickers():
    holdings = {'ADBE': 203616, 'AXP': 276428, 'BAX': 1842960, 'CPB': 692790, 'CI': 109204, 'STZ': 281435,
                'DHR': 320703, 'DOV': 43634, 'DWDP': 516360, 'FPAC': 158337, 'FPACU': 40360, 'IQV': 189357, 'SHY': 3336,
                'KDMN': 19568, 'MRK': 212038, 'PYPL': 338462, 'SPGI': 135952, 'CRM': 160940, 'SHPG': 299247,
                'BID': 264732, 'V': 197910, 'WP': 201011}
    weights, failed = bt.find_valid_tickers(holdings, 50, [], '2019-02-11')
    del holdings['FPACU']
    del holdings['SHPG']
    expected_weights = bt.value_to_weight(holdings)
    assert weights == approx(expected_weights, abs=1e-15)
    assert failed == ['SHPG', 'FPACU']


def test_find_valid_tickers_15():
    holdings = {'ADBE': 203616, 'AXP': 276428, 'BAX': 1842960, 'CPB': 692790, 'CI': 109204, 'STZ': 281435,
                'DHR': 320703, 'DOV': 43634, 'DWDP': 516360, 'FPAC': 158337, 'FPACU': 40360, 'IQV': 189357, 'SHY': 3336,
                'KDMN': 19568, 'MRK': 212038, 'PYPL': 338462, 'SPGI': 135952, 'CRM': 160940, 'SHPG': 299247,
                'BID': 264732, 'V': 197910, 'WP': 201011}
    trimmed = {'ADBE': 203616, 'AXP': 276428, 'BAX': 1842960, 'CPB': 692790, 'STZ': 281435,
               'DHR': 320703, 'DWDP': 516360, 'IQV': 189357,
               'MRK': 212038, 'PYPL': 338462, 'CRM': 160940,
               'BID': 264732, 'V': 197910, 'WP': 201011}
    weights, failed = bt.find_valid_tickers(holdings, 15, [], '2019-02-11')
    expected_weights = bt.value_to_weight(trimmed)
    assert weights == approx(expected_weights, abs=1e-15)
    assert failed == ['SHPG']


def test_find_valid_tickers_failed():
    holdings = {'ADBE': 203616, 'AXP': 276428, 'BAX': 1842960, 'CPB': 692790, 'CI': 109204, 'STZ': 281435,
                'DHR': 320703, 'DOV': 43634, 'DWDP': 516360, 'FPAC': 158337, 'FPACU': 40360, 'IQV': 189357, 'SHY': 3336,
                'KDMN': 19568, 'MRK': 212038, 'PYPL': 338462, 'SPGI': 135952, 'CRM': 160940, 'SHPG': 299247,
                'BID': 264732, 'V': 197910, 'WP': 201011}
    trimmed = {'ADBE': 203616, 'AXP': 276428, 'BAX': 1842960, 'CPB': 692790, 'STZ': 281435,
               'DHR': 320703, 'DWDP': 516360, 'FPAC': 158337, 'IQV': 189357,
               'MRK': 212038, 'PYPL': 338462, 'CRM': 160940,
               'BID': 264732, 'V': 197910, 'WP': 201011}
    weights, failed = bt.find_valid_tickers(holdings, 16, ['SHPG'], '2019-02-11')
    expected_weights = bt.value_to_weight(trimmed)
    assert weights == approx(expected_weights, abs=1e-15)
    assert failed == ['SHPG']


def test_ensure_valid_data():
    trimmed = {'ADBE': 203616, 'AXP': 276428, 'BAX': 1842960, 'CPB': 692790, 'STZ': 281435,
               'DHR': 320703, 'DWDP': 516360, 'FPAC': 158337, 'IQV': 189357,
               'MRK': 212038, 'PYPL': 338462, 'CRM': 160940,
               'BID': 264732, 'V': 197910, 'WP': 201011}
    expected_weights = bt.value_to_weight(trimmed)
    weights = bt.ensure_valid_data('000108514619000438', '2019-02-11', None, "0001040273", 15)
    assert weights == expected_weights


def test_get_values_missing_sell():
    date = '2219-02-11'
    portfolio = {"AAPL": 50, "WMT": 20, "BA": 22, "AIG": 14, "cash": 1}
    try:
        bt.get_values(portfolio, {"AAPL": 1.0}, date)
        assert False
    except bt.DataError:
        pass    # expected


def test_get_values_missing_buy():
    date = '2019-02-11'
    portfolio = {"AAPL": 50, "WMT": 20, "BA": 22, "AIG": 14, "cash": 1}
    try:
        bt.get_values(portfolio, {"AAPL": 0.0, "AAAAAAAA0": 1.0}, date)
        assert False
    except bt.DataError:
        pass  # expected


def test_compare_weights_simple():
    current = {"AAPL": 0.20, "WMT": 0.30, "BA": 0.22, "AIG": 0.28}
    next_weight = {"AAPL": 0.70, "WMT": 0.20, "NFLX": 0.10}
    delta = {"AAPL": 0.50, "NFLX": 0.10, "WMT": -0.10, "BA": -0.22, "AIG": -0.28}
    assert bt.compare_weights(current, next_weight) == approx(delta)


def test_compare_weights_complex():
    current = {"AAPL": 0.2760664185093840, "WMT": 0.0280680852812017,
               "BA": 0.3053757792818500, "AIG": 0.1439952931302880,
               "NFLX": 0.2464944237972760}
    next_weight = {"AAPL": 0.5, "WMT": 0.3, "BA": 0.1, "AIG": 0.0, "NFLX": 0.10}
    delta = {"AAPL": 0.223933581490616, "WMT": 0.271931914718798, "BA": -0.205375779281850,
             "AIG": -0.143995293130288, "NFLX": -0.146494423797276}
    assert bt.compare_weights(current, next_weight) == approx(delta)


def test_buy_sell_simple():
    portfolio_value = {"AAPL": 20, "WMT": 30, "BA": 22, "AIG": 28, "cash": 0.0}
    total = sum(portfolio_value.values())       # 100, portfolio_value included for reference
    portfolio = {"AAPL": 20, "WMT": 30, "BA": 22, "AIG": 28, "cash": 0}
    prices = {"AAPL": '1.0', "AIG": '1.0', "BA": '1.0', "WMT": '1.0', "NFLX": '1.0', 'cash': '1.0'}
    delta = {"AAPL": 0.50, "NFLX": 0.10, "WMT": -0.10, "BA": -0.22, "AIG": -0.28}
    expected_portfolio = {"AAPL": 70, "WMT": 20, "NFLX": 10}
    assert bt.buy_sell(portfolio, prices, delta, total) == expected_portfolio


def test_buy_sell_prices():
    portfolio_value = {"AAPL": 20, "WMT": 30, "BA": 22, "AIG": 28, "cash": 0.0}
    total = sum(portfolio_value.values())       # 100, portfolio_value included for reference
    portfolio = {"AAPL": 40, "WMT": 30, "BA": 22, "AIG": 28, "cash": 0}
    prices = {"AAPL": '0.5', "AIG": '1.0', "BA": '1.0', "WMT": '1.0', "NFLX": '2.0', 'cash': '1.0'}
    delta = {"AAPL": 0.50, "NFLX": 0.10, "WMT": -0.10, "BA": -0.22, "AIG": -0.28}
    expected_portfolio = {"AAPL": 140, "WMT": 20, "NFLX": 5}
    assert bt.buy_sell(portfolio, prices, delta, total) == expected_portfolio


def test_buy_sell_complex():
    portfolio_value = {"AAPL": 110*171.05, "WMT": 20*95.65, "BA": 51*408.10,
                       "AIG": 230*42.67, "NFLX": 350.0*48, "cash": 0.0}
    total = sum(portfolio_value.values())       # 68155.70, portfolio_value included for reference
    portfolio = {"AAPL": 110, "WMT": 20, "BA": 51, "AIG": 230, "NFLX": 48, "cash": 0}
    prices = {"AAPL": 171.05, "AIG": 42.67, "BA": 408.10, "WMT": 95.65, "NFLX": 350.0, 'cash': 1.0}
    delta = {"AAPL": 0.223933581490616, "WMT": 0.271931914718798, "BA": -0.205375779281850,
             "AIG": -0.143995293130288, "NFLX": -0.146494423797276}
    expected_portfolio = {"AAPL": 199, "WMT": 214, "BA": 17, "NFLX": 19, "cash": 59.95}
    assert bt.buy_sell(portfolio, prices, delta, total) == approx(expected_portfolio)


def test_backtest_nine():
    expected_backtest = {"num_stocks": "9",
                         "min_date": '2018-08-01',
                         'return': -7.95}
    num, backtest = bt.backtest("0001040273", '2018-08-01', '2019-02-08', 9, 100000)
    assert num == "9"
    assert backtest == expected_backtest


def test_backtest_five():
    expected_backtest = {"num_stocks": "5",
                         "min_date": '2018-08-01',
                         'return': -4.92}
    num, backtest = bt.backtest("0001040273", '2018-08-01', '2019-02-08', 5, 100000)
    assert num == "5"
    assert backtest == expected_backtest
