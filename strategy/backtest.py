from os import environ
from pymongo import MongoClient
from strategy.stockdata import get_data, recent_open, next_trading_day, DataError

client = MongoClient(environ['MONGO'])
MIN_13F_DATE = '2014-01-01'


def value_to_weight(holdings):
    total = sum(holdings.values())
    return {holding: value / total for holding, value in holdings.items()}


def db_get_forms(cik, min_date):
    db = client.form13f
    forms = db.forms.find({'cik': cik})
    form_names = []
    portfolio_date = {}
    for form13f in forms:
        if form13f['date'] < min_date:
            continue
        form_names.append(form13f['sec_id'])
        portfolio_date[form13f['sec_id']] = form13f['date']
    return form_names, portfolio_date


def db_get_form_holdings(form_name, cik, num_stocks, db):
    form13f = db.forms.find_one({'$and': [{'cik': cik}, {'sec_id': form_name}]})
    share_holdings = {holding['ticker']: holding['value'] for holding in form13f['holdings']
                      if holding['security_type'] == 'Share'}
    largest_tickers = sorted(share_holdings, key=share_holdings.get, reverse=True)[:num_stocks]
    largest_holdings = {ticker: share_holdings[ticker] for ticker in largest_tickers}
    return largest_holdings, share_holdings   # Ignoring put and call options to keep it simple


def ensure_valid_data(form_name, form_date, next_date, cik, num_stocks):
    db = client.form13f
    weights = {}
    failed = []
    while len(weights) < num_stocks:
        if len(failed) > num_stocks:
            raise RuntimeError("Data retrieval failed for max number of tickers: " + str(num_stocks))
        num_stocks_new = num_stocks + len(failed)
        largest, all_holdings = db_get_form_holdings(form_name, cik, num_stocks_new, db)
        tickers = [ticker for ticker in largest.keys() if ticker not in failed]
        failed.extend(get_data(tickers, form_date))
        successful = {ticker: value for ticker, value in largest.items() if ticker not in failed}
        weights = value_to_weight(successful)
        print("BACKTEST: " + str(len(weights)) + " valid weights from " + str(num_stocks_new) + " tickers")
        if len(all_holdings) < num_stocks:
            print("WARNING/BT: This filing has " + str(len(all_holdings)) +
                  " stocks, but asked for " + str(num_stocks_new) + ". Using all available.")
            break
    if next_date is not None:
        get_data(list(weights.keys()), next_date)  # Failed future data is ignored to avoid lookahead bias
    return weights


def get_values(portfolio, next_weight, date):
    db = client.form13f
    tickers = list(portfolio.keys())
    next_tickers = [ticker for ticker in next_weight.keys()]
    tickers.extend(next_tickers)
    tickers.remove('cash')
    portfolio_value = {'cash': portfolio['cash']}
    prices = {'cash': 1}
    for ticker in tickers:
        data = db.stockdata.find_one({'$and': [{'name': ticker}, {'history.' + date: {"$exists": True}}]})
        if data is not None:
            open_price = data['history'][date]['adjOpen']
        elif ticker in next_tickers:        # Current price is required for all next tickers but not current
            raise DataError("Cannot backtest further (unknown buy price for '" + ticker + "').")
        else:
            open_price = recent_open(ticker, date)

        if open_price is None:              # Need at least a price from some time in the past for current tickers
            raise DataError("Cannot backtest further (unknown sale price for '" + ticker + "').")
        if ticker in portfolio:
            portfolio_value[ticker] = portfolio[ticker] * float(open_price)
        prices[ticker] = float(open_price)
    return portfolio_value, prices


def compare_weights(current, next_weight):
    d_next = {ticker: value - (0 if current.get(ticker) is None else current.get(ticker))
              for ticker, value in next_weight.items()}
    delta = {ticker: value * -1 for ticker, value in current.items()}
    delta.update(d_next)
    return delta


def buy_sell(portfolio, str_prices, delta, total):
    prices = {ticker: float(price) for ticker, price in str_prices.items()}
    for holding in delta:           # Buying and selling at the same time is not practical without margin
        share_transaction = round((delta[holding] * total) / prices[holding])
        if holding not in portfolio:
            portfolio[holding] = share_transaction
        else:
            portfolio[holding] += share_transaction
        portfolio['cash'] -= share_transaction * prices[holding]
        if share_transaction < 0:
            print("BACKTEST: Sold " + str(abs(share_transaction)) + " of " + holding + " for " +
                  str(abs(round(share_transaction * prices[holding], 2))))
        else:
            print("BACKTEST: Bought " + str(share_transaction) + " of " + holding + " for " +
                  str(round(share_transaction * prices[holding], 2)))
    return {ticker: shares for ticker, shares in portfolio.items() if shares != 0}


def rebalance(portfolio, next_weight, date):
    portfolio_value, prices = get_values(portfolio, next_weight, date)
    total = sum(portfolio_value.values())
    print("BACKTEST: Portfolio value: $" + str(round(total, 2)) + " on " + date)
    delta = compare_weights(value_to_weight(portfolio_value), next_weight)
    return buy_sell(portfolio, prices, delta, total), total


def backtest(cik, min_date, num_stocks, initial_bank):
    if min_date < MIN_13F_DATE:
        print("WARNING/BT: Minimum date " + min_date + " less than global minimum. Using " + MIN_13F_DATE + " instead.")
        min_date = MIN_13F_DATE
    forms, form_dates = db_get_forms(cik, min_date)
    db = client.form13f
    total = 0
    trading_dates = {sec_id: next_trading_day(date) for sec_id, date in form_dates.items()}
    to_backtest = sorted(forms)
    portfolio = {"cash": initial_bank}
    for form in to_backtest:
        next_date_index = to_backtest.index(form) + 1
        next_date = to_backtest[next_date_index] if len(to_backtest) > next_date_index else None
        weights = ensure_valid_data(form, trading_dates[form], trading_dates.get(next_date), cik, num_stocks)
        portfolio, total = rebalance(portfolio, weights, trading_dates[form])
        print("BACKTEST: " + str(portfolio))
    db.backtest.update_one({'cik': cik},
                           {'$set': {str(num_stocks):
                                     {'num_stocks': str(len(portfolio) - 1),
                                      'min_date': min_date,
                                      'return': round(((total / initial_bank)-1)*100, 2)}}},
                           upsert=True)
