from os import environ
from pymongo import MongoClient
from strategy.stockdata import get_data, recent_open, next_trading_day, update_trading_days, DataError


client = MongoClient(environ['MONGO'])


def calculate_weights(holdings, total):
    return {holding: value / total for holding, value in holdings.items()}


def value_to_weight(largest_holdings, all_holdings):
    raw_values = {holding: all_holdings[holding] for holding in largest_holdings}
    return calculate_weights(raw_values, sum(raw_values.values()))


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
    largest_holdings = sorted(form13f['holdings']['shares'], key=form13f['holdings']['shares'].get, reverse=True)
    largest_holdings = largest_holdings[:num_stocks]
    return largest_holdings, form13f['holdings']['shares']   # Ignoring put and call options to keep it simple


def ensure_valid_data(form_name, form_date, next_date, cik, num_stocks):
    db = client.form13f
    weights = {}
    failed = []
    while len(weights) < num_stocks:
        num_stocks_new = num_stocks + len(failed)
        largest, all_holdings = db_get_form_holdings(form_name, cik, num_stocks_new, db)
        if len(failed) > num_stocks:
            raise RuntimeError("Something's wrong; data retrieval failed for maximum number of tickers: " + num_stocks)
        if len(all_holdings) < num_stocks:
            raise RuntimeError("This filing has " + str(len(all_holdings)) + " stocks, but asked for " + num_stocks_new)
        tickers = [ticker for ticker in largest if ticker not in failed]
        failed.extend(get_data(list(tickers), form_date))
        successful = [ticker for ticker in largest if ticker not in failed]
        weights = value_to_weight(successful, all_holdings)
        print("Calculated " + str(len(weights)) + " valid weights from " + str(num_stocks_new) + " tickers")
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
            open_price = data['history'][date]['open']
        elif ticker in next_tickers:        # Current price is required for all next tickers but not current
            raise DataError("Cannot backtest further (unknown buy price for '" + ticker + "').")
        else:
            open_price = recent_open(ticker, date, db)

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
            print("Sold " + str(abs(share_transaction)) + " of " + holding + " for " +
                  str(abs(round(share_transaction * prices[holding], 2))))
        else:
            print("Bought " + str(share_transaction) + " of " + holding + " for " +
                  str(round(share_transaction * prices[holding], 2)))
    return {ticker: shares for ticker, shares in portfolio.items() if shares != 0}


def rebalance(portfolio, next_weight, date):
    portfolio_value, prices = get_values(portfolio, next_weight, date)
    total = sum(portfolio_value.values())
    print("Portfolio value: $" + str(round(total, 2)) + " on " + date)
    delta = compare_weights(calculate_weights(portfolio_value, total), next_weight)
    return buy_sell(portfolio, prices, delta, total)


def backtest(cik, min_date, num_stocks, initial_bank):
    update_trading_days()
    forms, form_dates = db_get_forms(cik, min_date)
    db = client.form13f
    trading_dates = {sec_id: next_trading_day(date, db) for sec_id, date in form_dates.items()}
    to_backtest = sorted(forms)
    portfolio = {"cash": initial_bank}
    for form in to_backtest:
        next_date_index = to_backtest.index(form) + 1
        next_date = to_backtest[next_date_index] if len(to_backtest) > next_date_index else None
        weights = ensure_valid_data(form, trading_dates[form], trading_dates.get(next_date), cik, num_stocks)
        portfolio = rebalance(portfolio, weights, trading_dates[form])
        print(portfolio)
