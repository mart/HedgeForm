from os import environ
from pymongo import MongoClient
from strategy.stockdata import get_data, db_get_recent_open, get_next_trading_day


client = MongoClient(environ['MONGO'])
cusip_map = client.form13f.cusipmap.find_one()


def cusip_to_ticker(cusip):
    if cusip not in cusip_map:
        print("Could not find '" + cusip + "' in CUSIP mapping. Ignoring.")
        return None
    return cusip_map[cusip]


def calculate_weights(holdings, total):
    return {holding: value / total for holding, value in holdings.items()}


def value_to_weight(largest_holdings, all_holdings):
    raw_values = {holding: all_holdings[holding] for holding in largest_holdings}
    by_ticker = {cusip_to_ticker(cusip): value for cusip, value in raw_values.items() if cusip_to_ticker(cusip) is not None}
    return calculate_weights(by_ticker, sum(by_ticker.values()))


def db_get_forms(cik, min_year):
    db = client.form13f
    forms = db.forms.find({'cik': cik})
    form_names = []
    portfolio_date = {}
    for form13f in forms:
        form_names.append(form13f['sec_id'])
        portfolio_date[form13f['sec_id']] = form13f['date']
    return form_names, portfolio_date


def db_get_form_holdings(form_name, cik, num_stocks, db):
    form13f = db.forms.find_one({'$and': [{'cik': cik}, {'sec_id': form_name}]})
    largest_holdings = sorted(form13f['holdings'], key=form13f['holdings'].get, reverse=True)[:num_stocks]
    return largest_holdings, form13f['holdings']


def ensure_valid_data(form_name, form_date, next_date, cik, num_stocks):
    db = client.form13f
    weights = {}
    failed = []
    while len(weights) < num_stocks:
        if len(failed) > num_stocks:
            raise RuntimeError("Something's wrong; data retrieval failed for maximum number of tickers: " + num_stocks)
        num_stocks_new = num_stocks + len(failed)
        largest, all_holdings = db_get_form_holdings(form_name, cik, num_stocks_new, db)
        tickers = [cusip_to_ticker(cusip) for cusip in largest if cusip_to_ticker(cusip) not in failed]
        failed.extend(get_data(list(tickers), form_date))
        successful = [cusip for cusip in largest if cusip_to_ticker(cusip) not in failed]
        weights = value_to_weight(successful, all_holdings)
        print("Calculated " + str(len(weights)) + " valid weights from " + str(num_stocks_new) + " tickers")
    if next_date is not None:
        get_data(list(weights.keys()), next_date)  # Failed future data is ignored to avoid lookahead bias
    return weights


def get_values(portfolio, next_weight, date):
    db = client.form13f
    data = db.stockdata.find_one({'date': get_next_trading_day(date, db)})
    holdings = list(portfolio.keys())
    holdings.extend([holding for holding in next_weight.keys()])
    holdings.remove('cash')
    portfolio_value = {'cash': portfolio['cash']}
    prices = {'cash': 1}
    for holding in holdings:
        try:
            open_price = data['data'][holding]['open']
        except KeyError:
            open_price = db_get_recent_open(holding, date, db)
            if open_price is None:
                raise KeyError("Cannot backtest further (unknown sale price for '" + holding + "').")
        if holding in portfolio:
            portfolio_value[holding] = portfolio[holding] * float(open_price)
        prices[holding] = float(open_price)
    return portfolio_value, prices


def compare_weights(current, next_weight):
    d_next = {ticker: value - (0 if current.get(ticker) is None else current.get(ticker)) for ticker, value in next_weight.items()}
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


def backtest(cik, min_year, num_stocks, initial_bank):
    forms, form_dates = db_get_forms(cik, min_year)
    to_backtest = sorted(forms)
    portfolio = {"cash": initial_bank}
    for form in to_backtest:
        next_date_index = to_backtest.index(form) + 1
        next_date = to_backtest[next_date_index] if len(to_backtest) > next_date_index else None
        weights = ensure_valid_data(form, form_dates[form], form_dates.get(next_date), cik, num_stocks)
        portfolio = rebalance(portfolio, weights, form_dates[form])
        print(portfolio)


backtest("0001037389", "", 30, 10000)
