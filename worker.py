from strategy.edgar import update_filings
from strategy.backtest import backtest
from strategy.cusip import update_cusip_map


# update_cusip_map()


update_list = ['0001067983', '0001037389']


for cik in update_list:
    update_filings(cik)
    # backtest(cik, '2014-01-01', 30, 100000)
