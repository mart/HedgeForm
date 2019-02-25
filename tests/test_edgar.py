import strategy.edgar as ed
import json
from bs4 import BeautifulSoup


def test_valid_cusip_one():
    assert ed.valid_cusip("037833100")


def test_valid_cusip_invalid():
    assert not ed.valid_cusip("037833101")


def test_cusip_to_ticker_one():
    assert ed.cusip_to_ticker("037833100") == "AAPL"


def test_cusip_to_ticker_missing_zero():
    assert ed.cusip_to_ticker("37833100") == "AAPL"


def test_cusip_to_ticker_bad():
    assert ed.cusip_to_ticker("AAAAAAAA0") == "AAAAAAAA0"


def test_aggregate_holdings():
    holdings = {ed.SecurityType.SHARE: {"AOI": {
        "ticker": "AOI",
        "name": "Alliance One Intl Inc",
        "security_type": "Share",
        "value": 18640,
        "units": 6383641
        }},
        ed.SecurityType.PUT: {"ALDW": {
            "ticker": "ALDW",
            "name": "Alon Usa Partners Lp",
            "security_type": "Share",
            "value": 5484,
            "units": 329188
        }},
        ed.SecurityType.CALL: {"AIGWS": {
            "ticker": "AIGWS",
            "name": "American Intl Group Inc",
            "security_type": "Share",
            "value": 13418,
            "units": 656443
        }}
    }
    with open('./data/holdings4.json', "r") as file:
        expected_holdings = json.load(file)
    assert ed.aggregate_holdings(holdings) == expected_holdings['holdings']


def test_get_holdings_plain():
    with open('./data/form13_1.xml', "r") as xml:
        test = ed.get_holdings(BeautifulSoup(xml, "xml"))
    with open('./data/holdings1.json', "r") as file:
        expected_holdings = json.load(file)
    assert test == expected_holdings['holdings']


def test_get_holdings_call_option():
    with open('./data/form13_2.xml', "r") as xml:
        test = ed.get_holdings(BeautifulSoup(xml, "xml"))
    with open('./data/holdings2.json', "r") as file:
        expected_holdings = json.load(file)
    assert test == expected_holdings['holdings']


def test_get_holdings_put_option():
    with open('./data/form13_3.xml', "r") as xml:
        test = ed.get_holdings(BeautifulSoup(xml, "xml"))
    with open('./data/holdings3.json', "r") as file:
        expected_holdings = json.load(file)
    assert test == expected_holdings['holdings']


def test_get_link_and_date():
    with open('./data/filing_page.html') as page:
        soup = BeautifulSoup(page, "html.parser")
        link, date = ed.get_link_and_date(soup)
    assert link == 'https://www.sec.gov/Archives/edgar/data/1327388/000139834414005663/fp0012104_13fhr-table.xml'
    assert date == '2014-11-05'


def test_get_link_and_date_old():
    with open('./data/filing_page_old.html') as page:
        soup = BeautifulSoup(page, "html.parser")
        link, date = ed.get_link_and_date(soup)
    assert link is None
    assert date is None


def test_get_link_and_date_none():
    with open('./data/filing_page_none.html') as page:
        soup = BeautifulSoup(page, "html.parser")
        link, date = ed.get_link_and_date(soup)
    assert link is None
    assert date is None


