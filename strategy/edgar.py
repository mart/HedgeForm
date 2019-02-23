import requests
from os import environ
from bs4 import BeautifulSoup
from pymongo import MongoClient


client = MongoClient(environ['MONGO'])


class Form13F:
    def __init__(self, cik, sec_id, name, date, link, holdings):
        self.cik = cik
        self.name = name
        self.sec_id = sec_id
        self.date = date
        self.link = link
        self.holdings = holdings
        self.share_val = sum(self.holdings['shares'].values())
        self.total_val = \
            sum(self.holdings['shares'].values()) + \
            sum(self.holdings['puts'].values()) + \
            sum(self.holdings['calls'].values())
        self.num_holdings = count_holdings(self.holdings['shares'], self.holdings['puts'], self.holdings['calls'])
        self.gain = False


def count_holdings(shares, puts, calls):
    tickers = list(shares.keys())
    tickers.extend(list(puts.keys()))
    tickers.extend(list(calls.keys()))
    uniques = set(tickers)
    return len(uniques)


class Holdings:
    def __init__(self, shares, puts, calls, share_units, put_units, call_units):
        self.shares = shares
        self.puts = puts
        self.calls = calls
        self.share_units = share_units
        self.put_units = put_units
        self.call_units = call_units


class WebScrapeError(RuntimeError):
    pass


def check_cusip(cusip):   # For more information, see: https://en.wikipedia.org/wiki/CUSIP
    cusip = cusip if len(cusip) > 8 else '0'*(len(cusip) - 9) + cusip
    total = 0
    for i in range(len(cusip) - 1):
        if cusip[i].isdigit():
            val = int(cusip[i])
        elif cusip[i] == "#":
            val = 38
        elif cusip[i] == "@":
            val = 37
        elif cusip[i] == "*":
            val = 36
        else:
            val = ord(cusip[i]) - 55

        if i % 2 != 0:  # Odd indices are even digits
            val *= 2
        total += val // 10 + val % 10
    if (10 - total % 10) % 10 != int(cusip[8]):
        print('CUSIP did not match checksum: ' + cusip)
    return cusip


def cusip_to_ticker(cusip):
    cusip = check_cusip(cusip)
    db = client.form13f
    cusip_map = db.cusipmap.find_one()
    if cusip not in cusip_map:
        print("Could not find '" + cusip + "' in CUSIP mapping. Adding as CUSIP.")
        db.bad_cusip.insert_one({'cusip': cusip})
        return cusip
    return cusip_map[cusip]


def get_holdings(link):
    xml = BeautifulSoup(requests.get(link).content, "xml")
    shares = {}
    share_units = {}
    put_options = {}
    put_units = {}
    call_options = {}
    call_units = {}
    for holding in xml("infoTable"):
        ticker = cusip_to_ticker(str(holding.find("cusip").string))
        if holding.find("putCall") is not None:
            if str(holding.find("putCall").string) == 'PUT':
                put_options[ticker] = put_options.get(ticker, 0) + int(holding.find("value").string)
                put_units[ticker] = put_units.get(ticker, 0) + int(holding.find("sshPrnamt").string)
            elif str(holding.find("putCall").string) == 'CALL':
                call_options[ticker] = call_options.get(ticker, 0) + int(holding.find("value").string)
                call_units[ticker] = call_units.get(ticker, 0) + int(holding.find("sshPrnamt").string)
        else:
            shares[ticker] = shares.get(ticker, 0) + int(holding.find("value").string)
            share_units[ticker] = share_units.get(ticker, 0) + int(holding.find("sshPrnamt").string)
    return Holdings(shares, put_options, call_options, share_units, put_units, call_units)


def already_in_db(sec_id, db):
    return db.forms.find_one({'sec_id': sec_id}) is not None


def get_link_and_date(filing_link):
    url = "https://www.sec.gov" + filing_link
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    form_link = soup.select("table.tableFile > tr:nth-of-type(5) > td:nth-of-type(3) > a")[0].get("href")
    date_header = soup.select("div.formGrouping > div:nth-of-type(1)")[0].string
    if date_header != "Filing Date":
        raise WebScrapeError("SEC changed their EDGAR format! You'll have to edit the web scraper.")
    date = str(soup.select("div.formGrouping > div:nth-of-type(2)")[0].string)
    return "https://www.sec.gov" + form_link, date


def update_gains(cik):
    db = client.form13f
    dates = [form['date'] for form in db.forms.find({"cik": cik})]
    for form in db.forms.find({"cik": cik}):
        if not form['gain']:
            before_dates = [date for date in dates if date < form['date']]
            if before_dates:
                before_date = max(before_dates)
                last_form = db.forms.find_one({'cik': cik, 'date': before_date})
                gain = form['total_val'] - last_form['total_val']
                db.forms.update_one({'_id': form['_id']}, {'$set': {'gain': gain}})


def update_filings(cik, count=20):
    if count > 40:
        raise ValueError("Cannot get more than 40 filings - XML data may not be available that far back")
    db = client.form13f
    company_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=" + \
                  cik + "&type=13F&dateb=&owner=include&count=" + str(count)
    soup = BeautifulSoup(requests.get(company_url).content, "html.parser")
    company_string = soup.select(".companyName")[0].text
    name = company_string.split(" CIK")[0].title()
    filing_links = soup("a", id="documentsbutton")
    added = 0
    for filing in filing_links:
        if added == count:
            break
        if '[Amend]' in str(filing.parent.parent):
            continue
        sec_id = filing.get('href').split("/")[5]
        if not already_in_db(sec_id, db):
            link, date = get_link_and_date(filing.get("href"))
            holdings = get_holdings(link).__dict__
            form = Form13F(cik, sec_id, name, date, link, holdings)
            db.forms.insert_one(form.__dict__)
            print("Added form: " + sec_id + " for " + cik)
        added += 1
    update_gains(cik)
    if db.companies.find_one({"name": name, "cik": cik}) is None:
        db.companies.insert_one({"name": name, "cik": cik})
