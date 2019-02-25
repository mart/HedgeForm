from enum import Enum
import requests
from os import environ
from bs4 import BeautifulSoup
from pymongo import MongoClient


client = MongoClient(environ['MONGODB_URI'])
db = client.get_database()
MIN_13F_DATE = '2014-01-01'


class SecurityType(Enum):
    SHARE = 1
    CALL = 2
    PUT = 3


class Form13F:
    def __init__(self, cik, sec_id, name, date, link, holdings):
        self.cik = cik
        self.name = name
        self.sec_id = sec_id
        self.date = date
        self.link = link
        self.holdings = holdings
        self.share_val = sum([holding['value'] for holding in holdings if holding['security_type'] == 'Share'])
        self.total_val = sum([holding['value'] for holding in holdings])
        self.num_holdings = len(holdings)
        self.gain = False


class WebScrapeError(RuntimeError):
    pass


def valid_cusip(cusip):   # For more information, see: https://en.wikipedia.org/wiki/CUSIP
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
    return (10 - total % 10) % 10 == int(cusip[8])


def cusip_to_ticker(cusip):
    if len(cusip) < 9:
        cusip = '0'*(9 - len(cusip)) + cusip
    cusip = cusip.upper()
    if not valid_cusip(cusip):
        print('WARNING/ED: CUSIP did not match checksum: ' + cusip)
    cusip_map = db.cusipmap.find_one()
    if cusip not in cusip_map:
        print("WARNING/ED: Could not find '" + cusip + "' in CUSIP mapping. Adding as CUSIP.")
        db.bad_cusip.insert_one({'cusip': cusip})
        return cusip
    return cusip_map[cusip]


def aggregate_holdings(holdings):
    out = []
    for security_type in holdings.values():
        out.extend(security_type.values())
    return out


def get_holdings(xml):
    holdings = {SecurityType.SHARE: {}, SecurityType.PUT: {}, SecurityType.CALL: {}}
    for holding in xml("infoTable"):
        ticker = cusip_to_ticker(str(holding.find("cusip").string))
        security_type = SecurityType.SHARE
        if holding.find("putCall") is not None:
            if str(holding.find("putCall").string).upper() == 'PUT':
                security_type = SecurityType.PUT
            elif str(holding.find("putCall").string).upper() == 'CALL':
                security_type = SecurityType.CALL
        name = str(holding.find("nameOfIssuer").string).title().replace(" New", "")
        value = int(holding.find("value").string)
        units = int(holding.find("sshPrnamt").string)

        if holdings[security_type].get(ticker) is not None:
            holdings[security_type][ticker]['value'] += value
            holdings[security_type][ticker]['units'] += units
        else:
            holdings[security_type][ticker] = {"ticker": ticker, "name": name,
                                               "security_type": security_type.name.title(),
                                               "value": value, "units": units}
    return aggregate_holdings(holdings)


def already_in_db(sec_id):
    return db.forms.find_one({'sec_id': sec_id}) is not None


def get_link_and_date(soup):
    date_header = soup.select("div.formGrouping > div:nth-of-type(1)")[0].string
    if date_header != "Filing Date":
        raise WebScrapeError("SEC changed their EDGAR format! You'll have to edit the web scraper.")
    date = str(soup.select("div.formGrouping > div:nth-of-type(2)")[0].string)
    if date < MIN_13F_DATE:
        print("WARNING/ED: Skipping 13F filing from " + date + ". Filed before minimum date of: " + MIN_13F_DATE)
        return None, None
    if not soup.select("table.tableFile > tr:nth-of-type(5) > td:nth-of-type(3) > a"):
        print("WARNING/ED: Could not find XML from " + date + " Skipping.")
        return None, None
    form_link = soup.select("table.tableFile > tr:nth-of-type(5) > td:nth-of-type(3) > a")[0].get("href")
    return "https://www.sec.gov" + form_link, date


def update_gains(cik):
    dates = [form['date'] for form in db.forms.find({"cik": cik})]
    for form in db.forms.find({"cik": cik}):
        if not form['gain']:
            before_dates = [date for date in dates if date < form['date']]
            if before_dates:
                before_date = max(before_dates)
                last_form = db.forms.find_one({'cik': cik, 'date': before_date})
                gain = form['total_val'] - last_form['total_val']
                db.forms.update_one({'_id': form['_id']}, {'$set': {'gain': gain}})


def add_filings(cik, filing_links, count, name):
    updated = False
    added = 0
    for filing in filing_links:
        sec_id = filing.get('href').split("/")[5]

        if added == count:
            break
        if '[Amend]' in str(filing.parent.parent):
            continue

        if not already_in_db(sec_id):
            url = "https://www.sec.gov" + filing.get("href")
            soup = BeautifulSoup(requests.get(url).content, "html.parser")
            print("REQUEST/ED: 13F filing links page " + url)
            link, date = get_link_and_date(soup)
            if link is not None:
                updated = True
                xml = BeautifulSoup(requests.get(link).content, "xml")
                print("REQUEST/ED: 13F filing XML")
                holdings = get_holdings(xml)
                form = Form13F(cik, sec_id, name, date, link, holdings)
                db.forms.insert_one(form.__dict__)
                print("EDGAR: Added form: " + sec_id + " for " + cik)
            else:
                db.forms.insert_one({'sec_id': sec_id})
                print("WARNING/ED: Form missing from EDGAR: " + sec_id + " for " + cik)
        added += 1
    return updated


def update_filings(cik, count=20):
    if count > 40:
        raise ValueError("Cannot get more than 40 filings - XML data may not be available that far back")
    company_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=" + \
                  cik + "&type=13F&dateb=&owner=include&count=" + str(count)
    soup = BeautifulSoup(requests.get(company_url).content, "html.parser")
    print("REQUEST/ED: 13F filing company page " + cik)
    if soup.select(".companyName"):
        company_string = soup.select(".companyName")[0].text
    else:
        return False
    name = company_string.split(" CIK")[0].title()
    filing_links = soup("a", id="documentsbutton")
    updated = add_filings(cik, filing_links, count, name)
    update_gains(cik)
    if db.companies.find_one({"name": name, "cik": cik}) is None \
            and db.forms.find_one({'$and': [{'cik': cik}, {'date': {"$exists": True}}]}) is not None:
        db.companies.insert_one({"name": name, "cik": cik})
    return updated
