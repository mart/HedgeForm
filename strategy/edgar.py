import requests
from os import environ
from bs4 import BeautifulSoup
from pymongo import MongoClient


class Form13F:
    def __init__(self, cik, sec_id, date, link, holdings):
        self.cik = cik
        self.sec_id = sec_id
        self.date = date
        self.link = link
        # cusip: value
        self.holdings = holdings
        self.total_val = sum(self.holdings.values())


class WebScrapeError(RuntimeError):
    def __init__(self, message):
        super().__init__(message)


def get_holdings(link):
    xml = BeautifulSoup(requests.get(link).content, "xml")
    holdings = {}
    for holding in xml("infoTable"):
        holdings[str(holding.find("cusip").string)] = int(holding.find("value").string)
    return holdings


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


def update_filings(cik, count=20):
    client = MongoClient(environ['MONGO'])
    db = client.form13f
    company_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=" + \
                  cik + "&type=13F&dateb=&owner=include&count=" + str(count)
    soup = BeautifulSoup(requests.get(company_url).content, "html.parser")
    filing_links = soup("a", id="documentsbutton")[:count]
    for filing in filing_links:
        sec_id = filing.get("href").split("/")[5]
        if not already_in_db(sec_id, db):
            link, date = get_link_and_date(filing.get("href"))
            holdings = get_holdings(link)
            form = Form13F(cik, sec_id, date, link, holdings)
            db.forms.insert_one(form.__dict__)
            print("Added form: " + sec_id + " for " + cik)
