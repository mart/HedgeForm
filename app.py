from flask import Flask, render_template
from pymongo import MongoClient
from os import environ


app = Flask(__name__)
client = MongoClient(environ['MONGO'])
db = client.form13f


@app.route('/')
def home():
    companies = db.companies.find()
    data = []
    for company in companies:
        form = db.forms.find_one({'$query': {'cik': company['cik']}, '$orderby': {'date': -1}})
        form['num_holdings'] = "{:,}".format(len(form['holdings']))
        form['total_val'] = "{:,}".format(form['total_val'])
        data.append(form)
    data = sorted(data, key=lambda item: item['total_val'], reverse=True)
    return render_template('index.html', data=data)


@app.route('/faq')
def faq():
    data = db.companies.find()
    return render_template('faq.html', data=data)


@app.route('/company/<cik>')
def company(cik):
    return ""


if __name__ == '__main__':
    app.run()
