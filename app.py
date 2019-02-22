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
    for item in companies:
        form = db.forms.find_one({'$query': {'cik': item['cik']}, '$orderby': {'date': -1}})
        form['num_holdings'] = "{:,}".format(len(form['holdings']['shares']))
        form['total_val'] = "{:,}".format(form['total_val'])
        if form['gain'] > 0:
            form['gain_class'] = "green-gain"
        else:
            form['gain_class'] = "red-loss"
        form['gain'] = "{:,}".format(form['gain'])
        data.append(form)
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
