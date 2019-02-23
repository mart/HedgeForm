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
        form['num_holdings'] = "{:,}".format(form['num_holdings'])
        form['total_val'] = "{:,}".format(form['total_val'])
        if form['gain'] > 0:
            form['gain_class'] = "green-gain"
        else:
            form['gain_class'] = "red-loss"
        form['gain'] = "{:,}".format(form['gain'])
        data.append(form)
    return render_template('index.html', data=data, companies=db.companies.find())


@app.route('/faq')
def faq():
    companies = db.companies.find()
    return render_template('faq.html', companies=companies)


@app.route('/company/<cik>')
def company(cik):
    companies = db.companies.find()
    forms = db.forms.find({'cik': cik})
    data = []
    for form in forms:
        form['num_holdings'] = "{:,}".format(form['num_holdings'])
        form['total_val'] = "{:,}".format(form['total_val'])
        if form['gain'] > 0:
            form['gain_class'] = "green-gain"
        else:
            form['gain_class'] = "red-loss"
        form['gain'] = "{:,}".format(form['gain'])
        data.append(form)
    return render_template('cik.html', data=data, companies=companies)


@app.route('/company/<cik>/<sec_id>')
def form_page(cik, sec_id):
    data = ""
    return render_template('form.html', data=data)


if __name__ == '__main__':
    app.run()
