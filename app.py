from flask import Flask, render_template
from pymongo import MongoClient
from os import environ

app = Flask(__name__)
client = MongoClient(environ['MONGODB_URI'])
db = client.get_database()
num_stock_list = [5, 15, 50]


@app.route('/')
def home():
    companies = db.companies.find()
    data = []
    for item in companies:
        form = db.forms.find_one({'$query': {'cik': item['cik']}, '$orderby': {'date': -1}})
        form['backtest'] = db.backtest.find_one({'cik': item['cik']})
        if form['backtest'].get(str(max(num_stock_list))) is not None:
            form['return'] = format(form['backtest'][str(max(num_stock_list))]['return'], '.2f')
        else:
            form['return'] = 'N/A'
        form['num_holdings'] = "{:,}".format(form['num_holdings'])
        form['total_val'] = "{:,}".format(form['total_val'])
        if form['gain'] > 0:
            form['gain_class'] = "green-gain"
        else:
            form['gain_class'] = "red-loss"
        form['gain'] = "{:,}".format(form['gain'])
        data.append(form)
    return render_template('index.html', data=data, companies=sorted(db.companies.find(), key=lambda x: x['name']))


@app.route('/faq')
def faq():
    companies = sorted(db.companies.find(), key=lambda x: x['name'])
    return render_template('faq.html', companies=companies)


@app.route('/company/<cik>')
def company(cik):
    companies = sorted(db.companies.find(), key=lambda x: x['name'])
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
    return render_template('company.html', data=data, companies=companies)


@app.route('/company/<cik>/<sec_id>')
def form_page(cik, sec_id):
    companies = sorted(db.companies.find(), key=lambda x: x['name'])
    data = db.forms.find_one({'cik': cik, 'sec_id': sec_id})
    for holding in data['holdings']:
        holding['value'] = "{:,}".format(holding['value'])
        holding['units'] = "{:,}".format(holding['units'])
    return render_template('form.html', data=data, companies=companies)


@app.route('/options')
def options():
    companies = sorted(db.companies.find(), key=lambda x: x['name'])
    data = {}
    for item in companies:
        form = db.forms.find_one({'$query': {'cik': item['cik']}, '$orderby': {'date': -1}})
        options_val = form['total_val'] - form['share_val']
        if options_val > 0:
            data[item['cik']] = {'cik': item['cik'], 'name': item['name'], 'options_val': "{:,}".format(options_val)}
            data[item['cik']]['options_pc'] = round(((form['total_val'] - form['share_val']) / form['total_val'])*100, 1)
            share_num = len([holding for holding in form['holdings'] if holding['security_type'] == 'Share'])
            data[item['cik']]['options_num'] = form['num_holdings'] - share_num
    return render_template('options.html', data=data, companies=companies)


if __name__ == '__main__':
    app.run()
