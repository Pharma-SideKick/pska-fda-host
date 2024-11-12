import json
from flask import Flask
from flask import request
from flask_sqlalchemy import SQLAlchemy
from SPLSupport import DB

app = Flask(__name__)
database = DB()
'''
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///SPL.db'

sa = SQLAlchemy(app)

class SPLs(sa.Model):
    id = sa.Column('id', sa.Integer, primary_key=True)
    brand_name = sa.Column(sa.String)
    generic_name = sa.Column(sa.String)
    when_using = sa.Column(sa.String)
    do_not_us = sa.Column(sa.String)
    stop_us = sa.Column(sa.String)
    adverse_reactions = sa.Column(sa.String)
    drug_iteractions = sa.Column(sa.String)

'''
@app.route('/medSearch', methods=['GET'])
def GetMed():
    med = request.args.get('med')
    #print(f'Med Search Parameter: {med}')
    result = database.select_med(med).fetchone()
    #print(f'result: {result}')
    result = dict(id = result[0],
                  brand_name = result[1],
                  generic_name = result[2],
                  when_using = result[3],
                  do_not_use = result[4],
                  stop_use = result[5],
                  adverse_reactions = result[6],
                  drug_iteractions = result[7]
                  )
    #print(result)
    return json.dumps(result)

if __name__ == '__main__':
    app.run(debug=True)