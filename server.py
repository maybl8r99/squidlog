from flask import Flask, render_template, jsonify, Response
from Schema import *
from mongoengine import *
import time, json
from datetime import datetime, timedelta
from bson import json_util

dbname = 'squidlog'

app = Flask(__name__)
app.debug=True
conn = connect(dbname)
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/listbyhour/<fromdate>/<todate>')
def list(fromdate,todate):
    _date_from = datetime.fromtimestamp(time.mktime(time.strptime(fromdate,'%Y%m%d')))
    _date_to = datetime.fromtimestamp(time.mktime(time.strptime(todate,'%Y%m%d')))+timedelta(days=1)
    data = DomainBreakdown._get_collection().aggregate(
        [
            {
                '$match':{'$and': [{'datetimefrom':{'$gte':_date_from}},{'datetimeto':{'$lt':_date_to}}]}
            },
            {
                '$group':{'_id':{'dt':'$datetimefrom'},'count':{'$sum':1},'datetimeto':{'$max':'$datetimeto'}}
            },
            {   '$sort':{'datetimefrom':1}
            }
        ]
    )
    return Response(json_util.dumps(data),mimetype='application/json')
    #return (json.dumps(data,sort_keys=True,indent=4,default=json_util.default))

if __name__ == '__main__':
    app.run()
