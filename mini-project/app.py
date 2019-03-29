from flask import Flask, request, jsonify
from cassandra.cluster import Cluster
import requests

cluster = Cluster(['cassandra'])
session = cluster.connect()
app = Flask(__name__)


@app.route('/<mydata>', methods=['GET'])
def hello(mydata):
    crime_url_template ='https://data.police.uk/api/crimes-street/all-crime?lat={lat}&lng={lng}&date={data}'
    my_latitude = '51.52369'
    my_longitude = '-0.0395857'
    crime_url = crime_url_template.format(lat = my_latitude,lng = my_longitude,data = mydata)
    res = requests.get(crime_url)
    if res.ok:
        crimes = res.json()
    else:
        print(res.reason)
    cat_url_template = 'https://data.police.uk/api/crime-categories?date={date}'
    res = requests.get(cat_url_template.format(date = mydata))
    if res.ok:
        cate_json = res.json()
    else:
        print(res.reason)

    cat = {categ["url"]:categ["name"] for categ in cate_json}
    crime_cate_stats = dict.fromkeys(cat.keys(), 0)
    crime_cate_stats.pop("all-crime")
    for crime in crimes:
        crime_cate_stats[crime["cat"]] += 1
    for key in crime_cate_stats:
        rows = session.execute( """insert into crime.category(month,name,count) values('{}','{}',{})""".format(mydata,key,crime_cate_stats[key]))
    return('<h1>crime data for {} is created</h1>'.format(mydata))

@app.route('/<data>/<name>', methods=['GET'])
def profile(data,name):
    rows = session.execute( """select * from crime.category where month = '{}' and name= '{}' allow filtering""".format(data,name,))
    for crime in rows:
        return jsonify(crime)
        # return('<h1>{} in {} happened {} times!</h1>'.format(name, data, crime.count))
    return('<h1>That kind of crime does not exist!</h1>')
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
