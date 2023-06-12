from collections import defaultdict
from datetime import datetime
import json
import redis
import requests
import flask
from apscheduler.schedulers.background import BackgroundScheduler
from pymongo import MongoClient

app = flask.Flask(__name__)
cache = redis.Redis(host='192.168.1.15', port=6380)
print(cache.ping())

mongo_client = MongoClient('mongodb://contaMongo:!%40MongoConta@tccurbstads.com:27017/?authMechanism=DEFAULT')
mongo_db = mongo_client['urbs']
mongo_collection = mongo_db['localizacaoVeiculosRecentes']

def get_bus_data():
    print("getting bus data")
    response = requests.get('https://transporteservico.urbs.curitiba.pr.gov.br/getVeiculos.php?c=98ad8')
    return response.json()

def update_redis_data():
    bus_data = parse_bus_data()
    cache.delete('bus_data')
    grouped_by_line = defaultdict(list)
    for bus in bus_data:
        refresh_time = datetime.strptime(bus['refresh'], '%H:%M').time()
        refresh_datetime = datetime.combine(datetime.now().date(), refresh_time)
        bus['refresh'] = refresh_datetime
        grouped_by_line[bus['line']].append(bus)
    for bus in bus_data:
        cache.set(f"bus_{bus['id']}", f"{bus['lat']},{bus['lon']}")
    for line in grouped_by_line.keys():
        cache.set(f"line_{line}", json.dumps(grouped_by_line[line], default=str))

    mongo_collection.insert_many(bus_data)

def parse_bus_data():
    bus_data = get_bus_data()
    return [
        {
            'id': bus_data[key]['COD'],
            'lat': float(bus_data[key]['LAT']),
            'lon': float(bus_data[key]['LON']),
            'line': bus_data[key]['CODIGOLINHA'],
            'refresh': bus_data[key]['REFRESH']
        }
        for key in bus_data.keys()
    ]

scheduler = BackgroundScheduler()
scheduler.add_job(update_redis_data, 'interval', minutes=2)
scheduler.start()

update_redis_data()

@app.route('/bus', methods=['GET'])
def get_all_buses():
    bus_keys = cache.keys('bus_*')
    bus_data = []
    for key in bus_keys:
        bus_id = key.decode('utf-8')
        lat, lon = cache.get(key).decode('utf-8').split(',')
        bus_info = {
            'id': bus_id,
            'lat': float(lat),
            'lon': float(lon)
        }
        bus_data.append(bus_info)
    return flask.jsonify(bus_data)

@app.route('/bus/<bus_id>', methods=['GET'])
def get_bus(bus_id):
    bus_data = cache.get(bus_id)
    if bus_data is None:
        return flask.jsonify({'error': 'Bus not found'}), 404
    
    lat, lon = bus_data.decode('utf-8').split(',')
    response = {
        'lat': float(lat),
        'lon': float(lon)
    }
    return flask.jsonify(response)

@app.route('/line/<line_id>', methods=['GET'])
def get_line(line_id):
    line_data = cache.get(f"line_{line_id}")
    if line_data is None:
        return flask.jsonify({'error': 'Line not found'}), 404
    return line_data

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
