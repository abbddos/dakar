from confluent_kafka import Consumer 
import json
from queue import Queue
from flask import Flask, jsonify
import requests
import numpy as np 
import pandas as pd

def get_mean(x):
    return np.array(x).mean()

def consume_rally_data(topic, batch_size):
    conf = {
        'bootstrap.servers':'localhost:9092',
        'group.id':'mygroup',
        'auto.offset.reset':'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    data_queue = Queue()


    def consumer_loop():
        try:
            buffer = []
            while True:
                msg = consumer.poll(0.1)

                if msg is None:
                    continue 
                if msg.error():
                    print(f'Consumer Error: {msg.error()}')
                    continue
                data = json.loads(msg.value().decode('utf-8'))
                buffer.append(data)

                if len(buffer) == batch_size:
                    data_queue.put(buffer.copy())
                    buffer.clear()

        except KeyboardInterrupt:
            print('Terminated by keyboard interruption...') 
        
        finally:
            consumer.close()

    import threading

    thread = threading.Thread(target = consumer_loop)
    thread.daemon = True
    thread.start()

    return data_queue

def get_agg_data():
   
    try:
        if True:
            response = requests.get("http://localhost:5000/data")
            response.raise_for_status()
            response = response.content.decode('utf-8')
            response = json.loads(response)
            
            df = pd.json_normalize(response, errors='ignore')

            headers = [
            'vehicle performance.engine_rpm', 'vehicle performance.engine_temp',
            'vehicle performance.fuel_level', 'vehicle performance.oil_pressure',
            'vehicle performance.suspension_travel.FL',
            'vehicle performance.suspension_travel.FR',
            'vehicle performance.suspension_travel.RL',
            'vehicle performance.suspension_travel.RR',
            'vehicle performance.tire_pressure.FL',
            'vehicle performance.tire_pressure.FR',
            'vehicle performance.tire_pressure.RL',
            'vehicle performance.tire_pressure.RR',
            'vehicle performance.transmission_temp', 'weather.barometric_pressure',
            'weather.external_temp', 'weather.humidity'
            ]

            df1 = df.copy()
            for h in headers:
                df1[h] = df1[h].apply(lambda x: get_mean(x))

            df2 = df1[headers]
            df2 = df2.mean()

            df2['location.altitude'] = df1.iloc[4]['location.altitude'][2]
            df2['location.compass_heading'] = df1.iloc[4]['location.compass_heading'][2]
            df2['location.gps_coordinates'] = df1.iloc[4]['location.gps_coordinates'][2]
            df2['time stamp'] = df1.iloc[4]['time stamp']

            return df2.to_dict()
        else:
            return {'message': 'No data available yet'}
    
    except Exception as e:
        return {'message': str(e)}




app = Flask(__name__)

data_queue = consume_rally_data("dakar_rally_sim", batch_size=5)

@app.route('/data', methods=['GET'])
def get_data():
    try:
        # Check if there's data in the queue
        if not data_queue.empty():
            data = data_queue.get(timeout=1.0)  # Wait for data with timeout
            return jsonify(data)
        else:
            return jsonify({'message': 'No data available yet'}), 204  # No Content
    except queue.Empty:
        return jsonify({'message': 'Timed out waiting for data'}), 503  # Service Unavailable
    



@app.route('/agg_data', methods = ['GET'])
def agg_data():
    aggregated_data = get_agg_data()
    return jsonify(aggregated_data)

if __name__ == '__main__':
    app.run()
   

