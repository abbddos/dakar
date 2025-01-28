import dash 
from dash.dependencies import Output, Input 
import dash_core_components as dcc
import dash_html_components as html
import plotly 
import random 
import plotly.graph_objects as go 
from collections import deque
from confluent_kafka import Consumer 
import json
import numpy as np 
import pandas as pd 

def consume_rally_data(topic, batch_size):
    conf = {
        'bootstrap.servers':'localhost:9092',
        'group.id':'mygroup',
        'auto.offset.reset':'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    buffer = []
    return_data = []

   
    for _ in range(batch_size):
        msg = consumer.poll(1.0)
        if msg is None: 
            continue 
        if msg.error():
            print(f'Consumer Error: {msg.error()}')
            continue

        data = json.loads(msg.value().decode('utf-8'))
        buffer.append(data)

        if len(buffer) == batch_size:
            
            return_data.extend(buffer)
            buffer.clear()

    consumer.close()
    return json.dumps(return_data)

def get_mean(x):
    return np.array(x).mean()

def Rally_Data_Analysis(data_queue):
    data = json.loads(data_queue)
    df = pd.json_normalize(data, errors = 'ignore')
    
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

    df2['location.altitude'] = df1.iloc[-1]['location.altitude'][2]
    df2['location.compass_heading'] = df1.iloc[-1]['location.compass_heading'][2]
    df2['location.gps_coordinates'] = df1.iloc[-1]['location.gps_coordinates'][2]
    df2['time stamp'] = df1.iloc[-1]['time stamp']

    return df2.to_json()

X = []
Y = [] 

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1('ENGINE RPM'),
    dcc.Graph(id = 'live-graph', animate = True),
    dcc.Interval(id = 'graph-update', interval = 1000, n_intervals = 0)
])

@app.callback(
    Output('live-graph', 'figure'),
    Input('graph-update','n_intervals')
)
def update_graph_scatter(n):
    data_queue = consume_rally_data('dakar_rally_sim', 5)
    data = Rally_Data_Analysis(data_queue)
    data = json.loads(data)
    X.append(data['time stamp'])
    Y.append(data['vehicle performance.engine_rpm'])

    data = plotly.graph_objs.Scatter(
            x=list(X),
            y=list(Y),
            name='Scatter',
            mode= 'lines+markers'
    )

    return {'data': [data],
            'layout' : go.Layout(
                xaxis = dict(range=[min(X),max(X)]),
                yaxis = dict(range = [1000,8000])
                )
            }

if __name__ == '__main__':
    app.run_server()



