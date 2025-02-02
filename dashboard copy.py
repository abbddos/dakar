import dash 
from dash import Dash, dcc, html
from dash.dependencies import Output, Input 
import plotly 
import random 
import plotly.graph_objects as go 
from collections import deque
from confluent_kafka import Consumer 
import json
import numpy as np 
import pandas as pd
import plotly.subplots 

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

TIME = []
RPM = []
TMP = []
FLVL = []
OIL = []
TRAN = []
TRAN_FL = []
TRAN_FR = []
TRAN_RL = []
TRAN_RR = []
TFL = []
TFR = []
TRL = []
TRR = [] 

app = Dash(__name__, external_stylesheets=['style.css'])

app.layout = html.Div(className = "container", children = [
    html.H1('ENGINE PERFORMANCE'),
    dcc.Graph(id = 'live-graph', className="scatter-trace", animate = False),
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
    TIME.append(data['time stamp'])
    RPM.append(data['vehicle performance.engine_rpm'])
    TMP.append(data['vehicle performance.engine_temp'])
    FLVL.append(data['vehicle performance.fuel_level'])
    OIL.append(data['vehicle performance.oil_pressure'])
    TRAN.append(data['vehicle performance.transmission_temp'])
    TRAN_FL.append(data['vehicle performance.suspension_travel.FL'])
    TRAN_FR.append(data['vehicle performance.suspension_travel.FR'])
    TRAN_RL.append(data['vehicle performance.suspension_travel.RL'])
    TRAN_RR.append(data['vehicle performance.suspension_travel.RR'])
    TFL.append(data['vehicle performance.tire_pressure.FL'])
    TFR.append(data['vehicle performance.tire_pressure.FR'])
    TRL.append(data['vehicle performance.tire_pressure.RL'])
    TRR.append(data['vehicle performance.tire_pressure.RR'])

    SUSPENSION_TRAVEL = [TRAN_FL, TRAN_FR, TRAN_RL, TRAN_RR]
    TIRE_PRESSURE = [TFL, TFR, TRL, TRR]


    fig = plotly.subplots.make_subplots(rows = 3, cols = 2, shared_xaxes=True,
                                        subplot_titles=("Engine RPM", "Engine Temp", "Fuel Level", "Oil Pressure", "Transmission Temp"))

    RPM_data = plotly.graph_objs.Scatter(
            x=list(TIME),
            y=list(RPM),
            name='Engine RPM',
            mode= 'lines+markers'
    )

    TMP_data = plotly.graph_objs.Scatter(
            x=list(TIME),
            y=list(TMP),
            name='Engine TMP',
            mode= 'lines+markers'
    )

    FLVL_data = plotly.graph_objs.Scatter(
            x=list(TIME),
            y=list(FLVL),
            name='Fuel Level',
            mode= 'lines+markers'
    )

    OIL_data = plotly.graph_objs.Scatter(
            x=list(TIME),
            y=list(OIL),
            name='Oil Pressure',
            mode= 'lines+markers'
    )


    TRAN_data = plotly.graph_objs.Scatter(
            x=list(TIME),
            y=list(TRAN),
            name='Transmission TMP',
            mode= 'lines+markers'
    )

    fig.add_trace(RPM_data, row=1, col =1 )
    fig.add_trace(TMP_data, row=1, col =2 )

    fig.add_trace(FLVL_data, row=2, col=1)
    fig.add_trace(OIL_data, row=2, col = 2)

    fig.add_trace(TRAN_data, row = 3, col = 1)
 

    fig.update_xaxes(tickmode='auto')

    fig.update_yaxes(title_text="RPM", row=1, col=1)  
    fig.update_yaxes(title_text="TMP", row=1, col=2)  
    fig.update_yaxes(title_text="Fuel", row=2, col=1)  
    fig.update_yaxes(title_text="OIL Pressure", row=2, col=2)
    fig.update_yaxes(title_text="Transmission TMP", row=3, col=1)


    fig.update_layout( 
        height=900,
        xaxis_range=[0, max(TIME)], 
        yaxis_range=[1000, 8000],      
        yaxis2_range=[70, 110], 
        yaxis3_range = [0,100],
        yaxis4_range = [20,80],
        yaxis5_range = [70,120]      
    )

    return fig
    

if __name__ == '__main__':
    app.run_server(debug = True)



