from confluent_kafka import Consumer 
import json
import numpy as np 
import pandas as pd
import time 
import socket

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


import socket
import time

def broadcast_data():
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.bind(('localhost', 5000))
        client_socket.listen(5)

        while True:  # Keep the server running
            print("Waiting for client...")
            conn, addr = client_socket.accept()
            print('Connected by', addr)

            try:
                while True:  # Keep sending data to the connected client
                    data_queue = consume_rally_data('dakar_rally_sim', 5)
                    agg_data = Rally_Data_Analysis(data_queue)

                    conn.sendall(bytes(str(agg_data), encoding="utf-8"))
                    time.sleep(1)

            except BrokenPipeError:
                print("Client disconnected.")
                break  # Back to accepting new connections
            except Exception as e:  # Catch other potential errors
                print(f"An error occurred: {e}")
                break
            finally:
                conn.close()  # Close the *client* connection

    except KeyboardInterrupt:
        print("Terminated by Keyboard Interruption...")
    finally:
        client_socket.close()

if __name__ == "__main__":
    broadcast_data()


        
    