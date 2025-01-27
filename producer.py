import time
import json 
from confluent_kafka import Producer
from vehicle import DakarRallySimulator 

def run_simulation(dt):
    try:
        while True:

            conf = {'bootstrap.servers': 'localhost:9092'}
            producer = Producer(**conf)

            vh = DakarRallySimulator()
            data = {'vehicle performance': vh.simulate_vehicle_performance(),
                    'location':vh.simulate_gps_data(),
                    'weather': vh.simulate_environmental_data(),
                    'crew health': vh.simulate_crew_vitals(),
                    'time stamp': time.ctime()
                    }

            try:
                producer.produce('dakar_rally_sim', json.dumps(data).encode('utf-8'))
            except Exception as e:
                print(f'Exception while producing to kafka: {e}')

            finally: 
                producer.flush()
                time.sleep(dt)

    except KeyboardInterrupt:
            print('Terminated by keyboard interruption...')
    



if __name__ == "__main__":
    dt = 0.5
    print('producing...')
    run_simulation(dt)

