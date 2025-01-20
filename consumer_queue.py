from confluent_kafka import Consumer 
import json
from queue import Queue
from flask import Flask, jsonify

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

if __name__ == '__main__':
    app.run()
   

