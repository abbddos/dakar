from confluent_kafka import Consumer 
import json
import time


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




if __name__ == "__main__":

    try:
        while True:
            message = consume_rally_data('dakar_rally_sim', 5)
            print(message)
            print('-'*20)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print('Terminated by keyboard interruption... ')
