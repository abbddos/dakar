from confluent_kafka import Consumer 
import json


def consume_rally_data(topic, batch_size):
    conf = {
        'bootstrap.servers':'localhost:9092',
        'group.id':'mygroup',
        'auto.offset.reset':'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    buffer = []

   
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
            global return_data
            return_data = json.dumps(buffer)
            buffer.clear()

    consumer.close()
    return return_data




if __name__ == "__main__":

    try:
        while True:
            message = consume_rally_data('dakar_rally_sim', 5)
            print(message)
            print('-'*20)
    except KeyboardInterrupt:
        print('Terminated by keyboard interruption... ')
