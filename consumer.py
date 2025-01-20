from confluent_kafka import Consumer 
import json

def consume_rally_data(topic):
    conf = {
        'bootstrap.servers':'localhost:9092',
        'group.id':'mygroup',
        'auto.offset.reset':'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True: 
            msg = consumer.poll(1.0)

            if msg is None: 
                continue 
            if msg.error():
                print(f'Consumer Error: {msg.error()}')
                continue

            data = json.loads(msg.value().decode('utf-8'))
            print(data)
            print("-" * 20)

    except KeyboardInterrupt:
        pass 

    finally:
        consumer.close()


if __name__ == "__main__":
    consume_rally_data('dakar_rally_sim')
