import json, time, random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
products = [{'id': i} for i in range(1, 101)]

while True:
    click = {'product_id': random.choice(products)['id'], 'timestamp': int(time.time()*1000)}
    producer.send('clicks', value=click)
    print(f"Sent: {click}")
    time.sleep(random.uniform(0.1, 0.5))
