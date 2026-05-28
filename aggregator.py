import json, time
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict

consumer = KafkaConsumer('clicks', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

window = 300
clicks = defaultdict(list)

for msg in consumer:
    now = time.time()
    pid = msg.value['product_id']
    clicks[pid].append(now)
    clicks[pid] = [t for t in clicks[pid] if now - t <= window]
    if int(now) % 10 == 0:
        for pid, lst in clicks.items():
            if lst:
                producer.send('aggregated_clicks', value={'product_id': pid, 'count': len(lst)})
        print("Aggregated sent")
