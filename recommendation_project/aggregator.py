import json, time
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict
from datetime import datetime

consumer = KafkaConsumer('clicks', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

window_size = 300  # 5 минут в секундах
clicks = defaultdict(list)

for msg in consumer:
    now = time.time()
    pid = msg.value['product_id']
    clicks[pid].append(now)
    # удаляем старые (старше 5 минут)
    clicks[pid] = [t for t in clicks[pid] if now - t <= window_size]
    
    # раз в 10 секунд отправляем агрегат (для простоты)
    if int(now) % 10 == 0:
        agg = [{'product_id': pid, 'count': len(lst)} for pid, lst in clicks.items() if lst]
        for item in agg:
            producer.send('aggregated_clicks', value=item)
        print(f"Отправлен агрегат: {agg[:3]}...")