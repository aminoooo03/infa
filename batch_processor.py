import json, os
from kafka import KafkaConsumer
from google.cloud import bigquery
import redis
from collections import defaultdict

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
client = bigquery.Client(project='my-project-lab1-497719')

consumer = KafkaConsumer('aggregated_clicks', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='latest', consumer_timeout_ms=5000)
clicks = defaultdict(int)
for msg in consumer:
    pid = msg.value['product_id']
    cnt = msg.value['count']
    clicks[pid] += cnt
consumer.close()

query = """
SELECT product_id, SUM(quantity) as total_orders
FROM `my-project-lab1-497719.raw.orders`
GROUP BY product_id
"""
orders_df = client.query(query).to_dataframe()
orders = dict(zip(orders_df['product_id'], orders_df['total_orders']))

scores = {pid: clicks.get(pid,0)*0.7 + orders.get(pid,0)*0.3 for pid in set(clicks)|set(orders)}
top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]

r.delete('top_products')
for pid, score in top:
    r.hset('top_products', str(pid), score)
    print(f"Redis: {pid} -> {score}")

rows = [{'product_id': pid, 'score': score} for pid, score in top]
table = 'my-project-lab1-497719.dwh.top_products'
client.delete_rows(table, "1=1")
errors = client.insert_rows_json(table, rows)
if not errors:
    print("Written to BigQuery")
