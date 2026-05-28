import json
from kafka import KafkaConsumer
from google.cloud import bigquery
import redis
from collections import defaultdict
import pandas as pd
from io import StringIO

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
client = bigquery.Client(project='my-project-lab1-497719')

# 1. Читаем агрегаты из Kafka (последние сообщения)
consumer = KafkaConsumer('aggregated_clicks', bootstrap_servers='localhost:9092',
                        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                        auto_offset_reset='latest', consumer_timeout_ms=5000)
clicks = defaultdict(int)
for msg in consumer:
    pid = msg.value['product_id']
    cnt = msg.value['count']
    clicks[pid] += cnt
consumer.close()

# Если нет данных из Kafka, выходим
if not clicks:
    print("Нет данных из Kafka. Убедитесь, что генератор и агрегатор работают.")
    exit(0)

# 2. Получаем данные заказов из BigQuery
query = """
SELECT product_id, SUM(quantity) as total_orders
FROM `my-project-lab1-497719.raw.orders`
GROUP BY product_id
"""
orders_df = client.query(query).to_dataframe()
orders = dict(zip(orders_df['product_id'], orders_df['total_orders']))

# 3. Считаем score
scores = {}
all_products = set(clicks.keys()) | set(orders.keys())
for pid in all_products:
    c = clicks.get(pid, 0)
    o = orders.get(pid, 0)
    scores[pid] = c * 0.7 + o * 0.3

# 4. Топ-10
top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]

# 5. Пишем в Redis
r.delete('top_products')
for pid, score in top:
    r.hset('top_products', str(pid), score)
    print(f"Redis: {pid} -> {score}")

# 6. Пишем в BigQuery через загрузочное задание (load job)
table_id = 'my-project-lab1-497719.dwh.top_products'

# Удаляем старую таблицу, если она существует
client.delete_table(table_id, not_found_ok=True)

# Создаём новую таблицу
schema = [
    bigquery.SchemaField("product_id", "INT64"),
    bigquery.SchemaField("score", "FLOAT64"),
]
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)

# Записываем данные в DataFrame
df = pd.DataFrame(top, columns=['product_id', 'score'])

# Сохраняем DataFrame в CSV (в памяти) и загружаем в BigQuery
csv_string = df.to_csv(index=False, header=False)
csv_file = StringIO(csv_string)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)

load_job = client.load_table_from_file(
    csv_file, table_id, job_config=job_config
)
load_job.result()  # ожидаем завершения задания

print(f"Данные записаны в BigQuery: {len(top)} строк")
