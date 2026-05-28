cat > docker-compose.yml << 'EOF'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
EOF

cat > click_generator.py << 'EOF'
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
EOF

cat > aggregator.py << 'EOF'
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
EOF

cat > requirements.txt << 'EOF'
kafka-python
redis
google-cloud-bigquery
EOF

cat > batch_processor.py << 'EOF'
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
EOF

cat > batch_processor.py << 'EOF'
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

rows = [{'product_id': pid

ls
pip install kafka-python redis google-cloud-bigquery --user
docker-compose up -d
cat > docker-compose.yml << 'EOF'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
EOF

cat docker-compose.yml
docker-compose up -d
cat > docker-compose.yml << 'EOF'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
EOF

cat docker-compose.yml
docker-compose up -d
cat docker-compose.yml
docker run -d --name zookeeper -p 2181:2181 confluentinc/cp-zookeeper:latest
docker run -d --name kafka -p 9092:9092 --link zookeeper -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:latest
docker run -d --name redis -p 6379:6379 redis:alpine
docker ps
docker run -d --name zookeeper -p 2181:2181 confluentinc/cp-zookeeper:latest
docker rm -f zookeeper
docker run -d --name zookeeper -p 2181:2181 confluentinc/cp-zookeeper:latest
docker rm -f kafka
docker run -d --name kafka -p 9092:9092 --link zookeeper -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:latest
docker network create kafka-net
docker run -d --name zookeeper --network kafka-net -p 2181:2181 confluentinc/cp-zookeeper:latest
sleep 5
docker run -d --name kafka --network kafka-net -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:latest
docker ps
docker rm -f zookeeper kafka
docker network create kafka-net
docker run -d --name zookeeper --network kafka-net -p 2181:2181 confluentinc/cp-zookeeper:latest
sleep 10
docker run -d --name kafka --network kafka-net -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:latest
docker ps
docker ps -a
docker rm -f zookeeper kafka
docker network rm kafka-net
docker network create kafka-net
docker run -d --name zookeeper --network kafka-net confluentinc/cp-zookeeper:7.4.0
sleep 10
docker run -d --name kafka --network kafka-net -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_BROKER_ID=1 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:7.4.0
docker ps
docker ps -a | grep zookeeper
docker logs zookeeper --tail 30
docker rm -f zookeeper kafka
docker run -d --name zookeeper --network kafka-net -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper:7.4.0
sleep 10
docker run -d --name kafka --network kafka-net -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_BROKER_ID=1 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:7.4.0
docker ps
docker exec kafka kafka-topics --create --topic clicks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --topic aggregated_clicks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
nohup python click_generator.py > clicks.log 2>&1 &
nohup python aggregator.py > agg.log 2>&1 &
tail -f clicks.log
gcloud auth application-default login
bq mk --dataset my-project-lab1-497719:raw
bq mk --dataset my-project-lab1-497719:dwh
bq mk --dataset my-project-lab1-497719:raw
gcloud auth login
bq mk --dataset my-project-lab1-497719:raw
bq mk --dataset my-project-lab1-497719:dwh
bq query --use_legacy_sql=false "CREATE TABLE \`my-project-lab1-497719.raw.orders\` (order_id INT64, product_id INT64, quantity INT64, price FLOAT64, order_date TIMESTAMP)"
bq query --use_legacy_sql=false "INSERT INTO \`my-project-lab1-497719.raw.orders\` VALUES (1, 10, 5, 100.0, CURRENT_TIMESTAMP()), (2, 20, 3, 50.0, CURRENT_TIMESTAMP())"
bq query --use_legacy_sql=false "CREATE TABLE \`my-project-lab1-497719.dwh.top_products\` (product_id INT64, score FLOAT64)"
ps aux | grep python
python batch_processor.py
pip install pandas --user
python batch_processor.py
pip install db-dtypes --user
python batch_processor.py
cat > batch_processor_fixed.py << 'EOF'
import json
from kafka import KafkaConsumer
from google.cloud import bigquery
import redis
from collections import defaultdict

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
client = bigquery.Client(project='my-project-lab1-497719')

# Читаем агрегаты из Kafka (последние сообщения)
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

# Получаем данные заказов из BigQuery
query = """
SELECT product_id, SUM(quantity) as total_orders
FROM `my-project-lab1-497719.raw.orders`
GROUP BY product_id
"""
orders_df = client.query(query).to_dataframe()
orders = dict(zip(orders_df['product_id'], orders_df['total_orders']))

# Считаем score
scores = {}
all_products = set(clicks.keys()) | set(orders.keys())
for pid in all_products:
    c = clicks.get(pid, 0)
    o = orders.get(pid, 0)
    scores[pid] = c * 0.7 + o * 0.3

# Топ-10
top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]

# Пишем в Redis
r.delete('top_products')
for pid, score in top:
    r.hset('top_products', str(pid), score)
    print(f"Redis: {pid} -> {score}")

# Пишем в BigQuery: пересоздаём таблицу с новыми данными
table_id = 'my-project-lab1-497719.dwh.top_products'
# Удаляем старую таблицу и создаём заново (простой способ)
client.delete_table(table_id, not_found_ok=True)
schema = [
    bigquery.SchemaField("product_id", "INT64"),
    bigquery.SchemaField("score", "FLOAT64"),
]
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)

rows_to_insert = [{"product_id": pid, "score": score} for pid, score in top]
errors = client.insert_rows_json(table, rows_to_insert)
if not errors:
    print(f"Данные записаны в BigQuery: {len(rows_to_insert)} строк")
else:
    print("Ошибки при вставке:", errors)
EOF

python batch_processor_fixed.py
cat > batch_processor_load.py << 'EOF'
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
EOF

python batch_processor_load.py
ALTER SCHEMA `my-project-lab1-497719.raw` SET OPTIONS (description="Domain: clickstream, Owner: recommend-team, SLA: 99.9%")
bq query --use_legacy_sql=false "ALTER SCHEMA \`my-project-lab1-497719.raw\` SET OPTIONS (description='Domain: clickstream, Owner: recommend-team, SLA: 99.9%')"
cd ~
mkdir -p dbt_project/models
cd dbt_project
cat > dbt_project.yml << 'EOF'
name: 'recommendations'
version: '1.0.0'
config-version: 2
profile: 'default'
model-paths: ["models"]
EOF

cat > models/popular_products.sql << 'EOF'
{{ config(materialized='table') }}
SELECT product_id, COUNT(*) as click_count
FROM `my-project-lab1-497719.raw.clicks`
GROUP BY product_id
EOF

cat > models/top_products.sql << 'EOF'
{{ config(materialized='table') }}
WITH clicks AS (
  SELECT product_id, click_count 
  FROM {{ ref('popular_products') }}
),
orders AS (
  SELECT product_id, SUM(quantity) as total_orders 
  FROM `my-project-lab1-497719.raw.orders` 
  GROUP BY product_id
)
SELECT 
  c.product_id, 
  c.click_count*0.7 + COALESCE(o.total_orders,0)*0.3 as score
FROM clicks c 
LEFT JOIN orders o ON c.product_id = o.product_id
ORDER BY score DESC 
LIMIT 10
EOF

dbt run
pip install dbt-bigquery --user
export PATH=$PATH:~/.local/bin
echo 'export PATH=$PATH:~/.local/bin' >> ~/.bashrc
source ~/.bashrc
dbt --version
cd ~/dbt_project
dbt run
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << 'EOF'
default:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: my-project-lab1-497719
      dataset: dwh
      threads: 1
      timeout_seconds: 300
      location: US
      priority: interactive
EOF

cd ~/dbt_project
dbt run
cat > ~/.dbt/profiles.yml << 'EOF'
default:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: my-project-lab1-497719
      dataset: dwh
      threads: 1
      timeout_seconds: 300
      location: EU
      priority: interactive
EOF

cd ~/dbt_project
dbt run
bq query --use_legacy_sql=false "CREATE TABLE \`my-project-lab1-497719.raw.clicks\` (product_id INT64, timestamp INT64, user_id INT64)"
cd ~/dbt_project
dbt run
crontab -l
crontab -e
crontab -l
crontab -e
crontab -l
cd ~
git init
git add .
git commit -m "Full pipeline: streaming + batch + dbt + terraform"
git remote add origin https://github.com/TVOY_AKKAUNT/recommendation_pipeline.git
git branch -M main
git push -u origin main
cd ~
git init
git add .
git commit -m "Full pipeline: streaming + batch + dbt + terraform"
git remote add origin https://github.com/aminoooo03/infa
git remote set-url origin https://github.com/aminoooo03/infa
git push -u origin main
git branch
git add .
git commit -m "Initial commit"
git branch -M main
git push -u origin main
git config --global user.email "nakovaamina3@gmail.com"
git config --global user.name "aminoooo03"
