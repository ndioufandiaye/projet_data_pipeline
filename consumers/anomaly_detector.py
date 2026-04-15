from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('sales_topic', bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for msg in consumer:
    data = json.loads(msg.value)
    # Logique d'anomalie
    if data['Quantity'] > 1000 or data['UnitPrice'] < 0:
        print(f"ANOMALY DETECTED: {data['InvoiceNo']}")
        producer.send('alerts', json.dumps(data).encode('utf-8'))