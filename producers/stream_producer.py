import pandas as pd
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_csv():
    df = pd.read_csv('data/data.csv')
    for _, row in df.iterrows():
        message = row.to_dict()
        producer.send('sales_topic', message)
        print(f"Sent: {message['InvoiceNo']}")
        time.sleep(1) # Simulation temps réel

if __name__ == "__main__":
    stream_csv()