import json
from kafka import KafkaConsumer
import psycopg2

# Connexion à Postgres
conn = psycopg2.connect(
    host="localhost", database="ecommerce_db", 
    user="user", password="password"
)
cur = conn.cursor()

consumer = KafkaConsumer('sales_topic', bootstrap_servers=['localhost:9092'])

print("En attente de données pour insertion SQL...")
for msg in consumer:
    data = json.loads(msg.value)
    
    cur.execute("""
        INSERT INTO raw_sales (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (data['InvoiceNo'], data['StockCode'], data['Description'], 
          data['Quantity'], data['InvoiceDate'], data['UnitPrice'], 
          data['CustomerID'], data['Country']))
    
    conn.commit()