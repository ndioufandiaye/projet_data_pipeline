import marimo

app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import json
    from kafka import KafkaConsumer
    import psycopg2
    import time
    return KafkaConsumer, json, mo, psycopg2, time


@app.cell
def __():
    # Configuration de la connexion Postgres
    # Note : 'postgres' est le nom du service dans votre docker-compose
    try:
        conn = psycopg2.connect(
            host="postgres", 
            database="ecommerce_db", 
            user="toto", 
            password="toto1234",
            port=5432
        )
        cur = conn.cursor()
        print("✅ Connexion à PostgreSQL réussie !")
    except Exception as e:
        print(f"❌ Erreur de connexion Postgres : {e}")
    return conn, cur


@app.cell
def __():
    # Configuration du Consumer Kafka
    try:
        consumer = KafkaConsumer(
            'sales_topic',
            bootstrap_servers=['kafka:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        print("✅ Connecté au broker Kafka. En attente de messages...")
    except Exception as e:
        print(f"❌ Erreur Kafka : {e}")
        consumer = None
    return (consumer,)


@app.cell
def __(conn, consumer, cur, json):
    # Boucle d'insertion
    if consumer is not None:
        for msg in consumer:
            data = msg.value
            
            try:
                cur.execute("""
                    INSERT INTO raw_sales (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (data['InvoiceNo'], data['StockCode'], data['Description'], 
                      data['Quantity'], data['InvoiceDate'], data['UnitPrice'], 
                      data['CustomerID'], data['Country']))
                
                conn.commit()
                print(f"📥 Commande {data['InvoiceNo']} insérée en base.")
            except Exception as e:
                print(f"⚠️ Erreur lors de l'insertion : {e}")
                conn.rollback()
    else:
        print("En attente d'un consommateur valide...")
    return (data,)


if __name__ == "__main__":
    app.run()