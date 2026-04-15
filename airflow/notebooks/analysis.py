import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import json
    import time
    import pandas as pd
    from datetime import datetime
    from confluent_kafka import Producer, Consumer, KafkaError
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp, lit

    return Producer, SparkSession, col, current_timestamp, json, mo, time


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    # 🚀 Pipeline E-Commerce - ISI Dakar
    **Objectif :** Ingestion Temps Réel (Kafka), Stockage Medallion (MinIO) et Analytics (Spark).
    """)
    return


@app.cell
def _():
    # Configuration globale
    KAFKA_CONF = {'bootstrap.servers': 'kafka:9092'}
    TOPIC_MAIN = 'sales_topic'
    TOPIC_ALERTS = 'anomalies_topic'

    # Configuration MinIO / Spark
    S3_ENDPOINT = "http://minio:9000"
    S3_ACCESS_KEY = "minioadmin"
    S3_SECRET_KEY = "minioadmin123"
    return KAFKA_CONF, S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 1️⃣ Ingestion Temps Réel (Producer)
    """)
    return


@app.cell
def _(KAFKA_CONF, Producer, json, time):
    def delivery_report(err, msg):
        if err is not None:
            print(f'❌ Erreur : {err}')
        else:
            print(f'✅ Message livré à {msg.topic()} [{msg.partition()}]')

    producer = Producer(KAFKA_CONF)

    def stream_data_to_kafka(file_path, num_rows=100):
        import pandas as pd
        df = pd.read_csv(file_path)
        # On prend un échantillon pour simuler le flux
        sample = df.head(num_rows)

        for _, row in sample.iterrows():
            payload = row.to_dict()
            # Détection d'anomalie simple avant envoi (Optionnel ici, ou via Consumer)
            producer.produce(
                'sales_topic', 
                key=str(payload['InvoiceNo']), 
                value=json.dumps(payload),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(0.5) # Simule un délai réel
        producer.flush()

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 2️⃣ Traitement Medallion avec Spark (Batch/Micro-batch)
    """)
    return


@app.cell
def _(S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY, SparkSession):
    # Initialisation de la session Spark adaptée à ton environnement local[*]
    spark = SparkSession.builder \
        .appName("MedallionPipeline") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    return (spark,)


@app.cell
def _(col, current_timestamp, spark):
    def run_medallion_cycle():
        # 1. BRONZE : Lecture du CSV source (simulant l'atterrissage)
        df_raw = spark.read.csv("/opt/airflow/data/data.csv", header=True, inferSchema=True)
        df_raw.write.mode("overwrite").parquet("s3a://datalake/bronze/sales_raw")

        # 2. SILVER : Nettoyage & Transformation
        # On filtre les quantités négatives (retours) et on ajoute un timestamp d'ingestion
        df_silver = df_raw.filter(col("Quantity") > 0) \
            .withColumn("ingested_at", current_timestamp()) \
            .dropDuplicates(["InvoiceNo", "StockCode"])

        df_silver.write.mode("overwrite").parquet("s3a://datalake/silver/sales_cleaned")

        # 3. GOLD : Agrégation Business (Chiffre d'affaires par Pays)
        df_gold = df_silver.withColumn("TotalLine", col("Quantity") * col("UnitPrice")) \
            .groupBy("Country") \
            .sum("TotalLine") \
            .withColumnRenamed("sum(TotalLine)", "TotalRevenue")

        df_gold.write.mode("overwrite").parquet("s3a://datalake/gold/revenue_by_country")

        return df_gold.toPandas()

    # run_medallion_cycle()
    return


@app.cell
def _(KAFKA_CONF, Producer, json, time):
    def run_realtime_producer():
        import pandas as pd
        producer = Producer(KAFKA_CONF)
        df = pd.read_csv("/opt/airflow/data/data.csv")

        print("🚀 Lancement du flux temps réel vers Kafka...")

        # On simule l'envoi des 50 premières lignes
        for i, row in df.head(50).iterrows():
            data = row.to_dict()

            # Détection d'anomalie simple (Métier)
            if data['UnitPrice'] > 100:
                topic = "anomalies_topic"
            else:
                topic = "sales_topic"

            producer.produce(topic, key=str(data['InvoiceNo']), value=json.dumps(data))
            producer.poll(0)
            time.sleep(1) # Un message par seconde

        producer.flush()
        print("✨ Simulation terminée.")

    return


if __name__ == "__main__":
    app.run()
