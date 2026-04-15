from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuration
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin123"

def get_spark_session():
    return SparkSession.builder \
        .appName("Ecommerce_Medallion_Incremental") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()

def bronze_ingestion(**context):
    """Lit le CSV et écrit dans la couche Bronze (Brut)"""
    spark = get_spark_session()
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Chemin source (lié via volume Docker)
    source_path = "/opt/airflow/data/data.csv"
    bronze_path = f"s3a://datalake/bronze/sales/date={date_str}"

    df = spark.read.csv(source_path, header=True, inferSchema=True)
    
    # Simulation d'ingestion : on ajoute un timestamp technique
    df = df.withColumn("ingested_at", F.current_timestamp())
    
    df.write.mode("overwrite").parquet(bronze_path)
    spark.stop()
    return bronze_path

def silver_cleaning(**context):
    """Nettoie les données et écrit dans la couche Silver"""
    ti = context['ti']
    bronze_path = ti.xcom_pull(task_ids='ingest_bronze')
    spark = get_spark_session()
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    silver_path = f"s3a://datalake/silver/sales/date={date_str}"

    df = spark.read.parquet(bronze_path)
    
    # Nettoyage : suppression des doublons et lignes sans CustomerID
    # On filtre aussi les quantités > 0 (on ignore les retours pour le CA)
    df_clean = df.filter((F.col("CustomerID").isNotNull()) & (F.col("Quantity") > 0)) \
        .dropDuplicates(["InvoiceNo", "StockCode"])
    
    df_clean.write.mode("overwrite").parquet(silver_path)
    spark.stop()
    return silver_path

def gold_analytics(**context):
    """Agrégation métier et CA par pays vers la couche Gold"""
    ti = context['ti']
    silver_path = ti.xcom_pull(task_ids='clean_silver')
    spark = get_spark_session()
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    gold_path = f"s3a://datalake/gold/revenue_report/date={date_str}"

    df = spark.read.parquet(silver_path)
    
    # Calcul du Revenu par ligne et agrégation par pays
    df_revenue = df.withColumn("LineRevenue", F.col("Quantity") * F.col("UnitPrice")) \
        .groupBy("Country") \
        .agg(
        F.sum("LineRevenue").alias("TotalRevenue"),
        F.count("InvoiceNo").alias("TotalTransactions")
            )
    
    df_revenue.write.mode("overwrite").parquet(gold_path)
    
    # Affichage dans les logs Airflow pour vérification
    df_revenue.show()
    spark.stop()

# --- Définition du DAG ---
dag = DAG(
    dag_id="ecommerce_medallion_pipeline",
    start_date=dt.datetime(2026, 4, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["isi", "master2", "medallion", "minio"]
)

task_bronze = PythonOperator(
    task_id='ingest_bronze',
    python_callable=bronze_ingestion,
    dag=dag
)

task_silver = PythonOperator(
    task_id='clean_silver',
    python_callable=silver_cleaning,
    dag=dag
)

task_gold = PythonOperator(
    task_id='compute_gold',
    python_callable=gold_analytics,
    dag=dag
)

task_bronze >> task_silver >> task_gold
