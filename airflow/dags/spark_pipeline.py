from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql.functions import col # Pindahkan ke atas

def read_data_logic():
    spark = SparkSession.builder \
        .appName("Assignment Batch Processing with PySpark") \
        .getOrCreate()
    
    # 1. Membaca Dataset
    df_orders = spark.read.csv("/opt/airflow/data/orders.csv", header=True, inferSchema=True)
    df_order_items = spark.read.csv("/opt/airflow/data/order_items.csv", header=True, inferSchema=True)
    
    # 2. Join Tabel
    df_joined = df_orders.join(df_order_items, on="order_id", how="inner")
    
    # 3. Transformasi Data
    # Pembersihan Data 
    df_cleaned = df_joined.drop("order_item_id") \
        .filter(col("price") > 0) \
        .fillna(0, subset=["quantity"]) # Sesuaikan kolom yang mungkin null

    # Standarisasi Data
    df_standardized = df_cleaned.withColumn("price", col("price").cast("double")) \
        .withColumnRenamed("user_id", "customer_id")

    # Kolom Turunan
    df_transformed = df_standardized.withColumn("gmv", col("quantity") * col("price"))

    # 4. Simpan hasil join dan transformasi dalam format :
    # Parquet
    parquet_output_path = "/opt/airflow/data/transformed_orders_parquet"

    # Mode penyimpanan: overwrite
    df_transformed.write.mode("overwrite").parquet(parquet_output_path)
    print(f"Data Parquet berhasil disimpan ke: {parquet_output_path}")

    # 5. VERIFIKASI: MEMBACA KEMBALI FILE PARQUET
    df_verify = spark.read.parquet(parquet_output_path)
    print("Verifikasi Pembacaan Parquet (5 baris pertama):")
    df_verify.show(5)

    spark.stop()

with DAG(
    dag_id='spark_airflow_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task_read_csv = PythonOperator(
        task_id='read_datasets_task',
        python_callable=read_data_logic
    )