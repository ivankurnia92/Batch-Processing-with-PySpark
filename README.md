# Batch-Processing-with-PySpark

# Spark & Airflow Batch Data Pipeline

Proyek **batch data processing pipeline** menggunakan **Apache Airflow** sebagai orchestrator dan **Apache Spark (PySpark)** sebagai engine pemrosesan data.  
Pipeline ini membaca data CSV, melakukan join & transformasi, lalu menyimpan hasilnya dalam format **Parquet**.


---

## Arsitektur Pipeline

1. **Apache Airflow**
   - Mengatur workflow (DAG)
   - Menjalankan job Spark melalui `PythonOperator`

2. **Apache Spark (PySpark)**
   - Membaca data CSV
   - Join dan transformasi data
   - Menyimpan hasil dalam format Parquet

---

## Dataset

### Input
- `orders.csv`
- `order_items.csv`

### Output
- Data hasil transformasi dalam format **Parquet**

## Alur Proses

1. Membaca dataset CSV menggunakan Spark
2. Join tabel `orders` dan `order_items` berdasarkan `order_id`
3. Transformasi data:
   - Menghapus kolom tidak relevan
   - Filter data invalid (`price > 0`)
   - Handling nilai null
   - Standarisasi tipe data
   - Rename kolom `user_id` → `customer_id`
   - Membuat kolom turunan `gmv`
4. Menyimpan hasil dalam format Parquet
5. Verifikasi hasil dengan membaca kembali file Parquet

---

## Transformasi Data

- **Data Cleaning**
  - Drop kolom `order_item_id`
  - Filter harga <= 0
  - Fill null pada kolom `quantity`

- **Data Standardization**
  - Casting `price` ke `double`
  - Rename kolom agar lebih konsisten

- **Derived Column**
  - `gmv = quantity * price`

---

## Teknologi yang Digunakan

- Python 3
- Apache Airflow
- Apache Spark (PySpark)
- Parquet
- CSV

---
