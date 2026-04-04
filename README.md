# 🚀 Real-Time Data Engineering Pipeline (Kafka + PySpark)

## 📌 Overview

This project demonstrates a **real-time data engineering pipeline** built using **Apache Kafka** and **PySpark Structured Streaming**.

The pipeline simulates real-time order data, streams it through Kafka, processes it using Spark, and stores it in a **partitioned Parquet format** for efficient analytics.

---

## 🏗️ Architecture

```
Kafka Producer → Kafka Topic → PySpark Streaming → Parquet Storage
```

* **Producer** generates real-time order data
* **Kafka** handles streaming ingestion
* **PySpark** processes and transforms data
* **Parquet** stores partitioned output for analytics

---

## ⚙️ Tech Stack

* **Python**
* **Apache Kafka (KRaft Mode - ZooKeeper-less)**
* **PySpark Structured Streaming**
* **Parquet (Data Lake Storage)**

---

## ✨ Features

* 📡 Real-time data ingestion using Kafka Producer
* 🔄 Stream processing using PySpark Structured Streaming
* 🧹 Data cleansing and transformation
* ⏱️ Event-time and processing-time handling
* 📂 Partitioned Parquet output (`year/month/day`)
* 🔁 Fault tolerance using checkpointing
* ⚡ Micro-batch processing (every 30 seconds)

---

## 📁 Project Structure

```
data-engineering-pipeline/
│
├── producer/
│   └── orders_producer.py        # Kafka producer (data generator)
│
├── spark_jobs/
│   └── streaming_job.py          # PySpark streaming job
│
├── checkpoints/                 # Spark checkpoints (auto-created)
├── stream_output/               # Output data (Parquet)
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 🔄 Data Flow

1. Producer generates random order data:

   * order_id, user_id, product_id, amount, event_time
2. Kafka ingests data into `orders` topic
3. PySpark:

   * Reads stream from Kafka
   * Parses JSON
   * Filters invalid data
   * Adds processing timestamp
   * Extracts partition columns (year, month, day)
4. Data is written to Parquet in partitioned format

---

## ▶️ How to Run

### 1️⃣ Start Kafka Server (KRaft Mode)

```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

---

### 2️⃣ Create Kafka Topic

```bash
kafka-topics --create \
--topic orders \
--bootstrap-server localhost:9092
```

---

### 3️⃣ Verify Topic

```bash
kafka-topics --list --bootstrap-server localhost:9092
```

---

### 4️⃣ Run Kafka Producer

```bash
python producer/orders_producer.py
```

---

### 5️⃣ Run Spark Streaming Job

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
spark_jobs/streaming_job.py
```

---

## 📊 Output

* Data is stored in:

```
stream_output/orders/
```

* Partitioned by:

```
year=YYYY/month=MM/day=DD/
```

* Format: **Parquet (optimized for analytics)**

---

## ⚠️ Notes

* Kafka is configured in **KRaft mode (no ZooKeeper required)**
* Checkpoints ensure fault tolerance and recovery
* Delete checkpoints if schema changes:

```bash
rm -rf checkpoints/orders
```

---

## 🚀 Future Improvements

* Integrate with **AWS S3 / Data Lake**
* Add **Airflow orchestration**
* Load processed data into **Snowflake / Redshift**
* Implement **data quality checks**
* Add **monitoring (Prometheus/Grafana)**

---

## 💡 Key Learnings

* Built an end-to-end real-time pipeline
* Worked with Kafka streaming ingestion
* Implemented PySpark Structured Streaming
* Understood partitioning and checkpointing
* Learned modern Kafka (KRaft mode)