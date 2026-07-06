# Senior Data Engineering Architecture
## Logistics 4.0 Big Data Pipeline

This document details the standard logical components of the Medallion Data Architecture implemented in this repository.

### 1. Ingestion Layer (Message Brokering)
* **`docker-compose.yml` (Zookeeper & Kafka):** Manages cluster orchestration. Kafka brokers the initial high-throughput streaming events via the `dataco_orders` topic.
* **`stream_manager.py`:** A class-based Python streaming service simulating a backend system or application logic. It pushes payloads to Kafka.

### 2. Processing & Inference Layer (Streaming PySpark)
* **`spark_processor.py`:** Acts as the primary backend processor reading the unstructured data stream. It standardizes payloads to a rigorous schema and conducts low-latency ML inferencing natively on the stream using the precompiled model binary. Spark guarantees "Exactly-Once" or "At-Least-Once" transactional consistency via checkpointing.
* **`spark_jars/`** & **`spark_models/`:** Volumes persisting compiled dependencies (`.jar`) and the algorithmic Random Forest `.pb` model serialized by the trainer.

### 3. Machine Learning Operations (MLOps)
* **`train_model.py`:** Scheduled batch job used to execute distributed ML training against the legacy historic `DataCoSupplyChainDataset.csv`. Isolates cross-validation and evaluation outside the active database workload.
* **MLflow Platform (`mlruns/`):** Ephemeral and persistent storage backend for tuning metadata, parameters (`maxDepth`, `numTrees`), and artifact storage.

### 4. Storage Subsystems (Medallion Implementation)
* **MinIO (`postgres-init/`, `minio-data`):** On-premise Object Storage providing the robust S3-compatible backend. Receives raw streaming events for the **Bronze Layer**.
* **PostgreSQL (`logistics_db`):** Highly structured internal data warehouse. The `silver_orders` relation catches inferred structured output from PySpark to form the **Silver Layer**.

### 5. Orchestration (Apache Airflow & dbt)
* **`dags/` (pipeline_logistics_dag.py):** Orchestrates temporal triggers utilizing `S3KeysSensor`. If MinIO reports new arrivals, Airflow triggers semantic transformations.
* **`dbt_logistics/`:** Defines the semantic models compiling SQL schemas for the **Gold Layer** (`dim_customer`, `fact_orders`), abstracting logic cleanly from the Extract-Load processes.

### 6. Code Compliance
* **`.github/workflows/data_pipeline_ci.yml`:** Enforces standard enterprise configurations such as PEP-8 layout compliances via Flake8.
