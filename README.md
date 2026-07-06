# Logistics 4.0 Big Data Medallion Architecture

# Logistics 4.0 Big Data Medallion Architecture

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-black?logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-MicroBatch-E25A1C?logo=apachespark&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data_Warehouse-336791?logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?logo=dbt&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-MLOps-0194E2?logo=mlflow&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Control_Tower-009688?logo=fastapi&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

An enterprise-grade, real-time Machine Learning and Big Data streaming pipeline built for modern **Logistics 4.0** analytics. This pipeline ingests e-commerce delivery streams, predicts shipping delays through a self-tuning Machine Learning engine, and builds a robust, three-tiered data warehouse using the Medallion Architecture — all monitored through a unified industrial Control Tower.

## 💼 Business Impact

In a real-world supply chain context, this platform delivers immediate ROI by shifting logistics management from *reactive* to *predictive*:
- **Proactive Delay Mitigation:** By predicting late deliveries with 77% AUC-ROC at the exact moment an order is placed, supply chain managers can proactively reroute shipments.
- **Improved OTIF (On-Time In-Full):** The Control Tower's What-If simulation engine allows operators to instantly calculate the impact of upgrading delayed ground shipments to "Air Express", protecting customer satisfaction SLAs.
- **Cost & CO₂ Optimization:** Real-time visibility into high-risk geographical zones enables dynamic carrier reallocation, reducing both emergency shipping costs and unnecessary carbon emissions.

## 🏗️ System Architecture

```mermaid
graph TD
    %% Define Nodes
    Stream[stream_manager.py<br/>Multi-mode Generator] -->|Producer| Kafka[(Apache Kafka<br/>Message Broker)]
    Kafka -->|Consumer| Spark[PySpark Processor<br/>Micro-batch Streaming]
    
    %% ML Integration
    MLflow[(MLflow<br/>Model Registry)] -.->|Loads Model| Spark
    
    %% Medallion Data Flow
    Spark -->|Raw Dump| MinIO[(MinIO S3<br/>Bronze Layer)]
    Spark -->|Cleaned & Inferred| PostgresS[(PostgreSQL<br/>Silver Layer)]
    PostgresS -->|SQL Transform| dbt[dbt<br/>Data Build Tool]
    dbt -->|Dimensional Model| PostgresG[(PostgreSQL<br/>Gold Layer)]
    
    %% Orchestration
    Airflow[Apache Airflow<br/>Orchestration] -.->|Triggers| dbt
    
    %% BI & Control Tower
    PostgresG -->|Direct Query| PowerBI[PowerBI<br/>Strategic Dashboards]
    PostgresS -->|psycopg2| FastAPI[FastAPI Backend<br/>Control Tower API]
    FastAPI --> UI[Nginx Gateway<br/>Unified Dashboard UI]
    PowerBI -.-> UI
    
    %% Styling
    classDef bronze fill:#cd7f32,stroke:#333,stroke-width:1px,color:#fff;
    classDef silver fill:#c0c0c0,stroke:#333,stroke-width:1px,color:#000;
    classDef gold fill:#ffd700,stroke:#333,stroke-width:1px,color:#000;
    classDef stream fill:#e25a1c,stroke:#333,stroke-width:1px,color:#fff;
    classDef ui fill:#009688,stroke:#333,stroke-width:1px,color:#fff;

    class MinIO bronze;
    class PostgresS silver;
    class PostgresG gold;
    class Spark,Kafka stream;
    class UI,FastAPI ui;
```

---

## 🛠️ Technology Stack

### 1. Ingestion & Streaming
* **Apache Kafka & Zookeeper:** High-throughput streaming message broker cluster.
* **Kafka-UI:** Live operational dashboard for Kafka topic monitoring.
* **Python (Pandas):** Ingests the historical `DataCoSupplyChainDataset.csv`, applies a time-travel offset to emulate live ongoing transactions, and pushes them to Kafka via 5 configurable streaming modes.

### 2. Processing & Storage (Medallion Architecture)
* **Apache PySpark (MLlib):** Real-time backend data processor. Computes predictive validations on micro-batches using the trained Random Forest model.
* **MinIO (S3-Compatible):** On-premise Object Storage handling the **Bronze Layer** (Raw payload dump).
* **PostgreSQL:** Handles the highly structured internal SQL data warehouse for **Silver and Gold** layers (fact_orders, dim_customer, dim_geography, dim_product).

### 3. Orchestration, Machine Learning & BI
* **Apache Airflow:** Automates workflow triggering (S3KeySensors tracking incoming Bronze packets).
* **dbt (Data Build Tool):** Executes SQL semantic transformations building analytical Gold Layers.
* **MLflow:** MLOps tracking server logging hyperparameter search results, full metrics suite (Accuracy, F1, Precision, Recall, AUC-ROC) and serialized model artifacts.
* **PowerBI:** Strategic batch analysis engine, connecting directly to the PostgreSQL Gold layer for deep historical reporting and management dashboards.

### 4. Observability & CI/CD
* **Grafana & Loki:** Production observability stack indexing real-time pipeline logs across all streaming modes.
* **GitHub Actions:** CI/CD tunnel enforcing `Flake8` PEP-8 standards and validating Docker deployments on commits.

### 5. Control Tower (Industrial Dashboard)
* **Nginx Gateway & FastAPI:** Central UI unifying the full Medallion flow. Proxies all internal services (Airflow, Spark, MinIO, Grafana, MLflow) through a single authenticated gateway.
* **Dynamic Time Filtering & Analytics:** The frontend integrates a premium customizable date picker (Day/Week/Month/Year), which dynamically queries the PostgreSQL Silver layer via custom `/db/stats` and `/db/orders` API endpoints to display exact slice-in-time KPIs, live Map data, and emergency operative tables.
* **PowerBI Datamart Integration:** Direct external embed of analytical reports.
* **What-If Simulation:** Uses background Web Workers to simulate impact of converting delayed ground shipments to "Air Express" in real-time on KPIs without mutating the actual data.
* **Docker SDK Integration:** Live container health monitoring across all 16 services.

---

## 🤖 MLOps Pipeline — Model Training

The `train_model.py` script trains a **Random Forest Classifier** to predict `Late_delivery_risk` on 15 curated features from the supply chain schema.

### Features Used (15/25 schema columns)
| Type | Features |
|------|---------|
| **Numeric (6)** | Days for shipment (scheduled), Benefit per order, Sales per customer, Order Item Total, Order Item Discount, Product Price |
| **Categorical (9)** | Shipping Mode, Order Status, Customer Country, Order Country, Order Region, Category Name, Customer Segment, Department Name, Type |

> **Excluded:** IDs (3), raw dates (2), target leakage columns (Days for shipping real, Delivery Status), label (Late_delivery_risk), high-cardinality cities (Customer City, Order City — >3000 unique values).

### Hyperparameter Tuning (Anti-Overfitting)
Uses **`TrainValidationSplit`** with a `ParamGridBuilder` (4 combinations):
- `numTrees`: [50, 100]
- `maxDepth`: [6, 8]
- `minInstancesPerNode`: 5 (fixed — prevents leaf overfitting)
- `maxBins`: 256

Automatically detects over/underfitting by comparing validation vs test AUC-ROC (logs a warning if gap > 5%).

### Smart Mix Data Augmentation
~10% of the training set is augmented with **Scheduling Paradox** perturbations (Standard Class + 7-day window → Late=1) to make the model robust against the anomalies generated by the Mix and AIO streaming modes.

### Latest Run Results
| Metric | Score |
|--------|-------|
| **AUC-ROC** | **0.7701** |
| Accuracy | 0.7117 |
| F1-Score | 0.7069 |
| Precision | 0.7530 |
| Recall | 0.7117 |
| Val-Test Gap | **0.0395** ✅ (< 5% → no overfitting) |

---

## 📡 Streaming Modes

The `stream_manager.py` supports 5 modes, selectable from the Control Tower UI or CLI:

| Mode | CLI Flag | Description |
|------|----------|-------------|
| **Sain** | `--mode sain` | Clean nominal data — real dataset events, no corruption |
| **Chaos** | `--mode chaos` | 100% SmartMix anomalies — all events corrupted |
| **Mixte** | `--mode mix` | 50% SmartMix + 50% nominal — production-realistic testing |
| **IA Self-Learning** | `--mode ia` | AI feedback loop: 3 adaptive regimes based on prediction error rate |
| **⚡ AIO Premium** | `--mode aio` | All-In-One: 35% nominal / 30% AI boundary / 20% Smart Mix / 15% chaos |

### SmartMixCorruptor — Causally Coherent Anomalies
Unlike random corruption, each anomaly follows supply chain business logic:

| Anomaly Type | Weight | Business Logic |
|---|---|---|
| `SCHEDULING_PARADOX` | 20% | Standard Class + 6-9d window → Late=1 guaranteed |
| `REVENUE_INTEGRITY` | 18% | Negative total + excessive discount → fraud pattern |
| `GEO_ROUTING_FAILURE` | 15% | Origin country ≠ delivery region → routing failure |
| `DEMAND_SURGE` | 15% | Order volume ×8-15 → logistics capacity overwhelmed |
| `PRODUCT_SUBSTITUTION` | 12% | Out-of-stock → mode override → added delay |
| `NULL_FIELD` | 10% | Critical field nullification → tests pipeline validation |
| `EDGE_CASE` | 10% | Intentional decision boundary case — max learning value |

### AIO Mode — Uncertainty Sampling Architecture
```
For each event:
  1. Compute heuristic risk score (0.0 → 1.0)
  
  If score ∈ [0.20, 0.40]  → UNCERTAINTY SAMPLING (automatic redirect)
     └→ Becomes a Boundary Case (max pedagogical value for the model)
  
  Otherwise → weighted distribution:
     roll < 0.35  → Nominal    (35% — real data baseline)
     roll < 0.65  → AI Boundary (30% — frontier exploration)
     roll < 0.85  → Smart Mix  (20% — coherent anomalies)
     remainder    → Chaos      (15% — extreme edge cases)
```

### IA Mode — 3 Adaptive Regimes
| Error Rate | Regime | Behavior |
|---|---|---|
| < 20% | `CHALLENGING` | Model too confident → injects edge cases |
| 20–40% | `BOUNDARY` | At learning frontier → SmartMix anomalies |
| > 40% | `RESET` | Model confused → clean nominal data to re-baseline |

---

## ⚙️ Prerequisites & Installation

* **Docker Engine** & **Docker Desktop** (or Compose V2)
* **Python 3.10+** with pip packages: `kafka-python`, `pandas`, `logging-loki`
* The historical dataset (`DataCoSupplyChainDataset.csv`) placed at `../DataCoSupplyChainDataset.csv`

---

## 🚀 Execution & Command Reference

### Start the Infrastructure
```powershell
docker-compose up -d --build
```
*Spins up all 16 background services. Access the Control Tower at `http://localhost:8000`.*

### Train the ML Model (First Time Setup)
```powershell
# Copy training script into the Spark container
docker cp train_model.py 03_spark_master:/tmp/train_model.py

# Launch training with hyperparameter tuning (~10-15 min)
docker exec -it 03_spark_master spark-submit --master spark://spark-master:7077 /tmp/train_model.py
```
*Trains a Random Forest with TrainValidationSplit across 4 hyperparameter combinations. Results logged to MLflow.*

### Launch a Kafka Stream
```powershell
python stream_manager.py --mode sain    # Clean nominal data
python stream_manager.py --mode chaos   # 100% causally coherent anomalies
python stream_manager.py --mode mix     # 50% anomaly / 50% nominal
python stream_manager.py --mode ia      # AI self-learning with 3 adaptive regimes
python stream_manager.py --mode aio     # AIO Premium: optimized for model improvement
```

### Stop the Cluster
```powershell
docker-compose down
```

---

## 🔌 API Reference (Control Tower FastAPI)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /` | GET | Serves the Control Tower HTML dashboard |
| `GET /api/status` | GET | Live status of all 16 Docker containers via Docker SDK |
| `POST /api/stream/start?mode=X&delay=Y` | POST | Launches `stream_manager.py` in the selected mode |
| `POST /api/stream/stop` | POST | Gracefully terminates the active stream |
| `GET /db/tables` | GET | Lists all PostgreSQL tables with row counts and sizes |
| `GET /db/table/{name}?limit=N` | GET | Returns the last N rows from a given DWH table |
| `GET /db/stats` | GET | **[NEW]** Computes and returns dynamic aggregations (OTIF, Delay Rate, Revenue, CO2) filtered by precise dates or periods (e.g. `?period=week` or `?date_from=Y-M-D&date_to=Y-M-D`). |
| `GET /db/orders` | GET | **[NEW]** Retrieves specific, temporally-filtered payload rows to feed the interactive Map Digital Twin and the live Operational Emergencies table. |

---

## 🌐 Web Interfaces & Credentials

| Service | URL | Username | Password | Purpose |
|---------|-----|----------|----------|---------|
| **Control Tower** | [localhost:8000](http://localhost:8000) | `admin` | `pfa2026` | Unified architecture dashboard |
| **Apache Airflow** | via Control Tower | `admin` | `admin` | DAG orchestration |
| **Kafka UI** | via Control Tower | — | — | Real-time stream monitoring |
| **MinIO Console** | via Control Tower | `admin` | `pfa2026` | Raw S3 Bronze data lake |
| **Grafana** | via Control Tower | `admin` | `admin` | Loki log aggregation |
| **MLflow Server** | via Control Tower | — | — | ML experiment tracking |
| **Spark Master UI** | via Control Tower | — | — | Spark job monitoring |
| **PostgreSQL Viewer** | Control Tower → Postgres Warehouse | built-in | — | Native DWH explorer |

*All services are proxied through the Nginx gateway — no direct port access required.*

---

## 📁 Repository Structure

| File / Directory | Role |
|---|---|
| `docker-compose.yml` | Orchestrates all 16 containerized services with memory limits |
| `stream_manager.py` | Multi-mode Kafka data generator (sain / chaos / mix / ia / aio) |
| `spark_processor.py` | PySpark streaming consumer — Bronze→Silver Medallion with ML inference |
| `train_model.py` | Random Forest trainer with TrainValidationSplit hypertuning + Smart Mix augmentation |
| `Dockerfile.spark` | Custom Spark image with Unix user fix for Hadoop filesystem access |
| `control_tower/` | FastAPI app + Nginx gateway + HTML/CSS/JS dashboard |
| `control_tower/app.py` | FastAPI backend (stream control, Docker SDK, PostgreSQL native viewer) |
| `control_tower/nginx.conf` | Nginx reverse proxy configuration for all internal services |
| `dags/` | Apache Airflow DAG definitions |
| `dbt_logistics/` | dbt SQL transformations (Silver → Gold dimensional model) |
| `postgres-init/` | Bootstrap SQL schema (fact_orders, dim_customer, dim_geography, dim_product) |
| `spark_models/` | Serialized PipelineModel binary (persists between restarts) |
| `.github/workflows/` | GitHub Actions CI/CD (Flake8 linting + Docker validation) |
