# Contributing to Logistics 4.0 Medallion Pipeline

First off, thank you for considering contributing to this project! It's people like you that make the open-source and data engineering community such a great place to learn, inspire, and create.

This document serves as a set of guidelines for contributing to this repository.

## 🧠 Code of Conduct

By participating in this project, you are expected to uphold a professional and collaborative standard. Be respectful in your Pull Requests and Issues.

## 🛠️ How Can I Contribute?

### 1. Reporting Bugs
If you find a bug (e.g., a streaming anomaly that breaks Spark, or a UI glitch in the Control Tower), please open an issue. Provide:
- A clear, descriptive title.
- Steps to reproduce the behavior.
- Expected behavior vs actual behavior.
- Logs from Loki/Grafana or Docker Compose outputs.

### 2. Suggesting Enhancements
Enhancement suggestions are highly welcome! Some areas currently open for contribution:
- **MLOps:** Adding Evidently AI for data drift detection.
- **Architecture:** Migrating the Bronze/Silver layers from raw parquet to Delta Lake or Apache Iceberg on MinIO.
- **Frontend:** Expanding the FastAPI endpoints to serve more granular forecasting graphs.

### 3. Pull Requests
The process for submitting a PR is straightforward:
1. **Fork** the repo and create your branch from `main`.
2. **Setup your environment:** Ensure you have `.env` configured (see `.env.example`).
3. **Write code:** Follow PEP-8 guidelines for Python scripts.
4. **Test:** Currently, testing is manual via the Control Tower UI, but `pytest` frameworks are being integrated. Ensure `docker-compose up --build` succeeds locally.
5. **Lint:** We use `flake8`. Ensure your code passes before submitting.
6. **Submit:** Open a Pull Request with a comprehensive description of the changes.

## 📋 Code Architecture Overview
If you are contributing to the core logic, please familiarize yourself with the pipeline flow:
`stream_manager.py (Kafka Producer)` ➔ `Kafka Topic` ➔ `spark_processor.py (Consumer/Inference)` ➔ `MinIO (Bronze) / Postgres (Silver)` ➔ `dbt (Gold)` ➔ `Control Tower UI`.

## 👮‍♂️ Code Owners
- **Core Architecture & MLOps:** Mazhine
- **UI / Control Tower:** Mazhine

Thanks again for helping build the future of intelligent supply chains!
