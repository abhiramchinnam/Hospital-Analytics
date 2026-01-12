# Real-Time Patient Flow & Bed Occupancy Analytics 🏥

**Author:** Abhiram Chinnam
**Tech Stack:** Azure Event Hubs, Databricks (PySpark), ADLS Gen2, Synapse, Power BI, ADF.

---

## 📋 Overview
This project is an enterprise-grade data engineering solution designed to monitor hospital bed occupancy and patient flow in real-time. It ingests simulated patient event data, processes it via a **Medallion Architecture**, and serves analytics for decision-making.

**Key Features:**
* **Real-Time Ingestion:** Sub-second latency using Azure Event Hubs (Kafka Protocol).
* **Medallion Architecture:** Bronze (Raw), Silver (Clean), Gold (Star Schema).
* **SCD Type 2:** Handles historical data tracking for patient movements.
* **Schema Evolution:** Automatically adapts to changes in source data structure.
* **Fault Tolerance:** Uses Spark Checkpointing for exactly-once processing.

---

## 🏗️ Architecture

**Data Flow:**
1.  **Simulator:** Python script generates HL7-style patient events.
2.  **Ingest:** Events streamed to **Azure Event Hubs**.
3.  **Process (Databricks):**
    * *Bronze:* Raw Delta ingestion.
    * *Silver:* Parsing & Schema Evolution.
    * *Gold:* Aggregation & SCD Type 2 logic.
4.  **Store:** Data persisted in **ADLS Gen2** (Data Lake).
5.  **Serve:** **Azure Synapse Analytics** queries Gold tables.
6.  **Visualize:** **Power BI** dashboard for Bed Occupancy rates.

---

## 📂 Repository Structure

```bash
├── databricks_notebooks/
│   ├── 01_bronze_ingest.py     # Stream Event Hub -> Bronze Delta
│   ├── 02_silver_clean.py      # Parse JSON, Handle Nulls & Schema Drift
│   └── 03_gold_dim_fact.py     # Star Schema & SCD Type 2 Logic
├── simulator/
│   └── patient_flow_generator.py # Python script to generate fake patient data
├── pipeline_configs/
│   └── adf_pipeline.json       # Azure Data Factory pipeline export
├── docs/
│   └── architecture_diagram.png
└── README.md
