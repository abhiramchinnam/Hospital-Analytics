# Real-Time Patient Flow & Bed Occupancy Analytics  🏥 ⚡

**Author:** Abhiram Chinnam
**Tech Stack:** Azure Event Hubs, Azure Databricks (PySpark), ADLS Gen2, Azure Synapse Analytics, Power BI, Azure Data Factory.

---

## 📋 Overview
This project is an enterprise-grade data engineering solution designed to monitor hospital bed occupancy and patient flow in real-time. It ingests simulated HL7/IoT patient event data, processes it via a **Medallion Architecture**, and serves analytics for decision-making.

**Key Features:**
* **Real-Time Ingestion:** Sub-second latency using **Azure Event Hubs** (Kafka Protocol).
* **Medallion Architecture:** Bronze (Raw), Silver (Clean), Gold (Star Schema).
* **SCD Type 2:** Handles historical data tracking for patient movements (e.g., Dept Transfers).
* **Schema Evolution:** Automatically adapts to changes in source data structure without breaking pipelines.
* **Fault Tolerance:** Uses Spark **Checkpointing** for exactly-once processing.

---

## 🏗️ Architecture

**Data Flow:**
1.  **Simulator:** Python script generates HL7-style patient events (Admissions, Discharges).
2.  **Ingest:** Events streamed to **Azure Event Hubs**.
3.  **Process (Databricks):**
    * **🥉 Bronze:** Raw ingestion as Delta Tables (`raw_json` column).
    * **🥈 Silver:** Parsing, Type Casting, and **Schema Evolution** handling.
    * **🥇 Gold:** Aggregation & **Star Schema** modeling with **SCD Type 2**.
4.  **Store:** Data persisted in **ADLS Gen2** (Data Lakehouse pattern).
5.  **Serve:** **Azure Synapse Analytics** queries Gold tables via External Tables.
6.  **Orchestrate:** **Azure Data Factory** (`Goldtrigger`) manages the workflow.

---

## 📂 Repository Structure

```bash
real-time-patient-flow-azure/
│
├── databricks-notebooks/       # ETL Logic (PySpark Structured Streaming)
│   ├── 01_bronze_rawdata.py    # Ingest Stream -> Delta (Raw JSON)
│   ├── 02_silver_cleandata.py  # Clean, Parse, Handle Schema Drift
│   └── 03_gold_transform.py    # SCD Type 2 Logic & Star Schema
│
├── simulator/                  # Source Data Generation
│   └── patient_flow_generator.py # Python script to generate dummy patient events
│
├── adf-pipelines/              # Orchestration
│   └── pipeline2.json          # Azure Data Factory pipeline definition
│
├── sqlpool-queries/            # Data Warehousing Logic
│   └── synapse_external_tables.sql  # DDL scripts for Synapse
│
├── architecture/
│   └── architecture_diagram.jpg
│
└── README.md                   # Project Documentation
