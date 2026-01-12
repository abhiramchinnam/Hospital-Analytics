# Real-Time Patient Flow & Bed Occupancy Analytics 🏥 ⚡

![Architecture Diagram](architecture_diagram.jpg)
*(Architecture: Simulator → Event Hubs → Databricks → Synapse → Power BI)*

**Author:** Abhiram Chinnam  
**Tech Stack:** Azure Event Hubs, Azure Databricks (PySpark), ADLS Gen2, Azure Synapse Analytics, Power BI, Azure Data Factory.

---

## 📋 Executive Summary

**The Business Problem:**
"Midwest Health Alliance," a multi-hospital network, faced operational inefficiencies due to a lack of visibility into real-time patient flow. Bed occupancy data was only available via nightly batch reports, leading to overcrowding in Emergency Departments (ED) and inefficient staff allocation.

**The Solution:**
I engineered an enterprise-grade **Real-Time Data Streaming Platform**. The system ingests live patient event data, processes it via a **Medallion Architecture** on Azure Databricks, and serves up-to-the-minute dashboards on bed availability and wait times.

**Key Technical Achievements:**
* **Real-Time Latency:** Reduced data availability time from **24 hours to <2 minutes**.
* **Historical Tracking:** Implemented **SCD Type 2** to track patient history (e.g., Ward Transfers) without losing prior states.
* **Reliability:** Automated the workflow using **Azure Data Factory** (Pipeline: `pipeline2`), achieving consistent successful execution.

---

## 🏗️ Architecture & Data Flow

```mermaid
graph TD
    %% Define Styles
    classDef azure fill:#0072C6,stroke:#fff,stroke-width:2px,color:#fff;
    classDef databricks fill:#FF3621,stroke:#fff,stroke-width:2px,color:#fff;
    classDef storage fill:#207228,stroke:#fff,stroke-width:2px,color:#fff;

    subgraph "Ingestion"
        Src[IoT/HL7 Simulator] -->|Events| EH[Azure Event Hubs]
    end

    subgraph "Processing (Azure Databricks)"
        EH -->|Stream| NB1[bronze_layer.py]
        NB1 -->|Raw Delta| Bronze[(Container: bronze)]
        
        Bronze -->|Read| NB2[silver_layer.py]
        NB2 -->|Clean/Schema Evolution| Silver[(Container: silver)]
        
        Silver -->|Read| NB3[golden_layer.py]
        NB3 -->|Star Schema/SCD Type 2| Gold[(Container: gold)]
    end

    subgraph "Orchestration"
        ADF[Azure Data Factory<br/>Pipeline: pipeline2] -->|Trigger| NB1
        ADF -->|Trigger| NB2
        ADF -->|Trigger| NB3
    end

    subgraph "Serving"
        Gold -->|External Table| Synapse[Azure Synapse SQL]
        Synapse -->|Direct Query| PBI[Power BI Dashboard]
    end
📂 Repository Structure
Bash

Real-Time-Patient-Flow-Analytics/
│
├── databricks-notebooks/       # ETL Logic (PySpark Structured Streaming)
│   ├── bronze_layer.py         # Ingest Event Hub Stream -> Bronze Container
│   ├── silver_layer.py         # Parse JSON, Handle Nulls & Schema Evolution
│   └── golden_layer.py         # Star Schema Modeling & SCD Type 2 Logic
│
├── adf-pipelines/              # Orchestration
│   └── pipeline2.json          # Azure Data Factory Pipeline Definition
│
├── simulator/                  # Source Data Generator
│   └── patient_flow_generator.py # Python script generating HL7 events
│
├── sqlpool-queries/            # Warehousing
│   └── synapse_ddl.sql         # SQL scripts for External Tables
│
└── README.md                   # Project Documentation

🛠️ Technical Deep Dive
1. Ingestion Strategy (Bronze)
Objective: Capture raw data with zero loss.

Implementation: bronze_layer.py reads from Event Hubs using the Kafka protocol.

Fault Tolerance: Enabled Checkpointing. As seen in the logs, the _checkpoints folder ensures the stream resumes exactly where it left off after any failure (Exactly-Once Processing).

2. Transformation & Quality (Silver)
Objective: Handle Schema Drift.

Feature: Schema Evolution.

Code Logic: silver_layer.py uses .option("mergeSchema", "true"). This allows the pipeline to automatically adapt if the IoT simulator adds new fields (e.g., Sensor_ID) without breaking the job.

3. Analytics Modeling (Gold)
Objective: Track patient history accurately.

Feature: SCD Type 2 (Slowly Changing Dimensions).

Code Logic: golden_layer.py implements logic to track historical changes. The table includes effective_from, effective_to, and surrogate_key columns to track when a patient moves from "Emergency" to "ICU".

4. Orchestration & Automation
Tool: Azure Data Factory (ADF).

Pipeline: pipeline2.

Trigger: Goldtrigger (Scheduled Execution).

Monitoring: The pipeline successfully triggers all three Databricks notebooks in sequence, as evidenced by the ADF Monitor logs.


    class EH,ADF,Synapse azure;
    class NB1,NB2,NB3 databricks;
    class Bronze,Silver,Gold storage;
