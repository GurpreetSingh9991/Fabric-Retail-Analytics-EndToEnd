# 🚀 Enterprise Retail Analytics: End-to-End Microsoft Fabric Pipeline

## 📖 Executive Summary

This project showcases a complete **Microsoft Fabric** implementation designed to solve common retail data challenges: inconsistent formatting, missing business logic, and slow reporting cycles. By processing over **401,000 transactions**, the pipeline delivers a high-fidelity analytics suite tracking **$8.76M in revenue** with real-time scalability.

---

## 🏗️ Data Architecture: The Medallion Framework

I architected the solution using a **Medallion (Bronze-Silver-Gold) approach** to ensure data governance and a clear "Single Source of Truth".

### **1. Bronze Layer: Ingestion & Sanitization**

* **Asset**: `01_Data_Cleaning_Bronze.ipynb`.
* **Process**: Ingested raw CSV data into **OneLake**.
* **Logic**: Performed structural cleaning, including standardizing `InvoiceDate` formats, handling over 130k missing `Customer_ID` records, and removing non-product transactions to ensure a clean baseline.

### **2. Silver Layer: Business Logic & Feature Engineering**

* **Asset**: `02_Business_Logic_Flagging.ipynb`.
* **Process**: Utilized a **Hybrid Compute Model** (Pandas for agility, Spark for scale).
* **Logic**: Engineered custom business flags, including **Return Transaction detection**, **High-Value Customer segmenting**, and **Revenue calculation** ().
* **Persistence**: Converted finalized Pandas DataFrames into **Spark Delta Tables** to leverage ACID compliance and Fabric Metastore integration.

### **3. Gold Layer: Virtualized Analytics Consumption**

* **Asset**: `SQL_Scripts.sql`.
* **Process**: Instead of duplicating data, I engineered **SQL Views** on the Analytics Endpoint.
* **Logic**: Created complex analytical views to automate **Month-over-Month (MoM) Growth** and **Product Intelligence**, allowing for "Schema-on-Read" flexibility.

---

## 🛠️ Tech Stack & Key Features

* **Orchestration**: Microsoft Fabric Workspace.
* **Languages**: Python (Pandas/PySpark) & T-SQL.
* **Storage**: Delta Lake (Parquet).
* **Reporting**: Power BI with **DirectLake Connectivity** for sub-second query performance.

---

## 📈 Business Impact & Performance KPIs

* **Revenue Optimization**: Identified a **336.71% MoM growth** surge during peak seasons.
* **Market Expansion**: Mapped geographical performance showing the **UK as the core market ($6.7M)** while identifying emerging EU growth bubbles.
* **Operational Efficiency**: Automated the entire cleaning-to-reporting lifecycle, reducing manual data prep time by an estimated 90%.

---

## 📂 Project Structure

```text
├── Notebooks
│   ├── 01_Data_Cleaning_Bronze.ipynb  # Sanitization logic
│   └── 02_Business_Logic_Flagging.ipynb # Enrichment logic
├── SQL_Scripts
│   └── View_Definitions.sql           # Gold-layer KPI views
├── Reports
│   ├── Retail_Analytics_Final.pbix    # Interactive Dashboard
│   └── Screenshots/                   # Visual proof of work
└── README.md

```

---

### **How to Use this Repo**

1. **Ingest**: Upload raw retail CSV to the `Files` section of a Fabric Lakehouse.
2. **Clean**: Run Notebook 01 to generate the `cleaned_retail_data` table.
3. **Enrich**: Run Notebook 02 to apply business flags and save the `final_retail_transactions` table.
4. **Visualize**: Connect the Power BI `.pbix` file to your Fabric SQL Endpoint.

---
