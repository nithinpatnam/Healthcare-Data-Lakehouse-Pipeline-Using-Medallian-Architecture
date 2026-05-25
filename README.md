# Healthcare Data Lakehouse Pipeline – Project Overview

## Project Description

Developed an end-to-end **Healthcare Data Lakehouse Pipeline** using the **Medallion Architecture (Bronze, Silver, and Gold layers)** to efficiently ingest, process, transform, and analyze healthcare data for reporting and business insights.

The project was built using a cloud-based storage architecture where the healthcare source files were stored in an Azure Storage Account and processed through different layers of the data lakehouse to ensure scalability, data quality, and analytical readiness.

---

# Architecture Overview

## Source Layer

Created an Azure Storage Account named:

* **databricksampleextdl**

Inside the storage account, a container named:

* **mactores**

was created to store the project data.

The container contains the following folders:

* **input/**
* **output/**

### Input Folder

The `input` folder stores all the raw healthcare source files received from upstream systems.

### Output Folder

The `output` folder contains the Medallion Architecture layers:

* **bronze/**
* **silver/**
* **gold/**

---

# Data Flow Process

## 1. Data Ingestion – Bronze Layer

The raw healthcare data files were ingested from the `input` folder into the **Bronze Layer**.

### Process Used

* Accessed Azure Storage Account data using **Access Keys**.
* Connected the storage account with the processing environment securely.
* Loaded the raw source files into the Bronze layer without modifying the original data.

### Purpose of Bronze Layer

The Bronze layer acts as the **raw data repository** where:

* Original source data is preserved
* No major transformations are applied
* Data lineage and traceability are maintained

### Output

* Raw healthcare datasets stored in:

  * `output/bronze/`

---

## 2. Data Cleansing & Transformation – Silver Layer

The data from the Bronze layer was processed and moved into the **Silver Layer**.

### Operations Performed

* Data cleansing
* Handling null values
* Removing duplicates
* Standardizing column formats
* Applying required transformations
* Improving overall data quality

### Purpose of Silver Layer

The Silver layer contains:

* Cleaned data
* Structured and validated datasets
* Transformation-ready data for analytics

### Output

* Cleansed healthcare data stored in:

  * `output/silver/`

---

## 3. Data Aggregation & Business Analytics – Gold Layer

The transformed Silver layer data was further processed in the **Gold Layer**.

### Operations Performed

* Data aggregation
* Business-level calculations
* Analytical dataset preparation

### Purpose of Gold Layer

The Gold layer contains:

* Business-ready datasets
* Aggregated healthcare insights
* Reporting and dashboard-ready data

### Output

* Final analytical datasets stored in:

  * `output/gold/`

---

# Reporting & Visualization

The Gold layer datasets were designed to support:

* **Power BI dashboards**
* Business reports
* Healthcare analytics and insights

These reports help stakeholders analyze:

* Patient-related metrics
* Operational trends
* Healthcare performance indicators
* Data-driven decision making

---

# Technologies & Concepts Used

* Azure Storage Account
* Azure Data Lake Storage Concepts
* Databricks
* Medallion Architecture
* Data Lakehouse Architecture
* Access Key Authentication
* Data Cleansing & Transformation
* Data Aggregation
* Power BI Reporting

---

# End-to-End Pipeline Summary

1. Raw healthcare files are uploaded into the `input` folder.
2. Data is accessed securely from Azure Storage using Access Keys.
3. Raw data is ingested into the Bronze layer.
4. Bronze data is cleansed and transformed into the Silver layer.
5. Silver data is aggregated into business-ready Gold datasets.
6. Gold layer data is used for Power BI dashboards and reporting.

This project demonstrates a complete **modern healthcare data engineering pipeline** implementing scalable and structured data processing using the **Lakehouse Medallion Architecture** approach.
