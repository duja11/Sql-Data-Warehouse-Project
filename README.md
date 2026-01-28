# Sql-Data-Warehouse-Project
SQL-based Data Warehouse project using the Medallion Architecture (Bronze, Silver, Gold). It ingests raw ERP/CRM data, cleans and standardizes it, and delivers business-ready tables and KPIs for analytics and reporting.

## 1. Project Overview
This project demonstrates Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
Data Modeling: Developing fact and dimension tables optimized for analytical queries.
Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.

---

## 2. Architecture
**Medallion Architecture layers:**  
- **Bronze:** Raw ingested data  
- **Silver:** Cleaned and standardized data  
- **Gold:** Business-focused tables and KPIs
  <img width="1198" height="856" alt="image" src="https://github.com/user-attachments/assets/34da9b72-dcf1-4eb8-9b16-8251e1648b84" />


 ## 3.Project Requirements
Building the Data Warehouse (Data Engineering)
Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

Specifications
Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
Data Quality: Cleanse and resolve data quality issues prior to analysis.
Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
Scope: Focus on the latest dataset only; historization of data is not required.
Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.
BI: Analytics & Reporting (Data Analysis)
Objective
Develop SQL-based analytics to deliver detailed insights into:
Customer Behavior
Product Performance
Sales Trends
These insights empower stakeholders with key business metrics, enabling strategic decision-making.
