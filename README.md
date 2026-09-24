# AdventureWorks ETL and ELT Pipeline with Apache Airflow

This project is an assignment from **Dibimbing** using the **AdventureWorks** dataset. I implemented both an **ETL pipeline** and an **ELT pipeline** using Apache Airflow.

The project demonstrates data extraction, transformation, loading, orchestration, Dynamic Task Mapping, XCom, MinIO object storage, DuckDB, and Medallion Architecture.

## Project Overview

### ETL Pipeline

The ETL pipeline follows the traditional:

**Extract → Transform → Load**

1. **Extract** raw data from the AdventureWorks PostgreSQL database.
2. **Transform** the data by cleaning and preparing it in the staging layer, then creating the required dimensional tables used to answer the business questions.
3. **Load** the transformed data into the target database. A timestamp is also added to record when the data was loaded.

### ELT Pipeline

The ELT pipeline follows:

**Extract → Load → Transform**

The ELT implementation consists of two Airflow DAGs:

#### 1. Ingestion DAG

The ingestion DAG:

* Extracts data from the AdventureWorks PostgreSQL database.
* Uses **Dynamic Task Mapping** to run the same extraction task for multiple source tables.
* Uses **XCom** to pass small metadata between tasks, such as table names, row counts, and MinIO object paths.
* Stores the extracted raw data as CSV files in **MinIO**.

#### 2. Transform DAG

The transform DAG:

* Retrieves the raw CSV files from MinIO.
* Loads the raw data into **DuckDB**.
* Creates the **Silver layer** for staging and cleaning the data.
* Creates the **Gold layer** containing business-ready data required for analytical queries.
* Adds a `LoadTimestamp` to record when the Gold data was generated or refreshed.

## Architecture

```text
                AdventureWorks
                PostgreSQL
                     │
                     ▼
              Apache Airflow
                     │
          ┌──────────┴──────────┐
          │                     │
       ETL DAG             ELT Ingestion DAG
          │                     │
          ▼                     ▼
    Transform Data           MinIO
          │                  Bronze Layer
          ▼                     │
    Target Tables               ▼
                         DuckDB / Silver
                               │
                               ▼
                         DuckDB / Gold
                               │
                               ▼
                         Analytical Queries
```

## Medallion Architecture

The ELT pipeline implements a Medallion Architecture:

### Bronze

Raw CSV files extracted from AdventureWorks and stored in **MinIO**.

```text
MinIO
└── adventureworks-raw
    └── raw
        ├── HumanResources
        │   ├── Employee.csv
        │   ├── Department.csv
        │   ├── EmployeeDepartmentHistory.csv
        │   └── EmployeePayHistory.csv
        └── Person
            └── Person.csv
```

### Silver

Cleaned and prepared staging tables in DuckDB:

* `silver.stg_employee`
* `silver.stg_person`
* `silver.stg_department`
* `silver.stg_employee_department_history`
* `silver.stg_employee_pay_history`

### Gold

Business-ready analytical table:

* `gold.employee_current`

The Gold layer combines employee information, current department, latest pay rate, tenure, and department movement information.

## Technologies

* **Python**
* **Apache Airflow**
* **PostgreSQL**
* **MinIO**
* **DuckDB**
* **DBeaver**
* **Docker**
* **SQL**

## Airflow Features Used

### Dynamic Task Mapping

Dynamic Task Mapping allows one reusable Airflow task to be executed multiple times with different inputs.

For example, the same extraction function is used to extract:

* Employee
* Person
* Department
* EmployeeDepartmentHistory
* EmployeePayHistory

This avoids creating a separate extraction task for every table.

### XCom

XCom is used to pass small pieces of metadata between Airflow tasks.

The ingestion task returns information such as:

* Source schema
* Table name
* Number of extracted rows
* MinIO object path

The actual datasets are **not stored in XCom**.

## Analytical Questions

The Gold layer is designed to support questions such as:

1. Which department has the higher average employee tenure: Production or Engineering?
2. What is the correlation between employee pay rate and tenure?
3. What percentage of employees have changed departments?

## Project Structure

```text
adventureworks-airflow-pipeline/
│
├── dags/
│   ├── dag_hr_ingestion_erlandobrian.py
│   ├── dag_hr_transform_erlandobrian.py
│   └── dag_hr_workforce_erlandobrian.py
│
├── docker-compose-minio.yaml
├── .gitignore
└── README.md
```

## My Contribution

This project was completed as part of a Dibimbing Data Engineering assignment.

My implementation includes the ETL and ELT pipelines, including:

* Airflow DAG development
* Data extraction from PostgreSQL
* Dynamic Task Mapping
* XCom metadata handling
* MinIO integration
* DuckDB transformations
* Silver and Gold layers
* Medallion Architecture
* Analytical SQL queries
* Pipeline metadata and timestamps
