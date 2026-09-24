from airflow.decorators import dag, task
from datetime import datetime 
import boto3
import duckdb 
import os

@dag(
    dag_id="dag_hr_transform_erlandobrian",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
)   
def hr_transform():

    @task 
    def transform():
        s3=boto3.client(
            "s3", 
            endpoint_url="http://172.21.0.4:9000",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            region_name="us-east-1",
        )

        employee_response = s3.get_object(
            Bucket= "adventureworks-raw",
            Key="raw/HumanResources/Employee.csv",)

        person_response = s3.get_object(
            Bucket= "adventureworks-raw",
            Key="raw/Person/Person.csv",)

        department_response = s3.get_object(
            Bucket= "adventureworks-raw",
            Key="raw/HumanResources/Department.csv",)

        employee_department_history_response = s3.get_object(
            Bucket= "adventureworks-raw",
            Key="raw/HumanResources/EmployeeDepartmentHistory.csv",)

        employee_pay_history_response = s3.get_object(
            Bucket= "adventureworks-raw",
            Key="raw/HumanResources/EmployeePayHistory.csv",)

        employee_path = "/tmp/employee.csv"
        person_path = "/tmp/person.csv"
        department_path = "/tmp/department.csv"
        employee_department_history_path = "/tmp/employee_department_history.csv"
        employee_pay_history_path = "/tmp/employee_pay_history.csv"

        employee_csv = employee_response["Body"].read().decode("utf-8")
        person_csv = person_response ["Body"].read().decode("utf-8")
        department_csv = department_response ["Body"].read().decode("utf-8")
        employee_department_history_csv = employee_department_history_response ["Body"].read().decode("utf-8")
        employee_pay_history_csv = employee_pay_history_response ["Body"].read().decode("utf-8")

        with open(employee_path, "w", encoding="utf-8") as f:f.write(employee_csv)
        with open(person_path, "w", encoding="utf-8") as f:f.write(person_csv)
        with open(department_path, "w", encoding="utf-8") as f:f.write(department_csv)
        with open(employee_department_history_path, "w", encoding="utf-8") as f:f.write(employee_department_history_csv)
        with open(employee_pay_history_path, "w", encoding="utf-8") as f:f.write(employee_pay_history_csv)

        conn= duckdb.connect("/opt/airflow/dags/repo/dwh.duckdb")

        conn.execute("""CREATE OR REPLACE TABLE silver.stg_employee AS
            SELECT *
            FROM read_csv_auto(?)
            """, [employee_path])

        conn.execute("""CREATE OR REPLACE TABLE silver.stg_person AS
            SELECT *
            FROM read_csv_auto(?)
            """, [person_path])
        
        conn.execute("""CREATE OR REPLACE TABLE silver.stg_department AS
            SELECT *
            FROM read_csv_auto(?)
            """, [department_path])

        conn.execute("""CREATE OR REPLACE TABLE silver.stg_employee_department_history AS
            SELECT *
            FROM read_csv_auto(?)
            """, [employee_department_history_path])

        conn.execute("""CREATE OR REPLACE TABLE silver.stg_employee_pay_history AS
            SELECT *
            FROM read_csv_auto(?)
            """, [employee_pay_history_path])

        conn.execute("""
        CREATE OR REPLACE TABLE gold.employee_current AS
        WITH ranked_pay AS (
            SELECT
                "BusinessEntityID",
                "Rate",
                ROW_NUMBER() OVER (
                    PARTITION BY "BusinessEntityID"
                    ORDER BY "RateChangeDate" DESC
                    ) AS rn
            FROM silver.stg_employee_pay_history),

        pay_summary AS (
            SELECT "BusinessEntityID", "Rate" AS "LatestPayRate"
            FROM ranked_pay
            WHERE rn = 1),

        dept_summary AS (
            SELECT
                "BusinessEntityID",
                COUNT(DISTINCT "DepartmentID") - 1 AS "DeptChangeCount",
                COUNT(DISTINCT "DepartmentID") > 1 AS "IsMover"
            FROM silver.stg_employee_department_history
            GROUP BY "BusinessEntityID")

        SELECT
            e.BusinessEntityID AS EmployeeID,
            CONCAT_WS(' ', p.FirstName, p.MiddleName, p.LastName) AS FullName,
            e.JobTitle,
            e.HireDate,
            d.Name AS DepartmentName,
            ROUND(DATE_DIFF('day', e.HireDate, CURRENT_DATE) / 365.25,2) AS "TenureYears",
            ps.LatestPayRate,
            COALESCE(ds."IsMover", FALSE) AS "IsMover",
            COALESCE(ds."DeptChangeCount", 0) AS "DeptChangeCount",
            CURRENT_TIMESTAMP AS "LoadTimestamp"
        FROM silver.stg_employee e
        JOIN silver.stg_person p ON e.BusinessEntityID = p.BusinessEntityID
        JOIN silver.stg_employee_department_history edh ON e.BusinessEntityID = edh.BusinessEntityID AND edh.EndDate IS NULL
        JOIN silver.stg_department d ON edh.DepartmentID = d.DepartmentID
        JOIN pay_summary ps ON e.BusinessEntityID = ps.BusinessEntityID
        LEFT JOIN dept_summary ds ON e.BusinessEntityID = ds.BusinessEntityID
        WHERE DepartmentName  IN ('Engineering','Production')
        """)
        
        result = conn.execute("""SELECT * FROM gold.employee_current LIMIT 10""").fetchall()

        print(result)

        conn.close()

    transform()

hr_transform()