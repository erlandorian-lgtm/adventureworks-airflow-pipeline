from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

def check_connection():
    hook = PostgresHook(postgres_conn_id='adventure_works')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    print(f"Connection successful. Test query result: {result}")
    cursor.close()
    conn.close()

def extract():
    hook = PostgresHook(postgres_conn_id='adventure_works')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute('CREATE SCHEMA IF NOT EXISTS dwh;')

    cursor.execute('DROP TABLE IF EXISTS dwh.stg_hr_employees;')

    cursor.execute('''
        CREATE TABLE dwh.stg_hr_employees (
            "EmployeeID" INT,
            "FullName" VARCHAR(200),
            "JobTitle" VARCHAR(100),
            "DepartmentName" VARCHAR(100),
            "HireDate" DATE
        );
    ''')

    cursor.execute('''
        INSERT INTO dwh.stg_hr_employees ("EmployeeID", "FullName", "JobTitle", "DepartmentName", "HireDate")
        SELECT
            e."BusinessEntityID",
            p."FirstName" || ' ' || p."LastName",
            e."JobTitle",
            d."Name",
            e."HireDate"
        FROM "HumanResources"."Employee" e
        JOIN "Person"."Person" p ON e."BusinessEntityID" = p."BusinessEntityID"
        JOIN "HumanResources"."EmployeeDepartmentHistory" edh
            ON e."BusinessEntityID" = edh."BusinessEntityID" AND edh."EndDate" IS NULL
        JOIN "HumanResources"."Department" d ON edh."DepartmentID" = d."DepartmentID"
        WHERE d."Name" IN ('Production', 'Engineering');
    ''')

    cursor.execute('SELECT COUNT(*) FROM dwh.stg_hr_employees;')
    row_count = cursor.fetchone()[0]
    print(f"Extracted {row_count} rows into dwh.stg_hr_employees")

    conn.commit()
    cursor.close()
    conn.close()

def transform():
    hook = PostgresHook(postgres_conn_id='adventure_works')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS dwh.trf_hr_workforce;')

    cursor.execute('''
        CREATE TABLE dwh.trf_hr_workforce (
            "EmployeeID" INT,
            "FullName" VARCHAR(200),
            "JobTitle" VARCHAR(100),
            "DepartmentName" VARCHAR(100),
            "HireDate" DATE,
            "TenureYears" NUMERIC(5,2),
            "LatestPayRate" NUMERIC(10,2),
            "IsMover" BOOLEAN,
            "DeptChangeCount" INT
        );
    ''')

    cursor.execute('''
        INSERT INTO dwh.trf_hr_workforce
        WITH ranked_pay AS (
            SELECT
                "BusinessEntityID",
                "Rate",
                ROW_NUMBER() OVER (
                    PARTITION BY "BusinessEntityID"
                    ORDER BY "RateChangeDate" DESC
                ) AS rn
            FROM "HumanResources"."EmployeePayHistory"
        ),
        pay_summary AS (
            SELECT "BusinessEntityID", "Rate" AS "LatestPayRate"
            FROM ranked_pay
            WHERE rn = 1
        ),
        dept_summary AS (
            SELECT
                "BusinessEntityID",
                COUNT(*) AS "DeptChangeCount",
                COUNT(*) > 1 AS "IsMover"
            FROM "HumanResources"."EmployeeDepartmentHistory"
            GROUP BY "BusinessEntityID"
        )
        SELECT
            s."EmployeeID",
            s."FullName",
            s."JobTitle",
            s."DepartmentName",
            s."HireDate",
            DATE_PART('year', AGE(NOW(), s."HireDate")) AS "TenureYears",
            ps."LatestPayRate",
            COALESCE(ds."IsMover", FALSE) AS "IsMover",
            COALESCE(ds."DeptChangeCount", 0) AS "DeptChangeCount"
        FROM dwh.stg_hr_employees s
        LEFT JOIN pay_summary ps ON s."EmployeeID" = ps."BusinessEntityID"
        LEFT JOIN dept_summary ds ON s."EmployeeID" = ds."BusinessEntityID";
    ''')

    cursor.execute('SELECT COUNT(*) FROM dwh.trf_hr_workforce;')
    row_count = cursor.fetchone()[0]
    print(f"Transformed {row_count} rows into dwh.trf_hr_workforce")

    conn.commit()
    cursor.close()
    conn.close()

def load():
    hook = PostgresHook(postgres_conn_id='adventure_works')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dwh.fact_hr_workforce (
            "EmployeeID" INT,
            "FullName" VARCHAR(200),
            "JobTitle" VARCHAR(100),
            "DepartmentName" VARCHAR(100),
            "HireDate" DATE,
            "TenureYears" NUMERIC(5,2),
            "LatestPayRate" NUMERIC(10,2),
            "IsMover" BOOLEAN,
            "DeptChangeCount" INT,
            "LoadTimestamp" TIMESTAMP
        );
    ''')

    cursor.execute('TRUNCATE TABLE dwh.fact_hr_workforce;')

    cursor.execute('''
        INSERT INTO dwh.fact_hr_workforce
        SELECT
            "EmployeeID",
            "FullName",
            "JobTitle",
            "DepartmentName",
            "HireDate",
            "TenureYears",
            "LatestPayRate",
            "IsMover",
            "DeptChangeCount",
            NOW() AS "LoadTimestamp"
        FROM dwh.trf_hr_workforce;
    ''')

    cursor.execute('SELECT COUNT(*) FROM dwh.fact_hr_workforce;')
    row_count = cursor.fetchone()[0]
    print(f"Loaded {row_count} rows into dwh.fact_hr_workforce at {datetime.now()}")

    conn.commit()
    cursor.close()
    conn.close()
    

with DAG(
    dag_id='dag_hr_workforce_erlandobrian',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id='check_connection', python_callable=check_connection)
    t2 = PythonOperator(task_id='extract', python_callable=extract)
    t3 = PythonOperator(task_id='transform', python_callable=transform)
    t4 = PythonOperator(task_id='load', python_callable=load)

    t1 >> t2 >> t3 >> t4