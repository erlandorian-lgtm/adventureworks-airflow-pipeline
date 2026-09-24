from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import boto3
import csv
import io
import os


@dag(
    dag_id="dag_hr_ingestion_erlandobrian",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def hr_ingestion():

    tables = [
        ("HumanResources", "Employee"),
        ("Person", "Person"),
        ("HumanResources", "Department"),
        ("HumanResources", "EmployeeDepartmentHistory"),
        ("HumanResources", "EmployeePayHistory"),
    ]

    @task
    def extract_table(table_info):

        schema_name, table_name = table_info

        hook = PostgresHook(
            postgres_conn_id="adventure_works"
        )

        sql = f'SELECT * FROM "{schema_name}"."{table_name}"'

        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(sql)

        columns = [description[0] for description in cursor.description]

        records = cursor.fetchall()

        cursor.close()
        conn.close()

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)

        writer.writerow(columns)
        writer.writerows(records)

        s3 = boto3.client(
    		"s3",
    		endpoint_url=os.getenv("MINIO_ENDPOINT"),
    		aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    		aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    		region_name="us-east-1",
	)

        key = f"raw/{schema_name}/{table_name}.csv"

        s3.put_object(
            Bucket="adventureworks-raw",
            Key=key,
            Body=csv_buffer.getvalue().encode("utf-8"),
        )

        print(f"Table: {schema_name}.{table_name}")
        print(f"Rows extracted: {len(records)}")
        print(f"Uploaded to MinIO: {key}")

        return {
            "schema": schema_name,
            "table": table_name,
            "rows": len(records),
            "path": key,
        }

    extract_table.expand(table_info=tables)


hr_ingestion()