import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

TABLES = ["customers", "accounts", "transactions"]

def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v

def download_from_minio():
    MINIO_ENDPOINT = _env("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = _env("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = _env("MINIO_SECRET_KEY")
    BUCKET = _env("MINIO_BUCKET")
    LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

    print(f"✅ [download_minio] Starting. endpoint={MINIO_ENDPOINT} bucket={BUCKET}")
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    local_files: dict[str, list[str]] = {t: [] for t in TABLES}

    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        if not objects:
            print(f"⚠️  [download_minio] No objects found for prefix={prefix}")
            continue

        print(f"✅ [download_minio] Found {len(objects)} objects for {table}")
        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"✅ [download_minio] Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)

    total = sum(len(v) for v in local_files.values())
    print(f"✅ [download_minio] Finished. downloaded_files={total}")
    return local_files

def load_to_snowflake(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio") or {}
    if not local_files:
        print("⚠️  [load_snowflake] No files passed from XCom.")
        return

    SNOWFLAKE_USER = _env("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD = _env("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_ACCOUNT = _env("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_WAREHOUSE = _env("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DB = _env("SNOWFLAKE_DB")
    SNOWFLAKE_SCHEMA = _env("SNOWFLAKE_SCHEMA")

    print(f"✅ [load_snowflake] Connecting account={SNOWFLAKE_ACCOUNT} db={SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    try:
        for table, files in local_files.items():
            if not files:
                print(f"⚠️  [load_snowflake] No local files for {table}, skipping.")
                continue

            print(f"✅ [load_snowflake] Loading table={table} files={len(files)}")

            for f in files:
                cur.execute(f"PUT file://{f} @%{table} AUTO_COMPRESS=FALSE")
                print(f"✅ [load_snowflake] PUT {f} -> @%{table}")

            copy_sql = f"""
            COPY INTO {table}
            FROM @%{table}
            FILE_FORMAT=(TYPE=PARQUET)
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            ON_ERROR='CONTINUE'
            """
            cur.execute(copy_sql)
            print(f"✅ [load_snowflake] COPY INTO {table} done.")

    finally:
        cur.close()
        conn.close()
        print("✅ [load_snowflake] Connection closed.")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables",
    schedule="*/1 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    task1 >> task2
