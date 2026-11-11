import os
import sys

# 1. Enforce HADOOP_HOME with forward slashes
HADOOP_DIR = "C:/hadoop"
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = f"{HADOOP_DIR}/bin" + os.pathsep + os.environ.get("PATH", "")

# 2. Maven dependencies for Iceberg & S3A
SUBMIT_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
]
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {','.join(SUBMIT_PACKAGES)} pyspark-shell"

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("LakehouseVerifier")
    .master("local[*]")
    .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_DIR}")
    .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_DIR}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/iceberg")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*70)
print("             1. ICEBERG SNAPSHOT HISTORY (ACID LOG)")
print("="*70)
spark.sql("""
    SELECT committed_at, snapshot_id, operation, summary['total-records'] as total_records 
    FROM lakehouse.db.transactions.snapshots
    ORDER BY committed_at DESC
""").show(truncate=False)

print("\n" + "="*70)
print("             2. TRANSACTIONS SUMMARY BY CURRENCY")
print("="*70)
spark.sql("""
    SELECT currency, count(*) as count, round(sum(amount), 2) as total_vol, round(avg(amount), 2) as avg_amt
    FROM lakehouse.db.transactions
    GROUP BY currency
""").show(truncate=False)

print("\n" + "="*70)
print("             3. SAMPLE CLEAN TRANSACTIONS")
print("="*70)
spark.sql("""
    SELECT transaction_id, user_id, amount, currency, status, tx_date 
    FROM lakehouse.db.transactions 
    LIMIT 5
""").show(truncate=False)

print("\n" + "="*70)
print("             4. DEAD LETTER QUEUE (QUARANTINED ANOMALIES)")
print("="*70)
spark.sql("""
    SELECT transaction_id, user_id, amount, currency, rejection_reason 
    FROM lakehouse.db.dlq_events
""").show(truncate=False)

spark.stop()
