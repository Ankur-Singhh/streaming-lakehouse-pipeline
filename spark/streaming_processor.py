import os
import sys
import traceback

# 1. Enforce HADOOP_HOME with forward slashes (prevents escape-sequence stripping)
HADOOP_DIR = "C:/hadoop"
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = f"{HADOOP_DIR}/bin" + os.pathsep + os.environ.get("PATH", "")

# 2. Maven dependencies for Kafka, Iceberg, and AWS S3A
SUBMIT_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
]
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {','.join(SUBMIT_PACKAGES)} pyspark-shell"

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, to_timestamp, when, lit, to_date
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    print("[Spark Init] Initializing SparkSession with Iceberg & MinIO S3A catalogs...")
    spark = (
        SparkSession.builder
        .appName("FinancialLakehousePipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        # Direct JVM property pointing to absolute path with forward slashes
        .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_DIR}")
        .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_DIR}")
        # Iceberg Catalog Configurations
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/iceberg")
        # MinIO S3A Configurations
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[Spark Init] SparkSession ready with Iceberg Catalog!")

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Ensure Iceberg Database exists
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.db")

    # 1. Read Stream from Kafka
    print("[Kafka] Subscribing to financial_transactions...")
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "financial_transactions")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON payload
    parsed = (
        raw_stream
        .selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as payload")
        .select(
            col("kafka_key"),
            from_json(col("payload"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("tx_date", to_date(col("timestamp")))
    )

    # 3. Branching: Clean records vs Dead Letter Queue (DLQ)
    tagged = parsed.withColumn(
        "is_valid",
        when((col("amount") > 0) & (col("currency").isNotNull()), lit(True)).otherwise(lit(False))
    ).withColumn(
        "rejection_reason",
        when(col("amount") <= 0, lit("NEGATIVE_OR_ZERO_AMOUNT"))
        .when(col("currency").isNull(), lit("MISSING_CURRENCY"))
        .otherwise(lit(None))
    )

    clean_stream = tagged.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    dlq_stream = tagged.filter(col("is_valid") == False)

    # 4. Create Tables if they don't exist
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.db.transactions (
            transaction_id STRING,
            user_id STRING,
            merchant_id STRING,
            amount DOUBLE,
            currency STRING,
            payment_method STRING,
            status STRING,
            timestamp STRING,
            event_time TIMESTAMP,
            tx_date DATE
        )
        USING iceberg
        PARTITIONED BY (currency, tx_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.db.dlq_events (
            transaction_id STRING,
            user_id STRING,
            merchant_id STRING,
            amount DOUBLE,
            currency STRING,
            payment_method STRING,
            status STRING,
            timestamp STRING,
            event_time TIMESTAMP,
            tx_date DATE,
            is_valid BOOLEAN,
            rejection_reason STRING
        )
        USING iceberg
    """)

    print("[Iceberg] Tables ready. Streaming micro-batches to MinIO S3...")

    # Query 1: Clean stream sink into Iceberg Table
    clean_query = (
        clean_stream.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "s3a://warehouse/checkpoints/transactions")
        .toTable("lakehouse.db.transactions")
    )

    # Query 2: DLQ sink into DLQ Table
    dlq_query = (
        dlq_stream.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "s3a://warehouse/checkpoints/dlq")
        .toTable("lakehouse.db.dlq_events")
    )

    spark.streams.awaitAnyTermination()

except Exception as e:
    print(f"\n[Error] Lakehouse pipeline failed:\n{e}")
    traceback.print_exc()
    sys.exit(1)
