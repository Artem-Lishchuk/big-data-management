#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone


CONNECTOR_NAME = "pg-cdc-connector"
CDC_TOPICS = ["dbserver1.public.customers", "dbserver1.public.drivers"]
TAXI_TOPIC = "taxi-trips"

WATERMARK_TABLE = "lakehouse.system.job_watermarks"
BRONZE_CDC_TABLE = "lakehouse.cdc.bronze_cdc_events"
BRONZE_TAXI_TABLE = "lakehouse.taxi.bronze_raw_events"
SILVER_CUSTOMERS_TABLE = "lakehouse.cdc.silver_customers"
SILVER_DRIVERS_TABLE = "lakehouse.cdc.silver_drivers"
SILVER_TAXI_TABLE = "lakehouse.taxi.silver_trips"
GOLD_TRIPS_PER_HOUR_TABLE = "lakehouse.taxi.gold_trips_per_hour"
GOLD_AVERAGE_FARE_PER_ZONE_TABLE = "lakehouse.taxi.gold_average_fare_per_zone"
GOLD_REVENUE_PER_ZONE_TABLE = "lakehouse.taxi.gold_revenue_per_zone"
GOLD_TRIP_SEGMENTS_TABLE = "lakehouse.taxi.gold_trip_segments"
GOLD_SEGMENT_TRENDS_TABLE = "lakehouse.taxi.gold_segment_trends"
SPARK_PACKAGES = os.environ.get(
    "SPARK_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,"
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0",
)
VALIDATION_CATCHUP_PASSES = int(os.environ.get("VALIDATION_CATCHUP_PASSES", "6"))
VALIDATION_IDLE_PASSES = int(os.environ.get("VALIDATION_IDLE_PASSES", "2"))
VALIDATION_IDLE_WAIT_SECONDS = float(
    os.environ.get("VALIDATION_IDLE_WAIT_SECONDS", "2")
)


def ensure_package(package_name, import_name=None):
    import importlib.util

    if importlib.util.find_spec(import_name or package_name) is None:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-q", package_name]
        )


def get_env(name, default=None):
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def connect_url():
    return get_env("CONNECT_URL", "http://connect:8083")


def kafka_bootstrap():
    return get_env("KAFKA_BOOTSTRAP", "kafka:9092")


def pg_config():
    return {
        "host": get_env("PG_HOST", "postgres"),
        "port": int(get_env("PG_PORT", "5432")),
        "dbname": get_env("PG_DB", "sourcedb"),
        "user": get_env("PG_USER", "cdc_user"),
        "password": get_env("PG_PASSWORD", "cdc_pass"),
    }


def connector_config():
    pg = pg_config()
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": pg["host"],
        "database.port": str(pg["port"]),
        "database.user": pg["user"],
        "database.password": pg["password"],
        "database.dbname": pg["dbname"],
        "topic.prefix": "dbserver1",
        "table.include.list": "public.customers,public.drivers",
        "plugin.name": "pgoutput",
        "slot.name": "project3_slot",
        "publication.name": "project3_publication",
        "snapshot.mode": "initial",
        "tombstones.on.delete": "true",
        "decimal.handling.mode": "double",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
    }


def wait_for_connect(timeout_seconds=60):
    ensure_package("requests")
    import requests

    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(connect_url(), timeout=5)
            if response.ok:
                return
            last_error = f"HTTP {response.status_code}: {response.text}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(2)
    raise RuntimeError(f"Kafka Connect did not become ready: {last_error}")


def register_debezium():
    ensure_package("requests")
    import requests

    wait_for_connect()
    config = connector_config()
    response = requests.put(
        f"{connect_url()}/connectors/{CONNECTOR_NAME}/config",
        headers={"Content-Type": "application/json"},
        data=json.dumps(config),
        timeout=15,
    )
    response.raise_for_status()

    restart = requests.post(
        f"{connect_url()}/connectors/{CONNECTOR_NAME}/restart",
        timeout=15,
    )
    if restart.status_code not in (204, 409):
        restart.raise_for_status()

    print(
        json.dumps(
            {
                "connector": CONNECTOR_NAME,
                "snapshot_mode": config["snapshot.mode"],
                "tables": config["table.include.list"],
            },
            indent=2,
        )
    )


def connector_health_check():
    ensure_package("requests")
    import requests

    wait_for_connect()
    response = requests.get(
        f"{connect_url()}/connectors/{CONNECTOR_NAME}/status",
        timeout=10,
    )
    response.raise_for_status()

    status = response.json()
    connector_state = status.get("connector", {}).get("state")
    task_states = [task.get("state") for task in status.get("tasks", [])]
    if connector_state != "RUNNING":
        raise RuntimeError(
            f"Connector {CONNECTOR_NAME} is not RUNNING: {connector_state}"
        )
    if not task_states or any(state != "RUNNING" for state in task_states):
        raise RuntimeError(
            f"Connector tasks are not healthy: {json.dumps(status, indent=2)}"
        )

    print(json.dumps(status, indent=2))


def get_spark(app_name):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config(
            "spark.sql.catalog.lakehouse.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config(
            "spark.sql.catalog.lakehouse.s3.access-key-id",
            get_env("AWS_ACCESS_KEY_ID"),
        )
        .config(
            "spark.sql.catalog.lakehouse.s3.secret-access-key",
            get_env("AWS_SECRET_ACCESS_KEY"),
        )
        .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_namespaces(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.system")
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.cdc")
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")


def ensure_watermark_table(spark):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            job_name STRING,
            stream_name STRING,
            source_topic STRING,
            source_partition INT,
            last_offset BIGINT,
            updated_at TIMESTAMP
        ) USING iceberg
        """
    )


def ensure_bronze_cdc_table(spark):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_CDC_TABLE} (
            key BINARY,
            value BINARY,
            topic STRING,
            `partition` INT,
            `offset` BIGINT,
            `timestamp` TIMESTAMP,
            timestampType INT,
            key_str STRING,
            value_str STRING,
            source_table STRING,
            payload STRING,
            op STRING,
            before_json STRING,
            after_json STRING,
            ts_ms BIGINT,
            snapshot STRING,
            is_tombstone BOOLEAN,
            ingested_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (source_table)
        """
    )


def ensure_bronze_taxi_table(spark):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TAXI_TABLE} (
            key BINARY,
            value BINARY,
            topic STRING,
            `partition` INT,
            `offset` BIGINT,
            `timestamp` TIMESTAMP,
            timestampType INT,
            ingested_at TIMESTAMP
        ) USING iceberg
        """
    )


def ensure_silver_cdc_tables(spark):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_CUSTOMERS_TABLE} (
            id INT,
            name STRING,
            email STRING,
            country STRING,
            created_at TIMESTAMP,
            source_ts_ms BIGINT,
            source_op STRING,
            source_topic STRING,
            source_partition INT,
            source_offset BIGINT,
            updated_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_DRIVERS_TABLE} (
            id INT,
            name STRING,
            license_number STRING,
            rating DOUBLE,
            city STRING,
            active BOOLEAN,
            created_at TIMESTAMP,
            source_ts_ms BIGINT,
            source_op STRING,
            source_topic STRING,
            source_partition INT,
            source_offset BIGINT,
            updated_at TIMESTAMP
        ) USING iceberg
        """
    )


def existing_offsets_for_table(spark, table_name):
    from pyspark.sql import functions as F

    if not spark.catalog.tableExists(table_name):
        return None

    return (
        spark.table(table_name)
        .groupBy("topic", "partition")
        .agg(F.max("offset").alias("last_offset"))
    )


def read_new_kafka_rows(spark, topics, table_name):
    from pyspark.sql import functions as F

    batch = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap())
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    existing_offsets = existing_offsets_for_table(spark, table_name)
    if existing_offsets is not None:
        batch = (
            batch.join(existing_offsets, ["topic", "partition"], "left")
            .filter(F.col("offset") > F.coalesce(F.col("last_offset"), F.lit(-1)))
            .drop("last_offset")
        )

    return batch


def watermark_offsets_for_job(spark, job_name, bronze_rows, topic_column, partition_column):
    from pyspark.sql import functions as F

    if not spark.catalog.tableExists(WATERMARK_TABLE):
        return bronze_rows.withColumn("last_offset", F.lit(-1))

    watermarks = (
        spark.table(WATERMARK_TABLE)
        .filter(F.col("job_name") == job_name)
        .select(
            F.col("source_topic").alias(topic_column),
            F.col("source_partition").alias(partition_column),
            F.col("last_offset"),
        )
    )
    return bronze_rows.join(watermarks, [topic_column, partition_column], "left")


def update_watermarks(spark, job_name, stream_name, offsets_df):
    from pyspark.sql import functions as F

    if not offsets_df.take(1):
        return

    updates = (
        offsets_df.select(
            F.lit(job_name).alias("job_name"),
            F.lit(stream_name).alias("stream_name"),
            F.col("source_topic"),
            F.col("source_partition"),
            F.col("last_offset"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["job_name", "source_topic", "source_partition"])
    )
    updates.createOrReplaceTempView("watermark_updates")
    spark.sql(
        f"""
        MERGE INTO {WATERMARK_TABLE} AS target
        USING watermark_updates AS source
        ON target.job_name = source.job_name
           AND target.source_topic = source.source_topic
           AND target.source_partition = source.source_partition
        WHEN MATCHED THEN UPDATE SET
            target.stream_name = source.stream_name,
            target.last_offset = source.last_offset,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            job_name,
            stream_name,
            source_topic,
            source_partition,
            last_offset,
            updated_at
        ) VALUES (
            source.job_name,
            source.stream_name,
            source.source_topic,
            source.source_partition,
            source.last_offset,
            source.updated_at
        )
        """
    )


def run_bronze_cdc():
    from pyspark.sql import functions as F

    spark = get_spark("project3-bronze-cdc")
    try:
        ensure_namespaces(spark)
        ensure_bronze_cdc_table(spark)
        batch = read_new_kafka_rows(spark, CDC_TOPICS, BRONZE_CDC_TABLE)
        if not batch.take(1):
            print("No new CDC Kafka messages available.")
            return False

        bronze_rows = (
            batch.withColumn("key_str", F.col("key").cast("string"))
            .withColumn("value_str", F.col("value").cast("string"))
            .withColumn("source_table", F.regexp_extract("topic", r"dbserver1\.public\.(.+)$", 1))
            .withColumn("payload", F.get_json_object("value_str", "$.payload"))
            .withColumn("op", F.get_json_object("value_str", "$.payload.op"))
            .withColumn("before_json", F.get_json_object("value_str", "$.payload.before"))
            .withColumn("after_json", F.get_json_object("value_str", "$.payload.after"))
            .withColumn("ts_ms", F.get_json_object("value_str", "$.payload.ts_ms").cast("bigint"))
            .withColumn(
                "snapshot",
                F.get_json_object("value_str", "$.payload.source.snapshot"),
            )
            .withColumn("is_tombstone", F.col("value").isNull())
            .withColumn("ingested_at", F.current_timestamp())
            .select(
                "key",
                "value",
                "topic",
                "partition",
                "offset",
                "timestamp",
                "timestampType",
                "key_str",
                "value_str",
                "source_table",
                "payload",
                "op",
                "before_json",
                "after_json",
                "ts_ms",
                "snapshot",
                "is_tombstone",
                "ingested_at",
            )
        )

        bronze_rows = bronze_rows.cache()
        row_count = bronze_rows.count()
        bronze_rows.writeTo(BRONZE_CDC_TABLE).append()
        bronze_rows.unpersist()
        print(f"Wrote {row_count} CDC events to {BRONZE_CDC_TABLE}.")
        return True
    finally:
        spark.stop()


def normalize_cdc_source_table(rows):
    from pyspark.sql import functions as F

    return rows.withColumn(
        "source_table",
        F.when(
            F.col("source_table").isNull() | (F.trim(F.col("source_table")) == ""),
            F.regexp_extract("topic", r"dbserver1\.public\.(.+)$", 1),
        ).otherwise(F.col("source_table")),
    )


def decode_debezium_decimal(column, scale):
    from pyspark.sql import functions as F

    text_value = column.cast("string")
    return (
        F.when(text_value.isNull(), F.lit(None).cast("double"))
        .when(text_value.rlike(r"^-?\d+(\.\d+)?$"), text_value.cast("double"))
        .otherwise(
            F.conv(F.hex(F.unbase64(text_value)), 16, 10).cast("double")
            / F.lit(float(10**scale))
        )
    )


def customers_updates(events):
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True),
            StructField("created_at", LongType(), True),
        ]
    )
    return (
        events.withColumn(
            "row_json",
            F.when(F.col("op") == "d", F.col("before_json")).otherwise(F.col("after_json")),
        )
        .filter(F.col("row_json").isNotNull())
        .withColumn("row_data", F.from_json("row_json", schema))
        .select(
            F.col("row_data.id").alias("id"),
            F.col("row_data.name").alias("name"),
            F.col("row_data.email").alias("email"),
            F.col("row_data.country").alias("country"),
            F.expr("timestamp_micros(row_data.created_at)").alias("created_at"),
            F.col("ts_ms").alias("source_ts_ms"),
            F.col("op").alias("source_op"),
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.current_timestamp().alias("updated_at"),
        )
    )


def drivers_updates(events):
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("license_number", StringType(), True),
            StructField("city", StringType(), True),
            StructField("active", BooleanType(), True),
            StructField("created_at", LongType(), True),
        ]
    )
    return (
        events.withColumn(
            "row_json",
            F.when(F.col("op") == "d", F.col("before_json")).otherwise(F.col("after_json")),
        )
        .filter(F.col("row_json").isNotNull())
        .withColumn("row_data", F.from_json("row_json", schema))
        .select(
            F.col("row_data.id").alias("id"),
            F.col("row_data.name").alias("name"),
            F.col("row_data.license_number").alias("license_number"),
            decode_debezium_decimal(F.get_json_object("row_json", "$.rating"), 2).alias("rating"),
            F.col("row_data.city").alias("city"),
            F.col("row_data.active").alias("active"),
            F.expr("timestamp_micros(row_data.created_at)").alias("created_at"),
            F.col("ts_ms").alias("source_ts_ms"),
            F.col("op").alias("source_op"),
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.current_timestamp().alias("updated_at"),
        )
    )


def latest_per_primary_key(updates, primary_key):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy(primary_key).orderBy(
        F.col("source_ts_ms").desc_nulls_last(), F.col("source_offset").desc()
    )
    return (
        updates.withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def merge_cdc_updates(spark, updates, table_name, primary_key, columns, view_name):
    if not updates.take(1):
        return

    latest = latest_per_primary_key(updates, primary_key)
    latest.createOrReplaceTempView(view_name)
    update_clause = ",\n            ".join(
        f"target.{column} = source.{column}" for column in columns
    )
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"source.{column}" for column in columns)
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING {view_name} AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED AND source.source_op = 'd' THEN DELETE
        WHEN MATCHED AND source.source_op IN ('c', 'r', 'u') THEN UPDATE SET
            {update_clause}
        WHEN NOT MATCHED AND source.source_op IN ('c', 'r', 'u') THEN INSERT ({insert_columns})
        VALUES ({insert_values})
        """
    )


def run_silver_cdc():
    from pyspark.sql import functions as F

    spark = get_spark("project3-silver-cdc")
    try:
        ensure_namespaces(spark)
        ensure_watermark_table(spark)
        ensure_silver_cdc_tables(spark)

        if not spark.catalog.tableExists(BRONZE_CDC_TABLE):
            print("Bronze CDC table does not exist yet.")
            return False

        bronze = normalize_cdc_source_table(spark.table(BRONZE_CDC_TABLE))
        if not bronze.take(1):
            print("Bronze CDC table is empty.")
            return False

        customers_empty = not spark.table(SILVER_CUSTOMERS_TABLE).take(1)
        drivers_empty = not spark.table(SILVER_DRIVERS_TABLE).take(1)
        if customers_empty or drivers_empty:
            new_rows = bronze
            print("A CDC silver table is empty; performing full bronze backfill.")
        else:
            with_watermarks = watermark_offsets_for_job(
                spark,
                "silver-cdc",
                bronze,
                "topic",
                "partition",
            )
            new_rows = with_watermarks.filter(
                F.col("offset") > F.coalesce(F.col("last_offset"), F.lit(-1))
            ).drop("last_offset")
        if not new_rows.take(1):
            print("No new bronze CDC rows to merge.")
            return False

        actionable = new_rows.filter(~F.col("is_tombstone"))

        merge_cdc_updates(
            spark,
            customers_updates(actionable.filter(F.col("source_table") == "customers")),
            SILVER_CUSTOMERS_TABLE,
            "id",
            [
                "id",
                "name",
                "email",
                "country",
                "created_at",
                "source_ts_ms",
                "source_op",
                "source_topic",
                "source_partition",
                "source_offset",
                "updated_at",
            ],
            "customers_cdc_updates",
        )
        merge_cdc_updates(
            spark,
            drivers_updates(actionable.filter(F.col("source_table") == "drivers")),
            SILVER_DRIVERS_TABLE,
            "id",
            [
                "id",
                "name",
                "license_number",
                "rating",
                "city",
                "active",
                "created_at",
                "source_ts_ms",
                "source_op",
                "source_topic",
                "source_partition",
                "source_offset",
                "updated_at",
            ],
            "drivers_cdc_updates",
        )

        watermark_updates = (
            new_rows.groupBy("topic", "partition")
            .agg(F.max("offset").alias("last_offset"))
            .select(
                F.col("topic").alias("source_topic"),
                F.col("partition").alias("source_partition"),
                "last_offset",
            )
        )
        update_watermarks(spark, "silver-cdc", "bronze-cdc", watermark_updates)
        print("Silver CDC tables merged successfully.")
        return True
    finally:
        spark.stop()


def trip_schema():
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("VendorID", LongType(), True),
            StructField("tpep_pickup_datetime", StringType(), True),
            StructField("tpep_dropoff_datetime", StringType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("RatecodeID", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("payment_type", LongType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("Airport_fee", DoubleType(), True),
            StructField("cbd_congestion_fee", DoubleType(), True),
        ]
    )


def run_bronze_taxi():
    from pyspark.sql import functions as F

    spark = get_spark("project3-bronze-taxi")
    try:
        ensure_namespaces(spark)
        ensure_bronze_taxi_table(spark)
        batch = read_new_kafka_rows(spark, [TAXI_TOPIC], BRONZE_TAXI_TABLE)
        if not batch.take(1):
            print("No new taxi Kafka messages available.")
            return

        bronze_rows = batch.withColumn("ingested_at", F.current_timestamp()).select(
            "key",
            "value",
            "topic",
            "partition",
            "offset",
            "timestamp",
            "timestampType",
            "ingested_at",
        )
        bronze_rows = bronze_rows.cache()
        row_count = bronze_rows.count()
        bronze_rows.writeTo(BRONZE_TAXI_TABLE).append()
        bronze_rows.unpersist()
        print(f"Wrote {row_count} taxi events to {BRONZE_TAXI_TABLE}.")
    finally:
        spark.stop()


def transform_taxi_rows(spark, bronze_rows):
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    parsed = (
        bronze_rows.withColumn("value_str", F.col("value").cast("string"))
        .withColumn("trip", F.from_json(F.col("value_str"), trip_schema()))
        .select(
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.col("timestamp").alias("kafka_ingest_ts"),
            F.col("trip.*"),
        )
    )

    cleaned = (
        parsed.withColumn(
            "pickup_datetime",
            F.to_timestamp(
                F.regexp_replace("tpep_pickup_datetime", " ", "T"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn(
            "dropoff_datetime",
            F.to_timestamp(
                F.regexp_replace("tpep_dropoff_datetime", " ", "T"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
        .withColumn(
            "passenger_count",
            F.when(
                F.col("passenger_count").cast(IntegerType()).between(1, 9),
                F.col("passenger_count").cast(IntegerType()),
            ).otherwise(F.lit(None).cast(IntegerType())),
        )
        .withColumn(
            "trip_distance",
            F.when(F.col("trip_distance") > 0, F.col("trip_distance")).otherwise(None),
        )
        .withColumn(
            "RatecodeID",
            F.when(
                F.col("RatecodeID").cast(IntegerType()).between(1, 6),
                F.col("RatecodeID").cast(IntegerType()),
            ).otherwise(None),
        )
        .withColumn(
            "payment_type",
            F.when(F.col("payment_type").between(1, 6), F.col("payment_type")).otherwise(None),
        )
        .withColumn(
            "fare_amount",
            F.when(F.col("fare_amount") >= 0, F.col("fare_amount")).otherwise(None),
        )
        .withColumn(
            "total_amount",
            F.when(F.col("total_amount") >= 0, F.col("total_amount")).otherwise(None),
        )
        .withColumn("is_valid_window", F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .filter(F.col("is_valid_window"))
        .drop("is_valid_window")
        .withColumn(
            "trip_duration_min",
            F.round(
                (
                    F.unix_timestamp("dropoff_datetime")
                    - F.unix_timestamp("pickup_datetime")
                )
                / 60.0,
                2,
            ),
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
    )

    dedup_window = Window.partitionBy(
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID",
    ).orderBy(F.col("source_offset").desc())
    deduped = (
        cleaned.withColumn("row_number", F.row_number().over(dedup_window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    zones = spark.read.parquet(
        "/home/jovyan/project/data/taxi_zone_lookup.parquet"
    ).alias("zones")
    enriched = (
        deduped.alias("trip")
        .join(
            zones.select(
                F.col("LocationID").alias("pu_loc_id"),
                F.col("Zone").alias("pickup_zone"),
                F.col("Borough").alias("pickup_borough"),
            ),
            F.col("trip.PULocationID") == F.col("pu_loc_id"),
            "left",
        )
        .drop("pu_loc_id")
        .join(
            zones.select(
                F.col("LocationID").alias("do_loc_id"),
                F.col("Zone").alias("dropoff_zone"),
                F.col("Borough").alias("dropoff_borough"),
            ),
            F.col("trip.DOLocationID") == F.col("do_loc_id"),
            "left",
        )
        .drop("do_loc_id")
    )

    return enriched.select(
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "trip_duration_min",
        "pickup_date",
        "PULocationID",
        "pickup_zone",
        "pickup_borough",
        "DOLocationID",
        "dropoff_zone",
        "dropoff_borough",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "Airport_fee",
        "cbd_congestion_fee",
        "total_amount",
        "payment_type",
        "kafka_ingest_ts",
        "source_topic",
        "source_partition",
        "source_offset",
    )


def create_silver_taxi_table(initial_rows):
    (
        initial_rows.writeTo(SILVER_TAXI_TABLE)
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .partitionedBy("pickup_date")
        .create()
    )


def merge_silver_taxi(spark, updates):
    columns = [field.name for field in updates.schema.fields]
    updates.createOrReplaceTempView("silver_taxi_updates")
    update_clause = ",\n            ".join(
        f"target.{column} = source.{column}" for column in columns
    )
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"source.{column}" for column in columns)
    spark.sql(
        f"""
        MERGE INTO {SILVER_TAXI_TABLE} AS target
        USING silver_taxi_updates AS source
        ON target.VendorID = source.VendorID
           AND target.pickup_datetime = source.pickup_datetime
           AND target.dropoff_datetime = source.dropoff_datetime
           AND target.PULocationID = source.PULocationID
           AND target.DOLocationID = source.DOLocationID
        WHEN MATCHED THEN UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
        """
    )


def run_silver_taxi():
    from pyspark.sql import functions as F

    spark = get_spark("project3-silver-taxi")
    try:
        ensure_namespaces(spark)
        ensure_watermark_table(spark)

        if not spark.catalog.tableExists(BRONZE_TAXI_TABLE):
            print("Bronze taxi table does not exist yet.")
            return

        bronze = spark.table(BRONZE_TAXI_TABLE)
        if not bronze.take(1):
            print("Bronze taxi table is empty.")
            return

        with_watermarks = watermark_offsets_for_job(
            spark,
            "silver-taxi",
            bronze,
            "topic",
            "partition",
        )
        new_rows = with_watermarks.filter(
            F.col("offset") > F.coalesce(F.col("last_offset"), F.lit(-1))
        ).drop("last_offset")
        if not new_rows.take(1):
            print("No new bronze taxi rows to merge.")
            return

        silver_updates = transform_taxi_rows(spark, new_rows)
        if not silver_updates.take(1):
            print("No valid taxi rows produced from new bronze events.")
            watermark_updates = (
                new_rows.groupBy("topic", "partition")
                .agg(F.max("offset").alias("last_offset"))
                .select(
                    F.col("topic").alias("source_topic"),
                    F.col("partition").alias("source_partition"),
                    "last_offset",
                )
            )
            update_watermarks(spark, "silver-taxi", "bronze-taxi", watermark_updates)
            return

        if not spark.catalog.tableExists(SILVER_TAXI_TABLE):
            create_silver_taxi_table(silver_updates)
        else:
            merge_silver_taxi(spark, silver_updates)

        watermark_updates = (
            new_rows.groupBy("topic", "partition")
            .agg(F.max("offset").alias("last_offset"))
            .select(
                F.col("topic").alias("source_topic"),
                F.col("partition").alias("source_partition"),
                "last_offset",
            )
        )
        update_watermarks(spark, "silver-taxi", "bronze-taxi", watermark_updates)
        print("Silver taxi table updated successfully.")
    finally:
        spark.stop()


def write_gold_table(df, table_name):
    if not df.take(1):
        print(f"No rows to write for {table_name}.")
        return

    writer = (
        df.writeTo(table_name)
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "zstd")
    )
    if not df.sparkSession.catalog.tableExists(table_name):
        writer.partitionedBy("pickup_date").create()
    else:
        writer.overwritePartitions()


def run_gold_taxi():
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = get_spark("project3-gold-taxi")
    try:
        ensure_namespaces(spark)
        if not spark.catalog.tableExists(SILVER_TAXI_TABLE):
            print("Silver taxi table does not exist yet.")
            return

        silver = spark.table(SILVER_TAXI_TABLE)
        if not silver.take(1):
            print("Silver taxi table is empty.")
            return

        trips_per_hour = (
            silver.withColumn(
                "pickup_hour", F.date_trunc("hour", F.col("pickup_datetime"))
            )
            .groupBy("pickup_date", "pickup_hour")
            .count()
            .withColumnRenamed("count", "trip_count")
        )
        average_fare = (
            silver.groupBy(
                "pickup_date", "PULocationID", "pickup_borough", "pickup_zone"
            )
            .agg(F.round(F.avg("fare_amount"), 2).alias("average_fare"))
            .withColumnRenamed("PULocationID", "pickup_location_id")
        )
        revenue_per_zone = (
            silver.groupBy(
                "pickup_date", "PULocationID", "pickup_borough", "pickup_zone"
            )
            .agg(F.round(F.sum("total_amount"), 2).alias("total_revenue"))
            .withColumnRenamed("PULocationID", "pickup_location_id")
        )

        airport_trip = (
            F.lower(F.coalesce(F.col("pickup_zone"), F.lit(""))).contains("airport")
            | F.lower(F.coalesce(F.col("dropoff_zone"), F.lit(""))).contains("airport")
        )
        segmented = silver.withColumn(
            "segment_name",
            F.when(airport_trip, F.lit("Airport run"))
            .when(F.col("trip_distance") < 2, F.lit("Short hop"))
            .when(F.col("trip_distance") <= 10, F.lit("City ride"))
            .otherwise(F.lit("Long haul")),
        ).withColumn(
            "pickup_zone",
            F.coalesce(F.col("pickup_zone"), F.lit("Unknown")),
        )

        daily_totals = segmented.groupBy("pickup_date").agg(
            F.count("*").alias("daily_trip_count")
        )
        segment_zone_counts = segmented.groupBy(
            "pickup_date", "segment_name", "pickup_zone"
        ).agg(F.count("*").alias("pickup_zone_trip_count"))
        most_common_pickup_zone = (
            segment_zone_counts.withColumn(
                "zone_rank",
                F.row_number().over(
                    Window.partitionBy("pickup_date", "segment_name").orderBy(
                        F.col("pickup_zone_trip_count").desc(),
                        F.col("pickup_zone"),
                    )
                ),
            )
            .filter(F.col("zone_rank") == 1)
            .select("pickup_date", "segment_name", "pickup_zone")
            .withColumnRenamed("pickup_zone", "most_common_pickup_zone")
        )
        trip_segments = (
            segmented.groupBy("pickup_date", "segment_name")
            .agg(
                F.count("*").alias("trip_count"),
                F.round(F.avg("fare_amount"), 2).alias("average_fare"),
                F.round(F.avg("tip_amount"), 2).alias("average_tip"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("trip_duration_min"), 2).alias("average_trip_duration"),
            )
            .join(daily_totals, ["pickup_date"], "inner")
            .withColumn(
                "trip_share_pct",
                F.round(F.col("trip_count") / F.col("daily_trip_count") * 100, 2),
            )
            .withColumn(
                "revenue_per_trip",
                F.round(F.col("total_revenue") / F.col("trip_count"), 2),
            )
            .drop("daily_trip_count")
            .join(most_common_pickup_zone, ["pickup_date", "segment_name"], "left")
        )
        segment_trends = (
            trip_segments.select("pickup_date", "segment_name", "trip_count")
            .withColumn(
                "previous_trip_count",
                F.lag("trip_count").over(
                    Window.partitionBy("segment_name").orderBy("pickup_date")
                ),
            )
            .withColumn(
                "trip_count_change",
                F.col("trip_count") - F.col("previous_trip_count"),
            )
            .withColumn(
                "trip_count_change_pct",
                F.round(
                    F.when(
                        F.col("previous_trip_count").isNull()
                        | (F.col("previous_trip_count") == 0),
                        None,
                    ).otherwise(
                        F.col("trip_count_change")
                        / F.col("previous_trip_count")
                        * 100
                    ),
                    2,
                ),
            )
            .withColumn(
                "trend_direction",
                F.when(F.col("trip_count_change") > 0, F.lit("increasing"))
                .when(F.col("trip_count_change") < 0, F.lit("decreasing"))
                .when(F.col("trip_count_change") == 0, F.lit("flat")),
            )
        )

        write_gold_table(trips_per_hour, GOLD_TRIPS_PER_HOUR_TABLE)
        write_gold_table(average_fare, GOLD_AVERAGE_FARE_PER_ZONE_TABLE)
        write_gold_table(revenue_per_zone, GOLD_REVENUE_PER_ZONE_TABLE)
        write_gold_table(trip_segments, GOLD_TRIP_SEGMENTS_TABLE)
        write_gold_table(segment_trends, GOLD_SEGMENT_TRENDS_TABLE)
        print("Gold taxi tables refreshed successfully.")
    finally:
        spark.stop()


def postgres_connection():
    ensure_package("psycopg2-binary", "psycopg2")
    import psycopg2

    return psycopg2.connect(**pg_config())


def fetch_postgres_rows_with_connection(conn, table_name, columns):
    query = f"SELECT {', '.join(columns)} FROM {table_name} ORDER BY id"
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def fetch_postgres_rows(table_name, columns):
    conn = postgres_connection()
    try:
        return fetch_postgres_rows_with_connection(conn, table_name, columns)
    finally:
        conn.close()


@contextmanager
def locked_postgres_snapshot(table_specs):
    conn = postgres_connection()
    table_names = ", ".join(spec["postgres_table"] for spec in table_specs)
    try:
        conn.set_session(
            isolation_level="REPEATABLE READ",
            readonly=True,
            autocommit=False,
        )
        with conn.cursor() as cursor:
            cursor.execute(f"LOCK TABLE {table_names} IN SHARE MODE")

        snapshots = {
            spec["postgres_table"]: fetch_postgres_rows_with_connection(
                conn,
                spec["postgres_table"],
                spec["columns"],
            )
            for spec in table_specs
        }
        yield snapshots
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


def normalize_timestamp(value):
    if value is None:
        return None
    return value.replace(tzinfo=None)


def normalize_customers(rows):
    normalized = []
    for row in rows:
        if isinstance(row, dict):
            normalized.append(
                (
                    int(row["id"]),
                    row["name"],
                    row["email"],
                    row["country"],
                    normalize_timestamp(row["created_at"]),
                )
            )
        else:
            normalized.append(
                (
                    int(row[0]),
                    row[1],
                    row[2],
                    row[3],
                    normalize_timestamp(row[4]),
                )
            )
    return normalized


def normalize_drivers(rows):
    normalized = []
    for row in rows:
        if isinstance(row, dict):
            normalized.append(
                (
                    int(row["id"]),
                    row["name"],
                    row["license_number"],
                    round(float(row["rating"]), 2) if row["rating"] is not None else None,
                    row["city"],
                    bool(row["active"]) if row["active"] is not None else None,
                    normalize_timestamp(row["created_at"]),
                )
            )
        else:
            normalized.append(
                (
                    int(row[0]),
                    row[1],
                    row[2],
                    round(float(row[3]), 2) if row[3] is not None else None,
                    row[4],
                    bool(row[5]) if row[5] is not None else None,
                    normalize_timestamp(row[6]),
                )
            )
    return normalized


def describe_validation_mismatch(postgres_rows, silver_rows):
    postgres_by_id = {row[0]: row for row in postgres_rows}
    silver_by_id = {row[0]: row for row in silver_rows}
    missing_ids = sorted(postgres_by_id.keys() - silver_by_id.keys())[:5]
    extra_ids = sorted(silver_by_id.keys() - postgres_by_id.keys())[:5]
    changed_ids = []
    for row_id in sorted(postgres_by_id.keys() & silver_by_id.keys()):
        if postgres_by_id[row_id] != silver_by_id[row_id]:
            changed_ids.append(row_id)
            if len(changed_ids) == 5:
                break

    details = [
        f"PostgreSQL row count: {len(postgres_rows)}",
        f"Silver row count: {len(silver_rows)}",
    ]
    if missing_ids:
        details.append(f"Missing ids in silver: {missing_ids}")
    if extra_ids:
        details.append(f"Extra ids in silver: {extra_ids}")
    if changed_ids:
        first_id = changed_ids[0]
        details.append(f"Changed ids: {changed_ids}")
        details.append(f"PostgreSQL row for id {first_id}: {postgres_by_id[first_id]}")
        details.append(f"Silver row for id {first_id}: {silver_by_id[first_id]}")
    return "\n".join(details)


def catch_up_cdc_until_idle():
    idle_passes = 0
    for attempt in range(1, VALIDATION_CATCHUP_PASSES + 1):
        bronze_changed = bool(run_bronze_cdc())
        silver_changed = bool(run_silver_cdc())
        print(
            json.dumps(
                {
                    "validation_catchup_attempt": attempt,
                    "bronze_changed": bronze_changed,
                    "silver_changed": silver_changed,
                }
            )
        )
        if bronze_changed or silver_changed:
            idle_passes = 0
            continue

        idle_passes += 1
        if idle_passes >= VALIDATION_IDLE_PASSES:
            return
        time.sleep(VALIDATION_IDLE_WAIT_SECONDS)

    raise RuntimeError(
        "Validation could not stabilize the CDC pipeline before comparison. "
        f"Reached {VALIDATION_CATCHUP_PASSES} catch-up passes without {VALIDATION_IDLE_PASSES} idle passes."
    )


def validate_table_pair(
    spark,
    silver_table,
    postgres_table,
    columns,
    normalizer,
    postgres_rows=None,
):
    if not spark.catalog.tableExists(silver_table):
        raise RuntimeError(f"Missing silver table: {silver_table}")

    spark.catalog.refreshTable(silver_table)
    silver_rows = [
        row.asDict(recursive=True)
        for row in spark.table(silver_table).select(*columns).orderBy("id").collect()
    ]
    if postgres_rows is None:
        postgres_rows = fetch_postgres_rows(postgres_table, columns)

    normalized_silver = normalizer(silver_rows)
    normalized_postgres = normalizer(postgres_rows)
    if normalized_silver != normalized_postgres:
        raise RuntimeError(
            f"Validation failed for {postgres_table}: silver rows do not match PostgreSQL.\n"
            f"{describe_validation_mismatch(normalized_postgres, normalized_silver)}"
        )

    print(
        json.dumps(
            {
                "table": postgres_table,
                "rows": len(normalized_postgres),
                "matched": True,
            },
            indent=2,
        )
    )


def run_validation():
    table_specs = [
        {
            "silver_table": SILVER_CUSTOMERS_TABLE,
            "postgres_table": "customers",
            "columns": ["id", "name", "email", "country", "created_at"],
            "normalizer": normalize_customers,
        },
        {
            "silver_table": SILVER_DRIVERS_TABLE,
            "postgres_table": "drivers",
            "columns": [
                "id",
                "name",
                "license_number",
                "rating",
                "city",
                "active",
                "created_at",
            ],
            "normalizer": normalize_drivers,
        },
    ]

    with locked_postgres_snapshot(table_specs) as postgres_snapshots:
        catch_up_cdc_until_idle()

        spark = get_spark("project3-validation")
        try:
            for table_spec in table_specs:
                validate_table_pair(
                    spark,
                    table_spec["silver_table"],
                    table_spec["postgres_table"],
                    table_spec["columns"],
                    table_spec["normalizer"],
                    postgres_rows=postgres_snapshots[table_spec["postgres_table"]],
                )

            for table_name in [
                BRONZE_CDC_TABLE,
                BRONZE_TAXI_TABLE,
                SILVER_TAXI_TABLE,
                GOLD_TRIPS_PER_HOUR_TABLE,
                GOLD_AVERAGE_FARE_PER_ZONE_TABLE,
                GOLD_REVENUE_PER_ZONE_TABLE,
                GOLD_TRIP_SEGMENTS_TABLE,
                GOLD_SEGMENT_TRENDS_TABLE,
            ]:
                if spark.catalog.tableExists(table_name):
                    spark.catalog.refreshTable(table_name)
                    print(f"{table_name}: {spark.table(table_name).count()} rows")
        finally:
            spark.stop()


COMMANDS = {
    "register-debezium": register_debezium,
    "connector-health-check": connector_health_check,
    "bronze-cdc": run_bronze_cdc,
    "silver-cdc": run_silver_cdc,
    "bronze-taxi": run_bronze_taxi,
    "silver-taxi": run_silver_taxi,
    "gold-taxi": run_gold_taxi,
    "validate": run_validation,
}


def main():
    parser = argparse.ArgumentParser(description="Project 3 pipeline entrypoint")
    parser.add_argument("command", choices=sorted(COMMANDS))
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc).isoformat()
    print(json.dumps({"command": args.command, "started_at": started_at}))
    COMMANDS[args.command]()


if __name__ == "__main__":
    main()