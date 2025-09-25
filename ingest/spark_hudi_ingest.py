from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, monotonically_increasing_id, current_timestamp

import os

spark = (
    SparkSession.builder
    .appName("Covid19-Hudi-Ingest")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # S3A -> MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

raw_csv_path = "/data/raw/covid19.csv"
df_raw = spark.read.csv(raw_csv_path, header=True)

# Wide -> Long (JHU-like)
date_cols = df_raw.columns[4:]  # Province/State, Country/Region, Lat, Long are first 4
df_long = (
    df_raw.selectExpr(
        "`Province/State` as Province",
        "`Country/Region` as Country",
        "Lat",
        "Long",
        *[f"`{c}`" for c in date_cols]
    )
    .selectExpr(
        "Province", "Country", "Lat", "Long",
        "stack(" + str(len(date_cols)) + "," +
        ",".join([f"'{c}', `{c}`" for c in date_cols]) +
        ") as (ReportDate, Confirmed)"
    )
)

df_final = df_long.withColumn("ReportDate", expr("to_date(ReportDate, 'M/d/yy')"))
df_final = (
    df_final
    .withColumn("row_id", monotonically_increasing_id())  # primary key
    .withColumn("ingest_ts", current_timestamp())         # precombine
    .withColumn("year", expr("year(ReportDate)"))         # partition
)

hudi_options = {
    "hoodie.table.name": "covid19",
    "hoodie.datasource.write.recordkey.field": "row_id",
    "hoodie.datasource.write.precombine.field": "ingest_ts",
    "hoodie.datasource.write.partitionpath.field": "year",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",

    # Hive sync configs (correct prefix = hoodie)
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "covid19",
    "hoodie.datasource.hive_sync.partition_fields": "year",
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    # JDBC (optional, if you want sync via Postgres instead of HMS)
    # "hoodie.datasource.hive_sync.jdbc_url": "jdbc:postgresql://postgres:5432/metastore",
    # "hoodie.datasource.hive_sync.username": "hive",
    # "hoodie.datasource.hive_sync.password": "hive",
}


hudi_path = "s3a://hudi-datasets/covid19"

(
    df_final.write.format("hudi")
    .options(**hudi_options)
    .mode("overwrite")
    .save(hudi_path)
)

print("✅ Ingest complete: Hudi table 'default.covid19' is ready.")
