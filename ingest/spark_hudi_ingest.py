"""Spark ingestion job that reshapes the COVID-19 dataset and writes it as a Hudi table."""

from __future__ import annotations

import os
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column


RAW_DATA_PATH = os.getenv("COVID19_RAW_PATH", "/data/raw/covid19.csv")
HUDI_OUTPUT_PATH = os.getenv("HUDI_OUTPUT_PATH", "s3a://hudi-datasets/covid19")


def build_spark_session() -> SparkSession:
    """Create a Spark session configured to talk to MinIO and Hudi."""

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

    return (
        SparkSession.builder.appName("Covid19-Hudi-Ingest")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        # S3A -> MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def reshape_covid19(df_raw: DataFrame) -> DataFrame:
    """Convert the wide CSV format into a normalised long format."""

    date_columns = df_raw.columns[4:]  # First four columns are metadata fields.

    stacked = df_raw.selectExpr(
        "`Province/State` as Province",
        "`Country/Region` as Country",
        "Lat",
        "Long",
        *[f"`{column}`" for column in date_columns],
    ).selectExpr(
        "Province",
        "Country",
        "Lat",
        "Long",
        "stack(" +
        f"{len(date_columns)}," +
        ",".join([f"'{column}', `{column}`" for column in date_columns]) +
        ") as (ReportDate, Confirmed)",
    )

    cleaned = (
        stacked.withColumn("ReportDate", F.to_date("ReportDate", "M/d/yy"))
        .withColumn("Confirmed", F.col("Confirmed").cast("int"))
        .filter(F.col("ReportDate").isNotNull())
    )

    return cleaned


def add_hudi_columns(df: DataFrame) -> DataFrame:
    """Add the columns required by Hudi (record key, precombine key, partitions)."""

    def _coalesce(col_name: str) -> Column:
        return F.coalesce(F.col(col_name), F.lit(""))

    return (
        df.withColumn(
            "row_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    _coalesce("Province"),
                    _coalesce("Country"),
                    F.date_format("ReportDate", "yyyy-MM-dd"),
                ),
                256,
            ),
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("year", F.year("ReportDate"))
        .repartition("year")
    )


def build_hudi_options() -> Dict[str, str]:
    """Return the configuration required to write the Hudi table and sync to Hive."""

    return {
        "hoodie.table.name": "covid19",
        "hoodie.datasource.write.recordkey.field": "row_id",
        "hoodie.datasource.write.precombine.field": "ingest_ts",
        "hoodie.datasource.write.partitionpath.field": "year",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.keygenerator.type": "SIMPLE",
        # Hive sync configs
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
        "hoodie.datasource.hive_sync.database": "default",
        "hoodie.datasource.hive_sync.table": "covid19",
        "hoodie.datasource.hive_sync.partition_fields": "year",
        "hoodie.datasource.hive_sync.support_timestamp": "true",
    }


def main() -> None:
    spark = build_spark_session()
    try:
        df_raw = spark.read.csv(RAW_DATA_PATH, header=True)
        df_long = reshape_covid19(df_raw)
        df_final = add_hudi_columns(df_long)

        hudi_options = build_hudi_options()
        (
            df_final.write.format("hudi")
            .options(**hudi_options)
            .mode("overwrite")
            .save(HUDI_OUTPUT_PATH)
        )

        record_count = df_final.count()
        print(
            "✅ Ingest complete: default.covid19 registered with",
            f"{record_count} rows.",
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
