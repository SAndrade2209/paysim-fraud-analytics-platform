from pyspark.sql import SparkSession
from ingestion.config.settings import SPARK_GCP_JAR

def get_spark(app_name: str):
    spark = (
        SparkSession.builder
        .appName(app_name)

        .config(
            "spark.jars",
            SPARK_GCP_JAR
        )
        .config(
            "spark.jars.packages",
            ",".join([
                "net.snowflake:spark-snowflake_2.12:3.1.1",
                "net.snowflake:snowflake-jdbc:3.16.0"
            ])
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )

        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )

        .config(
            "spark.hadoop.google.cloud.auth.service.account.enable",
            "true"
        )

        .getOrCreate()
    )

    return spark