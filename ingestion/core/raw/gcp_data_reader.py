from pyspark.sql.functions import current_date, current_timestamp
from loguru import logger
from google.cloud.exceptions import NotFound
from ingestion.clients.gcp import get_bucket
from ingestion.data_parameters import DataParameters
from ingestion.clients.snowflake import get_snowflake_spark_options

class GCPDataReader:
    def __init__(self, spark, data_parameters : DataParameters):
        self.bucket = get_bucket(data_parameters.bucket_name)
        self.spark = spark
        self.gcp_table_name = data_parameters.gcp_table_name
        self.snowflake_table_name = data_parameters.snowflake_table_name
        self.error_path = f"gs://{self.bucket.name}/landing/error/{self.gcp_table_name}/"
        self.format = data_parameters.format
        self.snowflake_schema = data_parameters.snowflake_schema


    def list_data_files_from_gcp(self):
        logger.info(f"Listing data files from GCP")
        blobs = self.bucket.list_blobs(prefix=f'landing/incoming/{self.gcp_table_name}')
        file_paths = list(set(f"gs://{self.bucket.name}/{blob.name}"   for blob in blobs if not blob.name.endswith(f"/")))
        return file_paths

    def get_dataframe_from_inbound(self, inbound_files, landing_schema=None):
        logger.info(f"Getting dataframe from inbound files")
        if not inbound_files:
            logger.info(f"No inbound files found")
            return None
        else:
            logger.info(f"Found {len(inbound_files)} inbound files")
            if landing_schema:
                df = (
                    self.spark.read.format(self.format)
                    .schema(landing_schema)
                    .options(header="true")
                    .option("badRecordsPath", self.error_path)
                    .load(inbound_files)
                )
            else:
                df = (
                    self.spark.read.format(self.format)
                    .options(header="true", inferSchema="true")
                    .option("badRecordsPath", self.error_path)
                    .load(inbound_files)
                )
            logger.info(f"Returned df")
            return df

    def move_inbound_files_to_processed(self, inbound_files):
        logger.info(f"Moving files to processed")
        for file_path in inbound_files:
            blob_name = file_path.replace(f"gs://{self.bucket.name}/", "")
            source_blob = self.bucket.blob(blob_name)

            target_blob_name = blob_name.replace(
                "landing/incoming/",
                "landing/processed/",
                1
            )

            try:
                self.bucket.copy_blob(source_blob, self.bucket, target_blob_name)
                source_blob.delete()
                logger.info(f"Moved {blob_name} → processed/")
            except NotFound:
                logger.warning(
                    f"File {blob_name} not found in incoming/ — already moved or never uploaded. Skipping."
                )
        logger.info(f"Moved files to processed")

    def append_data_to_snowflake(self, df):
        if not df.isEmpty():
            logger.info(f"Appending data to snowflake")
            sf_options = get_snowflake_spark_options()
            sf_options["sfSchema"] = self.snowflake_schema

            # Use fully qualified name (DATABASE.SCHEMA.TABLE) so the JDBC
            # session does not need a default schema set.
            fully_qualified_table = f"{sf_options['sfDatabase']}.{self.snowflake_schema}.{self.snowflake_table_name}"
            logger.info(f"Snowflake table: {fully_qualified_table}")

            df.write \
                .format("snowflake") \
                .options(**sf_options) \
                .option("dbtable", fully_qualified_table) \
                .mode("append") \
                .save()
            logger.info(f"Snowflake table successfully appended to snowflake")

    def move_data_from_landing_to_raw(self, landing_schema=None):
        inbound_files = self.list_data_files_from_gcp()
        df = self.get_dataframe_from_inbound(inbound_files, landing_schema)
        if df and not df.isEmpty():
            df = df.withColumn("_ingestion_date", current_date()).withColumn("_raw_ingestion_timestamp", current_timestamp())

            self.append_data_to_snowflake(df)
            self.move_inbound_files_to_processed(inbound_files)
            logger.info(f"Moved data inbound files to processed")
        else:
            logger.warning("No data found")

