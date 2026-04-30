from src.clients.spark_builder import get_spark
from src.core.raw.gcp_data_reader import GCPDataReader
from src.data_parameters import DataParameters
from src.config.settings import GCP_BUCKET, RAW_SNOWFLAKE_SCHEMA

def main():
    spark = get_spark(app_name='raw_transactions')
    parameters = DataParameters(
        bucket_name=GCP_BUCKET,
        gcp_table_name="paysim",
        snowflake_table_name="raw_transactions",
        snowflake_schema=RAW_SNOWFLAKE_SCHEMA,
        format="csv"
    )

    gcp_data_reader = GCPDataReader(spark=spark, data_parameters=parameters)
    gcp_data_reader.move_data_from_landing_to_raw()

if __name__ == "__main__":
    main()