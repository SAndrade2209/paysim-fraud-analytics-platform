from src.clients.gcp import get_bucket
from src.clients.snowflake import get_snowflake_connection
from src.config.settings import *
from src.clients.spark_builder import get_spark

bucket = get_bucket(GCP_BUCKET)
print(bucket)

conn = get_snowflake_connection()
print("Snowflake connected")
