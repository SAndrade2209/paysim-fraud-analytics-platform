import os
from dotenv import load_dotenv

# Load .env from the project root regardless of the current working directory
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(dotenv_path=os.path.join(_PROJECT_ROOT, ".env"), override=True)

GCP_BUCKET = os.getenv("GCP_BUCKET")

INCOMING_PREFIX = "landing/incoming/"
PROCESSED_PREFIX = "landing/processed/"

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
RAW_SNOWFLAKE_SCHEMA = os.getenv("RAW_SNOWFLAKE_SCHEMA")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")

SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
SPARK_GCP_JAR = os.getenv("SPARK_GCP_JAR")