import snowflake.connector
from cryptography.hazmat.primitives import serialization
from ingestion.config.settings import (
SNOWFLAKE_PRIVATE_KEY_PATH
, SNOWFLAKE_USER
, SNOWFLAKE_ACCOUNT
, SNOWFLAKE_WAREHOUSE
, SNOWFLAKE_DATABASE
, SNOWFLAKE_PASSWORD
, RAW_SNOWFLAKE_SCHEMA)

def get_snowflake_connection():
    with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=None
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        private_key=pkb,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=RAW_SNOWFLAKE_SCHEMA
    )

def get_snowflake_spark_options():
    return {
        "sfURL": f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SNOWFLAKE_USER,
        "sfPassword": SNOWFLAKE_PASSWORD,
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": RAW_SNOWFLAKE_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE
    }