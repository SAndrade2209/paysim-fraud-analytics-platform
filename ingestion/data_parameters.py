from dataclasses import dataclass

@dataclass
class DataParameters:
    bucket_name: str
    gcp_table_name: str
    snowflake_table_name: str
    snowflake_schema: str
    format: str = "csv"