"""
Unit tests for GCPDataReader.
GCS and Spark are fully mocked — no real connections needed.
"""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def data_parameters():
    from ingestion.data_parameters import DataParameters
    return DataParameters(
        bucket_name="test-bucket",
        gcp_table_name="paysim",
        snowflake_table_name="raw_transactions",
        snowflake_schema="RAW",
        format="csv",
    )


@pytest.fixture
def mock_spark():
    return MagicMock()


@pytest.fixture
def reader(data_parameters, mock_spark):
    with patch("ingestion.core.raw.gcp_data_reader.get_bucket") as mock_get_bucket:
        mock_bucket = MagicMock()
        mock_bucket.name = "test-bucket"
        mock_get_bucket.return_value = mock_bucket

        from ingestion.core.raw.gcp_data_reader import GCPDataReader
        return GCPDataReader(spark=mock_spark, data_parameters=data_parameters)


# ── tests ─────────────────────────────────────────────────────────────────────

class TestListDataFilesFromGCP:
    def test_returns_file_paths(self, reader):
        blob1 = MagicMock()
        blob1.name = "landing/incoming/paysim/file1.csv"
        blob2 = MagicMock()
        blob2.name = "landing/incoming/paysim/file2.csv"
        reader.bucket.list_blobs.return_value = [blob1, blob2]

        result = reader.list_data_files_from_gcp()

        assert len(result) == 2
        assert all(p.startswith("gs://test-bucket/") for p in result)

    def test_excludes_directory_entries(self, reader):
        blob_dir = MagicMock()
        blob_dir.name = "landing/incoming/paysim/"
        blob_file = MagicMock()
        blob_file.name = "landing/incoming/paysim/file.csv"
        reader.bucket.list_blobs.return_value = [blob_dir, blob_file]

        result = reader.list_data_files_from_gcp()

        assert len(result) == 1

    def test_returns_empty_list_when_no_files(self, reader):
        reader.bucket.list_blobs.return_value = []
        result = reader.list_data_files_from_gcp()
        assert result == []


class TestGetDataframeFromInbound:
    def test_returns_none_when_no_files(self, reader):
        result = reader.get_dataframe_from_inbound([])
        assert result is None

    def test_reads_csv_files(self, reader):
        mock_df = MagicMock()
        (
            reader.spark.read
            .format.return_value
            .options.return_value
            .option.return_value
            .load.return_value
        ) = mock_df

        result = reader.get_dataframe_from_inbound(["gs://test-bucket/file.csv"])

        assert result == mock_df
        reader.spark.read.format.assert_called_once_with("csv")


class TestMoveInboundFilesToProcessed:
    def test_moves_files_to_processed(self, reader):
        source_blob = MagicMock()
        reader.bucket.blob.return_value = source_blob

        reader.move_inbound_files_to_processed(
            ["gs://test-bucket/landing/incoming/paysim/file.csv"]
        )

        reader.bucket.copy_blob.assert_called_once()
        source_blob.delete.assert_called_once()

    def test_skips_missing_files(self, reader):
        from google.cloud.exceptions import NotFound
        reader.bucket.blob.return_value = MagicMock()
        reader.bucket.copy_blob.side_effect = NotFound("not found")

        # Should not raise
        reader.move_inbound_files_to_processed(
            ["gs://test-bucket/landing/incoming/paysim/missing.csv"]
        )

