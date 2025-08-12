"""
Tests for sync functionality in mbx-db.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
from sqlalchemy.exc import SQLAlchemyError

from mbx_db.sync import (
    SyncResult,
    UpsertResult,
    SyncError,
    DatabaseSyncError,
    TransactionError,
    get_existing_records,
    upsert_records,
    sync_table_data,
)


class TestUpsertResult:
    """Test UpsertResult model."""

    def test_upsert_result_creation(self):
        """Test basic UpsertResult creation."""
        result = UpsertResult(table_name="test_table")
        assert result.table_name == "test_table"
        assert result.records_processed == 0
        assert result.records_created == 0
        assert result.records_updated == 0
        assert result.records_failed == 0
        assert result.errors == []
        assert result.duration_seconds == 0.0

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        result = UpsertResult(
            table_name="test_table",
            records_processed=10,
            records_created=6,
            records_updated=3,
            records_failed=1,
        )
        assert result.success_rate == 90.0  # (6+3)/10 * 100

    def test_success_rate_zero_processed(self):
        """Test success rate when no records processed."""
        result = UpsertResult(table_name="test_table")
        assert result.success_rate == 0.0


class TestSyncResult:
    """Test SyncResult model."""

    def test_sync_result_creation(self):
        """Test basic SyncResult creation."""
        result = SyncResult()
        assert result.total_tables == 0
        assert result.successful_tables == 0
        assert result.failed_tables == 0
        assert result.total_records_processed == 0
        assert result.table_results == []
        assert result.errors == []

    def test_add_table_result(self):
        """Test adding table results."""
        sync_result = SyncResult()
        table_result = UpsertResult(
            table_name="test_table",
            records_processed=10,
            records_created=8,
            records_updated=2,
            records_failed=0,
            duration_seconds=1.5,
        )

        sync_result.add_table_result(table_result)

        assert sync_result.total_tables == 1
        assert sync_result.successful_tables == 1
        assert sync_result.failed_tables == 0
        assert sync_result.total_records_processed == 10
        assert sync_result.total_records_created == 8
        assert sync_result.total_records_updated == 2
        assert sync_result.total_records_failed == 0
        assert sync_result.total_duration_seconds == 1.5
        assert len(sync_result.table_results) == 1

    def test_add_failed_table_result(self):
        """Test adding failed table results."""
        sync_result = SyncResult()
        table_result = UpsertResult(
            table_name="test_table",
            records_processed=10,
            records_created=5,
            records_updated=0,
            records_failed=5,
        )

        sync_result.add_table_result(table_result)

        assert sync_result.total_tables == 1
        assert sync_result.successful_tables == 0
        assert sync_result.failed_tables == 1

    def test_overall_success_rate(self):
        """Test overall success rate calculation."""
        sync_result = SyncResult()

        # Add successful table
        table_result1 = UpsertResult(
            table_name="table1",
            records_processed=10,
            records_created=8,
            records_updated=2,
            records_failed=0,
        )
        sync_result.add_table_result(table_result1)

        # Add partially failed table
        table_result2 = UpsertResult(
            table_name="table2",
            records_processed=10,
            records_created=5,
            records_updated=3,
            records_failed=2,
        )
        sync_result.add_table_result(table_result2)

        # Overall: (8+2+5+3)/(10+10) * 100 = 18/20 * 100 = 90%
        assert sync_result.overall_success_rate == 90.0


@pytest.mark.asyncio
class TestGetExistingRecords:
    """Test get_existing_records function."""

    async def test_get_existing_records_success(self):
        """Test successful retrieval of existing records."""
        # Mock engine and connection
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_conn = AsyncMock(spec=AsyncConnection)
        mock_engine.connect.return_value.__aenter__.return_value = mock_conn

        # Mock query result
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name", "value"]
        mock_result.__iter__.return_value = [
            (1, "test1", "value1"),
            (2, "test2", "value2"),
        ]
        mock_conn.execute.return_value = mock_result

        # Call function
        records = await get_existing_records(mock_engine, "test_table", "test_schema")

        # Verify results
        assert len(records) == 2
        assert records[0] == {"id": 1, "name": "test1", "value": "value1"}
        assert records[1] == {"id": 2, "name": "test2", "value": "value2"}

        # Verify query was called correctly
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0]
        assert 'SELECT * FROM "test_schema"."test_table"' in str(call_args[0])

    async def test_get_existing_records_empty(self):
        """Test retrieval when no records exist."""
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_conn = AsyncMock(spec=AsyncConnection)
        mock_engine.connect.return_value.__aenter__.return_value = mock_conn

        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name"]
        mock_result.__iter__.return_value = []
        mock_conn.execute.return_value = mock_result

        records = await get_existing_records(mock_engine, "empty_table")

        assert records == []

    async def test_get_existing_records_database_error(self):
        """Test handling of database errors."""
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_conn = AsyncMock(spec=AsyncConnection)
        mock_engine.connect.return_value.__aenter__.return_value = mock_conn

        # Mock database error
        mock_conn.execute.side_effect = SQLAlchemyError("Database connection failed")

        with pytest.raises(DatabaseSyncError) as exc_info:
            await get_existing_records(mock_engine, "test_table")

        assert "Failed to query existing records" in str(exc_info.value)


@pytest.mark.asyncio
class TestUpsertRecords:
    """Test upsert_records function."""

    async def test_upsert_records_empty_list(self):
        """Test upsert with empty record list."""
        mock_engine = AsyncMock(spec=AsyncEngine)

        result = await upsert_records(mock_engine, "test_table", [])

        assert result.table_name == "network.test_table"
        assert result.records_processed == 0
        assert result.records_created == 0
        assert result.records_updated == 0
        assert result.records_failed == 0

    async def test_upsert_records_success(self):
        """Test successful upsert operation."""
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_conn = AsyncMock(spec=AsyncConnection)
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn

        # Mock primary key query
        pk_result = MagicMock()
        pk_result.__iter__.return_value = [("id",)]
        mock_conn.execute.return_value = pk_result

        records = [
            {"id": 1, "name": "test1", "value": "value1"},
            {"id": 2, "name": "test2", "value": "value2"},
        ]

        result = await upsert_records(mock_engine, "test_table", records)

        assert result.table_name == "network.test_table"
        assert result.records_processed == 2
        assert result.records_created == 2  # Batch operation estimates as creates
        assert result.duration_seconds > 0

    async def test_upsert_records_with_conflict_columns(self):
        """Test upsert with specified conflict columns."""
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_conn = AsyncMock(spec=AsyncConnection)
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn

        records = [{"id": 1, "name": "test1"}]
        conflict_columns = ["id"]

        result = await upsert_records(
            mock_engine, "test_table", records, conflict_columns=conflict_columns
        )

        assert result.records_processed == 1
        # Should not query for primary key since conflict_columns provided
        assert mock_conn.execute.call_count >= 1  # At least the upsert query

    async def test_upsert_records_database_error(self):
        """Test handling of database errors during upsert."""
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_conn = AsyncMock(spec=AsyncConnection)
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn

        # Mock database error
        mock_conn.execute.side_effect = SQLAlchemyError("Constraint violation")

        records = [{"id": 1, "name": "test1"}]

        with pytest.raises(DatabaseSyncError) as exc_info:
            await upsert_records(mock_engine, "test_table", records)

        assert "Upsert operation failed" in str(exc_info.value)


@pytest.mark.asyncio
class TestSyncTableData:
    """Test sync_table_data function."""

    async def test_sync_table_data_empty(self):
        """Test sync with empty data."""
        mock_engine = AsyncMock(spec=AsyncEngine)

        result = await sync_table_data(mock_engine, "test_table", [])

        assert result.table_name == "network.test_table"
        assert result.records_processed == 0

    async def test_sync_table_data_dry_run(self):
        """Test sync in dry run mode."""
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_conn = AsyncMock(spec=AsyncConnection)
        mock_engine.connect.return_value.__aenter__.return_value = mock_conn

        # Mock existing records query
        existing_result = MagicMock()
        existing_result.keys.return_value = ["id", "name"]
        existing_result.__iter__.return_value = [(1, "existing")]

        # Mock primary key query
        pk_result = MagicMock()
        pk_result.__iter__.return_value = [("id",)]

        mock_conn.execute.side_effect = [existing_result, pk_result]

        data = [
            {"id": 1, "name": "updated"},  # Should be update
            {"id": 2, "name": "new"},  # Should be create
        ]

        result = await sync_table_data(mock_engine, "test_table", data, dry_run=True)

        assert result.records_processed == 2
        assert result.records_created == 1
        assert result.records_updated == 1

    @patch("mbx_db.sync.upsert_records")
    async def test_sync_table_data_actual_sync(self, mock_upsert):
        """Test actual sync operation (not dry run)."""
        mock_engine = AsyncMock(spec=AsyncEngine)

        # Mock upsert_records return value
        mock_upsert_result = UpsertResult(
            table_name="network.test_table",
            records_processed=2,
            records_created=1,
            records_updated=1,
        )
        mock_upsert.return_value = mock_upsert_result

        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

        result = await sync_table_data(mock_engine, "test_table", data)

        # Should call upsert_records
        mock_upsert.assert_called_once_with(
            mock_engine, "test_table", data, "network", None
        )

        # Should return the upsert result
        assert result.records_processed == 2
        assert result.records_created == 1
        assert result.records_updated == 1


class TestExceptions:
    """Test custom exception classes."""

    def test_sync_error(self):
        """Test SyncError exception."""
        error = SyncError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)

    def test_database_sync_error(self):
        """Test DatabaseSyncError exception."""
        error = DatabaseSyncError("Database error")
        assert str(error) == "Database error"
        assert isinstance(error, SyncError)

    def test_transaction_error(self):
        """Test TransactionError exception."""
        error = TransactionError("Transaction error")
        assert str(error) == "Transaction error"
        assert isinstance(error, SyncError)


if __name__ == "__main__":
    pytest.main([__file__])
