"""
Sync functionality for mbx-db package.

This module provides data synchronization capabilities including result models,
database operations, and transaction management for inventory data sync.
"""

from __future__ import annotations
from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)


class UpsertResult(BaseModel):
    """Result of an upsert operation on a single table."""

    table_name: str
    records_processed: int = 0
    records_created: int = 0
    records_updated: int = 0
    records_failed: int = 0
    errors: List[str] = Field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.records_processed == 0:
            return 0.0
        return (
            (self.records_created + self.records_updated) / self.records_processed
        ) * 100


class SyncResult(BaseModel):
    """Result of a complete sync operation across multiple tables."""

    total_tables: int = 0
    successful_tables: int = 0
    failed_tables: int = 0
    total_records_processed: int = 0
    total_records_created: int = 0
    total_records_updated: int = 0
    total_records_failed: int = 0
    total_duration_seconds: float = 0.0
    table_results: List[UpsertResult] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def overall_success_rate(self) -> float:
        """Calculate overall success rate as percentage."""
        if self.total_records_processed == 0:
            return 0.0
        successful_records = self.total_records_created + self.total_records_updated
        return (successful_records / self.total_records_processed) * 100

    def add_table_result(self, result: UpsertResult) -> None:
        """Add a table result and update totals."""
        self.table_results.append(result)
        self.total_tables += 1
        self.total_records_processed += result.records_processed
        self.total_records_created += result.records_created
        self.total_records_updated += result.records_updated
        self.total_records_failed += result.records_failed
        self.total_duration_seconds += result.duration_seconds

        if result.records_failed == 0 and result.records_processed > 0:
            self.successful_tables += 1
        elif result.records_failed > 0:
            self.failed_tables += 1


class SyncError(Exception):
    """Base exception for sync operations."""

    pass


class DatabaseSyncError(SyncError):
    """Database-specific sync errors."""

    pass


class TransactionError(SyncError):
    """Transaction management errors."""

    pass


async def get_existing_records(
    engine: AsyncEngine, table_name: str, schema: str = "network"
) -> List[Dict[str, Any]]:
    """
    Query existing records from a database table.

    Args:
        engine: SQLAlchemy async engine
        table_name: Name of the table to query
        schema: Database schema name (default: "network")

    Returns:
        List of dictionaries representing existing records

    Raises:
        DatabaseSyncError: If query fails
    """
    try:
        async with engine.connect() as conn:
            # Use text() for dynamic table/schema names
            query = text(f'SELECT * FROM "{schema}"."{table_name}"')
            result = await conn.execute(query)

            # Convert rows to dictionaries
            columns = result.keys()
            records = []
            for row in result:
                record = dict(zip(columns, row))
                records.append(record)

            logger.info(
                f"Retrieved {len(records)} existing records from {schema}.{table_name}"
            )
            return records

    except SQLAlchemyError as e:
        error_msg = (
            f"Failed to query existing records from {schema}.{table_name}: {str(e)}"
        )
        logger.error(error_msg)
        raise DatabaseSyncError(error_msg) from e


async def upsert_records(
    engine: AsyncEngine,
    table_name: str,
    records: List[Dict[str, Any]],
    schema: str = "network",
    conflict_columns: Optional[List[str]] = None,
) -> UpsertResult:
    """
    Upsert records into a database table with conflict resolution.

    Args:
        engine: SQLAlchemy async engine
        table_name: Name of the target table
        records: List of record dictionaries to upsert
        schema: Database schema name (default: "network")
        conflict_columns: Columns to use for conflict detection (uses primary key if None)

    Returns:
        UpsertResult with operation statistics

    Raises:
        DatabaseSyncError: If upsert operation fails
    """
    start_time = datetime.now()
    result = UpsertResult(table_name=f"{schema}.{table_name}")

    if not records:
        logger.info(f"No records to upsert for {schema}.{table_name}")
        return result

    result.records_processed = len(records)

    try:
        async with engine.begin() as conn:  # Use transaction
            # Get table metadata to determine primary key columns if not specified
            if conflict_columns is None:
                pk_query = text("""
                    SELECT column_name 
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = :schema 
                        AND tc.table_name = :table_name 
                        AND tc.constraint_type = 'PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                """)
                pk_result = await conn.execute(
                    pk_query, {"schema": schema, "table_name": table_name}
                )
                conflict_columns = [row[0] for row in pk_result]

                if not conflict_columns:
                    raise DatabaseSyncError(
                        f"No primary key found for {schema}.{table_name}"
                    )

            # Process records in batches to avoid memory issues
            batch_size = 100
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]

                try:
                    # Use PostgreSQL's ON CONFLICT for upsert
                    await _upsert_batch(
                        conn, schema, table_name, batch, conflict_columns, result
                    )

                except Exception as e:
                    # Handle batch failure - try individual records
                    logger.warning(
                        f"Batch upsert failed for {schema}.{table_name}, trying individual records: {str(e)}"
                    )
                    await _upsert_individual_records(
                        conn, schema, table_name, batch, conflict_columns, result
                    )

            logger.info(
                f"Upsert completed for {schema}.{table_name}: "
                f"{result.records_created} created, {result.records_updated} updated, "
                f"{result.records_failed} failed"
            )

    except SQLAlchemyError as e:
        error_msg = f"Upsert operation failed for {schema}.{table_name}: {str(e)}"
        logger.error(error_msg)
        result.errors.append(error_msg)
        result.records_failed = result.records_processed
        raise DatabaseSyncError(error_msg) from e

    finally:
        end_time = datetime.now()
        result.duration_seconds = (end_time - start_time).total_seconds()

    return result


async def _upsert_batch(
    conn: AsyncConnection,
    schema: str,
    table_name: str,
    records: List[Dict[str, Any]],
    conflict_columns: List[str],
    result: UpsertResult,
) -> None:
    """Perform batch upsert using PostgreSQL's ON CONFLICT."""
    if not records:
        return

    # Get all column names from the first record
    columns = list(records[0].keys())
    conflict_cols_str = ", ".join(f'"{col}"' for col in conflict_columns)
    update_cols_str = ", ".join(
        f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in conflict_columns
    )

    # Build the upsert query
    placeholders = ", ".join(f":{col}" for col in columns)

    if update_cols_str:
        query = text(f"""
            INSERT INTO "{schema}"."{table_name}" ({", ".join(f'"{col}"' for col in columns)})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols_str})
            DO UPDATE SET {update_cols_str}
        """)
    else:
        # If no non-conflict columns to update, just ignore conflicts
        query = text(f"""
            INSERT INTO "{schema}"."{table_name}" ({", ".join(f'"{col}"' for col in columns)})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols_str}) DO NOTHING
        """)

    # Execute batch insert
    await conn.execute(query, records)

    # For batch operations, we can't easily distinguish between inserts and updates
    # So we'll estimate based on existing records (this is a limitation of batch upserts)
    result.records_created += len(records)  # Approximate - could be updates


async def _upsert_individual_records(
    conn: AsyncConnection,
    schema: str,
    table_name: str,
    records: List[Dict[str, Any]],
    conflict_columns: List[str],
    result: UpsertResult,
) -> None:
    """Upsert records individually to handle failures gracefully."""
    for record in records:
        try:
            # Check if record exists
            where_conditions = " AND ".join(
                f'"{col}" = :{col}' for col in conflict_columns
            )
            check_query = text(
                f'SELECT 1 FROM "{schema}"."{table_name}" WHERE {where_conditions}'
            )

            check_params = {col: record[col] for col in conflict_columns}
            exists_result = await conn.execute(check_query, check_params)
            record_exists = exists_result.fetchone() is not None

            if record_exists:
                # Update existing record
                set_clause = ", ".join(
                    f'"{col}" = :{col}'
                    for col in record.keys()
                    if col not in conflict_columns
                )
                if set_clause:  # Only update if there are non-key columns
                    update_query = text(f"""
                        UPDATE "{schema}"."{table_name}" 
                        SET {set_clause}
                        WHERE {where_conditions}
                    """)
                    await conn.execute(update_query, record)
                result.records_updated += 1
            else:
                # Insert new record
                columns = list(record.keys())
                placeholders = ", ".join(f":{col}" for col in columns)
                insert_query = text(f"""
                    INSERT INTO "{schema}"."{table_name}" ({", ".join(f'"{col}"' for col in columns)})
                    VALUES ({placeholders})
                """)
                await conn.execute(insert_query, record)
                result.records_created += 1

        except Exception as e:
            error_msg = f"Failed to upsert individual record: {str(e)}"
            logger.error(error_msg)
            result.errors.append(error_msg)
            result.records_failed += 1


async def sync_table_data(
    engine: AsyncEngine,
    table_name: str,
    data: List[Dict[str, Any]],
    schema: str = "network",
    dry_run: bool = False,
    conflict_columns: Optional[List[str]] = None,
) -> UpsertResult:
    """
    Sync data to a database table with upsert functionality.

    Args:
        engine: SQLAlchemy async engine
        table_name: Name of the target table
        data: List of record dictionaries to sync
        schema: Database schema name (default: "network")
        dry_run: If True, only simulate the operation without making changes
        conflict_columns: Columns to use for conflict detection

    Returns:
        UpsertResult with operation statistics

    Raises:
        DatabaseSyncError: If sync operation fails
    """
    start_time = datetime.now()
    result = UpsertResult(table_name=f"{schema}.{table_name}")

    if not data:
        logger.info(f"No data to sync for {schema}.{table_name}")
        return result

    result.records_processed = len(data)

    if dry_run:
        logger.info(f"DRY RUN: Would sync {len(data)} records to {schema}.{table_name}")

        try:
            # In dry run, we can still check what would happen
            existing_records = await get_existing_records(engine, table_name, schema)
            existing_keys = set()

            # Determine conflict columns if not provided
            if conflict_columns is None:
                async with engine.connect() as conn:
                    pk_query = text("""
                        SELECT column_name 
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu 
                            ON tc.constraint_name = kcu.constraint_name
                        WHERE tc.table_schema = :schema 
                            AND tc.table_name = :table_name 
                            AND tc.constraint_type = 'PRIMARY KEY'
                        ORDER BY kcu.ordinal_position
                    """)
                    pk_result = await conn.execute(
                        pk_query, {"schema": schema, "table_name": table_name}
                    )
                    conflict_columns = [row[0] for row in pk_result]

            # Build set of existing record keys
            for record in existing_records:
                key = tuple(record.get(col) for col in conflict_columns)
                existing_keys.add(key)

            # Estimate creates vs updates
            for record in data:
                key = tuple(record.get(col) for col in conflict_columns)
                if key in existing_keys:
                    result.records_updated += 1
                else:
                    result.records_created += 1

        except Exception as e:
            logger.warning(
                f"Could not analyze dry run for {schema}.{table_name}: {str(e)}"
            )
            # In dry run, we'll just assume all are creates if we can't determine
            result.records_created = len(data)
            result.records_updated = 0
    else:
        # Perform actual upsert
        result = await upsert_records(
            engine, table_name, data, schema, conflict_columns
        )

    end_time = datetime.now()
    result.duration_seconds = (end_time - start_time).total_seconds()

    return result
