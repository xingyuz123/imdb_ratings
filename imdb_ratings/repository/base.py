"""
Base repository class for database operations.
"""

from abc import ABC, abstractmethod
from typing import Any
from supabase import Client
from imdb_ratings import logger
from imdb_ratings.config import get_settings
from imdb_ratings.exceptions import DatabaseOperationError

class BaseRepository(ABC):
    """Base class for all repositories."""

    def __init__(self, supabase_client: Client):
        """
        Initialize repository with Supabase client.

        Args:
            supabase_client: Supabase client instance.
        """
        self.client = supabase_client
        self.settings = get_settings()
        self.config = self.settings.supabase

    @property 
    @abstractmethod
    def table_name(self) -> str:
        """Return the table name for the repository."""
        pass

    def fetch_all(self, columns: str = "*") -> list[dict[str, Any]]:
        """
        Fetch all records from the table.

        Args:
            columns: Columns to select (default: "*")

        Returns:
            List of records as dictionaries.
        """

        all_data: list[dict[str, Any]] = []
        offset: int = 0

        logger.info(f"Fetching all data from {self.table_name} table")

        while True:
            try:
                batch_data = (
                    self.client.table(self.table_name)
                    .select(columns)
                    .offset(offset)
                    .limit(self.config.batch_size)
                    .execute()
                    .data
                )

                all_data.extend(batch_data)
                offset += self.config.batch_size

                logger.debug(f"Fetched {len(all_data)} records so far from {self.table_name}")

                # If the batch size is less than the batch size, we've fetched all records
                if len(batch_data) < self.config.batch_size:
                    break
            except Exception as e:
                logger.error(f"Unexpected error fetching from {self.table_name}: {e}")
                raise DatabaseOperationError(f"Failed to fetch data: {e}")

        logger.info(f"Total records fetched from {self.table_name}: {len(all_data)}")
        return all_data
    
    def upsert_batch(self, data: list[dict[str, Any]]) -> None:
        """
        Upsert data in batches.

        Args:
            data: list of dictionaries to upsert.
        """    
        total_records = len(data)

        if total_records == 0:
            logger.warning(f"No data to upsert for {self.table_name}")
            return
        
        logger.info(f"Upserting {total_records} records into {self.table_name} table")
        total_batches = (total_records + self.config.batch_size - 1) // self.config.batch_size

        for i in range(0, total_records, self.config.batch_size):
            batch = data[i:i+self.config.batch_size]
            batch_num = i // self.config.batch_size + 1

            try:
                self.client.table(self.table_name).upsert(batch).execute()
                if total_batches > 1:
                    logger.info(f"Successfully upserted batch {batch_num} of {total_batches}")
            except Exception as e:
                logger.error(f"Error upserting batch {batch_num} into {self.table_name}: {str(e)}")
                raise DatabaseOperationError(f"Failed to upsert batch {batch_num}: {str(e)}")

    def update(self, data: dict[str, Any], filters: dict[str, Any]) -> None:
        """
        Update records matching filters.

        Args:
            data: Data to update.
            filters: Filter conditions (using equality)
        """
        query = self.client.table(self.table_name).update(data)

        for key, value in filters.items():
            query = query.eq(key, value)

        try:
            query.execute()
        except Exception as e:
            logger.error(f"Error updating {self.table_name} table: {str(e)}")
            raise DatabaseOperationError(f"Failed to update {self.table_name}: {str(e)}")