"""
Turbopuffer Datasink for Ray Data

This module provides a custom datasink implementation for writing Ray Data datasets
to turbopuffer, a vector database built on object storage.
"""

import os
import logging
from typing import Any, Dict, Iterable, Optional, Callable

import pandas as pd

from ray.data.datasource.datasink import Datasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor

try:
    from turbopuffer import Turbopuffer
except ImportError:
    raise ImportError(
        "turbopuffer package is required. Install it with: pip install turbopuffer"
    )

logger = logging.getLogger(__name__)


class TurbopufferDatasink(Datasink):
    """
    A Ray Data datasink for writing data to turbopuffer vector database.

    This datasink allows you to write Ray datasets directly to turbopuffer,
    supporting both vector and attribute data with flexible schema configuration.

    Args:
        namespace: The turbopuffer namespace to write to
        api_key: turbopuffer API key. If None, reads from TURBOPUFFER_API_KEY env var
        region: turbopuffer region (default: "gcp-us-central1")
        distance_metric: Vector similarity metric ("cosine_distance" or "euclidean_squared")
        vector_column: Name of the column containing vectors (default: "vector")
        id_column: Name of the column containing document IDs (default: "id")
        upsert_mode: How to handle document conflicts ("upsert" or "patch")
        batch_size: Number of rows to write per batch (default: 1000)
        transform_fn: Optional function to transform data before writing

    Example:
        >>> import ray
        >>> from src.turbopuffer_datasink import TurbopufferDatasink
        >>>
        >>> # Create sample data with vectors and attributes
        >>> data = [
        ...     {
        ...         "id": 1,
        ...         "vector": [0.1, 0.2, 0.3],
        ...         "title": "Document 1",
        ...         "category": "tech"
        ...     },
        ...     {
        ...         "id": 2,
        ...         "vector": [0.4, 0.5, 0.6],
        ...         "title": "Document 2",
        ...         "category": "science"
        ...     }
        ... ]
        >>>
        >>> # Create Ray dataset
        >>> ds = ray.data.from_items(data)
        >>>
        >>> # Write to turbopuffer
        >>> datasink = TurbopufferDatasink(
        ...     namespace="my_documents",
        ...     distance_metric="cosine_distance"
        ... )
        >>> ds.write_datasink(datasink)
    """

    def __init__(
        self,
        namespace: str,
        api_key: Optional[str] = None,
        region: str = "gcp-us-central1",
        distance_metric: str = "cosine_distance",
        vector_column: str = "vector",
        id_column: str = "id",
        upsert_mode: str = "upsert",
        batch_size: int = 1000,
        transform_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    ):
        if distance_metric not in ["cosine_distance", "euclidean_squared"]:
            raise ValueError(
                "distance_metric must be 'cosine_distance' or 'euclidean_squared'"
            )

        if upsert_mode not in ["upsert", "patch"]:
            raise ValueError("upsert_mode must be 'upsert' or 'patch'")

        self.namespace = namespace
        self.api_key = api_key or os.environ.get("TURBOPUFFER_API_KEY")
        self.region = region
        self.distance_metric = distance_metric
        self.vector_column = vector_column
        self.id_column = id_column
        self.upsert_mode = upsert_mode
        self.batch_size = batch_size
        self.transform_fn = transform_fn

        if not self.api_key:
            raise ValueError(
                "API key must be provided either as parameter or TURBOPUFFER_API_KEY env var"
            )

        # Track write statistics
        self._rows_written = 0
        self._batches_written = 0
        self._errors = []

    def on_write_start(self) -> None:
        """Initialize turbopuffer client and prepare for writing."""
        logger.info(f"Starting write to turbopuffer namespace: {self.namespace}")

        # Reset statistics
        self._rows_written = 0
        self._batches_written = 0
        self._errors = []

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Dict[str, Any]:
        """
        Write blocks of data to turbopuffer.

        Args:
            blocks: Iterator of data blocks (Arrow tables or pandas DataFrames)
            ctx: Ray task context

        Returns:
            Dictionary with write statistics
        """
        # Initialize client for this task
        client = Turbopuffer(api_key=self.api_key, region=self.region)

        task_rows_written = 0
        task_batches_written = 0
        task_errors = []

        try:
            for block in blocks:
                # Convert block to pandas DataFrame for easier manipulation
                block_accessor = BlockAccessor.for_block(block)
                df = block_accessor.to_pandas()

                if df.empty:
                    continue

                # Apply transformation function if provided
                if self.transform_fn:
                    df = self.transform_fn(df)

                # Validate required columns
                if self.id_column not in df.columns:
                    raise ValueError(f"ID column '{self.id_column}' not found in data")

                # Check if this is a vector namespace or attribute-only
                has_vectors = self.vector_column in df.columns

                # Process data in batches
                for i in range(0, len(df), self.batch_size):
                    batch_df = df.iloc[i : i + self.batch_size]

                    try:
                        self._write_batch(client, batch_df, has_vectors)
                        task_rows_written += len(batch_df)
                        task_batches_written += 1

                    except Exception as e:
                        error_msg = (
                            f"Error writing batch {task_batches_written}: {str(e)}"
                        )
                        logger.error(error_msg)
                        task_errors.append(error_msg)

        except Exception as e:
            error_msg = f"Critical error in write task: {str(e)}"
            logger.error(error_msg)
            task_errors.append(error_msg)
            raise

        return {
            "rows_written": task_rows_written,
            "batches_written": task_batches_written,
            "errors": task_errors,
        }

    def _write_batch(
        self, client: Turbopuffer, batch_df: pd.DataFrame, has_vectors: bool
    ) -> None:
        """Write a single batch to turbopuffer."""
        # Prepare the write payload
        write_data: dict[Any, Any] = {}

        # Extract document IDs
        ids = batch_df[self.id_column].tolist()
        write_data["id"] = ids

        # Extract vectors if present
        # Extract vectors if present
        if has_vectors:
            vectors = []
            for vector in batch_df[self.vector_column]:
                # Convert numpy arrays to lists if needed
                if hasattr(vector, "tolist"):
                    vectors.append(vector.tolist())
                else:
                    vectors.append(vector)
            write_data["vector"] = vectors

        # Extract attributes (all columns except id and vector)
        attribute_columns = [
            col
            for col in batch_df.columns
            if col not in [self.id_column, self.vector_column]
        ]

        for col in attribute_columns:
            # Convert pandas Series to list, handling NaN values
            values = batch_df[col].where(pd.notna(batch_df[col]), None).tolist()
            write_data[col] = values

        # Prepare write request parameters
        write_params = {
            "namespace": self.namespace,
        }

        # Add distance metric if writing vectors
        if has_vectors:
            write_params["distance_metric"] = self.distance_metric

        # Choose write method based on upsert mode
        if self.upsert_mode == "upsert":
            write_params["upsert_columns"] = write_data
        else:  # patch mode
            write_params["patch_columns"] = write_data

        # Execute the write operation
        response = client.namespace(self.namespace).write(**write_params)

        logger.debug(f"Wrote batch: {response.rows_affected} rows affected.")

    def on_write_complete(self, write_results) -> None:
        """
        Called when all write tasks complete successfully.

        Args:
            write_results: List of results from all write tasks
        """
        # Aggregate statistics from all tasks
        total_rows = sum(
            result.get("rows_written", 0) for result in write_results.write_returns
        )
        total_batches = sum(
            result.get("batches_written") for result in write_results.write_returns
        )
        all_errors = []
        for result in write_results.write_returns:
            all_errors.extend(result["errors"])

        self._rows_written = total_rows
        self._batches_written = total_batches
        self._errors = all_errors

        total_batches = 0

        logger.info(
            f"Write completed successfully: {total_rows} rows written "
            f"in {total_batches} batches to namespace '{self.namespace}'"
        )

        if all_errors:
            logger.warning(f"Write completed with {len(all_errors)} errors")
            for error in all_errors:
                logger.warning(f"  - {error}")

    def on_write_failed(self, error: Exception) -> None:
        """
        Called when write operation fails.

        Args:
            error: The exception that caused the failure
        """
        error_msg = (
            f"Write to turbopuffer namespace '{self.namespace}' failed: {str(error)}"
        )
        logger.error(error_msg)
        self._errors.append(error_msg)

    def get_name(self) -> str:
        """Return a human-readable name for this datasink."""
        return f"Turbopuffer(namespace={self.namespace})"

    @property
    def supports_distributed_writes(self) -> bool:
        """Turbopuffer supports distributed writes across multiple tasks."""
        return True

    @property
    def num_rows_per_write(self) -> Optional[int]:
        """Return the target number of rows per write operation."""
        return self.batch_size

    def get_write_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the write operation.

        Returns:
            Dictionary containing write statistics
        """
        return {
            "rows_written": self._rows_written,
            "batches_written": self._batches_written,
            "error_count": len(self._errors),
            "errors": self._errors,
            "namespace": self.namespace,
            "distance_metric": self.distance_metric,
            "upsert_mode": self.upsert_mode,
        }
