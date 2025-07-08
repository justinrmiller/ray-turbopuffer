"""
Turbopuffer Datasource for Ray Data

This module provides a custom datasource implementation for reading data from
turbopuffer, a vector database built on object storage.
"""

import os
import logging
from typing import Any, Dict, Iterable, List, Optional, Union, Callable

import pyarrow as pa
import pandas as pd

from ray.data import Dataset
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.block import Block, BlockMetadata
from ray.util.annotations import PublicAPI

try:
    from turbopuffer import Turbopuffer
except ImportError:
    raise ImportError(
        "turbopuffer package is required. Install it with: pip install turbopuffer"
    )

from turbopuffer.types.row import Row

logger = logging.getLogger(__name__)


class TurbopufferDatasource(Datasource):
    """
    A Ray Data datasource for reading data from turbopuffer vector database.

    This datasource allows you to read data from turbopuffer namespaces into Ray datasets,
    supporting vector search, full-text search, filtering, and attribute-based queries.

    Args:
        namespace: The turbopuffer namespace to read from
        api_key: turbopuffer API key. If None, reads from TURBOPUFFER_API_KEY env var
        region: turbopuffer region (default: "gcp-us-central1")
        query_vector: Vector for similarity search (optional)
        filters: Filters to apply to the query (optional)
        rank_by: Ranking/ordering specification (optional)
        top_k: Maximum number of results per query (default: 1000)
        include_attributes: List of attributes to include, or True for all
        vector_encoding: Vector encoding format ("float" or "base64", default: "float")
        consistency_level: Read consistency ("strong" or "eventual", default: "strong")
        distance_metric: Vector similarity metric ("cosine_distance" or "euclidean_squared")
        transform_fn: Optional function to transform results before returning

    Example:
        >>> import ray
        >>> from src.turbopuffer_datasource import TurbopufferDatasource
        >>>
        >>> # Read all documents from a namespace
        >>> datasource = TurbopufferDatasource(
        ...     namespace="my_documents",
        ...     include_attributes=True
        ... )
        >>> ds = ray.data.read_datasource(datasource)
        >>>
        >>> # Vector similarity search
        >>> query_vector = [0.1, 0.2, 0.3, 0.4]
        >>> datasource = TurbopufferDatasource(
        ...     namespace="my_documents",
        ...     rank_by=("vector", "ANN", query_vector),
        ...     top_k=100,
        ... )
        >>> ds = ray.data.read_datasource(datasource)
        >>>
        >>> # Filtered search
        >>> datasource = TurbopufferDatasource(
        ...     namespace="my_documents",
        ...     filters=["category", "Eq", "tech"],
        ...     rank_by=["timestamp", "desc"],
        ...     top_k=50
        ... )
        >>> ds = ray.data.read_datasource(datasource)
    """

    def __init__(
        self,
        namespace: str,
        api_key: Optional[str] = None,
        region: str = "gcp-us-central1",
        query_vector: Optional[List[float]] = None,
        filters: Optional[Union[List, Dict]] = None,
        rank_by: Optional[List[str]] = None,
        top_k: int = 1000,
        include_attributes=None,
        vector_encoding: str = "float",
        consistency_level: str = "strong",
        distance_metric: str = "cosine_distance",
        transform_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    ):
        if include_attributes is None:
            include_attributes = List("id")

        if distance_metric not in ["cosine_distance", "euclidean_squared"]:
            raise ValueError(
                "distance_metric must be 'cosine_distance' or 'euclidean_squared'"
            )

        if vector_encoding not in ["float", "base64"]:
            raise ValueError("vector_encoding must be 'float' or 'base64'")

        if consistency_level not in ["strong", "eventual"]:
            raise ValueError("consistency_level must be 'strong' or 'eventual'")

        self.namespace = namespace
        self.api_key = api_key or os.environ.get("TURBOPUFFER_API_KEY")
        self.region = region
        self.query_vector = query_vector
        self.filters = filters
        self.rank_by = rank_by
        self.top_k = top_k
        self.include_attributes = include_attributes
        self.vector_encoding = vector_encoding
        self.consistency_level = consistency_level
        self.distance_metric = distance_metric
        self.transform_fn = transform_fn

        if not self.api_key:
            raise ValueError(
                "API key must be provided either as parameter or TURBOPUFFER_API_KEY env var"
            )

        # Initialize client for metadata operations
        self._client = Turbopuffer(api_key=self.api_key, region=self.region)

        # Cache for namespace metadata
        self._namespace_size = None
        self._estimated_size_bytes = None

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        return f"Turbopuffer(namespace={self.namespace})"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """
        Estimate the in-memory data size for the query results.

        Returns:
            Estimated size in bytes, or None if unknown
        """
        if self._estimated_size_bytes is not None:
            return self._estimated_size_bytes

        try:
            # Get approximate namespace size from a sample query
            sample_query = self._build_query_params(top_k=1)

            client = Turbopuffer(api_key=self.api_key, region=self.region)
            response = client.namespace(self.namespace).query(**sample_query)

            if hasattr(response, "stats") and hasattr(
                response.stats, "approx_namespace_size"
            ):
                self._namespace_size = response.stats.approx_namespace_size

                # Estimate based on expected result size
                expected_results = min(self.top_k, self._namespace_size)

                # Rough estimate: 1KB per document (adjustable based on actual data)
                estimated_doc_size = 1024

                self._estimated_size_bytes = expected_results * estimated_doc_size

            else:
                # Fallback estimate
                self._estimated_size_bytes = self.top_k * 1024

        except Exception as e:
            logger.warning(f"Failed to estimate data size: {e}")
            self._estimated_size_bytes = self.top_k * 1024

        return self._estimated_size_bytes

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """
        Generate a single read task (no parallelism supported).

        Args:
            parallelism: Requested read parallelism (ignored)

        Returns:
            List containing a single read task
        """
        return [
            TurbopufferReadTask(
                namespace=self.namespace,
                api_key=self.api_key,
                region=self.region,
                query_params=self._build_query_params(self.top_k),
                transform_fn=self.transform_fn,
                task_id=0,
            )
        ]

    def _build_query_params(self, top_k: int) -> Dict[str, Any]:
        """Build query parameters for turbopuffer API call."""
        params = {
            "namespace": self.namespace,
            "top_k": top_k if top_k else self.top_k,
            "include_attributes": self.include_attributes,
            "consistency": {"level": self.consistency_level},
        }

        # Add vector search parameters
        if self.query_vector is not None:
            params["query_vector"] = self.query_vector
            params["distance_metric"] = self.distance_metric

        # Add vector encoding
        if self.vector_encoding:
            params["vector_encoding"] = self.vector_encoding

        # Add filters
        if self.filters is not None:
            params["filters"] = self.filters

        # Add ranking/ordering
        if self.rank_by is not None:
            params["rank_by"] = self.rank_by

        return params

    @property
    def supports_distributed_reads(self) -> bool:
        """Turbopuffer datasource does not support distributed reads."""
        return False


class TurbopufferReadTask(ReadTask):
    """
    A read task for reading data from turbopuffer.

    This task executes a single query against turbopuffer and returns the results
    as Ray Data blocks.
    """

    def __init__(
        self,
        namespace: str,
        api_key: str,
        region: str,
        query_params: Dict[str, Any],
        transform_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        task_id: int = 0,
    ):
        self.namespace = namespace
        self.api_key = api_key
        self.region = region
        self.query_params = query_params
        self.transform_fn = transform_fn
        self.task_id = task_id

        # Initial metadata estimate
        self._metadata = BlockMetadata(
            num_rows=None,  # Will be set after query
            size_bytes=None,  # Will be estimated
            schema=None,  # Will be inferred from data
            input_files=None,
            exec_stats=None,
        )

    def __call__(self) -> Iterable[Block]:
        """Execute the read task and return data blocks."""
        try:
            # Initialize client for this task
            client = Turbopuffer(api_key=self.api_key, region=self.region)

            # Execute query
            logger.debug(
                f"Task {self.task_id}: Executing query on namespace {self.namespace}"
            )
            response = client.namespace(self.namespace).query(**self.query_params)

            # Extract documents from response
            documents = []
            if hasattr(response, "rows") and response.rows:
                print(f"Found {len(response.rows)} data blocks")
                documents = response.rows

            if not documents:
                # Return empty block if no results
                empty_df = pd.DataFrame()
                yield pa.Table.from_pandas(empty_df)
                return

            # Convert to pandas DataFrame
            df = self._documents_to_dataframe(documents)

            # Apply transformation if provided
            if self.transform_fn:
                df = self.transform_fn(df)

            # Convert to Arrow table and yield as block
            if not df.empty:
                table = pa.Table.from_pandas(df)

                # Update metadata
                self._metadata = BlockMetadata(
                    num_rows=len(df),
                    size_bytes=table.nbytes,
                    schema=table.schema,
                    input_files=None,
                    exec_stats=None,
                )

                yield table
            else:
                # Yield empty table if DataFrame is empty
                empty_table = pa.Table.from_pandas(pd.DataFrame())
                yield empty_table

        except Exception as e:
            logger.error(f"Task {self.task_id}: Failed to read from turbopuffer: {e}")
            raise

    def _documents_to_dataframe(self, documents: List[Row]) -> pd.DataFrame:
        """Convert turbopuffer documents to pandas DataFrame."""
        if not documents:
            return pd.DataFrame()

        # Extract all unique keys from documents
        all_keys = set()
        for doc in documents:
            all_keys.update(doc.dict().keys())

        # Build DataFrame with consistent columns
        data = {}
        for key in sorted(all_keys):  # Sort for consistent column order
            values = []
            for doc in documents:
                value = doc.dict().get(key)
                # Handle special distance field
                if key == "$dist" and value is not None:
                    value = float(value)
                values.append(value)
            data[key] = values

        df = pd.DataFrame(data)

        # Clean up column names (remove $ prefix for distance)
        if "$dist" in df.columns:
            df = df.rename(columns={"$dist": "distance"})

        return df
