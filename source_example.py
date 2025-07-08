"""
Example demonstrating TurbopufferDatasink and TurbopufferDatasource
showing the complete write-then-read workflow with different features.
"""

import ray
from src.turbopuffer_datasource import TurbopufferDatasource


if __name__ == "__main__":
    """
    Example of TurbopufferDatasink and TurbopufferDatasource showing various read scenarios.
    """

    print("\nVector similarity search...")

    query_vector = [0.1] * 64

    vector_search_datasource = TurbopufferDatasource(
        namespace="streaming_documents",
        filters=["And", [["batch_id", "Gte", 2]]],
        rank_by=("vector", "ANN", query_vector),
        top_k=5,
        include_attributes=[
            "id",
            "batch_id",
            "text",
            "text_length",
        ],  # add "vector" to retrieve the vector
    )

    vector_search_ds = ray.data.read_datasource(vector_search_datasource)
    vector_search_ds.show()

    print("\nFiltered search...")

    filtered_datasource = TurbopufferDatasource(
        namespace="streaming_documents",
        filters=["And", [["batch_id", "Gte", 2]]],
        rank_by=["batch_id", "asc"],
        top_k=5,
        include_attributes=["id", "batch_id", "text", "text_length"],
    )

    filtered_ds = ray.data.read_datasource(filtered_datasource)
    filtered_ds.show()
