from typing import List, Dict

import numpy as np
import pandas as pd

import ray
from src.turbopuffer_datasink import TurbopufferDatasink

if __name__ == "__main__":

    def generate_streaming_data() -> List[Dict]:
        """Simulate streaming data with embeddings from multiple sources."""
        sources = ["news", "blogs", "papers", "social"]
        data = []

        for batch_id in range(5):  # Simulate 5 batches of data
            for i in range(50):  # 50 records per batch
                doc_id = batch_id * 50 + i + 1
                data.append(
                    {
                        "id": doc_id,
                        "vector": np.random.rand(64).tolist(),  # Convert to list
                        "text": f"Document {doc_id} content from batch {batch_id}",
                        "source": str(
                            np.random.choice(sources)
                        ),  # Convert to Python string
                        "batch_id": batch_id,
                        "timestamp": f"2024-07-{(doc_id % 30) + 1:02d}T{(doc_id % 24):02d}:00:00Z",
                    }
                )

        return data

    # Generate streaming data
    streaming_data = generate_streaming_data()
    ds = ray.data.from_items(streaming_data)

    # Custom transformation for data enrichment
    def enrich_streaming_data(df: pd.DataFrame) -> pd.DataFrame:
        """Add computed features to streaming data."""
        # Add text length feature
        df["text_length"] = df["text"].str.len()

        # Add priority based on source
        source_priority = {"news": 1, "papers": 2, "blogs": 3, "social": 4}
        df["priority"] = df["source"].map(source_priority)

        return df

    # Write with optimized batching for streaming data
    turbopuffer_datasink = TurbopufferDatasink(
        namespace="streaming_documents",
        distance_metric="cosine_distance",
        batch_size=5,  # Smaller batches for streaming
        transform_fn=enrich_streaming_data,
    )

    ds.write_datasink(
        turbopuffer_datasink,
        ray_remote_args={"num_cpus": 0.5},  # Lower resource usage per task
        concurrency=10,  # Moderate concurrency for streaming
    )

    stats = ds.stats()
    print(stats)
