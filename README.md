# Ray-Turbopuffer

A Ray Data integration for [Turbopuffer](https://turbopuffer.com), a high-performance vector database built on object storage.

## Installation

## Quick Start

### Writing Data

```python
import ray
from ray_turbopuffer import TurbopufferDatasink

# Create sample data
data = [
    {"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Hello world", "category": "greeting"},
    {"id": 2, "vector": [0.4, 0.5, 0.6], "text": "Goodbye", "category": "farewell"}
]

# Write to Turbopuffer
ds = ray.data.from_items(data)
datasink = TurbopufferDatasink(
    namespace="my_documents",
    distance_metric="cosine_distance"
)
ds.write_datasink(datasink)
```

### Reading Data

```python
from ray_turbopuffer import TurbopufferDatasource

# Vector similarity search
datasource = TurbopufferDatasource(
    namespace="my_documents",
    rank_by=("vector", "ANN", [0.1, 0.2, 0.3]),
    top_k=10,
    include_attributes=["id", "text", "category"]
)
ds = ray.data.read_datasource(datasource)
ds.show()

# Filtered search
datasource = TurbopufferDatasource(
    namespace="my_documents",
    filters=["category", "Eq", "greeting"],
    rank_by=["id", "asc"]
)
ds = ray.data.read_datasource(datasource)
```

## Configuration

### Authentication

Set your Turbopuffer API key:

```bash
export TURBOPUFFER_API_KEY=your-api-key
```

Or pass it directly:

```python
datasink = TurbopufferDatasink(
    namespace="my_documents",
    api_key="your-api-key"
)
```

### Datasink Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `namespace` | str | required | Turbopuffer namespace to write to |
| `distance_metric` | str | "cosine_distance" | Vector similarity metric |
| `batch_size` | int | 1000 | Rows per write batch |
| `upsert_mode` | str | "upsert" | How to handle conflicts ("upsert" or "patch") |
| `transform_fn` | callable | None | Function to transform data before writing |

### Datasource Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `namespace` | str | required | Turbopuffer namespace to read from |
| `rank_by` | list/tuple | None | Ranking specification for results |
| `filters` | list | None | Query filters |
| `top_k` | int | 1000 | Maximum results to return |
| `include_attributes` | list/bool | ["id"] | Attributes to retrieve |

## Advanced Examples

### Data Transformation

```python
def enrich_data(df):
    df["timestamp"] = pd.Timestamp.now()
    df["word_count"] = df["text"].str.split().str.len()
    return df

datasink = TurbopufferDatasink(
    namespace="enriched_docs",
    transform_fn=enrich_data
)
```

### Complex Filtering

```python
# AND condition with multiple filters
datasource = TurbopufferDatasource(
    namespace="products",
    filters=["And", [
        ["price", "Gte", 10],
        ["price", "Lte", 100],
        ["category", "Eq", "electronics"]
    ]],
    rank_by=["price", "asc"]
)
```

### Distributed Writing

```python
# Configure Ray resources for parallel writes
ds.write_datasink(
    datasink,
    ray_remote_args={"num_cpus": 0.5},
    concurrency=10
)
```

## Performance Tips

1. **Batch Size**: Adjust `batch_size` based on your document size. Smaller batches for large documents, larger batches for small documents.

2. **Concurrency**: Set `concurrency` in `write_datasink()` to control parallelism.

3. **Vector Encoding**: Use `vector_encoding="base64"` for more efficient network transfer of large vectors.

4. **Streaming**: For real-time data, use smaller batch sizes and higher concurrency.

## Requirements

- Python >= 3.10
- Ray >= 2.47.1
- Turbopuffer >= 0.5.10

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
