# ray-turbopuffer
A Ray Source/Sink implementation for the TurboPuffer vector store.

Data Sink:
```
Running Dataset: dataset_2_0. Active & requested resources: 2/8 CPU, 2.6KB/1.0GB object store:  98%|████████████████████████████████████████▏| 196/200 [00:23<00:00, 9.68 row/s]2025-07-07 20:13:08,698rINFO streaming_executor.py:227 -- ✔️  Dataset dataset_2_0 execution finished in 24.27 seconds███████████████████████ | 196/200 [00:23<00:00, 9.68 row/s]
✔️  Dataset dataset_2_0 execution finished in 24.27 seconds: 100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:24<00:00, 8.24 row/s]
- Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 2.6KB object store: 100%|████████████████████████████████████████████████| 200/200 [00:24<00:00, 8.24 row/s]
2025-07-07 20:13:08,750 INFO dataset.py:4601 -- Data sink Turbopuffer(namespace=streaming_documents) finished. 250 rows and 146.9KB data written.
```
