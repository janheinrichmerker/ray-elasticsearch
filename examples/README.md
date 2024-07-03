# Examples

1. Configured your Ray cluster address in the `RAY_ADDRESS` environment variable.
2. Adapt the runtime environment in [`env.yml`](env.yml) to include your Elasticsearch host, index, and credentials.
3. Launch the examples like this:

```shell
# Write some test data into the index.
ray job submit --runtime-env examples/env.yml -- python write.py

# Sum the numbers of all documents in the index.
ray job submit --runtime-env examples/env.yml -- python read.py

# Sum the numbers of documents in the index that match a query.
ray job submit --runtime-env examples/env.yml -- python read_query.py

# Sum the numbers of documents in the index that match a query (using the `elasticsearch-dsl` query DSL).
ray job submit --runtime-env examples/env.yml -- python read_query_dsl.py
```
