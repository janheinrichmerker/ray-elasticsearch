from os import environ
from ray import init
from ray.data import range
from ray_elasticsearch import ElasticsearchDatasink

init()

sink = ElasticsearchDatasink(
    index=environ["ELASTICSEARCH_INDEX"],
    hosts=environ["ELASTICSEARCH_HOST"],
    http_auth=(
        environ["ELASTICSEARCH_USERNAME"],
        environ["ELASTICSEARCH_PASSWORD"],
    ),
    source_fields=["value"],
    meta_fields=["id"],
)

range(10_000, concurrency=100)\
    .rename_columns({"id": "value"}, concurrency=100) \
    .add_column("_id", lambda df: df["value"], concurrency=100) \
    .write_datasink(sink, concurrency=100)
print("Write complete.")
