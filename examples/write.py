from os import environ
from ray import init
from ray.data import range
from ray_elasticsearch import ElasticsearchDatasink

init()
sink = ElasticsearchDatasink(
    index=environ["ELASTICSEARCH_INDEX"],
    client_kwargs=dict(
        hosts=environ["ELASTICSEARCH_HOST"],
        http_auth=(
            environ["ELASTICSEARCH_USERNAME"],
            environ["ELASTICSEARCH_PASSWORD"],
        ),
    ),
)

range(10_000)\
    .map(lambda x: {"_source": x})\
    .write_datasink(sink)
print("Write complete.")
