from os import environ
from elasticsearch_dsl.query import Range
from ray import init
from ray.data import read_datasource
from ray_elasticsearch import ElasticsearchDatasource

init()

source = ElasticsearchDatasource(
    index=environ["ELASTICSEARCH_INDEX"],
    hosts=environ["ELASTICSEARCH_HOST"],
    http_auth=(
        environ["ELASTICSEARCH_USERNAME"],
        environ["ELASTICSEARCH_PASSWORD"],
    ),
    query=Range(value={"lt":  100}),
)

print(f"Num rows: {source.num_rows()}")
res = read_datasource(source, concurrency=100)\
    .sum("value")
print(f"Read complete. Sum: {res}")  # 4950
