from os import environ
from elasticsearch_dsl.query import Range
from ray import init
from ray.data import read_datasource
from ray_elasticsearch import ElasticsearchDslDatasource

init()
source = ElasticsearchDslDatasource(
    index=environ["ELASTICSEARCH_INDEX"],
    client_kwargs=dict(
        hosts=environ["ELASTICSEARCH_HOST"],
        http_auth=(
            environ["ELASTICSEARCH_USERNAME"],
            environ["ELASTICSEARCH_PASSWORD"],
        ),
    ),
    query=Range(id={"lte":  100}),
)
print(f"Num rows: {source.num_rows()}")
res = read_datasource(source)\
    .map(lambda x: x["_source"])\
    .sum("id")
print(f"Read complete. Sum: {res}")
