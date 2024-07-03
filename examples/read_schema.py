from os import environ
from pyarrow import field, schema, struct, int32
from ray import init
from ray.data import read_datasource
from ray_elasticsearch import ElasticsearchDatasource

init()
source = ElasticsearchDatasource(
    index=environ["ELASTICSEARCH_INDEX"],
    client_kwargs=dict(
        hosts=environ["ELASTICSEARCH_HOST"],
        http_auth=(
            environ["ELASTICSEARCH_USERNAME"],
            environ["ELASTICSEARCH_PASSWORD"],
        ),
    ),
    schema=schema([
        field(
            name="_source",
            type=struct([
                field(name="id", type=int32(), nullable=False)
            ]),
            nullable=False,
        )
    ])
)
print(f"Num rows: {source.num_rows()}")
res = read_datasource(source)\
    .map(lambda x: x["_source"])\
    .sum("id")
print(f"Read complete. Sum: {res}")
