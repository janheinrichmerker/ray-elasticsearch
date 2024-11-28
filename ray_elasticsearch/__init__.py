from importlib.metadata import PackageNotFoundError, version

# Try to determine package version.
try:
    __version__ = version("ray-elasticsearch")
except PackageNotFoundError:
    pass

# Re-export names.
from ray_elasticsearch.model import (
    IndexType as IndexType,
    QueryType as QueryType,
    OpType as OpType,
)
from ray_elasticsearch.datasource import (
    ElasticsearchDatasource as ElasticsearchDatasource,
)
from ray_elasticsearch.datasink import ElasticsearchDatasink as ElasticsearchDatasink
