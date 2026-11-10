from importlib.metadata import PackageNotFoundError, version

# Try to determine package version.
try:
    __version__ = version("ray-elasticsearch")
except PackageNotFoundError:
    pass

from ray_elasticsearch._model import IndexType, QueryType, SchemaType, OpType
from ray_elasticsearch._sink import ElasticsearchDatasink
from ray_elasticsearch._source import ElasticsearchDatasource
from ray_elasticsearch._utils import unwrap_document, unwrap_documents

__all__ = [
    "IndexType",
    "QueryType",
    "SchemaType",
    "OpType",
    "ElasticsearchDatasink",
    "ElasticsearchDatasource",
    "unwrap_document",
    "unwrap_documents",
]
