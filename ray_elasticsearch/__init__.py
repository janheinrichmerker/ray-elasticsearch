from importlib.metadata import PackageNotFoundError, version

# Try to determine package version.
try:
    __version__ = version("ray-elasticsearch")
except PackageNotFoundError:
    pass

# Re-export names.
from ray_elasticsearch._model import (
    IndexType as IndexType,
    QueryType as QueryType,
    SchemaType as SchemaType,
    OpType as OpType,
)
from ray_elasticsearch._sink import ElasticsearchDatasink as ElasticsearchDatasink
from ray_elasticsearch._source import ElasticsearchDatasource as ElasticsearchDatasource
from ray_elasticsearch._utils import (
    unwrap_document as unwrap_document,
    unwrap_documents as unwrap_documents,
)
