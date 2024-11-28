from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Mapping,
    Union,
)

if TYPE_CHECKING:
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias


from ray_elasticsearch.elasticsearch_compat import ELASTICSEARCH_DSL_AVAILABLE


# Determine index and query typing based on Elasticsearch DSL availability.
if ELASTICSEARCH_DSL_AVAILABLE or TYPE_CHECKING:
    from ray_elasticsearch.elasticsearch_compat import Document, Query

    IndexType: TypeAlias = Union[type[Document], str]
    QueryType: TypeAlias = Union[Query, Mapping[str, Any]]
else:
    IndexType: TypeAlias = str  # type: ignore
    QueryType: TypeAlias = Mapping[str, Any]  # type: ignore


OpType: TypeAlias = Literal["index", "create", "update", "delete"]
