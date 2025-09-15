from typing import (
    Any,
    Literal,
    Mapping,
    Union,
    TYPE_CHECKING,
)
from typing_extensions import TypeAlias  # type: ignore

from ray_elasticsearch._compat import Document, Query


# Determine index and query typing based on Elasticsearch DSL availability.
if (Document is not NotImplemented and Query is not NotImplemented) or TYPE_CHECKING:
    from ray_elasticsearch._compat import Document, Query

    IndexType: TypeAlias = Union[type[Document], str]  # type: ignore[no-redef]
    QueryType: TypeAlias = Union[Query, Mapping[str, Any]]  # type: ignore[no-redef]
elif Document is not NotImplemented and Query is not NotImplemented:
    IndexType: TypeAlias = Union[type[Document], str]  # type: ignore[no-redef,misc]
    QueryType: TypeAlias = Union[Query, Mapping[str, Any]]  # type: ignore[no-redef,misc]
else:
    IndexType: TypeAlias = str  # type: ignore[no-redef,misc]
    QueryType: TypeAlias = Mapping[str, Any]  # type: ignore[no-redef,misc]


OpType: TypeAlias = Literal["index", "create", "update", "delete"]
