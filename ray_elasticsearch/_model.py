from typing import Any, Literal, Mapping, Union

from pyarrow import Schema
from typing_extensions import TypeAlias  # type: ignore

from ray_elasticsearch._compat import Document, Query


# Determine index and query typing based on Elasticsearch DSL availability.
if Document is not NotImplemented and Query is not NotImplemented:
    from ray_elasticsearch._compat import Document, Query

    IndexType: TypeAlias = Union[type[Document], str]  # type: ignore[no-redef]
    QueryType: TypeAlias = Union[Query, Mapping[str, Any]]  # type: ignore[no-redef]
    SchemaType: TypeAlias = Union[Schema, type[Document]]  # type: ignore[no-redef]
else:
    IndexType: TypeAlias = str  # type: ignore[no-redef,misc]
    QueryType: TypeAlias = Mapping[str, Any]  # type: ignore[no-redef,misc]
    SchemaType: TypeAlias = Schema  # type: ignore[no-redef,misc]


OpType: TypeAlias = Literal["index", "create", "update", "delete"]
