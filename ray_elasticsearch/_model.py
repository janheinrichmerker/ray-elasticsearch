from typing import Any, Literal, Mapping, TypeAlias, Union, TYPE_CHECKING

from pyarrow import Schema

from ray_elasticsearch._compat import Document, Query

# Determine index and query typing based on Elasticsearch DSL availability.
if (Document is not NotImplemented and Query is not NotImplemented) or TYPE_CHECKING:
    from ray_elasticsearch._compat import Document, Query

    IndexType: TypeAlias = type[Document] | str
    QueryType: TypeAlias = Query | Mapping[str, Any]
    SchemaType: TypeAlias = Schema | type[Document]
else:
    IndexType: TypeAlias = str
    QueryType: TypeAlias = Mapping[str, Any]
    SchemaType: TypeAlias = Schema


OpType: TypeAlias = Literal["index", "create", "update", "delete"]
