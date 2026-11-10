from importlib.metadata import metadata, PackageNotFoundError, version
from packaging.version import Version
from typing import TYPE_CHECKING


def _is_installed(distribution_name: str) -> bool:
    try:
        metadata(distribution_name=distribution_name)
        return True
    except PackageNotFoundError:
        return False


def _is_version_at_least(distribution_name: str, at_least_version: str) -> bool:
    version_str = version(distribution_name=distribution_name)
    return Version(version_str) >= Version(at_least_version)


# Type checkers only see the canonical packages, whereas the actually installed package is resolved at runtime.
if TYPE_CHECKING:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import streaming_bulk
elif _is_installed("elasticsearch8"):
    from elasticsearch8 import Elasticsearch
    from elasticsearch8.helpers import streaming_bulk
elif _is_installed("elasticsearch7"):
    from elasticsearch7 import Elasticsearch
    from elasticsearch7.helpers import streaming_bulk
elif _is_installed("elasticsearch"):
    if not _is_version_at_least("elasticsearch", "7.0.0"):
        raise ImportError("Elasticsearch version 7.0.0 or higher is required.")
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import streaming_bulk
else:
    raise ImportError("Elasticsearch is not installed.")

if TYPE_CHECKING:
    from elasticsearch_dsl import (
        Document,
        InnerDoc,
        Field,
        Object,
        Nested,
        Text,
        Integer,
    )
    from elasticsearch_dsl.document import DocumentOptions
    from elasticsearch_dsl.query import Query
elif _is_installed("elasticsearch8-dsl"):
    if _is_version_at_least("elasticsearch8-dsl", "8.12.0"):
        raise ImportError("Elasticsearch DSL version below 8.12.0 is required.")
    from elasticsearch8_dsl import (
        Document,
        InnerDoc,
        Field,
        Object,
        Nested,
        Text,
        Integer,
    )
    from elasticsearch8_dsl.document import DocumentOptions
    from elasticsearch8_dsl.query import Query
elif _is_installed("elasticsearch7-dsl"):
    from elasticsearch7_dsl import (
        Document,
        InnerDoc,
        Field,
        Object,
        Nested,
        Text,
        Integer,
    )
    from elasticsearch7_dsl.document import DocumentOptions
    from elasticsearch7_dsl.query import Query
elif _is_installed("elasticsearch-dsl"):
    if not _is_version_at_least("elasticsearch-dsl", "7.0.0"):
        raise ImportError("Elasticsearch DSL version 7.0.0 or higher is required.")
    if _is_version_at_least("elasticsearch-dsl", "8.12.0"):
        raise ImportError("Elasticsearch DSL version below 8.12.0 is required.")
    from elasticsearch_dsl import (
        Document,
        InnerDoc,
        Field,
        Object,
        Nested,
        Text,
        Integer,
    )
    from elasticsearch_dsl.document import DocumentOptions
    from elasticsearch_dsl.query import Query
else:
    Document = NotImplemented
    InnerDoc = NotImplemented
    Field = NotImplemented
    DocumentOptions = NotImplemented
    Object = NotImplemented
    Nested = NotImplemented
    Text = NotImplemented
    Integer = NotImplemented
    Query = NotImplemented

__all__ = [
    "Elasticsearch",
    "streaming_bulk",
    "Document",
    "InnerDoc",
    "Field",
    "Object",
    "Nested",
    "Text",
    "Integer",
    "Query",
    "DocumentOptions",
]
