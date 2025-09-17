from importlib.metadata import metadata, PackageNotFoundError, version
from packaging.version import Version


def _is_installed(distribution_name: str) -> bool:
    try:
        metadata(distribution_name=distribution_name)
        return True
    except PackageNotFoundError:
        return False


def _is_version_at_least(distribution_name: str, at_least_version: str) -> bool:
    version_str = version(distribution_name=distribution_name)
    return Version(version_str) >= Version(at_least_version)


if _is_installed("elasticsearch8"):
    from elasticsearch8 import Elasticsearch as Elasticsearch  # type: ignore[no-redef]
    from elasticsearch8.helpers import streaming_bulk as streaming_bulk  # type: ignore[no-redef]
elif _is_installed("elasticsearch7"):
    from elasticsearch7 import Elasticsearch as Elasticsearch  # type: ignore[no-redef,assignment]
    from elasticsearch7.helpers import streaming_bulk as streaming_bulk  # type: ignore[no-redef,assignment]
elif _is_installed("elasticsearch"):
    if not _is_version_at_least("elasticsearch", "7.0.0"):
        raise ImportError("Elasticsearch version 7.0.0 or higher is required.")
    from elasticsearch import Elasticsearch as Elasticsearch  # type: ignore[no-redef,assignment]
    from elasticsearch.helpers import streaming_bulk as streaming_bulk  # type: ignore[no-redef,assignment]
else:
    raise ImportError("Elasticsearch is not installed.")

if _is_installed("elasticsearch8-dsl"):
    if _is_version_at_least("elasticsearch8-dsl", "8.12.0"):
        raise ImportError("Elasticsearch DSL version below 8.12.0 is required.")
    from elasticsearch8_dsl import (  # type: ignore[no-redef]
        Document as Document,
        InnerDoc as InnerDoc,
        Field as Field,
        Object as Object,
        Nested as Nested,
        Text as Text,
    )
    from elasticsearch8_dsl.query import Query as Query  # type: ignore[no-redef]
elif _is_installed("elasticsearch7-dsl"):
    from elasticsearch7_dsl import (  # type: ignore[no-redef,assignment]
        Document as Document,
        InnerDoc as InnerDoc,
        Field as Field,
        Object as Object,
        Nested as Nested,
        Text as Text,
    )
    from elasticsearch7_dsl.query import Query as Query  # type: ignore[no-redef,assignment]
elif _is_installed("elasticsearch-dsl"):
    if not _is_version_at_least("elasticsearch-dsl", "7.0.0"):
        raise ImportError("Elasticsearch DSL version 7.0.0 or higher is required.")
    if _is_version_at_least("elasticsearch-dsl", "8.12.0"):
        raise ImportError("Elasticsearch DSL version below 8.12.0 is required.")
    from elasticsearch_dsl import (  # type: ignore[no-redef,assignment]
        Document as Document,
        InnerDoc as InnerDoc,
        Field as Field,
        Object as Object,
        Nested as Nested,
        Text as Text,
    )
    from elasticsearch_dsl.query import Query as Query  # type: ignore[no-redef,assignment]
else:
    Document = NotImplemented  # type: ignore
    InnerDoc = NotImplemented  # type: ignore
    Field = NotImplemented  # type: ignore
    Object = NotImplemented  # type: ignore
    Nested = NotImplemented  # type: ignore
    Text = NotImplemented  # type: ignore
    Query = NotImplemented  # type: ignore
