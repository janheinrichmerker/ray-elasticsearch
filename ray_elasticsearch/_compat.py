from typing import Optional, TYPE_CHECKING


# Elasticsearch imports (will use major-version-locked package or default)
_import_error: Optional[ImportError] = ImportError()
if _import_error is not None:
    try:
        from elasticsearch8 import Elasticsearch as Elasticsearch  # type: ignore[no-redef]
        from elasticsearch8.helpers import streaming_bulk as streaming_bulk  # type: ignore[no-redef]

        _import_error = None
    except ImportError as e:
        _import_error = e
if _import_error is not None:
    try:
        from elasticsearch7 import Elasticsearch as Elasticsearch  # type: ignore[no-redef,assignment]
        from elasticsearch7.helpers import streaming_bulk as streaming_bulk  # type: ignore[no-redef,assignment]

        _import_error = None
    except ImportError as e:
        _import_error = e
if _import_error is not None:
    try:
        from elasticsearch6 import Elasticsearch as Elasticsearch  # type: ignore[no-redef,assignment]
        from elasticsearch6.helpers import streaming_bulk as streaming_bulk  # type: ignore[no-redef,assignment]

        _import_error = None
    except ImportError as e:
        _import_error = e
if _import_error is not None:
    try:
        from elasticsearch import Elasticsearch as Elasticsearch  # type: ignore[no-redef,assignment]
        from elasticsearch.helpers import streaming_bulk as streaming_bulk  # type: ignore[no-redef,assignment]

        _import_error = None
    except ImportError as e:
        _import_error = e
if _import_error is not None:
    raise _import_error


# Elasticsearch DSL imports (will use major-version-locked package or default):
_import_error_dsl: Optional[ImportError] = ImportError()
if _import_error_dsl is not None:
    try:
        from elasticsearch8_dsl import Document as Document  # type: ignore[no-redef]
        from elasticsearch8_dsl.query import Query as Query  # type: ignore[no-redef]

        _import_error_dsl = None
    except ImportError as e:
        _import_error_dsl = e
if _import_error_dsl is not None:
    try:
        from elasticsearch7_dsl import Document as Document  # type: ignore[no-redef,assignment]
        from elasticsearch7_dsl.query import Query as Query  # type: ignore[no-redef,assignment]

        _import_error_dsl = None
    except ImportError as e:
        _import_error_dsl = e
if _import_error_dsl is not None:
    try:
        from elasticsearch6_dsl import Document as Document  # type: ignore[no-redef,assignment]
        from elasticsearch6_dsl.query import Query as Query  # type: ignore[no-redef,assignment]

        _import_error_dsl = None
    except ImportError as e:
        _import_error_dsl = e
if _import_error_dsl is not None:
    try:
        from elasticsearch_dsl import Document as Document  # type: ignore[no-redef,assignment]
        from elasticsearch_dsl.query import Query as Query  # type: ignore[no-redef,assignment]

        _import_error_dsl = None
    except ImportError as e:
        _import_error_dsl = e
if _import_error_dsl is not None and not TYPE_CHECKING:
    Document = NotImplemented  # type: ignore
    Query = NotImplemented  # type: ignore
