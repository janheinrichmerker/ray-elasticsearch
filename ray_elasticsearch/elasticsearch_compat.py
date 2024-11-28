from typing import Any, Optional
from typing_extensions import TypeAlias  # type: ignore


# Elasticsearch imports (will use major-version-locked package or default):
_es_import_error: Optional[ImportError]
try:
    from elasticsearch8 import Elasticsearch as Elasticsearch  # type: ignore
    from elasticsearch8.helpers import streaming_bulk as streaming_bulk  # type: ignore

    _es_import_error = None
except ImportError as e:
    _es_import_error = e
if _es_import_error is not None:
    try:
        from elasticsearch7 import Elasticsearch as Elasticsearch  # type: ignore
        from elasticsearch7.helpers import streaming_bulk as streaming_bulk  # type: ignore

        _es_import_error = None
    except ImportError as e:
        _es_import_error = e
if _es_import_error is not None:
    try:
        from elasticsearch import Elasticsearch as Elasticsearch  # type: ignore
        from elasticsearch.helpers import streaming_bulk as streaming_bulk  # type: ignore

        _es_import_error = None
    except ImportError as e:
        _es_import_error = e
if _es_import_error is not None:
    raise _es_import_error


# Elasticsearch DSL imports (will use major-version-locked package or default):
_es_dsl_import_error: Optional[ImportError]
try:
    from elasticsearch_dsl8 import Document as Document  # type: ignore
    from elasticsearch_dsl8.query import Query as Query  # type: ignore

    _es_dsl_import_error = None
except ImportError as e:
    _es_dsl_import_error = e
if _es_dsl_import_error is not None:
    try:
        from elasticsearch7_dsl import Document as Document  # type: ignore
        from elasticsearch7_dsl.query import Query as Query  # type: ignore

        _es_dsl_import_error = None
    except ImportError as e:
        _es_dsl_import_error = e
if _es_dsl_import_error is not None:
    try:
        from elasticsearch_dsl import Document as Document  # type: ignore
        from elasticsearch_dsl.query import Query as Query  # type: ignore

        _es_dsl_import_error = None
    except ImportError as e:
        _es_dsl_import_error = e
if _es_dsl_import_error is not None:
    Document: TypeAlias = Any  # type: ignore
    Query: TypeAlias = Any  # type: ignore
ELASTICSEARCH_DSL_AVAILABLE = _es_dsl_import_error is None
