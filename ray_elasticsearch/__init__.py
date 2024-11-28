from functools import cached_property
from importlib.metadata import PackageNotFoundError, version
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
)

if TYPE_CHECKING:
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from pandas import DataFrame
from pyarrow import Schema, Table
from ray.data import Datasource, ReadTask, Datasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import BlockMetadata, Block


# Elasticsearch imports (will use major-version-locked package or default):
_es_import_error: Optional[ImportError]
try:
    from elasticsearch8 import Elasticsearch  # type: ignore
    from elasticsearch8.helpers import streaming_bulk  # type: ignore

    _es_import_error = None
except ImportError as e:
    _es_import_error = e
if _es_import_error is not None:
    try:
        from elasticsearch7 import Elasticsearch  # type: ignore
        from elasticsearch7.helpers import streaming_bulk  # type: ignore

        _es_import_error = None
    except ImportError as e:
        _es_import_error = e
if _es_import_error is not None:
    try:
        from elasticsearch import Elasticsearch  # type: ignore
        from elasticsearch.helpers import streaming_bulk  # type: ignore

        _es_import_error = None
    except ImportError as e:
        _es_import_error = e
if _es_import_error is not None:
    raise _es_import_error


# Elasticsearch DSL imports (will use major-version-locked package or default):
_es_dsl_import_error: Optional[ImportError]
try:
    from elasticsearch_dsl8 import Document  # type: ignore
    from elasticsearch_dsl8.query import Query  # type: ignore

    _es_dsl_import_error = None
except ImportError as e:
    _es_dsl_import_error = e
if _es_dsl_import_error is not None:
    try:
        from elasticsearch7_dsl import Document  # type: ignore
        from elasticsearch7_dsl.query import Query  # type: ignore

        _es_dsl_import_error = None
    except ImportError as e:
        _es_dsl_import_error = e
if _es_dsl_import_error is not None:
    try:
        from elasticsearch_dsl import Document  # type: ignore
        from elasticsearch_dsl.query import Query  # type: ignore

        _es_dsl_import_error = None
    except ImportError as e:
        _es_dsl_import_error = e
_es_dsl_available = _es_dsl_import_error is None


# Try to determine package version.
try:
    __version__ = version("ray-elasticsearch")
except PackageNotFoundError:
    pass


# Determine index and query typing based on Elasticsearch DSL availability.
if _es_dsl_available or TYPE_CHECKING:
    IndexType: TypeAlias = Union[type[Document], str]
    QueryType: TypeAlias = Union[Query, Mapping[str, Any]]
else:
    IndexType: TypeAlias = str  # type: ignore
    QueryType: TypeAlias = Mapping[str, Any]  # type: ignore


class ElasticsearchDatasource(Datasource):
    _index: IndexType
    _query: Optional[QueryType]
    _keep_alive: str
    _chunk_size: int
    _source_fields: Optional[Iterable[str]]
    _meta_fields: Optional[Iterable[str]]
    _meta_prefix: str
    _schema: Optional[Schema]
    _client_kwargs: dict[str, Any]

    def __init__(
        self,
        index: IndexType,
        query: Optional[QueryType] = None,
        keep_alive: str = "5m",
        chunk_size: int = 1000,
        source_fields: Optional[Iterable[str]] = None,
        meta_fields: Optional[Iterable[str]] = None,
        meta_prefix: str = "_",
        schema: Optional[Schema] = None,
        **client_kwargs,
    ) -> None:
        super().__init__()
        self._index = index
        self._query = query
        self._keep_alive = keep_alive
        self._chunk_size = chunk_size
        self._source_fields = source_fields
        self._meta_fields = meta_fields
        self._meta_prefix = meta_prefix
        self._schema = schema
        self._client_kwargs = client_kwargs

    @cached_property
    def _elasticsearch(self) -> Elasticsearch:
        return Elasticsearch(**self._client_kwargs)

    @cached_property
    def _index_name(self) -> str:
        return (
            self._index
            if isinstance(self._index, str)
            else self._index()._get_index(required=True)  # type: ignore
        )

    @cached_property
    def _source_field_set(self) -> Optional[AbstractSet[str]]:
        if self._source_fields is None:
            return None
        return set(self._source_fields)

    @cached_property
    def _meta_field_set(self) -> Optional[AbstractSet[str]]:
        if self._meta_fields is None:
            return None
        return set(self._meta_fields)

    @cached_property
    def _query_dict(self) -> Optional[Mapping[str, Any]]:
        if self._query is None:
            return None
        elif _es_dsl_available and isinstance(self._query, Query):
            return self._query.to_dict()
        else:
            return self._query

    def schema(self) -> Optional[Union[type, Schema]]:
        return self._schema

    @cached_property
    def _num_rows(self) -> int:
        return self._elasticsearch.count(
            index=self._index_name,
            body=(
                {
                    "query": self._query_dict,
                }
                if self._query_dict is not None
                else {}
            ),
        )["count"]

    def num_rows(self) -> int:
        return self._num_rows

    @cached_property
    def _estimated_inmemory_data_size(self) -> Optional[int]:
        stats = self._elasticsearch.indices.stats(
            index=self._index_name,
            metric="store",
        )
        if "store" not in stats["_all"]["total"]:
            return None
        return stats["_all"]["total"]["store"]["total_data_set_size_in_bytes"]

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._estimated_inmemory_data_size

    @staticmethod
    def _get_read_task(
        pit_id: str,
        query: Optional[Mapping[str, Any]],
        slice_id: int,
        slice_max: int,
        chunk_size: int,
        source_field_set: Optional[AbstractSet[str]],
        meta_field_set: Optional[AbstractSet[str]],
        meta_prefix: str,
        schema: Optional[Schema],
        client_kwargs: dict,
    ) -> ReadTask:
        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=schema,
            input_files=None,
            exec_stats=None,
        )

        def transform_row(row: Mapping[str, Any]) -> dict[str, Any]:
            meta: MutableMapping[str, Any] = {
                f"{meta_prefix}{key.removeprefix('_')}": value
                for key, value in row.items()
                if key.startswith("_")
            }
            if meta_field_set is not None:
                meta = {
                    key: value
                    for key, value in meta.items()
                    if key.removeprefix(meta_prefix) in meta_field_set
                }

            source: Mapping[str, Any] = {
                key: value for key, value in row.get("_source", {}).items()
            }
            if source_field_set is not None:
                source = {
                    key: value
                    for key, value in source.items()
                    if key in source_field_set
                }

            return {
                **meta,
                **source,
            }

        def iter_blocks() -> Iterator[Table]:
            elasticsearch = Elasticsearch(**client_kwargs)
            search_after: Any = None
            while True:
                response = elasticsearch.search(
                    pit={"id": pit_id},
                    query=query,
                    slice={"id": slice_id, "max": slice_max},
                    size=chunk_size,
                    search_after=search_after,
                    sort=["_shard_doc"],
                )
                hits: Sequence[Mapping[str, Any]] = response["hits"]["hits"]
                if len(hits) == 0:
                    break
                search_after = max(hit["sort"] for hit in hits)
                rows: list[dict[str, Any]] = [
                    transform_row(row)
                    for row in hits
                ]
                yield Table.from_pylist(
                    mapping=rows,
                    schema=(
                        schema
                        if schema is not None and isinstance(schema, Schema)
                        else None
                    ),
                )

        return ReadTask(
            read_fn=iter_blocks,
            metadata=metadata,
        )

    def get_read_tasks(self, parallelism: int) -> list[ReadTask]:
        pit_id: str = self._elasticsearch.open_point_in_time(
            index=self._index_name,
            keep_alive=self._keep_alive,
        )["id"]
        try:
            return [
                self._get_read_task(
                    pit_id=pit_id,
                    query=self._query_dict,
                    slice_id=i,
                    slice_max=parallelism,
                    chunk_size=self._chunk_size,
                    source_field_set=self._source_field_set,
                    meta_field_set=self._meta_field_set,
                    meta_prefix=self._meta_prefix,
                    client_kwargs=self._client_kwargs,
                    schema=self._schema,
                )
                for i in range(parallelism)
            ]
        except Exception as e:
            self._elasticsearch.close_point_in_time(body={"id": pit_id})
            raise e

    @property
    def supports_distributed_reads(self) -> bool:
        return True


OpType: TypeAlias = Literal["index", "create", "update", "delete"]


class ElasticsearchDatasink(Datasink):
    _index: IndexType
    _op_type: Optional[OpType]
    _chunk_size: int
    _source_fields: Optional[Iterable[str]]
    _meta_fields: Optional[Iterable[str]]
    _meta_prefix: str
    _max_chunk_bytes: int
    _max_retries: int
    _initial_backoff: Union[float, int]
    _max_backoff: Union[float, int]
    _client_kwargs: dict[str, Any]

    def __init__(
        self,
        index: str,
        op_type: Optional[OpType] = None,
        chunk_size: int = 500,
        source_fields: Optional[Iterable[str]] = None,
        meta_fields: Optional[Iterable[str]] = None,
        meta_prefix: str = "_",
        max_chunk_bytes: int = 100 * 1024 * 1024,
        max_retries: int = 0,
        initial_backoff: Union[float, int] = 2,
        max_backoff: Union[float, int] = 600,
        **client_kwargs,
    ) -> None:
        super().__init__()
        self._index = index
        self._op_type = op_type
        self._chunk_size = chunk_size
        self._source_fields = source_fields
        self._meta_fields = meta_fields
        self._meta_prefix = meta_prefix
        self._max_chunk_bytes = max_chunk_bytes
        self._max_retries = max_retries
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._client_kwargs = client_kwargs

    @staticmethod
    def _iter_block_rows(block: Block) -> Iterator[Mapping[str, Any]]:
        if isinstance(block, Table):
            yield from block.to_pylist()
        elif isinstance(block, DataFrame):
            for _, row in block.iterrows():
                yield row.to_dict()
        else:
            raise RuntimeError(f"Unknown block type: {type(block)}")

    @cached_property
    def _elasticsearch(self) -> Elasticsearch:
        return Elasticsearch(**self._client_kwargs)

    @cached_property
    def _index_name(self) -> str:
        return (
            self._index
            if isinstance(self._index, str)
            else self._index()._get_index(required=True)  # type: ignore
        )

    @cached_property
    def _source_field_set(self) -> Optional[AbstractSet[str]]:
        if self._source_fields is None:
            return None
        return set(self._source_fields)

    @cached_property
    def _meta_field_set(self) -> Optional[AbstractSet[str]]:
        if self._meta_fields is None:
            return None
        return set(self._meta_fields)

    def _transform_row(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        meta: MutableMapping[str, Any] = {
            f"_{key.removeprefix(self._meta_prefix)}": value
            for key, value in row.items()
            if key.startswith(self._meta_prefix)
        }
        if self._meta_field_set is not None:
            meta = {
                key: value
                for key, value in meta.items()
                if key.removeprefix("_") in self._meta_field_set
            }
        meta["_index"] = self._index_name
        if self._op_type is not None:
            meta["_op_type"] = self._op_type

        source: Mapping[str, Any] = {
            key: value
            for key, value in row.items()
            if not key.startswith(self._meta_prefix)
        }
        if self._source_field_set is not None:
            source = {
                key: value
                for key, value in source.items()
                if key in self._source_field_set
            }

        return {
            "_source": source,
            **meta,
        }

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        results = streaming_bulk(
            client=self._elasticsearch,
            actions=(
                self._transform_row(row)
                for block in blocks
                for row in self._iter_block_rows(block)
            ),
            chunk_size=self._chunk_size,
            max_chunk_bytes=self._max_chunk_bytes,
            raise_on_error=True,
            raise_on_exception=True,
            max_retries=self._max_retries,
            initial_backoff=self._initial_backoff,
            max_backoff=self._max_backoff,
        )
        for _ in results:
            pass

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def num_rows_per_write(self) -> Optional[int]:
        return None
