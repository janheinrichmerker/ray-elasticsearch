from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ray-elasticsearch")
except PackageNotFoundError:
    pass


from functools import cached_property
from itertools import chain
from typing import Any, Iterable, Iterator, Literal, Mapping, Optional, Union
from typing_extensions import TypeAlias

from pandas import DataFrame
from pyarrow import Schema, Table
from ray.data import Datasource, ReadTask, Datasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import BlockMetadata, Block

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


class ElasticsearchDatasource(Datasource):
    _index: str
    _query: Optional[Mapping[str, Any]]
    _keep_alive: str
    _chunk_size: int
    _client_kwargs: dict[str, Any]
    _schema: Optional[Union[type, Schema]]

    def __init__(
        self,
        index: str,
        query: Optional[Mapping[str, Any]] = None,
        keep_alive: str = "5m",
        chunk_size: int = 1000,
        client_kwargs: dict[str, Any] = {},
        schema: Optional[Union[type, Schema]] = None,
    ) -> None:
        super().__init__()
        self._index = index
        self._query = query
        self._keep_alive = keep_alive
        self._chunk_size = chunk_size
        self._client_kwargs = client_kwargs
        self._schema = schema

    @property
    def _elasticsearch(self) -> Elasticsearch:
        return Elasticsearch(**self._client_kwargs)

    def schema(self) -> Optional[Union[type, Schema]]:
        return self._schema

    @cached_property
    def _num_rows(self) -> int:
        return self._elasticsearch.count(
            index=self._index,
            body={
                "query": self._query,
            } if self._query is not None else {},
        )["count"]

    def num_rows(self) -> int:
        return self._num_rows

    @cached_property
    def _estimated_inmemory_data_size(self) -> int:
        stats = self._elasticsearch.indices.stats(
            index=self._index,
            metric="store",
        )
        return stats["_all"]["total"]["store"]["total_data_set_size_in_bytes"]

    def estimate_inmemory_data_size(self) -> int:
        return self._estimated_inmemory_data_size

    @staticmethod
    def _get_read_task(
        pit_id: str,
        query: Optional[Mapping[str, Any]],
        slice_id: int,
        slice_max: int,
        chunk_size: int,
        client_kwargs: dict[str, Any],
        schema: Optional[Union[type, Schema]],
    ) -> ReadTask:
        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=schema,
            input_files=None,
            exec_stats=None,
        )

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
                hits = response["hits"]["hits"]
                if len(hits) == 0:
                    break
                yield Table.from_pylist(
                    mapping=hits,
                    schema=(
                        schema
                        if schema is not None and isinstance(schema, Schema)
                        else None
                    ),
                )
                search_after = max(
                    hit["sort"]
                    for hit in hits
                )

        return ReadTask(
            read_fn=iter_blocks,
            metadata=metadata,
        )

    def get_read_tasks(self, parallelism: int) -> list[ReadTask]:
        pit_id: str = self._elasticsearch.open_point_in_time(
            index=self._index,
            keep_alive=self._keep_alive,
        )["id"]
        try:
            return [
                self._get_read_task(
                    pit_id=pit_id,
                    query=self._query,
                    slice_id=i,
                    slice_max=parallelism,
                    chunk_size=self._chunk_size,
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
    _index: str
    _op_type: Optional[OpType]
    _chunk_size: int
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
        max_chunk_bytes: int = 100 * 1024 * 1024,
        max_retries: int = 0,
        initial_backoff: Union[float, int] = 2,
        max_backoff: Union[float, int] = 600,
        client_kwargs: dict[str, Any] = {},
    ) -> None:
        super().__init__()
        self._index = index
        self._op_type = op_type
        self._chunk_size = chunk_size
        self._max_chunk_bytes = max_chunk_bytes
        self._max_retries = max_retries
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._client_kwargs = client_kwargs

    @staticmethod
    def _iter_block_rows(block: Block) -> Iterator[dict]:
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

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        rows: Iterable[dict] = chain.from_iterable(
            self._iter_block_rows(block)
            for block in blocks
        )
        rows = (
            {
                "_index": self._index,
                **row,
            }
            for row in rows
        )
        if self._op_type is not None:
            rows = (
                {
                    "_op_type": self._op_type,
                    **row,
                }
                for row in rows
            )
        results = streaming_bulk(
            client=self._elasticsearch,
            actions=rows,
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
if _es_dsl_import_error is None:

    class ElasticsearchDslDatasource(ElasticsearchDatasource):
        def __init__(
            self,
            index: Union[type[Document], str],
            query: Optional[Query] = None,
            keep_alive: str = "5m",
            chunk_size: int = 1000,
            client_kwargs: dict[str, Any] = {},
            schema: Optional[Union[type, Schema]] = None,
        ) -> None:
            super().__init__(
                index=(
                    index if isinstance(index, str) else
                    index()._get_index(required=True)  # type: ignore
                ),
                query=(
                    query.to_dict()
                    if query is not None
                    else None
                ),  # type: ignore
                keep_alive=keep_alive,
                chunk_size=chunk_size,
                client_kwargs=client_kwargs,
                # TODO: Infer schema from document type if not given.
                schema=schema,
            )

    class ElasticsearchDslDatasink(ElasticsearchDatasink):
        def __init__(
            self,
            index: Union[type[Document], str],
            op_type: Optional[OpType] = None,
            chunk_size: int = 500,
            max_chunk_bytes: int = 100 * 1024 * 1024,
            max_retries: int = 0,
            initial_backoff: Union[float, int] = 2,
            max_backoff: Union[float, int] = 600,
            client_kwargs: dict[str, Any] = {},
        ) -> None:
            super().__init__(
                index=(
                    index if isinstance(index, str) else
                    index()._get_index(required=True)  # type: ignore
                ),
                op_type=op_type,
                chunk_size=chunk_size,
                max_chunk_bytes=max_chunk_bytes,
                max_retries=max_retries,
                initial_backoff=initial_backoff,
                max_backoff=max_backoff,
                client_kwargs=client_kwargs,
            )
