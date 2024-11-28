from functools import cached_property
from typing import (
    AbstractSet,
    Any,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Union,
)

from pandas import DataFrame
from pyarrow import Table
from ray.data import Datasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block

from ray_elasticsearch.elasticsearch_compat import (
    Elasticsearch,
    streaming_bulk,
)
from ray_elasticsearch.model import IndexType, OpType


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
