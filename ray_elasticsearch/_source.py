from functools import cached_property
from typing import (
    AbstractSet,
    Any,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    TYPE_CHECKING,
)
from packaging.version import Version

from pyarrow import Schema, Table
from ray import __version__ as ray_version
from ray.data import Datasource, ReadTask
from ray.data.block import BlockMetadata

from ray_elasticsearch._compat import Elasticsearch, Query, Document
from ray_elasticsearch._schema import (
    complete_schema,
    schema_from_document,
    schema_from_elasticsearch,
)
from ray_elasticsearch._model import IndexType, QueryType, SchemaType


class ElasticsearchDatasource(Datasource):
    _index: Optional[IndexType]
    _query: Optional[QueryType]
    _keep_alive: str
    _chunk_size: int
    _source_fields: Optional[Iterable[str]]
    _meta_fields: Optional[Iterable[str]]
    _schema: Optional[SchemaType]
    _client_kwargs: dict[str, Any]

    def __init__(
        self,
        index: Optional[IndexType] = None,
        query: Optional[QueryType] = None,
        keep_alive: str = "5m",
        chunk_size: int = 1000,
        source_fields: Optional[Iterable[str]] = None,
        meta_fields: Optional[Iterable[str]] = None,
        schema: Optional[SchemaType] = None,
        **client_kwargs,
    ) -> None:
        super().__init__()
        self._index = index
        self._query = query
        self._keep_alive = keep_alive
        self._chunk_size = chunk_size
        self._source_fields = source_fields
        self._meta_fields = meta_fields
        self._schema = schema
        self._client_kwargs = client_kwargs

    @cached_property
    def _elasticsearch(self) -> Elasticsearch:
        return Elasticsearch(**self._client_kwargs)

    @cached_property
    def _index_name(self) -> Optional[str]:
        if self._index is None:
            return None
        elif isinstance(self._index, str):
            return self._index
        else:
            return self._index()._get_index(required=True)  # type: ignore

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
        elif Query is not NotImplemented and isinstance(self._query, Query):
            return self._query.to_dict()
        elif isinstance(self._query, dict):
            return self._query
        else:
            return None

    @cached_property
    def _safe_schema(self) -> Optional[Schema]:
        if self._schema is not None and isinstance(self._schema, Schema):
            return self._schema

        base_schema: Schema
        if (
            Document is not NotImplemented
            and self._schema is not None
            and isinstance(self._schema, type)
            and issubclass(self._schema, Document)
        ):
            base_schema = schema_from_document(
                document=self._schema,
            )
        if (
            Document is not NotImplemented
            and self._index is not None
            and isinstance(self._index, type)
            and issubclass(self._index, Document)
        ):
            base_schema = schema_from_document(
                document=self._index,
            )
        elif self._index_name is not None:
            base_schema = schema_from_elasticsearch(
                elasticsearch=self._elasticsearch,
                index=self._index_name,
            )
        else:
            return None

        return complete_schema(
            base_schema=base_schema,
            source_fields=self._source_fields,
            meta_fields=self._meta_fields,
        )

    def schema(self) -> Optional[Schema]:
        return self._safe_schema

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
        schema: Optional[Schema],
        client_kwargs: dict,
    ) -> ReadTask:
        if Version(ray_version) >= Version("2.47.0") or TYPE_CHECKING:
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                input_files=None,
                exec_stats=None,
            )
        else:
            metadata = BlockMetadata(  # type: ignore[call-arg]
                num_rows=None,
                size_bytes=None,
                schema=schema,
                input_files=None,
                exec_stats=None,
            )

        def transform_row(row: Mapping[str, Any]) -> dict[str, Any]:
            meta: Mapping[str, Any] = {
                key: value
                for key, value in row.items()
                if key.startswith("_") and key != "_source"
            }
            if meta_field_set is not None:
                meta = {
                    key: value
                    for key, value in meta.items()
                    if key.removeprefix("_") in meta_field_set
                }

            source_dict: Mapping[str, Any] = row.get("_source", {})
            source: Mapping[str, Any] = {
                key: value for key, value in source_dict.items()
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
                    seq_no_primary_term=True,
                    version=True,
                )
                hits: Sequence[Mapping[str, Any]] = response["hits"]["hits"]
                if len(hits) == 0:
                    break
                search_after = max(hit["sort"] for hit in hits)
                rows: list[dict[str, Any]] = [transform_row(row) for row in hits]
                try:
                    yield Table.from_pylist(
                        mapping=rows,
                        schema=schema,
                    )
                except Exception as e:
                    print(
                        f"Failed to create Table with schema {schema}, from rows: {rows}."
                    )
                    raise e

        return ReadTask(
            read_fn=iter_blocks,
            metadata=metadata,
        )

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
    ) -> list[ReadTask]:
        pit_id: str = self._elasticsearch.open_point_in_time(
            index=self._index_name if self._index_name is not None else "_all",
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
                    client_kwargs=self._client_kwargs,
                    schema=self._safe_schema,
                )
                for i in range(parallelism)
            ]
        except Exception as e:
            self._elasticsearch.close_point_in_time(body={"id": pit_id})
            raise e

    def get_name(self) -> str:
        if self._index_name is not None:
            return f"Elasticsearch({self._index_name})"
        else:
            return "Elasticsearch"

    @property
    def supports_distributed_reads(self) -> bool:
        return True
