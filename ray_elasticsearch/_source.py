from collections.abc import Iterable, Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from datetime import datetime, timezone
from functools import cached_property
from itertools import chain, repeat
from typing import TYPE_CHECKING, Any

from packaging.version import Version
from pyarrow import DataType, Schema, Table
from pyarrow.types import is_list, is_struct, is_timestamp
from ray import __version__ as ray_version
from ray.data import DataContext, Datasource, ReadTask
from ray.data.block import BlockMetadata

from ray_elasticsearch._compat import Document, Elasticsearch, Query
from ray_elasticsearch._model import IndexType, QueryType, SchemaType
from ray_elasticsearch._schema import (
    complete_schema,
    schema_from_document,
    schema_from_elasticsearch,
)
from ray_elasticsearch._utils import _index_name


def _parse_date(value: Any) -> datetime:
    # Numbers are epoch milliseconds, strings ISO 8601 dates, and dates without time zone are in UTC (like in Elasticsearch).
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)
    if isinstance(value, str) and value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


class ElasticsearchDatasource(Datasource):
    _index: IndexType | None
    _query: QueryType | None
    _keep_alive: str
    _chunk_size: int
    _source_fields: Iterable[str] | None
    _meta_fields: Iterable[str] | None
    _schema: SchemaType | None
    _client_kwargs: dict[str, Any]

    def __init__(
        self,
        index: IndexType | None = None,
        query: QueryType | None = None,
        keep_alive: str = "5m",
        chunk_size: int = 1000,
        source_fields: Iterable[str] | None = None,
        meta_fields: Iterable[str] | None = None,
        schema: SchemaType | None = None,
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
    def _index_name(self) -> str | None:
        return _index_name(self._index)

    @cached_property
    def _source_field_set(self) -> AbstractSet[str] | None:
        if self._source_fields is None:
            return None
        return set(self._source_fields)

    @cached_property
    def _meta_field_set(self) -> AbstractSet[str] | None:
        if self._meta_fields is None:
            return None
        return set(self._meta_fields)

    @cached_property
    def _query_dict(self) -> Mapping[str, Any] | None:
        if self._query is None:
            return None
        elif Query is not NotImplemented and isinstance(self._query, Query):
            return self._query.to_dict()
        elif isinstance(self._query, dict):
            return self._query
        else:
            return None

    @cached_property
    def _safe_schema(self) -> Schema | None:
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
        elif (
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

    def schema(self) -> Schema | None:
        return self._safe_schema

    @cached_property
    def _pit_id(self) -> str:
        return self._elasticsearch.open_point_in_time(
            index=self._index_name if self._index_name is not None else "_all",
            keep_alive=self._keep_alive,
        )["id"]

    @cached_property
    def _num_rows(self) -> int:
        return self._elasticsearch.search(
            pit={"id": self._pit_id, "keep_alive": self._keep_alive},
            query=self._query_dict,
            size=0,
            track_total_hits=True,
        )["hits"]["total"]["value"]

    def num_rows(self) -> int:
        return self._num_rows

    @cached_property
    def _estimated_inmemory_data_size(self) -> int | None:
        stats = self._elasticsearch.indices.stats(
            index=self._index_name,
            metric="store",
        )
        if "store" not in stats["_all"]["total"]:
            return None
        return stats["_all"]["total"]["store"]["total_data_set_size_in_bytes"]

    def estimate_inmemory_data_size(self) -> int | None:
        return self._estimated_inmemory_data_size

    @staticmethod
    def _get_read_task(
        pit_id: str,
        keep_alive: str,
        query: Mapping[str, Any] | None,
        slice_id: int | None,
        slice_max: int | None,
        num_rows: int | None,
        row_limit: int | None,
        chunk_size: int,
        source_field_set: AbstractSet[str] | None,
        meta_field_set: AbstractSet[str] | None,
        schema: Schema | None,
        client_kwargs: dict,
    ) -> ReadTask:
        if Version(ray_version) >= Version("2.47.0") or TYPE_CHECKING:
            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                input_files=None,
                exec_stats=None,
            )
        else:
            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                schema=schema,
                input_files=None,
                exec_stats=None,
            )

        def transform_value(value: Any, data_type: DataType) -> Any:
            """
            Recursively transform a value according to the given Arrow data type.

            For example, dates (e.g., ISO strings) are parsed at timestamp fields, which Arrow cannot convert by itself.
            """
            if value is None:
                return None
            elif is_timestamp(data_type):
                return _parse_date(value)
            elif is_struct(data_type) and isinstance(value, Mapping):
                return {
                    field.name: transform_value(value.get(field.name), field.type)
                    for field in data_type.fields
                }
            elif is_list(data_type) and isinstance(value, Sequence):
                return [transform_value(item, data_type.value_type) for item in value]
            return value

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

            source: Mapping[str, Any] = {
                key: transform_value(value, schema.field(key).type)
                if schema is not None and key in schema.names
                else value
                for key, value in row.get("_source", {}).items()
                if source_field_set is None or key in source_field_set
            }

            return {
                **meta,
                **source,
            }

        def iter_blocks() -> Iterator[Table]:
            elasticsearch = Elasticsearch(**client_kwargs)
            search_after: Any = None
            remaining = row_limit
            # Search the whole point in time if not sliced (Elasticsearch rejects a single slice).
            slice_kwargs: dict[str, Any] = (
                {"slice": {"id": slice_id, "max": slice_max}}
                if slice_id is not None and slice_max is not None
                else {}
            )
            while remaining is None or remaining > 0:
                response = elasticsearch.search(
                    pit={"id": pit_id, "keep_alive": keep_alive},
                    query=query,
                    **slice_kwargs,
                    # Do not fetch more documents than the row limit allows.
                    size=chunk_size
                    if remaining is None
                    else min(chunk_size, remaining),
                    search_after=search_after,
                    sort=["_shard_doc"],
                    seq_no_primary_term=True,
                    version=True,
                )
                hits: Sequence[Mapping[str, Any]] = response["hits"]["hits"]
                if len(hits) == 0:
                    break
                search_after = max(hit["sort"] for hit in hits)
                if remaining is not None:
                    remaining -= len(hits)
                rows: list[dict[str, Any]] = [transform_row(row) for row in hits]
                try:
                    yield Table.from_pylist(
                        mapping=rows,
                        schema=schema,
                    )
                except Exception:
                    print(
                        f"Failed to create Table with schema {schema}, from rows: {rows}."
                    )
                    raise

        return ReadTask(
            read_fn=iter_blocks,
            metadata=metadata,
        )

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: DataContext | None = None,
    ) -> list[ReadTask]:
        if parallelism < 1:
            raise ValueError(f"Parallelism must be at least 1, got {parallelism}.")
        pit_id = self._pit_id

        if parallelism == 1:
            # Elasticsearch rejects slicing into a single slice, so read the whole point in time in one unsliced task.
            num_rows = self._num_rows
            if per_task_row_limit is not None:
                num_rows = min(num_rows, per_task_row_limit)
            return [
                self._get_read_task(
                    pit_id=pit_id,
                    keep_alive=self._keep_alive,
                    query=self._query_dict,
                    slice_id=None,
                    slice_max=None,
                    num_rows=num_rows,
                    row_limit=per_task_row_limit,
                    chunk_size=self._chunk_size,
                    source_field_set=self._source_field_set,
                    meta_field_set=self._meta_field_set,
                    client_kwargs=self._client_kwargs,
                    schema=self._safe_schema,
                )
            ]

        slice_count_search_bodies: list[Mapping[str, Any]] = [
            {
                "pit": {"id": pit_id, "keep_alive": self._keep_alive},
                "size": 0,
                "track_total_hits": True,
                # Elasticsearch rejects a null query, so omit it instead.
                **({"query": self._query_dict} if self._query_dict is not None else {}),
                "slice": {"id": i, "max": parallelism},
            }
            for i in range(parallelism)
        ]
        # Requests within a point in time must not name an index, so all headers are empty.
        slice_count_searches = list(
            chain.from_iterable(zip(repeat({}), slice_count_search_bodies))
        )
        slice_counts: list[int | None] = [
            response["hits"]["total"]["value"] if "error" not in response else None
            for response in self._elasticsearch.msearch(body=slice_count_searches)[
                "responses"
            ]
        ]
        if per_task_row_limit is not None:
            # A read task never yields more rows than the row limit.
            slice_counts = [
                min(count, per_task_row_limit) if count is not None else None
                for count in slice_counts
            ]
        return [
            self._get_read_task(
                pit_id=pit_id,
                keep_alive=self._keep_alive,
                query=self._query_dict,
                slice_id=i,
                slice_max=parallelism,
                num_rows=slice_counts[i],
                row_limit=per_task_row_limit,
                chunk_size=self._chunk_size,
                source_field_set=self._source_field_set,
                meta_field_set=self._meta_field_set,
                client_kwargs=self._client_kwargs,
                schema=self._safe_schema,
            )
            for i in range(parallelism)
        ]

    def get_name(self) -> str:
        if self._index_name is not None:
            return f"Elasticsearch({self._index_name})"
        else:
            return "Elasticsearch"

    @property
    def supports_distributed_reads(self) -> bool:
        return True
