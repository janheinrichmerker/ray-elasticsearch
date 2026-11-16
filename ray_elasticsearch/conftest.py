from typing import Any, Mapping, Optional, Sequence, cast

from pyarrow import Table, concat_tables
from pytest import MonkeyPatch, fixture

from ray_elasticsearch import ElasticsearchDatasource


class FakeElasticsearch:
    """
    Minimal in-memory stand-in for the Elasticsearch client, returning a fixed mapping and fixed hits.
    """

    mappings: Mapping[str, Any] = {"properties": {}}
    hits: Sequence[Mapping[str, Any]] = ()

    def __init__(self, **kwargs: Any) -> None:
        self.indices = self

    def get_mapping(self, index: str) -> Mapping[str, Any]:
        return {index: {"mappings": self.mappings}}

    def open_point_in_time(self, index: str, keep_alive: str) -> Mapping[str, Any]:
        return {"id": "pit"}

    def close_point_in_time(self, body: Mapping[str, Any]) -> None:
        pass

    def msearch(self, body: Sequence[Mapping[str, Any]]) -> Any:
        return {"responses": [self.search(**search) for search in body[1::2]]}

    def search(
        self,
        search_after: Any | None = None,
        slice: Mapping[str, int] | None = None,
        size: int = 10,
        **kwargs: Any,
    ) -> Any:
        if slice is not None and slice["max"] < 2:
            # Like Elasticsearch, reject slicing into a single slice.
            raise ValueError("max must be greater than 1")
        # Distribute the hits round-robin across slices.
        hits = [
            {**hit, "sort": [i]}
            for i, hit in enumerate(self.hits)
            if slice is None or i % slice["max"] == slice["id"]
        ]
        total = {"value": len(hits), "relation": "eq"}
        if search_after is not None:
            hits = [hit for hit in hits if hit["sort"] > search_after]
        return {"hits": {"total": total, "hits": hits[:size]}}


@fixture
def fake_elasticsearch(monkeypatch: MonkeyPatch) -> type[FakeElasticsearch]:
    fake = type("_FakeElasticsearch", (FakeElasticsearch,), {})
    monkeypatch.setattr("ray_elasticsearch._source.Elasticsearch", fake)
    return fake


def read_table(datasource: ElasticsearchDatasource, parallelism: int = 2) -> Table:
    return concat_tables(
        [
            cast(Table, block)
            for task in datasource.get_read_tasks(parallelism=parallelism)
            for block in task()
        ]
    )
