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

    def search(self, search_after: Optional[Any] = None, **kwargs: Any) -> Any:
        if search_after is not None:
            return {"hits": {"hits": []}}
        return {
            "hits": {"hits": [{**hit, "sort": [i]} for i, hit in enumerate(self.hits)]}
        }


@fixture
def fake_elasticsearch(monkeypatch: MonkeyPatch) -> type[FakeElasticsearch]:
    fake = type("_FakeElasticsearch", (FakeElasticsearch,), {})
    monkeypatch.setattr("ray_elasticsearch._source.Elasticsearch", fake)
    return fake


def read_table(datasource: ElasticsearchDatasource) -> Table:
    return concat_tables(
        [
            cast(Table, block)
            for task in datasource.get_read_tasks(parallelism=1)
            for block in task()
        ]
    )
