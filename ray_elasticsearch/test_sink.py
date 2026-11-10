from typing import Any, Iterable, Iterator, Mapping

from pyarrow import Table
from pytest import MonkeyPatch, mark
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block

from ray_elasticsearch import ElasticsearchDatasink
from ray_elasticsearch.conftest import FakeElasticsearch


@mark.parametrize("block_type", ["pyarrow", "pandas"])
def test_write(monkeypatch: MonkeyPatch, block_type: str) -> None:
    recorded: list[Mapping[str, Any]] = []

    def _streaming_bulk(
        actions: Iterable[Mapping[str, Any]], **kwargs: Any
    ) -> Iterator[Any]:
        for action in actions:
            recorded.append(action)
            yield True, action

    monkeypatch.setattr("ray_elasticsearch._sink.Elasticsearch", FakeElasticsearch)
    monkeypatch.setattr("ray_elasticsearch._sink.streaming_bulk", _streaming_bulk)

    table = Table.from_pylist(
        [
            {"_id": "1", "title": "First", "tags": ["a", "b"], "views": 3},
            {"_id": "2", "title": "Second", "tags": [], "views": None},
        ]
    )
    # In pandas blocks, lists become NumPy arrays and missing numbers become NaN.
    block: Block = table if block_type == "pyarrow" else table.to_pandas()
    datasink = ElasticsearchDatasink(index="index")

    datasink.write([block], TaskContext(task_idx=0, op_name="Write"))

    assert recorded == [
        {
            "_id": "1",
            "_index": "index",
            "_source": {"title": "First", "tags": ["a", "b"], "views": 3},
        },
        {
            "_id": "2",
            "_index": "index",
            "_source": {"title": "Second", "tags": [], "views": None},
        },
    ]
