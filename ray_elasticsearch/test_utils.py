from typing import Any, Sequence

from pandas import DataFrame
from pyarrow import Table
from pytest import mark, skip
from ray.data.block import BlockAccessor, DataBatch

from ray_elasticsearch import unwrap_document, unwrap_documents
from ray_elasticsearch._compat import Document, InnerDoc, Integer, Nested, Text


def test_unwrap_document() -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _Document(Document):
        field = Text(required=True)

    def _expected(row: dict[str, Any]) -> dict[str, Any]:
        return {**row, "custom": row["field"]}

    @unwrap_document(_Document)
    def _actual(row: dict[str, Any], document: _Document) -> dict[str, Any]:
        return {**row, "custom": document.field}

    data = _Document(field="example").to_dict(include_meta=True)
    data = {
        **{k: v for k, v in data.items() if k != "_source"},
        **data["_source"],
    }

    expected = _expected(data)
    actual = _actual(data)

    assert expected == actual


def test_unwrap_documents() -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _Document(Document):
        field = Text(required=True)

    def _expected(batch: DataFrame) -> DataFrame:
        batch = batch.copy()
        batch["custom"] = [row["field"] for _, row in batch.iterrows()]
        return batch

    @unwrap_documents(_Document)
    def _actual(batch: DataFrame, documents: Sequence[_Document]) -> DataFrame:
        batch = batch.copy()
        batch["custom"] = [document.field for document in documents]
        return batch

    data = [_Document(field=f"example{i}").to_dict(include_meta=True) for i in range(5)]
    data = [
        {**{k: v for k, v in doc.items() if k != "_source"}, **doc["_source"]}
        for doc in data
    ]
    df = DataFrame(data)

    expected = _expected(df)
    actual = _actual(df)

    assert expected.equals(actual)


@mark.parametrize("batch_format", ["pyarrow", "pandas", "numpy"])
def test_unwrap_documents_batch_formats(batch_format: str) -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _Item(InnerDoc):
        name = Text(required=True)

    class _Document(Document):
        title = Text(required=True)
        items = Nested(_Item)
        views = Integer()

    # Lists of objects become NumPy arrays, and missing numbers become NaN in pandas and NumPy batches.
    table = Table.from_pylist(
        [
            {
                "_id": "1",
                "title": "First",
                "items": [{"name": "x"}, {"name": "y"}],
                "views": 3,
            },
            {"_id": "2", "title": "Second", "items": [], "views": None},
        ]
    )
    batch = BlockAccessor.for_block(table).to_batch_format(batch_format)

    actual: list[tuple[str, dict[str, Any]]] = []

    @unwrap_documents(_Document)
    def _map(batch: DataBatch, documents: Sequence[_Document]) -> DataBatch:
        actual.extend((document.meta.id, document.to_dict()) for document in documents)
        return batch

    assert _map(batch) is batch
    assert actual == [
        ("1", {"title": "First", "items": [{"name": "x"}, {"name": "y"}], "views": 3}),
        ("2", {"title": "Second"}),
    ]
